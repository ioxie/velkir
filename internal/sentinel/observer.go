/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sentinel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/resp"
)

// EndpointObservation is one sentinel pod's view captured by the most
// recent pollOnce sweep. Surfaced via Manager.EndpointObservations so
// the controller can write per-pod SentinelQuorum.Status records.
//
// Reachable=false means queryOne returned an error for this endpoint
// (dial / AUTH / SENTINEL command failure); the other fields are
// undefined in that case. PrimaryAddr is the sentinel's `host:port`
// answer to `SENTINEL GET-MASTER-ADDR-BY-NAME <master>`; the
// controller maps it to a pod name when writing SQ.Status.observedPrimary
// by cross-referencing the Valkey pod list.
type EndpointObservation struct {
	Name            string
	PrimaryAddr     string
	QuorumReachable bool
	Reachable       bool
	At              time.Time

	// KnownReplicas / KnownSentinels carry this sentinel's local
	// `num-slaves` / `num-other-sentinels` counts from the same
	// SENTINEL MASTER reply the pull tick already reads for the epoch
	// and down-state flags — no extra round-trip. They are meaningful
	// only when CountsValid is true.
	KnownReplicas  int
	KnownSentinels int
	// CountsValid is true only when the SENTINEL MASTER reply was read
	// AND both count fields (num-slaves, num-other-sentinels) parsed;
	// it implies Reachable==true. When false the two counts are
	// undefined and MUST be ignored by consumers (a RESP3-map /
	// renamed-field / version-skew reply leaves both counts unusable
	// even though the endpoint answered).
	CountsValid bool
}

// Defaults for the observer's pacing knobs. Exported so tests can
// override via Options without exporting global mutable state.
const (
	// PollInterval is the pull-tick cadence. 10s lines up with
	// the 180s failover-timeout floor — three poll cycles inside
	// a worst-case failover gives the operator plenty of
	// resolution for quorum + primary updates without thrashing
	// the sentinels.
	DefaultPollInterval = 10 * time.Second

	// PubsubReadDeadline caps how long we wait between pubsub
	// frames before issuing an in-band PING to distinguish
	// "channel idle" from "TCP gone". Sentinel hello pings on
	// __sentinel__:hello fire roughly every 2s, but those land
	// on a different channel — we explicitly subscribe only to
	// the +switch-master / +failover-end / +odown / +tilt set.
	// 30s is the sentinel client-link reconnect floor, so we want
	// to detect a stuck socket inside that window without firing
	// PING constantly on a healthy idle system.
	DefaultPubsubReadDeadline = 30 * time.Second

	// PingTimeout caps the in-band PING round-trip. If sentinel
	// can't reply within this, the connection is presumed wedged
	// and the observer reconnects.
	DefaultPingTimeout = 5 * time.Second

	// Reconnect backoff: 2s, 4s, 8s, 16s, 32s, 60s (capped).
	reconnectBackoffMin = 2 * time.Second
	reconnectBackoffMax = 60 * time.Second

	// initialPollBackoff is a brief pre-first-tick wait that gives
	// sentinel pods a chance to reach Ready before the first dial
	// attempt. Without it, a Pending sentinel STS at observer start
	// can stall the first pollOnce for the full
	// per-endpoint × dial-timeout budget; the wait is short so
	// cold-start snapshot latency stays bounded. Subsequent ticks
	// fire at PollInterval cadence with no extra wait.
	initialPollBackoff = 1 * time.Second

	// commandDeadline caps a single pull-tick command exchange
	// (dial + AUTH + GET-MASTER + CKQUORUM). 5s is generous on
	// an in-cluster RTT and tight enough to keep the 10s tick
	// from overlapping itself on a slow sentinel.
	commandDeadline = 5 * time.Second
)

// Options configures the observer. Zero-value Options is valid
// and uses the Default* constants above.
type Options struct {
	PollInterval       time.Duration
	PubsubReadDeadline time.Duration
	PingTimeout        time.Duration

	// AuthRetryAttempts is the per-pod max attempt count for
	// SetAuthPassAll's SET+verify round-trip. Zero falls back to
	// DefaultAuthRetryAttempts (3). Exposed so operators can tune
	// up on lossy networks without forking the binary; the
	// backoff sequence reuses authRetryBackoffs' last entry once
	// the configured attempts exceed its length.
	AuthRetryAttempts int
}

func (o Options) withDefaults() Options {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.PubsubReadDeadline <= 0 {
		o.PubsubReadDeadline = DefaultPubsubReadDeadline
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = DefaultPingTimeout
	}
	if o.AuthRetryAttempts <= 0 {
		o.AuthRetryAttempts = DefaultAuthRetryAttempts
	}
	return o
}

// observer is the per-CR worker. Held by the manager; not exported.
//
// Goroutine layout: one PSUBSCRIBE loop per sentinel endpoint
// (up to two — the "primary + warm standby" pattern) plus one
// pull-tick goroutine. Each goroutine reads ctx.Done() for
// shutdown; the manager's Remove cancels the parent context.
//
// The snapshotHolder is the lock-free read API (Snapshot via the
// Manager). The two o_down maps are the mu-guarded shared state:
// odown is fed by the push goroutines (+odown/-odown frames) and
// odownPull is level-reconciled by the pull-tick goroutine
// (reconcileODownPull under o.mu). Every publishLocked caller —
// push-side republish and pull-side alike — copies both maps into
// the snapshot (immutable from the consumer's viewpoint) under the
// mutex.
type observer struct {
	cr         observerKey
	masterName string
	password   string

	endpoints []Endpoint
	opts      Options

	holder   snapshotHolder
	recorder k8sevents.EventRecorder
	crObject *valkeyv1beta1.Valkey

	// events is the manager's shared push channel. The observer sends
	// a GenericEvent for its CR on each topology-changing pubsub event
	// so the reconciler wakes promptly. Send-only; nil in tests that
	// construct an observer without the push half (notify no-ops).
	events chan<- event.GenericEvent

	mu    sync.Mutex
	odown map[string]time.Time
	// odownPull is the pull-side sibling of odown, guarded by the same
	// o.mu. The pull tick level-reconciles it from each sentinel's
	// SENTINEL MASTER `flags` field (rising-edge first-seen stamp,
	// never refreshed; deleted only on a reached-and-clear reading);
	// publishLocked copies it into ObservedPrimary.ODownPull. It
	// corroborates the edge-triggered odown map so a lost -odown frame
	// can still be reconciled away by the next pull tick.
	odownPull map[string]time.Time

	// endpointObs holds the most recent per-endpoint observation set
	// produced by pollOnce — one EndpointObservation per o.endpoints
	// entry, in the same order, with `Reachable=false` for endpoints
	// whose queryOne failed. Stored in an atomic.Value so readers
	// (Manager.EndpointObservations) get a consistent snapshot
	// without taking o.mu. The SentinelQuorum status
	// writer reads this and SSA-patches each per-pod SQ resource so
	// `kubectl get sentinelquorums` finally shows non-empty PRIMARY
	// / QUORUM columns. Each pollOnce overwrites the slice; readers
	// always see the latest complete poll, never a partial one.
	endpointObs atomic.Value // []EndpointObservation

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// newObserver constructs an observer but does NOT start its
// goroutines. Call start() after construction.
func newObserver(cr observerKey, masterName, password string, endpoints []Endpoint, opts Options, recorder k8sevents.EventRecorder, eventCh chan<- event.GenericEvent) *observer {
	return &observer{
		cr:         cr,
		masterName: masterName,
		password:   password,
		endpoints:  endpoints,
		opts:       opts.withDefaults(),
		recorder:   recorder,
		events:     eventCh,
		odown:      make(map[string]time.Time),
		odownPull:  make(map[string]time.Time),
		// crObject is the recorder target — the observer never
		// has a live object to point at (it doesn't read kube
		// state directly), so we synthesize a minimal one with
		// just the namespace/name. Recorder treats it as an
		// ObjectReference target.
		crObject: &valkeyv1beta1.Valkey{},
	}
}

// start spins up the per-endpoint pubsub goroutines plus the
// pull-tick goroutine. ctx cancellation stops them all; wait()
// blocks until every goroutine has returned.
func (o *observer) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	o.cancel = cancel
	// crObject's TypeMeta is left empty — Recorder.Eventf only
	// needs ObjectReference fields (Kind, Name, Namespace, UID)
	// and synthesizes Kind from registered scheme. Setting
	// Name/Namespace is the minimum the recorder uses.
	o.crObject.Name = o.cr.Name
	o.crObject.Namespace = o.cr.Namespace

	// Bootstrap the snapshot with a "no observation yet"
	// placeholder so consumers get a deterministic
	// Quorum=Unknown / QuorumOK=false answer instead of !Present
	// until the first poll cycle lands. Source=none signals the
	// observer is alive but hasn't observed anything yet.
	o.holder.store(ObservedPrimary{
		Source:    SourceNone,
		Quorum:    QuorumStatusUnknown,
		UpdatedAt: time.Now(),
	})

	// Subscribe up to two endpoints — primary + warm standby.
	// Past index 1 we don't open a third subscription; the pull
	// tick is the catch-all.
	subTargets := o.endpoints
	if len(subTargets) > 2 {
		subTargets = subTargets[:2]
	}
	for _, ep := range subTargets {
		o.wg.Add(1)
		go o.runSubscribe(ctx, ep)
	}

	o.wg.Add(1)
	go o.runPoll(ctx)
}

// stop cancels and waits for goroutines to drain.
func (o *observer) stop() {
	if o.cancel != nil {
		o.cancel()
	}
	o.wg.Wait()
}

// snapshot returns the most recent ObservedPrimary published by
// the observer. !ok means the observer has not yet started or has
// been removed; consumers must treat this identically to
// QuorumOK=false (refuse to relabel).
func (o *observer) snapshot() (ObservedPrimary, bool) {
	return o.holder.load()
}

// runSubscribe is the long-lived PSUBSCRIBE loop for one sentinel
// endpoint. Reconnects with exponential backoff; emits per-pod
// connection metrics + the SentinelObserverReconnect event on
// each successful re-establishment.
func (o *observer) runSubscribe(ctx context.Context, ep Endpoint) {
	defer o.wg.Done()
	logger := log.FromContext(ctx).WithValues("cr", o.cr.String(), "sentinel", ep.Name, "addr", ep.Addr)

	backoff := reconnectBackoffMin
	first := true
	for {
		if ctx.Err() != nil {
			return
		}

		err := o.subscribeOnce(ctx, ep, &first, &backoff)
		// subscribeOnce returns nil only on context cancellation.
		// Any other return is a connection-level fault — log,
		// emit the lost-message event if appropriate, and back
		// off before retrying.
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			operatormetrics.SentinelObserverConnected.WithLabelValues(o.cr.Namespace, o.cr.Name, ep.Name).Set(0)
			return
		}
		if err != nil {
			operatormetrics.SentinelObserverConnected.WithLabelValues(o.cr.Namespace, o.cr.Name, ep.Name).Set(0)
			logger.V(1).Info("subscribe loop ended; backing off", "err", err.Error(), "backoff", backoff.String())
			if o.recorder != nil {
				o.recorder.Eventf(o.crObject, nil, corev1.EventTypeWarning,
					string(events.SentinelPubsubMessageLost), "SentinelPubsubLossObserve",
					"sentinel pubsub connection to %s lost: %s; reconnecting in %s",
					ep.Name, err.Error(), backoff,
				)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > reconnectBackoffMax {
			backoff = reconnectBackoffMax
		}
	}
}

// subscribeOnce dials, authenticates, subscribes, and reads pubsub
// messages until the connection drops or ctx is cancelled. Returns
// nil on context cancellation, an error on connection-level
// failure.
//
// `backoff` is reset to reconnectBackoffMin once the connection is
// successfully established (post-PSUBSCRIBE). Without the reset, a
// connection that stayed up for hours then dropped would re-enter
// the runSubscribe loop with the doubled-up old backoff still in
// place; the next failure would back off at 60s instead of the
// intended 2s start.
func (o *observer) subscribeOnce(ctx context.Context, ep Endpoint, first *bool, backoff *time.Duration) error {
	conn, err := dialSentinel(ctx, ep.Addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	// Force-close the conn on ctx cancellation so the blocking
	// pubsub read returns immediately; without this the read
	// sleeps until the next PubsubReadDeadline (30s) fires, and
	// observer.stop's wg.Wait blocks the same time. AfterFunc
	// runs the close on the runtime's shared cancellation
	// machinery — no per-subscribeOnce watcher goroutine to
	// accumulate under rapid reconnect loops.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	defer func() {
		// Filter ErrClosed because the AfterFunc above closes on
		// ctx-cancel; the deferred Close then sees the already-closed
		// conn on every shutdown and the resulting log noise would
		// drown V(1) signal during normal failover/reconnect cycles.
		if cerr := conn.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			log.FromContext(ctx).V(1).Info("close conn failed", "err", cerr)
		}
	}()

	rd := bufio.NewReaderSize(conn, readBufSize)

	if err := authIfNeeded(conn, rd, o.password); err != nil {
		return err
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand(
		"PSUBSCRIBE",
		"+switch-master",
		"+failover-end",
		"+failover-end-for-timeout",
		"+odown",
		"-odown",
		"+tilt",
		"-tilt",
	)); err != nil {
		return fmt.Errorf("write PSUBSCRIBE: %w", err)
	}
	// Read the seven psubscribe-ack arrays sentinel emits in
	// response (one per pattern). They produce empty
	// pubsubMessage values which we discard.
	for i := range 7 {
		if _, err := readPubsubMessage(rd); err != nil {
			return fmt.Errorf("read psubscribe ack %d: %w", i, err)
		}
	}

	// Connection up — bump metrics + emit reconnect event (only
	// after the first connect; we don't want to spam events
	// during initial bootstrap).
	operatormetrics.SentinelObserverConnected.WithLabelValues(o.cr.Namespace, o.cr.Name, ep.Name).Set(1)
	if !*first && o.recorder != nil {
		o.recorder.Eventf(o.crObject, nil, corev1.EventTypeNormal,
			string(events.SentinelObserverReconnect), "SentinelObserverReconnect",
			"sentinel pubsub connection to %s re-established", ep.Name,
		)
		operatormetrics.SentinelObserverReconnectsTotal.WithLabelValues(o.cr.Namespace, o.cr.Name, ep.Name).Inc()
	}
	*first = false
	// Connection up — reset the reconnect backoff so the NEXT
	// drop-and-reconnect cycle starts at min, not at whatever
	// doubled value the prior failure-chain ended on.
	*backoff = reconnectBackoffMin

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Read one frame with the pubsub deadline; on timeout,
		// PING-test the connection. If PING succeeds the link is
		// alive (sentinel is just quiet), we re-arm and continue.
		if err := conn.SetReadDeadline(time.Now().Add(o.opts.PubsubReadDeadline)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
		msg, err := readPubsubMessage(rd)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				pingMsg, pingErr := pingPubsub(conn, rd, time.Now().Add(o.opts.PingTimeout))
				if pingErr != nil {
					return fmt.Errorf("idle ping failed: %w", pingErr)
				}
				// A real event can arrive between the deadline expiry
				// and the PING; pingPubsub hands it back rather than
				// dropping it. The trailing PONG is absorbed by
				// readPubsubMessage on the next read (flat +PONG as a
				// non-array reply, or ["pong",""] via its unknown-array
				// default).
				if pingMsg.Channel != "" {
					o.dispatch(ctx, ep, pingMsg)
				}
				continue
			}
			return fmt.Errorf("read pubsub: %w", err)
		}
		if msg.Channel == "" {
			// Subscription-state ack or unknown frame — keep reading.
			continue
		}
		o.dispatch(ctx, ep, msg)
	}
}

// dispatch updates the observer's state from one decoded pubsub
// message and emits the per-event Prometheus counter. Each snapshot
// read-modify-write (load prev -> derive new fields -> store), together
// with any +odown map mutation, runs in one o.mu critical section so a
// concurrent pubsub or poll writer can't clobber the update.
func (o *observer) dispatch(ctx context.Context, ep Endpoint, msg pubsubMessage) {
	kind := ParseEventKind(msg.Channel)
	operatormetrics.SentinelPubsubMessagesTotal.WithLabelValues(o.cr.Namespace, o.cr.Name, string(kind)).Inc()

	switch kind {
	case KindSwitchMaster:
		ev, ok := ParseSwitchMaster(msg.Payload)
		if !ok {
			o.logParseFailure(ctx, msg)
			return
		}
		if ev.MasterName != o.masterName {
			return
		}
		// Pubsub is one observation; Quorum is conservatively preserved
		// from the prior snapshot until the next pull tick re-confirms
		// (we don't unilaterally upgrade Quorum from a single
		// +switch-master).
		prevSnap, published := o.republish(func(prev ObservedPrimary) (republishFields, bool) {
			// Monotonicity guard: +switch-master carries no epoch, so a
			// delayed, duplicated, or reordered frame can't be ordered by
			// one. Apply it only when it chains from the address we
			// currently believe is primary (ev.OldAddr == prev.Addr); a
			// stale or replayed frame switches away from an address that is
			// no longer current and is dropped, leaving the pull tick to
			// reconfirm. With no primary known yet (prev.Addr == "") there's
			// nothing to chain from, so accept and let the pull tick confirm.
			if prev.Addr != "" && ev.OldAddr != prev.Addr {
				return republishFields{}, false
			}
			return republishFields{
				quorum:       prev.Quorum,
				addr:         ev.NewAddr,
				epoch:        prev.Epoch,
				source:       SourcePubsub,
				lastPolledAt: prev.LastPolledAt,
			}, true
		})
		if !published {
			// Logged after republish has released o.mu, preserving the
			// original Unlock-then-log ordering on the drop path.
			log.FromContext(ctx).V(1).Info("sentinel observer: dropping non-chaining +switch-master",
				"cr", o.cr.String(), "masterName", ev.MasterName,
				"frameFrom", ev.OldAddr, "frameTo", ev.NewAddr, "current", prevSnap.Addr)
		}

	case KindODown:
		ev, ok := ParseMasterEvent(msg.Payload)
		if !ok {
			o.logParseFailure(ctx, msg)
			return
		}
		if ev.MasterName != o.masterName {
			return
		}
		// Mark this sentinel +odown and re-publish so consumers reading
		// the snapshot see the fresh ODown map; address/quorum unchanged.
		// The odown mutation runs inside republish's critical section.
		o.republish(func(prev ObservedPrimary) (republishFields, bool) {
			o.odown[ep.Name] = time.Now()
			return republishFields{
				quorum:       prev.Quorum,
				addr:         prev.Addr,
				epoch:        prev.Epoch,
				source:       prev.Source,
				lastPolledAt: prev.LastPolledAt,
			}, true
		})

	case KindODownClear:
		ev, ok := ParseMasterEvent(msg.Payload)
		if !ok {
			o.logParseFailure(ctx, msg)
			return
		}
		if ev.MasterName != o.masterName {
			return
		}
		o.republish(func(prev ObservedPrimary) (republishFields, bool) {
			delete(o.odown, ep.Name)
			return republishFields{
				quorum:       prev.Quorum,
				addr:         prev.Addr,
				epoch:        prev.Epoch,
				source:       prev.Source,
				lastPolledAt: prev.LastPolledAt,
			}, true
		})

	case KindFailoverEnd, KindFailoverEndTimeout:
		ev, ok := ParseMasterEvent(msg.Payload)
		if !ok {
			o.logParseFailure(ctx, msg)
			return
		}
		if ev.MasterName != o.masterName {
			return
		}
		// Failover-end carries the new primary address; treat as a
		// stronger pubsub signal than +switch-master alone.
		o.republish(func(prev ObservedPrimary) (republishFields, bool) {
			return republishFields{
				quorum:       prev.Quorum,
				addr:         ev.Addr,
				epoch:        prev.Epoch,
				source:       SourcePubsub,
				lastPolledAt: prev.LastPolledAt,
			}, true
		})

	case KindTilt, KindTiltClear, KindOther:
		// Surfaced to the metric counter only; no snapshot update.
		// Tilt resolution is the pull tick's job.
	}
}

// republishFields are the five ObservedPrimary fields a pubsub dispatch
// arm derives from the prior snapshot. publishLocked recomputes QuorumOK
// and UpdatedAt and copies o.odown, so those are deliberately not settable
// here — only the caller-controlled inputs are.
type republishFields struct {
	quorum       QuorumStatus
	addr         string
	epoch        int64
	source       Source
	lastPolledAt time.Time
}

// republish runs one snapshot read-modify-write under o.mu — the single
// home for the Lock -> load prev -> derive -> publishLocked -> Unlock ->
// notify discipline the four pubsub dispatch arms share, so the lost-update
// serialization lives in one place instead of one copy per arm.
// mutate receives the prior snapshot, applies any arm-specific o.odown
// change (which is why it runs under the lock), and returns the next
// published fields plus ok. ok=false aborts with no publish and no notify —
// the +switch-master non-chaining-frame drop — and the returned prior
// snapshot lets that caller log the drop after the lock is released,
// preserving the original Unlock-then-log ordering. notify runs after the
// unlock so the woken reconcile already reads the just-published snapshot.
func (o *observer) republish(mutate func(prev ObservedPrimary) (republishFields, bool)) (ObservedPrimary, bool) {
	o.mu.Lock()
	prev, _ := o.holder.load()
	f, ok := mutate(prev)
	if !ok {
		o.mu.Unlock()
		return prev, false
	}
	o.publishLocked(f.quorum, f.addr, f.epoch, f.source, f.lastPolledAt)
	o.mu.Unlock()
	o.notify()
	return prev, true
}

// logParseFailure emits a V(1) audit-trail entry for malformed
// sentinel pubsub payloads. Without it, parse failures are silently
// dropped (the dispatch return-on-!ok path) and a malformed-message
// regression in the upstream sentinel emitter would have no
// visibility in operator logs. V(1) keeps it off default emission
// so well-formed clusters don't see noise.
func (o *observer) logParseFailure(ctx context.Context, msg pubsubMessage) {
	log.FromContext(ctx).V(1).Info("malformed sentinel pubsub payload",
		"cr", o.cr.String(),
		"channel", msg.Channel,
		"payload", msg.Payload,
	)
}

// publishLocked writes a fresh ObservedPrimary. The caller MUST hold
// o.mu: the snapshot read-modify-write (load prev -> derive new fields
// -> store) and this odown-map copy run in one critical section so a
// second writer can't land a store between a writer's load and store and
// get clobbered. Always copies the +odown map AND the pull-side
// odownPull map so consumers reading Snapshot.ODown / Snapshot.ODownPull
// can iterate without locking. The QuorumOK bool
// surfaced to legacy consumers is derived from the tri-state Quorum
// input — Unknown collapses to false alongside Lost so callers that
// drive irreversible actions stay conservative. lastPolledAt is the
// poll-freshness stamp: callers on the pull-tick path pass time.Now();
// pub/sub re-publishes pass the prior snapshot's LastPolledAt so the
// poll-freshness clock only advances on a live poll.
func (o *observer) publishLocked(q QuorumStatus, addr string, epoch int64, src Source, lastPolledAt time.Time) {
	odownCopy := make(map[string]time.Time, len(o.odown))
	maps.Copy(odownCopy, o.odown)
	odownPullCopy := make(map[string]time.Time, len(o.odownPull))
	maps.Copy(odownPullCopy, o.odownPull)
	o.holder.store(ObservedPrimary{
		Addr:         addr,
		Epoch:        epoch,
		Quorum:       q,
		QuorumOK:     q == QuorumStatusOK,
		UpdatedAt:    time.Now(),
		LastPolledAt: lastPolledAt,
		Source:       src,
		ODown:        odownCopy,
		ODownPull:    odownPullCopy,
	})
}

// notify pushes a reconcile-trigger GenericEvent for this observer's
// CR onto the manager's shared channel. Non-blocking: a full buffer
// (reconciler briefly behind) drops the push rather than stalling the
// pubsub goroutine — the 10s pull tick is the safety net, so a missed
// push costs at most one poll cycle of reaction latency. Called after
// the o.mu critical section so the snapshot the woken reconcile reads
// already reflects this event. No-op when events is nil (test wiring
// without the push half).
func (o *observer) notify() {
	if o.events == nil {
		return
	}
	// Build a minimal object from o.cr (always set at construction) so
	// the push is correct even if dispatch ever runs before start()
	// populates crObject. The reconcile mapper reads only
	// namespace/name.
	ev := event.GenericEvent{Object: &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: o.cr.Namespace, Name: o.cr.Name},
	}}
	select {
	case o.events <- ev:
	default:
	}
}

// runPoll is the pull-tick goroutine. Fires every PollInterval,
// queries every endpoint for GET-MASTER-ADDR-BY-NAME + CKQUORUM,
// and updates the snapshot when at least QuorumThreshold sentinels
// agree on the same primary address.
func (o *observer) runPoll(ctx context.Context) {
	defer o.wg.Done()

	t := time.NewTicker(o.opts.PollInterval)
	defer t.Stop()
	// Brief pre-first-tick wait so a not-yet-Ready sentinel STS at
	// observer start doesn't blow the per-endpoint dial-timeout
	// budget on the first pollOnce. The snapshot still lands well
	// inside one PollInterval, just not within dial-time.
	select {
	case <-ctx.Done():
		return
	case <-time.After(initialPollBackoff):
	}
	o.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.pollOnce(ctx)
			// Without the explicit ctx.Err() check here, a ctx
			// cancellation that lands during pollOnce can be lost
			// to a same-instant t.C fire — Go's select randomizes
			// when both cases are ready — and another pollOnce
			// runs anyway, slowing observer shutdown.
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// pollOnce sweeps every endpoint and re-publishes the snapshot.
// Quorum threshold is min(majority of endpoints, 2) — the manager
// is responsible for not creating an observer on < 2 endpoints
// (mode=sentinel with replicas<2 is a soft warn at the webhook,
// but the observer itself is defensive: when len(eps)==1
// QuorumOK is always false because no two-of-N agreement is
// possible, and when len(eps)==2 we require both to agree).
func (o *observer) pollOnce(ctx context.Context) {
	pollCtx, cancel := context.WithTimeout(ctx, commandDeadline)
	defer cancel()

	type result struct {
		addr           string
		epoch          int64
		quorumOK       bool
		flags          *MasterFlags
		knownReplicas  int
		knownSentinels int
		countsValid    bool
		err            error
	}
	results := make([]result, len(o.endpoints))
	var wg sync.WaitGroup
	for i, ep := range o.endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			addr, epoch, qOK, flags, knownReplicas, knownSentinels, countsValid, err := o.queryOne(pollCtx, ep)
			results[i] = result{
				addr:           addr,
				epoch:          epoch,
				quorumOK:       qOK,
				flags:          flags,
				knownReplicas:  knownReplicas,
				knownSentinels: knownSentinels,
				countsValid:    countsValid,
				err:            err,
			}
		}(i, ep)
	}
	wg.Wait()

	// Publish per-endpoint observations BEFORE the aggregate so a
	// concurrent EndpointObservations reader sees fresh per-pod data
	// even if the aggregate decides to suppress an address swap
	// (Quorum=Lost branch below preserves prev.Addr for the
	// aggregate, but each endpoint should still surface what IT
	// reported). Pure atomic.Value store, no mutex — see
	// observer.endpointObs field comment.
	now := time.Now()
	endpointObs := make([]EndpointObservation, len(o.endpoints))
	for i := range o.endpoints {
		endpointObs[i] = EndpointObservation{
			Name:            o.endpoints[i].Name,
			PrimaryAddr:     results[i].addr,
			QuorumReachable: results[i].quorumOK,
			Reachable:       results[i].err == nil,
			At:              now,
			KnownReplicas:   results[i].knownReplicas,
			KnownSentinels:  results[i].knownSentinels,
			CountsValid:     results[i].countsValid,
		}
	}
	o.endpointObs.Store(endpointObs)

	// Build the pull-side o_down observations for this tick — one per
	// endpoint, nil flags for any sentinel we could not reach or whose
	// reply did not parse, so the reconciler leaves those entries
	// untouched (absence of evidence is not evidence of absence).
	odownObs := make([]odownPullObs, len(o.endpoints))
	for i := range o.endpoints {
		var f *MasterFlags
		if results[i].err == nil {
			f = results[i].flags
		}
		odownObs[i] = odownPullObs{name: o.endpoints[i].Name, flags: f}
	}

	// Tally agreement on (addr) and on (addr AND quorumOK) from the
	// SAME sentinel; take the max observed epoch (cluster-global value,
	// monotonic — a single sentinel reporting a stale epoch must not
	// pull the snapshot's epoch backward).
	addrCount := map[string]int{}
	addrQuorumCount := map[string]int{}
	reachable := 0
	maxEpoch := int64(0)
	for _, r := range results {
		if r.err != nil {
			continue
		}
		reachable++
		if r.addr != "" {
			addrCount[r.addr]++
			if r.quorumOK {
				// Count address-agreement and CKQUORUM-agreement from the
				// same sentinel together, so the quorum gate below can't
				// be satisfied by two disjoint threshold-sized subsets.
				addrQuorumCount[r.addr]++
			}
		}
		if r.epoch > maxEpoch {
			maxEpoch = r.epoch
		}
	}

	threshold := QuorumThreshold(len(o.endpoints))
	// The snapshot read-modify-write runs under o.mu so a concurrent
	// pubsub dispatch can't land a store between this tick's load and
	// store (or be clobbered by it) — the lost-update fix. The
	// section is pure CPU (all queryOne I/O is already done), so the
	// hold is brief.
	o.mu.Lock()
	defer o.mu.Unlock()
	// Level-reconcile the pull-side o_down map from this tick's
	// observations before either publish path fires (the
	// reachable<threshold degraded publish AND the normal publish
	// below both copy o.odownPull), so both carry the reconciled map.
	// Runs under o.mu with the snapshot read-modify-write.
	reconcileODownPull(o.odownPull, odownObs, now)
	prev, _ := o.holder.load()
	// Epoch only goes up — defensive against any single reachable
	// sentinel reporting a stale value (race window after a fresh
	// failover where one sentinel hasn't caught up yet).
	epoch := max(prev.Epoch, maxEpoch)

	if reachable < threshold {
		// Not enough sentinels reachable to decide. Publish
		// QuorumStatusUnknown so the hysteresis-gated
		// suppression accumulator preserves its prior state
		// across this observer-side unreachable window. Legacy
		// bool consumers see QuorumOK=false (Unknown collapses
		// to false at publish time) and continue to refuse
		// irreversible actions. The prior known address is kept
		// so consumers still have a hint while degraded.
		o.publishLocked(QuorumStatusUnknown, prev.Addr, epoch, SourcePoll, time.Now())
		return
	}

	// Pick the address with the most votes. Break ties deterministically
	// so the published Addr can't flap on equal vote counts: keep the
	// previously-published primary when it's tied for the lead (avoids a
	// spurious relabel), otherwise take the lexicographically smallest
	// address. Map-iteration order must not affect the result.
	var winner string
	var winnerVotes int
	for a, n := range addrCount {
		switch {
		case n > winnerVotes:
			winner, winnerVotes = a, n
		case n == winnerVotes:
			winner = breakAddrTie(winner, a, prev.Addr)
		}
	}
	// Quorum is OK only when a single threshold-sized set of sentinels
	// agrees on BOTH the winning address AND CKQUORUM. addrQuorumCount
	// counts exactly that intersection, and is ≤ winnerVotes, so this
	// also implies ≥ threshold agree on the address itself.
	quorumOK := addrQuorumCount[winner] >= threshold
	status := QuorumStatusLost
	if quorumOK {
		status = QuorumStatusOK
	}

	// Source=merge when both the prior pubsub address and the
	// fresh poll address agree. Otherwise SourcePoll.
	src := SourcePoll
	if prev.Addr != "" && prev.Addr == winner {
		src = SourceMerge
	}
	addr := winner
	if !quorumOK {
		// Degraded — keep the prior known address but flag
		// Quorum=Lost so consumers won't act on it.
		addr = prev.Addr
	}
	o.publishLocked(status, addr, epoch, src, time.Now())
}

// odownPullObs is one sentinel endpoint's pull-side observation for a
// single poll tick. flags==nil means the tick produced no usable
// reading for this endpoint (unreachable, errored, or an unparseable
// SENTINEL MASTER reply); the reconciler leaves any existing entry
// untouched. A non-nil flags is a genuine reached-sentinel reading.
type odownPullObs struct {
	name  string
	flags *MasterFlags
}

// reconcileODownPull level-reconciles the pull-side o_down first-seen
// map in place from one poll tick's per-endpoint observations. The
// rules encode "absence of evidence is not evidence of absence":
//
//   - flags==nil (unreachable / errored / unparseable) → leave the
//     entry untouched: neither stamp nor clear. A lost frame or a dead
//     sentinel must not mutate the map.
//   - o_down set → rising-edge stamp: record now ONLY when the key is
//     absent; while o_down persists KEEP the original stamp (never
//     refresh) so the entry ages out one failover-timeout after o_down
//     FIRST appeared — the escape valve that stops a stuck o_down from
//     vetoing operator hygiene forever.
//   - o_down clear on a REACHED sentinel → delete the entry: a
//     pull-confirmed clear, distinct from a lost -odown frame.
//
// s_down is never a stamp INPUT: a reply carrying only s_down on an
// absent key is a no-op (a lone subjective s_down does not brew an
// election). But an s_down-only reply is still o_down-clear, so on a
// key that already holds a stamp it falls through to the delete rule
// above — the pull tick has confirmed o_down is gone (an o_down ->
// s_down downgrade means the quorum no longer agrees the master is
// down).
func reconcileODownPull(m map[string]time.Time, obs []odownPullObs, now time.Time) {
	for _, ob := range obs {
		switch {
		case ob.flags == nil:
			// Untouched: no usable reading this tick.
		case ob.flags.ODown:
			if _, seen := m[ob.name]; !seen {
				m[ob.name] = now
			}
		default:
			delete(m, ob.name)
		}
	}
}

// breakAddrTie resolves a tie between two equally-voted sentinel
// addresses deterministically: the incumbent (previously-published)
// address wins if it is one of them — so a tie does not trigger a
// spurious relabel — otherwise the lexicographically smaller address
// wins. Commutative and associative, so the resolved winner is
// independent of map-iteration order across the tied set.
func breakAddrTie(x, y, incumbent string) string {
	switch {
	case x == incumbent:
		return x
	case y == incumbent:
		return y
	case x < y:
		return x
	default:
		return y
	}
}

// queryOne dials one sentinel, runs GET-MASTER-ADDR-BY-NAME +
// SENTINEL MASTER + CKQUORUM, and reports the result. The middle
// command pulls the cluster `config-epoch` so the snapshot's
// ObservedPrimary.Epoch carries a real monotonic value (consumed
// by missed-failover detection). Epoch is best-effort — if
// SENTINEL MASTER errors or its reply lacks config-epoch, we
// still return the addr/quorum from the other two commands.
//
// flags carries the parsed s_down / o_down markers from the SAME
// SENTINEL MASTER reply — no extra round-trip. It is non-nil only
// when the reply was read AND its `flags` field parsed; every
// error path returns nil so the pull-side reconciler leaves its map
// untouched (absence of evidence is not evidence of absence). A
// reached sentinel that fails only CKQUORUM still returns its flags.
//
// knownReplicas / knownSentinels carry the same reply's `num-slaves`
// and `num-other-sentinels` counts, and countsValid is true only when
// the MASTER read succeeded AND both counts parsed — so it implies
// the endpoint answered. They ride alongside flags from the one
// SENTINEL MASTER reply; a future unified field parse could read every
// count in a single scan. Every path that error-returns before the
// MASTER read yields 0,0,false; a reached sentinel that fails only
// CKQUORUM keeps its parsed counts, mirroring flags.
func (o *observer) queryOne(ctx context.Context, ep Endpoint) (addr string, epoch int64, quorumOK bool, flags *MasterFlags, knownReplicas int, knownSentinels int, countsValid bool, err error) {
	conn, err := dialSentinel(ctx, ep.Addr)
	if err != nil {
		return "", 0, false, nil, 0, 0, false, err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			log.FromContext(ctx).V(1).Info("close conn failed", "err", cerr)
		}
	}()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(commandDeadline))
	}

	rd := bufio.NewReaderSize(conn, readBufSize)

	if err := authIfNeeded(conn, rd, o.password); err != nil {
		return "", 0, false, nil, 0, 0, false, err
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "GET-MASTER-ADDR-BY-NAME", o.masterName)); err != nil {
		return "", 0, false, nil, 0, 0, false, fmt.Errorf("write GET-MASTER-ADDR-BY-NAME: %w", err)
	}
	addrReply, err := readReply(rd)
	if err != nil {
		return "", 0, false, nil, 0, 0, false, fmt.Errorf("read GET-MASTER-ADDR-BY-NAME: %w", err)
	}
	addr, _ = ParseGetMasterAddr(addrReply)

	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "MASTER", o.masterName)); err != nil {
		return addr, 0, false, nil, 0, 0, false, fmt.Errorf("write SENTINEL MASTER: %w", err)
	}
	masterReply, err := readReply(rd)
	if err == nil {
		// Best-effort: epoch is dropped on parse failure but the
		// rest of the call continues. Real-world sentinels always
		// emit config-epoch; the only failure path is a future
		// version dropping the field, which we'd want to notice
		// via missed-failover detection rather than crash on.
		epoch, _ = ParseSentinelMasterEpoch(masterReply)
		// Parse the down-state markers from the same reply — no extra
		// round-trip. Only a successfully parsed `flags` field yields
		// a non-nil observation; an unparseable reply leaves flags nil
		// so the reconciler treats it as no-evidence, not clear.
		if mf, ok := ParseSentinelMasterFlags(masterReply); ok {
			flags = &mf
		}
		// Topology counts from the same reply — no extra round-trip.
		// countsValid gates both: a RESP3-map / renamed-field reply
		// leaves the counts unusable even though the endpoint answered,
		// so consumers must not read a 0 as a real deficit.
		nSlaves, slavesOK := ParseSentinelMasterNumSlaves(masterReply)
		nSent, sentOK := ParseSentinelMasterNumOtherSentinels(masterReply)
		knownReplicas = nSlaves
		knownSentinels = nSent
		countsValid = slavesOK && sentOK
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "CKQUORUM", o.masterName)); err != nil {
		return addr, epoch, false, nil, 0, 0, false, fmt.Errorf("write CKQUORUM: %w", err)
	}
	quorumReply, err := readReply(rd)
	// CKQUORUM returns a -NOQUORUM error on failure; readReply
	// surfaces that as an error. Treat any error here as
	// quorumOK=false — the sentinel was still reached, so addr, epoch,
	// the parsed flags AND the parsed counts stay usable.
	if err != nil {
		return addr, epoch, false, flags, knownReplicas, knownSentinels, countsValid, nil
	}
	return addr, epoch, ParseCKQuorum(quorumReply), flags, knownReplicas, knownSentinels, countsValid, nil
}

// QuorumThreshold is the pool-majority count for an N-sentinel pool:
// for N>=3 the strict majority N/2+1; for N==2 both (2); for N<=1 the
// degenerate N itself. The webhook floor (sentinel mode requires
// replicas>=2) means N<=1 is defensive only. Exported so the
// reconciler can clamp a sub-majority spec.sentinel.quorum up to this
// floor for its own quorum-lost verdict, keeping that guard consistent
// with the observer's relabel guard which already gates on this count.
func QuorumThreshold(n int) int {
	if n <= 2 {
		return n
	}
	return n/2 + 1
}
