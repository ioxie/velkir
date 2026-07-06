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
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/logging"
)

// ManagerName identifies this runnable in controller-runtime
// startup logs.
const ManagerName = "sentinel-observer-manager"

// Manager is the singleton owner of per-CR sentinel observer
// goroutines. Held by the operator manager (via mgr.Add); the
// reconciler calls Ensure on each pass for sentinel-mode CRs and
// Remove on CR deletion.
//
// Thread safety: the observer registry (the observers map + rootCtx) is
// guarded by obsMu; the deferral predicate by mu (the two are split so
// the predicate, invoked while mu is held, can read an observer Snapshot
// under obsMu without re-entering mu — see the field comments). Ensure
// may block briefly while it stops a stale observer during an
// endpoint-set change; obsMu is held for the swap so callers can't see a
// half-rebuilt entry.
type Manager struct {
	recorder k8sevents.EventRecorder
	opts     Options

	// obsMu guards the observer registry: the observers map and rootCtx.
	// It is deliberately SEPARATE from mu (which guards the deferral
	// predicate): RecoverStrandedSentinels holds mu across the predicate
	// call, and the predicate's IsFailoverInFlight path reads an observer
	// Snapshot — which takes obsMu, NOT mu — so the predicate never
	// re-enters mu. A single shared mutex self-deadlocked here: the
	// predicate-holder held it across the predicate, and the predicate
	// re-locked it via Snapshot. Lock order is always mu→obsMu (only the
	// predicate path crosses; no observer-registry method takes mu), so
	// there is no ordering cycle.
	obsMu     sync.Mutex
	observers map[observerKey]*observer
	// rootCtx is set on Start and is the parent context for every
	// observer's goroutine tree. Nil before Start; Ensure refuses
	// (and reports an error) if rootCtx is nil so a misordered
	// startup surfaces as a clear failure rather than a
	// silently-orphaned observer. Guarded by obsMu.
	rootCtx context.Context

	// mu guards the deferral predicate ref (deferralPredicate); see
	// obsMu above for why the observer registry has its own lock.
	mu sync.Mutex

	// deferralPredicate gates operator-issued sentinel surgery
	// (RecoverStrandedSentinels). Returning true for cr means "defer
	// this REMOVE + MONITOR pass — the CR is mid-failover or under
	// sustained quorum-loss suppression, and touching sentinel state
	// now would race the sentinel's own config-epoch propagation".
	// Default returns false (always allow); the live FSM-state check is
	// plugged in by the reconciler once FSM state is persisted into
	// CR.Status.
	deferralPredicate func(cr observerKey) bool

	// events is the push half of hybrid push+pull observation. Each
	// observer sends a GenericEvent for its CR on +switch-master /
	// +failover-end / +odown / -odown; the reconciler consumes the
	// channel via a WatchesRawSource source.Channel so it reacts
	// within seconds instead of waiting for the 10s pull tick or the
	// multi-minute baseline watchdog. Buffered + non-blocking sends
	// (see observer.notify) so a briefly-behind reconciler can never
	// stall an observer's pubsub goroutine — a dropped push only
	// costs one pull cycle of latency.
	events chan event.GenericEvent

	// ghostSeen is the ghost-reap debounce state: per CR, a map of
	// ghost peer-IP → the first time that IP was observed absent from
	// the live sentinel-pod set. A ghost only becomes reap-eligible
	// once it has been continuously absent for ghostReapDebounce,
	// closing the controller-runtime cache-lag window in which a
	// brand-new live pod's already-gossiped announce-ip is briefly
	// missing from the operator's pod-list snapshot and would
	// otherwise look like a dead ghost. Guarded by mu (pure map work,
	// no I/O under the lock). Updated every RecoverStrandedSentinels
	// pass that carries a live-sentinel-IP set.
	ghostSeen map[observerKey]map[string]time.Time

	// clock returns the current time; injectable so the debounce can
	// be driven deterministically in tests without time.Sleep.
	// Defaults to time.Now in NewManager.
	clock func() time.Time
}

// ghostReapDebounce is how long a peer IP must be continuously absent
// from the live sentinel-pod set before the operator treats it as a
// dead ghost and reaps the survivor that still knows it. Sized above
// the controller-runtime informer-sync lag so a just-replaced
// sentinel's new announce-ip — already in survivors' gossip but not
// yet in the operator's pod-list snapshot — is never mistaken for a
// ghost. The per-reconcile cooldown paces passes, so this is a
// wall-clock floor, not a pass count.
const ghostReapDebounce = 30 * time.Second

// observerEventBuffer sizes the shared push channel. Generous so a
// burst across many CRs (e.g. a quorum flap that fans +odown to every
// observer) doesn't overflow before source.Channel's distributor
// drains it; overflow degrades gracefully to the pull tick anyway.
const observerEventBuffer = 1024

// Compile-time enforcement that Manager satisfies the
// controller-runtime Runnable + LeaderElectionRunnable interfaces.
var _ manager.Runnable = (*Manager)(nil)
var _ manager.LeaderElectionRunnable = (*Manager)(nil)

// NewManager constructs a Manager. recorder may be nil in tests
// that don't care about event emission; opts may be the zero value
// to take defaults from the package constants.
func NewManager(recorder k8sevents.EventRecorder, opts Options) *Manager {
	return &Manager{
		recorder:  recorder,
		opts:      opts.withDefaults(),
		observers: make(map[observerKey]*observer),
		events:    make(chan event.GenericEvent, observerEventBuffer),
		ghostSeen: make(map[observerKey]map[string]time.Time),
		clock:     time.Now,
	}
}

// Events exposes the push channel for the reconciler to wire into a
// WatchesRawSource source.Channel in SetupWithManager. Receive-only —
// only observers (via the Manager) send on it.
func (m *Manager) Events() <-chan event.GenericEvent { return m.events }

// ErrManagerNotStarted is returned by Ensure when called before
// Start has installed rootCtx. Transient at operator startup —
// controller-runtime starts Runnables in parallel, so the
// reconciler may begin reconciling before the sentinel manager's
// Start has run. The reconciler treats this as a soft fail and
// retries on the next reconcile (the rootCtx is set within
// milliseconds of mgr.Start). errors.Is callers can suppress
// log spam emitted before the manager is ready.
var ErrManagerNotStarted = errors.New("sentinel observer manager not started — Ensure called before Start; transient at operator startup, retries on next reconcile")

// Start implements manager.Runnable. Captures the manager context
// as the parent for all observer goroutines, then blocks until ctx
// is cancelled (leader-election loss or operator shutdown). On
// return, every observer's context is cancelled and we wait for
// them to drain so we don't leak goroutines past Stop.
func (m *Manager) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName(ManagerName)
	m.obsMu.Lock()
	m.rootCtx = ctx
	m.obsMu.Unlock()
	logger.Info("sentinel observer manager started")

	<-ctx.Done()

	// Drain: cancel each observer + wait for its goroutines.
	// Lock is taken once to snapshot the map; observers are
	// stopped outside the lock so a concurrent Remove() doesn't
	// deadlock.
	m.obsMu.Lock()
	toStop := make([]*observer, 0, len(m.observers))
	for _, o := range m.observers {
		toStop = append(toStop, o)
	}
	m.observers = make(map[observerKey]*observer)
	m.rootCtx = nil
	m.obsMu.Unlock()

	for _, o := range toStop {
		o.stop()
		// Pair with the Register in Ensure so a process-lifetime
		// shutdown evicts every observer's password from the
		// redaction registry. Symmetric to Remove.
		logging.DefaultRegistry.Forget(o.password)
	}
	logger.Info("sentinel observer manager stopped", "drained", len(toStop))
	return nil
}

// NeedLeaderElection implements LeaderElectionRunnable. The
// observer must run only on the leader — it issues SENTINEL
// CKQUORUM at 10s cadence per CR, and a non-leader replica running
// the observer would double the sentinel-side load without
// contributing to reconciler decisions (the non-leader doesn't
// publish the snapshot anywhere consumers read).
func (m *Manager) NeedLeaderElection() bool { return true }

// Ensure idempotently creates / updates an observer for the given
// CR. Behaviour:
//
//   - First call → create observer, start goroutines.
//   - Subsequent call with the SAME endpoints + masterName +
//     password → no-op (the live observer keeps its connection
//     state).
//   - Subsequent call with a CHANGED endpoints / masterName /
//     password → stop the old observer, start a fresh one. This
//     is rare in practice (sentinel pod IPs only churn on STS
//     recreate; password rotation requires an operator action)
//     but the swap is cheap (a few seconds of lost pubsub
//     coverage; the pull tick on the new observer fires
//     immediately).
//
// Returns an error if the manager hasn't started yet (rootCtx
// nil) — the caller (reconciler) treats this as a soft fail and
// retries on the next reconcile.
func (m *Manager) Ensure(ctx context.Context, cr observerKey, masterName, password string, endpoints []Endpoint) error {
	if masterName == "" {
		return fmt.Errorf("masterName required")
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("at least one sentinel endpoint required")
	}

	// Defensive copy + sort so equality compares deterministically.
	eps := append([]Endpoint(nil), endpoints...)
	sort.Slice(eps, func(i, j int) bool { return eps[i].Name < eps[j].Name })

	m.obsMu.Lock()
	if m.rootCtx == nil {
		m.obsMu.Unlock()
		return ErrManagerNotStarted
	}
	rootCtx := m.rootCtx

	prev, exists := m.observers[cr]
	if exists && observerConfigEqual(prev, masterName, password, eps) {
		m.obsMu.Unlock()
		return nil
	}
	// Either new or config-changed; install + start fresh observer
	// inside the lock so a concurrent Ensure or Remove on the same
	// cr can't interleave between map-install and start. Without
	// this, a racing Ensure(cr,vN+1) could see the just-installed
	// vN, replace it in the map, drain it (no-op since vN hasn't
	// started yet), then call start() on its own observer; meanwhile
	// the original Ensure's deferred start() spins up vN's
	// goroutines AFTER vN has been removed from the map, leaving an
	// orphan goroutine tree that survives until rootCtx cancels.
	// start() only spawns goroutines (no I/O wait), so holding the
	// lock across it is fast.
	o := newObserver(cr, masterName, password, eps, m.opts, m.recorder, m.events)
	o.start(rootCtx)
	m.observers[cr] = o
	// Register inside the locked region — atomically with the map
	// install — so a cross-CR race (Ensure(crA, sharedPw) +
	// concurrent Remove(crB, sharedPw)) can't observe a refcount=0
	// window where neither holder has the registration. Remove
	// takes the same obsMu, so by the time Remove's post-stop Forget
	// runs, this Register has already incremented the refcount.
	logging.DefaultRegistry.Register(password)
	m.obsMu.Unlock()

	if exists {
		// Stop the old observer outside the lock so its drain
		// doesn't block other callers. After stop() returns the
		// old observer's goroutines have exited and can no longer
		// emit log lines, so Forgetting the old password is safe
		// — the new observer's registration above keeps the
		// redactor live across the swap.
		prev.stop()
		logging.DefaultRegistry.Forget(prev.password)
	}
	return nil
}

// Remove cancels and forgets the observer for cr, and drops the CR's
// ghost-reap debounce state so a recreated same-name CR starts its
// debounce from zero instead of inheriting a stale first-seen stamp
// (which would make a re-detected ghost instantly reap-eligible,
// defeating the cache-lag window the debounce closes). Idempotent — a
// Remove on a CR that was never Ensure'd only clears any surgery-pass
// debounce state.
func (m *Manager) Remove(cr observerKey) {
	// ghostSeen is fed by surgery passes, not by the observer, so it
	// must be cleared even when no observer exists for cr. Guarded by
	// mu (the surgery-path lock), taken and released before obsMu so
	// the two locks never nest here.
	m.mu.Lock()
	delete(m.ghostSeen, cr)
	m.mu.Unlock()

	m.obsMu.Lock()
	o, exists := m.observers[cr]
	if !exists {
		m.obsMu.Unlock()
		return
	}
	delete(m.observers, cr)
	m.obsMu.Unlock()
	o.stop()
	// Pair with the Register in Ensure: the observer's password is
	// no longer used past this point on this CR. The registry's
	// refcount handles the case where another CR shares the same
	// Secret value.
	logging.DefaultRegistry.Forget(o.password)
}

// Snapshot returns the most recent ObservedPrimary for cr. The
// returned Snapshot has Present=false when no observer exists for
// cr OR the observer has not yet published its first observation.
// Consumers treat both cases identically: refuse to act, retry
// next reconcile.
func (m *Manager) Snapshot(cr observerKey) Snapshot {
	m.obsMu.Lock()
	o, exists := m.observers[cr]
	m.obsMu.Unlock()
	if !exists {
		return Snapshot{}
	}
	op, ok := o.snapshot()
	if !ok {
		return Snapshot{}
	}
	return Snapshot{Present: true, Primary: op}
}

// EndpointObservations returns the most recent per-endpoint
// observation set captured by the observer's pollOnce. Returns nil
// when no observer is registered for cr OR pollOnce has not yet
// completed its first sweep. Consumers (the controller's
// SentinelQuorum status writer) treat nil and empty identically:
// no write, retry next reconcile. Without this
// surface, SQ.Status stayed empty for the cluster's lifetime
// because the in-process observer's per-endpoint data was never
// exposed to the reconciler thread.
func (m *Manager) EndpointObservations(cr observerKey) []EndpointObservation {
	m.obsMu.Lock()
	o, exists := m.observers[cr]
	m.obsMu.Unlock()
	if !exists {
		return nil
	}
	v, ok := o.endpointObs.Load().([]EndpointObservation)
	if !ok {
		return nil
	}
	// Defensive copy — the observer overwrites this slice on each
	// pollOnce, and the reconciler may iterate the returned value
	// across an SSA-apply round-trip. Cheap (small N) and avoids
	// any cross-goroutine reads of the underlying array.
	out := make([]EndpointObservation, len(v))
	copy(out, v)
	return out
}

// Has reports whether the manager currently owns an observer for
// cr. Used by the reconciler's Ensure-then-Snapshot ordering tests.
func (m *Manager) Has(cr observerKey) bool {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	_, ok := m.observers[cr]
	return ok
}

// SetDeferralPredicate installs the predicate RecoverStrandedSentinels
// consults before issuing a REMOVE + MONITOR pass. Returning true
// defers the pass (the next reconcile retries). Default (predicate==nil)
// means "always allow". Safe to call any time — the predicate is read
// under m.mu inside RecoverStrandedSentinels.
func (m *Manager) SetDeferralPredicate(p func(cr observerKey) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deferralPredicate = p
}

// IssueAuthPass propagates the master's auth-pass to every
// supplied sentinel pod and verifies via SENTINEL MASTER read-
// back. Returns one AuthResult per endpoint (in the same order)
// AND emits Kubernetes events on the recorder: SentinelAuthApplied
// (Normal) per success, SentinelAuthNotApplied (Warning) per
// per-pod failure that survived the per-pod retry budget.
//
// Returns nil (no events emitted) when password is empty —
// no-AUTH CRs have nothing to propagate. Empty endpoints list
// also returns nil without events.
func (m *Manager) IssueAuthPass(ctx context.Context, cr observerKey, masterName, password string, endpoints []Endpoint) []AuthResult {
	if password == "" || len(endpoints) == 0 {
		return nil
	}
	results := setAuthPassAllWithAttempts(ctx, endpoints, masterName, password, m.opts.AuthRetryAttempts)
	m.emitAuthEvents(cr, results)
	return results
}

// IssueFailover dispatches `SENTINEL FAILOVER <masterName>` to one
// of the supplied sentinel endpoints. Iterates in slice order — the
// first endpoint that accepts (returns +OK) wins; failures fall
// through to the next. Returns nil on success, ErrFailoverInProgress
// when any sentinel reports a failover already running (treated as
// success by the caller — the failover IS happening), or a wrapped
// error describing the last attempt when every endpoint failed.
//
// Empty endpoints returns an error rather than nil — a no-op in this
// path is always a bug (the caller's preflight should have refused
// before invoking).
//
// Bypasses the deferral predicate: the reset path defers when the
// CR is mid-failover specifically because RESET races sentinel's
// internal config-epoch propagation; FAILOVER itself is the
// initiator of that propagation, so it must not be deferred. The
// reconciler's own preflight (offset-tolerance, role=primary strip)
// ensures we don't issue FAILOVER in pathological states.
//
// **DO NOT copy this bypass to other sentinel commands.** This is
// FAILOVER-specific. Any new IssueXyz method that consumes /
// reads / mutates sentinel config state (RESET, SET, MONITOR,
// REMOVE, FLUSHCONFIG) MUST honour the deferral predicate or it
// will race the config-epoch propagation that an in-flight failover
// is driving.
//
// Does NOT emit the FailoverInitiated event — that's the FSM's T14
// side-effect, fired by the caller via applyFSM after IssueFailover
// returns. The split keeps event emission tied to the FSM's
// transition record, not the wire-side dispatch detail.
func (m *Manager) IssueFailover(ctx context.Context, cr observerKey, masterName, password string, endpoints []Endpoint) error {
	if masterName == "" {
		return fmt.Errorf("masterName required")
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("at least one sentinel endpoint required")
	}
	var lastErr error
	for _, ep := range endpoints {
		err := FailoverOne(ctx, ep, masterName, password)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrFailoverInProgress) {
			// Some other sentinel (or the operator on a prior
			// reconcile) already kicked the failover; the
			// caller's FSM advances to FailoverInFlight either
			// way.
			return ErrFailoverInProgress
		}
		lastErr = fmt.Errorf("sentinel %s: %w", ep.Name, err)
	}
	return fmt.Errorf("all %d sentinel endpoint(s) failed; last: %w", len(endpoints), lastErr)
}

// RunInitialReset is the boot-time safety net for a leader that
// may have missed RESETs while non-leader. Iterates every
// passed-in CR (mode=sentinel only; caller filters) and decides
// per-CR whether sentinel state has drifted from current pod
// state. RESET fires only when the gate detects an anomaly; when
// it does fire, `SENTINEL MONITOR` follows immediately so
// sentinels learn the correct master IP rather than falling back
// to a stale on-disk pointer. Bypasses the deferral predicate
// entirely.
//
// Emits one InitialSentinelReset event per CR carrying the
// per-pod RESET outcomes, plus a SentinelMonitor event per
// sentinel reached. CRs that pass the probe gate (state already
// consistent) emit nothing — silence is the success signal.
//
// Caller (cmd/main.go runnable) is responsible for listing the CRs
// + computing the per-CR (endpoints, password, MasterName, MasterIP,
// Port, Quorum) tuple before invoking. This package owns the
// wire-side orchestration, not the kube-client-side discovery.
//
// Gating rule. For each CR:
//
//   - If MasterIP is empty (operator can't currently determine
//     which pod is master), SKIP — natural sentinel-driven recovery
//     is safer than a blind RESET that wipes topology.
//   - Probe each sentinel via SENTINEL get-master-addr-by-name.
//     If every reachable sentinel reports MasterIP exactly (host
//     part equals the operator's view), the cluster is consistent
//     — SKIP.
//   - Otherwise, an anomaly exists (stale master IP on at least one
//     sentinel, or a sentinel reporting an addr the operator can't
//     match against a live pod). Fire RESET, then MONITOR with the
//     operator's MasterIP, then re-propagate auth-pass.
type InitialResetTarget struct {
	CR         types.NamespacedName
	MasterName string
	Endpoints  []Endpoint
	Password   string
	// MasterIP is the IP address of the pod the operator currently
	// believes is the active Valkey master. Empty when the operator
	// can't determine (no pod reports role:master via INFO
	// replication; no pod labelled role=primary; cluster in initial
	// bootstrap). An empty MasterIP forces the gate to SKIP — RESET
	// without a target IP is the load-bearing failure mode this
	// rewrite is fixing.
	MasterIP string
	// Port is the Valkey data-plane port (typically 6379). Required
	// to construct the MONITOR command's <port> argument.
	Port int
	// Quorum is the spec.sentinel.quorum value — written into
	// sentinel state via MONITOR's <quorum> argument. Sentinel uses
	// it to decide failover eligibility.
	Quorum int
	// Tuning re-propagates the per-master timing knobs after
	// REMOVE + MONITOR. MONITOR re-populates with Sentinel's
	// hardcoded defaults, erasing the operator's tuning. Zero
	// values skip the corresponding SET (no-op tuning still works).
	Tuning MasterTuning
	// AllowGhostReap permits the ghost-reap class (REMOVE + MONITOR on
	// a gossiping survivor that still knows a dead peer run-id) on this
	// pass. The controller sets it false during a brewing election —
	// any survivor reports the master +odown — because the PING +
	// failover-in-progress surgery gates do not cover the
	// +sdown→+odown vote-gathering window, and false on the startup
	// path (RunInitialReset stays empty-peer-only). Empty-peer
	// stranded recovery is unaffected by this flag.
	AllowGhostReap bool
	// LiveValkeyIPs is the set of PodIPs of the CR's current valkey
	// (data-plane) pods. Enables the dead-master re-point class: a
	// sentinel whose monitored master addr matches NO live valkey pod
	// is monitoring a corpse and gets REMOVE + MONITOR'd at MasterIP
	// even though its peer-list is intact — the post-failover-storm
	// wedge where every address the quorum knows is dead. A nil/empty
	// set disables the class entirely (fail-safe against a degraded
	// pod-list read mass-re-pointing healthy sentinels); the startup
	// path leaves it nil on purpose.
	LiveValkeyIPs map[string]struct{}
	// AllowStaleEpochRepoint gates the live-but-different StaleEpoch
	// sub-class of the re-point (mirrors AllowGhostReap). When set, a
	// sentinel monitoring a LIVE-but-different pod that is provably behind
	// the per-sentinel config-epoch total order — and neither it nor the
	// destination is mid-election — is force-converged onto masterIP via
	// REMOVE + MONITOR. The startup path (RunInitialReset) leaves it false
	// and also leaves LiveValkeyIPs nil, so the whole re-point class stays
	// off there.
	AllowStaleEpochRepoint bool
	// SkipStrandedAddrs is the per-address re-wipe pacing set: keyed by
	// Endpoint.Addr, it names empty-peer stranded sentinels the caller
	// has decided NOT to re-wipe this pass because a prior wipe made no
	// progress and its lengthened per-address cadence has not elapsed (a
	// NetworkPolicy blocking `__sentinel__:hello` is the canonical
	// permanent-wedge cause). Applies ONLY to the empty-peer class:
	// matching sentinels are reported in SkippedStranded and excluded
	// from REMOVE + MONITOR, while a different freshly-stranded sentinel
	// on the same CR is still wiped in the same pass. A nil/empty set
	// wipes every empty-peer stranded sentinel (the pre-pacing
	// behaviour). The dead-master re-point and ghost-reap classes are
	// unaffected.
	SkipStrandedAddrs map[string]struct{}
}

// InitialResetOutcome reports, per CR, the stranded sentinels a
// RunInitialReset pass actually fired REMOVE + MONITOR against. The
// slice carries one entry only for CRs where recovery genuinely
// dispatched (probe found stranded sentinels AND a quorum was
// reachable) — CRs that no-op'd (consistent peer-lists, missing master
// IP, minority reachable) produce no entry. The controller uses it to
// emit an accurate `sentinel_reset_issued` audit entry without
// re-deriving the probe result, keeping this package audit-free.
type InitialResetOutcome struct {
	CR      types.NamespacedName
	Targets []string
}

// GhostHolder is a gossiping survivor (non-empty peer-list) that still
// knows at least one dead peer run-id — a "ghost" whose announced IP
// belongs to no current sentinel pod. The reap policy (debounce +
// one-at-a-time cap) is applied by the caller, not the pure
// classifier.
type GhostHolder struct {
	// Endpoint is the survivor sentinel holding the ghost(s).
	Endpoint Endpoint
	// GhostIPs are the peer IPs it knows that are absent from the live
	// sentinel-pod set.
	GhostIPs []string
}

// strandedClassification is the result of classifyStrandedSentinels:
// the sentinels selected for REMOVE + MONITOR recovery, their names,
// the count of reachable sentinels, and whether a quorum is reachable.
type strandedClassification struct {
	Targets   []Endpoint
	Stranded  []string
	Reachable int
	// QuorumReachable is false when fewer than QuorumThreshold of the
	// endpoints are reachable — the minority guard: a reachable
	// minority pointed at a possibly-stale master must not REMOVE +
	// MONITOR while the unreachable majority may hold a higher
	// config-epoch. Same threshold the observer's relabel path uses.
	QuorumReachable bool
	// GhostHolders are gossiping survivors that still know a dead peer
	// run-id. Populated only when liveSentinelIPs is non-empty;
	// reaping them is gated by the caller's debounce +
	// one-at-a-time cap (see RecoverStrandedSentinels). Distinct from
	// Targets so the empty-peer whole-cluster recovery is never capped.
	GhostHolders []GhostHolder
}

// classifyStrandedSentinels inspects peerView for the empty-peer-list
// "rebuilt pod with no preserved peer state" signature, counts the
// reachable sentinels, and reports whether a quorum is reachable. It
// is the shared classification + minority-guard chokepoint for both
// the startup safety-net (RunInitialReset) and the per-reconcile wedge
// recovery (RecoverStrandedSentinels) — keeping it in one place stops
// the split-brain guard drifting between the two callers.
//
// recoverErr decides whether a sentinel that answered with an error is
// nonetheless reachable-and-stranded. The wedge path passes
// isNoSuchMasterErr (a "no such master" reply is a recovery pass
// interrupted between REMOVE and MONITOR — reachable and stranded, and
// it must count toward reachable so the guard is not wrongly tripped);
// the startup path passes nil (every error is unclassifiable and
// skipped).
//
// liveSentinelIPs is the set of IPs of the current sentinel pods (the
// host part of each Endpoint.Addr, which IS the sentinel's announce-ip
// == status.podIP via the Downward API — same field on both sides,
// IP-only addressing webhook-enforced). A gossiping survivor that
// names a peer whose IP is in NO current pod is holding a dead ghost
// run-id; such survivors land in GhostHolders. A nil/empty set
// disables ghost detection entirely (startup path, or a degraded
// pod-list read) so a missing set can never drive a reap.
func classifyStrandedSentinels(
	peerView []SentinelsResult,
	endpoints []Endpoint,
	recoverErr func(error) bool,
	liveSentinelIPs map[string]struct{},
) strandedClassification {
	c := strandedClassification{}
	for i, r := range peerView {
		if r.Err != nil {
			if recoverErr != nil && recoverErr(r.Err) {
				c.Reachable++
				c.Targets = append(c.Targets, endpoints[i])
				c.Stranded = append(c.Stranded, endpoints[i].Name)
			}
			continue
		}
		c.Reachable++
		if len(r.Peers) == 0 {
			c.Targets = append(c.Targets, endpoints[i])
			c.Stranded = append(c.Stranded, r.Name)
			continue
		}
		// Ghost detection: a gossiping survivor still knowing a peer
		// whose IP is in no live sentinel pod. Empty-IP peers are never
		// ghosts (the wire parser already drops them; the synthetic
		// in-memory path is the only empty-IP source).
		if len(liveSentinelIPs) == 0 {
			continue
		}
		var ghosts []string
		for _, p := range r.Peers {
			if p.IP == "" {
				continue
			}
			if _, live := liveSentinelIPs[p.IP]; !live {
				ghosts = append(ghosts, p.IP)
			}
		}
		if len(ghosts) > 0 {
			c.GhostHolders = append(c.GhostHolders, GhostHolder{
				Endpoint: endpoints[i],
				GhostIPs: ghosts,
			})
		}
	}
	c.QuorumReachable = c.Reachable >= QuorumThreshold(len(endpoints))
	return c
}

// liveSentinelIPSet builds the set of live sentinel IPs from the host
// part of each endpoint's Addr. The host IS the sentinel's announce-ip
// (== status.podIP, Downward API; IP-only addressing webhook-enforced),
// so membership in this set is an exact "is this peer a live pod" test.
// Unparseable / empty hosts are skipped.
func liveSentinelIPSet(endpoints []Endpoint) map[string]struct{} {
	s := make(map[string]struct{}, len(endpoints))
	for _, ep := range endpoints {
		host, _, err := net.SplitHostPort(ep.Addr)
		if err != nil || host == "" {
			continue
		}
		s[host] = struct{}{}
	}
	return s
}

// repointClassification splits the re-point candidates into the two
// disjoint sub-classes the surgery treats differently:
//   - DeadMaster: monitored master addr matches no live valkey pod (a
//     corpse). Doomed-election filtered — shipped behaviour, unchanged.
//   - StaleEpoch: monitored master is a LIVE-but-different pod, and that
//     sentinel is provably behind the config-epoch total order (neither
//     it nor the destination mid-election). Bypasses the doomed filter
//     (its replica table has LIVE entries, so doomed would wrongly
//     reject it); its safety is the epoch total-order + the election
//     guards + the pre-surgery fresh re-check of BOTH the targets and
//     the destination cohort.
type repointClassification struct {
	DeadMaster []Endpoint
	StaleEpoch []Endpoint
	// StaleEpochCohort names the masterIP-cohort sentinels (monitored
	// host == masterIP) captured at classify, and AgreeEpoch is the max
	// config-epoch over that cohort (the classify-time frontier the
	// StaleEpoch class was armed against). Both are threaded to the
	// pre-surgery destination re-check so it can re-validate — on fresh
	// SENTINEL MASTER reads — that the destination is neither mid-election
	// nor fallen below AgreeEpoch since classification, symmetric to the
	// per-target re-check. Populated whenever the class-level guards pass
	// — including the common armed pass where every sentinel already
	// agrees on masterIP and StaleEpoch is empty — so consumers must gate
	// on len(StaleEpoch), never on the cohort, as
	// dropElectingStaleTargets does. All-zero only on a class disarm.
	StaleEpochCohort []Endpoint
	AgreeEpoch       int64
}

// selectRepointTargets classifies the re-point candidates into the
// dead-master (corpse) and stale-epoch (live-but-different, behind the
// config-epoch frontier) sub-classes.
//
// DeadMaster candidates are filtered through the doomed-election
// discriminator: a corpse-monitoring sentinel whose replica table still
// names a LIVE pod has a viable candidate — its own election (or a
// peer's) can succeed, and wiping it mid-vote could abort a recovery
// Sentinel was about to complete. Only a sentinel whose entire
// known-candidate set is dead is provably unrecoverable by Sentinel
// itself. An errored table is UNKNOWN, never doomed. StaleEpoch
// candidates get NO doomed filter (their replica table is live by
// construction); the epoch total-order + classify-time election guards
// carry their safety, with a pre-surgery fresh re-check in the caller.
//
// Entries already in beingWiped (empty-peer class) are skipped in both
// sub-classes; a nil/empty live set disables the whole class, and
// allowStaleEpoch=false leaves the StaleEpoch sub-class empty. ReplicasAll
// runs on the DeadMaster candidates only.
func selectRepointTargets(
	ctx context.Context,
	endpoints []Endpoint,
	masterName, password, masterIP string,
	liveValkeyIPs map[string]struct{},
	beingWiped map[string]struct{},
	allowStaleEpoch bool,
) repointClassification {
	var out repointClassification
	if len(liveValkeyIPs) == 0 {
		return out
	}
	probeView := ProbeAll(ctx, endpoints, masterName, password)
	classified := classifyRepointTargets(probeView, endpoints, liveValkeyIPs, masterIP, allowStaleEpoch)

	// Carry the classify-time destination cohort + frontier through
	// unfiltered — they are the destination (masterIP) endpoints, not
	// re-point targets, so the beingWiped filter below must not touch
	// them; the pre-surgery destination re-check reads them fresh.
	out.StaleEpochCohort = classified.StaleEpochCohort
	out.AgreeEpoch = classified.AgreeEpoch

	// StaleEpoch: skip beingWiped, no doomed filter — pass through.
	for _, ep := range classified.StaleEpoch {
		if _, dup := beingWiped[ep.Name]; dup {
			continue
		}
		out.StaleEpoch = append(out.StaleEpoch, ep)
	}

	// DeadMaster: doomed-election filter (unchanged shipped behaviour).
	if len(classified.DeadMaster) > 0 {
		replicaViews := ReplicasAll(ctx, classified.DeadMaster, masterName, password)
		for i, ep := range classified.DeadMaster {
			if _, dup := beingWiped[ep.Name]; dup {
				continue
			}
			rv := replicaViews[i]
			if rv.Err != nil || !replicaTableDoomed(rv.Replicas, liveValkeyIPs) {
				continue
			}
			out.DeadMaster = append(out.DeadMaster, ep)
		}
	}
	return out
}

// replicaTableDoomed reports whether a sentinel's known-replica table
// proves its own failover election can never succeed: every replica
// it knows is dead (no live valkey pod carries the IP), or it knows
// none at all. A single live entry means Sentinel has a viable
// promotion candidate and the operator must not intervene.
func replicaTableDoomed(replicas []ReplicaInfo, liveValkeyIPs map[string]struct{}) bool {
	for _, r := range replicas {
		if _, live := liveValkeyIPs[r.IP]; live {
			return false
		}
	}
	return true
}

// QuorumElectionDoomed fans out SENTINEL REPLICAS to the endpoints and
// reports whether a sentinel-side failover election is provably unable
// to succeed at quorum level: at least QuorumThreshold(len(endpoints))
// sentinels answered AND no reachable table names a live valkey pod.
// An errored table is UNKNOWN (counts as unreachable, never as
// doomed). This is the single authority for the doomed-election rule —
// the zero-master recovery election consults it before promoting, and
// the per-target re-point filter (selectRepointTargets) applies the
// same replicaTableDoomed primitive per sentinel.
func (m *Manager) QuorumElectionDoomed(ctx context.Context, endpoints []Endpoint, masterName, password string, liveValkeyIPs map[string]struct{}) bool {
	if len(endpoints) == 0 || len(liveValkeyIPs) == 0 {
		return false
	}
	views := ReplicasAll(ctx, endpoints, masterName, password)
	reachable := 0
	for _, rv := range views {
		if rv.Err != nil {
			continue
		}
		reachable++
		if !replicaTableDoomed(rv.Replicas, liveValkeyIPs) {
			return false
		}
	}
	return reachable >= QuorumThreshold(len(endpoints))
}

// classifyRepointTargets selects reachable sentinels to re-point onto
// the resolved master, split into two disjoint sub-classes.
//
// DeadMaster (corpse) — a sentinel whose monitored master address
// (SENTINEL get-master-addr-by-name) matches no live valkey pod. The
// pod object behind the monitored addr no longer exists (API-server
// truth, not a reachability guess), so the sentinel can never observe
// the master recover, and in the total-wedge state its known-replica
// set is equally dead so its own election can never succeed. A sentinel
// already reporting masterIP is left alone.
//
// StaleEpoch (live-but-different) — a sentinel monitoring a DIFFERENT
// LIVE pod that is PROVABLY behind the per-sentinel config-epoch total
// order, with neither it nor the destination mid-election. Historically
// this view was left untouched (gossip / Phase 7 own the
// reconciliation), but the post-real-failover lag slice can wedge it: a
// sentinel stuck on the pre-failover primary at a stale config-epoch
// while the quorum has moved on. Promoting that skip into a re-point is
// gated ONLY when the divergent sentinel is strictly behind the frontier
// AND the operator's masterIP view is itself current (see
// classifyStaleEpochTargets); computed only when allowStaleEpoch is set.
//
// Probe errors and empty addrs are skipped — the unreachable /
// no-such-master classes own those states. A nil/empty liveValkeyIPs or
// empty masterIP disables the whole class (fail-safe: a degraded
// pod-list read must never mass-re-point healthy sentinels).
func classifyRepointTargets(probeView []ProbeResult, endpoints []Endpoint, liveValkeyIPs map[string]struct{}, masterIP string, allowStaleEpoch bool) repointClassification {
	var out repointClassification
	if len(liveValkeyIPs) == 0 || masterIP == "" {
		return out
	}
	for i, r := range probeView {
		if r.Err != nil || r.Addr == "" {
			continue
		}
		host, _, err := net.SplitHostPort(r.Addr)
		if err != nil || host == "" {
			continue
		}
		if host == masterIP {
			continue
		}
		if _, live := liveValkeyIPs[host]; live {
			continue
		}
		out.DeadMaster = append(out.DeadMaster, endpoints[i])
	}
	if allowStaleEpoch {
		out.StaleEpoch, out.StaleEpochCohort, out.AgreeEpoch =
			classifyStaleEpochTargets(probeView, endpoints, liveValkeyIPs, masterIP)
	}
	return out
}

// classifyStaleEpochTargets selects the live-but-different re-point
// sub-class: sentinels monitoring a LIVE pod other than masterIP that
// are provably behind the per-sentinel config-epoch total order. Epoch
// and flags ride the same armed-pass ProbeAll (one SENTINEL MASTER per
// sentinel on the already-open conn).
//
// Epoch domain — an epoch-eligible probe is reachable (Err==nil),
// monitoring (Addr!=""), and carries a parsed epoch (EpochOK). Empty-addr
// stragglers never participate and never trip the conservative disarm.
//   - frontierEpoch = max epoch over epoch-eligible probes.
//   - agreeEpoch    = max epoch over epoch-eligible probes on masterIP.
//
// Class-level disarms (any ⇒ empty StaleEpoch this pass; DeadMaster
// unaffected):
//   - Pre-A conservative fail-safe: any reachable, monitoring sentinel
//     whose inline epoch read failed could be ahead of the frontier —
//     never let it vanish from the total order; disarm.
//   - Guard A (operator view current): unless a masterIP cohort exists
//     AND agreeEpoch==frontierEpoch, the operator's view may be stale
//     (some sentinel holds an epoch above masterIP's) — disarm. This
//     also no-ops the class through operator-MONITOR-reset states, where
//     the surgery reset masterIP's epoch to 0 (agreeEpoch==0<frontier).
//   - dest-election guard (destination election, pull-side): if any masterIP-cohort
//     sentinel's own flags show an election, the quorum is vote-gathering
//     to depose the destination — the wrong-direction case; disarm.
//
// Per-target guards:
//   - Guard B (target strictly behind, STRICT): select a divergent live
//     sentinel j iff j.EpochOK && j.Epoch < agreeEpoch. Proving DIRECTION
//     is what lets epoch beat a time-dwell: it can never fire the
//     wrong-direction re-point a stale-but-live operator view would trick
//     a dwell into. j at/above agreeEpoch may be ahead ⇒ skip.
//   - self-election guard (target's own election window): skip j whose own master
//     flags show an election — its epochs are not yet settled, so a
//     straggler cannot be told from a legitimate advancer.
//
// Returns the selected targets, the masterIP cohort (endpoints monitoring
// masterIP), and agreeEpoch (the cohort frontier). The cohort + agreeEpoch
// feed the caller's pre-surgery destination re-check; they are populated
// whenever the class-level guards pass, even when the second loop selects
// ZERO targets (the common armed pass where every sentinel agrees on
// masterIP) — a disarm returns all-zero. Consumers must gate on the
// target set, never on cohort emptiness.
func classifyStaleEpochTargets(probeView []ProbeResult, endpoints []Endpoint, liveValkeyIPs map[string]struct{}, masterIP string) (targets, cohort []Endpoint, agreeEpoch int64) {
	var frontierEpoch int64
	masterIPCohortExists := false
	for i, r := range probeView {
		if r.Err != nil || r.Addr == "" {
			continue
		}
		host, _, err := net.SplitHostPort(r.Addr)
		if err != nil || host == "" {
			continue
		}
		// Pre-A: a monitoring sentinel with an unprovable epoch could be
		// ahead of the frontier — disarm rather than let it vanish.
		if !r.EpochOK {
			return nil, nil, 0
		}
		if host == masterIP {
			// dest-election guard: an election on the destination's own master is
			// the wrong-direction case, caught before epochs bump.
			if flagsIndicateElection(r.Flags) {
				return nil, nil, 0
			}
			masterIPCohortExists = true
			cohort = append(cohort, endpoints[i])
			if r.Epoch > agreeEpoch {
				agreeEpoch = r.Epoch
			}
		}
		if r.Epoch > frontierEpoch {
			frontierEpoch = r.Epoch
		}
	}
	// Guard A: the operator's masterIP view must sit at the frontier.
	if !masterIPCohortExists || agreeEpoch != frontierEpoch {
		return nil, nil, 0
	}
	for i, r := range probeView {
		if r.Err != nil || r.Addr == "" || !r.EpochOK {
			continue
		}
		host, _, err := net.SplitHostPort(r.Addr)
		if err != nil || host == "" || host == masterIP {
			continue
		}
		if _, live := liveValkeyIPs[host]; !live {
			continue // corpse — the DeadMaster sub-class owns it.
		}
		// Guard B (strict): provably behind the config-epoch frontier.
		if r.Epoch >= agreeEpoch {
			continue
		}
		// self-election guard: skip a target mid/pre-election on its own master.
		if flagsIndicateElection(r.Flags) {
			continue
		}
		targets = append(targets, endpoints[i])
	}
	return targets, cohort, agreeEpoch
}

// selectGhostReapTarget updates the per-CR ghost-reap debounce state
// from this pass's detected ghosts and returns the single survivor to
// reap, or nil. It always refreshes the debounce (so the absent-time
// keeps accruing even on passes where allow is false, e.g. a brewing
// election deferred the reap); it returns a target only when allow is
// true. Selection requires a debounced ghost (continuously absent
// ≥ ghostReapDebounce — closes the cache-lag false-positive window);
// ghosts are reaped as healthy-state hygiene rather than on demand,
// because a ghost carried into a dead-master incident inflates the
// failover election majority when reaping is already +odown-vetoed.
// At most one survivor is returned (lowest name) — the one-at-a-time
// cap that keeps the surviving quorum's votes from being zeroed
// together.
func (m *Manager) selectGhostReapTarget(cr observerKey, holders []GhostHolder, liveIPs map[string]struct{}, allow bool) *Endpoint {
	now := m.clock()
	detected := make(map[string]struct{})
	for _, h := range holders {
		for _, ip := range h.GhostIPs {
			detected[ip] = struct{}{}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	seen := m.ghostSeen[cr]
	if seen == nil {
		seen = make(map[string]time.Time)
	}
	// Prune entries that reappeared in the live set or are no longer
	// detected as ghosts this pass (a transient ghost that vanished
	// must not carry a stale first-seen timestamp forward).
	for ip := range seen {
		if _, isLive := liveIPs[ip]; isLive {
			delete(seen, ip)
			continue
		}
		if _, still := detected[ip]; !still {
			delete(seen, ip)
		}
	}
	// Stamp first-seen-absent for newly detected ghosts.
	for ip := range detected {
		if _, ok := seen[ip]; !ok {
			seen[ip] = now
		}
	}
	if len(seen) == 0 {
		delete(m.ghostSeen, cr)
	} else {
		m.ghostSeen[cr] = seen
	}

	if !allow {
		return nil
	}
	var chosen *Endpoint
	for i := range holders {
		h := holders[i]
		// No demand-gate: ghosts are reaped as healthy-state hygiene on
		// debounce alone. The old gate (reap only when the ghost-inflated
		// election majority exceeded the live count) was unsatisfiable
		// exactly when it mattered — demand only materializes together
		// with an incident that sets +odown, and AllowGhostReap vetoes
		// reaping under +odown. A ghost carried into that incident then
		// blocks the sentinel election outright.
		reapable := false
		for _, ip := range h.GhostIPs {
			if ts, ok := seen[ip]; ok && now.Sub(ts) >= ghostReapDebounce {
				reapable = true
				break
			}
		}
		if !reapable {
			continue
		}
		if chosen == nil || h.Endpoint.Name < chosen.Name {
			ep := h.Endpoint
			chosen = &ep
		}
	}
	return chosen
}

func (m *Manager) RunInitialReset(ctx context.Context, targets []InitialResetTarget) []InitialResetOutcome {
	logger := log.FromContext(ctx).WithName(ManagerName)
	var outcomes []InitialResetOutcome
	for _, t := range targets {
		if len(t.Endpoints) == 0 {
			continue
		}
		if t.MasterIP == "" {
			// Defensive — operator could not determine current
			// master. RESET would wipe sentinel topology and leave
			// the cluster wedged at a stale on-disk pointer. Skip;
			// the per-reconcile stranded-sentinel detector picks up
			// recovery once observedMasterIP resolves.
			logger.Info("startup safety-net: master IP unknown; skipping RESET",
				"cr", t.CR.String(), "endpoints", len(t.Endpoints))
			continue
		}

		// Recovery strategy: REMOVE + MONITOR only on sentinels whose
		// peer-list is empty (`SENTINEL SENTINELS <name>` returns 0
		// peers). That is the unambiguous "rebuilt pod with no
		// preserved peer state" signature. Sentinels with non-empty
		// peer-lists are participating in gossip and will self-recover
		// via their own +odown → failover-election loop — touching
		// them is what caused the rc.10 / rc.11 wedge.
		//
		// REMOVE (not RESET) is the load-bearing primitive here — see
		// RemoveAll's doc for the "Duplicated master name" failure mode
		// plain RESET would leave the rebuilt sentinel wedged in.
		//
		// We do NOT run a SENTINEL SET known-sentinel pass — that
		// command is not a valid runtime option in Valkey or Redis
		// (sentinelSetCommand rejects unknown options with -ERR;
		// `known-sentinel` is a sentinel.conf directive only,
		// loaded at sentinel boot from disk). The rebuilt sentinel
		// rebuilds its peer-list via Pub/Sub gossip on the
		// `__sentinel__:hello` channel of its newly-MONITORed
		// master, populated by the surviving sentinels.
		// Unreachable sentinels can't be classified (recoverErr=nil):
		// skip them; the per-reconcile detector retries once they
		// come back.
		// Startup stays empty-peer-only (nil live-IP set disables ghost
		// detection): a leader-acquire must not mass-touch gossiping
		// survivors before the per-reconcile debounce state exists.
		peerView := SentinelsAll(ctx, t.Endpoints, t.MasterName, t.Password)
		classified := classifyStrandedSentinels(peerView, t.Endpoints, nil, nil)
		recoverTargets := classified.Targets
		stranded := classified.Stranded

		if len(recoverTargets) == 0 {
			logger.V(1).Info("startup safety-net: no stranded sentinels; skipping recovery",
				"cr", t.CR.String(), "masterIP", t.MasterIP,
				"endpoints", len(t.Endpoints))
			continue
		}

		// Minority guard: defer to the per-reconcile detector when fewer
		// than a quorum is reachable (see strandedClassification).
		if !classified.QuorumReachable {
			logger.Info("startup safety-net: only a minority of sentinels reachable; skipping REMOVE + MONITOR",
				"cr", t.CR.String(), "masterIP", t.MasterIP,
				"reachable", classified.Reachable, "endpoints", len(t.Endpoints),
				"quorum", QuorumThreshold(len(t.Endpoints)), "stranded", stranded)
			continue
		}

		logger.Info("startup safety-net: stranded sentinel(s) detected; firing REMOVE + MONITOR",
			"cr", t.CR.String(), "masterIP", t.MasterIP,
			"stranded", stranded, "recoverTargets", len(recoverTargets))
		results := RemoveAll(ctx, recoverTargets, t.MasterName, t.Password)
		// Record that this CR actually dispatched a reset (REMOVE+MONITOR
		// against the stranded set) so the controller can audit the
		// issuance accurately — the per-CR no-op paths above never reach
		// here, so they correctly produce no outcome/audit entry.
		outcomes = append(outcomes, InitialResetOutcome{CR: t.CR, Targets: stranded})
		var success, failure int
		var failed []string
		for _, r := range results {
			if r.Err == nil {
				success++
			} else {
				failure++
				failed = append(failed, r.Name)
			}
		}
		if m.recorder != nil {
			obj := &valkeyv1beta1.Valkey{}
			obj.Name = t.CR.Name
			obj.Namespace = t.CR.Namespace
			msg := fmt.Sprintf("startup safety-net SENTINEL REMOVE fired against %d stranded sentinels (%d ok, %d failed); stranded: %v",
				len(results), success, failure, stranded)
			if failure > 0 {
				msg = fmt.Sprintf("%s — failed: %v", msg, failed)
			}
			m.recorder.Eventf(obj, nil, corev1.EventTypeNormal,
				string(events.InitialSentinelReset), "InitialSentinelReset", "%s", msg)
		}

		// Filter to endpoints whose REMOVE actually succeeded —
		// only those are safe to MONITOR (MONITOR on an entry that
		// still exists returns "-ERR Duplicated master name").
		successfulEndpoints := make([]Endpoint, 0, len(results))
		successByName := make(map[string]struct{}, len(results))
		for _, r := range results {
			if r.Err == nil {
				successByName[r.Name] = struct{}{}
			}
		}
		for _, ep := range recoverTargets {
			if _, ok := successByName[ep.Name]; ok {
				successfulEndpoints = append(successfulEndpoints, ep)
			}
		}
		if len(successfulEndpoints) > 0 {
			monitorResults := MonitorAll(ctx, successfulEndpoints, t.MasterName, t.MasterIP, t.Password, t.Port, t.Quorum)
			m.emitMonitorEvents(t.CR, monitorResults)
			// Restore per-master tuning erased by MONITOR's
			// default-population (down-after-millis, failover-
			// timeout, parallel-syncs). Without this the rebuilt
			// sentinels honour Sentinel's hardcoded 30s default
			// and lag the rest of the cluster on future failovers.
			m.emitTuningEvents(t.CR, SetMasterTuningAll(ctx, successfulEndpoints, t.MasterName, t.Tuning, t.Password))
		} else {
			logger.Info("startup safety-net: every REMOVE failed; skipping MONITOR follow-up",
				"cr", t.CR.String(), "endpoints", len(t.Endpoints))
		}

		// Auth propagation rides on the same startup safety-net pass.
		// All reachable sentinels for this CR get auth-pass via SET +
		// verify; pods we just REMOVE+MONITOR'd had their auth-pass
		// wiped by definition. Skipped when password is empty
		// (no-AUTH CR).
		_ = m.IssueAuthPass(ctx, t.CR, t.MasterName, t.Password, t.Endpoints)
	}
	return outcomes
}

// StrandedRecoveryResult is the outcome of one RecoverStrandedSentinels
// pass. Reset/Monitor results carry per-sentinel wire outcomes; the
// manager emits the matching Kubernetes events internally (emitResetEvents
// / emitMonitorEvents), so the caller consumes the result purely for
// audit + observability.
type StrandedRecoveryResult struct {
	// Stranded names the sentinel pods this pass classified as
	// stranded (empty peer-list) PLUS any capped ghost-reap survivor
	// folded into the same REMOVE + MONITOR pass. Used for the
	// SentinelStrandedRecovery event / audit.
	Stranded []string
	// EmptyPeerStranded is the WIPED empty-peer stranded class (no
	// ghost-reap / re-point targets, and skip-set excluded — the
	// deliberately un-wiped subset lands in SkippedStranded instead).
	// The caller's no-progress linkup-stuck detector reads THIS — those
	// other classes have intact peer-lists and a "peer count still 0"
	// read-back does not apply to them.
	EmptyPeerStranded []string
	// SkippedStranded names the empty-peer stranded sentinels this pass
	// deliberately did NOT wipe because the caller listed their
	// Endpoint.Addr in SkipStrandedAddrs (per-address re-wipe pacing).
	// The caller carries these forward unchanged: their no-progress
	// count and last-wipe clock are preserved so a wedged sentinel is
	// re-probed every base window but re-wiped only on its lengthened
	// per-address cadence.
	SkippedStranded []string
	// Repointed names the sentinel pods this pass classified as
	// dead-master re-point targets (intact peer-list, monitored
	// master addr matching no live valkey pod). They ride the same
	// REMOVE + MONITOR surgery as Stranded; surfaced separately for
	// events/audit.
	Repointed []string
	// StaleEpochRepointed names the live-but-different stale-epoch
	// re-point class this pass converged: sentinels monitoring a LIVE pod
	// other than masterIP that were proven strictly behind the config-epoch
	// total order. They ride the same REMOVE + MONITOR surgery; surfaced
	// separately for events/audit. NOT folded into EmptyPeerStranded —
	// their peer-list is intact, a different convergence signal, same as
	// Repointed.
	StaleEpochRepointed []string
	// ResetResults / MonitorResults are non-nil iff the pass
	// actually fired RESET + MONITOR. Empty slices when no
	// stranded sentinels were detected (the happy case).
	ResetResults   []ResetResult
	MonitorResults []MonitorResult
	// AuthFailures names the wiped sentinel pods whose post-MONITOR
	// auth-pass re-propagation failed verification. A sentinel that
	// cannot AUTH against its master can never subscribe to
	// __sentinel__:hello, so it can never rebuild its peer-list — a
	// guaranteed no-progress cause the caller's stuck detector treats
	// as an immediate wedge. Empty when password is unset or all
	// re-propagations verified. Per-pod SentinelAuthNotApplied Warning
	// events are still emitted by IssueAuthPass.
	AuthFailures []string
	// Healthy is true only when this pass ran the full classification
	// and found NO stranded / re-point / ghost target — an
	// authoritative "the ensemble's peer-lists are intact" verdict,
	// distinct from a gate-deferred pass (minority / PING / failover /
	// deferral predicate), which returns the zero value with
	// Healthy=false so the caller leaves its no-progress state alone.
	Healthy bool
	// Probed is true iff this pass reached classification (SentinelsAll
	// ran). The caller stamps its probe-cadence clock only on Probed, so
	// the SentinelsAll classification probe debounces to base cadence for
	// BOTH fresh-strand pickup and wedged-sentinel recovery-detection.
	// The two pre-classification early-returns (empty endpoints/masterIP;
	// deferral predicate) leave it false; the three post-classification
	// gate-defers (minority / master-PING / failover-in-progress) set it
	// true so a sustained defer still debounces the classification probe.
	Probed bool
}

// partitionEmptyPeerClass splits the classified empty-peer stranded set
// by the per-address skip-set (keyed by Endpoint.Addr). It returns the
// skip-filtered REMOVE + MONITOR target slice, the wiped and skipped name
// lists (derived from ep.Name so the caller's addr-by-name map resolves
// them), and a beingWiped set seeded from the FULL class — wiped OR
// skipped — so the re-point / ghost selectors that consult beingWiped can
// never re-classify a paced empty-peer sentinel and wipe it behind the
// skip's back. A nil/empty skip set wipes every empty-peer sentinel.
func partitionEmptyPeerClass(targets []Endpoint, skip map[string]struct{}) (resetTargets []Endpoint, wipedNames, skippedNames []string, beingWiped map[string]struct{}) {
	beingWiped = make(map[string]struct{}, len(targets)+1)
	for _, ep := range targets {
		beingWiped[ep.Name] = struct{}{}
		if _, skipped := skip[ep.Addr]; skipped {
			skippedNames = append(skippedNames, ep.Name)
			continue
		}
		resetTargets = append(resetTargets, ep)
		wipedNames = append(wipedNames, ep.Name)
	}
	return resetTargets, wipedNames, skippedNames, beingWiped
}

// foldRepointClassification folds the re-point classification into the
// REMOVE + MONITOR set: DeadMaster (corpse) targets into out.Repointed
// and StaleEpoch (live-but-different) targets into out.StaleEpochRepointed.
// Both join beingWiped so the survivor election-veto set excludes them.
// The stale-epoch subset is returned separately for the pre-surgery
// re-check that may drop any that began an election since classify.
func foldRepointClassification(classification repointClassification, resetTargets []Endpoint, beingWiped map[string]struct{}, out *StrandedRecoveryResult) (updatedReset, staleEpochTargets []Endpoint) {
	for _, ep := range classification.DeadMaster {
		beingWiped[ep.Name] = struct{}{}
		resetTargets = append(resetTargets, ep)
		out.Repointed = append(out.Repointed, ep.Name)
	}
	for _, ep := range classification.StaleEpoch {
		beingWiped[ep.Name] = struct{}{}
		resetTargets = append(resetTargets, ep)
		out.StaleEpochRepointed = append(out.StaleEpochRepointed, ep.Name)
		staleEpochTargets = append(staleEpochTargets, ep)
	}
	return resetTargets, staleEpochTargets
}

// dropElectingStaleTargets is the pre-surgery fresh re-check on the
// stale-epoch subset. It re-validates BOTH sides of the re-point on fresh
// SENTINEL MASTER reads immediately before RemoveAll — symmetric, because
// the classify→RemoveAll window can move EITHER endpoint:
//   - Per target: recheckElectionQuiet drops any target that now indicates
//     an election (or cannot be confirmed quiet — fail-safe).
//   - Destination cohort: when any target still survives, destinationCohortQuiet
//     re-reads the masterIP-cohort sentinels; the ENTIRE surviving stale-epoch
//     subset is disarmed if ANY member now shows an election (or cannot be
//     confirmed not-electing — error / no flags), OR the cohort's MAX epoch has
//     fallen below the classify-time agreeEpoch frontier (max-based, symmetric
//     with classify Guard A — a single below-max member does not disarm). The
//     classify-time destination guards (frontier Guard A; dest-election guard)
//     are otherwise never re-checked across the window, so this closes the
//     destination side symmetrically.
//
// Drops are removed from BOTH the reset set and out.StaleEpochRepointed.
// Stale-epoch targets bypass the doomed filter and are excluded from the
// gate-2 survivor veto, so this fresh read is the only time-independent
// guarantee neither the targets nor the destination began an election (or
// moved the frontier) since classification.
func dropElectingStaleTargets(ctx context.Context, staleEpochTargets, resetTargets, cohort []Endpoint, agreeEpoch int64, masterName, password string, out *StrandedRecoveryResult) []Endpoint {
	if len(staleEpochTargets) == 0 {
		return resetTargets
	}
	stillQuiet := recheckElectionQuiet(ctx, staleEpochTargets, masterName, password)
	// Symmetric destination re-check: only worth reading the cohort when a
	// target still survives the per-target re-check. If the destination can
	// no longer be proven safe, disarm the whole surviving subset.
	if len(stillQuiet) > 0 && !destinationCohortQuiet(ctx, cohort, masterName, password, agreeEpoch) {
		stillQuiet = nil
	}
	return applyStaleTargetDrops(staleEpochTargets, stillQuiet, resetTargets, out)
}

// applyStaleTargetDrops removes from resetTargets and out.StaleEpochRepointed
// every stale-epoch target NOT in the surviving `quiet` set. `quiet` may be a
// proper subset of staleEpochTargets (per-target elections dropped) or empty
// (the whole surviving subset disarmed by the destination re-check).
func applyStaleTargetDrops(staleEpochTargets, quiet, resetTargets []Endpoint, out *StrandedRecoveryResult) []Endpoint {
	quietNames := make(map[string]struct{}, len(quiet))
	for _, ep := range quiet {
		quietNames[ep.Name] = struct{}{}
	}
	drop := make(map[string]struct{})
	for _, ep := range staleEpochTargets {
		if _, ok := quietNames[ep.Name]; !ok {
			drop[ep.Name] = struct{}{}
		}
	}
	if len(drop) == 0 {
		return resetTargets
	}
	filteredReset := make([]Endpoint, 0, len(resetTargets))
	for _, ep := range resetTargets {
		if _, dropped := drop[ep.Name]; !dropped {
			filteredReset = append(filteredReset, ep)
		}
	}
	filteredStale := make([]string, 0, len(out.StaleEpochRepointed))
	for _, n := range out.StaleEpochRepointed {
		if _, dropped := drop[n]; !dropped {
			filteredStale = append(filteredStale, n)
		}
	}
	out.StaleEpochRepointed = filteredStale
	return filteredReset
}

// untouchedSurvivors selects the gate-2 failover-in-progress veto set:
// reachable sentinels with a non-empty peer-list that are NOT being
// wiped this pass (neither the ghost-reap target nor any beingWiped
// member) — the veto must read only survivors the surgery leaves alone.
func untouchedSurvivors(peerView []SentinelsResult, endpoints []Endpoint, ghostTarget *Endpoint, beingWiped map[string]struct{}) []Endpoint {
	healthy := make([]Endpoint, 0, len(endpoints))
	for i, r := range peerView {
		if r.Err != nil || len(r.Peers) == 0 {
			continue
		}
		if ghostTarget != nil && endpoints[i].Name == ghostTarget.Name {
			continue
		}
		if _, wiped := beingWiped[endpoints[i].Name]; wiped {
			continue
		}
		healthy = append(healthy, endpoints[i])
	}
	return healthy
}

// RecoverStrandedSentinels is the per-reconcile wedge recovery
// path. It probes the supplied endpoints for peer-list state and
// fires RESET + MONITOR on sentinels whose peer-list is empty (the
// rebuilt-with-no-preserved-state signature) and — when the caller
// supplies LiveValkeyIPs — on sentinels whose monitored master addr
// matches no live valkey pod (the dead-master re-point signature:
// the post-failover-storm state where the sentinel's master AND its
// entire known-replica set are corpses, so neither the sentinel's
// own election nor gossip can ever converge it).
//
// Caller is responsible for the gating decision (typically: only
// invoke this when the observer reports QuorumOK=false AND at
// least one sentinel disagrees with the operator's view of the
// master). This method does NOT consult observer state — it
// trusts the gate the caller already applied and operates on
// the snapshot of endpoints + masterIP it was given.
//
// Skips recovery entirely when fewer than a quorum of sentinels
// are reachable: a reachable minority must not REMOVE + MONITOR
// against a possibly-stale master while the unreachable majority
// may hold a higher config-epoch. Uses the same quorum threshold
// as the observer's relabel gate.
//
// Two further surgery gates run before anything destructive fires:
// the resolved master must answer PING (a freshly-MONITORed sentinel
// learns replicas and peers ONLY from its master — registering an
// empty sentinel at a dead address strands it permanently), and no
// healthy survivor may report failover_in_progress (the surgery must
// not race an election a survivor is mid-driving).
//
// bypassQuorumDeferral lets the caller run this method while the
// quorum-loss suppression gate holds the deferral predicate active.
// Sustained quorum loss with empty-peer sentinels is exactly the
// state this method repairs — deferring it unconditionally deadlocks
// recovery (the gate waits for quorum, quorum waits for the repair).
// The caller MUST pass false when a failover is in flight; the
// PING + failover-in-progress gates still apply on the bypass path.
//
// Caller should re-poll peer counts via MasterPeerCountAll after
// returning to confirm gossip has rebuilt the peer-list; the
// 2s+ gossip-interval makes synchronous verification impractical
// inside the reconcile path.
func (m *Manager) RecoverStrandedSentinels(
	ctx context.Context,
	target InitialResetTarget,
	bypassQuorumDeferral bool,
) StrandedRecoveryResult {
	cr := target.CR
	masterName := target.MasterName
	masterIP := target.MasterIP
	port := target.Port
	quorum := target.Quorum
	tuning := target.Tuning
	endpoints := target.Endpoints
	password := target.Password

	logger := log.FromContext(ctx).WithName(ManagerName)
	out := StrandedRecoveryResult{}
	if len(endpoints) == 0 || masterIP == "" {
		return out
	}

	// Honour the deferral predicate: REMOVE + MONITOR + tuning mutate
	// sentinel config state and must not race the config-epoch
	// propagation that an in-flight failover is driving. The
	// per-reconcile dispatcher retries on the next pass once the
	// suppression flag clears — except on the quorum-repair bypass
	// documented above.
	m.mu.Lock()
	predicate := m.deferralPredicate
	m.mu.Unlock()
	if predicate != nil && predicate(cr) && !bypassQuorumDeferral {
		return out
	}

	// recoverErr=isNoSuchMasterErr: a sentinel that ANSWERS but reports
	// "no such master" has no master entry at all — the signature of a
	// recovery pass interrupted between REMOVE and MONITOR (a wider
	// window now that the surgery gates run first). It is reachable AND
	// stranded, not unreachable: counting it as unreachable can drop the
	// reachable total below quorum and wedge the only repair path. REMOVE
	// on it is already a no-op (removeOne treats "no such master" as
	// success), so it flows through the normal REMOVE + MONITOR repair.
	liveSentinelIPs := liveSentinelIPSet(endpoints)
	peerView := SentinelsAll(ctx, endpoints, masterName, password)
	classified := classifyStrandedSentinels(peerView, endpoints, isNoSuchMasterErr, liveSentinelIPs)
	// Classification ran: the caller may stamp its base-cadence probe
	// clock. Set before any gate-defer so a sustained minority/PING/
	// failover defer still debounces the SentinelsAll probe.
	out.Probed = true

	// Partition the empty-peer class by the caller's per-address re-wipe
	// pacing set. EVERY empty-peer sentinel — wiped OR skipped — is seeded
	// into beingWiped BEFORE the re-point/ghost selectors run, so a paced
	// sentinel that monitors a dead master is never re-classified into the
	// re-point or ghost class and re-wiped behind the skip's back. Only the
	// REMOVE + MONITOR target slice (resetTargets) is skip-filtered; the
	// skipped subset is reported in out.SkippedStranded and carried forward
	// unchanged by the caller.
	resetTargets, wipedNames, skippedNames, beingWiped := partitionEmptyPeerClass(classified.Targets, target.SkipStrandedAddrs)
	out.Stranded = wipedNames
	out.SkippedStranded = skippedNames
	// EmptyPeerStranded is the WIPED empty-peer subset, captured BEFORE
	// the ghost-reap target is folded into out.Stranded below. The
	// caller's no-progress / linkup-stuck detector reads this (not
	// out.Stranded): a ghost-reap target is a gossiping survivor with an
	// intact peer-list, so "peer count still 0" never applies to it and
	// counting it would falsely declare a healthy cluster linkup-stuck.
	out.EmptyPeerStranded = append([]string(nil), wipedNames...)

	// Dead-master re-point class: gossiping sentinels whose monitored
	// master addr matches no live valkey pod. Their master entry is
	// intact — the empty-peer classifier skips them — but the entry
	// points at a corpse and their known-replica set died with it, so
	// neither their own election nor gossip can ever converge them.
	// They join the same REMOVE + MONITOR set. Disabled when the
	// caller supplied no live-pod set (startup path; degraded list).
	//
	// The stale-epoch sub-class (a sentinel monitoring a LIVE-but-different
	// pod, proven strictly behind the config-epoch total order) rides the
	// same surgery but is captured separately (staleEpochTargets) so a
	// fresh SENTINEL MASTER re-check can drop any that began an election
	// between classify and REMOVE — see the pre-surgery re-check below.
	classification := selectRepointTargets(ctx, endpoints, masterName, password, masterIP, target.LiveValkeyIPs, beingWiped, target.AllowStaleEpochRepoint)
	resetTargets, staleEpochTargets := foldRepointClassification(classification, resetTargets, beingWiped, &out)

	// Ghost-reap selection refreshes the per-CR debounce state every
	// pass and returns a single gossiping survivor to reap — only when
	// AllowGhostReap is set AND a holder clears the debounce. Folded
	// into the same REMOVE + MONITOR pass as any empty-peer stranded
	// set but capped to one survivor (selectGhostReapTarget) so the
	// live quorum's votes are never zeroed together.
	var ghostTarget *Endpoint
	if len(liveSentinelIPs) > 0 {
		ghostTarget = m.selectGhostReapTarget(cr, classified.GhostHolders, liveSentinelIPs, target.AllowGhostReap)
	}
	if ghostTarget != nil {
		if _, dup := beingWiped[ghostTarget.Name]; dup {
			ghostTarget = nil
		}
	}

	if len(resetTargets) == 0 && ghostTarget == nil {
		if len(out.SkippedStranded) > 0 {
			// Skip-only pass: every empty-peer sentinel this pass found
			// was deliberately paced and nothing else needs wiping. Return
			// the paced set with Healthy=false so the caller carries the
			// tracker forward + refreshes freshness without firing surgery.
			// Short-circuits BEFORE the minority/PING/failover fan-out so a
			// fully-paced episode costs only the one classification probe
			// per base window.
			return out
		}
		// Full classification ran and found nothing to wipe: the
		// ensemble's peer-lists are intact. Authoritative healthy
		// verdict (distinct from the gate-abort exits below, which
		// return the zero value) so the caller can clear any
		// no-progress / linkup-stuck state it was tracking — but ONLY
		// when a quorum was reachable. A reachable minority that
		// happens to see no stranded peers is NOT an authoritative
		// all-clear: the unreachable majority may be stranded, so this
		// is treated as a defer (Healthy=false), same as the minority
		// guard below.
		out.Healthy = classified.QuorumReachable
		return out
	}

	// Minority guard: let the partitioned majority self-recover via
	// gossip when fewer than a quorum is reachable; the next reconcile
	// retries once a quorum is back (see strandedClassification).
	if !classified.QuorumReachable {
		logger.Info("wedge recovery: only a minority of sentinels reachable; skipping REMOVE + MONITOR",
			"cr", cr.String(), "masterIP", masterIP,
			"reachable", classified.Reachable, "endpoints", len(endpoints),
			"quorum", QuorumThreshold(len(endpoints)), "stranded", out.Stranded)
		// Debounce the classification probe (Probed=true); the empty
		// Stranded/Repointed/SkippedStranded set routes the caller to its
		// defer branch, which leaves the no-progress tracker alone. This
		// defer is reached only when a real wipe/ghost target exists, so it
		// discards the partition's SkippedStranded on purpose — a minority
		// defer can't re-confirm the wedge, so the stuck flag ages out via
		// freshness.
		return StrandedRecoveryResult{Probed: true}
	}

	// Surgery gate 1: the registration target must be alive. A master
	// that dies between the caller's resolution and this REMOVE +
	// MONITOR (or was resolved from stale state) would leave every
	// re-registered sentinel with no replica list and no peer gossip —
	// the permanent no-electorate wedge. Defer; the next reconcile
	// re-resolves.
	masterAddr := net.JoinHostPort(masterIP, strconv.Itoa(port))
	if err := masterReachable(ctx, masterAddr, password); err != nil {
		logger.Info("wedge recovery: master did not answer PING; deferring REMOVE + MONITOR",
			"cr", cr.String(), "masterAddr", masterAddr,
			"stranded", out.Stranded, "err", err.Error())
		// Debounce the classification probe; empty result routes the
		// caller to its defer branch (see the minority guard above).
		return StrandedRecoveryResult{Probed: true}
	}

	// Surgery gate 2: never race a live election. Healthy survivors
	// (reachable, non-empty peer-list — the ones NOT being wiped) are
	// asked for failover_in_progress; any affirmative defers the pass.
	// s_down/o_down do not veto — a survivor pointing at the dead
	// pre-incident master is exactly the state being repaired. The
	// ghost-reap and re-point targets are gossiping survivors
	// (non-empty peer-list) but they ARE being wiped, so they are
	// excluded from the veto set — the failover-in-progress check must
	// read only untouched survivors. (A dead-master re-point target
	// mid-election cannot win it: the doomed-election discriminator
	// admitted it only after verifying its entire known-replica table is
	// dead, so no candidate exists for its election to promote. A
	// stale-epoch re-point target has a LIVE replica table, so it is NOT
	// protected by the doomed test — its safety is the config-epoch
	// total-order + the classify-time self/dest-election guards + the
	// pre-surgery fresh SENTINEL MASTER re-check below, which re-validates
	// BOTH the targets AND the destination cohort and drops any target
	// whose side began an election — or moved the frontier — since
	// classification.)
	healthy := untouchedSurvivors(peerView, endpoints, ghostTarget, beingWiped)
	if anyFailoverInProgress(ctx, healthy, masterName, password) {
		logger.Info("wedge recovery: a survivor reports failover_in_progress; deferring REMOVE + MONITOR",
			"cr", cr.String(), "masterAddr", masterAddr, "stranded", out.Stranded)
		// Debounce the classification probe; empty result routes the
		// caller to its defer branch (see the minority guard above).
		return StrandedRecoveryResult{Probed: true}
	}

	// Pre-surgery fresh re-check on the stale-epoch subset. Those targets
	// bypass the doomed filter AND sit in beingWiped (excluded from the
	// gate-2 survivor veto), so a fresh SENTINEL MASTER read immediately
	// before RemoveAll is the only time-independent guarantee neither the
	// target nor the destination has begun an election (or moved the
	// config-epoch frontier) since classify. Symmetric: it re-reads the
	// targets (drop any now electing / unconfirmable) AND the masterIP
	// cohort (disarm the whole surviving subset if the destination is now
	// electing, has fallen below the classify-time agreeEpoch, or cannot be
	// confirmed safe). Filters both resetTargets and out.StaleEpochRepointed.
	//
	// Knowingly-accepted residual: the target's own monitored pod being
	// promoted to primary within the sub-second re-check→RemoveAll window is
	// bounded and harmless — a re-point never relabels a primary pod, the
	// wiped sentinel re-gossips to the elected master on the next reconcile,
	// and there is no split-brain / no data loss. Not worth a target-epoch
	// re-read for a full-failover-in-sub-second-window edge.
	resetTargets = dropElectingStaleTargets(ctx, staleEpochTargets, resetTargets, classification.StaleEpochCohort, classification.AgreeEpoch, masterName, password, &out)
	if len(resetTargets) == 0 && ghostTarget == nil {
		// The re-check pruned every target (a stale-epoch-only pass whose
		// whole subset began electing / lost its destination proof): no
		// surgery fires, so return before the "firing" log and RemoveAll
		// — ResetResults stays nil per its "non-nil iff the pass actually
		// fired" contract, and the caller's gate-defer branch leaves the
		// no-progress tracker alone for the next pass.
		return out
	}

	// Fold the capped ghost-reap target into the REMOVE + MONITOR set.
	// REMOVE drops its (stale-ghost-bearing) master entry, MONITOR
	// re-seeds it at the confirmed-live master, and gossip rebuilds its
	// LIVE peer-list within ~2s minus the dead ghosts.
	if ghostTarget != nil {
		resetTargets = append(resetTargets, *ghostTarget)
		out.Stranded = append(out.Stranded, ghostTarget.Name)
	}

	// Whole-cluster stranded (every reachable sentinel has an empty
	// peer-list) IS recoverable here — REMOVE + MONITOR on each
	// gives every sentinel the same MasterIP, they all subscribe to
	// __sentinel__:hello on that master, and gossip rebuilds the
	// peer-list within ~2s. The rc.11 cascade only happened when
	// RESET was applied to healthy sentinels (non-empty peer-list);
	// the empty-peer-list selectivity (and the one-at-a-time cap on the
	// ghost-reap class) makes the surgery safe.
	//
	// REMOVE (not RESET) is the load-bearing primitive here — see
	// RemoveAll's doc for the "Duplicated master name" failure mode
	// plain RESET would leave the rebuilt sentinel wedged in.
	logger.Info("wedge recovery: firing REMOVE + MONITOR",
		"cr", cr.String(), "masterIP", masterIP,
		"targets", out.Stranded, "repointed", out.Repointed,
		"staleEpochRepointed", out.StaleEpochRepointed)
	removeResults := RemoveAll(ctx, resetTargets, masterName, password)
	// Surface REMOVE outcomes via the existing reset-event channel
	// (Normal on success, Warning per failure) — the caller's event
	// observer treats both as "operator touched the sentinel's
	// master state" and there's no behavioural difference worth a
	// new event reason.
	out.ResetResults = make([]ResetResult, len(removeResults))
	for i, r := range removeResults {
		out.ResetResults[i] = ResetResult(r)
	}
	m.emitResetEvents(cr, out.ResetResults)

	successByName := make(map[string]struct{}, len(removeResults))
	for _, r := range removeResults {
		if r.Err == nil {
			successByName[r.Name] = struct{}{}
		}
	}
	successfulEndpoints := make([]Endpoint, 0, len(resetTargets))
	for _, ep := range resetTargets {
		if _, ok := successByName[ep.Name]; ok {
			successfulEndpoints = append(successfulEndpoints, ep)
		}
	}
	if len(successfulEndpoints) > 0 {
		out.MonitorResults = MonitorAll(ctx, successfulEndpoints, masterName, masterIP, password, port, quorum)
		m.emitMonitorEvents(cr, out.MonitorResults)
		// Auth re-propagation on the REMOVE'd sentinels only;
		// survivors weren't touched and still carry the correct
		// auth-pass from prior reconciles. REMOVE wiped the master
		// entry's auth-pass; this restores it so the sentinel can
		// AUTH against the new master and subscribe to
		// __sentinel__:hello for peer-list gossip.
		if password != "" {
			out.AuthFailures = authFailureNames(m.IssueAuthPass(ctx, cr, masterName, password, successfulEndpoints))
		}
		// Restore the operator's configured per-master tuning that
		// MONITOR's default-population erased (down-after-millis,
		// failover-timeout, parallel-syncs). Without this the
		// rebuilt sentinel would honour Sentinel's hardcoded 30s
		// down-after default and lag the rest of the cluster on
		// future failovers until its next pod restart re-reads
		// sentinel.conf.
		m.emitTuningEvents(cr, SetMasterTuningAll(ctx, successfulEndpoints, masterName, tuning, password))
	}
	return out
}

// authFailureNames returns the pod names whose auth-pass re-propagation
// definitively FAILED VERIFICATION (SENTINEL MASTER did not echo the
// value we SET — errAuthPassMismatch). That is the one auth error class
// that proves the sentinel can never AUTH against its master, so the
// caller's stuck detector treats it as an immediate no-progress wedge.
// Transient transport errors (dial / reset / timeout) are deliberately
// excluded — a single dial blip must not seed a recoverable sentinel
// straight to the stuck threshold; those flow through the ordinary
// per-surgery no-progress counting, where the threshold's repetition
// filters them out.
func authFailureNames(results []AuthResult) []string {
	var names []string
	for _, ar := range results {
		if errors.Is(ar.Err, errAuthPassMismatch) {
			names = append(names, ar.Name)
		}
	}
	return names
}

// emitMonitorEvents fires one Kubernetes event per MonitorResult.
// Mirrors emitResetEvents / emitAuthEvents — Normal on success,
// Warning per failure. No "all-failed" escalation; per-pod
// Warnings are sufficient for alerting and the SentinelResetFail
// chain already surfaces the parent failure.
func (m *Manager) emitMonitorEvents(cr observerKey, results []MonitorResult) {
	if m.recorder == nil || len(results) == 0 {
		return
	}
	obj := &valkeyv1beta1.Valkey{}
	obj.Name = cr.Name
	obj.Namespace = cr.Namespace
	for _, r := range results {
		if r.Err == nil {
			m.recorder.Eventf(obj, nil, corev1.EventTypeNormal,
				string(events.SentinelMonitorIssued), "SentinelMonitorIssue",
				"SENTINEL MONITOR on %s succeeded", r.Name)
			continue
		}
		m.recorder.Eventf(obj, nil, corev1.EventTypeWarning,
			string(events.SentinelMonitorFailed), "SentinelMonitorFail",
			"SENTINEL MONITOR on %s failed: %s", r.Name, r.Err.Error())
	}
}

// emitTuningEvents fires a Warning per surviving sentinel where the
// post-MONITOR tuning restore failed. Successful restores are routine
// and stay silent to avoid event spam; only a failure is surfaced —
// it silently reverts the pod to Sentinel's default timing knobs
// (e.g. the 30s down-after) and lags the cluster on future failovers.
func (m *Manager) emitTuningEvents(cr observerKey, results []TuningResult) {
	if m.recorder == nil || len(results) == 0 {
		return
	}
	obj := &valkeyv1beta1.Valkey{}
	obj.Name = cr.Name
	obj.Namespace = cr.Namespace
	for _, r := range results {
		if r.Err == nil {
			continue
		}
		m.recorder.Eventf(obj, nil, corev1.EventTypeWarning,
			string(events.SentinelTuningFailed), "SentinelTuningFail",
			"SENTINEL tuning restore on %s failed: %s; pod reverted to default timing knobs",
			r.Name, r.Err.Error())
	}
}

// ResetResult is one per-pod outcome from an operator-issued sentinel
// surgery pass, surfaced via emitResetEvents. Name is the sentinel pod
// name (matches the Endpoint.Name supplied to the surgery call). Err is
// nil on success; non-nil errors are dial-time, AUTH, or command-reply
// failures. RecoverStrandedSentinels converts its per-pod REMOVE
// outcomes into this shape so the operator emits one SentinelReset*
// event per touched sentinel through a single channel.
type ResetResult struct {
	Name string
	Err  error
}

// emitResetEvents fires one Kubernetes event per ResetResult, plus
// the all-failed escalation when every stranded target errored. cr is
// the CR target; the synthetic Valkey object carries just the
// namespace/name (Recorder treats it as an ObjectReference). The
// operator's only sentinel-surgery primitive is the stranded-recovery
// REMOVE + MONITOR pass; the SentinelReset* reason set names that
// surgery for back-compatible alerting.
func (m *Manager) emitResetEvents(cr observerKey, results []ResetResult) {
	if m.recorder == nil || len(results) == 0 {
		return
	}
	obj := &valkeyv1beta1.Valkey{}
	obj.Name = cr.Name
	obj.Namespace = cr.Namespace
	failures := 0
	for _, r := range results {
		if r.Err == nil {
			m.recorder.Eventf(obj, nil, corev1.EventTypeNormal,
				string(events.SentinelResetIssued), "SentinelResetIssue",
				"sentinel surgery on %s succeeded", r.Name)
			continue
		}
		failures++
		m.recorder.Eventf(obj, nil, corev1.EventTypeWarning,
			string(events.SentinelResetFailed), "SentinelResetFail",
			"sentinel surgery on %s: %s", r.Name, r.Err.Error())
	}
	if failures == len(results) {
		m.recorder.Eventf(obj, nil, corev1.EventTypeWarning,
			string(events.SentinelResetAllFailed), "SentinelResetAllFail",
			"sentinel surgery failed on every stranded target (%d/%d) — next reconcile will retry",
			failures, len(results))
	}
}

// emitAuthEvents fires one Kubernetes event per AuthResult: a
// SentinelAuthApplied (Normal) on success, SentinelAuthNotApplied
// (Warning) per per-pod failure that survived the per-pod retry
// budget. No "all-failed" escalation companion event — at most
// one auth-pass-propagation per CR per RESET / startup pass; the
// per-pod Warnings are sufficient for alerting.
func (m *Manager) emitAuthEvents(cr observerKey, results []AuthResult) {
	if m.recorder == nil || len(results) == 0 {
		return
	}
	obj := &valkeyv1beta1.Valkey{}
	obj.Name = cr.Name
	obj.Namespace = cr.Namespace
	for _, r := range results {
		if r.Err == nil {
			m.recorder.Eventf(obj, nil, corev1.EventTypeNormal,
				string(events.SentinelAuthApplied), "SentinelAuthApply",
				"SENTINEL SET auth-pass on %s succeeded (verified)", r.Name)
			continue
		}
		m.recorder.Eventf(obj, nil, corev1.EventTypeWarning,
			string(events.SentinelAuthNotApplied), "SentinelAuthNotApply",
			"SENTINEL SET auth-pass on %s failed after %d attempts: %s",
			r.Name, m.opts.AuthRetryAttempts, r.Err.Error())
	}
}

// observerConfigEqual returns true when the existing observer was
// configured with the exact (masterName, password, endpoints) the
// caller is about to Ensure with. Used by Ensure to elide the
// stop+start cycle on the no-op path.
func observerConfigEqual(o *observer, masterName, password string, endpoints []Endpoint) bool {
	if o.masterName != masterName || o.password != password {
		return false
	}
	if len(o.endpoints) != len(endpoints) {
		return false
	}
	// endpoints is already sorted by the caller; o.endpoints was
	// sorted on Ensure too (the only path that writes it).
	for i, ep := range endpoints {
		if o.endpoints[i] != ep {
			return false
		}
	}
	return true
}
