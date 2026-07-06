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

// Package sentinel hosts the per-CR Sentinel observer goroutine and
// the singleton SentinelObserverManager that owns its lifecycle.
//
// The observer maintains a hybrid push (PSUBSCRIBE on +switch-master,
// +failover-end, +failover-end-for-timeout, +odown, -odown, +tilt,
// -tilt) + pull (10s GET-MASTER-ADDR-BY-NAME / CKQUORUM tick) view of
// Sentinel state, surfaced to consumers as an atomic.Value snapshot.
// Pub/sub alone is never trusted — the pull tick is the backstop.
//
// Wire shape uses the same hand-rolled RESP-2 path the LagChecker
// uses (see internal/valkey/lagchecker.go); no third-party redis
// client is pulled in. PSUBSCRIBE rides a dedicated subscription
// connection — never shared with the pull-tick command socket — to
// avoid the Sentinel link-reconnect cycle thrashing the poll path.
package sentinel

import (
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Endpoint identifies one sentinel pod the observer can talk to.
// Name is the pod's metadata.name (used as a Prometheus label and
// in events so an admin reading the timeline can map back to a
// specific pod); Addr is the dial target as host:port (built by
// the caller from pod.Status.PodIP + the sentinel client port).
//
// Caller computes both fields from a Pod list — the observer
// doesn't read kube state directly so unit tests can wire to a
// net.Listen fake without spinning up an envtest apiserver.
type Endpoint struct {
	Name string
	Addr string
}

// Source labels how an ObservedPrimary value reached the snapshot.
// "pubsub" — the most recent +switch-master message updated it.
// "poll"  — the 10s tick observed the primary via
//
//	GET-MASTER-ADDR-BY-NAME.
//
// "merge" — both channels agreed on the same value within the
//
//	last poll cycle (used for newly-stable primaries after
//	reconnect, where the poll tick confirms the pubsub view).
//
// "none"  — no channel produced a value (boot-time, all channels
//
//	dead). Snapshot consumers must treat this as "unknown"
//	and refuse to relabel.
type Source string

const (
	SourceNone   Source = "none"
	SourcePubsub Source = "pubsub"
	SourcePoll   Source = "poll"
	SourceMerge  Source = "merge"
)

// QuorumStatus is the tri-state quorum signal the observer publishes.
// The pull tick computes this on every cycle; the pubsub paths
// preserve the prior cycle's value (a single pubsub frame is one
// observation, not a re-vote across peers).
//
// Consumers split into two classes:
//
//   - "Irreversible action" consumers (Phase 7 relabel, split-brain
//     guard) MUST treat QuorumStatusUnknown identically to
//     QuorumStatusLost — refuse to act when the observer can't see
//     enough peers to decide. The derived `QuorumOK bool` field
//     captures this semantic for legacy call sites.
//   - "Hysteresis-gated" consumers (the suppression-gate accumulator)
//     MUST treat QuorumStatusUnknown as a no-op so transient
//     observer-unreachable windows during the operator's own
//     recovery rollouts do not accumulate spurious loss-time.
type QuorumStatus int8

const (
	// QuorumStatusUnknown means the observer reached fewer than
	// QuorumThreshold(N) sentinel peers on the last pull tick, so
	// it cannot decide whether a quorum is reachable. Surfaced
	// during transient pod-recreation windows; the suppression
	// gate preserves its prior state across such windows.
	QuorumStatusUnknown QuorumStatus = 0
	// QuorumStatusOK means ≥QuorumThreshold(N) reachable sentinels
	// reported CKQUORUM=OK on a strict-majority primary address.
	QuorumStatusOK QuorumStatus = 1
	// QuorumStatusLost means ≥QuorumThreshold(N) sentinels were
	// reachable but the CKQUORUM / primary-agreement majority was
	// not met — a real quorum failure, not an observer-side
	// connectivity blip.
	QuorumStatusLost QuorumStatus = 2
)

// String renders the enum for logs / metric labels / events.
func (q QuorumStatus) String() string {
	switch q {
	case QuorumStatusOK:
		return "OK"
	case QuorumStatusLost:
		return "Lost"
	default:
		return "Unknown"
	}
}

// ObservedPrimary is the per-CR atomic snapshot the observer
// publishes to consumers. The pointer-to-value pattern is required
// by atomic.Value (the stored type must be consistent across stores
// and pointers preserve that without a wrapping struct).
//
// QuorumOK gates relabel: ≥ quorum sentinels must agree on the
// primary address before the operator strips role=primary off the
// outgoing pod and re-stamps it on the incoming. The reconciler
// reads this field; the failover triple-check reads ODown.
type ObservedPrimary struct {
	// Addr is the host:port the surviving sentinels report as the
	// current primary. Empty when QuorumOK is false (consumers
	// must check QuorumOK first; an empty Addr with QuorumOK=true
	// is a bug).
	Addr string
	// Epoch is the sentinel `config-epoch` pulled from the most
	// recent SENTINEL MASTER <name> reply on the poll-tick path —
	// pubsub events do not carry epoch (sentinel's +switch-master
	// payload is name+oldAddr+newAddr only), so the pubsub-side
	// publish carries forward the prior poll's epoch unchanged.
	// The published value is the max observed epoch across
	// reachable sentinels in the most recent poll, never going
	// backwards (defensive against a single sentinel reporting
	// stale epoch during a fresh failover). Consumers compare
	// against the epoch they last acted on to detect missed
	// failovers — particularly the failover-twice-back-to-the-same-
	// address scenario where Addr alone wouldn't change.
	Epoch int64
	// Quorum is the tri-state quorum signal from the most recent
	// pull cycle (or carried forward from a pubsub-only update).
	// See QuorumStatus for the consumer-class split. Producers of
	// new ObservedPrimary values write Quorum; QuorumOK is derived
	// at publish time.
	Quorum QuorumStatus
	// QuorumOK is true iff Quorum == QuorumStatusOK. Retained for
	// legacy consumers (FSM guard contexts, derive_state facts)
	// that drive irreversible actions and conservatively treat
	// Unknown the same as Lost. The reconciler's suppression-gate
	// path reads Quorum directly so Unknown ≠ Lost there.
	QuorumOK bool
	// UpdatedAt is the wall-clock time of the most recent update
	// to this snapshot from any source. Stale snapshots (older
	// than ~3× the pull tick) are treated as "all sentinels
	// unreachable" by consumers.
	UpdatedAt time.Time
	// LastPolledAt is the wall-clock time of the most recent pull
	// tick that wrote this snapshot. Unlike UpdatedAt — which the
	// pub/sub path also refreshes while carrying a prior quorum
	// forward — LastPolledAt advances ONLY on a live poll, so a
	// consumer can distinguish "a pull confirmed this recently" from
	// "pub/sub replayed a stale quorum forward". Zero until the first
	// poll lands; pub/sub re-publishes carry the prior poll's value
	// forward unchanged.
	LastPolledAt time.Time
	// Source is the channel that wrote this snapshot. See the
	// Source constants for semantics.
	Source Source
	// ODown is a per-sentinel-pod last-seen-+odown map, surfaced
	// for the failover triple-check (≥ quorum sentinels must
	// agree on +odown for the same target before failover
	// proceeds). Keys are sentinel pod names (matching the
	// Endpoint.Name labels); values are the wall-clock time the
	// most recent +odown was received from that pod for the CR's
	// primary master. A pod missing from the map means we have
	// not received +odown from it (or have received a -odown that
	// cleared it).
	//
	// Read by consumers as a snapshot — the observer rebuilds the
	// map on each ObservedPrimary write rather than mutating the
	// stored value, so consumers never need to lock.
	ODown map[string]time.Time
	// ODownPull is the pull-side sibling of ODown: a per-sentinel-pod
	// map of the wall-clock time the 10s pull tick FIRST observed
	// o_down in that pod's SENTINEL MASTER `flags` field, keyed by
	// sentinel pod name. Unlike ODown (edge-triggered by +odown /
	// -odown pubsub frames, refreshed on every frame), this map is
	// level-reconciled by the pull tick with a rising-edge first-seen
	// stamp that is never refreshed while o_down persists, and an
	// entry is deleted only when a REACHED sentinel reports o_down
	// clear — an unreachable pod or an unparseable reply leaves the
	// entry untouched.
	//
	// Consumed by the ghost-reap veto as a second, level-triggered
	// truth source so a lost -odown frame no longer latches the veto
	// forever; the rising-edge stamp ages the entry out one
	// failover-timeout after o_down first appeared (the escape valve).
	// NOT consumed by the failover triple-check / ConsensusODown,
	// which need last-seen recency semantics ODown alone provides.
	//
	// Rebuilt (copied) on each publish, like ODown, so lock-free
	// readers never lock.
	ODownPull map[string]time.Time
}

// Snapshot is the lock-free read API the observer publishes to
// consumers. It wraps the *ObservedPrimary atomic.Value plus a
// "no observation yet" sentinel so the consumer can distinguish
// "we have a snapshot saying QuorumOK=false" (real degraded view)
// from "the observer goroutine hasn't produced anything yet"
// (boot race).
type Snapshot struct {
	// Present is false when the observer has not yet written its
	// first ObservedPrimary (e.g., boot race between the
	// reconciler's first pass and the observer's first poll
	// tick). Consumers must treat !Present as "unknown" — the
	// same as ObservedPrimary{Source: SourceNone}.
	Present bool
	// Primary is the most recent ObservedPrimary the observer
	// wrote. Zero value when Present is false.
	Primary ObservedPrimary
}

// observerKey aliases types.NamespacedName so call sites read
// `key := observerKey{...}` rather than the kube type spelled
// out in full — the manager's map is keyed on it and the alias
// keeps signatures readable.
type observerKey = types.NamespacedName

// snapshotHolder is the per-observer atomic.Value carrier. The
// indirection (pointer to ObservedPrimary inside an atomic.Value)
// is required by the atomic.Value contract — the stored type must
// be consistent across stores. Wrapping in a one-field struct
// would also work but adds nothing; a *ObservedPrimary is the
// natural shape.
type snapshotHolder struct {
	v atomic.Value // holds *ObservedPrimary
}

func (h *snapshotHolder) load() (ObservedPrimary, bool) {
	raw := h.v.Load()
	if raw == nil {
		return ObservedPrimary{}, false
	}
	p, ok := raw.(*ObservedPrimary)
	if !ok || p == nil {
		return ObservedPrimary{}, false
	}
	return *p, true
}

func (h *snapshotHolder) store(p ObservedPrimary) {
	h.v.Store(&p)
}
