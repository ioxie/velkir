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

package controller

import (
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// perCRState is the single per-CR in-memory state bag. One entry per
// Valkey CR lives in ValkeyReconciler.perCR, keyed by
// types.NamespacedName, replacing the former dozen-plus parallel
// sync.Map trackers. A new per-CR tracker is a field here and is torn
// down by forgetCR's single Delete, so the "add a map, forget to clear
// it" two-sites-out-of-sync drift hazard is now a
// property of the type system rather than a reflection count-guard test.
//
// Locking model:
//   - reconcile serialises concurrent Reconciles for one CR (held for
//     the whole pass via lockFor). Reconciles for one CR being
//     serialised is why the sub-trackers can keep lightweight inner
//     mutexes (or none) on their hot paths.
//   - mu guards lazy creation of the pointer sub-trackers and every
//     read/write of the value-typed fields. It is never held while an
//     inner sub-tracker mutex is held, so the two-level locking cannot
//     deadlock.
type perCRState struct {
	// reconcile serialises concurrent Reconciles for one CR. Held for
	// the whole pass via lockFor. The stale-tracker pruner never resets
	// it: dropping a held mutex would let a concurrent re-reconcile
	// LoadOrStore a fresh one and race past serialisation.
	reconcile sync.Mutex

	// mu guards lazy creation of the pointer sub-trackers and all
	// reads/writes of the value-typed fields below.
	mu sync.Mutex

	// --- prunable: cleared by StaleTrackerPruner for vanished CRs ---
	// All edge detectors that re-seed or digests that recompute, so a
	// wrongful clear on a transient list-staleness is harmless.

	// quorum tracks the per-CR last-observed Quorum plus the sustained-
	// quorum-loss suppression gate and the stranded-recovery
	// no-progress / backoff bookkeeping.
	quorum *crQuorumState

	// rolloutTrigger tracks the per-CR "was a rollout pending last
	// reconcile" bit, used by rolloutTriggerEdge to fire
	// EventRolloutTrigger exactly once per spec-change edge rather than
	// on every reconcile while the rollout is in flight.
	rolloutTrigger *rolloutTriggerState

	// replicasRolled tracks the per-CR "was the FSM state last reconcile
	// already StateRolloutPrimary" bit, used by replicasRolledEdge to
	// fire EventAllReplicasRolled exactly once per "replicas finish,
	// primary stale" transition.
	replicasRolled *replicasRolledTracker

	// fsmTransition carries the per-CR last-observed FSM state used by
	// fsmTransitionEdge to fire abort and recovery transitions exactly
	// once per inter-state transition rather than on every reconcile
	// observation of the destination state.
	fsmTransition *fsmTransitionTracker

	// staleReplicas records when THIS operator instance first observed
	// each replica's replication-ready gate as False (pod UID →
	// first-observed time). Phase 8's stuck-replica recovery measures
	// staleness against this in-memory timestamp instead of the
	// apiserver PodCondition.LastTransitionTime, whose clock keeps
	// running across an operator restart. Resets on restart so the
	// post-restart 90s window starts fresh.
	staleReplicas *sync.Map // pod UID → time.Time

	// sqStatusDigest is the per-CR sha256 of the most recently
	// SSA-applied SentinelQuorum status set; the SQ writer skips the
	// per-pod SSA patches when the hash is unchanged. "" = unset.
	sqStatusDigest string

	// sqLastObservedAt is the observation poll-time of the most recent
	// SentinelQuorum status write. The writer's skip-when-unchanged
	// guard also forces a keep-alive re-stamp once this nears the
	// aggregator's freshness window, so a stable-content record's
	// LastObservedTime cannot freeze and age out (which would latch
	// PrimaryConfirmed to Unknown on a quiet-but-live cluster). Zero
	// value = no write yet.
	sqLastObservedAt time.Time

	// missingAuthSeen records when this operator instance first saw the
	// CR's referenced auth Secret missing; Phase 0d's requeue backoff
	// reads it (30s for 5min, 1m for 25, then 5m). Zero value = unset;
	// cleared on the reconcile that resolves the Secret.
	missingAuthSeen time.Time

	// noPrimarySince records when this operator instance first observed
	// the sustained "no primary-labeled pod" suppression state
	// (primaryLabeledCount==0 && len(pods)>0). Phase 8's bounded escape
	// measures the dwell against it and only fires past
	// staleReplicaEscapeDwell. Zero value = not currently
	// suppressed; cleared the instant a primary label reappears so the
	// dwell can never latch across a recovery.
	noPrimarySince time.Time

	// staleReplicaEscapeLastFired records the wall-clock of the most
	// recent Phase-8 bounded-escape delete; a per-CR cooldown bounds the
	// escape to at most one delete per staleReplicaEscapeCooldown. Zero
	// value = never fired.
	staleReplicaEscapeLastFired time.Time

	// --- NOT pruned: lifecycle-sensitive or self-clearing ---
	// Dropping these on a stale list-miss could re-fire an audit event,
	// re-open the failover strip window, or lose the rotation
	// OLD-password — so they are excluded from the pruner sweep (the
	// reconcile mutex is excluded for the same family of reasons).

	// manualRollout tracks the per-CR last-observed value of the
	// manual-rollout annotation, used by maybeAuditManualRollout to emit
	// EventManualRolloutTriggered exactly once per value change.
	manualRollout *manualRolloutState

	// failoverLatch records the per-CR "we just dispatched SENTINEL
	// FAILOVER" window — set right before the role=primary strip,
	// consulted by desiredRolesForCR + deriveState to suppress
	// re-stamping the label until the observer reports the new primary's
	// Addr (or the latch deadline expires). nil = no latch.
	failoverLatch *failoverInFlightLatch

	// switchMasterEdge is the observer-elected primary Addr for which a
	// T5 UnexpectedFailover event was already emitted, so
	// fsmSwitchMasterDispatch fires once per external-failover episode.
	// "" = re-armed/unset (the fire path always supplies a non-empty
	// Addr, so "" can never collide with a real edge value).
	switchMasterEdge string

	// authPassword holds the per-CR most-recently-observed auth password
	// value, used by maybeRotateAuth as the OLD password when a Secret
	// content change is detected. nil = cold cache.
	authPassword *authPasswordCacheEntry

	// primaryStability is the two-snapshot settling damp: it counts
	// consecutive fresh (LastPolledAt-advancing) observer polls naming the
	// same primary Addr. desiredRolesForCR consults it before MOVING the
	// role=primary label to a newly-observed primary, suppressing the
	// post-Ready label flap where the observer briefly oscillates between
	// addresses. nil = no observation recorded.
	primaryStability *primaryStabilityState

	// --- dual-master trackers: edge detectors, pruned by pruneStale ---
	// All of these are safe to drop on a stale list-miss (the next scan
	// re-stamps a real split), and dropping them for a reclaimed/
	// recreated same-name CR keeps its first split from inheriting the
	// dead CR's latched edges or exhausted attempt budget — STS pod
	// names are deterministic, so stale signatures would match.

	// dualMasterSelfHeal bounds Phase 7a's dual-master self-heal:
	// cooldown + exponential backoff + max-attempts so a split that the
	// self-heal can't unwedge doesn't thrash REPLICAOF/CLIENT KILL every
	// reconcile. Reset to nil whenever the cluster leaves the self-heal
	// trigger condition (a labeled primary reappears), so a fresh split
	// starts with a full attempt budget. nil = no attempts recorded.
	dualMasterSelfHeal *dualMasterSelfHealState

	// dualMasterDeferEdge is the last DualMasterSelfHealDeferred signature
	// emitted for the CR, so a sustained identical deferral (e.g. a steady
	// within-epsilon tie) emits one Warning per episode instead of one per
	// reconcile. "" = re-armed; re-armed on a signature change, on a
	// self-heal commit, or when a labeled primary reappears.
	dualMasterDeferEdge string

	// dualMasterDeferLastAt is the wall-clock of the last
	// DualMasterSelfHealDeferred emission. A signature CHANGE re-fires
	// only after dualMasterDeferMinInterval has elapsed since the last
	// emission, so two deferral reasons alternating pass-to-pass (e.g. a
	// sick pod flipping between the no-offset and epsilon-tie defers at
	// the 5s wedge cadence) page at most once per interval instead of
	// once per flip. Zero = no emission recorded.
	dualMasterDeferLastAt time.Time

	// dualMasterObserved is the most recent dual-master scan verdict:
	// non-nil while the last completed scan saw >=2 live pods
	// self-reporting role:master. Four producers stamp it: the two
	// condition-only scans (the labeled-primary orphan scan and the
	// Phase 7a self-heal scan) and the two event-firing scans (the
	// Phase 11 recovery survey and the replication no-labeled-primary
	// observation). updateStatus reads it freshness-gated (the scans run
	// only on passes that reach their phase) to drive Ready=False +
	// Degraded=DualMasterDivergence. The DualMasterObserved event now
	// edge-gates on the accumulated union of de-facto-master pod names
	// (see foldDualMasterEventUnion), fed only by the two firing scans, so
	// role churn within one split re-fires once per genuinely-new pod
	// rather than once per membership permutation, while offset churn
	// never re-fires it. Cleared by the first scan seeing <=1
	// self-reported master and on labeled-primary reappearance.
	dualMasterObserved *dualMasterObservation

	// dualMasterObservedEdge is the last DualMasterObserved event
	// signature (the accumulated de-facto-master pod-name union) emitted
	// for the CR. "" = re-armed; re-armed when the observation clears so
	// a NEW episode fires again.
	dualMasterObservedEdge string

	// dualMasterEventUnion is the accumulated, sorted, deduped set of
	// de-facto-master pod names seen across the CURRENT DualMasterObserved
	// event episode — the pods that have, at any scan, self-reported
	// role:master. It keys the DualMasterObserved event edge so role churn
	// within one split ({a,b} then {a,c} then {b,c}) pages once at episode
	// start and re-pages only when a genuinely-new pod joins, instead of once
	// per membership permutation. Accumulation is time-independent: a
	// persistent split whose scans land slower than the freshness window
	// (the replication no-primary producer at the steady replica-recheck
	// cadence) stays one episode and pages once, rather than re-paging every
	// slow scan. Fed ONLY by the two event-firing producers (the recovery
	// survey and the replication no-labeled-primary scan), which are
	// mode-exclusive per CR; the condition-only producers (labeled-primary
	// orphan scan, self-heal scan) never feed it, so the healthy labeled
	// primary they count toward the >=2 condition can never contaminate the
	// event key. nil = no episode. Reset with the observation only on a
	// <=1-master scan (clear), age-out (expire), or stale list-miss (prune).
	dualMasterEventUnion []string
}

// dualMasterObservation records one scan's dual-master verdict: the
// sorted de-facto-master pod names and the wall-clock of the scan.
type dualMasterObservation struct {
	observedAt time.Time
	pods       []string
}

// dualMasterSelfHealState records the per-CR self-heal attempt budget for
// the cooldown + exponential-backoff + max-attempts bound.
type dualMasterSelfHealState struct {
	lastAttempt  time.Time
	attemptCount int
}

// primaryStabilityState records the consecutive-fresh-poll count for one
// observed primary Addr. freshCount advances only when a poll's
// LastPolledAt is strictly newer than the last recorded one, so a pub/sub
// replay carrying a stale Addr forward does not inflate it.
type primaryStabilityState struct {
	addr        string
	firstPolled time.Time
	lastPolled  time.Time
	freshCount  int
}

// stateFor returns the per-CR state bag for key, creating it on first
// access. Every Reconcile materialises the entry via lockFor, so callers
// inside the reconcile path can rely on it being present.
func (r *ValkeyReconciler) stateFor(key types.NamespacedName) *perCRState {
	v, _ := r.perCR.LoadOrStore(key, &perCRState{})
	return v.(*perCRState)
}

// stateForIfPresent returns the per-CR state bag without creating it.
// Used by the cross-package read-only deferral predicates so a probe for
// an unobserved CR does not materialise state.
func (r *ValkeyReconciler) stateForIfPresent(key types.NamespacedName) (*perCRState, bool) {
	v, ok := r.perCR.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*perCRState), true
}

// lockFor returns the per-CR reconcile mutex, lazily created with the
// state bag. The returned pointer is stable for the lifetime of the
// entry (sync.Map never relocates values); forgetCR dropping the entry
// mid-reconcile is the documented teardown hazard callers guard against.
func (r *ValkeyReconciler) lockFor(key types.NamespacedName) *sync.Mutex {
	return &r.stateFor(key).reconcile
}

// --- lazy-init accessors for the pointer sub-trackers ---

func (s *perCRState) quorumTracker() *crQuorumState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.quorum == nil {
		s.quorum = &crQuorumState{}
	}
	return s.quorum
}

// quorumIfPresent returns the quorum tracker without creating it.
func (s *perCRState) quorumIfPresent() *crQuorumState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.quorum
}

func (s *perCRState) rolloutTriggerTracker() *rolloutTriggerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rolloutTrigger == nil {
		s.rolloutTrigger = &rolloutTriggerState{}
	}
	return s.rolloutTrigger
}

func (s *perCRState) replicasRolledTracker() *replicasRolledTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replicasRolled == nil {
		s.replicasRolled = &replicasRolledTracker{}
	}
	return s.replicasRolled
}

func (s *perCRState) fsmTransitionTracker() *fsmTransitionTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fsmTransition == nil {
		s.fsmTransition = &fsmTransitionTracker{}
	}
	return s.fsmTransition
}

// fsmTransitionIfPresent returns the FSM-transition tracker without
// creating it.
func (s *perCRState) fsmTransitionIfPresent() *fsmTransitionTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fsmTransition
}

func (s *perCRState) manualRolloutTracker(seed string) (*manualRolloutState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manualRollout == nil {
		s.manualRollout = &manualRolloutState{last: seed}
		return s.manualRollout, false // first observation — baseline seeded
	}
	return s.manualRollout, true
}

func (s *perCRState) staleReplicaTracker() *sync.Map {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staleReplicas == nil {
		s.staleReplicas = &sync.Map{}
	}
	return s.staleReplicas
}

// --- value-typed / no-inner-mutex trackers (guarded by s.mu) ---

func (s *perCRState) setFailoverLatch(l *failoverInFlightLatch) {
	s.mu.Lock()
	s.failoverLatch = l
	s.mu.Unlock()
}

func (s *perCRState) loadFailoverLatch() *failoverInFlightLatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failoverLatch
}

func (s *perCRState) clearFailoverLatch() {
	s.mu.Lock()
	s.failoverLatch = nil
	s.mu.Unlock()
}

func (s *perCRState) storeAuthPassword(e *authPasswordCacheEntry) {
	s.mu.Lock()
	s.authPassword = e
	s.mu.Unlock()
}

func (s *perCRState) loadAuthPassword() (*authPasswordCacheEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authPassword, s.authPassword != nil
}

func (s *perCRState) clearAuthPassword() {
	s.mu.Lock()
	s.authPassword = nil
	s.mu.Unlock()
}

// selfHealAttemptAllowed reports whether a dual-master self-heal attempt
// may run now under the cooldown + exponential-backoff + max-attempts
// bound. The first attempt is always allowed; subsequent attempts wait
// baseCooldown * 2^(attemptCount-1) (capped at maxBackoff) since the last
// attempt, and once attemptCount reaches maxAttempts no further attempt
// runs until resetSelfHeal clears the budget (a labeled primary
// reappearing). now is injected so the gate is table-testable.
func (s *perCRState) selfHealAttemptAllowed(now time.Time, baseCooldown, maxBackoff time.Duration, maxAttempts int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.dualMasterSelfHeal
	if st == nil || st.attemptCount == 0 {
		return true
	}
	if st.attemptCount >= maxAttempts {
		return false
	}
	backoff := baseCooldown << (st.attemptCount - 1)
	if backoff <= 0 || backoff > maxBackoff {
		backoff = maxBackoff
	}
	return !now.Before(st.lastAttempt.Add(backoff))
}

// recordSelfHealAttempt stamps an attempt at now, lazily creating the
// budget tracker. Pairs with selfHealAttemptAllowed.
func (s *perCRState) recordSelfHealAttempt(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dualMasterSelfHeal == nil {
		s.dualMasterSelfHeal = &dualMasterSelfHealState{}
	}
	s.dualMasterSelfHeal.attemptCount++
	s.dualMasterSelfHeal.lastAttempt = now
}

// resetSelfHeal clears the self-heal attempt budget so the next split
// starts fresh. Called whenever the cluster is no longer in the
// dual-master trigger condition (a pod carries the role=primary label).
func (s *perCRState) resetSelfHeal() {
	s.mu.Lock()
	s.dualMasterSelfHeal = nil
	s.mu.Unlock()
}

// fireDualMasterDeferEdge reports whether a DualMasterSelfHealDeferred
// event should fire for sig at now, recording (sig, now) when it does.
// Two suppressions compose:
//   - same signature → already emitted this episode, never re-fires;
//   - changed signature within dualMasterDeferMinInterval of the last
//     emission → rate-bounded. The recorded signature is deliberately
//     NOT updated here, so once the interval elapses the still-changed
//     reason fires — an alternating pair (A,B,A,B… every reconcile)
//     emits at most once per interval instead of once per flip.
func (s *perCRState) fireDualMasterDeferEdge(sig string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dualMasterDeferEdge == sig {
		return false
	}
	if !s.dualMasterDeferLastAt.IsZero() && now.Sub(s.dualMasterDeferLastAt) < dualMasterDeferMinInterval {
		return false
	}
	s.dualMasterDeferEdge = sig
	s.dualMasterDeferLastAt = now
	return true
}

// resetDualMasterDeferEdge re-arms the deferral edge so the next deferral
// emits regardless of its signature. The rate-bound stamp is kept: a
// commit or primary reappearance re-arms WHAT may fire, while the
// minimum emission interval still bounds HOW OFTEN.
func (s *perCRState) resetDualMasterDeferEdge() {
	s.mu.Lock()
	s.dualMasterDeferEdge = ""
	s.mu.Unlock()
}

// stampDualMasterObserved records a scan that saw >=2 live pods
// self-reporting role:master. pods are the de-facto-master pod names;
// they are sorted in place. Event emission is separate (see
// fireDualMasterObservedEdge) so producers that already carry their
// own messaging (the self-heal section, the orphan demotion loop) can
// stamp the condition without consuming the event edge. The event-edge
// union is accumulated separately (foldDualMasterEventUnion), never here,
// so the condition-only producers cannot feed the event key.
func (s *perCRState) stampDualMasterObserved(pods []string, now time.Time) {
	sort.Strings(pods)
	s.mu.Lock()
	s.dualMasterObserved = &dualMasterObservation{observedAt: now, pods: pods}
	s.mu.Unlock()
}

// fireDualMasterObservedEdge reports whether a DualMasterObserved event
// should fire for the given episode signature. The signature is the
// accumulated de-facto-master pod-name union for the episode (from
// foldDualMasterEventUnion), not the current scan's exact pod-set, so role
// churn within one split ({a,b} then {a,c} then {b,c}) pages once at
// episode start and only re-pages when a genuinely-new pod joins. A
// persistent split re-observed every scan pages once per episode;
// offsets are deliberately NOT part of the signature — they advance
// every pass even on an idle primary and would re-fire the event forever.
func (s *perCRState) fireDualMasterObservedEdge(sig string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dualMasterObservedEdge == sig {
		return false
	}
	s.dualMasterObservedEdge = sig
	return true
}

// foldDualMasterEventUnion accumulates the current scan's de-facto-master
// pod set into the current event episode's union and returns the sorted,
// comma-joined union to key the DualMasterObserved event edge. It is called
// ONLY by the two event-firing producers (the recovery survey and the
// replication no-labeled-primary scan), which are mode-exclusive per CR, so
// the union never mixes producers for a single CR and the condition-only
// scans can never inject their healthy labeled primary into the event key.
//
// The union grows monotonically across the episode, so a role permutation
// across scans keys to the same union and re-fires only when a genuinely-new
// pod joins. The episode is deliberately NOT bounded by a scan-gap timer:
// accumulation is time-independent, so a persistent split whose scans land
// slower than the freshness window (e.g. the replication no-primary producer
// at the steady replica-recheck cadence) stays one episode and pages once,
// while offset churn — absent from the key — never re-fires. The episode
// resets (union nil'd + edge re-armed) only when a scan sees <=1 master
// (clearDualMasterObserved), the observation ages out with no re-stamp
// (dualMasterActiveOrExpire), or a stale list-miss prunes it (pruneStale) —
// never on the accumulation path here.
func (s *perCRState) foldDualMasterEventUnion(pods []string) string {
	sorted := append([]string(nil), pods...)
	sort.Strings(sorted)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dualMasterEventUnion = mergeSortedUnique(s.dualMasterEventUnion, sorted)
	return strings.Join(s.dualMasterEventUnion, ",")
}

// mergeSortedUnique merges two pre-sorted string slices into a new
// sorted, deduplicated slice. Both inputs must already be sorted and
// neither is mutated; a value present in both inputs (or repeated within
// one) appears once via the suppress-equal-to-previous output guard.
func mergeSortedUnique(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	appendUnique := func(v string) {
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			appendUnique(a[i])
			i++
		case a[i] > b[j]:
			appendUnique(b[j])
			j++
		default:
			appendUnique(a[i])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		appendUnique(a[i])
	}
	for ; j < len(b); j++ {
		appendUnique(b[j])
	}
	return out
}

// clearDualMasterObserved records a completed scan that saw at most one
// self-reported master, clearing the observation and re-arming the
// event edge so a future split fires a fresh DualMasterObserved. It also
// drops the event-edge union so the next split starts a fresh episode.
func (s *perCRState) clearDualMasterObserved() {
	s.mu.Lock()
	s.resetDualMasterState()
	s.mu.Unlock()
}

// resetDualMasterState drops the dual-master observation, the event-edge
// latch, and the accumulated event union together, so the three lifecycle
// paths that end an episode (a <=1-master scan clear, an age-out with no
// re-stamp, a stale list-miss prune) share one reset set — a future
// dual-master field cannot be added to one path and forgotten in the
// others. Caller holds s.mu.
func (s *perCRState) resetDualMasterState() {
	s.dualMasterObserved = nil
	s.dualMasterObservedEdge = ""
	s.dualMasterEventUnion = nil
}

// dualMasterObservation returns the current dual-master stamp (nil when
// no split is recorded) — the freshness gating is the reader's job.
func (s *perCRState) dualMasterObservation() *dualMasterObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dualMasterObserved
}

// dualMasterActiveOrExpire reports whether a fresh dual-master
// observation is currently active (>=2 masters observed within the
// freshness window). When the stamp exists but has aged out — a
// producer stamped a split, then stopped running (mode flip, observer
// gone, CR paused, the split resolved off-cluster) — it is dropped and
// the event edge is re-armed here so a fresh same-pod-set episode fires
// DualMasterObserved again. The age-out drop also clears the event-edge
// union so the next episode accumulates from empty. isActive is the pure
// freshness predicate the caller injects (dualMasterActiveFromStamp) so
// this method stays clock-agnostic and lock-owning.
func (s *perCRState) dualMasterActiveOrExpire(isActive func(*dualMasterObservation) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if isActive(s.dualMasterObserved) {
		return true
	}
	// Not active. If a stale stamp lingers, drop it + re-arm the edge
	// (clearDualMasterObserved's effect, inline under the held lock).
	if s.dualMasterObserved != nil {
		s.resetDualMasterState()
	}
	return false
}

// observePrimaryStability folds one observer poll into the settling-damp
// tracker and returns the consecutive-fresh-poll count for addr plus the
// dwell (elapsed LastPolledAt span) over which that streak accumulated.
// A new addr resets the streak to 1; the same addr advances it only when
// polledAt is strictly newer than the last recorded poll (so a pub/sub
// replay of a stale Addr does not inflate the count). An empty addr
// resets the tracker and returns (0, 0).
func (s *perCRState) observePrimaryStability(addr string, polledAt time.Time) (int, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if addr == "" {
		s.primaryStability = nil
		return 0, 0
	}
	st := s.primaryStability
	if st == nil || st.addr != addr {
		s.primaryStability = &primaryStabilityState{
			addr:        addr,
			firstPolled: polledAt,
			lastPolled:  polledAt,
			freshCount:  1,
		}
		return 1, 0
	}
	if polledAt.After(st.lastPolled) {
		st.freshCount++
		st.lastPolled = polledAt
	}
	return st.freshCount, st.lastPolled.Sub(st.firstPolled)
}

// fireSwitchMasterEdge records host as the last-fired elected primary
// Addr and reports whether T5 should fire, returning false when host
// already matches the recorded value (already emitted this episode).
func (s *perCRState) fireSwitchMasterEdge(host string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.switchMasterEdge == host {
		return false
	}
	s.switchMasterEdge = host
	return true
}

func (s *perCRState) resetSwitchMasterEdge() {
	s.mu.Lock()
	s.switchMasterEdge = ""
	s.mu.Unlock()
}

// observeMissingAuthFirstSeen seeds the first-seen time to now on the
// first observation (atomic test-and-set) and returns the recorded time.
func (s *perCRState) observeMissingAuthFirstSeen(now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.missingAuthSeen.IsZero() {
		s.missingAuthSeen = now
	}
	return s.missingAuthSeen
}

func (s *perCRState) clearMissingAuthSeen() {
	s.mu.Lock()
	s.missingAuthSeen = time.Time{}
	s.mu.Unlock()
}

// observeNoPrimarySince seeds the sustained-no-primary first-seen time
// to now on the first suppressed observation (atomic test-and-set) and
// returns the recorded time.
func (s *perCRState) observeNoPrimarySince(now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noPrimarySince.IsZero() {
		s.noPrimarySince = now
	}
	return s.noPrimarySince
}

func (s *perCRState) clearNoPrimarySince() {
	s.mu.Lock()
	s.noPrimarySince = time.Time{}
	s.mu.Unlock()
}

// staleReplicaEscapeArmedAt reports whether the Phase-8 escape's
// loop-independent guards both pass at now: the sustained-no-primary
// dwell (guard 1) and the per-CR escape cooldown (guard 3). Phase 8
// hoists this ahead of its pod loop so the gate-less classification
// dial — the escape survey's only new wire call — never runs on a pass
// where either guard would discard the reading anyway.
func (s *perCRState) staleReplicaEscapeArmedAt(noPrimarySince, now time.Time) bool {
	return !noPrimarySince.IsZero() &&
		now.Sub(noPrimarySince) >= staleReplicaEscapeDwell &&
		s.staleReplicaEscapeAllowed(now)
}

// staleReplicaEscapeAllowed reports whether the Phase-8 bounded escape
// may fire this pass: it has never fired, or the last escape is older
// than the per-CR cooldown (staleReplicaEscapeCooldown).
func (s *perCRState) staleReplicaEscapeAllowed(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.staleReplicaEscapeLastFired.IsZero() || now.Sub(s.staleReplicaEscapeLastFired) >= staleReplicaEscapeCooldown
}

func (s *perCRState) recordStaleReplicaEscape(now time.Time) {
	s.mu.Lock()
	s.staleReplicaEscapeLastFired = now
	s.mu.Unlock()
}

// sqKeepAliveInterval is how long a SentinelQuorum-status write may be
// skipped on unchanged content before a keep-alive re-stamp is forced.
// Half the aggregator's freshness window so a re-stamp always lands
// with a full window to spare — records can't age out between writes.
const sqKeepAliveInterval = sentinelQuorumFreshnessWindow / 2

// sqStatusWriteSkippable reports whether the SentinelQuorum-status
// write can be skipped this pass. Two conditions must both hold: the
// per-pod observation digest is unchanged AND the last write is
// younger than sqKeepAliveInterval (so the records stay inside the
// aggregator's freshness window before the next reconcile re-stamps).
// The recorded digest is "" until first set; a real digest is sha256
// hex, so the first write is never skippable. The keep-alive forces a
// periodic re-stamp even on stable content so a quiet-but-live
// cluster's LastObservedTime cannot freeze and age out — the latch
// behind the PrimaryConfirmed=Unknown re-convergence gap.
func (s *perCRState) sqStatusWriteSkippable(digest string, obsAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqStatusDigest != digest || s.sqLastObservedAt.IsZero() {
		return false
	}
	return obsAt.Sub(s.sqLastObservedAt) < sqKeepAliveInterval
}

func (s *perCRState) setSQDigest(digest string, obsAt time.Time) {
	s.mu.Lock()
	s.sqStatusDigest = digest
	s.sqLastObservedAt = obsAt
	s.mu.Unlock()
}

// pruneStale resets the prunable trackers — the edge detectors and
// digests whose loss is safe on a transient list-staleness. The
// reconcile mutex and the lifecycle-sensitive trackers (manualRollout,
// failoverLatch, switchMasterEdge, authPassword, primaryStability) are
// preserved: dropping them on a stale list-miss could re-fire an audit,
// re-open a failover-strip window, or lose the rotation OLD-password
// (primaryStability is addr-keyed and self-correcting either way).
func (s *perCRState) pruneStale() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quorum = nil
	s.rolloutTrigger = nil
	s.replicasRolled = nil
	s.fsmTransition = nil
	s.staleReplicas = nil
	s.sqStatusDigest = ""
	s.sqLastObservedAt = time.Time{}
	s.missingAuthSeen = time.Time{}
	// Safe-to-drop timers: a wrongful prune only delays a bounded escape
	// (which re-arms next pass), never causes an incorrect delete.
	s.noPrimarySince = time.Time{}
	s.staleReplicaEscapeLastFired = time.Time{}
	// Dual-master trackers are all edge-detector state: safe to drop on
	// a stale list-miss (the next scan re-stamps a real split), and
	// dropping them for a reclaimed/recreated same-name CR avoids
	// suppressing its first DualMasterObserved / SelfHealDeferred event
	// or inheriting the dead CR's exhausted self-heal attempt budget
	// (STS pod names are deterministic, so stale signatures would match
	// and the deferral constants recur verbatim).
	s.resetDualMasterState()
	s.dualMasterDeferEdge = ""
	s.dualMasterDeferLastAt = time.Time{}
	s.dualMasterSelfHeal = nil
}
