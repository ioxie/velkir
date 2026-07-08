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
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// failoverInFlightLatchTTL bounds how long the per-CR latch suppresses
// role-relabel after a `SENTINEL FAILOVER` was issued. Sized at the
// sentinel failover-timeout default (180s) plus a 30s observer-tick
// allowance — long enough for the observer's +switch-master /
// +failover-end pubsub path to land, short enough that a sentinel-
// side stall doesn't wedge relabel forever (the next reconcile after
// the latch expires re-derives normally and the FSM transitions to
// Degraded via the FailoverStalled exit edge).
const failoverInFlightLatchTTL = 210 * time.Second

// nogoodslaveCooldownTTL is the back-off after a SENTINEL FAILOVER
// failed with NOGOODSLAVE — sentinel's view of the replicas hasn't
// caught up (typical when replicas were just recreated by a rolling
// image-tag bump and are mid-sync from the about-to-be-failed-over
// primary). A short cool-down latch suppresses immediate retries so
// sentinel's slaves-discovery has time to re-converge; the next
// reconcile after expiry re-enters the dispatcher with sentinel
// likely now agreeing the candidate is healthy.
const nogoodslaveCooldownTTL = 15 * time.Second

// maxRolloutSnapshotAge bounds how stale the observer snapshot may be
// for the rollout dispatcher to authorize a SENTINEL FAILOVER. A wedged
// pull-tick can leave the snapshot's QuorumOK=true while pub/sub keeps
// replaying the prior quorum forward — quorum no live pull has
// confirmed. Refusing on age forces a fresh poll to land before the
// outgoing primary's label is stripped. Sized at 3× the observer pull
// cadence: one missed tick is tolerated, a stalled observer is not. A
// non-positive value disables the gate.
const maxRolloutSnapshotAge = 3 * sentinel.DefaultPollInterval

// snapshotStale reports whether the observer snapshot is too old to
// authorize a failover: now - snap.Primary.LastPolledAt exceeds maxAge.
// It keys off LastPolledAt — the last live pull tick — NOT UpdatedAt,
// which the pub/sub path also refreshes while carrying a prior quorum
// forward; gating on UpdatedAt would wave through a replayed-but-stale
// quorum. The boundary is inclusive-fresh (age == maxAge is still
// fresh). A non-positive maxAge disables the check (always fresh). Pure
// so the staleness decision is table-testable without driving the
// observer.
func snapshotStale(snap sentinel.Snapshot, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}
	return now.Sub(snap.Primary.LastPolledAt) > maxAge
}

// failoverInFlightLatch records the per-CR "we just stripped
// role=primary and dispatched SENTINEL FAILOVER" state. Active until
// either:
//
//   - The observer's snapshot Addr changes from the pre-strip Addr —
//     +switch-master arrived, the snapshot now points at the new
//     primary, normal relabel can resume.
//   - The deadline expires — failover stalled or the observer is
//     wedged; the next reconcile clears the latch and re-derives.
//
// In-memory, per-CR. Lost on operator restart; the next reconcile
// re-derives state from observation (the post-failover cluster has
// a labeled new primary via the observer-driven path on the freshly
// elected leader).
type failoverInFlightLatch struct {
	preStripAddr string
	deadline     time.Time
	// preStripEpoch is the Sentinel config-epoch observed the instant
	// before the strip — the monotonic fencing token mirrored from the
	// durable marker's PreStripEpoch. An observer-Addr move carrying a
	// LOWER epoch is a stale view and does not exit the latch (see
	// active). Zero when the epoch was unparseable at strip time, which
	// makes the fence inert.
	preStripEpoch int64
}

// active is the pure enter/exit decision for the latch against the
// current observed primary addr+epoch. now is injected so the decision
// is table-testable. It returns false (latch no longer holds the
// critical section) once EITHER the deadline has passed — the
// timeout/escape that keeps the section from being an infinite hold —
// OR the observer has moved off the pre-strip primary with an epoch at
// least the one the strip ran under. An observer-Addr move whose epoch
// is lower than preStripEpoch is a stale sentinel view of an election
// this dispatch already superseded; it is refused (the latch stays
// active) so the roll cannot re-stamp a pre-failover primary. A zero
// observed epoch against a real fence is treated the same — not yet
// superseded. When preStripEpoch is zero the fence is inert and the
// move-off clears as before.
func (l *failoverInFlightLatch) active(now time.Time, observedAddr string, observedEpoch int64) bool {
	if now.After(l.deadline) {
		return false
	}
	if observedAddr != "" && observedAddr != l.preStripAddr {
		if l.preStripEpoch > 0 && observedEpoch < l.preStripEpoch {
			return true
		}
		return false
	}
	return true
}

// failoverLatchSet records that the operator just dispatched
// SENTINEL FAILOVER for cr. preStripAddr is the observer's Addr the
// instant before role=primary was stripped — the latch clears when
// the observer's Addr no longer matches (i.e., +switch-master moved
// the snapshot to the new primary). Idempotent: a re-set with the
// same key overwrites the deadline. Sets a zero epoch fence (the
// epoch-aware path is failoverLatchSetWithDeadline).
func (r *ValkeyReconciler) failoverLatchSet(cr types.NamespacedName, preStripAddr string) {
	r.failoverLatchSetWithDeadline(cr, preStripAddr, 0, time.Now().Add(failoverInFlightLatchTTL))
}

// failoverLatchSetWithDeadline is failoverLatchSet with an explicit
// deadline + epoch fence, so the in-memory latch and the durable
// FailoverDispatch marker (persistFailoverDispatch) share one deadline
// AND one PreStripEpoch — a rehydrated latch then has the same expiry
// and fence as the process-local one it replaces.
func (r *ValkeyReconciler) failoverLatchSetWithDeadline(cr types.NamespacedName, preStripAddr string, preStripEpoch int64, deadline time.Time) {
	r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
		preStripAddr:  preStripAddr,
		deadline:      deadline,
		preStripEpoch: preStripEpoch,
	})
}

// failoverLatchClear drops the latch for cr. Idempotent.
func (r *ValkeyReconciler) failoverLatchClear(cr types.NamespacedName) {
	r.stateFor(cr).clearFailoverLatch()
}

// failoverLatchSetCooldown installs a short-lived latch (TTL =
// nogoodslaveCooldownTTL) after a SENTINEL FAILOVER failed with
// NOGOODSLAVE. preStripAddr is the same pre-strip primary observed
// at the failed dispatch — the latch still clears on
// observer-Addr-change, so a sentinel that does eventually promote
// the candidate (via its own internal failover) unwedges the latch
// regardless of the cool-down deadline. The cool-down only suppresses
// retries, so it carries no epoch fence.
func (r *ValkeyReconciler) failoverLatchSetCooldown(cr types.NamespacedName, preStripAddr string) {
	r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
		preStripAddr: preStripAddr,
		deadline:     time.Now().Add(nogoodslaveCooldownTTL),
	})
}

// failoverLatchActive reports whether the latch is currently set AND
// the observer hasn't yet seen a superseding new primary. Auto-clears
// on deadline expiry OR on an epoch-honoured observer-Addr-change so
// callers on the maintenance path don't have to remember to drain
// stale latches.
//
// currentObservedAddr / currentObservedEpoch are the observer
// snapshot's most recent primary Addr+config-epoch (Addr "" / epoch 0
// when no snapshot is present). When the Addr differs from the
// pre-strip Addr AND the observed epoch is at least the one the strip
// ran under, the latch clears: +switch-master moved the snapshot to a
// genuinely newer primary, normal relabel can resume. A lower-epoch
// move-off is a stale view and is refused (latch stays active) — the
// epoch fence the durable PreStripEpoch carries.
func (r *ValkeyReconciler) failoverLatchActive(cr types.NamespacedName, currentObservedAddr string, currentObservedEpoch int64) bool {
	ps, ok := r.stateForIfPresent(cr)
	if !ok {
		return false
	}
	latch := ps.loadFailoverLatch()
	if latch == nil {
		return false
	}
	if latch.active(time.Now(), currentObservedAddr, currentObservedEpoch) {
		return true
	}
	ps.clearFailoverLatch()
	return false
}

// failoverLatchActivePure is the non-mutating read of the latch used by
// the IsFailoverInFlight critical-section predicate, which is consulted
// from several call sites per reconcile (and cross-package via the
// deferral predicate). Draining a stale latch is the maintenance path's
// job (maintainFailoverDispatchMarker), not a predicate read's, so this
// variant never clears. Same enter/exit + epoch-fence decision as
// failoverLatchActive.
func (r *ValkeyReconciler) failoverLatchActivePure(cr types.NamespacedName, currentObservedAddr string, currentObservedEpoch int64) bool {
	ps, ok := r.stateForIfPresent(cr)
	if !ok {
		return false
	}
	latch := ps.loadFailoverLatch()
	if latch == nil {
		return false
	}
	return latch.active(time.Now(), currentObservedAddr, currentObservedEpoch)
}

// persistFailoverDispatch durably records an in-flight
// strip-then-`SENTINEL FAILOVER` on Status.Rollout.FailoverDispatch,
// written BEFORE the role=primary strip so the suppression survives an
// operator restart. Unlike the best-effort SuspendedFrom stamp this
// returns its error: the strip is gated on a durable record, so a failed
// persist (conflict included) must abort the dispatch — a strip without
// the marker reopens the post-restart wrong-primary window this closes.
// The next reconcile retries with a fresh CR.
func (r *ValkeyReconciler) persistFailoverDispatch(ctx context.Context, v *valkeyv1beta1.Valkey, preStripAddr string, preStripEpoch int64, deadline time.Time) error {
	orig := v.DeepCopy()
	if v.Status.Rollout == nil {
		v.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
	}
	v.Status.Rollout.FailoverDispatch = &valkeyv1beta1.FailoverDispatchStatus{
		PreStripAddr:  preStripAddr,
		PreStripEpoch: preStripEpoch,
		Deadline:      &metav1.Time{Time: deadline},
	}
	if err := r.Status().Patch(ctx, v, client.MergeFrom(orig)); err != nil {
		// Roll the in-memory copy back so a same-reconcile read does not
		// see a marker the apiserver rejected.
		v.Status.Rollout = orig.Status.Rollout
		return err
	}
	return nil
}

// clearFailoverDispatch drops Status.Rollout.FailoverDispatch. Idempotent;
// best-effort (a conflict is benign — the next reconcile's
// maintainFailoverDispatchMarker re-clears once the window is observed
// closed). A non-conflict error is logged at V(1) so a persistent write
// failure surfaces instead of being silently swallowed.
func (r *ValkeyReconciler) clearFailoverDispatch(ctx context.Context, v *valkeyv1beta1.Valkey) {
	if v.Status.Rollout == nil || v.Status.Rollout.FailoverDispatch == nil {
		return
	}
	orig := v.DeepCopy()
	v.Status.Rollout.FailoverDispatch = nil
	if err := r.Status().Patch(ctx, v, client.MergeFrom(orig)); err != nil && !apierrors.IsConflict(err) {
		logf.FromContext(ctx).V(1).Info("clearFailoverDispatch: status patch failed (non-conflict)",
			"valkey", v.Name, "namespace", v.Namespace, "err", err.Error())
	}
}

// maintainFailoverDispatchMarker reconciles the durable FailoverDispatch
// marker against the live observer snapshot and the in-memory latch, once
// per reconcile (called at the top of reconcileRoleLabels, before
// desiredRolesForCR). Two responsibilities:
//
//   - Rehydrate: after an operator restart the in-memory latch is gone
//     but the durable marker survives. Reconstruct the in-memory latch
//     from the marker (when it has not yet expired) so the role-relabel
//     suppression — desiredRolesForCR's latch check and the
//     runPrimaryRolloutDispatch hold — keeps the pre-strip primary from
//     being re-stamped mid-election.
//   - Clear: once the observer's snapshot moves off the pre-strip address
//     (`+switch-master` landed) or the deadline passes, the suppression
//     window is over; drop the durable marker so it does not outlive the
//     failover.
//
// No-op when no marker is set — the common case, including all
// non-sentinel modes (the marker is only ever written on the sentinel
// primary-rollout path).
func (r *ValkeyReconciler) maintainFailoverDispatchMarker(ctx context.Context, v *valkeyv1beta1.Valkey, cr types.NamespacedName) {
	if v.Status.Rollout == nil || v.Status.Rollout.FailoverDispatch == nil {
		return
	}
	marker := v.Status.Rollout.FailoverDispatch

	observedAddr := ""
	var observedEpoch int64
	if r.SentinelObserver != nil {
		if snap := r.SentinelObserver.Snapshot(cr); snap.Present {
			observedAddr = snap.Primary.Addr
			observedEpoch = snap.Primary.Epoch
		}
	}

	// Rehydrate the in-memory latch from the durable marker when it is
	// absent (post-restart) and the marker has not expired. A still-live
	// in-memory latch is left untouched (the operator never crashed). The
	// marker's PreStripEpoch rides along so the rehydrated latch fences
	// stale observations exactly as the process-local one did.
	if ps, ok := r.stateForIfPresent(cr); !ok || ps.loadFailoverLatch() == nil {
		if marker.Deadline != nil && time.Now().Before(marker.Deadline.Time) {
			r.failoverLatchSetWithDeadline(cr, marker.PreStripAddr, marker.PreStripEpoch, marker.Deadline.Time)
		}
	}

	// failoverLatchActive auto-clears the in-memory latch on deadline
	// expiry or an epoch-honoured observer-address-change; mirror that
	// onto the durable marker so it does not outlive the failover.
	if !r.failoverLatchActive(cr, observedAddr, observedEpoch) {
		r.clearFailoverDispatch(ctx, v)
	}
}

// runPrimaryRolloutDispatch is the master-aware primary-rollout
// entry point — wired from reconcileSentinelOrchestration after the
// per-CR observer is Ensured. Drives the T14 transition with two
// preconditions: (a) at least one replica's `master_repl_offset -
// slave_repl_offset` is within `spec.rollout.maxLagBytes`, and (b)
// the outgoing primary's `role=primary` label is stripped BEFORE
// `SENTINEL FAILOVER` so the `<cr>` Service stops routing writes
// during the election window.
//
// Returns true when the FSM advanced via T14 (success path) OR T15
// (no-suitable-replica refusal path), so the caller can record the
// dispatch happened in this reconcile pass.
//
// No-op (returns false) when:
//
//   - state != StateRolloutPrimary (FSM not in the hand-off region)
//   - QuorumOK=false (split-brain guard; observer hasn't confirmed)
//   - latch already active (a prior reconcile dispatched; we wait for
//     the observer's +switch-master, no re-strip, no re-FAILOVER)
//
// Error-handling contract: any preflight or wire-side error logs at
// V(1) and returns false (the FSM stays in RolloutPrimary; the next
// reconcile retries). The strip+FAILOVER pair is best-effort
// idempotent — re-stripping an already-stripped pod is a no-op
// patch; re-issuing FAILOVER returns -INPROG which the wire layer
// surfaces as ErrFailoverInProgress (treated as success).
func (r *ValkeyReconciler) runPrimaryRolloutDispatch(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	state orchestration.State,
	masterName, password string,
	endpoints []sentinel.Endpoint,
) bool {
	log := logf.FromContext(ctx)
	if state != orchestration.StateRolloutPrimary {
		return false
	}
	if r.SentinelObserver == nil {
		return false
	}
	snap := r.SentinelObserver.Snapshot(cr)
	if !snap.Present || !snap.Primary.QuorumOK {
		// QuorumOK=false → desiredRolesForCR's split-brain guard
		// already suppressed relabel; no point in stripping a
		// pod whose label might already be wrong. The FSM stays
		// in RolloutPrimary; T15 will eventually fire if the
		// observer can't confirm the primary.
		return false
	}
	if now := time.Now(); snapshotStale(snap, now, maxRolloutSnapshotAge) {
		// QuorumOK=true but the snapshot is older than the freshness
		// window: the pull-tick is wedged and pub/sub is replaying a
		// stale quorum forward. Defer rather than strip the primary and
		// FAILOVER on quorum no live pull confirmed; the next reconcile
		// re-checks once a fresh poll lands.
		age := now.Sub(snap.Primary.LastPolledAt)
		log.V(1).Info("primary-rollout: deferring FAILOVER — observer snapshot stale",
			"age", age.Round(time.Second), "maxAge", maxRolloutSnapshotAge)
		r.emitFailoverDeferredStale(v, age)
		return false
	}
	if r.failoverLatchActive(cr, snap.Primary.Addr, snap.Primary.Epoch) {
		// Prior reconcile already dispatched; observer hasn't
		// reported +switch-master yet. Hold position — relabel
		// suppression is in place via desiredRolesForCR's latch
		// check.
		return false
	}

	// Step 1: identify outgoing primary + candidates.
	primary, candidates, err := r.classifyPrimaryRolloutPods(ctx, v)
	if err != nil {
		log.V(1).Info("primary-rollout: classify pods failed", "err", err.Error())
		return false
	}
	if primary == nil {
		// No pod carries role=primary — Phase 7 hasn't relabelled
		// yet (or pre-bootstrap). Wait for the next reconcile;
		// derive_state.go won't return RolloutPrimary in this
		// shape so we typically don't get here.
		return false
	}
	if len(candidates) == 0 {
		// Single-pod replication mode — no replica to promote.
		// Webhook should have soft-warned at admission; emit
		// NoSuitableReplica + route to T15 → Degraded.
		r.emitNoSuitableReplica(v, "no replica candidates available")
		_, _, _ = r.applyFSM(v, orchestration.StateRolloutPrimary,
			orchestration.EventReconcileTick,
			orchestration.GuardCtx{QuorumOK: true, CandidateReplicaReady: false})
		return true
	}

	// Step 2: offset-tolerance preflight.
	maxLag := rolloutMaxLagBytes(v)
	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	candidate := pickPromotionCandidate(ctx, checker, candidates, password, maxLag)
	if candidate == nil {
		r.emitNoSuitableReplica(v, fmt.Sprintf(
			"no replica within %d bytes of primary offset (maxLagBytes); checked %d candidate(s)",
			maxLag, len(candidates)))
		_, _, _ = r.applyFSM(v, orchestration.StateRolloutPrimary,
			orchestration.EventReconcileTick,
			orchestration.GuardCtx{QuorumOK: true, CandidateReplicaReady: false})
		return true
	}

	// Step 3: strip role=primary BEFORE issuing FAILOVER. The
	// Service `<cr>` selector immediately stops routing writes to
	// the outgoing primary, closing the write-loss window during the
	// election. Latch is set BEFORE the strip so a racing reconcile
	// (unlikely — per-CR mutex serialises) doesn't re-stamp the
	// label between strip and FAILOVER.
	preStripAddr := snap.Primary.Addr
	// PreStripEpoch is the config-epoch the operator is acting under — the
	// monotonic fence later observations/actions are checked against.
	// Best-effort: zero when sentinel hasn't surfaced an epoch yet, which
	// leaves the fence inert.
	preStripEpoch := snap.Primary.Epoch
	deadline := time.Now().Add(failoverInFlightLatchTTL)
	// Persist the strip intent to the CR status BEFORE the strip. If the
	// operator crashes between the strip and the observer's
	// +switch-master, the in-memory latch is lost but this durable marker
	// survives: the next operator rehydrates the suppression latch from it
	// (maintainFailoverDispatchMarker) and does not re-stamp role=primary
	// on the pre-strip primary mid-election. Gating the strip on a
	// successful persist is the point — a strip without the durable record
	// reopens the very crash window this closes.
	if perr := r.persistFailoverDispatch(ctx, v, preStripAddr, preStripEpoch, deadline); perr != nil {
		log.V(0).Info("primary-rollout: failed to persist strip intent; deferring strip+FAILOVER",
			"pod", primary.Name, "err", perr.Error())
		return false
	}
	r.failoverLatchSetWithDeadline(cr, preStripAddr, preStripEpoch, deadline)
	if stripErr := r.stripPrimaryLabel(ctx, primary); stripErr != nil {
		log.V(0).Info("primary-rollout: failed to strip role=primary; will retry", "pod", primary.Name, "err", stripErr.Error())
		// Clear the latch + durable marker so the next reconcile retries
		// cleanly. Leaving either set without an actual strip would just
		// suppress relabel for no reason.
		r.failoverLatchClear(cr)
		r.clearFailoverDispatch(ctx, v)
		return false
	}

	// Step 4: SENTINEL FAILOVER. INPROG is treated as success — a
	// failover is happening, we just didn't initiate it on this
	// round.
	failErr := r.SentinelObserver.IssueFailover(ctx, cr, masterName, password, endpoints)
	if failErr != nil && !errors.Is(failErr, sentinel.ErrFailoverInProgress) {
		log.V(0).Info("primary-rollout: SENTINEL FAILOVER failed; rolling back strip and applying cool-down latch",
			"pod", primary.Name, "err", failErr.Error())
		// Roll back the strip so the Service keeps routing writes
		// to the still-primary pod. Restore failure is non-fatal:
		// Phase 7 of the next reconcile re-stamps the label from the
		// observer-snapshot view (still pointing at this pod).
		if restoreErr := r.restorePrimaryLabel(ctx, primary); restoreErr != nil {
			log.V(0).Info("primary-rollout: rollback restore failed; Phase 7 will re-stamp on next reconcile",
				"pod", primary.Name, "err", restoreErr.Error())
		}
		// The strip was rolled back (label restored), so the durable
		// in-flight marker no longer reflects reality — drop it before
		// the cool-down latch so a restart during the cool-down does not
		// suppress relabel of the still-primary pod. Restore-then-clear
		// ordering keeps a crash in this window harmless: a lingering
		// marker only re-suppresses relabel of a pod that is already
		// correctly labelled primary.
		r.clearFailoverDispatch(ctx, v)
		// Replace the in-flight latch with a short cool-down latch
		// so the next reconcile doesn't immediately re-enter the
		// dispatcher and re-fail. Sentinel's slaves-discovery
		// typically re-converges within nogoodslaveCooldownTTL after
		// a replica's replication state stabilises (the canonical
		// trigger of NOGOODSLAVE is "candidate is mid-sync"). Without
		// the cool-down the operator hot-retries against an
		// observer that's still reporting the same NOGOODSLAVE shape
		// for 10–20 seconds, burning multiple Failed→Recovered event
		// pairs and stretching the post-rollout role=primary search
		// out of the e2e budget.
		r.failoverLatchSetCooldown(cr, preStripAddr)
		return false
	}

	// Step 5: T14's SideEffect emits FailoverInitiated via the FSM-
	// canonical event recorder. The candidate / primary detail goes
	// to a structured log line rather than a duplicate CR event so
	// alert rules and event-deduplication tooling see one event per
	// transition.
	_, _, matched := r.applyFSM(v, orchestration.StateRolloutPrimary,
		orchestration.EventReconcileTick,
		orchestration.GuardCtx{QuorumOK: true, CandidateReplicaReady: true})
	if !matched {
		// Defensive: if T14 didn't match (FSM transition table
		// drift), at least record what we did so an admin can
		// trace.
		log.V(0).Info("primary-rollout: FSM did not match T14 — transition table drift?",
			"state", state, "candidate", candidate.Name, "primary", primary.Name)
	}
	log.V(0).Info("primary-rollout: SENTINEL FAILOVER dispatched",
		"primary", primary.Name, "candidate", candidate.Name, "maxLagBytes", maxLag)
	// Audit the privileged FAILOVER on the operator-driven rollout path.
	// rollout_id is the target STS revision the rollout converges to, read
	// off the promotion candidate (replicas roll to target before the
	// primary, so the candidate already carries the target revision hash).
	auditFailover(ctx, cr, primary.Name, candidate.Name, candidate.Labels[stsRevisionLabel])
	return true
}

// classifyPrimaryRolloutPods lists valkey pods for cr and returns the
// outgoing primary (the one labelled role=primary) plus the candidate
// list (every other valkey pod that is a viable promotion target:
// not terminating, IP published, Ready). Returns (nil, nil, err) on
// list failure; (nil, candidates, nil) when no pod carries
// role=primary (caller treats as "no work, wait for Phase 7").
func (r *ValkeyReconciler) classifyPrimaryRolloutPods(ctx context.Context, v *valkeyv1beta1.Valkey) (*corev1.Pod, []corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return nil, nil, fmt.Errorf("listing valkey pods: %w", err)
	}
	var primary *corev1.Pod
	candidates := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		p := pods.Items[i]
		if p.Labels[RoleLabel] == roleValuePrimary {
			primary = &pods.Items[i]
			continue
		}
		if p.DeletionTimestamp != nil {
			// Mid-termination — promoting a pod the roll is about
			// to replace hands the election a vanishing target.
			continue
		}
		if p.Status.PodIP == "" {
			// No IP → can't dial; skip. The next reconcile
			// (post-pod-IP-publish) will pick this candidate
			// up.
			continue
		}
		if !podReady(&p) {
			// Still booting / mid initial sync. The hand-off gate
			// (ReplicasReadyForHandoff) normally defers the whole
			// dispatch before this point; this filter covers the
			// dispatcher's own fresher pod list.
			continue
		}
		candidates = append(candidates, p)
	}
	return primary, candidates, nil
}

// pickPromotionCandidate runs the offset-tolerance preflight: for
// each candidate, dial via LagChecker and reject any whose
// `master_repl_offset - slave_repl_offset` exceeds maxLagBytes OR
// whose master link is reported down. Among the eligible set, prefer
// the candidate with the lowest lag (closest to the primary's
// offset).
//
// Returns nil when no candidate qualifies. Best-effort: dial errors
// disqualify that candidate but do not fail the whole preflight.
//
// `master`-self-reporting candidates (Role=="master" in INFO
// replication) are eligible: the operator's role=replica label
// disagrees with the pod's runtime view, almost certainly because
// a sentinel-driven flip happened that Phase 7 hasn't yet
// relabelled. Trust the pod — promoting an already-promoted pod is
// a no-op.
func pickPromotionCandidate(ctx context.Context, checker valkey.LagChecker, candidates []corev1.Pod, password string, maxLagBytes int64) *corev1.Pod {
	type scored struct {
		pod *corev1.Pod
		lag int64
	}
	eligible := make([]scored, 0, len(candidates))
	for i := range candidates {
		p := &candidates[i]
		addr := net.JoinHostPort(p.Status.PodIP, fmt.Sprintf("%d", valkey.DefaultPort))
		state, err := checker.CheckLag(ctx, addr, password)
		if err != nil {
			continue
		}
		if state.Role == "master" {
			// Pod self-reports as primary — sentinel already
			// flipped it; treat as fully eligible (lag=0 so it
			// wins the sort tie-break too).
			eligible = append(eligible, scored{pod: p, lag: 0})
			continue
		}
		if !state.LinkUp {
			continue
		}
		if state.LagBytes > maxLagBytes {
			continue
		}
		eligible = append(eligible, scored{pod: p, lag: state.LagBytes})
	}
	if len(eligible) == 0 {
		return nil
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].lag != eligible[j].lag {
			return eligible[i].lag < eligible[j].lag
		}
		// Tie-break on pod name for determinism — sentinel picks
		// based on its own criteria (replica-priority, runid),
		// but the candidate we name in the FailoverInitiated
		// event message should be stable across reconciles for
		// any given lag tie.
		return eligible[i].pod.Name < eligible[j].pod.Name
	})
	return eligible[0].pod
}

// stripPrimaryLabel removes the `velkir.ioxie.dev/role` label from the
// outgoing primary pod so the `<cr>` Service selector stops routing
// writes during the election window. Idempotent: a pod that already lacks
// the label is patched as a no-op (the strategic-merge patch
// preserves field-manager ordering).
func (r *ValkeyReconciler) stripPrimaryLabel(ctx context.Context, primary *corev1.Pod) error {
	if _, ok := primary.Labels[RoleLabel]; !ok {
		return nil
	}
	old := primary.DeepCopy()
	delete(primary.Labels, RoleLabel)
	if err := r.Patch(ctx, primary, client.StrategicMergeFrom(old)); err != nil {
		return fmt.Errorf("stripping role label from %s: %w", primary.Name, err)
	}
	if r.Recorder != nil {
		// PodLabelReconciled is the canonical "we touched a pod
		// label" reason — re-used here so event consumers see one
		// uniform reason for both Phase 7 stamps and this strip.
		r.Recorder.Eventf(primary, nil, corev1.EventTypeNormal, string(events.PodLabelReconciled), "PrimaryLabelStripBeforeFailover",
			"stripped role=primary from %s before SENTINEL FAILOVER", primary.Name)
	}
	auditRoleLabel(ctx, types.NamespacedName{Namespace: primary.Namespace, Name: primary.Labels[CRLabel]},
		primary.Name, old.Labels[RoleLabel], roleLabelUnset, "switch-master")
	return nil
}

// restorePrimaryLabel re-stamps `velkir.ioxie.dev/role=primary` on a pod
// the operator just stripped. Used to roll back the strip when the
// subsequent SENTINEL FAILOVER fails — leaving the pod stripped
// would block writes (Service has no role=primary endpoint) for as
// long as the failoverInFlightLatch is held. Restoring the label
// lets the next reconcile re-derive cleanly: Phase 7 sees the label
// already present (no patch); the dispatcher retries strip+FAILOVER
// from scratch.
//
// Idempotent: a pod that already carries `role=primary` is patched
// as a no-op.
func (r *ValkeyReconciler) restorePrimaryLabel(ctx context.Context, primary *corev1.Pod) error {
	if primary.Labels[RoleLabel] == roleValuePrimary {
		return nil
	}
	old := primary.DeepCopy()
	if primary.Labels == nil {
		primary.Labels = map[string]string{}
	}
	primary.Labels[RoleLabel] = roleValuePrimary
	if err := r.Patch(ctx, primary, client.StrategicMergeFrom(old)); err != nil {
		return fmt.Errorf("restoring role label on %s: %w", primary.Name, err)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(primary, nil, corev1.EventTypeNormal, string(events.PodLabelReconciled), "PrimaryLabelRestoreAfterFailoverError",
			"restored role=primary on %s after SENTINEL FAILOVER failed", primary.Name)
	}
	fromVal := old.Labels[RoleLabel]
	if fromVal == "" {
		fromVal = roleLabelUnset
	}
	auditRoleLabel(ctx, types.NamespacedName{Namespace: primary.Namespace, Name: primary.Labels[CRLabel]},
		primary.Name, fromVal, roleValuePrimary, "switch-master")
	return nil
}

// emitNoSuitableReplica fires the NoSuitableReplica Warning event
// against the CR. NoSuitableReplica is the specific "primary roll
// cannot proceed because no replica meets offset tolerance" reason;
// T15's PrimaryRolloutBlocked is the broader companion event.
func (r *ValkeyReconciler) emitNoSuitableReplica(v *valkeyv1beta1.Valkey, detail string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(v, nil, corev1.EventTypeWarning, string(events.NoSuitableReplica), "PrimaryRolloutPreflight",
		"failover refused: %s", detail)
}

// emitFailoverDeferredStale fires a FailoverSuppressed Warning event
// when the rollout dispatcher declines to authorize a FAILOVER because
// the observer snapshot is older than maxRolloutSnapshotAge. Reuses the
// FailoverSuppressed reason — the stale-snapshot case is one of its
// documented disagreement signals — so on-call sees the same event
// family as the other failover-decision deferrals.
func (r *ValkeyReconciler) emitFailoverDeferredStale(v *valkeyv1beta1.Valkey, age time.Duration) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(v, nil, corev1.EventTypeWarning, string(events.FailoverSuppressed), "PrimaryRolloutDispatch",
		"deferring SENTINEL FAILOVER: observer snapshot is %s old (> max %s); the pull tick has not confirmed quorum recently — awaiting a fresh poll",
		age.Round(time.Second), maxRolloutSnapshotAge)
}

// rolloutMaxLagBytes reads spec.rollout.maxLagBytes with the
// defaulter's 10000-byte value as the fallback. The defaulter writes
// a positive value on every Create/Update; a zero
// value means either "bypassed admission" (envtest without webhooks,
// direct apiserver write) OR the user explicitly set zero (CRD CEL
// rule allows >= 0). For zero, treat as the spec default — a true
// zero-tolerance preflight would refuse every failover the moment
// any write hit the primary post-INFO-snapshot, which is not a
// useful operational stance and the validator already warns on it.
func rolloutMaxLagBytes(v *valkeyv1beta1.Valkey) int64 {
	if v.Spec.Rollout.MaxLagBytes <= 0 {
		return 10000
	}
	return v.Spec.Rollout.MaxLagBytes
}
