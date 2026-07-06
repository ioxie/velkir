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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// observedFacts is the read-only view of cluster state the FSM
// integration layer needs in order to derive a State enum and to fill
// orchestration.GuardCtx for an Apply call. Each field is computed once
// per reconcile pass against a pod list + snapshot, then handed to the
// pure deriveStateFromFacts decision table below.
//
// Keeping the struct flat (no methods, no live cluster reads) makes
// deriveStateFromFacts trivially table-testable and forces every
// observation to land in exactly one place — the deriveState method —
// rather than being scattered across reconcile sub-phases.
type observedFacts struct {
	// IsStandalone short-circuits the FSM: standalone mode has no
	// sentinel, no rollout choreography, and no failover, so the
	// only meaningful FSM states are Bootstrap and Steady.
	IsStandalone bool

	// PodCount is the number of valkey-component pods observed for
	// the CR (sentinel pods are excluded). Zero means we are
	// pre-bootstrap — the STS may not have created its first pod
	// yet.
	PodCount int

	// QuorumOK reflects the latest sentinel snapshot's QuorumOK
	// flag, treating an absent snapshot (observer not yet
	// produced its first poll) as false. Standalone mode hits the
	// IsStandalone short-circuit above and never inspects this.
	QuorumOK bool

	// FailoverInFlight is true when the sentinel observer's most
	// recent snapshot indicates a sentinel-driven failover is
	// mid-flight (between SENTINEL FAILOVER issue and
	// +failover-end). Currently hard-wired to false until the
	// +switch-master observer event is wired into the snapshot;
	// tracking the field explicitly keeps the decision table's
	// structure stable.
	FailoverInFlight bool

	// PrimaryAtTargetRevision is true when the pod currently
	// labelled role=primary carries the STS's UpdateRevision in
	// its controller-revision-hash label. False when no primary
	// pod is observed.
	PrimaryAtTargetRevision bool

	// AllReplicasAtTargetRevision is true when every replica pod
	// observed carries the STS UpdateRevision. True vacuously
	// when no replica pods exist (single-pod replication / pre-
	// scale-out).
	AllReplicasAtTargetRevision bool

	// AllPodsAtTargetRevision combines primary + replica
	// coverage: every pod (regardless of role) matches the STS
	// UpdateRevision. Equivalent to PrimaryAtTargetRevision &&
	// AllReplicasAtTargetRevision when at least one pod exists,
	// kept as a separate field so the decision table's "Steady"
	// row reads off a single bool.
	AllPodsAtTargetRevision bool

	// ReplicasReadyForHandoff is true when the observed replica set
	// is complete (spec replicas minus the primary) AND every
	// replica is non-terminating, Ready, and replication-healthy.
	// Gates the RolloutPrimary hand-off: revision labels alone flip
	// AllReplicasAtTargetRevision the instant a rolled replica is
	// recreated, while the pod is still booting or mid initial sync
	// — dispatching SENTINEL FAILOVER in that window promotes an
	// unsettled replica and races the roll. Mirrors the in-flight
	// gates reconcilePodRollout applies before each replica delete.
	ReplicasReadyForHandoff bool
}

// deriveStateFromFacts maps observedFacts to a State.
// Pure function — no I/O, no kube reads, no time. Every input lands
// in the observedFacts struct so the table-driven test enumerates
// every branch deterministically.
//
// Decision order (priority high → low):
//
//  1. PodCount == 0 → Bootstrap (pre-pod).
//  2. IsStandalone → Steady (single-pod mode; FSM doesn't drive
//     rollout for standalone, but Steady gives downstream consumers
//     a non-Bootstrap signal once pods exist).
//  3. !QuorumOK → Degraded (sentinel mode lost quorum; the rollout
//     state machine refuses to advance until quorum returns).
//  4. FailoverInFlight → FailoverInFlight (sentinel-driven failover
//     is mid-flight; the operator observes, doesn't act).
//  5. AllPodsAtTargetRevision → Steady (no rollout in progress).
//  6. AllReplicasAtTargetRevision && !PrimaryAtTargetRevision →
//     RolloutPrimary (replicas are caught up, primary is the only
//     stale pod — hand-off territory) — but only once
//     ReplicasReadyForHandoff confirms the replica set is complete
//     and settled; until then the state stays RolloutReplicas so
//     the hand-off (and its SENTINEL FAILOVER) waits out the last
//     replica's restart cycle.
//  7. PrimaryAtTargetRevision && !AllReplicasAtTargetRevision →
//     RolloutReplicas (primary already at target — atypical but
//     possible if STS controller bypassed the operator; replicas
//     still need rolling).
//  8. fallback → RolloutReplicas (mid-rollout: some replicas at
//     target, some old, primary still old — replicas roll first).
//
// The function never returns StateRolloutPending, StateRolloutComplete,
// StateDegradedQuorumLost, or "" (the CRDeleted exit sentinel) —
// those states are entered only via FSM transitions, not derived
// from observation. deriveState answers only the "where do we look
// like we are right now" question.
func deriveStateFromFacts(f observedFacts) orchestration.State {
	if f.PodCount == 0 {
		return orchestration.StateBootstrap
	}
	if f.IsStandalone {
		return orchestration.StateSteady
	}
	if !f.QuorumOK {
		return orchestration.StateDegraded
	}
	if f.FailoverInFlight {
		return orchestration.StateFailoverInFlight
	}
	if f.AllPodsAtTargetRevision {
		return orchestration.StateSteady
	}
	if f.AllReplicasAtTargetRevision && !f.PrimaryAtTargetRevision {
		if !f.ReplicasReadyForHandoff {
			return orchestration.StateRolloutReplicas
		}
		return orchestration.StateRolloutPrimary
	}
	if f.PrimaryAtTargetRevision && !f.AllReplicasAtTargetRevision {
		return orchestration.StateRolloutReplicas
	}
	return orchestration.StateRolloutReplicas
}

// deriveState computes the FSM State and the observedFacts from the
// once-per-reconcile pod snapshot + StatefulSet plus the
// sentinel observer snapshot, so callers can hand the same
// observation to a subsequent applyFSM call without re-reading.
//
// Pure with respect to the cluster: it performs no kube reads — the
// caller fetches pods + sts once at the top of the reconcile (where a
// fetch failure blocks the pass) and threads them in. A nil sts means
// the StatefulSet isn't present yet, a legitimate pre-bootstrap
// transient that reads as Bootstrap.
func (r *ValkeyReconciler) deriveState(v *valkeyv1beta1.Valkey, pods []corev1.Pod, sts *appsv1.StatefulSet) (orchestration.State, observedFacts) {
	facts := observedFacts{
		IsStandalone: v.Spec.Mode == valkeyv1beta1.ModeStandalone,
	}

	// Sentinel snapshot — nil-safe (test injection or pre-startup
	// race) and !Present-safe (observer alive but pre-first-poll).
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	var observedAddr string
	var observedEpoch int64
	if !facts.IsStandalone && r.SentinelObserver != nil {
		snap := r.SentinelObserver.Snapshot(cr)
		if snap.Present {
			facts.QuorumOK = snap.Primary.QuorumOK
			observedAddr = snap.Primary.Addr
			observedEpoch = snap.Primary.Epoch
		}
	}
	// Failover-in-flight latch override. When
	// runPrimaryRolloutDispatch has dispatched SENTINEL FAILOVER but
	// the observer hasn't yet reported the new primary's Addr
	// (+switch-master not yet landed), pin facts.FailoverInFlight=true
	// so deriveStateFromFacts returns StateFailoverInFlight instead
	// of falling through to Bootstrap on the no-primary-labelled
	// pod topology. Auto-clears when the observer Addr changes to a
	// genuinely newer primary (snapshot moved off the pre-strip primary
	// with an epoch ≥ the strip's — a lower-epoch move is a stale view
	// and the section is held) or when the latch's deadline expires (the
	// escape: a wedged failover then re-derives, landing in Degraded on
	// the no-quorum/no-primary topology rather than holding the critical
	// section indefinitely).
	if r.failoverLatchActive(cr, observedAddr, observedEpoch) {
		facts.FailoverInFlight = true
	}

	// nil sts == StatefulSet not created yet; pre-STS is a legitimate
	// transient that reads as Bootstrap.
	if sts == nil {
		return deriveStateFromFacts(facts), facts
	}
	target := sts.Status.UpdateRevision

	facts.PodCount = len(pods)
	if facts.PodCount == 0 {
		return deriveStateFromFacts(facts), facts
	}

	var (
		primarySeen      bool
		primaryAtTarget  bool
		replicasAtTarget = true
		anyReplicaSeen   bool
		replicaCount     int
		replicasSettled  = true
	)
	for i := range pods {
		p := &pods[i]
		atTarget := target != "" && p.Labels[stsRevisionLabel] == target
		if p.Labels[RoleLabel] == roleValuePrimary {
			primarySeen = true
			primaryAtTarget = atTarget
			continue
		}
		anyReplicaSeen = true
		replicaCount++
		if !atTarget {
			replicasAtTarget = false
		}
		if p.DeletionTimestamp != nil || !podReady(p) || !podReplicationHealthy(p) {
			replicasSettled = false
		}
	}
	facts.PrimaryAtTargetRevision = primarySeen && primaryAtTarget
	facts.AllReplicasAtTargetRevision = !anyReplicaSeen || replicasAtTarget
	// Complete = one replica pod per spec replica minus the primary. A
	// replica mid-recreate is either still listed (terminating → not
	// settled) or already gone (count short) — both hold the hand-off.
	expectedReplicas := max(int(v.Spec.Valkey.Replicas)-1, 0)
	facts.ReplicasReadyForHandoff = replicasSettled && replicaCount == expectedReplicas
	facts.AllPodsAtTargetRevision = facts.PrimaryAtTargetRevision && facts.AllReplicasAtTargetRevision
	if !primarySeen && anyReplicaSeen {
		// Pre-bootstrap-completion (no primary labelled yet): the
		// AllPodsAtTargetRevision short-circuit above can only be
		// true when a primary exists and matches. Without a primary
		// the bootstrap label hasn't been stamped yet — Phase 7
		// hasn't run, sentinel quorum may not yet have agreed —
		// fall back to Bootstrap regardless of replica revisions.
		return orchestration.StateBootstrap, facts
	}

	return deriveStateFromFacts(facts), facts
}
