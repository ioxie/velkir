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

package orchestration

import (
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

// Condition type names. Constants live here so test assertions can
// reference the same identifier the production code emits — drift
// catches at compile time, not on a fragile string match.
const (
	TypeReady              = "Ready"
	TypeAvailable          = "Available"
	TypeProgressing        = "Progressing"
	TypeReconciled         = "Reconciled"
	TypeDegraded           = "Degraded"
	TypeReplicationHealthy = "ReplicationHealthy"
	TypeBootstrapComplete  = "BootstrapComplete"
	// TypePrimaryConfirmed flips True when a strict majority of fresh
	// SentinelQuorum records agree on the same non-empty observed
	// primary pod. False otherwise (no majority, all records stale,
	// not in sentinel mode). Driven by the sqaggregate package.
	TypePrimaryConfirmed = "PrimaryConfirmed"
	// TypeQuorumLost flips True when the count of fresh SentinelQuorum
	// records reporting QuorumReachable=true is below
	// spec.sentinel.quorum. Empty SQ list (no data yet) holds the
	// condition as Unknown rather than firing prematurely. Driven by
	// the sqaggregate package.
	TypeQuorumLost = "QuorumLost"
	// TypeSentinelTopologyReconciled is a decoupled hygiene condition:
	// True/InSync while the sentinel-known peer and replica counts match
	// spec, False/SentinelTopologyMismatch while a sustained deficit is
	// active, True/NotApplicable outside sentinel mode. Driven solely by
	// evalSentinelTopology from the freshness-gated per-CR read; it is
	// consulted by NO other evaluator (not derivePhase, evalReady, or
	// evalDegraded) so it can never worsen the incident ladder.
	TypeSentinelTopologyReconciled = "SentinelTopologyReconciled"
)

// Reasons for the SentinelTopologyReconciled condition.
const (
	ReasonSentinelTopologyInSync   = "InSync"
	ReasonSentinelTopologyMismatch = "SentinelTopologyMismatch"
)

// Phase strings. Derived from the condition tuple; surfaced as a
// single cosmetic field on the CR's `status.phase`.
const (
	PhasePending     = "Pending"
	PhaseProgressing = "Progressing"
	PhaseAvailable   = "Available"
	PhaseReady       = "Ready"
	PhaseDegraded    = "Degraded"
	PhasePaused      = "Paused"
)

// Reasons for the BootstrapComplete condition. The four values are
// closed; new reasons require an events-catalog update.
const (
	ReasonBootstrapNotConfigured  = "BootstrapNotConfigured"
	ReasonBootstrapReplicating    = "Replicating"
	ReasonBootstrapPromoted       = "PromotedFromBootstrap"
	ReasonBootstrapFailed         = "BootstrapFailed"
	ReasonBootstrapAlreadyLatched = "AlreadyLatched"
)

// Common reason strings used across multiple evaluators.
const (
	ReasonAsExpected    = "AsExpected"
	ReasonNotApplicable = "NotApplicable"
	ReasonProgressing   = "Progressing"
	ReasonAvailable     = "Available"
	ReasonReady         = "Ready"
	ReasonReconciled    = "Reconciled"
	ReasonDegraded      = "Degraded"
	ReasonReplicaWait   = "WaitingForReplicas"
	ReasonNoSTS         = "StatefulSetMissing"
	ReasonReconcileErr  = "ReconcileError"
	// ReasonRolloutStalled pairs the Degraded condition with the
	// per-pod readiness watchdog expiring on a master-aware rolling
	// update step. Mirrors the events.RolloutStalled Reason emitted
	// in the same reconcile pass; the two surface the same incident
	// at different durabilities (event = one-shot, condition = sticky
	// until the next evaluator pass observes the watchdog disarmed).
	ReasonRolloutStalled = "RolloutStalled"

	// ReasonHANotMet pairs the Degraded condition with the
	// runtime-side check on `mode=sentinel` CRs whose
	// `spec.valkey.replicas` is below the 2-replica floor required
	// for sentinel to perform a failover. The validating webhook
	// already emits a Warning at admission time matching this
	// wording (validateSentinelHA in
	// internal/webhook/v1beta1/valkey_validator.go); this reason
	// is the runtime-condition counterpart so users see the same
	// signal on `kubectl describe valkey` after the CR lands.
	// Cleared (Status=False, Reason=AsExpected) the moment the user
	// patches replicas up to ≥2.
	ReasonHANotMet = "HANotMet"

	// ReasonSplitBrain pairs the Degraded condition with the
	// observer-side split-brain guard: the sentinel observer's
	// snapshot reports QuorumOK=false (fewer than `quorum`
	// sentinels agree on the primary, or fewer than `quorum`
	// sentinels are reachable at all). Phase 7 suppresses pod
	// role-label writes while in this state; the Degraded condition
	// surfaces the same observation so dashboards and
	// `kubectl describe` show a stable signal alongside the
	// `SplitBrainDetected` event and the
	// `valkey_split_brain_detections_total` counter. Cleared the
	// moment QuorumOK flips back to true.
	ReasonSplitBrain = "SplitBrain"

	// ReasonNoMasterAgreement pairs Ready=False AND Degraded=True
	// with Phase 7's detection that the sentinel observer's reported
	// primary Addr matches NO current valkey pod's PodIP. This is
	// the cascading-wedge state: sentinels point at a defunct IP,
	// the operator can't safely stamp role=primary anywhere, and
	// the `<cr>` Service should not route writes. Distinct from
	// SplitBrain (sentinels disagreeing) — here they may agree, but
	// on a dead IP. Cleared when an observer snapshot reports an
	// Addr matching a live pod (sentinels recovered) OR when the
	// observer reports !Present (re-evaluated as a transient
	// boot-race rather than a wedge).
	ReasonNoMasterAgreement = "NoMasterAgreement"

	// ReasonMasterLost pairs Ready=False with the operator's
	// per-reconcile INFO-replication probe finding the pod currently
	// labelled role=primary unresponsive — the dead-master-still-
	// labelled window between a primary's process death and Sentinel
	// promoting a replacement. Distinct from NoMasterAgreement (which
	// requires a quorum-backed observer Addr pointing at no live pod):
	// MasterLost fires off direct liveness, so it covers the
	// down-after / election window where the observer still reports
	// QuorumOK against the now-dead primary (CKQUORUM ≠ liveness).
	// Status only — Sentinel still owns the election; the operator
	// drives no failover. Clears automatically once a (re)labelled
	// primary answers INFO again.
	ReasonMasterLost = "MasterLost"

	// ReasonDualMasterDivergence pairs Ready=False AND Degraded=True
	// with a dual-master scan observing two or more live pods
	// self-reporting role:master — active data divergence: writes are
	// landing on more than one primary, and the diverging writes on
	// the eventual loser will be discarded by the resolving resync.
	// Highest-precedence Degraded arm among the runtime wedge signals
	// because a real split usually co-occurs with quorum loss, and
	// the divergence — not the quorum state — is the fact that pages.
	// The operator surfaces but does not demote outside a failover
	// section (no fencing epoch exists there); inside one, the
	// bounded self-heal acts and this reason clears with the split.
	// Derived from the freshness-gated per-CR observation stamped by
	// any of four scans — the sentinel Phase 7a self-heal scan and
	// Phase 11 recovery survey, and the replication labeled-primary
	// orphan scan and no-labeled-primary observation scan; clears when
	// a scan sees at most one self-reported master or the observation
	// ages out.
	ReasonDualMasterDivergence = "DualMasterDivergence"

	// ReasonSentinelPeerLinkupStuck pairs Degraded=True with the
	// operator's stranded-sentinel repair failing to converge: a
	// sentinel wiped by REMOVE + MONITOR still reports an empty
	// peer-list after the configured number of consecutive surgeries,
	// so gossip provably cannot rebuild (auth verifying wrong, or a
	// NetworkPolicy blocking the `__sentinel__:hello` channel). More
	// specific than QuorumLost — it names WHY quorum can't recover —
	// so it outranks it. Does NOT force Ready=False: quorum may still
	// be intact among the survivors while one rebuilt sentinel is
	// stuck; the operator has backed off its destructive surgery and
	// surfaces the wedge for manual intervention. Derived from the
	// freshness-gated per-CR no-progress tracker; clears on a healthy
	// classification or when the stuck sentinel recovers.
	ReasonSentinelPeerLinkupStuck = "SentinelPeerLinkupStuck"
)

// Observation is the input to Evaluate — the per-reconcile snapshot
// the reconciler hands to the status package. It deliberately mirrors
// only the subset of cluster state the standalone-mode evaluators
// need; richer types (sentinel quorum agreement, rollout state)
// extend this struct as those phases land.
type Observation struct {
	// CR is the Valkey resource being reconciled. The evaluators read
	// spec fields (mode, bootstrapNode presence, etc.) and the prior
	// status conditions (for the BootstrapComplete latch).
	CR *valkeyv1beta1.Valkey

	// STS is the data-plane StatefulSet observed in the API server,
	// or nil if not yet present. Replicas / ReadyReplicas drive the
	// Ready / Available / Progressing evaluators.
	STS *appsv1.StatefulSet

	// ReconcileError is the error returned by the most recent
	// reconcile pass, or nil. Drives the Reconciled condition.
	ReconcileError error

	// Paused indicates the CR carried the pause annotation at this
	// reconcile. The status package keeps the prior conditions
	// untouched while paused; the reconciler is responsible for
	// short-circuiting before status work runs at all.
	Paused bool

	// RolloutWatchdog is the verdict from Check
	// against status.rollout.masterAware at the top of the reconcile.
	// Active=true + Expired=true drives Degraded=True with reason
	// RolloutStalled; the inactive zero value is the no-op default.
	RolloutWatchdog Result

	// SentinelQuorum is the per-CR aggregation result computed by
	// sqaggregate.Aggregate from the per-pod SentinelQuorum records.
	// Drives Conditions[type=PrimaryConfirmed, type=QuorumLost]; the
	// zero value (empty result with no fresh count) holds both
	// conditions as Unknown so a sentinel-mode CR before its first
	// SQ-write doesn't show spurious False conditions. Only meaningful
	// when CR.Spec.Mode == sentinel; standalone / replication CRs see
	// NotApplicable.
	SentinelQuorum sqaggregate.Result

	// SplitBrainActive is true when the sentinel observer's current
	// published snapshot reports a real quorum loss (Present &&
	// Quorum==QuorumStatusLost) on a sentinel-mode CR — this drives the
	// Degraded condition with reason=SplitBrain. Gated on Lost, NOT on
	// !QuorumOK: Quorum==Unknown ("no data yet" — the observer reached
	// fewer than a quorum of peers, e.g. on operator restart) is not
	// split-brain and must not flap the signal. Phase 7's relabel guard
	// is separate and stricter — it refuses to relabel on the broader
	// !QuorumOK (Unknown OR Lost); see `desiredRolesForCR`. The reconciler
	// derives this in the deferred status closure from a fresh
	// `SentinelObserver.Snapshot` read; standalone / replication CRs
	// always see false because Phase 7 short-circuits to bootstrap
	// roles before consulting the observer.
	SplitBrainActive bool

	// NoMasterAgreementActive is true when Phase 7 detects that the
	// sentinel observer's reported primary Addr matches no current
	// valkey pod's PodIP. The reconciler computes this in the
	// deferred status closure mirroring SplitBrainActive's pattern.
	// When true, the Ready condition forces False (write traffic
	// would hit a non-master) and Degraded fires with
	// ReasonNoMasterAgreement.
	NoMasterAgreementActive bool

	// MasterLostActive is true when the operator's per-reconcile
	// INFO-replication probe of the pod labelled role=primary has been
	// contiguously failing for at least the CR's down-after window —
	// the labelled primary's valkey process is unresponsive and
	// Sentinel has not yet promoted a replacement. The reconciler
	// derives this in updateStatus from the same per-CR state that
	// drives the MasterInfoTimeoutSeconds gauge, gated on the down-after
	// hysteresis so a single slow probe can't flap Ready and the
	// operator never calls the master lost before Sentinel's own
	// death-detection window. The reconciler additionally gates the read
	// on a recent probe observation: the probe runs only on passes that
	// reach Phase 11, so an early-return reconcile (PVC gate, list/apply
	// error, sentinel-orchestration guard) leaves the latch untouched —
	// the reconciler treats a stale latch (no measurement this pass) as
	// not-lost rather than pinning Ready off it. When true, the Ready
	// condition forces False with ReasonMasterLost. It does NOT force
	// Degraded: a bounded down-after / election window is expected
	// Sentinel operation, not a latched degradation — Degraded
	// continues to track the sustained wedge signals
	// (NoMasterAgreement / SplitBrain / QuorumLost). It also does NOT
	// force Available False: Available is a read-path signal (replicas
	// still serve reads while the primary is dead), so a healthy-STS CR
	// stays Available=True through a MasterLost window — mirroring the
	// existing NoMasterAgreement behaviour; consumers needing write-
	// availability gate on Ready (see evalAvailable). Inherently false
	// during bootstrap and operator-driven rollouts (no labelled
	// primary → the probe takes no measurement), so it never
	// false-positives outside a real labelled-primary failure.
	MasterLostActive bool

	// DualMasterActive is true when the operator's most recent
	// dual-master scan — the sentinel Phase 7a self-heal scan (inside a
	// failover section) or Phase 11 recovery survey (outside one), or
	// the replication labeled-primary orphan scan or no-labeled-primary
	// observation scan — observed two or more live pods self-reporting
	// role:master. The reconciler derives this in updateStatus from a
	// freshness-gated per-CR stamp: the scans run only on passes that
	// reach their phase, so an early-return reconcile leaves the
	// stamp untouched and a stale stamp (past the freshness window)
	// reads as inactive rather than pinning the condition. When true,
	// Ready forces False (writes are split across primaries) and
	// Degraded fires with ReasonDualMasterDivergence at the highest
	// precedence among the wedge signals — a divergence usually
	// co-occurs with quorum loss and must not be masked by it.
	DualMasterActive bool

	// SentinelPeerLinkupStuckActive is true when the operator's
	// stranded-sentinel repair has failed to converge: a wiped
	// sentinel still reports an empty peer-list after the configured
	// consecutive REMOVE + MONITOR surgeries. The reconciler derives it
	// in updateStatus from the freshness-gated per-CR no-progress
	// tracker (surgeries run only on passes reaching Phase 11, so a
	// stale flag ages out rather than latching). When true, Degraded
	// fires with ReasonSentinelPeerLinkupStuck above QuorumLost — it
	// names why quorum can't recover — but does NOT force Ready=False
	// (the survivor quorum may still serve).
	SentinelPeerLinkupStuckActive bool

	// QuorumSuppressionActive is the per-CR suppression-gate state
	// read by the QuorumLost condition evaluator. The gate has
	// built-in hysteresis: it flips active after 60s of sustained
	// NOQUORUM observations and clears after 2 consecutive
	// CKQUORUM=OK polls. Routing the condition through the gate
	// instead of the raw aggregator result smooths the user-visible
	// signal during recovery rollouts — without the gate, each
	// transient NOQUORUM observation (e.g. a pod replacement
	// briefly disturbing sentinel agreement) would flip the
	// condition True/False repeatedly. With it, the condition
	// tracks the same signal that gates the QuorumLost /
	// QuorumReached events; user-facing CR state stays consistent
	// with what those events say.
	QuorumSuppressionActive bool

	// Replication carries the per-pod replication-ready aggregate for
	// replication/sentinel modes, folded by the reconciler from the
	// ReplicationReadyGate pod conditions. It drives the
	// ReplicationHealthy condition to a definite value (never Unknown)
	// so `kubectl wait --for=condition=ReplicationHealthy` terminates.
	// Standalone leaves the zero value — evalReplicationHealthy
	// short-circuits to NotApplicable before reading it.
	Replication ReplicationObservation

	// SentinelTopologyMismatchActive / SentinelTopologySentinelDeficit /
	// SentinelTopologyReplicaDeficit are derived in updateStatus from the
	// freshness-gated per-CR topology read. They drive ONLY
	// TypeSentinelTopologyReconciled (via evalSentinelTopology) and are
	// deliberately consulted by no other evaluator, so the decoupled
	// hygiene condition can never move Ready / Degraded / phase.
	SentinelTopologyMismatchActive  bool
	SentinelTopologySentinelDeficit int
	SentinelTopologyReplicaDeficit  int
}

// ReplicationObservation is the per-CR replication-ready aggregate the
// reconciler folds from the ReplicationReadyGate pod conditions.
type ReplicationObservation struct {
	// GateEnabled mirrors spec.valkey.readinessGate.enabled. When false
	// the operator doesn't gate on master_link/lag, so the condition
	// resolves to True/NotApplicable rather than Unknown.
	GateEnabled bool
	// ReadyReplicas counts replica-role pods whose ReplicationReadyGate
	// condition is True (master_link up + lag within budget).
	ReadyReplicas int
}

// Evaluate runs every condition evaluator against the observation and
// derives the `phase` string from the resulting tuple. Returns the
// full conditions list (deterministic order matching the constants
// above) and the phase.
func Evaluate(o Observation) ([]metav1.Condition, string) {
	conds := []metav1.Condition{
		evalReady(o),
		evalAvailable(o),
		evalProgressing(o),
		evalReconciled(o),
		evalDegraded(o),
		evalReplicationHealthy(o),
		evalBootstrapComplete(o),
		evalPrimaryConfirmed(o),
		evalQuorumLost(o),
		evalSentinelTopology(o),
	}
	// The evaluators (and findPriorCondition) all guard o.CR == nil;
	// keep the ObservedGeneration stamp consistent so a nil CR can't
	// panic the deferred status closure here. o.CR is non-nil in
	// production (the reconciler fetches it before any status work),
	// but the guard makes Evaluate total.
	if o.CR != nil {
		for i := range conds {
			conds[i].ObservedGeneration = o.CR.Generation
		}
	}
	return conds, derivePhase(conds, o)
}

// findPriorCondition returns the prior status's condition of the
// given type, or nil. Used by latching evaluators (BootstrapComplete)
// and by the message-stability machinery — Evaluate writes a
// canonical message; if the prior condition already had it the caller
// preserves LastTransitionTime via meta.SetStatusCondition.
func findPriorCondition(o Observation, t string) *metav1.Condition {
	if o.CR == nil {
		return nil
	}
	for i := range o.CR.Status.Conditions {
		c := &o.CR.Status.Conditions[i]
		if c.Type == t {
			return c
		}
	}
	return nil
}
