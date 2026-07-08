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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValkeyStatus is the observed state.
type ValkeyStatus struct {
	// Conditions reflect the current state of the Valkey resource.
	// Standard Kubernetes condition semantics apply. Notable
	// operator-specific condition types:
	//
	//   - Ready:               True when the CR is fully reconciled
	//                          and all replicas are observed Ready.
	//   - Available:           True when at least one replica is
	//                          observed Ready (writes routable).
	//   - Progressing:         True while the operator is actively
	//                          making changes (rollout / bootstrap).
	//   - Reconciled:          True when the most recent reconcile
	//                          pass returned no error.
	//   - Degraded:            True when an operational signal blocks
	//                          forward progress (split-brain, rollout
	//                          stalled, HA floor not met, quorum loss).
	//   - ReplicationHealthy:  True when replication-mode replicas
	//                          are connected and within the lag floor.
	//   - BootstrapComplete:   True once the seed-bootstrap latched.
	//   - PrimaryConfirmed:    True when a strict majority of fresh
	//                          SentinelQuorum records agree on the
	//                          observed primary pod. Sentinel mode
	//                          only. Unknown before the first
	//                          SentinelQuorum write lands.
	//   - QuorumLost:          True when the sentinel-quorum
	//                          suppression gate is active. The gate
	//                          flips True only after ≥ the configured
	//                          loss threshold (default 60s) of
	//                          continuously-reachable-but-NOQUORUM
	//                          observations — transient sentinel
	//                          disagreement on a single poll does
	//                          NOT flip this True. The gate flips
	//                          False after the configured recovery
	//                          hysteresis (default 2 consecutive OK
	//                          polls). Unknown when no fresh
	//                          SentinelQuorum observations exist
	//                          (e.g., immediately after CR creation).
	//                          Sentinel mode only; False for
	//                          standalone / replication.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase is a single cosmetic string derived from Conditions per
	// the API conventions doc. Surfaces in `kubectl get` columns and
	// dashboards; the operator's own logic always reads Conditions
	// directly. Values: Pending | Progressing | Available | Ready |
	// Degraded | Paused.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Rollout tracks the operator-driven rollout substate. Populated
	// only while a rollout (PVC resize, config-change, image bump) is
	// in flight; cleared by the operator once the substate machine
	// reaches its terminal phase.
	// +optional
	Rollout *RolloutStatus `json:"rollout,omitempty"`

	// PrimaryPod is the name of the pod a strict majority of fresh
	// SentinelQuorum records agree is the current primary, derived
	// each reconcile by the operator-side aggregator. Empty when no
	// majority is reached, when all SentinelQuorum records are stale
	// beyond the freshness window, or when the CR is not in sentinel
	// mode. Pairs with Conditions[type=PrimaryConfirmed]: True when
	// PrimaryPod is non-empty and the majority condition was met,
	// False otherwise.
	// +optional
	PrimaryPod string `json:"primaryPod,omitempty"`
}

// RolloutStatus carries the substate of any operator-driven rollout in flight.
// Currently only the PVC-resize substate is wired; future rollout flavors
// (config-change, image bump, primary failover) attach their own fields here.
type RolloutStatus struct {
	// PVCResize tracks the orphan-delete-recreate substate for the PVC
	// resize flow. Set on resize intent detection; cleared once the flow
	// reaches Verified.
	// +optional
	PVCResize *PVCResizeStatus `json:"pvcResize,omitempty"`

	// MasterAware tracks the per-pod readiness watchdog for the
	// master-aware rolling-update flow. Set when the operator deletes a
	// data-plane pod during a rollout step (replica or primary); cleared
	// when the replacement pod becomes Ready or the watchdog declares
	// the rollout stalled. Drives the RolloutStalled event + transition
	// to Degraded when the deadline passes without the replacement
	// reaching Ready.
	// +optional
	MasterAware *MasterAwareRolloutStatus `json:"masterAware,omitempty"`

	// AuthRotation tracks the auth-Secret hot-rotation substate. Set
	// when an auth-Secret content change is detected; transitions
	// through InProgress → Succeeded (then settles to Idle) or, on
	// partial-success, into Failed (rotation reverted) or Partial
	// (revert also failed; cluster in mixed-credential state and
	// requires operator intervention). Empty / Idle when no rotation
	// is in flight.
	// +optional
	AuthRotation *AuthRotationStatus `json:"authRotation,omitempty"`

	// SuspendedFrom records the rollout state in flight at the moment
	// the CR transitioned into Degraded on quorum loss. Stamped on the
	// abort edge (RolloutPending → Degraded stamps "RolloutPending";
	// RolloutReplicas → Degraded stamps "RolloutReplicas") and cleared
	// on the recovery edges out of Degraded (Degraded → Steady or
	// Degraded → RolloutPending). Drives the rollout-state-machine
	// dispatch between the DegradedResolved and RolloutResumed events,
	// so the audit trail can distinguish "abort was from a pending
	// rollout, recovery resumed it cleanly" from "abort was from steady
	// state, recovery returned cleanly". Empty when no rollout was in
	// flight at the abort, or after recovery has cleared it.
	// +optional
	// +kubebuilder:validation:Enum=RolloutPending;RolloutReplicas
	SuspendedFrom *string `json:"suspendedFrom,omitempty"`

	// FailoverDispatch records a strip-then-`SENTINEL FAILOVER` that was
	// in flight on the operator-driven primary-rollout path. The operator
	// strips `role=primary` off the outgoing primary before issuing the
	// failover (so the `<cr>` Service stops routing writes during the
	// election); this marker is written durably right before that strip,
	// so an operator crash mid-election self-heals deterministically
	// instead of relying on observer-snapshot timing. The next operator's
	// reconcile rehydrates its in-memory failover-suppression latch from
	// this marker and does not re-stamp `role=primary` on the pre-strip
	// primary while the election is still in progress. Cleared once the
	// observer confirms the new primary (the snapshot address moves off
	// the pre-strip address) or the marker deadline passes. Empty when no
	// failover is in flight.
	// +optional
	FailoverDispatch *FailoverDispatchStatus `json:"failoverDispatch,omitempty"`
}

// FailoverDispatchStatus is the durable mirror of the operator's
// in-memory failover-in-flight latch. It records that
// runPrimaryRolloutDispatch stripped `role=primary` off the outgoing
// primary and dispatched `SENTINEL FAILOVER`, written to status BEFORE
// the strip so an operator crash between the strip and the observer's
// `+switch-master` does not lose the in-flight record. On the next
// reconcile the suppression latch is rehydrated from this marker, so
// `role=primary` is not re-stamped on the pre-strip primary mid-election
// — which would reopen the write-to-old-primary window the strip-first
// ordering exists to close.
type FailoverDispatchStatus struct {
	// PreStripAddr is the observer-reported primary `ip:port` captured
	// the instant before the `role=primary` strip. While the live
	// observer snapshot still reports this address the election has not
	// completed and role re-stamping stays suppressed; the marker clears
	// when the snapshot moves off it.
	//
	// The pattern accepts an IPv4 or bracketed-IPv6 host with a numeric
	// port — the operator writes `net.JoinHostPort` form (`10.0.0.1:6379`
	// or `[fe80::1]:6379`) — so it rejects an obviously-malformed
	// hand-edited value at admission rather than silently no-op'ing on
	// the deadline backstop, without rejecting a valid IPv6 address.
	// +optional
	// +kubebuilder:validation:Pattern=`^(\[[0-9a-fA-F:]+\]|([0-9]{1,3}\.){3}[0-9]{1,3}):[0-9]{1,5}$`
	PreStripAddr string `json:"preStripAddr,omitempty"`

	// Deadline bounds how long the durable suppression holds. Sized at
	// the in-memory latch TTL (the sentinel failover-timeout default plus
	// an observer-tick allowance). Past it a wedged failover no longer
	// suppresses re-derivation — the next reconcile clears the marker and
	// re-derives the primary from observation.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`

	// PreStripEpoch is the monotonic fencing token: the Sentinel
	// config-epoch the operator observed the instant before stripping
	// role=primary, recorded here at strip time. An observation or action
	// carrying a LOWER epoch is a stale view of an election this dispatch
	// already superseded and is refused — it must not exit the critical
	// section or seed a primary from a pre-failover snapshot. Best-effort:
	// zero when the config-epoch could not be parsed at strip time, in
	// which case the fence is inert (no observation is refused).
	// +optional
	PreStripEpoch int64 `json:"preStripEpoch,omitempty"`
}

// MasterAwareRolloutStatus is the persisted watchdog substate for the
// master-aware rolling-update flow. The operator stamps WaitingForPod
// + Deadline immediately after deleting a data-plane pod; each
// subsequent reconcile checks the deadline against now() and emits
// RolloutStalled + sets Degraded=True reason=RolloutStalled if the pod
// hasn't become Ready by then. The per-pod replicaReadyTimeoutSeconds
// floor is enforced in the validating webhook (60s) and ceiling
// (3600s); the operator clamps to those bounds defensively.
type MasterAwareRolloutStatus struct {
	// WaitingForPod is the name of the pod the operator is currently
	// waiting on. Empty means no pod-deletion is in flight (so the
	// watchdog is inactive even if Deadline is set, which is a transient
	// post-clear state). The pod name is sufficient for the watchdog
	// because the StatefulSet recreates pods at the same name.
	// +optional
	WaitingForPod string `json:"waitingForPod,omitempty"`

	// Deadline is the wall-clock cutoff for the replacement pod to
	// reach Ready. Set to (DeletedAt + spec.rollout.replicaReadyTimeoutSeconds)
	// at deletion time. A nil Deadline means the watchdog is inactive
	// (paired with WaitingForPod == "").
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`

	// DeletedAt timestamps the pod-deletion that armed this watchdog.
	// Surfaced for kubectl get observability and for log-line context;
	// the deadline computation reads only Deadline.
	// +optional
	DeletedAt *metav1.Time `json:"deletedAt,omitempty"`
}

// PVCResizeStatus is the persisted substate for the PVC resize flow.
//
// Phases progress linearly:
//
//	Detected → Validated → StsOrphaned → PVCsPatched → StsRecreated → Verified
//
// Any substate may transition to Aborted on a terminal failure (shrink reject,
// missing allowVolumeExpansion, CSI 422, 10-minute substate stall). On Aborted
// the next reconcile retries from Detected after a backoff that scales with
// Attempt and is capped at one hour.
type PVCResizeStatus struct {
	// Phase is the current substate.
	// +kubebuilder:validation:Enum=Detected;Validated;StsOrphaned;PVCsPatched;StsRecreated;Verified;Aborted
	// +optional
	Phase string `json:"phase,omitempty"`

	// Attempt counts how many times the resize flow has restarted from
	// Detected after an Aborted state. Drives the inter-attempt backoff
	// (exponential, capped at 1h).
	// +optional
	Attempt int32 `json:"attempt,omitempty"`

	// StartedAt timestamps the first transition into Detected for the
	// current attempt.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// LastTransitionAt timestamps the most recent phase change. Used by
	// the per-substate 10-minute stall guard.
	// +optional
	LastTransitionAt *metav1.Time `json:"lastTransitionAt,omitempty"`

	// LastStuckEventAt timestamps the most recent PVCExpansionStuck
	// event emission for the current stall window. Used to dedup the
	// stuck-event emission across reconciles: once the per-substate
	// stall threshold (10 minutes since LastTransitionAt) is crossed,
	// the dispatcher emits one event and stamps this field, then
	// suppresses further emissions until the substate moves (which
	// refreshes LastTransitionAt and so resets the dedup window).
	// Without this dedup the event would re-fire every requeue (~5s)
	// once stalled, drowning the events stream.
	// +optional
	LastStuckEventAt *metav1.Time `json:"lastStuckEventAt,omitempty"`

	// Message is a short human-readable description of the current phase,
	// including the reason for the most recent Aborted transition.
	// +optional
	Message string `json:"message,omitempty"`
}

// PVCResizePhase is the type-safe enumeration of PVCResizeStatus.Phase
// values. Mirrors the +kubebuilder Enum tag on the field so controller
// code can reference phases without stringly-typed comparisons.
type PVCResizePhase string

const (
	PVCResizePhaseDetected     PVCResizePhase = "Detected"
	PVCResizePhaseValidated    PVCResizePhase = "Validated"
	PVCResizePhaseStsOrphaned  PVCResizePhase = "StsOrphaned"
	PVCResizePhasePVCsPatched  PVCResizePhase = "PVCsPatched"
	PVCResizePhaseStsRecreated PVCResizePhase = "StsRecreated"
	PVCResizePhaseVerified     PVCResizePhase = "Verified"
	PVCResizePhaseAborted      PVCResizePhase = "Aborted"
)

// AuthRotationStatus is the persisted substate for the auth-Secret
// hot-rotation flow. The reconciler edge-detects a Secret content
// change by comparing the current Secret's password-bytes hash against
// the most recently observed hash stored here; on first observation
// it stamps the hash and stays Idle. On a real change it walks the
// data plane (replicas first, master last) via internal/valkey.RotateAuth
// and transitions through InProgress → Succeeded; on partial failure
// it reverts the successfully-updated pods and transitions to Failed
// (revert succeeded, cluster back on the prior credential) or Partial
// (revert also failed; cluster in mixed-credential state, requires
// human intervention). The hash compare is over the Secret content
// (the `password` data key) — resourceVersion alone false-triggers
// on metadata-only edits.
type AuthRotationStatus struct {
	// Phase is the current substate.
	// +kubebuilder:validation:Enum=Idle;InProgress;Succeeded;Failed;Partial
	// +optional
	Phase string `json:"phase,omitempty"`

	// ObservedSecretHash is the hex-encoded SHA-256 of the auth Secret
	// password content the operator most recently observed (and, in
	// the Succeeded case, fully applied across the data plane). The
	// next reconcile compares the current Secret's hash against this
	// to edge-detect a content change. Empty until the first
	// observation. Hashing the password content (not the Secret bytes
	// or resourceVersion) avoids false triggers on metadata-only edits
	// and lets the field persist across operator restarts without
	// storing the password itself.
	//
	// Privacy caveat: SHA-256 is not a password hash (no salt, no
	// KDF). For strong, high-entropy passwords this hash is brute-
	// force resistant — it serves only as a content-change marker.
	// For weak passwords (low entropy, dictionary-derived) the hash
	// is reversible via offline brute-force or rainbow tables.
	// Status is world-readable to anyone with `get/list valkeys` on
	// the namespace, so weak passwords leak through this field.
	// Operators are expected to use auth Secrets meeting the
	// project's MinTokenLen entropy gate; the redaction registry
	// emits a warning event on shorter passwords.
	// +optional
	ObservedSecretHash string `json:"observedSecretHash,omitempty"`

	// StartedAt timestamps the most recent transition into InProgress.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// LastTransitionAt timestamps the most recent phase change.
	// +optional
	LastTransitionAt *metav1.Time `json:"lastTransitionAt,omitempty"`

	// Message is a short human-readable description of the current
	// phase, including (for Failed/Partial) a summary of the affected
	// endpoints so an operator can map the substate back to specific
	// pods without consulting events.
	// +optional
	Message string `json:"message,omitempty"`
}

// AuthRotationPhase is the type-safe enumeration of
// AuthRotationStatus.Phase values. Mirrors the +kubebuilder Enum tag
// on the field so controller code can reference phases without
// stringly-typed comparisons.
type AuthRotationPhase string

const (
	// AuthRotationPhaseIdle means no rotation is in flight. Either no
	// content change has been observed since the most recent
	// successful rotation, or the operator has not yet observed an
	// initial Secret content. The substate may be absent entirely
	// instead of being explicitly stamped Idle; helpers treat empty
	// and Idle interchangeably.
	AuthRotationPhaseIdle AuthRotationPhase = "Idle"
	// AuthRotationPhaseInProgress is set the moment a content change
	// is detected and the operator begins driving RotateAuth across
	// the data plane.
	AuthRotationPhaseInProgress AuthRotationPhase = "InProgress"
	// AuthRotationPhaseSucceeded is set when every data-plane pod
	// accepted the new credential (replicas first, master last).
	// Settles back to Idle on the next reconcile after the operator
	// emits the SecretRotated event and updates the cached old
	// password.
	AuthRotationPhaseSucceeded AuthRotationPhase = "Succeeded"
	// AuthRotationPhaseFailed is set when at least one pod did not
	// accept the new credential AND the operator successfully reverted
	// every successfully-updated pod back to the prior credential. The
	// cluster is on the old password; a fresh Secret content edit can
	// re-drive the rotation.
	AuthRotationPhaseFailed AuthRotationPhase = "Failed"
	// AuthRotationPhasePartial is set when at least one pod did not
	// accept the new credential AND the revert also failed on at
	// least one of the successfully-updated pods. The cluster is in
	// mixed-credential state; the affected endpoints are named in
	// the SecretRotationPartial event message and the substate
	// Message field, so the operator-of-the-operator can re-align
	// them manually.
	AuthRotationPhasePartial AuthRotationPhase = "Partial"
)
