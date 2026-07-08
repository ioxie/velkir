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

package events

// PodLabelReconciled is emitted when the operator patches the
// `velkir.ioxie.dev/role` label on a valkey pod. The "from→to"
// transition lives in the message body so dashboards can distinguish
// the initial primary-stamp (no prior value) from a steady-state
// flip (replica→primary, primary→replica). Sentinel-aware label
// flips on `+switch-master` join the same Reason; the distinction
// is encoded in the message, not in a separate Reason, so alert
// rules can group by Reason without having to re-classify per source.
//
// Informational; investigate if frequent under steady state.
const PodLabelReconciled Reason = "PodLabelReconciled"

// ReplicationGatePatched is emitted when Phase 8 patches the
// `velkir.ioxie.dev/replication-ready` condition on a pod's status.
// The message body carries the previous and new condition states
// plus the observed lag-bytes for replicas, so a True→False flip
// on a previously-Ready replica is distinguishable from the
// initial False→True transition during sync-up.
//
// Informational at the event level; the condition itself feeds
// kube-scheduler via the standard pod-readiness machinery.
const ReplicationGatePatched Reason = "ReplicationGatePatched"

// ReplicationGateCheckFailed is emitted when the operator's
// LagChecker can't reach a replica pod (TCP dial timeout,
// authentication failure, malformed RESP) and consequently can't
// patch the gate condition. Distinct from ReplicationGatePatched
// so dashboards can chart "we couldn't decide" separately from
// "we decided False (replica is behind)". Sustained
// ReplicationGateCheckFailed across a CR is the symptom the
// ValkeyReplicaUnreachable alert keys on.
//
// Alertable at sustained rate.
const ReplicationGateCheckFailed Reason = "ReplicationGateCheckFailed"

// ReplicationPrimaryLost is emitted when the operator observes a
// non-standalone CR with at least one valkey pod present but no
// pod currently labelled `velkir.ioxie.dev/role=primary`.
// Replication mode is the manual-failover shape: the operator
// does NOT auto-promote a replica when the primary is gone —
// the user is responsible for restoring the primary pod, or for
// migrating to `mode: sentinel` to gain HA. This event is the
// user-visible signal that the write surface is degraded; sentinel
// mode drives auto-recovery via the per-CR sentinel observer.
//
// Alertable on persistence — a primary-less replication CR has
// no write path even if `<cr>-ro` reads keep working.
const ReplicationPrimaryLost Reason = "ReplicationPrimaryLost"

// NoMasterAgreement is emitted by Phase 7 when the sentinel observer
// snapshot reports QuorumOK=true with a primary Addr that matches
// NO current valkey pod's PodIP — the cascading-wedge state where
// sentinels agree on a defunct master IP. Phase 7 suppresses
// role-label writes until the observer publishes an Addr matching
// a live pod. The same observation flips Ready=False + Degraded=True
// (reason=NoMasterAgreement) via the deferred status closure, so
// `kubectl describe valkey` surfaces the cause alongside this event.
// Alertable on sustained firing — every reconcile pass that observes
// the anomaly emits one event (subject to EventRecorder server-side
// dedup); a stuck NoMasterAgreement needs operator intervention.
const NoMasterAgreement Reason = "NoMasterAgreement"

// OrphanMasterDemoted is emitted (Normal) on the happy path of
// Phase 7a — a pod was running as master despite carrying the
// role=replica label, and `REPLICAOF` successfully landed to
// demote it back into the replication chain. The dual-kill
// scenario (master pod + operator pod killed simultaneously) is
// the canonical producer: the master pod's PVC survives, so the
// recreated pod boots back up as master before the new operator's
// Phase 7 stamps the label.
const OrphanMasterDemoted Reason = "OrphanMasterDemoted"

// OrphanMasterDemotionFailed is emitted (Warning) when REPLICAOF
// fails against an orphan master pod. The next reconcile retries;
// alertable on sustained firing — a stuck orphan is a data-loss
// hazard on the next failover.
const OrphanMasterDemotionFailed Reason = "OrphanMasterDemotionFailed"

// PrimaryClientsDropped is emitted (Normal) after the operator
// observes a pod's role label flip from `primary` to `replica` and
// successfully issues `CLIENT KILL TYPE normal SKIPME yes` on that
// pod. Closes pooled write-connections so clients reconnect via the
// Service abstraction and land on the new primary — without this,
// long-lived client pools (the common case for production
// Redis/Valkey libraries) stay ESTABLISHED on the now-replica and
// every write returns `-READONLY` until the application restarts.
// Carries the integer count of dropped connections; non-zero is
// the load-bearing audit trail.
const PrimaryClientsDropped Reason = "PrimaryClientsDropped"

// PrimaryClientsDropFailed is emitted (Warning) when `CLIENT KILL`
// against a demoted pod fails. Non-fatal — the label patch already
// succeeded, traffic is re-routed via the Service, but pooled
// clients that opened during the prior-primary window will see
// `-READONLY` until they reconnect on their own (typically tied to
// the application's connection-pool max-idle timeout).
const PrimaryClientsDropFailed Reason = "PrimaryClientsDropFailed"

// OrphanMasterDataDivergence is emitted (Warning) when an orphan
// master pod's `master_repl_offset` exceeds the elected master's
// at detection time. The diff bytes will be discarded by the
// upcoming REPLICAOF resync (the orphan re-syncs from the elected
// master, losing whatever writes it accepted in its prior life).
// The event is the audit trail for the data loss — without it,
// the loss is invisible to monitoring (the cluster appears healthy
// at every other layer).
const OrphanMasterDataDivergence Reason = "OrphanMasterDataDivergence"

// AllValkeyPodsDown is emitted when the cluster has at least one
// valkey data-plane pod present but none of them carries a role
// label (`velkir.ioxie.dev/role` ∈ {primary, replica}). Distinct from
// ReplicationPrimaryLost which keys on the primary specifically:
// AllValkeyPodsDown is the louder "Phase 7 ran but produced no
// role assignment for any pod" signal — typically a transient
// state during STS recreate, an in-progress label-strip during a
// failover step, or (in sentinel mode) the observer reporting
// QuorumOK=false so Phase 7 suppressed the relabel and no prior
// labels remained.
//
// Alertable on persistence — sustained firing means the operator
// has lost its grip on the role-labelling contract and the
// role-targeted Services have no working selector path.
const AllValkeyPodsDown Reason = "AllValkeyPodsDown"

// StaleReplicaEscapeDeleted is emitted (Warning) when, after a long
// sustained window with no primary-labeled pod, the operator deletes
// the least-fresh stale replica in a single dead lineage to force
// StatefulSet re-creation and break a stalled recovery. Fires only in a
// state the recovery-promotion path itself would admit (every replica
// link-down, pointing at one dead master, no reachable de-facto master,
// no dial failures this pass); the highest-applied-offset replica (the
// promotion candidate) is always preserved. The delete issues no
// sentinel command and performs no role relabel. Rate-bounded to one
// per staleReplicaEscapeCooldown per CR.
//
// Alertable on repeated firing — a cluster that keeps re-entering the
// escape has a recovery that never converges.
const StaleReplicaEscapeDeleted Reason = "StaleReplicaEscapeDeleted"
