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

// ScaleRefused is emitted when the operator declines to apply a
// scale change because the new replica count would remove the
// pod currently labelled `velkir.ioxie.dev/role=primary`. Distinct
// from `ScaleDeferred` (mid-rollout temporal deferral) — this
// is structural: the user asked for an unsafe shape and
// the operator left the STS at its prior replica count to
// preserve the primary. Recovery: failover first to relocate the
// primary to a lower ordinal, or restore the higher replica
// count and try again.
//
// Alertable on persistent firing (a stuck-spec CR).
const ScaleRefused Reason = "ScaleRefused"

// ScalePrecheckFailed is emitted when the operator can't read the
// existing StatefulSet to compute the scale-down safety check
// (e.g. apiserver 5xx, dial timeout, RBAC drift). The reconcile
// is errored so controller-runtime retries with backoff; the STS
// apply is skipped this pass — no writes can land while the safety
// check is blind. Distinct from `ScaleRefused` so dashboards can
// chart "I don't know yet" vs "I know and refused".
//
// Alertable on persistent firing (likely a real apiserver or
// RBAC issue, not a transient blip).
const ScalePrecheckFailed Reason = "ScalePrecheckFailed"

// ScaleDeferred is emitted when the operator declines to apply a
// replica-count change because a rollout / failover is currently
// in flight. The desired replica count is held back; the STS keeps
// its prior count until the FSM returns to Steady, at which point
// the next reconcile applies the deferred scale normally. Distinct
// from `ScaleRefused` — that's the structural primary-removal
// guard (regardless of FSM state); this one is temporal (any
// scale, any direction, but only while not Steady).
//
// Informational; user-edits-during-rollout are expected and the
// deferral message names the in-flight FSM state.
const ScaleDeferred Reason = "ScaleDeferred"

// PodRolledForConfig is emitted when Phase 9 deletes a stale-
// revision pod to let the StatefulSet controller recreate it
// against the latest pod-template revision (driven by the
// `velkir.ioxie.dev/config-hash` annotation that derives from the
// rendered valkey.conf + init script). One event per pod
// deletion; carries the target pod name and the new revision
// so dashboards can chart rollout progress per CR.
//
// Informational; the rollout-stuck alert lives at the absence-of
// level (no PodRolledForConfig + sustained revision drift), not
// at the per-emission level.
const PodRolledForConfig Reason = "PodRolledForConfig"

// RolloutDeferred is emitted when the only remaining stale pod is
// the primary — the master-aware rolling path drives the primary
// rotation via failover-then-recreate, so a steady-state RolloutDeferred
// is the bookmark for "replica side done, primary leg pending."
// Fires once per rollout when the operator has finished the replica-
// side recreations and the primary is still on the old revision;
// suppressed thereafter to avoid alert noise on a steady-state
// stuck-on-primary CR.
//
// Informational; the rollout-bypass alert keys on a sustained gap
// between rollout-started and rollout-complete that includes
// this Reason in the timeline.
const RolloutDeferred Reason = "RolloutDeferred"

// RolloutStarted is emitted when the master-aware rolling-update
// state machine (orchestration package) leaves Steady on a detected
// pod-template / config / annotation rollout trigger. Pairs with
// RolloutCompleted at the closing edge so a Grafana panel can chart
// rollout duration per CR.
//
// Informational; the PrometheusRule pack alerts on a sustained gap
// between RolloutStarted and RolloutCompleted, not on this event
// alone.
const RolloutStarted Reason = "RolloutStarted"

// RolloutCompleted is emitted when the rollout state machine reaches
// Steady from RolloutComplete after the old primary's replacement
// pod is Ready + replication-healthy. The bookend to RolloutStarted.
//
// Informational; absence-of-emission within the rollout-stuck SLO is
// the alert-worthy signal, not the emission itself.
const RolloutCompleted Reason = "RolloutCompleted"

// QuorumLost is emitted when the rollout FSM enters
// StateDegradedQuorumLost because CKQUORUM has been NOQUORUM
// continuously for ≥ 60s. Distinct from RolloutAbortedQuorumLost:
// the latter fires on the immediate in-rollout abort when quorum
// drops mid-step; QuorumLost fires on the sustained-NOQUORUM
// threshold escalation that absorbs from any state including
// Steady, Bootstrap, and even already-Degraded.
//
// While in StateDegradedQuorumLost the operator suppresses all
// SENTINEL MONITOR / SENTINEL RESET / SENTINEL SET issuance
// (observation-only). The state exits via QuorumReached once the
// 2-poll CKQUORUM-OK hysteresis passes.
//
// Alertable; persistent firing on the same CR points at sentinel
// ensemble instability (sentinel pod placement, sentinel network
// partition, sentinel quorum-config drift) that warrants
// investigation.
const QuorumLost Reason = "QuorumLost"

// QuorumReached is emitted when the rollout FSM exits
// StateDegradedQuorumLost after observing 2 consecutive CKQUORUM-OK
// polls — the hysteresis cutoff exists because a transient blip
// during recovery would otherwise immediately resume work that the
// next NOQUORUM tick would re-suspend. The reconciler dispatches
// the resume target (Steady or RolloutPending) via
// status.rollout.suspendedFrom.
//
// Informational; pairs with the prior QuorumLost in the timeline.
const QuorumReached Reason = "QuorumReached"

// RolloutAbortedQuorumLost is emitted when the rollout state machine
// transitions from a mid-rollout state (RolloutPending or
// RolloutReplicas) to Degraded because CKQUORUM returned NOQUORUM
// or split-brain was detected. The rollout is suspended; the machine
// resumes via RolloutResumed when quorum recovers.
//
// Alertable on persistence — repeated quorum loss during rollouts
// suggests sentinel-side instability worth investigating.
const RolloutAbortedQuorumLost Reason = "RolloutAbortedQuorumLost"

// RolloutResumed is emitted when the rollout state machine returns
// from Degraded to RolloutPending — quorum recovered while a rollout
// was in flight, and the machine picks up where the suspension
// happened (status.rollout.suspendedFrom drives the resume target).
//
// Informational; pairs with the prior RolloutAbortedQuorumLost in
// the timeline.
const RolloutResumed Reason = "RolloutResumed"

// RolloutStalled is emitted when the per-pod readiness watchdog
// (status.rollout.masterAware) deadline elapses without the
// replacement pod reaching Ready. Drives Degraded=True with
// reason=RolloutStalled so the user sees both a one-shot event
// and a durable condition. The watchdog is disarmed in the same
// reconcile pass so the event fires exactly once per expiry;
// future Arm + expiry cycles produce fresh events.
//
// Alertable; the canonical "rollout step is stuck" surface —
// sustained firing across multiple CRs is the signal that the
// rolling-update path is unhealthy fleet-wide.
const RolloutStalled Reason = "RolloutStalled"

// ReplicasRolled is emitted when the rollout state machine finishes
// the per-replica delete-and-wait phase (RolloutReplicas →
// RolloutPrimary). Marks the boundary between the cheap part of a
// rollout (replicas: no failover needed) and the expensive part
// (primary: requires SENTINEL FAILOVER).
//
// Informational; useful for rollout-progress dashboards.
const ReplicasRolled Reason = "ReplicasRolled"

// PrimaryRolloutBlocked is emitted when the rollout state machine
// can't transition from RolloutPrimary to FailoverInFlight because
// either CKQUORUM is failing or no candidate replica is healthy
// enough to receive the failover. The machine moves to Degraded;
// the rollout resumes on quorum + candidate recovery.
//
// Alertable on persistence.
const PrimaryRolloutBlocked Reason = "PrimaryRolloutBlocked"

// SpecChangeDeferred is emitted when the user mutates the CR spec
// (e.g., changes spec.replicas or spec.valkey.image) during an
// active RolloutReplicas pass. The machine continues the
// in-flight rollout to a clean stopping point; the deferred change
// is applied on the next reconcile after RolloutComplete returns
// to Steady.
//
// Informational; users see this when a hot-edit lands during a
// rollout.
const SpecChangeDeferred Reason = "SpecChangeDeferred"

// NoSuitableReplica is emitted when the operator's primary-rollout
// path needs a replica candidate to receive the failover but no
// candidate qualifies (none has `master_link_status=up` AND
// replication-offset delta below the lag threshold). The state
// machine moves to Degraded; the rollout resumes when at least
// one replica becomes lag-compliant. Distinct from
// `PrimaryRolloutBlocked` which is the broader "primary roll
// can't proceed" reason — `NoSuitableReplica` is the specific
// candidate-not-found subcase.
//
// Alertable on persistence — sustained firing means replication is
// genuinely lagging across the whole replica set, which warrants
// operator-of-the-operator investigation (large-write spike,
// replica resource pressure, network partition).
const NoSuitableReplica Reason = "NoSuitableReplica"
