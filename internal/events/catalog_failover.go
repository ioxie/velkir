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

// FailoverSuppressed is emitted when the operator's failover-decision
// triple-check (CKQUORUM ∧ +odown consensus ∧ Pod.Ready=False)
// blocks an otherwise-pending failover because at least one of the
// three signals disagrees. The most common case is sentinel-side
// signals saying the primary is down while kubelet still reports
// Pod.Ready=True — almost always a sentinel-network or
// sentinel-probe issue, not a real primary failure: the
// disagreement is in the sentinel layer, not the data plane. The
// event message names the specific
// signal that disagreed (NoQuorum, NoODownConsensus, PodReady,
// NoObserverSnapshot) so on-call can disambiguate without re-running
// the operator's logic by hand. The +odown / Pod.Ready=True
// mismatch is the canonical sentinel-vs-kubelet disagreement signal
// that the operator must refuse to act on.
//
// Informational; persistent firing on the same CR points at sentinel
// or pod-readiness-probe instability that warrants investigation.
const FailoverSuppressed Reason = "FailoverSuppressed"

// FailoverInitiated is emitted when the rollout state machine issues
// SENTINEL FAILOVER as part of RolloutPrimary → FailoverInFlight.
// Pairs with FailoverSucceeded / FailoverSucceededWithTimeout /
// FailoverAborted / FailoverStalled at the closing edge.
//
// Informational; one event per primary rollout.
const FailoverInitiated Reason = "FailoverInitiated"

// FailoverSucceeded is emitted when the sentinel observer reports
// +failover-end and the new primary address has stabilised
// (FailoverInFlight → RolloutComplete). The happy-path closing
// edge of FailoverInitiated.
//
// Informational.
const FailoverSucceeded Reason = "FailoverSucceeded"

// FailoverSucceededWithTimeout is emitted on +failover-end-for-timeout —
// sentinels declared the failover successful but at least one replica
// did not reconfigure within the configured failover-timeout.
// The CR enters RolloutComplete with a soft-warn Degraded condition
// (reason=ReplicasNotReconfigured) so the user knows about the
// degraded replica without blocking rollout completion.
//
// Alertable on the paired Degraded condition, not on this Reason
// alone.
const FailoverSucceededWithTimeout Reason = "FailoverSucceededWithTimeout"

// FailoverAborted is emitted when the sentinel observer reports any
// `-failover-abort-*` pub/sub message during FailoverInFlight.
// The state machine moves to Degraded; recovery requires human
// intervention (sentinel logs typically explain why the failover
// was aborted: candidate replica disappeared, quorum lost
// mid-failover, etc.).
//
// Alertable.
const FailoverAborted Reason = "FailoverAborted"

// FailoverStalled is emitted when the wall-clock backstop
// (failover-timeout + 30s) elapses during FailoverInFlight without
// any +failover-end pub/sub message arriving. Distinct from
// FailoverSucceededWithTimeout because no closing event was
// observed at all — the failover may have completed silently or may
// be genuinely stuck.
//
// Alertable; investigate sentinel pub/sub health and the candidate
// replica's master_link_status.
const FailoverStalled Reason = "FailoverStalled"

// UnexpectedFailover is emitted when the sentinel observer reports
// +switch-master while the rollout state machine is in Steady — i.e.,
// a failover happened that the operator did not initiate.
// The machine moves to FailoverInFlight to wait for +failover-end
// and converge state, but the event records that something else
// (a sentinel-side decision, a manual `SENTINEL FAILOVER`, a primary
// pod loss) drove the failover.
//
// Alertable on a non-zero rate — frequent unexpected failovers
// suggest the primary pod is unstable.
const UnexpectedFailover Reason = "UnexpectedFailover"

// SplitBrainDetected is emitted when the Phase 8 split-brain
// detector trips: ≥2 pods report `INFO replication role:master`
// for the same CR. The rollout state machine moves any in-rollout
// state to Degraded; the operator suppresses SENTINEL MONITOR /
// RESET / SET issuance while in this state to avoid re-stamping
// the wrong primary.
//
// Alertable; one of the load-bearing safety invariants.
const SplitBrainDetected Reason = "SplitBrainDetected"
