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

// DualMasterSelfHealInitiated is emitted (Warning) when Phase 7a enters
// the bounded dual-master self-heal: no pod carries the role=primary
// label, the sentinel quorum view is unusable (NoMasterAgreement or a
// stale SentinelQuorum), a failover is in flight, and two or more pods
// report role=master. The message names the de-facto masters and the
// chosen survivor (highest master_repl_offset). The self-heal only ever
// demotes losers; it never promotes without quorum.
//
// Alertable on sustained firing — a cluster that keeps re-entering
// self-heal has an unresolved split that quorum recovery isn't clearing.
const DualMasterSelfHealInitiated Reason = "DualMasterSelfHealInitiated"

// DualMasterSelfHealDemoted is emitted (Normal) for each losing de-facto
// master the self-heal demotes via REPLICAOF + CLIENT KILL onto the
// elected survivor. The message carries the loser pod, its offset, and
// the survivor it was re-pointed at — the audit trail for the writes the
// resync discards.
const DualMasterSelfHealDemoted Reason = "DualMasterSelfHealDemoted"

// DualMasterSelfHealDeferred is emitted (Warning) when the self-heal
// refuses to act: the two highest de-facto-master offsets are within the
// safety epsilon (no unambiguous survivor — demoting either could lose
// the more-advanced data), the survivor's epoch is below the failover
// fence (PreStripEpoch), the lineage cross-check against PreStripAddr
// fails, or the per-CR cooldown / max-attempts bound is in effect. The
// message names the reason; the next reconcile re-evaluates.
//
// Alertable on persistence — a sustained deferral means the operator
// cannot safely resolve the split and an operator must intervene.
const DualMasterSelfHealDeferred Reason = "DualMasterSelfHealDeferred"

// DualMasterObserved is emitted (Warning) when two or more live pods
// self-report role:master in a shape where no demotion path is admitted:
// the sentinel Phase 11 recovery survey OUTSIDE an operator failover
// section (no fencing epoch to demote against), or the replication
// no-labeled-primary observation scan (no elected primary to fence
// against). Before this event existed, nothing surfaced at all. The
// message names the de-facto masters and their master_repl_offsets; the
// paired Degraded reason (DualMasterDivergence) and the
// valkey_dual_master_observed gauge carry the same signal. Edge-gated on
// the accumulated union of de-facto-master pod names for the current
// episode; accumulation is time-independent, and the episode ends only
// when a complete scan sees at most one master, the observation ages
// out, or the tracker is pruned. A persistent split therefore pages once
// at episode start and re-pages only when a genuinely-new pod joins —
// role churn across scans and slow scan cadences alike never re-fire it.
//
// Alertable immediately — active data divergence: writes are landing
// on more than one primary and every diverging write on the eventual
// loser will be discarded by the resync that resolves the split.
const DualMasterObserved Reason = "DualMasterObserved"
