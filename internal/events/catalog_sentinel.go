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

// SentinelSTSRestartNeeded is emitted when the sentinel StatefulSet's
// pod-template hash changes in a way that the OnDelete update
// strategy won't roll automatically (image, resources, the rendered
// sentinel.conf hash). Sentinel split-brain risk during a multi-pod
// restart means the operator does NOT auto-delete sentinel pods on
// template change today; this event surfaces the "manual delete (or
// SENTINEL RESET) needed" signal so an alert keying on it can page
// the operator-of-the-operator.
//
// Alertable; the PrometheusRule pack pairs this with a rule that
// fires on a non-zero rate over a 30-minute window.
const SentinelSTSRestartNeeded Reason = "SentinelSTSRestartNeeded"

// SentinelResetIssued is emitted (one per surviving sentinel pod)
// when the operator successfully ran `SENTINEL RESET *` against
// that pod. Surviving = the sentinels that did NOT just get
// replaced — the new pod is freshly bootstrapped and doesn't need
// RESET; running it would clear its newly-discovered peers.
// Triggered by sentinel-pod replacement detection or by the
// post-failover deferred-flush. Pairs with the ghost-sentinel-
// accumulation invariant in the sentinel package.
//
// Informational; one event per pod RESET so dashboards can chart
// per-pod success rate.
const SentinelResetIssued Reason = "SentinelResetIssued"

// SentinelResetFailed is emitted (one per failed pod) when the
// operator's SENTINEL RESET * round-trip to a specific surviving
// sentinel hits a dial-time, AUTH, write, or read-reply failure.
// The orchestration continues with the other survivors; one
// failed pod doesn't abort the whole RESET pass. Repeated firings
// against the same pod indicate a wedged sentinel that needs
// operator-of-the-operator attention.
//
// Alertable on sustained per-pod firing; a single transient
// failure resolves on the next pod-replacement event without
// further action.
const SentinelResetFailed Reason = "SentinelResetFailed"

// SentinelResetAllFailed is emitted exactly once per RESET pass
// where every survivor errored — escalation of the per-pod
// SentinelResetFailed events. The operator does NOT retry on its
// own; the next sentinel-pod replacement event drives the next
// attempt.
//
// Alertable.
const SentinelResetAllFailed Reason = "SentinelResetAllFailed"

// SentinelAuthApplied is emitted (one per sentinel pod) when the
// operator successfully ran `SENTINEL SET <masterName> auth-pass
// <password>` against that pod and verified by reading the
// auth-pass field back via `SENTINEL MASTER <masterName>`.
// Triggered after every `SENTINEL RESET *` (RESET clears the
// per-master runtime state including auth-pass) and on the
// startup safety net pass — keeps surviving sentinels in lock-
// step with the auth Secret in `auth.existingSecret`.
//
// Informational; one event per pod success so dashboards can
// chart per-pod propagation rate.
const SentinelAuthApplied Reason = "SentinelAuthApplied"

// SentinelAuthNotApplied is emitted (one per failed pod) when the
// operator's SENTINEL SET + verify round-trip to a specific
// sentinel pod hits a dial-time, AUTH, write, read-reply, or
// verification mismatch failure that survived the per-pod retry
// budget. The orchestration continues with the other pods; one
// failed pod doesn't abort the whole propagation pass.
//
// Alertable on sustained per-pod firing — a sentinel that can't
// be reached or whose auth-pass keeps failing verification will
// reject the operator's RPCs and break the failover triple-check.
const SentinelAuthNotApplied Reason = "SentinelAuthNotApplied"

// InitialSentinelReset is emitted on the operator startup safety
// net — the initial RESET pass that clears state the operator may
// have missed while not-leader. The RESET is gated by a probe (see
// internal/sentinel.RunInitialReset) — the event fires only when
// the gate detected an anomaly and chose to fire; a consistent
// post-restart probe is silent. Carries the per-pod outcome tally
// + the disagreement detail in the message body.
//
// Informational; the rate is bounded (once per CR per leader-
// acquire on actual anomaly), no alerting.
const InitialSentinelReset Reason = "InitialSentinelReset"

// SentinelMonitorIssued is emitted per surviving sentinel pod after
// a RESET-then-MONITOR rebind. Mirrors SentinelResetIssued's
// per-pod semantics: one event per pod, Normal level. The MONITOR
// step is what re-establishes the master pointer after a RESET wipes
// sentinel's in-memory state — without it, sentinels fall back to
// their on-disk sentinel.conf and may pin to a stale master IP.
const SentinelMonitorIssued Reason = "SentinelMonitorIssued"

// SentinelMonitorFailed is emitted per pod where SENTINEL MONITOR
// returned an error (dial-time, AUTH, or sentinel-reported error
// reply). Alertable on sustained firing — a sentinel that can't
// accept MONITOR will remain in a half-RESET state until the next
// recovery pass.
const SentinelMonitorFailed Reason = "SentinelMonitorFailed"

// SentinelTuningFailed is emitted per surviving sentinel pod where the
// post-MONITOR tuning restore (down-after-milliseconds, failover-
// timeout, parallel-syncs) returned an error. Without the restore the
// rebuilt sentinel silently reverts to Sentinel's hardcoded defaults
// (e.g. the 30s down-after) and lags the rest of the cluster on future
// failovers — alertable on sustained firing.
const SentinelTuningFailed Reason = "SentinelTuningFailed"

// SentinelStrandedRecovery is emitted (Normal) when the operator
// detects a stranded sentinel (peer-list empty) and fires
// RESET + MONITOR to bring it back into the cluster. One event
// per recovery pass, naming the stranded pods. The rebuilt
// sentinel rejoins the gossip ring once it subscribes to
// __sentinel__:hello on the newly-MONITORed master; this event
// is the dispatched-recovery audit trail.
const SentinelStrandedRecovery Reason = "SentinelStrandedRecovery"

// SentinelDeadMasterRepoint is emitted (Warning) when the operator
// detects gossiping sentinels whose monitored master address matches
// no live valkey pod and re-points them (REMOVE + MONITOR) at the
// resolved live master. This is the post-failover-storm wedge repair:
// the sentinels' master AND their entire known-replica set are
// corpses, so neither their own election nor gossip could ever
// converge them. Warning (not Normal) because reaching this state
// means the cluster served no writes until this pass — alertable on
// repeated firing.
const SentinelDeadMasterRepoint Reason = "SentinelDeadMasterRepoint"

// SentinelStaleEpochRepoint is emitted (Warning) when, WITHIN an already-armed
// re-point pass (the operator arms re-point only during the quorum-loss /
// corpse-aggregate window — a healthy quorum naming a live master runs no
// re-point at all), the operator detects a sentinel monitoring a
// LIVE-but-different pod that is proven strictly behind the config-epoch total
// order — with neither it nor the destination mid-election — and force-converges
// it onto the resolved master via REMOVE + MONITOR. The signature this repairs
// is a divergent sentinel pinned to a superseded primary at a stale
// config-epoch, a state neither its own election nor gossip resolves. Because
// the arming is scoped to the re-point pass, a minority sentinel lagging under
// an otherwise-healthy quorum is out of scope until the next quorum disruption
// re-arms the class. Warning (not Normal) because a lagging sentinel can
// mislead a failover vote; alertable on repeated firing.
const SentinelStaleEpochRepoint Reason = "SentinelStaleEpochRepoint"

// RecoveryPromotionInitiated is emitted (Warning) when the zero-master
// recovery election promotes a replica via REPLICAOF NO ONE: no live
// pod self-reports master, every replica's master host matches no
// live pod, and the sentinel quorum's monitored address is equally
// dead — the state where Sentinel's own election is impossible (its
// candidate set died with the master). The message names the chosen
// pod, its replication offset, and the dead address the quorum was
// monitoring. The label flip still waits for sentinel quorum
// agreement (Phase 7); this event marks the data-plane promotion
// only. Alertable — a cluster entering recovery promotion lost its
// entire sentinel-known topology at once.
const RecoveryPromotionInitiated Reason = "RecoveryPromotionInitiated"

// RecoveryPromotionFailed is emitted (Warning) when the REPLICAOF NO
// ONE issuance of a zero-master recovery election returns an error.
// The per-CR cooldown re-arms and the next reconcile re-evaluates —
// sustained firing means the elected candidate is unreachable on the
// data plane and an operator should look at pod networking.
const RecoveryPromotionFailed Reason = "RecoveryPromotionFailed"

// SentinelPeerLinkupStuck is emitted (Warning) once per stuck episode
// when a sentinel wiped by REMOVE + MONITOR is still classified with an
// empty peer-list after the configured number of consecutive
// no-progress surgeries — the operator's repair provably isn't
// converging, so it backs the surgery off exponentially instead of
// re-wiping at fixed cadence and surfaces this for manual intervention.
// Probable cause: the sentinel's auth-pass re-propagation fails
// verification (it can't AUTH against its master), or a NetworkPolicy
// blocks the __sentinel__:hello gossip channel — either prevents the
// rebuilt sentinel from ever re-learning its peers. Alertable.
const SentinelPeerLinkupStuck Reason = "SentinelPeerLinkupStuck"

// SentinelTopologyMismatch is emitted (Warning) once per active
// episode when the sentinel-reported peer count (num-other-sentinels)
// or replica count (num-slaves) has stayed below spec for the full
// debounce window. Observation-only: the operator issues no sentinel
// command to remediate — it reads the counts the pull tick already
// fetched and surfaces the sustained deficit. Distinct from
// SentinelPeerLinkupStuck, which is the remediation-failure signal of
// the stranded-sentinel surgery path; this is a routine hygiene signal
// decoupled from the incident ladder. Alertable on sustained firing.
const SentinelTopologyMismatch Reason = "SentinelTopologyMismatch"

// SentinelObserverReconnect is emitted when the per-CR sentinel
// observer goroutine has dropped its PSUBSCRIBE subscription to a
// specific sentinel pod and successfully re-established it (or
// promoted its warm standby to primary). Carried alongside the
// SentinelObserverReconnectsTotal counter so the timeline pairs
// with the per-pod metric series.
//
// Informational on individual emissions; the PrometheusRule pack
// alerts on a sustained reconnect rate per (cr, sentinel_pod),
// not on this event alone.
const SentinelObserverReconnect Reason = "SentinelObserverReconnect"

// SentinelPubsubMessageLost is emitted when the observer's
// PSUBSCRIBE read deadline expires without any keep-alive activity
// AND the in-band PING fails — the connection is presumed gone and
// the observer is about to close-and-reconnect. Distinct from
// SentinelObserverReconnect: this records the lossy edge (we may
// have missed a +switch-master between disconnect and the pull
// tick's catch-up); the reconnect Reason records the recovery edge.
//
// Alertable on rate; sustained loss means the pull-tick is the only
// signal the observer has, and split-brain detection latency grows.
const SentinelPubsubMessageLost Reason = "SentinelPubsubMessageLost"

// SentinelRollDeferred is emitted (Normal) when Phase 3 holds the
// sentinel StatefulSet apply because a failover is in flight or a
// valkey data-plane roll is active. The sentinel roll must yield to the
// election window — rolling a sentinel pod mid-failover can drop the
// surviving quorum below the threshold needed to complete the election.
// The reconcile requeues and re-checks once the critical section clears
// (bounded by the FailoverDispatch deadline escape, so the defer can
// never hold indefinitely).
//
// Informational per emission; frequent identical events aggregate via
// the recorder. A sustained-firing alert would indicate a failover or
// valkey roll that is not converging.
const SentinelRollDeferred Reason = "SentinelRollDeferred"
