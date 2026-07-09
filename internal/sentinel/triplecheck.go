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

package sentinel

import (
	"fmt"
	"time"
)

// TripleCheckReason names which signal blocked a failover decision.
// It is part of the FailoverDecision returned from EvaluateTripleCheck
// so callers can branch on the specific cause (event reason, log,
// status condition) without parsing the human-readable message.
type TripleCheckReason string

const (
	// TripleCheckReasonOK is the only non-blocking reason: all three
	// signals agree.
	TripleCheckReasonOK TripleCheckReason = ""
	// TripleCheckReasonNoSnapshot fires when the observer has not
	// published its first ObservedPrimary yet (boot race between the
	// reconciler's first pass and the observer's first poll tick).
	// The decision is effectively "unknown" — the operator must wait
	// for the observer to converge before acting.
	TripleCheckReasonNoSnapshot TripleCheckReason = "NoObserverSnapshot"
	// TripleCheckReasonNoQuorum fires when CKQUORUM did not return OK
	// on enough sentinels (Snapshot.Primary.QuorumOK == false). This
	// is the entry signal for the Degraded+QuorumLost FSM branch.
	TripleCheckReasonNoQuorum TripleCheckReason = "NoQuorum"
	// TripleCheckReasonNoODownConsensus fires when fewer than `quorum`
	// sentinels have a non-stale +odown for the target. CKQUORUM may
	// still be OK (sentinels are reachable to each other) but they
	// have not agreed the primary is objectively-down.
	TripleCheckReasonNoODownConsensus TripleCheckReason = "NoODownConsensus"
	// TripleCheckReasonPodReady fires when sentinels say the primary
	// is down but kubelet says the pod is Ready. This is almost
	// always a sentinel-network or sentinel-probe issue
	// (tilt-adjacent), not a real primary failure — the operator
	// must NOT initiate a failover. Sentinel pub/sub alone is
	// never trusted; without kubelet's Pod.Ready=False to
	// corroborate, the +odown stream may be the sentinel layer's
	// view of a transient probe issue rather than the primary
	// actually being down. Pairs with the FailoverSuppressed event
	// reason at the call site.
	TripleCheckReasonPodReady TripleCheckReason = "PodReady"
	// TripleCheckReasonStaleSnapshot fires when the observer HAS
	// published a snapshot but its ObservedPrimary.UpdatedAt is older
	// than MaxSnapshotAge: the pull-tick (poll) goroutine is wedged
	// while pub/sub keeps re-publishing the prior QuorumOK forward
	// (the +switch-master path carries prev.Quorum). The data can't
	// be trusted, so this is NoSnapshot-class — the operator defers
	// exactly as for a missing snapshot rather than authorizing a
	// failover on quorum no live pull confirmed. Kept distinct from
	// NoSnapshot so on-call can tell "observer never started" from
	// "observer's pull tick stalled".
	TripleCheckReasonStaleSnapshot TripleCheckReason = "StaleObserverSnapshot"
)

// TripleCheckInputs bundles the three signals an operator-initiated
// failover decision requires. All three must agree before the
// operator takes action. The operator's own probe is intentionally
// NOT in the decision — operator-side HTTP probes can't reliably
// distinguish "pod's network is unreachable from this operator
// pod" from "pod is actually down", because they don't see
// kubelet's view of the pod's containerStatus. Relying on
// operator-only signals can fire a misdirected failover during a
// node freeze that kubelet has not yet declared NotReady.
type TripleCheckInputs struct {
	// Snapshot is the observer's most recent published view. The
	// QuorumOK field carries the CKQUORUM signal; the ODown map
	// carries the per-sentinel last-seen +odown timestamps. The
	// Snapshot.Present discriminator separates "no data yet" from
	// "data says quorum lost" — both block failover but for
	// different reasons (see TripleCheckReason).
	Snapshot Snapshot
	// PodReady reflects kubelet's most recent Pod.conditions[Ready]
	// for the outgoing primary, sourced from the controller-runtime
	// cache. False when kubelet has marked the pod NotReady (the
	// signal we want to see before a failover).
	PodReady bool
	// Quorum is the CR's sentinel.spec.Quorum value — the minimum
	// number of sentinels that must agree for an action to proceed.
	// EvaluateTripleCheck uses this to convert the ODown map size
	// into a consensus boolean.
	Quorum int32
	// ODownStaleness is the cutoff age beyond which a sentinel's
	// last-seen +odown timestamp is treated as missing. Operators
	// typically set this to ~3× the observer pull tick (so a
	// transient missed message doesn't drop a sentinel out of
	// consensus, but a sentinel that has been silent for several
	// ticks is correctly excluded).
	ODownStaleness time.Duration
	// MaxSnapshotAge is the cutoff beyond which the snapshot's
	// ObservedPrimary.UpdatedAt is treated as stale and a failover is
	// refused even when QuorumOK is true. This backstops the "never
	// trust pub/sub alone" invariant: a wedged poll goroutine while
	// pub/sub keeps re-publishing prev.Quorum can hold a stale
	// QuorumOK=true alive indefinitely, and without this gate the
	// triple-check would authorize a failover on quorum data no live
	// pull confirmed. Callers set this to ~3× the observer
	// PollInterval (one missed tick must not trip it; several must).
	// A non-positive value disables the gate — a test escape that
	// mirrors ODownStaleness<=0; production callers always pass a
	// positive value.
	MaxSnapshotAge time.Duration
	// Now is the wall-clock the evaluation is reasoning about.
	// Injectable so tests can drive the staleness cutoff
	// deterministically.
	Now time.Time
}

// FailoverDecision is the structured result of EvaluateTripleCheck.
// Allow is the boolean answer; Reason and Detail let callers emit
// precise events / status conditions without re-deriving the cause.
type FailoverDecision struct {
	// Allow is true only when all three signals agree. Callers must
	// not initiate a failover when Allow is false.
	Allow bool
	// Reason classifies why Allow is false. Empty when Allow is
	// true. Suitable as a Kubernetes Event reason or status
	// condition reason after lookup against the project's event
	// catalog.
	Reason TripleCheckReason
	// Detail is a human-readable explanation suitable for the
	// event message field or a log line. Empty when Allow is true.
	Detail string
}

// EvaluateTripleCheck decides whether the operator may initiate a
// failover. A present, fresh observer snapshot is the precondition;
// then three signals must agree:
//
//  1. CKQUORUM OK on the observer's snapshot
//     (Snapshot.Primary.QuorumOK).
//  2. ≥ Quorum sentinels report +odown for the target with a
//     non-stale last-seen timestamp.
//  3. kubelet says PodReady=False on the outgoing primary.
//
// The snapshot must also be fresh: if its ObservedPrimary.UpdatedAt
// is older than MaxSnapshotAge the decision defers, so a wedged pull
// tick cannot let pub/sub re-publish stale quorum into a failover.
// This is the ONLY failover-decision gate the operator should
// consult. It is intentionally pure (no I/O, no cache reads) so the
// caller wires up the inputs once and the function stays
// table-test-friendly.
func EvaluateTripleCheck(in TripleCheckInputs) FailoverDecision {
	if !in.Snapshot.Present {
		return FailoverDecision{
			Allow:  false,
			Reason: TripleCheckReasonNoSnapshot,
			Detail: "observer has not published a snapshot yet; failover decision deferred until the next observer tick",
		}
	}
	// Freshness gate: a present snapshot whose last live pull is
	// older than MaxSnapshotAge can't be trusted — pub/sub may be
	// re-publishing a QuorumOK that no recent poll confirmed. Checked
	// before QuorumOK because a stale snapshot's QuorumOK (true or
	// false) is itself unreliable, so the action is "defer until a
	// fresh pull lands", not "enter Degraded+QuorumLost". Inclusive
	// boundary (age == MaxSnapshotAge is still fresh) matches
	// ConsensusODown. Disabled when MaxSnapshotAge <= 0 (test escape).
	if in.MaxSnapshotAge > 0 {
		age := in.Now.Sub(in.Snapshot.Primary.UpdatedAt)
		if age > in.MaxSnapshotAge {
			return FailoverDecision{
				Allow:  false,
				Reason: TripleCheckReasonStaleSnapshot,
				Detail: fmt.Sprintf("observer snapshot is %s old (> max %s); the pull tick has not confirmed quorum recently, so deferring failover until a fresh poll lands", age.Round(time.Second), in.MaxSnapshotAge),
			}
		}
	}
	if !in.Snapshot.Primary.QuorumOK {
		return FailoverDecision{
			Allow:  false,
			Reason: TripleCheckReasonNoQuorum,
			Detail: "CKQUORUM did not return OK; entering Degraded+QuorumLost rather than failing over",
		}
	}
	consensus := ConsensusODown(in.Snapshot.Primary.ODown, in.Now, in.ODownStaleness)
	if consensus < int(in.Quorum) {
		return FailoverDecision{
			Allow:  false,
			Reason: TripleCheckReasonNoODownConsensus,
			Detail: fmt.Sprintf("only %d sentinels report +odown (need quorum=%d); letting sentinel run its course", consensus, in.Quorum),
		}
	}
	if in.PodReady {
		// All sentinel-side signals agree the primary is down, but
		// kubelet disagrees. In practice this state — sentinels
		// report +odown but kubelet still reports Pod.Ready=True —
		// is almost always a sentinel-network or sentinel-probe
		// issue, not a real primary failure: the disagreement is in
		// the sentinel layer, not the data plane. The operator must
		// NOT failover.
		return FailoverDecision{
			Allow:  false,
			Reason: TripleCheckReasonPodReady,
			Detail: "sentinels report the primary down, but kubelet says Pod.Ready=True; suppressing failover until kubelet agrees",
		}
	}
	return FailoverDecision{Allow: true}
}

// ConsensusODown returns the number of distinct sentinels whose
// last-seen +odown timestamp is within `staleness` of `now`. Used by
// EvaluateTripleCheck to convert the observer's per-sentinel ODown
// map into a consensus count comparable against
// sentinel.spec.Quorum. Exported because the reconciler also uses it
// for the audit-trail event message ("3 of 3 sentinels report
// +odown") without re-running the full triple-check.
func ConsensusODown(odown map[string]time.Time, now time.Time, staleness time.Duration) int {
	if staleness <= 0 {
		// Treat any non-zero entry as fresh — the test path uses
		// staleness=0 to mean "ignore the time check entirely".
		// Production callers always pass a positive staleness, so
		// this is a deliberate test-only shape rather than a footgun.
		count := 0
		for _, t := range odown {
			if !t.IsZero() {
				count++
			}
		}
		return count
	}
	cutoff := now.Add(-staleness)
	count := 0
	for _, t := range odown {
		if t.IsZero() {
			continue
		}
		if t.After(cutoff) || t.Equal(cutoff) {
			count++
		}
	}
	return count
}
