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

// Package sqaggregate computes the per-CR SentinelQuorum aggregation.
// For a Valkey CR with N sentinel pods, the
// operator owns N SentinelQuorum records — each carrying one
// sentinel's view of the topology. This package merges those views
// into authoritative Valkey.Status fields:
//
//   - Status.PrimaryPod = pod name a strict majority of FRESH records
//     agree on (empty if no majority or all observations empty).
//   - Conditions[type=PrimaryConfirmed]=True when that majority is
//     reached.
//   - Conditions[type=QuorumLost]=True when fewer than the configured
//     quorum count of FRESH records carry QuorumReachable=true.
//
// Freshness gate: a SentinelQuorum participates only if
// LastObservedTime is within `freshnessWindow` of now. Stale records
// are dropped silently — the count is exposed in Result for diagnostic
// logging at the call site.
//
// Pure-function shape: the helper takes a clock-now value, the
// configured window + quorum, and the slice of records. No I/O. Tests
// drive the aggregator with synthetic SQs and a synthetic clock.
package sqaggregate

import (
	"time"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Result is the aggregator verdict.
type Result struct {
	// PrimaryPod is the pod name that a strict majority of fresh
	// records agreed on. Empty when no majority is reached, or when
	// all fresh records carry an empty ObservedPrimary (sentinels not
	// yet converged).
	PrimaryPod string

	// PrimaryConfirmed is true when PrimaryPod is non-empty AND a
	// strict majority of fresh records reported it. Drives
	// Conditions[type=PrimaryConfirmed].
	PrimaryConfirmed bool

	// QuorumLost is true iff Quorum == sentinel.QuorumStatusLost.
	// Retained for legacy call sites that consume the bool directly;
	// new code should read Quorum to distinguish Lost from Unknown.
	QuorumLost bool

	// Quorum is the tri-state aggregator verdict. Unknown is
	// surfaced when no fresh records are available — same semantic
	// the observer publishes on its own boundary when reaching fewer
	// than `QuorumThreshold(N)` sentinel peers. Consumers driving
	// hysteresis-gated state (e.g. evalQuorumLost) MUST distinguish
	// Unknown from Lost; consumers driving irreversible actions
	// MUST treat Unknown identically to Lost.
	Quorum sentinel.QuorumStatus

	// FreshCount is the count of records that passed the freshness
	// gate. Surfaced for diagnostic logging.
	FreshCount int

	// StaleCount is the count of records dropped by the freshness
	// gate. Surfaced for diagnostic logging.
	StaleCount int
}

// Aggregate computes the per-CR aggregation across the supplied
// SentinelQuorum records. `now` and `freshnessWindow` together drive
// the freshness gate (a record participates iff
// `now.Sub(LastObservedTime) <= freshnessWindow` AND LastObservedTime
// is non-zero); `quorum` is the count from spec.sentinel.quorum that
// gates QuorumLost.
//
// Empty input or all-stale input returns a zero-value Result with
// FreshCount/StaleCount populated for the caller to log.
func Aggregate(now time.Time, freshnessWindow time.Duration, quorum int32, sqs []valkeyv1beta1.SentinelQuorum) Result {
	var (
		fresh    int
		stale    int
		votes    = map[string]int{}
		quorumOK int
	)

	for i := range sqs {
		st := sqs[i].Status
		if st.LastObservedTime == nil || st.LastObservedTime.IsZero() {
			stale++
			continue
		}
		if now.Sub(st.LastObservedTime.Time) > freshnessWindow {
			stale++
			continue
		}
		fresh++
		if st.ObservedPrimary != "" {
			votes[st.ObservedPrimary]++
		}
		if st.QuorumReachable != nil && *st.QuorumReachable {
			quorumOK++
		}
	}

	out := Result{FreshCount: fresh, StaleCount: stale}

	// Strict-majority on a non-empty ObservedPrimary. "Strict" means
	// > half — exactly half of records agreeing isn't enough (a 2-2
	// split with a fifth empty record would otherwise wrongly confirm).
	for pod, count := range votes {
		if count*2 > fresh {
			out.PrimaryPod = pod
			out.PrimaryConfirmed = true
			break
		}
	}

	// Tri-state Quorum:
	//   - Unknown when no fresh records exist AND the input wasn't
	//     empty-but-fresh (a brand-new CR with zero SQs is also
	//     Unknown — there's no data either way).
	//   - Lost when fresh records reporting QuorumReachable=true are
	//     below the configured quorum (including all-stale input,
	//     which means the sentinel pods stopped flowing data — a
	//     degradation signal).
	//   - OK otherwise.
	//
	// Legacy QuorumLost bool mirrors `Quorum == QuorumStatusLost` so
	// existing call sites keep their semantics; tri-state consumers
	// (notably evalQuorumLost) read Quorum to distinguish Unknown.
	switch {
	case len(sqs) == 0:
		out.Quorum = sentinel.QuorumStatusUnknown
	case fresh == 0:
		// All input went stale — the data plane stopped flowing
		// observations. Treat as Lost so the Degraded path fires.
		out.Quorum = sentinel.QuorumStatusLost
		out.QuorumLost = true
	case int32(quorumOK) < quorum: //nolint:gosec // quorumOK bounded by len(sqs)
		out.Quorum = sentinel.QuorumStatusLost
		out.QuorumLost = true
	default:
		out.Quorum = sentinel.QuorumStatusOK
	}

	return out
}
