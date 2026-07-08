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

package sqaggregate

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

var fixedNow = time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)

const window = 60 * time.Second

func sq(observedPrimary string, quorumOK *bool, age time.Duration) valkeyv1beta1.SentinelQuorum {
	t := metav1.NewTime(fixedNow.Add(-age))
	return valkeyv1beta1.SentinelQuorum{
		Status: valkeyv1beta1.SentinelQuorumStatus{
			ObservedPrimary:  observedPrimary,
			QuorumReachable:  quorumOK,
			LastObservedTime: &t,
		},
	}
}

// stale is a record whose age exceeds the freshness window.
func stale(observedPrimary string) valkeyv1beta1.SentinelQuorum {
	return sq(observedPrimary, new(true), 5*time.Minute)
}

func TestAggregate_EmptyInput(t *testing.T) {
	t.Parallel()
	got := Aggregate(fixedNow, window, 2, nil)
	if got.PrimaryConfirmed || got.QuorumLost {
		t.Errorf("empty input should produce no condition flips: %+v", got)
	}
	if got.FreshCount != 0 || got.StaleCount != 0 {
		t.Errorf("empty input should report 0 fresh + 0 stale: %+v", got)
	}
}

func TestAggregate_AllStale_QuorumLost(t *testing.T) {
	t.Parallel()
	// All three records are 5min old (window is 60s) — none participate.
	// QuorumLost fires because 0 fresh QuorumOK records < quorum=2.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		stale("vk-0"), stale("vk-0"), stale("vk-0"),
	})
	if got.PrimaryConfirmed {
		t.Errorf("PrimaryConfirmed should not fire on all-stale input: %+v", got)
	}
	if !got.QuorumLost {
		t.Errorf("QuorumLost should fire when fresh QuorumOK count (0) < quorum (2): %+v", got)
	}
	if got.FreshCount != 0 || got.StaleCount != 3 {
		t.Errorf("expected fresh=0 stale=3, got fresh=%d stale=%d", got.FreshCount, got.StaleCount)
	}
}

func TestAggregate_StrictMajority_Confirmed(t *testing.T) {
	t.Parallel()
	// 3-of-3 fresh records all agree on vk-0 → PrimaryConfirmed=True,
	// PrimaryPod="vk-0", QuorumLost=False (3 fresh QuorumOK >= quorum=2).
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", new(true), 5*time.Second),
		sq("vk-0", new(true), 5*time.Second),
		sq("vk-0", new(true), 5*time.Second),
	})
	if !got.PrimaryConfirmed || got.PrimaryPod != "vk-0" {
		t.Errorf("expected PrimaryConfirmed=true, PrimaryPod=vk-0; got %+v", got)
	}
	if got.QuorumLost {
		t.Errorf("QuorumLost should be false when 3 fresh QuorumOK >= quorum 2: %+v", got)
	}
}

func TestAggregate_TwoOfThree_Confirmed(t *testing.T) {
	t.Parallel()
	// Strict majority: 2 of 3 fresh records on vk-1 → PrimaryConfirmed.
	// 2*2 > 3 ✓.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-1", new(true), 5*time.Second),
		sq("vk-1", new(true), 5*time.Second),
		sq("vk-2", new(true), 5*time.Second),
	})
	if !got.PrimaryConfirmed || got.PrimaryPod != "vk-1" {
		t.Errorf("expected PrimaryConfirmed=true, PrimaryPod=vk-1; got %+v", got)
	}
}

func TestAggregate_TwoOfFour_NoMajority(t *testing.T) {
	t.Parallel()
	// 2-2 split — strict-majority requires > half, so NEITHER pod
	// confirms. PrimaryPod stays empty.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-1", new(true), 5*time.Second),
		sq("vk-1", new(true), 5*time.Second),
		sq("vk-2", new(true), 5*time.Second),
		sq("vk-2", new(true), 5*time.Second),
	})
	if got.PrimaryConfirmed {
		t.Errorf("2-2 split must NOT confirm (strict majority is > half): %+v", got)
	}
	if got.PrimaryPod != "" {
		t.Errorf("PrimaryPod should be empty on no-majority: %+v", got)
	}
}

func TestAggregate_StaleRecordExcluded_FreshMajorityWins(t *testing.T) {
	t.Parallel()
	// Stale dissenter (vk-X) doesn't participate; fresh 2/2 on vk-1 is
	// a strict majority of fresh records and confirms.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-1", new(true), 5*time.Second),
		sq("vk-1", new(true), 5*time.Second),
		stale("vk-X"),
	})
	if !got.PrimaryConfirmed || got.PrimaryPod != "vk-1" {
		t.Errorf("stale dissenter should be excluded: %+v", got)
	}
	if got.FreshCount != 2 || got.StaleCount != 1 {
		t.Errorf("expected fresh=2 stale=1, got fresh=%d stale=%d", got.FreshCount, got.StaleCount)
	}
}

func TestAggregate_EmptyObservation_NotAVote(t *testing.T) {
	t.Parallel()
	// A fresh record with empty ObservedPrimary doesn't vote (sentinel
	// not yet converged). 1 of 1 vote on vk-0 — but votes need strict
	// majority of FRESH count (3 records, all fresh), so 1 < 2 → no
	// majority on a non-empty primary.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", new(true), 5*time.Second),
		sq("", new(true), 5*time.Second),
		sq("", new(true), 5*time.Second),
	})
	if got.PrimaryConfirmed {
		t.Errorf("1 of 3 fresh non-empty votes shouldn't confirm: %+v", got)
	}
	if got.PrimaryPod != "" {
		t.Errorf("PrimaryPod should be empty: %+v", got)
	}
}

func TestAggregate_BoundaryFreshness(t *testing.T) {
	t.Parallel()
	// Record exactly at the freshness window edge — should still
	// participate. window=60s, age=60s → now.Sub(LastObserved)=60s
	// which is == window (NOT >); the gate is `> freshnessWindow`.
	got := Aggregate(fixedNow, window, 1, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", new(true), 60*time.Second),
	})
	if got.FreshCount != 1 {
		t.Errorf("60s-old record at exact window edge should be fresh; got fresh=%d stale=%d",
			got.FreshCount, got.StaleCount)
	}

	// One nanosecond past the window → stale.
	got2 := Aggregate(fixedNow, window, 1, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", new(true), 60*time.Second+time.Nanosecond),
	})
	if got2.StaleCount != 1 {
		t.Errorf("60s+1ns past window should be stale; got fresh=%d stale=%d",
			got2.FreshCount, got2.StaleCount)
	}
}

func TestAggregate_NilQuorumReachable_DoesNotCount(t *testing.T) {
	t.Parallel()
	// QuorumReachable=nil means sentinel hasn't reported (unset
	// pointer is the unknown state). 0 fresh QuorumOK records < quorum
	// 2 → QuorumLost fires.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", nil, 5*time.Second),
		sq("vk-0", nil, 5*time.Second),
	})
	if !got.QuorumLost {
		t.Errorf("nil QuorumReachable should not count as OK: %+v", got)
	}
}

func TestAggregate_MajorityAndQuorumLost_Coexist(t *testing.T) {
	t.Parallel()
	// 3 fresh records all agree on vk-0 (PrimaryConfirmed=true), but
	// only 1 of 3 carries QuorumReachable=true (a sentinel-side reachability
	// flap). With quorum=2, QuorumLost fires too — both conditions are
	// independent.
	got := Aggregate(fixedNow, window, 2, []valkeyv1beta1.SentinelQuorum{
		sq("vk-0", new(true), 5*time.Second),
		sq("vk-0", new(false), 5*time.Second),
		sq("vk-0", new(false), 5*time.Second),
	})
	if !got.PrimaryConfirmed || got.PrimaryPod != "vk-0" {
		t.Errorf("primary should still confirm: %+v", got)
	}
	if !got.QuorumLost {
		t.Errorf("quorum-lost should also fire: %+v", got)
	}
}

// TestAggregate_QuorumTriState pins the tri-state Quorum field on
// Result for every aggregator output shape. The QuorumLost bool is
// kept in sync (true iff Quorum == QuorumStatusLost); future
// regressions in either field are caught here.
func TestAggregate_QuorumTriState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		sqs        []valkeyv1beta1.SentinelQuorum
		quorum     int32
		wantQuorum sentinel.QuorumStatus
		wantQLBool bool
		wantFresh  int
	}{
		{
			name:       "empty input — no data, Unknown",
			sqs:        nil,
			quorum:     2,
			wantQuorum: sentinel.QuorumStatusUnknown,
			wantQLBool: false,
			wantFresh:  0,
		},
		{
			name: "all-stale input — data plane stopped flowing, Lost",
			sqs: []valkeyv1beta1.SentinelQuorum{
				stale("vk-0"), stale("vk-0"), stale("vk-0"),
			},
			quorum:     2,
			wantQuorum: sentinel.QuorumStatusLost,
			wantQLBool: true,
			wantFresh:  0,
		},
		{
			name: "fresh majority QuorumOK — OK",
			sqs: []valkeyv1beta1.SentinelQuorum{
				sq("vk-0", new(true), 5*time.Second),
				sq("vk-0", new(true), 5*time.Second),
				sq("vk-0", new(true), 5*time.Second),
			},
			quorum:     2,
			wantQuorum: sentinel.QuorumStatusOK,
			wantQLBool: false,
			wantFresh:  3,
		},
		{
			name: "fresh records but quorumOK count below quorum — Lost",
			sqs: []valkeyv1beta1.SentinelQuorum{
				sq("vk-0", new(true), 5*time.Second),
				sq("vk-0", new(false), 5*time.Second),
				sq("vk-0", new(false), 5*time.Second),
			},
			quorum:     2,
			wantQuorum: sentinel.QuorumStatusLost,
			wantQLBool: true,
			wantFresh:  3,
		},
		{
			name: "fresh records but all nil QuorumReachable — Lost",
			sqs: []valkeyv1beta1.SentinelQuorum{
				sq("vk-0", nil, 5*time.Second),
				sq("vk-0", nil, 5*time.Second),
			},
			quorum:     2,
			wantQuorum: sentinel.QuorumStatusLost,
			wantQLBool: true,
			wantFresh:  2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Aggregate(fixedNow, window, tc.quorum, tc.sqs)
			if got.Quorum != tc.wantQuorum {
				t.Errorf("Quorum = %s; want %s", got.Quorum, tc.wantQuorum)
			}
			if got.QuorumLost != tc.wantQLBool {
				t.Errorf("QuorumLost bool = %v; want %v", got.QuorumLost, tc.wantQLBool)
			}
			if got.FreshCount != tc.wantFresh {
				t.Errorf("FreshCount = %d; want %d", got.FreshCount, tc.wantFresh)
			}
			// Invariant: QuorumLost bool MUST mirror Quorum == Lost.
			if got.QuorumLost != (got.Quorum == sentinel.QuorumStatusLost) {
				t.Errorf("invariant violated: QuorumLost=%v but Quorum=%s",
					got.QuorumLost, got.Quorum)
			}
		})
	}
}
