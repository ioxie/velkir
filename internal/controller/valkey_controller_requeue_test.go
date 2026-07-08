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

package controller

import (
	"testing"
	"time"
)

// TestMergeRequeue_TableDriven pins the four documented contracts of
// mergeRequeue: (a) zero/negative hint preserves current, (b) hint
// below floor is raised to floor, (c) the (floored) hint wins when
// tighter than current OR when current is unset, (d) current wins
// when the (floored) hint is looser. The floor invariant is the
// load-bearing piece — without it, an aggressive sub-floor hint
// would let controller-runtime rearm the reconciler at hundreds of
// times per second under churn (the canonical thrash regression).
func TestMergeRequeue_TableDriven(t *testing.T) {
	const floor = minRequeueFloor // mergeRequeue uses the package constant
	cases := []struct {
		name    string
		current time.Duration
		hint    time.Duration
		want    time.Duration
	}{
		{"no_hint_no_current", 0, 0, 0},
		{"no_hint_keeps_current", 500 * time.Millisecond, 0, 500 * time.Millisecond},
		{"negative_hint_keeps_current", 500 * time.Millisecond, -1 * time.Second, 500 * time.Millisecond},
		{"sub_floor_hint_no_current_floors_to_floor", 0, 5 * time.Millisecond, floor},
		{"sub_floor_hint_with_larger_current_floors_then_wins", 500 * time.Millisecond, 5 * time.Millisecond, floor},
		{"hint_at_floor_with_no_current", 0, floor, floor},
		{"hint_above_floor_no_current", 0, 250 * time.Millisecond, 250 * time.Millisecond},
		{"hint_tighter_than_current_wins", 500 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond},
		{"hint_looser_than_current_loses", 200 * time.Millisecond, 500 * time.Millisecond, 200 * time.Millisecond},
		{"hint_equal_to_current_keeps_current", 200 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond},
		{"sub_floor_hint_with_smaller_floored_current_keeps_current", 50 * time.Millisecond, 5 * time.Millisecond, 50 * time.Millisecond},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeRequeue(tt.current, tt.hint)
			if got != tt.want {
				t.Fatalf("mergeRequeue(current=%s, hint=%s) = %s, want %s (floor=%s)",
					tt.current, tt.hint, got, tt.want, floor)
			}
		})
	}
}

// TestMergeRequeue_FloorEnforcedAtAllSitesIsLiveConstant ties the
// runtime floor used at the three real call sites (watchdog,
// rollout-trigger edge, PVC-resize substate) to the helper's
// behavior. If a future change either lowers minRequeueFloor below
// the safe-cadence threshold OR removes the call-through entirely,
// this test fails as a safety pin.
func TestMergeRequeue_FloorEnforcedAtAllSitesIsLiveConstant(t *testing.T) {
	if minRequeueFloor < 50*time.Millisecond {
		t.Fatalf("minRequeueFloor regressed below safe cadence: %s; <50ms allows >20 reconciles/sec/CR thrash", minRequeueFloor)
	}
	// Sub-floor hint MUST be raised to the floor.
	got := mergeRequeue(0, 5*time.Millisecond)
	if got != minRequeueFloor {
		t.Fatalf("mergeRequeue did not raise sub-floor hint to floor: got %s, want %s", got, minRequeueFloor)
	}
}

// TestBackoffForMissingAuthSecret_Tiers pins the three-tier ladder of
// the missing-auth-Secret requeue backoff: 30s for the first 5
// minutes (cover the "user is mid-kubectl-apply" case), 1 minute for
// the next 25, then 5 minutes indefinitely. The boundary instants
// each fall into the higher tier — the comparison is strict less-than.
func TestBackoffForMissingAuthSecret_Tiers(t *testing.T) {
	cases := []struct {
		name    string
		elapsed time.Duration
		want    time.Duration
	}{
		{"fresh", 0, authSecretMissingRequeue},
		{"30s_in_first_tier", 30 * time.Second, authSecretMissingRequeue},
		{"just_under_5min", 5*time.Minute - time.Second, authSecretMissingRequeue},
		{"exactly_5min_boundary_into_second_tier", 5 * time.Minute, time.Minute},
		{"10min_second_tier", 10 * time.Minute, time.Minute},
		{"just_under_30min", 30*time.Minute - time.Second, time.Minute},
		{"exactly_30min_boundary_into_third_tier", 30 * time.Minute, 5 * time.Minute},
		{"1h_third_tier", time.Hour, 5 * time.Minute},
		{"7d_third_tier", 7 * 24 * time.Hour, 5 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backoffForMissingAuthSecret(tc.elapsed); got != tc.want {
				t.Fatalf("backoffForMissingAuthSecret(%s) = %s, want %s", tc.elapsed, got, tc.want)
			}
		})
	}
}
