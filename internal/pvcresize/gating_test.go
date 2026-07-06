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

package pvcresize

import (
	"testing"
	"time"
)

func TestStallElapsed(t *testing.T) {
	now := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name           string
		lastTransition time.Time
		want           time.Duration
	}{
		{"zero lastTransition returns zero (defensive)", time.Time{}, 0},
		{"5 minutes elapsed", now.Add(-5 * time.Minute), 5 * time.Minute},
		{"exactly at threshold", now.Add(-StallThreshold), StallThreshold},
		{"past threshold", now.Add(-30 * time.Minute), 30 * time.Minute},
		{"future lastTransition (clock skew) returns negative", now.Add(1 * time.Minute), -1 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := StallElapsed(tc.lastTransition, now); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsStalled(t *testing.T) {
	now := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name           string
		lastTransition time.Time
		want           bool
	}{
		{"zero lastTransition is not stalled", time.Time{}, false},
		{"5 minutes elapsed not stalled", now.Add(-5 * time.Minute), false},
		{"exactly at threshold is stalled", now.Add(-StallThreshold), true},
		{"past threshold is stalled", now.Add(-30 * time.Minute), true},
		{"1ns shy of threshold is not stalled", now.Add(-StallThreshold + time.Nanosecond), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsStalled(tc.lastTransition, now); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBackoffForAttempt(t *testing.T) {
	cases := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{"attempt 0 (defensive) clamps to attempt 1", 0, 1 * time.Minute},
		{"attempt -1 (defensive) clamps to attempt 1", -1, 1 * time.Minute},
		{"attempt 1", 1, 1 * time.Minute},
		{"attempt 2", 2, 5 * time.Minute},
		{"attempt 3", 3, 15 * time.Minute},
		{"attempt 4 hits the cap", 4, 1 * time.Hour},
		{"attempt 100 caps at 1h", 100, 1 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := BackoffForAttempt(tc.attempt); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBackoffRemaining(t *testing.T) {
	now := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name           string
		lastTransition time.Time
		attempt        int32
		want           time.Duration
	}{
		{"zero lastTransition returns zero (defensive)", time.Time{}, 1, 0},
		{"attempt 1 at t=0: full minute remains", now, 1, 1 * time.Minute},
		{"attempt 1 at t=30s: 30s remains", now.Add(-30 * time.Second), 1, 30 * time.Second},
		{"attempt 1 at t=1m exactly: zero remaining", now.Add(-1 * time.Minute), 1, 0},
		{"attempt 1 well past backoff: zero remaining (no negative)", now.Add(-1 * time.Hour), 1, 0},
		{"attempt 2 at t=0: 5m remains", now, 2, 5 * time.Minute},
		{"attempt 4 at t=0: 1h remains (cap)", now, 4, 1 * time.Hour},
		{"attempt 4 at t=30m: 30m remains (cap halved)", now.Add(-30 * time.Minute), 4, 30 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := BackoffRemaining(tc.lastTransition, tc.attempt, now); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
