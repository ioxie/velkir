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

// TestNewPrimaryStable_TwoPollSettlingDamp pins the two-snapshot settling
// damp tracker: the consecutive-fresh-poll count for an observed primary
// Addr advances only on a strictly-newer LastPolledAt (so a pub/sub
// replay of a stale Addr can't inflate it), resets on an Addr change, and
// reports the dwell over which the streak accrued. desiredRolesForCR
// requires the count to reach 2 before moving the role=primary label.
func TestNewPrimaryStable_TwoPollSettlingDamp(t *testing.T) {
	s := &perCRState{}
	t0 := time.Now()

	// First fresh poll naming addr A: streak starts at 1 (not yet stable).
	if c, d := s.observePrimaryStability("10.0.0.2:6379", t0); c != 1 || d != 0 {
		t.Fatalf("first observation: (count=%d, dwell=%s), want (1, 0)", c, d)
	}
	// A pub/sub replay carrying the same Addr with no newer LastPolledAt
	// must NOT advance the count.
	if c, _ := s.observePrimaryStability("10.0.0.2:6379", t0); c != 1 {
		t.Errorf("replay at same poll time: count=%d, want 1 (no inflation)", c)
	}
	// A second, strictly-newer fresh poll naming A advances to 2 (stable)
	// and reports the dwell across the streak.
	if c, d := s.observePrimaryStability("10.0.0.2:6379", t0.Add(2*time.Second)); c != 2 || d != 2*time.Second {
		t.Errorf("second fresh poll: (count=%d, dwell=%s), want (2, 2s)", c, d)
	}
	// A different Addr resets the streak to 1.
	if c, d := s.observePrimaryStability("10.0.0.3:6379", t0.Add(3*time.Second)); c != 1 || d != 0 {
		t.Errorf("addr change: (count=%d, dwell=%s), want (1, 0)", c, d)
	}
	// An empty Addr clears the tracker entirely.
	if c, d := s.observePrimaryStability("", t0.Add(4*time.Second)); c != 0 || d != 0 {
		t.Errorf("empty addr: (count=%d, dwell=%s), want (0, 0)", c, d)
	}
}
