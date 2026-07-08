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

	"k8s.io/apimachinery/pkg/types"

	"github.com/ioxie/velkir/internal/orchestration"
)

// TestShouldBypassQuorumDeferral pins the deadlock-fix trigger:
// stranded-sentinel recovery bypasses the suppression deferral ONLY
// when quorum-loss suppression is active AND no failover is in flight.
// A polarity flip (drop the `!`, or `||` instead of `&&`) changes a row
// here.
func TestShouldBypassQuorumDeferral(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	cases := []struct {
		name       string
		suppressed bool
		failover   bool
		wantBypass bool
	}{
		{"suppressed, no failover -> bypass", true, false, true},
		{"suppressed, failover in flight -> defer", true, true, false},
		{"not suppressed, no failover -> defer", false, false, false},
		{"not suppressed, failover in flight -> defer", false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &ValkeyReconciler{}
			r.stateFor(cr).quorum = &crQuorumState{suppressionActive: tc.suppressed}
			lastState := orchestration.StateSteady
			if tc.failover {
				lastState = orchestration.StateFailoverInFlight
			}
			r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: lastState}

			if got := r.shouldBypassQuorumDeferral(cr); got != tc.wantBypass {
				t.Errorf("shouldBypassQuorumDeferral = %v, want %v", got, tc.wantBypass)
			}
		})
	}
}
