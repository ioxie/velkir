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

package orchestration

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEvalDegraded_LinkupStuckFires(t *testing.T) {
	c := evalDegraded(Observation{
		CR:                            sentinelCRForOrch(),
		STS:                           healthySTSForOrch(),
		SentinelPeerLinkupStuckActive: true,
	})
	if c.Status != metav1.ConditionTrue || c.Reason != ReasonSentinelPeerLinkupStuck {
		t.Errorf("Degraded = %s/%s; want True/%s", c.Status, c.Reason, ReasonSentinelPeerLinkupStuck)
	}
}

func TestEvalDegraded_LinkupStuckOutranksQuorumLost(t *testing.T) {
	// Both active: linkup-stuck names WHY quorum can't recover, so it
	// must be the surfaced reason.
	c := evalDegraded(Observation{
		CR:                            sentinelCRForOrch(),
		STS:                           healthySTSForOrch(),
		SentinelPeerLinkupStuckActive: true,
		QuorumSuppressionActive:       true,
	})
	if c.Reason != ReasonSentinelPeerLinkupStuck {
		t.Errorf("Degraded.Reason = %q; want %q (linkup-stuck outranks QuorumLost)", c.Reason, ReasonSentinelPeerLinkupStuck)
	}
}

func TestEvalDegraded_DualMasterOutranksLinkupStuck(t *testing.T) {
	// Active data divergence is worse than a stuck repair.
	c := evalDegraded(Observation{
		CR:                            sentinelCRForOrch(),
		STS:                           healthySTSForOrch(),
		DualMasterActive:              true,
		SentinelPeerLinkupStuckActive: true,
	})
	if c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Degraded.Reason = %q; want %q (dual-master outranks linkup-stuck)", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalReady_LinkupStuckDoesNotForceReadyFalse(t *testing.T) {
	// Linkup-stuck does NOT gate Ready — the survivor quorum may still
	// serve while one rebuilt sentinel is stuck.
	c := evalReady(Observation{
		CR:                            sentinelCRForOrch(),
		STS:                           healthySTSForOrch(),
		SentinelPeerLinkupStuckActive: true,
	})
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %s; want True (linkup-stuck must not force Ready False)", c.Status)
	}
}
