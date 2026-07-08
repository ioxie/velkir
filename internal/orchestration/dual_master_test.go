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

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func TestEvalReady_DualMasterForcesFalse(t *testing.T) {
	// All STS replicas Ready by kubelet's lights, but two pods accept
	// writes as master. Ready MUST flip False with the divergence
	// reason so consumers stop writing into the split.
	o := Observation{
		CR:               sentinelCRForOrch(),
		STS:              healthySTSForOrch(),
		DualMasterActive: true,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionFalse {
		t.Errorf("Ready.Status = %v; want False (DualMaster must override healthy STS)", c.Status)
	}
	if c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Ready.Reason = %q; want %q", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalReady_DualMasterOutranksNoMasterAgreementAndMasterLost(t *testing.T) {
	// A real split usually co-occurs with the other wedge signals;
	// the divergence is the most severe fact and must be the one
	// surfaced.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		DualMasterActive:        true,
		NoMasterAgreementActive: true,
		MasterLostActive:        true,
	}
	c := evalReady(o)
	if c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Ready.Reason = %q; want %q (divergence outranks the other wedge signals)", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalDegraded_DualMasterFires(t *testing.T) {
	o := Observation{
		CR:               sentinelCRForOrch(),
		STS:              healthySTSForOrch(),
		DualMasterActive: true,
	}
	c := evalDegraded(o)
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Degraded.Status = %v; want True", c.Status)
	}
	if c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Degraded.Reason = %q; want %q", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalDegraded_DualMasterOutranksQuorumSignals(t *testing.T) {
	// The split usually co-occurs with quorum loss; slotting the
	// divergence below the quorum arms would mask it exactly when it
	// matters. All four observation flags on: divergence must win.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		DualMasterActive:        true,
		QuorumSuppressionActive: true,
		NoMasterAgreementActive: true,
		SplitBrainActive:        true,
	}
	c := evalDegraded(o)
	if c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Degraded.Reason = %q; want %q (divergence outranks quorum signals)", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalDegraded_RuntimeFaultsStillOutrankDualMaster(t *testing.T) {
	// The runtime-fault arms stay above: an operator that made no
	// progress at all this pass is a strictly-worse headline.
	o := Observation{
		CR:               sentinelCRForOrch(),
		STS:              healthySTSForOrch(),
		DualMasterActive: true,
		ReconcileError:   assertErr("apply failed"),
	}
	if c := evalDegraded(o); c.Reason != ReasonReconcileErr {
		t.Errorf("Degraded.Reason = %q; want ReconcileErr (runtime fault outranks observation)", c.Reason)
	}
}

func TestEvalDegraded_DualMasterNotSentinelGated(t *testing.T) {
	// The observation is currently produced by sentinel-mode scans
	// only, but the evaluator deliberately does not mode-gate: a
	// replication-mode producer added later must not silently no-op
	// here.
	v := &valkeyv1beta1.Valkey{}
	v.Spec.Mode = valkeyv1beta1.ModeReplication
	v.Spec.Valkey.Replicas = 3
	o := Observation{
		CR:               v,
		STS:              healthySTSForOrch(),
		DualMasterActive: true,
	}
	if c := evalDegraded(o); c.Reason != ReasonDualMasterDivergence {
		t.Errorf("Degraded.Reason = %q; want %q for a replication-mode CR", c.Reason, ReasonDualMasterDivergence)
	}
}

func TestEvalDegraded_DualMasterCleared(t *testing.T) {
	o := Observation{
		CR:  sentinelCRForOrch(),
		STS: healthySTSForOrch(),
	}
	if c := evalDegraded(o); c.Status != metav1.ConditionFalse {
		t.Errorf("Degraded.Status = %v; want False with no observation flags", c.Status)
	}
}
