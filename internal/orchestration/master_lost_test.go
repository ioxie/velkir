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

func TestEvalReady_MasterLostForcesFalse(t *testing.T) {
	// All STS replicas are Ready, but the pod labelled role=primary
	// has stopped answering INFO (dead-master-still-labelled during
	// the down-after / election window). Ready MUST flip False with
	// the MasterLost reason so consumers stop routing writes to a
	// black-holing primary Service.
	o := Observation{
		CR:               sentinelCRForOrch(),
		STS:              healthySTSForOrch(),
		MasterLostActive: true,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionFalse {
		t.Errorf("Ready.Status = %v; want False (MasterLost must override healthy STS)", c.Status)
	}
	if c.Reason != ReasonMasterLost {
		t.Errorf("Ready.Reason = %q; want %q", c.Reason, ReasonMasterLost)
	}
}

func TestEvalReady_MasterLostCleared(t *testing.T) {
	// Probe recovered (or a replacement primary was relabelled and
	// answers INFO) → no MasterLost, healthy STS → Ready True. This
	// is the automatic-clear path the acceptance requires.
	o := Observation{
		CR:               sentinelCRForOrch(),
		STS:              healthySTSForOrch(),
		MasterLostActive: false,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Ready.Status = %v; want True (master responsive + healthy STS)", c.Status)
	}
	if c.Reason != ReasonReady {
		t.Errorf("Ready.Reason = %q; want %q", c.Reason, ReasonReady)
	}
}

func TestEvalReady_NoMasterAgreementOutranksMasterLost(t *testing.T) {
	// Both active. NoMasterAgreement is the more-specific wedge
	// (sentinels agree, on a dead IP) and must win the Ready reason.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		NoMasterAgreementActive: true,
		MasterLostActive:        true,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionFalse {
		t.Errorf("Ready.Status = %v; want False", c.Status)
	}
	if c.Reason != ReasonNoMasterAgreement {
		t.Errorf("Ready.Reason = %q; want %q (more-specific reason wins)", c.Reason, ReasonNoMasterAgreement)
	}
}
