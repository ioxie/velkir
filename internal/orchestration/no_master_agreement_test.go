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

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// sentinelCRForOrch returns a sentinel-mode CR with replicas=3 (HA
// floor — the orchestration tests don't need to vary this).
func sentinelCRForOrch() *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{}
	v.Spec.Mode = valkeyv1beta1.ModeSentinel
	v.Spec.Valkey.Replicas = 3
	return v
}

// healthySTSForOrch returns an STS with all 3 desired replicas
// reporting Ready (the orchestration tests don't vary these).
func healthySTSForOrch() *appsv1.StatefulSet {
	desired := int32(3)
	return &appsv1.StatefulSet{
		Spec:   appsv1.StatefulSetSpec{Replicas: &desired},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
}

func TestEvalReady_NoMasterAgreementForcesFalse(t *testing.T) {
	// All STS replicas are Ready (the kubelet says pods are
	// healthy) but the cluster has no agreed master. Ready MUST
	// flip False with the specific NoMasterAgreement reason so
	// consumers know not to send writes.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		NoMasterAgreementActive: true,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionFalse {
		t.Errorf("Ready.Status = %v; want False (NoMasterAgreement must override healthy STS)", c.Status)
	}
	if c.Reason != ReasonNoMasterAgreement {
		t.Errorf("Ready.Reason = %q; want %q", c.Reason, ReasonNoMasterAgreement)
	}
}

func TestEvalReady_NoMasterAgreementCleared(t *testing.T) {
	// Without NoMasterAgreement and healthy STS, Ready should be
	// True. This is the recovery path — clearing the wedge must
	// flip Ready back.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		NoMasterAgreementActive: false,
	}
	c := evalReady(o)
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Ready.Status = %v; want True (no anomaly + healthy STS)", c.Status)
	}
	if c.Reason != ReasonReady {
		t.Errorf("Ready.Reason = %q; want %q", c.Reason, ReasonReady)
	}
}

func TestEvalDegraded_NoMasterAgreementOutranksSplitBrain(t *testing.T) {
	// Both flags active. Precedence rule: NoMasterAgreement is
	// more specific (sentinels agree but on a dead IP) and must
	// win.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		NoMasterAgreementActive: true,
		SplitBrainActive:        true,
	}
	c := evalDegraded(o)
	if c.Status != metav1.ConditionTrue {
		t.Errorf("Degraded.Status = %v; want True", c.Status)
	}
	if c.Reason != ReasonNoMasterAgreement {
		t.Errorf("Degraded.Reason = %q; want %q (more-specific reason wins)", c.Reason, ReasonNoMasterAgreement)
	}
}

func TestEvalDegraded_ReconcileErrStillOutranksNoMasterAgreement(t *testing.T) {
	// ReconcileError is a runtime fault — strictly worse than an
	// observation-only signal. Precedence holds.
	o := Observation{
		CR:                      sentinelCRForOrch(),
		STS:                     healthySTSForOrch(),
		NoMasterAgreementActive: true,
		ReconcileError:          assertErr("phase 7: snapshot stale"),
	}
	c := evalDegraded(o)
	if c.Reason != ReasonReconcileErr {
		t.Errorf("Degraded.Reason = %q; want ReconcileErr (runtime fault outranks observation)", c.Reason)
	}
}

type orchErr string

func (e orchErr) Error() string { return string(e) }

func assertErr(s string) error { return orchErr(s) }
