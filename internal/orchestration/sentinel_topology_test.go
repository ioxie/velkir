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

func condByType(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

// TestEvalSentinelTopology_DecoupledFromReadyDegraded pins the core
// decoupling property: toggling SentinelTopologyMismatchActive flips
// SentinelTopologyReconciled but leaves Ready, Degraded, and phase
// byte-identical — the hygiene signal never worsens the incident ladder.
func TestEvalSentinelTopology_DecoupledFromReadyDegraded(t *testing.T) {
	base := Observation{
		CR:  sentinelCRForOrch(),
		STS: healthySTSForOrch(),
	}
	active := base
	active.SentinelTopologyMismatchActive = true
	active.SentinelTopologySentinelDeficit = 1
	active.SentinelTopologyReplicaDeficit = 2

	clean, cleanPhase := Evaluate(base)
	dirty, dirtyPhase := Evaluate(active)

	if cleanPhase != dirtyPhase {
		t.Errorf("phase drifted: clean=%q dirty=%q (topology must not move phase)", cleanPhase, dirtyPhase)
	}
	for _, typ := range []string{TypeReady, TypeDegraded} {
		a := condByType(clean, typ)
		b := condByType(dirty, typ)
		if a == nil || b == nil {
			t.Fatalf("%s condition missing (clean=%v dirty=%v)", typ, a, b)
		}
		if a.Status != b.Status || a.Reason != b.Reason || a.Message != b.Message {
			t.Errorf("%s drifted when only topology changed: clean=%+v dirty=%+v", typ, a, b)
		}
	}

	// The topology condition itself MUST differ.
	tc := condByType(dirty, TypeSentinelTopologyReconciled)
	if tc == nil {
		t.Fatal("SentinelTopologyReconciled condition missing")
	}
	if tc.Status != metav1.ConditionFalse || tc.Reason != ReasonSentinelTopologyMismatch {
		t.Errorf("active topology condition = %v/%s; want False/%s", tc.Status, tc.Reason, ReasonSentinelTopologyMismatch)
	}
	wantMsg := "sentinel topology below spec: sentinels short by 1 (peer-gossip gap); replicas short by 2"
	if tc.Message != wantMsg {
		t.Errorf("active topology message = %q; want %q", tc.Message, wantMsg)
	}
	if cc := condByType(clean, TypeSentinelTopologyReconciled); cc == nil ||
		cc.Status != metav1.ConditionTrue || cc.Reason != ReasonSentinelTopologyInSync {
		t.Errorf("clean topology condition = %+v; want True/%s", cc, ReasonSentinelTopologyInSync)
	}
}

// TestEvalSentinelTopology_InSyncAndNotApplicable pins the two positive
// resolutions: sentinel-mode + inactive → True/InSync; non-sentinel mode
// → True/NotApplicable so `kubectl wait` terminates.
func TestEvalSentinelTopology_InSyncAndNotApplicable(t *testing.T) {
	inSync := evalSentinelTopology(Observation{CR: sentinelCRForOrch()})
	if inSync.Status != metav1.ConditionTrue || inSync.Reason != ReasonSentinelTopologyInSync {
		t.Errorf("sentinel-mode inactive = %v/%s; want True/%s", inSync.Status, inSync.Reason, ReasonSentinelTopologyInSync)
	}

	na := evalSentinelTopology(Observation{CR: minimalCR()}) // standalone
	if na.Status != metav1.ConditionTrue || na.Reason != ReasonNotApplicable {
		t.Errorf("non-sentinel mode = %v/%s; want True/%s", na.Status, na.Reason, ReasonNotApplicable)
	}

	// nil CR is also NotApplicable (Evaluate stays total).
	nilCR := evalSentinelTopology(Observation{})
	if nilCR.Status != metav1.ConditionTrue || nilCR.Reason != ReasonNotApplicable {
		t.Errorf("nil CR = %v/%s; want True/%s", nilCR.Status, nilCR.Reason, ReasonNotApplicable)
	}
}

// TestSentinelTopologyDetail pins the deterministic dimension-list
// builder shared by the condition message and the event note: only
// positive-deficit dimensions, fixed order sentinels-then-replicas.
func TestSentinelTopologyDetail(t *testing.T) {
	cases := []struct {
		name       string
		sDef, rDef int
		want       string
	}{
		{"both", 1, 2, "sentinels short by 1 (peer-gossip gap); replicas short by 2"},
		{"sentinels only", 2, 0, "sentinels short by 2 (peer-gossip gap)"},
		{"replicas only", 0, 3, "replicas short by 3"},
		{"none", 0, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SentinelTopologyDetail(tc.sDef, tc.rDef); got != tc.want {
				t.Errorf("SentinelTopologyDetail(%d,%d) = %q; want %q", tc.sDef, tc.rDef, got, tc.want)
			}
		})
	}
}
