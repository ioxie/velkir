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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

func minimalCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "default", Generation: 1},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
		},
	}
}

// stsWithReady builds a synthetic STS with `desired` replicas, of which
// `ready` are reporting Ready. The status evaluators never read the
// Spec.Selector / Spec.ServiceName / Spec.Template fields, so the
// minimal shape here is enough to drive the Ready / Available /
// Progressing branches deterministically.
//
//nolint:unparam // desired stays at 1 while only standalone-mode CRs ship; replication tests will pass higher values
func stsWithReady(desired, ready int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		Spec:   appsv1.StatefulSetSpec{Replicas: new(desired)},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: ready, Replicas: desired},
	}
}

// TestConditionMessageStability pins the canonical-message contract:
// each (condition type, reason) pair maps to a stable canonical message.
// Re-running Evaluate against the same observation produces
// byte-identical messages — without this pin, a regression where a
// reason gets a freshly-formatted message on every reconcile would
// flap LastTransitionTime via meta.SetStatusCondition's "Reason
// changed → Status unchanged is fine but Message change still
// updates" path, producing perpetual status churn.
func TestConditionMessageStability(t *testing.T) {
	cases := []struct {
		name string
		obs  Observation
	}{
		{"no-sts", Observation{CR: minimalCR()}},
		{"sts-not-ready", Observation{CR: minimalCR(), STS: stsWithReady(1, 0)}},
		{"sts-fully-ready", Observation{CR: minimalCR(), STS: stsWithReady(1, 1)}},
		{"reconcile-error", Observation{CR: minimalCR(), STS: stsWithReady(1, 1), ReconcileError: errBoom{}}},
		{"paused", Observation{CR: minimalCR(), Paused: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, _ := Evaluate(tc.obs)
			second, _ := Evaluate(tc.obs)
			third, _ := Evaluate(tc.obs)

			// Compare by (type, message) tuple — if a reason
			// changes the message string but the rest holds, this
			// catches it. LastTransitionTime is intentionally
			// omitted (it's set by meta.SetStatusCondition, not
			// by the evaluators).
			tup := func(conds []metav1.Condition) []string {
				out := make([]string, 0, len(conds))
				for _, c := range conds {
					out = append(out, c.Type+"|"+string(c.Status)+"|"+c.Reason+"|"+c.Message)
				}
				sort.Strings(out)
				return out
			}
			a, b, c := tup(first), tup(second), tup(third)
			if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(b, c) {
				t.Errorf("messages drifted across calls\nrun 1: %v\nrun 2: %v\nrun 3: %v", a, b, c)
			}
		})
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// TestBootstrapCompleteLatch pins the latch contract: once
// BootstrapComplete reaches True (PromotedFromBootstrap), nothing
// the operator observes after that flips it back. Quorum loss,
// reconcile errors, STS removal — all must leave it True.
func TestBootstrapCompleteLatch(t *testing.T) {
	cr := minimalCR()
	cr.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{Host: "primary.example.com", Port: 6379}

	// Simulate the prior reconcile having stamped True/PromotedFromBootstrap.
	cr.Status.Conditions = []metav1.Condition{{
		Type:    TypeBootstrapComplete,
		Status:  metav1.ConditionTrue,
		Reason:  ReasonBootstrapPromoted,
		Message: "bootstrap import completed; CR is now self-hosted",
	}}

	// Now feed adverse observations and assert the latch holds.
	adverse := []Observation{
		{CR: cr, STS: nil, ReconcileError: errBoom{}},          // reconcile error
		{CR: cr, STS: stsWithReady(1, 0)},                      // STS not ready
		{CR: cr, STS: stsWithReady(1, 1), ReconcileError: nil}, // back to healthy
		{CR: cr, Paused: true},                                 // paused
	}
	for i, obs := range adverse {
		conds, _ := Evaluate(obs)
		bc := findCondition(conds, TypeBootstrapComplete)
		if bc == nil {
			t.Fatalf("case %d: BootstrapComplete missing from output", i)
		}
		if bc.Status != metav1.ConditionTrue {
			t.Errorf("case %d: BootstrapComplete = %s; want True (latched)", i, bc.Status)
		}
		if bc.Reason != ReasonBootstrapPromoted {
			t.Errorf("case %d: BootstrapComplete reason = %s; want %s", i, bc.Reason, ReasonBootstrapPromoted)
		}
	}
}

func TestBootstrapComplete_NoBootstrapConfigured_LatchedTrue(t *testing.T) {
	conds, _ := Evaluate(Observation{CR: minimalCR()})
	bc := findCondition(conds, TypeBootstrapComplete)
	if bc.Status != metav1.ConditionTrue || bc.Reason != ReasonBootstrapNotConfigured {
		t.Errorf("got %s/%s; want True/BootstrapNotConfigured", bc.Status, bc.Reason)
	}
}

func TestBootstrapComplete_BootstrapConfigured_StartsReplicating(t *testing.T) {
	cr := minimalCR()
	cr.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{Host: "p", Port: 6379}
	conds, _ := Evaluate(Observation{CR: cr})
	bc := findCondition(conds, TypeBootstrapComplete)
	if bc.Status != metav1.ConditionFalse || bc.Reason != ReasonBootstrapReplicating {
		t.Errorf("got %s/%s; want False/Replicating", bc.Status, bc.Reason)
	}
}

func TestPhase_Pending_NoSTS(t *testing.T) {
	_, phase := Evaluate(Observation{CR: minimalCR()})
	if phase != PhasePending {
		t.Errorf("phase = %s; want %s", phase, PhasePending)
	}
}

func TestPhase_Ready_AllReplicasReady(t *testing.T) {
	_, phase := Evaluate(Observation{CR: minimalCR(), STS: stsWithReady(1, 1)})
	if phase != PhaseReady {
		t.Errorf("phase = %s; want %s", phase, PhaseReady)
	}
}

func TestPhase_Progressing_NoReplicasReady(t *testing.T) {
	_, phase := Evaluate(Observation{CR: minimalCR(), STS: stsWithReady(1, 0)})
	if phase != PhaseProgressing {
		t.Errorf("phase = %s; want %s", phase, PhaseProgressing)
	}
}

func TestPhase_Paused(t *testing.T) {
	_, phase := Evaluate(Observation{CR: minimalCR(), Paused: true})
	if phase != PhasePaused {
		t.Errorf("phase = %s; want %s", phase, PhasePaused)
	}
}

func TestPhase_Degraded_OnReconcileError(t *testing.T) {
	_, phase := Evaluate(Observation{CR: minimalCR(), STS: stsWithReady(1, 1), ReconcileError: errBoom{}})
	if phase != PhaseDegraded {
		t.Errorf("phase = %s; want %s", phase, PhaseDegraded)
	}
}

// TestDerivePhase_ReconciledFalse_IsDegraded pins the defensive
// decoupling: derivePhase surfaces a reconcile failure as
// PhaseDegraded by consulting Reconciled=False directly, so a failing
// reconcile can never read as a healthy phase even if Degraded is not
// True. evalDegraded folds ReconcileError into Degraded=True today, so
// this hand-builds the condition tuple (Reconciled=False, Degraded=
// False, Ready=True) to isolate the new arm — without it, the
// Ready=True branch would derive PhaseReady.
func TestDerivePhase_ReconciledFalse_IsDegraded(t *testing.T) {
	conds := []metav1.Condition{
		{Type: TypeReconciled, Status: metav1.ConditionFalse},
		{Type: TypeDegraded, Status: metav1.ConditionFalse},
		{Type: TypeReady, Status: metav1.ConditionTrue},
		{Type: TypeAvailable, Status: metav1.ConditionTrue},
	}
	if got := derivePhase(conds, Observation{CR: minimalCR(), STS: stsWithReady(1, 1)}); got != PhaseDegraded {
		t.Errorf("derivePhase with Reconciled=False = %s; want %s", got, PhaseDegraded)
	}
}

// subHASentinelCR builds the minimal sentinel-mode CR shape that
// triggers the HANotMet path: mode=sentinel + replicas below the
// 2-replica failover floor. The validating webhook accepts this
// shape with a Warning at admission; the tests pin the runtime
// counterpart on `Conditions[type=Degraded]`.
func subHASentinelCR(replicas int32) *valkeyv1beta1.Valkey {
	cr := minimalCR()
	cr.Spec.Mode = valkeyv1beta1.ModeSentinel
	cr.Spec.Valkey.Replicas = replicas
	return cr
}

func TestDegraded_HANotMet_FiresOnSubHASentinel(t *testing.T) {
	cases := []int32{0, 1}
	for _, r := range cases {
		t.Run(fmt.Sprintf("replicas=%d", r), func(t *testing.T) {
			conds, phase := Evaluate(Observation{CR: subHASentinelCR(r)})
			deg := findCondition(conds, TypeDegraded)
			if deg == nil {
				t.Fatal("Degraded condition missing")
			}
			if deg.Status != metav1.ConditionTrue {
				t.Errorf("Degraded.Status = %s; want True", deg.Status)
			}
			if deg.Reason != ReasonHANotMet {
				t.Errorf("Degraded.Reason = %s; want %s", deg.Reason, ReasonHANotMet)
			}
			if phase != PhaseDegraded {
				t.Errorf("phase = %s; want %s", phase, PhaseDegraded)
			}
		})
	}
}

func TestDegraded_HANotMet_ClearsAtTwoReplicas(t *testing.T) {
	conds, _ := Evaluate(Observation{CR: subHASentinelCR(2)})
	deg := findCondition(conds, TypeDegraded)
	if deg == nil {
		t.Fatal("Degraded condition missing")
	}
	if deg.Status != metav1.ConditionFalse {
		t.Errorf("Degraded.Status = %s; want False at replicas=2", deg.Status)
	}
	if deg.Reason != ReasonAsExpected {
		t.Errorf("Degraded.Reason = %s; want %s at replicas=2", deg.Reason, ReasonAsExpected)
	}
}

func TestDegraded_HANotMet_NotApplicableToStandalone(t *testing.T) {
	// A standalone CR with any replica count must NOT trigger the
	// HA gap — the gap is sentinel-mode-only because there's no
	// failover quorum to satisfy in standalone mode. Standalone
	// replicas=1 is the canonical happy path.
	conds, _ := Evaluate(Observation{CR: minimalCR()})
	deg := findCondition(conds, TypeDegraded)
	if deg == nil {
		t.Fatal("Degraded condition missing")
	}
	if deg.Status != metav1.ConditionFalse {
		t.Errorf("standalone Degraded.Status = %s; want False", deg.Status)
	}
	if deg.Reason == ReasonHANotMet {
		t.Errorf("standalone CR should never carry Reason=HANotMet")
	}
}

func TestDegraded_HANotMet_OutprecedencedByReconcileError(t *testing.T) {
	// Both a sub-HA sentinel config AND a runtime reconcile error
	// hold simultaneously. The runtime fault is more actionable
	// (transient, operator can fix it) than the static config gap
	// (permanent until user patches replicas), so ReconcileError
	// must win the precedence ladder.
	conds, _ := Evaluate(Observation{
		CR:             subHASentinelCR(1),
		ReconcileError: errBoom{},
	})
	deg := findCondition(conds, TypeDegraded)
	if deg.Reason != ReasonReconcileErr {
		t.Errorf("Degraded.Reason = %s; want %s (ReconcileError outprecedences HANotMet)",
			deg.Reason, ReasonReconcileErr)
	}
}

func TestReplicationHealthy_StandaloneIsAlwaysTrue(t *testing.T) {
	conds, _ := Evaluate(Observation{CR: minimalCR()})
	rh := findCondition(conds, TypeReplicationHealthy)
	if rh.Status != metav1.ConditionTrue || rh.Reason != ReasonNotApplicable {
		t.Errorf("got %s/%s; want True/NotApplicable", rh.Status, rh.Reason)
	}
}

// TestReplicationHealthy_NonStandalone_NeverUnknown pins the
// condition resolves to a definite value for replication/sentinel
// modes from the per-pod ReplicationReadyGate aggregate. It previously
// hung at Unknown, which deadlocks
// `kubectl wait --for=condition=ReplicationHealthy` indefinitely.
func TestReplicationHealthy_NonStandalone_NeverUnknown(t *testing.T) {
	cases := []struct {
		name       string
		mode       valkeyv1beta1.Mode
		replicas   int32
		repl       ReplicationObservation
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{"gate disabled", valkeyv1beta1.ModeReplication, 2, ReplicationObservation{GateEnabled: false}, metav1.ConditionTrue, ReasonNotApplicable},
		{"replicas=1 no peers", valkeyv1beta1.ModeReplication, 1, ReplicationObservation{GateEnabled: true}, metav1.ConditionTrue, ReasonNotApplicable},
		{"all replicas ready", valkeyv1beta1.ModeReplication, 3, ReplicationObservation{GateEnabled: true, ReadyReplicas: 2}, metav1.ConditionTrue, ReasonAsExpected},
		{"some replica waiting", valkeyv1beta1.ModeReplication, 3, ReplicationObservation{GateEnabled: true, ReadyReplicas: 1}, metav1.ConditionFalse, ReasonReplicaWait},
		{"sentinel all ready", valkeyv1beta1.ModeSentinel, 3, ReplicationObservation{GateEnabled: true, ReadyReplicas: 2}, metav1.ConditionTrue, ReasonAsExpected},
		{"sentinel waiting", valkeyv1beta1.ModeSentinel, 3, ReplicationObservation{GateEnabled: true, ReadyReplicas: 0}, metav1.ConditionFalse, ReasonReplicaWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := minimalCR()
			cr.Spec.Mode = tc.mode
			cr.Spec.Valkey.Replicas = tc.replicas
			conds, _ := Evaluate(Observation{CR: cr, Replication: tc.repl})
			rh := findCondition(conds, TypeReplicationHealthy)
			if rh.Status == metav1.ConditionUnknown {
				t.Fatalf("ReplicationHealthy must never be Unknown for %s mode (#514)", tc.mode)
			}
			if rh.Status != tc.wantStatus || rh.Reason != tc.wantReason {
				t.Errorf("got %s/%s; want %s/%s", rh.Status, rh.Reason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestDegradedFlippedFalse_DetectsRecovery(t *testing.T) {
	prior := []metav1.Condition{{Type: TypeDegraded, Status: metav1.ConditionTrue}}
	current := []metav1.Condition{{Type: TypeDegraded, Status: metav1.ConditionFalse}}
	if !DegradedFlippedFalse(prior, current) {
		t.Error("expected DegradedFlippedFalse to detect True->False transition")
	}
}

func TestDegradedFlippedFalse_IgnoresStableFalse(t *testing.T) {
	conds := []metav1.Condition{{Type: TypeDegraded, Status: metav1.ConditionFalse}}
	if DegradedFlippedFalse(conds, conds) {
		t.Error("DegradedFlippedFalse should not fire when prior was already False")
	}
}

func TestEvaluate_StampsObservedGeneration(t *testing.T) {
	cr := minimalCR()
	cr.Generation = 42
	conds, _ := Evaluate(Observation{CR: cr})
	for _, c := range conds {
		if c.ObservedGeneration != 42 {
			t.Errorf("%s ObservedGeneration = %d; want 42", c.Type, c.ObservedGeneration)
		}
	}
}

// TestEvaluate_NilCR_DoesNotPanic pins the nil-CR guard: the
// ObservedGeneration stamp loop (and every evaluator) must tolerate a
// nil o.CR without panicking. o.CR is non-nil in production, but the
// deferred status closure must never panic on a defensive nil — a
// panic there would crash the reconcile goroutine. With no CR there is
// no generation to observe, so ObservedGeneration stays 0.
func TestEvaluate_NilCR_DoesNotPanic(t *testing.T) {
	conds, _ := Evaluate(Observation{CR: nil})
	if len(conds) == 0 {
		t.Fatal("Evaluate returned no conditions for a nil CR")
	}
	for _, c := range conds {
		if c.ObservedGeneration != 0 {
			t.Errorf("%s ObservedGeneration = %d; want 0 (nil CR has no generation)", c.Type, c.ObservedGeneration)
		}
	}
}

// TestDegraded_RolloutStalled pins the watchdog-driven Degraded
// branch: an Active+Expired watchdog flips Degraded=True with
// reason=RolloutStalled, takes precedence over a
// concurrent reconcile error (the watchdog message is more
// actionable), and surfaces the offending pod name + deadline in
// the message so a `kubectl describe` reader sees the cause without
// digging into events.
func TestDegraded_RolloutStalled(t *testing.T) {
	deadline := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	cr := minimalCR()
	cr.Spec.Mode = valkeyv1beta1.ModeReplication
	cr.Spec.Valkey.Replicas = 3

	cases := []struct {
		name       string
		obs        Observation
		wantStatus metav1.ConditionStatus
		wantReason string
		wantInMsg  string
		wantDegPhs bool // expect derivePhase = PhaseDegraded
	}{
		{
			name: "active+expired flips Degraded with RolloutStalled",
			obs: Observation{
				CR:  cr,
				STS: stsWithReady(3, 3),
				RolloutWatchdog: Result{
					Active:   true,
					Expired:  true,
					PodName:  "vk-2",
					Deadline: deadline,
				},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonRolloutStalled,
			wantInMsg:  `pod "vk-2"`,
			wantDegPhs: true,
		},
		{
			name: "active+!expired leaves Degraded False",
			obs: Observation{
				CR:  cr,
				STS: stsWithReady(3, 3),
				RolloutWatchdog: Result{
					Active:   true,
					Expired:  false,
					PodName:  "vk-2",
					Deadline: deadline.Add(5 * time.Minute),
				},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonAsExpected,
		},
		{
			name: "inactive watchdog has no effect",
			obs: Observation{
				CR:              cr,
				STS:             stsWithReady(3, 3),
				RolloutWatchdog: Result{},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonAsExpected,
		},
		{
			name: "watchdog wins over concurrent ReconcileError",
			obs: Observation{
				CR:             cr,
				STS:            stsWithReady(3, 3),
				ReconcileError: errBoom{},
				RolloutWatchdog: Result{
					Active:   true,
					Expired:  true,
					PodName:  "vk-1",
					Deadline: deadline,
				},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonRolloutStalled, // not ReconcileError
			wantInMsg:  `pod "vk-1"`,
			wantDegPhs: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conds, phase := Evaluate(tc.obs)
			d := findCondition(conds, TypeDegraded)
			if d == nil {
				t.Fatal("Degraded condition missing from output")
			}
			if d.Status != tc.wantStatus {
				t.Errorf("Status = %s; want %s", d.Status, tc.wantStatus)
			}
			if d.Reason != tc.wantReason {
				t.Errorf("Reason = %s; want %s", d.Reason, tc.wantReason)
			}
			if tc.wantInMsg != "" && !contains(d.Message, tc.wantInMsg) {
				t.Errorf("Message = %q; want to contain %q", d.Message, tc.wantInMsg)
			}
			if tc.wantDegPhs && phase != PhaseDegraded {
				t.Errorf("phase = %s; want %s when watchdog expired", phase, PhaseDegraded)
			}
		})
	}
}

// TestDegraded_SplitBrain pins the observer-driven Degraded branch:
// SplitBrainActive=true flips Degraded=True with reason=SplitBrain,
// holds its precedence across the ladder, and clears the moment the
// observer re-publishes a QuorumOK=true snapshot.
func TestDegraded_SplitBrain(t *testing.T) {
	deadline := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	sentinelCR := minimalCR()
	sentinelCR.Spec.Mode = valkeyv1beta1.ModeSentinel
	sentinelCR.Spec.Valkey.Replicas = 3

	cases := []struct {
		name       string
		obs        Observation
		wantStatus metav1.ConditionStatus
		wantReason string
		wantInMsg  string
		wantNotMsg string
		wantPhase  string
	}{
		{
			// The mutation-detection case for the SplitBrain
			// branch: SplitBrainActive=true on an otherwise-healthy
			// sentinel CR means no other precedence rule fires —
			// only the SplitBrain branch can produce reason=SplitBrain
			// here, so deleting `if o.SplitBrainActive { ... }` from
			// evalDegraded would fall through to AsExpected and this
			// case would fail.
			name:       "split-brain alone flips Degraded with reason=SplitBrain",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), SplitBrainActive: true},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonSplitBrain,
			wantInMsg:  "quorum lost",
			wantPhase:  PhaseDegraded,
		},
		{
			// Negative pin: SplitBrainActive=false on the same
			// healthy CR must NOT carry the SplitBrain message.
			// Catches a regression where the branch fires
			// unconditionally.
			name:       "split-brain inactive leaves Degraded False on healthy sentinel CR",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), SplitBrainActive: false},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonAsExpected,
			wantNotMsg: "quorum lost",
			wantPhase:  PhaseReady,
		},
		{
			name: "RolloutStalled outprecedences SplitBrain — watchdog message is more actionable",
			obs: Observation{
				CR:               sentinelCR,
				STS:              stsWithReady(3, 3),
				SplitBrainActive: true,
				RolloutWatchdog: Result{
					Active:   true,
					Expired:  true,
					PodName:  "vk-2",
					Deadline: deadline,
				},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonRolloutStalled,
			wantInMsg:  `pod "vk-2"`,
			wantNotMsg: "quorum lost",
			wantPhase:  PhaseDegraded,
		},
		{
			name: "ReconcileError outprecedences SplitBrain — runtime fault > observation-only signal",
			obs: Observation{
				CR:               sentinelCR,
				STS:              stsWithReady(3, 3),
				SplitBrainActive: true,
				ReconcileError:   errBoom{},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonReconcileErr,
			wantNotMsg: "quorum lost",
			wantPhase:  PhaseDegraded,
		},
		{
			name: "SplitBrain outprecedences HANotMet — runtime > static gap",
			obs: Observation{
				CR:               subHASentinelCR(1),
				SplitBrainActive: true,
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonSplitBrain,
			wantInMsg:  "quorum lost",
			wantPhase:  PhaseDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conds, phase := Evaluate(tc.obs)
			d := findCondition(conds, TypeDegraded)
			if d == nil {
				t.Fatal("Degraded condition missing from output")
			}
			if d.Status != tc.wantStatus {
				t.Errorf("Status = %s; want %s", d.Status, tc.wantStatus)
			}
			if d.Reason != tc.wantReason {
				t.Errorf("Reason = %s; want %s", d.Reason, tc.wantReason)
			}
			if tc.wantInMsg != "" && !contains(d.Message, tc.wantInMsg) {
				t.Errorf("Message = %q; want to contain %q", d.Message, tc.wantInMsg)
			}
			if tc.wantNotMsg != "" && contains(d.Message, tc.wantNotMsg) {
				t.Errorf("Message = %q; must NOT contain %q (other branch fired)", d.Message, tc.wantNotMsg)
			}
			if tc.wantPhase != "" && phase != tc.wantPhase {
				t.Errorf("phase = %s; want %s", phase, tc.wantPhase)
			}
		})
	}
}

// TestPrimaryConfirmed pins the three branches of evalPrimaryConfirmed
// NotApplicable for non-sentinel, Unknown for no-fresh-data,
// True/False driven by the aggregator result.
func TestPrimaryConfirmed(t *testing.T) {
	t.Parallel()
	standalone := minimalCR()
	sentinelCR := minimalCR()
	sentinelCR.Spec.Mode = valkeyv1beta1.ModeSentinel

	cases := []struct {
		name       string
		obs        Observation
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{"non-sentinel → NotApplicable",
			Observation{CR: standalone},
			metav1.ConditionTrue, ReasonNotApplicable},
		{"sentinel + zero result → Unknown",
			Observation{CR: sentinelCR},
			metav1.ConditionUnknown, ReasonNoFreshObservation},
		{"sentinel + confirmed",
			Observation{CR: sentinelCR, SentinelQuorum: sqaggregate.Result{
				PrimaryPod: "vk-0", PrimaryConfirmed: true, FreshCount: 3,
			}},
			metav1.ConditionTrue, ReasonPrimaryConfirmed},
		{"sentinel + fresh-but-no-majority",
			Observation{CR: sentinelCR, SentinelQuorum: sqaggregate.Result{
				PrimaryConfirmed: false, FreshCount: 4,
			}},
			metav1.ConditionFalse, ReasonNoPrimaryMajority},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conds, _ := Evaluate(tc.obs)
			pc := findCondition(conds, TypePrimaryConfirmed)
			if pc == nil {
				t.Fatal("PrimaryConfirmed condition missing")
			}
			if pc.Status != tc.wantStatus || pc.Reason != tc.wantReason {
				t.Errorf("got %s/%s; want %s/%s", pc.Status, pc.Reason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

// TestQuorumLost pins the three branches of evalQuorumLost.
func TestQuorumLost(t *testing.T) {
	t.Parallel()
	standalone := minimalCR()
	sentinelCR := minimalCR()
	sentinelCR.Spec.Mode = valkeyv1beta1.ModeSentinel

	cases := []struct {
		name       string
		obs        Observation
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{"non-sentinel → NotApplicable",
			Observation{CR: standalone},
			metav1.ConditionFalse, ReasonNotApplicable},
		{"sentinel + zero result → Unknown",
			Observation{CR: sentinelCR},
			metav1.ConditionUnknown, ReasonNoFreshObservation},
		// Pre-fix, raw aggregator QuorumLost=true drove condition True
		// directly. Post-fix, the condition follows the suppression
		// gate — a single transient NOQUORUM observation (raw=true
		// without the 60s sustained-loss threshold met) is no longer
		// enough to flip the user-visible condition.
		{"sentinel + raw quorum-lost + gate not yet active (transient, below threshold)",
			Observation{CR: sentinelCR, SentinelQuorum: sqaggregate.Result{
				QuorumLost: true, Quorum: sentinel.QuorumStatusLost, FreshCount: 3,
			}},
			metav1.ConditionFalse, ReasonQuorumOK},
		// Sustained NOQUORUM has flipped the suppression gate on; the
		// condition fires True alongside the QuorumLost event.
		{"sentinel + gate active (≥60s sustained NOQUORUM)",
			Observation{
				CR:                      sentinelCR,
				SentinelQuorum:          sqaggregate.Result{QuorumLost: true, FreshCount: 3},
				QuorumSuppressionActive: true,
			},
			metav1.ConditionTrue, ReasonQuorumLost},
		// Recovery: gate still active (2-poll hysteresis hasn't fired
		// yet) but raw aggregator now reports OK on this observation.
		// Stays True so a transient observer flicker doesn't show
		// users a False reading mid-recovery.
		{"sentinel + raw quorum-OK + gate still active (hysteresis hold)",
			Observation{
				CR:                      sentinelCR,
				SentinelQuorum:          sqaggregate.Result{QuorumLost: false, Quorum: sentinel.QuorumStatusOK, FreshCount: 3},
				QuorumSuppressionActive: true,
			},
			metav1.ConditionTrue, ReasonQuorumLost},
		// Observer-unreachable window while the gate is active: no
		// fresh SentinelQuorum records on this poll. The gate must
		// keep the condition True (FreshCount=0 must NOT clear the
		// hysteresis hold) — otherwise a recovery rollout that
		// briefly drops observer reachability would prematurely
		// flip the condition False even though the cluster's gate
		// hasn't yet seen 2 consecutive OK polls.
		{"sentinel + no fresh observation + gate still active (FreshCount=0 hysteresis hold)",
			Observation{
				CR:                      sentinelCR,
				SentinelQuorum:          sqaggregate.Result{QuorumLost: false, FreshCount: 0},
				QuorumSuppressionActive: true,
			},
			metav1.ConditionTrue, ReasonQuorumLost},
		{"sentinel + raw quorum-OK + gate cleared (post-recovery)",
			Observation{CR: sentinelCR, SentinelQuorum: sqaggregate.Result{
				QuorumLost: false, Quorum: sentinel.QuorumStatusOK, FreshCount: 3,
			}},
			metav1.ConditionFalse, ReasonQuorumOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conds, _ := Evaluate(tc.obs)
			ql := findCondition(conds, TypeQuorumLost)
			if ql == nil {
				t.Fatal("QuorumLost condition missing")
			}
			if ql.Status != tc.wantStatus || ql.Reason != tc.wantReason {
				t.Errorf("got %s/%s; want %s/%s", ql.Status, ql.Reason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

// TestDegraded_QuorumLost pins the gated-suppression Degraded arm
// a sentinel CR whose per-CR quorum suppression gate is active
// flips Degraded=True with reason=QuorumLost and phase=Degraded — even
// on a fully-Ready StatefulSet, the exact scenario where a sustained
// quorum loss used to read as Available/Ready. The arm outprecedences
// the observer-snapshot sentinel signals (NoMasterAgreement,
// SplitBrain) and is itself outprecedenced by the runtime faults
// (RolloutStalled, ReconcileError).
func TestDegraded_QuorumLost(t *testing.T) {
	deadline := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	sentinelCR := minimalCR()
	sentinelCR.Spec.Mode = valkeyv1beta1.ModeSentinel
	sentinelCR.Spec.Valkey.Replicas = 3

	cases := []struct {
		name       string
		obs        Observation
		wantStatus metav1.ConditionStatus
		wantReason string
		wantInMsg  string
		wantNotMsg string
		wantPhase  string
	}{
		{
			// The bug scenario: gate active on an
			// otherwise fully-Ready cluster. Pre-fix this read
			// Degraded=False, phase=Ready; the fix surfaces both
			// Degraded=True and phase=Degraded. Also the
			// mutation-detection case — only this arm produces
			// reason=QuorumLost here, so deleting it falls through to
			// AsExpected / PhaseReady and this case fails.
			name:       "gate active on a ready cluster flips Degraded + phase",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), QuorumSuppressionActive: true},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonQuorumLost,
			wantInMsg:  "suppression gate active",
			wantPhase:  PhaseDegraded,
		},
		{
			// Negative pin: gate inactive on the same healthy CR
			// leaves Degraded False and phase Ready.
			name:       "gate inactive leaves Degraded False on healthy sentinel CR",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), QuorumSuppressionActive: false},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonAsExpected,
			wantNotMsg: "suppression gate active",
			wantPhase:  PhaseReady,
		},
		{
			// Mode guard: the flag on a standalone CR must NOT flip
			// Degraded. QuorumSuppressionActive is sentinel-only, and
			// the arm's guard keeps Degraded consistent with the
			// QuorumLost condition (NotApplicable / False here).
			name:       "gate flag on standalone CR is a no-op (mode guard)",
			obs:        Observation{CR: minimalCR(), STS: stsWithReady(1, 1), QuorumSuppressionActive: true},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonAsExpected,
			wantNotMsg: "suppression gate active",
			wantPhase:  PhaseReady,
		},
		{
			name:       "QuorumLost outprecedences SplitBrain — gated suppression is the durable signal",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), QuorumSuppressionActive: true, SplitBrainActive: true},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonQuorumLost,
			wantNotMsg: "quorum lost",
			wantPhase:  PhaseDegraded,
		},
		{
			name:       "QuorumLost outprecedences NoMasterAgreement",
			obs:        Observation{CR: sentinelCR, STS: stsWithReady(3, 3), QuorumSuppressionActive: true, NoMasterAgreementActive: true},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonQuorumLost,
			wantNotMsg: "matches no current valkey pod",
			wantPhase:  PhaseDegraded,
		},
		{
			name: "ReconcileError outprecedences QuorumLost — runtime fault > absorption state",
			obs: Observation{
				CR: sentinelCR, STS: stsWithReady(3, 3),
				QuorumSuppressionActive: true, ReconcileError: errBoom{},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonReconcileErr,
			wantNotMsg: "suppression gate active",
			wantPhase:  PhaseDegraded,
		},
		{
			name: "RolloutStalled outprecedences QuorumLost — watchdog message is more actionable",
			obs: Observation{
				CR: sentinelCR, STS: stsWithReady(3, 3),
				QuorumSuppressionActive: true,
				RolloutWatchdog: Result{
					Active: true, Expired: true, PodName: "vk-1", Deadline: deadline,
				},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonRolloutStalled,
			wantInMsg:  `pod "vk-1"`,
			wantNotMsg: "suppression gate active",
			wantPhase:  PhaseDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conds, phase := Evaluate(tc.obs)
			d := findCondition(conds, TypeDegraded)
			if d == nil {
				t.Fatal("Degraded condition missing from output")
			}
			if d.Status != tc.wantStatus {
				t.Errorf("Status = %s; want %s", d.Status, tc.wantStatus)
			}
			if d.Reason != tc.wantReason {
				t.Errorf("Reason = %s; want %s", d.Reason, tc.wantReason)
			}
			if tc.wantInMsg != "" && !contains(d.Message, tc.wantInMsg) {
				t.Errorf("Message = %q; want to contain %q", d.Message, tc.wantInMsg)
			}
			if tc.wantNotMsg != "" && contains(d.Message, tc.wantNotMsg) {
				t.Errorf("Message = %q; must NOT contain %q (other branch fired)", d.Message, tc.wantNotMsg)
			}
			if tc.wantPhase != "" && phase != tc.wantPhase {
				t.Errorf("phase = %s; want %s", phase, tc.wantPhase)
			}
		})
	}
}

// TestPhase_QuorumLost_IndependentOfDegraded pins derivePhase's
// independent QuorumLost check: even in the defensive case
// where the Degraded condition is False, QuorumLost=True must still
// drive phase=Degraded so the cosmetic phase can never read healthy
// while the FSM is in its quorum-loss absorption state. Built by
// calling derivePhase directly with a hand-shaped tuple — the only way
// to exercise the phase check in isolation from the Degraded arm
// (Evaluate always fires both together).
func TestPhase_QuorumLost_IndependentOfDegraded(t *testing.T) {
	conds := []metav1.Condition{
		{Type: TypeReady, Status: metav1.ConditionTrue},
		{Type: TypeAvailable, Status: metav1.ConditionTrue},
		{Type: TypeDegraded, Status: metav1.ConditionFalse},
		{Type: TypeQuorumLost, Status: metav1.ConditionTrue},
	}
	if got := derivePhase(conds, Observation{STS: stsWithReady(3, 3)}); got != PhaseDegraded {
		t.Errorf("derivePhase = %s; want %s when QuorumLost=True and Degraded=False", got, PhaseDegraded)
	}
	// Negative: QuorumLost=False on the same tuple reads Ready — the
	// check must not fire unconditionally.
	conds[3].Status = metav1.ConditionFalse
	if got := derivePhase(conds, Observation{STS: stsWithReady(3, 3)}); got != PhaseReady {
		t.Errorf("derivePhase = %s; want %s when QuorumLost=False", got, PhaseReady)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
