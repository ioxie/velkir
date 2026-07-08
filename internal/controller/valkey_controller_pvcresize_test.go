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
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// pvcResizeTestScheme is the scheme the fake client recognises:
// just core (ConfigMap is unused but Status patches need the
// scheme to resolve Valkey) plus the project's v1beta1.
func pvcResizeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme.AddToScheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1.AddToScheme: %v", err)
	}
	return s
}

// crWithPVCResize builds a minimal Valkey CR carrying the supplied
// PVCResize substate. Used by the helper-unit tests so each case
// only spells out the fields under inspection.
func crWithPVCResize(phase valkeyv1beta1.PVCResizePhase, lastTransition *metav1.Time, attempt int32) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk0"
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		PVCResize: &valkeyv1beta1.PVCResizeStatus{
			Phase:            string(phase),
			Attempt:          attempt,
			LastTransitionAt: lastTransition,
		},
	}
	return v
}

func TestPVCResizeIsStalled(t *testing.T) {
	now := metav1.Now()
	tenMinAgo := metav1.NewTime(now.Add(-11 * time.Minute))
	freshTime := metav1.NewTime(now.Add(-30 * time.Second))
	cases := []struct {
		name string
		v    *valkeyv1beta1.Valkey
		want bool
	}{
		{"absent rollout: not stalled", &valkeyv1beta1.Valkey{}, false},
		{"Aborted (terminal): not stalled (uses backoff path instead)",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseAborted, &tenMinAgo, 1), false},
		{"Verified (in flight per current taxonomy) past threshold: stalled",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseVerified, &tenMinAgo, 1), true},
		{"StsOrphaned past threshold: stalled",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseStsOrphaned, &tenMinAgo, 1), true},
		{"PVCsPatched fresh: not stalled",
			crWithPVCResize(valkeyv1beta1.PVCResizePhasePVCsPatched, &freshTime, 1), false},
		{"in-flight phase with nil LastTransitionAt: not stalled",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseValidated, nil, 1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pvcResizeIsStalled(tc.v); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPVCResizeAbortedWaitRemaining(t *testing.T) {
	now := metav1.Now()
	tenSecondsAgo := metav1.NewTime(now.Add(-10 * time.Second))
	twoMinAgo := metav1.NewTime(now.Add(-2 * time.Minute))

	cases := []struct {
		name        string
		v           *valkeyv1beta1.Valkey
		wantNonZero bool
	}{
		{"absent rollout returns zero", &valkeyv1beta1.Valkey{}, false},
		{"nil LastTransitionAt returns zero",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseAborted, nil, 1), false},
		{"attempt 1 fresh Aborted has remaining wait",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseAborted, &tenSecondsAgo, 1), true},
		{"attempt 1 past 1m has zero remaining",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseAborted, &twoMinAgo, 1), false},
		{"attempt 4 fresh Aborted has cap remaining",
			crWithPVCResize(valkeyv1beta1.PVCResizePhaseAborted, &tenSecondsAgo, 4), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pvcResizeAbortedWaitRemaining(tc.v)
			if (got > 0) != tc.wantNonZero {
				t.Errorf("got remaining=%v (nonZero=%v), wantNonZero=%v", got, got > 0, tc.wantNonZero)
			}
		})
	}
}

func TestMaybeEmitPVCStuckEvent_FiresOnce(t *testing.T) {
	s := pvcResizeTestScheme(t)
	stalledStart := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	v := crWithPVCResize(valkeyv1beta1.PVCResizePhaseStsOrphaned, &stalledStart, 1)

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Client: c, Recorder: rec}

	if err := r.maybeEmitPVCStuckEvent(context.Background(), v); err != nil {
		t.Fatalf("first call: %v", err)
	}
	got := drainEventsForStuck(rec)
	if len(got) != 1 || !strings.Contains(got[0], "PVCExpansionStuck") {
		t.Fatalf("first emission missing or wrong: %v", got)
	}
	// Second call within the same stall window should be a no-op.
	if err := r.maybeEmitPVCStuckEvent(context.Background(), v); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := drainEventsForStuck(rec); len(got) != 0 {
		t.Errorf("second emission should have been suppressed (LastStuckEventAt dedup), got: %v", got)
	}

	// Verify LastStuckEventAt was persisted on the in-memory CR.
	if v.Status.Rollout.PVCResize.LastStuckEventAt == nil {
		t.Error("LastStuckEventAt should have been stamped on the CR")
	}
}

func TestMaybeEmitPVCStuckEvent_ReEmitsAfterPhaseAdvance(t *testing.T) {
	s := pvcResizeTestScheme(t)
	earlierStall := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	v := crWithPVCResize(valkeyv1beta1.PVCResizePhaseStsOrphaned, &earlierStall, 1)
	// Pre-stamp LastStuckEventAt to indicate "we already emitted for
	// the earlier stall", then advance LastTransitionAt as if the
	// substate moved to a fresh PVCsPatched phase that ALSO stalled.
	priorEmit := metav1.NewTime(earlierStall.Add(time.Second))
	v.Status.Rollout.PVCResize.LastStuckEventAt = &priorEmit

	freshStall := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	v.Status.Rollout.PVCResize.Phase = string(valkeyv1beta1.PVCResizePhasePVCsPatched)
	v.Status.Rollout.PVCResize.LastTransitionAt = &freshStall

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Client: c, Recorder: rec}

	if err := r.maybeEmitPVCStuckEvent(context.Background(), v); err != nil {
		t.Fatalf("emit: %v", err)
	}
	got := drainEventsForStuck(rec)
	if len(got) != 1 || !strings.Contains(got[0], "PVCExpansionStuck") {
		t.Errorf("expected fresh emission after phase advance, got: %v", got)
	}
}

func drainEventsForStuck(rec *k8sevents.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// Compile-time check on the corev1 import — the fake client builder
// path uses it via WithObjects implicitly. Keeping the import gives
// future test additions a Pod / ConfigMap fixture to lean on
// without re-importing.
var _ = corev1.NamespaceDefault
