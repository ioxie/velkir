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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// deprecationTestScheme builds a minimal scheme for the deprecation
// observation tests: corev1 (Secret, Event) + valkeyv1beta1 (CR).
func deprecationTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 add to scheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1 add to scheme: %v", err)
	}
	return s
}

// crForDeprecationTest returns a CR with a synthetic test annotation
// that the test registry's predicate keys on. Mode=standalone keeps
// the CR shape minimal — the deprecation sweep is mode-agnostic.
func crForDeprecationTest(name string, withMarker bool) *valkeyv1beta1.Valkey {
	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
		},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
		},
	}
	if withMarker {
		cr.Annotations = map[string]string{
			"velkir.ioxie.dev/test-deprecated-marker": "yes",
		}
	}
	return cr
}

// testDeprecations is the synthetic registry the tests inject. The
// predicate keys on a test-only annotation so production CRs (which
// never set it) don't accidentally fire. ProductionDeprecations is
// empty today; the framework is exercised here without polluting the
// production sweep.
var testDeprecations = []FieldDeprecation{
	{
		Path:          "metadata.annotations[\"velkir.ioxie.dev/test-deprecated-marker\"]",
		RemovalWindow: "removed-in-v0.4 (test fixture)",
		Predicate: func(v *valkeyv1beta1.Valkey) bool {
			return v != nil && v.Annotations["velkir.ioxie.dev/test-deprecated-marker"] == "yes"
		},
	},
}

// drainDeprecationRecorder pulls every queued event from the
// FakeRecorder. Mirrors the helper in short_password_emit_test.go
// because FakeRecorder's Events channel is consumed locally per
// test.
func drainDeprecationRecorder(rec *k8sevents.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

// TestCheckDeprecations_EmitsOnPredicateMatch pins the basic emission
// path: a CR whose annotation matches the registry predicate produces
// exactly one FieldDeprecated event on the wired recorder.
func TestCheckDeprecations_EmitsOnPredicateMatch(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr := crForDeprecationTest("vk", true)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:       c,
		Scheme:       scheme,
		Recorder:     rec,
		Deprecator:   events.NewDeprecator(rec),
		Deprecations: testDeprecations,
	}

	r.checkDeprecations(cr)
	got := drainDeprecationRecorder(rec)
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], string(events.FieldDeprecated)) {
		t.Errorf("event missing FieldDeprecated reason: %q", got[0])
	}
	if !strings.Contains(got[0], "test-deprecated-marker") {
		t.Errorf("event missing field path: %q", got[0])
	}
	if !strings.Contains(got[0], "removed-in-v0.4") {
		t.Errorf("event missing removal window: %q", got[0])
	}
	if !strings.Contains(got[0], corev1.EventTypeNormal) {
		t.Errorf("event should be Normal type: %q", got[0])
	}
}

// TestCheckDeprecations_NoEmitOnPredicateMiss pins the steady-state
// path: a CR without the deprecated marker produces zero events. This
// also covers production behaviour today — ProductionDeprecations is
// empty, and even with the test registry installed, a CR without the
// marker is a no-op.
func TestCheckDeprecations_NoEmitOnPredicateMiss(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr := crForDeprecationTest("vk", false)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:       c,
		Scheme:       scheme,
		Recorder:     rec,
		Deprecator:   events.NewDeprecator(rec),
		Deprecations: testDeprecations,
	}

	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("expected 0 events on predicate miss, got %d: %v", len(got), got)
	}
}

// TestCheckDeprecations_DedupAcrossReconciles is the load-bearing
// integration assertion: the second observation of the
// same (CR, field) tuple MUST NOT produce a duplicate event. This
// exercises the dedup contract at the reconciler-integration level
// (Deprecator field on the reconciler + injected registry + wired
// recorder), not just at the unit level in
// internal/events/deprecation_test.go.
func TestCheckDeprecations_DedupAcrossReconciles(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr := crForDeprecationTest("vk", true)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:       c,
		Scheme:       scheme,
		Recorder:     rec,
		Deprecator:   events.NewDeprecator(rec),
		Deprecations: testDeprecations,
	}

	// First observation — emit fires.
	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 1 {
		t.Fatalf("first observation: expected 1 event, got %d: %v", len(got), got)
	}

	// Second observation of the same CR + same predicate — MUST be
	// deduped. This mirrors the situation where controller-runtime
	// requeues the same CR within one process lifetime.
	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("second observation MUST be deduped, got %d events: %v", len(got), got)
	}

	// Third observation, same tuple — still deduped (dedup is durable
	// across the process lifetime, not just one pair of calls).
	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("third observation MUST be deduped, got %d events: %v", len(got), got)
	}
}

// TestCheckDeprecations_SeparateCRsBothEmit pins the per-(CR, field)
// scope of the dedup: two distinct CRs both carrying the marker each
// emit once.
func TestCheckDeprecations_SeparateCRsBothEmit(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr1 := crForDeprecationTest("vk1", true)
	cr2 := crForDeprecationTest("vk2", true)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr1, cr2).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:       c,
		Scheme:       scheme,
		Recorder:     rec,
		Deprecator:   events.NewDeprecator(rec),
		Deprecations: testDeprecations,
	}

	r.checkDeprecations(cr1)
	r.checkDeprecations(cr2)
	if got := drainDeprecationRecorder(rec); len(got) != 2 {
		t.Errorf("two distinct CRs should each emit once: got %d events: %v", len(got), got)
	}
}

// TestReconcileDeletion_ClearsDeprecationDedup is the dedup-on-delete
// integration assertion: when reconcileDeletion runs, the Deprecator's
// per-CR entries must be Forgotten so a recreated CR with the same
// identity re-emits on its first observation rather than inheriting
// the prior CR's silenced state.
func TestReconcileDeletion_ClearsDeprecationDedup(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr := crForDeprecationTest("vk", true)
	// reconcileDeletion expects the PVC-retention finalizer to be
	// present so it exercises the finalizer-removal branch (and the
	// Forget calls that follow it).
	cr.Finalizers = []string{PVCRetentionFinalizer}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	reporter := events.NewShortAuthPasswordReporter(rec)
	deprecator := events.NewDeprecator(rec)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: reporter,
		Deprecator:                deprecator,
		Deprecations:              testDeprecations,
	}

	// Step 1: first observation emits + seeds the dedup entry.
	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 1 {
		t.Fatalf("expected 1 event after first observation, got %d", len(got))
	}

	// Step 2: reconcileDeletion runs — must Forget the dedup entry.
	if err := r.reconcileDeletion(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}

	// Step 3: a recreated CR with the same name+namespace MUST
	// re-emit on its first observation (dedup cleared by Forget).
	cr2 := crForDeprecationTest("vk", true)
	r.checkDeprecations(cr2)
	if got := drainDeprecationRecorder(rec); len(got) != 1 {
		t.Errorf("recreated CR with same identity must re-emit (Forget should have cleared dedup), got %d events: %v", len(got), got)
	}
}

// TestCheckDeprecations_NilDeprecatorIsNoop pins the nil-safety
// contract: a reconciler constructed without a Deprecator (the common
// shape for unit tests not exercising the observer) must not panic
// when checkDeprecations is called.
func TestCheckDeprecations_NilDeprecatorIsNoop(t *testing.T) {
	cr := crForDeprecationTest("vk", true)
	r := &ValkeyReconciler{
		Deprecations: testDeprecations,
		// Deprecator: nil
	}
	r.checkDeprecations(cr) // must not panic
}

// TestCheckDeprecations_EmptyRegistryIsNoop pins production behaviour
// today: ProductionDeprecations is empty, so the per-reconcile sweep
// produces zero events regardless of CR shape. If a future change
// accidentally flips the empty-registry path to emit (e.g., a default
// "all fields are deprecated" predicate slip), this test catches it.
func TestCheckDeprecations_EmptyRegistryIsNoop(t *testing.T) {
	cr := crForDeprecationTest("vk", true)
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Recorder:     rec,
		Deprecator:   events.NewDeprecator(rec),
		Deprecations: ProductionDeprecations,
	}

	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("empty production registry must produce zero events, got %d: %v", len(got), got)
	}
}

// TestCheckDeprecations_MultipleMatchingPredicatesAllFire pins the
// registry-loop contract: a single CR that matches MULTIPLE registry
// entries on the same reconcile pass produces one event per matching
// entry (each (CR, Path) tuple has its own dedup slot). Without this
// test the loop body could regress to break-on-first-match and the
// dedup invariant would still pass the single-entry tests.
func TestCheckDeprecations_MultipleMatchingPredicatesAllFire(t *testing.T) {
	scheme := deprecationTestScheme(t)
	cr := crForDeprecationTest("vk", true)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:     c,
		Scheme:     scheme,
		Recorder:   rec,
		Deprecator: events.NewDeprecator(rec),
		Deprecations: []FieldDeprecation{
			{
				Path:          "spec.fieldA",
				RemovalWindow: "removed-in-v0.4",
				Predicate:     func(*valkeyv1beta1.Valkey) bool { return true },
			},
			{
				Path:          "spec.fieldB",
				RemovalWindow: "removed-in-v0.5",
				Predicate:     func(*valkeyv1beta1.Valkey) bool { return true },
			},
		},
	}

	// First pass: both matching predicates fire — one event per path.
	r.checkDeprecations(cr)
	got := drainDeprecationRecorder(rec)
	if len(got) != 2 {
		t.Fatalf("expected 2 events (one per matching predicate), got %d: %v", len(got), got)
	}

	// Second pass: both paths are now in the dedup set; zero new events.
	// This proves the per-Path dedup composes correctly across the loop —
	// not just per-CR.
	r.checkDeprecations(cr)
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("second pass MUST dedup all matching paths, got %d events: %v", len(got), got)
	}
}

// TestCheckDeprecations_NilPredicateSkipped pins the registry guard:
// a malformed entry with a nil Predicate must be skipped rather than
// panicking or always firing.
func TestCheckDeprecations_NilPredicateSkipped(t *testing.T) {
	cr := crForDeprecationTest("vk", true)
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Recorder:   rec,
		Deprecator: events.NewDeprecator(rec),
		Deprecations: []FieldDeprecation{
			{Path: "spec.broken", RemovalWindow: "removed-in-v0.4", Predicate: nil},
		},
	}

	r.checkDeprecations(cr) // must not panic
	if got := drainDeprecationRecorder(rec); len(got) != 0 {
		t.Errorf("nil-predicate entry must be skipped silently, got %d events: %v", len(got), got)
	}
}
