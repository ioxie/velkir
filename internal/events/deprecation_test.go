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

package events

import (
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sevents "k8s.io/client-go/tools/events"
)

// minObj is the smallest client.Object — embedded TypeMeta + ObjectMeta
// — sufficient to exercise Deprecator without pulling in the v1beta1
// package (which would re-introduce an import cycle through the
// reconciler).
type minObj struct {
	metav1.TypeMeta
	metav1.ObjectMeta
}

func (m *minObj) DeepCopyObject() runtime.Object {
	cp := *m
	return &cp
}

func newCR(ns, name string) *minObj {
	return &minObj{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
	}
}

func TestDeprecator_DeduplicatesSameTuple(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	d := NewDeprecator(rec)

	obj := newCR("ns1", "cr1")

	if !d.Emit(obj, "spec.valkey.legacyFlag", "removed-in-v0.3") {
		t.Fatal("first Emit should return true (event posted)")
	}
	if d.Emit(obj, "spec.valkey.legacyFlag", "removed-in-v0.3") {
		t.Fatal("second Emit on same tuple should return false (deduped)")
	}
	if d.Emit(obj, "spec.valkey.legacyFlag", "removed-in-v0.3") {
		t.Fatal("third Emit on same tuple should return false (deduped)")
	}

	if got := drainRecorder(rec); len(got) != 1 {
		t.Fatalf("recorder got %d events, want 1: %v", len(got), got)
	}
}

func TestDeprecator_SeparateTuplesAllEmit(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	d := NewDeprecator(rec)

	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")
	c := newCR("ns2", "cr1")

	d.Emit(a, "spec.foo", "removed-in-v0.3")
	d.Emit(b, "spec.foo", "removed-in-v0.3") // different name
	d.Emit(c, "spec.foo", "removed-in-v0.3") // different namespace
	d.Emit(a, "spec.bar", "removed-in-v0.4") // different field

	if got := drainRecorder(rec); len(got) != 4 {
		t.Fatalf("recorder got %d events, want 4: %v", len(got), got)
	}
}

func TestDeprecator_ForgetReleasesDedup(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	d := NewDeprecator(rec)

	obj := newCR("ns1", "cr1")

	d.Emit(obj, "spec.foo", "removed-in-v0.3")
	d.Emit(obj, "spec.foo", "removed-in-v0.3") // deduped

	d.Forget(obj)

	if !d.Emit(obj, "spec.foo", "removed-in-v0.3") {
		t.Fatal("after Forget, same tuple should re-emit")
	}

	if got := drainRecorder(rec); len(got) != 2 {
		t.Fatalf("recorder got %d events, want 2: %v", len(got), got)
	}
}

func TestDeprecator_ForgetScopedToOneCR(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	d := NewDeprecator(rec)

	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")

	d.Emit(a, "spec.foo", "removed-in-v0.3")
	d.Emit(b, "spec.foo", "removed-in-v0.3")

	d.Forget(a)

	if !d.Emit(a, "spec.foo", "removed-in-v0.3") {
		t.Fatal("Forget(a) should release a's dedup")
	}
	if d.Emit(b, "spec.foo", "removed-in-v0.3") {
		t.Fatal("Forget(a) must NOT release b's dedup")
	}

	if got := drainRecorder(rec); len(got) != 3 {
		t.Fatalf("recorder got %d events, want 3: %v", len(got), got)
	}
}

func TestDeprecator_NilSafe(t *testing.T) {
	var d *Deprecator
	obj := newCR("ns", "cr")
	if d.Emit(obj, "spec.x", "removed-in-v0.3") {
		t.Fatal("nil Deprecator must return false from Emit")
	}
	d.Forget(obj) // must not panic

	d2 := NewDeprecator(nil)
	if d2.Emit(obj, "spec.x", "removed-in-v0.3") {
		t.Fatal("Deprecator with nil recorder must return false from Emit")
	}

	d3 := NewDeprecator(k8sevents.NewFakeRecorder(1))
	if d3.Emit(nil, "spec.x", "removed-in-v0.3") {
		t.Fatal("Emit on nil object must return false")
	}
}

func TestDeprecator_EventMessageShape(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(1)
	d := NewDeprecator(rec)
	obj := newCR("ns1", "cr1")
	d.Emit(obj, "spec.valkey.legacyFlag", "removed-in-v0.3")

	got := <-rec.Events
	if !strings.Contains(got, string(FieldDeprecated)) {
		t.Errorf("event missing reason FieldDeprecated: %q", got)
	}
	if !strings.Contains(got, "spec.valkey.legacyFlag") {
		t.Errorf("event missing field path: %q", got)
	}
	if !strings.Contains(got, "removed-in-v0.3") {
		t.Errorf("event missing removal window: %q", got)
	}
	if !strings.Contains(got, corev1.EventTypeNormal) {
		t.Errorf("event missing Normal type: %q", got)
	}
}

func TestDeprecator_ConcurrentEmitsDedupCorrectly(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(100)
	d := NewDeprecator(rec)
	obj := newCR("ns1", "cr1")

	var wg sync.WaitGroup
	emitted := make(chan bool, 50)
	for range 50 {
		wg.Go(func() {
			emitted <- d.Emit(obj, "spec.foo", "removed-in-v0.3")
		})
	}
	wg.Wait()
	close(emitted)

	trueCount := 0
	for v := range emitted {
		if v {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Errorf("exactly one concurrent Emit should win, got %d", trueCount)
	}
	if got := drainRecorder(rec); len(got) != 1 {
		t.Errorf("recorder got %d events, want 1", len(got))
	}
}

func drainRecorder(rec *k8sevents.FakeRecorder) []string {
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
