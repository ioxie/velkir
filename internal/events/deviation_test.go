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
	k8sevents "k8s.io/client-go/tools/events"
)

func TestDeviationEmitter_DeduplicatesSameTuple(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	e := NewDeviationEmitter(rec)
	obj := newCR("ns1", "cr1")

	if !e.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "minAvailable too low") {
		t.Fatal("first Emit should return true (event posted)")
	}
	if e.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "minAvailable too low") {
		t.Fatal("second Emit on same tuple should return false (deduped)")
	}
	if e.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "minAvailable too low") {
		t.Fatal("third Emit on same tuple should return false (deduped)")
	}

	if got := drainRecorder(rec); len(got) != 1 {
		t.Fatalf("recorder got %d events, want 1: %v", len(got), got)
	}
}

func TestDeviationEmitter_SeparateTuplesAllEmit(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	e := NewDeviationEmitter(rec)
	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")

	// Same reason, different field (valkey vs sentinel PDB) → both emit:
	// the field discriminator is what keeps the two from collapsing.
	e.Emit(a, PDBTooPermissive, "spec.valkey.pdb", "m1")
	e.Emit(a, PDBTooPermissive, "spec.sentinel.pdb", "m2")               // different field
	e.Emit(a, RolloutFragileQuorum, "spec.rollout.maxUnavailable", "m3") // different reason
	e.Emit(b, PDBTooPermissive, "spec.valkey.pdb", "m1")                 // different CR

	if got := drainRecorder(rec); len(got) != 4 {
		t.Fatalf("recorder got %d events, want 4: %v", len(got), got)
	}
}

func TestDeviationEmitter_ForgetReleasesDedup(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	e := NewDeviationEmitter(rec)
	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")

	e.Emit(a, PDBTooPermissive, "spec.valkey.pdb", "m")
	e.Emit(a, PDBTooPermissive, "spec.valkey.pdb", "m") // deduped
	e.Emit(b, PDBTooPermissive, "spec.valkey.pdb", "m")

	e.Forget(a)

	if !e.Emit(a, PDBTooPermissive, "spec.valkey.pdb", "m") {
		t.Fatal("after Forget(a), a's tuple should re-emit")
	}
	if e.Emit(b, PDBTooPermissive, "spec.valkey.pdb", "m") {
		t.Fatal("Forget(a) must NOT release b's dedup")
	}

	if got := drainRecorder(rec); len(got) != 3 {
		t.Fatalf("recorder got %d events, want 3: %v", len(got), got)
	}
}

func TestDeviationEmitter_NilSafe(t *testing.T) {
	var e *DeviationEmitter
	obj := newCR("ns", "cr")
	if e.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "m") {
		t.Fatal("nil emitter must return false from Emit")
	}
	e.Forget(obj) // must not panic

	e2 := NewDeviationEmitter(nil)
	if e2.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "m") {
		t.Fatal("emitter with nil recorder must return false from Emit")
	}

	e3 := NewDeviationEmitter(k8sevents.NewFakeRecorder(1))
	if e3.Emit(nil, PDBTooPermissive, "spec.valkey.pdb", "m") {
		t.Fatal("Emit on nil object must return false")
	}
}

func TestDeviationEmitter_EventMessageShape(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(1)
	e := NewDeviationEmitter(rec)
	obj := newCR("ns1", "cr1")
	e.Emit(obj, AntiAffinityTooPermissive, "spec.valkey.affinity", "no same-pod-set term")

	got := <-rec.Events
	if !strings.Contains(got, string(AntiAffinityTooPermissive)) {
		t.Errorf("event missing reason AntiAffinityTooPermissive: %q", got)
	}
	if !strings.Contains(got, "no same-pod-set term") {
		t.Errorf("event missing message body: %q", got)
	}
	if !strings.Contains(got, corev1.EventTypeWarning) {
		t.Errorf("event missing Warning type: %q", got)
	}
}

func TestDeviationEmitter_ConcurrentEmitsDedupCorrectly(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(100)
	e := NewDeviationEmitter(rec)
	obj := newCR("ns1", "cr1")

	var wg sync.WaitGroup
	emitted := make(chan bool, 50)
	for range 50 {
		wg.Go(func() {
			emitted <- e.Emit(obj, PDBTooPermissive, "spec.valkey.pdb", "m")
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
