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

func TestShortAuthPasswordReporter_DeduplicatesSameTuple(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	r := NewShortAuthPasswordReporter(rec)

	obj := newCR("ns1", "cr1")

	if !r.Emit(obj, "valkey-auth", 5, 8) {
		t.Fatal("first Emit should return true (event posted)")
	}
	if r.Emit(obj, "valkey-auth", 5, 8) {
		t.Fatal("second Emit on same tuple should return false (deduped)")
	}
	if r.Emit(obj, "valkey-auth", 4, 8) {
		t.Fatal("Emit on same tuple with different passwordLen should still dedup — key is the tuple, not the length")
	}

	if got := drainRecorder(rec); len(got) != 1 {
		t.Fatalf("recorder got %d events, want 1: %v", len(got), got)
	}
}

func TestShortAuthPasswordReporter_SeparateTuplesAllEmit(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	r := NewShortAuthPasswordReporter(rec)

	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")
	c := newCR("ns2", "cr1")

	r.Emit(a, "valkey-auth", 5, 8)
	r.Emit(b, "valkey-auth", 5, 8) // different name
	r.Emit(c, "valkey-auth", 5, 8) // different namespace
	r.Emit(a, "other-auth", 5, 8)  // different secret name

	if got := drainRecorder(rec); len(got) != 4 {
		t.Fatalf("recorder got %d events, want 4: %v", len(got), got)
	}
}

func TestShortAuthPasswordReporter_ForgetReleasesDedup(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	r := NewShortAuthPasswordReporter(rec)

	obj := newCR("ns1", "cr1")

	r.Emit(obj, "valkey-auth", 5, 8)
	r.Emit(obj, "valkey-auth", 5, 8) // deduped

	r.Forget(obj)

	if !r.Emit(obj, "valkey-auth", 5, 8) {
		t.Fatal("after Forget, same tuple should re-emit")
	}

	if got := drainRecorder(rec); len(got) != 2 {
		t.Fatalf("recorder got %d events, want 2: %v", len(got), got)
	}
}

func TestShortAuthPasswordReporter_ForgetScopedToOneCR(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(10)
	r := NewShortAuthPasswordReporter(rec)

	a := newCR("ns1", "cr1")
	b := newCR("ns1", "cr2")

	r.Emit(a, "valkey-auth", 5, 8)
	r.Emit(b, "valkey-auth", 5, 8)

	r.Forget(a)

	if !r.Emit(a, "valkey-auth", 5, 8) {
		t.Fatal("Forget(a) should release a's dedup")
	}
	if r.Emit(b, "valkey-auth", 5, 8) {
		t.Fatal("Forget(a) must NOT release b's dedup")
	}

	if got := drainRecorder(rec); len(got) != 3 {
		t.Fatalf("recorder got %d events, want 3: %v", len(got), got)
	}
}

func TestShortAuthPasswordReporter_NilSafe(t *testing.T) {
	var r *ShortAuthPasswordReporter
	obj := newCR("ns", "cr")
	if r.Emit(obj, "valkey-auth", 5, 8) {
		t.Fatal("nil reporter must return false from Emit")
	}
	r.Forget(obj) // must not panic

	r2 := NewShortAuthPasswordReporter(nil)
	if r2.Emit(obj, "valkey-auth", 5, 8) {
		t.Fatal("reporter with nil recorder must return false from Emit")
	}

	r3 := NewShortAuthPasswordReporter(k8sevents.NewFakeRecorder(1))
	if r3.Emit(nil, "valkey-auth", 5, 8) {
		t.Fatal("Emit on nil object must return false")
	}
	if r3.Emit(obj, "", 5, 8) {
		t.Fatal("Emit with empty secretName must return false")
	}
}

func TestShortAuthPasswordReporter_EventMessageShape(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(1)
	r := NewShortAuthPasswordReporter(rec)
	obj := newCR("ns1", "cr1")
	r.Emit(obj, "valkey-auth", 5, 8)

	got := <-rec.Events
	if !strings.Contains(got, string(AuthSecretShortPassword)) {
		t.Errorf("event missing reason AuthSecretShortPassword: %q", got)
	}
	if !strings.Contains(got, "valkey-auth") {
		t.Errorf("event missing secret name: %q", got)
	}
	if !strings.Contains(got, "5") {
		t.Errorf("event missing passwordLen value: %q", got)
	}
	if !strings.Contains(got, "8") {
		t.Errorf("event missing MinTokenLen value: %q", got)
	}
	if !strings.Contains(got, corev1.EventTypeWarning) {
		t.Errorf("event missing Warning type (should be Warning, not Normal): %q", got)
	}
}

func TestShortAuthPasswordReporter_MessageDoesNotLeakPasswordContent(t *testing.T) {
	// Defensive: the reporter takes passwordLen, not the password
	// itself, so it physically can't leak the value. Pin that contract.
	rec := k8sevents.NewFakeRecorder(1)
	r := NewShortAuthPasswordReporter(rec)
	obj := newCR("ns1", "cr1")
	r.Emit(obj, "valkey-auth", 5, 8)

	got := <-rec.Events
	// Pin the absence of any character set that could be the password
	// value. The Emit signature only carries an int length, so this
	// is checking the message template, not the call site.
	for _, candidate := range []string{"password=", "secretvalue", "rawpassword"} {
		if strings.Contains(got, candidate) {
			t.Errorf("event leaked an unexpected substring %q: %q", candidate, got)
		}
	}
}

func TestShortAuthPasswordReporter_ConcurrentEmitsDedupCorrectly(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(100)
	r := NewShortAuthPasswordReporter(rec)
	obj := newCR("ns1", "cr1")

	var wg sync.WaitGroup
	emitted := make(chan bool, 50)
	for range 50 {
		wg.Go(func() {
			emitted <- r.Emit(obj, "valkey-auth", 5, 8)
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
