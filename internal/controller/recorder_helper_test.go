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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// TestRecordEventf_NilRecorderNoOps pins the helper's nil-safe
// contract. Tests that construct ValkeyReconciler without a Recorder
// (the common shape for envtest-free unit tests) must not panic on
// emit-site invocation.
func TestRecordEventf_NilRecorderNoOps(t *testing.T) {
	r := &ValkeyReconciler{}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	// Plain call — no panic, no return value to inspect; the
	// invariant is "doesn't crash with nil Recorder".
	r.recordEventf(v, corev1.EventTypeNormal, "TestReason", "TestAction",
		"%s + %d", "hello", 42)

	// Empty/zero-value object is also safe.
	r.recordEventf(nil, corev1.EventTypeWarning, "TestReason", "TestAction", "")
}

// TestRecordEventf_LiveRecorderForwards verifies the helper forwards
// args verbatim to a real (fake) Recorder when one is set, including
// the always-nil `related` field.
func TestRecordEventf_LiveRecorderForwards(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(4)
	r := &ValkeyReconciler{Recorder: rec}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	r.recordEventf(v, corev1.EventTypeWarning, "TestReason", "TestAction",
		"pod %s lag %d bytes", "vk-0", 1234)

	got := drainAllEvents(rec.Events)
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1: %v", len(got), got)
	}
	// FakeRecorder formats as "<type> <reason> <message>"; the
	// `action` is observable on the real recorder via series
	// keys but the fake doesn't expose it. Check the message
	// shape and the eventtype/reason wiring.
	if !strings.Contains(got[0], "Warning") || !strings.Contains(got[0], "TestReason") ||
		!strings.Contains(got[0], "pod vk-0 lag 1234 bytes") {
		t.Fatalf("event %q does not match expected shape", got[0])
	}
}
