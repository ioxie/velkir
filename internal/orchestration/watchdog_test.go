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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// fixedNow is the synthetic clock used across tests. The 2026 baseline
// is deliberately near the project's actual development date so a
// future "now ≈ Deadline" boundary case reads naturally in failure
// output.
var fixedNow = time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)

func TestCheck_NilStatus(t *testing.T) {
	t.Parallel()
	got := Check(fixedNow, nil)
	if got.Active {
		t.Errorf("nil status: Active=true, want false")
	}
}

func TestCheck_EmptyPodName_Inactive(t *testing.T) {
	t.Parallel()
	deadline := metav1.NewTime(fixedNow.Add(5 * time.Minute))
	status := &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: "",
		Deadline:      &deadline,
	}
	got := Check(fixedNow, status)
	if got.Active {
		t.Errorf("empty WaitingForPod: Active=true, want false (defensive against partial-write race)")
	}
}

func TestCheck_NilDeadline_Inactive(t *testing.T) {
	t.Parallel()
	status := &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: "v-0",
		Deadline:      nil,
	}
	got := Check(fixedNow, status)
	if got.Active {
		t.Errorf("nil Deadline: Active=true, want false")
	}
}

func TestCheck_BeforeDeadline_NotExpired(t *testing.T) {
	t.Parallel()
	deadline := metav1.NewTime(fixedNow.Add(5 * time.Minute))
	status := &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: "v-0",
		Deadline:      &deadline,
	}
	got := Check(fixedNow, status)
	if !got.Active {
		t.Fatalf("Active=false, want true (deadline 5min ahead)")
	}
	if got.Expired {
		t.Errorf("Expired=true at now < deadline")
	}
	if got.PodName != "v-0" {
		t.Errorf("PodName=%q, want v-0", got.PodName)
	}
}

func TestCheck_AtDeadline_Expired(t *testing.T) {
	t.Parallel()
	// Boundary: now == Deadline → Expired=true (≥ semantics, not strict
	// >). Catches a future flip from `!now.Before(d)` to `now.After(d)`
	// that would silently delay the stall by one tick.
	deadline := metav1.NewTime(fixedNow)
	status := &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: "v-0",
		Deadline:      &deadline,
	}
	got := Check(fixedNow, status)
	if !got.Active {
		t.Fatalf("Active=false, want true")
	}
	if !got.Expired {
		t.Errorf("Expired=false at now == deadline; want true (≥ semantics)")
	}
}

func TestCheck_AfterDeadline_Expired(t *testing.T) {
	t.Parallel()
	deadline := metav1.NewTime(fixedNow.Add(-1 * time.Minute))
	status := &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: "v-0",
		Deadline:      &deadline,
	}
	got := Check(fixedNow, status)
	if !got.Active {
		t.Fatalf("Active=false, want true")
	}
	if !got.Expired {
		t.Errorf("Expired=false 1min past deadline; want true")
	}
}

func TestArm_StampsAllFields(t *testing.T) {
	t.Parallel()
	got := Arm(fixedNow, "v-2", 300, nil)
	if got == nil {
		t.Fatal("Arm returned nil")
	}
	if got.WaitingForPod != "v-2" {
		t.Errorf("WaitingForPod=%q, want v-2", got.WaitingForPod)
	}
	if got.Deadline == nil {
		t.Fatal("Deadline=nil")
	}
	wantDeadline := fixedNow.Add(300 * time.Second)
	if !got.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("Deadline=%v, want %v", got.Deadline.Time, wantDeadline)
	}
	if got.DeletedAt == nil {
		t.Fatal("DeletedAt=nil")
	}
	if !got.DeletedAt.Time.Equal(fixedNow) {
		t.Errorf("DeletedAt=%v, want %v", got.DeletedAt.Time, fixedNow)
	}
}

func TestArm_ClampsBelowFloor(t *testing.T) {
	t.Parallel()
	// Webhook floor is 60s; a status-patch that bypasses validation
	// passing 30s should be clamped up.
	got := Arm(fixedNow, "v-0", 30, nil)
	wantDeadline := fixedNow.Add(60 * time.Second)
	if !got.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("Deadline=%v with timeout=30, want %v (clamped to 60s floor)",
			got.Deadline.Time, wantDeadline)
	}
}

func TestArm_ClampsAboveCeiling(t *testing.T) {
	t.Parallel()
	// Webhook ceiling is 3600s; a status-patch passing 7200 should be
	// clamped down.
	got := Arm(fixedNow, "v-0", 7200, nil)
	wantDeadline := fixedNow.Add(3600 * time.Second)
	if !got.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("Deadline=%v with timeout=7200, want %v (clamped to 3600s ceiling)",
			got.Deadline.Time, wantDeadline)
	}
}

func TestArm_AcceptsExactBoundaryValues(t *testing.T) {
	t.Parallel()
	at60 := Arm(fixedNow, "v-0", 60, nil)
	if at60.Deadline.Sub(fixedNow) != 60*time.Second {
		t.Errorf("60s exact value clamped unexpectedly to %v", at60.Deadline.Sub(fixedNow))
	}
	at3600 := Arm(fixedNow, "v-0", 3600, nil)
	if at3600.Deadline.Sub(fixedNow) != 3600*time.Second {
		t.Errorf("3600s exact value clamped unexpectedly to %v", at3600.Deadline.Sub(fixedNow))
	}
}

func TestDisarm_ReturnsZeroValueStruct(t *testing.T) {
	t.Parallel()
	got := Disarm()
	if got == nil {
		t.Fatal("Disarm returned nil; want zero-value struct (semantics distinguish 'cleared cleanly' from 'never armed')")
	}
	if got.WaitingForPod != "" || got.Deadline != nil || got.DeletedAt != nil {
		t.Errorf("Disarm returned non-zero struct: %+v", *got)
	}
	// Round-tripping a Disarm'd status through Check must report Inactive.
	if Check(fixedNow, got).Active {
		t.Errorf("Disarm'd status still Active per Check")
	}
}

func TestArm_IdempotentForSameVictim(t *testing.T) {
	t.Parallel()
	first := Arm(fixedNow, "v-1", 300, nil)
	// A re-arm for the SAME victim must NOT push the deadline out — a
	// reconcile storm while the replacement is still pending would
	// otherwise reset the timer every tick and the watchdog never elapses.
	later := fixedNow.Add(4 * time.Minute)
	second := Arm(later, "v-1", 300, first)
	if second != first {
		t.Fatalf("re-arm for same victim returned a new status; want the existing one unchanged")
	}
	if !second.Deadline.Time.Equal(first.Deadline.Time) {
		t.Errorf("deadline moved on re-arm: got %v, want %v (must be idempotent)",
			second.Deadline.Time, first.Deadline.Time)
	}
}

func TestArm_RearmsDifferentVictim(t *testing.T) {
	t.Parallel()
	// Moving to a new victim pod arms a fresh deadline stamped at the new
	// now — the prior victim's watchdog no longer applies.
	first := Arm(fixedNow, "v-1", 300, nil)
	later := fixedNow.Add(4 * time.Minute)
	second := Arm(later, "v-2", 300, first)
	if second == first {
		t.Fatal("re-arm for a different victim returned the stale status; want a fresh one")
	}
	if second.WaitingForPod != "v-2" {
		t.Errorf("WaitingForPod=%q, want v-2", second.WaitingForPod)
	}
	wantDeadline := later.Add(300 * time.Second)
	if !second.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("Deadline=%v, want %v (fresh stamp at new now)", second.Deadline.Time, wantDeadline)
	}
}

func TestArm_RearmsWhenExistingHasNilDeadline(t *testing.T) {
	t.Parallel()
	// Defensive: an existing status for the same victim but with a nil
	// deadline (partial-write) is not "armed", so Arm builds fresh rather
	// than returning the broken status.
	partial := &valkeyv1beta1.MasterAwareRolloutStatus{WaitingForPod: "v-1", Deadline: nil}
	got := Arm(fixedNow, "v-1", 300, partial)
	if got.Deadline == nil {
		t.Fatal("Arm returned a status with nil deadline; want a fresh deadline stamped")
	}
}
