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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// The durable FailoverDispatch marker (Status.Rollout.FailoverDispatch)
// is the crash-safe mirror of the in-memory failover latch. These
// tests pin the persist / clear / rehydrate behaviour that closes the
// latch-then-strip crash window; the full +switch-master crash-restart
// scenario lives in the envtest (TestPersistStripIntent_CrashRestart_*).

// fdTestAddr is an RFC 5737 TEST-NET-2 address used only by these unit
// tests, so the repeated marker addr does not collide with the
// package's many "10.0.0.x:6379" literals (goconst match-constant).
const fdTestAddr = "198.51.100.10:6379"

func fdTestCR() *valkeyv1beta1.Valkey {
	return newCR(valkeyv1beta1.ModeSentinel)
}

func fdTestKey() types.NamespacedName {
	return types.NamespacedName{Namespace: "ns", Name: "vk0"}
}

func TestPersistFailoverDispatch_SetsMarkerAndPersists(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}

	deadline := time.Now().Add(time.Minute)
	if err := r.persistFailoverDispatch(context.Background(), v, fdTestAddr, 0, deadline); err != nil {
		t.Fatalf("persistFailoverDispatch: %v", err)
	}

	// In-memory copy reflects the write.
	if v.Status.Rollout == nil {
		t.Fatal("Rollout status must be materialised after persist")
	}
	fd := v.Status.Rollout.FailoverDispatch
	if fd == nil {
		t.Fatal("in-memory FailoverDispatch must be set after persist")
		return
	}
	if fd.PreStripAddr != fdTestAddr {
		t.Errorf("PreStripAddr = %q, want %q", fd.PreStripAddr, fdTestAddr)
	}
	if fd.Deadline == nil {
		t.Fatal("Deadline must be set")
	}

	// And the marker is durable: re-read from the (fake) apiserver.
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), fdTestKey(), got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if got.Status.Rollout == nil || got.Status.Rollout.FailoverDispatch == nil {
		t.Fatal("FailoverDispatch must be persisted to the apiserver")
	}
	if got.Status.Rollout.FailoverDispatch.PreStripAddr != fdTestAddr {
		t.Errorf("persisted PreStripAddr = %q, want %q",
			got.Status.Rollout.FailoverDispatch.PreStripAddr, fdTestAddr)
	}
}

// TestPersistFailoverDispatch_RecordsPreStripEpoch pins that the
// config-epoch the operator acted under is recorded on the durable
// marker at strip time, so the fence survives a restart and later
// self-heal / render-fallback actions can refuse a lower-epoch
// action/observation.
func TestPersistFailoverDispatch_RecordsPreStripEpoch(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}

	const preStripEpoch int64 = 42
	deadline := time.Now().Add(time.Minute)
	if err := r.persistFailoverDispatch(context.Background(), v, fdTestAddr, preStripEpoch, deadline); err != nil {
		t.Fatalf("persistFailoverDispatch: %v", err)
	}

	if fd := v.Status.Rollout.FailoverDispatch; fd == nil || fd.PreStripEpoch != preStripEpoch {
		t.Fatalf("in-memory PreStripEpoch = %v, want %d", fd, preStripEpoch)
	}

	// Durable: the epoch round-trips through the apiserver.
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), fdTestKey(), got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if got.Status.Rollout == nil || got.Status.Rollout.FailoverDispatch == nil {
		t.Fatal("FailoverDispatch must be persisted")
	}
	if gotEpoch := got.Status.Rollout.FailoverDispatch.PreStripEpoch; gotEpoch != preStripEpoch {
		t.Errorf("persisted PreStripEpoch = %d, want %d", gotEpoch, preStripEpoch)
	}
}

func TestClearFailoverDispatch_RemovesMarker(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr: fdTestAddr,
			Deadline:     &metav1.Time{Time: time.Now().Add(time.Minute)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}

	r.clearFailoverDispatch(context.Background(), v)
	if v.Status.Rollout.FailoverDispatch != nil {
		t.Fatal("in-memory FailoverDispatch must be nil after clear")
	}
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), fdTestKey(), got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if got.Status.Rollout != nil && got.Status.Rollout.FailoverDispatch != nil {
		t.Fatal("FailoverDispatch must be cleared on the apiserver")
	}

	// Idempotent: a second clear with the field already absent is a no-op.
	r.clearFailoverDispatch(context.Background(), v)
}

// TestMaintainFailoverDispatchMarker_RehydratesAcrossRestart is the core
// assertion: after an operator crash the in-memory latch is gone but
// the durable marker survives, and maintainFailoverDispatchMarker must
// reconstruct the latch so role re-stamping stays suppressed while the
// observer still reports the pre-strip primary (snapshot-not-yet-present
// modelled as SentinelObserver=nil → observedAddr "").
func TestMaintainFailoverDispatchMarker_RehydratesAcrossRestart(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr: fdTestAddr,
			Deadline:     &metav1.Time{Time: time.Now().Add(failoverInFlightLatchTTL)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	// SentinelObserver nil → observedAddr "" (post-restart, snapshot not
	// yet republished). Fresh reconciler → no in-memory latch (the crash).
	r := &ValkeyReconciler{Client: c}
	cr := fdTestKey()

	if r.failoverLatchActive(cr, fdTestAddr, 0) {
		t.Fatal("pre-condition: in-memory latch must be absent after a simulated restart")
	}

	r.maintainFailoverDispatchMarker(context.Background(), v, cr)

	if !r.failoverLatchActive(cr, fdTestAddr, 0) {
		t.Fatal("latch must be rehydrated from the durable marker so suppression survives the restart")
	}
	if v.Status.Rollout.FailoverDispatch == nil {
		t.Fatal("durable marker must be retained while the failover is still in flight")
	}
}

// TestMaintainFailoverDispatchMarker_RehydratesEpochFence: the rehydrated
// latch must carry the marker's PreStripEpoch, so the epoch fence on the
// critical-section exit survives an operator restart — a lower-epoch
// observer move-off does not exit the rehydrated latch.
func TestMaintainFailoverDispatchMarker_RehydratesEpochFence(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr:  fdTestAddr,
			PreStripEpoch: 9,
			Deadline:      &metav1.Time{Time: time.Now().Add(failoverInFlightLatchTTL)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}
	cr := fdTestKey()

	r.maintainFailoverDispatchMarker(context.Background(), v, cr)

	// A move-off carrying a LOWER epoch than the rehydrated fence (8 < 9)
	// is a stale view and must not exit the critical section.
	if !r.failoverLatchActive(cr, "203.0.113.7:6379", 8) {
		t.Fatal("rehydrated latch must carry the marker epoch and hold against a lower-epoch move-off")
	}
}

// TestMaintainFailoverDispatchMarker_ClearsOnDeadlineExpiry: a marker
// whose deadline has passed must not rehydrate the latch and must drop the
// durable marker, so a wedged failover does not suppress re-derivation
// forever.
func TestMaintainFailoverDispatchMarker_ClearsOnDeadlineExpiry(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr: fdTestAddr,
			Deadline:     &metav1.Time{Time: time.Now().Add(-time.Second)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}
	cr := fdTestKey()

	r.maintainFailoverDispatchMarker(context.Background(), v, cr)

	if r.failoverLatchActive(cr, fdTestAddr, 0) {
		t.Fatal("an expired marker must not rehydrate an active latch")
	}
	if v.Status.Rollout.FailoverDispatch != nil {
		t.Fatal("an expired marker must be cleared")
	}
}

// TestMaintainFailoverDispatchMarker_NoMarkerIsNoop: the common path (no
// failover in flight, incl. all non-sentinel modes) touches nothing.
func TestMaintainFailoverDispatchMarker_NoMarkerIsNoop(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}
	cr := fdTestKey()

	r.maintainFailoverDispatchMarker(context.Background(), v, cr)

	if v.Status.Rollout != nil {
		t.Fatal("maintain must not materialise Rollout status when no marker is set")
	}
	if r.failoverLatchActive(cr, fdTestAddr, 0) {
		t.Fatal("maintain must not create a latch when no marker is set")
	}
}

// TestMaintainFailoverDispatchMarker_LiveLatchUntouched: when the operator
// did not crash (the in-memory latch is still live), maintain leaves it
// alone and keeps the marker while the window is open.
func TestMaintainFailoverDispatchMarker_LiveLatchUntouched(t *testing.T) {
	scheme := authRotationScheme(t)
	v := fdTestCR()
	deadline := time.Now().Add(failoverInFlightLatchTTL)
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr: fdTestAddr,
			Deadline:     &metav1.Time{Time: deadline},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v).WithStatusSubresource(v).Build()
	r := &ValkeyReconciler{Client: c}
	cr := fdTestKey()
	r.failoverLatchSetWithDeadline(cr, fdTestAddr, 0, deadline)

	r.maintainFailoverDispatchMarker(context.Background(), v, cr)

	if !r.failoverLatchActive(cr, fdTestAddr, 0) {
		t.Fatal("a live in-memory latch must remain active after maintain")
	}
	if v.Status.Rollout.FailoverDispatch == nil {
		t.Fatal("the durable marker must be retained while the window is open")
	}
}
