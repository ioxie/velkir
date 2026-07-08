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

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// observeSplitBrain reports a real split-brain signal (Present with
// Quorum==QuorumStatusLost) on a sentinel-mode CR with a non-nil
// observer. The unit tests below pin the four short-circuit paths, the
// Quorum=Unknown fix (the observer can't reach a quorum of peers,
// e.g. on operator restart — must NOT report split-brain), and the
// shared gate predicate for all four Quorum states. Phase 7 itself is
// covered in phase7_test.go; this file complements it by pinning the
// deferred-status closure side without coupling to the reconciler's
// reconcileRoleLabels integration.

func TestObserveSplitBrain_NilCR_ReturnsFalse(t *testing.T) {
	r := &ValkeyReconciler{}
	if r.observeSplitBrain(nil) {
		t.Fatal("nil CR must return false (avoids panic in the deferred status closure)")
	}
}

func TestObserveSplitBrain_Standalone_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeStandalone)
	if r.observeSplitBrain(v) {
		t.Fatal("standalone mode must never report split-brain (Phase 7 short-circuits to bootstrap roles)")
	}
}

func TestObserveSplitBrain_Replication_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeReplication)
	if r.observeSplitBrain(v) {
		t.Fatal("replication mode must never report split-brain (Phase 7 short-circuits to bootstrap roles)")
	}
}

func TestObserveSplitBrain_NilObserver_ReturnsFalse(t *testing.T) {
	// Nil observer is the standalone-build / boot-race shape.
	// Phase 7 falls back to bootstrap roles, so the condition
	// path must agree: no split-brain claim without an observer
	// to source it from.
	r := &ValkeyReconciler{SentinelObserver: nil}
	v := newCR(valkeyv1beta1.ModeSentinel)
	if r.observeSplitBrain(v) {
		t.Fatal("nil observer must return false (no snapshot source means no claim)")
	}
}

func TestObserveSplitBrain_Sentinel_PreSnapshot_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	// No Ensure call → Snapshot returns Present=false → helper
	// returns false. Pins the boot-race shape: the operator must
	// not flip Degraded=SplitBrain on a fresh CR before the
	// observer has produced its first poll-tick snapshot.
	if r.observeSplitBrain(v) {
		t.Fatal("pre-snapshot (Present=false) must return false — fresh CRs must not flap Degraded")
	}
}

func TestObserveSplitBrain_Sentinel_QuorumUnknown_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	// All-unreachable endpoints: the observer reaches fewer than a
	// quorum of peers, so the first poll publishes Quorum=Unknown
	// (QuorumOK=false, Source != none). Unknown means "no data yet",
	// NOT split-brain — observeSplitBrain must return false so an
	// operator restart of a healthy cluster does not flap
	// Degraded=SplitBrain.
	if err := mgr.Ensure(context.Background(), cr, "vk0", "",
		[]sentinel.Endpoint{
			{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"},
			{Name: "vk0-sentinel-1", Addr: "127.0.0.1:2"},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// Wait for the first published Unknown snapshot (a real poll ran,
	// so Source != none and QuorumOK=false).
	deadline := time.Now().Add(5 * time.Second)
	var snap sentinel.Snapshot
	for time.Now().Before(deadline) {
		snap = mgr.Snapshot(cr)
		if snap.Present && !snap.Primary.QuorumOK && snap.Primary.Source != sentinel.SourceNone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if snap.Primary.Quorum != sentinel.QuorumStatusUnknown {
		t.Fatalf("all-unreachable endpoints must publish Quorum=Unknown; got %v", snap.Primary.Quorum)
	}
	if r.observeSplitBrain(v) {
		t.Fatal("Quorum=Unknown (observer can't reach a quorum of peers) must NOT report split-brain (#557)")
	}
}

// TestSnapshotReportsSplitBrain_GatesOnLostNotUnknown pins the shared
// gate predicate for all four Quorum states. Producing a genuine
// QuorumStatusLost via the real Manager needs ≥quorum reachable but
// disagreeing sentinels (not feasible in a unit test), so the gate logic
// itself is pinned here with constructed snapshots; the Unknown→false
// path is additionally exercised end-to-end through the Manager above.
func TestSnapshotReportsSplitBrain_GatesOnLostNotUnknown(t *testing.T) {
	cases := []struct {
		name string
		snap sentinel.Snapshot
		want bool
	}{
		{"not present", sentinel.Snapshot{Present: false, Primary: sentinel.ObservedPrimary{Quorum: sentinel.QuorumStatusLost}}, false},
		{"present unknown (restart placeholder)", sentinel.Snapshot{Present: true, Primary: sentinel.ObservedPrimary{Quorum: sentinel.QuorumStatusUnknown}}, false},
		{"present ok", sentinel.Snapshot{Present: true, Primary: sentinel.ObservedPrimary{Quorum: sentinel.QuorumStatusOK, QuorumOK: true}}, false},
		{"present lost", sentinel.Snapshot{Present: true, Primary: sentinel.ObservedPrimary{Quorum: sentinel.QuorumStatusLost}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := snapshotReportsSplitBrain(tc.snap); got != tc.want {
				t.Fatalf("snapshotReportsSplitBrain(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
