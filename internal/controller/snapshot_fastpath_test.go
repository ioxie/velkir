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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// TestFreshSnapshotPrimaryIP pins the snapshot-fast-path gate:
// the observer snapshot may answer "who is the master" only
// when it is present, quorum-backed, fresh (last live poll within
// maxRolloutSnapshotAge), and carries a parseable host:port Addr. Every
// other case must return "" so observedMasterIPForCR falls back to
// dialing rather than acting on a stale / quorum-lost address.
func TestFreshSnapshotPrimaryIP(t *testing.T) {
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-time.Second) // within the 30s window
	stale := now.Add(-(maxRolloutSnapshotAge + time.Second))

	snap := func(present, quorumOK bool, addr string, polled time.Time) sentinel.Snapshot {
		return sentinel.Snapshot{
			Present: present,
			Primary: sentinel.ObservedPrimary{
				Addr:         addr,
				QuorumOK:     quorumOK,
				LastPolledAt: polled,
			},
		}
	}

	tests := []struct {
		name string
		snap sentinel.Snapshot
		want string
	}{
		{"present+quorum+fresh+valid", snap(true, true, "10.0.0.1:6379", fresh), "10.0.0.1"},
		{"not present", snap(false, true, "10.0.0.1:6379", fresh), ""},
		{"quorum lost", snap(true, false, "10.0.0.1:6379", fresh), ""},
		{"stale poll", snap(true, true, "10.0.0.1:6379", stale), ""},
		{"empty addr", snap(true, true, "", fresh), ""},
		{"addr without port", snap(true, true, "10.0.0.1", fresh), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := freshSnapshotPrimaryIP(tc.snap, now); got != tc.want {
				t.Errorf("freshSnapshotPrimaryIP = %q, want %q", got, tc.want)
			}
		})
	}
}

// fastpathPodAIP / fastpathPodBIP are the two seeded pods' IPs shared
// by the observedMasterIPForCR tests below.
const (
	fastpathPodAIP = "10.0.0.1"
	fastpathPodBIP = "10.0.0.2"
)

// TestObservedMasterIPForCR_PrefersFreshSnapshot pins the snapshot fast-path:
// when no pod carries the role=primary label, observedMasterIPForCR
// resolves the master from a fresh, quorum-backed observer snapshot and
// does NOT re-dial INFO replication on every valkey pod. The snapshot
// names a live pod's IP (pod B) so it passes the live-pod
// cross-check, while the LagChecker is seeded with a master on the
// OTHER pod — a regression to the dial path would both be observable
// (callCount > 0) and return pod A's IP instead.
func TestObservedMasterIPForCR_PrefersFreshSnapshot(t *testing.T) {
	scheme := orphanTestScheme(t)
	// No role=primary pod — forces the function past the label lookup.
	pods := []client.Object{
		labelledValkeyPod("vk0-0", fastpathPodAIP, roleValueReplica),
		labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
	}
	checker := newFakeLagChecker()
	checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: valkey.RoleMaster}

	mgr, cancel := startedManager(t, nil)
	defer cancel()

	// A fake sentinel that names 10.0.0.2:6379 (a live pod) as the
	// primary with a healthy quorum, so the observer publishes a fresh
	// Present snapshot that survives the live-pod cross-check.
	rs, err := newRecoveringSentinel(fastpathPodBIP, 1)
	if err != nil {
		t.Fatalf("newRecoveringSentinel: %v", err)
	}
	defer rs.Stop()

	cr := orphanTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	if err := mgr.Ensure(context.Background(), crKey, "vk0", "",
		[]sentinel.Endpoint{{Name: "vk0-sentinel-0", Addr: rs.Addr()}}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Wait for the observer's first poll to publish the fresh snapshot.
	deadline := time.Now().Add(3 * time.Second)
	for {
		snap := mgr.Snapshot(crKey)
		if snap.Present && snap.Primary.QuorumOK && snap.Primary.Addr == fastpathPodBIP+":6379" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("observer never published a fresh quorum snapshot; got %+v", snap)
		}
		time.Sleep(10 * time.Millisecond)
	}

	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:       checker,
		SentinelObserver: mgr,
	}
	got, _ := r.observedMasterIPForCR(context.Background(), cr, "")
	if got != fastpathPodBIP {
		t.Errorf("master IP = %q, want 10.0.0.2 (from snapshot)", got)
	}
	if n := checker.callCount(fastpathPodAIP+":6379") + checker.callCount(fastpathPodBIP+":6379"); n != 0 {
		t.Errorf("fast-path must not dial INFO replication; got %d CheckLag calls", n)
	}
}

// TestObservedMasterIPForCR_RejectsSnapshotIPWithNoLivePod pins the live-pod cross-check:
// a fresh, quorum-backed snapshot whose primary IP matches no current
// valkey pod must NOT be returned — after a chaotic roll the rebuilt
// sentinels can unanimously name a dead address, and acting on it
// re-MONITORs stranded sentinels at that address forever. The resolver
// must fall through to the INFO-replication dial fallback, whose
// single-master requirement is the safe arbiter.
func TestObservedMasterIPForCR_RejectsSnapshotIPWithNoLivePod(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", fastpathPodAIP, roleValueReplica),
		labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
	}
	checker := newFakeLagChecker()
	checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: valkey.RoleMaster}

	mgr, cancel := startedManager(t, nil)
	defer cancel()

	// The fake sentinel names 10.0.0.9:6379 — no pod has that IP.
	rs, err := newRecoveringSentinel("10.0.0.9", 1)
	if err != nil {
		t.Fatalf("newRecoveringSentinel: %v", err)
	}
	defer rs.Stop()

	cr := orphanTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	if err := mgr.Ensure(context.Background(), crKey, "vk0", "",
		[]sentinel.Endpoint{{Name: "vk0-sentinel-0", Addr: rs.Addr()}}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		snap := mgr.Snapshot(crKey)
		if snap.Present && snap.Primary.QuorumOK && snap.Primary.Addr == "10.0.0.9:6379" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("observer never published a fresh quorum snapshot; got %+v", snap)
		}
		time.Sleep(10 * time.Millisecond)
	}

	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:       checker,
		SentinelObserver: mgr,
	}
	got, _ := r.observedMasterIPForCR(context.Background(), cr, "")
	if got != fastpathPodAIP {
		t.Errorf("master IP = %q, want 10.0.0.1 (snapshot IP is dead; dial fallback must arbitrate)", got)
	}
	if checker.callCount(fastpathPodAIP+":6379") == 0 {
		t.Error("dead snapshot IP must force the dial fallback; got zero CheckLag calls")
	}
}

// TestObservedMasterIPForCR_FallsBackToDialWhenNoObserver pins the
// fallback half: with no observer (and so no snapshot),
// the function still resolves the master by dialing INFO replication on
// each pod — the prior behaviour is preserved, not removed.
func TestObservedMasterIPForCR_FallsBackToDialWhenNoObserver(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", fastpathPodAIP, roleValueReplica),
		labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
	}
	checker := newFakeLagChecker()
	// Pod-1 actually reports role=master via INFO (the dial path's signal).
	checker.byAddr[fastpathPodBIP+":6379"] = valkey.LagState{Role: valkey.RoleMaster}

	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:       checker,
		SentinelObserver: nil,
	}
	got, _ := r.observedMasterIPForCR(context.Background(), orphanTestCR(), "")
	if got != fastpathPodBIP {
		t.Errorf("master IP = %q, want 10.0.0.2 (from INFO dial fallback)", got)
	}
	if checker.callCount(fastpathPodBIP+":6379") == 0 {
		t.Error("fallback must dial INFO replication; got zero CheckLag calls")
	}
}

// TestObservedMasterIPForCR_SamePassProbeReuse pins the reuse of this
// pass's observeMasterInfoTimeout reading: a fresh HEALTHY probe
// accepts the labeled primary with zero extra dials, while a fresh
// probe that saw the pod DISCLAIM mastership (answered INFO as a
// replica — demoted under the label) must fall through to the dial
// arbiter with the same acceptance criteria as the confirmation dial.
func TestObservedMasterIPForCR_SamePassProbeReuse(t *testing.T) {
	scheme := orphanTestScheme(t)
	cr := orphanTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	pods := func() []client.Object {
		return []client.Object{
			labelledValkeyPod("vk0-0", fastpathPodAIP, roleValuePrimary),
			labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
		}
	}

	t.Run("fresh healthy probe accepts the label with no extra dial", func(t *testing.T) {
		checker := newFakeLagChecker()
		r := &ValkeyReconciler{
			Client:     fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods()...).Build(),
			LagChecker: checker,
		}
		state := r.stateFor(crKey).quorumTracker()
		state.mu.Lock()
		state.masterInfoObservedAt = time.Now()
		state.masterInfoRoleDisclaimed = false
		state.mu.Unlock()

		got, _ := r.observedMasterIPForCR(context.Background(), cr, "")
		if got != fastpathPodAIP {
			t.Errorf("master IP = %q, want the labeled primary %q", got, fastpathPodAIP)
		}
		if n := checker.callCount(fastpathPodAIP + ":6379"); n != 0 {
			t.Errorf("fresh healthy probe must be reused, got %d confirmation dials", n)
		}
	})

	t.Run("fresh disclaimed probe falls through to the dial arbiter", func(t *testing.T) {
		checker := newFakeLagChecker()
		// The labeled pod answers as a replica; the real master is pod B.
		checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: "slave"}
		checker.byAddr[fastpathPodBIP+":6379"] = valkey.LagState{Role: valkey.RoleMaster}
		r := &ValkeyReconciler{
			Client:     fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods()...).Build(),
			LagChecker: checker,
		}
		state := r.stateFor(crKey).quorumTracker()
		state.mu.Lock()
		state.masterInfoObservedAt = time.Now()
		state.masterInfoRoleDisclaimed = true
		state.mu.Unlock()

		got, _ := r.observedMasterIPForCR(context.Background(), cr, "")
		if got != fastpathPodBIP {
			t.Errorf("master IP = %q, want the dial-arbitrated master %q (label disclaimed)", got, fastpathPodBIP)
		}
	})
}

// TestObserveMasterInfoTimeout_CapturesRoleDisclaim pins the PRODUCER
// half of the same-pass reuse: the probe must record whether the
// labeled primary answered INFO with a non-master role, so the
// resolver's fast path and the flag it reads cannot drift apart.
func TestObserveMasterInfoTimeout_CapturesRoleDisclaim(t *testing.T) {
	cr := orphanTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	pods := []corev1.Pod{
		*labelledValkeyPod("vk0-0", fastpathPodAIP, roleValuePrimary),
	}
	cases := []struct {
		name string
		role string
		want bool
	}{
		{"role slave disclaims", "slave", true},
		{"role master does not", valkey.RoleMaster, false},
		{"empty role trusts the label", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checker := newFakeLagChecker()
			checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: tc.role}
			r := &ValkeyReconciler{LagChecker: checker}
			r.observeMasterInfoTimeout(context.Background(), crKey, "", pods)
			state := r.stateFor(crKey).quorumTracker()
			state.mu.Lock()
			got := state.masterInfoRoleDisclaimed
			state.mu.Unlock()
			if got != tc.want {
				t.Errorf("masterInfoRoleDisclaimed = %v, want %v for role %q", got, tc.want, tc.role)
			}
		})
	}
}

// TestObservedMasterIPForCR_FullPassDisclaim wires the producer and
// consumer together the way the production reconcile does: the probe
// pass runs against the VALKEY pods (the round-3 wiring fix), then the
// resolver must refuse the demoted-under-label primary and let the
// dial arbiter pick the real master.
func TestObservedMasterIPForCR_FullPassDisclaim(t *testing.T) {
	scheme := orphanTestScheme(t)
	cr := orphanTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	pods := []client.Object{
		labelledValkeyPod("vk0-0", fastpathPodAIP, roleValuePrimary),
		labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
	}
	checker := newFakeLagChecker()
	// The labeled pod was demoted under its label; pod B is the master.
	checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: "slave"}
	checker.byAddr[fastpathPodBIP+":6379"] = valkey.LagState{Role: valkey.RoleMaster}
	r := &ValkeyReconciler{
		Client:     fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker: checker,
	}

	r.observeMasterInfoTimeout(context.Background(), crKey, "", []corev1.Pod{
		*labelledValkeyPod("vk0-0", fastpathPodAIP, roleValuePrimary),
		*labelledValkeyPod("vk0-1", fastpathPodBIP, roleValueReplica),
	})
	got, _ := r.observedMasterIPForCR(context.Background(), cr, "")
	if got != fastpathPodBIP {
		t.Errorf("master IP = %q, want %q (labeled primary disclaimed on the probe pass)", got, fastpathPodBIP)
	}
}

// TestReconcileSentinelOrchestration_ProbesValkeyPods guards the
// call-site argument the round-3 wiring fix corrected: the primary
// INFO probe inside reconcileSentinelOrchestration must be fed the
// VALKEY pod set (where role=primary lives), not the sentinel pods it
// lists for endpoints. If the argument regresses to the sentinel set,
// the probe never finds a labeled primary, the disclaim flag stays
// false, and this test fails.
func TestReconcileSentinelOrchestration_ProbesValkeyPods(t *testing.T) {
	scheme := orphanTestScheme(t)
	cr := orphanTestCR()
	cr.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "vk0", Quorum: 2}
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}

	sentinelPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vk0-sentinel-0",
			Namespace: cr.Namespace,
			Labels: map[string]string{
				CRLabel:        cr.Name,
				ComponentLabel: componentSentinel,
			},
		},
		Status: corev1.PodStatus{PodIP: "10.9.0.1"},
	}
	valkeyPrimary := labelledValkeyPod("vk0-0", fastpathPodAIP, roleValuePrimary)

	checker := newFakeLagChecker()
	// The labeled valkey primary answers INFO as a replica — the probe
	// must observe this and set the disclaim flag.
	checker.byAddr[fastpathPodAIP+":6379"] = valkey.LagState{Role: "slave"}

	mgr, _, cancel := startedManagerForReconciler(t)
	defer cancel()

	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(sentinelPod, valkeyPrimary).Build(),
		LagChecker:       checker,
		SentinelObserver: mgr,
	}
	r.reconcileSentinelOrchestration(context.Background(), cr, orchestration.StateSteady,
		[]corev1.Pod{*valkeyPrimary}, "")

	state := r.stateFor(crKey).quorumTracker()
	state.mu.Lock()
	disclaimed := state.masterInfoRoleDisclaimed
	observedAt := state.masterInfoObservedAt
	state.mu.Unlock()
	if observedAt.IsZero() {
		t.Fatal("the primary INFO probe never ran — the call site is not feeding it the valkey pods")
	}
	if !disclaimed {
		t.Fatal("the labeled primary self-reported role:slave; the probe pass must set masterInfoRoleDisclaimed")
	}
}
