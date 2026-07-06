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
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Phase 7 unit tests cover the desiredRolesForCR helper without an
// envtest apiserver — the integrated patch loop
// (reconcileRoleLabels) is exercised by the existing Ginkgo suite;
// this file pins the pure-logic surface so a regression in the
// snapshot mapping or the split-brain suppress signal doesn't sneak
// in via a non-mode test.
//
// The QuorumOK=true → primary-by-pod-IP scenario isn't unit-tested
// here because it requires a populated Snapshot, which the Manager
// only publishes through its observer goroutine. Driving that from
// a unit test means standing up a fake sentinel TCP server, which
// we already do in internal/sentinel/observer_test.go — duplicating
// it here would just re-test the same wiring. The Ginkgo envtest
// cases for split-brain injection + quorum recovery cover the
// integrated path.

// newCR builds a minimal CR object for the helper-under-test.
// Name is fixed because every test uses the same one — the helper's
// behaviour doesn't depend on the name (it does on namespace +
// mode). Caller passes the mode to drive the standalone /
// replication / sentinel branches.
func newCR(mode valkeyv1beta1.Mode) *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk0"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: mode},
	}
}

// newPod builds a test pod with the canonical StatefulSet-stamped
// `apps.kubernetes.io/pod-index` label derived from the trailing
// `-<n>` suffix of name (so `vk0-0` → `"0"`, `vk0-1` → `"1"`).
// Mirrors what the StatefulSet controller produces in production on
// K8s ≥ 1.28; lets the tests exercise the label-first path of
// `desiredRoleForPod` rather than its name-suffix fallback.
func newPod(name, ip string) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			Labels:    map[string]string{},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
	if i := strings.LastIndex(name, "-"); i >= 0 {
		pod.Labels[podIndexLabel] = name[i+1:]
	}
	return pod
}

// startedManager wires a sentinel.Manager started against a
// background ctx so tests can populate Snapshot via Ensure on the
// real Manager surface rather than mocking. Returns the cancel
// func; tests defer it.
func startedManager(t *testing.T, rec k8sevents.EventRecorder) (*sentinel.Manager, context.CancelFunc) {
	t.Helper()
	m := sentinel.NewManager(rec, sentinel.Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = m.Start(ctx) }()
	// Spin until Start has installed rootCtx (Ensure refuses with
	// "manager not started" until then).
	probe := types.NamespacedName{Namespace: "_probe", Name: "_probe"}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := m.Ensure(ctx, probe, "_probe", "",
			[]sentinel.Endpoint{{Name: "_probe", Addr: "127.0.0.1:1"}})
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.Remove(probe)
	return m, cancel
}

func TestDesiredRolesForCR_Standalone_BootstrapRule(t *testing.T) {
	r := &ValkeyReconciler{}
	v := newCR(valkeyv1beta1.ModeStandalone)
	pods := []corev1.Pod{newPod("vk0-0", "10.0.0.1")}
	roles, suppress := r.desiredRolesForCR(v, pods)
	if suppress {
		t.Fatal("standalone must never suppress relabel")
	}
	if roles["vk0-0"] != roleValuePrimary {
		t.Errorf("standalone pod-0 must be primary, got %q", roles["vk0-0"])
	}
}

func TestDesiredRolesForCR_Replication_BootstrapRule(t *testing.T) {
	r := &ValkeyReconciler{}
	v := newCR(valkeyv1beta1.ModeReplication)
	pods := []corev1.Pod{
		newPod("vk0-0", "10.0.0.1"),
		newPod("vk0-1", "10.0.0.2"),
		newPod("vk0-2", "10.0.0.3"),
	}
	roles, suppress := r.desiredRolesForCR(v, pods)
	if suppress {
		t.Fatal("replication mode must never suppress relabel")
	}
	if roles["vk0-0"] != roleValuePrimary {
		t.Errorf("replication pod-0 must be primary, got %q", roles["vk0-0"])
	}
	if roles["vk0-1"] != roleValueReplica || roles["vk0-2"] != roleValueReplica {
		t.Errorf("replication non-zero pods must be replicas, got %v", roles)
	}
}

func TestDesiredRolesForCR_Sentinel_NoObserverManager_FallbackBootstrap(t *testing.T) {
	r := &ValkeyReconciler{SentinelObserver: nil}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{newPod("vk0-0", "10.0.0.1"), newPod("vk0-1", "10.0.0.2")}
	roles, suppress := r.desiredRolesForCR(v, pods)
	if suppress {
		t.Fatal("nil-observer fallback must not suppress")
	}
	if roles["vk0-0"] != roleValuePrimary || roles["vk0-1"] != roleValueReplica {
		t.Errorf("nil-observer fallback must use bootstrap rule, got %v", roles)
	}
}

func TestDesiredRolesForCR_Sentinel_PreSnapshot_NoExistingLabels_BootstrapRule(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{newPod("vk0-0", "10.0.0.1"), newPod("vk0-1", "10.0.0.2")}
	// First-bootstrap case: no pod carries a role label yet and the
	// observer hasn't published a snapshot. Bootstrap rule fires so
	// the ro-Service selector has something to match while the
	// observer comes up.
	roles, suppress := r.desiredRolesForCR(v, pods)
	if suppress {
		t.Fatal("pre-snapshot must not suppress (boot-race fallback)")
	}
	if roles["vk0-0"] != roleValuePrimary {
		t.Errorf("pre-snapshot first-bootstrap must use bootstrap rule, got vk0-0=%q", roles["vk0-0"])
	}
}

func TestDesiredRolesForCR_Sentinel_PreSnapshot_ExistingLabels_PreservedNotReBootstrapped(t *testing.T) {
	// After a kill cascade (operator pod + sentinel pod +
	// non-master valkey pod), the new operator's sentinel observer
	// hasn't yet re-established quorum (`!snap.Present`) — but the
	// surviving pods still carry their post-failover labels. The
	// bootstrap rule would blindly stamp pod-0=primary even when
	// sentinels have promoted a different pod, routing writes via
	// the `<cr>` Service to a replica until the observer
	// reconverged (~10-55s on a real cluster). Trusting the
	// existing labels keeps the slice pointed at the actual primary
	// while the observer comes back.
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{
		newPod("vk0-0", "10.0.0.1"), // recreated, no label
		newPod("vk0-1", "10.0.0.2"),
		newPod("vk0-2", "10.0.0.3"),
	}
	// Sentinels promoted vk0-2 during the operator-down window.
	pods[1].Labels = map[string]string{RoleLabel: roleValueReplica}
	pods[2].Labels = map[string]string{RoleLabel: roleValuePrimary}

	roles, suppress := r.desiredRolesForCR(v, pods)
	if suppress {
		t.Fatal("preserve-existing-labels must not set the suppress flag (no event needed)")
	}
	if roles != nil {
		t.Errorf("preserve-existing-labels must return nil desired (no patches); got %v", roles)
	}
}

func TestDesiredRolesForCR_Sentinel_QuorumUnknown_SuppressesWithoutEmit(t *testing.T) {
	// Scenario: all sentinel endpoints unreachable → the observer
	// reaches fewer than a quorum of peers and publishes Quorum=Unknown
	// (QuorumOK=false). Phase 7 must STILL suppress the relabel (refuse
	// to act on incomplete agreement), but must NOT emit
	// SplitBrainDetected or increment the counter — Unknown is "no data
	// yet" (e.g. an operator restart), not a real split-brain.
	// The event + counter share one gate, so an absent event proves the
	// counter was not incremented either.
	mgrRec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, mgrRec)
	defer cancel()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		SentinelObserver: mgr,
		Recorder:         rec,
	}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := mgr.Ensure(context.Background(), cr, "vk0", "",
		[]sentinel.Endpoint{
			// All-unreachable endpoints → first poll publishes
			// Quorum=Unknown after dial timeouts roll up.
			{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"},
			{Name: "vk0-sentinel-1", Addr: "127.0.0.1:2"},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// Wait for the first poll-tick snapshot to land. The pull
	// tick is 50ms; dial timeout to a closed port is fast; the
	// snapshot must publish Present + Quorum=Unknown within ~5s.
	deadline := time.Now().Add(5 * time.Second)
	var snap sentinel.Snapshot
	for time.Now().Before(deadline) {
		snap = mgr.Snapshot(cr)
		if snap.Present && !snap.Primary.QuorumOK && snap.Primary.Source != sentinel.SourceNone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !snap.Present || snap.Primary.Quorum != sentinel.QuorumStatusUnknown {
		t.Fatalf("expected Present + Quorum=Unknown snapshot within deadline; got %+v", snap)
	}

	pods := []corev1.Pod{newPod("vk0-0", "10.0.0.1")}
	// Drain any setup-related events first.
	drainEvents(rec)
	roles, suppress := r.desiredRolesForCR(v, pods)
	if !suppress {
		t.Fatalf("Quorum=Unknown (QuorumOK=false) must still suppress relabel; got roles=%v suppress=%v", roles, suppress)
	}
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "SplitBrainDetected") {
			t.Errorf("Quorum=Unknown must NOT emit SplitBrainDetected (#557); got event %q", e)
		}
	}
}

func TestDesiredRolesForCR_Sentinel_NilRecorder_NoPanic(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	// Reconciler with nil Recorder — desiredRolesForCR must not panic
	// on the Quorum-not-OK suppress path with no recorder wired.
	r := &ValkeyReconciler{SentinelObserver: mgr, Recorder: nil}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := mgr.Ensure(context.Background(), cr, "vk0", "",
		[]sentinel.Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// Wait for the boot-time placeholder OR a degraded poll snapshot
	// (Quorum=Unknown when all endpoints are unreachable); either
	// drives desiredRolesForCR down its suppress path.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := mgr.Snapshot(cr)
		if s.Present && !s.Primary.QuorumOK && s.Primary.Source != sentinel.SourceNone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	pods := []corev1.Pod{newPod("vk0-0", "10.0.0.1")}
	// Must not panic.
	_, _ = r.desiredRolesForCR(v, pods)
}

// TestDesiredRoleForPod_LabelFirst pins the
// `apps.kubernetes.io/pod-index` label as the primary signal —
// a future StatefulSet improvement that decouples pod names from
// ordinals (long-discussed upstream) would silently break the
// name-suffix fallback without this label-first path.
func TestDesiredRoleForPod_LabelFirst(t *testing.T) {
	cases := []struct {
		name     string
		podName  string
		podIndex string // "" means no pod-index label set
		want     string
	}{
		{"pod-index 0 → primary even with mismatched name", "weird-name", "0", roleValuePrimary},
		{"pod-index 1 → replica even with -0 suffix", "vk0-0", "1", roleValueReplica},
		{"pod-index absent + name vk0-0 → primary (fallback)", "vk0-0", "", roleValuePrimary},
		{"pod-index absent + name vk0-1 → replica (fallback)", "vk0-1", "", roleValueReplica},
		{"pod-index 5 → replica", "vk0-5", "5", roleValueReplica},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: tc.podName},
			}
			if tc.podIndex != "" {
				pod.Labels = map[string]string{podIndexLabel: tc.podIndex}
			}
			got := desiredRoleForPod(pod, "vk0")
			if got != tc.want {
				t.Errorf("desiredRoleForPod(name=%q, podIndex=%q, crName=vk0) = %q, want %q",
					tc.podName, tc.podIndex, got, tc.want)
			}
		})
	}
}

// drainEvents pulls every event currently buffered on rec.
func drainEvents(rec *k8sevents.FakeRecorder) []string {
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
