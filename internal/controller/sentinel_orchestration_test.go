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
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Trigger 1 + Trigger 3 unit tests cover the in-memory state-
// transition logic without an envtest apiserver. The integrated
// path (Phase 11 inside Reconcile) is exercised by the existing
// Ginkgo suite once it gains sentinel-mode coverage; this file
// pins the pure-logic surface so a regression in the UID tracker
// or the QuorumOK edge detector doesn't sneak in via a non-mode
// test.

func newSentinelPod(name, ip string, uid types.UID) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			UID:       uid,
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

func TestSentinelEndpointsFromPods_SkipsMissingIP(t *testing.T) {
	r := &ValkeyReconciler{}
	pods := []corev1.Pod{
		newSentinelPod("vk0-sentinel-0", "10.0.0.1", "u-0"),
		newSentinelPod("vk0-sentinel-1", "", "u-1"), // pending
		newSentinelPod("vk0-sentinel-2", "10.0.0.3", "u-2"),
	}
	got := r.sentinelEndpointsFromPods(pods)
	if len(got) != 2 {
		t.Fatalf("expected 2 endpoints (pending pod skipped), got %d: %+v", len(got), got)
	}
	if got[0].Name != "vk0-sentinel-0" || got[1].Name != "vk0-sentinel-2" {
		t.Errorf("endpoint names misordered: %+v", got)
	}
	expectedAddr := net.JoinHostPort("10.0.0.1", strconv.Itoa(int(defaultSentinelPort)))
	if got[0].Addr != expectedAddr {
		t.Errorf("Addr=%q, want %q", got[0].Addr, expectedAddr)
	}
}

// startedManagerForReconciler builds a sentinel.Manager, starts it,
// and returns the manager, the FakeRecorder it emits events into,
// and the cancel func tests defer to drain the manager. Callers
// that need to assert on reconciler events too can wire the same
// rec on ValkeyReconciler.Recorder; sentinel.Manager and the
// controller both emit through k8s.io/client-go/tools/events now,
// so the two streams interleave on a single channel keyed by the
// unique Reason name (SentinelResetIssued vs QuorumLost vs ...).
func startedManagerForReconciler(t *testing.T) (*sentinel.Manager, *k8sevents.FakeRecorder, context.CancelFunc) {
	t.Helper()
	rec := k8sevents.NewFakeRecorder(64)
	m := sentinel.NewManager(rec, sentinel.Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = m.Start(ctx) }()
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
	return m, rec, cancel
}

// drainAllEvents pulls every event currently buffered on the supplied
// channel. Works for both record.FakeRecorder.Events and
// k8sevents.FakeRecorder.Events (both are `chan string`).
func drainAllEvents(events <-chan string) []string {
	var out []string
	for {
		select {
		case e := <-events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// sentinelRollScheme is a scheme carrying the core + apps + valkey types
// the Phase-3 sentinel-roll tests apply and read.
func sentinelRollScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, valkeyv1beta1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("AddToScheme: %v", err)
		}
	}
	return s
}

// sentinelRollPod builds a sentinel pod for crName at the given STS
// revision with a PodIP, carrying the labels listSentinelPodsFor selects on.
func sentinelRollPod(crName, name, ip, revision string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			Labels: map[string]string{
				CRLabel:          crName,
				ComponentLabel:   componentSentinel,
				stsRevisionLabel: revision,
			},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// peerCountStub returns a sentinelPeerCountFn that reports the supplied
// per-pod num-other-sentinels counts (missing pods report 0).
func peerCountStub(counts map[string]int) func(context.Context, []sentinel.Endpoint, string, string) []sentinel.MasterPeerCountResult {
	return func(_ context.Context, endpoints []sentinel.Endpoint, _, _ string) []sentinel.MasterPeerCountResult {
		out := make([]sentinel.MasterPeerCountResult, len(endpoints))
		for i, ep := range endpoints {
			out[i] = sentinel.MasterPeerCountResult{Name: ep.Name, Count: counts[ep.Name]}
		}
		return out
	}
}

// TestSentinelRoll_OnePodAtATime_GatedOnRejoin pins the per-pod re-join
// gate on the sentinel STS roll: the partition advances one ordinal at a
// time, and only once the most-recently-rolled sentinel reports
// num-other-sentinels >= quorum-1. The StatefulSet rolls highest-ordinal
// first, so the rolled set is a top suffix and the gate watches its
// lowest (most recent) member.
func TestSentinelRoll_OnePodAtATime_GatedOnRejoin(t *testing.T) {
	scheme := sentinelRollScheme(t)
	const crName = "sr0"
	stsName := crName + "-sentinel"
	mkCR := func() *valkeyv1beta1.Valkey {
		v := newCR(valkeyv1beta1.ModeSentinel)
		v.Namespace = "ns"
		v.Name = crName
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
			MasterName: "mymaster",
			Replicas:   3,
			Quorum:     2, // re-join threshold = quorum-1 = 1
		}
		return v
	}
	rollingSTS := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: stsName},
			Status: appsv1.StatefulSetStatus{
				UpdateRevision:  "rev-new",
				CurrentRevision: "rev-old",
			},
		}
	}
	podName := func(ord int) string { return stsName + "-" + strconv.Itoa(ord) }
	pod := func(ord int, ip, rev string) *corev1.Pod {
		return sentinelRollPod(crName, podName(ord), ip, rev)
	}
	ctx := context.Background()

	t.Run("no roll pending — partition 0", func(t *testing.T) {
		v := mkCR()
		sts := rollingSTS()
		sts.Status.CurrentRevision = "rev-new" // settled
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(v, sts).Build()
		r := &ValkeyReconciler{Client: c}
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 0 || requeue != 0 {
			t.Fatalf("settled STS: got (part=%d, requeue=%v, err=%v), want (0, 0, nil)", part, requeue, err)
		}
	})

	t.Run("missing STS — partition 0 (initial creation)", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(v).Build()
		r := &ValkeyReconciler{Client: c}
		part, _, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 0 {
			t.Fatalf("missing STS: got (part=%d, err=%v), want (0, nil)", part, err)
		}
	})

	t.Run("nothing rolled yet — start at the top ordinal", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-old"),
			pod(1, "10.0.0.1", "rev-old"),
			pod(0, "10.0.0.0", "rev-old"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 2 || requeue == 0 {
			t.Fatalf("start: got (part=%d, requeue=%v, err=%v), want (2, >0, nil)", part, requeue, err)
		}
	})

	t.Run("top pod rolled but NOT re-joined — hold the partition", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-new"), // rolled
			pod(1, "10.0.0.1", "rev-old"),
			pod(0, "10.0.0.0", "rev-old"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		// Top pod reports 0 peers → below quorum-1 → not re-joined.
		r.sentinelPeerCountFn = peerCountStub(map[string]int{podName(2): 0})
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 2 || requeue == 0 {
			t.Fatalf("hold: got (part=%d, requeue=%v, err=%v), want (2 [hold], >0, nil)", part, requeue, err)
		}
	})

	t.Run("top pod rolled AND re-joined — advance one ordinal", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-new"), // rolled
			pod(1, "10.0.0.1", "rev-old"),
			pod(0, "10.0.0.0", "rev-old"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		// Top pod sees 1 peer == quorum-1 → re-joined → advance.
		r.sentinelPeerCountFn = peerCountStub(map[string]int{podName(2): 1})
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 1 || requeue == 0 {
			t.Fatalf("advance: got (part=%d, requeue=%v, err=%v), want (1, >0, nil)", part, requeue, err)
		}
	})

	t.Run("all rolled — release the partition", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-new"),
			pod(1, "10.0.0.1", "rev-new"),
			pod(0, "10.0.0.0", "rev-new"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 0 || requeue != 0 {
			t.Fatalf("all-rolled: got (part=%d, requeue=%v, err=%v), want (0, 0, nil)", part, requeue, err)
		}
	})

	t.Run("rolled pod wire-error — hold (the Err half of the re-join predicate)", func(t *testing.T) {
		v := mkCR()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-new"), // rolled
			pod(1, "10.0.0.1", "rev-old"),
			pod(0, "10.0.0.0", "rev-old"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		// A wire error must read as not-re-joined regardless of count —
		// Count is above threshold so only the Err check can hold here.
		r.sentinelPeerCountFn = func(_ context.Context, endpoints []sentinel.Endpoint, _, _ string) []sentinel.MasterPeerCountResult {
			out := make([]sentinel.MasterPeerCountResult, len(endpoints))
			for i, ep := range endpoints {
				out[i] = sentinel.MasterPeerCountResult{Name: ep.Name, Count: 99, Err: errors.New("wire fail")}
			}
			return out
		}
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 2 || requeue == 0 {
			t.Fatalf("wire-error: got (part=%d, requeue=%v, err=%v), want (2 [hold], >0, nil)", part, requeue, err)
		}
	})

	t.Run("degenerate quorum (1) disables the re-join gate — advance unconditionally", func(t *testing.T) {
		v := mkCR()
		v.Spec.Sentinel.Quorum = 1 // threshold = quorum-1 = 0 → gate disabled
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			v, rollingSTS(),
			pod(2, "10.0.0.2", "rev-new"), // rolled
			pod(1, "10.0.0.1", "rev-old"),
			pod(0, "10.0.0.0", "rev-old"),
		).Build()
		r := &ValkeyReconciler{Client: c}
		// Top pod reports 0 peers — would hold if the gate were active.
		r.sentinelPeerCountFn = peerCountStub(map[string]int{podName(2): 0})
		part, requeue, err := r.computeSentinelRollPartition(ctx, v, "")
		if err != nil || part != 1 || requeue == 0 {
			t.Fatalf("degenerate-quorum: got (part=%d, requeue=%v, err=%v), want (1 [advance, gate off], >0, nil)", part, requeue, err)
		}
	})
}
