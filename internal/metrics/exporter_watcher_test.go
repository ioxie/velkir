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

package metrics

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newWatcherTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func newPod(ns, cr, name string, withExporter, ready bool) *corev1.Pod {
	containers := []corev1.Container{{Name: "valkey"}}
	if withExporter {
		containers = append(containers, corev1.Container{Name: exporterContainerName})
	}
	cond := corev1.PodCondition{Type: corev1.PodReady}
	if ready {
		cond.Status = corev1.ConditionTrue
	} else {
		cond.Status = corev1.ConditionFalse
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "velkir",
				"app.kubernetes.io/instance":   cr,
			},
		},
		Spec: corev1.PodSpec{Containers: containers},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{cond},
		},
	}
}

// TestHasExporterContainer pins the contract: the gauge fires only
// when a container literally named "exporter" exists.
func TestHasExporterContainer(t *testing.T) {
	yes := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "valkey"}, {Name: exporterContainerName},
	}}}
	if !hasExporterContainer(yes) {
		t.Errorf("hasExporterContainer = false; want true when exporter container present")
	}
	no := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "valkey"},
	}}}
	if hasExporterContainer(no) {
		t.Errorf("hasExporterContainer = true; want false when exporter container absent")
	}
}

// TestPodIsReady pins the readiness probe: missing condition or
// status != True both mean "not ready". The gauge contract depends
// on this — a pod in Pending phase with no conditions yet must
// stamp 0, not 1.
func TestPodIsReady(t *testing.T) {
	cases := []struct {
		name       string
		conditions []corev1.PodCondition
		want       bool
	}{
		{"empty conditions", nil, false},
		{"ready=true", []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, true},
		{"ready=false", []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}, false},
		{"ready=unknown", []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionUnknown}}, false},
		{"only-non-ready-conditions", []corev1.PodCondition{
			{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{Conditions: tc.conditions}}
			if got := podIsReady(pod); got != tc.want {
				t.Errorf("podIsReady = %v; want %v", got, tc.want)
			}
		})
	}
}

// TestScanOnceStampsAndEvicts exercises the cross-pass eviction
// path: pod present in pass 1 then deleted before pass 2 must have
// its series removed, not left behind with its last value.
func TestScanOnceStampsAndEvicts(t *testing.T) {
	ns, cr := "default", "evict-cr"
	pod1 := newPod(ns, cr, "evict-cr-0", true, true)
	pod2 := newPod(ns, cr, "evict-cr-1", true, false)

	c := newWatcherTestClient(t, pod1, pod2)
	w := &ExporterWatcher{Client: c, Log: logr.Discard()}

	w.scanOnce(context.Background())

	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues(ns, cr, "evict-cr-0")); got != 1 {
		t.Errorf("evict-cr-0 = %v; want 1 (exporter present, ready)", got)
	}
	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues(ns, cr, "evict-cr-1")); got != 0 {
		t.Errorf("evict-cr-1 = %v; want 0 (exporter present, not ready)", got)
	}

	// Drop pod-1 from the cache to simulate deletion, then re-scan.
	if err := c.Delete(context.Background(), pod1); err != nil {
		t.Fatalf("delete pod1: %v", err)
	}
	w.scanOnce(context.Background())

	// Series for the deleted pod must be gone, not lingering at 1.
	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues(ns, cr, "evict-cr-0")); got != 0 {
		t.Errorf("evict-cr-0 after deletion = %v; want 0 (series should be evicted, "+
			"so a fresh WithLabelValues returns the zero value of a brand-new series)", got)
	}
	// Surviving pod's series stays.
	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues(ns, cr, "evict-cr-1")); got != 0 {
		t.Errorf("evict-cr-1 after deletion = %v; want 0 (still not ready)", got)
	}
}

// TestScanOncePodWithoutInstanceLabelSkipped guards the empty-name
// fallback: a pod tagged managed-by=velkir but missing the
// app.kubernetes.io/instance label must NOT produce a series with
// an empty `name` value (would corrupt the cardinality budget and
// be unattributable in the dashboard).
func TestScanOncePodWithoutInstanceLabelSkipped(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "orphan",
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "velkir",
				// no instance label
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "valkey"}, {Name: exporterContainerName},
		}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	c := newWatcherTestClient(t, pod)
	w := &ExporterWatcher{Client: c, Log: logr.Discard()}
	w.scanOnce(context.Background())

	// No assertion on a specific empty-string label — the contract is
	// "did the series get registered at all". A WithLabelValues call
	// here would create the empty series we're trying to avoid; instead
	// confirm the watcher's own bookkeeping records nothing was stamped.
	if len(w.lastStamped) != 0 {
		t.Errorf("lastStamped = %v; want empty (orphan pod skipped)", w.lastStamped)
	}
}

// TestPodWithoutExporterContainerStamps0 guards the
// metrics.enabled=false code path: a CR with the sidecar disabled
// produces pods with no exporter container, and the gauge for those
// pods must be 0 (correct: nothing to scrape).
func TestPodWithoutExporterContainerStamps0(t *testing.T) {
	ns, cr := "default", "no-exporter"
	pod := newPod(ns, cr, "no-exporter-0", false, true)
	c := newWatcherTestClient(t, pod)
	w := &ExporterWatcher{Client: c, Log: logr.Discard()}
	w.scanOnce(context.Background())

	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues(ns, cr, "no-exporter-0")); got != 0 {
		t.Errorf("no-exporter-0 = %v; want 0 (sidecar disabled, ready)", got)
	}
}
