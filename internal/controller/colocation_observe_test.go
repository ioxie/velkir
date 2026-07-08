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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func nodePod(name, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
	}
}

func countColocatedEvents(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, "PodsCoLocated") {
			n++
		}
	}
	return n
}

func TestWarnOnSameNodeColocation_EmitsOnSharedNode(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{nodePod("p0", "node-a"), nodePod("p1", "node-a")}

	r.warnOnSameNodeColocation(v, pods, componentValkey)

	if n := countColocatedEvents(drainAllEvents(rec.Events)); n != 1 {
		t.Fatalf("emitted %d PodsCoLocated events; want 1", n)
	}
}

func TestWarnOnSameNodeColocation_NoEmitWhenSpread(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{nodePod("p0", "node-a"), nodePod("p1", "node-b")}

	r.warnOnSameNodeColocation(v, pods, componentSentinel)

	if n := countColocatedEvents(drainAllEvents(rec.Events)); n != 0 {
		t.Fatalf("emitted %d PodsCoLocated events for cross-node pods; want 0", n)
	}
}

func TestWarnOnSameNodeColocation_NoEmitSinglePod(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{nodePod("p0", "node-a")}

	r.warnOnSameNodeColocation(v, pods, componentValkey)

	if n := countColocatedEvents(drainAllEvents(rec.Events)); n != 0 {
		t.Fatalf("emitted %d PodsCoLocated events for a single pod; want 0", n)
	}
}

func TestWarnOnSameNodeColocation_IgnoresUnscheduledPods(t *testing.T) {
	// Pods without a NodeName (pending/unscheduled) carry no
	// co-location signal and must not trip the warning.
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	pods := []corev1.Pod{nodePod("p0", ""), nodePod("p1", "")}

	r.warnOnSameNodeColocation(v, pods, componentValkey)

	if n := countColocatedEvents(drainAllEvents(rec.Events)); n != 0 {
		t.Fatalf("emitted %d PodsCoLocated events for unscheduled pods; want 0", n)
	}
}
