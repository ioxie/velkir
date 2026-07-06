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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// Reconciler-driven envtest specs for the watchdog integration.
// Exercises the deferred closure's Check + emit + disarm path
// against a CR whose MasterAware substate has been pre-armed by a
// hand-written status patch (the Arm side lands with the
// master-aware rolling code).
var _ = Describe("ValkeyReconciler readiness watchdog", func() {
	ctx := context.Background()

	makeCR := func(name string) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeStandalone,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 1},
			},
		}
	}

	cleanup := func(name string) {
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	armStatus := func(name string, sub *valkeyv1beta1.MasterAwareRolloutStatus) {
		cr := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr)).To(Succeed())
		patched := cr.DeepCopy()
		if patched.Status.Rollout == nil {
			patched.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
		}
		patched.Status.Rollout.MasterAware = sub
		Expect(k8sClient.Status().Patch(ctx, patched, client.MergeFrom(cr))).To(Succeed())
	}

	getCR := func(name string) *valkeyv1beta1.Valkey {
		out := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, out)).To(Succeed())
		return out
	}

	findCondition := func(cr *valkeyv1beta1.Valkey, t string) *metav1.Condition {
		for i := range cr.Status.Conditions {
			if cr.Status.Conditions[i].Type == t {
				return &cr.Status.Conditions[i]
			}
		}
		return nil
	}

	drainEvents := func(rec *k8sevents.FakeRecorder) []string {
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

	It("flips Degraded=True/RolloutStalled and disarms the substate when the deadline has passed", func() {
		const name = "vk-wd-expired"
		cr := makeCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanup, name)

		recorder := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}

		// First reconcile materialises STS / configmaps / etc.;
		// drains any setup-time events from the recorder so the
		// post-watchdog event count is unambiguous.
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())
		_ = drainEvents(recorder)

		// Hand-patch the status to install an already-expired
		// watchdog substate. The Arm side (deletion + stamp) lands
		// with the master-aware rolling code; until then the
		// integration's defensive Check side is exercised by
		// pre-arming through the status subresource.
		past := metav1.NewTime(time.Now().Add(-1 * time.Minute))
		armStatus(name, &valkeyv1beta1.MasterAwareRolloutStatus{
			WaitingForPod: name + "-2",
			Deadline:      &past,
			DeletedAt:     ptrTime(time.Now().Add(-6 * time.Minute)),
		})

		// Re-reconcile so the deferred closure observes the armed substate.
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())

		// Degraded=True/RolloutStalled, message names the pod, substate disarmed.
		out := getCR(name)
		deg := findCondition(out, orchestration.TypeDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionTrue))
		Expect(deg.Reason).To(Equal(orchestration.ReasonRolloutStalled))
		Expect(deg.Message).To(ContainSubstring(name + "-2"))

		Expect(out.Status.Rollout).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware.WaitingForPod).To(BeEmpty())
		Expect(out.Status.Rollout.MasterAware.Deadline).To(BeNil())

		// Exactly one RolloutStalled warning event recorded.
		evs := drainEvents(recorder)
		stallCount := 0
		for _, e := range evs {
			if strings.Contains(e, "RolloutStalled") {
				stallCount++
			}
		}
		Expect(stallCount).To(Equal(1), "expected one RolloutStalled event; got events: %v", evs)
	})

	It("does not flip Degraded or emit when the deadline is in the future", func() {
		const name = "vk-wd-armed"
		cr := makeCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanup, name)

		recorder := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}

		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())
		_ = drainEvents(recorder)

		// Watchdog armed with a deadline 5 minutes in the future.
		future := metav1.NewTime(time.Now().Add(5 * time.Minute))
		armStatus(name, &valkeyv1beta1.MasterAwareRolloutStatus{
			WaitingForPod: name + "-2",
			Deadline:      &future,
			DeletedAt:     ptrTime(time.Now()),
		})

		result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())

		// Degraded stays False; substate untouched; no RolloutStalled event.
		out := getCR(name)
		deg := findCondition(out, orchestration.TypeDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionFalse))
		Expect(deg.Reason).To(Equal(orchestration.ReasonAsExpected))

		Expect(out.Status.Rollout).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware.WaitingForPod).To(Equal(name + "-2"))
		Expect(out.Status.Rollout.MasterAware.Deadline).NotTo(BeNil())

		// RequeueAfter was bumped to wake near the deadline (< 5min,
		// >0). Loose bound — clock skew between patch + read makes
		// equality flaky.
		Expect(result.RequeueAfter).To(BeNumerically(">", 0))
		Expect(result.RequeueAfter).To(BeNumerically("<=", 5*time.Minute))

		evs := drainEvents(recorder)
		for _, e := range evs {
			Expect(e).NotTo(ContainSubstring("RolloutStalled"))
		}
	})

	It("skips watchdog evaluation while paused", func() {
		const name = "vk-wd-paused"
		cr := makeCR(name)
		cr.Annotations = map[string]string{PauseAnnotation: "true"}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanup, name)

		recorder := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}

		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())
		_ = drainEvents(recorder)

		// Arm with a past deadline; while paused, the closure must
		// not fire the event or disarm.
		past := metav1.NewTime(time.Now().Add(-1 * time.Minute))
		armStatus(name, &valkeyv1beta1.MasterAwareRolloutStatus{
			WaitingForPod: name + "-2",
			Deadline:      &past,
			DeletedAt:     ptrTime(time.Now().Add(-6 * time.Minute)),
		})

		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())

		out := getCR(name)
		Expect(out.Status.Rollout).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware).NotTo(BeNil())
		Expect(out.Status.Rollout.MasterAware.WaitingForPod).To(Equal(name+"-2"),
			"paused CR should leave the watchdog substate untouched")

		evs := drainEvents(recorder)
		for _, e := range evs {
			Expect(e).NotTo(ContainSubstring("RolloutStalled"))
		}
	})
})

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}
