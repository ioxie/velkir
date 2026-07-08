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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// TestOwnsStatefulSetTriggersReconcile pins the watch wiring named in
// the exit criteria: an STS update on an STS owned by a
// Valkey CR must enqueue a Valkey reconcile. The bridge from pod-level
// readiness signals to the Valkey reconciler runs through this watch
// — kubelet refreshes STS.Status when pod-readiness changes, the
// Owns(&appsv1.StatefulSet{}) source carries that update upward via
// the owner reference, and the reconciler then re-derives CR.Status.
//
// The test runs the production SetupWithManager unmodified and observes
// the `controller_runtime_reconcile_total{controller="valkey"}` counter
// before and after a deliberate STS.Status update. A non-zero delta
// proves the watch was wired and the trigger chain works end-to-end.
var _ = Describe("Owns(StatefulSet) wiring", func() {
	It("TestOwnsStatefulSetTriggersReconcile: STS update enqueues a Valkey reconcile", func() {
		mgr, err := ctrl.New(cfg, ctrl.Options{
			Scheme:         scheme.Scheme,
			Metrics:        metricsserver.Options{BindAddress: "0"},
			LeaderElection: false,
		})
		Expect(err).NotTo(HaveOccurred())

		r := &ValkeyReconciler{
			Client:                  mgr.GetClient(),
			Scheme:                  mgr.GetScheme(),
			Recorder:                k8sevents.NewFakeRecorder(64),
			MaxConcurrentReconciles: 1,
		}
		Expect(r.SetupWithManager(mgr)).To(Succeed())

		mgrCtx, mgrCancel := context.WithCancel(context.Background())
		DeferCleanup(mgrCancel)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())

		const name = "owns-sts"
		cr := &valkeyv1beta1.Valkey{
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
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() {
			fresh := &valkeyv1beta1.Valkey{}
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, fresh); err == nil {
				_ = k8sClient.Delete(context.Background(), fresh, client.GracePeriodSeconds(0))
			}
			_ = k8sClient.DeleteAllOf(context.Background(), &appsv1.StatefulSet{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: name})
		})

		key := types.NamespacedName{Name: name, Namespace: "default"}
		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, key, sts)
		}, 30*time.Second, 100*time.Millisecond).Should(Succeed(),
			"initial reconcile must create the STS before the Owns watch can be exercised")

		// Settle: wait for the reconcile counter to stop moving so the
		// "before" snapshot is stable when we mutate the STS below.
		Eventually(func() bool {
			a := valkeyReconcileCount()
			time.Sleep(300 * time.Millisecond)
			return valkeyReconcileCount() == a
		}, 15*time.Second, 200*time.Millisecond).Should(BeTrue(),
			"reconciler must settle before measuring the STS-triggered delta")
		before := valkeyReconcileCount()

		// Simulate kubelet's STS.Status refresh after pod-readiness
		// changes — the production trigger this test pins. CR.Spec is
		// untouched, so the only path to a fresh reconcile is the
		// Owns(&appsv1.StatefulSet{}) watch.
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		sts.Status.Replicas = 1
		sts.Status.ReadyReplicas = 1
		sts.Status.CurrentReplicas = 1
		sts.Status.AvailableReplicas = 1
		sts.Status.ObservedGeneration = sts.Generation
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		Eventually(valkeyReconcileCount, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">", before),
			"Owns(&appsv1.StatefulSet{}) watch on the valkey controller must enqueue a reconcile when an owned STS changes; "+
				"a missing delta means the wiring in ValkeyReconciler.SetupWithManager dropped Owns(StatefulSet) "+
				"or the watch's owner-ref handler stopped resolving Valkey CRs")
	})
})

// valkeyReconcileCount reads the controller-runtime
// `controller_runtime_reconcile_total` counter for the `valkey`
// controller from controller-runtime's global metric registry. Sums
// across the `result` label so reconciles counted under success /
// error / requeue all contribute. Returns 0 when the metric isn't yet
// registered (the manager hasn't reconciled anything), which is a
// valid baseline.
func valkeyReconcileCount() int {
	mfs, err := metrics.Registry.Gather()
	if err != nil {
		return 0
	}
	var total int
	for _, mf := range mfs {
		if mf.GetName() != "controller_runtime_reconcile_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var match bool
			for _, l := range m.GetLabel() {
				if l.GetName() == "controller" && l.GetValue() == "valkey" {
					match = true
					break
				}
			}
			if match {
				total += int(m.GetCounter().GetValue())
			}
		}
	}
	return total
}
