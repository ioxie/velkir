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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Reconciler-driven envtest specs for per-CR SentinelQuorum
// creation. Exercises reconcileSentinelQuorums
// directly with a synthetic pod list — the broader sentinel
// orchestration (observer manager, IP discovery, suppression gate)
// has its own coverage in sentinel_orchestration_test.go and isn't
// re-driven here.
var _ = Describe("ValkeyReconciler per-pod SentinelQuorum (#113)", func() {
	ctx := context.Background()

	makeCR := func(name string) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeSentinel,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey:   valkeyv1beta1.ValkeyPodSpec{Replicas: 3},
				Sentinel: &valkeyv1beta1.SentinelPodSpec{Replicas: 3, Quorum: 2, MasterName: "mymaster"},
			},
		}
	}

	makePod := func(crName, podName, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentSentinel,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "sentinel",
					Image: "valkey/valkey:8.1.6-alpine",
				}},
			},
			Status: corev1.PodStatus{PodIP: ip},
		}
	}

	cleanupCR := func(name string) {
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	It("creates one SentinelQuorum per sentinel pod with the immutable spec stamped", func() {
		const name = "vk-sq-create"
		cr := makeCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)

		// Refetch so OwnerReference UID is populated for the new SQs.
		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		pods := []corev1.Pod{
			*makePod(name, name+"-sentinel-0", "10.0.0.1"),
			*makePod(name, name+"-sentinel-1", "10.0.0.2"),
			*makePod(name, name+"-sentinel-2", "10.0.0.3"),
		}

		Expect(r.reconcileSentinelQuorums(ctx, fetched, pods)).To(Succeed())

		// Each pod has exactly one SQ with the right shape.
		for i := range pods {
			sq := &valkeyv1beta1.SentinelQuorum{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pods[i].Name, Namespace: "default"}, sq)).To(Succeed())
			Expect(sq.Spec.Valkey).To(Equal(name))
			Expect(sq.Spec.PodName).To(Equal(pods[i].Name))
			// owner-ref to the CR for cascade-delete.
			Expect(sq.OwnerReferences).To(HaveLen(1))
			Expect(sq.OwnerReferences[0].UID).To(Equal(fetched.UID))
			Expect(sq.OwnerReferences[0].Kind).To(Equal("Valkey"))
			Expect(*sq.OwnerReferences[0].Controller).To(BeTrue())
		}

		// Cleanup the SQs (envtest GC doesn't fire owner-ref cascade).
		for i := range pods {
			sq := &valkeyv1beta1.SentinelQuorum{
				ObjectMeta: metav1.ObjectMeta{Name: pods[i].Name, Namespace: "default"},
			}
			_ = k8sClient.Delete(ctx, sq)
		}
	})

	It("is idempotent — re-applying the same pod set produces no spec drift", func() {
		const name = "vk-sq-idem"
		cr := makeCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)

		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		pods := []corev1.Pod{*makePod(name, name+"-sentinel-0", "10.0.1.1")}

		Expect(r.reconcileSentinelQuorums(ctx, fetched, pods)).To(Succeed())

		first := &valkeyv1beta1.SentinelQuorum{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pods[0].Name, Namespace: "default"}, first)).To(Succeed())
		firstResVer := first.ResourceVersion

		// Re-apply with same shape; CEL immutability rule doesn't fire
		// because spec bytes are unchanged, and SSA produces an empty
		// patch (no resourceVersion bump).
		Expect(r.reconcileSentinelQuorums(ctx, fetched, pods)).To(Succeed())

		second := &valkeyv1beta1.SentinelQuorum{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pods[0].Name, Namespace: "default"}, second)).To(Succeed())
		Expect(second.ResourceVersion).To(Equal(firstResVer),
			"SSA re-apply should be a no-op when spec is unchanged")
		Expect(second.Spec.Valkey).To(Equal(name))
		Expect(second.Spec.PodName).To(Equal(pods[0].Name))

		_ = k8sClient.Delete(ctx, second)
	})

	It("is a no-op when the pod list is empty", func() {
		const name = "vk-sq-empty"
		cr := makeCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)

		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		Expect(r.reconcileSentinelQuorums(ctx, fetched, nil)).To(Succeed())
		Expect(r.reconcileSentinelQuorums(ctx, fetched, []corev1.Pod{})).To(Succeed())

		// No SentinelQuorums should have been created.
		sqs := &valkeyv1beta1.SentinelQuorumList{}
		Expect(k8sClient.List(ctx, sqs, client.InNamespace("default"))).To(Succeed())
		for i := range sqs.Items {
			Expect(sqs.Items[i].Spec.Valkey).NotTo(Equal(name),
				"empty pod list should produce zero SQs for this CR")
		}
	})
})
