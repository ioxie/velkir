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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// SentinelQuorum CRD CEL acceptance / rejection coverage. Mirrors the
// Valkey CRD validation suite shape — paired accept/reject per rule,
// plus the spec-immutability rules under Update.

var _ = Describe("SentinelQuorum CRD validation", func() {
	ctx := context.Background()

	cleanupSQ := func(sq *valkeyv1beta1.SentinelQuorum) {
		_ = k8sClient.Delete(ctx, sq)
	}

	validRecord := func(name string, mutate func(*valkeyv1beta1.SentinelQuorum)) *valkeyv1beta1.SentinelQuorum {
		sq := &valkeyv1beta1.SentinelQuorum{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.SentinelQuorumSpec{
				Valkey:  "test-valkey",
				PodName: "test-valkey-sentinel-0",
			},
		}
		if mutate != nil {
			mutate(sq)
		}
		return sq
	}

	Context("Spec validation", func() {
		It("accepts a minimal valid record", func() {
			sq := validRecord("sq-baseline", nil)
			Expect(k8sClient.Create(ctx, sq)).To(Succeed())
			DeferCleanup(cleanupSQ, sq)
		})

		It("rejects a record with empty spec.valkey", func() {
			sq := validRecord("sq-empty-valkey", func(sq *valkeyv1beta1.SentinelQuorum) {
				sq.Spec.Valkey = ""
			})
			err := k8sClient.Create(ctx, sq)
			Expect(err).To(HaveOccurred())
		})

		It("rejects spec.valkey with uppercase characters", func() {
			sq := validRecord("sq-uppercase-valkey", func(sq *valkeyv1beta1.SentinelQuorum) {
				sq.Spec.Valkey = "Test-Valkey"
			})
			err := k8sClient.Create(ctx, sq)
			Expect(err).To(HaveOccurred())
		})

		It("rejects spec.podName with underscore", func() {
			sq := validRecord("sq-underscore-pod", func(sq *valkeyv1beta1.SentinelQuorum) {
				sq.Spec.PodName = "test_pod_0"
			})
			err := k8sClient.Create(ctx, sq)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Spec immutability (CEL)", func() {
		It("rejects an Update that mutates spec.valkey", func() {
			sq := validRecord("sq-valkey-immutable", nil)
			Expect(k8sClient.Create(ctx, sq)).To(Succeed())
			DeferCleanup(cleanupSQ, sq)

			sq.Spec.Valkey = "different-valkey"
			err := k8sClient.Update(ctx, sq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.valkey is immutable"))
		})

		It("rejects an Update that mutates spec.podName", func() {
			sq := validRecord("sq-pod-immutable", nil)
			Expect(k8sClient.Create(ctx, sq)).To(Succeed())
			DeferCleanup(cleanupSQ, sq)

			sq.Spec.PodName = "different-pod"
			err := k8sClient.Update(ctx, sq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.podName is immutable"))
		})
	})

	Context("Status (sentinel-pod-set)", func() {
		It("accepts a status update with all fields populated", func() {
			sq := validRecord("sq-status-full", nil)
			Expect(k8sClient.Create(ctx, sq)).To(Succeed())
			DeferCleanup(cleanupSQ, sq)

			sq.Status.ObservedPrimary = "test-valkey-0"
			sq.Status.ObservedReplicas = []string{"test-valkey-1", "test-valkey-2"}
			sq.Status.QuorumReachable = new(true)
			now := metav1.Now()
			sq.Status.LastObservedTime = &now
			Expect(k8sClient.Status().Update(ctx, sq)).To(Succeed())
		})

		It("rejects ObservedReplicas with more than 10 entries", func() {
			sq := validRecord("sq-too-many-replicas", nil)
			Expect(k8sClient.Create(ctx, sq)).To(Succeed())
			DeferCleanup(cleanupSQ, sq)

			sq.Status.ObservedReplicas = []string{
				"r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10",
			}
			err := k8sClient.Status().Update(ctx, sq)
			Expect(err).To(HaveOccurred())
		})
	})
})
