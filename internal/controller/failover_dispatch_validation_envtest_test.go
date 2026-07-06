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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// status.rollout.failoverDispatch.preStripAddr carries a
// kubebuilder ip:port pattern so an obviously-malformed hand-edited
// status is rejected at admission instead of silently no-op'ing on the
// deadline backstop. The operator only ever writes net.JoinHostPort
// form (IPv4 or bracketed-IPv6), so the pattern must accept both and
// reject gross malformation — a too-strict IPv4-only pattern would
// reject a valid IPv6 cluster's addr and break the marker write.
var _ = Describe("FailoverDispatch.PreStripAddr admission pattern (#550)", func() {
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

	setPreStripAddr := func(name, addr string) error {
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err != nil {
			return err
		}
		cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
			FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
				PreStripAddr: addr,
				Deadline:     &metav1.Time{Time: time.Now().Add(time.Minute)},
			},
		}
		return k8sClient.Status().Update(ctx, cr)
	}

	It("accepts IPv4 and bracketed-IPv6 ip:port, rejects malformed values", func() {
		const name = "i550-prestripaddr"
		Expect(k8sClient.Create(ctx, makeCR(name))).To(Succeed())
		DeferCleanup(func() {
			cr := &valkeyv1beta1.Valkey{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err == nil {
				_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
			}
		})

		// Valid forms the operator actually writes (net.JoinHostPort).
		Expect(setPreStripAddr(name, "10.0.0.1:6379")).To(Succeed(),
			"IPv4 ip:port must be accepted")
		Expect(setPreStripAddr(name, "[fe80::1]:6379")).To(Succeed(),
			"bracketed-IPv6 ip:port must be accepted")

		// Obviously-malformed values rejected at admission. Unbracketed
		// IPv6 is intentionally rejected — net.JoinHostPort always
		// brackets IPv6, so the operator never produces that shape.
		for _, bad := range []string{"not-an-addr", "10.0.0.1", "10.0.0.1:", ":6379", "fe80::1:6379"} {
			err := setPreStripAddr(name, bad)
			Expect(err).To(HaveOccurred(),
				"malformed preStripAddr %q must be rejected at admission", bad)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(),
				"rejection of %q must be a schema Invalid error, got %v", bad, err)
		}
	})
})
