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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestCacheFilter_UnlabeledSecret pins the load-bearing read-path
// contract for user-supplied auth Secrets.
//
// The manager wires `cache.ByObject{Label: managed-by=velkir}`
// over Secret. User-supplied auth Secrets (referenced by
// spec.auth.secretName) do NOT carry that label, so:
//
//   - mgr.GetClient().Get(unlabeledSecret) returns NotFound — the
//     informer never list-watched it into the cache.
//   - mgr.GetAPIReader().Get(unlabeledSecret) reads the apiserver
//     directly and resolves successfully.
//
// The reconciler's user-Secret reads route through APIReader so this
// architecture works without forcing users to label their Secrets
// with managed-by=velkir. Drift in either direction (cache
// silently picks up unlabeled Secrets, or APIReader stops bypassing
// the cache) breaks the rationale documented in
// docs/security/{deployment-posture,rbac-audit}.md.
var _ = Describe("Cache filter on user-supplied Secrets", func() {
	const (
		managedByLabel = "app.kubernetes.io/managed-by"
		managedByValue = "velkir"
		testNamespace  = "default"
	)

	var (
		mgr        ctrl.Manager
		mgrCtx     context.Context
		mgrCancel  context.CancelFunc
		labeledKey = types.NamespacedName{Namespace: testNamespace, Name: "auth-labeled"}
		unlabKey   = types.NamespacedName{Namespace: testNamespace, Name: "auth-unlabeled"}
	)

	BeforeEach(func() {
		// Production-shape cache.Options: the same `cache.ByObject{Label: ...}`
		// pattern cmd/main.go::buildCacheOptions wires for owned types.
		selector := labels.SelectorFromSet(labels.Set{managedByLabel: managedByValue})
		cacheOpts := cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {Label: selector},
			},
		}

		var err error
		mgr, err = ctrl.New(cfg, ctrl.Options{
			Scheme:         scheme.Scheme,
			Cache:          cacheOpts,
			Metrics:        metricsserver.Options{BindAddress: "0"},
			LeaderElection: false,
		})
		Expect(err).NotTo(HaveOccurred())

		mgrCtx, mgrCancel = context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		// Wait for the cache to start so list-watch is established before
		// the test creates Secrets.
		Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())
	})

	AfterEach(func() {
		mgrCancel()
		// Best-effort cleanup; envtest keeps state across Describe blocks.
		_ = k8sClient.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: labeledKey.Namespace, Name: labeledKey.Name}})
		_ = k8sClient.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: unlabKey.Namespace, Name: unlabKey.Name}})
	})

	createSecret := func(key types.NamespacedName, lbls map[string]string) {
		s := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace,
				Name:      key.Name,
				Labels:    lbls,
			},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"password": "p"},
		}
		Expect(k8sClient.Create(context.Background(), s)).To(Succeed())
	}

	It("cached client returns NotFound for unlabeled Secret; APIReader resolves it", func() {
		createSecret(labeledKey, map[string]string{managedByLabel: managedByValue})
		createSecret(unlabKey, nil)

		// Cache must observe the labeled Secret before the test asserts
		// on its presence; the informer ListWatch is async.
		Eventually(func() error {
			return mgr.GetClient().Get(context.Background(), labeledKey, &corev1.Secret{})
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		By("cached client.Get on unlabeled Secret → NotFound")
		err := mgr.GetClient().Get(context.Background(), unlabKey, &corev1.Secret{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"cached Get should NotFound an unlabeled Secret; got %v", err)

		By("APIReader.Get on unlabeled Secret → success")
		got := &corev1.Secret{}
		Expect(mgr.GetAPIReader().Get(context.Background(), unlabKey, got)).To(Succeed())
		Expect(string(got.Data["password"])).To(Equal("p"))

		By("APIReader.Get on labeled Secret also succeeds (sanity)")
		got = &corev1.Secret{}
		Expect(mgr.GetAPIReader().Get(context.Background(), labeledKey, got)).To(Succeed())
		Expect(string(got.Data["password"])).To(Equal("p"))
	})
})
