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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// recordingReconciler records every reconcile.Request it receives onto a
// buffered channel, so a test can assert which CR(s) an event enqueued.
type recordingReconciler struct {
	seen chan reconcile.Request
}

func (rr *recordingReconciler) Reconcile(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
	select {
	case rr.seen <- req:
	default:
	}
	return reconcile.Result{}, nil
}

// TestAuthSecretWatchEnqueuesOwningCR pins the contract: a change to
// the user-supplied auth Secret (spec.auth.secretName) enqueues the owning
// Valkey CR immediately, without widening the manager's label-filtered data
// cache.
//
// The manager is wired with the production-shape Secret cache
// (cache.ByObject{Label: managed-by=velkir}, per
// cmd/main.go::buildCacheOptions). The user auth Secret carries no such
// label, so the main cache never list-watches it (mgr.GetClient().Get →
// NotFound). authSecretWatchSource runs a separate, metadata-only, unlabeled
// Secret informer that observes it anyway and maps it back to the owning CR
// through the spec.auth.secretName field index.
//
// The test wires authSecretWatchSource onto a uniquely-named controller (the
// production controller is named "valkey"; a second "valkey" in-process trips
// controller-runtime's global name-uniqueness guard) with a recording
// reconciler, so it observes exactly which CR each Secret event enqueues.
var _ = Describe("Auth-Secret watch", func() {
	It("TestAuthSecretWatchEnqueuesOwningCR: rotating the referenced auth Secret enqueues the owning CR", func() {
		const (
			managedByLabel = "app.kubernetes.io/managed-by"
			managedByValue = "velkir"
			ns             = "default"
			crName         = "auth-watch-cr"
			secretName     = "auth-watch-secret"
			bystander      = "auth-watch-bystander"
		)
		crReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: crName}}

		// Production-shape filtered Secret cache: the same selector
		// cmd/main.go::buildCacheOptions wires for owned types.
		selector := labels.SelectorFromSet(labels.Set{managedByLabel: managedByValue})
		mgr, err := ctrl.New(cfg, ctrl.Options{
			Scheme:         scheme.Scheme,
			Metrics:        metricsserver.Options{BindAddress: "0"},
			LeaderElection: false,
			Cache: cache.Options{
				ByObject: map[client.Object]cache.ByObject{
					&corev1.Secret{}: {Label: selector},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		r := &ValkeyReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
		}
		src, err := r.authSecretWatchSource(mgr)
		Expect(err).NotTo(HaveOccurred())

		rec := &recordingReconciler{seen: make(chan reconcile.Request, 128)}
		Expect(builder.ControllerManagedBy(mgr).
			Named("auth-secret-watch-test").
			For(&valkeyv1beta1.Valkey{}).
			WatchesRawSource(src).
			Complete(rec)).To(Succeed())

		mgrCtx, mgrCancel := context.WithCancel(context.Background())
		DeferCleanup(mgrCancel)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())

		// drain empties the recorder so the next assertion measures only
		// requests enqueued after this point.
		drain := func() {
			for {
				select {
				case <-rec.seen:
				default:
					return
				}
			}
		}
		// sawCRReq reports whether a reconcile request for the CR landed.
		sawCRReq := func() bool {
			for {
				select {
				case req := <-rec.seen:
					if req == crReq {
						return true
					}
				default:
					return false
				}
			}
		}

		secretKey := types.NamespacedName{Namespace: ns, Name: secretName}
		// The referenced auth Secret is user-supplied: created WITHOUT the
		// managed-by label, so it lives outside the main filtered cache.
		authSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: secretName},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"password": "initial-pass-0123456789"},
		}
		Expect(k8sClient.Create(ctx, authSecret)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: secretName}}, client.GracePeriodSeconds(0))
			_ = k8sClient.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: bystander}}, client.GracePeriodSeconds(0))
		})

		cr := &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeStandalone,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 1},
				Auth:   &valkeyv1beta1.AuthSpec{SecretName: secretName},
			},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() {
			fresh := &valkeyv1beta1.Valkey{}
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: crName, Namespace: ns}, fresh); err == nil {
				_ = k8sClient.Delete(context.Background(), fresh, client.GracePeriodSeconds(0))
			}
		})

		By("boundary: the unlabeled auth Secret is absent from the filtered main cache but resolvable via APIReader")
		Expect(apierrors.IsNotFound(mgr.GetClient().Get(ctx, secretKey, &corev1.Secret{}))).To(BeTrue(),
			"the user auth Secret must NOT be in the label-filtered main cache — the watch must not widen the data cache")
		Expect(mgr.GetAPIReader().Get(ctx, secretKey, &corev1.Secret{})).To(Succeed())

		// Let the For(CR-create) + Secret-create baseline events flush, then
		// clear them so the rotation assertion measures only the rotation.
		Eventually(sawCRReq, 15*time.Second, 100*time.Millisecond).Should(BeTrue(),
			"creating the CR (or its auth Secret) must drive at least one reconcile before the rotation is measured")
		time.Sleep(500 * time.Millisecond)
		drain()

		By("rotating the auth Secret (data update) enqueues a reconcile of the owning CR")
		rotated := &corev1.Secret{}
		Expect(mgr.GetAPIReader().Get(ctx, secretKey, rotated)).To(Succeed())
		rotated.StringData = map[string]string{"password": "rotated-pass-9876543210"}
		Expect(k8sClient.Update(ctx, rotated)).To(Succeed())

		Eventually(sawCRReq, 15*time.Second, 100*time.Millisecond).Should(BeTrue(),
			"a change to the referenced auth Secret must enqueue the owning CR via authSecretWatchSource; "+
				"a missing request means the dedicated metadata-only Secret watch or its field-index map dropped the event")

		time.Sleep(500 * time.Millisecond)
		drain()

		By("an unrelated, unreferenced Secret must NOT enqueue a reconcile of the CR")
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: bystander},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"password": "unrelated"},
		})).To(Succeed())
		Consistently(sawCRReq, 3*time.Second, 250*time.Millisecond).Should(BeFalse(),
			"a Secret no CR references must resolve to zero reconcile requests through the field-index map")
	})
})
