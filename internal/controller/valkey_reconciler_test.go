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
	"reflect"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/valkey"
)

// Test fixture constants for STS revisions used across the rollout
// specs. Extracted so the test bodies don't trip the
// goconst linter (the strings repeat across four spec setups);
// production code references the actual revision strings via
// sts.Status, never these literals.
const (
	testRevOld = "rev-OLD"
	testRevNew = "rev-NEW"
)

// fakeLagChecker is the stand-in injected into ValkeyReconciler.LagChecker
// for envtest specs. Returns canned LagState per pod address; tests
// preload the map and the checker is read-only afterwards.
type fakeLagChecker struct {
	mu           sync.Mutex
	byAddr       map[string]valkey.LagState
	errAddr      map[string]error
	calls        map[string]int
	lastPassword string
}

func newFakeLagChecker() *fakeLagChecker {
	return &fakeLagChecker{
		byAddr:  map[string]valkey.LagState{},
		errAddr: map[string]error{},
		calls:   map[string]int{},
	}
}

func (f *fakeLagChecker) CheckLag(_ context.Context, addr, password string) (valkey.LagState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[addr]++
	f.lastPassword = password
	if e, ok := f.errAddr[addr]; ok {
		return valkey.LagState{}, e
	}
	return f.byAddr[addr], nil
}

func (f *fakeLagChecker) callCount(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[addr]
}

// recordedPassword returns the password most recently passed to
// CheckLag. Pins that phases use the password threaded from the
// single Phase-0d resolution rather than re-reading the auth Secret.
func (f *fakeLagChecker) recordedPassword() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastPassword
}

// podOrdinal returns the ordinal suffix of a StatefulSet pod name
// (e.g. `vk0-3` → `"3"`), used by the envtest seeders to stamp the
// `apps.kubernetes.io/pod-index` label the StatefulSet controller sets
// on every pod in production (K8s ≥ 1.28), so the label-first path of
// `desiredRoleForPod` / `ordinalFromPod` runs against the same shape
// production sees instead of accidentally exercising only the
// name-suffix fallback. Returns "" for a name without an ordinal suffix.
func podOrdinal(podName string) string {
	if i := strings.LastIndex(podName, "-"); i >= 0 {
		return podName[i+1:]
	}
	return ""
}

// Reconciler-driven envtest specs. The named tests
// `TestSTSApplyIdempotent` and `TestReconcilerRespectsPauseAnnotation`
// live inside this Describe.

var _ = Describe("ValkeyReconciler", func() {
	ctx := context.Background()

	makeCR := func(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
		cr := &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeStandalone,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey: valkeyv1beta1.ValkeyPodSpec{
					// Defaulter would stamp this in production; envtest
					// doesn't run the webhook chain.
					Replicas: 1,
				},
			},
		}
		if mutate != nil {
			mutate(cr)
		}
		return cr
	}

	cleanupCR := func(name string) {
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	cleanupOwned := func(crName string) {
		// Cascade isn't enforced in envtest, so clean owned types manually.
		_ = k8sClient.DeleteAllOf(ctx, &appsv1.StatefulSet{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: crName})
		_ = k8sClient.DeleteAllOf(ctx, &corev1.Service{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: crName})
		_ = k8sClient.DeleteAllOf(ctx, &corev1.ConfigMap{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: crName})
		_ = k8sClient.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: crName}, client.GracePeriodSeconds(0))
		_ = k8sClient.DeleteAllOf(ctx, &policyv1.PodDisruptionBudget{}, client.InNamespace("default"), client.MatchingLabels{CRLabel: crName})
	}

	reconcileOnce := func(name string) {
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())
	}

	// reconcileOnceWithRecorder mirrors reconcileOnce but injects a
	// FakeRecorder so specs can assert on emitted events. Returns
	// the recorder so the caller can drain its buffered channel.
	// The buffer is sized generously (32) so a noisy reconcile
	// doesn't drop events into the void mid-test.
	reconcileOnceWithRecorder := func(name string) *k8sevents.FakeRecorder {
		rec := k8sevents.NewFakeRecorder(32)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())
		return rec
	}

	// drainEvents reads everything currently buffered on the fake
	// recorder's channel. Lets specs assert "this Reason was
	// emitted" or "no Warning Reason was emitted" without racing
	// the controller's emit goroutine.
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

	containsReason := func(events []string, reason string) bool {
		for _, e := range events {
			if strings.Contains(e, " "+reason+" ") {
				return true
			}
		}
		return false
	}

	Context("Per-CR mutex contention (#532)", func() {
		lockedCount := func(name string) float64 {
			return testutil.ToFloat64(
				operatormetrics.ReconciliationsLockedTotal.WithLabelValues("Valkey", "default", name))
		}

		// The per-CR mutex must be non-blocking: when a concurrent
		// reconcile of the same CR holds it, the contending pass requeues
		// and ticks reconciliations_locked_total instead of parking a
		// workqueue worker on a blocking Lock().
		It("requeues and increments reconciliations_locked_total instead of blocking when the mutex is held", func() {
			const name = "lock-contended"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			key := types.NamespacedName{Name: name, Namespace: "default"}

			// Acquire the same *sync.Mutex the Reconcile under test will
			// TryLock, simulating a concurrent reconcile already holding it.
			held := r.lockFor(key)
			Expect(held.TryLock()).To(BeTrue(), "precondition: per-CR mutex must be free before the test holds it")
			defer held.Unlock()

			// Delta over a process-global counter — read before/after rather
			// than Reset() so the spec stays order-independent and safe under
			// parallel execution.
			before := lockedCount(name)

			// Run Reconcile off the test goroutine with a timeout. The mutex
			// is held, so a non-blocking TryLock must return promptly; a
			// regression to a blocking Lock() would deadlock here, which the
			// timeout surfaces as a fast, diagnostic failure instead of a
			// suite-wide hang.
			type recOut struct {
				result reconcile.Result
				err    error
			}
			out := make(chan recOut, 1)
			go func() {
				res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
				out <- recOut{res, err}
			}()

			var got recOut
			Eventually(out, 5*time.Second).Should(Receive(&got),
				"contended Reconcile must return without blocking; a hang means the per-CR mutex regressed to a blocking Lock()")
			Expect(got.err).NotTo(HaveOccurred())
			Expect(got.result.RequeueAfter).To(Equal(lockContendedRequeue),
				"contended reconcile must requeue on the floored cadence (%v), not block on the lock; got %v",
				lockContendedRequeue, got.result.RequeueAfter)
			Expect(lockedCount(name)-before).To(Equal(float64(1)),
				"TryLock miss must increment reconciliations_locked_total exactly once")
		})

		// Negative control: an uncontended reconcile acquires the lock and
		// must never tick the locked counter. Without this, the assertion
		// above could pass against a counter that always reads 1.
		It("does not increment reconciliations_locked_total on the uncontended path", func() {
			const name = "lock-free"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			before := lockedCount(name)
			reconcileOnce(name)

			Expect(lockedCount(name)-before).To(Equal(float64(0)),
				"an uncontended reconcile must acquire the lock and leave the locked counter unchanged")
		})
	})

	Context("STS apply idempotency", func() {
		It("TestSTSApplyIdempotent: STS spec is byte-equal across repeated reconciles", func() {
			const name = "idem-sts"
			cr := makeCR(name, nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			first := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, first)).To(Succeed())
			firstSpec := first.Spec.DeepCopy()

			reconcileOnce(name)
			reconcileOnce(name)

			second := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, second)).To(Succeed())
			Expect(reflect.DeepEqual(firstSpec, &second.Spec)).To(BeTrue(),
				"STS spec drifted across reconciles\nfirst:  %#v\nsecond: %#v", firstSpec, &second.Spec)
		})

		It("Sentinel STS spec is byte-equal across repeated reconciles", func() {
			const name = "idem-sentinel-sts"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
				v.Spec.Valkey.Replicas = 3
				v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
					MasterName:            "mymaster",
					Replicas:              3,
					Quorum:                2,
					DownAfterMilliseconds: 30000,
					FailoverTimeout:       180000,
					ParallelSyncs:         1,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			// First reconcile: valkey STS created; pod-0 has no IP yet so
			// sentinel STS is deferred. Bootstrap CM applied (empty value).
			reconcileOnce(name)
			sentinelSTS := &appsv1.StatefulSet{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sentinel", Namespace: "default"}, sentinelSTS)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"sentinel STS must be deferred until pod-0 has a PodIP; got err=%v", err)

			// Bootstrap CM exists with empty seedMasterIP — the operator
			// applies it on every pass to keep the data key present.
			bootstrap := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffixSentinelBootstrap, Namespace: "default"}, bootstrap)).To(Succeed())
			Expect(bootstrap.Data).To(HaveKey("seedMasterIP"))

			// Seed pod-0 with a PodIP so the next reconcile creates the
			// sentinel STS (envtest's STS controller doesn't materialise pods).
			seedPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name + "-0",
					Namespace: "default",
					Labels: map[string]string{
						podIndexLabel:     podOrdinal(name + "-0"),
						CRLabel:           name,
						ComponentLabel:    componentValkey,
						ManagedByLabel:    ManagedByValue,
						AppNameLabel:      "valkey",
						AppInstanceLabel:  name,
						AppComponentLabel: componentValkey,
						AppPartOfLabel:    "velkir",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				},
			}
			Expect(k8sClient.Create(ctx, seedPod)).To(Succeed())
			seedPod.Status.PodIP = "10.0.0.42"
			Expect(k8sClient.Status().Update(ctx, seedPod)).To(Succeed())

			reconcileOnce(name)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sentinel", Namespace: "default"}, sentinelSTS)).To(Succeed())
			firstSpec := sentinelSTS.Spec.DeepCopy()

			// Bootstrap CM should now carry the seed.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffixSentinelBootstrap, Namespace: "default"}, bootstrap)).To(Succeed())
			Expect(bootstrap.Data["seedMasterIP"]).To(Equal("10.0.0.42"))

			reconcileOnce(name)
			reconcileOnce(name)
			second := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sentinel", Namespace: "default"}, second)).To(Succeed())
			Expect(reflect.DeepEqual(firstSpec, &second.Spec)).To(BeTrue(),
				"sentinel STS spec drifted across reconciles\nfirst:  %#v\nsecond: %#v", firstSpec, &second.Spec)

			// Both sentinel Services exist post-bootstrap.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sentinel", Namespace: "default"}, &corev1.Service{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sentinel-headless", Namespace: "default"}, &corev1.Service{})).To(Succeed())
		})
	})

	Context("Pause annotation", func() {
		It("TestReconcilerRespectsPauseAnnotation: paused CR observes no owned resources", func() {
			const name = "paused-cr"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Annotations = map[string]string{PauseAnnotation: "true"}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			// The reconciler must NOT have stamped any owned artifact while
			// paused. Each Get should return NotFound.
			sts := &appsv1.StatefulSet{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected STS NotFound while paused; got %v", err)

			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected client Service NotFound while paused; got %v", err)

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name + "-valkey-conf", Namespace: "default"}, cm)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected ConfigMap NotFound while paused; got %v", err)
		})

		It("resumes reconciliation after the pause annotation is removed", func() {
			const name = "unpaused-cr"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Annotations = map[string]string{PauseAnnotation: "true"}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)
			sts := &appsv1.StatefulSet{}
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts))).To(BeTrue())

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			delete(fetched.Annotations, PauseAnnotation)
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			reconcileOnce(name)

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
		})
	})

	Context("PVCRetention finalizer (#90)", func() {
		// stripFinalizers force-removes finalizers so envtest cleanup can
		// drop the CR (envtest has no controller-runtime to run the
		// reconcileDeletion path on test exit). Production code never
		// strips the finalizer this way.
		stripFinalizers := func(name string) {
			fetched := &valkeyv1beta1.Valkey{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched); err != nil {
				return
			}
			if len(fetched.Finalizers) == 0 {
				return
			}
			patched := fetched.DeepCopy()
			patched.Finalizers = nil
			_ = k8sClient.Patch(ctx, patched, client.MergeFrom(fetched))
		}

		It("adds the pvc-retention finalizer on the first reconcile of a fresh CR", func() {
			const name = "fz-add"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(stripFinalizers, name)
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			Expect(fetched.Finalizers).To(ContainElement(PVCRetentionFinalizer))
		})

		It("is idempotent — repeated reconciles add the finalizer exactly once", func() {
			const name = "fz-idem"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(stripFinalizers, name)
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)
			reconcileOnce(name)
			reconcileOnce(name)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			count := 0
			for _, f := range fetched.Finalizers {
				if f == PVCRetentionFinalizer {
					count++
				}
			}
			Expect(count).To(Equal(1), "finalizer should be present exactly once; got finalizers %v", fetched.Finalizers)
		})

		It("removes the finalizer after reconcileDeletion completes (Retain policy)", func() {
			const name = "fz-rm-retain"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(stripFinalizers, name)
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			// Trigger delete — finalizer keeps the CR around in
			// Terminating until a subsequent reconcile strips it.
			toDelete := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, toDelete)).To(Succeed())

			terminating := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, terminating)).To(Succeed())
			Expect(terminating.DeletionTimestamp).NotTo(BeNil(), "deletionTimestamp should be set after Delete")
			Expect(terminating.Finalizers).To(ContainElement(PVCRetentionFinalizer), "finalizer should still be present pre-reconcile")

			// Reconcile while deleting — runs reconcileDeletion which
			// applies the Retain policy and strips the finalizer; the
			// apiserver then drops the CR.
			reconcileOnce(name)

			gone := &valkeyv1beta1.Valkey{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, gone)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "CR should be gone after reconcileDeletion stripped the finalizer; got err=%v", err)
		})

		It("removes the finalizer after reconcileDeletion completes (Delete policy)", func() {
			// Mirror of the Retain test, but the Delete policy
			// exercises the owner-ref-injection branch of
			// reconcileDeletion before the finalizer-strip step.
			const name = "fz-rm-delete"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.PVCRetentionPolicy = valkeyv1beta1.PVCRetentionDelete
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(stripFinalizers, name)
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			toDelete := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, toDelete)).To(Succeed())

			reconcileOnce(name)

			gone := &valkeyv1beta1.Valkey{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, gone)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "CR should be gone after Delete-policy reconcileDeletion stripped the finalizer; got err=%v", err)
		})

		It("does not add the pvc-retention finalizer when the CR is already deleting", func() {
			// Edge case: a CR that arrives at Reconcile already deleting
			// (DeletionTimestamp set) and without a pvc-retention
			// finalizer must NOT have the finalizer stamped on. Otherwise
			// the operator would re-block GC on a terminating CR and the
			// reconcileDeletion path would have to clean up immediately —
			// race-prone and pointless work.
			//
			// Hold the CR in `Terminating` with a non-pvc-retention
			// "test-keep-around" finalizer so envtest's apiserver doesn't
			// drop the CR the moment Delete is called. That way Reconcile
			// observes a CR with `!DeletionTimestamp.IsZero()` and no
			// pvc-retention finalizer — the exact condition the production
			// guard handles.
			const name = "fz-skip-on-deleting"
			const holdFinalizer = "velkir.ioxie.dev/test-keep-around"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(stripFinalizers, name)
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			// Add a non-pvc-retention finalizer so the CR stays in
			// Terminating after Delete. The pvc-retention finalizer is
			// intentionally NOT added — the guard's job is to make sure
			// reconcile doesn't add it on a deleting CR.
			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			patched := fetched.DeepCopy()
			Expect(controllerutil.AddFinalizer(patched, holdFinalizer)).To(BeTrue(), "test setup: hold finalizer must be a fresh add")
			Expect(k8sClient.Patch(ctx, patched, client.MergeFrom(fetched))).To(Succeed())

			// Delete the CR — DeletionTimestamp gets set; the hold
			// finalizer keeps the object around so Reconcile can observe
			// the deleting state.
			toDelete := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, toDelete)).To(Succeed())

			terminating := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, terminating)).To(Succeed())
			Expect(terminating.DeletionTimestamp).NotTo(BeNil(), "test setup: deletionTimestamp must be set before reconcile")
			Expect(terminating.Finalizers).To(ContainElement(holdFinalizer), "test setup: hold finalizer must still be present")
			Expect(terminating.Finalizers).NotTo(ContainElement(PVCRetentionFinalizer), "test precondition: pvc-retention finalizer must be absent before reconcile")

			// Reconcile against the deleting CR. The Add path is gated
			// on !deleting, so it must skip; reconcileDeletion runs
			// (PVCs are noop because the test doesn't create any) and
			// the Remove path is gated on ContainsFinalizer, so it
			// also skips.
			reconcileOnce(name)

			// Production guard verified: pvc-retention finalizer was NOT
			// added to a deleting CR. Hold finalizer is still present
			// (we never removed it).
			afterReconcile := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, afterReconcile)).To(Succeed())
			Expect(afterReconcile.Finalizers).NotTo(ContainElement(PVCRetentionFinalizer), "guard failed: pvc-retention finalizer was added to a deleting CR")
			Expect(afterReconcile.Finalizers).To(ContainElement(holdFinalizer), "test sanity: hold finalizer should still keep CR around")
		})
	})

	Context("Created artifacts shape", func() {
		It("creates the client Service, headless Service, ConfigMap, and STS for a minimal CR", func() {
			const name = "shape-cr"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-valkey-conf", Namespace: "default"}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey("valkey.conf"))

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
			Expect(sts.Spec.ServiceName).To(Equal(name + "-headless"))
			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("valkey/valkey:8.1.6-alpine"))
			Expect(sts.Spec.Template.Spec.SecurityContext.RunAsNonRoot).NotTo(BeNil())
			Expect(*sts.Spec.Template.Spec.SecurityContext.RunAsNonRoot).To(BeTrue())
			Expect(sts.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation).NotTo(BeNil())
			Expect(*sts.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation).To(BeFalse())

			clientSvc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, clientSvc)).To(Succeed())
			Expect(clientSvc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))

			headless := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-headless", Namespace: "default"}, headless)).To(Succeed())
			Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		})

		It("stamps Status.Conditions and Status.Phase on the CR after reconcile", func() {
			// Baseline coverage: a reconcile must update CR.Status — the
			// happy path stamps at least the Reconciled condition and
			// derives a non-empty Phase. Without this assertion, a
			// reconciler whose status-update path silently no-ops would
			// pass the artifact-shape spec above (which only checks
			// child resources) — classic coverage illusion.
			const name = "status-cr"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			Expect(fetched.Status.Conditions).NotTo(BeEmpty(),
				"reconciler must stamp at least one condition; got %#v", fetched.Status.Conditions)
			Expect(fetched.Status.Phase).NotTo(BeEmpty(),
				"reconciler must derive a non-empty Phase; got %q", fetched.Status.Phase)

			conditionTypes := map[string]bool{}
			for _, c := range fetched.Status.Conditions {
				conditionTypes[c.Type] = true
			}
			Expect(conditionTypes).To(HaveKey(orchestration.TypeReconciled),
				"Reconciled condition must be present after a successful reconcile; got %v", conditionTypes)
		})

		// A steady-state reconcile must return a non-zero
		// RequeueAfter so a missed watch event (informer cache resync
		// gap, watch-reconnect bookmark drift) can't strand the CR
		// until operator restart. The baseline watchdog fires after
		// at most baselineReconcileWatchdog; a tighter substate hint
		// is allowed to win (mergeRequeue semantics).
		It("returns a non-zero RequeueAfter so missed watch events don't strand the CR", func() {
			const name = "watchdog-cr"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0),
				"reconcile must always set RequeueAfter (steady-state watchdog) so missed watch events don't strand the CR; got %v", result.RequeueAfter)
			Expect(result.RequeueAfter).To(BeNumerically("<=", baselineReconcileWatchdog),
				"RequeueAfter must not exceed baselineReconcileWatchdog (%v); got %v", baselineReconcileWatchdog, result.RequeueAfter)
		})

		// Integration coverage for the sentinel-mode requeue path: a real
		// sentinel Reconcile must drive the status defer (which calls
		// applyStatusRequeue) and return a bounded, positive requeue. This
		// exercises the wired path end-to-end; the keep-alive merge VALUE
		// (that a sentinel CR's baseline requeue is tightened to the
		// keep-alive interval) is the mutation-killing assertion in
		// TestApplyStatusRequeue — it cannot be isolated through Reconcile
		// in envtest, where a bootstrapping sentinel CR's body requeue
		// always dominates the 30s keep-alive (no converged sentinel
		// cluster without a kubelet to ready the pods).
		It("drives the status-defer requeue path on a sentinel-mode Reconcile", func() {
			const name = "keepalive-cr"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
				v.Spec.Valkey.Replicas = 3
				v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
					MasterName: "mymaster", Replicas: 3, Quorum: 2,
					DownAfterMilliseconds: 30000, FailoverTimeout: 180000, ParallelSyncs: 1,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0),
				"a sentinel reconcile must always set a requeue so missed watch events don't strand the CR")
			Expect(result.RequeueAfter).To(BeNumerically("<=", baselineReconcileWatchdog),
				"requeue must not exceed the baseline watchdog (%v); got %v", baselineReconcileWatchdog, result.RequeueAfter)
		})

		// A reconcile that early-returns before Phase 11 (here: nil
		// SentinelObserver → the sentinel-orchestration guard fires) must
		// NOT refresh masterInfoObservedAt. The every-pass status defer
		// reads that timestamp to decide whether the MasterLost latch is
		// fresh enough to drive Ready; if an early-return path bumped it,
		// a stale latch would masquerade as a current measurement.
		It("does not refresh masterInfoObservedAt on a reconcile that skips the probe", func() {
			const name = "stale-observe-cr"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
				v.Spec.Valkey.Replicas = 3
				v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
					MasterName: "mymaster", Replicas: 3, Quorum: 2,
					DownAfterMilliseconds: 30000, FailoverTimeout: 180000, ParallelSyncs: 1,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			key := types.NamespacedName{Name: name, Namespace: "default"}

			// Seed a stale MasterLost latch as if a prior Phase-11 pass
			// stamped it, then went quiet (no probe since).
			staleObs := time.Now().Add(-2 * masterInfoProbeFreshnessWindow)
			since := time.Now().Add(-2 * time.Second)
			st := r.stateFor(key).quorumTracker()
			st.mu.Lock()
			st.masterInfoTimeoutSince = &since
			st.masterInfoObservedAt = staleObs
			st.mu.Unlock()

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			st.mu.Lock()
			gotObs := st.masterInfoObservedAt
			st.mu.Unlock()
			Expect(gotObs).To(Equal(staleObs),
				"an early-return reconcile (probe skipped) must leave masterInfoObservedAt untouched; got %v want %v", gotObs, staleObs)
		})

		It("includes the exporter sidecar only when metrics.enabled=true", func() {
			const name = "metrics-cr"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				t := true
				v.Spec.Metrics.Enabled = &t
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(2))
			Expect(sts.Spec.Template.Spec.Containers[1].Name).To(Equal("exporter"))
		})

		It("init container and announce-IP wiring", func() {
			const name = "m22-wiring"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			// The init-scripts ConfigMap is required; its absence means
			// the init container would CrashLoopBackOff at pod start.
			scripts := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-init-scripts", Namespace: "default"}, scripts)).To(Succeed())
			Expect(scripts.Data).To(HaveKey("render-valkey-conf.sh"))
			Expect(scripts.Data["render-valkey-conf.sh"]).To(ContainSubstring("_POD_IP_"))

			// The valkey-conf ConfigMap now carries the renderer's
			// template — the placeholder the init container will
			// substitute must be present.
			conf := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-valkey-conf", Namespace: "default"}, conf)).To(Succeed())
			Expect(conf.Data["valkey.conf"]).To(ContainSubstring("replica-announce-ip _POD_IP_"))

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())

			// Init container shape: name, env (Downward API), mounts.
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			ic := sts.Spec.Template.Spec.InitContainers[0]
			Expect(ic.Name).To(Equal("render-config"))
			var sawPodIP, sawPodName, sawAppName bool
			for _, e := range ic.Env {
				switch e.Name {
				case "POD_IP":
					Expect(e.ValueFrom).NotTo(BeNil())
					Expect(e.ValueFrom.FieldRef).NotTo(BeNil())
					Expect(e.ValueFrom.FieldRef.FieldPath).To(Equal("status.podIP"))
					sawPodIP = true
				case "POD_NAME":
					Expect(e.ValueFrom).NotTo(BeNil())
					Expect(e.ValueFrom.FieldRef).NotTo(BeNil())
					Expect(e.ValueFrom.FieldRef.FieldPath).To(Equal("metadata.name"))
					sawPodName = true
				case "APP_NAME":
					Expect(e.Value).To(Equal(name))
					sawAppName = true
				}
			}
			Expect(sawPodIP).To(BeTrue(), "init container must wire POD_IP via Downward API")
			Expect(sawPodName).To(BeTrue(), "init container must wire POD_NAME via Downward API")
			Expect(sawAppName).To(BeTrue(), "init container must wire APP_NAME from CR name")

			// Main container has the CLI flag + matching POD_IP env.
			main := sts.Spec.Template.Spec.Containers[0]
			Expect(main.Args).To(ContainElements("--replica-announce-ip", "$(POD_IP)"))
			var mainSawPodIP bool
			for _, e := range main.Env {
				if e.Name == "POD_IP" {
					Expect(e.ValueFrom).NotTo(BeNil())
					Expect(e.ValueFrom.FieldRef.FieldPath).To(Equal("status.podIP"))
					mainSawPodIP = true
				}
			}
			Expect(mainSawPodIP).To(BeTrue(), "main valkey container must wire POD_IP for the CLI flag substitution")
			Expect(main.Command).To(ContainElement("/config/valkey.conf"),
				"main valkey container should read the substituted config from the shared emptyDir")

			// Bootstrap mount must be optional so a CR without sentinels
			// (which don't ship the producer side) still schedules cleanly.
			var sawBootstrapVolume bool
			for _, vol := range sts.Spec.Template.Spec.Volumes {
				if vol.Name == "bootstrap" {
					Expect(vol.ConfigMap).NotTo(BeNil())
					Expect(vol.ConfigMap.Optional).NotTo(BeNil())
					Expect(*vol.ConfigMap.Optional).To(BeTrue())
					sawBootstrapVolume = true
				}
			}
			Expect(sawBootstrapVolume).To(BeTrue(), "pod must mount the sentinel-bootstrap ConfigMap (optional)")
		})

		It("<cr>-ro Service exists with role=replica selector", func() {
			const name = "m23-ro-svc"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			ro := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-ro", Namespace: "default"}, ro)).To(Succeed())
			Expect(ro.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			Expect(ro.Spec.Selector).To(HaveKeyWithValue(RoleLabel, roleValueReplica),
				"<cr>-ro must select pods with role=replica")
			Expect(ro.Spec.Selector).To(HaveKeyWithValue(ComponentLabel, componentValkey),
				"<cr>-ro must still scope by component to avoid cross-CR leakage")
			Expect(ro.Spec.Selector).To(HaveKeyWithValue(CRLabel, name),
				"<cr>-ro must still scope by CR")
			Expect(ro.Spec.Ports).To(HaveLen(1))
			Expect(ro.Spec.Ports[0].Port).To(Equal(int32(6379)))

			client := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, client)).To(Succeed())
			Expect(client.Spec.Selector).To(HaveKeyWithValue(RoleLabel, roleValuePrimary),
				"client Service selector must require role=primary so writes only land on the writeable pod")
		})

		It("Phase 7 stamps role=primary on pod-0 and role=replica on others", func() {
			const name = "m23-role-stamp"
			cr := makeCR(name, nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			// Reconcile once so the CR + owned ConfigMaps + STS exist;
			// the STS itself doesn't materialise pods in envtest, so
			// the test seeds the pods directly with the labels the
			// real STS would stamp.
			reconcileOnce(name)

			seedPod := func(podName string) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:     podOrdinal(podName),
							CRLabel:           name,
							ComponentLabel:    componentValkey,
							ManagedByLabel:    ManagedByValue,
							AppNameLabel:      "valkey",
							AppInstanceLabel:  name,
							AppComponentLabel: componentValkey,
							AppPartOfLabel:    "velkir",
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "valkey",
							Image: "valkey/valkey:8.1.6-alpine",
						}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
			}
			seedPod(name + "-0")
			seedPod(name + "-1")
			seedPod(name + "-2")

			// Re-reconcile so Phase 7 sees the seeded pods.
			reconcileOnce(name)

			pod0 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
			Expect(pod0.Labels).To(HaveKeyWithValue(RoleLabel, roleValuePrimary),
				"pod-0 should be labelled role=primary at bootstrap")

			for _, podName := range []string{name + "-1", name + "-2"} {
				p := &corev1.Pod{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: "default"}, p)).To(Succeed())
				Expect(p.Labels).To(HaveKeyWithValue(RoleLabel, roleValueReplica),
					"non-zero ordinal pods should be labelled role=replica")
			}

			// Idempotency: a third reconcile must not perturb labels
			// or churn the apiserver. Capture resourceVersions and
			// verify they're stable.
			pod0Before := pod0.ResourceVersion
			reconcileOnce(name)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
			Expect(pod0.ResourceVersion).To(Equal(pod0Before),
				"reconcileRoleLabels must be idempotent — no patch when current==desired")
		})

		It("pod template stamps ReplicationReadyGate when mode != standalone", func() {
			const name = "m24-gate-stamp"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				// Mirror the defaulter (envtest doesn't fire it):
				// non-standalone mode + ReadinessGate.Enabled stays nil
				// → stamp by default. The buildReadinessGates helper
				// treats nil as "stamp on".
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.ReadinessGates).To(HaveLen(1),
				"replication-mode pods must carry one readinessGate entry")
			Expect(sts.Spec.Template.Spec.ReadinessGates[0].ConditionType).To(Equal(ReplicationReadyGate))
		})

		It("pod template does NOT stamp the gate in standalone", func() {
			const name = "m24-no-gate-standalone"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.ReadinessGates).To(BeEmpty(),
				"standalone pods must not carry the replication-ready gate")
		})

		It("pod template OPT-OUT honoured (Enabled=false on replication)", func() {
			const name = "m24-gate-optout"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				v.Spec.Valkey.ReadinessGate.Enabled = new(false)
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.ReadinessGates).To(BeEmpty(),
				"explicit opt-out must suppress the gate even in replication mode")
		})

		It("Phase 8 patches gate True for primary, decides per-LagState for replicas", func() {
			const name = "m24-phase8"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(1 << 20)) // 1 MiB
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			fake := newFakeLagChecker()
			fake.byAddr["10.0.0.11:6379"] = valkey.LagState{Role: "slave", LinkUp: true, LagBytes: 100}     // pod-1: in-sync
			fake.byAddr["10.0.0.12:6379"] = valkey.LagState{Role: "slave", LinkUp: true, LagBytes: 1 << 25} // pod-2: behind (32 MiB)
			fake.errAddr["10.0.0.13:6379"] = context.DeadlineExceeded                                       // pod-3: unreachable

			r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}

			// Seed pods with the gate in spec + matching role labels +
			// PodIPs so Phase 8 has something to evaluate. envtest
			// doesn't run a kubelet, so we own the pod.Status writes.
			seedPod := func(podName, role, ip string) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:     podOrdinal(podName),
							CRLabel:           name,
							ComponentLabel:    componentValkey,
							ManagedByLabel:    ManagedByValue,
							AppNameLabel:      "valkey",
							AppInstanceLabel:  name,
							AppComponentLabel: componentValkey,
							AppPartOfLabel:    "velkir",
							RoleLabel:         role,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "valkey",
							Image: "valkey/valkey:8.1.6-alpine",
						}},
						ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				p.Status.PodIP = ip
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			}
			seedPod(name+"-0", roleValuePrimary, "10.0.0.10")
			seedPod(name+"-1", roleValueReplica, "10.0.0.11")
			seedPod(name+"-2", roleValueReplica, "10.0.0.12")
			seedPod(name+"-3", roleValueReplica, "10.0.0.13")

			_, err := r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			condFor := func(podName string) *corev1.PodCondition {
				p := &corev1.Pod{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: "default"}, p)).To(Succeed())
				return findReplicationCondition(p)
			}
			expectGate := func(podName string, want corev1.ConditionStatus, msgSubstr string) {
				cond := condFor(podName)
				Expect(cond).NotTo(BeNil(), "pod %s missing the replication-ready condition", podName)
				Expect(cond.Status).To(Equal(want), "pod %s gate status; message=%q", podName, cond.Message)
				if msgSubstr != "" {
					Expect(cond.Message).To(ContainSubstring(msgSubstr))
				}
			}

			expectGate(name+"-0", corev1.ConditionTrue, "primary")
			expectGate(name+"-1", corev1.ConditionTrue, "link up")
			expectGate(name+"-2", corev1.ConditionFalse, "lag")
			expectGate(name+"-3", corev1.ConditionFalse, "lag check failed")

			// Pod-0's primary path: Phase 8's gate-flip is a short-
			// circuit (no dial). Phase 7a (orphan-master detection)
			// runs first, queries the primary's INFO to verify it
			// actually reports role=master — exactly one dial per
			// reconcile from that path. Equal(1) pins "Phase 7a
			// dialed once, Phase 8 short-circuited" — a regression
			// that adds duplicate primary dials (or that removes
			// Phase 7a's safety check) trips here.
			Expect(fake.callCount("10.0.0.10:6379")).To(Equal(1),
				"Phase 7a dials primary once per reconcile to verify role; Phase 8 short-circuits — combined: exactly 1")
			Expect(fake.callCount("10.0.0.11:6379")).To(BeNumerically(">=", 1),
				"in-sync replica must be checked at least once")

			// Idempotency: status patch is a no-op when condition already
			// matches desired. Capture pod-0's transition time, re-reconcile,
			// confirm it didn't move.
			pod0Before := condFor(name + "-0").LastTransitionTime
			_, err = r.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(condFor(name+"-0").LastTransitionTime).To(Equal(pod0Before),
				"primary's gate condition must not be re-stamped on idempotent reconcile")
		})

		It("scale-up applies the new replica count straight through", func() {
			const name = "m25-scale-up"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(2)))

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			fetched.Spec.Valkey.Replicas = 4
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			rec := reconcileOnceWithRecorder(name)

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(4)),
				"scale-up should apply directly — no primary-removal risk on growth")
			events := drainEvents(rec)
			Expect(containsReason(events, "ScaleRefused")).To(BeFalse(),
				"scale-up must not emit ScaleRefused; got events: %v", events)
		})

		It("scale-down applies cleanly when primary stays inside the new range", func() {
			const name = "m25-scale-down-ok"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			pod0 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name + "-0",
					Namespace: "default",
					Labels: map[string]string{
						CRLabel:        name,
						ComponentLabel: componentValkey,
						RoleLabel:      roleValuePrimary,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod0)).To(Succeed())

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			fetched.Spec.Valkey.Replicas = 2
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			rec := reconcileOnceWithRecorder(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(2)),
				"scale-down should apply when primary stays inside the new range")
			events := drainEvents(rec)
			Expect(containsReason(events, "ScaleRefused")).To(BeFalse(),
				"safe scale-down must not emit ScaleRefused; got events: %v", events)
		})

		It("scale-down REFUSED when it would remove the primary", func() {
			const name = "m25-scale-down-refused"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 5
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			// Stand in for a sentinel-driven flip — primary label
			// sits on pod-3 instead of the bootstrap default.
			pod3 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name + "-3",
					Namespace: "default",
					Labels: map[string]string{
						CRLabel:        name,
						ComponentLabel: componentValkey,
						RoleLabel:      roleValuePrimary,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod3)).To(Succeed())

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			fetched.Spec.Valkey.Replicas = 3
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			rec := reconcileOnceWithRecorder(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(5)),
				"scale-down crossing the primary's ordinal must leave the STS at the prior count")
			events := drainEvents(rec)
			Expect(containsReason(events, "ScaleRefused")).To(BeTrue(),
				"refused scale-down must emit ScaleRefused; got events: %v", events)

			// A safer scale-down — primary at ord 3, target 4 — applies.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			fetched.Spec.Valkey.Replicas = 4
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			rec2 := reconcileOnceWithRecorder(name)

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(4)),
				"scale-down to N where primary ordinal < N should apply normally")
			events = drainEvents(rec2)
			Expect(containsReason(events, "ScaleRefused")).To(BeFalse(),
				"safe scale-down must not re-emit ScaleRefused; got events: %v", events)
		})

		It("Phase 9 deletes highest-ordinal stale REPLICA first; primary untouched", func() {
			const name = "m26-rollout"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			sts.Status.UpdateRevision = testRevNew
			sts.Status.CurrentRevision = testRevOld
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			seedPod := func(podName, role, revHash string) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:              podOrdinal(podName),
							CRLabel:                    name,
							ComponentLabel:             componentValkey,
							RoleLabel:                  role,
							"controller-revision-hash": revHash,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				p.Status.Conditions = []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}}
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			}
			seedPod(name+"-0", roleValuePrimary, testRevOld)
			seedPod(name+"-1", roleValueReplica, testRevOld)
			seedPod(name+"-2", roleValueReplica, testRevOld)

			rec := reconcileOnceWithRecorder(name)

			gone := &corev1.Pod{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-2", Namespace: "default"}, gone)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"highest-ordinal stale replica pod-2 should be deleted; got: %v", err)

			pod1 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, pod1)).To(Succeed())
			Expect(pod1.DeletionTimestamp).To(BeNil(),
				"pod-1 must wait its turn — only one delete per tick")

			pod0 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
			Expect(pod0.DeletionTimestamp).To(BeNil(),
				"primary pod-0 must NOT be deleted — that's primary-rollout territory")

			events := drainEvents(rec)
			Expect(containsReason(events, "PodRolledForConfig")).To(BeTrue(),
				"PodRolledForConfig must fire on each delete; got events: %v", events)
		})

		It("Phase 9 emits RolloutDeferred when only the primary is stale", func() {
			const name = "m26-deferred"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			sts.Status.UpdateRevision = testRevNew
			sts.Status.CurrentRevision = testRevOld
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			seedPod := func(podName, role, revHash string) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:              podOrdinal(podName),
							CRLabel:                    name,
							ComponentLabel:             componentValkey,
							RoleLabel:                  role,
							"controller-revision-hash": revHash,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				p.Status.Conditions = []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}}
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			}
			// Replica already at the new revision; primary still stale
			// — Phase 9 has nothing to delete and should emit
			// RolloutDeferred.
			seedPod(name+"-0", roleValuePrimary, testRevOld)
			seedPod(name+"-1", roleValueReplica, testRevNew)

			rec := reconcileOnceWithRecorder(name)

			pod0 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
			Expect(pod0.DeletionTimestamp).To(BeNil(),
				"primary must NOT be deleted in the config-bump scope")

			events := drainEvents(rec)
			Expect(containsReason(events, "RolloutDeferred")).To(BeTrue(),
				"RolloutDeferred must fire when only primary is stale; got events: %v", events)
		})

		It("in-flight gate defers next delete when a pod is terminating (DeletionTimestamp)", func() {
			const name = "m26-gate-deletion"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			sts.Status.UpdateRevision = testRevNew
			sts.Status.CurrentRevision = testRevOld
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			seedPod := func(podName, role, revHash string, finalizer bool) *corev1.Pod {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:              podOrdinal(podName),
							CRLabel:                    name,
							ComponentLabel:             componentValkey,
							RoleLabel:                  role,
							"controller-revision-hash": revHash,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				if finalizer {
					p.Finalizers = []string{"velkir.ioxie.dev/test-block"}
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
				return p
			}
			seedPod(name+"-0", roleValuePrimary, testRevOld, false)
			pod1 := seedPod(name+"-1", roleValueReplica, testRevOld, false)
			pod2 := seedPod(name+"-2", roleValueReplica, testRevOld, true)

			// Simulate "we already deleted pod-2 last reconcile" — the
			// finalizer keeps the object around so DeletionTimestamp
			// surfaces on a Get, mirroring real-world graceful
			// termination.
			Expect(k8sClient.Delete(ctx, pod2)).To(Succeed())
			DeferCleanup(func() {
				cur := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-2", Namespace: "default"}, cur); err == nil {
					cur.Finalizers = nil
					_ = k8sClient.Update(ctx, cur)
				}
			})
			refetched := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-2", Namespace: "default"}, refetched)).To(Succeed())
			Expect(refetched.DeletionTimestamp).NotTo(BeNil(), "test fixture: pod-2 must show DeletionTimestamp")

			rec := reconcileOnceWithRecorder(name)

			// Pod-1 must NOT have been deleted — the gate held on
			// pod-2's pending termination.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, pod1)).To(Succeed())
			Expect(pod1.DeletionTimestamp).To(BeNil(),
				"in-flight gate must defer next delete while pod-2 is still terminating")

			events := drainEvents(rec)
			Expect(containsReason(events, "PodRolledForConfig")).To(BeFalse(),
				"no further roll until pod-2's recreation finishes; got: %v", events)
		})

		It("in-flight gate defers next delete when a pod is NotReady", func() {
			const name = "m26-gate-notready"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sts)).To(Succeed())
			sts.Status.UpdateRevision = testRevNew
			sts.Status.CurrentRevision = testRevOld
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			seedPod := func(podName, role, revHash string, ready corev1.ConditionStatus) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:              podOrdinal(podName),
							CRLabel:                    name,
							ComponentLabel:             componentValkey,
							RoleLabel:                  role,
							"controller-revision-hash": revHash,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}}
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			}
			// pod-2 is the recently-recreated replacement that hasn't
			// reached Ready yet; pod-1 is still on the old revision and
			// must wait its turn even though pod-2 has no
			// DeletionTimestamp.
			seedPod(name+"-0", roleValuePrimary, testRevOld, corev1.ConditionTrue)
			seedPod(name+"-1", roleValueReplica, testRevOld, corev1.ConditionTrue)
			seedPod(name+"-2", roleValueReplica, testRevNew, corev1.ConditionFalse)

			rec := reconcileOnceWithRecorder(name)

			pod1 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, pod1)).To(Succeed())
			Expect(pod1.DeletionTimestamp).To(BeNil(),
				"in-flight gate must defer next delete while pod-2 is NotReady")

			events := drainEvents(rec)
			Expect(containsReason(events, "PodRolledForConfig")).To(BeFalse(),
				"no further roll until pod-2 reaches Ready; got: %v", events)
		})

		It("ReplicationPrimaryLost fires when no pod carries role=primary", func() {
			const name = "m27-primary-lost"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			// Seed only NON-pod-0 pods so no pod ever carries role=primary
			// after Phase 7. desiredRoleForPod assigns role=replica to
			// any non-zero ordinal; with no pod-0 the post-Phase-7 list
			// has zero primaries and the detection path must fire.
			seedReplica := func(podName string) {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels: map[string]string{
							podIndexLabel:  podOrdinal(podName),
							CRLabel:        name,
							ComponentLabel: componentValkey,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
			}
			seedReplica(name + "-1")
			seedReplica(name + "-2")

			rec := reconcileOnceWithRecorder(name)
			events := drainEvents(rec)
			Expect(containsReason(events, "ReplicationPrimaryLost")).To(BeTrue(),
				"ReplicationPrimaryLost must fire when no pod carries role=primary; got: %v", events)
		})

		It("ReplicationPrimaryLost does NOT fire in standalone", func() {
			const name = "m27-no-loss-standalone"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			rec := reconcileOnceWithRecorder(name)
			events := drainEvents(rec)
			Expect(containsReason(events, "ReplicationPrimaryLost")).To(BeFalse(),
				"standalone must not enter the primary-loss path; got: %v", events)
		})

		It("Phase 9 short-circuits in standalone mode", func() {
			const name = "m26-standalone"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			rec := reconcileOnceWithRecorder(name)

			events := drainEvents(rec)
			Expect(containsReason(events, "PodRolledForConfig")).To(BeFalse(),
				"standalone must not enter the rollout path; got: %v", events)
			Expect(containsReason(events, "RolloutDeferred")).To(BeFalse(),
				"standalone must not enter the rollout path; got: %v", events)
		})

		It("configurationOverrides bumps the rendered hash", func() {
			const name = "m22-override-hash"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)
			before := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-valkey-conf", Namespace: "default"}, before)).To(Succeed())
			beforeHash := before.Annotations[ConfigHashAnnotation]
			Expect(beforeHash).NotTo(BeEmpty())

			// Mutate ConfigurationOverrides and re-reconcile; the
			// rendered output must change, and so must the hash that
			// drives rollout triggers.
			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			fetched.Spec.Valkey.ConfigurationOverrides = map[string]string{"maxmemory": "1gb"}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			reconcileOnce(name)

			after := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-valkey-conf", Namespace: "default"}, after)).To(Succeed())
			Expect(after.Annotations[ConfigHashAnnotation]).NotTo(Equal(beforeHash),
				"editing configurationOverrides must flip the rendered hash so pods roll on the next apply")
			Expect(after.Data["valkey.conf"]).To(ContainSubstring("maxmemory 1gb"))
		})
	})

	Context("Single-shot annotation self-clear", func() {
		It("strips velkir.ioxie.dev/accept-pvc-loss after a successful reconcile", func() {
			const name = "single-shot"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Annotations = map[string]string{AcceptPVCLossAnnotation: "true"}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			Expect(fetched.Annotations).NotTo(HaveKey(AcceptPVCLossAnnotation))
		})

		It("preserves arbitrary user annotations", func() {
			const name = "user-ann"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Annotations = map[string]string{"team": "platform", AcceptPVCLossAnnotation: "true"}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fetched)).To(Succeed())
			Expect(fetched.Annotations).To(HaveKeyWithValue("team", "platform"))
			Expect(fetched.Annotations).NotTo(HaveKey(AcceptPVCLossAnnotation))
		})
	})

	Context("PDB phase", func() {
		It("does not create a PDB for replicas=1 (standalone)", func() {
			const name = "no-pdb"
			Expect(k8sClient.Create(ctx, makeCR(name, nil))).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			pdb := &policyv1.PodDisruptionBudget{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, pdb)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"phase 6 must short-circuit for standalone replicas=1; expected PDB NotFound, got err=%v", err)
			pdbCluster := &policyv1.PodDisruptionBudget{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name + suffixClusterPDB, Namespace: "default"}, pdbCluster)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"standalone must not create the cluster PDB either; got err=%v", err)
		})

		// TestPDBRoleAgnostic — the cluster PDB's selector spans the
		// valkey side of the CR by CRLabel + ComponentLabel only and
		// stays invariant under arbitrary role-label mutations. The
		// selector's invariance is what backs the runtime invariant
		// (currentHealthy > 0 across label transitions): envtest has
		// no kube-controller-manager to recompute status, so the test
		// asserts the property the PDB controller would observe.
		It("TestPDBRoleAgnostic: selector matches every valkey pod across the failover and manual-relabel paths", func() {
			const name = "pdb-roleag"
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffixClusterPDB, Namespace: "default"}, pdb)).To(Succeed(),
				"cluster PDB must exist for replicas=3")
			Expect(pdb.Spec.Selector).NotTo(BeNil(), "cluster PDB selector must be set")
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue(CRLabel, name))
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue(ComponentLabel, componentValkey))
			Expect(pdb.Spec.Selector.MatchLabels).NotTo(HaveKey(RoleLabel),
				"role-agnostic invariant: selector must not pin RoleLabel; otherwise the role-strip window opens a zero-match gap")
			Expect(pdb.Spec.Selector.MatchExpressions).To(BeEmpty(),
				"cluster PDB must rely on matchLabels alone so a future RoleLabel matchExpression cannot smuggle role-awareness back in")
			Expect(pdb.Spec.MinAvailable).NotTo(BeNil(), "cluster PDB minAvailable must derive from replicas - 1")
			Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(2),
				"derived default must be replicas - 1 (3 - 1 = 2)")
			Expect(pdb.OwnerReferences).To(HaveLen(1))
			Expect(pdb.OwnerReferences[0].Name).To(Equal(name))

			selector := labels.SelectorFromValidatedSet(pdb.Spec.Selector.MatchLabels)

			// Seed three valkey pods under this CR. Ordinal 0 starts
			// primary; the other two are replicas — the bootstrap
			// topology the reconciler would have stamped in production.
			podLabels := func(role string) map[string]string {
				m := map[string]string{
					CRLabel:        name,
					ComponentLabel: componentValkey,
				}
				if role != "" {
					m[RoleLabel] = role
				}
				return m
			}
			seedPod := func(podName, role string) *corev1.Pod {
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: "default",
						Labels:    podLabels(role),
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
				return p
			}
			pod0 := seedPod(name+"-0", roleValuePrimary)
			pod1 := seedPod(name+"-1", roleValueReplica)
			pod2 := seedPod(name+"-2", roleValueReplica)

			countMatches := func(stage string) {
				GinkgoHelper()
				pods := &corev1.PodList{}
				Expect(k8sClient.List(ctx, pods, client.InNamespace("default"),
					client.MatchingLabels{CRLabel: name, ComponentLabel: componentValkey})).To(Succeed())
				matched := 0
				for i := range pods.Items {
					if selector.Matches(labels.Set(pods.Items[i].Labels)) {
						matched++
					}
				}
				Expect(matched).To(Equal(3),
					"%s: cluster PDB selector must match every valkey pod of the CR (got %d of 3); a drop here is the zero-match gap the cluster PDB exists to close", stage, matched)
			}

			patchLabels := func(p *corev1.Pod, mutate func(map[string]string)) {
				GinkgoHelper()
				fresh := &corev1.Pod{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: p.Name, Namespace: p.Namespace}, fresh)).To(Succeed())
				old := fresh.DeepCopy()
				if fresh.Labels == nil {
					fresh.Labels = map[string]string{}
				}
				mutate(fresh.Labels)
				Expect(k8sClient.Patch(ctx, fresh, client.StrategicMergeFrom(old))).To(Succeed())
			}

			// Baseline — every pod carries its bootstrap role label.
			countMatches("baseline (primary + 2 replicas labelled)")

			// Failover-initiated path: the master-aware rollout
			// strips the primary role label BEFORE issuing SENTINEL
			// FAILOVER so the Service stops routing writes during
			// the election window. Role-scoped PDBs briefly have
			// zero matches on the primary side; the cluster PDB
			// must not.
			patchLabels(pod0, func(m map[string]string) { delete(m, RoleLabel) })
			countMatches("failover path: primary role label stripped")

			// A replica is promoted to primary; there's a brief
			// window where both the old- and new-primary intents
			// coexist. The cluster PDB selector must keep the same
			// match count regardless of the intermediate shape.
			patchLabels(pod1, func(m map[string]string) { m[RoleLabel] = roleValuePrimary })
			countMatches("failover path: replica promoted to primary")

			patchLabels(pod0, func(m map[string]string) { m[RoleLabel] = roleValueReplica })
			countMatches("failover path: ex-primary relabelled to replica (transition complete)")

			// Manual-relabel path: an operator-driven relabel after
			// sentinel reports a new primary IP. The label patches
			// pass through the same primitives, exercising the same
			// invariant from a different entry point.
			patchLabels(pod1, func(m map[string]string) { delete(m, RoleLabel) })
			patchLabels(pod2, func(m map[string]string) { delete(m, RoleLabel) })
			countMatches("manual relabel: both replica-labelled pods cleared mid-transition")

			patchLabels(pod2, func(m map[string]string) { m[RoleLabel] = roleValuePrimary })
			patchLabels(pod0, func(m map[string]string) { m[RoleLabel] = roleValueReplica })
			patchLabels(pod1, func(m map[string]string) { m[RoleLabel] = roleValueReplica })
			countMatches("manual relabel: settled on pod2 primary, others replica")
		})

		It("honors a user-supplied MaxUnavailable override on the cluster PDB", func() {
			const name = "pdb-override"
			one := intstr.FromInt32(1)
			cr := makeCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MaxUnavailable: &one}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanupCR, name)
			DeferCleanup(cleanupOwned, name)

			reconcileOnce(name)

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffixClusterPDB, Namespace: "default"}, pdb)).To(Succeed())
			Expect(pdb.Spec.MinAvailable).To(BeNil(), "override path must not also stamp MinAvailable (PDB API rejects both)")
			Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
		})
	})
})
