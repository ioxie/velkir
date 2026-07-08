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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/defaults"
)

// regression gate: a CR admitted WITHOUT the defaulting webhook
// (envtest runs no webhooks — exactly the failurePolicy=Ignore outage
// shape) must render byte-identical ConfigMaps, config hashes, and STS
// pod templates before and after the defaults are later persisted to
// the stored object. Pre-normalization, the late-arriving defaults
// changed the render inputs (min-replicas lines, probes, resources),
// flipped the cmHash, and config-rolled every pod mid-bootstrap.
var _ = Describe("reconcile-side spec normalization (#583)", func() {
	ctx := context.Background()

	It("renders identically before and after late default persistence", func() {
		const name = "i583-normalize"
		// Only the fields CEL forces a user to set — the most
		// un-defaulted sentinel CR that can exist in etcd.
		cr := &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeSentinel,
				Sentinel: &valkeyv1beta1.SentinelPodSpec{
					Replicas:   3,
					Quorum:     2,
					MasterName: "mymaster",
				},
			},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		})

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		crKey := types.NamespacedName{Namespace: "default", Name: name}

		renderAll := func() (string, string, map[string]string, string) {
			v := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, crKey, v)).To(Succeed())
			// Mirror Reconcile Phase 0a': normalize the in-memory view
			// before any render.
			defaults.ApplySpecDefaults(v)
			valkeyHash, err := r.reconcileConfigMaps(ctx, v)
			Expect(err).NotTo(HaveOccurred())
			sentinelHash, err := r.reconcileSentinelConfigMaps(ctx, v)
			Expect(err).NotTo(HaveOccurred())

			cmData := map[string]string{}
			for _, suffix := range []string{suffixValkeyConf, suffixSentinelConf} {
				cm := &corev1.ConfigMap{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name + suffix}, cm)).To(Succeed())
				for k, val := range cm.Data {
					cmData[suffix+"/"+k] = val
				}
			}

			// Pod template parity via the pure builders — the STS
			// revision is a function of these.
			valkeySTS, err := json.Marshal(buildValkeySTS(v, valkeyHash))
			Expect(err).NotTo(HaveOccurred())
			sentinelSTS, err := json.Marshal(buildSentinelSTS(v, sentinelHash, 0))
			Expect(err).NotTo(HaveOccurred())
			return valkeyHash, sentinelHash, cmData, string(valkeySTS) + "\x00" + string(sentinelSTS)
		}

		valkeyHash1, sentinelHash1, cmData1, templates1 := renderAll()

		// The reconciler's renders must not have persisted any spec
		// change: the stored CR is still at generation 1.
		stored := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, crKey, stored)).To(Succeed())
		Expect(stored.Generation).To(Equal(int64(1)),
			"render pass must not write spec defaults back to the stored CR")

		// Late default persistence: the defaulting webhook comes back
		// (or an operator upgrade brings new stamps) and the stored
		// spec gains every default in one UPDATE.
		defaults.ApplySpecDefaults(stored)
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())
		Expect(k8sClient.Get(ctx, crKey, stored)).To(Succeed())
		Expect(stored.Generation).To(BeNumerically(">", int64(1)),
			"precondition: the simulated late defaulting must be a real spec change")

		valkeyHash2, sentinelHash2, cmData2, templates2 := renderAll()

		Expect(valkeyHash2).To(Equal(valkeyHash1),
			"valkey config hash moved when defaults were persisted — admission state leaked into the render")
		Expect(sentinelHash2).To(Equal(sentinelHash1),
			"sentinel config hash moved when defaults were persisted — admission state leaked into the render")
		Expect(cmData2).To(Equal(cmData1),
			"rendered ConfigMap content changed when defaults were persisted")
		Expect(templates2).To(Equal(templates1),
			"STS pod templates changed when defaults were persisted — pods would config-roll")
	})

	// Enforces the "never write the normalized spec back" contract via the
	// REAL Reconcile entrypoint (not a manual ApplySpecDefaults mirror):
	// after a full reconcile of an un-defaulted CR, the stored spec must be
	// byte-identical to what was created. A future write path that persists
	// the normalized view (full Update of the CR, or a Patch whose base
	// isn't normalized) bumps generation and fails here.
	It("Reconcile does not persist normalized spec defaults to the stored CR", func() {
		const name = "i583-nowriteback"
		// Standalone reconciles with the least scaffolding (no sentinel
		// infra) while still exercising every spec-shaping stamp
		// (replicas, probes, resources, readinessGate, PDB-skip).
		cr := &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeStandalone},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(func() {
			fresh := &valkeyv1beta1.Valkey{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, fresh); err == nil {
				_ = k8sClient.Delete(ctx, fresh, client.GracePeriodSeconds(0))
			}
		})

		stored := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, stored)).To(Succeed())
		specBefore := stored.Spec.DeepCopy()
		genBefore := stored.Generation

		r := &ValkeyReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Recorder:  k8sevents.NewFakeRecorder(64),
		}
		// A couple of reconciles to be sure no later pass persists defaults.
		for range 2 {
			_, err := r.Reconcile(ctx, reconcileRequestFor(name, "default"))
			Expect(err).NotTo(HaveOccurred())
		}

		after := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, after)).To(Succeed())
		Expect(after.Generation).To(Equal(genBefore),
			"Reconcile bumped spec generation — it persisted normalized defaults")
		Expect(after.Spec).To(Equal(*specBefore),
			"stored spec changed after Reconcile — normalized defaults leaked back to the API")
	})
})

func reconcileRequestFor(name, ns string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}}
}
