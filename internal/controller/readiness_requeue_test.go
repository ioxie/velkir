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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/valkey"
)

// Phase 8 readiness-gate requeue cadence. The 5s requeue used to fire
// for every gated replica regardless of gate status, so a healthy
// steady-state cluster re-polled replication lag every 5s forever — load
// that scaled linearly with CR count. Now a replica whose gate is already
// True re-checks at the relaxed replicaSteadyRecheck cadence — lengthened
// toward the 5-min baseline watchdog once the owned-Pod watch and observer
// push began covering every non-lag wake-up. A replica still
// converging (gate not yet True) keeps the fast readinessGateRequeue so
// it catches up quickly, and a single converging pod keeps the whole CR
// fast.
var _ = Describe("Phase 8 readiness-gate requeue cadence", func() {
	ctx := context.Background()

	makeCR := func(name string, replicas int32) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeReplication,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey: valkeyv1beta1.ValkeyPodSpec{
					Replicas:    replicas,
					Persistence: &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")},
					ReadinessGate: valkeyv1beta1.ReadinessGateSpec{
						Enabled:     new(true),
						MaxLagBytes: new(int64(1 << 20)),
					},
				},
			},
		}
	}

	// seedPod creates a gated valkey pod for crName with the given role
	// and an optional ReplicationReadyGate condition (nil = condition
	// absent, i.e. not-yet-True / converging).
	seedPod := func(name, crName, ip, role string, gate *corev1.ConditionStatus) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentValkey,
					RoleLabel:      role,
				},
			},
			Spec: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		if gate != nil {
			now := metav1.Now()
			p.Status.Conditions = []corev1.PodCondition{{
				Type:               ReplicationReadyGate,
				Status:             *gate,
				LastTransitionTime: now,
				LastProbeTime:      now,
			}}
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	cleanup := func(crName string, podNames ...string) {
		for _, n := range podNames {
			p := &corev1.Pod{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: n, Namespace: "default"}, p); err == nil {
				_ = k8sClient.Delete(ctx, p, client.GracePeriodSeconds(0))
			}
		}
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: crName, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	gateTrue := corev1.ConditionTrue
	gateFalse := corev1.ConditionFalse

	It("steady replica (gate True) → relaxed cadence, not 5s", func() {
		const name = "rq-steady"
		cr := makeCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPod(name+"-0", name, "10.0.1.10", roleValuePrimary, &gateTrue)
		seedPod(name+"-1", name, "10.0.1.11", roleValueReplica, &gateTrue)
		DeferCleanup(cleanup, name, name+"-0", name+"-1")

		checker := newFakeLagChecker()
		// Caught-up replica: link up, zero lag → gate stays True, so the
		// requeue is the steady-state value (not a flip-driven 5s).
		checker.byAddr["10.0.1.11:6379"] = valkey.LagState{Role: "slave", LinkUp: true, LagBytes: 0}
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: checker}

		requeue, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(Equal(replicaSteadyRecheck),
			"a steady (gate True) replica must re-poll at the relaxed cadence, not the 5s converging cadence")
		// The steady-state lag re-poll relaxes toward the 5-min
		// baselineReconcileWatchdog now that the Pod watch and
		// observer push cover every non-lag wake-up. Pin the
		// relaxation (≥2m, bounded by the watchdog) and the invariant
		// that the bootstrap fast-path stays strictly faster — a
		// regression that re-couples them or reverts toward 5s fails here.
		Expect(requeue).To(BeNumerically(">", readinessGateRequeue),
			"steady-state cadence must stay slower than the bootstrap fast-path")
		Expect(requeue).To(BeNumerically(">=", 2*time.Minute),
			"steady-state lag re-poll must be relaxed toward the 5-min baseline (#554)")
		Expect(requeue).To(BeNumerically("<=", baselineReconcileWatchdog),
			"steady-state cadence must not exceed the baseline watchdog floor")
	})

	It("converging replica (gate False) → fast 5s cadence", func() {
		const name = "rq-converging"
		cr := makeCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPod(name+"-0", name, "10.0.2.10", roleValuePrimary, &gateTrue)
		seedPod(name+"-1", name, "10.0.2.11", roleValueReplica, &gateFalse)
		DeferCleanup(cleanup, name, name+"-0", name+"-1")

		checker := newFakeLagChecker()
		checker.byAddr["10.0.2.11:6379"] = valkey.LagState{Role: "slave", LinkUp: false} // still catching up
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: checker}

		requeue, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(Equal(readinessGateRequeue),
			"a converging (gate not yet True) replica must poll fast")
	})

	It("mixed: one converging + one steady → fast cadence wins", func() {
		const name = "rq-mixed"
		cr := makeCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPod(name+"-0", name, "10.0.3.10", roleValuePrimary, &gateTrue)
		seedPod(name+"-1", name, "10.0.3.11", roleValueReplica, &gateTrue)
		seedPod(name+"-2", name, "10.0.3.12", roleValueReplica, &gateFalse)
		DeferCleanup(cleanup, name, name+"-0", name+"-1", name+"-2")

		checker := newFakeLagChecker()
		checker.byAddr["10.0.3.11:6379"] = valkey.LagState{Role: "slave", LinkUp: true, LagBytes: 0}
		checker.byAddr["10.0.3.12:6379"] = valkey.LagState{Role: "slave", LinkUp: false}
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: checker}

		requeue, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(Equal(readinessGateRequeue),
			"a single converging replica must keep the whole CR on the fast cadence")
		// Guard: relaxing the steady cadence must never stretch
		// the re-evaluation window while any pod is still converging — a
		// not-yet-Ready pod pulls the whole CR back below the relaxed
		// steady cadence so a wrong-pod/role state is re-checked promptly.
		Expect(requeue).To(BeNumerically("<", replicaSteadyRecheck),
			"a converging pod must pull the CR below the relaxed steady cadence (wrong-pod re-evaluation window guard)")
	})

	It("steady primary only → no requeue (primaries don't drive cadence)", func() {
		const name = "rq-primary"
		// replicas=2 satisfies the mode=replication CRD validator; the
		// fixture only needs the single primary pod to prove a steady
		// primary contributes no requeue.
		cr := makeCR(name, 2)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPod(name+"-0", name, "10.0.4.10", roleValuePrimary, &gateTrue)
		DeferCleanup(cleanup, name, name+"-0")

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: newFakeLagChecker()}
		requeue, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeZero(),
			"a steady primary must not drive the requeue cadence")
	})
})
