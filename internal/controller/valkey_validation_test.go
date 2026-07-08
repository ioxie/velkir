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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// CEL acceptance / rejection coverage for the Valkey CRD's standalone subset.
// Each It exercises one rule with a paired accept-shape and reject-shape. The
// envtest API server applies defaulting + CEL just as a real cluster would.

const svcLoadBalancer = "LoadBalancer"

var _ = Describe("Valkey CRD validation", func() {
	ctx := context.Background()

	standaloneCR := func(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
		cr := &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeStandalone,
				// Defaulter would stamp this in production; envtest
				// doesn't run the webhook chain so the schema-level
				// CEL would otherwise fail with "no such key:
				// replicas" on the standalone-replicas-==-1 rule.
				Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 1},
			},
		}
		if mutate != nil {
			mutate(cr)
		}
		return cr
	}

	cleanup := func(cr *valkeyv1beta1.Valkey) {
		_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
	}

	Context("CR name (root-level CEL)", func() {
		It("accepts an RFC-1035 DNS label up to 35 characters", func() {
			cr := standaloneCR("ok-name-35chars-xxxxxxxxxxxxxxxxxxx", nil)
			Expect(cr.Name).To(HaveLen(35))
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects a name containing uppercase characters", func() {
			cr := standaloneCR("Bad-Name", nil)
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("RFC-1035 DNS label"))
		})

		It("rejects a name longer than 35 characters", func() {
			cr := standaloneCR(strings.Repeat("a", 36), nil)
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("RFC-1035 DNS label"))
		})

		It("rejects a name starting with a digit", func() {
			cr := standaloneCR("1-bad-start", nil)
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("RFC-1035 DNS label"))
		})
	})

	Context("Mode enum gate", func() {
		It("accepts mode=standalone", func() {
			cr := standaloneCR("mode-standalone", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts mode=replication with required preconditions", func() {
			cr := standaloneCR("mode-replication", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts mode=sentinel with required preconditions", func() {
			cr := standaloneCR("mode-sentinel", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
					MasterName:            "test-master",
					Replicas:              3,
					Quorum:                2,
					DownAfterMilliseconds: 30000,
					FailoverTimeout:       180000,
					ParallelSyncs:         1,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})
		It("rejects updates to spec.mode (immutable in v1beta1)", func() {
			cr := standaloneCR("mode-immutable", nil) // mode=standalone, replicas=1
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)

			// Flip to replication and satisfy replication's own CEL
			// preconditions, so the only rule left to fail is the
			// mode-immutability rule — keeps the assertion unambiguous
			// rather than co-firing the replication shape rules.
			cr.Spec.Mode = valkeyv1beta1.ModeReplication
			cr.Spec.Valkey.Replicas = 2
			cr.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
			err := k8sClient.Update(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.mode is immutable"))
		})
	})

	Context("Replication-mode CEL preconditions", func() {
		It("rejects mode=replication when replicas < 2", func() {
			cr := standaloneCR("repl-one-replica", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 1
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mode=replication requires valkey.replicas >= 2"))
		})

		It("rejects mode=replication without persistence", func() {
			cr := standaloneCR("repl-no-persistence", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mode=replication requires spec.valkey.persistence"))
		})

		It("accepts MinReplicasToWrite at floor (1)", func() {
			one := int32(1)
			cr := standaloneCR("repl-min-write", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
				v.Spec.Valkey.MinReplicasToWrite = &one
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects MinReplicasToWrite below floor", func() {
			zero := int32(0)
			cr := standaloneCR("repl-min-write-zero", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
				v.Spec.Valkey.MinReplicasToWrite = &zero
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minReplicasToWrite"))
		})

		It("accepts MinReplicasMaxLag at floor (10)", func() {
			ten := int32(10)
			cr := standaloneCR("repl-max-lag", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
				v.Spec.Valkey.MinReplicasMaxLag = &ten
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects MinReplicasMaxLag below floor", func() {
			five := int32(5)
			cr := standaloneCR("repl-max-lag-tight", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 2
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
				v.Spec.Valkey.MinReplicasMaxLag = &five
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("minReplicasMaxLag"))
		})
	})

	Context("Sentinel-mode CEL preconditions", func() {
		// validSentinelCR returns a sentinel-mode CR shape that passes
		// every sentinel-side CEL by default. The per-test mutator
		// narrows down to the field under test.
		validSentinelCR := func(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
			return standaloneCR(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
				v.Spec.Valkey.Replicas = 3
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
					Size: resource.MustParse("1Gi"),
				}
				v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
					MasterName:            "test-master",
					Replicas:              3,
					Quorum:                2,
					DownAfterMilliseconds: 30000,
					FailoverTimeout:       180000,
					ParallelSyncs:         1,
				}
				if mutate != nil {
					mutate(v)
				}
			})
		}

		It("rejects mode=sentinel without spec.sentinel", func() {
			cr := standaloneCR("sent-no-sentinel", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeSentinel
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.sentinel is required when mode=sentinel"))
		})

		It("rejects sentinel.replicas < 3", func() {
			cr := validSentinelCR("sent-two-sentinels", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.Replicas = 2
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sentinel.replicas must be >= 3"))
		})

		// Sentinel timing-floor enforcement (downAfterMilliseconds >= 1000,
		// failoverTimeout >= 180000) moved from CRD CEL to the validating
		// webhook so the `velkir.ioxie.dev/allow-aggressive-timeouts`
		// annotation can grant the documented bypass — CEL bound to
		// SentinelPodSpec `self` cannot read `metadata.annotations`. envtest
		// here exercises the apiserver schema layer only (no validating
		// webhook running), so a sub-floor sentinel CR now reaches etcd —
		// the rejection lives in the unit-test suite for the validator
		// (`internal/webhook/v1beta1/valkey_validator_test.go ::
		// TestValidator_SentinelTimingFloors_*`).
		It("apiserver accepts sub-floor sentinel timing — floor enforcement is webhook-side now (#102)", func() {
			cr := validSentinelCR("sent-aggressive-down-cel-relaxed", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.DownAfterMilliseconds = 5000
				v.Spec.Sentinel.FailoverTimeout = 30000
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects masterName with disallowed characters", func() {
			cr := validSentinelCR("sent-bad-master-name", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.MasterName = "Test_Master"
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("masterName"))
		})

		It("rejects updates to masterName (D12 immutability)", func() {
			cr := validSentinelCR("sent-mn-immutable", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)

			cr.Spec.Sentinel.MasterName = "different-master"
			err := k8sClient.Update(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("masterName is immutable"))
		})
	})

	Context("Standalone replicas constraint (CEL rule 10)", func() {
		It("accepts replicas=1", func() {
			cr := standaloneCR("replicas-one", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.Replicas = 1
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects replicas>1 in standalone mode", func() {
			cr := standaloneCR("replicas-two", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.Replicas = 2
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mode=standalone requires valkey.replicas == 1"))
		})
	})

	Context("Missing valkey.replicas — CEL has() guard (#515)", func() {
		// Previously these shapes errored "no such key: replicas" at
		// admission whenever the mutating webhook hadn't already stamped
		// valkey.replicas (failurePolicy=Ignore, timeout, namespaceSelector
		// miss). replicas has no CRD structural default, so an unset value
		// reaches CEL absent. The has() guard admits the unset shape and
		// defers the value to the defaulter. Replicas=0 here serialises to
		// absent via omitempty — the same object the apiserver sees when a
		// user omits the field. envtest runs no defaulter, so this is the
		// exact pre-default object the un-guarded rule choked on.
		It("admits standalone with spec.valkey set but replicas unset (repro)", func() {
			cr := standaloneCR("standalone-no-replicas", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.Replicas = 0
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("4Gi")}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("admits replication with persistence but replicas unset", func() {
			cr := standaloneCR("replication-no-replicas", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Mode = valkeyv1beta1.ModeReplication
				v.Spec.Valkey.Replicas = 0
				v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("admits bootstrapNode with replicas unset", func() {
			cr := standaloneCR("bootstrap-no-replicas", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.Replicas = 0
				v.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{
					Host: "primary.example.com",
					Port: 6379,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})
	})

	Context("LoadBalancer source ranges", func() {
		It("accepts client type=LoadBalancer when source ranges are provided", func() {
			cr := standaloneCR("lb-with-ranges", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Service.Client.Type = svcLoadBalancer
				v.Spec.Service.Client.LoadBalancerSourceRanges = []string{"10.0.0.0/8"}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects client type=LoadBalancer without source ranges", func() {
			cr := standaloneCR("lb-no-ranges", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Service.Client.Type = svcLoadBalancer
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("loadBalancerSourceRanges is required"))
		})

		It("rejects sentinel service type=LoadBalancer without source ranges", func() {
			cr := standaloneCR("sentinel-lb-no-ranges", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Service.Sentinel.Type = svcLoadBalancer
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("loadBalancerSourceRanges is required"))
		})
	})

	Context("Readiness gate (CEL: not meaningful for standalone)", func() {
		It("accepts an absent readinessGate", func() {
			cr := standaloneCR("rg-absent", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts readinessGate.enabled=false", func() {
			cr := standaloneCR("rg-off", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.ReadinessGate = valkeyv1beta1.ReadinessGateSpec{Enabled: new(false)}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects readinessGate.enabled=true in standalone", func() {
			cr := standaloneCR("rg-on", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.ReadinessGate = valkeyv1beta1.ReadinessGateSpec{Enabled: new(true)}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("readinessGate.enabled is not meaningful"))
		})
	})

	Context("sentinelAuthSecretName cross-field rule", func() {
		It("rejects sentinelAuthSecretName when mode is not sentinel", func() {
			cr := standaloneCR("sentinel-auth-on-standalone", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Auth = &valkeyv1beta1.AuthSpec{
					SentinelAuthSecretName: "irrelevant",
				}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sentinelAuthSecretName is only permitted when mode=sentinel"))
		})

		It("accepts a regular auth.secretName on standalone", func() {
			cr := standaloneCR("auth-secret-only", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Auth = &valkeyv1beta1.AuthSpec{
					SecretName: "valkey-auth",
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})
	})

	Context("BootstrapNode shape", func() {
		It("accepts a valid host:port", func() {
			cr := standaloneCR("bootstrap-ok", func(v *valkeyv1beta1.Valkey) {
				v.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{
					Host: "primary.example.com",
					Port: 6379,
				}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects an empty host", func() {
			cr := standaloneCR("bootstrap-empty-host", func(v *valkeyv1beta1.Valkey) {
				v.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{
					Host: "",
					Port: 6379,
				}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("host"))
		})

		It("rejects a port outside [1, 65535]", func() {
			cr := standaloneCR("bootstrap-bad-port", func(v *valkeyv1beta1.Valkey) {
				v.Spec.BootstrapNode = &valkeyv1beta1.BootstrapNodeSpec{
					Host: "primary.example.com",
					Port: 70000,
				}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("port"))
		})
	})

	Context("PDBSpec mutual exclusivity", func() {
		It("accepts pdb with only minAvailable set", func() {
			cr := standaloneCR("pdb-min-only", func(v *valkeyv1beta1.Valkey) {
				min := intstr.FromInt(1)
				v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts pdb with only maxUnavailable set", func() {
			cr := standaloneCR("pdb-max-only", func(v *valkeyv1beta1.Valkey) {
				max := intstr.FromInt(1)
				v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MaxUnavailable: &max}
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects pdb with both minAvailable and maxUnavailable set", func() {
			cr := standaloneCR("pdb-both", func(v *valkeyv1beta1.Valkey) {
				min := intstr.FromInt(1)
				max := intstr.FromInt(1)
				v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min, MaxUnavailable: &max}
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	Context("Rollout MaxLagBytes bounds (CEL rule 21 + field-level Min/Max)", func() {
		It("accepts rollout.maxLagBytes=0 (lower bound)", func() {
			cr := standaloneCR("rollout-lag-zero", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Rollout.MaxLagBytes = 0
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts rollout.maxLagBytes=10000 (default)", func() {
			cr := standaloneCR("rollout-lag-default", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Rollout.MaxLagBytes = 10000
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("accepts rollout.maxLagBytes=10485760 (10 MiB upper bound)", func() {
			cr := standaloneCR("rollout-lag-max", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Rollout.MaxLagBytes = 10485760
			})
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)
		})

		It("rejects rollout.maxLagBytes=-1 (below field-level Minimum)", func() {
			cr := standaloneCR("rollout-lag-neg", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Rollout.MaxLagBytes = -1
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxLagBytes"))
		})

		It("rejects rollout.maxLagBytes=10485761 (above field-level Maximum)", func() {
			cr := standaloneCR("rollout-lag-over", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Rollout.MaxLagBytes = 10485761
			})
			err := k8sClient.Create(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxLagBytes"))
		})
	})

	Context("Defaulting (sanity)", func() {
		It("fills in defaults from the apiserver schema for a minimal CR", func() {
			cr := standaloneCR("defaults-image", nil)
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(cleanup, cr)

			fetched := &valkeyv1beta1.Valkey{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), fetched)).To(Succeed())
			Expect(fetched.Spec.PVCRetentionPolicy).To(Equal(valkeyv1beta1.PVCRetentionRetain))
			Expect(fetched.Spec.Service.Client.Type).To(Equal("ClusterIP"))
			Expect(fetched.Spec.Service.Sentinel.Type).To(Equal("ClusterIP"))
			Expect(fetched.Spec.Valkey.Replicas).To(BeEquivalentTo(1))
			Expect(fetched.Spec.Rollout.MaxUnavailable).To(BeEquivalentTo(1))
			Expect(fetched.Spec.Rollout.ReplicaReadyTimeoutSeconds).To(BeEquivalentTo(300))
			Expect(fetched.Spec.Rollout.MaxLagBytes).To(BeEquivalentTo(10000))
			// Image repository/tag defaults moved from schema markers to the
			// defaulting webhook, so they are not applied by apiserver
			// schema defaulting (this envtest does not run the webhook); they are
			// covered by TestDefaulter_StampsImageDefaults in the webhook package.
		})
	})
})
