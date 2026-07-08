//go:build e2e
// +build e2e

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

package e2e

import (
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// Quiet-window coverage for `mode: sentinel`: a routine operator
// restart against an already-healthy cluster must not produce a new
// split-brain signal. The startup safety-net re-polls every sentinel
// on leader-acquire; on a healthy cluster that re-poll must observe
// quorum agreement on the first snapshot and stay silent — no
// SplitBrainDetected event, no Degraded=SplitBrain, no counter advance.
//
// This Describe is the steady-state counterpart of the partition
// scenario in valkey_sentinel_test.go (which asserts the signal DOES
// fire when ≥quorum sentinels are unreachable): here the cluster is
// never partitioned, so the same signals must remain quiet across the
// restart. A separate Describe (own ns BeforeAll + per-spec CR cleanup)
// keeps the long restart window out of the main sentinel suite's
// Ordered run, matching how the dual-kill / scale-down scenarios are
// segregated into their own files.
var _ = Describe("Valkey sentinel — operator-restart quiet-window", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label. Ginkgo randomises
		// top-level container order, so this Describe may run before any
		// other Describe's BeforeAll; create + label our own ns up front
		// rather than depending on registration order. The label makes
		// the chart-deploy operator's webhooks fire on CRs in this ns
		// (namespaceSelector override the harness sets). --overwrite makes
		// a double-stamp a no-op.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Async force-delete of every Valkey CR between specs. `--wait=false`
		// avoids blocking on the pvc-retention finalizer when a spec ends
		// mid-reconcile; unique per-spec CR names sidestep collisions even
		// while a prior CR is still Terminating.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
	})

	It("operator restart of a healthy cluster emits no new split-brain signal", func() {
		const (
			crName     = "sentinel-restart-quiet"
			secretName = "restart-quiet-auth"
			masterName = "mymaster"
		)

		// operatorLabel / operatorNS mirror the operator-kill scenario in
		// the sentinel suite: kustomize-deploy defaults, with shared-cluster
		// runs overriding via E2E_OPERATOR_LABEL / E2E_OPERATOR_NAMESPACE.
		operatorLabel := envOrDefault("E2E_OPERATOR_LABEL", "control-plane=controller-manager")
		operatorNS := namespace

		// degradedReason reads the Degraded condition's reason off the CR.
		// Returns "" on any transient kubectl error so it is safe to call
		// inside Eventually / Consistently (an empty reason can never equal
		// "SplitBrain", so a read flake never trips the quiet-window check).
		degradedReason := func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
			))
			return strings.TrimSpace(out)
		}

		// assertSinglePrimaryOnPod0 pins the bootstrap topology: exactly
		// one of the three valkey pods carries role=primary, and it is
		// pod-0 (sentinel mode keeps the bootstrap primary until the first
		// failover — none happens in this scenario). Run before and after
		// the restart so a regression that drops or duplicates the primary
		// label is caught alongside the split-brain assertions.
		assertSinglePrimaryOnPod0 := func() {
			GinkgoHelper()
			primaries := 0
			for _, ord := range []string{"0", "1", "2"} {
				role := strings.TrimSpace(mustRun(exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-"+ord,
					"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
				)))
				if role == "primary" {
					primaries++
					Expect(ord).To(Equal("0"),
						"the sole primary must be pod-0 (bootstrap topology, no failover in this scenario)")
				}
			}
			Expect(primaries).To(Equal(1),
				"a healthy 3+3 cluster must have exactly one primary")
		}

		By("creating the auth Secret for the sentinel cluster")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", secretName,
			"--from-literal=password=changeme",
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "secret", secretName,
				"--ignore-not-found", "--wait=false",
			))
		})

		By("bootstrapping a healthy auth-enabled sentinel CR (3 valkey + 3 sentinel)")
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
  auth:
    secretName: ` + secretName + `
    secretKey: password
    sentinelAuthSecretName: ` + secretName + `
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		By("settling to a healthy steady state before the restart (Degraded=False)")
		// Wait for Degraded to read False: any transient bootstrap-window
		// sentinel-discovery disagreement (sentinels still finding each
		// other) clears once quorum converges, flipping Degraded off
		// SplitBrain back to False. Observing False here means the
		// bootstrap split-brain tail (if any) is over, so the event
		// baseline captured below is stable.
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 2*time.Minute, 5*time.Second).Should(Equal("False"),
			"a healthy 3+3 cluster must settle to Degraded=False before the restart")
		Expect(degradedReason()).NotTo(Equal("SplitBrain"),
			"no split-brain may be active on the settled cluster (precondition)")

		By("verifying the bootstrap topology: exactly one primary, on pod-0")
		assertSinglePrimaryOnPod0()

		By("recording the pre-restart SplitBrainDetected event baseline")
		// SplitBrainDetected is emitted from the same reconcile branch
		// that increments the split-brain detection counter (1:1
		// emit-side coupling on the `QuorumOK==false` guard), so an
		// unchanged event count across the restart already implies the
		// counter did not advance. The direct /metrics scrape at the end
		// of the spec corroborates that by reading the counter value
		// itself, in both kustomize and shared-cluster deploy modes.
		eventsBefore := countEventsByReason(crName, "SplitBrainDetected")

		By("recording the operator pod name + UID for the restart")
		// UID, not just name: the Deployment recreates with the same
		// label-selector identity but a fresh UID, so the post-restart
		// liveness check waits for a pod whose UID differs from the
		// deleted one (otherwise an Eventually firing against the still-
		// Terminating old pod would report Ready from a doomed pod).
		opPodNameBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "get", "pods",
			"-l", operatorLabel,
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(opPodNameBefore).NotTo(BeEmpty(),
			"operator pod must be running before the restart")
		opPodUIDBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "get", "pod", opPodNameBefore,
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(opPodUIDBefore).NotTo(BeEmpty())

		By("force-deleting the operator pod (crash-like restart, grace-period=0)")
		// Force + grace=0 skips the graceful leader-lease release, so the
		// new pod must re-acquire leadership and run its startup safety-net
		// sentinel re-poll — the path whose job is to recover a cluster
		// that drifted during downtime, and which must stay silent when
		// the cluster is in fact healthy.
		_ = mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "delete", "pod", opPodNameBefore,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the Deployment to recreate the operator pod with a fresh UID and Ready=True")
		var opPodNameAfter string
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "get", "pods",
				"-l", operatorLabel,
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }} {{ .metadata.uid }} "+
					"{{ range .status.conditions }}{{ if eq .type \"Ready\" }}{{ .status }}{{ end }}{{ end }}"+
					"\n{{ end }}{{ end }}",
			))
			if err != nil {
				return ""
			}
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 3 {
					continue
				}
				name, uid, ready := fields[0], fields[1], fields[2]
				if uid == opPodUIDBefore || ready != "True" {
					continue
				}
				opPodNameAfter = name
				return name
			}
			return ""
		}, 3*time.Minute, 2*time.Second).ShouldNot(BeEmpty(),
			"Deployment must recreate the operator pod and the new pod must reach Ready=True post-restart")
		Expect(opPodNameAfter).NotTo(Equal(opPodNameBefore),
			"the new operator pod must differ from the deleted one (Deployment recreate, not the old Terminating pod)")

		By("asserting no new split-brain signal across the restart + startup re-poll window")
		// The core invariant: a restart against an already-healthy
		// cluster stays quiet. Across a window comfortably longer than
		// leader-election re-acquisition plus the observer's first poll
		// cycles, assert (a) no new SplitBrainDetected event beyond the
		// pre-restart baseline — and therefore no counter advance — and
		// (b) the Degraded condition never reports reason=SplitBrain. A
		// regression where the startup safety-net re-poll re-fires
		// split-brain on a healthy cluster trips both.
		//
		// `<=` (not `==`) on the count tolerates a transient kubectl read
		// returning 0 without a false failure: a genuine new event makes
		// the count strictly exceed the baseline on the next poll, while a
		// flake-zero stays at-or-below it. The window starts at new-pod-
		// Ready (before leadership ack) so it brackets the entire
		// re-acquire + re-poll sequence where a spurious signal would fire.
		Consistently(func(g Gomega) {
			g.Expect(countEventsByReason(crName, "SplitBrainDetected")).To(
				BeNumerically("<=", eventsBefore),
				"no new SplitBrainDetected event may fire on a healthy-cluster operator restart")
			g.Expect(degradedReason()).NotTo(Equal("SplitBrain"),
				"Degraded must never report reason=SplitBrain on a healthy-cluster operator restart")
		}, 90*time.Second, 5*time.Second).Should(Succeed())

		By("verifying the cluster stays healthy after the restart (Ready=True, single primary on pod-0)")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 2*time.Minute, 5*time.Second).Should(Equal("True"),
			"the CR must remain Ready=True after the operator restart")
		assertSinglePrimaryOnPod0()

		By("scraping /metrics directly: the split-brain detection counter must be absent or zero")
		// Direct counter confirmation, complementing the event-count proxy
		// above: on a cluster that never split-brained, the per-CR
		// valkey_split_brain_detections_total series is either absent (a
		// CounterVec series is created lazily on first increment) or zero. A
		// regression that fired a spurious detection would publish a positive
		// value. The Has() liveness check proves the scrape returned a
		// functional exposition page first, so an absent counter can't mask a
		// broken scrape path.
		metrics, err := utils.OperatorMetrics(operatorNS)
		Expect(err).NotTo(HaveOccurred(), "scraping the operator /metrics endpoint")
		Expect(metrics.Has("go_goroutines")).To(BeTrue(),
			"the /metrics scrape must return a functional exposition page")
		if val, found := metrics.Value(
			"valkey_split_brain_detections_total",
			map[string]string{"namespace": e2eNamespace, "name": crName},
		); found {
			Expect(val).To(BeNumerically("==", 0),
				"the per-CR split-brain detection counter must not advance on a healthy-cluster operator restart")
		}
	})
})
