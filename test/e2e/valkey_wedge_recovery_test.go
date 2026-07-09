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
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// Dead-master wedge recovery: two permanent-outage scenarios the
// operator must help resolve. Scenario 1 is a total wedge — every
// address the sentinel quorum knows is dead, so Sentinel's own failover
// provably cannot resolve it (its candidate set died with the master)
// and the operator's dead-master re-point + zero-master recovery
// election are the only escape. Scenario 2 kills the master and one
// sentinel while the replicas survive, so Sentinel may promote a
// replica on its own; there the operator's guaranteed contribution is
// recovering the force-recreated, now peer-empty sentinel via
// RESET+MONITOR. Each scenario pins the operator recovery path(s) it is
// guaranteed to exercise; without operator involvement the cluster
// stays Ready=False forever (the state previously required a full
// teardown to escape).
//
// Both scenarios ride one Ordered container on one CR: scenario 2
// deliberately continues from scenario 1's recovered state, mirroring
// how compound failures arrive in sequence on a real cluster.
var _ = Describe("Valkey sentinel — dead-master wedge recovery", Ordered, func() {

	const crName = "wedge-recovery"

	BeforeAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
  sentinel:
    masterName: mymaster
    replicas: 3
`)
		waitForReady(crName, 5*time.Minute)
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", crName,
			"--ignore-not-found", "--wait=false",
		))
	})

	// Scenario 1 — the total wedge: every valkey pod force-deleted at
	// once. The sentinels keep monitoring the dead master address and
	// their replica tables hold only dead addresses; the recreated pods
	// come up with new IPs the sentinels have never heard of. Nothing
	// sentinel-side can ever converge this.
	It("recovers after every sentinel-known address dies at once", func() {
		By("force-deleting all valkey pods simultaneously")
		podsOut := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=valkey",
			"-o", "jsonpath={.items[*].metadata.name}",
		))
		pods := strings.Fields(strings.TrimSpace(podsOut))
		Expect(pods).To(HaveLen(3), "expected the 3 bootstrapped valkey pods")
		args := append([]string{"-n", e2eNamespace, "delete", "pod",
			"--force", "--grace-period=0", "--wait=false"}, pods...)
		mustRun(exec.Command("kubectl", args...))

		assertWedgeRecovered(crName, "SentinelDeadMasterRepoint", "RecoveryPromotionInitiated")
	})

	// Scenario 2 — the compound kill: operator + one sentinel + the
	// current master die together. The rebuilt sentinel comes back
	// empty; the survivors keep a ghost identity and a dead master
	// entry; the replacement operator pod must reconstruct the master
	// from ground truth before any recovery can fire.
	It("recovers when operator, one sentinel, and the master die together", func() {
		By("locating the current primary pod")
		primary := ""
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=valkey,velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			primary = strings.TrimSpace(out)
			return primary
		}, 2*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "a primary-labeled pod must exist before the kill")

		By("locating the operator pod")
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(),
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty())

		By("force-deleting operator + sentinel-0 + primary simultaneously")
		mustRun(exec.Command("kubectl", "-n", dualkillOperatorNS(), "delete", "pod", operatorPod,
			"--force", "--grace-period=0", "--wait=false"))
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "delete", "pod",
			crName+"-sentinel-0", primary,
			"--force", "--grace-period=0", "--wait=false"))

		assertWedgeRecovered(crName, "SentinelStrandedRecovery",
			"SentinelDeadMasterRepoint", "RecoveryPromotionInitiated")
	})
})

// assertWedgeRecovered pins the full convergence contract after a
// dead-master wedge: the CR returns to Ready, exactly one pod carries
// the primary label, and every sentinel's monitored master address is
// a live valkey pod's IP. The budget is generous — the recovery chain
// is promotion (when needed) → REMOVE+MONITOR re-point → gossip →
// Phase 7 relabel → replica reattach, with per-stage cooldowns —
// but bounded: the pre-fix behavior is Ready=False forever.
func assertWedgeRecovered(crName string, operatorRecoveryReasons ...string) {
	GinkgoHelper()

	By("waiting for the CR to return to Ready=True")
	waitForReady(crName, 8*time.Minute)

	By("verifying exactly one primary-labeled pod")
	Eventually(func() int {
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=valkey,velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[*].metadata.name}",
		))
		return len(strings.Fields(strings.TrimSpace(out)))
	}, 3*time.Minute, 5*time.Second).Should(Equal(1), "exactly one primary-labeled pod after recovery")

	By("verifying every sentinel monitors a live valkey pod")
	Eventually(func() error {
		ipsOut, err := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=valkey",
			"-o", "jsonpath={.items[*].status.podIP}",
		))
		if err != nil {
			return fmt.Errorf("list pod IPs: %w", err)
		}
		live := map[string]bool{}
		for _, ip := range strings.Fields(strings.TrimSpace(ipsOut)) {
			live[ip] = true
		}
		return forEachSentinel(crName, func(pod string) error {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", pod, "-c", "sentinel", "--",
				"sh", "-c",
				sentinelCLI("", "SENTINEL get-master-addr-by-name mymaster")+" | head -1 | tr -d '\\r'",
			))
			if err != nil {
				return fmt.Errorf("%s: get-master-addr: %w", pod, err)
			}
			addr := strings.TrimSpace(out)
			if !live[addr] {
				return fmt.Errorf("%s still monitors %q which is no live pod IP", pod, addr)
			}
			return nil
		})
	}, 4*time.Minute, 10*time.Second).Should(Succeed(),
		"all sentinels must converge onto a live master address")

	By("verifying an operator recovery event fired (proving operator involvement in the recovery)")
	Eventually(func() error {
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "events",
			"--field-selector", "involvedObject.name="+crName,
			"-o", "jsonpath={range .items[*]}{.reason}{\"\\n\"}{end}",
		))
		for _, reason := range operatorRecoveryReasons {
			if strings.Contains(out, reason) {
				return nil
			}
		}
		return fmt.Errorf("no operator recovery event among [%s] in reasons:\n%s",
			strings.Join(operatorRecoveryReasons, ", "), out)
	}, 2*time.Minute, 10*time.Second).Should(Succeed(),
		"recovery must be attributable to an operator recovery path")
}
