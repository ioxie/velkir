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

// False-positive-rate coverage for `mode: sentinel` at the sanctioned default
// downAfterMilliseconds (3000). A transient *sub-down-after* stall of the
// primary valkey process — a freeze shorter than the down-after window — must
// NOT trip a spurious +sdown/failover: Sentinel needs `downAfterMilliseconds`
// of continuous non-response before it marks +sdown, so a stall well under
// that window must leave the topology untouched. This is the steady-state
// safety counterpart to the sustained-loss failover scenarios in
// valkey_sentinel_test.go (which assert failover DOES fire on a real primary
// loss); here the freeze is deliberately too short to be a real failure.
//
// The stall is induced with SIGSTOP/SIGCONT on the primary's valkey-server,
// which is PID 1 in the data-plane container (the entrypoint is
// `valkey-server <conf>`, no shell wrapper), self-signalled from inside the
// container as the same non-root UID — same-user signalling is permitted, and
// a `kubectl exec` spawns a fresh process even while PID 1 is stopped, so the
// resume call lands. The freeze window is chosen well below the readiness-probe
// failure threshold (~30s = period 10s × failureThreshold 3) so the kubelet
// never evicts the frozen pod and confounds the sentinel-only signal under test.
//
// A separate Describe (own ns BeforeAll + per-spec CR cleanup) keeps the chaos
// window out of the main sentinel suite's Ordered run, matching how the
// dual-kill / scale-down / restart-quiet scenarios are segregated.
var _ = Describe("Valkey sentinel — aggressive-timeout false-positive quiet-window", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label. Ginkgo randomises top-level
		// container order, so create + label our own ns up front rather than
		// depending on registration order. --overwrite makes a double-stamp a
		// no-op.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Async force-delete of every Valkey CR between specs — unique per-spec
		// CR names sidestep collisions even while a prior CR is Terminating.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
	})

	It("a sub-down-after primary stall produces no spurious failover at the default down-after", func() {
		const (
			crName     = "sentinel-aggro-quiet"
			secretName = "aggro-quiet-auth"
			masterName = "mymaster"
			// downAfterMS is the sanctioned default; pinned explicitly so the
			// stall-window math below is independent of the defaulter and the
			// scenario keeps testing the documented default value.
			downAfterMS = 3000
			// stallSeconds is comfortably below downAfterMS/1000 (3s) so
			// Sentinel's down-after timer never elapses, AND far below the
			// readiness-probe failure threshold (~30s) so the kubelet never
			// evicts the frozen pod and confounds the sentinel-only signal.
			stallSeconds = 2
		)

		// degradedReason reads the Degraded condition's reason off the CR.
		// Returns "" on any transient kubectl error so it is safe to call inside
		// Consistently (an empty reason can never equal "SplitBrain", so a read
		// flake never trips the quiet-window check).
		degradedReason := func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
			))
			return strings.TrimSpace(out)
		}

		// assertSinglePrimaryOnPod0 pins the bootstrap topology: exactly one of
		// the three valkey pods carries role=primary, and it is pod-0 (no
		// failover happens in this scenario). Run before and after the stall so
		// a regression that fails over (or drops/duplicates the primary label)
		// is caught.
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

		By(fmt.Sprintf("bootstrapping a healthy auth-enabled sentinel CR at downAfterMilliseconds=%d", downAfterMS))
		applyCR(fmt.Sprintf(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: %s
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: %s
    replicas: 3
    downAfterMilliseconds: %d
  auth:
    secretName: %s
    secretKey: password
    sentinelAuthSecretName: %s
    sentinelAuthSecretKey: password
`, crName, masterName, downAfterMS, secretName, secretName))
		waitForReady(crName, 5*time.Minute)

		By("settling to a healthy steady state (Degraded=False) before the stall")
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 2*time.Minute, 5*time.Second).Should(Equal("False"),
			"a healthy 3+3 cluster must settle to Degraded=False before the stall")
		assertSinglePrimaryOnPod0()

		By("confirming the default-band CR surfaces the durable WarnAggressiveTimeouts deviation Event")
		// downAfterMilliseconds=3000 sits in the accepted-but-aggressive band
		// [1000, 30000), so the operator re-surfaces the admission
		// below-recommended warning as a durable, latched Warning Event — the
		// reconciler half of the warning surface. Latched once-per-(CR, reason, field) per
		// process, so it lands on the CR's first reconcile and persists.
		Eventually(func() int {
			return countEventsByReason(crName, "WarnAggressiveTimeouts")
		}, 2*time.Minute, 5*time.Second).Should(BeNumerically(">=", 1),
			"a sentinel CR at the default down-after (3000ms, below the recommended 30000ms) must emit a durable WarnAggressiveTimeouts Event")

		By("recording failover-signal baselines before the stall")
		baselineFailover := countEventsByReason(crName, "FailoverInitiated")
		baselineUnexpected := countEventsByReason(crName, "UnexpectedFailover")
		baselineSplitBrain := countEventsByReason(crName, "SplitBrainDetected")

		By(fmt.Sprintf("freezing the primary valkey-server (pod-0, PID 1) for %ds — a transient sub-down-after stall", stallSeconds))
		// kill -STOP 1 freezes valkey-server (PID 1); kill -CONT 1 resumes it.
		// Sentinel sees the master unresponsive for the freeze window, then
		// responsive again — a window shorter than downAfterMilliseconds, so
		// the down-after timer resets without ever reaching +sdown.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"sh", "-c", "kill -STOP 1",
		))
		time.Sleep(stallSeconds * time.Second)
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"sh", "-c", "kill -CONT 1",
		))

		By("asserting no spurious failover signal fired across the stall + recovery window")
		// The core invariant: a stall shorter than downAfterMilliseconds must
		// not trip Sentinel's +sdown, so no FailoverInitiated / UnexpectedFailover
		// fires and no split-brain is declared. `<=` (not `==`) tolerates a
		// transient kubectl read returning a low count without a false failure;
		// a genuine new event makes the count strictly exceed the baseline.
		Consistently(func(g Gomega) {
			g.Expect(countEventsByReason(crName, "FailoverInitiated")).To(
				BeNumerically("<=", baselineFailover),
				"no FailoverInitiated event may fire on a sub-down-after stall")
			g.Expect(countEventsByReason(crName, "UnexpectedFailover")).To(
				BeNumerically("<=", baselineUnexpected),
				"no UnexpectedFailover event may fire on a sub-down-after stall")
			g.Expect(countEventsByReason(crName, "SplitBrainDetected")).To(
				BeNumerically("<=", baselineSplitBrain),
				"no split-brain may be declared on a sub-down-after stall")
			g.Expect(degradedReason()).NotTo(Equal("SplitBrain"),
				"Degraded must never report reason=SplitBrain on a sub-down-after stall")
		}, 30*time.Second, 5*time.Second).Should(Succeed())

		By("verifying the cluster stays healthy and the primary stays on pod-0 after recovery")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 2*time.Minute, 5*time.Second).Should(Equal("True"),
			"the CR must remain Ready=True after the transient stall")
		assertSinglePrimaryOnPod0()
	})
})
