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

// Keep-alive requeue cadence for `mode: sentinel`: once a sentinel CR
// is converged and QUIET (no rollout, no failover, no spec change),
// the only thing scheduling the next reconcile is the status keep-alive
// requeue merged into the reconcile Result. Each reconcile re-stamps
// the SentinelQuorum records' lastObservedTime; the aggregator drops
// records older than its freshness window, flipping PrimaryConfirmed
// off True. So on a quiet cluster the SQ re-stamp cadence IS the
// keep-alive wiring made observable: if the merge into the Result is
// dropped, reconciles stop, the records freeze, and this spec's
// advance-within-a-window assertion fails.
//
// This is the real-cluster counterpart of the unit merge test: envtest
// cannot cover the call-site because a bootstrapping CR's body requeue
// always dominates there (no kubelet, so no converged sentinel
// cluster). A separate Describe (own ns BeforeAll + per-spec CR
// cleanup) keeps the long observation window out of the main sentinel
// suite's Ordered run, matching the sibling quiet-window scenarios.
var _ = Describe("Valkey sentinel — keep-alive requeue cadence", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label; see the sibling
		// quiet-window Describe for why this cannot rely on another
		// container's BeforeAll having run first.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
	})

	It("re-stamps SentinelQuorum within the freshness window and holds PrimaryConfirmed on a quiet cluster", func() {
		const (
			crName     = "sentinel-keepalive-cadence"
			secretName = "keepalive-cadence-auth"
			masterName = "mymaster"
			// The aggregator's freshness window (60s; keep-alive re-stamps
			// at half that). Mirrored here rather than imported: the spec
			// must pin the externally observable contract, not track an
			// internal constant that a regression could move.
			freshnessWindow = 60 * time.Second
			// Slack on top of the window for kubectl round-trips and the
			// write itself; cadence is ~30s, so window+slack is ~2.5
			// expected re-stamps — comfortably flake-safe while still
			// failing fast when the keep-alive is unwired.
			cadenceSlack = 30 * time.Second
		)

		sqNames := []string{
			crName + "-sentinel-0",
			crName + "-sentinel-1",
			crName + "-sentinel-2",
		}

		// lastObserved reads one SQ record's status.lastObservedTime,
		// parsed; returns the zero time on any transient kubectl/parse
		// error so it is safe inside Eventually.
		lastObserved := func(sq string) time.Time {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sentinelquorums.velkir.ioxie.dev", sq,
				"-o", "jsonpath={.status.lastObservedTime}",
			))
			t, err := time.Parse(time.RFC3339, strings.TrimSpace(out))
			if err != nil {
				return time.Time{}
			}
			return t
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
		// Generous bootstrap budget: a single-node e2e profile (everything
		// co-located, soft anti-affinity) converges a 3+3 cluster in ~5
		// minutes; the signal here is the post-convergence cadence, not
		// bootstrap latency, and Eventually returns as soon as Ready.
		waitForReady(crName, 10*time.Minute)

		By("settling to a quiet steady state (Degraded=False, PrimaryConfirmed=True)")
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 2*time.Minute, 5*time.Second).Should(Equal("False"),
			"a healthy 3+3 cluster must settle to Degraded=False before the cadence window")
		Eventually(func() string {
			return getCondition(crName, "PrimaryConfirmed")
		}, 2*time.Minute, 5*time.Second).Should(Equal("True"),
			"PrimaryConfirmed must be True on the settled cluster (precondition)")

		By("waiting for every SentinelQuorum record to carry a lastObservedTime")
		Eventually(func() bool {
			for _, sq := range sqNames {
				if lastObserved(sq).IsZero() {
					return false
				}
			}
			return true
		}, 2*time.Minute, 5*time.Second).Should(BeTrue(),
			"all three SQ records must be stamped before the cadence window")

		// Two consecutive rounds: each SQ record must advance past its
		// round baseline within one freshness window (+slack). Two rounds
		// prove sustained cadence rather than one incidental reconcile
		// (e.g. a tail event from bootstrap) re-stamping once.
		for round := 1; round <= 2; round++ {
			By(fmt.Sprintf("round %d/2: every SQ record re-stamps within the freshness window", round))
			baseline := map[string]time.Time{}
			for _, sq := range sqNames {
				baseline[sq] = lastObserved(sq)
				Expect(baseline[sq].IsZero()).To(BeFalse(),
					"SQ %q must have a parsable lastObservedTime baseline", sq)
			}
			Eventually(func() bool {
				for _, sq := range sqNames {
					cur := lastObserved(sq)
					if cur.IsZero() || !cur.After(baseline[sq]) {
						return false
					}
				}
				return true
			}, freshnessWindow+cadenceSlack, 5*time.Second).Should(BeTrue(),
				"every SentinelQuorum lastObservedTime must advance within the freshness window on a quiet cluster — fails when the keep-alive requeue is not merged into the reconcile Result")
		}

		By("PrimaryConfirmed held True across the whole quiet window")
		// The user-visible invariant the cadence protects: records never
		// aged out, so aggregation never flipped PrimaryConfirmed off
		// True at any point after settling.
		Expect(getCondition(crName, "PrimaryConfirmed")).To(Equal("True"),
			"PrimaryConfirmed must still be True after two cadence rounds")
	})
})
