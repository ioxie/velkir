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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// dualkillOperatorNS returns the namespace the operator runs in, with the
// same env override knob the suite-level diagnostics dump uses. Hoisted
// here because the dual-kill tests need to read it from multiple
// scopes (BeforeAll, individual Its) and the suite's value is scoped
// inside the dumpDiagnosticsOnFailure closure.
func dualkillOperatorNS() string {
	if v := os.Getenv("E2E_OPERATOR_NAMESPACE"); v != "" {
		return v
	}
	return "velkir-system"
}

// dualkillOperatorLabel returns the selector that picks the operator
// Deployment's pods. Same env-override shape as dualkillOperatorNS.
func dualkillOperatorLabel() string {
	if v := os.Getenv("E2E_OPERATOR_LABEL"); v != "" {
		return v
	}
	return "control-plane=controller-manager"
}

// sentinelCLI builds a shell command that runs valkey-cli against the local
// sentinel port inside the sentinel container. The container carries no auth
// env var (the operator injects the sentinel password only into the
// config-render init container), so the password must come from the test:
// pass the literal the spec created its auth Secret with, or "" when the CR
// has auth disabled.
func sentinelCLI(authPass, command string) string {
	auth := ""
	if authPass != "" {
		auth = fmt.Sprintf("-a %s --no-auth-warning ", authPass)
	}
	return fmt.Sprintf("valkey-cli -p 26379 %s%s 2>/dev/null", auth, command)
}

// forEachSentinel runs fn against every sentinel pod of crName, returning
// the first error fn yields — shaped to compose inside an Eventually.
func forEachSentinel(crName string, fn func(pod string) error) error {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=sentinel",
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	if err != nil {
		return fmt.Errorf("list sentinels: %w", err)
	}
	pods := strings.Fields(strings.TrimSpace(out))
	if len(pods) == 0 {
		return fmt.Errorf("no sentinel pods found for %s", crName)
	}
	for _, s := range pods {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}

// sentinelMasterNumOtherSentinels reads num-other-sentinels from one
// sentinel pod's SENTINEL MASTER reply (the count of peer sentinels it
// currently knows). Returns "" on any read error so an Eventually retries
// rather than hard-failing on a transient exec miss.
func sentinelMasterNumOtherSentinels(pod string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "exec", pod, "-c", "sentinel", "--",
		"sh", "-c",
		sentinelCLI("", "SENTINEL master mymaster")+" | awk '/^num-other-sentinels$/{getline; print; exit}' | tr -d '\\r' || true",
	))
	return strings.TrimSpace(out)
}

// sentinelOnNode returns the name of a sentinel pod of crName scheduled on
// node, or the first sentinel as a fallback when none matches. On
// single-node minikube every sentinel shares the node, so the fallback is
// the common path there.
func sentinelOnNode(crName, node string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=sentinel",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.spec.nodeName}{\"\\n\"}{end}",
	))
	var fallback string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if fallback == "" {
			fallback = f[0]
		}
		if len(f) >= 2 && f[1] == node {
			return f[0]
		}
	}
	return fallback
}

// Dual-kill end-to-end coverage. Four scenarios; the first three each
// replay a specific incident from valkey-test/cruft, the fourth asserts
// a write-routing correctness invariant — so a future regression trips
// a CI signal:
//
//  1. **Orphan-master demotion** — issue-operator-dualkill-orphan-master
//     (rc.8 residual; fixed by Phase 7a). Dual-kill master +
//     operator → recreated master comes back with PVC intact and
//     starts as role=master internally; Phase 7a must demote via
//     REPLICAOF. Pre-fix bug: orphan pod left as a silent split-brain
//     master with divergent data; next failover would discard writes.
//
//  2. **Sentinel startup-reset gating** — issue-operator-startup-reset
//     -breaks-cluster-after-pod-restart (rc.7 cascading wedge; fixed
//     in rc.8 via RunInitialReset probe gate + SENTINEL MONITOR
//     follow-up). On the consistent-state path (operator restart
//     while sentinels already agree), the safety-net RESET must NOT
//     fire — silence is the success signal. Pre-fix: unconditional
//     RESET wiped sentinel topology, cluster wedged at a defunct
//     master IP.
//
//  3. **Quorum-observer not stuck after bootstrap** —
//     issue-operator-quorum-observer-stuck-after-startup (rc.5/.6
//     latch bug; fixed in rc.7). Once sentinels reach quorum the
//     operator's Degraded=SplitBrain condition must clear within the
//     observer's republish window. Pre-fix: the condition latched on
//     boot-time AUTH failures and never cleared even after auth was
//     fixed.
//
//  4. **Write-Service replica exclusion** — kill cascade (operator +
//     sentinel + a non-primary valkey pod). The `<cr>` write Service
//     selects role=primary only; if the restarting operator transiently
//     stamps a replica as primary before its observer reconverges, that
//     replica enters the write Service EndpointSlice and fresh client
//     connections routed there receive -READONLY on writes — a
//     data-correctness hazard. A background EndpointSlice watcher
//     plus a fresh-connection role probe assert the write Service only
//     ever resolves to the primary throughout recovery.
//
// All four scenarios share the same Describe block so they reuse
// the existing BeforeAll namespace + label setup the other sentinel
// specs depend on. JustAfterEach diagnostics fire from the suite-
// level helper.

var _ = Describe("Valkey sentinel — dual-kill scenarios", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label. Mirrors the
		// pattern in valkey_sentinel_test.go's Describe — Ginkgo
		// v2 randomises top-level Describe order so this can run
		// before the sentinel Describe's BeforeAll fires.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Async delete with finalizer-bypass so spec teardown doesn't
		// hang on retained PVCs from a mid-rollout abort.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
	})

	It("orphan-master demotion: dual-kill master+operator → recreated master demoted via REPLICAOF", func() {
		const crName = "dk-orphan"

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("confirming the bootstrap primary is pod-0 (precondition)")
		Expect(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))).To(Equal("primary"))

		By("locating the operator pod so we can dual-kill")
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(), "-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty(), "operator pod must be present in %s", dualkillOperatorNS())

		By("seeding a write on the bootstrap master so we can later assert convergence")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "dk-orphan-seed", "v1",
		))

		By("dual-killing master pod + operator pod simultaneously")
		// Two fire-and-forget delete commands back-to-back. Either
		// ordering reproduces the rc.8 residual: master pod's PVC
		// survives, it boots back up as role:master before the new
		// operator's Phase 7 stamps the label.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-0",
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "delete", "pod", operatorPod,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the cluster to stabilise after the dual-kill")
		// Eventually the new operator runs Phase 7a; it observes the
		// recreated former-master is labelled replica but reporting
		// role:master via INFO replication, and issues REPLICAOF.
		// Window: leader-elect (~10s) + startup-reset (~5s) + first
		// reconcile (~5s) + INFO probe (~5s) + REPLICAOF (~5s) +
		// kubelet pod-Ready (~10s) ≈ 40s baseline; 3-minute cap for
		// CI variance.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			return strings.TrimSpace(out)
		}, 3*time.Minute, 5*time.Second).Should(Or(Equal("primary"), Equal("replica")),
			"pod-0 must carry a role label after recovery")

		By("verifying NO pod is left as an orphan master (label=replica + INFO role=master)")
		// Iterate every pod labelled role=replica. For each, exec
		// INFO replication and assert role != master. A pod that
		// fails this check is the canonical orphan-master state
		// Phase 7a must prevent.
		Eventually(func() error {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			if err != nil {
				return fmt.Errorf("list replicas: %w", err)
			}
			for _, pod := range strings.Fields(strings.TrimSpace(out)) {
				role, err := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", pod, "-c", "valkey", "--",
					"sh", "-c",
					"valkey-cli -p 6379 -a \"$VALKEY_AUTH_PASS\" INFO replication 2>/dev/null | awk -F: '$1==\"role\"{print $2}' | tr -d '\\r' || true",
				))
				if err != nil {
					return fmt.Errorf("INFO replication on %s: %w", pod, err)
				}
				role = strings.TrimSpace(role)
				if role == "master" {
					return fmt.Errorf("pod %s is an ORPHAN: label=replica but INFO replication reports role=master (Phase 7a should have demoted it)", pod)
				}
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"Phase 7a must demote any pod whose role=replica label disagrees with INFO replication role=master")

		By("verifying the OrphanMasterDemoted event fired (audit trail for the demotion)")
		// The event is the forensic audit trail. If the orphan was
		// in fact detected and demoted, this event fires once per
		// successful REPLICAOF. EventRecorder server-side dedup may
		// merge repeats — we only check presence-at-least-once.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "events",
				"--field-selector", "reason=OrphanMasterDemoted",
				"-o", "jsonpath={.items[*].reason}",
			))
			return strings.TrimSpace(out)
		}, 3*time.Minute, 5*time.Second).Should(ContainSubstring("OrphanMasterDemoted"),
			"OrphanMasterDemoted event must fire — without it, the demotion may have been silent (Phase 7a didn't run or didn't observe the orphan)")

		By("verifying the seeded write survived the recovery (data plane convergence)")
		// The write was on the bootstrap primary before the kill.
		// After recovery, it must be visible from whatever pod is
		// now labelled primary (sentinel-elected, may be pod-0
		// returning or a peer). Read via Service to verify routing.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "run", "dk-orphan-check",
				"--rm", "-i", "--restart=Never", "--image=valkey/valkey:8.1.7-alpine",
				"--", "valkey-cli", "-h", crName, "-p", "6379", "GET", "dk-orphan-seed",
			))
			return strings.TrimSpace(out)
		}, 3*time.Minute, 5*time.Second).Should(ContainSubstring("v1"),
			"the seeded write must survive recovery; data loss here = the wrong pod was promoted")
	})

	It("startup-reset gating: dual-kill on consistent state → safety-net RESET does NOT fire", func() {
		const crName = "dk-reset"

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
    masterName: mymaster
    replicas: 3
`)
		waitForReady(crName, 5*time.Minute)

		By("locating the operator pod to dual-kill")
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(), "-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty())

		By("recording the pre-kill InitialSentinelReset event count baseline")
		// The rc.8 gating fix is "RESET only fires on actual
		// anomaly". A consistent-state operator restart must
		// produce zero NEW InitialSentinelReset events for this
		// CR.
		baselineCount := countEventsForReason(crName, "InitialSentinelReset")

		By("dual-killing master + operator")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-0",
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "delete", "pod", operatorPod,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the cluster to stabilise")
		// Same window as the orphan-master scenario.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`,
			))
			return strings.TrimSpace(out)
		}, 5*time.Minute, 5*time.Second).Should(Equal("True"),
			"cluster must reach Ready=True after dual-kill recovery")

		By("verifying no defunct master IP wedge: every sentinel reports a LIVE pod's IP as master")
		// The pre-fix bug had sentinels stuck at a defunct master IP
		// (the prior pod's IP that the StatefulSet replaced with a
		// new IP). Post-fix, every sentinel's
		// SENTINEL get-master-addr-by-name must resolve to a pod
		// that currently exists.
		Eventually(func() error {
			sentinels, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=sentinel",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			if err != nil {
				return fmt.Errorf("list sentinels: %w", err)
			}
			currentPodIPs := podIPSet(crName)
			for _, s := range strings.Fields(strings.TrimSpace(sentinels)) {
				out, err := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", s, "-c", "sentinel", "--",
					"sh", "-c",
					sentinelCLI("", "SENTINEL get-master-addr-by-name mymaster")+" | head -1 || true",
				))
				if err != nil {
					return fmt.Errorf("get-master-addr-by-name on %s: %w", s, err)
				}
				reportedIP := strings.TrimSpace(out)
				if reportedIP == "" {
					return fmt.Errorf("sentinel %s returned empty master IP — RESET may have wiped state without a MONITOR follow-up", s)
				}
				if !currentPodIPs[reportedIP] {
					return fmt.Errorf("sentinel %s reports master=%s but no current valkey pod has that IP (defunct IP wedge — pre-rc.8 cascading bug)", s, reportedIP)
				}
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"every sentinel must report a master IP matching a live valkey pod")

		By("verifying the InitialSentinelReset event count did NOT increase")
		// rc.8's RunInitialReset gating: probe consistent state →
		// skip RESET → no event emission. The pre-fix bug fired
		// the event unconditionally on every leader-acquire. A
		// regression that reverts to unconditional firing would
		// increase the count past the baseline. We allow at most
		// 1 NEW event (in case the dual-kill DOES create a
		// transient anomaly the gate legitimately responds to);
		// >1 = the gate is broken.
		Eventually(func() int {
			return countEventsForReason(crName, "InitialSentinelReset") - baselineCount
		}, 3*time.Minute, 5*time.Second).Should(BeNumerically("<=", 1),
			"InitialSentinelReset events should fire ≤1× post-dual-kill (rc.8 gating) — more indicates regression to pre-rc.8 unconditional RESET")
	})

	It("triple-kill master+sentinel+operator: peer-list re-bootstrap recovers the cluster (#456)", func() {
		const crName = "dk-tripkill"

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("identifying the current master (the kill target)")
		// On bootstrap the operator stamps role=primary on pod-0. We
		// re-derive at run-time rather than assuming pod-0 stays
		// primary across the BeforeAll → It boundary.
		masterPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(masterPod).NotTo(BeEmpty(), "precondition: exactly one pod must carry role=primary")

		By("locating the operator pod to triple-kill")
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(), "-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty())

		By("triple-killing master valkey pod + first sentinel pod + operator pod simultaneously")
		// Three fire-and-forget delete commands. Reproduces the
		// scenario where the master valkey pod's IP changes on
		// recreation; sentinels retain the OLD master IP; the
		// restarted operator (leader re-acquire) re-establishes the
		// cluster via RunInitialReset's REMOVE + MONITOR on stranded
		// sentinels, so the cluster recovers without manual
		// intervention.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", masterPod,
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-sentinel-0",
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "delete", "pod", operatorPod,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the cluster to converge back to Ready=True")
		// Pre-fix the cluster wedges indefinitely (the bug report
		// observed ~5 min then gave up). Post-fix the operator's
		// peer-restore pass lets sentinels reach quorum, +odown
		// fires, +switch-master elects, role labels land, Service
		// EndpointSlice populates. Budget: 4 min — generous enough
		// to absorb pod-recreate + leader-elect + startup-reset +
		// failover-timeout (the aggressive 30s timeout under the
		// allow-aggressive-timeouts annotation keeps this bounded).
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 4*time.Minute, 5*time.Second).Should(Equal("True"),
			"cluster must recover to Ready=True after triple-kill — pre-#456 the operator wedged indefinitely")

		By("verifying every sentinel sees at least quorum-1 peers (peer-list restored)")
		// quorum=2 by default for replicas=3, so each sentinel must
		// see ≥1 peer. Pre-fix, num-other-sentinels=0 on every
		// sentinel (RESET wiped peer-list, never re-bootstrapped).
		// Post-fix, SENTINEL SET known-sentinel re-registers peers
		// after RESET so each sentinel sees ≥2 peers (quorum=2
		// implies each needs to see the other 2 to confirm +odown).
		Eventually(func() error {
			sentinels, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=sentinel",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			if err != nil {
				return fmt.Errorf("list sentinels: %w", err)
			}
			for _, s := range strings.Fields(strings.TrimSpace(sentinels)) {
				out, err := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", s, "-c", "sentinel", "--",
					"sh", "-c",
					sentinelCLI("", "SENTINEL master mymaster")+" | awk '/^num-other-sentinels$/{getline; print; exit}' | tr -d '\\r' || true",
				))
				if err != nil {
					return fmt.Errorf("SENTINEL master on %s: %w", s, err)
				}
				numPeersStr := strings.TrimSpace(out)
				if numPeersStr == "" {
					return fmt.Errorf("sentinel %s returned empty num-other-sentinels — SENTINEL master query failed", s)
				}
				// quorum-1 = 1 minimum for the default 2-quorum
				// shape. Stricter: ≥ replicas-1 = 2 means every
				// peer is known, which is the post-recovery
				// steady-state.
				if numPeersStr == "0" {
					return fmt.Errorf("sentinel %s reports num-other-sentinels=0 — peer-list lost and never re-bootstrapped (#456 cascade)", s)
				}
			}
			return nil
		}, 4*time.Minute, 5*time.Second).Should(Succeed(),
			"every sentinel must see ≥1 peer after recovery — pre-#456 RESET wiped peer-list and Pub/Sub auto-discovery had no peer to bootstrap from")

		By("verifying exactly one pod is labelled role=primary (Service selector points to a real pod)")
		Eventually(func() int {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return len(strings.Fields(strings.TrimSpace(out)))
		}, 3*time.Minute, 5*time.Second).Should(Equal(1),
			"exactly one pod must carry role=primary after recovery (pre-#456: zero primaries because the SplitBrain suppression gate blocked label writes indefinitely)")

		By("recording whether SentinelStrandedRecovery fired (best-effort — single-sentinel kill rarely strands)")
		// Best-effort, NOT a recovery guarantee. This scenario kills
		// only ONE sentinel, so the two survivors re-gossip the rebuilt
		// sentinel within ~2s via __sentinel__:hello. The operator's
		// per-reconcile stranded-detector fires only when a reachable
		// sentinel has an empty peer-list while quorum is reachable, so
		// with a single kill it frequently observes no stranded window
		// at all and the audit event legitimately never fires — yet the
		// cluster has fully recovered (asserted above: Ready=True, peers
		// restored, exactly one primary). Gating on the event here
		// asserts a best-effort signal as a hard guarantee and flakes.
		// The firing path itself is hard-asserted by the 2-sentinel
		// "sentinel quorum chaos" spec below, where a stranded
		// window is guaranteed. Observe here without gating.
		_, _ = fmt.Fprintf(GinkgoWriter,
			"SentinelStrandedRecovery events for %s after triple-kill recovery: %d (best-effort, not gated)\n",
			crName, countEventsForReason(crName, "SentinelStrandedRecovery"))
	})

	It("master+co-located sentinel killed with operator ALIVE: native Sentinel failover recovers, no survivor-RESET wedge (#678)", func() {
		const crName = "dk-678"

		// Distinct from the triple-kill: the operator stays ALIVE
		// throughout. The signature of the operator's self-recovery is the operator's own
		// per-reconcile pod-replacement watcher firing a topology-wiping
		// SENTINEL RESET * at the surviving sentinels ~5s after the master
		// node died — erasing their replica + peer lists so Sentinel could
		// mark the dead master +sdown but never escalate to +odown / elect.
		// The cluster wedged: every pod stayed role:slave at a dead master
		// and only a manual `replicaof no one` recovered it. With the
		// replacement-RESET path removed, the survivors keep their replica
		// + peer lists, so Sentinel's own +sdown → +odown → election
		// completes unaided.
		//
		// Scope: this asserts the wedge fix for a single node-loss event
		// (the fast, common failure — one master+sentinel pair lost). A
		// separate, slower concern is that survivors never auto-forget a
		// replaced sentinel's old run-id, so dead "ghost" run-ids
		// accumulate across MANY replacements within one operator leader
		// term and can eventually inflate the failover-election quorum
		// denominator. Cleared today only at operator restart
		// (RunInitialReset); safe mid-term ghost reaping is tracked
		// separately. Exercising multi-replacement accumulation belongs with
		// that fix (it needs a cluster run to validate the bound), so it
		// is deliberately out of scope here.
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("confirming steady state: all 3 sentinels see num-other-sentinels=2")
		Eventually(func() error {
			return forEachSentinel(crName, func(s string) error {
				n := sentinelMasterNumOtherSentinels(s)
				if n != "2" {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q, want 2 (not yet fully gossiped)", s, n)
				}
				return nil
			})
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"all sentinels must converge to num-other-sentinels=2 before the kill")

		By("identifying the current master pod and its node")
		masterPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(masterPod).NotTo(BeEmpty(), "precondition: exactly one pod must carry role=primary")
		masterNode := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", masterPod,
			"-o", "jsonpath={.spec.nodeName}",
		)))

		By("selecting a sentinel co-located with the master (single-node minikube: any sentinel qualifies)")
		coSentinel := sentinelOnNode(crName, masterNode)
		Expect(coSentinel).NotTo(BeEmpty(), "could not find a sentinel pod to co-kill with the master")

		By("killing the master pod + its co-located sentinel simultaneously — operator UNTOUCHED")
		// The operator is deliberately NOT killed: its per-reconcile
		// watcher must be live so a regression (re-introducing the
		// replacement-RESET) actually fires against the survivors and
		// wedges the cluster, failing this spec.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", masterPod,
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", coSentinel,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("verifying the cluster recovers to Ready=True with NO manual intervention")
		// Pre-fix: wedged indefinitely (the incident needed a human
		// `replicaof no one`). Post-fix: native Sentinel election plus
		// the operator's orphan-master demotion converge inside the
		// aggressive-timeout window.
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 4*time.Minute, 5*time.Second).Should(Equal("True"),
			"cluster must recover autonomously after the master+sentinel kill — pre-#678 the survivor RESET wedged it with no master anywhere")

		By("verifying exactly one pod is labelled role=primary and it self-reports role:master")
		Eventually(func() int {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return len(strings.Fields(strings.TrimSpace(out)))
		}, 3*time.Minute, 5*time.Second).Should(Equal(1),
			"exactly one pod must carry role=primary after the autonomous failover (pre-#678: zero primaries — every pod stuck role:slave at the dead master)")

		Eventually(func() string {
			primary, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil || strings.TrimSpace(primary) == "" {
				return ""
			}
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", strings.TrimSpace(primary), "-c", "valkey", "--",
				"sh", "-c", "valkey-cli -p 6379 INFO replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\\r'",
			))
			return strings.TrimSpace(out)
		}, 3*time.Minute, 5*time.Second).Should(Equal("master"),
			"the labelled primary must self-report role:master — a real election elected a live master, not just a relabel of a dead one")

		By("verifying every sentinel re-converges to num-other-sentinels=2 (no permanent peer-list wipe)")
		Eventually(func() error {
			return forEachSentinel(crName, func(s string) error {
				n := sentinelMasterNumOtherSentinels(s)
				if n != "2" {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q, want 2 (peer-list not fully restored)", s, n)
				}
				return nil
			})
		}, 4*time.Minute, 5*time.Second).Should(Succeed(),
			"every sentinel must re-converge to full peer awareness after recovery")
	})

	It("repeated sentinel replacement with operator ALIVE: ghost run-ids are reaped, failover still elects (#681)", func() {
		const crName = "dk-681"

		// #678 stopped the operator wiping survivors during a master-down
		// window, but it also stopped clearing the dead peer run-ids a
		// replaced sentinel leaves behind. Those "ghosts" inflate
		// Sentinel's failover-election denominator; enough of them within
		// one operator leader term stalls failover — the #678 wedge class
		// reached by slow accumulation. The operator now reaps ghost-holding
		// survivors (REMOVE+MONITOR, gated on master-alive + no-+odown +
		// quorum, debounced, one-at-a-time, demand-gated). This drives many
		// replacements with the operator left ALIVE and asserts ghosts do
		// not accumulate unbounded AND a subsequent master kill still elects.
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("confirming steady state: all 3 sentinels see num-other-sentinels=2")
		Eventually(func() error {
			return forEachSentinel(crName, func(s string) error {
				if n := sentinelMasterNumOtherSentinels(s); n != "2" {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q, want 2", s, n)
				}
				return nil
			})
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"all sentinels must converge to num-other-sentinels=2 before the churn")

		By("replacing sentinel pods repeatedly (operator stays alive) so ghost run-ids accumulate")
		// Rotate through the three sentinel pods; each force-delete brings
		// the pod back with a fresh UID + PodIP, leaving the old IP as a
		// ghost on every survivor. >= the ~4-replacement wedge threshold.
		for i := 0; i < 6; i++ {
			pod := fmt.Sprintf("%s-sentinel-%d", crName, i%3)
			_ = mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", pod,
				"--force", "--grace-period=0", "--wait=false",
			))
			// Let the StatefulSet recreate it and the survivors re-gossip
			// the new instance before the next replacement.
			Eventually(func() string {
				out, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "pod", pod,
					"-o", "jsonpath={.status.phase}",
				))
				return strings.TrimSpace(out)
			}, 2*time.Minute, 3*time.Second).Should(Equal("Running"),
				"replaced sentinel %s must come back Running before the next replacement", pod)
		}

		By("verifying ghost run-ids do not accumulate unbounded (the reap keeps the known-sentinel count bounded)")
		// The demand-gate intentionally leaves a small number of harmless
		// ghosts (those that do not yet threaten the election majority), so
		// the assertion is a BOUND, not equality. For a 3-sentinel cluster
		// the reap fires once a survivor's known count would block the
		// election (num-other-sentinels = 5: 2 live + 3 ghosts), leaving at
		// most 2 demand-gated ghosts (num-other-sentinels <= 4). Without the
		// reap the count climbs monotonically with every replacement (toward
		// 8 = 2 live + 6 ghosts) and stalls failover; with it, each survivor
		// stays at or below 4.
		Eventually(func() error {
			return forEachSentinel(crName, func(s string) error {
				n := sentinelMasterNumOtherSentinels(s)
				v, err := strconv.Atoi(n)
				if err != nil {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q not numeric: %w", s, n, err)
				}
				if v > 4 {
					return fmt.Errorf("sentinel %s num-other-sentinels=%d — ghosts accumulating past the reap bound (want <= 4)", s, v)
				}
				return nil
			})
		}, 6*time.Minute, 10*time.Second).Should(Succeed(),
			"ghost run-ids must be reaped so the known-sentinel count stays bounded after repeated replacement")

		By("killing the master after the churn: native failover must still elect (ghosts did not block the election)")
		masterPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(masterPod).NotTo(BeEmpty(), "precondition: exactly one pod must carry role=primary")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", masterPod,
			"--force", "--grace-period=0", "--wait=false",
		))
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 4*time.Minute, 5*time.Second).Should(Equal("True"),
			"failover must complete after the master kill — pre-#681 accumulated ghosts inflate the election majority and stall it")
		Eventually(func() int {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return len(strings.Fields(strings.TrimSpace(out)))
		}, 3*time.Minute, 5*time.Second).Should(Equal(1),
			"exactly one pod must carry role=primary after the post-churn failover")
	})

	It("sentinel quorum chaos: kill 2 of 3 sentinels → peers re-bootstrap, single master agreed (#499)", func() {
		const (
			crName   = "dk-sentchaos"
			authPass = "changeme"
		)

		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "create", "secret", "generic", "auth-sentchaos",
			"--from-literal=password="+authPass))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "secret", "auth-sentchaos",
				"--ignore-not-found", "--wait=false"))
		})

		// Auth is enabled deliberately: the SENTINEL master reply then
		// carries auth-pass fields, so the operator's stranded-recovery
		// parser must walk past unrelated keys in the flat key/value
		// array to read num-other-sentinels. The kill below exercises
		// that path with an auth-bearing reply.
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
    masterName: mymaster
    replicas: 3
  auth:
    secretName: auth-sentchaos
    secretKey: password
    sentinelAuthSecretName: auth-sentchaos
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		listSentinels := func() ([]string, error) {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=sentinel",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			if err != nil {
				return nil, fmt.Errorf("list sentinels: %w", err)
			}
			return strings.Fields(strings.TrimSpace(out)), nil
		}

		// numOtherSentinels reads the num-other-sentinels value from a
		// sentinel's SENTINEL master reply. The reply is a flat
		// alternating key/value list; the value is the line after the
		// key. The sentinel port requires auth here, so the probe
		// passes the password the spec created the Secret with.
		numOtherSentinels := func(sentinelPod string) (string, error) {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", sentinelPod, "-c", "sentinel", "--",
				"sh", "-c",
				sentinelCLI(authPass, "SENTINEL master mymaster")+" | awk '/^num-other-sentinels$/{getline; print; exit}' | tr -d '\\r' || true",
			))
			if err != nil {
				return "", fmt.Errorf("SENTINEL master on %s: %w", sentinelPod, err)
			}
			return strings.TrimSpace(out), nil
		}

		By("confirming steady state: all 3 sentinels report num-other-sentinels=2")
		Eventually(func() error {
			pods, err := listSentinels()
			if err != nil {
				return err
			}
			if len(pods) != 3 {
				return fmt.Errorf("expected 3 sentinel pods, saw %d", len(pods))
			}
			for _, s := range pods {
				n, err := numOtherSentinels(s)
				if err != nil {
					return err
				}
				if n != "2" {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q, want 2 (not yet fully gossiped)", s, n)
				}
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"all sentinels must converge to num-other-sentinels=2 before the chaos kill")

		By("capturing the operator pod + restart count (parser-panic regression guard)")
		// The operator is NOT killed in this scenario, so its container
		// must never restart. A panic while parsing the auth-bearing
		// SENTINEL master reply during recovery would increment the
		// restart count — we assert it stays put after recovery.
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(), "-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty())
		operatorRestarts := func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", dualkillOperatorNS(), "get", "pod", operatorPod,
				"-o", "jsonpath={.status.containerStatuses[*].restartCount}",
			))
			return strings.TrimSpace(out)
		}
		baselineRestarts := operatorRestarts()
		strandedBaseline := countEventsForReason(crName, "SentinelStrandedRecovery")

		By("force-deleting 2 of 3 sentinels simultaneously (sentinel-1 + sentinel-2); data plane + operator untouched")
		// Breaks Sentinel-protocol quorum (1 of 3 reachable) without
		// touching any valkey pod, so no failover is triggered and the
		// master stays primary throughout. The rebuilt sentinels boot
		// with an empty peer-list (sentinel.conf is on emptyDir), and
		// the operator's stranded-sentinel recovery must re-point them
		// (REMOVE + MONITOR) so they rejoin the gossip ring.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-sentinel-1",
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-sentinel-2",
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for all 3 sentinels to re-establish full peer awareness (num-other-sentinels >= 2)")
		// Recovery cannot fire while only 1 sentinel is reachable (the
		// operator skips REMOVE+MONITOR below quorum reachability); it
		// resumes once a rebuilt pod is dialable again. >= 2 is the
		// fully re-gossiped steady state, so budget generously for
		// pod-recreate plus the next reconcile passes.
		Eventually(func() error {
			pods, err := listSentinels()
			if err != nil {
				return err
			}
			if len(pods) != 3 {
				return fmt.Errorf("expected 3 sentinel pods, saw %d", len(pods))
			}
			for _, s := range pods {
				n, err := numOtherSentinels(s)
				if err != nil {
					return err
				}
				if n == "" {
					return fmt.Errorf("sentinel %s returned empty num-other-sentinels", s)
				}
				v, err := strconv.Atoi(n)
				if err != nil {
					return fmt.Errorf("sentinel %s num-other-sentinels=%q not numeric: %w", s, n, err)
				}
				if v < 2 {
					return fmt.Errorf("sentinel %s num-other-sentinels=%d, want >= 2 (peers not fully re-bootstrapped)", s, v)
				}
			}
			return nil
		}, 4*time.Minute, 5*time.Second).Should(Succeed(),
			"every sentinel must re-discover both peers after the dual sentinel kill")

		By("verifying all sentinels agree on a single, live master IP")
		Eventually(func() error {
			pods, err := listSentinels()
			if err != nil {
				return err
			}
			currentPodIPs := podIPSet(crName)
			seen := make(map[string]bool)
			for _, s := range pods {
				out, err := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", s, "-c", "sentinel", "--",
					"sh", "-c",
					sentinelCLI(authPass, "SENTINEL get-master-addr-by-name mymaster")+" | head -1 || true",
				))
				if err != nil {
					return fmt.Errorf("get-master-addr-by-name on %s: %w", s, err)
				}
				ip := strings.TrimSpace(out)
				if ip == "" {
					return fmt.Errorf("sentinel %s returned empty master IP", s)
				}
				if !currentPodIPs[ip] {
					return fmt.Errorf("sentinel %s reports master=%s but no live valkey pod has that IP", s, ip)
				}
				seen[ip] = true
			}
			if len(seen) != 1 {
				return fmt.Errorf("sentinels disagree on master IP: %v", seen)
			}
			return nil
		}, 4*time.Minute, 5*time.Second).Should(Succeed(),
			"all sentinels must converge on one live master IP after recovery")

		By("verifying the CR is not Degraded after recovery")
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 3*time.Minute, 5*time.Second).Should(Equal("False"),
			"CR must clear Degraded once sentinels re-reach quorum")

		By("verifying the stranded-sentinel recovery path ran")
		Eventually(func() int {
			return countEventsForReason(crName, "SentinelStrandedRecovery") - strandedBaseline
		}, 4*time.Minute, 5*time.Second).Should(BeNumerically(">=", 1),
			"SentinelStrandedRecovery must fire to re-point the rebuilt sentinels")

		By("verifying the operator never crashed parsing the auth-bearing SENTINEL master replies")
		Expect(operatorRestarts()).To(Equal(baselineRestarts),
			"operator restart count must not increase — an auth-bearing/malformed SENTINEL master reply must not panic the recovery path")
	})

	It("split-brain condition clears after sentinels reach quorum (no boot-time latch)", func() {
		const crName = "dk-quorum"

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
    masterName: mymaster
    replicas: 3
`)
		// Wait for Ready=True. This implicitly asserts the
		// pre-rc.7 latch is fixed — if Degraded=SplitBrain were
		// stuck from bootstrap, Ready would never reach True (the
		// readiness gate's coupling with Degraded would block).
		waitForReady(crName, 5*time.Minute)

		By("verifying Degraded is False (no boot-time SplitBrain latch)")
		// The pre-rc.7 bug was: boot-time sentinel-auth failure →
		// observer reports QuorumOK=false → Degraded=SplitBrain
		// latched indefinitely, even after auth recovered. Post-rc.7,
		// once sentinels reach quorum, the observer publishes
		// QuorumOK=true and the condition clears.
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 3*time.Minute, 5*time.Second).Should(Equal("False"),
			"Degraded must clear once sentinels reach quorum — pre-rc.7 the condition latched at boot and never cleared")

		By("verifying the QuorumLost condition is False or Unknown (sentinels reachable)")
		// QuorumLost being True would mean fewer than `quorum`
		// sentinels are reachable. On a healthy 3-sentinel
		// deployment this must be False.
		Eventually(func() string {
			return getCondition(crName, "QuorumLost")
		}, 2*time.Minute, 5*time.Second).ShouldNot(Equal("True"),
			"QuorumLost must not be True on a healthy 3-sentinel cluster")

		By("verifying role labels are written (no stuck suppression)")
		// The pre-fix bug also blocked Phase 7 from writing role
		// labels (the SplitBrain guard suppresses relabel). If
		// labels are present, Phase 7 ran cleanly — the canonical
		// inverse of the stuck-suppression bug.
		Eventually(func() int {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return len(strings.Fields(strings.TrimSpace(out)))
		}, 3*time.Minute, 5*time.Second).Should(Equal(1),
			"exactly one pod must carry role=primary — pre-rc.7 the SplitBrain latch blocked Phase 7 from ever writing labels")
	})

	It("write-Service replica exclusion: kill cascade never routes a fresh connection to a read-only replica (#502)", func() {
		const (
			crName   = "dk-writesvc"
			authPass = "changeme"
			probePod = crName + "-probe"
		)

		By("creating the auth Secret the cluster and the probe authenticate with")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", crName+"-auth",
			"--from-literal=password="+authPass,
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "secret", crName+"-auth", "--ignore-not-found",
			))
		})

		By("provisioning an auth-enabled 3+3 sentinel cluster (aggressive timeouts bound recovery)")
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
  auth:
    secretName: ` + crName + `-auth
    secretKey: password
    sentinelAuthSecretName: ` + crName + `-auth
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		By("confirming the bootstrap primary is pod-0 (it is never a kill target, so it stays primary)")
		// The primary pod is not killed and ≥quorum sentinels stay alive,
		// so no failover is expected — pod-0 remains the only valid write
		// target throughout. Any other pod in the write Service
		// EndpointSlice is therefore a mislabel that would route writes to
		// a read-only backend.
		Expect(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))).To(Equal("primary"))

		By("selecting a non-primary (replica) valkey pod to kill")
		replicas := strings.Fields(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
			"-o", "jsonpath={.items[*].metadata.name}",
		))))
		Expect(replicas).NotTo(BeEmpty(), "precondition: at least one role=replica pod must exist")
		killReplica := replicas[0]
		replicaIP := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", killReplica,
			"-o", "jsonpath={.status.podIP}",
		)))

		By("locating the operator pod to kill alongside the sentinel + replica")
		operatorPod := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "get", "pods",
			"-l", dualkillOperatorLabel(), "-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(operatorPod).NotTo(BeEmpty(), "operator pod must be present in %s", dualkillOperatorNS())

		By("launching a fresh-connection role probe pod (~100ms cadence)")
		// Each loop iteration opens a NEW connection to the write Service
		// VIP, so it samples kube-proxy's current EndpointSlice routing.
		// `valkey-cli ROLE` prints master/slave on its first line; a
		// `slave` line proves a write connection reached a read-only
		// replica. Connection failures during the chaos window print
		// nothing (an availability blip is tolerated — it is not the
		// correctness hazard under test). The loop outlives the 4-minute
		// convergence budget so it samples the whole window.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "run", probePod,
			"--restart=Never", "--image=valkey/valkey:8.1.7-alpine", "--",
			"sh", "-c",
			fmt.Sprintf("i=0; while [ $i -lt 3000 ]; do valkey-cli -h %s -p 6379 -a %s --no-auth-warning ROLE 2>/dev/null | head -1; sleep 0.1; i=$((i+1)); done",
				crName, authPass),
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", probePod,
				"--ignore-not-found", "--force", "--grace-period=0", "--wait=false",
			))
		})

		By("waiting for the probe pod to be Running before the kill cascade")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", probePod,
				"-o", "jsonpath={.status.phase}",
			))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).Should(Equal("Running"),
			"probe pod must be Running so it samples across the kill window")

		By("starting the background write-Service EndpointSlice watcher (~150ms cadence)")
		// The watcher records every membership sample of the `<cr>` write
		// Service. utils.Run (not mustRun) is used so a transient kubectl
		// error never fails from this goroutine — all assertions run on
		// the main goroutine after the watcher stops.
		var (
			mu        sync.Mutex
			epSamples int
			epViol    []string
		)
		stop := make(chan struct{})
		watcherDone := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(watcherDone)
			ticker := time.NewTicker(150 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					namesOut, _ := utils.Run(exec.Command(
						"kubectl", "-n", e2eNamespace, "get", "endpoints", crName,
						"-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}",
					))
					ipsOut, _ := utils.Run(exec.Command(
						"kubectl", "-n", e2eNamespace, "get", "endpoints", crName,
						"-o", "jsonpath={.subsets[*].addresses[*].ip}",
					))
					mu.Lock()
					epSamples++
					for _, n := range strings.Fields(strings.TrimSpace(namesOut)) {
						if n != crName+"-0" {
							epViol = append(epViol, fmt.Sprintf("sample %d: write Service exposed non-primary pod %q", epSamples, n))
						}
					}
					if replicaIP != "" {
						for _, ip := range strings.Fields(strings.TrimSpace(ipsOut)) {
							if ip == replicaIP {
								epViol = append(epViol, fmt.Sprintf("sample %d: write Service exposed killed-replica IP %q", epSamples, ip))
							}
						}
					}
					mu.Unlock()
				}
			}
		}()

		By("kill cascade: operator pod + one sentinel + one replica, simultaneously (force, no grace)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", killReplica,
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-sentinel-0",
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", dualkillOperatorNS(), "delete", "pod", operatorPod,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for convergence: Ready=True, exactly one role=primary, Degraded=False")
		Eventually(func() error {
			if r := getCondition(crName, "Ready"); r != "True" {
				return fmt.Errorf("Ready=%q", r)
			}
			if d := getCondition(crName, "Degraded"); d != "False" {
				return fmt.Errorf("Degraded=%q", d)
			}
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			if n := len(strings.Fields(strings.TrimSpace(out))); n != 1 {
				return fmt.Errorf("role=primary pod count=%d", n)
			}
			return nil
		}, 4*time.Minute, 3*time.Second).Should(Succeed(),
			"cluster must converge to exactly one primary, Ready=True, Degraded=False after the kill cascade")

		By("draining a final sampling buffer, then stopping the watcher")
		// A short grace lets any lagging relabel / EndpointSlice update
		// surface in the samples before we stop and assert.
		time.Sleep(3 * time.Second)
		close(stop)
		<-watcherDone

		By("asserting the write Service EndpointSlice never exposed a replica during recovery")
		mu.Lock()
		gotSamples := epSamples
		gotViol := append([]string(nil), epViol...)
		mu.Unlock()
		Expect(gotSamples).To(BeNumerically(">", 0), "watcher must have collected at least one EndpointSlice sample")
		Expect(gotViol).To(BeEmpty(),
			"the %q write Service must only ever resolve to the primary (pod-0); a replica in its EndpointSlice routes writes to a read-only backend: %v",
			crName, gotViol)

		By("asserting no fresh connection ever reached a read-only replica (probe never saw role=slave)")
		probeLog := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "logs", probePod,
		))
		Expect(probeLog).To(ContainSubstring("master"),
			"probe never observed a master — the probe was non-functional, so a clean run is not real evidence")
		Expect(probeLog).NotTo(ContainSubstring("slave"),
			"a fresh client connection via the write Service resolved to a replica (role=slave) — writes there receive -READONLY (data-correctness hazard)")

		By("final steady-state: the write Service EndpointSlice contains exactly the primary")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "endpoints", crName,
				"-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}",
			))
			return strings.TrimSpace(out)
		}, 1*time.Minute, 3*time.Second).Should(Equal(crName+"-0"),
			"after recovery the write Service must resolve to exactly the primary pod-0")
	})

	It("graceful primary delete: failover survives the Terminating-but-serving drain without routing a write to a replica (#498)", func() {
		const (
			crName   = "gd-primary"
			authPass = "changeme"
			probePod = crName + "-probe"
			seedKey  = "gd-seed-key"
			seedVal  = "survives-failover"
		)

		By("creating the auth Secret the cluster and probe authenticate with")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", crName+"-auth",
			"--from-literal=password="+authPass,
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "secret", crName+"-auth", "--ignore-not-found",
			))
		})

		By("provisioning an auth-enabled 3+3 sentinel cluster (aggressive timeouts bound failover)")
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
  auth:
    secretName: ` + crName + `-auth
    secretKey: password
    sentinelAuthSecretName: ` + crName + `-auth
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		oldPrimary := crName + "-0"
		By("confirming the bootstrap primary is pod-0 (the graceful-delete target)")
		Expect(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", oldPrimary,
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))).To(Equal("primary"), "precondition: pod-0 must be the bootstrap primary")

		By("seeding a key on the primary and confirming it replicated (replication-healthy precondition)")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", oldPrimary, "-c", "valkey", "--",
			"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning SET %s %s", authPass, seedKey, seedVal),
		))
		Eventually(func() string {
			replicas := strings.Fields(strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
				"-o", "jsonpath={.items[*].metadata.name}",
			))))
			if len(replicas) == 0 {
				return ""
			}
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", replicas[0], "-c", "valkey", "--",
				"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning GET %s", authPass, seedKey),
			))
			return strings.TrimSpace(out)
		}, 1*time.Minute, 3*time.Second).Should(Equal(seedVal),
			"seeded key must replicate to a replica before the fault")

		By("launching a timestamped fresh-connection role probe (~100ms cadence)")
		// Each iteration opens a NEW connection to the `<cr>` write Service
		// VIP and prints `<unix> <role>`. A `slave` line proves a fresh
		// write connection reached a read-only replica. Timestamps let us
		// separate the brief drain transient from the post-settle window
		// the invariant is asserted over. Connection blips during the
		// failover gap print only the timestamp (tolerated availability dip,
		// not the correctness hazard under test). The probe is
		// wall-clock-bounded (900s) so it outlives the convergence
		// deadline + settle even under a slow-but-legal failover — a
		// fixed iteration count can exhaust before the settle window
		// and leave the post-settle guard with zero samples.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "run", probePod,
			"--restart=Never", "--image=valkey/valkey:8.1.7-alpine", "--",
			"sh", "-c",
			fmt.Sprintf("end=$(( $(date +%%s) + 900 )); while [ $(date +%%s) -lt $end ]; do echo \"$(date +%%s) $(valkey-cli -h %s -p 6379 -a %s --no-auth-warning ROLE 2>/dev/null | head -1)\"; sleep 0.1; done",
				crName, authPass),
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", probePod,
				"--ignore-not-found", "--force", "--grace-period=0", "--wait=false",
			))
		})

		By("waiting for the probe pod to be Running before the graceful delete")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", probePod,
				"-o", "jsonpath={.status.phase}",
			))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).Should(Equal("Running"),
			"probe pod must be Running so it samples across the failover window")

		By("gracefully deleting the primary pod (default SIGTERM + grace period + preStop; NO --force)")
		// The graceful path exercises the preStop safety hook + endpoint
		// removal before process stop — the Terminating-but-serving window
		// the force-delete specs skip entirely.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", oldPrimary, "--wait=false",
		))

		By("waiting for failover: a different pod becomes primary, Ready=True, Degraded=False")
		var newPrimary string
		Eventually(func() error {
			if r := getCondition(crName, "Ready"); r != "True" {
				return fmt.Errorf("Ready=%q", r)
			}
			if d := getCondition(crName, "Degraded"); d != "False" {
				return fmt.Errorf("Degraded=%q", d)
			}
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			primaries := strings.Fields(strings.TrimSpace(out))
			if len(primaries) != 1 {
				return fmt.Errorf("role=primary count=%d", len(primaries))
			}
			if primaries[0] == oldPrimary {
				return fmt.Errorf("primary still %s — sentinel has not yet elected a new primary", oldPrimary)
			}
			newPrimary = primaries[0]
			return nil
		}, 4*time.Minute, 3*time.Second).Should(Succeed(),
			"a different pod must gain role=primary with Ready=True, Degraded=False after the graceful delete")

		By("confirming the new primary self-reports INFO role:master")
		Expect(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", newPrimary, "-c", "valkey", "--",
			"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning INFO replication | awk -F: '/^role:/{print $2}' | tr -d '\\r'", authPass),
		)))).To(Equal("master"),
			"the elected pod must self-report role:master")

		By("anchoring the settle boundary on the probe's own clock at new-primary confirmation")
		// The boundary must come from the probe pod's `date +%s` samples,
		// not the test machine's time.Now(): cross-host skew shifts the
		// window arbitrarily, and a single snapshot read taken the instant
		// the boundary passes sees zero qualifying samples even from a
		// healthy probe. Anchor on the newest sample at confirmation time,
		// then re-read until samples land past anchor+15 — the probe keeps
		// sampling on its 900s wall-clock budget.
		var anchorTs int64
		for _, line := range strings.Split(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "logs", probePod,
		)), "\n") {
			f := strings.Fields(strings.TrimSpace(line))
			if len(f) == 0 {
				continue
			}
			if ts, err := strconv.ParseInt(f[0], 10, 64); err == nil && ts > anchorTs {
				anchorTs = ts
			}
		}
		Expect(anchorTs).NotTo(BeZero(),
			"probe produced no parseable samples before the settle window — the probe was non-functional")
		settleBoundary := anchorTs + 15

		By("asserting no fresh connection landed on a replica after the settle window (#498 invariant)")
		// Guard against a vacuous pass: zero post-settle samples would let
		// BeEmpty() below hold without the invariant ever being exercised.
		// Require real evidence first, re-reading until it accumulates.
		var probeLog string
		var postSettleSlave []string
		postSettleSamples := 0 // valid role lines (master or slave) inside the post-settle window
		Eventually(func() int {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "logs", probePod,
			))
			if err != nil {
				return 0
			}
			probeLog = out
			postSettleSlave = postSettleSlave[:0]
			postSettleSamples = 0
			for _, line := range strings.Split(probeLog, "\n") {
				f := strings.Fields(strings.TrimSpace(line))
				if len(f) != 2 {
					continue // timestamp-only (connection blip) or blank — tolerated
				}
				ts, err := strconv.ParseInt(f[0], 10, 64)
				if err != nil {
					continue
				}
				if ts < settleBoundary {
					continue // pre-settle sample — drain transient, not the invariant window
				}
				postSettleSamples++
				if f[1] == "slave" {
					postSettleSlave = append(postSettleSlave, line)
				}
			}
			return postSettleSamples
		}, 2*time.Minute, 3*time.Second).Should(BeNumerically(">", 0),
			"probe emitted no role samples after the settle window (probe-clock ts >= anchor+15) — a clean (no-slave) result is not real evidence the #498 invariant held")
		Expect(probeLog).To(ContainSubstring("master"),
			"probe never observed a master — the probe was non-functional, so a clean run is not real evidence")
		Expect(postSettleSlave).To(BeEmpty(),
			"after the settle window, no fresh write-Service connection may resolve to a read-only replica (role=slave): %v", postSettleSlave)

		By("asserting the seeded key survived the failover on the new primary")
		Expect(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", newPrimary, "-c", "valkey", "--",
			"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning GET %s", authPass, seedKey),
		)))).To(Equal(seedVal),
			"the seeded key must persist on the new primary after failover")

		By("final steady-state: the write Service EndpointSlice resolves to exactly the new primary")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "endpoints", crName,
				"-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}",
			))
			// kubectl (k8s ≥1.33) emits a "v1 Endpoints is deprecated"
			// Warning that utils.Run folds into the output on its own line;
			// drop it and keep only the jsonpath target name(s).
			var targets []string
			for _, ln := range strings.Split(out, "\n") {
				if ln = strings.TrimSpace(ln); ln != "" && !strings.HasPrefix(ln, "Warning:") {
					targets = append(targets, ln)
				}
			}
			return strings.TrimSpace(strings.Join(targets, " "))
		}, 1*time.Minute, 3*time.Second).Should(Equal(newPrimary),
			"after recovery the write Service must resolve to exactly the new primary")
	})

	It("sequential failover storm: back-to-back primary kills each elect a fresh primary with clean per-failover state reset (#500)", func() {
		const (
			crName   = "fs-storm"
			authPass = "changeme"
			probePod = crName + "-probe"
			kills    = 4
		)
		// Generous/configurable budgets: this spec is about state
		// correctness across consecutive elections, not failover latency,
		// so a slow single-node node must not produce timing-only failures.
		const (
			perKillBudget  = 3 * time.Minute
			interKillGrace = 10 * time.Second
			settleWindow   = 15 * time.Second
		)

		By("creating the auth Secret the cluster and the probe authenticate with")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", crName+"-auth",
			"--from-literal=password="+authPass,
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "secret", crName+"-auth", "--ignore-not-found",
			))
		})

		By("provisioning an auth-enabled 3+3 sentinel cluster (aggressive timeouts bound recovery)")
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
  auth:
    secretName: ` + crName + `-auth
    secretKey: password
    sentinelAuthSecretName: ` + crName + `-auth
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		By("launching a timestamped fresh-connection role probe (~100ms cadence, time-bounded to outlast the storm)")
		// Timestamped so the post-final-election no-replica-write window is
		// assertable. Time-bounded on wall clock (not a fixed iteration
		// count): the storm's duration varies with per-kill recovery, and a
		// count-bounded loop could exhaust before the final settle window —
		// making the invariant pass vacuously (the postSettleSamples guard
		// below is the backstop). 900s comfortably outlasts the worst-case
		// storm (kills*perKillBudget + grace + settle).
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "run", probePod,
			"--restart=Never", "--image=valkey/valkey:8.1.7-alpine", "--",
			"sh", "-c",
			fmt.Sprintf("end=$(( $(date +%%s) + 900 )); while [ $(date +%%s) -lt $end ]; do echo \"$(date +%%s) $(valkey-cli -h %s -p 6379 -a %s --no-auth-warning ROLE 2>/dev/null | head -1)\"; sleep 0.1; done",
				crName, authPass),
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", probePod,
				"--ignore-not-found", "--force", "--grace-period=0", "--wait=false",
			))
		})

		By("waiting for the probe pod to be Running before the first kill")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", probePod,
				"-o", "jsonpath={.status.phase}",
			))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).Should(Equal("Running"),
			"probe pod must be Running so it samples across the storm")

		primaryPods := func() []string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return strings.Fields(strings.TrimSpace(out))
		}

		By("recording the bootstrap primary")
		var prevPrimary string
		Eventually(func() string {
			if p := primaryPods(); len(p) == 1 {
				prevPrimary = p[0]
			}
			return prevPrimary
		}, 1*time.Minute, 3*time.Second).ShouldNot(BeEmpty(),
			"exactly one bootstrap primary must be labelled before the storm")

		for iter := 1; iter <= kills; iter++ {
			victim := prevPrimary
			By(fmt.Sprintf("kill %d/%d: force-deleting the current primary %q", iter, kills, victim))
			mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", victim,
				"--force", "--grace-period=0", "--wait=false",
			))

			By(fmt.Sprintf("kill %d/%d: waiting for a DIFFERENT pod to gain role=primary, Ready, Degraded=False", iter, kills))
			var elected string
			Eventually(func() error {
				if r := getCondition(crName, "Ready"); r != "True" {
					return fmt.Errorf("Ready=%q", r)
				}
				if d := getCondition(crName, "Degraded"); d != "False" {
					return fmt.Errorf("Degraded=%q", d)
				}
				p := primaryPods()
				if len(p) != 1 {
					return fmt.Errorf("role=primary count=%d", len(p))
				}
				if p[0] == victim {
					return fmt.Errorf("primary still the killed pod %q — no fresh election yet", victim)
				}
				elected = p[0]
				return nil
			}, perKillBudget, 3*time.Second).Should(Succeed(),
				"kill %d/%d: a different pod must gain role=primary with Ready=True, Degraded=False", iter, kills)

			By(fmt.Sprintf("kill %d/%d: new primary %q self-reports INFO role:master", iter, kills, elected))
			// A stale per-failover artifact (leaked client-kill target,
			// observer cooldown, rollout bookkeeping) would surface here as
			// a promoted pod that never actually becomes master.
			Expect(strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", elected, "-c", "valkey", "--",
				"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning INFO replication | awk -F: '/^role:/{print $2}' | tr -d '\\r'", authPass),
			)))).To(Equal("master"),
				"the freshly-elected pod must self-report role:master on iteration %d", iter)

			prevPrimary = elected

			if iter < kills {
				By("inter-kill grace so per-failover state settles before the next kill")
				time.Sleep(interKillGrace)
			}
		}

		By("anchoring the settle boundary on the probe's own clock at the final election")
		// Same rationale as the graceful-delete spec above: the boundary
		// must come from the probe pod's `date +%s` samples, not the test
		// machine's clock — cross-host skew shifts the window arbitrarily,
		// and a single snapshot read taken the instant the boundary passes
		// sees zero qualifying samples even from a healthy probe.
		var anchorTs int64
		for _, line := range strings.Split(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "logs", probePod,
		)), "\n") {
			f := strings.Fields(strings.TrimSpace(line))
			if len(f) == 0 {
				continue
			}
			if ts, err := strconv.ParseInt(f[0], 10, 64); err == nil && ts > anchorTs {
				anchorTs = ts
			}
		}
		Expect(anchorTs).NotTo(BeZero(),
			"probe produced no parseable samples during the storm — the probe was non-functional")
		settleBoundary := anchorTs + int64(settleWindow/time.Second)

		By("asserting steady state after the storm: exactly one primary, Ready=True, Degraded=False")
		Eventually(func() error {
			if r := getCondition(crName, "Ready"); r != "True" {
				return fmt.Errorf("Ready=%q", r)
			}
			if d := getCondition(crName, "Degraded"); d != "False" {
				return fmt.Errorf("Degraded=%q", d)
			}
			if p := primaryPods(); len(p) != 1 {
				return fmt.Errorf("role=primary count=%d", len(p))
			}
			return nil
		}, 2*time.Minute, 3*time.Second).Should(Succeed(),
			"after the storm the cluster must hold exactly one primary, Ready=True, Degraded=False")

		By("asserting no fresh connection landed on a replica after the final settle window")
		// Re-read the probe log until samples land past the boundary —
		// the probe keeps sampling on its 900s wall-clock budget, so a
		// single snapshot at the boundary instant is not real evidence
		// either way.
		var probeLog string
		var postSettleSlave []string
		postSettleSamples := 0 // valid role lines (master or slave) past the final settle point
		Eventually(func() int {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "logs", probePod,
			))
			if err != nil {
				return 0
			}
			probeLog = out
			postSettleSlave = postSettleSlave[:0]
			postSettleSamples = 0
			for _, line := range strings.Split(probeLog, "\n") {
				f := strings.Fields(strings.TrimSpace(line))
				if len(f) != 2 {
					continue // timestamp-only (connection blip) or blank — tolerated
				}
				ts, err := strconv.ParseInt(f[0], 10, 64)
				if err != nil {
					continue
				}
				if ts < settleBoundary {
					continue // pre-settle sample — chaos transient, not the invariant window
				}
				postSettleSamples++
				if f[1] == "slave" {
					postSettleSlave = append(postSettleSlave, line)
				}
			}
			return postSettleSamples
		}, 2*time.Minute, 3*time.Second).Should(BeNumerically(">", 0),
			"probe emitted no role samples after the final settle window (probe-clock ts >= anchor+settle) — a clean (no-slave) result is not real evidence")
		Expect(probeLog).To(ContainSubstring("master"),
			"probe never observed a master — the probe was non-functional, so a clean run is not real evidence")
		Expect(postSettleSlave).To(BeEmpty(),
			"after the final election no fresh write-Service connection may resolve to a read-only replica (role=slave): %v", postSettleSlave)
	})
})

// countEventsForReason returns the number of Events with the given
// reason scoped to e2eNamespace + the CR (involvedObject.name match).
// Used by the startup-reset gating test to measure the delta of
// new InitialSentinelReset events fired during the dual-kill window.
func countEventsForReason(crName, reason string) int {
	GinkgoHelper()
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "events",
		"--field-selector", fmt.Sprintf("reason=%s,involvedObject.name=%s", reason, crName),
		"-o", "jsonpath={.items[*].reason}",
	))
	if err != nil {
		return 0
	}
	return len(strings.Fields(strings.TrimSpace(out)))
}

// podIPSet returns the set of current valkey pod IPs for the CR.
// Used to assert that sentinel-reported master IPs match LIVE pods
// (the defunct-IP wedge from pre-rc.8).
func podIPSet(crName string) map[string]bool {
	GinkgoHelper()
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/component=valkey",
		"-o", "jsonpath={range .items[*]}{.status.podIP}{\"\\n\"}{end}",
	))
	if err != nil {
		return nil
	}
	out = strings.TrimSpace(out)
	set := make(map[string]bool)
	for _, line := range strings.Split(out, "\n") {
		ip := strings.TrimSpace(line)
		if ip != "" {
			set[ip] = true
		}
	}
	return set
}
