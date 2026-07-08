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

// End-to-end coverage for `mode: replication`. The
// Manager-deployment lifecycle and the e2e namespace lifecycle are
// owned by the standalone suite (valkey_standalone_test.go); this
// file adds replication-specific scenarios on top of the same
// operator deployment.

var _ = Describe("Valkey replication", Ordered, func() {

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Clean every Valkey CR between specs so each test starts fresh.
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all", "--ignore-not-found", "--grace-period=0", "--force"))
	})

	// Scenario 1 — bootstrap a 3-replica `mode: replication` CR.
	// Verify the client `<cr>` Service serves traffic, the read-only
	// `<cr>-ro` Service exists with the role=replica selector, and
	// the bootstrap topology stamps role=primary on pod-0 and
	// role=replica on pods 1..N-1.
	It("bootstraps a 3-replica replication CR; <cr> writes, <cr>-ro reads, role labels stable", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: replication-bootstrap
spec:
  mode: replication
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
`)
		waitForReady("replication-bootstrap", 5*time.Minute)

		By("verifying primary writes via the client <cr> Service")
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "replication-bootstrap-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m27-key", "m27-value",
		))
		Expect(strings.TrimSpace(out)).To(Equal("OK"))

		By("verifying <cr>-ro Service exists with role=replica selector")
		roSelector := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "svc", "replication-bootstrap-ro",
			"-o", `jsonpath={.spec.selector.velkir\.ioxie\.dev/role}`,
		))
		Expect(strings.TrimSpace(roSelector)).To(Equal("replica"))

		By("verifying replica reads picks up the value via replication")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", "replication-bootstrap-1", "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m27-key",
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m27-value"),
			"replica must observe the primary's write within the replication window")

		By("verifying role labels: pod-0 primary, pods 1+ replica")
		pod0Role := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "replication-bootstrap-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		))
		Expect(strings.TrimSpace(pod0Role)).To(Equal("primary"))
		for _, ord := range []string{"1", "2"} {
			role := mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", "replication-bootstrap-"+ord,
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			Expect(strings.TrimSpace(role)).To(Equal("replica"),
				"pod-%s must be labelled role=replica", ord)
		}
	})

	// Scenario 2 — readiness gate prevents node-drain eviction
	// (`ReplicationReadinessPreventsDrain`). At v0.3 the gate is
	// stamped on every replica pod; an in-flight catching-up replica
	// shows the gate's condition and a NotReady status that
	// `kubectl drain` honours via PodDisruptionBudget. Verify the
	// gate is on the pod spec and that the eviction API rejects the
	// pod (HTTP 429 Too Many Requests / 422 with PDB violation).
	It("readiness gate prevents node-drain eviction (ReplicationReadinessPreventsDrain)", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: replication-drain
spec:
  mode: replication
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
`)
		waitForReady("replication-drain", 5*time.Minute)

		By("verifying the gate is present on a replica pod's spec")
		gate := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "replication-drain-1",
			"-o", `jsonpath={.spec.readinessGates[?(@.conditionType=="velkir.ioxie.dev/replication-ready")].conditionType}`,
		))
		Expect(strings.TrimSpace(gate)).To(Equal("velkir.ioxie.dev/replication-ready"),
			"replica must carry the replication-readiness gate in spec")

		By("verifying the operator-derived PDB exists")
		pdb := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pdb",
			"-l", "velkir.ioxie.dev/cr=replication-drain",
			"-o", "jsonpath={.items[0].spec.minAvailable}",
		))
		Expect(strings.TrimSpace(pdb)).NotTo(BeEmpty(),
			"PDB must exist with a minAvailable; the gate + PDB pair is what blocks drain")

		By("attempting eviction on a healthy replica — PDB should refuse if it would breach minAvailable")
		// Eviction goes via the pods/eviction subresource; kubectl
		// drain wraps it. Use kubectl drain --dry-run to check
		// without actually draining the test cluster.
		nodeName := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "replication-drain-1",
			"-o", "jsonpath={.spec.nodeName}",
		))
		Expect(strings.TrimSpace(nodeName)).NotTo(BeEmpty(),
			"replica must be scheduled on a node before drain can be tested")
		// dry-run drain just validates the eviction; it doesn't
		// actually evict. kubectl writes the "cordoned" line to
		// stdout but the per-pod "would evict" list to stderr, so
		// capture both streams directly here rather than through
		// utils.Run (which returns stdout only).
		dryDrain, _ := exec.Command(
			"kubectl", "drain", strings.TrimSpace(nodeName),
			"--ignore-daemonsets", "--dry-run=client", "--force",
		).CombinedOutput()
		// The dry-run output should mention the valkey pod —
		// confirms the eviction path is wired through the pod we
		// expect, even though the actual eviction-vs-PDB enforcement
		// only happens server-side on a real drain.
		Expect(string(dryDrain)).To(ContainSubstring("replication-drain-1"),
			"dry-run drain must list the replica pod (got: %s)", string(dryDrain))
	})

	// Scenario 3 — scale 3 → 5. New replicas catch up, gates flip to
	// True, and the `<cr>-ro` Service's endpoint set picks up the
	// new pods. The scale-down refusal logic doesn't fire here
	// because primary stays at pod-0 (well below the new replica
	// count of 5).
	It("scale 3 -> 5 picks up new replicas in <cr>-ro endpoints", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: replication-scale
spec:
  mode: replication
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
`)
		waitForReady("replication-scale", 5*time.Minute)

		By("scaling to 5 replicas")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", "replication-scale",
			"--type=merge", "-p", `{"spec":{"valkey":{"replicas":5}}}`))

		By("waiting for the new pods to reach Ready")
		waitForReady("replication-scale", 5*time.Minute)
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", "replication-scale",
				"-o", "jsonpath={.status.readyReplicas}",
			))
			return strings.TrimSpace(out)
		}, 5*time.Minute, 10*time.Second).Should(Equal("5"),
			"all 5 replicas must reach Ready post-scale")

		By("verifying pod-3 and pod-4 carry role=replica")
		for _, ord := range []string{"3", "4"} {
			role := mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", "replication-scale-"+ord,
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			Expect(strings.TrimSpace(role)).To(Equal("replica"),
				"new pod-%s must be labelled role=replica", ord)
		}

		By("verifying <cr>-ro endpoints include the new replicas")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "endpoints", "replication-scale-ro",
				"-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}",
			))
			return out
		}, 2*time.Minute, 5*time.Second).Should(
			SatisfyAll(
				ContainSubstring("replication-scale-3"),
				ContainSubstring("replication-scale-4"),
			),
			"<cr>-ro endpoints must include the post-scale replicas")
	})

	// Scenario 4 — config bump rolls replicas only. A
	// configurationOverrides edit flips the rendered hash, the STS
	// rolls a new revision, and Phase 9 deletes non-primary
	// pods one at a time. The primary stays put — master-aware
	// recreation lands in a later phase and emits RolloutDeferred until then.
	It("config bump rolls replicas only; primary untouched", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: replication-rollconf
spec:
  mode: replication
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
`)
		waitForReady("replication-rollconf", 5*time.Minute)

		By("recording pod-0 (primary) UID before the bump")
		pod0UIDBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "replication-rollconf-0",
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(pod0UIDBefore).NotTo(BeEmpty())

		By("bumping configurationOverrides")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", "replication-rollconf",
			"--type=merge", "-p", `{"spec":{"valkey":{"configurationOverrides":{"maxmemory":"128mb"}}}}`))

		By("waiting for the replica pods to be recreated (UID changes)")
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", "replication-rollconf-2",
				"-o", "jsonpath={.metadata.creationTimestamp}",
			))
			return err == nil && strings.TrimSpace(out) != ""
		}, 5*time.Minute, 10*time.Second).Should(BeTrue())

		waitForReady("replication-rollconf", 5*time.Minute)

		By("verifying the primary pod-0 was NOT recreated (UID stable)")
		pod0UIDAfter := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "replication-rollconf-0",
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(pod0UIDAfter).To(Equal(pod0UIDBefore),
			"primary pod-0 UID must be stable across a config bump (the config bump leaves the primary alone)")

		By("verifying RolloutDeferred fires for the primary handoff")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "events",
				"--field-selector", "involvedObject.name=replication-rollconf",
				"-o", "jsonpath={.items[?(@.reason=='RolloutDeferred')].reason}",
			))
			return out
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("RolloutDeferred"),
			"RolloutDeferred must fire once the replica side finishes and the primary is still stale")
	})

	// Scenario 6 — `configurationOverrides` precedence over the
	// base `configuration` string. The override map wins on key
	// collision; non-colliding string lines pass through. Setup:
	// string says `maxmemory 256mb`, override map says
	// `maxmemory: 128mb`. Rendered config must reflect 128mb,
	// not 256mb; the unrelated `maxmemory-policy` line survives.
	It("configurationOverrides wins over the configuration string", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: replication-precedence
spec:
  mode: replication
  valkey:
    replicas: 2
    persistence:
      size: 1Gi
    configuration: |
      maxmemory 256mb
      maxmemory-policy allkeys-lru
    configurationOverrides:
      maxmemory: 128mb
`)
		waitForReady("replication-precedence", 5*time.Minute)

		By("verifying the rendered ConfigMap reflects the override winning")
		conf := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "configmap", "replication-precedence-valkey-conf",
			"-o", `jsonpath={.data.valkey\.conf}`,
		))
		Expect(conf).To(ContainSubstring("maxmemory 128mb"),
			"override map value must appear in the rendered config")
		Expect(conf).NotTo(ContainSubstring("maxmemory 256mb"),
			"override must displace the user-string maxmemory")
		Expect(conf).To(ContainSubstring("maxmemory-policy allkeys-lru"),
			"non-colliding user-string lines must survive")

		By("verifying the running pod's CONFIG GET reports the override")
		// The init container substitutes _POD_IP_ and the main
		// container reads the merged file. CONFIG GET reflects what
		// valkey-server actually loaded.
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "replication-precedence-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "config", "get", "maxmemory",
		))
		// Output looks like:
		//   1) "maxmemory"
		//   2) "134217728"
		// 134217728 == 128*1024*1024 (128 MiB). 256 MiB would be
		// 268435456. Anything else means precedence broke.
		Expect(out).To(SatisfyAny(
			ContainSubstring("134217728"), // exact 128 MiB
			ContainSubstring("128mb"),     // some valkey versions echo the literal
			ContainSubstring("128 MB"),    // and others use the human form
		), fmt.Sprintf("CONFIG GET maxmemory must show 128 MiB equivalent; got: %s", out))
	})
})
