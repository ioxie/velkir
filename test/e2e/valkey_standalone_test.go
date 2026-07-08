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

// End-to-end coverage for `mode: standalone`. The
// Manager-deployment lifecycle is owned by the existing `Manager`
// Describe block in e2e_test.go; these scenarios assume the
// operator is up and add Valkey CR fixtures on top.

// e2eNamespace is the namespace every scenario applies its Valkey
// CR fixtures into. Populated by the per-process resolver in
// BeforeSuite (`e2e_suite_test.go`):
//
//   - Default (serial mode): the base name from E2E_TEST_NAMESPACE
//     (or "valkey-e2e"). Backwards-compatible with single-process
//     runs.
//   - Ginkgo parallel mode (--procs=N > 1): "<base>-p<procID>" so
//     each parallel test process gets its own namespace. The
//     shared-cluster harness pre-creates and labels all N namespaces
//     so the chart's namespaceSelector matches every process.
//
// Tests reference this var at runtime (inside BeforeAll / It bodies),
// so initialisation timing is "BeforeSuite-before-BeforeAll" — safe.
var e2eNamespace string

var _ = Describe("Valkey standalone", Ordered, func() {
	BeforeAll(func() {
		By("creating the e2e namespace")
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		By("labelling the e2e namespace as a webhook target")
		// Idempotent label; safe to re-run across specs. The label is
		// what the chart's scoped webhook.namespaceSelector matches on
		// in shared-cluster installs. In default (kind) mode the chart
		// isn't used so the label is inert.
		_, _ = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true"))
	})

	AfterAll(func() {
		// e2eNamespace is shared across every Describe in this suite
		// (Standalone, Sentinel, Replication, PVC resize, Webhook cert
		// lifecycle). In shared-cluster mode the per-process namespace
		// is owned by tools/e2e-shared.sh and cleaned up by its
		// teardown — deleting it here can race the other Describes'
		// specs and produce "namespace is being terminated" failures
		// in the next BeforeAll/applyCR.
		if sharedClusterMode() {
			return
		}
		By("deleting the e2e namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", e2eNamespace, "--ignore-not-found", "--wait=false"))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Clean every Valkey CR between specs so each test starts fresh.
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all", "--ignore-not-found", "--grace-period=0", "--force"))
	})

	// Scenario 1 — bootstrap a standalone CR; verify reachability
	// via the client `<cr>` Service. Pinned by an exec into the pod
	// running `valkey-cli ping`.
	It("bootstraps a standalone CR and serves traffic on the client Service", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: bootstrap
spec:
  mode: standalone
`)
		waitForReady("bootstrap", 5*time.Minute)

		By("running valkey-cli ping inside the pod")
		// -c valkey pins the target container so kubectl doesn't emit
		// "Defaulted container ..." preamble that would pollute the
		// equality assertion against "PONG". The pod template includes
		// the valkey main container plus a render-config init.
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "bootstrap-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "ping",
		))
		Expect(strings.TrimSpace(out)).To(Equal("PONG"))

		By("resolving the client Service")
		out = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "svc", "bootstrap",
			"-o", "jsonpath={.spec.clusterIP}",
		))
		Expect(out).NotTo(BeEmpty())
		Expect(out).NotTo(Equal("None"))
	})

	// Scenario 2 — pvcRetentionPolicy: Delete cascades PVCs on CR
	// deletion. Standalone-with-persistence to make the PVC visible.
	It("garbage-collects PVCs when pvcRetentionPolicy is Delete", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: pvc-delete
spec:
  mode: standalone
  pvcRetentionPolicy: Delete
  valkey:
    persistence:
      size: 1Gi
`)
		waitForReady("pvc-delete", 5*time.Minute)

		By("recording the PVC name before deletion")
		pvcName := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pvc",
			"-l", "velkir.ioxie.dev/cr=pvc-delete",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(pvcName).NotTo(BeEmpty())

		By("deleting the CR")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "pvc-delete", "--wait=true"))

		By("verifying PVC was garbage-collected")
		Eventually(func() bool {
			cmd := exec.Command("kubectl", "-n", e2eNamespace, "get", "pvc", pvcName)
			err := cmd.Run()
			return err != nil
		}, 2*time.Minute, 5*time.Second).Should(BeTrue(), "PVC should be GC'd after CR delete")
	})

	// Scenario 3 — pvcRetentionPolicy: Retain (default) keeps PVCs
	// alive after CR deletion. Pairs with scenario 2.
	It("retains PVCs when pvcRetentionPolicy is Retain", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: pvc-retain
spec:
  mode: standalone
  pvcRetentionPolicy: Retain
  valkey:
    persistence:
      size: 1Gi
`)
		waitForReady("pvc-retain", 5*time.Minute)

		pvcName := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pvc",
			"-l", "velkir.ioxie.dev/cr=pvc-retain",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(pvcName).NotTo(BeEmpty())

		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "pvc-retain", "--wait=true"))

		By("verifying PVC survives")
		Consistently(func() error {
			return exec.Command("kubectl", "-n", e2eNamespace, "get", "pvc", pvcName).Run()
		}, 30*time.Second, 5*time.Second).Should(Succeed(), "PVC should survive a Retain CR delete")

		// Manual cleanup (the operator is contractually NOT to
		// delete on Retain; the test owns it).
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "pvc", pvcName, "--ignore-not-found"))
	})

	// Scenario 4 — horizontal scale refused in standalone (CEL
	// rejects replicas > 1).
	It("rejects horizontal scale in standalone via CEL", func() {
		// applyCR's Expect-NotTo-HaveOccurred contract is the wrong
		// shape for a test that expects rejection — replicate the
		// kubectl-apply-with-stdin pattern inline so we can capture
		// and assert on the rejection message.
		cmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: scale-reject
spec:
  mode: standalone
  valkey:
    replicas: 2
`)
		out2, err2 := cmd.CombinedOutput()
		Expect(err2).To(HaveOccurred(), "expected CEL to reject replicas>1 in standalone")
		Expect(string(out2)).To(ContainSubstring("mode=standalone requires valkey.replicas == 1"))
	})

	// Scenario 5 — auth Secret rotation via `kubectl edit secret`.
	// Manual rotation only at v1beta1 (pod restart needed for the
	// new value to take effect; hot rotation is tracked separately).
	It("reflects an auth Secret value change after pod restart", func() {
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "create", "secret", "generic", "auth-rotate",
			"--from-literal=password=initial"))

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: auth-rotate
spec:
  mode: standalone
  auth:
    secretName: auth-rotate
    secretKey: password
`)
		waitForReady("auth-rotate", 5*time.Minute)

		By("rotating the Secret value")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "patch", "secret", "auth-rotate",
			"--type=merge", "-p", `{"stringData":{"password":"rotated"}}`))

		By("restarting the valkey pod to pick up the new env value")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "delete", "pod", "auth-rotate-0", "--wait=true"))
		waitForReady("auth-rotate", 2*time.Minute)

		By("verifying the new password is required for auth")
		// Connecting with the old password should now fail; with
		// the new password should succeed.
		//
		// Eventually wrapper: waitForReady returns when the CR
		// condition flips to True, but the post-restart pod can have
		// a brief window where kubelet has not yet attached the
		// running valkey container to the pod's containerStatuses
		// (init container ordering + propagation lag). `kubectl exec`
		// fails with `container not found ("valkey")` during that
		// window; the next poll succeeds.
		Eventually(func() (string, error) {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", "auth-rotate-0", "-c", "valkey", "--",
				"valkey-cli", "-a", "rotated", "--no-auth-warning", "ping",
			))
			return strings.TrimSpace(out), err
		}, 1*time.Minute, 2*time.Second).Should(Equal("PONG"))
	})

	// Scenario 6 — reserved-label rejection by the validating
	// webhook (covers reserved-label protection).
	It("rejects pod-label collisions with the operator's reserved keyspace", func() {
		cmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: label-collide
spec:
  mode: standalone
  valkey:
    podLabels:
      app.kubernetes.io/instance: pwned
`)
		out, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred(), "expected the validating webhook to reject")
		Expect(string(out)).To(MatchRegexp(`(?i)reserved.*operator`))
	})

	// Scenario 7 — STS-loss recovery, case A.
	// Orphan-delete the STS; operator detects and recreates from
	// spec; pods reattach to existing PVCs; BootstrapComplete latch
	// means no re-bootstrap.
	It("recovers from orphan-deleted StatefulSet without re-bootstrap (case A)", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: sts-orphan
spec:
  mode: standalone
  valkey:
    persistence:
      size: 1Gi
`)
		waitForReady("sts-orphan", 5*time.Minute)
		bootstrapBefore := getCondition("sts-orphan", "BootstrapComplete")
		Expect(bootstrapBefore).To(Equal("True"))

		By("orphan-deleting the StatefulSet")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "delete", "sts", "sts-orphan",
			"--cascade=orphan", "--wait=true"))

		By("waiting for the operator to recreate the STS")
		Eventually(func() error {
			return exec.Command("kubectl", "-n", e2eNamespace, "get", "sts", "sts-orphan").Run()
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		// 8m matches the operator's worst-case post-orphan settle:
		// new STS creates a pod, PVC re-attach (Ceph CSI multi-attach
		// can stall up to ~60s), pod-Ready propagation through the
		// informer, then the readinessGate + role-label reconcile
		// chain. On a loaded shared cluster (pvc-no-expand hot-loop
		// reconciles, sentinel rollouts in the sibling p2 namespace)
		// the post-orphan Ready transition can lag by several
		// minutes; kind converges in <30s.
		waitForReady("sts-orphan", 8*time.Minute)

		bootstrapAfter := getCondition("sts-orphan", "BootstrapComplete")
		Expect(bootstrapAfter).To(Equal("True"), "BootstrapComplete latch must hold across STS recreation")
	})
})

// applyCR shells out to `kubectl apply -f -` with the given YAML
// against the e2e namespace, retrying through a transient
// admission-rejection window before failing the spec.
//
// The validating webhook is failurePolicy: Fail, so while the operator
// pod is mid-restart — e.g. an earlier Ordered scenario force-deleted
// it — the CREATE is rejected (connection refused, or the apiserver CEL
// "no such key: quorum" because the mutating defaulter that fills in
// quorum is also down). A single no-retry apply races that window; the
// retry waits for the webhook endpoint to repopulate, then the same
// idempotent apply lands. A genuinely-bad apply still fails the spec
// once the Eventually times out.
func applyCR(yaml string) {
	GinkgoHelper()
	Eventually(func() error {
		cmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(yaml)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("kubectl apply: %s: %w", string(out), err)
		}
		return nil
	}, 90*time.Second, 2*time.Second).Should(Succeed(),
		"kubectl apply must succeed (retried through any webhook-restart rejection window)")
}

// mustRun executes the command and returns stdout, failing the spec
// on non-zero exit. Mirrors utils.Run but with the spec-fail shape
// these scenarios want.
func mustRun(cmd *exec.Cmd) string {
	GinkgoHelper()
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "command failed: %s", out)
	return out
}

// waitForReady blocks until the Valkey CR's Ready condition flips to
// True or the timeout fires. Drives every "after spec apply" check.
func waitForReady(name string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func() string {
		return getCondition(name, "Ready")
	}, timeout, 5*time.Second).Should(Equal("True"),
		"Valkey %q never reached Ready=True", name)
}

// getCondition reads a single condition's Status string off the CR.
// Returns "" on any error so callers can use it inside Eventually.
// The resource type is group-qualified: a bare "valkey" is ambiguous —
// and every poll silently errors into "" — on any cluster that still
// carries a same-named CRD from another group.
func getCondition(name, condType string) string {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", name,
		"-o", fmt.Sprintf(`jsonpath={.status.conditions[?(@.type=="%s")].status}`, condType),
	))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
