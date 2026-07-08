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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// End-to-end coverage for the PVC resize sub-state-machine.
//
// The flow is intentionally fan-shaped — three independent paths
// share the same admission + substate plumbing but exercise distinct
// branches of the detector + dispatcher:
//
//   - shrink-reject       — admission-time CEL rule rejects the
//                           kubectl patch; the operator never sees the
//                           shrunk spec.
//   - expansion-not-supported
//                         — the StorageClass advertises
//                           allowVolumeExpansion=false; the detector
//                           short-circuits with PVCExpansionNotSupported
//                           and writes phase=Aborted.
//   - happy path          — the detector enters the substate machine;
//                           a real expansion-capable CSI updates each
//                           PVC's status.capacity once the operator
//                           stamps PVCsPatched; the dispatcher walks the
//                           remaining substates to Verified and clears.
//                           Opt-in (real CSI only) — see the spec.
//
// Two test-managed StorageClasses back the second and third scenarios.
// The provisioner is env-overridable via E2E_PVC_RESIZE_PROVISIONER:
// kind runs default to rancher.io/local-path; shared clusters override
// to a provisioner that's actually installed (e.g. openebs.io/local).
// Only allowVolumeExpansion varies between the two classes — the
// operator's verdict for the abort path turns on the StorageClass
// field, not on actual driver capability. The happy path, by contrast,
// needs a CSI driver that actually resizes the volume and writes
// pvc.status.capacity back; it is opt-in via E2E_PVC_RESIZE_SC_EXPAND
// (see the happy-path spec) and skipped when only a hostpath
// provisioner is available.

const (
	pvcResizeSCExpandDefault    = "valkey-test-expand"
	pvcResizeSCNoExpand         = "valkey-test-no-expand"
	pvcResizeProvisionerDefault = "rancher.io/local-path"
	pvcResizeReconcileGap       = 10 * time.Second
)

// pvcResizeSCExpand is the StorageClass used by the happy-path spec.
// When E2E_PVC_RESIZE_SC_EXPAND is unset, the suite creates a
// synthetic SC named "valkey-test-expand" with the
// E2E_PVC_RESIZE_PROVISIONER provisioner (kind default
// rancher.io/local-path). When set (shared-cluster runs), the suite
// reuses an existing cluster-owned SC by name and skips SC creation
// — needed for CSI provisioners with non-trivial parameters (clusterID,
// secrets, pool — e.g. rook-ceph) where a bare synthetic SC manifest
// would fail to provision.
var pvcResizeSCExpand = envOrDefault("E2E_PVC_RESIZE_SC_EXPAND", pvcResizeSCExpandDefault)

// pvcResizeExpandSCIsRealCSI reports whether the happy-path spec has a
// StorageClass whose CSI driver actually performs volume expansion and
// writes pvc.status.capacity back to the requested size. The operator's
// substate machine holds at PVCsPatched until that round-trip completes
// (the dispatcher requeues while status.capacity < the desired size), so
// without a real external resize controller the happy path can never
// reach Verified.
//
// kind/minikube's stock hostpath provisioner has no external resize
// controller — status.capacity stays at the original size forever — so
// the spec is opt-in: it runs only when E2E_PVC_RESIZE_SC_EXPAND points
// at an existing cluster-owned, expansion-capable CSI StorageClass (set
// by tools/e2e-shared.sh, e.g. ceph-block). When unset, the suite falls
// back to a synthetic hostpath SC that cannot expand and the spec skips.
func pvcResizeExpandSCIsRealCSI() bool {
	return os.Getenv("E2E_PVC_RESIZE_SC_EXPAND") != ""
}

var _ = Describe("Valkey PVC resize", Ordered, func() {
	BeforeAll(func() {
		By("ensuring the e2e namespace exists")
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))

		// Idempotent pre-cleanup — a prior run that crashed past
		// BeforeAll may have left the no-expand SC behind. The
		// expand SC is only test-owned when E2E_PVC_RESIZE_SC_EXPAND
		// is unset; an env-supplied (cluster-owned) name is never
		// deleted.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "sc", pvcResizeSCNoExpand, "--ignore-not-found", "--wait=true"))

		By("creating the no-expand test StorageClass")
		prov := envOrDefault("E2E_PVC_RESIZE_PROVISIONER", pvcResizeProvisionerDefault)
		applyClusterScoped(storageClassYAML(pvcResizeSCNoExpand, prov, false))

		if os.Getenv("E2E_PVC_RESIZE_SC_EXPAND") == "" {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "sc", pvcResizeSCExpand, "--ignore-not-found", "--wait=true"))
			By("creating the expand test StorageClass")
			applyClusterScoped(storageClassYAML(pvcResizeSCExpand, prov, true))
		} else {
			By(fmt.Sprintf("reusing existing expand StorageClass %q (E2E_PVC_RESIZE_SC_EXPAND)", pvcResizeSCExpand))
		}
	})

	AfterAll(func() {
		By("deleting the test no-expand StorageClass")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "sc", pvcResizeSCNoExpand, "--ignore-not-found", "--wait=false"))
		if os.Getenv("E2E_PVC_RESIZE_SC_EXPAND") == "" {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "sc", pvcResizeSCExpand, "--ignore-not-found", "--wait=false"))
		}
		// Namespace shared with the other Describe blocks — leave it.
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Wipe every Valkey CR, Pod, and PVC between specs. Local-path
		// PVs are node-local hostpath; orphaning them across tests would
		// confuse the next spec's bind step.
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force"))
		// The happy-path resize orphan-deletes the StatefulSet (StsOrphaned
		// substate), leaving pods with no ownerRef that outlive the CR/STS
		// deletion above. Such a pod keeps the kubernetes.io/pvc-protection
		// finalizer pinned on its PVC, so the PVC delete below blocks until
		// the pod is gone — which hangs the whole suite when a resize is
		// interrupted mid-flight. Delete pods explicitly first; orphaned
		// pods have no controller to recreate them.
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "pod", "--all",
			"--ignore-not-found", "--grace-period=0", "--force"))
		_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "pvc", "--all",
			"--ignore-not-found", "--grace-period=0", "--force"))
	})

	// Path 1 — admission-time shrink rejection.
	//
	// The CR-level CEL rule in api/v1beta1/valkey_types.go rejects
	// any update where spec.valkey.persistence.size decreases. The
	// operator never observes the shrunk spec; the reconciler is not
	// involved.
	It("rejects a shrink request at admission time (CEL)", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: pvc-shrink
spec:
  mode: standalone
  valkey:
    persistence:
      size: 4Gi
`)
		waitForReady("pvc-shrink", 5*time.Minute)

		By("attempting to shrink persistence.size from 4Gi to 2Gi")
		cmd := exec.Command("kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", "pvc-shrink",
			"--type=merge", "-p", `{"spec":{"valkey":{"persistence":{"size":"2Gi"}}}}`)
		out, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred(), "expected CEL to reject the shrink: %s", string(out))
		Expect(string(out)).To(ContainSubstring("cannot decrease"),
			"rejection message must reference the storage-monotonicity CEL rule; got: %s", string(out))

		By("confirming the operator never stamped pvcResize substate")
		Consistently(func() string {
			return strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", "pvc-shrink",
				"-o", "jsonpath={.status.rollout.pvcResize.phase}",
			)))
		}, pvcResizeReconcileGap, 2*time.Second).Should(BeEmpty(),
			"a rejected shrink must never reach the reconciler — substate must stay unset")

		By("confirming no PVCResizeInitiated event ever fires")
		// The substate poll above only catches a stamped-then-cleared
		// window. The event check is independent: PVCResizeInitiated
		// is emitted on Validated entry; its absence proves the
		// detector never observed a shrunk spec.
		Expect(findEventReason("pvc-shrink", "PVCResizeInitiated")).To(BeEmpty(),
			"PVCResizeInitiated must never fire when admission rejects the shrink")
	})

	// Path 2 — StorageClass without allowVolumeExpansion.
	//
	// The detector reads the SC referenced by the first PVC and short-
	// circuits with PVCExpansionNotSupported when allowVolumeExpansion
	// is false. The substate machine never enters orphan-delete; the
	// substate moves directly to Aborted. The Aborted state arms the
	// per-attempt backoff (1m on Attempt=1) before the next retry —
	// long enough that this test asserts the terminal state without
	// racing the next reconcile.
	It("aborts when the StorageClass does not allow volume expansion", func() {
		applyCR(fmt.Sprintf(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: pvc-no-expand
spec:
  mode: standalone
  valkey:
    persistence:
      storageClass: %s
      size: 4Gi
`, pvcResizeSCNoExpand))
		waitForReady("pvc-no-expand", 5*time.Minute)

		By("patching persistence.size from 4Gi to 8Gi")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", "pvc-no-expand",
			"--type=merge", "-p", `{"spec":{"valkey":{"persistence":{"size":"8Gi"}}}}`))

		By("waiting for phase=Aborted")
		Eventually(func() string {
			return getPVCResizePhase("pvc-no-expand")
		}, 2*time.Minute, 5*time.Second).Should(Equal("Aborted"),
			"detector must short-circuit to Aborted when allowVolumeExpansion=false")

		By("verifying PVCExpansionNotSupported event fired")
		Eventually(func() string {
			return findEventReason("pvc-no-expand", "PVCExpansionNotSupported")
		}, 1*time.Minute, 5*time.Second).Should(Equal("PVCExpansionNotSupported"),
			"PVCExpansionNotSupported event must surface the rejection")
	})

	// Path 3 — happy path, against a real expansion-capable CSI.
	//
	// The detector enters the substate machine; the dispatcher walks
	// Validated → StsOrphaned → PVCsPatched. PVCsPatched polls the
	// PVCs' status.capacity until they reflect the new size — the
	// CSI driver's job. With a real allowVolumeExpansion=true CSI
	// StorageClass the driver resizes the underlying volume and writes
	// status.capacity back, and the dispatcher walks StsRecreated →
	// Verified → terminal complete (substate cleared). Events fire on
	// both ends (Initiated, Complete); the gauge transition 0→1→0 is
	// the same code path as the substate writes — asserting status +
	// events implies the gauge transition without scraping /metrics.
	//
	// The kind/minikube hostpath provisioner has no external resize
	// controller, so status.capacity never reaches the target and the
	// operator (correctly) holds at PVCsPatched forever. The spec is
	// therefore opt-in: it runs only when E2E_PVC_RESIZE_SC_EXPAND
	// points at a cluster-owned expansion-capable CSI StorageClass
	// (tools/e2e-shared.sh sets it, e.g. ceph-block) and skips otherwise.
	It("drives the substate machine to Verified on the happy path", func() {
		if !pvcResizeExpandSCIsRealCSI() {
			Skip("happy-path PVC resize needs a real expansion-capable CSI that " +
				"writes pvc.status.capacity back to the requested size; the " +
				"kind/minikube hostpath provisioner cannot expand, so the operator " +
				"correctly holds at PVCsPatched. Set E2E_PVC_RESIZE_SC_EXPAND to a " +
				"cluster-owned expansion CSI StorageClass (tools/e2e-shared.sh does " +
				"this) to enable it.")
		}
		applyCR(fmt.Sprintf(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: pvc-expand
spec:
  mode: standalone
  valkey:
    persistence:
      storageClass: %s
      size: 4Gi
`, pvcResizeSCExpand))
		waitForReady("pvc-expand", 5*time.Minute)

		By("patching persistence.size from 4Gi to 8Gi")
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", "pvc-expand",
			"--type=merge", "-p", `{"spec":{"valkey":{"persistence":{"size":"8Gi"}}}}`))

		By("verifying PVCResizeInitiated event fires")
		Eventually(func() string {
			return findEventReason("pvc-expand", "PVCResizeInitiated")
		}, 1*time.Minute, 5*time.Second).Should(Equal("PVCResizeInitiated"),
			"PVCResizeInitiated must surface the detector's entry into the substate machine")

		By("waiting for the dispatcher to patch each PVC's spec (PVCsPatched phase)")
		Eventually(func() string {
			return getPVCResizePhase("pvc-expand")
		}, 2*time.Minute, 5*time.Second).Should(Equal("PVCsPatched"),
			"dispatcher must orphan-delete the STS and patch PVCs to the new size")

		By("listing the PVCs the dispatcher patched to the new size")
		pvcs := listPVCNames("pvc-expand")
		Expect(pvcs).NotTo(BeEmpty(), "at least one PVC must be present after orphan-delete")
		for _, pvcName := range pvcs {
			By("verifying the PVC carries the operator's CR + component labels")
			// The PVCs are created by the STS controller from the
			// volumeClaimTemplate that buildValkeyDataPVC stamps with
			// ownedLabels(componentValkey). If those labels were lost
			// in translation, the operator's label-keyed selectors
			// (reconcileDeletion, reconcilePVCResize) would miss the
			// PVC silently — assert the label round-trip here to pin
			// the contract.
			labels := mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pvc", pvcName,
				"-o", "jsonpath={.metadata.labels.velkir\\.ioxie\\.dev/cr},{.metadata.labels.velkir\\.ioxie\\.dev/component}",
			))
			Expect(strings.TrimSpace(labels)).To(Equal("pvc-expand,valkey"),
				"PVC %q must carry velkir.ioxie.dev/cr=pvc-expand and velkir.ioxie.dev/component=valkey labels", pvcName)
		}

		By("waiting for the CSI driver to complete the expansion (substate clears via pvc.status.capacity = 8Gi)")
		// The operator's dispatcher polls each PVC's status.capacity
		// until the CSI driver reports the expansion is done. With a
		// real allowVolumeExpansion=true StorageClass (e.g. rook-ceph
		// on the shared cluster) the CSI controller picks up the
		// dispatcher's spec.resources.requests.storage patch and
		// resizes the underlying volume, then writes status.capacity
		// back to the new size. The substate machine advances
		// PVCsPatched → StsRecreated → Verified → terminal-complete
		// (status.rollout.pvcResize cleared) once that round-trip
		// finishes. 8m budget covers Ceph RBD resize latency under
		// shared-cluster load.
		Eventually(func() string {
			return getPVCResizePhase("pvc-expand")
		}, 8*time.Minute, 5*time.Second).Should(BeEmpty(),
			"dispatcher must clear pvcResize substate after Verified (CSI completed the expansion)")

		By("verifying PVCResizeComplete event fires")
		Eventually(func() string {
			return findEventReason("pvc-expand", "PVCResizeComplete")
		}, 1*time.Minute, 5*time.Second).Should(Equal("PVCResizeComplete"),
			"PVCResizeComplete must surface the terminal-success transition")

		By("verifying the CR remains Ready after the resize completes")
		waitForReady("pvc-expand", 2*time.Minute)
	})
})

// storageClassYAML renders the StorageClass manifest for the resize
// test fixtures. Both classes use kind's default provisioner and
// differ only on allowVolumeExpansion — the field the operator's
// detector keys on. Binding mode is left at the default (Immediate):
// WaitForFirstConsumer requires the provisioner to read the scheduled
// node, which minikube's stock storage-provisioner SA cannot do (no
// nodes RBAC), and the resize abort/expand assertions don't depend on
// late binding.
func storageClassYAML(name, provisioner string, allowExpand bool) string {
	return fmt.Sprintf(`
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: %s
allowVolumeExpansion: %t
reclaimPolicy: Delete
`, name, provisioner, allowExpand)
}

// applyClusterScoped pipes a YAML manifest to `kubectl apply -f -`
// without a namespace flag (StorageClasses, CRDs, etc.). Failures
// fail the spec immediately, same shape as applyCR.
func applyClusterScoped(yaml string) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kubectl apply (cluster-scoped): %s", string(out))
}

// getPVCResizePhase returns status.rollout.pvcResize.phase for the
// named CR. Empty string when the substate is absent (steady state,
// or terminal-complete cleared the field).
func getPVCResizePhase(name string) string {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", name,
		"-o", "jsonpath={.status.rollout.pvcResize.phase}",
	))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// findEventReason returns the matching reason string when an event
// with that reason exists on the named Valkey CR, otherwise empty.
// Matched via jsonpath against the events API so a missing event
// returns "" rather than erroring (caller uses Eventually).
func findEventReason(name, reason string) string {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "events",
		"--field-selector", "involvedObject.name="+name,
		"-o", fmt.Sprintf("jsonpath={.items[?(@.reason=='%s')].reason}", reason),
	))
	if err != nil {
		return ""
	}
	// jsonpath emits the reason once per matching event, space-
	// separated. The exact count doesn't matter; take the first if
	// any are present, otherwise return empty so Eventually keeps
	// polling.
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// listPVCNames returns the names of every PVC labeled for the given
// Valkey CR. The substate machine targets these PVCs in PVCsPatched;
// the happy-path spec enumerates them to verify the operator's labels
// round-tripped onto the STS-created PVCs.
func listPVCNames(crName string) []string {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pvc",
		"-l", "velkir.ioxie.dev/cr="+crName,
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	if err != nil {
		return nil
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return nil
	}
	return strings.Fields(trimmed)
}
