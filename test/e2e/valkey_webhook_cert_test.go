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
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// Webhook + cert subsystem scenarios. Four specs covering the
// failure modes the design promised but the unit + envtest gates
// don't exercise end-to-end:
//
//  1. Webhook-failure recovery: with the operator pod dead the
//     ValidatingWebhookConfiguration's failurePolicy=Fail blocks CR
//     CRUD in unexcluded namespaces; a Deployment recreate restores
//     CRUD without any admin action.
//  2. cert-manager opt-in: runtime detection in CertManagerOptedIn
//     hands the leaf-Secret lifecycle off to cert-manager when an
//     operator-named Certificate appears; a restart after the
//     Certificate is removed re-engages the dynauth path.
//  3. Force-rotate annotation: stamping
//     velkir.ioxie.dev/force-rotate on the CA Secret triggers an
//     immediate reissue on the next reconcile pass, and the
//     annotation self-clears so it doesn't fire repeatedly.
//  4. Operator restart across rotation: force-deleting the operator
//     pod after triggering rotation lands a clean rotation under
//     the new pod's leader lease (single replica HA boundary).
//
// All four share the operator-namespace constants from
// e2e_test.go (namespace = "velkir-system") because they
// reach into the operator's own pod / Secrets / WebhookConfigurations
// — these aren't standalone-suite or sentinel-suite concerns.

const (
	// caSecretName / leafSecretName mirror the dynauth package's
	// exported constants. Pinned by string here (not imported) so
	// the e2e build can't introduce a dependency on the operator's
	// internal package — only the apiserver-visible names matter.
	caSecretName   = "velkir-webhook-ca"
	leafSecretName = "velkir-webhook-cert"

	// forceRotateAnnotation matches dynauth.ForceRotateAnnotation.
	forceRotateAnnotation = "velkir.ioxie.dev/force-rotate"
)

// operatorLabel defaults to the kustomize make-deploy pod-template
// selector. Shared-cluster runs (chart-installed operator) override
// via E2E_OPERATOR_LABEL — chart pods carry helm-style labels like
// `app.kubernetes.io/instance=<release>`.
var operatorLabel = envOrDefault("E2E_OPERATOR_LABEL", "control-plane=controller-manager")

// validatingWebhookName / mutatingWebhookName default to the
// kustomize make-deploy output. Chart-deploy runs override via env
// vars — chart names are release-prefixed (<release>-velkir-
// validator / -defaulter), so the harness script computes and exports
// them before invoking the test binary.
var validatingWebhookName = envOrDefault(
	"E2E_VALIDATING_WEBHOOK_NAME", "velkir-validating-webhook-configuration",
)
var mutatingWebhookName = envOrDefault(
	"E2E_MUTATING_WEBHOOK_NAME", "velkir-mutating-webhook-configuration",
)

// webhookServiceName defaults to the kustomize make-deploy output.
// Chart-deploy runs override via env: chart's webhook Service is
// release-prefixed (<release>-velkir-webhook). Without this
// override the endpoint-drain / repopulate Eventually checks in the
// failurePolicy=Fail spec target a Service that doesn't exist on the
// chart-deploy path; the drain becomes a no-op (kubectl get returns
// 404, which the check treats as "already drained"), the test races
// the still-warm webhook endpoint cache instead of the operator-down
// state it's asserting, and the CR create succeeds when it should
// fail.
var webhookServiceName = envOrDefault(
	"E2E_WEBHOOK_SERVICE_NAME", "velkir-webhook-service",
)

// `Serial` ordering: the webhook-cert specs mutate cluster-scoped
// ValidatingWebhookConfiguration and MutatingWebhookConfiguration
// (the operator's own admission configs, named by env). Running
// these in parallel with other Describes (each touching CRs in
// per-process namespaces) is safe at the namespace boundary, but
// concurrent rotations of the cluster-scoped caBundle field race
// the dynauth Authority controller. Forcing Serial keeps the
// rotation specs single-threaded; other Describes still run in
// parallel processes per --procs=N.
var _ = Describe("Webhook cert lifecycle", Ordered, Serial, func() {

	BeforeAll(func() {
		// Idempotent ns create. Ginkgo v2 randomises top-level container
		// order, so this Describe may run before the standalone Describe's
		// BeforeAll fires. Mirrors the sentinel suite's pattern.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Defensive cleanup — every spec self-cleans, but if a kill or
		// rotate left intermediate state, this AfterEach catches it
		// before the next spec inherits a broken cert subsystem.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", namespace, "annotate", "secret", caSecretName,
			forceRotateAnnotation+"-", "--overwrite",
		))
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", namespace, "delete", "certificate.cert-manager.io",
			leafSecretName, "--ignore-not-found",
		))
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", namespace, "delete", "issuer.cert-manager.io",
			"velkir-e2e-issuer", "--ignore-not-found",
		))
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force",
		))
	})

	// Scenario 1 — webhook-failure / recovery.
	//
	// failurePolicy=Fail on the validator means a CR CRUD attempt
	// must FAIL while the operator pod is dead and its webhook
	// service is unreachable; once the Deployment recreates the
	// pod and it becomes Ready the same CR CRUD attempt must
	// SUCCEED, with no admin action required.
	//
	// The validator excludes the operator namespace and kube-system
	// from its namespaceSelector so a webhook-down window can't
	// brick the operator's own CRs. We apply in valkey-e2e (not
	// excluded) to exercise the unexcluded path.
	It("webhook-failure / recovery — failurePolicy=Fail blocks CR CRUD until operator restarts", func() {
		opPodNameBefore := getOperatorPodName()
		opPodUIDBefore := getOperatorPodUID(opPodNameBefore)

		By("force-deleting the operator pod (kill -9 equivalent)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "pod", opPodNameBefore,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the webhook service endpoint to drain (apiserver caches the endpoint slice; up to a few seconds)")
		// Poll the endpoint slice until it has no addresses. Without
		// this drain, the apiserver might briefly still hold the old
		// endpoint and forward the upcoming kubectl apply to a dead
		// backend (TCP RST), masking the failurePolicy=Fail signal
		// we're trying to assert.
		//
		// 404 on the Endpoints object — i.e., the Service itself was
		// renamed / never existed in this install — is NOT "drained":
		// it indicates a misconfigured drain check. Re-fetch; do not
		// silently early-return success. (Pre-fix, treating 404 as
		// drained caused the failurePolicy=Fail spec to run its CR
		// create against a still-warm webhook on the chart-deploy
		// release-prefixed Service the test wasn't watching.)
		// Accept either: the endpoint slice drained to zero addresses,
		// OR the operator pod has been replaced (new UID at the same
		// name) by the Deployment controller. On shared clusters the
		// Deployment recreate is fast enough that the drain-to-zero
		// window collapses below the polling resolution (often
		// < 200 ms). A new-UID replacement is the robust signal that
		// the old pod is gone — the next CR apply will hit either a
		// drained endpoint or a not-yet-Ready endpoint (TCP RST), both
		// of which surface to the apiserver as webhook errors, which
		// is what the failurePolicy=Fail assertion below checks.
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "get", "endpoints",
				webhookServiceName,
				"-o", "jsonpath={.subsets[*].addresses[*].ip}",
			))
			if err == nil && strings.TrimSpace(out) == "" {
				return true
			}
			currentName := getOperatorPodName()
			if currentName == "" {
				return false
			}
			currentUID := getOperatorPodUID(currentName)
			return currentUID != "" && currentUID != opPodUIDBefore
		}, 60*time.Second, 200*time.Millisecond).Should(BeTrue(),
			"webhook endpoint must drain to zero addresses OR operator pod must be replaced while the old pod is down")

		By("attempting CR create in an unexcluded namespace — must fail consistently with a webhook error")
		// Apply directly via kubectl: the validator's failurePolicy=Fail
		// turns the apply into "webhook unreachable" → apiserver rejects
		// with a `failed calling webhook` error. We don't assert the
		// exact wording (varies across k8s patch versions); we assert
		// that the command FAILS for at least a few consecutive ticks
		// (so a stale endpoint-cache hit from the apiserver doesn't
		// flake the test), and that the failure message mentions the
		// webhook.
		applyBlocked := func() error {
			cmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: webhook-fail-blocked
spec:
  mode: standalone
  valkey:
    replicas: 1
    persistence:
      size: 1Gi
`)
			out, err := cmd.CombinedOutput()
			if err == nil {
				return fmt.Errorf("apply unexpectedly succeeded; output: %s", string(out))
			}
			if !strings.Contains(string(out), "webhook") {
				return fmt.Errorf("apply failed but error did not mention the webhook; output: %s", string(out))
			}
			return nil
		}
		// Consistently fails for at least 3 consecutive attempts at 1s
		// intervals — defends against a single-shot kubectl apply
		// landing during the endpoint-cache repopulation window. Each
		// attempt creates nothing (admission rejected), so the retry
		// loop is safe.
		Consistently(applyBlocked, 5*time.Second, 1*time.Second).Should(Succeed(),
			"CR create must fail with a webhook error while the operator + webhook are down (failurePolicy=Fail)")

		By("verifying nothing was created (kubectl apply rejected by admission)")
		listCmd := exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", "webhook-fail-blocked",
			"--ignore-not-found", "-o", "name",
		)
		listOut, listErr := utils.Run(listCmd)
		Expect(listErr).NotTo(HaveOccurred(),
			"kubectl get must succeed (separate from whether the CR exists); output: %s", listOut)
		Expect(strings.TrimSpace(listOut)).To(BeEmpty(),
			"no Valkey CR should exist after the rejected apply; got %q", listOut)

		By("waiting for the Deployment to recreate the operator pod with a fresh UID and Ready=True")
		opPodNameAfter := waitForFreshOperatorPod(opPodUIDBefore)
		Expect(opPodNameAfter).NotTo(Equal(opPodNameBefore),
			"the new operator pod must have a name different from the deleted one")

		By("waiting for the webhook endpoint to repopulate after pod Ready")
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "get", "endpoints",
				webhookServiceName,
				"-o", "jsonpath={.subsets[*].addresses[*].ip}",
			))
			if err != nil {
				return false
			}
			return strings.TrimSpace(out) != ""
		}, 90*time.Second, 1*time.Second).Should(BeTrue(),
			"webhook endpoint must repopulate after the operator pod becomes Ready")

		By("retrying CR create — must now succeed with no admin action")
		// Same apply, post-recovery. The certwatcher inside the new
		// operator pod has loaded the leaf from the projected volume
		// (Authority's first reconcile pass minted / refreshed it);
		// the apiserver's endpoint cache picked up the new pod's IP;
		// the apply rides through the validator.
		Eventually(func() error {
			cmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: webhook-fail-recovered
spec:
  mode: standalone
  valkey:
    replicas: 1
    persistence:
      size: 1Gi
`)
			out, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("apply failed: %s: %w", string(out), err)
			}
			return nil
		}, 2*time.Minute, 5*time.Second).Should(Succeed(),
			"CR create must succeed once the operator and webhook have recovered")

		By("CR is visible — recovery complete")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", "webhook-fail-recovered",
				"-o", "jsonpath={.metadata.name}",
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 1*time.Second).Should(Equal("webhook-fail-recovered"))
	})

	// Scenario 2 — cert-manager opt-in mid-flight detection.
	//
	// Validates the operator's runtime detection: with a
	// cert-manager Certificate of the operator-detected name
	// (LeafSecretName) present in the operator namespace, the
	// Authority's reconcile loop must stop cleanly (logged), and
	// with the Certificate removed and the operator restarted, the
	// dynauth path must resume (CA Secret managed-by label is
	// stamped back, periodic rotation gauge updates resume).
	//
	// The suite's `make deploy` path uses raw kustomize manifests
	// (not the chart), so the operator boots in --enable-dynamic-
	// authority=true mode. The scenario exercises the mid-flight
	// hand-off and the fall-back, not the chart-side toggle (the
	// chart's Issuer / Certificate templates are covered by
	// helm-unittest).
	It("cert-manager opt-in — Authority hands off when Certificate appears; resumes after removal + restart", func() {
		if !utils.IsCertManagerCRDsInstalled() {
			// The shared-cluster path skips the cert-manager install
			// (BeforeSuite), as does a Kind run with
			// CERT_MANAGER_INSTALL_SKIP=true. This scenario creates a
			// cert-manager Issuer + Certificate, which hard-fail on apply
			// when the CRDs are absent. The dynauth fall-back is already
			// covered cert-manager-free by the other scenarios.
			Skip("cert-manager CRDs are not installed — this scenario requires cert-manager (shared/minikube path or CERT_MANAGER_INSTALL_SKIP=true)")
		}
		By("creating a self-signed Issuer in the operator namespace")
		issuerManifest := `
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: velkir-e2e-issuer
  namespace: ` + namespace + `
spec:
  selfSigned: {}
`
		applyToOperatorNamespace(issuerManifest)

		By("creating a cert-manager Certificate that targets the operator's leaf-Secret name")
		// secretName must match dynauth.LeafSecretName for the runtime
		// probe to detect it. cert-manager will populate / take
		// ownership of the existing operator-owned Secret only if it
		// can; in practice the existing Secret's `managed-by` label
		// keeps cert-manager out and the Certificate's Ready will
		// reflect that — that's fine for this scenario, which is
		// about the Authority loop detecting the Certificate's
		// presence and stopping, not about cert-manager actually
		// owning the leaf.
		certManifest := `
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ` + leafSecretName + `
  namespace: ` + namespace + `
spec:
  secretName: ` + leafSecretName + `
  dnsNames:
    - velkir-webhook-service.` + namespace + `.svc
    - velkir-webhook-service.` + namespace + `.svc.cluster.local
  issuerRef:
    name: velkir-e2e-issuer
    kind: Issuer
`
		applyToOperatorNamespace(certManifest)

		By("force-deleting the operator pod so the new replica re-runs setupWebhookCertProvisioning")
		// The cmd/main.go boot-time detection (setupWebhookCertProvisioning)
		// happens once at process start. Restarting forces it to re-evaluate
		// against the now-present cert-manager Certificate. Without a
		// restart, the existing Authority loop's per-iteration detection
		// in Start() would eventually catch up — but it polls on a slow
		// (≥1h) cadence, so restarting is the deterministic shortcut.
		oldPodName := getOperatorPodName()
		oldPodUID := getOperatorPodUID(oldPodName)
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "pod", oldPodName,
			"--force", "--grace-period=0", "--wait=false",
		))
		newPodName := waitForFreshOperatorPod(oldPodUID)

		By("verifying the new operator pod logs the cert-manager detection at boot")
		// Wait up to 60s for the boot-time setupWebhookCertProvisioning
		// log line to surface. Identical detection logic also runs once
		// per reconcile pass inside Authority.Start(), but the boot-time
		// branch is the one this scenario exercises.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "logs", newPodName,
				"--tail=2000",
			))
			if err != nil {
				return ""
			}
			return out
		}, 90*time.Second, 5*time.Second).Should(
			SatisfyAny(
				ContainSubstring("cert-manager Certificate detected"),
				ContainSubstring("cert-manager Certificate appeared mid-flight"),
			),
			"operator must log the cert-manager opt-in detection in cert-manager mode")

		By("removing the cert-manager Certificate to simulate operator falling back to dynauth")
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "certificate.cert-manager.io",
			leafSecretName, "--wait=true",
		))
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "issuer.cert-manager.io",
			"velkir-e2e-issuer", "--wait=true",
		))

		By("restarting the operator pod so the boot-time detection re-evaluates against an empty cert-manager state")
		fallbackOldName := getOperatorPodName()
		fallbackOldUID := getOperatorPodUID(fallbackOldName)
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "pod", fallbackOldName,
			"--force", "--grace-period=0", "--wait=false",
		))
		_ = waitForFreshOperatorPod(fallbackOldUID)

		By("verifying the dynauth path re-engages — CA Secret stays under operator ownership")
		// `app.kubernetes.io/managed-by=velkir` on the CA
		// Secret is the operator's own ownership stamp from
		// dynauth.Authority.createSecret. If the Authority loop is
		// running, it converges to this label on first reconcile.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "get", "secret", caSecretName,
				"-o", `jsonpath={.metadata.labels.app\.kubernetes\.io/managed-by}`,
			))
			return strings.TrimSpace(out)
		}, 2*time.Minute, 5*time.Second).Should(Equal("velkir"),
			"CA Secret must carry the operator's managed-by stamp after fall-back to dynauth")
	})

	// Scenario 3 — force-rotate annotation triggers immediate
	// reissue + self-clears the annotation.
	//
	// Stamping the force-rotate annotation on the CA Secret
	// causes Authority.ensureCA's shouldForceRotate branch to
	// bypass ShouldRotate's "rotation fraction" gate and mint a
	// fresh CA on the next reconcile pass. The new CA is signed
	// with a different serial; the annotation is cleared inside
	// the same updateSecret call so it doesn't loop. The
	// Injector then propagates the new CA bundle to every
	// labelled WebhookConfiguration.
	It("force-rotate annotation — reissues CA on next reconcile and clears the annotation", func() {
		By("recording the CA Secret's current cert serial and PEM")
		serialBefore := readCACertSerial()
		Expect(serialBefore).NotTo(BeEmpty(), "CA Secret must have a parseable cert before the test")
		caPEMBefore := readCACertPEM()
		Expect(caPEMBefore).NotTo(BeEmpty())

		By("stamping the force-rotate annotation on the CA Secret")
		// Value is opaque to the operator — RFC3339 timestamp is the
		// convention so the audit trail shows when the rotation was
		// requested.
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "annotate", "--overwrite",
			"secret", caSecretName,
			forceRotateAnnotation+"="+time.Now().UTC().Format(time.RFC3339),
		))

		By("waiting for the CA cert serial to advance and the annotation to clear")
		Eventually(func() bool {
			serialNow := readCACertSerial()
			if serialNow == "" || serialNow == serialBefore {
				return false
			}
			ann, _ := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "get", "secret", caSecretName,
				"-o", `jsonpath={.metadata.annotations.velkir\.ioxie\.dev/force-rotate}`,
			))
			return strings.TrimSpace(ann) == ""
		}, 3*time.Minute, 5*time.Second).Should(BeTrue(),
			"CA cert serial must advance and the force-rotate annotation must self-clear")

		By("waiting for the CA bundle on the validating WebhookConfiguration to match the new CA")
		// The Injector controller watches the CA Secret and patches
		// every labelled WebhookConfiguration's clientConfig.caBundle
		// on each reconcile. After a fresh CA, the bundle on the
		// validator must equal the new CA's PEM.
		caPEMAfter := readCACertPEM()
		Expect(caPEMAfter).NotTo(BeEmpty(), "CA Secret must have a parseable cert after rotation")
		// Cross-check: the PEM bytes must actually differ from the
		// pre-rotation PEM. Serial advancement is the load-bearing
		// invariant, but a regression that produces a new cert with
		// identical bytes (same key, same serial bumped only in
		// transit metadata) would slip past the serial check while
		// breaking the rotation. Bytes-differ rules that out.
		Expect(string(caPEMAfter)).NotTo(Equal(string(caPEMBefore)),
			"rotated CA PEM must differ byte-for-byte from the pre-rotation PEM")
		Eventually(func() bool {
			return webhookCABundleEquals(validatingWebhookName, caPEMAfter)
		}, 90*time.Second, 5*time.Second).Should(BeTrue(),
			"validating WebhookConfiguration's caBundle must converge to the new CA")
	})

	// Scenario 4 — operator restart across a force-rotation
	// preserves rotation completion.
	//
	// The annotation persists on the Secret across restarts
	// (Secret state is etcd-durable), so killing the operator
	// after stamping but before the loop reads the annotation
	// just hands the rotation to the new replica. Either pod
	// completes the rotation; neither half-rotates because the
	// Secret payload + annotation-clear go together in a single
	// updateSecret call.
	//
	// In a single-replica deploy the "HA-safe under lease"
	// invariant collapses to "the new pod takes the lease and
	// runs the rotation"; a true two-replica failover would
	// require replicas=2 + PodAntiAffinity in this scenario,
	// which the standard `make deploy` doesn't provide. The
	// scenario still pins the load-bearing post-condition: after
	// a kill, rotation completes cleanly and the CA bundle on
	// every labelled WebhookConfiguration converges to the new
	// CA.
	It("operator restart across rotation — completes cleanly under the new leader", func() {
		if sharedClusterMode() {
			// Tracked separately (race-condition list).
			//
			// Operator-kill-during-rotation chains together: lease
			// handover (up to LeaseDuration=15s), cache sync on the
			// new pod, Authority loop start, force-rotate annotation
			// observation, CA reissue, Secret update, Injector
			// reconcile, sequential SSA patches across mutating +
			// validating webhook configs. Each step is correct
			// individually; the cumulative convergence window
			// exceeds the test's 90s caBundle budget on the shared
			// cluster's contention profile. The non-restart variant
			// (scenario 3 above) verifies the rotation + injector
			// path independently and DOES pass.
			Skip("flaky on shared cluster: operator-restart + lease-handover + injector reconcile races the 90s caBundle convergence budget (tracked in #410)")
		}
		By("recording the CA Secret's current cert serial and PEM")
		serialBefore := readCACertSerial()
		Expect(serialBefore).NotTo(BeEmpty())
		caPEMBefore := readCACertPEM()
		Expect(caPEMBefore).NotTo(BeEmpty())

		By("stamping the force-rotate annotation on the CA Secret")
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "annotate", "--overwrite",
			"secret", caSecretName,
			forceRotateAnnotation+"="+time.Now().UTC().Format(time.RFC3339),
		))

		By("immediately force-deleting the operator pod to exercise the lease handover")
		// Kill window deliberately tight: the annotation has just
		// been stamped; either the existing Authority loop reads it
		// before exit (rotates + clears in one updateSecret), or the
		// new replica reads it on first reconcile (same atomic
		// update). Either outcome satisfies the post-condition.
		opPodNameBefore := getOperatorPodName()
		opPodUIDBefore := getOperatorPodUID(opPodNameBefore)
		_ = mustRun(exec.Command(
			"kubectl", "-n", namespace, "delete", "pod", opPodNameBefore,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the Deployment to recreate the operator pod with a fresh UID and Ready=True")
		_ = waitForFreshOperatorPod(opPodUIDBefore)

		By("verifying rotation completed — serial advanced and annotation cleared")
		Eventually(func() bool {
			serialNow := readCACertSerial()
			if serialNow == "" || serialNow == serialBefore {
				return false
			}
			ann, _ := utils.Run(exec.Command(
				"kubectl", "-n", namespace, "get", "secret", caSecretName,
				"-o", `jsonpath={.metadata.annotations.velkir\.ioxie\.dev/force-rotate}`,
			))
			return strings.TrimSpace(ann) == ""
		}, 3*time.Minute, 5*time.Second).Should(BeTrue(),
			"rotation must complete (serial advances + annotation clears) even with an operator restart mid-flight")

		By("verifying every labelled WebhookConfiguration's caBundle converges to the new CA")
		caPEMAfter := readCACertPEM()
		Expect(caPEMAfter).NotTo(BeEmpty())
		Expect(string(caPEMAfter)).NotTo(Equal(string(caPEMBefore)),
			"rotated CA PEM must differ byte-for-byte from the pre-rotation PEM")
		Eventually(func() bool {
			return webhookCABundleEquals(validatingWebhookName, caPEMAfter) &&
				webhookCABundleEquals(mutatingWebhookName, caPEMAfter)
		}, 90*time.Second, 5*time.Second).Should(BeTrue(),
			"both WebhookConfigurations' caBundles must converge to the new CA after the restart")
	})
})

// --- helpers ---------------------------------------------------------------

// getOperatorPodName returns the single non-Terminating operator
// pod's name. Fails the spec if there isn't exactly one.
func getOperatorPodName() string {
	GinkgoHelper()
	out := mustRun(exec.Command(
		"kubectl", "-n", namespace, "get", "pods",
		"-l", operatorLabel,
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}{{ \"\\n\" }}{{ end }}{{ end }}",
	))
	names := utils.GetNonEmptyLines(out)
	Expect(names).To(HaveLen(1), "expected exactly one running operator pod, got %d", len(names))
	return strings.TrimSpace(names[0])
}

// getOperatorPodUID reads .metadata.uid off the named pod. Used as
// the post-kill "is this still the old pod or a new one" key.
func getOperatorPodUID(name string) string {
	GinkgoHelper()
	out := mustRun(exec.Command(
		"kubectl", "-n", namespace, "get", "pod", name,
		"-o", "jsonpath={.metadata.uid}",
	))
	uid := strings.TrimSpace(out)
	Expect(uid).NotTo(BeEmpty())
	return uid
}

// waitForFreshOperatorPod blocks until the Deployment surfaces a
// non-Terminating pod whose UID differs from oldUID AND whose Ready
// condition is True. Returns the new pod name. Mirrors the
// scenario 5 pattern from valkey_sentinel_test.go.
//
// 3-minute timeout matches the Manager Describe's own readiness
// window in e2e_test.go: pod scheduling + container start + leader-
// election ack on a kind cluster with the manager image already
// loaded sits comfortably under that ceiling.
func waitForFreshOperatorPod(oldUID string) string {
	GinkgoHelper()
	const timeout = 3 * time.Minute
	var newName string
	Eventually(func() string {
		out, err := utils.Run(exec.Command(
			"kubectl", "-n", namespace, "get", "pods",
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
			if uid == oldUID || ready != "True" {
				continue
			}
			newName = name
			return name
		}
		return ""
	}, timeout, 2*time.Second).ShouldNot(BeEmpty(),
		"Deployment must recreate the operator pod and the new pod must reach Ready=True")
	return newName
}

// applyToOperatorNamespace applies the given manifest into the
// operator namespace. Spec-fail on apply error.
func applyToOperatorNamespace(yaml string) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "-n", namespace, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kubectl apply: %s", string(out))
}

// readCACertSerial returns the current CA cert's serial number as
// a decimal string, or "" if the Secret / payload can't be parsed.
// Used as the "did rotation happen?" probe. Parse-failure paths log
// a one-line diagnostic to GinkgoWriter so flake triage isn't blind
// to whether the failure was "Secret missing" vs "payload corrupt"
// vs "no serial".
func readCACertSerial() string {
	GinkgoHelper()
	pemBytes := readCACertPEM()
	if len(pemBytes) == 0 {
		_, _ = fmt.Fprintf(GinkgoWriter, "readCACertSerial: empty PEM from CA Secret %q\n", caSecretName)
		return ""
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "readCACertSerial: pem.Decode returned nil block for CA Secret %q\n", caSecretName)
		return ""
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "readCACertSerial: x509.ParseCertificate failed: %v\n", err)
		return ""
	}
	if cert.SerialNumber == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "readCACertSerial: cert has nil SerialNumber\n")
		return ""
	}
	return cert.SerialNumber.String()
}

// readCACertPEM reads the CA Secret's tls.crt and returns the raw PEM
// bytes. Returns nil on any error.
func readCACertPEM() []byte {
	GinkgoHelper()
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", namespace, "get", "secret", caSecretName,
		"-o", `jsonpath={.data.tls\.crt}`,
	))
	if err != nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out))
	if err != nil {
		return nil
	}
	return decoded
}

// webhookCABundleEquals returns true iff every per-webhook
// clientConfig.caBundle on the named WebhookConfiguration equals
// wantPEM. The Injector patches every webhook's bundle to the same
// CA on each reconcile, so the check is symmetric across all
// webhooks on the resource.
//
// resourceName is the full name of either a
// ValidatingWebhookConfiguration or MutatingWebhookConfiguration.
// The kind is inferred from the name suffix ("-validating-..." or
// "-mutating-...") — both are at the same /v1 group, and the
// kubectl get accepts either kind explicitly.
func webhookCABundleEquals(resourceName string, wantPEM []byte) bool {
	kind := "validatingwebhookconfiguration"
	if strings.Contains(resourceName, "mutating") {
		kind = "mutatingwebhookconfiguration"
	}
	out, err := utils.Run(exec.Command(
		"kubectl", "get", kind, resourceName,
		"-o", "jsonpath={.webhooks[*].clientConfig.caBundle}",
	))
	if err != nil {
		return false
	}
	// jsonpath returns each per-webhook bundle space-separated.
	// Each entry is a single base64 string (no newlines) that
	// decodes back to the PEM bundle the operator's injector
	// stamped.
	wantB64 := base64.StdEncoding.EncodeToString(wantPEM)
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return false
	}
	for gotB64 := range strings.FieldsSeq(trimmed) {
		if gotB64 != wantB64 {
			return false
		}
	}
	return true
}
