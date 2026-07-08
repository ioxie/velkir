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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/velkir:v0.0.1"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting velkir e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

// E2E_SHARED_CLUSTER=true switches the suite to "shared-cluster mode":
// no local docker build, no kind image load, no cert-manager install or
// teardown. The caller (typically tools/e2e-shared.sh) installed the
// operator out-of-band with a unique release name + namespace, so the
// BeforeAll in e2e_test.go also skips make install / make deploy, and
// the AfterAll skips make undeploy / make uninstall — the suite never
// touches cluster-scoped CRDs it didn't create.
//
// Default (E2E_SHARED_CLUSTER unset) preserves the original kind +
// make-deploy flow.
func sharedClusterMode() bool {
	return os.Getenv("E2E_SHARED_CLUSTER") == "true"
}

var _ = BeforeSuite(func() {
	// Resolve the per-process e2e namespace before any spec BeforeAll
	// fires. Parallel mode is detected via SuiteConfig.ParallelTotal
	// (>1 means ginkgo --procs=N>1); GinkgoParallelProcess() alone is
	// not sufficient because it returns 1 for the first parallel
	// process too, which would collapse process-1 onto the bare base
	// name while the harness pre-creates only the per-process
	// <base>-pN namespaces.
	base := envOrDefault("E2E_TEST_NAMESPACE", "valkey-e2e")
	suiteCfg, _ := GinkgoConfiguration()
	if suiteCfg.ParallelTotal > 1 {
		e2eNamespace = fmt.Sprintf("%s-p%d", base, GinkgoParallelProcess())
	} else {
		e2eNamespace = base
	}

	if sharedClusterMode() {
		_, _ = fmt.Fprintf(GinkgoWriter,
			"E2E_SHARED_CLUSTER=true — skipping docker build, kind load, cert-manager install. "+
				"namespace=%s (procID=%d)\n",
			e2eNamespace, GinkgoParallelProcess())
		return
	}

	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	setupCertManager()
})

var _ = AfterSuite(func() {
	if sharedClusterMode() {
		return
	}
	teardownCertManager()
})

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
