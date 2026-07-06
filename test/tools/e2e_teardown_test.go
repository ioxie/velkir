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

// Package tools holds characterization tests for the contributor-only
// shell tooling under tools/.
//
// This file is a golden-master test pinning the kubectl/helm teardown
// command sequence emitted by tools/e2e-shared.sh's cleanup trap and by
// tools/e2e-shared-cleanup.sh. The two scripts duplicate the same
// teardown today (asserted only by a comment); this test pins the exact
// emitted sequence — flags, the --grace-period=0 --force deletes, the
// instance-label cluster-scoped sweep, and the conditional CRD-delete
// branch — across the KUBE_CONTEXT set/unset and CRD-removal cases, so a
// later consolidation can be proven behaviour-preserving.
//
// It works by putting recording stubs for kubectl/helm/go on PATH: each
// stub appends its argv to a record file, so no real cluster is touched.
// e2e-shared-cleanup.sh is invoked directly; e2e-shared.sh is run to
// completion (all stages stubbed) so its EXIT-trap cleanup fires, and the
// teardown commands are then filtered out of the full record.
package tools

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const kubectlStub = `#!/usr/bin/env bash
echo "kubectl $*" >> "$E2E_REC"
# Strip a leading "--context <ctx>" (present when KUBE_CONTEXT is set) so
# the subcommand parsing below sees $1 as the verb either way. The record
# above already captured the full argv including the context flag.
if [[ "$1" == "--context" ]]; then
  shift 2
fi
if [[ "$1" == "config" && "$2" == "current-context" ]]; then
  echo "stub-ctx"
  exit 0
fi
case "$1" in
  get)
    case "$2" in
      ns) exit 1 ;;                                   # absent -> e2e-shared preflight proceeds
      crd) [[ "${STUB_CRDS_PRESENT:-0}" == "1" ]] && exit 0 || exit 1 ;;
      *) exit 0 ;;                                    # empty stdout -> no foreign operator
    esac
    ;;
  *) exit 0 ;;
esac
`

const helmStub = `#!/usr/bin/env bash
echo "helm $*" >> "$E2E_REC"
exit 0
`

// goStub lets e2e-shared.sh's suite step "succeed" so the script exits 0
// and the EXIT trap runs the full teardown (not the KEEP_ON_FAILURE
// early-return). It is never recorded — it emits no teardown command.
const goStub = `#!/usr/bin/env bash
exit 0
`

// repoRoot resolves the module root from this test file's location
// (<root>/test/tools/e2e_teardown_test.go).
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func writeStub(t *testing.T, dir, name, body string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
}

// recordTeardown runs scriptPath under bash with recording stubs on PATH
// and returns the teardown command lines (the kubectl deletes + helm
// uninstalls) it emitted, in order.
func recordTeardown(t *testing.T, scriptPath string, extraEnv []string, args ...string) []string {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	rec := filepath.Join(dir, "rec.log")
	writeStub(t, dir, "kubectl", kubectlStub)
	writeStub(t, dir, "helm", helmStub)
	writeStub(t, dir, "go", goStub)

	cmd := exec.Command(bash, append([]string{scriptPath}, args...)...)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"E2E_REC="+rec,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	if out, runErr := cmd.CombinedOutput(); runErr != nil {
		t.Fatalf("script %s failed: %v\n%s", scriptPath, runErr, out)
	}

	data, err := os.ReadFile(rec)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var teardown []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		// Teardown is exactly the deletes + uninstalls; install-phase
		// calls (get/create/label/wait/api-resources/version, helm
		// install) carry neither token.
		if strings.Contains(line, " delete ") || strings.Contains(line, " uninstall ") {
			teardown = append(teardown, line)
		}
	}
	return teardown
}

func kubectlPrefix(ctx string) string {
	if ctx == "" {
		return "kubectl"
	}
	return "kubectl --context " + ctx
}

func helmPrefix(ctx string) string {
	if ctx == "" {
		return "helm"
	}
	return "helm --kube-context " + ctx
}

// expectedTeardown is the hand-written golden: the teardown command
// sequence both scripts must emit. crdsRelease is "" for the manual
// cleanup script (which has no CRDs helm release); when non-empty (the
// e2e-shared.sh trap path) a `helm uninstall <crdsRelease>` precedes the
// `delete crd`. removeCRDs selects the CRD-delete tail.
func expectedTeardown(
	ctx string, namespaces []string, release, opNS string, removeCRDs bool, crdsRelease string,
) []string {
	k := kubectlPrefix(ctx)
	h := helmPrefix(ctx)
	clusterScoped := "clusterrole,clusterrolebinding,validatingwebhookconfiguration,mutatingwebhookconfiguration"

	var out []string
	for _, ns := range namespaces {
		out = append(out, k+" -n "+ns+" delete valkey --all --grace-period=0 --force --ignore-not-found")
		out = append(out, k+" -n "+ns+" delete sentinelquorum --all --grace-period=0 --force --ignore-not-found")
	}
	out = append(out, h+" uninstall "+release+" -n "+opNS+" --ignore-not-found")
	for _, ns := range namespaces {
		out = append(out, k+" delete ns "+ns+" --ignore-not-found --wait=false")
	}
	out = append(out, k+" delete ns "+opNS+" --ignore-not-found --wait=false")
	out = append(out, k+" delete "+clusterScoped+" -l app.kubernetes.io/instance="+release+" --ignore-not-found")
	if removeCRDs {
		if crdsRelease != "" {
			out = append(out, h+" uninstall "+crdsRelease+" -n "+opNS+" --ignore-not-found")
		}
		crdNames := "valkeys.velkir.ioxie.dev sentinelquorums.velkir.ioxie.dev"
		out = append(out, k+" delete crd "+crdNames+" --ignore-not-found --timeout=30s")
	}
	return out
}

func assertSequence(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("teardown command count = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("teardown[%d]\n got: %q\nwant: %q", i, got[i], want[i])
		}
	}
}

// TestE2ESharedCleanupScript pins tools/e2e-shared-cleanup.sh (the manual
// KEEP_ON_FAILURE cleanup) across its argument forms and KUBE_CONTEXT.
func TestE2ESharedCleanupScript(t *testing.T) {
	script := filepath.Join(repoRoot(t), "tools", "e2e-shared-cleanup.sh")

	cases := []struct {
		name    string
		ctx     string
		nsCSV   string
		nsList  []string
		remove  bool
		extra   []string
		cliArgs []string
	}{
		{
			name:    "single ns, no context, no crd removal",
			nsCSV:   "ns-a",
			nsList:  []string{"ns-a"},
			cliArgs: []string{"rel", "op", "ns-a"},
		},
		{
			name:    "csv namespaces with --remove-crds",
			nsCSV:   "ns-a,ns-b",
			nsList:  []string{"ns-a", "ns-b"},
			remove:  true,
			cliArgs: []string{"rel", "op", "ns-a,ns-b", "--remove-crds"},
		},
		{
			name:    "KUBE_CONTEXT set adds context args",
			ctx:     "kube-ctx",
			nsCSV:   "ns-a",
			nsList:  []string{"ns-a"},
			extra:   []string{"KUBE_CONTEXT=kube-ctx"},
			cliArgs: []string{"rel", "op", "ns-a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := recordTeardown(t, script, tc.extra, tc.cliArgs...)
			want := expectedTeardown(tc.ctx, tc.nsList, "rel", "op", tc.remove, "")
			assertSequence(t, got, want)
		})
	}
}

// TestE2ESharedTrapCleanup pins the cleanup-trap teardown in
// tools/e2e-shared.sh across KUBE_CONTEXT and the CRDS-installed-by-this-run
// branch. With E2E_RUN_ID=test the derived names are deterministic.
func TestE2ESharedTrapCleanup(t *testing.T) {
	script := filepath.Join(repoRoot(t), "tools", "e2e-shared.sh")
	const (
		release     = "valkey-e2e-test"
		crdsRelease = "valkey-e2e-crds-test"
		opNS        = "valkey-e2e-op-test"
		testNS      = "valkey-e2e-test-test"
	)

	cases := []struct {
		name      string
		ctx       string
		crdsByRun bool
		extra     []string
	}{
		{
			name:      "no context, CRDs installed by this run",
			crdsByRun: true,
			extra:     []string{"E2E_RUN_ID=test"},
		},
		{
			name:      "KUBE_CONTEXT set, CRDs installed by this run",
			ctx:       "stub-ctx",
			crdsByRun: true,
			extra:     []string{"E2E_RUN_ID=test", "KUBE_CONTEXT=stub-ctx"},
		},
		{
			name:      "CRDs pre-existing (not installed by this run) — no CRD teardown",
			crdsByRun: false,
			extra:     []string{"E2E_RUN_ID=test", "STUB_CRDS_PRESENT=1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := recordTeardown(t, script, tc.extra)
			crdsRel := ""
			if tc.crdsByRun {
				crdsRel = crdsRelease
			}
			want := expectedTeardown(tc.ctx, []string{testNS}, release, opNS, tc.crdsByRun, crdsRel)
			assertSequence(t, got, want)
		})
	}
}
