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

	. "github.com/onsi/ginkgo/v2"

	"github.com/ioxie/velkir/test/utils"
)

// namespace is the namespace the operator runs in. Defaults to the
// kustomize make-deploy target's "velkir-system"; in
// shared-cluster mode the chart-install harness sets
// E2E_OPERATOR_NAMESPACE to a unique value per run so multiple test
// runs (or a test run alongside a production operator install) don't
// collide on namespaced resources.
var namespace = envOrDefault("E2E_OPERATOR_NAMESPACE", "velkir-system")

// envOrDefault returns os.Getenv(key) when set + non-empty, else def.
// Local to this package so the env-driven defaults stay self-contained.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// dumpDiagnosticsOnFailure writes a structured snapshot of the test
// namespace (Valkey + SentinelQuorum CRs, pods, events) and the
// operator deployment's recent logs to GinkgoWriter when the
// currently-finishing spec failed. Wire it into each top-level
// Describe via JustAfterEach so triage from a shared-cluster CI run
// doesn't require the cluster to stay around after the suite — the
// run log itself carries enough state to reconstruct what the
// operator saw.
//
// Best-effort: every kubectl shell-out is wrapped so a failure to
// fetch one bucket of diagnostics never masks the original spec
// failure. Skipped specs and passing specs short-circuit.
func dumpDiagnosticsOnFailure() {
	report := CurrentSpecReport()
	if !report.Failed() {
		return
	}
	if e2eNamespace == "" {
		// BeforeSuite didn't run to completion or set the package
		// var — nothing to dump against.
		return
	}
	// `namespace` (e2e_test.go:42) is itself defaulted from
	// E2E_OPERATOR_NAMESPACE, so it carries the right value in both
	// the env-set and unset paths. Re-read the env directly so the
	// data flow stays explicit at the call site.
	opNS := envOrDefault("E2E_OPERATOR_NAMESPACE", "velkir-system")
	opLabel := envOrDefault("E2E_OPERATOR_LABEL", "control-plane=controller-manager")

	header := fmt.Sprintf("\n=== DIAGNOSTICS for failed spec ===\nspec=%s\ntest-ns=%s\noperator-ns=%s\noperator-label=%s\n",
		report.FullText(), e2eNamespace, opNS, opLabel)
	_, _ = fmt.Fprint(GinkgoWriter, header)

	for _, step := range []struct {
		title string
		cmd   *exec.Cmd
	}{
		{"Valkey + SentinelQuorum CRs (yaml)",
			exec.Command("kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev,sentinelquorums.velkir.ioxie.dev", "-o", "yaml")},
		{"Pods (wide)",
			exec.Command("kubectl", "-n", e2eNamespace, "get", "pods", "-o", "wide")},
		{"Pod descriptions",
			exec.Command("kubectl", "-n", e2eNamespace, "describe", "pods")},
		{"StatefulSets",
			exec.Command("kubectl", "-n", e2eNamespace, "get", "sts", "-o", "wide")},
		{"Services + Endpoints",
			exec.Command("kubectl", "-n", e2eNamespace, "get", "svc,endpoints", "-o", "wide")},
		{"Events (most recent last)",
			exec.Command("kubectl", "-n", e2eNamespace, "get", "events", "--sort-by=.lastTimestamp")},
		{"Operator pod logs (last 400 lines)",
			exec.Command("kubectl", "-n", opNS, "logs", "-l", opLabel, "--tail=400", "--all-containers=true")},
	} {
		out, err := utils.Run(step.cmd)
		_, _ = fmt.Fprintf(GinkgoWriter, "--- %s ---\n", step.title)
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "(diagnostic fetch failed: %v)\n", err)
		}
		_, _ = fmt.Fprint(GinkgoWriter, out)
		if len(out) > 0 && out[len(out)-1] != '\n' {
			_, _ = fmt.Fprintln(GinkgoWriter)
		}
	}

	// Per-valkey-pod replication + role state, plus rendered valkey.conf.
	// These three queries together reveal the entire data-plane reality
	// for the failed spec: where each pod thinks the primary is, whether
	// the replication link is up, what lag is observed, and what config
	// the init container actually generated. Best-effort: exec failures
	// (pod not yet ready / container not found / valkey-cli rejected)
	// are reported inline but don't abort.
	podList, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/component=valkey",
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	if err == nil {
		names := strings.Fields(strings.TrimSpace(podList))
		for _, pod := range names {
			for _, probe := range []struct {
				title string
				args  []string
			}{
				{"INFO replication",
					[]string{"valkey-cli", "-p", "6379", "INFO", "replication"}},
				{"CONFIG GET replicaof",
					[]string{"valkey-cli", "-p", "6379", "CONFIG", "GET", "replicaof"}},
				{"rendered /config/valkey.conf",
					[]string{"sh", "-c", "tail -20 /config/valkey.conf 2>/dev/null || echo '(unreadable)'"}},
			} {
				args := append([]string{"-n", e2eNamespace, "exec", pod, "-c", "valkey", "--"}, probe.args...)
				out, exErr := utils.Run(exec.Command("kubectl", args...))
				_, _ = fmt.Fprintf(GinkgoWriter, "--- pod %s — %s ---\n", pod, probe.title)
				if exErr != nil {
					_, _ = fmt.Fprintf(GinkgoWriter, "(exec failed: %v)\n", exErr)
				}
				_, _ = fmt.Fprint(GinkgoWriter, out)
				if len(out) > 0 && out[len(out)-1] != '\n' {
					_, _ = fmt.Fprintln(GinkgoWriter)
				}
			}
		}
	}
	_, _ = fmt.Fprintln(GinkgoWriter, "=== END DIAGNOSTICS ===")
}
