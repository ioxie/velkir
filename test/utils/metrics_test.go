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

package utils

import (
	"strings"
	"testing"
)

func TestBuildMetricsReaderManifest_StampsTeardownLabelOnAllObjects(t *testing.T) {
	// Shared-cluster mode: the harness exports the teardown selector. All
	// three objects (SA + cluster-scoped Role + Binding) must carry it so the
	// tools/e2e-shared.sh `delete ... -l <selector>` sweep reaps the
	// cluster-scoped pair instead of leaking one per run.
	t.Setenv("E2E_OPERATOR_LABEL", "app.kubernetes.io/instance=my-release")

	m := buildMetricsReaderManifest("op-ns-abc123")

	if got := strings.Count(m, "app.kubernetes.io/instance: my-release"); got != 3 {
		t.Errorf("teardown label must appear on all 3 objects; got %d occurrence(s)\n%s", got, m)
	}
	// Pin the indentation so the label actually nests under metadata.
	if !strings.Contains(m, "\n  labels:\n    app.kubernetes.io/instance: my-release") {
		t.Errorf("labels block not correctly indented under metadata\n%s", m)
	}
}

func TestBuildMetricsReaderManifest_NoLabelEnv_NoLabelsBlock(t *testing.T) {
	// Kind `make test-e2e` path (selector unset) — the whole cluster is torn
	// down, so the objects need no label and none must be emitted (keeps the
	// prior unlabeled manifest byte-for-byte).
	t.Setenv("E2E_OPERATOR_LABEL", "")

	m := buildMetricsReaderManifest("op-ns")

	if strings.Contains(m, "labels:") {
		t.Errorf("no E2E_OPERATOR_LABEL → no labels block expected\n%s", m)
	}
	// The core objects are still rendered.
	for _, want := range []string{"kind: ServiceAccount", "kind: ClusterRole", "kind: ClusterRoleBinding"} {
		if !strings.Contains(m, want) {
			t.Errorf("manifest missing %q", want)
		}
	}
}

func TestMetricsReaderLabels_MalformedEnv_ReturnsEmpty(t *testing.T) {
	// A value with no '=' (or an empty key/value) is not a usable
	// key=value selector — emit nothing rather than malformed YAML.
	for _, raw := range []string{"no-equals-sign", "=only-value", "only-key=", ""} {
		t.Setenv("E2E_OPERATOR_LABEL", raw)
		if got := metricsReaderLabels(); got != "" {
			t.Errorf("E2E_OPERATOR_LABEL=%q must yield no labels block; got %q", raw, got)
		}
	}
}
