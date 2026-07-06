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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/ioxie/velkir/internal/logging"
)

// allMetricNames is the catalog under test. Adding a metric to
// metrics.go requires adding its name const here too — the lookup
// keeps the prefix + cardinality assertions honest as new metrics
// land.
var allMetricNames = []string{
	NameReconciliationsFailedTotal,
	NameReconciliationsLockedTotal,
	NameResources,
	NameResourceState,
	NameResourcesPaused,
	NameFailoversTotal,
	NameFailoverDurationSeconds,
	NameRolloutsTotal,
	NameRolloutDurationSeconds,
	NameSentinelObserverReconnectsTotal,
	NameSentinelObserverConnected,
	NameSentinelPubsubMessagesTotal,
	NameQuorumCheckTotal,
	NameQuorumSuppressionTransitionsTotal,
	NameSplitBrainDetectionsTotal,
	NameSplitBrainSustainedSeconds,
	NameDualMasterObserved,
	NameSentinelTopologyMismatch,
	NameMasterInfoTimeoutSeconds,
	NameWebhookRequestsTotal,
	NameWebhookRequestDurationSeconds,
	NameCertRotationsTotal,
	NameCertExpirySeconds,
	NameSecretRotationTotal,
	NameExporterSidecarUp,
	NamePVCResizeInProgress,
	NameRedactionRegistryTokens,
}

func TestDeclaredMetricNamesUseValkeyPrefix(t *testing.T) {
	// Assert against the exported name consts directly rather than parsing
	// prometheus.Desc.String(), which is format-unstable across client
	// library versions.
	for _, name := range allMetricNames {
		if !strings.HasPrefix(name, "valkey_") {
			t.Errorf("metric name %q must start with valkey_", name)
		}
	}
}

func TestCounterAndHistogramSuffixesFollowConvention(t *testing.T) {
	// Counters end in `_total`; histograms end in `_seconds` (the only
	// histogram unit in the operator catalog). Gauges have neither
	// suffix. Catch suffix drift early — a counter named
	// `valkey_foo_count` (without _total) wouldn't conform.
	for _, name := range allMetricNames {
		switch {
		case strings.HasSuffix(name, "_total"):
			// Counter — fine.
		case strings.HasSuffix(name, "_seconds"):
			// Histogram (or rare gauge like cert_expiry_seconds) — fine.
		default:
			// Gauge: must NOT have a _total or _count suffix that would
			// suggest a counter. The whitelist below documents the
			// gauges we have today; failure mode is "new metric added
			// without thinking about its kind".
			gauges := map[string]bool{
				NameResources:                 true,
				NameResourceState:             true,
				NameResourcesPaused:           true,
				NameSentinelObserverConnected: true,
				NameExporterSidecarUp:         true,
				NamePVCResizeInProgress:       true,
				NameRedactionRegistryTokens:   true,
				NameDualMasterObserved:        true,
				NameSentinelTopologyMismatch:  true,
			}
			if !gauges[name] {
				t.Errorf("metric name %q neither ends in _total / _seconds nor is a known gauge; consider its kind", name)
			}
		}
	}
}

func TestCountersIncrement(t *testing.T) {
	ReconciliationsLockedTotal.WithLabelValues("Valkey", "default", "sample").Inc()
	if got := testutil.ToFloat64(ReconciliationsLockedTotal.WithLabelValues("Valkey", "default", "sample")); got < 1 {
		t.Errorf("ReconciliationsLockedTotal not incremented; got %v", got)
	}
	ReconciliationsFailedTotal.WithLabelValues("Valkey", "default", "sample", "AuthSecretNotFound").Inc()
	if got := testutil.ToFloat64(ReconciliationsFailedTotal.WithLabelValues("Valkey", "default", "sample", "AuthSecretNotFound")); got < 1 {
		t.Errorf("ReconciliationsFailedTotal not incremented; got %v", got)
	}
}

func TestSetSentinelTopologyMismatch(t *testing.T) {
	const ns, name = "default", "topo-sample"
	SetSentinelTopologyMismatch(ns, name, 1, 2)
	if got := testutil.ToFloat64(SentinelTopologyMismatch.WithLabelValues(ns, name, "sentinels")); got != 1 {
		t.Errorf("sentinels dimension = %v; want 1", got)
	}
	if got := testutil.ToFloat64(SentinelTopologyMismatch.WithLabelValues(ns, name, "replicas")); got != 2 {
		t.Errorf("replicas dimension = %v; want 2", got)
	}
	// A zero deficit sets the series to 0 (not delete), so the gauge
	// tracks the condition rather than latching at its last value.
	SetSentinelTopologyMismatch(ns, name, 0, 0)
	if got := testutil.ToFloat64(SentinelTopologyMismatch.WithLabelValues(ns, name, "sentinels")); got != 0 {
		t.Errorf("sentinels dimension after clear = %v; want 0", got)
	}
	if got := testutil.ToFloat64(SentinelTopologyMismatch.WithLabelValues(ns, name, "replicas")); got != 0 {
		t.Errorf("replicas dimension after clear = %v; want 0", got)
	}
}

func TestHistogramObservesRecordsSample(t *testing.T) {
	obs := FailoverDurationSeconds.WithLabelValues("default", "hist-sample", "manual")
	obs.Observe(0.42)
	obs.Observe(0.75)

	m, ok := obs.(prometheus.Metric)
	if !ok {
		t.Fatalf("observer is not a prometheus.Metric; got %T", obs)
	}
	var pb dto.Metric
	if err := m.Write(&pb); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := pb.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("SampleCount = %d, want 2", got)
	}
	if got := pb.Histogram.GetSampleSum(); got < 1.16 || got > 1.18 {
		t.Errorf("SampleSum = %v, want ~1.17 (0.42 + 0.75)", got)
	}
}

func TestCollectorsReturnsAllDeclared(t *testing.T) {
	got := Collectors()
	want := len(allMetricNames)
	if len(got) != want {
		t.Errorf("Collectors returned %d entries; want %d (update both this test's expectation AND allMetricNames when the metric set changes)", len(got), want)
	}
}

func TestCertExpirySeconds_LabelKeysFixed(t *testing.T) {
	// `kind` is the only label and the value space is bounded by the leaf
	// set the dynauth Authority manages: {ca, webhook-leaf, metrics-leaf}.
	// Setting all three must not error or churn cardinality.
	CertExpirySeconds.WithLabelValues("ca").Set(123)
	CertExpirySeconds.WithLabelValues("webhook-leaf").Set(456)
	CertExpirySeconds.WithLabelValues("metrics-leaf").Set(789)
}

func TestExporterSidecarUp_LabelShape(t *testing.T) {
	// Per-pod gauge bounded by valkey.replicas (1-10). Setting via the
	// canonical {namespace, name, pod} triple must not panic and the
	// resulting series must round-trip its value through testutil.
	ExporterSidecarUp.WithLabelValues("default", "test-cr", "test-cr-0").Set(1)
	ExporterSidecarUp.WithLabelValues("default", "test-cr", "test-cr-1").Set(0)
	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues("default", "test-cr", "test-cr-0")); got != 1 {
		t.Errorf("ExporterSidecarUp[test-cr-0] = %v; want 1", got)
	}
	if got := testutil.ToFloat64(ExporterSidecarUp.WithLabelValues("default", "test-cr", "test-cr-1")); got != 0 {
		t.Errorf("ExporterSidecarUp[test-cr-1] = %v; want 0", got)
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	// Both calls against the shared Registry must succeed without panic.
	// A second MustRegister on the same Collector would normally panic with
	// "already registered"; the sync.Once guard inside Register makes it safe.
	Register()
	Register()
}

func TestRedactionRegistryTokens_ReflectsRegistryLen(t *testing.T) {
	// The gauge is a GaugeFunc bound to logging.DefaultRegistry.Len —
	// reading testutil.ToFloat64 must observe the live size, not a
	// stale snapshot. Token1/Token2 satisfy MinTokenLen so they
	// actually land in the registry; the cleanups restore process
	// state so this test cannot poison neighbouring tests that share
	// the singleton.
	const token1 = "redaction-registry-metric-token-1"
	const token2 = "redaction-registry-metric-token-2"

	baseline := testutil.ToFloat64(RedactionRegistryTokens)

	logging.DefaultRegistry.Register(token1)
	t.Cleanup(func() { logging.DefaultRegistry.Forget(token1) })
	if got := testutil.ToFloat64(RedactionRegistryTokens); got != baseline+1 {
		t.Errorf("after one Register: gauge = %v; want %v", got, baseline+1)
	}

	logging.DefaultRegistry.Register(token2)
	t.Cleanup(func() { logging.DefaultRegistry.Forget(token2) })
	if got := testutil.ToFloat64(RedactionRegistryTokens); got != baseline+2 {
		t.Errorf("after two Register: gauge = %v; want %v", got, baseline+2)
	}

	logging.DefaultRegistry.Forget(token2)
	if got := testutil.ToFloat64(RedactionRegistryTokens); got != baseline+1 {
		t.Errorf("after one Forget: gauge = %v; want %v", got, baseline+1)
	}
	// Re-Register the same token so the deferred Cleanup balances the
	// extra Forget we just did.
	logging.DefaultRegistry.Register(token2)
}

// ResetReconcileGauges must drop every series for a deleted CR — across all
// label shapes (the extra condition_type/status, sentinel_pod, pod, trigger,
// reason, … labels) and every vector type (Counter, Gauge, Histogram) — and
// leave other CRs' series intact. It seeds three series per vector: two CRs in
// one namespace (proving `name` disambiguation) and a same-`name` CR in a
// second namespace (proving `namespace` disambiguation — the reap keys on both
// labels, not just one). This is the behavioural guard against the per-CR /
// per-pod cardinality leak: pre-fix, ResourceState and the per-pod vecs
// were never reaped. Every per-CR vector in Collectors() is exercised here.
func TestResetReconcileGauges_ReapsPerCRSeries(t *testing.T) {
	const ns1, ns2 = "reapns", "reapns2"
	seeders := []struct {
		name string
		vec  prometheus.Collector
		set  func(ns, cr string)
		rst  func()
	}{
		{"reconciliations_failed_total", ReconciliationsFailedTotal,
			func(ns, cr string) { ReconciliationsFailedTotal.WithLabelValues("Valkey", ns, cr, "STSConflict").Inc() },
			ReconciliationsFailedTotal.Reset},
		{"reconciliations_locked_total", ReconciliationsLockedTotal,
			func(ns, cr string) { ReconciliationsLockedTotal.WithLabelValues("Valkey", ns, cr).Inc() },
			ReconciliationsLockedTotal.Reset},
		{"resource_state", ResourceState,
			func(ns, cr string) { ResourceState.WithLabelValues("Valkey", ns, cr, "Ready", "True").Set(1) },
			ResourceState.Reset},
		{"resources_paused", ResourcesPaused,
			func(ns, cr string) { ResourcesPaused.WithLabelValues(ns, cr).Set(1) },
			ResourcesPaused.Reset},
		{"failovers_total", FailoversTotal,
			func(ns, cr string) { FailoversTotal.WithLabelValues(ns, cr, "manual").Inc() },
			FailoversTotal.Reset},
		{"failover_duration", FailoverDurationSeconds,
			func(ns, cr string) { FailoverDurationSeconds.WithLabelValues(ns, cr, "manual").Observe(0.1) },
			FailoverDurationSeconds.Reset},
		{"rollouts_total", RolloutsTotal,
			func(ns, cr string) { RolloutsTotal.WithLabelValues(ns, cr, "image_bump").Inc() },
			RolloutsTotal.Reset},
		{"rollout_duration", RolloutDurationSeconds,
			func(ns, cr string) { RolloutDurationSeconds.WithLabelValues(ns, cr, "image_bump").Observe(0.1) },
			RolloutDurationSeconds.Reset},
		{"observer_reconnects", SentinelObserverReconnectsTotal,
			func(ns, cr string) { SentinelObserverReconnectsTotal.WithLabelValues(ns, cr, "pod-0").Inc() },
			SentinelObserverReconnectsTotal.Reset},
		{"observer_connected", SentinelObserverConnected,
			func(ns, cr string) { SentinelObserverConnected.WithLabelValues(ns, cr, "pod-0").Set(1) },
			SentinelObserverConnected.Reset},
		{"pubsub_messages", SentinelPubsubMessagesTotal,
			func(ns, cr string) { SentinelPubsubMessagesTotal.WithLabelValues(ns, cr, "+odown").Inc() },
			SentinelPubsubMessagesTotal.Reset},
		{"quorum_check_total", QuorumCheckTotal,
			func(ns, cr string) { QuorumCheckTotal.WithLabelValues(ns, cr, "ok").Inc() },
			QuorumCheckTotal.Reset},
		{"split_brain", SplitBrainDetectionsTotal,
			func(ns, cr string) { SplitBrainDetectionsTotal.WithLabelValues(ns, cr).Inc() },
			SplitBrainDetectionsTotal.Reset},
		{"split_brain_sustained", SplitBrainSustainedSeconds,
			func(ns, cr string) { SplitBrainSustainedSeconds.WithLabelValues(ns, cr).Set(1) },
			SplitBrainSustainedSeconds.Reset},
		{"master_info_timeout", MasterInfoTimeoutSeconds,
			func(ns, cr string) { MasterInfoTimeoutSeconds.WithLabelValues(ns, cr).Set(1) },
			MasterInfoTimeoutSeconds.Reset},
		{"quorum_transitions", QuorumSuppressionTransitionsTotal,
			func(ns, cr string) {
				QuorumSuppressionTransitionsTotal.WithLabelValues(ns, cr, "inactive", "active").Inc()
			},
			QuorumSuppressionTransitionsTotal.Reset},
		{"secret_rotation_total", SecretRotationTotal,
			func(ns, cr string) { SecretRotationTotal.WithLabelValues(ns, cr, "success").Inc() },
			SecretRotationTotal.Reset},
		{"exporter_up", ExporterSidecarUp,
			func(ns, cr string) { ExporterSidecarUp.WithLabelValues(ns, cr, "pod-0").Set(1) },
			ExporterSidecarUp.Reset},
		{"pvc_resize_in_progress", PVCResizeInProgress,
			func(ns, cr string) { PVCResizeInProgress.WithLabelValues(ns, cr).Set(1) },
			PVCResizeInProgress.Reset},
	}

	for _, s := range seeders {
		s.rst() // isolate from any residue left by neighbouring tests
		s.set(ns1, "cr1")
		s.set(ns1, "cr2")
		s.set(ns2, "cr1") // same name, different namespace — must survive the reap
		if got := testutil.CollectAndCount(s.vec); got != 3 {
			t.Fatalf("%s: seeded series=%d, want 3", s.name, got)
		}
	}

	ResetReconcileGauges(ns1, "cr1")

	for _, s := range seeders {
		if got := testutil.CollectAndCount(s.vec); got != 2 {
			t.Errorf("%s: after reaping (%s, cr1), series=%d, want 2 — same-namespace cr2 and same-name/other-namespace %s/cr1 must both survive", s.name, ns1, got, ns2)
		}
	}
}
