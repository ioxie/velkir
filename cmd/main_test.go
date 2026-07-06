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

package main

import (
	"crypto/tls"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/labels"

	operatorconfig "github.com/ioxie/velkir/internal/config"
)

// --- applyTo overlay semantics --------------------------------------------

func TestApplyTo_OverlaysOnlyExplicitFlags(t *testing.T) {
	cfg := operatorconfig.Default()
	originalAddr := cfg.Metrics.BindAddress
	originalID := cfg.LeaderElection.ID

	f := flags{
		metricsAddr:   ":7777",
		leaderElect:   false,
		probeAddr:     ":12345",
		setExplicitly: map[string]bool{},
	}
	f.applyTo(cfg)

	if cfg.Metrics.BindAddress != originalAddr {
		t.Errorf("BindAddress mutated despite flag being unset: %q → %q", originalAddr, cfg.Metrics.BindAddress)
	}
	if cfg.LeaderElection.ID != originalID {
		t.Errorf("LeaderElection.ID should be untouched; got %q", cfg.LeaderElection.ID)
	}
	if !cfg.LeaderElectionEnabled() {
		t.Errorf("LeaderElection.Enabled flipped despite flag unset")
	}
}

func TestApplyTo_FlagMarkedExplicitOverlays(t *testing.T) {
	cfg := operatorconfig.Default()
	f := flags{
		metricsAddr:   ":9191",
		metricsSecure: false,
		leaderElect:   false,
		probeAddr:     ":9090",
		setExplicitly: map[string]bool{
			"metrics-bind-address":      true,
			"metrics-secure":            true,
			"leader-elect":              true,
			"health-probe-bind-address": true,
		},
	}
	f.applyTo(cfg)

	if got := cfg.Metrics.BindAddress; got != ":9191" {
		t.Errorf("Metrics.BindAddress = %q, want :9191", got)
	}
	if cfg.MetricsSecure() {
		t.Errorf("Metrics.Secure should be false after overlay")
	}
	if cfg.LeaderElectionEnabled() {
		t.Errorf("LeaderElection.Enabled should be false after overlay")
	}
	if got := cfg.HealthProbeBindAddress; got != ":9090" {
		t.Errorf("HealthProbeBindAddress = %q, want :9090", got)
	}
}

// --- buildWebhookOptions -------------------------------------------------

func TestBuildWebhookOptions_ConfigDefaults(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.Webhook.Port = 9999
	cfg.Webhook.CertDir = "/etc/cfg-certs"

	opts := buildWebhookOptions(cfg, flags{}, nil)

	if opts.Port != 9999 {
		t.Errorf("Port = %d, want 9999", opts.Port)
	}
	if opts.CertDir != "/etc/cfg-certs" {
		t.Errorf("CertDir = %q, want /etc/cfg-certs", opts.CertDir)
	}
	if opts.CertName != "" || opts.KeyName != "" {
		t.Errorf("CertName/KeyName should be empty when no flag override; got %q / %q",
			opts.CertName, opts.KeyName)
	}
}

func TestBuildWebhookOptions_FlagOverridesConfig(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.Webhook.CertDir = "/config/dir"

	f := flags{
		webhookCertPath: "/flag/dir",
		webhookCertName: "my.crt",
		webhookCertKey:  "my.key",
	}
	opts := buildWebhookOptions(cfg, f, nil)

	if opts.CertDir != "/flag/dir" {
		t.Errorf("CertDir = %q, want /flag/dir (flag override)", opts.CertDir)
	}
	if opts.CertName != "my.crt" {
		t.Errorf("CertName = %q, want my.crt", opts.CertName)
	}
	if opts.KeyName != "my.key" {
		t.Errorf("KeyName = %q, want my.key", opts.KeyName)
	}
}

func TestBuildWebhookOptions_TLSOptsPassedThrough(t *testing.T) {
	cfg := operatorconfig.Default()
	marker := []func(*tls.Config){func(c *tls.Config) { c.NextProtos = []string{"marker"} }}

	opts := buildWebhookOptions(cfg, flags{}, marker)
	if len(opts.TLSOpts) != 1 {
		t.Fatalf("TLSOpts len = %d, want 1", len(opts.TLSOpts))
	}
	tc := &tls.Config{}
	opts.TLSOpts[0](tc)
	if len(tc.NextProtos) != 1 || tc.NextProtos[0] != "marker" {
		t.Errorf("TLSOpts not passed through; got NextProtos = %v", tc.NextProtos)
	}
}

// --- buildMetricsOptions ------------------------------------------------

func TestBuildMetricsOptions_SecureAttachesAuthFilter(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.Metrics.Secure = new(true)
	cfg.Metrics.BindAddress = ":9443"

	opts := buildMetricsOptions(cfg, flags{}, nil)

	if !opts.SecureServing {
		t.Errorf("SecureServing should be true")
	}
	if opts.FilterProvider == nil {
		t.Errorf("FilterProvider should be set for secure metrics")
	}
	if opts.BindAddress != ":9443" {
		t.Errorf("BindAddress = %q, want :9443", opts.BindAddress)
	}
}

func TestBuildMetricsOptions_InsecureNoAuthFilter(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.Metrics.Secure = new(false)

	opts := buildMetricsOptions(cfg, flags{}, nil)

	if opts.SecureServing {
		t.Errorf("SecureServing should be false")
	}
	if opts.FilterProvider != nil {
		t.Errorf("FilterProvider should not be set for insecure metrics")
	}
}

func TestBuildMetricsOptions_FlagCertPathPopulatesCertFields(t *testing.T) {
	cfg := operatorconfig.Default()
	f := flags{
		metricsCertPath: "/m/dir",
		metricsCertName: "m.crt",
		metricsCertKey:  "m.key",
	}
	opts := buildMetricsOptions(cfg, f, nil)

	if opts.CertDir != "/m/dir" || opts.CertName != "m.crt" || opts.KeyName != "m.key" {
		t.Errorf("metrics cert flag passthrough = {%q, %q, %q}; want {/m/dir, m.crt, m.key}",
			opts.CertDir, opts.CertName, opts.KeyName)
	}
}

// --- buildCacheOptions --------------------------------------------------

func TestBuildCacheOptions_NamespaceScope(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.WatchNamespaces = []string{"ns-a", "ns-b"}
	sel, _ := labels.Parse("app.kubernetes.io/managed-by=velkir")

	opts := buildCacheOptions(sel, cfg)

	if _, ok := opts.DefaultNamespaces["ns-a"]; !ok {
		t.Errorf("ns-a missing: %v", opts.DefaultNamespaces)
	}
	if _, ok := opts.DefaultNamespaces["ns-b"]; !ok {
		t.Errorf("ns-b missing: %v", opts.DefaultNamespaces)
	}
	if len(opts.DefaultNamespaces) != 2 {
		t.Errorf("DefaultNamespaces has %d entries, want 2", len(opts.DefaultNamespaces))
	}
}

func TestBuildCacheOptions_ClusterScopeWhenNoNamespaces(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.WatchNamespaces = nil
	sel, _ := labels.Parse("app.kubernetes.io/managed-by=velkir")

	opts := buildCacheOptions(sel, cfg)

	if opts.DefaultNamespaces != nil {
		t.Errorf("DefaultNamespaces should be nil for cluster-scope; got %v", opts.DefaultNamespaces)
	}
}

// TestBuildCacheOptions_ByObjectMatchesOwnedTypes asserts that the map has
// exactly one ByObject entry per type in cacheOwnedTypes — detects both
// accidental drops AND stray additions. The test consumes cacheOwnedTypes
// directly (the source of truth from main.go) so the invariant can't drift.
func TestBuildCacheOptions_ByObjectMatchesOwnedTypes(t *testing.T) {
	cfg := operatorconfig.Default()
	sel, _ := labels.Parse("app.kubernetes.io/managed-by=velkir")

	opts := buildCacheOptions(sel, cfg)

	if got, want := len(opts.ByObject), len(cacheOwnedTypes); got != want {
		t.Errorf("ByObject has %d entries; cacheOwnedTypes has %d — these must match", got, want)
	}
	byType := map[reflect.Type]bool{}
	for k := range opts.ByObject {
		byType[reflect.TypeOf(k)] = true
	}
	for _, obj := range cacheOwnedTypes {
		if !byType[reflect.TypeOf(obj)] {
			t.Errorf("owned type %T missing from cache.ByObject", obj)
		}
	}
}

// --- buildManagerOptions (composer) -------------------------------------

func TestBuildManagerOptions_ThreadsConfig(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.WatchNamespaces = []string{"ns-a"}
	cfg.LeaderElection.ID = "custom-lease"
	cfg.LeaderElection.LeaseDuration.Duration = 25 * time.Second
	cfg.LeaderElection.RenewDeadline.Duration = 15 * time.Second
	cfg.LeaderElection.RetryPeriod.Duration = 3 * time.Second

	opts, err := buildManagerOptions(cfg, flags{})
	if err != nil {
		t.Fatalf("buildManagerOptions: %v", err)
	}
	if opts.LeaderElectionID != "custom-lease" {
		t.Errorf("LeaderElectionID = %q, want custom-lease", opts.LeaderElectionID)
	}
	if !opts.LeaderElection {
		t.Errorf("LeaderElection should default to true")
	}
	if got := *opts.LeaseDuration; got != 25*time.Second {
		t.Errorf("LeaseDuration = %s, want 25s", got)
	}
	if got := *opts.RenewDeadline; got != 15*time.Second {
		t.Errorf("RenewDeadline = %s, want 15s", got)
	}
	if _, ok := opts.Cache.DefaultNamespaces["ns-a"]; !ok {
		t.Errorf("Cache.DefaultNamespaces missing ns-a: %v", opts.Cache.DefaultNamespaces)
	}
}

func TestBuildManagerOptions_RejectsInvalidSelector(t *testing.T) {
	cfg := operatorconfig.Default()
	cfg.CacheManagedBySelector = "!!not a selector"

	_, err := buildManagerOptions(cfg, flags{})
	if err == nil {
		t.Fatal("expected error for invalid label selector")
	}
}

// --- buildTLSOpts --------------------------------------------------------

func TestBuildTLSOpts_DisablesHTTP2ByDefault(t *testing.T) {
	opts := buildTLSOpts(flags{enableHTTP2: false})
	if len(opts) != 2 {
		t.Fatalf("expected 2 TLS options (MinVersion pin + HTTP/2 disable); got %d", len(opts))
	}
	tc := &tls.Config{}
	for _, o := range opts {
		o(tc)
	}
	if len(tc.NextProtos) != 1 || tc.NextProtos[0] != "http/1.1" {
		t.Errorf("HTTP/2-disable did not restrict NextProtos; got %v", tc.NextProtos)
	}
	if tc.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %#x, want %#x (TLS 1.3)", tc.MinVersion, tls.VersionTLS13)
	}
}

func TestBuildTLSOpts_HTTP2EnabledStillPinsMinVersion(t *testing.T) {
	opts := buildTLSOpts(flags{enableHTTP2: true})
	if len(opts) != 1 {
		t.Fatalf("expected 1 TLS option (MinVersion pin) when HTTP/2 is on; got %d", len(opts))
	}
	tc := &tls.Config{}
	opts[0](tc)
	if tc.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %#x, want %#x (TLS 1.3)", tc.MinVersion, tls.VersionTLS13)
	}
	if len(tc.NextProtos) != 0 {
		t.Errorf("HTTP/2-enabled path must not restrict NextProtos; got %v", tc.NextProtos)
	}
}

// --- parseTestOverrides ---------------------------------------------------

func TestParseTestOverrides_FlagDisabledIgnoresEnv(t *testing.T) {
	t.Setenv("VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "5")
	t.Setenv("VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "1")
	t.Setenv("VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "10")
	got, err := parseTestOverrides(false)
	if err != nil {
		t.Fatalf("parseTestOverrides(false) returned err: %v", err)
	}
	if got.LossThreshold != 0 || got.RecoveryPolls != 0 || got.Phase11Timeout != 0 {
		t.Errorf("flag=false but env applied: %+v", got)
	}
}

func TestParseTestOverrides_FlagEnabledAppliesEnv(t *testing.T) {
	t.Setenv("VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "5")
	t.Setenv("VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "1")
	t.Setenv("VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "10")
	got, err := parseTestOverrides(true)
	if err != nil {
		t.Fatalf("parseTestOverrides(true) returned err: %v", err)
	}
	if got.LossThreshold != 5*time.Second {
		t.Errorf("LossThreshold: got %s, want 5s", got.LossThreshold)
	}
	if got.RecoveryPolls != 1 {
		t.Errorf("RecoveryPolls: got %d, want 1", got.RecoveryPolls)
	}
	if got.Phase11Timeout != 10*time.Second {
		t.Errorf("Phase11Timeout: got %s, want 10s", got.Phase11Timeout)
	}
}

func TestParseTestOverrides_FlagEnabledUnsetEnvLeavesZero(t *testing.T) {
	// Use Setenv("", "") to explicitly clear; t.Setenv with empty
	// string is rejected, so use Unsetenv via os.
	t.Setenv("VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "")
	t.Setenv("VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "")
	t.Setenv("VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "")
	got, err := parseTestOverrides(true)
	if err != nil {
		t.Fatalf("parseTestOverrides(true) returned err: %v", err)
	}
	if got.LossThreshold != 0 || got.RecoveryPolls != 0 || got.Phase11Timeout != 0 {
		t.Errorf("unset env should leave fields zero: %+v", got)
	}
}

func TestParseTestOverrides_RejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"loss-suppression non-numeric", "VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "abc"},
		{"loss-suppression zero", "VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "0"},
		{"loss-suppression negative", "VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "-1"},
		{"recovery-polls non-numeric", "VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "abc"},
		{"recovery-polls zero", "VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "0"},
		{"phase11 non-numeric", "VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "abc"},
		{"phase11 zero", "VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC", "")
			t.Setenv("VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS", "")
			t.Setenv("VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC", "")
			t.Setenv(tc.key, tc.val)
			_, err := parseTestOverrides(true)
			if err == nil {
				t.Fatalf("expected error for %s=%q, got nil", tc.key, tc.val)
			}
		})
	}
}
