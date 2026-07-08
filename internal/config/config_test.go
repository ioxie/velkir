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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()
	if !c.LeaderElectionEnabled() {
		t.Errorf("expected leader election enabled by default")
	}
	if !c.LeaderElectionReleaseOnCancel() {
		t.Errorf("expected releaseOnCancel default true")
	}
	if !c.MetricsSecure() {
		t.Errorf("expected metrics secure default true")
	}
	if got := c.LeaderElection.LeaseDuration.Duration; got != 30*time.Second {
		t.Errorf("LeaseDuration default = %s, want 30s", got)
	}
	if got := c.LeaderElection.RenewDeadline.Duration; got != 20*time.Second {
		t.Errorf("RenewDeadline default = %s, want 20s", got)
	}
	if got := c.LeaderElection.RetryPeriod.Duration; got != 4*time.Second {
		t.Errorf("RetryPeriod default = %s, want 4s", got)
	}
	if got := c.MaxConcurrentReconciles; got != 4 {
		t.Errorf("MaxConcurrentReconciles default = %d, want 4", got)
	}
	if got := c.CacheManagedBySelector; got != DefaultManagedByLabel {
		t.Errorf("CacheManagedBySelector default = %q, want %q", got, DefaultManagedByLabel)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Default failed validation: %v", err)
	}
}

func TestLoadMissingFileReturnsDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if cfg.LeaderElection.ID != "velkir.ioxie.dev" {
		t.Errorf("missing file should have fallen back to defaults; got id %q", cfg.LeaderElection.ID)
	}
}

func TestLoadEmptyPath(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if cfg == nil {
		t.Fatal("Load empty returned nil config")
	}
}

func TestLoadPartialOverlaysDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
leaderElection:
  namespace: custom-ns
  leaseDuration: 45s
metrics:
  bindAddress: ":9000"
watchNamespaces: ["production", "staging"]
maxConcurrentReconciles: 8
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Overridden fields.
	if got := cfg.LeaderElection.Namespace; got != "custom-ns" {
		t.Errorf("Namespace = %q, want custom-ns", got)
	}
	if got := cfg.LeaderElection.LeaseDuration.Duration; got != 45*time.Second {
		t.Errorf("LeaseDuration = %s, want 45s", got)
	}
	if got := cfg.Metrics.BindAddress; got != ":9000" {
		t.Errorf("Metrics.BindAddress = %q, want :9000", got)
	}
	if got := len(cfg.WatchNamespaces); got != 2 {
		t.Errorf("WatchNamespaces len = %d, want 2", got)
	}
	if got := cfg.MaxConcurrentReconciles; got != 8 {
		t.Errorf("MaxConcurrentReconciles = %d, want 8", got)
	}
	// Non-overridden fields keep defaults.
	if got := cfg.LeaderElection.ID; got != "velkir.ioxie.dev" {
		t.Errorf("ID = %q, want default velkir.ioxie.dev", got)
	}
	if got := cfg.Webhook.Port; got != 9443 {
		t.Errorf("Webhook.Port = %d, want default 9443", got)
	}
	if !cfg.LeaderElectionEnabled() {
		t.Errorf("LeaderElection.Enabled lost its default")
	}
}

func TestLoadUnknownFieldRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("bogusKey: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error on unknown field")
	}
	if !strings.Contains(err.Error(), "bogusKey") && !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("error should mention the unknown field; got: %v", err)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr string
	}{
		{
			"negative lease",
			func(c *Config) { c.LeaderElection.LeaseDuration.Duration = -1 },
			"durations must be positive",
		},
		{
			"renew >= lease",
			func(c *Config) {
				c.LeaderElection.LeaseDuration.Duration = 10 * time.Second
				c.LeaderElection.RenewDeadline.Duration = 10 * time.Second
			},
			"renewDeadline",
		},
		{
			"retry >= renew",
			func(c *Config) {
				c.LeaderElection.RetryPeriod.Duration = c.LeaderElection.RenewDeadline.Duration
			},
			"retryPeriod",
		},
		{
			"zero reconciles",
			func(c *Config) { c.MaxConcurrentReconciles = 0 },
			"maxConcurrentReconciles",
		},
		{
			"empty selector",
			func(c *Config) { c.CacheManagedBySelector = "" },
			"cacheManagedBySelector",
		},
		{
			"watch namespace kube-system",
			func(c *Config) { c.WatchNamespaces = []string{"production", "kube-system"} },
			`"kube-system" is reserved`,
		},
		{
			"watch namespace kube-public",
			func(c *Config) { c.WatchNamespaces = []string{"kube-public"} },
			`"kube-public" is reserved`,
		},
		{
			"watch namespace kube-node-lease",
			func(c *Config) { c.WatchNamespaces = []string{"kube-node-lease"} },
			`"kube-node-lease" is reserved`,
		},
		{
			"watch namespace kube- prefix future",
			func(c *Config) { c.WatchNamespaces = []string{"kube-flannel"} },
			`"kube-flannel" is reserved`,
		},
		{
			"watch namespace empty entry",
			func(c *Config) { c.WatchNamespaces = []string{"production", ""} },
			"must not be empty",
		},
		{
			"watch namespace whitespace entry",
			func(c *Config) { c.WatchNamespaces = []string{"   "} },
			"must not be empty",
		},
		{
			"watch namespace surrounding whitespace",
			func(c *Config) { c.WatchNamespaces = []string{" production"} },
			"surrounding whitespace",
		},
		{
			"watch namespace duplicate",
			func(c *Config) { c.WatchNamespaces = []string{"production", "production"} },
			"duplicated",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q should contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateWatchNamespacesAccepts(t *testing.T) {
	tests := []struct {
		name string
		nss  []string
	}{
		{"nil cluster-wide", nil},
		{"empty slice cluster-wide", []string{}},
		{"single tenant ns", []string{"production"}},
		{"multiple tenant ns", []string{"production", "staging", "qa"}},
		{"hyphenated names", []string{"team-a", "team-b-prod"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			c.WatchNamespaces = tc.nss
			if err := c.Validate(); err != nil {
				t.Errorf("Validate(%v) = %v, want nil", tc.nss, err)
			}
		})
	}
}

func TestLoadRejectsReservedWatchNamespace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `watchNamespaces: ["production", "kube-system"]` + "\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error on reserved namespace")
	}
	if !strings.Contains(err.Error(), `"kube-system" is reserved`) {
		t.Errorf("error %q should mention kube-system reserved", err)
	}
}
