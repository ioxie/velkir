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

// Package config loads the operator's YAML configuration from a mounted
// ConfigMap. The canonical path is /etc/velkir/config.yaml; flags
// on cmd/main.go can override individual fields after load.
//
// The ConfigMap should be projected read-only (the Helm chart does this by
// default). The operator restarts on configuration changes; Validate runs at
// startup to surface bad input as a fast crash-loop rather than a silently
// over-scoped reconciler.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// reservedNamespacePrefix is the prefix Kubernetes itself reserves for system
// namespaces (kube-system, kube-public, kube-node-lease, and any future
// system namespace the project introduces). The operator refuses to watch
// any namespace under this prefix — listing one would let a ConfigMap edit
// escalate the operator's blast radius into the control plane.
const reservedNamespacePrefix = "kube-"

// DefaultPath is the canonical on-disk location for the operator config. The
// Helm chart mounts a ConfigMap here via the operator Deployment spec.
const DefaultPath = "/etc/velkir/config.yaml"

// DefaultManagedByLabel is the value of the app.kubernetes.io/managed-by label
// the operator stamps on every resource it owns, and the selector it uses when
// filtering cached informers for those owned types.
const DefaultManagedByLabel = "app.kubernetes.io/managed-by=velkir"

// Config is the operator-wide runtime configuration. Every field has a
// production-appropriate default; Load populates unset fields.
type Config struct {
	// WatchNamespaces restricts the set of namespaces the operator observes.
	// A nil or empty slice means "watch cluster-wide" (default).
	WatchNamespaces []string `json:"watchNamespaces,omitempty"`

	// LeaderElection controls the Kubernetes Lease-based leader election path.
	LeaderElection LeaderElectionConfig `json:"leaderElection,omitempty"`

	// Metrics controls the Prometheus metrics server.
	Metrics MetricsConfig `json:"metrics,omitempty"`

	// HealthProbeBindAddress is the listen address for /healthz and /readyz.
	HealthProbeBindAddress string `json:"healthProbeBindAddress,omitempty"`

	// Webhook controls the admission webhook server.
	Webhook WebhookConfig `json:"webhook,omitempty"`

	// CacheManagedBySelector is the label selector applied to informers for
	// the operator-owned resource types (Secret, ConfigMap, Pod, Service,
	// StatefulSet, PodDisruptionBudget). Does NOT apply to the Valkey CR
	// itself, which users author without operator labels.
	CacheManagedBySelector string `json:"cacheManagedBySelector,omitempty"`

	// MaxConcurrentReconciles caps Valkey-reconciler parallelism.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`
}

// LeaderElectionConfig mirrors controller-runtime manager.Options fields with
// the subset we want exposed for operators.
type LeaderElectionConfig struct {
	// Enabled toggles leader election. nil is treated as true.
	Enabled *bool `json:"enabled,omitempty"`

	// Namespace holds the Lease. Empty string falls back to the pod's
	// own namespace (controller-runtime behaviour).
	Namespace string `json:"namespace,omitempty"`

	// ID is the lock name. Deterministic so operator restarts pick up the
	// same Lease instead of creating a second one.
	ID string `json:"id,omitempty"`

	LeaseDuration metav1.Duration `json:"leaseDuration,omitempty"`
	RenewDeadline metav1.Duration `json:"renewDeadline,omitempty"`
	RetryPeriod   metav1.Duration `json:"retryPeriod,omitempty"`

	// ReleaseOnCancel releases the Lease immediately on graceful shutdown so
	// standbys take over in <1s instead of waiting for LeaseDuration. Safe
	// because the binary exits promptly after Start() returns.
	ReleaseOnCancel *bool `json:"releaseOnCancel,omitempty"`
}

// MetricsConfig configures the Prometheus metrics server.
type MetricsConfig struct {
	BindAddress string `json:"bindAddress,omitempty"`
	Secure      *bool  `json:"secure,omitempty"`
}

// WebhookConfig configures the admission webhook server.
type WebhookConfig struct {
	Port    int    `json:"port,omitempty"`
	CertDir string `json:"certDir,omitempty"`
}

// Default returns a fully-populated Config with production-appropriate values.
// Callers should use Default then mutate, or Load which also defaults.
func Default() *Config {
	return &Config{
		LeaderElection: LeaderElectionConfig{
			Enabled: new(true),
			ID:      "velkir.ioxie.dev",
			// Latency-tolerant defaults: a transient API-server slowdown
			// must not make the manager miss its renew deadline and exit
			// (on a single-replica install that restart is pure downtime,
			// no failover benefit). The renew window here is 10s of
			// headroom; chart values can tighten these where faster
			// standby takeover outweighs blip-tolerance. Invariant
			// enforced by Validate: retryPeriod < renewDeadline < leaseDuration.
			LeaseDuration:   metav1.Duration{Duration: 30 * time.Second},
			RenewDeadline:   metav1.Duration{Duration: 20 * time.Second},
			RetryPeriod:     metav1.Duration{Duration: 4 * time.Second},
			ReleaseOnCancel: new(true),
		},
		Metrics: MetricsConfig{
			BindAddress: ":8443",
			Secure:      new(true),
		},
		HealthProbeBindAddress: ":8081",
		Webhook: WebhookConfig{
			Port:    9443,
			CertDir: "/tmp/k8s-webhook-server/serving-certs",
		},
		CacheManagedBySelector:  DefaultManagedByLabel,
		MaxConcurrentReconciles: 4,
	}
}

// Load reads the YAML configuration at path, overlays it onto Default(),
// validates the result, and returns the merged Config. A missing file is NOT
// an error — callers that mount the ConfigMap optionally rely on that path.
// An empty path returns Default().
//
// Load validates the on-disk content. Callers that layer additional mutations
// on top (e.g. cmd/main.go's flag overlay) must re-invoke Validate themselves
// because the new values may violate constraints the on-disk file satisfied.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if err := decodeInto(f, cfg); err != nil {
		return nil, fmt.Errorf("config: decode %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	return cfg, nil
}

func decodeInto(r io.Reader, cfg *Config) error {
	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	return yaml.UnmarshalStrict(buf, cfg)
}

// Validate surfaces configuration errors before the manager starts. Returns
// the first error encountered; callers should surface it as a fatal.
func (c *Config) Validate() error {
	le := c.LeaderElection
	if le.LeaseDuration.Duration <= 0 || le.RenewDeadline.Duration <= 0 || le.RetryPeriod.Duration <= 0 {
		return fmt.Errorf("leaderElection durations must be positive (got lease=%s renew=%s retry=%s)",
			le.LeaseDuration.Duration, le.RenewDeadline.Duration, le.RetryPeriod.Duration)
	}
	if le.RenewDeadline.Duration >= le.LeaseDuration.Duration {
		return fmt.Errorf("leaderElection.renewDeadline (%s) must be less than leaseDuration (%s)",
			le.RenewDeadline.Duration, le.LeaseDuration.Duration)
	}
	if le.RetryPeriod.Duration >= le.RenewDeadline.Duration {
		return fmt.Errorf("leaderElection.retryPeriod (%s) must be less than renewDeadline (%s)",
			le.RetryPeriod.Duration, le.RenewDeadline.Duration)
	}
	if c.MaxConcurrentReconciles < 1 {
		return fmt.Errorf("maxConcurrentReconciles must be >= 1 (got %d)", c.MaxConcurrentReconciles)
	}
	if c.CacheManagedBySelector == "" {
		return fmt.Errorf("cacheManagedBySelector must not be empty")
	}
	if err := validateWatchNamespaces(c.WatchNamespaces); err != nil {
		return err
	}
	return nil
}

// validateWatchNamespaces rejects empty entries, duplicates, and any namespace
// reserved by Kubernetes for control-plane use. Cluster-wide watch (nil/empty
// slice) bypasses this check by design.
func validateWatchNamespaces(nss []string) error {
	seen := make(map[string]struct{}, len(nss))
	for i, ns := range nss {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("watchNamespaces[%d] must not be empty", i)
		}
		if ns != strings.TrimSpace(ns) {
			return fmt.Errorf("watchNamespaces[%d] %q has surrounding whitespace", i, ns)
		}
		if strings.HasPrefix(ns, reservedNamespacePrefix) {
			return fmt.Errorf("watchNamespaces[%d] %q is reserved (the %q prefix is Kubernetes-reserved)",
				i, ns, reservedNamespacePrefix)
		}
		if _, dup := seen[ns]; dup {
			return fmt.Errorf("watchNamespaces[%d] %q is duplicated", i, ns)
		}
		seen[ns] = struct{}{}
	}
	return nil
}

// LeaderElectionEnabled returns the effective toggle, defaulting to true when
// the pointer field is unset.
func (c *Config) LeaderElectionEnabled() bool {
	if c.LeaderElection.Enabled == nil {
		return true
	}
	return *c.LeaderElection.Enabled
}

// LeaderElectionReleaseOnCancel returns the effective toggle, defaulting to
// true when the pointer field is unset.
func (c *Config) LeaderElectionReleaseOnCancel() bool {
	if c.LeaderElection.ReleaseOnCancel == nil {
		return true
	}
	return *c.LeaderElection.ReleaseOnCancel
}

// MetricsSecure returns the effective metrics-secure toggle.
func (c *Config) MetricsSecure() bool {
	if c.Metrics.Secure == nil {
		return true
	}
	return *c.Metrics.Secure
}
