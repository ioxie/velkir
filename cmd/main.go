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
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatorconfig "github.com/ioxie/velkir/internal/config"
	"github.com/ioxie/velkir/internal/controller"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/logging"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/webhook/dynauth"
	webhookv1beta1 "github.com/ioxie/velkir/internal/webhook/v1beta1"
	// +kubebuilder:scaffold:imports
)

// envPodNamespace is the env var (set via Downward API in the chart) that
// tells the operator its own namespace. Required when --enable-dynamic-
// authority is set; the Authority + Injector both work in this namespace.
const envPodNamespace = "POD_NAMESPACE"

// defaultWebhookServiceName / defaultMetricsServiceName are the Service
// short names the cert-bootstrap leaf SAN lists are built from. Overridable
// via --webhook-service-name / --metrics-service-name for chart releases
// that change the Service naming (e.g. helm release-name overrides).
const (
	defaultWebhookServiceName = "velkir-webhook"
	defaultMetricsServiceName = "velkir-metrics"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	// version is the operator's release version. The default "dev" is
	// overridden at build time via `-ldflags "-X main.version=<tag>"`
	// (see Dockerfile + release.yaml VERSION build-arg). Logged once at
	// startup so cluster operators can correlate behaviour to a specific
	// release without re-scraping the running image's labels.
	version = "dev"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(valkeyv1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// flags captures the CLI surface. Only flags the user explicitly sets on the
// command line overlay the on-disk config (see setExplicitly). zap's
// --zap-* flag family is bound to the same FlagSet so operators can tune
// log encoding and level without a rebuild.
type flags struct {
	configPath             string
	metricsAddr            string
	metricsSecure          bool
	probeAddr              string
	leaderElect            bool
	webhookCertPath        string
	webhookCertName        string
	webhookCertKey         string
	webhookServiceName     string
	enableDynamicAuthority bool
	metricsCertPath        string
	metricsCertName        string
	metricsCertKey         string
	metricsServiceName     string
	enableHTTP2            bool
	// bootstrapOnly switches main() to "run the dynauth bootstrap and
	// exit" mode used by the chart's init container. See the
	// --bootstrap-only flag wireup in parseFlags for the full
	// rationale.
	bootstrapOnly bool

	// allowTestOverrides opts the operator into reading the
	// VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC /
	// VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS /
	// VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC env vars and applying them
	// to the reconciler's suppression-gate floors and Phase 11
	// orchestration deadline. Off by default — prod operators must
	// never override these load-bearing safety floors. The flag exists
	// so shared-cluster e2e scenarios can tighten the floors for the
	// gate-entry / gate-exit specs where the production floor
	// dominates scenario wall time.
	allowTestOverrides bool

	// setExplicitly is populated by flag.Visit after flag.Parse. Keys are
	// flag names; presence means the user set the flag on the command line.
	// Needed because Go's `flag` package can't distinguish an unset bool
	// from an explicitly-false bool.
	setExplicitly map[string]bool
}

func main() {
	zapOpts := zap.Options{}
	f := parseFlags(&zapOpts)

	// logging.New wraps the controller-runtime zap logger with a
	// redacting Core driven by logging.DefaultRegistry. Reconcilers
	// that read Secret values must call logging.DefaultRegistry.Register
	// on the value (and Forget on rotation/CR delete) so the redactor
	// scrubs subsequent log emissions across every derived logger.
	ctrl.SetLogger(logging.New(zapOpts))

	setupLog.Info("starting velkir", "version", version)

	// Bootstrap-only mode runs the dynauth init path and exits.
	// Branches before config load + manager setup because none of
	// that is needed for the mint-Secrets pass — the bootstrap path
	// just needs --webhook-service-name + --metrics-service-name +
	// POD_NAMESPACE. Returning here keeps the init container's
	// blast radius minimal: no controllers register, no informer
	// cache spins up, no webhook server.
	if f.bootstrapOnly {
		if err := runBootstrap(f); err != nil {
			setupLog.Error(err, "webhook cert bootstrap failed")
			os.Exit(1)
		}
		setupLog.Info("webhook cert bootstrap complete")
		return
	}

	cfg, err := operatorconfig.Load(f.configPath)
	if err != nil {
		setupLog.Error(err, "loading operator config", "path", f.configPath)
		os.Exit(1)
	}
	f.applyTo(cfg)
	// Load validates the on-disk content; we validate again after the flag
	// overlay because --metrics-bind-address etc. can violate constraints
	// that the on-disk file satisfied.
	if err := cfg.Validate(); err != nil {
		setupLog.Error(err, "invalid operator config after flag overlay")
		os.Exit(1)
	}
	setupLog.Info("operator config loaded",
		"watchNamespaces", cfg.WatchNamespaces,
		"leaderElection", cfg.LeaderElectionEnabled(),
		"leaderElectionID", cfg.LeaderElection.ID,
		"metricsBindAddress", cfg.Metrics.BindAddress,
		"webhookPort", cfg.Webhook.Port,
		"maxConcurrentReconciles", cfg.MaxConcurrentReconciles,
	)

	// Register operator metrics with the controller-runtime shared registry
	// before the manager wires up /metrics. MustRegister panics on duplicate
	// registration, which surfaces init-order bugs at startup rather than
	// first-scrape.
	operatormetrics.Register()

	mgrOpts, err := buildManagerOptions(cfg, f)
	if err != nil {
		setupLog.Error(err, "building manager options")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "starting manager")
		os.Exit(1)
	}

	sentinelMgr := setupSentinelObserver(mgr, f)

	// Shared AuthSecretShortPassword reporter: both the reconciler and
	// the sentinel startup safety net resolve auth Secrets, and a CR
	// can be touched by both within seconds of leader-acquire. Sharing
	// one reporter means the dedup is correct across components — the
	// per-CR-Secret tuple emits at most once even when both paths
	// observe the same short password.
	shortAuthPasswordReporter := events.NewShortAuthPasswordReporter(mgr.GetEventRecorder("valkey-controller"))

	registerSentinelStartupReset(mgr, sentinelMgr, shortAuthPasswordReporter)

	valkeyReconciler := setupValkeyReconciler(mgr, cfg, f, sentinelMgr, shortAuthPasswordReporter)

	registerStaleTrackerPruner(mgr, valkeyReconciler)

	if err := webhookv1beta1.SetupValkeyWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "registering webhook", "webhook", "Valkey")
		os.Exit(1)
	}

	registerExporterWatcher(mgr)

	// Cert provisioning runs after the webhook is registered so the chart's
	// projected Secret + the registered admission endpoint are wired in
	// the same order on every replica.
	if err := setupWebhookCertProvisioning(mgr, f); err != nil {
		setupLog.Error(err, "wiring webhook cert provisioning")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding readyz")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "running manager")
		os.Exit(1)
	}
}

// setupSentinelObserver constructs and registers the leader-gated
// sentinel observer manager — a singleton that owns per-CR PSUBSCRIBE
// goroutines + 10s pull tick. Constructed before the reconciler so the
// reconciler can hold a reference and consume Snapshot in Phase 7.
// Note: a POLL_SEC override below the production default does not
// tighten the reconciler's suppressed-gate requeue pace
// (quorumSuppressedRequeue stays at the const) — gate-exit latency in
// such runs is bounded by that const, not the shortened poll cadence.
func setupSentinelObserver(mgr ctrl.Manager, f flags) *sentinel.Manager {
	sentinelOpts := sentinel.Options{}
	if f.allowTestOverrides {
		if v := os.Getenv("VALKEY_OPERATOR_SENTINEL_OBSERVER_POLL_SEC"); v != "" {
			secs, perr := strconv.Atoi(v)
			if perr == nil && secs <= 0 {
				perr = fmt.Errorf("value must be positive, got %d", secs)
			}
			if perr != nil {
				setupLog.Error(perr, "VALKEY_OPERATOR_SENTINEL_OBSERVER_POLL_SEC: must be a positive integer", "value", v)
				os.Exit(1)
			}
			sentinelOpts.PollInterval = time.Duration(secs) * time.Second
		}
	}
	sentinelMgr := sentinel.NewManager(
		mgr.GetEventRecorder(sentinel.ManagerName),
		sentinelOpts,
	)
	if err := mgr.Add(sentinelMgr); err != nil {
		setupLog.Error(err, "registering sentinel observer manager")
		os.Exit(1)
	}
	return sentinelMgr
}

// registerSentinelStartupReset wires the leader-gated one-shot sentinel
// startup safety net: it lists every Valkey CR with mode=sentinel, lists
// each CR's sentinel pods, and fires `SENTINEL RESET *` against every
// endpoint to clear stale state the operator may have missed while
// not-leader. Emits one InitialSentinelReset event per CR (handled inside
// the manager's RunInitialReset). No-op for clusters with zero
// sentinel-mode CRs OR sentinel-mode CRs whose sentinel STS hasn't been
// created yet.
func registerSentinelStartupReset(
	mgr ctrl.Manager,
	sentinelMgr *sentinel.Manager,
	reporter *events.ShortAuthPasswordReporter,
) {
	if err := mgr.Add(&controller.SentinelStartupReset{
		Client:                    mgr.GetClient(),
		APIReader:                 mgr.GetAPIReader(),
		SentinelObserver:          sentinelMgr,
		ShortAuthPasswordReporter: reporter,
	}); err != nil {
		setupLog.Error(err, "registering sentinel startup safety net")
		os.Exit(1)
	}
}

// setupValkeyReconciler parses the test-override tunables, constructs the
// Valkey reconciler, wires the deferral gate, and registers it. The
// deferral predicate is set BEFORE SetupWithManager so it is live by the
// time the first reconcile runs.
func setupValkeyReconciler(
	mgr ctrl.Manager,
	cfg *operatorconfig.Config,
	f flags,
	sentinelMgr *sentinel.Manager,
	reporter *events.ShortAuthPasswordReporter,
) *controller.ValkeyReconciler {
	tunables, err := parseTestOverrides(f.allowTestOverrides)
	if err != nil {
		setupLog.Error(err, "parsing --allow-test-overrides env vars")
		os.Exit(1)
	}
	if f.allowTestOverrides {
		setupLog.Info("test overrides enabled — load-bearing safety floors may be relaxed; do NOT use in production",
			"quorumLossSuppressionSec", int(tunables.LossThreshold/time.Second),
			"quorumRecoveryHysteresisPolls", tunables.RecoveryPolls,
			"phase11TimeoutSec", int(tunables.Phase11Timeout/time.Second))
	}

	valkeyReconciler := &controller.ValkeyReconciler{
		Client:                    mgr.GetClient(),
		APIReader:                 mgr.GetAPIReader(),
		Scheme:                    mgr.GetScheme(),
		MaxConcurrentReconciles:   cfg.MaxConcurrentReconciles,
		WatchNamespaces:           cfg.WatchNamespaces,
		SentinelObserver:          sentinelMgr,
		FSM:                       orchestration.NewMachine(),
		ShortAuthPasswordReporter: reporter,
		Tunables:                  tunables,
		// Deprecation observer. The production registry
		// (controller.ProductionDeprecations) is empty today — v1beta1
		// is additive-only since v0.1.0 — so the per-reconcile sweep
		// is a no-op until the first field deprecation lands and a
		// FieldDeprecation entry is appended to the registry.
		Deprecator:   events.NewDeprecator(mgr.GetEventRecorder("valkey-controller")),
		Deprecations: controller.ProductionDeprecations,
		// Best-practice deviation observer — re-surfaces the
		// validating webhook's ephemeral admission warnings as durable
		// Warning Events, deduplicated per (CR, reason, field) per
		// process lifetime.
		DeviationEmitter: events.NewDeviationEmitter(mgr.GetEventRecorder("valkey-controller")),
	}
	// Wire the deferral gate: the reconciler owns both the per-CR
	// sustained-NOQUORUM suppression tracker AND the per-CR last-
	// observed FSM state. The composed predicate defers the operator's
	// stranded-sentinel REMOVE + MONITOR surgery when EITHER is active —
	// sustained quorum loss OR a sentinel-driven failover mid-flight —
	// so the surgery never races the sentinel's own config-epoch
	// propagation; the next reconcile retries once the gate clears.
	// Predicate must be set BEFORE SetupWithManager so it is live by
	// the time the first reconcile runs.
	sentinelMgr.SetDeferralPredicate(valkeyReconciler.DeferralPredicate)
	if err := valkeyReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "registering controller", "controller", "Valkey")
		os.Exit(1)
	}
	return valkeyReconciler
}

// registerStaleTrackerPruner wires the leader-gated periodic sweep that
// evicts per-CR tracker entries pinned to a vanished CR. Per-CR tracker
// entries in the reconciler's sync.Map fields are cleared on the observed
// CR-delete code path; a missed delete (operator restart between Get and
// the cleanup write, watch interruption that swallows the delete event)
// leaves entries pinned to a vanished CR. The sweep lists live CRs
// cluster-wide and evicts entries whose key is no longer present.
func registerStaleTrackerPruner(mgr ctrl.Manager, reconciler *controller.ValkeyReconciler) {
	if err := mgr.Add(&controller.StaleTrackerPruner{
		Client:     mgr.GetClient(),
		Reconciler: reconciler,
		Interval:   controller.DefaultStaleTrackerPruneInterval,
	}); err != nil {
		setupLog.Error(err, "registering stale tracker pruner")
		os.Exit(1)
	}
}

// registerExporterWatcher wires the leader-gated periodic sweep that
// maintains the per-pod valkey_exporter_sidecar_up gauge. Reads pod.Status
// from the shared cache (already populated by cacheOwnedTypes), so adds no
// new apiserver pressure beyond the existing informer.
func registerExporterWatcher(mgr ctrl.Manager) {
	if err := mgr.Add(&operatormetrics.ExporterWatcher{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("exporter-watcher"),
	}); err != nil {
		setupLog.Error(err, "registering exporter watcher")
		os.Exit(1)
	}
}

// parseFlags registers the flag surface and populates the returned flags.
// The *zap.Options pointer is threaded through so zap's --zap-* flags bind
// onto the same FlagSet the rest of the operator flags use.
func parseFlags(zapOpts *zap.Options) flags {
	var f flags
	flag.StringVar(&f.configPath, "config", operatorconfig.DefaultPath,
		"Path to the operator config YAML. Missing file falls back to built-in defaults.")
	flag.StringVar(&f.metricsAddr, "metrics-bind-address", "",
		"Override metrics.bindAddress from config. Use :8080 for HTTP, :8443 for HTTPS, or 0 to disable.")
	flag.BoolVar(&f.metricsSecure, "metrics-secure", true,
		"Serve metrics over HTTPS (default true). Use --metrics-secure=false for HTTP.")
	flag.StringVar(&f.probeAddr, "health-probe-bind-address", "",
		"Override healthProbeBindAddress from config.")
	flag.BoolVar(&f.leaderElect, "leader-elect", false,
		"Enable leader election. Omitted = take value from config (default true).")
	flag.StringVar(&f.webhookCertPath, "webhook-cert-path", "", "Directory containing the webhook certificate.")
	flag.StringVar(&f.webhookCertName, "webhook-cert-name", "tls.crt", "Webhook certificate filename.")
	flag.StringVar(&f.webhookCertKey, "webhook-cert-key", "tls.key", "Webhook key filename.")
	flag.StringVar(&f.webhookServiceName, "webhook-service-name", defaultWebhookServiceName,
		"Service name the webhook leaf cert's DNS SANs are built from. Must match the chart's webhook Service.")
	flag.BoolVar(&f.enableDynamicAuthority, "enable-dynamic-authority", true,
		"Enable the in-process CA + leaf cert lifecycle. "+
			"Set to false when cert-manager owns the webhook cert "+
			"(chart toggle webhook.certManager.enabled=true).")
	flag.StringVar(&f.metricsCertPath, "metrics-cert-path", "", "Directory containing the metrics server certificate.")
	flag.StringVar(&f.metricsCertName, "metrics-cert-name", "tls.crt", "Metrics server certificate filename.")
	flag.StringVar(&f.metricsCertKey, "metrics-cert-key", "tls.key", "Metrics server key filename.")
	flag.StringVar(&f.metricsServiceName, "metrics-service-name", defaultMetricsServiceName,
		"Service name the metrics leaf cert's DNS SANs are built from. Must match the chart's metrics Service.")
	flag.BoolVar(&f.enableHTTP2, "enable-http2", false, "Enable HTTP/2 on metrics + webhook servers.")
	flag.BoolVar(&f.bootstrapOnly, "bootstrap-only", false,
		"Run the dynauth webhook-cert bootstrap path (mint initial CA + leaf Secrets if absent, "+
			"wait for kubelet to project them onto the webhook + metrics cert volumes) and exit. "+
			"Used by the chart's init container to side-step the chicken-and-egg where the webhook "+
			"server requires the cert file at startup but the periodic Authority loop cannot run "+
			"until the manager has started. No-op when --enable-dynamic-authority=false or when "+
			"cert-manager is the cert provisioner.")
	flag.BoolVar(&f.allowTestOverrides, "allow-test-overrides", false,
		"Read the VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC, "+
			"VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS, and "+
			"VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC env vars and override the "+
			"reconciler's suppression-gate floors and Phase 11 deadline. "+
			"Off by default — production operators must never set this flag. "+
			"Exists for shared-cluster e2e scenarios where the safety floor "+
			"dominates spec wall time.")

	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	f.setExplicitly = map[string]bool{}
	flag.Visit(func(fl *flag.Flag) { f.setExplicitly[fl.Name] = true })
	return f
}

// applyTo overlays explicitly-set flag values onto cfg. Flags the user
// omitted leave the config value untouched, so flag defaults never stomp
// on values sourced from the on-disk config.
func (f flags) applyTo(cfg *operatorconfig.Config) {
	if f.setExplicitly["metrics-bind-address"] {
		cfg.Metrics.BindAddress = f.metricsAddr
	}
	if f.setExplicitly["metrics-secure"] {
		cfg.Metrics.Secure = new(f.metricsSecure)
	}
	if f.setExplicitly["health-probe-bind-address"] {
		cfg.HealthProbeBindAddress = f.probeAddr
	}
	if f.setExplicitly["leader-elect"] {
		cfg.LeaderElection.Enabled = new(f.leaderElect)
	}
}

// cacheOwnedTypes is the single source of truth for which operator-owned
// types the informer cache filters by label. Both buildCacheOptions and the
// unit test consume this slice — adding a type here lands it in both places.
//
// The root Valkey CR is authored by users and must NOT appear here (or
// r.Get on any Valkey would miss). This list is for operator-created
// objects only.
//
// Load-bearing invariant: every resource the operator creates MUST carry
// the app.kubernetes.io/managed-by=velkir label. If that label is
// stripped (human edit, mutating admission from another operator, etc.)
// the object becomes invisible to the cache even though it still exists
// in etcd. Don't remove the label in any reconciler output.
var cacheOwnedTypes = []client.Object{
	&corev1.Secret{},
	&corev1.ConfigMap{},
	&corev1.Pod{},
	&corev1.Service{},
	&appsv1.StatefulSet{},
	&policyv1.PodDisruptionBudget{},
}

// buildTLSOpts returns the TLS configuration applied to metrics + webhook
// servers. HTTP/2 is disabled by default to sidestep CVE-2023-44487 and
// CVE-2023-39325 (HTTP/2 Rapid Reset, Stream Cancellation). Both branches
// pin MinVersion=TLS 1.3 explicitly so a future Go or controller-runtime
// release that lowers the negotiated minimum can't silently re-admit
// TLS 1.1 / 1.0 against the operator's webhook + metrics endpoints.
func buildTLSOpts(f flags) []func(*tls.Config) {
	pinMinVersion := func(c *tls.Config) {
		c.MinVersion = tls.VersionTLS13
	}
	if f.enableHTTP2 {
		return []func(*tls.Config){pinMinVersion}
	}
	return []func(*tls.Config){
		pinMinVersion,
		func(c *tls.Config) {
			c.NextProtos = []string{"http/1.1"}
		},
	}
}

// buildWebhookOptions resolves the webhook server options from config + flags.
// Flag values (--webhook-cert-path/-name/-key) override the config file.
func buildWebhookOptions(cfg *operatorconfig.Config, f flags, tlsOpts []func(*tls.Config)) webhook.Options {
	opts := webhook.Options{
		Port:    cfg.Webhook.Port,
		CertDir: cfg.Webhook.CertDir,
		TLSOpts: tlsOpts,
	}
	if f.webhookCertPath != "" {
		opts.CertDir = f.webhookCertPath
		opts.CertName = f.webhookCertName
		opts.KeyName = f.webhookCertKey
	}
	return opts
}

// buildMetricsOptions resolves the metrics server options from config + flags.
// When SecureServing is on, an auth filter is attached so only authenticated
// + authorized service accounts can scrape.
func buildMetricsOptions(cfg *operatorconfig.Config, f flags, tlsOpts []func(*tls.Config)) metricsserver.Options {
	opts := metricsserver.Options{
		BindAddress:   cfg.Metrics.BindAddress,
		SecureServing: cfg.MetricsSecure(),
		TLSOpts:       tlsOpts,
	}
	if opts.SecureServing {
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if f.metricsCertPath != "" {
		opts.CertDir = f.metricsCertPath
		opts.CertName = f.metricsCertName
		opts.KeyName = f.metricsCertKey
	}
	return opts
}

// buildCacheOptions constructs the informer-cache configuration: a label
// selector applied to every owned type, plus optional namespace scoping.
// Empty WatchNamespaces yields cluster-scope.
func buildCacheOptions(selector labels.Selector, cfg *operatorconfig.Config) cache.Options {
	byObject := make(map[client.Object]cache.ByObject, len(cacheOwnedTypes))
	for _, obj := range cacheOwnedTypes {
		byObject[obj] = cache.ByObject{Label: selector}
	}
	opts := cache.Options{ByObject: byObject}
	if len(cfg.WatchNamespaces) > 0 {
		opts.DefaultNamespaces = make(map[string]cache.Config, len(cfg.WatchNamespaces))
		for _, ns := range cfg.WatchNamespaces {
			opts.DefaultNamespaces[ns] = cache.Config{}
		}
	}
	return opts
}

// setupWebhookCertProvisioning wires the dynauth Authority + caBundle
// injector into the manager when --enable-dynamic-authority is true and
// no cert-manager Certificate of our name exists in the operator
// namespace. Both controllers are leader-gated (Authority via its
// NeedLeaderElection, injector via controller-runtime's default for
// Reconciler-based controllers), so standby replicas don't race.
//
// Skipping the wireup is the correct behaviour when cert-manager owns
// the cert lifecycle (chart toggle webhook.certManager.enabled=true) —
// otherwise both controllers would fight over the same Secret and the
// same caBundle field on every reconcile.
//
// POD_NAMESPACE is required when --enable-dynamic-authority is on; an
// empty value would cause the Authority to write Secrets cluster-scoped
// (impossible) or to "" (rejected by the apiserver). Fail fast with a
// clear message rather than emit garbage requests at runtime.
func setupWebhookCertProvisioning(mgr ctrl.Manager, f flags) error {
	if !f.enableDynamicAuthority {
		setupLog.Info("dynamic-authority disabled by flag; expecting cert-manager to provision webhook certs")
		return nil
	}

	ns := os.Getenv(envPodNamespace)
	if ns == "" {
		return fmt.Errorf("%s env var is empty; required when --enable-dynamic-authority is true "+
			"(set via Downward API in the chart)", envPodNamespace)
	}

	// Runtime detection: even with --enable-dynamic-authority on, defer
	// to cert-manager when an operator-named Certificate exists. Belt-
	// and-braces against a chart misconfiguration that flips one toggle
	// without the other.
	optedIn, err := dynauth.CertManagerOptedIn(context.Background(), mgr.GetAPIReader(), ns)
	if err != nil {
		return fmt.Errorf("probe cert-manager opt-in: %w", err)
	}
	if optedIn {
		setupLog.Info("cert-manager Certificate detected; skipping dynamic-authority wireup",
			"namespace", ns, "certificate", dynauth.LeafSecretName)
		return nil
	}

	leaves := []dynauth.LeafSpec{
		dynauth.WebhookLeafSpec(f.webhookServiceName, ns),
		dynauth.MetricsLeafSpec(f.metricsServiceName, ns),
	}
	authority := &dynauth.Authority{
		Client:    mgr.GetClient(),
		Namespace: ns,
		Leaves:    leaves,
		Recorder:  mgr.GetEventRecorder("dynauth"),
		Log:       ctrl.Log.WithName("dynauth"),
	}
	if err := mgr.Add(manager.RunnableFunc(authority.Start)); err != nil {
		return fmt.Errorf("register Authority runnable: %w", err)
	}

	injector := &dynauth.Injector{
		Client:    mgr.GetClient(),
		Namespace: ns,
	}
	if err := injector.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("register caBundle injector: %w", err)
	}

	setupLog.Info("dynamic-authority wired",
		"namespace", ns,
		"webhookService", f.webhookServiceName,
		"metricsService", f.metricsServiceName,
		"leafSecrets", []string{dynauth.LeafSecretName, dynauth.MetricsLeafSecretName})
	return nil
}

// runBootstrap is the --bootstrap-only code path. Synchronously
// mints the dynauth CA + leaf Secrets via a one-shot client (no
// manager, no cache, no controllers), then waits for kubelet to
// project the cert files into the mounted volumes before returning.
//
// The wait closes a race: kubelet's secretManager picks up the
// freshly-created Secret immediately, but the per-pod volume
// projection updates on the kubelet sync interval. Without the wait
// the main container could start before its volume is projected and
// crash-loop on a missing tls.crt; the wait lets the init container
// hold the pod's progress until the file is actually on disk.
//
// No-ops when --enable-dynamic-authority=false (cert-manager owns the
// lifecycle) or when a cert-manager Certificate of the operator's
// leaf-Secret name already exists in the namespace (mid-flight
// hand-off equivalent).
func runBootstrap(f flags) error {
	if !f.enableDynamicAuthority {
		setupLog.Info("--bootstrap-only with --enable-dynamic-authority=false; nothing to do")
		return nil
	}
	ns := os.Getenv(envPodNamespace)
	if ns == "" {
		return fmt.Errorf("%s env var is empty; required for --bootstrap-only with "+
			"--enable-dynamic-authority=true (set via Downward API in the chart)", envPodNamespace)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	restCfg := ctrl.GetConfigOrDie()
	bootClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("build bootstrap client: %w", err)
	}

	optedIn, err := dynauth.CertManagerOptedIn(ctx, bootClient, ns)
	if err != nil {
		return fmt.Errorf("probe cert-manager opt-in: %w", err)
	}
	if optedIn {
		setupLog.Info("cert-manager Certificate detected; bootstrap is a no-op",
			"namespace", ns, "certificate", dynauth.LeafSecretName)
		return nil
	}

	leaves := []dynauth.LeafSpec{
		dynauth.WebhookLeafSpec(f.webhookServiceName, ns),
		dynauth.MetricsLeafSpec(f.metricsServiceName, ns),
	}
	authority := &dynauth.Authority{
		Client:    bootClient,
		Namespace: ns,
		Leaves:    leaves,
		Log:       ctrl.Log.WithName("dynauth-bootstrap"),
	}
	if err := authority.Bootstrap(ctx); err != nil {
		return fmt.Errorf("mint webhook + metrics cert Secrets: %w", err)
	}
	setupLog.Info("dynauth bootstrap Secrets created or verified",
		"namespace", ns,
		"leafSecrets", []string{dynauth.LeafSecretName, dynauth.MetricsLeafSecretName})

	// Wait for kubelet to project the freshly-created Secret content
	// into the mounted webhook + metrics cert volumes. The init
	// container's mount paths come from the same cert-path flags
	// the main container uses, so both see the same projection.
	for label, certDir := range map[string]string{
		"webhook": f.webhookCertPath,
		"metrics": f.metricsCertPath,
	} {
		if certDir == "" {
			continue
		}
		target := filepath.Join(certDir, "tls.crt")
		if err := waitForProjectedFile(ctx, target); err != nil {
			return fmt.Errorf("wait for %s cert at %s: %w", label, target, err)
		}
		setupLog.Info("kubelet projected cert", "kind", label, "path", target)
	}
	return nil
}

// parseTestOverrides reads the three test-override env vars and
// returns the populated tunables struct. When allowed=false (the
// production default), the env vars are ignored entirely and a
// zero-valued tunables is returned — the reconciler then falls back
// to the const safety floors. When allowed=true and an env var is
// set, the value is parsed and validated; a malformed value fails
// startup so a misconfigured chart override surfaces immediately
// rather than silently relaxing the floor.
//
// Env var contract:
//
//   - VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC: positive int
//     (seconds). Overrides the 60s gate-entry threshold.
//   - VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS: positive
//     int (count of distinct observer polls). Overrides the 2-poll
//     recovery hysteresis.
//   - VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC: positive int (seconds).
//     Overrides Phase 11's 30s orchestration deadline.
//
// Each var is independently optional — an unset var leaves its
// corresponding field zero and the reconciler defaults that field.
func parseTestOverrides(allowed bool) (controller.QuorumSuppressionTunables, error) {
	var t controller.QuorumSuppressionTunables
	if !allowed {
		return t, nil
	}
	loss, set, err := parsePositiveIntEnv("VALKEY_OPERATOR_QUORUM_LOSS_SUPPRESSION_SEC")
	if err != nil {
		return t, err
	}
	if set {
		t.LossThreshold = time.Duration(loss) * time.Second
	}
	polls, set, err := parsePositiveIntEnv("VALKEY_OPERATOR_QUORUM_RECOVERY_HYSTERESIS_POLLS")
	if err != nil {
		return t, err
	}
	if set {
		t.RecoveryPolls = polls
	}
	phase11, set, err := parsePositiveIntEnv("VALKEY_OPERATOR_PHASE11_TIMEOUT_SEC")
	if err != nil {
		return t, err
	}
	if set {
		t.Phase11Timeout = time.Duration(phase11) * time.Second
	}
	authority, set, err := parsePositiveIntEnv("VALKEY_OPERATOR_AUTHORITY_MIN_INTERVAL_SEC")
	if err != nil {
		return t, err
	}
	if set {
		d := time.Duration(authority) * time.Second
		// Shrink BOTH bounds together. The min-only path would leave a
		// long-lived CA polled at the 24h ceiling — force-rotate
		// annotation tests would never observe a reconcile inside
		// their 3-minute budget.
		dynauth.SetMinRotationInterval(d)
		dynauth.SetMaxRotationInterval(d)
	}
	return t, nil
}

// parsePositiveIntEnv reads name from the environment as a positive
// integer. It returns (0, false, nil) when the var is unset, (n, true,
// nil) for a valid positive value, and a "<name>=%q: must be a positive
// integer" error when the value is non-numeric or non-positive.
func parsePositiveIntEnv(name string) (int, bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false, fmt.Errorf("%s=%q: must be a positive integer", name, v)
	}
	return n, true, nil
}

// waitForProjectedFile polls path until it exists or the context
// expires. Used by runBootstrap to bound the init container's hold
// on the pod's progress against an unbounded kubelet sync delay.
//
// kubelet's atomic_writer projects Secret content via a `..data`
// dir + symlinks (so updates are atomic). os.Stat follows symlinks
// transparently — IsNotExist returns true only when the leaf target
// doesn't exist yet, which is exactly the state we're waiting out
// of. No special handling for the symlink layer is required.
func waitForProjectedFile(ctx context.Context, path string) error {
	const pollInterval = 2 * time.Second
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("file %s not projected before context expired: %w", path, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

func buildManagerOptions(cfg *operatorconfig.Config, f flags) (ctrl.Options, error) {
	selector, err := labels.Parse(cfg.CacheManagedBySelector)
	if err != nil {
		return ctrl.Options{}, fmt.Errorf("cacheManagedBySelector %q: %w", cfg.CacheManagedBySelector, err)
	}

	tlsOpts := buildTLSOpts(f)

	return ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       buildMetricsOptions(cfg, f, tlsOpts),
		WebhookServer:                 webhook.NewServer(buildWebhookOptions(cfg, f, tlsOpts)),
		HealthProbeBindAddress:        cfg.HealthProbeBindAddress,
		LeaderElection:                cfg.LeaderElectionEnabled(),
		LeaderElectionID:              cfg.LeaderElection.ID,
		LeaderElectionNamespace:       cfg.LeaderElection.Namespace,
		LeaseDuration:                 new(cfg.LeaderElection.LeaseDuration.Duration),
		RenewDeadline:                 new(cfg.LeaderElection.RenewDeadline.Duration),
		RetryPeriod:                   new(cfg.LeaderElection.RetryPeriod.Duration),
		LeaderElectionReleaseOnCancel: cfg.LeaderElectionReleaseOnCancel(),
		Cache:                         buildCacheOptions(selector, cfg),
	}, nil
}
