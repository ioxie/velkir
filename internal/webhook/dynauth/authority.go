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

package dynauth

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/metrics"
)

// Resource names + labels + annotations are public so the chart templates,
// the injector controller, and operator users can reference them with type
// safety. Don't string-literal these elsewhere.
const (
	// CASecretName is the operator-wide root CA Secret. Its `ca.crt` is the
	// trust anchor for every leaf the Authority mints (webhook + metrics).
	// Despite the legacy "-webhook-" suffix, it's no longer webhook-specific.
	//
	// One root CA signing both the webhook and metrics leaves is a
	// deliberate choice, not an oversight: a compromise of this single CA
	// Secret forges trust for both TLS surfaces at once. That blast radius
	// is accepted because both leaves are already minted and held by the
	// same operator process from the same Secret namespace — splitting into
	// per-surface CAs would not raise the bar for an attacker who has
	// reached the operator's namespace, while it would double the rotation
	// and bootstrap surface. Revisit only if the metrics endpoint is ever
	// served by a separately-trusted component.
	CASecretName = "velkir-webhook-ca"

	// LeafSecretName / MetricsLeafSecretName are the per-consumer leaf
	// Secrets. The operator binary mounts each as a projected volume so the
	// controller-runtime servers can certwatcher-reload on rotation without
	// a restart.
	LeafSecretName        = "velkir-webhook-cert"
	MetricsLeafSecretName = "velkir-metrics-cert"

	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByValue      = "velkir"
	CertRoleLabel       = "velkir.ioxie.dev/cert-role"
	CertRoleCA          = "ca"
	CertRoleLeaf        = "webhook-leaf"
	CertRoleMetricsLeaf = "metrics-leaf"
	InjectCALabel       = "velkir.ioxie.dev/inject-ca"
	InjectCALabelTrue   = "true"

	// ForceRotateAnnotation on either Secret triggers an out-of-band reissue
	// the next time the periodic loop wakes. The annotation value is opaque
	// — convention is an RFC3339 timestamp so the audit trail records when
	// the operator triggered it — but the operator only checks for
	// presence. Cleared on successful rotation so it doesn't fire again.
	ForceRotateAnnotation = "velkir.ioxie.dev/force-rotate"

	// CABundleFieldOwner is the SSA field manager the caBundle injector
	// stamps on its WebhookConfiguration patches. Distinct from the main
	// reconciler's owner so a user manually editing caBundle surfaces a
	// clean SSA conflict instead of getting silently overwritten.
	CABundleFieldOwner = "velkir-ca-injector"

	// Gauge label values for valkey_cert_expiry_seconds. Stable strings —
	// the ValkeyCertExpiringSoon PrometheusRule reads the gauge without
	// a label filter, so adding a new value automatically widens the alert.
	expiryLabelCA          = "ca"
	expiryLabelWebhookLeaf = "webhook-leaf"
	expiryLabelMetricsLeaf = "metrics-leaf"
)

// CA + leaf lifetime defaults. Overridable so the e2e harness can
// shrink lifetimes to seconds without subverting the production path.
var (
	DefaultCALifetime   = 5 * 365 * 24 * time.Hour
	DefaultLeafLifetime = 365 * 24 * time.Hour
)

// LeafSpec describes one leaf certificate the Authority manages. All leaves
// share a single root CA in CASecretName; only the SAN list, target Secret,
// and cert-role label vary per leaf.
type LeafSpec struct {
	// SecretName is the Secret holding the leaf cert+key. Stable across
	// rotations.
	SecretName string
	// DNSNames are the SANs stamped on the leaf. Must include every hostname
	// the consumer will dial (typically the Service short name + the
	// cluster.local FQDN).
	DNSNames []string
	// CertRole is stamped as a label on the Secret for triage.
	CertRole string
	// ExpiryGaugeLabel is the value used for the kind label on
	// valkey_cert_expiry_seconds. Stable across rotations so the gauge time
	// series stays continuous.
	ExpiryGaugeLabel string
}

// Authority is a leader-elected controller-runtime Runnable that ensures
// the CA + per-leaf Secrets exist and stay ahead of expiry. Standby
// replicas never run it (NeedLeaderElection=true) — they consume each
// Secret indirectly via projected volume + certwatcher in cmd/main.go,
// so the rotation loop has a single owner and no two-replica race.
type Authority struct {
	Client    client.Client
	Namespace string

	// Leaves is the per-consumer leaf list. All leaves are signed by the
	// single root CA. Must be non-empty (Start rejects an empty list).
	Leaves []LeafSpec

	// Tunables. Zero values fall back to the package defaults.
	CALifetime   time.Duration
	LeafLifetime time.Duration

	// Recorder is optional; nil disables event emission.
	Recorder k8sevents.EventRecorder

	// Now is the clock the rotation predicate consults. nil = time.Now.
	// Tests pin this for deterministic boundary assertions.
	Now func() time.Time

	Log logr.Logger
}

// NeedLeaderElection makes the manager skip Start on standby replicas.
// All write paths (Secret create/update, force-rotate annotation handling)
// live behind this gate; only the elected leader mints certs.
func (a *Authority) NeedLeaderElection() bool { return true }

// Start blocks until ctx is cancelled, the cert-manager opt-in is
// detected, or a fatal error occurs. It performs an immediate ensure-
// then-rotate pass, then sleeps for the next-check interval and repeats.
// Errors are logged + recorded as Events but never abort the loop, so a
// transient API blip doesn't take down cert provisioning permanently.
//
// Each pass re-checks for cert-manager opt-in (a Certificate resource of
// our webhook-leaf name appearing in the operator namespace). If it's now
// present — e.g. Helm/GitOps applied the cert-manager toggle after this
// replica started — Start returns nil cleanly, deferring to cert-manager
// for the rest of the process lifetime. This closes the temporal race
// where the once-at-startup detection could leave replicas disagreeing
// after a mid-flight toggle.
func (a *Authority) Start(ctx context.Context) error {
	if len(a.Leaves) == 0 {
		return fmt.Errorf("Authority requires at least one LeafSpec")
	}
	a.applyDefaults()

	for {
		// Mid-flight cert-manager opt-in check. If cert-manager has appeared
		// since process start, hand the cert lifecycle off cleanly.
		if optedIn, err := CertManagerOptedIn(ctx, a.Client, a.Namespace); err != nil {
			a.Log.V(1).Info("cert-manager opt-in probe failed; continuing dynauth loop", "err", err.Error())
		} else if optedIn {
			a.Log.Info("cert-manager Certificate appeared mid-flight; stopping dynauth loop",
				"namespace", a.Namespace, "certificate", LeafSecretName)
			return nil
		}

		nextCheck, err := a.reconcileOnce(ctx)
		if err != nil {
			// reconcileOnce already refreshed every readable cert's expiry
			// gauge and scaled nextCheck to the soonest-expiring cert, so a
			// failing pass still drives ValkeyCertExpiringSoon and retries
			// sooner as expiry nears. The validating webhook is
			// failurePolicy: Fail — a leaf that reaches NotAfter blocks every
			// Valkey admission cluster-wide, so a stuck rotation is a cluster
			// availability dependency, not just cert hygiene.
			a.Log.Error(err, "reconcile failed; will retry", "retryIn", nextCheck.String())
			a.recordEvent(corev1.EventTypeWarning, events.WebhookCertRotationFailed, "WebhookCertRotateFail", "%s", err.Error())
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(nextCheck):
		}
	}
}

// Post-failure retry scheduling. On a reconcile error the loop doesn't sit
// at a flat interval while a leaf burns down toward NotAfter: the retry is
// scaled to the soonest-expiring observed cert so attempts get denser as
// expiry nears. Promoted to vars (mirroring the rotation-interval knobs in
// rotation.go) so tests can pin them; production callers must not widen
// errorRetryMin or the loop could hot-spin the apiserver on a persistent
// failure.
var (
	// errorRetryMax caps the post-failure retry — the far-from-expiry case,
	// matching the historical flat 1h backoff.
	errorRetryMax = time.Hour
	// errorRetryMin floors it so a cert at or past expiry can't hot-spin
	// the apiserver on a failure that won't self-heal (e.g. RBAC loss).
	errorRetryMin = time.Minute
	// errorRetryDivisor sets retry ≈ remaining/divisor inside the danger
	// zone, so once the soonest cert is within errorRetryDivisor×errorRetryMax
	// of expiry the loop begins retrying sooner than errorRetryMax.
	errorRetryDivisor = 10
)

// errorRetryInterval returns how long to wait before the next reconcile
// after a failed pass. soonestRemaining is the smallest NotAfter-now over
// the certs observed this pass; observed=false means no cert was readable,
// so urgency is unknown and we fall back to the conservative cap.
func errorRetryInterval(soonestRemaining time.Duration, observed bool) time.Duration {
	if !observed {
		return errorRetryMax
	}
	retry := soonestRemaining / time.Duration(errorRetryDivisor)
	retry = max(retry, errorRetryMin)
	retry = min(retry, errorRetryMax)
	return retry
}

// Bootstrap runs a single reconcile pass to ensure the CA + every
// leaf Secret exists, then returns. Used by the chart's init
// container (cmd/main.go --bootstrap-only) to mint the webhook +
// metrics cert Secrets BEFORE the main container starts.
//
// Without this, controller-runtime's webhook server (which loads
// tls.crt at manager.Start time, not lazily on first request) would
// fatal on a missing file — and the periodic Authority loop that
// would mint that file can't run until after manager.Start has
// succeeded. Bootstrap closes the chicken-and-egg by running the
// mint synchronously via a caller-supplied client, before the
// manager is even constructed.
//
// Idempotent: if every Secret already exists with parseable
// content, no writes happen. Returns when the pass completes or on
// the first error. Does NOT start the periodic rotation loop —
// the caller is expected to exit after Bootstrap; the main
// container's Authority runnable (registered the normal way under
// mgr.Add) takes over rotation post-manager.Start.
func (a *Authority) Bootstrap(ctx context.Context) error {
	if len(a.Leaves) == 0 {
		return fmt.Errorf("Authority requires at least one LeafSpec")
	}
	a.applyDefaults()
	_, err := a.reconcileOnce(ctx)
	return err
}

// reconcileOnce ensures the CA Secret and every leaf Secret are valid and
// returns the sleep interval until the next check (the smallest of the
// CA's and every leaf's next-rotation-check, so an early-rotating leaf
// wakes us sooner than the CA would).
//
// A failing pass is never fatal and never silent: every cert that can
// still be read has its expiry gauge refreshed from the cert actually in
// use (the old, soon-to-expire one when a rotation write fails), errors
// from all certs are accumulated rather than short-circuiting on the
// first, and the returned interval is scaled to the soonest-expiring cert
// so retries get denser as expiry nears. This is what keeps a stuck
// rotation visible to ValkeyCertExpiringSoon: a gauge frozen at its last
// good value would read as healthy exactly while the served leaf burns
// down toward the failurePolicy: Fail cliff.
func (a *Authority) reconcileOnce(ctx context.Context) (time.Duration, error) {
	now := a.Now()
	var errs []error

	// soonest tracks the smallest NotAfter-now over every cert observed
	// this pass; it drives both the failure-retry urgency and is the
	// signal that at least one gauge was refreshed.
	var soonest time.Duration
	haveObserved := false
	observe := func(label string, cert *x509.Certificate) {
		if cert == nil {
			return
		}
		rem := cert.NotAfter.Sub(now)
		metrics.CertExpirySeconds.WithLabelValues(label).Set(rem.Seconds())
		if !haveObserved || rem < soonest {
			soonest = rem
			haveObserved = true
		}
	}

	caCert, caRotated, err := a.ensureCA(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("ensure CA: %w", err))
	} else if caRotated {
		a.recordEvent(corev1.EventTypeNormal, events.WebhookCertRotated, "WebhookCertRotate",
			"reissued root CA in Secret %s", CASecretName)
	}
	observe(expiryLabelCA, caCert)

	// Reload the CA's PEM material for leaf signing. Even when the CA
	// wasn't rotated this pass, ensureLeaf needs the full PEM (cert + key)
	// to mint or re-sign each leaf. Skip it when the CA is unhealthy — we
	// can't sign against a CA we couldn't establish, but we can still
	// observe each leaf already on disk.
	var ca CertMaterial
	caUsable := false
	if caCert != nil && err == nil {
		caMat, loadErr := a.loadSecretMaterial(ctx, CASecretName)
		if loadErr != nil {
			errs = append(errs, fmt.Errorf("load CA Secret material: %w", loadErr))
		} else {
			ca = caMat
			caUsable = true
		}
	}

	earliestNext := maxRotationInterval
	if caCert != nil {
		if caNext := NextRotationCheck(caCert, now, RotationFraction); caNext < earliestNext {
			earliestNext = caNext
		}
	}

	for i := range a.Leaves {
		spec := a.Leaves[i]
		var leafCert *x509.Certificate
		if caUsable {
			lc, leafRotated, leafErr := a.ensureLeaf(ctx, spec, ca, caRotated)
			if leafErr != nil {
				errs = append(errs, fmt.Errorf("ensure leaf %s: %w", spec.SecretName, leafErr))
			} else if leafRotated {
				a.recordEvent(corev1.EventTypeNormal, events.WebhookCertRotated, "WebhookCertRotate",
					"reissued leaf in Secret %s", spec.SecretName)
			}
			leafCert = lc
		} else {
			// No usable CA this pass: don't attempt to sign, but keep the
			// gauge live from the leaf already on disk so a CA-side outage
			// doesn't also freeze the leaf expiry signal.
			lc, obsErr := a.observeLeaf(ctx, spec)
			if obsErr != nil {
				errs = append(errs, fmt.Errorf("observe leaf %s: %w", spec.SecretName, obsErr))
			}
			leafCert = lc
		}
		// Refresh per-leaf expiry gauge every pass so the value Prometheus
		// scrapes is always within `nextCheck` of accurate, even when a
		// cert wasn't (or couldn't be) rotated this pass.
		observe(spec.ExpiryGaugeLabel, leafCert)
		if leafCert != nil {
			if leafNext := NextRotationCheck(leafCert, now, RotationFraction); leafNext < earliestNext {
				earliestNext = leafNext
			}
		}
	}

	if len(errs) > 0 {
		return errorRetryInterval(soonest, haveObserved), errors.Join(errs...)
	}
	return earliestNext, nil
}

// observeLeaf reads spec's leaf Secret and parses its cert without any
// rotation attempt. It exists so reconcileOnce can keep the expiry gauge
// live on passes where the CA material is unavailable (so signing would be
// unsafe) but the leaf cert on disk is still readable.
func (a *Authority) observeLeaf(ctx context.Context, spec LeafSpec) (*x509.Certificate, error) {
	sec, err := a.getSecret(ctx, spec.SecretName)
	if err != nil {
		return nil, err
	}
	return ParseCert(materialFromSecret(sec).CertPEM)
}

// ensureCA returns the parsed CA cert and whether a fresh CA was minted
// this pass. Callers use the rotated flag to force a leaf reissue: a new
// CA invalidates any leaf signed by the old one.
func (a *Authority) ensureCA(ctx context.Context) (*x509.Certificate, bool, error) {
	sec, err := a.getSecret(ctx, CASecretName)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	if sec == nil {
		mat, err := GenerateCA(a.Now(), a.CALifetime)
		if err != nil {
			return nil, false, err
		}
		if err := a.createSecret(ctx, CASecretName, CertRoleCA, mat, mat.CertPEM); err != nil {
			return nil, false, err
		}
		c, err := ParseCert(mat.CertPEM)
		return c, true, err
	}

	mat := materialFromSecret(sec)
	c, err := ParseCert(mat.CertPEM)
	if err != nil {
		// Corrupt CA — reissue. Don't soldier on with a bad cert; the
		// apiserver will reject any leaf signed by it.
		a.Log.Info("CA Secret payload unparseable; reissuing", "err", err.Error())
		return a.rotateCA(ctx, sec)
	}

	if a.shouldForceRotate(sec) || ShouldRotate(c, a.Now(), RotationFraction) {
		newCert, rotated, rerr := a.rotateCA(ctx, sec)
		if rerr != nil {
			// Rotation write failed: the old CA is still the trust anchor in
			// use, so report it (with the error) and let reconcileOnce keep
			// its expiry gauge live rather than freezing at the last good pass.
			return c, false, rerr
		}
		return newCert, rotated, nil
	}
	return c, false, nil
}

func (a *Authority) rotateCA(ctx context.Context, sec *corev1.Secret) (*x509.Certificate, bool, error) {
	mat, err := GenerateCA(a.Now(), a.CALifetime)
	if err != nil {
		return nil, false, err
	}
	if err := a.updateSecret(ctx, sec, mat, mat.CertPEM); err != nil {
		return nil, false, err
	}
	c, err := ParseCert(mat.CertPEM)
	return c, true, err
}

// ensureLeaf signs spec's leaf against the supplied CA. caRotated forces a
// reissue regardless of the rotation predicate — the cascade keeps every
// leaf in sync with the current CA.
func (a *Authority) ensureLeaf(ctx context.Context, spec LeafSpec, ca CertMaterial, caRotated bool) (*x509.Certificate, bool, error) {
	sec, err := a.getSecret(ctx, spec.SecretName)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	if sec == nil {
		mat, err := GenerateLeaf(a.Now(), a.LeafLifetime, spec.DNSNames, ca)
		if err != nil {
			return nil, false, err
		}
		if err := a.createSecret(ctx, spec.SecretName, spec.CertRole, mat, ca.CertPEM); err != nil {
			return nil, false, err
		}
		c, err := ParseCert(mat.CertPEM)
		return c, true, err
	}

	mat := materialFromSecret(sec)
	c, err := ParseCert(mat.CertPEM)
	if err != nil {
		a.Log.Info("leaf Secret payload unparseable; reissuing", "secret", spec.SecretName, "err", err.Error())
		return a.rotateLeaf(ctx, sec, spec, ca)
	}
	// A leaf signed by a previous CA verifies as a cert but is rejected by
	// every consumer trusting the current bundle. This happens when a crash
	// lands between the CA Secret update and the leaf reissue: on the next
	// pass caRotated is false and the rotation predicate sees a young leaf,
	// so without this check nothing would ever heal the chain.
	staleChain := false
	if caCert, caErr := ParseCert(ca.CertPEM); caErr == nil {
		if sigErr := c.CheckSignatureFrom(caCert); sigErr != nil {
			staleChain = true
			a.Log.Info("leaf not signed by current CA; reissuing",
				"secret", spec.SecretName, "err", sigErr.Error())
		}
	}
	if caRotated || staleChain || a.shouldForceRotate(sec) || ShouldRotate(c, a.Now(), RotationFraction) {
		newCert, rotated, rerr := a.rotateLeaf(ctx, sec, spec, ca)
		if rerr != nil {
			// Rotation write failed: the old leaf is still what's mounted and
			// served, so report it (with the error). Its declining NotAfter
			// must keep driving the expiry gauge — a frozen gauge is exactly
			// what lets a stuck rotation slip past ValkeyCertExpiringSoon
			// until failurePolicy: Fail turns it into a cluster-wide outage.
			return c, false, rerr
		}
		return newCert, rotated, nil
	}
	return c, false, nil
}

func (a *Authority) rotateLeaf(ctx context.Context, sec *corev1.Secret, spec LeafSpec, ca CertMaterial) (*x509.Certificate, bool, error) {
	mat, err := GenerateLeaf(a.Now(), a.LeafLifetime, spec.DNSNames, ca)
	if err != nil {
		return nil, false, err
	}
	if err := a.updateSecret(ctx, sec, mat, ca.CertPEM); err != nil {
		return nil, false, err
	}
	c, err := ParseCert(mat.CertPEM)
	return c, true, err
}

// --- Secret CRUD -----------------------------------------------------------

func (a *Authority) getSecret(ctx context.Context, name string) (*corev1.Secret, error) {
	var sec corev1.Secret
	err := a.Client.Get(ctx, types.NamespacedName{Namespace: a.Namespace, Name: name}, &sec)
	if err != nil {
		return nil, err
	}
	return &sec, nil
}

func (a *Authority) createSecret(ctx context.Context, name, role string, mat CertMaterial, caPEM []byte) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: a.Namespace,
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				CertRoleLabel:  role,
			},
			// Initialised to an empty (non-nil) map so future code that
			// writes to annotations on a fresh Secret without a nil-check
			// can't panic. shouldForceRotate's read path is already nil-
			// safe, so this is preventive consistency rather than a fix.
			Annotations: map[string]string{},
		},
		Type: corev1.SecretTypeTLS,
		Data: secretPayload(mat, caPEM),
	}
	if err := a.Client.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	// IgnoreAlreadyExists handles the multi-replica first-install race.
	// The next reconcile pass will read the Secret some other replica
	// wrote and converge.
	return nil
}

func (a *Authority) updateSecret(ctx context.Context, sec *corev1.Secret, mat CertMaterial, caPEM []byte) error {
	sec = sec.DeepCopy()
	sec.Data = secretPayload(mat, caPEM)
	// Clear the force-rotate annotation so it doesn't fire repeatedly on
	// every reconcile pass after a rotation has landed.
	delete(sec.Annotations, ForceRotateAnnotation)
	return a.Client.Update(ctx, sec)
}

func (a *Authority) loadSecretMaterial(ctx context.Context, name string) (CertMaterial, error) {
	sec, err := a.getSecret(ctx, name)
	if err != nil {
		return CertMaterial{}, err
	}
	return materialFromSecret(sec), nil
}

func (a *Authority) shouldForceRotate(sec *corev1.Secret) bool {
	_, ok := sec.Annotations[ForceRotateAnnotation]
	return ok
}

// --- helpers ---------------------------------------------------------------

func (a *Authority) applyDefaults() {
	if a.CALifetime == 0 {
		a.CALifetime = DefaultCALifetime
	}
	if a.LeafLifetime == 0 {
		a.LeafLifetime = DefaultLeafLifetime
	}
	if a.Now == nil {
		a.Now = time.Now
	}
}

func (a *Authority) recordEvent(eventType string, reason events.Reason, action, note string, args ...any) {
	if a.Recorder == nil {
		return
	}
	// Anchor on the CA Secret: it's the artifact users open first when
	// debugging cert issues, so events are easy to find via
	// `kubectl describe secret velkir-webhook-ca`. The new
	// events.EventRecorder Eventf takes a runtime.Object as the
	// `regarding` arg, so a minimal *corev1.Secret with namespace +
	// name carries the involved-object reference even when the actual
	// Secret hasn't yet been created (first-mint case).
	regarding := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: a.Namespace,
			Name:      CASecretName,
		},
	}
	a.Recorder.Eventf(regarding, nil, eventType, string(reason), action, note, args...)
}

func secretPayload(mat CertMaterial, caPEM []byte) map[string][]byte {
	return map[string][]byte{
		corev1.TLSCertKey:       mat.CertPEM,
		corev1.TLSPrivateKeyKey: mat.KeyPEM,
		// `ca.crt` carries the trust anchor — the signing CA's cert, NOT
		// a copy of this Secret's own leaf. A consumer that mounts a leaf
		// Secret and reads ca.crt to verify a peer needs the CA here; the
		// previous leaf-cert mirror made ca.crt useless as a trust bundle
		// and ambiguous during triage. For the CA Secret itself the caller
		// passes the CA cert (== mat.CertPEM), so ca.crt stays the CA.
		"ca.crt": caPEM,
	}
}

func materialFromSecret(sec *corev1.Secret) CertMaterial {
	return CertMaterial{
		CertPEM: sec.Data[corev1.TLSCertKey],
		KeyPEM:  sec.Data[corev1.TLSPrivateKeyKey],
	}
}

// WebhookLeafSpec returns the canonical webhook leaf for the given Service
// short name + namespace. Centralised so cmd/main.go and tests stay in sync
// on the SAN shape.
func WebhookLeafSpec(serviceName, namespace string) LeafSpec {
	return LeafSpec{
		SecretName: LeafSecretName,
		CertRole:   CertRoleLeaf,
		DNSNames: []string{
			serviceName + "." + namespace + ".svc",
			serviceName + "." + namespace + ".svc.cluster.local",
		},
		ExpiryGaugeLabel: expiryLabelWebhookLeaf,
	}
}

// MetricsLeafSpec returns the canonical metrics leaf for the given Service
// short name + namespace.
func MetricsLeafSpec(serviceName, namespace string) LeafSpec {
	return LeafSpec{
		SecretName: MetricsLeafSecretName,
		CertRole:   CertRoleMetricsLeaf,
		DNSNames: []string{
			serviceName + "." + namespace + ".svc",
			serviceName + "." + namespace + ".svc.cluster.local",
		},
		ExpiryGaugeLabel: expiryLabelMetricsLeaf,
	}
}
