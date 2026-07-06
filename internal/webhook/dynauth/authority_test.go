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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/ioxie/velkir/internal/metrics"
)

// newAuthority assembles a test Authority over the fake client. Uses tiny
// lifetimes so tests can advance the clock past expiry without dealing in
// real years.
func newAuthority(t *testing.T, now time.Time, objs ...client.Object) (*Authority, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Authority{
		Client:    c,
		Namespace: "velkir",
		Leaves: []LeafSpec{
			WebhookLeafSpec("velkir-webhook", "velkir"),
		},
		CALifetime:   24 * time.Hour, // tiny so rotation predicate is easy to hit
		LeafLifetime: 12 * time.Hour,
		Now:          func() time.Time { return now },
		Log:          zap.New(zap.UseDevMode(true)),
	}, c
}

func TestReconcileOnce_FreshNamespace_CreatesBothSecrets(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, now)

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcileOnce: %v", err)
	}

	for _, name := range []string{CASecretName, LeafSecretName} {
		var sec corev1.Secret
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: name}, &sec); err != nil {
			t.Errorf("Secret %s not created: %v", name, err)
			continue
		}
		if sec.Type != corev1.SecretTypeTLS {
			t.Errorf("Secret %s type=%v, want kubernetes.io/tls", name, sec.Type)
		}
		if len(sec.Data[corev1.TLSCertKey]) == 0 || len(sec.Data[corev1.TLSPrivateKeyKey]) == 0 {
			t.Errorf("Secret %s missing tls.crt or tls.key", name)
		}
	}
}

// TestReconcileOnce_LeafCACrtIsSigningCA pins that a leaf Secret's
// `ca.crt` carries the signing CA's cert (the real trust anchor), not a
// mirror of the leaf's own cert — and that the CA Secret's `ca.crt` is
// still the CA itself.
func TestReconcileOnce_LeafCACrtIsSigningCA(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, now)

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcileOnce: %v", err)
	}

	get := func(name string) corev1.Secret {
		var sec corev1.Secret
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: name}, &sec); err != nil {
			t.Fatalf("get Secret %s: %v", name, err)
		}
		return sec
	}
	ca := get(CASecretName)
	leaf := get(LeafSecretName)

	// CA Secret: ca.crt mirrors its own cert (it IS the trust anchor).
	if string(ca.Data["ca.crt"]) != string(ca.Data[corev1.TLSCertKey]) {
		t.Errorf("CA Secret ca.crt != its own tls.crt; the CA should be its own trust anchor")
	}
	// Leaf Secret: ca.crt is the signing CA's cert, NOT the leaf's cert.
	if string(leaf.Data["ca.crt"]) != string(ca.Data[corev1.TLSCertKey]) {
		t.Errorf("leaf Secret ca.crt is not the signing CA cert")
	}
	if string(leaf.Data["ca.crt"]) == string(leaf.Data[corev1.TLSCertKey]) {
		t.Errorf("leaf Secret ca.crt mirrors the leaf cert (the #490 bug); want the CA cert")
	}

	// Rotation cascade: advance past the CA's NotAfter (24h in
	// newAuthority) so the CA reissues and forces a leaf re-sign. The
	// leaf's ca.crt must FOLLOW the new trust anchor, not stay pinned to
	// the old CA cert.
	oldCACert := append([]byte(nil), ca.Data[corev1.TLSCertKey]...)
	a.Now = func() time.Time { return now.Add(48 * time.Hour) }
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("post-expiry reconcileOnce: %v", err)
	}
	caRot := get(CASecretName)
	leafRot := get(LeafSecretName)
	if string(caRot.Data[corev1.TLSCertKey]) == string(oldCACert) {
		t.Fatalf("CA not rotated past expiry; ca.crt-follows-new-CA check precondition unmet")
	}
	if string(leafRot.Data["ca.crt"]) != string(caRot.Data[corev1.TLSCertKey]) {
		t.Errorf("after CA rotation, leaf ca.crt did not follow the new CA cert")
	}
}

// TestReconcileOnce_LeafSignedByStaleCA_Reissues pins the crash-window heal:
// a crash between the CA Secret update and the leaf reissue leaves a young,
// parseable leaf signed by the OLD CA. On the next pass caRotated is false
// and the rotation predicate sees plenty of lifetime left — the chain check
// must still detect the broken issuer linkage and reissue the leaf.
func TestReconcileOnce_LeafSignedByStaleCA_Reissues(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, now)

	// Mint CA + leaf normally.
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var leafBefore corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: LeafSecretName}, &leafBefore)
	leafCertBefore := append([]byte(nil), leafBefore.Data[corev1.TLSCertKey]...)

	// Simulate the crash window: a NEW CA landed in the Secret but the leaf
	// reissue never ran. The leaf is now signed by a CA nobody trusts.
	newCA, err := GenerateCA(now, a.CALifetime)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	var caSec corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caSec)
	caSec.Data = secretPayload(newCA, newCA.CertPEM)
	if err := c.Update(context.Background(), &caSec); err != nil {
		t.Fatalf("swap CA Secret: %v", err)
	}

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("post-swap reconcile: %v", err)
	}

	var leafAfter corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: LeafSecretName}, &leafAfter)
	if string(leafAfter.Data[corev1.TLSCertKey]) == string(leafCertBefore) {
		t.Fatalf("leaf signed by a stale CA was not reissued")
	}
	newCACert, err := ParseCert(newCA.CertPEM)
	if err != nil {
		t.Fatalf("parse new CA: %v", err)
	}
	reissued, err := ParseCert(leafAfter.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatalf("parse reissued leaf: %v", err)
	}
	if err := reissued.CheckSignatureFrom(newCACert); err != nil {
		t.Errorf("reissued leaf not signed by current CA: %v", err)
	}
	if string(leafAfter.Data["ca.crt"]) != string(newCA.CertPEM) {
		t.Errorf("reissued leaf ca.crt is not the current CA cert")
	}
}

func TestReconcileOnce_ValidSecrets_NoChange(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, now)

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	var caBefore corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caBefore)
	caCertBefore := append([]byte(nil), caBefore.Data[corev1.TLSCertKey]...)

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	var caAfter corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caAfter)
	if string(caAfter.Data[corev1.TLSCertKey]) != string(caCertBefore) {
		t.Errorf("CA cert changed across no-op reconcile (rotation predicate misfired)")
	}
}

func TestReconcileOnce_PastExpiry_RotatesBoth(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, t0)

	// Mint at t0.
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var caT0 corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caT0)
	caCertT0 := append([]byte(nil), caT0.Data[corev1.TLSCertKey]...)

	// Advance clock past the CA's NotAfter (CA lifetime = 24h in newAuthority).
	a.Now = func() time.Time { return t0.Add(48 * time.Hour) }
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("expired reconcile: %v", err)
	}
	var caT1 corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caT1)

	if string(caT1.Data[corev1.TLSCertKey]) == string(caCertT0) {
		t.Errorf("CA cert NOT rotated after passing expiry")
	}

	// Leaf must also be rotated (cascade).
	leafCert, err := ParseCert(caT1.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatalf("parse new CA: %v", err)
	}
	if !leafCert.NotAfter.After(t0.Add(24 * time.Hour)) {
		t.Errorf("new CA NotAfter=%v didn't advance past original expiry", leafCert.NotAfter)
	}
}

func TestReconcileOnce_ForceRotateAnnotation_TriggersReissueAndClearsAnnotation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	a, c := newAuthority(t, t0)

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	// Stamp force-rotate on the CA.
	var ca corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &ca)
	caCertBefore := append([]byte(nil), ca.Data[corev1.TLSCertKey]...)
	if ca.Annotations == nil {
		ca.Annotations = map[string]string{}
	}
	ca.Annotations[ForceRotateAnnotation] = "2026-05-01T12:00:00Z"
	if err := c.Update(context.Background(), &ca); err != nil {
		t.Fatalf("stamp annotation: %v", err)
	}

	// Advance the clock just enough that NewCA's NotBefore differs (avoids
	// the brand-new "indistinguishable Secret" race in the fake client).
	a.Now = func() time.Time { return t0.Add(time.Minute) }
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("force-rotate reconcile: %v", err)
	}

	var caAfter corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &caAfter)
	if string(caAfter.Data[corev1.TLSCertKey]) == string(caCertBefore) {
		t.Errorf("force-rotate annotation didn't trigger CA reissue")
	}
	if _, still := caAfter.Annotations[ForceRotateAnnotation]; still {
		t.Errorf("force-rotate annotation still present after rotation; should be cleared")
	}
}

func TestStart_ExitsWhenCertManagerOptsInMidFlight(t *testing.T) {
	// Authority is up and running; mid-flight a cert-manager Certificate
	// of our leaf name appears in the namespace. The Authority's loop must
	// notice on its next pass and return cleanly without erroring.
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}

	// Pre-stage a cert-manager Certificate in our namespace.
	cmCert := &unstructured.Unstructured{}
	cmCert.SetGroupVersionKind(CertManagerCertificateGVK)
	cmCert.SetNamespace("velkir")
	cmCert.SetName(LeafSecretName)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cmCert).
		Build()

	a := &Authority{
		Client:    c,
		Namespace: "velkir",
		Leaves: []LeafSpec{
			WebhookLeafSpec("velkir-webhook", "velkir"),
		},
		CALifetime:   24 * time.Hour,
		LeafLifetime: 12 * time.Hour,
		Now:          func() time.Time { return now },
		Log:          zap.New(zap.UseDevMode(true)),
	}

	// Start should detect the Certificate on the first pass and return nil
	// without ever creating a CA Secret. Bound it with a short timeout so
	// a regression that fails to detect doesn't hang the test suite.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start returned err on cert-manager opt-in detection: %v", err)
	}

	// Sanity: the Authority did NOT mint a CA Secret on its way out.
	var sec corev1.Secret
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: CASecretName}, &sec)
	if err == nil {
		t.Errorf("Authority minted CA Secret despite cert-manager opt-in")
	}
}

func TestReconcileOnce_MultiLeaf_BothSecretsCreated(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	a := &Authority{
		Client:    c,
		Namespace: "velkir",
		Leaves: []LeafSpec{
			WebhookLeafSpec("velkir-webhook", "velkir"),
			MetricsLeafSpec("velkir-metrics", "velkir"),
		},
		CALifetime:   24 * time.Hour,
		LeafLifetime: 12 * time.Hour,
		Now:          func() time.Time { return now },
		Log:          zap.New(zap.UseDevMode(true)),
	}

	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcileOnce: %v", err)
	}

	for _, name := range []string{CASecretName, LeafSecretName, MetricsLeafSecretName} {
		var sec corev1.Secret
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: name}, &sec); err != nil {
			t.Errorf("Secret %s not created: %v", name, err)
			continue
		}
		if len(sec.Data[corev1.TLSCertKey]) == 0 {
			t.Errorf("Secret %s missing tls.crt", name)
		}
	}

	// Each leaf carries the SAN for its own Service — verify they don't
	// share a leaf cert (the second leaf must be signed independently).
	var webhook, metricsLeaf corev1.Secret
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: LeafSecretName}, &webhook)
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "velkir", Name: MetricsLeafSecretName}, &metricsLeaf)
	if string(webhook.Data[corev1.TLSCertKey]) == string(metricsLeaf.Data[corev1.TLSCertKey]) {
		t.Errorf("webhook and metrics leaves share the same cert PEM; expected distinct SAN-bound leaves")
	}

	webhookCert, err := ParseCert(webhook.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatalf("parse webhook leaf: %v", err)
	}
	if got := webhookCert.DNSNames; len(got) == 0 || got[0] != "velkir-webhook.velkir.svc" {
		t.Errorf("webhook leaf SANs=%v, want velkir-webhook.* prefix", got)
	}

	metricsCert, err := ParseCert(metricsLeaf.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatalf("parse metrics leaf: %v", err)
	}
	if got := metricsCert.DNSNames; len(got) == 0 || got[0] != "velkir-metrics.velkir.svc" {
		t.Errorf("metrics leaf SANs=%v, want velkir-metrics.* prefix", got)
	}
}

func TestStart_RejectsEmptyLeaves(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	a := &Authority{
		Client:    c,
		Namespace: "velkir",
		Log:       zap.New(zap.UseDevMode(true)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := a.Start(ctx)
	if err == nil {
		t.Fatal("Start with empty Leaves should error")
	}
}

func TestErrorRetryInterval(t *testing.T) {
	cases := []struct {
		name      string
		remaining time.Duration
		observed  bool
		want      time.Duration
	}{
		{"unobserved falls back to the cap", time.Hour, false, errorRetryMax},
		{"far from expiry caps at max", 30 * 24 * time.Hour, true, errorRetryMax},
		{"inside the danger zone scales down", 30 * time.Minute, true, 3 * time.Minute},
		{"near expiry floors at min", 30 * time.Second, true, errorRetryMin},
		{"at the min boundary", 10 * time.Minute, true, errorRetryMin},
		{"past expiry floors at min", -5 * time.Minute, true, errorRetryMin},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorRetryInterval(tc.remaining, tc.observed); got != tc.want {
				t.Errorf("errorRetryInterval(%s, %v)=%s, want %s", tc.remaining, tc.observed, got, tc.want)
			}
		})
	}
}

// A rotation write that fails (RBAC loss, apiserver issue) must NOT freeze
// the expiry gauge: reconcileOnce has to refresh it from the old, still-
// served cert so its declining remaining time keeps driving
// ValkeyCertExpiringSoon, and it must keep processing sibling leaves rather
// than short-circuiting on the first failure. The retry also escalates
// below the flat 1h once a served leaf is near expiry.
func TestReconcileOnce_RotationWriteFailure_RefreshesGaugeAndEscalatesRetry(t *testing.T) {
	metrics.CertExpirySeconds.Reset()

	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}

	// Fail Update only for the webhook leaf Secret; CA + metrics-leaf writes
	// succeed. The initial mint goes through Create, so it is unaffected.
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cli client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if obj.GetName() == LeafSecretName {
					return apierrors.NewInternalError(fmt.Errorf("synthetic update failure for %s", LeafSecretName))
				}
				return cli.Update(ctx, obj, opts...)
			},
		}).
		Build()

	a := &Authority{
		Client:    c,
		Namespace: "velkir",
		Leaves: []LeafSpec{
			WebhookLeafSpec("velkir-webhook", "velkir"),
			MetricsLeafSpec("velkir-metrics", "velkir"),
		},
		CALifetime:   24 * time.Hour,
		LeafLifetime: 12 * time.Hour,
		Now:          func() time.Time { return t0 },
		Log:          zap.New(zap.UseDevMode(true)),
	}

	// Mint everything at t0 (Create path — succeeds).
	if _, err := a.reconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial mint reconcile: %v", err)
	}

	// 30 min before the leaves' 12h NotAfter both leaves are inside the
	// rotation window (threshold = 10%·12h = 1.2h remaining). The webhook
	// leaf's rotation write fails; the metrics leaf's succeeds. The 24h CA is
	// nowhere near its window, so it isn't rewritten this pass.
	nowT := t0.Add(11*time.Hour + 30*time.Minute)
	a.Now = func() time.Time { return nowT }

	next, err := a.reconcileOnce(context.Background())
	if err == nil {
		t.Fatal("expected reconcileOnce to error when the webhook leaf rotation write fails")
	}
	if !strings.Contains(err.Error(), LeafSecretName) {
		t.Errorf("error %q does not name the failing leaf Secret %q", err.Error(), LeafSecretName)
	}

	// The fix: the webhook-leaf gauge is refreshed from the OLD (still-served)
	// cert's declining remaining time — ~1800s — NOT frozen at the ~43200s
	// value from the last successful pass.
	webhookRem := testutil.ToFloat64(metrics.CertExpirySeconds.WithLabelValues(expiryLabelWebhookLeaf))
	if math.Abs(webhookRem-1800) > 5 {
		t.Errorf("webhook-leaf expiry gauge=%.0fs, want ~1800s (refreshed from the old cert despite the failed rotation write)", webhookRem)
	}

	// The sibling metrics leaf rotated successfully on the same pass, so its
	// gauge reflects a fresh 12h cert — proving a partial failure doesn't
	// short-circuit the remaining leaves.
	metricsRem := testutil.ToFloat64(metrics.CertExpirySeconds.WithLabelValues(expiryLabelMetricsLeaf))
	if metricsRem < 11*3600 {
		t.Errorf("metrics-leaf expiry gauge=%.0fs, want ~43200s (sibling leaf still rotated on a partial-failure pass)", metricsRem)
	}

	// Retry escalates below the historical flat 1h now that a served leaf is
	// within 30 min of expiry, but never below the hot-spin floor.
	if next >= errorRetryMax {
		t.Errorf("retry interval=%s, want < %s (escalated because a leaf is near expiry)", next, errorRetryMax)
	}
	if next < errorRetryMin {
		t.Errorf("retry interval=%s, want >= %s (hot-spin floor)", next, errorRetryMin)
	}
}

func TestCreateSecret_AlreadyExists_NotAnError(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	preexisting := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "velkir",
			Name:      CASecretName,
		},
		// Empty Data on purpose: mirrors the "another replica created the
		// shell, hadn't populated content yet" race window.
	}
	a, _ := newAuthority(t, now, preexisting)

	mat, err := GenerateCA(now, time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if err := a.createSecret(context.Background(), CASecretName, CertRoleCA, mat, mat.CertPEM); err != nil {
		t.Errorf("createSecret returned error on AlreadyExists: %v", err)
	}
}
