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

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// shortPasswordTestScheme builds a minimal scheme for the short-
// password emission tests: corev1 (Secret) + valkeyv1beta1 (CR).
func shortPasswordTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 add to scheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1 add to scheme: %v", err)
	}
	return s
}

// shortPwdTestNamespace is the canonical namespace for all
// short-password test fixtures — every CR + Secret in this file lives
// in the same namespace, so hoisting the literal keeps the helpers
// minimal without flunking the unparam linter.
const shortPwdTestNamespace = "ns"

// crWithAuthSecret returns a CR with auth.secretName wired to the
// given Secret name. Tests vary the Secret content via authSecret.
func crWithAuthSecret(name, secretName string) *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shortPwdTestNamespace,
		},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			Auth: &valkeyv1beta1.AuthSpec{
				SecretName: secretName,
			},
		},
	}
}

func authSecretShort(name string, password []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shortPwdTestNamespace},
		Data:       map[string][]byte{"password": password},
	}
}

// TestLookupAuthPassword_EmitsWarningOnShortPassword pins the core
// integration: when the auth Secret carries a sub-MinTokenLen
// password, lookupAuthPassword emits AuthSecretShortPassword exactly
// once per (CR, secretName) tuple per reporter lifetime.
func TestLookupAuthPassword_EmitsWarningOnShortPassword(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	// 5 chars — under MinTokenLen=8, non-empty.
	secret := authSecretShort("vk-auth", []byte("short"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: events.NewShortAuthPasswordReporter(rec),
	}

	pwd, err := lookupAuthPasswordForTest(t, r, cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword returned error: %v", err)
	}
	if pwd != "short" {
		t.Fatalf("expected password %q, got %q", "short", pwd)
	}

	// First lookup should have emitted exactly one warning.
	first := drainShortPwdRecorder(rec)
	if len(first) != 1 {
		t.Fatalf("first lookup: expected 1 event, got %d: %v", len(first), first)
	}
	got := first[0]
	if !strings.Contains(got, string(events.AuthSecretShortPassword)) {
		t.Errorf("event missing reason AuthSecretShortPassword: %q", got)
	}
	if !strings.Contains(got, "vk-auth") {
		t.Errorf("event missing secret name: %q", got)
	}
	if !strings.Contains(got, corev1.EventTypeWarning) {
		t.Errorf("event should be Warning type: %q", got)
	}
	// Defensive: the password value itself must NOT appear.
	if strings.Contains(got, "short") {
		// "short" is a 5-char ordinary English word that could appear
		// in event prose; pin only that we don't echo back the literal
		// password VALUE in a way that'd survive any future template
		// rephrasing.
		if strings.Contains(got, "password \"short\"") || strings.Contains(got, "password=short") {
			t.Errorf("event leaked password value: %q", got)
		}
	}

	// Second lookup with the same CR + secretName must NOT re-emit
	// (process-lifetime dedup per the reporter contract).
	if _, err := lookupAuthPasswordForTest(t, r, cr); err != nil {
		t.Fatalf("second lookupAuthPassword returned error: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 0 {
		t.Errorf("second lookup should be deduped, but got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_EmitsAtLengthBelowBoundary pins the
// off-by-one defense: MinTokenLen=8, so length=7 (one below) MUST
// trigger emission. If the comparison were ever flipped to
// `pwdLen <= MinTokenLen` or `pwdLen <= MinTokenLen-1`, this test
// would catch it.
func TestLookupAuthPassword_EmitsAtLengthBelowBoundary(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	// MinTokenLen-1 = 7 — must trigger.
	secret := authSecretShort("vk-auth", []byte("1234567"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: events.NewShortAuthPasswordReporter(rec),
	}

	if _, err := lookupAuthPasswordForTest(t, r, cr); err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 1 {
		t.Errorf("7-char password (MinTokenLen-1) MUST trigger emission, got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_NoEmitOnHealthyPassword pins the steady-
// state path: a password meeting MinTokenLen produces zero events.
func TestLookupAuthPassword_NoEmitOnHealthyPassword(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	// Exactly MinTokenLen=8 — boundary case, must NOT trigger.
	secret := authSecretShort("vk-auth", []byte("12345678"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: events.NewShortAuthPasswordReporter(rec),
	}

	if _, err := lookupAuthPasswordForTest(t, r, cr); err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 0 {
		t.Errorf("8-char password should NOT trigger emission, got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_NoEmitOnNoAuth pins the no-auth path: when
// the CR has no spec.auth set, the resolver returns "" with no
// emission (no Secret to inspect, no smell to report).
func TestLookupAuthPassword_NoEmitOnNoAuth(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			// Auth: nil
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: events.NewShortAuthPasswordReporter(rec),
	}

	pwd, err := lookupAuthPasswordForTest(t, r, cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if pwd != "" {
		t.Errorf("expected empty password for no-auth CR, got %q", pwd)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 0 {
		t.Errorf("no-auth CR should NOT trigger emission, got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_NoEmitOnEmptyPasswordKey pins the case
// where the Secret exists but its `password` data key is missing or
// empty. Returning an empty password is the existing contract; the
// short-password reporter MUST NOT fire on length-zero passwords —
// that's an absent credential, not a configuration smell of "too
// short to redact."
func TestLookupAuthPassword_NoEmitOnEmptyPasswordKey(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	secret := authSecretShort("vk-auth", []byte{}) // empty value

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	rec := k8sevents.NewFakeRecorder(10)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: events.NewShortAuthPasswordReporter(rec),
	}

	if _, err := lookupAuthPasswordForTest(t, r, cr); err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 0 {
		t.Errorf("empty password should NOT trigger emission, got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_SeparateCRsBothEmit pins that the
// per-(CR, secretName) dedup tuple is correctly scoped — two
// distinct CRs sharing the same reporter both emit.
func TestLookupAuthPassword_SeparateCRsBothEmit(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr1 := crWithAuthSecret("vk1", "vk1-auth")
	cr2 := crWithAuthSecret("vk2", "vk2-auth")
	s1 := authSecretShort("vk1-auth", []byte("short"))
	s2 := authSecretShort("vk2-auth", []byte("short"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr1, cr2, s1, s2).Build()
	rec := k8sevents.NewFakeRecorder(10)
	reporter := events.NewShortAuthPasswordReporter(rec)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: reporter,
	}

	if _, err := lookupAuthPasswordForTest(t, r, cr1); err != nil {
		t.Fatalf("cr1 lookup: %v", err)
	}
	if _, err := lookupAuthPasswordForTest(t, r, cr2); err != nil {
		t.Fatalf("cr2 lookup: %v", err)
	}

	got := drainShortPwdRecorder(rec)
	if len(got) != 2 {
		t.Fatalf("two distinct CRs should each emit once: got %d events: %v", len(got), got)
	}
}

// TestReconcileDeletion_ClearsShortPasswordDedup pins the
// dedup-contract behaviour on CR deletion: when reconcileDeletion
// removes the finalizer, it must also call Forget on the reporter
// so a recreated CR with the same name+namespace re-emits on its
// first reconcile rather than inheriting the prior CR's silenced
// state.
func TestReconcileDeletion_ClearsShortPasswordDedup(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	// reconcileDeletion expects the CR to carry the finalizer it
	// removes; otherwise the finalizer-removal branch is skipped.
	cr.Finalizers = []string{PVCRetentionFinalizer}
	secret := authSecretShort("vk-auth", []byte("short"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	rec := k8sevents.NewFakeRecorder(10)
	reporter := events.NewShortAuthPasswordReporter(rec)
	r := &ValkeyReconciler{
		Client:                    c,
		Scheme:                    scheme,
		Recorder:                  rec,
		ShortAuthPasswordReporter: reporter,
	}

	// Step 1: first lookup emits the warning and seeds the dedup
	// entry for (ns/vk/vk-auth).
	if _, err := lookupAuthPasswordForTest(t, r, cr); err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 1 {
		t.Fatalf("expected 1 event after first lookup, got %d", len(got))
	}

	// Step 2: reconcileDeletion runs — must Forget the dedup entry.
	if err := r.reconcileDeletion(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}

	// Step 3: a fresh CR with the same name+namespace+secretName
	// MUST re-emit on its first lookup (dedup cleared by Forget).
	cr2 := crWithAuthSecret("vk", "vk-auth")
	if _, err := lookupAuthPasswordForTest(t, r, cr2); err != nil {
		t.Fatalf("post-delete lookup: %v", err)
	}
	if got := drainShortPwdRecorder(rec); len(got) != 1 {
		t.Errorf("recreated CR with same identity must re-emit (Forget should have cleared dedup), got %d events: %v", len(got), got)
	}
}

// TestLookupAuthPassword_NilReporterIsNoopAndNonFatal pins the
// nil-safety contract: a reconciler without a reporter wired
// continues to resolve auth passwords without panicking, just
// without the secondary surfacing.
func TestLookupAuthPassword_NilReporterIsNoopAndNonFatal(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	secret := authSecretShort("vk-auth", []byte("short"))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	r := &ValkeyReconciler{
		Client: c,
		Scheme: scheme,
		// Recorder + ShortAuthPasswordReporter both nil
	}

	pwd, err := lookupAuthPasswordForTest(t, r, cr)
	if err != nil {
		t.Fatalf("nil-reporter lookup must not error: %v", err)
	}
	if pwd != "short" {
		t.Errorf("nil-reporter still returns password: got %q", pwd)
	}
}

// lookupAuthPasswordForTest wraps r.lookupAuthPassword for tests:
// invokes the call against context.Background(), registers the
// returned cleanup via t.Cleanup so each test releases its
// DefaultRegistry registration (process-wide singleton — leaks would
// pollute Snapshot across tests), and returns the (pwd, err) pair so
// test bodies stay compact.
func lookupAuthPasswordForTest(t *testing.T, r *ValkeyReconciler, v *valkeyv1beta1.Valkey) (string, error) {
	t.Helper()
	pwd, cleanup, err := r.lookupAuthPassword(context.Background(), v)
	t.Cleanup(cleanup)
	return pwd, err
}

// drainShortPwdRecorder mirrors drainRecorder from
// internal/events/deprecation_test.go but exists here because
// FakeRecorder's channel is in the events test package and not
// re-exported.
func drainShortPwdRecorder(rec *k8sevents.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-rec.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}
