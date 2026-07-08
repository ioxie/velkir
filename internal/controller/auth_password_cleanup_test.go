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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/logging"
)

// authSecretLong returns a Secret carrying a password ≥ MinTokenLen so
// the redaction registry actually registers it (the contract under
// test). The byte slice must already be ≥ logging.MinTokenLen long.
func authSecretLong(name string, password []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shortPwdTestNamespace},
		Data:       map[string][]byte{"password": password},
	}
}

// defaultRegistryHas returns true if the process-wide
// logging.DefaultRegistry currently lists token. Used to assert the
// auto-register / auto-cleanup contract end-to-end without injecting
// a fake registry (the production path uses DefaultRegistry directly).
//
// Tests using this helper MUST register against unique passwords
// (e.g., per-test prefixes) — the registry is process-wide and
// refcounted, so two tests reusing the same token would both appear
// in Snapshot() and cross-talk on assertion checks (especially under
// -parallel).
func defaultRegistryHas(token string) bool {
	return slices.Contains(logging.DefaultRegistry.Snapshot(), token)
}

// TestLookupAuthPassword_AutoRegistersInDefaultRegistry pins the
// load-bearing contract: a successful lookup MUST leave
// the password registered in DefaultRegistry, and invoking the
// returned cleanup MUST evict it.
func TestLookupAuthPassword_AutoRegistersInDefaultRegistry(t *testing.T) {
	// Unique-per-test password (well above MinTokenLen) so concurrent
	// tests sharing DefaultRegistry don't collide on the snapshot
	// check.
	const pwd = "i347-reconciler-pwd-3f9a2c-LONGENOUGH"

	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	secret := authSecretLong("vk-auth", []byte(pwd))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	got, cleanup, err := r.lookupAuthPassword(context.Background(), cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on success path")
	}
	// Hygiene fallback: if any assertion below fires t.Fatal before
	// the explicit cleanup() runs, t.Cleanup still evicts the token
	// from DefaultRegistry. Idempotent — Forget on an already-evicted
	// token is a no-op. The explicit cleanup() below is what the test
	// asserts on; this defer-via-Cleanup is just leak prevention.
	t.Cleanup(cleanup)

	if got != pwd {
		t.Fatalf("password mismatch: got %q want %q", got, pwd)
	}
	if !defaultRegistryHas(pwd) {
		t.Fatal("DefaultRegistry missing password after lookup; auto-register did not fire")
	}

	// Explicit invocation IS the assertion-driving call: the test
	// pins that calling cleanup() actually evicts the registration.
	cleanup()
	if defaultRegistryHas(pwd) {
		t.Fatal("DefaultRegistry still has password after cleanup; cleanup did not Forget")
	}
}

// TestLookupAuthPassword_CleanupNonNilOnNoAuth pins the no-auth
// path's cleanup contract: even when no Secret read happens, the
// caller must receive a non-nil cleanup so `defer cleanup()` is
// always safe (no nil-func panic).
func TestLookupAuthPassword_CleanupNonNilOnNoAuth(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: shortPwdTestNamespace},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			// Auth: nil
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	pwd, cleanup, err := r.lookupAuthPassword(context.Background(), cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if pwd != "" {
		t.Errorf("expected empty password on no-auth, got %q", pwd)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on no-auth path")
	}
	cleanup() // must be safe to invoke (no-op)
}

// TestLookupAuthPassword_CleanupNonNilOnSecretReadError pins the
// error-path cleanup contract: a Secret read failure must still
// produce a non-nil cleanup so `defer cleanup()` placed before the
// err check stays safe.
func TestLookupAuthPassword_CleanupNonNilOnSecretReadError(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	// CR references a Secret that the fake client doesn't know about
	// → Get returns NotFound.
	cr := crWithAuthSecret("vk", "missing-auth")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	pwd, cleanup, err := r.lookupAuthPassword(context.Background(), cr)
	if err == nil {
		t.Fatal("expected NotFound error, got nil")
	}
	if pwd != "" {
		t.Errorf("expected empty password on error, got %q", pwd)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on error path")
	}
	cleanup() // must be safe to invoke (no-op)
}

// TestSentinelStartupReset_lookupAuthPassword_AutoRegistersInDefaultRegistry
// mirrors TestLookupAuthPassword_AutoRegistersInDefaultRegistry for
// the SentinelStartupReset receiver. The two functions duplicate the
// password-resolver path (the reconciler's lookupAuthPassword is
// unexported on *ValkeyReconciler), so the contract must be pinned on
// both receivers — otherwise the safety-net path can silently drift
// from the reconciler path.
func TestSentinelStartupReset_lookupAuthPassword_AutoRegistersInDefaultRegistry(t *testing.T) {
	const pwd = "i347-startupreset-pwd-7b4e1d-LONGENOUGH"

	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "vk-auth")
	secret := authSecretLong("vk-auth", []byte(pwd))

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr, secret).Build()
	s := &SentinelStartupReset{Client: c}

	got, cleanup, err := s.lookupAuthPassword(context.Background(), cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on success path")
	}
	// Hygiene fallback — see the reconciler-side variant for rationale.
	t.Cleanup(cleanup)

	if got != pwd {
		t.Fatalf("password mismatch: got %q want %q", got, pwd)
	}
	if !defaultRegistryHas(pwd) {
		t.Fatal("DefaultRegistry missing password after SentinelStartupReset lookup; auto-register did not fire")
	}

	cleanup()
	if defaultRegistryHas(pwd) {
		t.Fatal("DefaultRegistry still has password after cleanup; cleanup did not Forget")
	}
}

// TestSentinelStartupReset_lookupAuthPassword_CleanupNonNilOnNoAuth
// mirrors TestLookupAuthPassword_CleanupNonNilOnNoAuth for the
// SentinelStartupReset receiver — closes the symmetry gap so a
// regression on the safety-net path's no-auth branch can't slip past
// the reconciler-only test.
func TestSentinelStartupReset_lookupAuthPassword_CleanupNonNilOnNoAuth(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: shortPwdTestNamespace},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			// Auth: nil
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	s := &SentinelStartupReset{Client: c}

	pwd, cleanup, err := s.lookupAuthPassword(context.Background(), cr)
	if err != nil {
		t.Fatalf("lookupAuthPassword: %v", err)
	}
	if pwd != "" {
		t.Errorf("expected empty password on no-auth, got %q", pwd)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on no-auth path")
	}
	cleanup()
}

// TestSentinelStartupReset_lookupAuthPassword_CleanupNonNilOnError
// pins the error-path cleanup for the safety-net receiver — same
// guarantee as the reconciler version.
func TestSentinelStartupReset_lookupAuthPassword_CleanupNonNilOnError(t *testing.T) {
	scheme := shortPasswordTestScheme(t)
	cr := crWithAuthSecret("vk", "missing-auth")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	s := &SentinelStartupReset{Client: c}

	pwd, cleanup, err := s.lookupAuthPassword(context.Background(), cr)
	if err == nil {
		t.Fatal("expected NotFound error, got nil")
	}
	if pwd != "" {
		t.Errorf("expected empty password on error, got %q", pwd)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on error path")
	}
	cleanup()
}

// TestLookupAuthPasswordWithRedaction_RegistersSentinelAuthPassword pins
// the dedup drift fix: when spec.auth.sentinelAuthSecretName is set,
// the shared helper registers BOTH the master and the sentinel-auth
// password for redaction (the sentinel startup-reset path previously
// registered only the master password, leaving the sentinel-auth value
// unscrubbed). Asserted against the shared helper directly so it covers
// both the reconciler and the startup-reset call sites.
func TestLookupAuthPasswordWithRedaction_RegistersSentinelAuthPassword(t *testing.T) {
	const masterPwd = "i495-master-pwd-7c1d4e-LONGENOUGH"
	const sentinelPwd = "i495-sentinel-pwd-9b2f8a-LONGENOUGH"

	scheme := shortPasswordTestScheme(t)
	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: shortPwdTestNamespace},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Auth: &valkeyv1beta1.AuthSpec{
				SecretName:             "vk-auth",
				SentinelAuthSecretName: "vk-sentinel-auth",
			},
		},
	}
	masterSecret := authSecretLong("vk-auth", []byte(masterPwd))
	sentinelSecret := authSecretLong("vk-sentinel-auth", []byte(sentinelPwd))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, masterSecret, sentinelSecret).Build()

	// reporter nil is fine — reportShortAuthPassword is nil-safe.
	got, cleanup, err := lookupAuthPasswordWithRedaction(context.Background(), c, nil, cr)
	if err != nil {
		t.Fatalf("lookupAuthPasswordWithRedaction: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup must be non-nil on success path")
	}
	t.Cleanup(cleanup)

	if got != masterPwd {
		t.Fatalf("master password mismatch: got %q want %q", got, masterPwd)
	}
	if !defaultRegistryHas(masterPwd) {
		t.Error("DefaultRegistry missing MASTER password after lookup")
	}
	if !defaultRegistryHas(sentinelPwd) {
		t.Error("DefaultRegistry missing SENTINEL-auth password after lookup (the #495 drift this fix closes)")
	}

	cleanup()
	if defaultRegistryHas(masterPwd) {
		t.Error("DefaultRegistry still has master password after cleanup")
	}
	if defaultRegistryHas(sentinelPwd) {
		t.Error("DefaultRegistry still has sentinel-auth password after cleanup; composite cleanup did not Forget both")
	}
}
