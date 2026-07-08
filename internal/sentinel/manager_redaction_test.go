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

package sentinel

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ioxie/velkir/internal/logging"
)

// snapshotContains reports whether logging.DefaultRegistry currently
// has token registered. Used by the lifecycle tests below to assert
// the Manager's Register/Forget pairs without inspecting unexported
// refcounts directly.
func snapshotContains(t *testing.T, token string) bool {
	t.Helper()
	return slices.Contains(logging.DefaultRegistry.Snapshot(), token)
}

// uniquePassword returns a per-test password long enough to clear
// MinTokenLen and unique per t.Name so concurrent test runs don't
// collide on the package-level DefaultRegistry.
func uniquePassword(t *testing.T) string {
	t.Helper()
	// Sanitize t.Name (slashes from subtest names) into a token
	// shape that cannot accidentally match log content.
	name := strings.ReplaceAll(t.Name(), "/", "-")
	return "redact-" + name + "-aabbccdd"
}

// TestManager_EnsureRegistersPasswordSurvivesReconcileScopedForget
// pins the load-bearing claim: after Manager.Ensure runs,
// the observer's password is held in the redaction registry under
// the manager's own registration. The reconcile-scoped Forget that
// fires on reconcile return does NOT evict the token because the
// manager-side registration keeps the refcount above zero. Only
// Remove (or Start drain on operator shutdown) drops it.
//
// Without this contract, observer goroutines emitting AUTH errors
// post-reconcile would slip the password through the redactor —
// the gap this contract was filed against.
func TestManager_EnsureRegistersPasswordSurvivesReconcileScopedForget(t *testing.T) {
	password := uniquePassword(t)
	t.Cleanup(func() {
		// Belt-and-braces eviction in case a test failure stranded
		// a registration on the package singleton; idempotent over
		// the refcount.
		for snapshotContains(t, password) {
			logging.DefaultRegistry.Forget(password)
		}
	})

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 20 {
		queuePollReplies(fs, true)
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}

	// Mirror the reconciler's RegisterScoped pattern: the
	// reconciler reads the Secret and registers the password for
	// the duration of its pass.
	reconcileForget := logging.DefaultRegistry.RegisterScoped(password)

	if err := m.Ensure(context.Background(), cr, "vk0", password, endpoints); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !snapshotContains(t, password) {
		t.Fatal("password missing from registry after Ensure (expected reconcile + manager registrations)")
	}

	// Reconcile returns: deferred RegisterScoped cleanup runs.
	reconcileForget()

	// The manager's own Register keeps the password alive — the
	// observer goroutines are still running and may still emit
	// AUTH errors that need redaction.
	if !snapshotContains(t, password) {
		t.Fatal("password evicted after reconcile-scoped Forget — Manager.Ensure did not register on its own behalf")
	}

	// Symmetric Remove drops the manager's registration. With no
	// other holders, the token is gone from the registry.
	m.Remove(cr)
	if snapshotContains(t, password) {
		t.Fatal("password still in registry after Manager.Remove — Forget pair missing or refcount stuck")
	}
}

// TestManager_EnsureSamePasswordIdempotentDoesNotLeakRefcount checks
// that re-Ensure with identical (masterName, password, endpoints)
// is a no-op for the registry too — the observerConfigEqual short-
// circuit must skip the Register/Forget pair so repeated reconciles
// don't blow the refcount unboundedly.
func TestManager_EnsureSamePasswordIdempotentDoesNotLeakRefcount(t *testing.T) {
	password := uniquePassword(t)
	t.Cleanup(func() {
		for snapshotContains(t, password) {
			logging.DefaultRegistry.Forget(password)
		}
	})

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 40 {
		queuePollReplies(fs, true)
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}

	if err := m.Ensure(context.Background(), cr, "vk0", password, endpoints); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	// Three more identical Ensures — every one must short-circuit
	// at observerConfigEqual without touching the registry.
	for range 3 {
		if err := m.Ensure(context.Background(), cr, "vk0", password, endpoints); err != nil {
			t.Fatalf("repeat Ensure: %v", err)
		}
	}

	// One Remove must fully evict — if the idempotent path leaked
	// extra Registers, the refcount would survive past Remove and
	// the password would still be registered.
	m.Remove(cr)
	if snapshotContains(t, password) {
		t.Fatal("password still in registry after one Remove — repeated Ensure leaked extra Register calls")
	}
}

// TestManager_EnsureSwapsPasswordOnChange pins the rotation
// behaviour: when Ensure sees a different password for the same CR,
// it Registers the new and Forgets the old. After the swap, only
// the new password is held; the old one is fully released.
func TestManager_EnsureSwapsPasswordOnChange(t *testing.T) {
	oldPassword := uniquePassword(t) + "-old"
	newPassword := uniquePassword(t) + "-new"
	t.Cleanup(func() {
		for snapshotContains(t, oldPassword) {
			logging.DefaultRegistry.Forget(oldPassword)
		}
		for snapshotContains(t, newPassword) {
			logging.DefaultRegistry.Forget(newPassword)
		}
	})

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 40 {
		queuePollReplies(fs, true)
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}

	if err := m.Ensure(context.Background(), cr, "vk0", oldPassword, endpoints); err != nil {
		t.Fatalf("Ensure(old): %v", err)
	}
	if !snapshotContains(t, oldPassword) {
		t.Fatal("old password missing from registry after first Ensure")
	}

	if err := m.Ensure(context.Background(), cr, "vk0", newPassword, endpoints); err != nil {
		t.Fatalf("Ensure(new): %v", err)
	}
	if snapshotContains(t, oldPassword) {
		t.Fatal("old password still in registry after password swap — Forget(prev) missing")
	}
	if !snapshotContains(t, newPassword) {
		t.Fatal("new password missing from registry after swap — Register(new) missing")
	}

	m.Remove(cr)
	if snapshotContains(t, newPassword) {
		t.Fatal("new password still in registry after Remove")
	}
}

// TestManager_StartDrainForgetsAllPasswords pins the operator-
// shutdown path: when Manager.Start's context is cancelled, every
// observer's password is evicted from the registry alongside the
// goroutine drain. Without the matched Forget, a long-running
// process whose leader-election loops over the lifetime would
// accumulate every prior password in the redaction set.
func TestManager_StartDrainForgetsAllPasswords(t *testing.T) {
	pwA := uniquePassword(t) + "-A"
	pwB := uniquePassword(t) + "-B"
	t.Cleanup(func() {
		for snapshotContains(t, pwA) {
			logging.DefaultRegistry.Forget(pwA)
		}
		for snapshotContains(t, pwB) {
			logging.DefaultRegistry.Forget(pwB)
		}
	})

	m, cancel, wait := startManager(t)

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 40 {
		queuePollReplies(fs, true)
	}

	if err := m.Ensure(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vkA"},
		"vkA", pwA,
		[]Endpoint{{Name: "vkA-sentinel-0", Addr: fs.Addr()}},
	); err != nil {
		t.Fatalf("Ensure A: %v", err)
	}
	if err := m.Ensure(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vkB"},
		"vkB", pwB,
		[]Endpoint{{Name: "vkB-sentinel-0", Addr: fs.Addr()}},
	); err != nil {
		t.Fatalf("Ensure B: %v", err)
	}

	cancel()
	wait()

	if snapshotContains(t, pwA) {
		t.Error("pwA still in registry after Start drain")
	}
	if snapshotContains(t, pwB) {
		t.Error("pwB still in registry after Start drain")
	}
}

// TestManager_EmptyPasswordIsRegistryNoop confirms a no-AUTH CR
// (empty password) doesn't pollute the registry. Register/Forget
// short-circuit on len < MinTokenLen, so an empty-password Ensure
// → Remove sequence touches zero registry slots.
func TestManager_EmptyPasswordIsRegistryNoop(t *testing.T) {
	startLen := logging.DefaultRegistry.Len()

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 20 {
		queuePollReplies(fs, true)
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := m.Ensure(context.Background(), cr, "vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got := logging.DefaultRegistry.Len(); got != startLen {
		t.Errorf("registry grew on no-AUTH Ensure: start=%d got=%d", startLen, got)
	}
	m.Remove(cr)
	if got := logging.DefaultRegistry.Len(); got != startLen {
		t.Errorf("registry size shifted across no-AUTH Ensure/Remove: start=%d got=%d", startLen, got)
	}
}

// TestManager_PostReconcileLogIsRedacted is the integration shape
// of the load-bearing test: with the Ensure-side Register in
// effect, a log emission AFTER the reconcile-scoped Forget has run
// (mimicking an observer goroutine emitting an AUTH error past
// the reconcile boundary) is still redacted by the SAME redacting
// Core production wires through logging.New().
//
// The buffered logger uses logging.WrapWithRedaction against
// DefaultRegistry — same shape production uses — so a divergence
// between the registered token and what the production redactor
// actually scrubs would surface here.
func TestManager_PostReconcileLogIsRedacted(t *testing.T) {
	password := uniquePassword(t)
	t.Cleanup(func() {
		for snapshotContains(t, password) {
			logging.DefaultRegistry.Forget(password)
		}
	})

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs := newFakeSentinel(t)
	defer fs.Stop()
	queuePsubscribeAcks(fs)
	for range 20 {
		queuePollReplies(fs, true)
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}

	// Reconciler's Register would be the only registration in the
	// pre-fix shape; once its deferred Forget fires, observer
	// goroutines emitting AUTH errors past the reconcile boundary
	// would leak the password.
	reconcileForget := logging.DefaultRegistry.RegisterScoped(password)
	if err := m.Ensure(context.Background(), cr, "vk0", password, endpoints); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	reconcileForget()

	// Build a buffered logger using the SAME redacting Core
	// production uses (logging.WrapWithRedaction). A divergence
	// between the registered token and what the redactor actually
	// scrubs (field handling, escape semantics, level gating) would
	// fail here.
	var buf bytes.Buffer
	log := buildBufferedRedactingLogger(&buf, logging.DefaultRegistry)

	log.Error(nil, "AUTH error: "+password)
	out := buf.String()
	if strings.Contains(out, password) {
		t.Errorf("post-reconcile observer-style emission leaked password:\n%s", out)
	}
	if !strings.Contains(out, logging.RedactedPlaceholder) {
		t.Errorf("redaction placeholder %q never appeared:\n%s",
			logging.RedactedPlaceholder, out)
	}

	m.Remove(cr)

	// After Remove the registry no longer holds this password
	// (nothing else has it, refcount=0). A subsequent emission of
	// the same string via the same logger leaks — the production
	// redactor scrubs against the live Snapshot, which now omits
	// the password.
	buf.Reset()
	log.Error(nil, "AUTH error: "+password)
	if !strings.Contains(buf.String(), password) {
		t.Errorf("expected password to leak after Remove (no longer registered) but it did not:\n%s", buf.String())
	}
}

// TestManager_EnsureValidationErrorDoesNotTouchRegistry pins the
// fast-fail invariant: when Ensure rejects bad inputs (empty
// masterName / empty endpoints) before any observer is created,
// the registry is not touched. Without this, a refactor that
// hoisted the Register call before the validation block would
// silently leak refcounts on every misconfigured CR.
func TestManager_EnsureValidationErrorDoesNotTouchRegistry(t *testing.T) {
	password := uniquePassword(t)
	t.Cleanup(func() {
		for snapshotContains(t, password) {
			logging.DefaultRegistry.Forget(password)
		}
	})

	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// Empty masterName — should error before any registry mutation.
	if err := m.Ensure(context.Background(), cr, "", password,
		[]Endpoint{{Name: "s0", Addr: "127.0.0.1:1"}}); err == nil {
		t.Fatal("expected error on empty masterName")
	}
	if snapshotContains(t, password) {
		t.Error("registry contains password after validation-error path — Register escaped the fast-fail")
	}

	// Empty endpoints — same expectation.
	if err := m.Ensure(context.Background(), cr, "vk0", password, nil); err == nil {
		t.Fatal("expected error on empty endpoints slice")
	}
	if snapshotContains(t, password) {
		t.Error("registry contains password after empty-endpoints path — Register escaped the fast-fail")
	}
}

// buildBufferedRedactingLogger constructs a logr.Logger whose
// scrub layer is the production redacting Core (via
// logging.WrapWithRedaction), routing output to buf. Used by the
// integration test above to assert that the production
// redactor — not a test stub — scrubs observer-style emissions
// past the reconcile boundary.
func buildBufferedRedactingLogger(buf *bytes.Buffer, registry *logging.Registry) logr.Logger {
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	})
	inner := zapcore.NewCore(enc, zapcore.AddSync(buf), zapcore.DebugLevel)
	zl := uberzap.New(inner, uberzap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return logging.WrapWithRedaction(c, registry)
	}))
	return zapr.NewLogger(zl)
}
