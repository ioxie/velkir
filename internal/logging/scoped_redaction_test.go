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

package logging

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestRegisterScoped_RedactsAuthErrorEmissions pins the integration path
// the reconciler relies on: scope-register a password via RegisterScoped,
// log an AUTH-style error whose body carries the password (mimicking the
// `fmt.Errorf("AUTH: %w", err)` and `fmt.Errorf("AUTH: unexpected reply
// %v", reply)` shapes in internal/sentinel/conn.go and
// internal/valkey/lagchecker.go), and assert the placeholder appears
// instead of the raw secret. Catches regressions in the RegisterScoped
// → DefaultRegistry → redacting Core wiring without depending on the
// reconciler's runtime construction.
//
// Uses an isolated logr.Logger (NOT ctrl.SetLogger / ctrl.Log) to dodge
// controller-runtime's once-only delegating-sink semantics, and an
// isolated *Registry (NOT DefaultRegistry) so a test failure mid-flight
// can't leave a stale registration on the package singleton — the
// TestLogsHaveNoRawPasswords sibling exercises the DefaultRegistry +
// ctrl.SetLogger production-wiring path, so that route remains covered.
func TestRegisterScoped_RedactsAuthErrorEmissions(t *testing.T) {
	const password = "auth-error-path-secret-2026abcd"

	registry := NewRegistry()
	cleanup := registry.RegisterScoped(password)
	t.Cleanup(cleanup)

	var buf bytes.Buffer
	log := buildRedactingTestLogger(&buf, registry).WithName("test-scoped-redaction")

	// Mirror the two leak shapes the issue calls out: a fmt-wrapped error
	// returned from authIfNeeded ("AUTH: <inner>") and the unexpected-
	// reply printf where %v could echo the password.
	wrapped := fmt.Errorf("AUTH: %w", errors.New("ERR password rejected: "+password))
	log.Error(wrapped, "primary handshake failed")

	unexpected := fmt.Errorf("AUTH: unexpected reply %v", password)
	log.Error(unexpected, "sentinel handshake failed")

	out := buf.String()
	if strings.Contains(out, password) {
		t.Errorf("password leaked through AUTH error path:\n%s", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("redaction placeholder %q never appeared; logger may not be wired:\n%s",
			RedactedPlaceholder, out)
	}
}

// TestRegisterScoped_AfterCleanupNoRedaction confirms cleanup actually
// drops the registration — once the deferred cleanup runs, subsequent
// log emissions of the same password are NOT redacted. Pins the
// reference-counted Forget contract on the wrapper helper.
func TestRegisterScoped_AfterCleanupNoRedaction(t *testing.T) {
	const password = "scoped-after-cleanup-secret-12345"

	registry := NewRegistry()
	cleanup := registry.RegisterScoped(password)
	cleanup()

	var buf bytes.Buffer
	log := buildRedactingTestLogger(&buf, registry).WithName("test-after-cleanup")

	log.Info("AUTH error: " + password)

	if !strings.Contains(buf.String(), password) {
		t.Errorf("expected password to leak (no longer registered) but it did not:\n%s", buf.String())
	}
}
