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

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
)

// TestLogsHaveNoRawPasswords feeds known-secret material into every
// shape the operator's log surface
// emits — message string, structured field, error wrapping, derived
// logger via WithValues — and assert no raw password reaches the encoder.
//
// The test wires the redacting core into ctrl.SetLogger so it exercises
// the same path the manager uses at runtime (operator code calls
// ctrl.Log.WithName(...).Info(...) — the test must intercept at exactly
// that boundary). Because ctrl.SetLogger mutates process-wide state,
// the previous logger is restored via t.Cleanup so neighbouring tests
// aren't poisoned.
func TestLogsHaveNoRawPasswords(t *testing.T) {
	const (
		secretAuth     = "primary-pass-rosebud-2026"
		secretSentinel = "sentinel-pass-citizenkane-1941"
		secretInError  = "embedded-in-err-tex-avery-1942"
		secretInBound  = "bound-via-with-fritz-lang-1931"
	)

	// Register all four secrets with the package's DefaultRegistry — the
	// same registry the production New() factory wires the redactor
	// against. Cleanup restores prior state so other tests aren't
	// affected.
	for _, s := range []string{secretAuth, secretSentinel, secretInError, secretInBound} {
		DefaultRegistry.Register(s)
		t.Cleanup(func() { DefaultRegistry.Forget(s) })
	}

	// Capture log output to a buffer via a redacting Core wrapped around
	// a JSON-encoding inner Core. zapr translates the underlying
	// *zap.Logger into the logr.Logger ctrl.SetLogger expects.
	var buf bytes.Buffer
	prev := ctrl.Log
	ctrl.SetLogger(buildRedactingTestLogger(&buf, DefaultRegistry))
	t.Cleanup(func() { ctrl.SetLogger(prev) })

	log := ctrl.Log.WithName("test-redaction")

	// Shape 1: secret in the message string (the most common
	// accidental-leak path: fmt.Sprintf carrying a credential).
	log.Info(fmt.Sprintf("connecting with auth=%s", secretAuth))

	// Shape 2: secret in a structured field via key/value pairs.
	log.Info("rendering config", "auth", secretSentinel)

	// Shape 3: secret embedded in an error returned from a downstream
	// call (the path that actually motivated this stage — third-party
	// libraries often print the URL or AUTH fragment in error text).
	log.Error(errors.New("AUTH "+secretInError+" rejected"), "primary handshake failed")

	// Shape 4: secret bound at WithValues time, then a derived logger
	// emits an unrelated message. Tests the bind-time scrub.
	derived := log.WithValues("auth-context", "user=admin pass="+secretInBound)
	derived.Info("derived emission")

	out := buf.String()

	for _, leaked := range []string{secretAuth, secretSentinel, secretInError, secretInBound} {
		if strings.Contains(out, leaked) {
			t.Errorf("raw secret %q leaked into log output:\n%s", leaked, out)
		}
	}

	// And the placeholder MUST appear at least once, otherwise the test
	// passes for the wrong reason — e.g. the captured buffer is empty
	// because the logger wasn't actually wired through.
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("redaction placeholder %q never appeared; logger was not exercised:\n%s",
			RedactedPlaceholder, out)
	}
}

// TestLogsBoundBeforeRegister_NotRetroactivelyRedacted pins the With-time
// snapshot limitation called out in the package doc: a token registered
// AFTER WithValues bound the field does NOT retroactively redact the
// already-bound value. zapr's WithValues binds the accumulated kv pairs
// onto the underlying zap.Logger via .With (zapr.go:213), which routes
// through this Core's With (snapshot semantics) rather than per-call
// fields through Write (live semantics).
//
// The test exists so a future change to the snapshot semantics — e.g.,
// switching to live-pointer reads on bound fields, or zapr changing its
// internal WithValues path — trips here loudly and forces an explicit
// doc update, rather than silently shifting the contract under callers
// who rely on the documented "bind CR identifiers into the logger
// context, log credentials inline" discipline.
//
// Corollary tested: Message-level redaction on the same derived logger
// DOES pick up late-registered tokens via the shared Registry pointer
// — the snapshot is per-bound-field, not per-derived-logger.
//
// The test uses an isolated Registry and the logr.Logger returned from
// buildRedactingTestLogger directly (NOT ctrl.SetLogger / ctrl.Log) to
// avoid the controller-runtime delegating sink's interaction with prior
// tests' Cleanup ordering. The TestLogsHaveNoRawPasswords sibling does
// exercise the ctrl.SetLogger wiring path, so the production wireup
// path remains covered.
func TestLogsBoundBeforeRegister_NotRetroactivelyRedacted(t *testing.T) {
	const (
		preBindSecret = "bound-before-register-aaaaaaaa"
		lateMsgSecret = "registered-after-bind-bbbbbbbb"
	)

	registry := NewRegistry()
	var buf bytes.Buffer
	log := buildRedactingTestLogger(&buf, registry)

	// Bind FIRST, register SECOND — the documented limitation.
	boundLog := log.WithName("post-bind-register").WithValues(
		"auth", "user=admin pass="+preBindSecret,
	)

	registry.Register(preBindSecret)
	registry.Register(lateMsgSecret)

	// Bound field captured pre-register: limitation says it leaks.
	boundLog.Info("emit with pre-bound creds")
	out := buf.String()
	if !strings.Contains(out, preBindSecret) {
		t.Fatalf("With-time snapshot semantics changed: pre-bind-registered secret %q "+
			"was redacted in bound field. If this is a deliberate semantics change, "+
			"update the package doc + redact.go's With godoc and remove this test.\n%s",
			preBindSecret, out)
	}

	// Corollary: Message-level redaction on the same derived logger DOES
	// scrub late-registered tokens — the snapshot is per-bound-field,
	// not per-derived-logger.
	buf.Reset()
	boundLog.Info("late-secret-leak " + lateMsgSecret)
	out = buf.String()
	if strings.Contains(out, lateMsgSecret) {
		t.Errorf("late-registered token leaked into Message of derived logger; "+
			"shared Registry pointer should pick it up at Write time:\n%s", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Errorf("expected %s placeholder for late-registered Message token:\n%s",
			RedactedPlaceholder, out)
	}
}

// buildRedactingTestLogger constructs a logr.Logger that mirrors the
// production wiring shape: zap JSON encoder, redacting core wrap via
// uberzap.WrapCore, zapr translation to logr. Targeting buf instead of
// stderr is the only deviation from logging.New().
func buildRedactingTestLogger(buf *bytes.Buffer, registry *Registry) logr.Logger {
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	})
	inner := zapcore.NewCore(enc, zapcore.AddSync(buf), zapcore.DebugLevel)
	zl := uberzap.New(inner, uberzap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return newRedactingCore(c, registry)
	}))
	return zapr.NewLogger(zl)
}
