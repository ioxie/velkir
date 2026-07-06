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

	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newZapLogger constructs a *zap.Logger that encodes JSON to buf, wrapped
// by the redacting Core driven by registry. Tests assert on the buffer's
// raw bytes — the redactor's contract is "no registered token survives
// to the encoder", which is shape-independent.
func newZapLogger(t *testing.T, buf *bytes.Buffer, registry *Registry) *uberzap.Logger {
	t.Helper()
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	})
	syncer := zapcore.AddSync(buf)
	inner := zapcore.NewCore(enc, syncer, zapcore.DebugLevel)
	wrapped := newRedactingCore(inner, registry)
	return uberzap.New(wrapped)
}

func TestRedact_ScrubsMessage(t *testing.T) {
	r := NewRegistry()
	r.Register("the-password-is-rosebud-1234")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("auth failed: the-password-is-rosebud-1234 was rejected")

	out := buf.String()
	if strings.Contains(out, "rosebud-1234") {
		t.Fatalf("raw token leaked into log output: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
}

func TestRedact_ScrubsStringField(t *testing.T) {
	r := NewRegistry()
	r.Register("field-secret-value-9999")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("authenticating", uberzap.String("auth", "user=admin pass=field-secret-value-9999"))

	out := buf.String()
	if strings.Contains(out, "field-secret-value-9999") {
		t.Fatalf("raw token leaked through string field: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
}

func TestRedact_ScrubsErrorField(t *testing.T) {
	r := NewRegistry()
	r.Register("error-leaked-secret-aaaaaaaa")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	err := errors.New("dial valkey-0:6379: AUTH error-leaked-secret-aaaaaaaa rejected")
	log.Error("connecting to primary", uberzap.Error(err))

	out := buf.String()
	if strings.Contains(out, "error-leaked-secret-aaaaaaaa") {
		t.Fatalf("raw token leaked through error field: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
}

type secretStringer struct{ payload string }

func (s secretStringer) String() string { return s.payload }

func TestRedact_ScrubsStringerField(t *testing.T) {
	r := NewRegistry()
	r.Register("stringer-leaked-bbbbbbbb")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("rendered url",
		uberzap.Stringer("url", secretStringer{payload: "redis://admin:stringer-leaked-bbbbbbbb@valkey-0/0"}))

	out := buf.String()
	if strings.Contains(out, "stringer-leaked-bbbbbbbb") {
		t.Fatalf("raw token leaked through stringer field: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
}

type secretBag struct {
	User string `json:"user"`
	Pass string `json:"pass"`
}

func TestRedact_ScrubsReflectField(t *testing.T) {
	r := NewRegistry()
	r.Register("reflect-leaked-dddddddd")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	bag := secretBag{User: "admin", Pass: "reflect-leaked-dddddddd"}
	log.Info("connecting", uberzap.Reflect("creds", bag))

	out := buf.String()
	if strings.Contains(out, "reflect-leaked-dddddddd") {
		t.Fatalf("raw token leaked through reflect field: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
	// The structured shape collapses to a string-valued field — the
	// safety-net trade-off documented on redactField.
	if !strings.Contains(out, `"creds":"`) {
		t.Fatalf("expected creds field to be re-emitted as a string: %q", out)
	}
}

type secretObject struct{ payload string }

// MarshalLogObject implements zapcore.ObjectMarshaler so a zap.Object
// field exercises the ObjectType code path.
func (s secretObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("payload", s.payload)
	return nil
}

func TestRedact_ScrubsObjectField(t *testing.T) {
	r := NewRegistry()
	r.Register("object-leaked-ffffffff")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("emitting object",
		uberzap.Object("ctx", secretObject{payload: "AUTH=object-leaked-ffffffff issued"}))

	out := buf.String()
	if strings.Contains(out, "object-leaked-ffffffff") {
		t.Fatalf("raw token leaked through object field: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected %s placeholder in output: %q", RedactedPlaceholder, out)
	}
}

type marshalErrorObject struct{ err error }

func (m marshalErrorObject) MarshalLogObject(_ zapcore.ObjectEncoder) error {
	return m.err
}

func TestRedact_ObjectMarshalErrorEmitsPlaceholder(t *testing.T) {
	// When the object marshaler itself fails, we MUST NOT leak the
	// original object into the log — emit the marshal-failed
	// placeholder so a leaked Secret in an unmarshalable struct can't
	// survive via the error path.
	r := NewRegistry()
	r.Register("never-marshaled-gggggggg")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("emitting bad object",
		uberzap.Object("ctx", marshalErrorObject{err: errors.New("never-marshaled-gggggggg in error path")}))

	out := buf.String()
	if strings.Contains(out, "never-marshaled-gggggggg") {
		t.Fatalf("raw token leaked through marshal-error path: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder+":marshal-failed") {
		t.Fatalf("expected marshal-failed placeholder in output: %q", out)
	}
}

type unmarshalableValue struct {
	C chan int // chan can't JSON-marshal — exercises the json.Marshal-fails path
}

func TestRedact_ReflectMarshalErrorEmitsPlaceholder(t *testing.T) {
	r := NewRegistry()
	r.Register("never-marshaled-hhhhhhhh")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	// Pair the unmarshalable value with a registered token that, if
	// the original struct survived, would be visible in some encoding.
	// Even without encoding the struct, the redactor's safety-net
	// must replace the field with the placeholder, not pass the raw
	// value through.
	log.Info("emitting bad reflect",
		uberzap.Reflect("ctx", unmarshalableValue{C: make(chan int)}),
		uberzap.String("hint", "side-channel: never-marshaled-hhhhhhhh"))

	out := buf.String()
	if !strings.Contains(out, RedactedPlaceholder+":marshal-failed") {
		t.Fatalf("expected marshal-failed placeholder for unmarshalable reflect field: %q", out)
	}
	if strings.Contains(out, "never-marshaled-hhhhhhhh") {
		t.Fatalf("string-field token leaked: %q", out)
	}
}

func TestRedact_LeavesNonStringFieldsAlone(t *testing.T) {
	r := NewRegistry()
	r.Register("12345678") // pure-digits, MinTokenLen-length
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	// 12345678 as an int field should NOT be redacted — non-string types
	// pass through unchanged. The redactor is a string-oriented safety
	// net, not a deep walker.
	log.Info("count event", uberzap.Int("count", 12345678))

	out := buf.String()
	if !strings.Contains(out, "12345678") {
		t.Fatalf("integer field was unexpectedly scrubbed: %q", out)
	}
}

func TestRedact_FastPathWhenNoTokens(t *testing.T) {
	r := NewRegistry()
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("nothing to redact: rosebud was here")

	out := buf.String()
	if !strings.Contains(out, "rosebud was here") {
		t.Fatalf("empty registry should leave message untouched: %q", out)
	}
	if strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("empty registry must not insert placeholder: %q", out)
	}
}

func TestRedact_OverlappingTokens(t *testing.T) {
	// "supersecret-12345678" contains "secret-12" as a substring. Either
	// scrub order produces output free of both tokens — we just want
	// to assert the longer token's payload doesn't survive even if the
	// shorter one runs first.
	r := NewRegistry()
	r.Register("supersecret-12345678")
	r.Register("secret-12") // exactly MinTokenLen=8 ... wait, 9 chars. Both ≥ MinTokenLen.
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	log.Info("token: supersecret-12345678 was issued")

	out := buf.String()
	if strings.Contains(out, "supersecret-12345678") {
		t.Fatalf("longer token survived: %q", out)
	}
	if strings.Contains(out, "12345678") {
		// 12345678 is a substring of the long token — once the long
		// token is replaced, this digit run should also be gone.
		t.Fatalf("digit suffix of long token survived: %q", out)
	}
}

func TestRedact_WithBoundFields(t *testing.T) {
	// Bound fields go through With() — the redactor must scrub them at
	// bind time so subsequent emissions don't print the raw value.
	r := NewRegistry()
	r.Register("bound-secret-cccccccc")
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r).With(
		uberzap.String("auth", "user=admin pass=bound-secret-cccccccc"),
	)

	log.Info("derived logger emission")

	out := buf.String()
	if strings.Contains(out, "bound-secret-cccccccc") {
		t.Fatalf("With-time-bound secret leaked: %q", out)
	}
}

func TestRedact_LateRegisteredTokenScrubsFutureWrites(t *testing.T) {
	// Register a token AFTER constructing the logger; the next Write
	// should pick it up via the shared Registry pointer.
	r := NewRegistry()
	var buf bytes.Buffer
	log := newZapLogger(t, &buf, r)

	r.Register("late-registered-secret-eeeeeeee")
	log.Info("after-late-registration: late-registered-secret-eeeeeeee leaked here")

	out := buf.String()
	if strings.Contains(out, "late-registered-secret-eeeeeeee") {
		t.Fatalf("late-registered token did not scrub future write: %q", out)
	}
}

// TestRedact_SyncReturnsInnerError ensures the wrapper doesn't swallow
// Sync errors (the inner Core may report a flush failure that the manager
// surfaces at shutdown).
func TestRedact_SyncReturnsInnerError(t *testing.T) {
	c := &fakeCore{syncErr: fmt.Errorf("flush failed")}
	w := newRedactingCore(c, NewRegistry())
	if err := w.Sync(); err == nil || err.Error() != "flush failed" {
		t.Fatalf("Sync should propagate inner error; got %v", err)
	}
}

// TestRedact_EnabledDelegatesToInner pins the level-enablement path.
func TestRedact_EnabledDelegatesToInner(t *testing.T) {
	c := &fakeCore{enabledLevel: zapcore.WarnLevel}
	w := newRedactingCore(c, NewRegistry())
	if w.Enabled(zapcore.DebugLevel) {
		t.Fatal("Debug should be disabled when inner says Warn-and-up")
	}
	if !w.Enabled(zapcore.ErrorLevel) {
		t.Fatal("Error should be enabled when inner says Warn-and-up")
	}
}

type fakeCore struct {
	enabledLevel zapcore.Level
	syncErr      error
	writes       []zapcore.Entry
}

func (f *fakeCore) Enabled(lvl zapcore.Level) bool { return lvl >= f.enabledLevel }
func (f *fakeCore) With([]zapcore.Field) zapcore.Core {
	return &fakeCore{enabledLevel: f.enabledLevel, syncErr: f.syncErr}
}
func (f *fakeCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if f.Enabled(ent.Level) {
		return ce.AddCore(ent, f)
	}
	return ce
}
func (f *fakeCore) Write(ent zapcore.Entry, _ []zapcore.Field) error {
	f.writes = append(f.writes, ent)
	return nil
}
func (f *fakeCore) Sync() error { return f.syncErr }
