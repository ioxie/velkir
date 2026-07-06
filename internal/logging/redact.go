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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
)

// redactingCore wraps a zapcore.Core and scrubs every registered token
// from the entry message and string-bearing field values before delegating
// to the inner Core.
//
// The wrapper is constructed once at logger build time and threads through
// the manager's logger tree via .With() — every derived logger inherits
// the same scrub pass. The token registry is shared by reference, so
// late-arriving tokens (a Secret read mid-reconcile) take effect on the
// next emission.
type redactingCore struct {
	inner  zapcore.Core
	tokens *Registry
}

// newRedactingCore wraps c with a redaction layer driven by registry.
// Internal constructor; production wiring goes through New() and the
// public WrapWithRedaction() entrypoint, both of which call this.
// Tests reach for it directly via WrapWithRedaction.
func newRedactingCore(c zapcore.Core, registry *Registry) zapcore.Core {
	return &redactingCore{inner: c, tokens: registry}
}

// WrapWithRedaction returns c wrapped in the production scrub layer
// driven by registry. Exposed so tests in other packages can build
// a buffered logger that exercises the SAME redacting Core production
// uses (rather than a synthetic substitute that masks divergence).
// Pairs with uberzap.WrapCore at logger build time:
//
//	zl := uberzap.New(inner, uberzap.WrapCore(func(c zapcore.Core) zapcore.Core {
//	    return logging.WrapWithRedaction(c, logging.DefaultRegistry)
//	}))
//
// Production wires this through New() against DefaultRegistry; tests
// may pass any Registry to keep state-isolation.
func WrapWithRedaction(c zapcore.Core, registry *Registry) zapcore.Core {
	return newRedactingCore(c, registry)
}

func (c *redactingCore) Enabled(lvl zapcore.Level) bool { return c.inner.Enabled(lvl) }
func (c *redactingCore) Sync() error                    { return c.inner.Sync() }

// With redacts the bound fields against the current token snapshot and
// wraps the inner Core's With result so the same scrub layer applies to
// future emissions.
//
// Snapshot semantics: tokens registered AFTER this call don't retroactively
// scrub the bound fields. The package-level doc comment calls out the
// implication for callers (don't bind credential-bearing values into a
// logger context — log them inline so the Write path picks them up).
// Applies to both native zap Logger.With and ctrl.Log's WithValues —
// zapr binds the accumulated kv pairs onto the underlying zap.Logger
// via zap.Logger.With, which routes through this Core's With.
func (c *redactingCore) With(fields []zapcore.Field) zapcore.Core {
	tokens := c.tokens.Snapshot()
	redacted := redactFields(fields, tokens)
	return &redactingCore{
		inner:  c.inner.With(redacted),
		tokens: c.tokens,
	}
}

// Check delegates to the inner Core's enablement decision and registers
// this Core as the destination so Write goes through the redaction pass.
func (c *redactingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

// Write redacts ent.Message and any string-bearing fields, then forwards
// to the inner Core. An empty token snapshot is the no-op fast path —
// avoids the per-call allocation when no Secrets are registered yet
// (operator startup, between reconciles in a test environment).
func (c *redactingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	tokens := c.tokens.Snapshot()
	if len(tokens) == 0 {
		return c.inner.Write(ent, fields)
	}
	ent.Message = redactString(ent.Message, tokens)
	return c.inner.Write(ent, redactFields(fields, tokens))
}

// redactString runs literal-substring replacement of every token. Order
// of replacement is unspecified by the snapshot iteration, but for the
// "one token contains another" case the result is the same regardless
// of order — once an occurrence has been replaced with the placeholder,
// further passes don't re-match the same substring.
func redactString(s string, tokens []string) string {
	for _, t := range tokens {
		if strings.Contains(s, t) {
			s = strings.ReplaceAll(s, t, RedactedPlaceholder)
		}
	}
	return s
}

// redactFields rebuilds the field slice with every string-bearing field
// scrubbed. Numeric/bool/object types pass through unchanged — the
// redactor isn't a deep walker (see package doc for the tradeoff).
func redactFields(fields []zapcore.Field, tokens []string) []zapcore.Field {
	if len(tokens) == 0 || len(fields) == 0 {
		return fields
	}
	out := make([]zapcore.Field, len(fields))
	for i, f := range fields {
		out[i] = redactField(f, tokens)
	}
	return out
}

// redactField returns a copy of f with any string-bearing payload
// scrubbed. The set of scrubbed types covers the field shapes the
// operator actually emits via Info/Error/With — extending past this
// set means inspecting reflect.Value, which is both slow and
// unbounded in shape.
//
// ReflectType / ObjectType are scrubbed by rendering the value to
// JSON in memory, redacting the rendered text, and re-emitting as a
// String field. This loses the encoder-side structure (the value
// shows up as a JSON-string instead of a nested object), which is
// the safety-net trade-off: when a registered token leaks through a
// reflective or custom-marshaled field, prefer "scrubbed flat
// rendering" over "structured but containing the secret".
func redactField(f zapcore.Field, tokens []string) zapcore.Field {
	switch f.Type {
	case zapcore.StringType:
		f.String = redactString(f.String, tokens)
	case zapcore.ErrorType:
		// Field.Interface holds the error; rebuild with a new error
		// whose Error() returns the scrubbed string. The original
		// error chain is severed — acceptable because logs aren't a
		// place to programmatically unwrap (handlers should inspect
		// errors before logging, not after).
		if err, ok := f.Interface.(error); ok && err != nil {
			f.Interface = errors.New(redactString(err.Error(), tokens))
		}
	case zapcore.StringerType:
		// Same shape as ErrorType: replace the underlying with a
		// stringer that yields the scrubbed text.
		if s, ok := f.Interface.(fmt.Stringer); ok && s != nil {
			f.Interface = stringerFunc(redactString(s.String(), tokens))
		}
	case zapcore.ReflectType:
		// zap.Reflect("k", anything) — Field.Interface holds the
		// value zap would JSON-encode at write time. Pre-encode here
		// so we can scrub the rendered bytes.
		return redactToString(f, jsonMarshal(f.Interface), tokens)
	case zapcore.ObjectMarshalerType:
		// zap.Object("k", marshaler) — Field.Interface implements
		// zapcore.ObjectMarshaler. Run it against an in-memory
		// MapObjectEncoder, JSON-marshal the captured map, scrub.
		om, ok := f.Interface.(zapcore.ObjectMarshaler)
		if !ok || om == nil {
			return f
		}
		enc := zapcore.NewMapObjectEncoder()
		if err := om.MarshalLogObject(enc); err != nil {
			return redactToString(f, nil, tokens)
		}
		return redactToString(f, jsonMarshal(enc.Fields), tokens)
	}
	return f
}

// jsonMarshal returns the JSON encoding of v, or nil on failure. The
// caller treats nil as "marshal failed; emit a hard placeholder
// rather than leaking the original".
func jsonMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}

// redactToString rewrites f as a String field carrying the scrubbed
// text of body. When body is nil (marshal failed upstream), the
// rewrite uses the safety-net placeholder so a leaked Secret can't
// survive a marshal-error path.
func redactToString(f zapcore.Field, body []byte, tokens []string) zapcore.Field {
	if body == nil {
		f.Type = zapcore.StringType
		f.String = RedactedPlaceholder + ":marshal-failed"
		f.Interface = nil
		return f
	}
	f.Type = zapcore.StringType
	f.String = redactString(string(body), tokens)
	f.Interface = nil
	return f
}

// stringerFunc adapts a string into the fmt.Stringer interface so a
// redacted Stringer field can be re-published without dragging the
// original (potentially Secret-holding) struct through the log.
type stringerFunc string

func (s stringerFunc) String() string { return string(s) }
