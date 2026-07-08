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

// Package logging is the operator's redacting log layer. Every controller
// logger is constructed from this package's New so a forgotten field name,
// a stray fmt.Errorf carrying a credential, or a third-party library
// bubbling a connection-URL error never reaches stdout/journald in raw
// form.
//
// Two pieces:
//
//   - Registry: a thread-safe, refcounted set of "banned tokens" — strings
//     the redactor scrubs from every log emission. Reconcilers feed the
//     registry as they read Secret values (Register on read, Forget on
//     CR delete or Secret rotation), so the set tracks the live blast
//     radius rather than a static list.
//
//   - Wrapped zapcore.Core: intercepts every Write and With, replacing
//     each registered token with a fixed placeholder in ent.Message and
//     in string-bearing field types (StringType, ErrorType, StringerType).
//     Numeric and reflect-typed fields aren't scanned — the redactor is
//     a safety net against accidental string-formatting of credentials,
//     not a guarantee against arbitrary structured leaks (audit-emit any
//     code path that hands a Secret object to .Info or .Error).
//
// Two known limitations, called out so the next reader doesn't have to
// re-derive them:
//
//   - With-time snapshot: a logger derived via WithValues binds redacted
//     fields at derivation time. Tokens registered AFTER WithValues
//     don't retroactively redact the bound fields. The reconciler should
//     therefore never bind a credential into a logger context — bind the
//     CR namespace/name, log the credential-bearing error inline so the
//     Write path picks the live registry up. Applies to both native zap
//     `Logger.With(...)` and ctrl.Log's `WithValues(...)` (zapr binds
//     accumulated kv pairs through `zapcore.Core.With` on the underlying
//     zap.Logger).
//
//   - Substring redaction is O(N tokens × M message length). At expected
//     scales (tens of CRs, ones-of-Secrets-per-CR) this is invisible;
//     past hundreds of tokens it warrants a benchmark and possibly an
//     Aho-Corasick automaton. Documented at the registry, not eagerly
//     optimised.
package logging
