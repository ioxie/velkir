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
	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// FieldDeprecation describes one deprecated CR field for the per-reconcile
// deprecation sweep. Predicate is evaluated against the live CR each
// reconcile; when it returns true, the Deprecator emits a
// FieldDeprecated event for (namespace/name/Path) — deduplicated per
// process lifetime by internal/events.Deprecator.
//
// Path is the JSON-path of the deprecated field (e.g.
// `spec.valkey.legacyFlag`); RemovalWindow describes when removal is
// planned (e.g. `removed-in-v0.4`). Both appear in the event message
// verbatim so an operator-of-the-operator reading
// `kubectl get events` can plan the rewrite without grepping the
// changelog.
//
// Predicate receives the live CR from the informer cache and MUST NOT
// mutate it — the cache returns a shared pointer reused across every
// reader in the process (status writers, status readers, all
// reconcilers), so any in-place edit corrupts other paths silently.
// Read-only field inspection only.
type FieldDeprecation struct {
	Path          string
	RemovalWindow string
	Predicate     func(*valkeyv1beta1.Valkey) bool
}

// ProductionDeprecations is the production registry walked each reconcile.
// Empty today — the v1beta1 contract is additive-only since v0.1.0.
// The first real field deprecation lands by appending one entry here;
// the reconciler wiring + event-emission path are already in place,
// so no further plumbing is required.
var ProductionDeprecations = []FieldDeprecation{}

// checkDeprecations walks the reconciler's registry and emits
// FieldDeprecated for every entry whose Predicate matches the current
// CR. Called from Reconcile after Phase 0b' (finalizer add), so the CR
// is non-deleting and non-paused by the time this runs. Dedup is
// per-(CR, Path) tuple for the process lifetime; the in-memory dedup
// set lives in events.Deprecator and resets on operator restart, so
// the first reconcile after a restart re-emits.
//
// No-op when Deprecator is nil (tests without an event recorder) or
// when the registry is empty (production today).
func (r *ValkeyReconciler) checkDeprecations(v *valkeyv1beta1.Valkey) {
	if r.Deprecator == nil || v == nil {
		return
	}
	for _, d := range r.Deprecations {
		if d.Predicate == nil || !d.Predicate(v) {
			continue
		}
		r.Deprecator.Emit(v, d.Path, d.RemovalWindow)
	}
}
