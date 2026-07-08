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
	webhookv1beta1 "github.com/ioxie/velkir/internal/webhook/v1beta1"
)

// emitDeviations re-surfaces the best-practice deviations the
// validating webhook reports as ephemeral admission warnings as durable,
// queryable Warning Events. The detection is the webhook's Deviations(o)
// — the single source of truth, so the admission-warning text and the
// Event message stay in lock-step instead of drifting between two copies
// of the rules. Dedup is per-(CR, reason, field) for the process
// lifetime (events.DeviationEmitter); the in-memory set resets on
// operator restart, so the first reconcile after a restart re-emits.
//
// Called from Reconcile right after checkDeprecations, so the CR is
// non-deleting and non-paused by the time this runs. No-op when
// DeviationEmitter is nil (tests without an event recorder).
func (r *ValkeyReconciler) emitDeviations(v *valkeyv1beta1.Valkey) {
	if r.DeviationEmitter == nil || v == nil {
		return
	}
	for _, d := range webhookv1beta1.Deviations(v) {
		r.DeviationEmitter.Emit(v, d.Reason, d.Field, d.Message)
	}
}
