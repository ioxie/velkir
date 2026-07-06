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

package events

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Deprecator emits FieldDeprecated events for CR fields exercised
// during a CR's reconcile loop. Per (namespace/name/field) tuple it
// emits at most one event per process lifetime — the in-memory dedup
// set resets on operator restart, so the first reconcile after a
// restart re-emits.
//
// The Reason for every emission is FieldDeprecated. The event message
// names the deprecated field and gives the deprecation window so the
// operator-of-the-operator can plan the field rewrite without grepping
// the changelog.
//
// Threadsafe — the per-CR mutex in the reconciler may not be held
// when the emit fires (the controller package owns its own concurrency
// model); keep the registry independent.
type Deprecator struct {
	recorder events.EventRecorder

	mu      sync.Mutex
	emitted map[string]struct{}
}

// NewDeprecator wires an EventRecorder. A nil recorder is allowed;
// callers don't need to special-case the no-recorder path.
func NewDeprecator(rec events.EventRecorder) *Deprecator {
	return &Deprecator{
		recorder: rec,
		emitted:  map[string]struct{}{},
	}
}

// Emit posts a FieldDeprecated event for `obj.field` if the same
// tuple hasn't already been emitted this process lifetime. `field`
// is the JSON-path of the deprecated field (e.g.
// `spec.valkey.legacyFlag`); `removalWindow` describes when removal
// is planned (e.g. `removed-in-v0.3`); both appear in the event
// message verbatim.
//
// Returns true when the event was actually emitted, false when
// suppressed by the dedup set. Callers that want to count
// deprecation-active CRs (for a metric, say) can branch on the
// return.
func (d *Deprecator) Emit(obj client.Object, field, removalWindow string) bool {
	if d == nil || d.recorder == nil || obj == nil {
		return false
	}
	key := obj.GetNamespace() + "/" + obj.GetName() + "/" + field
	d.mu.Lock()
	if _, seen := d.emitted[key]; seen {
		d.mu.Unlock()
		return false
	}
	d.emitted[key] = struct{}{}
	d.mu.Unlock()
	d.recorder.Eventf(obj, nil, corev1.EventTypeNormal, string(FieldDeprecated),
		"FieldDeprecate",
		"spec field %q is deprecated; %s — switch off it before the next minor",
		field, removalWindow)
	return true
}

// Forget drops the dedup entry for one CR — call it on CR delete so
// a recreated CR with the same name re-emits next reconcile rather
// than inheriting the prior CR's silenced state.
func (d *Deprecator) Forget(obj client.Object) {
	if d == nil || obj == nil {
		return
	}
	prefix := obj.GetNamespace() + "/" + obj.GetName() + "/"
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.emitted {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(d.emitted, k)
		}
	}
}
