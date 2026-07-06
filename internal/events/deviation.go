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
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeviationEmitter emits Warning events for best-practice deviations
// (PDBTooPermissive, AntiAffinityTooPermissive, RolloutFragileQuorum, …)
// observed during a CR's reconcile loop. The validating webhook surfaces
// the same deviations as admission warnings, which are ephemeral — shown
// once at apply time and then gone; this emitter re-surfaces them as
// durable, queryable Events so an operator-of-the-operator can
// `kubectl get events` the deviations after the fact.
//
// Per (namespace/name/reason/field) tuple it emits at most one event per
// process lifetime — the in-memory dedup set resets on operator restart,
// so the first reconcile after a restart re-emits. The field
// discriminator lets a CR whose valkey AND sentinel PDB are both
// too-permissive surface as two distinct PDBTooPermissive events.
//
// Threadsafe — the per-CR reconcile mutex may not be held when the emit
// fires; the registry is independent (mirrors Deprecator).
type DeviationEmitter struct {
	recorder events.EventRecorder

	mu      sync.Mutex
	emitted map[string]struct{}
}

// NewDeviationEmitter wires an EventRecorder. A nil recorder is allowed;
// callers don't need to special-case the no-recorder path.
func NewDeviationEmitter(rec events.EventRecorder) *DeviationEmitter {
	return &DeviationEmitter{
		recorder: rec,
		emitted:  map[string]struct{}{},
	}
}

// Emit posts a Warning event with the given Reason for obj's field if the
// (namespace/name/reason/field) tuple hasn't already been emitted this
// process lifetime. message is the human-readable explanation (the
// admission-warning text without the reason prefix). Returns true when
// the event was actually emitted, false when suppressed by the dedup set
// or when the emitter / recorder / obj is nil.
func (e *DeviationEmitter) Emit(obj client.Object, reason Reason, field, message string) bool {
	if e == nil || e.recorder == nil || obj == nil {
		return false
	}
	key := obj.GetNamespace() + "/" + obj.GetName() + "/" + string(reason) + "/" + field
	e.mu.Lock()
	if _, seen := e.emitted[key]; seen {
		e.mu.Unlock()
		return false
	}
	e.emitted[key] = struct{}{}
	e.mu.Unlock()
	e.recorder.Eventf(obj, nil, corev1.EventTypeWarning, string(reason),
		"BestPracticeDeviation", "%s", message)
	return true
}

// Forget drops every dedup entry for one CR — call it on CR delete so a
// recreated CR with the same identity re-emits next reconcile rather than
// inheriting the prior CR's silenced state.
func (e *DeviationEmitter) Forget(obj client.Object) {
	if e == nil || obj == nil {
		return
	}
	prefix := obj.GetNamespace() + "/" + obj.GetName() + "/"
	e.mu.Lock()
	defer e.mu.Unlock()
	for k := range e.emitted {
		if strings.HasPrefix(k, prefix) {
			delete(e.emitted, k)
		}
	}
}
