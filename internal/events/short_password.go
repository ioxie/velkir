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

// ShortAuthPasswordReporter emits AuthSecretShortPassword warning
// events when the operator reads an auth Secret whose `password` data
// key is shorter than the redactor's MinTokenLen floor. Per
// (namespace/name/secretName) tuple it emits at most one event per
// process lifetime; the in-memory dedup set resets on operator
// restart so the first reconcile after a restart re-emits. A Secret
// rename (different secretName) re-keys the dedup and re-emits even
// without a restart.
//
// Threadsafe — independent of any per-CR mutex. Layered next to
// Deprecator because both reporters share the dedup-on-CR-tuple
// pattern.
type ShortAuthPasswordReporter struct {
	recorder events.EventRecorder

	mu      sync.Mutex
	emitted map[string]struct{}
}

// NewShortAuthPasswordReporter wires an EventRecorder. A nil recorder
// is allowed; callers don't need to special-case the no-recorder path.
func NewShortAuthPasswordReporter(rec events.EventRecorder) *ShortAuthPasswordReporter {
	return &ShortAuthPasswordReporter{
		recorder: rec,
		emitted:  map[string]struct{}{},
	}
}

// Emit posts an AuthSecretShortPassword warning event for `obj` if the
// (namespace/name/secretName) tuple hasn't been emitted yet this
// process lifetime. The caller is responsible for the length
// comparison; the reporter trusts its inputs and only handles dedup +
// emission. `passwordLen` and `minLen` appear in the event message
// verbatim so the operator-of-the-operator can read the exact gap
// without grepping the logs.
//
// Returns true when the event was actually emitted, false when
// suppressed by the dedup set or by nil/empty-input guards. Callers
// that want to pair the emission with a structured INFO log line can
// branch on the return so the log fires exactly once per dedup
// boundary instead of on every reconcile read.
func (r *ShortAuthPasswordReporter) Emit(obj client.Object, secretName string, passwordLen, minLen int) bool {
	if r == nil || r.recorder == nil || obj == nil || secretName == "" {
		return false
	}
	key := obj.GetNamespace() + "/" + obj.GetName() + "/" + secretName
	r.mu.Lock()
	if _, seen := r.emitted[key]; seen {
		r.mu.Unlock()
		return false
	}
	r.emitted[key] = struct{}{}
	r.mu.Unlock()
	r.recorder.Eventf(obj, nil, corev1.EventTypeWarning, string(AuthSecretShortPassword),
		"AuthSecretShortPassword",
		"auth Secret %q has password length %d (< MinTokenLen %d); the redaction registry will not scrub this value from operator logs",
		secretName, passwordLen, minLen)
	return true
}

// Forget drops the dedup entries for one CR — call it on CR delete so
// a recreated CR with the same name re-emits next reconcile rather
// than inheriting the prior CR's silenced state.
//
// O(N) over the dedup-set size (one pass per call). Acceptable at the
// expected steady-state size (CR count × Secrets-per-CR, typically
// well under 1000). Revisit with a nested map keyed by
// (namespace+name) → (secretName) if the dedup set ever grows past
// that range.
func (r *ShortAuthPasswordReporter) Forget(obj client.Object) {
	if r == nil || obj == nil {
		return
	}
	prefix := obj.GetNamespace() + "/" + obj.GetName() + "/"
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.emitted {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(r.emitted, k)
		}
	}
}
