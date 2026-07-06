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
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/event"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// TestOperationalAnnotationChangePredicate pins the finding that edits to
// an operational annotation (pause/unpause, single-shot opt-ins, manual
// rollout) — which leave .metadata.generation untouched — fire the
// predicate so the reconciler wakes without waiting for the baseline
// watchdog. Operator-written config-hash and unrelated annotations do
// not fire it.
func TestOperationalAnnotationChangePredicate(t *testing.T) {
	pred := operationalAnnotationChangePredicate()
	mk := func(ann map[string]string) *valkeyv1beta1.Valkey {
		v := &valkeyv1beta1.Valkey{}
		v.Annotations = ann
		return v
	}
	cases := []struct {
		name     string
		old, new map[string]string
		want     bool
	}{
		{"pause added", nil, map[string]string{PauseAnnotation: "true"}, true},
		{"pause removed", map[string]string{PauseAnnotation: "true"}, nil, true},
		{"force-rotate value changed", map[string]string{ForceRotateAnnotation: "a"}, map[string]string{ForceRotateAnnotation: "b"}, true},
		{"manual rollout bumped", map[string]string{ManualRolloutAnnotation: "1"}, map[string]string{ManualRolloutAnnotation: "2"}, true},
		{"accept-pvc-loss added", nil, map[string]string{AcceptPVCLossAnnotation: "true"}, true},
		{"unrelated annotation changed", map[string]string{"team": "a"}, map[string]string{"team": "b"}, false},
		{"config-hash only (operator-written, excluded)", map[string]string{ConfigHashAnnotation: "x"}, map[string]string{ConfigHashAnnotation: "y"}, false},
		{"no operational change", map[string]string{PauseAnnotation: "true"}, map[string]string{PauseAnnotation: "true"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := event.UpdateEvent{ObjectOld: mk(c.old), ObjectNew: mk(c.new)}
			if got := pred.Update(e); got != c.want {
				t.Fatalf("Update(old=%v, new=%v)=%v want %v", c.old, c.new, got, c.want)
			}
		})
	}

	t.Run("nil objects do not panic and return false", func(t *testing.T) {
		if pred.Update(event.UpdateEvent{}) {
			t.Fatalf("nil objects: want false")
		}
	})
}
