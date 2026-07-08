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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// observerPushCR is the CR name used by the observer-push mapper tests.
// A const so the literal isn't repeated across assertions (goconst).
const observerPushCR = "obs0"

// TestMapObserverEventToCR pins the observer-push → reconcile-request
// mapping: the event object's namespace/name become exactly one
// reconcile request for that CR.
func TestMapObserverEventToCR(t *testing.T) {
	r := &ValkeyReconciler{}
	reqs := r.mapObserverEventToCR(context.Background(), &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "obs-ns", Name: observerPushCR},
	})
	if len(reqs) != 1 {
		t.Fatalf("expected 1 reconcile request, got %d", len(reqs))
	}
	if reqs[0].Namespace != "obs-ns" || reqs[0].Name != observerPushCR {
		t.Errorf("request = %v; want obs-ns/%s", reqs[0].NamespacedName, observerPushCR)
	}
}

// TestMapObserverEventToCR_NoIdentity confirms the defensive guard:
// an object with no name (or a nil interface) yields no request rather
// than enqueuing an empty key.
func TestMapObserverEventToCR_NoIdentity(t *testing.T) {
	r := &ValkeyReconciler{}
	if reqs := r.mapObserverEventToCR(context.Background(), &valkeyv1beta1.Valkey{}); reqs != nil {
		t.Errorf("expected nil for an identity-less object, got %v", reqs)
	}
	var nilObj client.Object
	if reqs := r.mapObserverEventToCR(context.Background(), nilObj); reqs != nil {
		t.Errorf("expected nil for a nil object, got %v", reqs)
	}
}
