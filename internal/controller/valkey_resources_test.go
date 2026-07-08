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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Pins the three-row contract for the operator's default-request
// stamping: nil → defaults stamped; explicit empty map → preserved
// (so the kubelet falls back to its own defaults / LimitRange);
// partial → honoured verbatim. The nil-vs-empty distinction is the
// regression vector — `len(nil) == 0 && len(map{}) == 0`, so a
// `len(...) == 0` predicate would over-default the empty-map case.

func resourceListEqual(a, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if av.Cmp(bv) != 0 {
			return false
		}
	}
	return true
}

func TestValkeyContainerResources_NilRequests_StampsDefaults(t *testing.T) {
	t.Parallel()
	got := valkeyContainerResources(corev1.ResourceRequirements{})
	if got.Requests == nil {
		t.Fatalf("nil requests must stamp operator defaults; got Requests=nil")
	}
	want := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}
	if !resourceListEqual(*got.Requests, want) {
		t.Errorf("default-stamped requests: got %v, want %v", *got.Requests, want)
	}
}

func TestValkeyContainerResources_EmptyMap_PreservesNoRequests(t *testing.T) {
	t.Parallel()
	got := valkeyContainerResources(corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
	})
	if got.Requests != nil {
		t.Errorf("explicit empty-map requests must round-trip as no-requests; got %v", *got.Requests)
	}
}

func TestValkeyContainerResources_PartialRequests_HonoursUser(t *testing.T) {
	t.Parallel()
	user := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("200m"),
	}
	got := valkeyContainerResources(corev1.ResourceRequirements{Requests: user})
	if got.Requests == nil {
		t.Fatalf("partial user requests must be preserved; got Requests=nil")
	}
	if !resourceListEqual(*got.Requests, user) {
		t.Errorf("partial requests altered: got %v, want %v", *got.Requests, user)
	}
}

func TestValkeyContainerResources_LimitsPassThroughUnmodified(t *testing.T) {
	t.Parallel()
	limits := corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("512Mi"),
	}
	got := valkeyContainerResources(corev1.ResourceRequirements{Limits: limits})
	if got.Limits == nil {
		t.Fatalf("limits must pass through; got Limits=nil")
	}
	if !resourceListEqual(*got.Limits, limits) {
		t.Errorf("limits altered: got %v, want %v", *got.Limits, limits)
	}
	// Limits-only spec leaves Requests nil → defaults still stamped.
	if got.Requests == nil {
		t.Errorf("limits-only spec must still stamp default requests")
	}
}
