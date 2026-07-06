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
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// TestScaleDownPrimaryRefusal pins the scale-down safety decision:
// a scale-down is refused when the primary ordinal can't be determined
// (an apiserver List flake must never silently wave a primary-removing
// scale-down through) or when the target replica count would delete the
// primary's pod. A non-scale-down is never refused.
func TestScaleDownPrimaryRefusal(t *testing.T) {
	errFlake := apierrors.NewInternalError(fmt.Errorf("apiserver flake"))
	cases := []struct {
		name                         string
		desired, current, primaryOrd int32
		primaryErr                   error
		want                         bool
	}{
		{"not a scale-down (equal)", 3, 3, 0, nil, false},
		{"not a scale-down (scale-up)", 5, 3, 0, nil, false},
		{"scale-down, no primary labelled", 2, 5, -1, nil, false},
		{"scale-down keeps primary in range", 4, 5, 0, nil, false},
		{"scale-down would remove primary (below)", 2, 5, 3, nil, true},
		{"scale-down would remove primary (at boundary)", 3, 5, 3, nil, true},
		{"list error refuses by default", 2, 5, -1, errFlake, true},
		{"list error refuses even with low primary ordinal", 4, 5, 0, errFlake, true},
		{"list error on a non-scale-down is still not refused", 5, 5, -1, errFlake, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := scaleDownPrimaryRefusal(c.desired, c.current, c.primaryOrd, c.primaryErr); got != c.want {
				t.Fatalf("scaleDownPrimaryRefusal(%d,%d,%d,err=%v)=%v want %v",
					c.desired, c.current, c.primaryOrd, c.primaryErr, got, c.want)
			}
		})
	}
}

// TestFindPrimaryOrdinal pins the primary-ordinal disambiguation: a List
// error returns a non-nil error (so the caller can refuse-by-default)
// rather than collapsing into the -1 "no primary labelled" sentinel.
func TestFindPrimaryOrdinal(t *testing.T) {
	s := pvcResizeTestScheme(t)
	cr := &valkeyv1beta1.Valkey{}
	cr.Name = "vk"
	cr.Namespace = "ns"

	t.Run("no primary labelled returns -1, nil", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).Build()
		r := &ValkeyReconciler{Client: c}
		ord, err := r.findPrimaryOrdinal(context.Background(), cr)
		if err != nil || ord != -1 {
			t.Fatalf("got (%d, %v), want (-1, nil)", ord, err)
		}
	})

	t.Run("single primary returns its ordinal", func(t *testing.T) {
		p := dataPlanePod("vk-2", "10.0.0.2", roleValuePrimary)
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(&p).Build()
		r := &ValkeyReconciler{Client: c}
		ord, err := r.findPrimaryOrdinal(context.Background(), cr)
		if err != nil || ord != 2 {
			t.Fatalf("got (%d, %v), want (2, nil)", ord, err)
		}
	})

	t.Run("two primaries returns the lowest ordinal", func(t *testing.T) {
		p1 := dataPlanePod("vk-3", "10.0.0.3", roleValuePrimary)
		p2 := dataPlanePod("vk-1", "10.0.0.1", roleValuePrimary)
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(&p1, &p2).Build()
		r := &ValkeyReconciler{Client: c}
		ord, err := r.findPrimaryOrdinal(context.Background(), cr)
		if err != nil || ord != 1 {
			t.Fatalf("got (%d, %v), want (1, nil)", ord, err)
		}
	})

	t.Run("list error returns the error, not a collapsed -1", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
					return apierrors.NewInternalError(fmt.Errorf("apiserver flake"))
				},
			}).Build()
		r := &ValkeyReconciler{Client: c}
		ord, err := r.findPrimaryOrdinal(context.Background(), cr)
		if err == nil {
			t.Fatalf("got (%d, nil), want a non-nil error so the scale-down guard refuses by default", ord)
		}
		if ord != -1 {
			t.Fatalf("ordinal on error = %d, want -1", ord)
		}
	})
}
