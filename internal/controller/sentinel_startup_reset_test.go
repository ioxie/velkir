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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// sentinelEndpointsScheme registers the types the fake client needs to serve
// the pod List sentinelEndpointsForCR issues.
func sentinelEndpointsScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1.AddToScheme: %v", err)
	}
	return s
}

// labeledSentinelPod builds a pod carrying the (cr, component) label pair the
// sentinelEndpointsForCR selector keys on, so a case can vary either label to
// exercise selection.
func labeledSentinelPod(name, crValue, component, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			Labels: map[string]string{
				CRLabel:        crValue,
				ComponentLabel: component,
			},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// TestSentinelStartupReset_sentinelEndpointsForCR is a golden-master
// characterization of the startup-reset endpoint build before the shared
// extraction: it pins the selector, the PodIP filter, the
// "<podIP>:26379" Endpoint shape (defaultSentinelPort), and the empty-but-
// non-nil result so the subsequent consolidation can be proven byte-for-byte
// equivalent. The reconciler half (sentinelEndpointsFromPods) is already
// exercised by sentinel_orchestration_test.go; this pins the previously
// untested SentinelStartupReset path.
func TestSentinelStartupReset_sentinelEndpointsForCR(t *testing.T) {
	scheme := sentinelEndpointsScheme(t)
	cr := &valkeyv1beta1.Valkey{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk0"}}

	tests := []struct {
		name string
		pods []client.Object
		// want maps pod name -> expected Endpoint.Addr; the absence of a pod
		// name means that pod must NOT appear in the result.
		want map[string]string
	}{
		{
			name: "every sentinel pod with a PodIP becomes an endpoint",
			pods: []client.Object{
				labeledSentinelPod("vk0-sentinel-0", "vk0", componentSentinel, "10.0.0.1"),
				labeledSentinelPod("vk0-sentinel-1", "vk0", componentSentinel, "10.0.0.2"),
				labeledSentinelPod("vk0-sentinel-2", "vk0", componentSentinel, "10.0.0.3"),
			},
			want: map[string]string{
				"vk0-sentinel-0": "10.0.0.1:26379",
				"vk0-sentinel-1": "10.0.0.2:26379",
				"vk0-sentinel-2": "10.0.0.3:26379",
			},
		},
		{
			name: "pods without a PodIP yet are skipped",
			pods: []client.Object{
				labeledSentinelPod("vk0-sentinel-0", "vk0", componentSentinel, "10.0.0.1"),
				labeledSentinelPod("vk0-sentinel-1", "vk0", componentSentinel, ""),
			},
			want: map[string]string{
				"vk0-sentinel-0": "10.0.0.1:26379",
			},
		},
		{
			name: "no sentinel pods yields an empty result",
			pods: nil,
			want: map[string]string{},
		},
		{
			name: "pods for another CR or another component are not selected",
			pods: []client.Object{
				// right CR, wrong component (a data pod).
				labeledSentinelPod("vk0-0", "vk0", "valkey", "10.0.0.9"),
				// right component, wrong CR.
				labeledSentinelPod("other-sentinel-0", "other", componentSentinel, "10.0.0.8"),
			},
			want: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.pods...).Build()
			s := &SentinelStartupReset{Client: c}

			got, err := s.sentinelEndpointsForCR(context.Background(), cr)
			if err != nil {
				t.Fatalf("sentinelEndpointsForCR: unexpected error: %v", err)
			}
			// The build always returns an initialized slice, never nil, so a
			// caller can range it without a nil guard.
			if got == nil {
				t.Fatal("endpoint slice must be non-nil even when empty")
			}

			gotByName := make(map[string]string, len(got))
			for _, ep := range got {
				if _, dup := gotByName[ep.Name]; dup {
					t.Errorf("duplicate endpoint for pod %q", ep.Name)
				}
				gotByName[ep.Name] = ep.Addr
			}
			if len(gotByName) != len(tt.want) {
				t.Fatalf("endpoint set = %v, want %v", gotByName, tt.want)
			}
			for name, addr := range tt.want {
				if gotByName[name] != addr {
					t.Errorf("endpoint %q: Addr = %q, want %q", name, gotByName[name], addr)
				}
			}
		})
	}
}

// TestSentinelStartupReset_sentinelEndpointsForCR_ListError pins the error
// contract: a failed pod List surfaces as a nil result plus the
// "listing sentinel pods: %w"-wrapped cause, so the wrap is preserved across
// the extraction.
func TestSentinelStartupReset_sentinelEndpointsForCR_ListError(t *testing.T) {
	scheme := sentinelEndpointsScheme(t)
	cr := &valkeyv1beta1.Valkey{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk0"}}

	listErr := errors.New("synthetic list failure")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return listErr
			},
		}).Build()
	s := &SentinelStartupReset{Client: c}

	got, err := s.sentinelEndpointsForCR(context.Background(), cr)
	if err == nil {
		t.Fatal("expected an error when the pod List fails")
	}
	if got != nil {
		t.Errorf("result must be nil on List error, got %v", got)
	}
	if !errors.Is(err, listErr) {
		t.Errorf("error must wrap the List failure (errors.Is); got %v", err)
	}
	if !strings.Contains(err.Error(), "listing sentinel pods:") {
		t.Errorf("error must carry the \"listing sentinel pods:\" prefix; got %q", err.Error())
	}
}
