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

package v1beta1

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// recordingHandler counts how many times its Handle is called and
// always returns Allowed. The test asserts on the counter to confirm
// whether the wrapper short-circuited the call.
type recordingHandler struct{ calls atomic.Int32 }

func (r *recordingHandler) Handle(_ context.Context, _ admission.Request) admission.Response {
	r.calls.Add(1)
	return admission.Allowed("")
}

func TestAdmissionSizeLimit_PassesWhenUnderLimit(t *testing.T) {
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{
		Operation: v1.Create,
		Object:    runtime.RawExtension{Raw: make([]byte, 5*1024*1024)}, // 5 MiB
	}}
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected pass-through Allowed; got %+v", resp)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("expected inner handler called once; got %d", got)
	}
}

func TestAdmissionSizeLimit_RejectsWhenObjectOverLimit(t *testing.T) {
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	overLimit := maxAdmissionRequestBytes + 1
	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{
		Operation: v1.Create,
		Object:    runtime.RawExtension{Raw: make([]byte, overLimit)},
	}}
	resp := h.Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatalf("expected rejection; got Allowed")
	}
	if got, want := resp.Result.Code, int32(http.StatusBadRequest); got != want {
		t.Fatalf("expected status %d; got %d (message=%q)", want, got, resp.Result.Message)
	}
	// Message must name both the actual size and the limit so an
	// operator triaging rejection logs can tell why the cap fired and
	// by how much it was exceeded.
	if !strings.Contains(resp.Result.Message, fmt.Sprintf("%d bytes", overLimit)) {
		t.Errorf("expected message to include actual byte count %d; got %q", overLimit, resp.Result.Message)
	}
	if !strings.Contains(resp.Result.Message, fmt.Sprintf("%d-byte limit", maxAdmissionRequestBytes)) {
		t.Errorf("expected message to include the limit %d; got %q", maxAdmissionRequestBytes, resp.Result.Message)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Errorf("inner handler must not run when size check fails; got %d calls", got)
	}
}

func TestAdmissionSizeLimit_RejectsWhenOldObjectOverLimit(t *testing.T) {
	// DELETE carries the doomed object on OldObject.Raw, not Object.Raw.
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{
		Operation: v1.Delete,
		OldObject: runtime.RawExtension{Raw: make([]byte, maxAdmissionRequestBytes+1)},
	}}
	resp := h.Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatalf("expected rejection on oversized OldObject; got Allowed")
	}
	if got, want := resp.Result.Code, int32(http.StatusBadRequest); got != want {
		t.Fatalf("expected status %d; got %d", want, got)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Errorf("inner handler must not run when size check fails; got %d calls", got)
	}
}

func TestAdmissionSizeLimit_RejectsUpdateWhenEitherSideOverLimit(t *testing.T) {
	// UPDATE carries both the new object on Object.Raw AND the
	// pre-update object on OldObject.Raw. The cap applies to the larger
	// of the two — a small new object paired with a giant OldObject
	// must still reject.
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{
		Operation: v1.Update,
		Object:    runtime.RawExtension{Raw: make([]byte, 1024)}, // small new
		OldObject: runtime.RawExtension{Raw: make([]byte, maxAdmissionRequestBytes+1)},
	}}
	resp := h.Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatalf("expected rejection on oversized OldObject during UPDATE; got Allowed")
	}
	if got, want := resp.Result.Code, int32(http.StatusBadRequest); got != want {
		t.Fatalf("expected status %d; got %d", want, got)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Errorf("inner handler must not run when size check fails; got %d calls", got)
	}
}

func TestAdmissionSizeLimit_AllowsAtBoundary(t *testing.T) {
	// Exactly maxAdmissionRequestBytes is allowed; only > limit is rejected.
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{
		Operation: v1.Create,
		Object:    runtime.RawExtension{Raw: make([]byte, maxAdmissionRequestBytes)},
	}}
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected pass-through at boundary; got %+v", resp)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("expected inner handler called once; got %d", got)
	}
}

func TestAdmissionSizeLimit_PassesEmptyRequest(t *testing.T) {
	// CONNECT-style requests have empty Object and OldObject; the
	// wrapper must pass them through.
	inner := &recordingHandler{}
	h := admissionSizeLimitHandler(inner)

	req := admission.Request{AdmissionRequest: v1.AdmissionRequest{Operation: v1.Connect}}
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected pass-through on empty body; got %+v", resp)
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("expected inner handler called once; got %d", got)
	}
}
