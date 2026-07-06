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

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// maxAdmissionRequestBytes caps the operator-side admission payload at
// 10 MiB as a defense-in-depth bound. The apiserver enforces its own
// admission-request size limit (typically 3 MiB by default), but the
// operator must not assume that bound is in place — a misconfigured
// apiserver, an aggregated apiserver in the request path, or a future
// CRD conversion could surface a larger payload here. Refusing
// oversized payloads at handler entry stops decode-time memory
// amplification: a 10 MiB JSON body can balloon to many times that
// size in Go-struct memory once Unmarshal walks every nested map and
// slice. The raw request bytes are already buffered by the webhook
// machinery before this wrapper runs, so the check does not avoid the
// raw-byte read itself — but it fires before either typed wrapper
// (validator or defaulter) calls Decode, so it stops the decode-time
// amplification rather than letting Unmarshal expand the payload first.
const maxAdmissionRequestBytes = 10 * 1024 * 1024

// admissionSizeLimitHandler wraps next with an early size check. The
// limit applies to whichever of req.Object.Raw / req.OldObject.Raw is
// larger so that UPDATE (both populated) and DELETE (only OldObject)
// are bounded by the same rule. The wrapper returns 400 Bad Request
// before next.Handle runs; the inner handler's metric / log path is
// not entered.
func admissionSizeLimitHandler(next admission.Handler) admission.Handler {
	return admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
		n := len(req.Object.Raw)
		if oldN := len(req.OldObject.Raw); oldN > n {
			n = oldN
		}
		if n > maxAdmissionRequestBytes {
			return admission.Errored(http.StatusBadRequest,
				fmt.Errorf("admission request body %d bytes exceeds the operator's %d-byte limit", n, maxAdmissionRequestBytes))
		}
		return next.Handle(ctx, req)
	})
}
