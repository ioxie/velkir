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
	"strconv"
	"time"

	operatormetrics "github.com/ioxie/velkir/internal/metrics"
)

// recordWebhookDuration observes one webhook admission round-trip on
// the operator-side budget and emits the
// `valkey_webhook_request_duration_seconds` histogram with the
// supplied labels. Called as `defer recordWebhookDuration(time.Now(),
// …)` at the top of each typed Default / Validate* method.
//
// code is derived from the typed return: nil error → 200 (admission
// allowed); non-nil error → 400 (validation rejection — the typed
// validator surfaces field.ErrorList through error). Granular code
// shapes (5xx for transient internal errors, 422 for unprocessable
// entity) require lifting up to the raw admission.Handler — out of
// scope here. The 200/400 split is enough for the
// ValkeyWebhookLatencyHigh alert to fire on operator-side latency
// regardless of admission outcome.
//
// operation is the AdmissionRequest.Operation literal ("CREATE" /
// "UPDATE" / "DELETE") the caller knows from its method name. The
// defaulter passes "*" because the typed CustomDefaulter signature
// doesn't carry the operation through and CREATE/UPDATE share the
// Default body anyway.
func recordWebhookDuration(start time.Time, webhook, operation string, retErr error) {
	code := 200
	if retErr != nil {
		code = 400
	}
	operatormetrics.WebhookRequestDurationSeconds.
		WithLabelValues(webhook, operation, strconv.Itoa(code)).
		Observe(time.Since(start).Seconds())
}
