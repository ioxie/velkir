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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// captureValidateAudit runs ValidateCreate for v under an admission
// context carrying user, with a logger that records every audit emission.
// Returns the captured lines and the validation error so callers can
// assert both the verdict and the audit trail.
func captureValidateAudit(t *testing.T, user string, v *valkeyv1beta1.Valkey) ([]string, error) {
	t.Helper()
	var lines []string
	sink := funcr.New(func(prefix, args string) {
		lines = append(lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 1})
	ctx := log.IntoContext(admissionContext(user), logr.New(sink.GetSink()))
	_, err := (&ValkeyCustomValidator{}).ValidateCreate(ctx, v)
	return lines, err
}

func auditLinesFor(lines []string, event string) []string {
	var out []string
	for _, l := range lines {
		if strings.Contains(l, `"event"="`+event+`"`) {
			out = append(out, l)
		}
	}
	return out
}

func TestValidator_AdmissionRejected_EmitsAuditPerField(t *testing.T) {
	// Sub-floor down-after with no bypass annotation → one field error.
	v := sentinelCR("reject-audit", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.DownAfterMilliseconds = 500
	})
	lines, err := captureValidateAudit(t, "alice@example.com", v)
	if err == nil {
		t.Fatal("expected rejection, got nil error")
	}
	rejected := auditLinesFor(lines, "admission_rejected")
	if len(rejected) != 1 {
		t.Fatalf("expected exactly 1 admission_rejected audit line, got %d: %v", len(rejected), rejected)
	}
	for _, want := range []string{
		`"cr"="default/reject-audit"`,
		`"requestor"="alice@example.com"`,
		`"field"="spec.sentinel.downAfterMilliseconds"`,
		// reason is the stable field.ErrorType code, not the free-text
		// detail (which could carry user config / secrets).
		`"reason"="FieldValueInvalid"`,
	} {
		if !strings.Contains(rejected[0], want) {
			t.Errorf("admission_rejected line missing %q in: %s", want, rejected[0])
		}
	}
}

func TestValidator_AdmissionRejected_NotEmittedOnCleanCR(t *testing.T) {
	v := validStandalone("clean-audit", nil)
	lines, err := captureValidateAudit(t, "alice@example.com", v)
	if err != nil {
		t.Fatalf("expected acceptance, got: %v", err)
	}
	if got := auditLinesFor(lines, "admission_rejected"); len(got) != 0 {
		t.Errorf("clean CR must not emit admission_rejected; got %v", got)
	}
}

func TestValidator_AggressiveTimeoutsAccepted_EmitsAudit(t *testing.T) {
	// Bypass annotation + sub-floor timings → admitted, escape hatch used.
	v := sentinelCR("aggro-audit", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{AllowAggressiveTimeoutsAnnotation: "true"}
		v.Spec.Sentinel.DownAfterMilliseconds = 500
		v.Spec.Sentinel.FailoverTimeout = 60000
	})
	lines, err := captureValidateAudit(t, "bob@example.com", v)
	if err != nil {
		t.Fatalf("expected acceptance under bypass, got: %v", err)
	}
	accepted := auditLinesFor(lines, "aggressive_timeouts_accepted")
	if len(accepted) != 1 {
		t.Fatalf("expected exactly 1 aggressive_timeouts_accepted line, got %d: %v", len(accepted), accepted)
	}
	for _, want := range []string{
		`"cr"="default/aggro-audit"`,
		`"requestor"="bob@example.com"`,
		`"down_after_ms"="500"`,
		`"failover_timeout_ms"="60000"`,
	} {
		if !strings.Contains(accepted[0], want) {
			t.Errorf("aggressive_timeouts_accepted line missing %q in: %s", want, accepted[0])
		}
	}
	// A clean accept must NOT also look like a rejection.
	if got := auditLinesFor(lines, "admission_rejected"); len(got) != 0 {
		t.Errorf("bypass-accepted CR must not emit admission_rejected; got %v", got)
	}
}

func TestValidator_AggressiveTimeoutsAccepted_NotEmittedAtFloor(t *testing.T) {
	// At-floor sentinel CR, no bypass → admitted, no escape hatch.
	v := sentinelCR("floor-audit", nil)
	lines, err := captureValidateAudit(t, "carol@example.com", v)
	if err != nil {
		t.Fatalf("expected acceptance, got: %v", err)
	}
	if got := auditLinesFor(lines, "aggressive_timeouts_accepted"); len(got) != 0 {
		t.Errorf("at-floor CR must not emit aggressive_timeouts_accepted; got %v", got)
	}
}

// A rejected CR that ALSO carries the bypass annotation must not emit the
// accept event — the escape hatch only "accepts" when the CR is admitted.
func TestValidator_AggressiveTimeoutsAccepted_NotEmittedWhenRejected(t *testing.T) {
	v := sentinelCR("aggro-but-rejected", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{AllowAggressiveTimeoutsAnnotation: "true"}
		v.Spec.Sentinel.DownAfterMilliseconds = 500
		v.Spec.Sentinel.FailoverTimeout = 60000
		// Independent hard rejection: a banned operator-owned config directive.
		v.Spec.Valkey.Configuration = "maxmemory 100mb\nsentinel monitor x 1.2.3.4 6379 2\n"
	})
	lines, err := captureValidateAudit(t, "dave@example.com", v)
	if err == nil {
		t.Fatal("expected rejection from the banned directive")
	}
	if got := auditLinesFor(lines, "aggressive_timeouts_accepted"); len(got) != 0 {
		t.Errorf("rejected CR must not emit aggressive_timeouts_accepted; got %v", got)
	}
}
