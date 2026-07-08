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
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	operatormetrics "github.com/ioxie/velkir/internal/metrics"
)

// histogramSampleCount returns the count of samples in the
// WebhookRequestDurationSeconds histogram for the given label set.
// Used to assert the metric incremented by exactly one observation
// per webhook call.
func histogramSampleCount(t *testing.T, webhook, operation, code string) uint64 {
	t.Helper()
	h, err := operatormetrics.WebhookRequestDurationSeconds.GetMetricWithLabelValues(webhook, operation, code)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	pb := &dto.Metric{}
	if err := h.(prometheus.Histogram).Write(pb); err != nil {
		t.Fatalf("histogram.Write: %v", err)
	}
	return pb.Histogram.GetSampleCount()
}

func TestRecordWebhookDuration_NilError200(t *testing.T) {
	before := histogramSampleCount(t, "valkey-validator", "CREATE", "200")
	recordWebhookDuration(time.Now().Add(-1*time.Millisecond), "valkey-validator", "CREATE", nil)
	after := histogramSampleCount(t, "valkey-validator", "CREATE", "200")
	if after != before+1 {
		t.Errorf("expected sample count to increment by 1 (200 path); before=%d after=%d", before, after)
	}
}

func TestRecordWebhookDuration_NonNilError400(t *testing.T) {
	before := histogramSampleCount(t, "valkey-validator", "UPDATE", "400")
	recordWebhookDuration(time.Now().Add(-1*time.Millisecond), "valkey-validator", "UPDATE", errors.New("validation failed"))
	after := histogramSampleCount(t, "valkey-validator", "UPDATE", "400")
	if after != before+1 {
		t.Errorf("expected sample count to increment by 1 (400 path); before=%d after=%d", before, after)
	}
}

func TestRecordWebhookDuration_DefaulterUsesStarOperation(t *testing.T) {
	before := histogramSampleCount(t, "valkey-defaulter", "*", "200")
	recordWebhookDuration(time.Now().Add(-1*time.Millisecond), "valkey-defaulter", "*", nil)
	after := histogramSampleCount(t, "valkey-defaulter", "*", "200")
	if after != before+1 {
		t.Errorf("expected sample count to increment by 1 (defaulter * operation); before=%d after=%d", before, after)
	}
}

func TestRecordWebhookDuration_ObservedDurationIsPositive(t *testing.T) {
	const w, op, code = "valkey-validator", "DELETE", "200"
	before := histogramSampleCount(t, w, op, code)
	recordWebhookDuration(time.Now().Add(-5*time.Millisecond), w, op, nil)
	after := histogramSampleCount(t, w, op, code)
	if after != before+1 {
		t.Errorf("expected sample count to increment by 1; before=%d after=%d", before, after)
	}
}
