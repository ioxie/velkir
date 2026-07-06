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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// captureAudit runs fn with a logr.Logger that records every emission so
// tests can assert the audit line shape without stdout interception.
func captureAudit(t *testing.T, fn func(ctx context.Context)) []string {
	t.Helper()
	var lines []string
	captured := funcr.New(func(prefix, args string) {
		lines = append(lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 1})
	fn(log.IntoContext(context.Background(), logr.New(captured.GetSink())))
	return lines
}

func TestJoinSortedTargets_Deterministic(t *testing.T) {
	a := joinSortedTargets([]string{"s2", "s0", "s1"})
	b := joinSortedTargets([]string{"s1", "s2", "s0"})
	if a != b {
		t.Fatalf("joinSortedTargets must be order-independent; %q != %q", a, b)
	}
	if a != "s0,s1,s2" {
		t.Errorf("joinSortedTargets = %q, want s0,s1,s2", a)
	}
	if got := joinSortedTargets(nil); got != "" {
		t.Errorf("joinSortedTargets(nil) = %q, want empty", got)
	}
}
func TestAuditSentinelReset_Shape(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: pvcLossTestCRName}
	lines := captureAudit(t, func(ctx context.Context) {
		auditSentinelReset(ctx, cr, []string{"s1", "s0"}, "degraded")
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{
		`"event"="sentinel_reset_issued"`,
		`"cr"="ns/vk0"`,
		`"reason"="degraded"`,
		`"targets"="s0,s1"`, // sorted, joined
		`"requestor"="operator:reconciler"`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("audit line missing %q in: %s", want, lines[0])
		}
	}
}

func TestAuditRoleLabel_Shape(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: pvcLossTestCRName}
	lines := captureAudit(t, func(ctx context.Context) {
		auditRoleLabel(ctx, cr, "vk0-1", "<unset>", "primary", "switch-master")
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{
		`"event"="pod_label_reconciled"`,
		`"cr"="ns/vk0"`,
		`"pod"="vk0-1"`,
		`"from"="<unset>"`,
		`"to"="primary"`,
		`"trigger"="switch-master"`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("audit line missing %q in: %s", want, lines[0])
		}
	}
}

func TestAuditFailover_Shape(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: pvcLossTestCRName}
	lines := captureAudit(t, func(ctx context.Context) {
		auditFailover(ctx, cr, "vk0-0", "vk0-1", "vk0-7c9f")
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{
		`"event"="sentinel_failover_issued"`,
		`"cr"="ns/vk0"`,
		`"old_primary"="vk0-0"`,
		`"new_primary"="vk0-1"`,
		`"reason"="rollout"`,
		`"rollout_id"="vk0-7c9f"`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("failover line missing %q in: %s", want, lines[0])
		}
	}
}

func TestAuditFailover_OmitsEmptyRolloutID(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: pvcLossTestCRName}
	lines := captureAudit(t, func(ctx context.Context) {
		auditFailover(ctx, cr, "vk0-0", "vk0-1", "")
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d: %v", len(lines), lines)
	}
	if strings.Contains(lines[0], "rollout_id") {
		t.Errorf("empty rollout_id must be omitted; got: %s", lines[0])
	}
}

func TestAuditReconciliationPaused_Shape(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: pvcLossTestCRName}
	lines := captureAudit(t, func(ctx context.Context) {
		auditReconciliationPaused(ctx, cr, 42)
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{
		`"event"="reconciliation_paused"`,
		`"cr"="ns/vk0"`,
		`"generation"="42"`,
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("paused line missing %q in: %s", want, lines[0])
		}
	}
}

func newValkeyWithRolloutGen(gen string) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = pvcLossTestCRName
	if gen != "" {
		v.Annotations = map[string]string{ManualRolloutAnnotation: gen}
	}
	return v
}

func TestMaybeAuditManualRollout_SeedThenEdge(t *testing.T) {
	r := &ValkeyReconciler{}
	v := newValkeyWithRolloutGen("1")

	// First observation seeds the baseline without emitting — a CR that
	// already carries the annotation (or the first pass after a restart)
	// is not a fresh trigger.
	seed := captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	if len(seed) != 0 {
		t.Fatalf("first observation must not emit; got %v", seed)
	}

	// Re-observing the same value is a no-op.
	same := captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	if len(same) != 0 {
		t.Fatalf("unchanged value must not emit; got %v", same)
	}

	// Bumping the value emits exactly one entry carrying both generations.
	v.Annotations[ManualRolloutAnnotation] = "2"
	bump := captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	if len(bump) != 1 {
		t.Fatalf("value bump must emit once; got %d: %v", len(bump), bump)
	}
	for _, want := range []string{
		`"event"="manual_rollout_triggered"`,
		`"cr"="ns/vk0"`,
		`"generation"="2"`,
		`"previous_generation"="1"`,
	} {
		if !strings.Contains(bump[0], want) {
			t.Errorf("audit line missing %q in: %s", want, bump[0])
		}
	}
}

func TestMaybeAuditManualRollout_NilAnnotationsSafe(t *testing.T) {
	// A CR fetched with no annotations has a nil Annotations map. Reading
	// a nil map is safe in Go (yields ""), so this must neither panic nor
	// emit — it just seeds an empty baseline.
	r := &ValkeyReconciler{}
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = pvcLossTestCRName
	lines := captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	if len(lines) != 0 {
		t.Fatalf("nil-annotations CR must not emit; got %v", lines)
	}
}

func TestMaybeAuditManualRollout_ClearIsNotTrigger(t *testing.T) {
	r := &ValkeyReconciler{}
	v := newValkeyWithRolloutGen("7")
	// Seed at "7".
	_ = captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	// Remove the annotation entirely: not a rollout trigger.
	delete(v.Annotations, ManualRolloutAnnotation)
	cleared := captureAudit(t, func(ctx context.Context) { r.maybeAuditManualRollout(ctx, v) })
	if len(cleared) != 0 {
		t.Fatalf("clearing the annotation must not emit; got %v", cleared)
	}
}
