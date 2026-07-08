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

// clientkill_audit_test.go covers the audit-trail emission for the
// CLIENT KILL destructive data-plane action. Reuses the
// package-level captureAuditLines / firstAuditLine helpers and the
// crForRotation / dataPlanePod fixtures defined alongside the auth-
// rotation audit tests.

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/ioxie/velkir/internal/valkey"
)

// fakeClientKillIssuer records calls and returns a canned count/err so
// issueClientKillOnDemoted can be exercised without opening sockets.
type fakeClientKillIssuer struct {
	killed int
	err    error
	calls  int
}

func (f *fakeClientKillIssuer) KillNormalClients(_ context.Context, _, _ string) (int, error) {
	f.calls++
	return f.killed, f.err
}

var _ valkey.ClientKillIssuer = (*fakeClientKillIssuer)(nil)

// TestIssueClientKillOnDemoted_AuditEmitsOnDrop pins the audit-trail
// record for CLIENT KILL: a kill that drops >0 connections emits one
// clients_killed entry carrying pod, count, and reason.
func TestIssueClientKillOnDemoted_AuditEmitsOnDrop(t *testing.T) {
	r := &ValkeyReconciler{ClientKillIssuer: &fakeClientKillIssuer{killed: 4}}
	v := crForRotation()
	demoted := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.issueClientKillOnDemoted(ctx, v, []*corev1.Pod{&demoted}, "pw"); err != nil {
			t.Fatalf("issueClientKillOnDemoted: %v", err)
		}
	})
	got, ok := firstAuditLine(lines)
	if !ok {
		t.Fatalf("no audit line emitted; lines=%v", lines)
	}
	// Exactly one audit emission for one demoted pod — guards against a
	// duplicate-emit regression that firstAuditLine alone wouldn't catch.
	auditCount := 0
	for _, l := range lines {
		if strings.Contains(l, "audit") && strings.Contains(l, `"event"=`) {
			auditCount++
		}
	}
	if auditCount != 1 {
		t.Errorf("want exactly 1 audit line for 1 demoted pod, got %d: %v", auditCount, lines)
	}
	for _, want := range []string{
		`"event"="clients_killed"`,
		`"cr"="ns/vk"`,
		`"pod"="` + rotTestReplicaA + `"`,
		`"count"="4"`,
		`"reason"="primary_demotion"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("audit line missing %q in: %s", want, got)
		}
	}
}

// TestIssueClientKillOnDemoted_NoAuditOnZeroDrop pins the guard that a
// kill dropping zero connections changed no live state and must NOT emit
// an audit entry — otherwise every reconcile that re-runs the best-effort
// pass on already-drained pods would flood the compliance stream.
func TestIssueClientKillOnDemoted_NoAuditOnZeroDrop(t *testing.T) {
	r := &ValkeyReconciler{ClientKillIssuer: &fakeClientKillIssuer{killed: 0}}
	v := crForRotation()
	demoted := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.issueClientKillOnDemoted(ctx, v, []*corev1.Pod{&demoted}, "pw"); err != nil {
			t.Fatalf("issueClientKillOnDemoted: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on zero-drop kill (must be silent): %s", got)
	}
}
