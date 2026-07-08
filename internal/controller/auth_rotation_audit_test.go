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

// auth_rotation_audit_test.go isolates audit-emission tests for the
// data-plane rotation orchestrator. The split keeps
// auth_rotation_test.go under the soft 1500-line review threshold
// while letting the audit contract — the most compliance-relevant
// part of the rotation surface — live in one cohesive file with its
// own captureAuditLines / firstAuditLine helpers.
//
// File contents (all in this package):
//
//   - captureAuditLines + firstAuditLine: hook a logr funcr sink
//     onto a context so audit.Log emissions land in a slice, then
//     filter to the audit logger's lines.
//   - TestDriveAuthRotation_AuditEmits_*: positive coverage for the
//     three terminal branches (Succeeded / Failed / Partial).
//   - TestDriveAuthRotation_AuditNotEmittedOn{,Failed,Partial}Conflict:
//     gating-on-Status-persistence invariant for each terminal
//     branch.
//   - TestMaybeRotateAuth_AuditNotEmittedOn{FirstObservation,
//     NoAuthCleanup, SteadyState, SettleSucceededToIdle, CacheMiss}:
//     non-terminal maybeRotateAuth paths must NOT emit. Defends
//     against accidental emission outside terminal transitions.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/valkey"
)

// captureAuditLines hooks a logr funcr sink onto the context so any
// audit.Log emission produced during fn lands in the returned slice.
// Mirrors the audit package's own test helper but lives here so the
// rotation tests can assert audit-line shape end-to-end through the
// controller's emitAuthRotationAudit path. Sink Verbosity=1 matches
// audit.Log's V(0) emission level.
func captureAuditLines(t *testing.T, fn func(ctx context.Context)) []string {
	t.Helper()
	var lines []string
	sink := funcr.New(func(prefix, args string) {
		lines = append(lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 1})
	ctx := logf.IntoContext(context.Background(), logr.New(sink.GetSink()))
	fn(ctx)
	return lines
}

// firstAuditLine returns the first line that looks like an audit
// emission (contains the controller-runtime "audit" logger prefix).
// audit.Log writes via log.WithName("audit") so the rendered prefix
// includes "audit"; non-audit logs from the rotation code path
// (e.g. "auth rotation: missed rotation window") are filtered out so
// the assertion focuses on the audit stream alone.
func firstAuditLine(lines []string) (string, bool) {
	for _, l := range lines {
		if strings.Contains(l, "audit") && strings.Contains(l, `"event"=`) {
			return l, true
		}
	}
	return "", false
}

// TestDriveAuthRotation_AuditEmits_Succeeded pins the audit-trail
// emission on the all-success path: one auth_secret_rotated entry
// with outcome=succeeded, the post-change hash echoed as post_hash,
// pods_on_new_credential equal to the data-plane size, pods_on_old_credential=0. The
// audit line is the durable compliance record; the user-visible
// SecretRotated Event is asserted by TestDriveAuthRotation_AllSuccess.
func TestDriveAuthRotation_AuditEmits_Succeeded(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))
	postHash := hashAuthSecret(secret)

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	got, ok := firstAuditLine(lines)
	if !ok {
		t.Fatalf("no audit line emitted; lines=%v", lines)
	}
	for _, want := range []string{
		`"event"="auth_secret_rotated"`,
		`"cr"="ns/vk"`,
		`"outcome"="succeeded"`,
		`"secret"="ns/vk-auth"`,
		`"pods_on_new_credential"="3"`,
		`"pods_on_old_credential"="0"`,
		`"pre_hash"="` + hashPrefix(rotTestOldHash) + `"`,
		`"post_hash"="` + hashPrefix(postHash) + `"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("audit line missing %q in: %s", want, got)
		}
	}
}

// TestDriveAuthRotation_AuditEmits_Failed pins the audit emission on
// the rotation-fails-then-revert-succeeds path: outcome=failed,
// pods_on_new_credential=0 (everything reverted), pods_on_old_credential equal to the
// successfully-rotated subset. post_hash equals pre_hash because the
// cluster is back on the old credential.
func TestDriveAuthRotation_AuditEmits_Failed(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()
	rotateCallNum := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCallNum++
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			if rotateCallNum == 1 {
				for _, ep := range replicas {
					var rerr error
					if ep.Name == rotTestReplicaB {
						rerr = errors.New("AUTH failed")
					}
					out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: rerr})
				}
				out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
				return out
			}
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	got, ok := firstAuditLine(lines)
	if !ok {
		t.Fatalf("no audit line emitted; lines=%v", lines)
	}
	for _, want := range []string{
		`"event"="auth_secret_rotated"`,
		`"outcome"="failed"`,
		`"secret"="ns/vk-auth"`,
		`"pods_on_new_credential"="0"`,
		`"pods_on_old_credential"="2"`, // 1 master + 1 replica succeeded forward → both reverted
		`"pre_hash"="` + hashPrefix(rotTestOldHash) + `"`,
		`"post_hash"="` + hashPrefix(rotTestOldHash) + `"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("audit line missing %q in: %s", want, got)
		}
	}
}

// TestDriveAuthRotation_AuditEmits_Partial pins the audit emission on
// the worst-case path: outcome=partial, pods_on_new_credential counts pods that
// successfully rotated and FAILED to revert (still on new password),
// pods_on_old_credential counts pods that successfully rotated and successfully
// reverted (back on old password). Mixed-credential state.
func TestDriveAuthRotation_AuditEmits_Partial(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()
	rotateCallNum := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCallNum++
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			if rotateCallNum == 1 {
				// Forward: vk-2 fails; vk-1 + master succeed.
				for _, ep := range replicas {
					var rerr error
					if ep.Name == rotTestReplicaB {
						rerr = errors.New("AUTH failed")
					}
					out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: rerr})
				}
				out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
				return out
			}
			// Revert: master fails (still on new); vk-1 succeeds (back on old).
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster, Err: errors.New("revert AUTH failed")})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	got, ok := firstAuditLine(lines)
	if !ok {
		t.Fatalf("no audit line emitted; lines=%v", lines)
	}
	for _, want := range []string{
		`"event"="auth_secret_rotated"`,
		`"outcome"="partial"`,
		`"secret"="ns/vk-auth"`,
		`"pods_on_new_credential"="1"`, // master: rotated forward, failed to revert → still on new
		`"pods_on_old_credential"="1"`, // vk-1: rotated forward, reverted successfully → back on old
		`"pre_hash"="` + hashPrefix(rotTestOldHash) + `"`,
		`"post_hash"="` + hashPrefix(rotTestOldHash) + `"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("audit line missing %q in: %s", want, got)
		}
	}
}

// TestDriveAuthRotation_AuditNotEmittedOnConflict pins the contract
// that the audit line is gated on durable Status persistence — a
// Conflict on the terminal Succeeded Status patch must NOT leave a
// phantom audit entry behind. Mirrors the event-suppression
// invariant from TestDriveAuthRotation_StatusConflictDoesNotAdvanceCache
// and TestDriveAuthRotation_FailedConflictDoesNotEmitEvent.
func TestDriveAuthRotation_AuditNotEmittedOnConflict(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica).
		WithStatusSubresource(cr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if v, ok := obj.(*valkeyv1beta1.Valkey); ok && v.Status.Rollout != nil && v.Status.Rollout.AuthRotation != nil && v.Status.Rollout.AuthRotation.Phase == "Succeeded" {
					return apierrors.NewConflict(
						corev1.Resource("valkeys"),
						v.Name,
						fmt.Errorf("synthetic conflict on Succeeded patch"))
				}
				return cli.Status().Patch(ctx, obj, patch, statusPatchOpts(opts)...)
			},
		}).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on Status Conflict: %s", got)
	}
}

// TestMaybeRotateAuth_AuditNotEmittedOnFirstObservation pins the
// invariant that the first-observation path (Idle stamp on initial
// hash) does NOT emit an audit line. The audit event is reserved for
// terminal rotation transitions; bootstrap observations don't qualify.
// A regression that adds emitAuthRotationAudit to the first-
// observation branch would create false-positive "rotation succeeded"
// records for every CR's first reconcile after auth was added.
func TestMaybeRotateAuth_AuditNotEmittedOnFirstObservation(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	// No Status.Rollout: first-ever observation — observedHash == "".
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
	}
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on first observation (should be reserved for terminal transitions): %s", got)
	}
	// Sanity: the first-observation path DID stamp Idle.
	if cr.Status.Rollout == nil || cr.Status.Rollout.AuthRotation == nil ||
		cr.Status.Rollout.AuthRotation.Phase != rotTestPhaseIdle {
		t.Errorf("first observation must stamp Idle phase; got %+v", cr.Status.Rollout)
	}
}

// TestMaybeRotateAuth_AuditNotEmittedOnNoAuthCleanup pins the
// invariant that clearing Status.Rollout.AuthRotation when the CR
// loses its auth Secret does NOT emit an audit line — the cleanup
// is a state reset, not a terminal rotation transition. Regression
// guard for the no-auth → had-auth → no-auth lifecycle.
func TestMaybeRotateAuth_AuditNotEmittedOnNoAuthCleanup(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Spec.Auth = nil // CR no longer references an auth Secret.
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseSucceeded,
			ObservedSecretHash: rotTestOldHash,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)

	lines := captureAuditLines(t, func(ctx context.Context) {
		// secret == nil signals "no auth", driver clears the substate.
		if err := r.maybeRotateAuth(ctx, cr, nil); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on no-auth cleanup (must be silent): %s", got)
	}
	// Sanity: the cleanup path actually ran. Without this guard a
	// silent error in clearAuthRotationStatus that left the substate
	// untouched would still satisfy the no-audit assertion.
	if cr.Status.Rollout != nil && cr.Status.Rollout.AuthRotation != nil {
		t.Errorf("no-auth cleanup must null Status.Rollout.AuthRotation; got %+v", cr.Status.Rollout.AuthRotation)
	}
}

// TestDriveAuthRotation_AuditNotEmittedOnFailedConflict pins the
// audit-suppression invariant for the Failed terminal branch:
// the synthetic Conflict on the Failed Status patch must NOT leave
// a phantom audit entry behind. Mirrors
// TestDriveAuthRotation_FailedConflictDoesNotEmitEvent for the
// audit stream side of the contract.
func TestDriveAuthRotation_AuditNotEmittedOnFailedConflict(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica).
		WithStatusSubresource(cr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if v, ok := obj.(*valkeyv1beta1.Valkey); ok && v.Status.Rollout != nil && v.Status.Rollout.AuthRotation != nil && v.Status.Rollout.AuthRotation.Phase == rotTestPhaseFailed {
					return apierrors.NewConflict(
						corev1.Resource("valkeys"),
						v.Name,
						fmt.Errorf("synthetic conflict on Failed patch"))
				}
				return cli.Status().Patch(ctx, obj, patch, statusPatchOpts(opts)...)
			},
		}).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			// Forward fails on the replica; succeeded set has master only.
			// Revert call (succeeded master only) all-success → tries to
			// write Failed → synthetic Conflict.
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: errors.New("AUTH failed")})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on Failed-Conflict: %s", got)
	}
}

// TestDriveAuthRotation_AuditNotEmittedOnPartialConflict pins the
// audit-suppression invariant for the Partial terminal branch:
// a Conflict on the Partial Status patch must NOT emit a phantom
// audit entry. Symmetric with the Failed/Succeeded conflict tests;
// closes the test-coverage gap surfaced in review.
func TestDriveAuthRotation_AuditNotEmittedOnPartialConflict(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if v, ok := obj.(*valkeyv1beta1.Valkey); ok && v.Status.Rollout != nil && v.Status.Rollout.AuthRotation != nil && v.Status.Rollout.AuthRotation.Phase == rotTestPhasePartial {
					return apierrors.NewConflict(
						corev1.Resource("valkeys"),
						v.Name,
						fmt.Errorf("synthetic conflict on Partial patch"))
				}
				return cli.Status().Patch(ctx, obj, patch, statusPatchOpts(opts)...)
			},
		}).
		Build()
	rotateCallNum := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCallNum++
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			if rotateCallNum == 1 {
				// Forward: vk-2 fails; vk-1 + master succeed.
				for _, ep := range replicas {
					var rerr error
					if ep.Name == rotTestReplicaB {
						rerr = errors.New("AUTH failed")
					}
					out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: rerr})
				}
				out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
				return out
			}
			// Revert: master fails (still on new) → triggers Partial branch.
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster, Err: errors.New("revert AUTH failed")})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on Partial-Conflict: %s", got)
	}
	// Sanity: the driver actually reached the Partial branch
	// (forward partial-fail → revert master fails). Without this
	// guard a refactor that short-circuited before the Partial path
	// (e.g. into Failed or steady-state) would silently satisfy the
	// no-audit assertion. The synthetic Conflict means Status didn't
	// persist, but the in-memory CR DOES carry the Partial phase
	// computed by writeAuthRotationStatus on the patched copy
	// pre-Patch — that signals the branch ran.
	if rotateCallNum != 2 {
		t.Errorf("rotateAuth called %d time(s), want 2 (forward + revert) — Partial branch not reached", rotateCallNum)
	}
}

// TestMaybeRotateAuth_AuditNotEmittedOnSteadyState pins the
// invariant that a reconcile observing the Secret content unchanged
// (currentHash == observedHash) does NOT emit an audit line — the
// hash-match branch is the operator's "nothing to do" path and the
// audit event is reserved for terminal rotation transitions.
func TestMaybeRotateAuth_AuditNotEmittedOnSteadyState(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))
	steadyHash := hashAuthSecret(secret)
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseIdle,
			ObservedSecretHash: steadyHash,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
	}

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on steady-state reconcile (must be silent): %s", got)
	}
	// Sanity: the steady-state branch is a no-op on Status — the
	// ObservedSecretHash must NOT advance (it's already at steadyHash)
	// and the Phase must hold at Idle. Without these guards a
	// regression that short-circuited via the first-observation
	// branch (which DOES patch Status) would silently satisfy the
	// no-audit assertion despite reaching a different code path.
	if got := cr.Status.Rollout.AuthRotation.ObservedSecretHash; got != steadyHash {
		t.Errorf("steady-state must NOT advance ObservedSecretHash; got %q want %q", got, steadyHash)
	}
	if got := cr.Status.Rollout.AuthRotation.Phase; got != rotTestPhaseIdle {
		t.Errorf("steady-state must hold Phase=Idle; got %q", got)
	}
}

// TestMaybeRotateAuth_AuditNotEmittedOnSettleSucceededToIdle pins
// the Idle-settle path: the reconcile after a successful rotation
// transitions Status from Succeeded → Idle WITHOUT re-emitting
// auth_secret_rotated. The original Succeeded transition emitted
// the audit line; the settle is housekeeping and must stay silent
// or every successful rotation would be double-counted in the
// compliance stream.
func TestMaybeRotateAuth_AuditNotEmittedOnSettleSucceededToIdle(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))
	hash := hashAuthSecret(secret)
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseSucceeded,
			ObservedSecretHash: hash,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
	}

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on Succeeded → Idle settle (must be silent): %s", got)
	}
	// Sanity: the settle actually advanced to Idle. Without this
	// guard a regression that left the Phase at Succeeded would still
	// satisfy the no-audit assertion.
	if got := cr.Status.Rollout.AuthRotation.Phase; got != rotTestPhaseIdle {
		t.Errorf("settle path must transition Succeeded → Idle; phase=%q", got)
	}
}

// TestMaybeRotateAuth_AuditNotEmittedOnCacheMiss pins the
// operator-restart path: a reconcile sees a content-hash mismatch
// (Secret changed) but the in-memory password cache is empty (or
// stale). Without the OLD password the operator cannot drive
// AUTH-with-old + SET-new safely, so it stamps the new hash as the
// authoritative observation and lets the rotation slip — the cache-
// miss path is documented in maybeRotateAuth as "missed rotation
// window". It must NOT emit an audit line: no rotation drove, and
// emitting `outcome=succeeded` for a passive observation would
// poison compliance dashboards.
func TestMaybeRotateAuth_AuditNotEmittedOnCacheMiss(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseIdle,
			ObservedSecretHash: rotTestOldHash, // arbitrary previous hash
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
	}
	// Cache intentionally NOT populated — simulates operator restart.
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	lines := captureAuditLines(t, func(ctx context.Context) {
		if err := r.maybeRotateAuth(ctx, cr, secret); err != nil {
			t.Fatalf("maybeRotateAuth: %v", err)
		}
	})
	if got, ok := firstAuditLine(lines); ok {
		t.Errorf("audit line emitted on cache-miss (must be silent): %s", got)
	}
	// Sanity: the cache-miss path advanced the hash without rotation.
	if got := cr.Status.Rollout.AuthRotation.ObservedSecretHash; got != hashAuthSecret(secret) {
		t.Errorf("cache-miss path must adopt the current Secret content hash; got %q", got)
	}
}
