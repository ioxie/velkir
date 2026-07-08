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

// auth_rotation_metric_test.go pins the valkey_secret_rotation_total
// counter wiring: each terminal rotation outcome must increment the
// counter exactly once with the matching `result` label, and — like the
// event + audit emissions it sits beside — must NOT increment when the
// terminal Status patch fails to persist (Conflict). The metric is
// specified in lockstep with the audit log (the audit-side contract
// lives in auth_rotation_audit_test.go), so the RotateAuthFn fixtures
// here mirror those three outcome drives. The `result` enum
// {success,reverted,failed} coarsens the audit `outcome`
// {succeeded,failed,partial}: an audit `failed` (clean revert) maps to
// result=reverted, and an audit `partial` (mixed-credential) maps to
// result=failed — these tests are the executable record of that mapping.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/valkey"
)

// allSuccessRotate succeeds on every endpoint forward — the all-success
// terminal path (audit outcome=succeeded → result=success).
func allSuccessRotate(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
	out := make([]valkey.PodResult, 0, len(replicas)+1)
	for _, ep := range replicas {
		out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
	}
	out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
	return out
}

// revertedRotateFn returns a fresh fixture for the clean-revert outcome:
// forward fails on replicaB (replicaA + master succeed), then the revert
// of the successfully-rotated subset all-succeeds — cluster back on the
// old credential (Status=Failed, audit outcome=failed → result=reverted).
// Stateful per call sequence, so each test gets its own counter.
func revertedRotateFn() rotateAuthFunc {
	call := 0
	return func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
		call++
		out := make([]valkey.PodResult, 0, len(replicas)+1)
		if call == 1 {
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
		// Revert: all-success → clean rollback to old credential.
		for _, ep := range replicas {
			out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
		}
		out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
		return out
	}
}

// partialRotateFn returns a fresh fixture for the mixed-credential
// outcome: forward fails on replicaB, then the revert fails on the master
// (stuck on new) — Status=Partial, audit outcome=partial → result=failed.
func partialRotateFn() rotateAuthFunc {
	call := 0
	return func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
		call++
		out := make([]valkey.PodResult, 0, len(replicas)+1)
		if call == 1 {
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
		// Revert: master fails (stuck on new) → mixed-credential Partial.
		for _, ep := range replicas {
			out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
		}
		out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster, Err: errors.New("revert AUTH failed")})
		return out
	}
}

// rotationFixture builds the shared rotation fixture: 1 primary + 2
// replicas, cached old password, Idle starting phase. extraBuild lets a
// caller layer interceptors onto the fake client before Build().
func rotationFixture(t *testing.T, rotateFn rotateAuthFunc, extraBuild func(*fake.ClientBuilder) *fake.ClientBuilder) (*ValkeyReconciler, *valkeyv1beta1.Valkey, *corev1.Secret) {
	t.Helper()
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseIdle,
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica)
	b := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr)
	if extraBuild != nil {
		b = extraBuild(b)
	}
	r := &ValkeyReconciler{
		Client:       b.Build(),
		Recorder:     k8sevents.NewFakeRecorder(8),
		RotateAuthFn: rotateFn,
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	return r, cr, authSecret("vk-auth", []byte(rotTestNewPwd))
}

// assertSingleRotationMetric checks the counter holds exactly one sample,
// carrying wantResult — pinning both the value (1) and that no other
// `result` label was incremented on the same drive.
func assertSingleRotationMetric(t *testing.T, cr *valkeyv1beta1.Valkey, wantResult string) {
	t.Helper()
	got := testutil.ToFloat64(operatormetrics.SecretRotationTotal.WithLabelValues(cr.Namespace, cr.Name, wantResult))
	if got != 1 {
		t.Errorf("valkey_secret_rotation_total{result=%q} = %v; want 1", wantResult, got)
	}
	if n := testutil.CollectAndCount(operatormetrics.SecretRotationTotal); n != 1 {
		t.Errorf("expected exactly one labelled series after one rotation; got %d (a different result label was also incremented)", n)
	}
}

// conflictOnPhase returns an interceptor that fails the terminal Status
// patch stamping conflictPhase with a synthetic Conflict, exercising the
// `if persisted` guard's false branch.
func conflictOnPhase(conflictPhase string) func(*fake.ClientBuilder) *fake.ClientBuilder {
	return func(b *fake.ClientBuilder) *fake.ClientBuilder {
		return b.WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, _ string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if v, ok := obj.(*valkeyv1beta1.Valkey); ok &&
					v.Status.Rollout != nil && v.Status.Rollout.AuthRotation != nil &&
					v.Status.Rollout.AuthRotation.Phase == conflictPhase {
					return apierrors.NewConflict(corev1.Resource("valkeys"), v.Name,
						fmt.Errorf("synthetic conflict on %s patch", conflictPhase))
				}
				return cli.Status().Patch(ctx, obj, patch, statusPatchOpts(opts)...)
			},
		})
	}
}

// driveOnce runs maybeRotateAuth through rotateFn, counting rotate calls
// so a caller can prove the intended terminal branch was actually reached
// (forward-only=1, forward+revert=2) even when the outcome is suppressed.
func driveOnce(t *testing.T, rotateFn rotateAuthFunc, extraBuild func(*fake.ClientBuilder) *fake.ClientBuilder) (cr *valkeyv1beta1.Valkey, rotateCalls int) {
	t.Helper()
	wrapped := func(ctx context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, o, n string) []valkey.PodResult {
		rotateCalls++
		return rotateFn(ctx, replicas, master, o, n)
	}
	r, cr, secret := rotationFixture(t, wrapped, extraBuild)
	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	return cr, rotateCalls
}

// --- positive: each terminal outcome increments its result label once ---

func TestSecretRotationMetric_Success(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	cr, calls := driveOnce(t, allSuccessRotate, nil)
	if calls != 1 {
		t.Fatalf("rotate called %d time(s), want 1 (forward-only success) — branch not reached", calls)
	}
	assertSingleRotationMetric(t, cr, "success")
}

func TestSecretRotationMetric_Reverted(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	cr, calls := driveOnce(t, revertedRotateFn(), nil)
	if calls != 2 {
		t.Fatalf("rotate called %d time(s), want 2 (forward+revert) — Failed branch not reached", calls)
	}
	assertSingleRotationMetric(t, cr, "reverted")
}

func TestSecretRotationMetric_Failed(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	cr, calls := driveOnce(t, partialRotateFn(), nil)
	if calls != 2 {
		t.Fatalf("rotate called %d time(s), want 2 (forward+revert) — Partial branch not reached", calls)
	}
	assertSingleRotationMetric(t, cr, "failed")
}

// --- negative: a Conflict on the terminal Status patch suppresses the
// metric (lockstep with the audit-line suppression in
// auth_rotation_audit_test.go). Guards against a refactor moving the
// .Inc() outside the `if persisted` block. ---

func TestSecretRotationMetric_NotIncrementedOnSuccessConflict(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	_, calls := driveOnce(t, allSuccessRotate, conflictOnPhase(rotTestPhaseSucceeded))
	if calls != 1 {
		t.Fatalf("rotate called %d time(s), want 1 — Succeeded branch not reached", calls)
	}
	if n := testutil.CollectAndCount(operatormetrics.SecretRotationTotal); n != 0 {
		t.Errorf("metric incremented despite Status Conflict on Succeeded patch; got %d series, want 0", n)
	}
}

func TestSecretRotationMetric_NotIncrementedOnRevertedConflict(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	_, calls := driveOnce(t, revertedRotateFn(), conflictOnPhase(rotTestPhaseFailed))
	if calls != 2 {
		t.Fatalf("rotate called %d time(s), want 2 — Failed branch not reached", calls)
	}
	if n := testutil.CollectAndCount(operatormetrics.SecretRotationTotal); n != 0 {
		t.Errorf("metric incremented despite Status Conflict on Failed patch; got %d series, want 0", n)
	}
}

func TestSecretRotationMetric_NotIncrementedOnFailedConflict(t *testing.T) {
	operatormetrics.SecretRotationTotal.Reset()
	_, calls := driveOnce(t, partialRotateFn(), conflictOnPhase(rotTestPhasePartial))
	if calls != 2 {
		t.Fatalf("rotate called %d time(s), want 2 — Partial branch not reached", calls)
	}
	if n := testutil.CollectAndCount(operatormetrics.SecretRotationTotal); n != 0 {
		t.Errorf("metric incremented despite Status Conflict on Partial patch; got %d series, want 0", n)
	}
}
