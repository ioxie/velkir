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
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/valkey"
)

// authRotationScheme builds a minimal scheme for the rotation tests:
// corev1 (Pod, Secret) + valkeyv1beta1 (the CR + status subresource).
func authRotationScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 add to scheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1 add to scheme: %v", err)
	}
	return s
}

// Test-fixture string constants. Extracted to satisfy goconst on the
// tests that re-use these literals; the test bodies become slightly
// noisier but the linter passes and the intent is unchanged.
const (
	rotTestCRName         = "vk"
	rotTestPrimary        = "vk-0"
	rotTestReplicaA       = "vk-1"
	rotTestReplicaB       = "vk-2"
	rotTestPhaseIdle      = "Idle"
	rotTestPhaseSucceeded = "Succeeded"
	rotTestPhaseFailed    = "Failed"
	rotTestPhasePartial   = "Partial"
	rotTestOldPwd         = "old-pwd"
	rotTestNewPwd         = "new-pwd"
	rotTestOldHash        = "h-old"
)

// crForRotation returns a minimal Valkey CR with auth wired to a
// named Secret. Tests vary Status.Rollout / Spec details via field
// assignment after the call.
func crForRotation() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rotTestCRName,
			Namespace: "ns",
		},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Auth: &valkeyv1beta1.AuthSpec{
				SecretName: rotTestCRName + "-auth",
			},
		},
	}
}

func authSecret(name string, password []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
		},
		Data: map[string][]byte{"password": password},
	}
}

func dataPlanePod(name, ip, role string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				CRLabel:        "vk",
				ComponentLabel: componentValkey,
				RoleLabel:      role,
			},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// drainAllEventsLocal mirrors fsm_test.go's drainAllEvents. Reads up
// to 16 events from a non-blocking channel and returns the strings.
// Defined locally to avoid coupling this file to fsm_test.go's
// internal helper.
func drainAllEventsLocal(ch <-chan string) []string {
	out := []string{}
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestHashAuthSecret pins the hash function's contract — same content
// → same hash; metadata-only edits (resourceVersion change) do NOT
// change the hash since we only hash the password bytes.
func TestHashAuthSecret(t *testing.T) {
	t.Run("nil secret", func(t *testing.T) {
		if h := hashAuthSecret(nil); h != "" {
			t.Fatalf("nil → %q, want empty", h)
		}
	})
	t.Run("empty data", func(t *testing.T) {
		if h := hashAuthSecret(&corev1.Secret{}); h != "" {
			t.Fatalf("missing password key → %q, want empty", h)
		}
	})
	t.Run("missing password key", func(t *testing.T) {
		s := &corev1.Secret{Data: map[string][]byte{"other": []byte("x")}}
		if h := hashAuthSecret(s); h != "" {
			t.Fatalf("missing password key → %q, want empty", h)
		}
	})
	t.Run("same content same hash", func(t *testing.T) {
		s1 := authSecret("a", []byte("hunter2"))
		s2 := authSecret("a", []byte("hunter2"))
		if hashAuthSecret(s1) != hashAuthSecret(s2) {
			t.Fatalf("hashes differ for identical content")
		}
	})
	t.Run("different content different hash", func(t *testing.T) {
		s1 := authSecret("a", []byte("hunter2"))
		s2 := authSecret("a", []byte("hunter3"))
		if hashAuthSecret(s1) == hashAuthSecret(s2) {
			t.Fatalf("hashes match for different content")
		}
	})
	t.Run("metadata-only change does not change hash", func(t *testing.T) {
		s1 := authSecret("a", []byte("hunter2"))
		s2 := authSecret("a", []byte("hunter2"))
		s2.ResourceVersion = "999" // simulates a metadata-only edit
		s2.Annotations = map[string]string{"unrelated": "annotation"}
		if hashAuthSecret(s1) != hashAuthSecret(s2) {
			t.Fatalf("metadata-only change altered the hash")
		}
	})
	t.Run("empty password bytes treated as no-auth", func(t *testing.T) {
		// A Secret with `password` key present but bytes empty should
		// hash to "" (the operator's "no auth" sentinel) so the driver
		// treats it the same as an absent key. SHA-256 of empty bytes
		// IS a valid digest — but rotating against an empty password
		// is meaningless, so the helper short-circuits.
		s := &corev1.Secret{Data: map[string][]byte{"password": {}}}
		if h := hashAuthSecret(s); h != "" {
			t.Fatalf("empty bytes → %q, want empty (no-auth equivalence)", h)
		}
	})
}

// TestCurrentAuthRotationPhase pins the phase reader's defaults. Empty
// status / nil substate / unset phase string all map to Idle.
func TestCurrentAuthRotationPhase(t *testing.T) {
	t.Run("nil rollout", func(t *testing.T) {
		v := &valkeyv1beta1.Valkey{}
		if got := currentAuthRotationPhase(v); got != valkeyv1beta1.AuthRotationPhaseIdle {
			t.Fatalf("nil rollout → %q, want Idle", got)
		}
	})
	t.Run("nil AuthRotation substate", func(t *testing.T) {
		v := &valkeyv1beta1.Valkey{
			Status: valkeyv1beta1.ValkeyStatus{Rollout: &valkeyv1beta1.RolloutStatus{}},
		}
		if got := currentAuthRotationPhase(v); got != valkeyv1beta1.AuthRotationPhaseIdle {
			t.Fatalf("nil substate → %q, want Idle", got)
		}
	})
	t.Run("empty phase string", func(t *testing.T) {
		v := &valkeyv1beta1.Valkey{
			Status: valkeyv1beta1.ValkeyStatus{
				Rollout: &valkeyv1beta1.RolloutStatus{
					AuthRotation: &valkeyv1beta1.AuthRotationStatus{},
				},
			},
		}
		if got := currentAuthRotationPhase(v); got != valkeyv1beta1.AuthRotationPhaseIdle {
			t.Fatalf("empty phase → %q, want Idle", got)
		}
	})
	t.Run("explicit phases round-trip", func(t *testing.T) {
		for _, phase := range []valkeyv1beta1.AuthRotationPhase{
			valkeyv1beta1.AuthRotationPhaseIdle,
			valkeyv1beta1.AuthRotationPhaseInProgress,
			valkeyv1beta1.AuthRotationPhaseSucceeded,
			valkeyv1beta1.AuthRotationPhaseFailed,
			valkeyv1beta1.AuthRotationPhasePartial,
		} {
			v := &valkeyv1beta1.Valkey{
				Status: valkeyv1beta1.ValkeyStatus{
					Rollout: &valkeyv1beta1.RolloutStatus{
						AuthRotation: &valkeyv1beta1.AuthRotationStatus{Phase: string(phase)},
					},
				},
			}
			if got := currentAuthRotationPhase(v); got != phase {
				t.Errorf("phase=%q → got %q", phase, got)
			}
		}
	})
}

func TestObservedAuthSecretHash(t *testing.T) {
	t.Run("nil rollout returns empty", func(t *testing.T) {
		v := &valkeyv1beta1.Valkey{}
		if got := observedAuthSecretHash(v); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})
	t.Run("returns set hash", func(t *testing.T) {
		v := &valkeyv1beta1.Valkey{
			Status: valkeyv1beta1.ValkeyStatus{
				Rollout: &valkeyv1beta1.RolloutStatus{
					AuthRotation: &valkeyv1beta1.AuthRotationStatus{
						ObservedSecretHash: "deadbeef",
					},
				},
			},
		}
		if got := observedAuthSecretHash(v); got != "deadbeef" {
			t.Fatalf("got %q, want deadbeef", got)
		}
	})
}

// TestClassifyRotateResults pins the failure/success splitter. Order
// preservation matters because endpointNames(...) renders the slice
// directly into Status.Message and event bodies.
func TestClassifyRotateResults(t *testing.T) {
	results := []valkey.PodResult{
		{Endpoint: valkey.Endpoint{Name: rotTestPrimary}, Phase: valkey.RotationPhaseReplica, Err: nil},
		{Endpoint: valkey.Endpoint{Name: rotTestReplicaA}, Phase: valkey.RotationPhaseReplica, Err: errors.New("boom")},
		{Endpoint: valkey.Endpoint{Name: rotTestReplicaB}, Phase: valkey.RotationPhaseMaster, Err: nil},
	}
	failed, succeeded := classifyRotateResults(results)
	if len(failed) != 1 || failed[0].Endpoint.Name != rotTestReplicaA {
		t.Fatalf("failed=%v, want [%s]", failed, rotTestReplicaA)
	}
	if len(succeeded) != 2 || succeeded[0].Endpoint.Name != rotTestPrimary || succeeded[1].Endpoint.Name != rotTestReplicaB {
		t.Fatalf("succeeded=%v, want [%s, %s]", succeeded, rotTestPrimary, rotTestReplicaB)
	}
}

func TestSplitDataPlaneEndpoints(t *testing.T) {
	t.Run("no master labelled returns ok=false", func(t *testing.T) {
		pods := []corev1.Pod{
			dataPlanePod("vk-0", "10.0.0.1", roleValueReplica),
			dataPlanePod("vk-1", "10.0.0.2", roleValueReplica),
		}
		_, _, ok := splitDataPlaneEndpoints(pods)
		if ok {
			t.Fatalf("ok=true, want false (no master labelled)")
		}
	})

	t.Run("happy path: master + replicas, sorted", func(t *testing.T) {
		pods := []corev1.Pod{
			dataPlanePod("vk-2", "10.0.0.3", roleValueReplica),
			dataPlanePod("vk-0", "10.0.0.1", roleValuePrimary),
			dataPlanePod("vk-1", "10.0.0.2", roleValueReplica),
		}
		replicas, master, ok := splitDataPlaneEndpoints(pods)
		if !ok {
			t.Fatal("ok=false, want true")
		}
		if master.Name != "vk-0" {
			t.Errorf("master=%q, want vk-0", master.Name)
		}
		if len(replicas) != 2 {
			t.Fatalf("replicas=%d, want 2", len(replicas))
		}
		if replicas[0].Name != rotTestReplicaA || replicas[1].Name != rotTestReplicaB {
			t.Errorf("replicas not name-sorted: %v", replicas)
		}
		if master.Addr != "10.0.0.1:6379" {
			t.Errorf("master addr=%q, want 10.0.0.1:6379", master.Addr)
		}
	})

	t.Run("any pod missing PodIP refuses rotation", func(t *testing.T) {
		// Converge invariant: rotation refuses to act on a partial pod
		// view. If any pod's PodIP is unobserved (informer-cache lag,
		// pod still scheduling) we cannot rotate it on this pass, and
		// silently rotating only the observable subset would advance
		// ObservedSecretHash and strand the missing pod on the old
		// credential. Defer until the next reconcile sees every pod.
		pods := []corev1.Pod{
			dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary),
			dataPlanePod(rotTestReplicaA, "", roleValueReplica),
		}
		_, _, ok := splitDataPlaneEndpoints(pods)
		if ok {
			t.Fatal("ok=true on unobserved PodIP, want false")
		}
	})

	t.Run("any unlabelled pod refuses rotation", func(t *testing.T) {
		// Converge invariant: same shape as the PodIP-missing case.
		// On a fresh sentinel cluster the secret-edit reconcile can
		// race the async role-labeler; an unlabelled pod here means
		// the labeler hasn't caught up yet. Refuse rotation until the
		// full set is labelled so no pod gets stranded on the old
		// password.
		pods := []corev1.Pod{
			dataPlanePod("vk-0", "10.0.0.1", roleValuePrimary),
			dataPlanePod("vk-1", "10.0.0.2", ""),
		}
		_, _, ok := splitDataPlaneEndpoints(pods)
		if ok {
			t.Fatal("ok=true on unlabelled pod, want false")
		}
	})

	t.Run("split-brain (multiple primaries) returns ok=false", func(t *testing.T) {
		// Transient split-brain during failover: two pods labelled
		// primary at the same time. The driver must refuse to rotate
		// rather than silently picking last-iterated. Pod-list order
		// from the apiserver is not stable, so a silent pick risks
		// rotating against the wrong primary.
		pods := []corev1.Pod{
			dataPlanePod(rotTestPrimary, "10.0.0.1", roleValuePrimary),
			dataPlanePod(rotTestReplicaA, "10.0.0.2", roleValuePrimary),
			dataPlanePod(rotTestReplicaB, "10.0.0.3", roleValueReplica),
		}
		replicas, master, ok := splitDataPlaneEndpoints(pods)
		if ok {
			t.Fatalf("ok=true on split-brain (got master=%q), want false", master.Name)
		}
		if len(replicas) != 0 {
			t.Errorf("replicas leaked through split-brain refusal: %v", replicas)
		}
		if master.Name != "" || master.Addr != "" {
			t.Errorf("master leaked through split-brain refusal: %+v", master)
		}
	})
}

func TestSplitRevertEndpoints(t *testing.T) {
	succeeded := []valkey.PodResult{
		{Endpoint: valkey.Endpoint{Name: "vk-2", Addr: "10.0.0.3:6379"}, Phase: valkey.RotationPhaseReplica},
		{Endpoint: valkey.Endpoint{Name: "vk-0", Addr: "10.0.0.1:6379"}, Phase: valkey.RotationPhaseMaster},
		{Endpoint: valkey.Endpoint{Name: "vk-1", Addr: "10.0.0.2:6379"}, Phase: valkey.RotationPhaseReplica},
	}
	replicas, master := splitRevertEndpoints(succeeded)
	if master.Name != "vk-0" {
		t.Errorf("master=%q, want vk-0", master.Name)
	}
	if len(replicas) != 2 {
		t.Fatalf("replicas=%d, want 2", len(replicas))
	}
	if replicas[0].Name != rotTestReplicaA || replicas[1].Name != rotTestReplicaB {
		t.Errorf("replicas not sorted: %v", replicas)
	}
}

func TestEndpointNames(t *testing.T) {
	t.Run("empty returns empty string", func(t *testing.T) {
		if got := endpointNames(nil); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})
	t.Run("sorted comma-joined", func(t *testing.T) {
		results := []valkey.PodResult{
			{Endpoint: valkey.Endpoint{Name: "vk-c"}},
			{Endpoint: valkey.Endpoint{Name: "vk-a"}},
			{Endpoint: valkey.Endpoint{Name: "vk-b"}},
		}
		if got := endpointNames(results); got != "vk-a,vk-b,vk-c" {
			t.Fatalf("got %q, want vk-a,vk-b,vk-c", got)
		}
	})
}

// TestAuthPasswordCache pins the cache helpers' read/write contract
// and per-CR isolation.
func TestAuthPasswordCache(t *testing.T) {
	r := &ValkeyReconciler{}
	k1 := types.NamespacedName{Namespace: "ns", Name: "vk1"}
	k2 := types.NamespacedName{Namespace: "ns", Name: "vk2"}

	if _, ok := r.lookupAuthPasswordCache(k1); ok {
		t.Fatal("cache hit on cold key")
	}

	r.cacheAuthPassword(k1, "pwdA", "hashA")
	r.cacheAuthPassword(k2, "pwdB", "hashB")

	got1, ok := r.lookupAuthPasswordCache(k1)
	if !ok || got1.password != "pwdA" || got1.hash != "hashA" {
		t.Errorf("k1: got %v, want pwdA/hashA", got1)
	}
	got2, ok := r.lookupAuthPasswordCache(k2)
	if !ok || got2.password != "pwdB" || got2.hash != "hashB" {
		t.Errorf("k2: got %v, want pwdB/hashB", got2)
	}

	r.cacheAuthPassword(k1, "pwdA2", "hashA2")
	got1b, _ := r.lookupAuthPasswordCache(k1)
	if got1b.password != "pwdA2" || got1b.hash != "hashA2" {
		t.Errorf("re-cache did not overwrite: %v", got1b)
	}
	got2b, _ := r.lookupAuthPasswordCache(k2)
	if got2b.password != "pwdB" {
		t.Errorf("k2 contaminated: %v", got2b)
	}
}

// TestWriteAuthRotationStatus pins the writer's contract:
//
//   - first write stamps StartedAt + LastTransitionAt for InProgress.
//   - terminal phase carries StartedAt forward (so dashboards can
//     compute total rotation duration).
//   - same-phase + same-hash + same-message is a no-op (avoids
//     hot-loop status churn).
//   - Idle clears StartedAt.
func TestWriteAuthRotationStatus(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	// First write: InProgress stamps StartedAt + LastTransitionAt.
	ctx := context.Background()
	persisted, err := r.writeAuthRotationStatus(ctx, cr, valkeyv1beta1.AuthRotationPhaseInProgress, "h1", "rotating")
	if err != nil {
		t.Fatalf("InProgress write: %v", err)
	}
	if !persisted {
		t.Fatal("InProgress write reported persisted=false on a clean fake client")
	}
	got := cr.Status.Rollout.AuthRotation
	if got == nil {
		t.Fatal("AuthRotation not stamped")
		return
	}
	if got.Phase != "InProgress" || got.ObservedSecretHash != "h1" || got.Message != "rotating" {
		t.Errorf("stamped fields: %+v", got)
	}
	if got.StartedAt == nil || got.LastTransitionAt == nil {
		t.Errorf("StartedAt or LastTransitionAt not stamped: %+v", got)
	}
	startedAt := got.StartedAt.DeepCopy()

	// Same-phase + same-hash + same-message: idempotent. Treats as
	// persisted=true because the prior reconcile durably stamped it.
	persisted, err = r.writeAuthRotationStatus(ctx, cr, valkeyv1beta1.AuthRotationPhaseInProgress, "h1", "rotating")
	if err != nil {
		t.Fatalf("idempotent write: %v", err)
	}
	if !persisted {
		t.Error("idempotent re-write reported persisted=false")
	}
	if !cr.Status.Rollout.AuthRotation.StartedAt.Equal(startedAt) {
		t.Error("idempotent write changed StartedAt")
	}

	// Transition to Succeeded: carries StartedAt forward.
	persisted, err = r.writeAuthRotationStatus(ctx, cr, valkeyv1beta1.AuthRotationPhaseSucceeded, "h1-new", "ok")
	if err != nil {
		t.Fatalf("Succeeded write: %v", err)
	}
	if !persisted {
		t.Error("Succeeded write reported persisted=false on a clean fake client")
	}
	got = cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhaseSucceeded {
		t.Errorf("phase=%q, want Succeeded", got.Phase)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(startedAt) {
		t.Errorf("Succeeded did not carry StartedAt forward: %v vs %v", got.StartedAt, startedAt)
	}

	// Transition to Idle: clears StartedAt.
	if _, err := r.writeAuthRotationStatus(ctx, cr, valkeyv1beta1.AuthRotationPhaseIdle, "h1-new", ""); err != nil {
		t.Fatalf("Idle write: %v", err)
	}
	got = cr.Status.Rollout.AuthRotation
	if got.StartedAt != nil {
		t.Errorf("Idle did not clear StartedAt: %v", got.StartedAt)
	}
}

func TestClearAuthRotationStatus(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseSucceeded,
			ObservedSecretHash: "x",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	if err := r.clearAuthRotationStatus(context.Background(), cr); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if cr.Status.Rollout.AuthRotation != nil {
		t.Errorf("clear did not drop AuthRotation: %+v", cr.Status.Rollout.AuthRotation)
	}

	// Idempotent: another clear is a no-op.
	if err := r.clearAuthRotationStatus(context.Background(), cr); err != nil {
		t.Errorf("second clear: %v", err)
	}
}

// TestMaybeRotateAuth_NoSecret cleans the substate when the CR loses
// auth (Spec.Auth removed or Secret deleted). Secret nil → maybeRotateAuth
// passes through clearAuthRotationStatus.
func TestMaybeRotateAuth_NoSecret(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{Phase: "Idle", ObservedSecretHash: "stale"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	if err := r.maybeRotateAuth(context.Background(), cr, nil); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	if cr.Status.Rollout.AuthRotation != nil {
		t.Errorf("substate not cleared: %+v", cr.Status.Rollout.AuthRotation)
	}
}

func TestMaybeRotateAuth_FirstObservation(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	secret := authSecret("vk-auth", []byte("hunter2"))
	wantHash := hashAuthSecret(secret)
	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	got := cr.Status.Rollout.AuthRotation
	if got == nil || got.Phase != rotTestPhaseIdle {
		t.Fatalf("first observation did not stamp Idle: %+v", got)
	}
	if got.ObservedSecretHash != wantHash {
		t.Errorf("ObservedSecretHash=%q, want %q", got.ObservedSecretHash, wantHash)
	}
	cached, ok := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if !ok || cached.password != "hunter2" {
		t.Errorf("password not cached: %+v / ok=%v", cached, ok)
	}
	if cached.hash != wantHash {
		t.Errorf("cached.hash=%q, want %q", cached.hash, wantHash)
	}
}

func TestMaybeRotateAuth_SteadyState(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	secret := authSecret("vk-auth", []byte("hunter2"))
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: hashAuthSecret(secret),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	// Steady-state: status unchanged, but cache seeded so next change
	// has the OLD password to drive RotateAuth.
	cached, ok := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if !ok || cached.password != "hunter2" {
		t.Errorf("steady-state cache seed failed: %+v / ok=%v", cached, ok)
	}
}

func TestMaybeRotateAuth_SettleSucceededToIdle(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	secret := authSecret("vk-auth", []byte("hunter2"))
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhaseSucceeded,
			ObservedSecretHash: hashAuthSecret(secret),
			Message:            "ok",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	if got := cr.Status.Rollout.AuthRotation.Phase; got != rotTestPhaseIdle {
		t.Errorf("phase=%q, want Idle (Succeeded settles on next reconcile)", got)
	}
}

func TestMaybeRotateAuth_PartialIsSticky(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	secret := authSecret("vk-auth", []byte("hunter3")) // changed content
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              rotTestPhasePartial,
			ObservedSecretHash: "older-hash",
			Message:            "do not touch",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	got := cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhasePartial {
		t.Errorf("Partial not sticky: %q", got.Phase)
	}
	if got.Message != "do not touch" {
		t.Errorf("Partial message overwritten: %q", got.Message)
	}
}

// TestMaybeRotateAuth_CacheMiss covers the operator-restart path:
// observed-hash is set in Status, but the in-memory cache is cold.
// Driver records new hash without rotation.
func TestMaybeRotateAuth_CacheMiss(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: "h-before-restart",
		},
	}
	secret := authSecret("vk-auth", []byte("new-content"))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{
		Client: c,
		// RotateAuthFn left nil; this path must NOT reach RotateAuth.
		// We assert by leaving the production fn in place; if the
		// driver calls it the test will fail by attempting real TCP
		// dials against zero-value endpoints.
	}

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	got := cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhaseIdle {
		t.Errorf("phase=%q, want Idle (cache miss → adopt new hash without rotation)", got.Phase)
	}
	if got.ObservedSecretHash != hashAuthSecret(secret) {
		t.Errorf("ObservedSecretHash not advanced: %q", got.ObservedSecretHash)
	}
}

// TestMaybeRotateAuth_FailoverInFlight covers the deferral path: a
// CR currently in StateFailoverInFlight should NOT drive rotation
// even when Secret content changes. State is read from
// fsmTransitionTrackers via IsFailoverInFlight.
func TestMaybeRotateAuth_FailoverInFlight(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: "old-hash",
		},
	}
	secret := authSecret("vk-auth", []byte("new-content"))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	r := &ValkeyReconciler{Client: c}

	// Seed the cache so the cache-miss short-circuit doesn't hide the
	// deferral path.
	key := client.ObjectKeyFromObject(cr)
	r.cacheAuthPassword(key, "old-content", "old-hash")

	// Force IsFailoverInFlight to report true by stamping the FSM
	// transition tracker.
	r.stateFor(types.NamespacedName{Namespace: "ns", Name: "vk"}).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	// Status untouched; driver deferred without writing.
	got := cr.Status.Rollout.AuthRotation
	if got.ObservedSecretHash != "old-hash" {
		t.Errorf("ObservedSecretHash advanced during deferral: %q", got.ObservedSecretHash)
	}
	if got.Phase != rotTestPhaseIdle {
		t.Errorf("phase=%q, want Idle (mid-failover defer)", got.Phase)
	}
}

// TestDriveAuthRotation_AllSuccess covers the happy path: every pod
// accepts the new credential. Status → Succeeded, SecretRotated event
// emitted, cache advanced.
func TestDriveAuthRotation_AllSuccess(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	primary := dataPlanePod("vk-0", "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod("vk-1", "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod("vk-2", "10.0.0.3", roleValueReplica)

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()

	rec := k8sevents.NewFakeRecorder(8)
	rotateCalls := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCalls++
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

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	if rotateCalls != 1 {
		t.Errorf("rotateAuth called %d time(s), want 1 (no revert needed on all-success)", rotateCalls)
	}
	got := cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhaseSucceeded {
		t.Errorf("phase=%q, want Succeeded", got.Phase)
	}
	if got.ObservedSecretHash != hashAuthSecret(secret) {
		t.Errorf("hash not advanced: %q", got.ObservedSecretHash)
	}
	cached, _ := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if cached.password != rotTestNewPwd {
		t.Errorf("cache not advanced: %v", cached)
	}
	events := drainAllEventsLocal(rec.Events)
	if len(events) != 1 {
		t.Fatalf("events=%v, want exactly 1 SecretRotated event", events)
	}
	if !strings.Contains(events[0], "SecretRotated") {
		t.Errorf("event missing SecretRotated reason: %q", events[0])
	}
	// Message body must name the rotation outcome counts so a
	// dashboard reading events alongside Status sees consistent
	// signal — a regression that empties the Eventf args would slip
	// past a name-only assertion.
	if !strings.Contains(events[0], "1 master") || !strings.Contains(events[0], "2 replica") {
		t.Errorf("event message missing rotation counts: %q", events[0])
	}
}

// TestDriveAuthRotation_PartialThenRevertSuccess covers the
// rotation-fails-then-revert-succeeds path: at least one pod fails the
// rotation, revert succeeds on every successfully-updated pod, status
// → Failed, SecretRotationFailed event emitted, cache and observed
// hash unchanged (so retry needs a fresh content edit).
func TestDriveAuthRotation_PartialThenRevertSuccess(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	originalHash := rotTestOldHash
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: originalHash,
		},
	}
	primary := dataPlanePod("vk-0", "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod("vk-1", "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod("vk-2", "10.0.0.3", roleValueReplica)

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()

	rec := k8sevents.NewFakeRecorder(8)
	rotateCallNum := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, oldPwd, newPwd string) []valkey.PodResult {
			rotateCallNum++
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			if rotateCallNum == 1 {
				// Forward call: fail vk-2 (replica), succeed others.
				for _, ep := range replicas {
					var rerr error
					if ep.Name == "vk-2" {
						rerr = errors.New("AUTH failed")
					}
					out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: rerr})
				}
				out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
				// Sanity check the args while we're here.
				if oldPwd != rotTestOldPwd || newPwd != rotTestNewPwd {
					t.Errorf("forward call args: old=%q new=%q", oldPwd, newPwd)
				}
				return out
			}
			// Revert call: succeed everywhere.
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			if oldPwd != rotTestNewPwd || newPwd != rotTestOldPwd {
				t.Errorf("revert call args: old=%q new=%q", oldPwd, newPwd)
			}
			return out
		},
	}

	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, originalHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	if rotateCallNum != 2 {
		t.Errorf("rotateAuth called %d time(s), want 2 (forward + revert)", rotateCallNum)
	}
	got := cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhaseFailed {
		t.Errorf("phase=%q, want Failed", got.Phase)
	}
	if got.ObservedSecretHash != originalHash {
		t.Errorf("hash should NOT advance on Failed: got %q want %q", got.ObservedSecretHash, originalHash)
	}
	if !strings.Contains(got.Message, "vk-2") {
		t.Errorf("message missing failed pod name: %q", got.Message)
	}
	cached, _ := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if cached.password != rotTestOldPwd {
		t.Errorf("cache should not advance on Failed: %+v", cached)
	}
	events := drainAllEventsLocal(rec.Events)
	if len(events) != 1 {
		t.Fatalf("events=%v, want exactly 1 SecretRotationFailed event", events)
	}
	if !strings.Contains(events[0], "SecretRotationFailed") {
		t.Errorf("event missing SecretRotationFailed reason: %q", events[0])
	}
	if !strings.Contains(events[0], rotTestReplicaB) {
		t.Errorf("event message missing failed pod name %q: %q", rotTestReplicaB, events[0])
	}
	if !strings.Contains(events[0], "reverted") {
		t.Errorf("event message missing revert outcome: %q", events[0])
	}
}

// TestDriveAuthRotation_PartialThenRevertFailure covers the worst
// case: rotation partially failed AND revert also failed on at least
// one pod. Status → Partial, SecretRotationPartial event, cluster in
// mixed-credential state.
func TestDriveAuthRotation_PartialThenRevertFailure(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	originalHash := rotTestOldHash
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: originalHash,
		},
	}
	primary := dataPlanePod("vk-0", "10.0.0.1", roleValuePrimary)
	replica1 := dataPlanePod("vk-1", "10.0.0.2", roleValueReplica)
	replica2 := dataPlanePod("vk-2", "10.0.0.3", roleValueReplica)

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica1, &replica2).
		WithStatusSubresource(cr).
		Build()

	rec := k8sevents.NewFakeRecorder(8)
	rotateCallNum := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCallNum++
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			if rotateCallNum == 1 {
				// Forward: fail vk-2, succeed vk-1 + master.
				for _, ep := range replicas {
					var rerr error
					if ep.Name == "vk-2" {
						rerr = errors.New("AUTH failed")
					}
					out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: rerr})
				}
				out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
				return out
			}
			// Revert: fail master.
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster, Err: errors.New("revert AUTH failed")})
			return out
		},
	}

	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, originalHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	got := cr.Status.Rollout.AuthRotation
	if got.Phase != rotTestPhasePartial {
		t.Errorf("phase=%q, want Partial", got.Phase)
	}
	if got.ObservedSecretHash != originalHash {
		t.Errorf("hash should NOT advance on Partial: got %q", got.ObservedSecretHash)
	}
	if !strings.Contains(got.Message, rotTestReplicaB) || !strings.Contains(got.Message, rotTestPrimary) {
		t.Errorf("Partial message must name both failed-rotation and failed-revert pods: %q", got.Message)
	}
	events := drainAllEventsLocal(rec.Events)
	if len(events) != 1 {
		t.Fatalf("events=%v, want exactly 1 SecretRotationPartial event", events)
	}
	if !strings.Contains(events[0], "SecretRotationPartial") {
		t.Errorf("event missing SecretRotationPartial reason: %q", events[0])
	}
	if !strings.Contains(events[0], rotTestReplicaB) || !strings.Contains(events[0], rotTestPrimary) {
		t.Errorf("event message must name both failed-rotation and failed-revert pods: %q", events[0])
	}
	if !strings.Contains(events[0], "mixed-credential") {
		t.Errorf("event message missing mixed-credential descriptor: %q", events[0])
	}
}

// TestDriveAuthRotation_NilRecorder asserts the driver does NOT panic
// when r.Recorder is nil. A nil Recorder happens at startup before
// SetupWithManager populates it; the rotation path runs inside
// Reconcile so it should never see nil in production, but the nil
// guards in driveAuthRotation are defensive against a future caller
// path that might miss the population step. Asserts no panic + status
// transitions normally.
func TestDriveAuthRotation_NilRecorder(t *testing.T) {
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
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: nil, // nil intentionally
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

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth panicked or errored with nil recorder: %v", err)
	}
	if got := cr.Status.Rollout.AuthRotation.Phase; got != rotTestPhaseSucceeded {
		t.Errorf("phase=%q, want Succeeded (status path independent of Recorder)", got)
	}
}

// TestDriveAuthRotation_AllSuccess_StatusBeforeCache pins the patch-
// then-cache ordering in the all-success path. The cache MUST NOT be
// advanced until the Status.Patch succeeds, otherwise a Patch error
// leaves the cache stale (newPwd) while Status still shows the old
// observed hash — and the next reconcile would treat the user's new
// content as a still-pending change with the stale cache as the
// "old" credential, which would then be wrong.
func TestDriveAuthRotation_AllSuccess_StatusBeforeCache(t *testing.T) {
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
		Build()

	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(4),
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
	wantHash := hashAuthSecret(secret)

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	// On success: cache advanced AND Status advanced.
	cached, ok := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if !ok || cached.password != rotTestNewPwd {
		t.Errorf("cache did not advance after successful Status patch: %+v / ok=%v", cached, ok)
	}
	if cached.hash != wantHash {
		t.Errorf("cache.hash=%q, want %q (must match the just-stamped ObservedSecretHash)", cached.hash, wantHash)
	}
	if got := cr.Status.Rollout.AuthRotation.ObservedSecretHash; got != wantHash {
		t.Errorf("Status.ObservedSecretHash=%q, want %q", got, wantHash)
	}
}

// TestDriveAuthRotation_StatusConflictDoesNotAdvanceCache pins the
// load-bearing invariant: when the all-success Status.Patch returns
// a Conflict (treated as a benign "lost the race", patch did NOT
// land), the password cache MUST NOT advance. Otherwise the cache
// would hold the new password while Status still claims the old
// hash, and the next reconcile would mis-classify the cluster as
// "still on old credential, drive rotation" using the (now stale)
// cached old password — driving rotation of a cluster that's
// actually already on the new password.
func TestDriveAuthRotation_StatusConflictDoesNotAdvanceCache(t *testing.T) {
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

	// Inject Conflict on Status patches that touch the Succeeded phase.
	patchCount := 0
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &primary, &replica).
		WithStatusSubresource(cr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				patchCount++
				if v, ok := obj.(*valkeyv1beta1.Valkey); ok && v.Status.Rollout != nil && v.Status.Rollout.AuthRotation != nil && v.Status.Rollout.AuthRotation.Phase == "Succeeded" {
					// Synthetic Conflict — simulates another writer
					// touching the CR between fetch and patch.
					return apierrors.NewConflict(
						corev1.Resource("valkeys"),
						v.Name,
						fmt.Errorf("synthetic conflict for test"))
				}
				return cli.Status().Patch(ctx, obj, patch, statusPatchOpts(opts)...)
			},
		}).
		Build()

	rec := k8sevents.NewFakeRecorder(4)
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
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

	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth returned error on Conflict (should be benign): %v", err)
	}

	// Cache MUST NOT have advanced — old pwd, old hash still cached.
	cached, ok := r.lookupAuthPasswordCache(client.ObjectKeyFromObject(cr))
	if !ok {
		t.Fatal("cache evaporated entirely on Conflict")
	}
	if cached.password != rotTestOldPwd {
		t.Errorf("cache.password advanced on Conflict: got %q, want %q (old)", cached.password, rotTestOldPwd)
	}
	if cached.hash != rotTestOldHash {
		t.Errorf("cache.hash advanced on Conflict: got %q, want %q (old)", cached.hash, rotTestOldHash)
	}

	// SecretRotated event MUST NOT have fired — Status didn't persist,
	// so dashboards reading events alongside Status would see a
	// rotation that didn't happen.
	if got := drainAllEventsLocal(rec.Events); len(got) != 0 {
		t.Errorf("SecretRotated event emitted on Conflict: %v", got)
	}

	// Status MUST NOT show the new hash — the synthetic Conflict
	// rejected the Succeeded patch, so the in-memory v.Status that
	// writeAuthRotationStatus would have updated is also untouched.
	// Re-fetch from the fake client to be sure (no cache shenanigans).
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cr), got); err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	if got.Status.Rollout == nil || got.Status.Rollout.AuthRotation == nil {
		t.Fatal("AuthRotation substate evaporated on Conflict")
	}
	if got.Status.Rollout.AuthRotation.ObservedSecretHash != rotTestOldHash {
		t.Errorf("Status.ObservedSecretHash advanced on Conflict: got %q, want %q (old)",
			got.Status.Rollout.AuthRotation.ObservedSecretHash, rotTestOldHash)
	}
}

// TestDriveAuthRotation_FailedConflictDoesNotEmitEvent pins the same
// invariant for the Failed terminal path as
// TestDriveAuthRotation_StatusConflictDoesNotAdvanceCache pins for
// Succeeded: a Conflict on the Failed Status patch must NOT emit a
// SecretRotationFailed event, otherwise dashboards see a phantom
// failure that Status doesn't reflect (and the next reconcile may
// re-emit on retry).
func TestDriveAuthRotation_FailedConflictDoesNotEmitEvent(t *testing.T) {
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

	rec := k8sevents.NewFakeRecorder(4)
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
		RotateAuthFn: func(_ context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, _, _ string) []valkey.PodResult {
			out := make([]valkey.PodResult, 0, len(replicas)+1)
			// Forward fails on the replica; succeeded set has master only.
			for _, ep := range replicas {
				out = append(out, valkey.PodResult{Endpoint: ep, Phase: valkey.RotationPhaseReplica, Err: errors.New("AUTH failed")})
			}
			out = append(out, valkey.PodResult{Endpoint: master, Phase: valkey.RotationPhaseMaster})
			return out
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)
	secret := authSecret("vk-auth", []byte(rotTestNewPwd))

	// Drive: forward partial-fail → revert succeeds → tries to write
	// Failed → synthetic Conflict → no event must fire.
	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth returned error on Failed-Conflict (should be benign): %v", err)
	}

	// Event MUST NOT have fired — Status didn't persist.
	if got := drainAllEventsLocal(rec.Events); len(got) != 0 {
		t.Errorf("SecretRotationFailed event emitted on Failed-Conflict: %v", got)
	}
}

// statusPatchOpts converts SubResourcePatchOption to PatchOption for
// the controller-runtime fake client's Status().Patch fall-through.
// This is a local shim because interceptor.Funcs.SubResourcePatch
// expects SubResourcePatchOption while client.SubResourceClient.Patch
// takes the same type — the cast is a no-op at runtime but the
// compiler needs the intermediate slice.
func statusPatchOpts(opts []client.SubResourcePatchOption) []client.SubResourcePatchOption {
	return opts
}

// TestDriveAuthRotation_NoMasterDefers covers the pre-bootstrap /
// mid-failover path where no pod is labelled role=primary. The driver
// must not advance any state and must not call RotateAuth.
func TestDriveAuthRotation_NoMasterDefers(t *testing.T) {
	scheme := authRotationScheme(t)
	cr := crForRotation()
	cr.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		AuthRotation: &valkeyv1beta1.AuthRotationStatus{
			Phase:              "Idle",
			ObservedSecretHash: rotTestOldHash,
		},
	}
	// Two replicas, no primary.
	r1 := dataPlanePod("vk-1", "10.0.0.2", roleValueReplica)
	r2 := dataPlanePod("vk-2", "10.0.0.3", roleValueReplica)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, &r1, &r2).
		WithStatusSubresource(cr).
		Build()
	rec := k8sevents.NewFakeRecorder(4)
	rotateCalls := 0
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: rec,
		RotateAuthFn: func(_ context.Context, _ []valkey.Endpoint, _ valkey.Endpoint, _, _ string) []valkey.PodResult {
			rotateCalls++
			return nil
		},
	}
	r.cacheAuthPassword(client.ObjectKeyFromObject(cr), rotTestOldPwd, rotTestOldHash)

	secret := authSecret("vk-auth", []byte(rotTestNewPwd))
	if err := r.maybeRotateAuth(context.Background(), cr, secret); err != nil {
		t.Fatalf("maybeRotateAuth: %v", err)
	}
	if rotateCalls != 0 {
		t.Errorf("rotateAuth called %d time(s), want 0 (no master labelled)", rotateCalls)
	}
	if cr.Status.Rollout.AuthRotation.ObservedSecretHash != rotTestOldHash {
		t.Errorf("hash advanced during defer: %q", cr.Status.Rollout.AuthRotation.ObservedSecretHash)
	}
	if got := drainAllEventsLocal(rec.Events); len(got) != 0 {
		t.Errorf("events emitted during defer: %v", got)
	}
}
