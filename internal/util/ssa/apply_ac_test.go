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

package ssa_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ioxie/velkir/internal/util/ssa"
)

// TestApplyAC_CreatesAndIsIdempotent mirrors the legacy
// TestSSAIdempotency but exercises the runtime.ApplyConfiguration path.
// Two applies of the same builder-constructed apply-config produce no
// server-side change after the first call (resourceVersion stable). The
// new path doesn't need ManagedFields stripping — apply-configs don't
// expose a ManagedFields setter — so this test also serves as the
// regression pin that the new helper doesn't accidentally touch
// ManagedFields-related machinery.
func TestApplyAC_CreatesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	const ns = "default"
	const name = "ssa-ac-idem"

	cm := corev1ac.ConfigMap(name, ns).
		WithLabels(map[string]string{"app": "test"}).
		WithData(map[string]string{"k": "v1"})

	if err := ssa.ApplyAC(ctx, k8s, cm); err != nil {
		t.Fatalf("first ApplyAC: %v", err)
	}

	got1 := &corev1.ConfigMap{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, got1); err != nil {
		t.Fatalf("get after first apply: %v", err)
	}
	if got1.Data["k"] != "v1" {
		t.Errorf("data[k] = %q; want v1", got1.Data["k"])
	}
	rv1 := got1.ResourceVersion

	// Second apply with identical shape — no-op patch.
	if err := ssa.ApplyAC(ctx, k8s, cm); err != nil {
		t.Fatalf("second ApplyAC: %v", err)
	}
	got2 := &corev1.ConfigMap{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, got2); err != nil {
		t.Fatalf("get after second apply: %v", err)
	}
	if got2.ResourceVersion != rv1 {
		t.Errorf("idempotent re-apply changed resourceVersion: %s -> %s",
			rv1, got2.ResourceVersion)
	}

	t.Cleanup(func() {
		_ = k8s.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
	})
}

// TestApplyAC_FieldOwnerStamped verifies the helper's default FieldOwner
// (`velkir`) is applied to every field set by the call — same
// invariant the legacy ssa.Apply enforces, but exercised through the new
// API surface to catch a regression where the FieldOwner option is
// dropped or mis-typed.
func TestApplyAC_FieldOwnerStamped(t *testing.T) {
	ctx := context.Background()
	const ns = "default"
	const name = "ssa-ac-fo"

	cm := corev1ac.ConfigMap(name, ns).WithData(map[string]string{"k": "v"})
	if err := ssa.ApplyAC(ctx, k8s, cm); err != nil {
		t.Fatalf("ApplyAC: %v", err)
	}

	got := &corev1.ConfigMap{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	var seenOurOwner bool
	for _, mf := range got.ManagedFields {
		if mf.Manager == string(ssa.FieldOwnerApply) {
			seenOurOwner = true
			break
		}
	}
	if !seenOurOwner {
		t.Errorf("no managedFields entry for FieldOwner %q; got %d entries: %v",
			ssa.FieldOwnerApply, len(got.ManagedFields), got.ManagedFields)
	}

	t.Cleanup(func() {
		_ = k8s.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
	})
}

// TestApplyAC_CustomFieldOwner pins the option-override contract:
// caller-supplied client.FieldOwner wins because the helper appends opts
// AFTER the default. Mirrors TestApplyWithCustomFieldOwner from the
// legacy path.
func TestApplyAC_CustomFieldOwner(t *testing.T) {
	ctx := context.Background()
	const ns = "default"
	const name = "ssa-ac-custom"
	const customOwner = client.FieldOwner("ssa-ac-custom-owner-test")

	cm := corev1ac.ConfigMap(name, ns).WithData(map[string]string{"k": "v"})
	if err := ssa.ApplyAC(ctx, k8s, cm, customOwner); err != nil {
		t.Fatalf("ApplyAC with custom owner: %v", err)
	}

	got := &corev1.ConfigMap{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	var seenCustom bool
	for _, mf := range got.ManagedFields {
		if mf.Manager == string(customOwner) {
			seenCustom = true
			break
		}
	}
	if !seenCustom {
		t.Errorf("custom FieldOwner override not honored: managedFields=%v", got.ManagedFields)
	}

	t.Cleanup(func() {
		_ = k8s.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
	})
}

// silenceUnusedImport keeps apierrors imported even when only a subset of
// tests in this file consume it via t.Cleanup paths that no-op on
// already-deleted objects.
var _ = apierrors.IsNotFound
