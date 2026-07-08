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
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func saTestCR() *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		Spec: valkeyv1beta1.ValkeySpec{
			Mode:   valkeyv1beta1.ModeStandalone,
			Image:  valkeyv1beta1.ImageSpec{Valkey: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"}},
			Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 1},
		},
	}
	v.Name = "cr"
	v.Namespace = "ns"
	return v
}

func saTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add valkey scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbacv1 scheme: %v", err)
	}
	return scheme
}

// buildServiceAccount renders a bare, owner-ref'd, RBAC-less SA: identity
// + owned labels + owner-ref, and nothing else. In particular the SA must
// NOT carry an automount toggle — the pods own that (the SA-level field is
// only a default the pod-level setting overrides).
func TestBuildServiceAccount_BareAndOwned(t *testing.T) {
	v := saTestCR()
	sa := buildServiceAccount(v, valkeyServiceAccountName(v), componentValkey)

	if sa.Name == nil || *sa.Name != "cr-valkey" {
		t.Fatalf("SA name = %v; want cr-valkey", sa.Name)
	}
	if sa.Namespace == nil || *sa.Namespace != "ns" {
		t.Fatalf("SA namespace = %v; want ns", sa.Namespace)
	}
	if sa.Labels[ComponentLabel] != componentValkey {
		t.Errorf("SA %s label = %q; want %q", ComponentLabel, sa.Labels[ComponentLabel], componentValkey)
	}
	if sa.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("SA %s label = %q; want %q", ManagedByLabel, sa.Labels[ManagedByLabel], ManagedByValue)
	}
	if len(sa.OwnerReferences) != 1 || sa.OwnerReferences[0].Name == nil || *sa.OwnerReferences[0].Name != "cr" {
		t.Fatalf("SA owner-references = %+v; want one ref to cr", sa.OwnerReferences)
	}
	if sa.AutomountServiceAccountToken != nil {
		t.Errorf("SA AutomountServiceAccountToken is set; the pods own that toggle, not the SA")
	}
}

// The data-plane pods must run as their dedicated SA with token automount
// disabled — they need no Kubernetes API access, so no API credential
// should ever be mounted into the container.
func TestBuildValkeySTS_DedicatedSA_NoTokenAutomount(t *testing.T) {
	v := saTestCR()
	spec := buildValkeySTS(v, testCMHash).Spec.Template.Spec

	if spec.ServiceAccountName == nil || *spec.ServiceAccountName != "cr-valkey" {
		t.Fatalf("valkey pod serviceAccountName = %v; want cr-valkey", spec.ServiceAccountName)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Fatalf("valkey pod automountServiceAccountToken = %v; want false", spec.AutomountServiceAccountToken)
	}
}

func TestBuildSentinelSTS_DedicatedSA_NoTokenAutomount(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	spec := buildSentinelSTS(v, testCMHash, 0).Spec.Template.Spec

	if spec.ServiceAccountName == nil || *spec.ServiceAccountName != "cr-sentinel" {
		t.Fatalf("sentinel pod serviceAccountName = %v; want cr-sentinel", spec.ServiceAccountName)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Fatalf("sentinel pod automountServiceAccountToken = %v; want false", spec.AutomountServiceAccountToken)
	}
}

// Standalone/replication CRs have no sentinel pods, so the sentinel SA has
// no consumer and must not be created; only the valkey SA is applied.
func TestReconcileServiceAccounts_Standalone_OnlyValkeySA(t *testing.T) {
	scheme := saTestScheme(t)
	cr := saTestCR()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileServiceAccounts(context.Background(), cr); err != nil {
		t.Fatalf("reconcileServiceAccounts: %v", err)
	}

	if err := c.Get(context.Background(), types.NamespacedName{Name: "cr-valkey", Namespace: "ns"}, &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("valkey SA not created: %v", err)
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "cr-sentinel", Namespace: "ns"}, &corev1.ServiceAccount{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("sentinel SA should be absent in standalone mode; got err=%v", err)
	}

	// RBAC-less by design: the operator must never wire a Role or
	// RoleBinding onto the per-CR SA — the data-plane pods need zero API
	// access. Guards against a future regression that grants the SA RBAC.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cr-valkey", Namespace: "ns"}, &rbacv1.Role{}); !apierrors.IsNotFound(err) {
		t.Fatalf("no Role should exist for the valkey SA; got err=%v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "cr-valkey", Namespace: "ns"}, &rbacv1.RoleBinding{}); !apierrors.IsNotFound(err) {
		t.Fatalf("no RoleBinding should exist for the valkey SA; got err=%v", err)
	}
}

// Sentinel-mode CRs run sentinel pods, so both per-CR SAs are applied.
func TestReconcileServiceAccounts_Sentinel_CreatesBothSAs(t *testing.T) {
	scheme := saTestScheme(t)
	cr := sentinelTestCR()
	cr.Name = "cr"
	cr.Namespace = "ns"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileServiceAccounts(context.Background(), cr); err != nil {
		t.Fatalf("reconcileServiceAccounts: %v", err)
	}

	for _, name := range []string{"cr-valkey", "cr-sentinel"} {
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "ns"}, &corev1.ServiceAccount{}); err != nil {
			t.Fatalf("SA %s not created: %v", name, err)
		}
	}
}
