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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func exporterTestCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Image: valkeyv1beta1.ImageSpec{
				Exporter: valkeyv1beta1.ContainerImage{
					Repository: "oliver006/redis_exporter",
					Tag:        "v1.83.0-alpine",
				},
			},
		},
	}
}

func TestBuildExporterContainer_NoAuth_NoREDIS_PASSWORD(t *testing.T) {
	// Anonymous Valkey deployment — no spec.auth — exporter
	// connects anonymously. REDIS_PASSWORD must NOT be set.
	v := exporterTestCR()
	ac := buildExporterContainer(v)

	if redisAddr := findEnv(ac, "REDIS_ADDR"); redisAddr == nil {
		t.Error("REDIS_ADDR env unexpectedly absent")
	}
	if pw := findEnv(ac, "REDIS_PASSWORD"); pw != nil {
		t.Errorf("REDIS_PASSWORD must NOT be set when spec.auth is nil; got: %+v", pw)
	}
}

func TestBuildExporterContainer_AuthFromSecret_InjectsREDIS_PASSWORD(t *testing.T) {
	// CR carries spec.auth → exporter must receive REDIS_PASSWORD
	// from the SAME Secret the valkey + sentinel containers consume.
	v := exporterTestCR()
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret", SecretKey: "pw"}
	ac := buildExporterContainer(v)

	assertSecretEnv(t, ac, "REDIS_PASSWORD", "user-secret", "pw")
}

func TestBuildExporterContainer_AuthDefaultKey_FallsBackToPassword(t *testing.T) {
	// SecretKey deliberately empty — must fall back to
	// defaultAuthSecretKey ("password"). Mirrors the same fallback
	// shape sentinel_sts.go uses for VALKEY_AUTH_PASS.
	v := exporterTestCR()
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret"}
	ac := buildExporterContainer(v)

	assertSecretEnv(t, ac, "REDIS_PASSWORD", "user-secret", defaultAuthSecretKey)
}

func TestBuildExporterContainer_AuthSecretNameEmpty_NoPassword(t *testing.T) {
	// spec.auth present but SecretName empty — treat as no-auth.
	// (The webhook defaulter normally guarantees a non-empty
	// SecretName when spec.auth is set, but defensive.)
	v := exporterTestCR()
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretKey: "pw"}
	ac := buildExporterContainer(v)

	if pw := findEnv(ac, "REDIS_PASSWORD"); pw != nil {
		t.Errorf("REDIS_PASSWORD must NOT be set when SecretName is empty; got: %+v", pw)
	}
}

func TestBuildExporterContainer_DefaultResources_NoCPULimit(t *testing.T) {
	// Default shape: requests cpu=10m/mem=32Mi, limits mem=64Mi only —
	// NO CPU limit. A 100m CPU limit trips CFS-quota bucketing on
	// every 30s scrape and lights up CPUThrottlingHighAlert on a
	// sidecar whose long-run usage is ~1m.
	v := exporterTestCR()
	ac := buildExporterContainer(v)
	if ac.Resources == nil {
		t.Fatal("Resources unexpectedly nil")
	}
	req := ac.Resources.Requests
	if req == nil {
		t.Fatal("Resources.Requests unexpectedly nil")
		return
	}
	if got := (*req)[corev1.ResourceCPU]; got.String() != "10m" {
		t.Errorf("default cpu request: got %s, want 10m", got.String())
	}
	if got := (*req)[corev1.ResourceMemory]; got.String() != "32Mi" {
		t.Errorf("default memory request: got %s, want 32Mi", got.String())
	}
	lim := ac.Resources.Limits
	if lim == nil {
		t.Fatal("Resources.Limits unexpectedly nil")
		return
	}
	if _, ok := (*lim)[corev1.ResourceCPU]; ok {
		t.Errorf("default must NOT carry a CPU limit (CFS-quota throttling hazard); got %v", (*lim)[corev1.ResourceCPU])
	}
	if got := (*lim)[corev1.ResourceMemory]; got.String() != "64Mi" {
		t.Errorf("default memory limit: got %s, want 64Mi", got.String())
	}
}

func TestBuildExporterContainer_ResourcesOverride_ReplacesDefaults(t *testing.T) {
	// spec.metrics.resources set → operator stamps exactly what the
	// user asked for, including a CPU limit if they re-add one
	// deliberately. No default re-injection.
	v := exporterTestCR()
	v.Spec.Metrics.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("96Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	ac := buildExporterContainer(v)
	if ac.Resources == nil {
		t.Fatal("Resources unexpectedly nil")
	}
	req := ac.Resources.Requests
	if got := (*req)[corev1.ResourceCPU]; got.String() != "50m" {
		t.Errorf("override cpu request: got %s, want 50m", got.String())
	}
	if got := (*req)[corev1.ResourceMemory]; got.String() != "96Mi" {
		t.Errorf("override memory request: got %s, want 96Mi", got.String())
	}
	lim := ac.Resources.Limits
	if got := (*lim)[corev1.ResourceCPU]; got.String() != "500m" {
		t.Errorf("override cpu limit: got %s, want 500m", got.String())
	}
	if got := (*lim)[corev1.ResourceMemory]; got.String() != "128Mi" {
		t.Errorf("override memory limit: got %s, want 128Mi", got.String())
	}
}

func TestBuildExporterContainer_ResourcesOverride_EmptyStruct_FallsBackToDefaults(t *testing.T) {
	// `spec.metrics.resources: {}` (both Requests and Limits nil) is
	// indistinguishable from the field being unset — the operator falls
	// back to the default (no CPU limit + 64Mi memory limit). Pin the
	// semantics so a future change of the "is set" probe in
	// exporterResources is forced to re-decide this case explicitly.
	v := exporterTestCR()
	v.Spec.Metrics.Resources = corev1.ResourceRequirements{}
	ac := buildExporterContainer(v)
	if ac.Resources == nil {
		t.Fatal("Resources unexpectedly nil")
	}
	lim := ac.Resources.Limits
	if lim == nil {
		t.Fatal("Resources.Limits unexpectedly nil; expected default (memory only)")
		return
	}
	if _, ok := (*lim)[corev1.ResourceCPU]; ok {
		t.Errorf("empty-struct override must fall back to no-CPU-limit default; got cpu limit %v", (*lim)[corev1.ResourceCPU])
	}
	if got := (*lim)[corev1.ResourceMemory]; got.String() != "64Mi" {
		t.Errorf("empty-struct override must fall back to default memory limit; got %s want 64Mi", got.String())
	}
}

func TestBuildExporterContainer_ResourcesOverride_RequestsOnly_NoMagicLimits(t *testing.T) {
	// User explicitly set only requests → operator stamps only
	// requests. No magic-merge of default limits — predictable shape.
	v := exporterTestCR()
	v.Spec.Metrics.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("25m"),
		},
	}
	ac := buildExporterContainer(v)
	if ac.Resources == nil {
		t.Fatal("Resources unexpectedly nil")
	}
	if ac.Resources.Requests == nil {
		t.Fatal("Resources.Requests unexpectedly nil")
	}
	if got := (*ac.Resources.Requests)[corev1.ResourceCPU]; got.String() != "25m" {
		t.Errorf("override-only request: got %s, want 25m", got.String())
	}
	if ac.Resources.Limits != nil {
		t.Errorf("override-requests-only must NOT stamp default limits; got %+v", *ac.Resources.Limits)
	}
}
