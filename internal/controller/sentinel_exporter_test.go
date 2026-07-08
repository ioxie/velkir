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

	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func sentinelExporterTestCR() *valkeyv1beta1.Valkey {
	v := sentinelTestCR()
	v.Spec.Image.Exporter = valkeyv1beta1.ContainerImage{
		Repository: "oliver006/redis_exporter",
		Tag:        "v1.83.0-alpine",
	}
	return v
}

func TestBuildSentinelExporterContainer_TargetsSentinelPort(t *testing.T) {
	// The sentinel exporter must point at the Sentinel listener
	// (localhost:26379), NOT the data-plane 6379 — that is what makes
	// oliver006/redis_exporter emit redis_sentinel_* series.
	ac := buildSentinelExporterContainer(sentinelExporterTestCR())
	e := findEnv(ac, "REDIS_ADDR")
	if e == nil || e.Value == nil {
		t.Fatal("REDIS_ADDR env unexpectedly absent")
	}
	if *e.Value != "redis://localhost:26379" {
		t.Errorf("sentinel exporter REDIS_ADDR = %q; want redis://localhost:26379 (sentinel port, not the data-plane 6379)", *e.Value)
	}
}

func TestBuildSentinelExporterContainer_AuthFromSecret_InjectsREDIS_PASSWORD(t *testing.T) {
	// The sentinel listener's requirepass matches the master, so the
	// exporter authenticates to 26379 with the SAME Secret the data-plane
	// exporter uses.
	v := sentinelExporterTestCR()
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret", SecretKey: "pw"}
	assertSecretEnv(t, buildSentinelExporterContainer(v), "REDIS_PASSWORD", "user-secret", "pw")
}

func TestBuildSentinelExporterContainer_AuthDefaultKey_FallsBackToPassword(t *testing.T) {
	v := sentinelExporterTestCR()
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret"}
	assertSecretEnv(t, buildSentinelExporterContainer(v), "REDIS_PASSWORD", "user-secret", defaultAuthSecretKey)
}

func TestBuildSentinelExporterContainer_NoAuth_NoPassword(t *testing.T) {
	ac := buildSentinelExporterContainer(sentinelExporterTestCR())
	if pw := findEnv(ac, "REDIS_PASSWORD"); pw != nil {
		t.Errorf("REDIS_PASSWORD must NOT be set when spec.auth is nil; got %+v", pw)
	}
}

func TestBuildSentinelExporterContainer_HonorsReadOnlyRootFS(t *testing.T) {
	// Managed containers run readOnlyRootFilesystem with a writable
	// /tmp. The sentinel pod already ships the tmp emptyDir.
	ac := buildSentinelExporterContainer(sentinelExporterTestCR())
	if ac.SecurityContext == nil || ac.SecurityContext.ReadOnlyRootFilesystem == nil || !*ac.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("sentinel exporter must set securityContext.readOnlyRootFilesystem=true (#526)")
	}
	hasTmp := false
	for _, m := range ac.VolumeMounts {
		if m.MountPath != nil && *m.MountPath == "/tmp" {
			hasTmp = true
		}
	}
	if !hasTmp {
		t.Error("sentinel exporter needs a writable /tmp mount under readOnlyRootFilesystem")
	}
}

func TestBuildSentinelSTS_MetricsEnabled_AppendsExporter(t *testing.T) {
	v := sentinelExporterTestCR()
	v.Name = "cr"
	v.Namespace = "ns"
	v.Spec.Metrics.Enabled = new(true)

	containers := buildSentinelSTS(v, "deadbeef", 0).Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("metrics enabled: want 2 containers (sentinel + exporter); got %d", len(containers))
	}
	var exp *corev1ac.ContainerApplyConfiguration
	for i := range containers {
		if containers[i].Name != nil && *containers[i].Name == "exporter" {
			exp = &containers[i]
		}
	}
	if exp == nil {
		t.Fatal("exporter container not appended to sentinel STS when metrics enabled")
		return
	}
	// The PodMonitor sample selects component=sentinel and scrapes the
	// container port named "metrics" — pin that name + the exporter port.
	if len(exp.Ports) != 1 || exp.Ports[0].Name == nil || *exp.Ports[0].Name != "metrics" {
		t.Fatalf("exporter must expose a port named metrics (PodMonitor contract); got %+v", exp.Ports)
	}
	if exp.Ports[0].ContainerPort == nil || *exp.Ports[0].ContainerPort != defaultExporterPort {
		t.Fatalf("exporter metrics containerPort = %v; want %d", exp.Ports[0].ContainerPort, defaultExporterPort)
	}
	if e := findEnv(exp, "REDIS_ADDR"); e == nil || e.Value == nil || *e.Value != "redis://localhost:26379" {
		t.Fatalf("exporter REDIS_ADDR = %v; want redis://localhost:26379", e)
	}
}

func TestBuildSentinelSTS_MetricsDisabled_NoExporter(t *testing.T) {
	// Default (Enabled nil) and explicit false both yield the sentinel
	// container alone — no exporter, no metrics port.
	v := sentinelExporterTestCR()
	v.Name = "cr"

	containers := buildSentinelSTS(v, "deadbeef", 0).Spec.Template.Spec.Containers
	if len(containers) != 1 || containers[0].Name == nil || *containers[0].Name != "sentinel" {
		t.Fatalf("metrics unset: want exactly the sentinel container; got %d", len(containers))
	}

	v.Spec.Metrics.Enabled = new(false)
	if got := len(buildSentinelSTS(v, "deadbeef", 0).Spec.Template.Spec.Containers); got != 1 {
		t.Fatalf("metrics=false: want 1 container; got %d", got)
	}
}
