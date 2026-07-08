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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Tests for the valkey-container preStop safety net — the in-pod
// failover trigger that bypasses sentinels' down-after-milliseconds
// wait window when the operator is unavailable. The hook is purely
// declarative (Pod spec stamped at buildValkeyContainer time); these
// tests verify the stamp is correct + only fires in sentinel mode.

const envRedisCliAuth = "REDISCLI_AUTH"

func valkeyCRWithMode(mode valkeyv1beta1.Mode, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: mode,
			Image: valkeyv1beta1.ImageSpec{
				Valkey: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
			},
		},
	}
	if mode == valkeyv1beta1.ModeSentinel {
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "mymaster"}
	}
	if mutate != nil {
		mutate(v)
	}
	return v
}

// envName extracts the Name field from an EnvVar apply-config; nil
// pointer returns the empty string. Used by the env-var assertions
// below since apply-configs use *string for required-feeling fields.
func envName(e *corev1ac.EnvVarApplyConfiguration) string {
	if e == nil || e.Name == nil {
		return ""
	}
	return *e.Name
}

func envValue(e *corev1ac.EnvVarApplyConfiguration) string {
	if e == nil || e.Value == nil {
		return ""
	}
	return *e.Value
}

func TestBuildValkeyContainer_Standalone_NoPreStopHook(t *testing.T) {
	t.Parallel()
	v := valkeyCRWithMode(valkeyv1beta1.ModeStandalone, nil)
	c := buildValkeyContainer(v)
	if c.Lifecycle != nil {
		t.Errorf("standalone mode must not stamp a Lifecycle hook; got %+v", c.Lifecycle)
	}
	for _, e := range c.Env {
		name := envName(&e)
		if name == "MASTER_NAME" || name == "SENTINEL_HOST" || name == envRedisCliAuth {
			t.Errorf("standalone mode must not stamp preStop env var %q", name)
		}
	}
}

func TestBuildValkeyContainer_Sentinel_StampsPreStopExec(t *testing.T) {
	t.Parallel()
	v := valkeyCRWithMode(valkeyv1beta1.ModeSentinel, nil)
	c := buildValkeyContainer(v)
	if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
		t.Fatalf("sentinel mode must stamp Lifecycle.PreStop.Exec; got %+v", c.Lifecycle)
	}
	cmd := c.Lifecycle.PreStop.Exec.Command
	if len(cmd) != 3 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
		t.Errorf("expected [/bin/sh -c <script>], got %v", cmd)
	}
	script := cmd[2]
	for _, want := range []string{
		"INFO replication",
		"SENTINEL FAILOVER",
		`"$MASTER_NAME"`,
		`"$SENTINEL_HOST"`,
		"role:slave", // not literal, but the case branches on `slave`
		"exit 0",
	} {
		if want == "role:slave" {
			// The script branches on `case "$ROLE"`; ROLE is parsed
			// from `INFO replication` `role:slave|master`. Verify the
			// branch literal.
			if !strings.Contains(script, "slave)") {
				t.Errorf("script missing `slave)` case branch")
			}
			continue
		}
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q\nfull script:\n%s", want, script)
		}
	}
}

func TestBuildValkeyContainer_Sentinel_StampsRequiredEnvVars(t *testing.T) {
	t.Parallel()
	v := valkeyCRWithMode(valkeyv1beta1.ModeSentinel, nil)
	c := buildValkeyContainer(v)
	envByName := make(map[string]corev1ac.EnvVarApplyConfiguration, len(c.Env))
	for i := range c.Env {
		envByName[envName(&c.Env[i])] = c.Env[i]
	}
	masterName := envByName["MASTER_NAME"]
	if got := envValue(&masterName); got != "mymaster" {
		t.Errorf("MASTER_NAME=%q want %q", got, "mymaster")
	}
	sentinelHost := envByName["SENTINEL_HOST"]
	if got := envValue(&sentinelHost); got != "vk0-sentinel-headless.ns.svc.cluster.local" {
		t.Errorf("SENTINEL_HOST=%q want %q", got, "vk0-sentinel-headless.ns.svc.cluster.local")
	}
	if _, ok := envByName[envRedisCliAuth]; ok {
		t.Errorf("REDISCLI_AUTH must NOT be stamped when no auth.secretName configured")
	}
}

func TestBuildValkeyContainer_Sentinel_WithAuth_StampsRedisCliAuth(t *testing.T) {
	t.Parallel()
	v := valkeyCRWithMode(valkeyv1beta1.ModeSentinel, func(v *valkeyv1beta1.Valkey) {
		v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "vk0-auth"}
	})
	c := buildValkeyContainer(v)
	var found *corev1ac.EnvVarApplyConfiguration
	for i := range c.Env {
		if envName(&c.Env[i]) == "REDISCLI_AUTH" {
			found = &c.Env[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("REDISCLI_AUTH env var must be stamped when auth.secretName is set")
		return
	}
	if found.ValueFrom == nil || found.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("REDISCLI_AUTH must source from SecretKeyRef; got %+v", found.ValueFrom)
	}
	ref := found.ValueFrom.SecretKeyRef
	if ref.Name == nil || *ref.Name != "vk0-auth" {
		t.Errorf("SecretKeyRef.Name=%v want %q", ref.Name, "vk0-auth")
	}
	if ref.Key == nil || *ref.Key != "password" {
		t.Errorf("SecretKeyRef.Key=%v want %q (default)", ref.Key, "password")
	}
}

func TestBuildValkeyContainer_Sentinel_RespectsCustomSecretKey(t *testing.T) {
	t.Parallel()
	v := valkeyCRWithMode(valkeyv1beta1.ModeSentinel, func(v *valkeyv1beta1.Valkey) {
		v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "vk0-auth", SecretKey: "custom-key"}
	})
	c := buildValkeyContainer(v)
	for i := range c.Env {
		e := &c.Env[i]
		if envName(e) == envRedisCliAuth {
			ref := e.ValueFrom.SecretKeyRef
			if ref.Key == nil || *ref.Key != "custom-key" {
				t.Errorf("custom SecretKey not honoured: got %v want %q", ref.Key, "custom-key")
			}
			return
		}
	}
	t.Errorf("REDISCLI_AUTH not stamped")
}

func TestPreStopScript_DoesNotShellInjectMasterName(t *testing.T) {
	t.Parallel()
	// MASTER_NAME comes from spec.sentinel.masterName which is
	// validated by the admission webhook (CEL pattern). Even if
	// it weren't, the script uses double-quoted "$MASTER_NAME" so
	// shell substitution treats the value as a single word — no
	// $(...) or backtick expansion. This test pins the quoting so
	// a future "convenience" edit doesn't strip the quotes.
	if !strings.Contains(valkeyPreStopScript, `"$MASTER_NAME"`) {
		t.Errorf("MASTER_NAME must be referenced as \"$MASTER_NAME\" (double-quoted) for shell-injection safety")
	}
	if !strings.Contains(valkeyPreStopScript, `"$SENTINEL_HOST"`) {
		t.Errorf("SENTINEL_HOST must be referenced as \"$SENTINEL_HOST\" (double-quoted)")
	}
}
