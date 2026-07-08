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

/*
Copyright 2026.
*/

package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/valkeyconf"
)

func sentinelTestCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Image: valkeyv1beta1.ImageSpec{
				Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
				Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
			},
			Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 3},
			Sentinel: &valkeyv1beta1.SentinelPodSpec{
				MasterName:            "mymaster",
				Replicas:              3,
				Quorum:                2,
				DownAfterMilliseconds: 30000,
				FailoverTimeout:       180000,
				ParallelSyncs:         1,
			},
		},
	}
}

func TestBuildSentinelSTS_BasicShape(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Namespace = "ns"

	ac := buildSentinelSTS(v, "deadbeef", 0)
	if ac.Name == nil || *ac.Name != "cr-sentinel" {
		t.Fatalf("STS name = %v; want cr-sentinel", ac.Name)
	}
	if ac.Namespace == nil || *ac.Namespace != "ns" {
		t.Fatalf("STS namespace = %v; want ns", ac.Namespace)
	}
	if ac.Spec == nil {
		t.Fatal("STS spec is nil")
	}
	if ac.Spec.Replicas == nil || *ac.Spec.Replicas != 3 {
		t.Fatalf("STS replicas = %v; want 3", ac.Spec.Replicas)
	}
	if ac.Spec.ServiceName == nil || *ac.Spec.ServiceName != "cr-sentinel-headless" {
		t.Fatalf("STS serviceName = %v; want cr-sentinel-headless", ac.Spec.ServiceName)
	}
	if ac.Spec.UpdateStrategy == nil || ac.Spec.UpdateStrategy.Type == nil || *ac.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		t.Fatalf("STS updateStrategy = %v; want RollingUpdate (sentinels are peers; auto-roll on CM hash flip)", ac.Spec.UpdateStrategy)
	}
	if ac.Spec.PodManagementPolicy == nil || *ac.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Fatalf("STS podManagementPolicy = %v; want Parallel (sentinels are peers, no ordinal-0 dependency)", ac.Spec.PodManagementPolicy)
	}
	// Selector + pod-template labels share the component=sentinel marker
	if got := ac.Spec.Selector.MatchLabels[ComponentLabel]; got != componentSentinel {
		t.Fatalf("STS selector component = %q; want %q", got, componentSentinel)
	}
	if ac.Spec.Template == nil || ac.Spec.Template.Spec == nil {
		t.Fatal("STS template/spec nil")
	}
	containers := ac.Spec.Template.Spec.Containers
	if len(containers) != 1 || containers[0].Name == nil || *containers[0].Name != "sentinel" {
		t.Fatalf("expected exactly one sentinel container; got %d", len(containers))
	}
	if *containers[0].Image != "valkey/valkey:8.1.6-alpine" {
		t.Fatalf("sentinel image = %q; want valkey/valkey:8.1.6-alpine", *containers[0].Image)
	}
	if len(containers[0].Ports) != 1 || *containers[0].Ports[0].ContainerPort != valkeyconf.SentinelPort {
		t.Fatalf("sentinel container port = %v; want %d", containers[0].Ports, valkeyconf.SentinelPort)
	}
	if len(ac.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected exactly one init container; got %d", len(ac.Spec.Template.Spec.InitContainers))
	}
}

// TestBuildSentinelSTS_TerminationGracePeriodIs30 pins the per-workload
// grace: sentinels run no master-failover preStop, so they use 30s rather
// than reusing valkey's 60s failover-drain budget.
func TestBuildSentinelSTS_TerminationGracePeriodIs30(t *testing.T) {
	ac := buildSentinelSTS(sentinelTestCR(), "deadbeef", 0)
	got := ac.Spec.Template.Spec.TerminationGracePeriodSeconds
	if got == nil || *got != 30 {
		t.Fatalf("sentinel terminationGracePeriodSeconds = %v; want 30 (no failover preStop, unlike valkey's 60s)", got)
	}
}

func TestBuildSentinelSTS_PodTemplateAnnotationsCarryHash(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"

	ac := buildSentinelSTS(v, "deadbeef", 0)
	annotations := ac.Spec.Template.Annotations
	if annotations[ConfigHashAnnotation] != "deadbeef" {
		t.Fatalf("ConfigHash annotation = %q; want deadbeef", annotations[ConfigHashAnnotation])
	}
}

func TestBuildSentinelSTS_ManualRolloutAnnotationProjected(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Annotations = map[string]string{ManualRolloutAnnotation: "bump-1"}

	ac := buildSentinelSTS(v, "deadbeef", 0)
	annotations := ac.Spec.Template.Annotations
	if annotations[ManualRolloutAnnotation] != "bump-1" {
		t.Fatalf("ManualRollout annotation = %q; want bump-1", annotations[ManualRolloutAnnotation])
	}
}

func TestBuildSentinelVolumes_AllPresent(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"

	vols := buildSentinelVolumes(v)
	names := map[string]bool{}
	for _, vol := range vols {
		if vol.Name != nil {
			names[*vol.Name] = true
		}
	}
	for _, want := range []string{"sentinel-template", "sentinel-init-scripts", "sentinel-conf", "bootstrap"} {
		if !names[want] {
			t.Errorf("missing volume %q; got %v", want, names)
		}
	}
}

func TestBuildSentinelInitContainer_MountsAndCommand(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"

	ac := buildSentinelInitContainer(v)
	if ac.Name == nil || *ac.Name != "render-sentinel-config" {
		t.Fatalf("init container name = %v; want render-sentinel-config", ac.Name)
	}
	if len(ac.Command) != 2 || ac.Command[1] != sentinelRenderScriptPath {
		t.Fatalf("init container command = %v; want /bin/sh %s", ac.Command, sentinelRenderScriptPath)
	}

	// Assert structural shape (path + readOnly) for each load-bearing
	// mount, not just presence-by-name — a future swap of mount paths
	// or read-only flags must surface as a test failure.
	type want struct {
		path     string
		readOnly bool
	}
	wantMounts := map[string]want{
		"sentinel-template":     {path: mountSentinelTemplate, readOnly: true},
		"sentinel-init-scripts": {path: mountSentinelInitScripts, readOnly: true},
		"sentinel-conf":         {path: mountSentinelConf, readOnly: false}, // writable: init renders here, sentinel reads
		"bootstrap":             {path: mountSentinelBootstrap, readOnly: true},
	}
	seen := map[string]bool{}
	for _, m := range ac.VolumeMounts {
		if m.Name == nil {
			continue
		}
		w, ok := wantMounts[*m.Name]
		if !ok {
			continue
		}
		seen[*m.Name] = true
		if m.MountPath == nil || *m.MountPath != w.path {
			t.Errorf("init container mount %q MountPath = %v; want %q", *m.Name, m.MountPath, w.path)
		}
		gotRO := m.ReadOnly != nil && *m.ReadOnly
		if gotRO != w.readOnly {
			t.Errorf("init container mount %q ReadOnly = %v; want %v", *m.Name, gotRO, w.readOnly)
		}
	}
	for name := range wantMounts {
		if !seen[name] {
			t.Errorf("init container missing mount %q", name)
		}
	}
}

func TestBuildSentinelContainer_RunsValkeySentinel(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	// buildSentinelContainer consumes the defaulter-stamped probe fields;
	// set distinct liveness/readiness probes here so propagation — and the
	// fact that they are two separate objects with different timings — is
	// observable without pulling in the webhook defaulter.
	v.Spec.Sentinel.CustomLivenessProbe = &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(valkeyconf.SentinelPort)}},
		PeriodSeconds:    10,
		FailureThreshold: 6,
	}
	v.Spec.Sentinel.CustomReadinessProbe = &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(valkeyconf.SentinelPort)}},
		PeriodSeconds:    5,
		FailureThreshold: 3,
	}

	ac := buildSentinelContainer(v)
	if len(ac.Command) != 2 || ac.Command[0] != "valkey-sentinel" {
		t.Fatalf("sentinel command = %v; want valkey-sentinel <conf>", ac.Command)
	}
	if !strings.HasPrefix(ac.Command[1], mountSentinelConf+"/") {
		t.Fatalf("sentinel conf path = %q; want prefix %q", ac.Command[1], mountSentinelConf+"/")
	}
	// Sentinel container does NOT mount the bootstrap CM (init does the read);
	// only the shared sentinel-conf emptyDir is needed at runtime.
	mounts := map[string]bool{}
	for _, m := range ac.VolumeMounts {
		if m.Name != nil {
			mounts[*m.Name] = true
		}
	}
	if !mounts["sentinel-conf"] {
		t.Errorf("sentinel container missing sentinel-conf mount; got %v", mounts)
	}
	if mounts["sentinel-template"] {
		t.Errorf("sentinel container should not mount the template ConfigMap (init owns substitution)")
	}
	if ac.ReadinessProbe == nil || ac.ReadinessProbe.TCPSocket == nil {
		t.Errorf("sentinel container missing tcpSocket readinessProbe on port 26379")
	}
	if ac.LivenessProbe == nil || ac.LivenessProbe.TCPSocket == nil {
		t.Errorf("sentinel container missing tcpSocket livenessProbe on port 26379")
	}
	// Regression guard against the former single shared probe object:
	// liveness and readiness must carry their own (different) timing.
	if ac.LivenessProbe != nil && (ac.LivenessProbe.PeriodSeconds == nil || *ac.LivenessProbe.PeriodSeconds != 10) {
		t.Errorf("liveness periodSeconds = %v; want 10", ac.LivenessProbe.PeriodSeconds)
	}
	if ac.ReadinessProbe != nil && (ac.ReadinessProbe.PeriodSeconds == nil || *ac.ReadinessProbe.PeriodSeconds != 5) {
		t.Errorf("readiness periodSeconds = %v; want 5", ac.ReadinessProbe.PeriodSeconds)
	}
}

func TestRenderSentinelInitScript_Stable(t *testing.T) {
	a := renderSentinelInitScript()
	b := renderSentinelInitScript()
	if a != b {
		t.Fatal("renderSentinelInitScript output is not deterministic")
	}
	for _, want := range []string{"_POD_IP_", "_SEED_MASTER_IP_", "/bootstrap/seedMasterIP", "/sentinel-template/sentinel.conf", "/sentinel/sentinel.conf"} {
		if !strings.Contains(a, want) {
			t.Errorf("init script missing %q", want)
		}
	}
}

// Pins the auth-pass + requirepass append path for the sentinel →
// master auth and operator → sentinel listener auth flows
// (without requirepass on the sentinel side, the operator's
// observer's AUTH call fails with WRONGPASS / no-password-configured
// and every sentinel is marked unreachable, latching SplitBrain
// indefinitely). Both directives must:
//   - guard the append behind VALKEY_AUTH_PASS being set (so no-auth
//     CRs render a clean file without trailing whitespace),
//   - use printf (NOT sed) for the password to keep sed metacharacters
//     in the secret value from corrupting the rendered file,
//   - append, not overwrite, so the rest of the template survives.
//
// Without this test the next refactor of the script can silently drop
// either directive again — both bugs were missing-line bugs and only
// surface post-deploy (failover failing for auth-pass; SplitBrain
// latching for requirepass).
func TestRenderSentinelInitScript_AppendsAuthPassAndRequirepassWhenSet(t *testing.T) {
	s := renderSentinelInitScript()
	for _, want := range []string{
		`${VALKEY_AUTH_PASS:-}`,                                     // guard against unset under set -u
		`printf 'sentinel auth-pass %s %s\n'`,                       // sentinel → master, discrete format string (sed-safe)
		`printf 'requirepass %s\n'`,                                 // operator → sentinel listener, same shape
		`AUTHPASS_VALUE="${SENTINEL_AUTH_PASS:-$VALKEY_AUTH_PASS}"`, // separate sentinel auth Secret overrides master pw for auth-pass
		`"${MASTER_NAME}"`,                                          // master name from env (per-CR, not script-baked)
		`"${VALKEY_AUTH_PASS}"`,                                     // password from env (requirepass leg)
		`"${AUTHPASS_VALUE}"`,                                       // resolved auth-pass value (override-or-fallback leg)
		`>> "/sentinel/sentinel.conf"`,                              // append, not overwrite
	} {
		if !strings.Contains(s, want) {
			t.Errorf("init script missing required token %q", want)
		}
	}
}

// The TestBuildSentinelInitContainer_AuthEnv_* family pins that
// buildSentinelInitContainer threads MASTER_NAME always and the
// VALKEY_AUTH_PASS / SENTINEL_AUTH_PASS env vars only when the
// corresponding auth Secret fields are set. The renderSentinelInitScript
// test (above) pins the script's USE of these env vars; these tests
// pin the container's WIRING. Both halves of the contract together
// close the sentinel auth-secret wiring. Split into separate top-level tests (rather than
// subtests of one parent) to keep cyclomatic complexity under the
// linter's gocyclo > 30 floor.

func TestBuildSentinelInitContainer_AuthEnv_AuthDisabled(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	// Override masterName to a non-default value so the assertion
	// actually proves buildSentinelInitContainer reads
	// v.Spec.Sentinel.MasterName — using sentinelTestCR's hardcoded
	// "mymaster" would be a tautology (the assertion would pass
	// even if the builder ignored the field and emitted a literal).
	v.Spec.Sentinel.MasterName = "shard-A-master"
	// Auth left nil — the default for sentinelTestCR.
	ac := buildSentinelInitContainer(v)

	masterName := findEnv(ac, "MASTER_NAME")
	if masterName == nil || masterName.Value == nil || *masterName.Value != "shard-A-master" {
		t.Errorf("MASTER_NAME env = %+v; want Value=shard-A-master", masterName)
	}
	if v := findEnv(ac, "VALKEY_AUTH_PASS"); v != nil {
		t.Errorf("VALKEY_AUTH_PASS env unexpectedly set when spec.auth is nil: %+v", v)
	}
	if sap := findEnv(ac, "SENTINEL_AUTH_PASS"); sap != nil {
		t.Errorf("SENTINEL_AUTH_PASS env unexpectedly set when spec.auth is nil: %+v", sap)
	}
}

func TestBuildSentinelInitContainer_AuthEnv_ValkeyAuthPassFromSecret(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret", SecretKey: "pw"}
	ac := buildSentinelInitContainer(v)
	assertSecretEnv(t, ac, "VALKEY_AUTH_PASS", "user-secret", "pw")
}

func TestBuildSentinelInitContainer_AuthEnv_ValkeyAuthPassDefaultKey(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	// SecretKey deliberately empty — exercises the defaultAuthSecretKey fallback.
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "user-secret"}
	ac := buildSentinelInitContainer(v)
	assertSecretEnv(t, ac, "VALKEY_AUTH_PASS", "user-secret", defaultAuthSecretKey)
}

// Pins that sentinelAuthSecretName populates a SEPARATE
// SENTINEL_AUTH_PASS env var so the init script's auth-pass directive
// can use a different password than the master's requirepass.
func TestBuildSentinelInitContainer_AuthEnv_SentinelAuthSecretOverride(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{
		SecretName:             "master-secret",
		SecretKey:              "pw",
		SentinelAuthSecretName: "sentinel-secret",
		SentinelAuthSecretKey:  "sentinel-pw",
	}
	ac := buildSentinelInitContainer(v)
	assertSecretEnv(t, ac, "VALKEY_AUTH_PASS", "master-secret", "pw")
	assertSecretEnv(t, ac, "SENTINEL_AUTH_PASS", "sentinel-secret", "sentinel-pw")
}

func TestBuildSentinelInitContainer_AuthEnv_SentinelAuthAbsent(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "master-secret", SecretKey: "pw"}
	ac := buildSentinelInitContainer(v)
	if sap := findEnv(ac, "SENTINEL_AUTH_PASS"); sap != nil {
		t.Errorf("SENTINEL_AUTH_PASS env unexpectedly set when sentinelAuthSecretName is empty: %+v", sap)
	}
}

func TestBuildSentinelInitContainer_AuthEnv_SentinelAuthDefaultKey(t *testing.T) {
	v := sentinelTestCR()
	v.Name = "cr"
	v.Spec.Auth = &valkeyv1beta1.AuthSpec{
		SecretName:             "master-secret",
		SecretKey:              "pw",
		SentinelAuthSecretName: "sentinel-secret",
		// SentinelAuthSecretKey empty — exercise the default fallback.
	}
	ac := buildSentinelInitContainer(v)
	assertSecretEnv(t, ac, "SENTINEL_AUTH_PASS", "sentinel-secret", defaultAuthSecretKey)
}

// assertSecretEnv encapsulates the per-env shape assertion — extracted
// so individual test functions stay below the gocyclo > 30 floor and
// failure messages stay informative.
func assertSecretEnv(t *testing.T, ac *corev1ac.ContainerApplyConfiguration, envName, wantSecret, wantKey string) {
	t.Helper()
	e := findEnv(ac, envName)
	if e == nil {
		t.Fatalf("%s env missing; auth Secret not wired into init container", envName)
		return
	}
	if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("%s must source from secretKeyRef; got %+v", envName, e.ValueFrom)
	}
	ref := e.ValueFrom.SecretKeyRef
	if ref.Name == nil || *ref.Name != wantSecret {
		t.Errorf("%s secretKeyRef.Name = %v; want %q", envName, ref.Name, wantSecret)
	}
	if ref.Key == nil || *ref.Key != wantKey {
		t.Errorf("%s secretKeyRef.Key = %v; want %q", envName, ref.Key, wantKey)
	}
}

// findEnv returns the env var with the given name on the container
// applyconfiguration, or nil if absent. Caller pattern is "did the
// operator wire env X with the right source"; nil-vs-set is the
// load-bearing distinction.
func findEnv(ac *corev1ac.ContainerApplyConfiguration, name string) *corev1ac.EnvVarApplyConfiguration {
	for i := range ac.Env {
		if ac.Env[i].Name != nil && *ac.Env[i].Name == name {
			return &ac.Env[i]
		}
	}
	return nil
}

func TestReconcileSentinelInfra_NoOpForNonSentinel(t *testing.T) {
	v := &valkeyv1beta1.Valkey{
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
		},
	}
	r := &ValkeyReconciler{}
	requeue, err := r.reconcileSentinelInfra(nil, v, orchestration.StateSteady, "") //nolint:staticcheck // nil context is fine for the early-return path
	if err != nil {
		t.Fatalf("standalone CR should be no-op; got %v", err)
	}
	if requeue != 0 {
		t.Fatalf("standalone CR should not request requeue; got %v", requeue)
	}
}

func TestReconcileSentinelInfra_GuardsOnMissingSentinelSpec(t *testing.T) {
	v := &valkeyv1beta1.Valkey{
		Spec: valkeyv1beta1.ValkeySpec{
			Mode:     valkeyv1beta1.ModeSentinel,
			Sentinel: nil,
		},
	}
	r := &ValkeyReconciler{}
	requeue, err := r.reconcileSentinelInfra(nil, v, orchestration.StateSteady, "") //nolint:staticcheck
	if err != nil || requeue != 0 {
		t.Fatalf("sentinel mode with nil Sentinel should be no-op; got err=%v requeue=%v", err, requeue)
	}
	v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: ""}
	requeue, err = r.reconcileSentinelInfra(nil, v, orchestration.StateSteady, "") //nolint:staticcheck
	if err != nil || requeue != 0 {
		t.Fatalf("sentinel mode with empty MasterName should be no-op; got err=%v requeue=%v", err, requeue)
	}
}

// TestReconcileSentinelSTS_DefersDuringFailover pins the Phase-3 gate:
// the sentinel STS apply is held (requeued, SentinelRollDeferred emitted)
// whenever a failover is in flight or the valkey data plane is mid-roll,
// and proceeds (applies the STS) once the window is clear.
func TestReconcileSentinelSTS_DefersDuringFailover(t *testing.T) {
	scheme := sentinelRollScheme(t)
	const crName = "sr0"
	stsName := types.NamespacedName{Namespace: "ns", Name: crName + suffixSentinel}
	v := sentinelTestCR()
	v.Namespace = "ns"
	v.Name = crName
	// A role=primary valkey pod with an IP so seedMasterIPForCR resolves a
	// non-empty seed and the reconcile reaches the gate.
	primary := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      crName + "-0",
			Labels: map[string]string{
				CRLabel:        crName,
				ComponentLabel: componentValkey,
				RoleLabel:      roleValuePrimary,
			},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.10"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(v, primary).WithStatusSubresource(v).Build()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{Client: c, Recorder: rec}
	cr := types.NamespacedName{Namespace: "ns", Name: crName}
	ctx := context.Background()

	deferredEvent := func() bool {
		for _, e := range drainAllEvents(rec.Events) {
			if strings.Contains(e, string(events.SentinelRollDeferred)) {
				return true
			}
		}
		return false
	}

	// Case A — failover in flight: defer. The in-memory latch makes
	// IsFailoverInFlight(cr) true (nil observer → observed addr "").
	r.failoverLatchSet(cr, "10.0.0.10:6379")
	requeue, err := r.reconcileSentinelInfra(ctx, v, orchestration.StateSteady, "")
	if err != nil {
		t.Fatalf("defer-on-failover: unexpected err %v", err)
	}
	if requeue != sentinelRollDeferRequeue {
		t.Fatalf("defer-on-failover: requeue=%v want %v", requeue, sentinelRollDeferRequeue)
	}
	if !deferredEvent() {
		t.Fatal("defer-on-failover: expected a SentinelRollDeferred event")
	}
	// The STS apply was skipped — it must not exist yet.
	if err := c.Get(ctx, stsName, &appsv1.StatefulSet{}); err == nil {
		t.Fatal("defer-on-failover: sentinel STS must NOT be applied while deferring")
	}
	r.failoverLatchClear(cr)

	// Case B — valkey roll active: defer.
	requeue, err = r.reconcileSentinelInfra(ctx, v, orchestration.StateRolloutReplicas, "")
	if err != nil {
		t.Fatalf("defer-on-valkey-roll: unexpected err %v", err)
	}
	if requeue != sentinelRollDeferRequeue {
		t.Fatalf("defer-on-valkey-roll: requeue=%v want %v", requeue, sentinelRollDeferRequeue)
	}
	if !deferredEvent() {
		t.Fatal("defer-on-valkey-roll: expected a SentinelRollDeferred event")
	}

	// Case C — window clear: proceeds; the sentinel STS is applied.
	requeue, err = r.reconcileSentinelInfra(ctx, v, orchestration.StateSteady, "")
	if err != nil {
		t.Fatalf("clear-window: unexpected err %v", err)
	}
	if requeue == sentinelRollDeferRequeue {
		t.Fatalf("clear-window: must not defer; got requeue=%v", requeue)
	}
	if err := c.Get(ctx, stsName, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("clear-window: sentinel STS must be applied; get err %v", err)
	}
}

func TestServiceNames_Stable(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"cr" + suffixSentinel, "cr-sentinel"},
		{"cr" + suffixSentinelHeadless, "cr-sentinel-headless"},
		{"cr" + suffixSentinelConf, "cr-sentinel-conf"},
		{"cr" + suffixSentinelInitScripts, "cr-sentinel-init-scripts"},
		{"cr" + suffixSentinelBootstrap, "cr-sentinel-bootstrap"},
	}
	for _, tc := range cases {
		if tc.input != tc.want {
			t.Errorf("suffix concat = %q; want %q", tc.input, tc.want)
		}
	}
	// Sanity check that the preStop hook's expected `<cr>-sentinel-headless`
	// host matches the headless Service name this package builds. Both come
	// from the same suffix constant.
	if !strings.HasSuffix(corev1.NamespaceDefault+"."+(corev1.NamespaceDefault+suffixSentinelHeadless), suffixSentinelHeadless) {
		t.Fatal("preStop hook expected host suffix drifted from suffixSentinelHeadless")
	}
}
