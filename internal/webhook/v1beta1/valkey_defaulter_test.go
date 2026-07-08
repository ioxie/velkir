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

package v1beta1

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func newStandalone(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeStandalone},
	}
	if mutate != nil {
		mutate(v)
	}
	return v
}

func runDefault(t *testing.T, v *valkeyv1beta1.Valkey) {
	t.Helper()
	if err := (&ValkeyCustomDefaulter{}).Default(context.Background(), v); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
}

func TestDefaulter_StampsManagedByLabel(t *testing.T) {
	v := newStandalone("ml-empty", nil)
	runDefault(t, v)
	if got := v.Labels[ManagedByLabel]; got != ManagedByValue {
		t.Fatalf("ManagedBy label = %q; want %q", got, ManagedByValue)
	}
}

func TestDefaulter_PreservesUserManagedByLabel(t *testing.T) {
	v := newStandalone("ml-user", func(v *valkeyv1beta1.Valkey) {
		v.Labels = map[string]string{ManagedByLabel: "external-tool"}
	})
	runDefault(t, v)
	if got := v.Labels[ManagedByLabel]; got != "external-tool" {
		t.Fatalf("user-set ManagedBy was overwritten to %q", got)
	}
}

// TestDefaulter_StampsImageDefaults pins the move of the per-component
// image defaults out of the CRD schema and into the webhook: an unset image
// must receive the same repository/tag the schema `+kubebuilder:default`
// markers previously baked in.
func TestDefaulter_StampsImageDefaults(t *testing.T) {
	v := newStandalone("img-empty", nil)
	runDefault(t, v)
	if got := v.Spec.Image.Valkey; got.Repository != defaultValkeyImageRepo || got.Tag != defaultValkeyImageTag {
		t.Errorf("Valkey image = %+v; want %s:%s", got, defaultValkeyImageRepo, defaultValkeyImageTag)
	}
	if got := v.Spec.Image.Sentinel; got.Repository != defaultValkeyImageRepo || got.Tag != defaultValkeyImageTag {
		t.Errorf("Sentinel image = %+v; want %s:%s", got, defaultValkeyImageRepo, defaultValkeyImageTag)
	}
	if got := v.Spec.Image.Exporter; got.Repository != defaultExporterImageRepo || got.Tag != defaultExporterImageTag {
		t.Errorf("Exporter image = %+v; want %s:%s", got, defaultExporterImageRepo, defaultExporterImageTag)
	}
}

// TestDefaulter_PreservesUserImage confirms a user-set component image is not
// overwritten, while the still-unset components receive their defaults
// (per-component, matching the prior struct-default semantics).
func TestDefaulter_PreservesUserImage(t *testing.T) {
	v := newStandalone("img-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey = valkeyv1beta1.ContainerImage{Repository: "myrepo/valkey", Tag: "9.0.0"}
	})
	runDefault(t, v)
	if got := v.Spec.Image.Valkey; got.Repository != "myrepo/valkey" || got.Tag != "9.0.0" {
		t.Errorf("user-set Valkey image overwritten to %+v", got)
	}
	if got := v.Spec.Image.Sentinel; got.Repository != defaultValkeyImageRepo || got.Tag != defaultValkeyImageTag {
		t.Errorf("Sentinel image = %+v; want default %s:%s", got, defaultValkeyImageRepo, defaultValkeyImageTag)
	}
	if got := v.Spec.Image.Exporter; got.Repository != defaultExporterImageRepo || got.Tag != defaultExporterImageTag {
		t.Errorf("Exporter image = %+v; want default %s:%s", got, defaultExporterImageRepo, defaultExporterImageTag)
	}
}

func TestDefaulter_StampsValkeyLivenessProbe(t *testing.T) {
	v := newStandalone("probe-liveness", nil)
	runDefault(t, v)

	p := v.Spec.Valkey.CustomLivenessProbe
	if p == nil {
		t.Fatal("CustomLivenessProbe was nil after defaulting")
	}
	if p.Exec == nil {
		t.Fatal("liveness probe handler must be exec (#462); tcpSocket is invisible to frozen-process states")
	}
	// Pin the exact command shape so future refactors that reorder
	// or rename flags break this test loudly. The reconciler's
	// `injectAuthIntoValkeyCLIProbe` keys off `Command[0]=="valkey-cli"`
	// AND the absence of a preceding `-a`, so changes to the flag set
	// must be paired with the auth-injection logic.
	wantCmd := []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"}
	if !slices.Equal(p.Exec.Command, wantCmd) {
		t.Errorf("liveness exec command = %v; want %v (exact shape pin — change auth-inject path if updating)", p.Exec.Command, wantCmd)
	}
	if p.PeriodSeconds*p.FailureThreshold < 60 {
		t.Errorf("periodSeconds*failureThreshold = %d; need >= 60s to ride out RDB-load grace",
			p.PeriodSeconds*p.FailureThreshold)
	}
	if p.InitialDelaySeconds != 30 {
		t.Errorf("InitialDelaySeconds = %d; want 30", p.InitialDelaySeconds)
	}
}

func TestDefaulter_StampsValkeyReadinessProbe(t *testing.T) {
	v := newStandalone("probe-readiness", nil)
	runDefault(t, v)

	p := v.Spec.Valkey.CustomReadinessProbe
	if p == nil {
		t.Fatal("CustomReadinessProbe was nil after defaulting")
	}
	if p.Exec == nil {
		t.Fatal("readiness handler was not exec")
	}
	if len(p.Exec.Command) == 0 || p.Exec.Command[0] != "valkey-cli" {
		t.Errorf("readiness exec command = %v; want [valkey-cli ...]", p.Exec.Command)
	}
	wantHost := false
	for _, arg := range p.Exec.Command {
		if arg == "127.0.0.1" {
			wantHost = true
		}
	}
	if !wantHost {
		t.Errorf("readiness exec must hit loopback IP, got %v", p.Exec.Command)
	}
}

func TestDefaulter_PreservesUserProbe(t *testing.T) {
	userProbe := &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(9999)}},
		PeriodSeconds: 7,
	}
	v := newStandalone("probe-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.CustomLivenessProbe = userProbe
	})
	runDefault(t, v)
	if v.Spec.Valkey.CustomLivenessProbe.PeriodSeconds != 7 {
		t.Errorf("user probe period was overwritten: %d", v.Spec.Valkey.CustomLivenessProbe.PeriodSeconds)
	}
}

func TestDefaulter_StampsResources(t *testing.T) {
	v := newStandalone("res-empty", nil)
	runDefault(t, v)

	req := v.Spec.Valkey.Resources.Requests
	lim := v.Spec.Valkey.Resources.Limits
	if req.Cpu().String() != "100m" {
		t.Errorf("requests.cpu = %s; want 100m", req.Cpu().String())
	}
	if req.Memory().String() != "256Mi" {
		t.Errorf("requests.memory = %s; want 256Mi", req.Memory().String())
	}
	if lim.Cpu().String() != "500m" {
		t.Errorf("limits.cpu = %s; want 500m", lim.Cpu().String())
	}
	if lim.Memory().String() != "512Mi" {
		t.Errorf("limits.memory = %s; want 512Mi", lim.Memory().String())
	}
}

func TestDefaulter_PreservesUserResources(t *testing.T) {
	v := newStandalone("res-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("250m"),
		}
	})
	runDefault(t, v)
	if v.Spec.Valkey.Resources.Requests.Cpu().String() != "250m" {
		t.Errorf("user requests.cpu was overwritten: %s",
			v.Spec.Valkey.Resources.Requests.Cpu().String())
	}
	if v.Spec.Valkey.Resources.Limits == nil {
		t.Error("user-set requests should not block limits defaulting")
	}
}

// TestDefaulter_PartialUserRequests_NotMerged pins the all-or-nothing
// shape of stampValkeyResources: a non-nil Resources.Requests opts the
// user out of all key-level defaulting (the alternative — per-key merge
// — is also defensible, but is not the current contract). If a future
// design swap makes the defaulter merge per-key, this test fails and
// forces the change to be deliberate.
func TestDefaulter_PartialUserRequests_NotMerged(t *testing.T) {
	v := newStandalone("res-partial", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("250m"),
		}
	})
	runDefault(t, v)
	if _, ok := v.Spec.Valkey.Resources.Requests[corev1.ResourceMemory]; ok {
		t.Error("partial user Requests should not be merged with default keys")
	}
}

func TestDefaulter_ReadinessGate_StandaloneStampsOff(t *testing.T) {
	v := newStandalone("rg-standalone", nil)
	runDefault(t, v)
	if v.Spec.Valkey.ReadinessGate.Enabled == nil {
		t.Fatal("ReadinessGate.Enabled was not stamped")
	}
	if *v.Spec.Valkey.ReadinessGate.Enabled {
		t.Error("standalone should stamp ReadinessGate.Enabled=false")
	}
}

func TestDefaulter_ReadinessGate_PreservesUserExplicitTrue(t *testing.T) {
	v := newStandalone("rg-explicit-true", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ReadinessGate.Enabled = new(true)
	})
	runDefault(t, v)
	if v.Spec.Valkey.ReadinessGate.Enabled == nil || !*v.Spec.Valkey.ReadinessGate.Enabled {
		t.Error("user-set Enabled=true was overwritten")
	}
}

func TestDefaulter_ReadinessGate_StampsMaxLagBytes(t *testing.T) {
	v := newStandalone("rg-lag", nil)
	runDefault(t, v)
	got := v.Spec.Valkey.ReadinessGate.MaxLagBytes
	if got == nil {
		t.Fatalf("MaxLagBytes = nil; want pointer to %d", 1<<20)
	}
	if *got != 1<<20 {
		t.Errorf("*MaxLagBytes = %d; want %d", *got, 1<<20)
	}
}

func TestDefaulter_ReadinessGate_PreservesUserSetMaxLagBytes(t *testing.T) {
	// `*int64` lets the defaulter distinguish unset from a deliberate
	// user-supplied value. A user that wants a tighter (or looser) lag
	// budget than the 1 MiB default must round-trip through kubectl
	// apply unchanged.
	v := newStandalone("rg-lag-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(524288))
	})
	runDefault(t, v)
	got := v.Spec.Valkey.ReadinessGate.MaxLagBytes
	if got == nil {
		t.Fatalf("MaxLagBytes = nil; user-set 524288 should be preserved")
	}
	if *got != 524288 {
		t.Errorf("*MaxLagBytes = %d; user-set 524288 should be preserved", *got)
	}
}

func TestDefaulter_ReadinessGate_PreservesExplicitZero(t *testing.T) {
	// Explicit zero is meaningless in practice (a replica would never
	// catch up to exact-byte parity on a steadily-written primary) and
	// the validator emits a Warning for it, but the defaulter must
	// preserve it so the validator's warning fires once per CR rather
	// than every reconcile (re-stamping a default would clobber the
	// user's value and silently mask the warning).
	v := newStandalone("rg-lag-zero", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(0))
	})
	runDefault(t, v)
	got := v.Spec.Valkey.ReadinessGate.MaxLagBytes
	if got == nil {
		t.Fatalf("MaxLagBytes = nil; explicit zero should be preserved")
	}
	if *got != 0 {
		t.Errorf("*MaxLagBytes = %d; explicit zero should be preserved", *got)
	}
}

func TestDefaulter_RolloutMaxLagBytes_StampsDefault(t *testing.T) {
	// Schema-level kubebuilder default already does this on most paths;
	// the defaulter clause is defense-in-depth against client-side
	// defaulting drift (older typed Go clients, kubectl apply against an
	// older CRD). Asserts the webhook stamps 10000 even when the
	// apiserver defaulting did not run (Default invoked on a struct the
	// apiserver never saw — the Idempotent test uses the same shape).
	v := newStandalone("rollout-lag-default", nil)
	runDefault(t, v)
	if got := v.Spec.Rollout.MaxLagBytes; got != 10000 {
		t.Errorf("Rollout.MaxLagBytes = %d; want 10000", got)
	}
}

func TestDefaulter_RolloutMaxLagBytes_PreservesUserSet(t *testing.T) {
	v := newStandalone("rollout-lag-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Rollout.MaxLagBytes = 524288
	})
	runDefault(t, v)
	if got := v.Spec.Rollout.MaxLagBytes; got != 524288 {
		t.Errorf("Rollout.MaxLagBytes = %d; user-set 524288 should be preserved", got)
	}
}

func TestDefaulter_RolloutMaxLagBytes_ZeroTreatedAsUnset(t *testing.T) {
	// Zero is the documented sentinel for "unset" on this field — mirrors
	// how MaxUnavailable defaults. The "literal zero means no lag
	// tolerance" interpretation would block the rolling-update window
	// forever on the slightest replica lag, so the defaulter intentionally
	// re-stamps 10000 over an explicit zero.
	v := newStandalone("rollout-lag-zero", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Rollout.MaxLagBytes = 0
	})
	runDefault(t, v)
	if got := v.Spec.Rollout.MaxLagBytes; got != 10000 {
		t.Errorf("Rollout.MaxLagBytes = %d; explicit zero should stamp to 10000 default", got)
	}
}

func TestDefaulter_PDB_StandaloneNone(t *testing.T) {
	// Replicas=1 (standalone) → no PDB. Setting one is a derived field
	// the defaulter shouldn't add.
	v := newStandalone("pdb-standalone", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 1
	})
	runDefault(t, v)
	if v.Spec.Valkey.PDB != nil {
		t.Errorf("standalone should not stamp a PDB; got %+v", v.Spec.Valkey.PDB)
	}
}

func TestDefaulter_PDB_DerivesMinAvailable(t *testing.T) {
	// Forward-compat: when replication mode opens, replicas>=2 should
	// get a derived PDB. We exercise the codepath now even though the
	// schema rejects mode=replication today, because the defaulter is
	// mode-blind for this field — it keys off replicas count.
	v := newStandalone("pdb-derived", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 3
	})
	runDefault(t, v)
	if v.Spec.Valkey.PDB == nil {
		t.Fatal("PDB was not derived for replicas=3")
	}
	if got := v.Spec.Valkey.PDB.MinAvailable; got == nil || got.IntValue() != 2 {
		t.Errorf("derived MinAvailable = %v; want 2 (replicas-1)", got)
	}
}

func TestDefaulter_PreservesUserPDB(t *testing.T) {
	custom := intstr.FromString("50%")
	v := newStandalone("pdb-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &custom}
	})
	runDefault(t, v)
	if v.Spec.Valkey.PDB.MinAvailable.String() != "50%" {
		t.Errorf("user-set PDB was overwritten: %s",
			v.Spec.Valkey.PDB.MinAvailable.String())
	}
}

// TestDefaulter_Idempotent pins the idempotence contract: applying
// the defaulter twice yields exactly the same object. A regression
// here typically shows up as a `kubectl apply` perpetual-diff bug.
func TestDefaulter_Idempotent(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*valkeyv1beta1.Valkey)
	}{
		{"minimal-standalone", nil},
		{"with-auth", func(v *valkeyv1beta1.Valkey) {
			v.Spec.Auth = &valkeyv1beta1.AuthSpec{SecretName: "valkey-auth"}
		}},
		{"with-user-labels", func(v *valkeyv1beta1.Valkey) {
			v.Labels = map[string]string{"team": "platform"}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := newStandalone("idem-"+tc.name, tc.mutate)

			runDefault(t, v)
			first := v.DeepCopy()
			runDefault(t, v)

			if !reflect.DeepEqual(first, v) {
				t.Errorf("defaulter not idempotent\nfirst: %#v\nsecond: %#v", first, v)
			}
		})
	}
}

// TestDefaulterQuantityIntegerStable: the defaulter must emit
// resource quantities in canonical integer-form so JSON
// round-trips don't flip "0.5" ↔ "500m" between reconciles.
func TestDefaulterQuantityIntegerStable(t *testing.T) {
	v := newStandalone("qty-stable", nil)
	runDefault(t, v)

	// Marshal → unmarshal → re-default → assert byte-identical JSON
	first, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("first marshal: %v", err)
	}

	roundTrip := &valkeyv1beta1.Valkey{}
	if err := json.Unmarshal(first, roundTrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	runDefault(t, roundTrip)

	second, err := json.Marshal(roundTrip)
	if err != nil {
		t.Fatalf("second marshal: %v", err)
	}

	if string(first) != string(second) {
		t.Errorf("defaulter quantity output not stable across reconciles\nfirst:  %s\nsecond: %s",
			string(first), string(second))
	}

	// Concretely: the canonical Quantity strings the defaulter stamps
	// must serialise as their input form ("100m", not "0.1"; "256Mi",
	// not "268435456").
	if got := v.Spec.Valkey.Resources.Requests.Cpu().String(); got != "100m" {
		t.Errorf("requests.cpu serialised as %q; want %q", got, "100m")
	}
	if got := v.Spec.Valkey.Resources.Requests.Memory().String(); got != "256Mi" {
		t.Errorf("requests.memory serialised as %q; want %q", got, "256Mi")
	}
}

func TestDefaulter_MetricsDefaults(t *testing.T) {
	v := newStandalone("metrics-default", nil)
	runDefault(t, v)
	if v.Spec.Metrics.Enabled == nil || *v.Spec.Metrics.Enabled {
		t.Error("metrics.enabled default should be false")
	}
	if v.Spec.Metrics.PodMonitor.Enabled == nil || *v.Spec.Metrics.PodMonitor.Enabled {
		t.Error("metrics.podMonitor.enabled default should be false")
	}
	if v.Spec.Metrics.PodMonitor.ScrapeInterval != "30s" {
		t.Errorf("metrics.podMonitor.scrapeInterval = %s; want 30s",
			v.Spec.Metrics.PodMonitor.ScrapeInterval)
	}
}

func TestDefaulter_StandaloneStampsReplicasOne(t *testing.T) {
	v := newStandalone("standalone-replicas-default", nil)
	runDefault(t, v)
	if got := v.Spec.Valkey.Replicas; got != 1 {
		t.Errorf("standalone replicas = %d; want 1", got)
	}
}

func TestDefaulter_ReplicationStampsReplicasTwo(t *testing.T) {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-default", Namespace: "default"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeReplication},
	}
	runDefault(t, v)
	if got := v.Spec.Valkey.Replicas; got != 2 {
		t.Errorf("replication replicas = %d; want 2 (the replication baseline)", got)
	}
}

func TestDefaulter_PreservesUserSetReplicas(t *testing.T) {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-explicit", Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode:   valkeyv1beta1.ModeReplication,
			Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 5},
		},
	}
	runDefault(t, v)
	if got := v.Spec.Valkey.Replicas; got != 5 {
		t.Errorf("user-set replicas was clobbered to %d; want 5", got)
	}
}

// --- write-loss floor: defaulter stamps min-replicas
// 1 / 10 for replication+sentinel, leaves standalone nil. ---

func TestDefaulter_MinReplicas_StandaloneLeavesNil(t *testing.T) {
	v := newStandalone("minrepl-standalone", nil)
	runDefault(t, v)
	if v.Spec.Valkey.MinReplicasToWrite != nil {
		t.Errorf("standalone must not stamp MinReplicasToWrite; got %d", *v.Spec.Valkey.MinReplicasToWrite)
	}
	if v.Spec.Valkey.MinReplicasMaxLag != nil {
		t.Errorf("standalone must not stamp MinReplicasMaxLag; got %d", *v.Spec.Valkey.MinReplicasMaxLag)
	}
}

func TestDefaulter_MinReplicas_ReplicationStampsFloor(t *testing.T) {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "minrepl-replication", Namespace: "default"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeReplication},
	}
	runDefault(t, v)
	if v.Spec.Valkey.MinReplicasToWrite == nil || *v.Spec.Valkey.MinReplicasToWrite != 1 {
		t.Errorf("replication should stamp MinReplicasToWrite=1; got %v", v.Spec.Valkey.MinReplicasToWrite)
	}
	if v.Spec.Valkey.MinReplicasMaxLag == nil || *v.Spec.Valkey.MinReplicasMaxLag != 10 {
		t.Errorf("replication should stamp MinReplicasMaxLag=10; got %v", v.Spec.Valkey.MinReplicasMaxLag)
	}
}

func TestDefaulter_MinReplicas_SentinelStampsFloor(t *testing.T) {
	v := newSentinelMinimal("minrepl-sentinel")
	runDefault(t, v)
	if v.Spec.Valkey.MinReplicasToWrite == nil || *v.Spec.Valkey.MinReplicasToWrite != 1 {
		t.Errorf("sentinel should stamp MinReplicasToWrite=1; got %v", v.Spec.Valkey.MinReplicasToWrite)
	}
	if v.Spec.Valkey.MinReplicasMaxLag == nil || *v.Spec.Valkey.MinReplicasMaxLag != 10 {
		t.Errorf("sentinel should stamp MinReplicasMaxLag=10; got %v", v.Spec.Valkey.MinReplicasMaxLag)
	}
}

func TestDefaulter_MinReplicas_PreservesUserValues(t *testing.T) {
	// A user that sets a stricter floor than the stamped default keeps it
	// — the defaulter only fills nils.
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "minrepl-user", Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeReplication,
			Valkey: valkeyv1beta1.ValkeyPodSpec{
				MinReplicasToWrite: new(int32(2)),
				MinReplicasMaxLag:  new(int32(20)),
			},
		},
	}
	runDefault(t, v)
	if v.Spec.Valkey.MinReplicasToWrite == nil || *v.Spec.Valkey.MinReplicasToWrite != 2 {
		t.Errorf("user-set MinReplicasToWrite=2 was clobbered; got %v", v.Spec.Valkey.MinReplicasToWrite)
	}
	if v.Spec.Valkey.MinReplicasMaxLag == nil || *v.Spec.Valkey.MinReplicasMaxLag != 20 {
		t.Errorf("user-set MinReplicasMaxLag=20 was clobbered; got %v", v.Spec.Valkey.MinReplicasMaxLag)
	}
}

// --- Sentinel-mode defaulting ---

func newSentinelMinimal(name string) *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Sentinel: &valkeyv1beta1.SentinelPodSpec{
				MasterName: "test-master",
			},
		},
	}
}

func TestDefaulter_SentinelStampsValkeyReplicasThree(t *testing.T) {
	v := newSentinelMinimal("sent-replicas-default")
	runDefault(t, v)
	if got := v.Spec.Valkey.Replicas; got != 3 {
		t.Errorf("sentinel valkey replicas = %d; want 3 (sentinel baseline)", got)
	}
}

func TestDefaulter_SentinelStampsTimingDefaults(t *testing.T) {
	v := newSentinelMinimal("sent-timing-defaults")
	runDefault(t, v)
	if got := v.Spec.Sentinel.Replicas; got != 3 {
		t.Errorf("sentinel.replicas = %d; want 3 (defaulter fallback when apiserver default didn't fire)", got)
	}
	if got := v.Spec.Sentinel.DownAfterMilliseconds; got != 3000 {
		t.Errorf("sentinel.downAfterMilliseconds = %d; want 3000 (#461 default lowered for sub-10s recovery SLO)", got)
	}
	if got := v.Spec.Sentinel.FailoverTimeout; got != 180000 {
		t.Errorf("sentinel.failoverTimeout = %d; want 180000", got)
	}
	if got := v.Spec.Sentinel.ParallelSyncs; got != 1 {
		t.Errorf("sentinel.parallelSyncs = %d; want 1", got)
	}
}

func TestDefaulter_SentinelDerivesQuorum(t *testing.T) {
	tests := []struct {
		name     string
		replicas int32
		want     int32
	}{
		// Spot-checks of the ceil((r+1)/2) identity:
		{"replicas=3", 3, 2},
		{"replicas=4", 4, 3},
		{"replicas=5", 5, 3},
		{"replicas=7", 7, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := newSentinelMinimal("sent-quorum-" + tc.name)
			v.Spec.Sentinel.Replicas = tc.replicas
			runDefault(t, v)
			if got := v.Spec.Sentinel.Quorum; got != tc.want {
				t.Errorf("derived quorum at replicas=%d = %d; want %d", tc.replicas, got, tc.want)
			}
		})
	}
}

func TestDefaulter_PreservesUserSetSentinelQuorum(t *testing.T) {
	v := newSentinelMinimal("sent-quorum-explicit")
	v.Spec.Sentinel.Quorum = 4
	v.Spec.Sentinel.Replicas = 5
	runDefault(t, v)
	if got := v.Spec.Sentinel.Quorum; got != 4 {
		t.Errorf("user-set sentinel.quorum was clobbered to %d; want 4", got)
	}
}

func TestDefaulter_SentinelStampsGuaranteedQoSResources(t *testing.T) {
	v := newSentinelMinimal("sent-qos-default")
	runDefault(t, v)
	r := v.Spec.Sentinel.Resources
	if r.Requests == nil || r.Limits == nil {
		t.Fatalf("sentinel resources requests/limits not stamped: %+v", r)
	}
	if !r.Requests.Cpu().Equal(*r.Limits.Cpu()) {
		t.Errorf("sentinel CPU requests=%s limits=%s; want equal for Guaranteed QoS",
			r.Requests.Cpu(), r.Limits.Cpu())
	}
	if !r.Requests.Memory().Equal(*r.Limits.Memory()) {
		t.Errorf("sentinel Memory requests=%s limits=%s; want equal for Guaranteed QoS",
			r.Requests.Memory(), r.Limits.Memory())
	}
}

func TestDefaulter_StampsSentinelLivenessProbe(t *testing.T) {
	v := newSentinelMinimal("sent-probe-liveness")
	runDefault(t, v)

	p := v.Spec.Sentinel.CustomLivenessProbe
	if p == nil {
		t.Fatal("sentinel CustomLivenessProbe was nil after defaulting")
	}
	if p.TCPSocket == nil {
		t.Fatal("sentinel liveness handler must be tcpSocket (never coupled to quorum/exec state)")
	}
	if got := p.TCPSocket.Port.IntValue(); got != 26379 {
		t.Errorf("sentinel liveness probe port = %d; want 26379", got)
	}
	// 60s anti-flap window: long enough to ride out a transient stall,
	// short enough that a frozen sentinel doesn't linger in the quorum.
	if p.PeriodSeconds*p.FailureThreshold < 60 {
		t.Errorf("periodSeconds*failureThreshold = %d; need >= 60s anti-flap window",
			p.PeriodSeconds*p.FailureThreshold)
	}
	if p.TimeoutSeconds < 3 {
		t.Errorf("sentinel liveness timeoutSeconds = %d; want >= 3", p.TimeoutSeconds)
	}
	if p.InitialDelaySeconds != 30 {
		t.Errorf("sentinel liveness InitialDelaySeconds = %d; want 30", p.InitialDelaySeconds)
	}
}

func TestDefaulter_StampsSentinelReadinessProbe(t *testing.T) {
	v := newSentinelMinimal("sent-probe-readiness")
	runDefault(t, v)

	p := v.Spec.Sentinel.CustomReadinessProbe
	if p == nil {
		t.Fatal("sentinel CustomReadinessProbe was nil after defaulting")
	}
	if p.TCPSocket == nil {
		t.Fatal("sentinel readiness must be tcpSocket-only — a degraded-quorum sentinel must still accept discovery")
	}
	if p.Exec != nil {
		t.Error("sentinel readiness must not be exec (no CKQUORUM coupling)")
	}
	if got := p.TCPSocket.Port.IntValue(); got != 26379 {
		t.Errorf("sentinel readiness probe port = %d; want 26379", got)
	}
	// Readiness is the fast lane: ~15s so a wedged sentinel drops out of
	// discovery quickly, and deliberately tighter than the 60s liveness
	// window so it never triggers a restart before liveness does.
	if p.PeriodSeconds*p.FailureThreshold >= v.Spec.Sentinel.CustomLivenessProbe.PeriodSeconds*v.Spec.Sentinel.CustomLivenessProbe.FailureThreshold {
		t.Errorf("readiness window (%ds) must be tighter than liveness window (%ds)",
			p.PeriodSeconds*p.FailureThreshold,
			v.Spec.Sentinel.CustomLivenessProbe.PeriodSeconds*v.Spec.Sentinel.CustomLivenessProbe.FailureThreshold)
	}
}

func TestDefaulter_PreservesUserSentinelProbe(t *testing.T) {
	userProbe := &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(9999)}},
		PeriodSeconds: 7,
	}
	v := newSentinelMinimal("sent-probe-user")
	v.Spec.Sentinel.CustomLivenessProbe = userProbe
	runDefault(t, v)
	if v.Spec.Sentinel.CustomLivenessProbe.PeriodSeconds != 7 {
		t.Errorf("user sentinel probe period was overwritten: %d", v.Spec.Sentinel.CustomLivenessProbe.PeriodSeconds)
	}
	if port := v.Spec.Sentinel.CustomLivenessProbe.TCPSocket.Port.IntValue(); port != 9999 {
		t.Errorf("user sentinel probe port was overwritten: %d; want 9999", port)
	}
}

func TestDefaulter_PreservesUserSentinelResources(t *testing.T) {
	v := newSentinelMinimal("sent-resources-explicit")
	v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	runDefault(t, v)
	if got := v.Spec.Sentinel.Resources.Requests.Cpu().String(); got != "250m" {
		t.Errorf("user-set sentinel CPU requests was clobbered to %s; want 250m", got)
	}
}

func TestDefaulter_SentinelMirrorsRequestsOnlyToGuaranteed(t *testing.T) {
	v := newSentinelMinimal("sent-requests-only")
	// Requests set, Limits intentionally unset — Burstable without the mirror.
	v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	runDefault(t, v)
	lim := v.Spec.Sentinel.Resources.Limits
	if lim == nil {
		t.Fatal("requests-only sentinel resources were not mirrored into limits — pod would be Burstable")
	}
	if !lim.Cpu().Equal(resource.MustParse("200m")) || !lim.Memory().Equal(resource.MustParse("256Mi")) {
		t.Errorf("limits not mirrored from requests: cpu=%s mem=%s; want 200m/256Mi", lim.Cpu(), lim.Memory())
	}
	// Requests must be left untouched (defaulter fills, never overrides).
	req := v.Spec.Sentinel.Resources.Requests
	if !req.Cpu().Equal(resource.MustParse("200m")) || !req.Memory().Equal(resource.MustParse("256Mi")) {
		t.Errorf("requests were altered: cpu=%s mem=%s; want 200m/256Mi", req.Cpu(), req.Memory())
	}
}

func TestDefaulter_SentinelMirrorsLimitsOnlyToGuaranteed(t *testing.T) {
	v := newSentinelMinimal("sent-limits-only")
	v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("300m"),
			corev1.ResourceMemory: resource.MustParse("192Mi"),
		},
	}
	runDefault(t, v)
	req := v.Spec.Sentinel.Resources.Requests
	if req == nil {
		t.Fatal("limits-only sentinel resources were not mirrored into requests")
	}
	if !req.Cpu().Equal(resource.MustParse("300m")) || !req.Memory().Equal(resource.MustParse("192Mi")) {
		t.Errorf("requests not mirrored from limits: cpu=%s mem=%s; want 300m/192Mi", req.Cpu(), req.Memory())
	}
}

func TestDefaulter_SentinelDerivesPDBMinAvailable(t *testing.T) {
	v := newSentinelMinimal("sent-pdb-default")
	runDefault(t, v)
	if v.Spec.Sentinel.PDB == nil {
		t.Fatalf("sentinel PDB not derived (stampSentinelDefaults must run before stampPDB)")
	}
	got := v.Spec.Sentinel.PDB.MinAvailable
	if got == nil || got.IntValue() != 2 {
		t.Errorf("sentinel PDB minAvailable = %v; want 2 (replicas-1 with replicas=3)", got)
	}
}

func TestDefaulter_StampsRolloutDefaults(t *testing.T) {
	v := newStandalone("ro-empty", nil)
	runDefault(t, v)
	if v.Spec.Rollout.MaxUnavailable != 1 {
		t.Errorf("MaxUnavailable = %d; want 1", v.Spec.Rollout.MaxUnavailable)
	}
	if v.Spec.Rollout.ReplicaReadyTimeoutSeconds != 300 {
		t.Errorf("ReplicaReadyTimeoutSeconds = %d; want 300", v.Spec.Rollout.ReplicaReadyTimeoutSeconds)
	}
	// FailoverGracePeriodSeconds NOT defaulted: 0 IS the well-defined
	// "compute at reconcile time" sentinel.
	if v.Spec.Rollout.FailoverGracePeriodSeconds != 0 {
		t.Errorf("FailoverGracePeriodSeconds = %d; want 0 (sentinel for compute-at-reconcile)", v.Spec.Rollout.FailoverGracePeriodSeconds)
	}
}

func TestDefaulter_PreservesUserSetRolloutValues(t *testing.T) {
	v := newStandalone("ro-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Rollout.MaxUnavailable = 2
		v.Spec.Rollout.FailoverGracePeriodSeconds = 200
		v.Spec.Rollout.ReplicaReadyTimeoutSeconds = 600
	})
	runDefault(t, v)
	if v.Spec.Rollout.MaxUnavailable != 2 {
		t.Errorf("MaxUnavailable = %d; want 2 (user-set preserved)", v.Spec.Rollout.MaxUnavailable)
	}
	if v.Spec.Rollout.FailoverGracePeriodSeconds != 200 {
		t.Errorf("FailoverGracePeriodSeconds = %d; want 200", v.Spec.Rollout.FailoverGracePeriodSeconds)
	}
	if v.Spec.Rollout.ReplicaReadyTimeoutSeconds != 600 {
		t.Errorf("ReplicaReadyTimeoutSeconds = %d; want 600", v.Spec.Rollout.ReplicaReadyTimeoutSeconds)
	}
}

func TestDefaulter_RolloutIdempotent(t *testing.T) {
	v := newStandalone("ro-idem", nil)
	runDefault(t, v)
	first := v.Spec.Rollout
	runDefault(t, v)
	if v.Spec.Rollout != first {
		t.Errorf("Rollout changed on second Default(): first=%+v, second=%+v", first, v.Spec.Rollout)
	}
}

// admissionContext builds a context plumbing an admission.Request whose
// userInfo.username is `user`. Used by the requestor-stamping tests; the
// production webhook always plumbs this via NewContextWithRequest at the
// CustomDefaulter dispatch site.
func admissionContext(user string) context.Context {
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UserInfo: authnv1.UserInfo{Username: user},
		},
	}
	return admission.NewContextWithRequest(context.Background(), req)
}

func runDefaultWithUser(t *testing.T, v *valkeyv1beta1.Valkey, user string) {
	t.Helper()
	if err := (&ValkeyCustomDefaulter{}).Default(admissionContext(user), v); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
}

func TestDefaulter_RequestorStamp_AcceptPVCLoss(t *testing.T) {
	v := newStandalone("rs-pvc", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/accept-pvc-loss": "true",
		}
	})
	runDefaultWithUser(t, v, "alice")

	if got := v.Annotations["velkir.ioxie.dev/accept-pvc-loss-requestor"]; got != "alice" {
		t.Errorf("requestor sibling = %q; want alice", got)
	}
}

func TestDefaulter_RequestorStamp_AllFourTriggers(t *testing.T) {
	v := newStandalone("rs-all", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{}
		for _, trig := range operatorTriggerAnnotations {
			v.Annotations[trig] = "true"
		}
	})
	runDefaultWithUser(t, v, "ops-team")

	for _, trig := range operatorTriggerAnnotations {
		key := trig + requestorAnnotationSuffix
		if got := v.Annotations[key]; got != "ops-team" {
			t.Errorf("%s = %q; want ops-team", key, got)
		}
	}
}

func TestDefaulter_RequestorStamp_OverwritesPriorRequestor(t *testing.T) {
	// Last user to touch the CR while the trigger is "true" wins —
	// closes the spoofing path where someone pre-sets the requestor
	// sibling themselves.
	v := newStandalone("rs-overwrite", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/force-rotate":           "true",
			"velkir.ioxie.dev/force-rotate-requestor": "spoofed",
		}
	})
	runDefaultWithUser(t, v, "system:serviceaccount:ops:rotator")

	if got := v.Annotations["velkir.ioxie.dev/force-rotate-requestor"]; got != "system:serviceaccount:ops:rotator" {
		t.Errorf("requestor = %q; want overwrite by service account", got)
	}
}

func TestDefaulter_RequestorStamp_StripsWhenTriggerRemoved(t *testing.T) {
	// The trigger annotation was removed in this admission (only the
	// stale -requestor sibling remains). Defaulter must clean up.
	v := newStandalone("rs-strip", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/accept-pvc-loss-requestor": "alice",
		}
	})
	runDefaultWithUser(t, v, "bob")

	if _, ok := v.Annotations["velkir.ioxie.dev/accept-pvc-loss-requestor"]; ok {
		t.Errorf("stale requestor sibling not stripped after trigger removal")
	}
}

func TestDefaulter_RequestorStamp_StripsWhenTriggerNotTrue(t *testing.T) {
	// The validator rejects non-"true" values for these annotations
	// (Step 1), but the defaulter runs first. If a non-"true"
	// value sneaks past the defaulter (e.g. invariant violation), the
	// requestor sibling must NOT be stamped — the validator will then
	// reject the whole admission.
	v := newStandalone("rs-not-true", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/accept-pvc-loss":           "false",
			"velkir.ioxie.dev/accept-pvc-loss-requestor": "carol",
		}
	})
	runDefaultWithUser(t, v, "dave")

	if _, ok := v.Annotations["velkir.ioxie.dev/accept-pvc-loss-requestor"]; ok {
		t.Errorf("requestor sibling preserved with non-\"true\" trigger value")
	}
}

func TestDefaulter_RequestorStamp_NoStampForUnrelatedAnnotations(t *testing.T) {
	v := newStandalone("rs-unrelated", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"app.example.com/foo": "bar",
		}
	})
	runDefaultWithUser(t, v, "alice")

	for _, trig := range operatorTriggerAnnotations {
		key := trig + requestorAnnotationSuffix
		if _, ok := v.Annotations[key]; ok {
			t.Errorf("unexpected requestor sibling %s set on a CR with no operator triggers", key)
		}
	}
}

func TestDefaulter_RequestorStamp_AnonymousRequest_StripsRatherThanStampsEmpty(t *testing.T) {
	// An admission with empty username (e.g. an anonymous internal
	// request) must NOT result in `<trigger>-requestor=""` — that
	// would be worse than no sibling at all because audit code would
	// emit an empty-string requestor instead of falling back to the
	// "operator:reconciler" default.
	v := newStandalone("rs-anon", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/paused":           "true",
			"velkir.ioxie.dev/paused-requestor": "alice",
		}
	})
	runDefaultWithUser(t, v, "")

	if _, ok := v.Annotations["velkir.ioxie.dev/paused-requestor"]; ok {
		t.Errorf("anonymous request stamped or preserved a requestor sibling")
	}
}

func TestDefaulter_RequestorStamp_NoAdmissionContext_NoOp(t *testing.T) {
	// Test paths that call Default() with a bare context.Background()
	// must not panic or stamp anything — the production webhook always
	// plumbs the request via NewContextWithRequest, so the test path
	// is the only one that hits the no-Request branch.
	v := newStandalone("rs-no-ctx", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{
			"velkir.ioxie.dev/accept-pvc-loss": "true",
		}
	})
	runDefault(t, v) // bare context.Background()

	if _, ok := v.Annotations["velkir.ioxie.dev/accept-pvc-loss-requestor"]; ok {
		t.Errorf("requestor sibling stamped without admission.Request in context")
	}
}

func TestDefaulter_RequestorStamp_LeavesAnnotationsNilWhenEmpty(t *testing.T) {
	// A CR with no annotations and no triggers must end up with
	// Annotations == nil after Default() — the JSON round-trip and the
	// fixture-equality tests pin the nil case.
	v := newStandalone("rs-nil-map", nil)
	runDefaultWithUser(t, v, "alice")

	if v.Annotations != nil && len(v.Annotations) == 0 {
		t.Errorf("annotations is empty (non-nil) map; expected nil")
	}
}
