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
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/utils/ptr"
)

// TestAcTolerations_MatchesJSONRoundtrip asserts the builder-chain
// implementation produces an apply-config equivalent (after JSON
// serialization) to what `roundtripJSON` would emit on the same
// input. Pins the conversion correctness for the fix that
// dropped JSON marshal/unmarshal from the Toleration ac path.
func TestAcTolerations_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  []corev1.Toleration
	}{
		{
			name: "empty slice",
			src:  nil,
		},
		{
			name: "wildcard exists",
			src: []corev1.Toleration{
				{Operator: corev1.TolerationOpExists},
			},
		},
		{
			name: "node not-ready with seconds",
			src: []corev1.Toleration{
				{
					Key:               "node.kubernetes.io/not-ready",
					Operator:          corev1.TolerationOpExists,
					Effect:            corev1.TaintEffectNoExecute,
					TolerationSeconds: new(int64(300)),
				},
			},
		},
		{
			name: "dedicated equal",
			src: []corev1.Toleration{
				{
					Key:      "dedicated",
					Operator: corev1.TolerationOpEqual,
					Value:    "valkey",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		},
		{
			name: "explicit zero TolerationSeconds — pointer to 0 differs from nil",
			src: []corev1.Toleration{
				{
					Key:               "x",
					Operator:          corev1.TolerationOpExists,
					Effect:            corev1.TaintEffectNoExecute,
					TolerationSeconds: new(int64(0)),
				},
			},
		},
		{
			name: "multiple",
			src: []corev1.Toleration{
				{Key: "a", Operator: corev1.TolerationOpExists},
				{Key: "b", Operator: corev1.TolerationOpEqual, Value: "v"},
				{Operator: corev1.TolerationOpExists},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := acTolerations(tc.src)
			want := acTolerationsViaJSON(tc.src)

			gotJSON, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(want)
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acTolerations diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

// acTolerationsViaJSON is the prior implementation kept inline for
// the equivalence test only — never called from production code.
func acTolerationsViaJSON(src []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.TolerationApplyConfiguration, len(src))
	for i := range src {
		ac := &corev1ac.TolerationApplyConfiguration{}
		data, _ := json.Marshal(&src[i])
		_ = json.Unmarshal(data, ac)
		out[i] = ac
	}
	return out
}

// BenchmarkAcTolerations measures the per-call cost of the
// builder-chain implementation vs the JSON-roundtrip baseline
// (the prior shape) on a representative pod-spec input.
func BenchmarkAcTolerations(b *testing.B) {
	src := []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))},
		{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "valkey", Effect: corev1.TaintEffectNoSchedule},
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acTolerations(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acTolerationsViaJSON(src)
		}
	})
}

// TestAcDNSConfig_MatchesJSONRoundtrip pins acDNSConfig's builder
// implementation to JSON-roundtrip equivalence across the field
// permutations the operator must preserve.
func TestAcDNSConfig_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  *corev1.PodDNSConfig
	}{
		{name: "nil", src: nil},
		{name: "empty struct", src: &corev1.PodDNSConfig{}},
		{
			name: "nameservers only",
			src:  &corev1.PodDNSConfig{Nameservers: []string{"1.1.1.1", "8.8.8.8"}},
		},
		{
			name: "searches only",
			src:  &corev1.PodDNSConfig{Searches: []string{"svc.cluster.local", "cluster.local"}},
		},
		{
			name: "options without value",
			src: &corev1.PodDNSConfig{Options: []corev1.PodDNSConfigOption{
				{Name: "ndots"},
				{Name: "single-request"},
			}},
		},
		{
			name: "options with value",
			src: &corev1.PodDNSConfig{Options: []corev1.PodDNSConfigOption{
				{Name: "ndots", Value: new("2")},
				{Name: "timeout", Value: new("5")},
			}},
		},
		{
			name: "all three fields populated",
			src: &corev1.PodDNSConfig{
				Nameservers: []string{"10.0.0.10"},
				Searches:    []string{"ns.svc.cluster.local"},
				Options: []corev1.PodDNSConfigOption{
					{Name: "edns0"},
					{Name: "ndots", Value: new("5")},
				},
			},
		},
		{
			name: "explicit empty option value pointer",
			src: &corev1.PodDNSConfig{Options: []corev1.PodDNSConfigOption{
				{Name: "x", Value: new("")},
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := acDNSConfig(tc.src)
			want := acDNSConfigViaJSON(tc.src)
			gotJSON, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(want)
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acDNSConfig diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func acDNSConfigViaJSON(src *corev1.PodDNSConfig) *corev1ac.PodDNSConfigApplyConfiguration {
	if src == nil {
		return nil
	}
	out := &corev1ac.PodDNSConfigApplyConfiguration{}
	data, _ := json.Marshal(src)
	_ = json.Unmarshal(data, out)
	return out
}

func BenchmarkAcDNSConfig(b *testing.B) {
	src := &corev1.PodDNSConfig{
		Nameservers: []string{"10.0.0.10", "8.8.8.8"},
		Searches:    []string{"ns.svc.cluster.local", "svc.cluster.local", "cluster.local"},
		Options: []corev1.PodDNSConfigOption{
			{Name: "ndots", Value: new("5")},
			{Name: "edns0"},
		},
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acDNSConfig(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acDNSConfigViaJSON(src)
		}
	})
}

// TestAcResourceClaims_MatchesJSONRoundtrip pins acResourceClaims's
// builder output to JSON-roundtrip equivalence.
func TestAcResourceClaims_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  []corev1.ResourceClaim
	}{
		{name: "nil"},
		{name: "empty name (Name has no omitempty on source)", src: []corev1.ResourceClaim{{}}},
		{name: "name only", src: []corev1.ResourceClaim{{Name: "gpu"}}},
		{name: "name + request", src: []corev1.ResourceClaim{{Name: "gpu", Request: "foo"}}},
		{
			name: "multiple",
			src: []corev1.ResourceClaim{
				{Name: "gpu", Request: "shared"},
				{Name: "fpga"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotJSON, err := json.Marshal(acResourceClaims(tc.src))
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(acResourceClaimsViaJSON(tc.src))
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acResourceClaims diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func acResourceClaimsViaJSON(src []corev1.ResourceClaim) []*corev1ac.ResourceClaimApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.ResourceClaimApplyConfiguration, len(src))
	for i := range src {
		ac := &corev1ac.ResourceClaimApplyConfiguration{}
		data, _ := json.Marshal(&src[i])
		_ = json.Unmarshal(data, ac)
		out[i] = ac
	}
	return out
}

func BenchmarkAcResourceClaims(b *testing.B) {
	src := []corev1.ResourceClaim{
		{Name: "gpu-shared", Request: "shared"},
		{Name: "fpga"},
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acResourceClaims(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acResourceClaimsViaJSON(src)
		}
	})
}

// TestAcTopologySpreadConstraints_MatchesJSONRoundtrip pins the builder
// to JSON-roundtrip equivalence. The required fields (MaxSkew=0,
// TopologyKey="", WhenUnsatisfiable="") MUST emit on the source's
// JSON shape, so the builder always stamps them.
func TestAcTopologySpreadConstraints_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  []corev1.TopologySpreadConstraint
	}{
		{name: "empty slice"},
		{name: "zero-value required fields (still emitted)", src: []corev1.TopologySpreadConstraint{{}}},
		{
			name: "minimal",
			src: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: corev1.DoNotSchedule,
			}},
		},
		{
			name: "with label selector",
			src: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       "kubernetes.io/hostname",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "valkey", "role": "primary"},
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{Key: "tier", Operator: metav1.LabelSelectorOpIn, Values: []string{"prod", "stage"}},
						{Key: "evicted", Operator: metav1.LabelSelectorOpDoesNotExist},
					},
				},
			}},
		},
		{
			name: "with optional pointers + match-label-keys",
			src: []corev1.TopologySpreadConstraint{{
				MaxSkew:            2,
				TopologyKey:        "topology.kubernetes.io/zone",
				WhenUnsatisfiable:  corev1.DoNotSchedule,
				MinDomains:         new(int32(3)),
				NodeAffinityPolicy: ptr.To(corev1.NodeInclusionPolicyHonor),
				NodeTaintsPolicy:   ptr.To(corev1.NodeInclusionPolicyIgnore),
				MatchLabelKeys:     []string{"pod-template-hash"},
			}},
		},
		{
			name: "multiple",
			src: []corev1.TopologySpreadConstraint{
				{MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: corev1.DoNotSchedule},
				{MaxSkew: 3, TopologyKey: "host", WhenUnsatisfiable: corev1.ScheduleAnyway},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotJSON, err := json.Marshal(acTopologySpreadConstraints(tc.src))
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(acTopologySpreadConstraintsViaJSON(tc.src))
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acTopologySpreadConstraints diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func acTopologySpreadConstraintsViaJSON(src []corev1.TopologySpreadConstraint) []*corev1ac.TopologySpreadConstraintApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.TopologySpreadConstraintApplyConfiguration, len(src))
	for i := range src {
		ac := &corev1ac.TopologySpreadConstraintApplyConfiguration{}
		data, _ := json.Marshal(&src[i])
		_ = json.Unmarshal(data, ac)
		out[i] = ac
	}
	return out
}

func BenchmarkAcTopologySpreadConstraints(b *testing.B) {
	src := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app.kubernetes.io/name": "valkey"},
		},
	}}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acTopologySpreadConstraints(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acTopologySpreadConstraintsViaJSON(src)
		}
	})
}

// TestAcProbe_MatchesJSONRoundtrip pins the builder for Probe and its
// inline ProbeHandler union to JSON-roundtrip equivalence across the
// four handler shapes plus the six numeric scheduling knobs.
func TestAcProbe_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  *corev1.Probe
	}{
		{name: "nil", src: nil},
		{name: "empty struct", src: &corev1.Probe{}},
		{
			name: "exec",
			src: &corev1.Probe{
				ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "true"}}},
				InitialDelaySeconds: 5,
				PeriodSeconds:       10,
			},
		},
		{
			name: "httpget with headers",
			src: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path:   "/healthz",
					Port:   intstr.FromInt(8080),
					Host:   "localhost",
					Scheme: corev1.URISchemeHTTPS,
					HTTPHeaders: []corev1.HTTPHeader{
						{Name: "Authorization", Value: "Bearer x"},
						{Name: "X-Probe", Value: "y"},
					},
				}},
				TimeoutSeconds:                3,
				SuccessThreshold:              1,
				FailureThreshold:              5,
				TerminationGracePeriodSeconds: new(int64(30)),
			},
		},
		{
			name: "tcpsocket",
			src: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromString("valkey"), Host: "127.0.0.1",
				}},
			},
		},
		{
			name: "grpc with service",
			src: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{
					Port: 9000, Service: new("health.v1"),
				}},
			},
		},
		{
			name: "grpc with nil service (omits field)",
			src: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 9000}},
			},
		},
		{
			name: "explicit zero termination grace pointer",
			src: &corev1.Probe{
				ProbeHandler:                  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(80)}},
				TerminationGracePeriodSeconds: new(int64(0)),
			},
		},
		{
			name: "handler set, all numeric scheduling fields zero",
			src: &corev1.Probe{
				ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(80)}},
				InitialDelaySeconds: 0,
				TimeoutSeconds:      0,
				PeriodSeconds:       0,
				SuccessThreshold:    0,
				FailureThreshold:    0,
			},
		},
		{
			name: "string port intstr",
			src: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path: "/", Port: intstr.FromString("metrics"),
				}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotJSON, err := json.Marshal(acProbe(tc.src))
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(acProbeViaJSON(tc.src))
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acProbe diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func acProbeViaJSON(src *corev1.Probe) *corev1ac.ProbeApplyConfiguration {
	if src == nil {
		return nil
	}
	out := &corev1ac.ProbeApplyConfiguration{}
	data, _ := json.Marshal(src)
	_ = json.Unmarshal(data, out)
	return out
}

func BenchmarkAcProbe(b *testing.B) {
	src := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/healthz", Port: intstr.FromInt(8080), Scheme: corev1.URISchemeHTTP,
			HTTPHeaders: []corev1.HTTPHeader{{Name: "X-Probe", Value: "y"}},
		}},
		InitialDelaySeconds:           5,
		PeriodSeconds:                 10,
		TimeoutSeconds:                3,
		SuccessThreshold:              1,
		FailureThreshold:              5,
		TerminationGracePeriodSeconds: new(int64(30)),
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acProbe(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acProbeViaJSON(src)
		}
	})
}

// TestAcPodSecurityContext_MatchesJSONRoundtrip pins acPodSecurityContext
// (the underlying converter for restrictedPodSecurityContext) to
// JSON-roundtrip equivalence.
func TestAcPodSecurityContext_MatchesJSONRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		src  *corev1.PodSecurityContext
	}{
		{name: "nil"},
		{name: "empty struct", src: &corev1.PodSecurityContext{}},
		{
			name: "operator restricted defaults",
			src: &corev1.PodSecurityContext{
				RunAsNonRoot:   new(true),
				RunAsUser:      new(int64(1000)),
				RunAsGroup:     new(int64(1000)),
				FSGroup:        new(int64(1000)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		},
		{
			name: "with sysctls + supplemental groups",
			src: &corev1.PodSecurityContext{
				SupplementalGroups: []int64{1000, 2000},
				Sysctls: []corev1.Sysctl{
					{Name: "net.core.somaxconn", Value: "1024"},
					{Name: "vm.swappiness", Value: "0"},
				},
				FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
			},
		},
		{
			name: "selinux + apparmor + windows + seccomp localhost",
			src: &corev1.PodSecurityContext{
				SELinuxOptions: &corev1.SELinuxOptions{
					User:  "system_u",
					Role:  "object_r",
					Type:  "container_t",
					Level: "s0:c123,c456",
				},
				WindowsOptions: &corev1.WindowsSecurityContextOptions{
					RunAsUserName: new("ContainerUser"),
					HostProcess:   new(false),
				},
				SeccompProfile: &corev1.SeccompProfile{
					Type:             corev1.SeccompProfileTypeLocalhost,
					LocalhostProfile: new("profiles/op.json"),
				},
				AppArmorProfile: &corev1.AppArmorProfile{
					Type:             corev1.AppArmorProfileTypeLocalhost,
					LocalhostProfile: new("k8s-apparmor-example-deny-write"),
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotJSON, err := json.Marshal(acPodSecurityContext(tc.src))
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(acPodSecurityContextViaJSON(tc.src))
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acPodSecurityContext diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

// TestRestrictedPodSecurityContext_DefaultsFSGroupChangePolicy pins the
// fsGroupChangePolicy=OnRootMismatch default stamped when the user leaves it
// unset (without it kubelet recursively chowns the data PVC on every pod
// start), and confirms an explicit user value wins.
func TestRestrictedPodSecurityContext_DefaultsFSGroupChangePolicy(t *testing.T) {
	always := corev1.FSGroupChangeAlways
	cases := []struct {
		name string
		user *corev1.PodSecurityContext
		want corev1.PodFSGroupChangePolicy
	}{
		{name: "nil user", user: nil, want: corev1.FSGroupChangeOnRootMismatch},
		{name: "empty user", user: &corev1.PodSecurityContext{}, want: corev1.FSGroupChangeOnRootMismatch},
		{
			name: "user override wins",
			user: &corev1.PodSecurityContext{FSGroupChangePolicy: &always},
			want: corev1.FSGroupChangeAlways,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := restrictedPodSecurityContext(tc.user)
			if got == nil || got.FSGroupChangePolicy == nil {
				t.Fatalf("FSGroupChangePolicy not set; got=%+v", got)
			}
			if *got.FSGroupChangePolicy != tc.want {
				t.Errorf("FSGroupChangePolicy = %q, want %q", *got.FSGroupChangePolicy, tc.want)
			}
		})
	}
}

func acPodSecurityContextViaJSON(src *corev1.PodSecurityContext) *corev1ac.PodSecurityContextApplyConfiguration {
	if src == nil {
		return nil
	}
	out := &corev1ac.PodSecurityContextApplyConfiguration{}
	data, _ := json.Marshal(src)
	_ = json.Unmarshal(data, out)
	return out
}

func BenchmarkAcPodSecurityContext(b *testing.B) {
	src := &corev1.PodSecurityContext{
		RunAsNonRoot:   new(true),
		RunAsUser:      new(int64(1000)),
		RunAsGroup:     new(int64(1000)),
		FSGroup:        new(int64(1000)),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acPodSecurityContext(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acPodSecurityContextViaJSON(src)
		}
	})
}

// TestAcAffinity_MatchesJSONRoundtrip pins acAffinity (the largest
// converter, with three nested aggregates) to JSON-roundtrip
// equivalence across the spectrum of affinity shapes the operator
// must preserve.
func TestAcAffinity_MatchesJSONRoundtrip(t *testing.T) {
	podTerm := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "valkey"},
		},
		Namespaces:        []string{"ns1", "ns2"},
		TopologyKey:       "topology.kubernetes.io/zone",
		NamespaceSelector: &metav1.LabelSelector{},
		MatchLabelKeys:    []string{"pod-template-hash"},
		MismatchLabelKeys: []string{"role"},
	}
	cases := []struct {
		name string
		src  *corev1.Affinity
	}{
		{name: "nil"},
		{name: "empty struct", src: &corev1.Affinity{}},
		{
			name: "node affinity required only",
			src: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "role", Operator: corev1.NodeSelectorOpIn, Values: []string{"valkey"}},
							{Key: "evicted", Operator: corev1.NodeSelectorOpDoesNotExist},
						},
						MatchFields: []corev1.NodeSelectorRequirement{
							{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}},
						},
					}},
				},
			}},
		},
		{
			name: "node affinity preferred only",
			src: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
					Weight: 50,
					Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "tier", Operator: corev1.NodeSelectorOpIn, Values: []string{"prod"}},
					}},
				}},
			}},
		},
		{
			name: "pod affinity required",
			src: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{podTerm},
			}},
		},
		{
			name: "pod anti-affinity preferred",
			src: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight:          75,
					PodAffinityTerm: podTerm,
				}},
			}},
		},
		{
			name: "pod anti-affinity required",
			src: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{podTerm},
			}},
		},
		{
			name: "all three set",
			src: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{Key: "role", Operator: corev1.NodeSelectorOpExists},
							},
						}},
					},
				},
				PodAffinity: &corev1.PodAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{podTerm},
				},
				PodAntiAffinity: &corev1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
						Weight: 1, PodAffinityTerm: podTerm,
					}},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotJSON, err := json.Marshal(acAffinity(tc.src))
			if err != nil {
				t.Fatalf("marshal got: %v", err)
			}
			wantJSON, err := json.Marshal(acAffinityViaJSON(tc.src))
			if err != nil {
				t.Fatalf("marshal want: %v", err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("acAffinity diverges from JSON-roundtrip baseline:\n  got=%s\n want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func acAffinityViaJSON(src *corev1.Affinity) *corev1ac.AffinityApplyConfiguration {
	if src == nil {
		return nil
	}
	out := &corev1ac.AffinityApplyConfiguration{}
	data, _ := json.Marshal(src)
	_ = json.Unmarshal(data, out)
	return out
}

func BenchmarkAcAffinity(b *testing.B) {
	src := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "valkey"}},
					TopologyKey:   "kubernetes.io/hostname",
				},
			}},
		},
	}
	b.Run("builder", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acAffinity(src)
		}
	})
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = acAffinityViaJSON(src)
		}
	})
}
