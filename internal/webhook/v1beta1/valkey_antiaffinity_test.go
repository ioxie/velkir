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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

const antiAffinityWarnPrefix = "AntiAffinityTooPermissive"

// assertSoftSamePodSet asserts aff carries exactly the soft same-pod-set
// anti-affinity the defaulter stamps: one preferred term, weight 100,
// node-hostname topology, selecting the given CR + component.
func assertSoftSamePodSet(t *testing.T, aff *corev1.Affinity, crName, component string) {
	t.Helper()
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected a stamped PodAntiAffinity for the %s set; got %+v", component, aff)
	}
	paa := aff.PodAntiAffinity
	if n := len(paa.RequiredDuringSchedulingIgnoredDuringExecution); n != 0 {
		t.Errorf("expected no required (hard) terms; got %d", n)
	}
	if n := len(paa.PreferredDuringSchedulingIgnoredDuringExecution); n != 1 {
		t.Fatalf("expected exactly 1 preferred term; got %d", n)
	}
	term := paa.PreferredDuringSchedulingIgnoredDuringExecution[0]
	if term.Weight != 100 {
		t.Errorf("weight = %d; want 100", term.Weight)
	}
	if term.PodAffinityTerm.TopologyKey != antiAffinityTopologyKey {
		t.Errorf("topologyKey = %q; want %q", term.PodAffinityTerm.TopologyKey, antiAffinityTopologyKey)
	}
	ls := term.PodAffinityTerm.LabelSelector
	if ls == nil {
		t.Fatal("expected a LabelSelector on the stamped term")
	}
	if ls.MatchLabels[crLabelKey] != crName || ls.MatchLabels[componentLabelKey] != component {
		t.Errorf("selector MatchLabels = %v; want {%s:%s, %s:%s}",
			ls.MatchLabels, crLabelKey, crName, componentLabelKey, component)
	}
}

func hasAntiAffinityWarn(warnings []string) bool {
	for _, w := range warnings {
		if strings.Contains(w, antiAffinityWarnPrefix) {
			return true
		}
	}
	return false
}

// --- Defaulter: stampAntiAffinity ---

func TestDefaulter_AntiAffinity_StampsValkeySet(t *testing.T) {
	v := newStandalone("aa-valkey", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 2
	})
	runDefault(t, v)
	assertSoftSamePodSet(t, v.Spec.Valkey.Affinity, "aa-valkey", componentValkey)
}

func TestDefaulter_AntiAffinity_StampsSentinelSet(t *testing.T) {
	v := newStandalone("aa-sent", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 3
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{Replicas: 3}
	})
	runDefault(t, v)
	assertSoftSamePodSet(t, v.Spec.Valkey.Affinity, "aa-sent", componentValkey)
	assertSoftSamePodSet(t, v.Spec.Sentinel.Affinity, "aa-sent", componentSentinel)
}

func TestDefaulter_AntiAffinity_StandaloneReplicas1None(t *testing.T) {
	v := newStandalone("aa-standalone", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 1
	})
	runDefault(t, v)
	if v.Spec.Valkey.Affinity != nil && v.Spec.Valkey.Affinity.PodAntiAffinity != nil {
		t.Errorf("replicas=1 must not stamp anti-affinity; got %+v",
			v.Spec.Valkey.Affinity.PodAntiAffinity)
	}
}

func TestDefaulter_AntiAffinity_PreservesUserPodAntiAffinity(t *testing.T) {
	userPAA := &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey:   "topology.kubernetes.io/zone",
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "data"}},
		}},
	}
	v := newStandalone("aa-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.Affinity = &corev1.Affinity{PodAntiAffinity: userPAA}
	})
	runDefault(t, v)
	if !reflect.DeepEqual(v.Spec.Valkey.Affinity.PodAntiAffinity, userPAA) {
		t.Errorf("user PodAntiAffinity was modified: %+v", v.Spec.Valkey.Affinity.PodAntiAffinity)
	}
}

func TestDefaulter_AntiAffinity_PreservesOtherAffinityKinds(t *testing.T) {
	na := &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key: "disktype", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"},
				}},
			}},
		},
	}
	v := newStandalone("aa-node", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 2
		v.Spec.Valkey.Affinity = &corev1.Affinity{NodeAffinity: na}
	})
	runDefault(t, v)
	if !reflect.DeepEqual(v.Spec.Valkey.Affinity.NodeAffinity, na) {
		t.Errorf("NodeAffinity was clobbered: %+v", v.Spec.Valkey.Affinity.NodeAffinity)
	}
	assertSoftSamePodSet(t, v.Spec.Valkey.Affinity, "aa-node", componentValkey)
}

func TestDefaulter_AntiAffinity_Idempotent(t *testing.T) {
	v := newStandalone("aa-idem", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 3
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{Replicas: 3}
	})
	runDefault(t, v)
	snapshot := v.DeepCopy()
	runDefault(t, v)
	if !reflect.DeepEqual(v, snapshot) {
		t.Errorf("defaulter not idempotent for anti-affinity:\n first:  %+v\n second: %+v",
			snapshot.Spec.Valkey.Affinity, v.Spec.Valkey.Affinity)
	}
}

// --- Validator: antiAffinityDeviations ---

func TestValidator_AntiAffinity_NoWarnWhenUnset(t *testing.T) {
	// replicas≥2 but Affinity unset: the defaulter stamps the soft
	// default, so the validator must not warn (mirror: unset PDB).
	v := sentinelCR("aa-unset", nil)
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if hasAntiAffinityWarn(w) {
		t.Errorf("did not expect an anti-affinity warning for an unset Affinity; got %v", w)
	}
}

func TestValidator_AntiAffinity_WarnsWhenNoSamePodSetTerm(t *testing.T) {
	// User replaced the default with a cross-set-only term on the
	// valkey set → no same-pod-set spread → warn (and still accept).
	v := sentinelCR("aa-relaxed", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey:   antiAffinityTopologyKey,
						LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{componentLabelKey: componentSentinel}},
					},
				}},
			},
		}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("anti-affinity deviation must never reject; got %v", err)
	}
	if !hasAntiAffinityWarn(w) {
		t.Errorf("expected an %s warning; got %v", antiAffinityWarnPrefix, w)
	}
}

func TestValidator_AntiAffinity_NoWarnOnHardSamePodSet(t *testing.T) {
	v := sentinelCR("aa-hard", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey:   antiAffinityTopologyKey,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{componentLabelKey: componentValkey}},
				}},
			},
		}
	})
	w, _ := runValidate(t, v)
	if hasAntiAffinityWarn(w) {
		t.Errorf("a hard same-pod-set term is stricter than the default — no warn expected; got %v", w)
	}
}

func TestValidator_AntiAffinity_NoWarnOnSoftSamePodSet(t *testing.T) {
	v := sentinelCR("aa-soft", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey:   antiAffinityTopologyKey,
						LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{componentLabelKey: componentValkey}},
					},
				}},
			},
		}
	})
	w, _ := runValidate(t, v)
	if hasAntiAffinityWarn(w) {
		t.Errorf("a soft same-pod-set term matches the default — no warn expected; got %v", w)
	}
}

func TestValidator_AntiAffinity_NoWarnOnMatchExpressionsSamePodSet(t *testing.T) {
	v := sentinelCR("aa-matchexpr", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey: antiAffinityTopologyKey,
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{{
								Key:      componentLabelKey,
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{componentValkey},
							}},
						},
					},
				}},
			},
		}
	})
	w, _ := runValidate(t, v)
	if hasAntiAffinityWarn(w) {
		t.Errorf("a MatchExpressions In on the component label covers the set — no warn expected; got %v", w)
	}
}
