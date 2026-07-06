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

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Unit tests for the "data" volume wiring in the STS builder. Two
// shapes share the same name (dataVolumeName) so the valkey
// container's volumeMount resolves regardless of which one is in
// flight:
//
//   - Persistence == nil → an emptyDir volume entry on the pod
//     template.
//   - Persistence != nil → no volume entry on the pod template; the
//     STS controller projects one from the volumeClaimTemplate.

// dataVolumeName matches the volume / VCT name produced by
// buildValkeyVolumes + buildValkeyDataPVC. Hoisted so the test
// assertions reference a single source of truth (goconst).
const dataVolumeName = "data"

func TestBuildValkeySTS_DataVolume_EmptyDirWhenNoPersistence(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Spec.Valkey.Persistence = nil

	sts := buildValkeySTS(v, testCMHash)
	if sts == nil || sts.Spec == nil {
		t.Fatal("buildValkeySTS returned nil spec")
	}
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Errorf("expected no volumeClaimTemplates when Persistence is nil; got %d", len(sts.Spec.VolumeClaimTemplates))
	}

	var dataVol *string
	for i := range sts.Spec.Template.Spec.Volumes {
		vol := &sts.Spec.Template.Spec.Volumes[i]
		if vol.Name != nil && *vol.Name == dataVolumeName {
			dataVol = vol.Name
			if vol.EmptyDir == nil {
				t.Errorf("pod template's %q volume must be emptyDir when Persistence is nil", dataVolumeName)
			}
		}
	}
	if dataVol == nil {
		t.Errorf("pod template must include an emptyDir %q volume when Persistence is nil", dataVolumeName)
	}
}

func TestBuildValkeySTS_DataVolume_VCTWhenPersistenceSet(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
		StorageClass: new("test-expand"),
		Size:         resource.MustParse("4Gi"),
		AccessModes:  []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
	}

	sts := buildValkeySTS(v, testCMHash)
	if sts == nil || sts.Spec == nil {
		t.Fatal("buildValkeySTS returned nil spec")
	}

	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected one volumeClaimTemplate when Persistence is set; got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	vct := &sts.Spec.VolumeClaimTemplates[0]
	if vct.Name == nil || *vct.Name != dataVolumeName {
		t.Errorf("VCT name = %v, want %q", vct.Name, dataVolumeName)
	}
	if vct.Spec == nil {
		t.Fatal("VCT spec is nil")
	}
	if vct.Spec.StorageClassName == nil || *vct.Spec.StorageClassName != "test-expand" {
		t.Errorf("VCT StorageClassName = %v, want \"test-expand\"", vct.Spec.StorageClassName)
	}
	if vct.Spec.Resources == nil || vct.Spec.Resources.Requests == nil {
		t.Fatal("VCT spec.resources.requests is nil")
	}
	got := (*vct.Spec.Resources.Requests)[corev1.ResourceStorage]
	want := resource.MustParse("4Gi")
	if got.Cmp(want) != 0 {
		t.Errorf("VCT storage request = %s, want %s", got.String(), want.String())
	}
	if len(vct.Spec.AccessModes) != 1 || vct.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("VCT access modes = %v, want [ReadWriteOnce]", vct.Spec.AccessModes)
	}

	// VCT labels carry the CR + component selectors so the operator's
	// label-keyed PVC selectors (reconcileDeletion, reconcilePVCResize)
	// match the PVCs the STS controller creates from this template.
	if vct.Labels[CRLabel] != v.Name {
		t.Errorf("VCT label %q = %q, want %q", CRLabel, vct.Labels[CRLabel], v.Name)
	}
	if vct.Labels[ComponentLabel] != componentValkey {
		t.Errorf("VCT label %q = %q, want %q", ComponentLabel, vct.Labels[ComponentLabel], componentValkey)
	}

	// Pod template must NOT carry a manual data volume entry when
	// the VCT is in play — the STS controller injects one keyed on the
	// VCT name. A duplicate manual entry would conflict.
	for i := range sts.Spec.Template.Spec.Volumes {
		vol := &sts.Spec.Template.Spec.Volumes[i]
		if vol.Name != nil && *vol.Name == dataVolumeName {
			t.Errorf("pod template must NOT include a manual %q volume when VCT is set", dataVolumeName)
		}
	}
}

func TestBuildValkeyDataPVC_StorageClassOptional(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
		Size: resource.MustParse("2Gi"),
	}
	pvc := buildValkeyDataPVC(v, v.Spec.Valkey.Persistence)
	if pvc.Spec.StorageClassName != nil {
		t.Errorf("StorageClassName should be omitted (cluster default) when Persistence.StorageClass is nil; got %q", *pvc.Spec.StorageClassName)
	}

	empty := ""
	v.Spec.Valkey.Persistence.StorageClass = &empty
	pvc = buildValkeyDataPVC(v, v.Spec.Valkey.Persistence)
	if pvc.Spec.StorageClassName != nil {
		t.Errorf("StorageClassName should be omitted when Persistence.StorageClass is the empty string; got %q", *pvc.Spec.StorageClassName)
	}
}

// CRD schema applies +kubebuilder:default={"ReadWriteOnce"} on
// AccessModes with MinItems=1, so the defaulter ensures the slice
// is never empty in practice. The builder still handles the empty
// case defensively (the field is omitted, which makes K8s apply
// cluster defaults) — this test pins that behaviour so a future
// refactor doesn't silently start emitting an empty AccessModes list.
func TestBuildValkeyDataPVC_EmptyAccessModesOmitsField(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
		Size: resource.MustParse("1Gi"),
	}
	pvc := buildValkeyDataPVC(v, v.Spec.Valkey.Persistence)
	if len(pvc.Spec.AccessModes) != 0 {
		t.Errorf("AccessModes should be omitted when PersistenceSpec.AccessModes is empty; got %v", pvc.Spec.AccessModes)
	}
}
