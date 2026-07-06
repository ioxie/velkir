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

package pvcresize

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mustQty(s string) resource.Quantity {
	return resource.MustParse(s)
}

func pvcAt(size string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-cr-0"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: mustQty(size)},
			},
		},
	}
}

func scWithExpansion(allow bool) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: "default"},
		AllowVolumeExpansion: new(allow),
	}
}

func TestDetect(t *testing.T) {
	cases := []struct {
		name string
		in   Inputs
		want Outcome
	}{
		{
			name: "no persistence (zero desired)",
			in: Inputs{
				DesiredSize:  resource.Quantity{},
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeNoChange,
		},
		{
			name: "no PVCs yet (bootstrap window)",
			in: Inputs{
				DesiredSize: mustQty("8Gi"),
				PVCs:        nil,
			},
			want: OutcomeNoChange,
		},
		{
			name: "desired equals current — steady state",
			in: Inputs{
				DesiredSize:  mustQty("8Gi"),
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeNoChange,
		},
		{
			name: "desired smaller than current — shrink rejected",
			in: Inputs{
				DesiredSize:  mustQty("4Gi"),
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeShrinkRejected,
		},
		{
			name: "desired larger but expansion not allowed",
			in: Inputs{
				DesiredSize:  mustQty("16Gi"),
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: scWithExpansion(false),
			},
			want: OutcomeExpansionNotSupported,
		},
		{
			name: "desired larger but StorageClass missing",
			in: Inputs{
				DesiredSize:  mustQty("16Gi"),
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: nil,
			},
			want: OutcomeExpansionNotSupported,
		},
		{
			name: "desired larger but allowVolumeExpansion field unset (nil)",
			in: Inputs{
				DesiredSize: mustQty("16Gi"),
				PVCs:        []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: &storagev1.StorageClass{
					ObjectMeta:           metav1.ObjectMeta{Name: "default"},
					AllowVolumeExpansion: nil,
				},
			},
			want: OutcomeExpansionNotSupported,
		},
		{
			name: "desired larger and expansion allowed — resize needed",
			in: Inputs{
				DesiredSize:  mustQty("16Gi"),
				PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeResizeNeeded,
		},
		{
			name: "smallest current size drives the comparison (mixed PVC set)",
			in: Inputs{
				DesiredSize: mustQty("12Gi"),
				PVCs: []corev1.PersistentVolumeClaim{
					pvcAt("16Gi"),
					pvcAt("8Gi"),
					pvcAt("16Gi"),
				},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeResizeNeeded,
		},
		{
			name: "all PVCs already at desired (smallest == desired)",
			in: Inputs{
				DesiredSize: mustQty("8Gi"),
				PVCs: []corev1.PersistentVolumeClaim{
					pvcAt("8Gi"),
					pvcAt("8Gi"),
				},
				StorageClass: scWithExpansion(true),
			},
			want: OutcomeNoChange,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Detect(tc.in)
			if got.Outcome != tc.want {
				t.Errorf("Detect outcome = %v, want %v (current=%s desired=%s)",
					got.Outcome, tc.want, got.Current.String(), got.Desired.String())
			}
		})
	}
}

func TestDetect_CapacityFieldsCarry(t *testing.T) {
	in := Inputs{
		DesiredSize:  mustQty("16Gi"),
		PVCs:         []corev1.PersistentVolumeClaim{pvcAt("8Gi")},
		StorageClass: scWithExpansion(true),
	}
	got := Detect(in)
	if got.Outcome != OutcomeResizeNeeded {
		t.Fatalf("expected OutcomeResizeNeeded, got %v", got.Outcome)
	}
	if got.Current.String() != "8Gi" {
		t.Errorf("Current = %q, want 8Gi", got.Current.String())
	}
	if got.Desired.String() != "16Gi" {
		t.Errorf("Desired = %q, want 16Gi", got.Desired.String())
	}
}
