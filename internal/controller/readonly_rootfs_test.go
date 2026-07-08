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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Managed data-plane containers (valkey, sentinel, exporter) must
// run with readOnlyRootFilesystem=true, backed by a writable `tmp`
// emptyDir mounted at /tmp (which also backs the non-persistent
// `dir /tmp`). Init containers are intentionally left writable-root.

func rofsStandaloneCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			Image: valkeyv1beta1.ImageSpec{
				Valkey: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
			},
		},
	}
}

func rootFSReadOnly(c *corev1ac.ContainerApplyConfiguration) bool {
	return c.SecurityContext != nil &&
		c.SecurityContext.ReadOnlyRootFilesystem != nil &&
		*c.SecurityContext.ReadOnlyRootFilesystem
}

func containerMountsTmp(c *corev1ac.ContainerApplyConfiguration) bool {
	for _, m := range c.VolumeMounts {
		if m.Name != nil && *m.Name == "tmp" && m.MountPath != nil && *m.MountPath == "/tmp" {
			return true
		}
	}
	return false
}

func volumesHaveTmpEmptyDir(vols []*corev1ac.VolumeApplyConfiguration) bool {
	for _, vol := range vols {
		if vol.Name != nil && *vol.Name == "tmp" && vol.EmptyDir != nil {
			return true
		}
	}
	return false
}

func TestReadOnlyRootFS_ValkeyContainerAndVolumes(t *testing.T) {
	v := rofsStandaloneCR()
	c := buildValkeyContainer(v)
	if !rootFSReadOnly(c) {
		t.Error("valkey container must set securityContext.readOnlyRootFilesystem=true")
	}
	if !containerMountsTmp(c) {
		t.Error("valkey container must mount the tmp emptyDir at /tmp")
	}
	if !volumesHaveTmpEmptyDir(buildValkeyVolumes(v)) {
		t.Error("valkey pod volumes must include a writable tmp emptyDir")
	}
}

func TestReadOnlyRootFS_ExporterContainer(t *testing.T) {
	v := exporterTestCR()
	c := buildExporterContainer(v)
	if !rootFSReadOnly(c) {
		t.Error("exporter container must set securityContext.readOnlyRootFilesystem=true")
	}
	// The exporter previously mounted nothing — readOnlyRootFilesystem
	// needs a writable /tmp for the redis_exporter process.
	if !containerMountsTmp(c) {
		t.Error("exporter container must mount the tmp emptyDir at /tmp")
	}
}

func TestReadOnlyRootFS_SentinelContainerAndVolumes(t *testing.T) {
	v := sentinelTestCR()
	c := buildSentinelContainer(v)
	if !rootFSReadOnly(c) {
		t.Error("sentinel container must set securityContext.readOnlyRootFilesystem=true")
	}
	if !containerMountsTmp(c) {
		t.Error("sentinel container must mount the tmp emptyDir at /tmp")
	}
	if !volumesHaveTmpEmptyDir(buildSentinelVolumes(v)) {
		t.Error("sentinel pod volumes must include a writable tmp emptyDir")
	}
}

func TestReadOnlyRootFS_NonPersistentValkeyHasWritableDataPath(t *testing.T) {
	// Non-persistent standalone renders `dir /tmp`; with a read-only
	// root that path MUST be a writable mount or valkey-server can't
	// write its dump and fails to start.
	v := rofsStandaloneCR() // no persistence → dir /tmp
	if v.Spec.Valkey.Persistence != nil {
		t.Fatal("precondition: this case is the non-persistent (emptyDir) path")
	}
	if !volumesHaveTmpEmptyDir(buildValkeyVolumes(v)) {
		t.Fatal("non-persistent valkey must back `dir /tmp` with a writable tmp emptyDir")
	}
	if !containerMountsTmp(buildValkeyContainer(v)) {
		t.Error("non-persistent valkey container must mount tmp at /tmp")
	}
}
