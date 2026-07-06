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
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/util/ssa"
)

// podMonitorGVK is the typed identity of the prometheus-operator
// PodMonitor CRD. We treat the type as unstructured so the operator
// doesn't have to vendor prometheus-operator's typed API — the CRD
// is optional infrastructure (clusters without prometheus-operator
// installed must still install the chart cleanly).
var podMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "PodMonitor",
}

// reconcilePodMonitor stamps a PodMonitor for the CR's exporter
// sidecars when spec.metrics.enabled=true AND
// spec.metrics.podMonitor.enabled=true. The PodMonitor selects
// pods labelled with this CR's identifier + componentValkey, and
// scrapes the exporter sidecar's named "metrics" port.
//
// Relabelings inject `valkey_instance` and `valkey_role` onto the
// scraped redis_* series so the chart-shipped dashboards filter
// correctly (and so the operator-side gauges keyed on `name` align
// with the exporter-side series keyed on `valkey_instance` — both
// hold the CR name; the duplication is by design).
//
// When the user disables podMonitor (or the whole metrics surface),
// any previously-applied PodMonitor is deleted; the CR's
// OwnerReference would also collect it on CR deletion but explicit
// cleanup avoids leaving a stale PodMonitor for the duration of a
// cluster's life.
//
// Cluster without prometheus-operator installed: SSA returns
// NoMatchError; treat as a non-error best-effort and log so the
// user understands the spec.metrics.podMonitor toggle was a no-op.
func (r *ValkeyReconciler) reconcilePodMonitor(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	log := logf.FromContext(ctx)
	metricsOn := v.Spec.Metrics.Enabled != nil && *v.Spec.Metrics.Enabled
	pmOn := v.Spec.Metrics.PodMonitor.Enabled != nil && *v.Spec.Metrics.PodMonitor.Enabled

	if !metricsOn || !pmOn {
		// Best-effort delete. NotFound and NoMatch both mean
		// "nothing to clean up" (the user toggled off without ever
		// applying, or prometheus-operator isn't installed). Other
		// errors propagate — a permission denial should surface as
		// a reconcile failure so the operator surfaces RBAC drift.
		obj := emptyPodMonitor(v)
		if err := r.Delete(ctx, obj); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				return nil
			}
			return fmt.Errorf("deleting PodMonitor %s/%s: %w", v.Namespace, v.Name, err)
		}
		return nil
	}

	obj := buildPodMonitor(v)
	if err := ssa.ApplyUnstructured(ctx, r.Client, obj, client.ForceOwnership); err != nil {
		if meta.IsNoMatchError(err) {
			log.Info("PodMonitor CRD not installed in cluster; spec.metrics.podMonitor.enabled is a no-op until prometheus-operator (or equivalent) is installed",
				"cr", v.Namespace+"/"+v.Name)
			return nil
		}
		return fmt.Errorf("applying PodMonitor %s/%s: %w", v.Namespace, v.Name, err)
	}
	return nil
}

// emptyPodMonitor returns the minimal unstructured object Delete()
// needs to identify which PodMonitor to remove. Used on the
// metrics-disabled path; spec is irrelevant for delete.
func emptyPodMonitor(v *valkeyv1beta1.Valkey) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(podMonitorGVK)
	obj.SetName(v.Name)
	obj.SetNamespace(v.Namespace)
	return obj
}

// buildPodMonitor renders the PodMonitor for v's exporter sidecars.
//
// Endpoint settings:
//
//   - `port: metrics` is the container port name on the exporter
//     sidecar (see buildExporterContainer). Using the name (not a
//     numeric value) keeps the PodMonitor stable across exporter-
//     port renames.
//   - `interval` follows spec.metrics.podMonitor.scrapeInterval; the
//     webhook defaulter guarantees a non-empty value, but we
//     defensively fall back to 30s.
//   - relabelings inject the labels chart-shipped dashboards filter
//     on:
//   - `pod` — pod name, so panels can show "{{pod}}" legends.
//   - `valkey_instance` — the CR name. Comes from the pod label
//     `velkir.ioxie.dev/cr` (sanitised by kubernetes SD to
//     `velkir_ioxie_dev_cr`). Exporter-side metrics use this label
//     key; operator-side metrics use `name` for the same value
//     — we stamp BOTH so dashboards filter consistently across
//     the two metric families.
//   - `name` — alias of valkey_instance for the same reason.
//   - `valkey_role` — primary/replica. Required by panels that
//     distinguish lag direction or per-role memory usage.
func buildPodMonitor(v *valkeyv1beta1.Valkey) *unstructured.Unstructured {
	interval := v.Spec.Metrics.PodMonitor.ScrapeInterval
	if interval == "" {
		interval = "30s"
	}

	relabelings := []any{
		map[string]any{
			"sourceLabels": []any{"__meta_kubernetes_pod_name"},
			"targetLabel":  "pod",
		},
		map[string]any{
			"sourceLabels": []any{"__meta_kubernetes_pod_label_velkir_ioxie_dev_cr"},
			"targetLabel":  "valkey_instance",
		},
		map[string]any{
			"sourceLabels": []any{"__meta_kubernetes_pod_label_velkir_ioxie_dev_cr"},
			"targetLabel":  "name",
		},
		map[string]any{
			"sourceLabels": []any{"__meta_kubernetes_pod_label_velkir_ioxie_dev_role"},
			"targetLabel":  "valkey_role",
		},
	}

	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "PodMonitor",
			"metadata": map[string]any{
				"name":      v.Name,
				"namespace": v.Namespace,
				"labels": map[string]any{
					CRLabel:                        v.Name,
					ComponentLabel:                 componentValkey,
					"app.kubernetes.io/managed-by": "velkir",
				},
			},
			"spec": map[string]any{
				"selector": map[string]any{
					"matchLabels": map[string]any{
						CRLabel:        v.Name,
						ComponentLabel: componentValkey,
					},
				},
				"namespaceSelector": map[string]any{
					"matchNames": []any{v.Namespace},
				},
				"podMetricsEndpoints": []any{
					map[string]any{
						"port":        "metrics",
						"interval":    interval,
						"relabelings": relabelings,
					},
				},
			},
		},
	}
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion:         valkeyv1beta1.GroupVersion.String(),
			Kind:               "Valkey",
			Name:               v.Name,
			UID:                v.UID,
			Controller:         new(true),
			BlockOwnerDeletion: new(true),
		},
	})
	return obj
}
