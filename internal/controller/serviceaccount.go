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

	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/util/ssa"
)

// reconcileServiceAccounts applies a dedicated, RBAC-less ServiceAccount
// for the CR's data-plane pods so they no longer run as the namespace
// `default` SA. The valkey and sentinel pods need no Kubernetes API
// access, so the SAs carry no Role/RoleBinding, and the pods disable
// token automount (see buildValkeySTS / buildSentinelSTS) — no API
// credential is ever mounted into a data-plane container.
//
// `<cr>-valkey` exists in every mode; `<cr>-sentinel` only in sentinel
// mode (it has no consumer otherwise). Both are owner-ref'd to the CR so
// they are garbage-collected on CR deletion.
func (r *ValkeyReconciler) reconcileServiceAccounts(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	if err := ssa.ApplyAC(ctx, r.Client, buildServiceAccount(v, valkeyServiceAccountName(v), componentValkey)); err != nil {
		return fmt.Errorf("applying valkey ServiceAccount: %w", err)
	}
	if v.Spec.Mode == valkeyv1beta1.ModeSentinel {
		if err := ssa.ApplyAC(ctx, r.Client, buildServiceAccount(v, sentinelServiceAccountName(v), componentSentinel)); err != nil {
			return fmt.Errorf("applying sentinel ServiceAccount: %w", err)
		}
	}
	return nil
}

// valkeyServiceAccountName / sentinelServiceAccountName are the per-CR SA
// names the data-plane pod specs reference; kept as helpers so the
// builder and the pod-spec wiring can't drift.
func valkeyServiceAccountName(v *valkeyv1beta1.Valkey) string   { return v.Name + "-valkey" }
func sentinelServiceAccountName(v *valkeyv1beta1.Valkey) string { return v.Name + "-sentinel" }

// buildServiceAccount renders a bare ServiceAccount apply-config: owner-
// ref'd to the CR, carrying the standard owned labels, and deliberately
// nothing else — no RBAC, no imagePullSecrets, no SA-level automount
// toggle (the pods set automountServiceAccountToken=false themselves).
func buildServiceAccount(v *valkeyv1beta1.Valkey, name, component string) *corev1ac.ServiceAccountApplyConfiguration {
	return corev1ac.ServiceAccount(name, v.Namespace).
		WithLabels(ownedLabels(v, component)).
		WithOwnerReferences(crOwnerRef(v))
}
