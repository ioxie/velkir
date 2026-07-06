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

package ssa

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ApplyAC server-side-applies a typed `runtime.ApplyConfiguration`. This is
// the controller-runtime v0.23+ API: callers pass a builder-constructed
// `*ApplyConfiguration` (e.g. `corev1ac.ConfigMap(name, ns).WithData(...)`)
// instead of a populated typed object. The builder types live under
// `k8s.io/client-go/applyconfigurations/...` for built-in resources;
// CRD apply-configs require running `applyconfiguration-gen` against the
// project's API types and are tracked separately (follow-up phases).
//
// Compared to the legacy `Apply` (above):
//
//   - **No DeepCopy needed.** The builder pattern produces a fresh value
//     per call site; there's no caller-owned object to mutate.
//   - **No ManagedFields stripping.** Apply-configs don't expose a
//     ManagedFields setter, so the `metadata.managedFields must be nil`
//     422 trap can't happen.
//   - **No SA1019 staticcheck suppression.** The new `client.Client.Apply`
//     method is current API; `client.Apply` (the patch constant) is the
//     deprecated one this helper replaces.
//
// FieldOwner default + opts merge mirror the legacy helper exactly: the
// default `velkir` owner is prepended to opts so caller-supplied
// `client.FieldOwner(...)` overrides win. Caller-supplied
// `client.ForceOwnership` enables takeover of conflicting fields.
//
// Migration phases:
//
//   - Phase 1 (this PR): helper + ConfigMap call sites.
//   - Phase 2: Service call sites (3 sites in valkey_controller.go,
//     comparable shape complexity to ConfigMap).
//   - Phase 3: StatefulSet call site (1 site, ~46 lines of nested
//     PodSpec/Container/Volume builders).
//   - Phase 4: dynauth webhook configurations (List → modify → apply
//     pattern; harder to express as a from-scratch apply-config).
//   - Phase 5: SentinelQuorum CRD path — needs `applyconfiguration-gen`
//     wired into the codegen pipeline first.
//
// The legacy `Apply` / `ApplyStatus` stay in place during the migration
// so unmigrated callers keep working.
func ApplyAC(ctx context.Context, c client.Client, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return c.Apply(ctx, obj, append([]client.ApplyOption{FieldOwnerApply}, opts...)...)
}

// ApplyACStatus is the status-subresource analogue of ApplyAC. Use for
// CR-owned status fields whose apply-config builder includes a `WithStatus`
// helper. The same field-ownership tracking applies separately per
// subresource (status changes don't affect spec field-managers and vice
// versa).
func ApplyACStatus(ctx context.Context, c client.Client, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return c.Status().Apply(ctx, obj, append([]client.SubResourceApplyOption{FieldOwnerApply}, opts...)...)
}

// FieldOwnerApply mirrors FieldOwner but typed as client.FieldOwner — the
// same value, but with the type the new Apply API expects (it satisfies
// both `client.PatchOption` for the legacy path and `client.ApplyOption`
// for the new path via type assertions in controller-runtime).
//
// Defining a typed alias avoids per-callsite reconstruction or unsafe
// re-typing; both helpers reference one constant.
const FieldOwnerApply = client.FieldOwner("velkir")
