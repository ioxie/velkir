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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ApplyUnstructured is the chokepoint for SSA writes on resources we
// don't vendor typed Go types for — today only `monitoring.coreos.com`
// PodMonitor / ServiceMonitor (prometheus-operator is optional
// infrastructure; the chart installs cleanly on clusters without it).
//
// The newer `client.Client.Apply` method requires
// `runtime.ApplyConfiguration` (a typed builder pattern with
// `IsApplyConfiguration()`), which `*unstructured.Unstructured`
// cannot satisfy. Until typed ApplyConfigurations for
// monitoring.coreos.com become available (which would require
// vendoring prometheus-operator), the only SSA path for these
// objects is the legacy `client.Patch(client.Apply, ...)` form.
//
// staticcheck flags `client.Apply` as deprecated; the deprecation
// notice points at the typed `client.Client.Apply` method which is
// the right home for typed builders. For unstructured the
// deprecated form is still load-bearing — so this helper carries
// the single nolint allowance in the codebase, the custom lint
// plugin's `ssa_use_helper` rule keeps all other call sites
// honest by routing through here.
//
// Caller-supplied opts override the operator's default
// FieldOwner; pass `client.ForceOwnership` when conflicting field
// managers need to be displaced (typical for reconciler-owned
// resources).
func ApplyUnstructured(ctx context.Context, c client.Client, obj *unstructured.Unstructured, opts ...client.PatchOption) error {
	allOpts := append([]client.PatchOption{FieldOwner}, opts...)
	//nolint:staticcheck // client.Apply remains the only SSA patch type for unstructured objects
	return c.Patch(ctx, obj, client.Apply, allOpts...)
}
