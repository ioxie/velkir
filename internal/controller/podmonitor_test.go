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
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

const (
	testPMCRName      = "bench"
	testPMCRNamespace = "valkey-bench"
)

func newCRWithMetrics(metricsEnabled, podMonEnabled bool, interval string) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPMCRName,
			Namespace: testPMCRNamespace,
			UID:       types.UID("aaaa-bbbb-cccc-dddd"),
		},
	}
	v.Spec.Metrics.Enabled = new(metricsEnabled)
	v.Spec.Metrics.PodMonitor.Enabled = new(podMonEnabled)
	v.Spec.Metrics.PodMonitor.ScrapeInterval = interval
	return v
}

func TestBuildPodMonitor_BasicShape(t *testing.T) {
	v := newCRWithMetrics(true, true, "15s")
	obj := buildPodMonitor(v)

	if got := obj.GetAPIVersion(); got != "monitoring.coreos.com/v1" {
		t.Errorf("apiVersion = %q; want monitoring.coreos.com/v1", got)
	}
	if got := obj.GetKind(); got != "PodMonitor" {
		t.Errorf("kind = %q; want PodMonitor", got)
	}
	if got := obj.GetName(); got != testPMCRName {
		t.Errorf("name = %q; want bench (CR name)", got)
	}
	if got := obj.GetNamespace(); got != testPMCRNamespace {
		t.Errorf("namespace = %q; want valkey-bench", got)
	}

	// Owner reference back to the CR, controller=true (so GC collects on CR delete).
	refs := obj.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("ownerReferences = %d; want exactly 1", len(refs))
	}
	if refs[0].Name != "bench" || refs[0].Kind != "Valkey" {
		t.Errorf("owner = %s/%s; want Valkey/bench", refs[0].Kind, refs[0].Name)
	}
	if refs[0].UID != "aaaa-bbbb-cccc-dddd" {
		t.Errorf("owner UID = %q; want aaaa-bbbb-cccc-dddd", refs[0].UID)
	}
	if refs[0].Controller == nil || !*refs[0].Controller {
		t.Errorf("owner.Controller = %v; want true", refs[0].Controller)
	}
}

func TestBuildPodMonitor_SelectorAndNamespaceScope(t *testing.T) {
	v := newCRWithMetrics(true, true, "30s")
	obj := buildPodMonitor(v)

	// matchLabels must include both CR label + component label so we
	// don't sweep up Valkey CRs in the same namespace.
	spec, ok := obj.Object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec not a map: %#v", obj.Object["spec"])
	}
	selector, ok := spec["selector"].(map[string]any)
	if !ok {
		t.Fatalf("spec.selector not a map: %#v", spec["selector"])
	}
	ml, ok := selector["matchLabels"].(map[string]any)
	if !ok {
		t.Fatalf("spec.selector.matchLabels not a map: %#v", selector["matchLabels"])
	}
	if ml[CRLabel] != testPMCRName {
		t.Errorf("matchLabels[%q] = %v; want bench", CRLabel, ml[CRLabel])
	}
	if ml[ComponentLabel] != componentValkey {
		t.Errorf("matchLabels[%q] = %v; want %s", ComponentLabel, ml[ComponentLabel], componentValkey)
	}

	// namespaceSelector pins to the CR's namespace — cluster-wide
	// scrape would mix up cohabiting Valkey installs.
	nss, ok := spec["namespaceSelector"].(map[string]any)
	if !ok {
		t.Fatalf("spec.namespaceSelector not a map: %#v", spec["namespaceSelector"])
	}
	names, ok := nss["matchNames"].([]any)
	if !ok || len(names) != 1 || names[0] != testPMCRNamespace {
		t.Errorf("namespaceSelector.matchNames = %v; want [valkey-bench]", names)
	}
}

func TestBuildPodMonitor_EndpointAndRelabel(t *testing.T) {
	v := newCRWithMetrics(true, true, "15s")
	obj := buildPodMonitor(v)
	spec := obj.Object["spec"].(map[string]any)

	endpoints, ok := spec["podMetricsEndpoints"].([]any)
	if !ok || len(endpoints) != 1 {
		t.Fatalf("podMetricsEndpoints = %v; want exactly 1", endpoints)
	}
	ep := endpoints[0].(map[string]any)
	if ep["port"] != "metrics" {
		t.Errorf("port = %v; want metrics (named, not numeric)", ep["port"])
	}
	if ep["interval"] != "15s" {
		t.Errorf("interval = %v; want 15s (from spec)", ep["interval"])
	}

	relabels, ok := ep["relabelings"].([]any)
	if !ok {
		t.Fatalf("relabelings not a slice: %#v", ep["relabelings"])
	}
	// Every dashboard panel that filters per-CR depends on these
	// four targetLabels. Asserting set-membership rather than order
	// so reordering for readability doesn't break the test.
	want := map[string]bool{
		"pod":             false,
		"valkey_instance": false,
		"name":            false,
		"valkey_role":     false,
	}
	for _, rraw := range relabels {
		r := rraw.(map[string]any)
		tl, _ := r["targetLabel"].(string)
		if _, ok := want[tl]; ok {
			want[tl] = true
		}
	}
	for tl, seen := range want {
		if !seen {
			t.Errorf("missing relabel targetLabel=%q; dashboards require it", tl)
		}
	}
}

func TestBuildPodMonitor_IntervalDefaults(t *testing.T) {
	// Empty interval falls back to 30s — defensive: webhook
	// defaulter normally guarantees a value, but the builder must
	// not stamp an invalid-empty endpoint when the defaulter is
	// bypassed (envtest paths, future re-shapings).
	v := newCRWithMetrics(true, true, "")
	obj := buildPodMonitor(v)
	spec := obj.Object["spec"].(map[string]any)
	endpoints := spec["podMetricsEndpoints"].([]any)
	ep := endpoints[0].(map[string]any)
	if ep["interval"] != "30s" {
		t.Errorf("interval = %v; want 30s fallback when spec.scrapeInterval is empty", ep["interval"])
	}
}

func TestEmptyPodMonitor_DeleteShape(t *testing.T) {
	// emptyPodMonitor is what Delete() sees on the disabled path. It
	// MUST carry the right GVK + name + namespace so the apiserver
	// can identify the target, but spec is irrelevant.
	v := newCRWithMetrics(false, false, "")
	obj := emptyPodMonitor(v)

	if got := obj.GetAPIVersion(); got != "monitoring.coreos.com/v1" {
		t.Errorf("apiVersion = %q; want monitoring.coreos.com/v1", got)
	}
	if got := obj.GetKind(); got != "PodMonitor" {
		t.Errorf("kind = %q; want PodMonitor", got)
	}
	if got := obj.GetName(); got != testPMCRName {
		t.Errorf("name = %q; want bench", got)
	}
	if got := obj.GetNamespace(); got != testPMCRNamespace {
		t.Errorf("namespace = %q; want valkey-bench", got)
	}
	if _, hasSpec := obj.Object["spec"]; hasSpec {
		t.Errorf("emptyPodMonitor has spec field; delete-only object should not carry it")
	}
}

// pmRecordingClient is a minimal client.Client mock for testing
// reconcilePodMonitor's branching. Implementations record the last
// Patch / Delete call and let the test return either nil or a
// scripted error (typically NoMatchError for the CRD-missing path).
type pmRecordingClient struct {
	client.Client
	patched    runtime.Object
	patchErr   error
	deleted    runtime.Object
	deleteErr  error
	patchCount int
	delCount   int
}

func (c *pmRecordingClient) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	c.patched = obj
	c.patchCount++
	return c.patchErr
}

func (c *pmRecordingClient) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	c.deleted = obj
	c.delCount++
	return c.deleteErr
}

func TestReconcilePodMonitor_DisabledMetrics_DeletesPodMonitor(t *testing.T) {
	// metrics.enabled=false → reconcilePodMonitor must Delete the
	// PodMonitor (in case it was previously stamped) and return nil.
	v := newCRWithMetrics(false, true, "30s")
	c := &pmRecordingClient{deleteErr: apierrors.NewNotFound(schema.GroupResource{Group: "monitoring.coreos.com", Resource: "podmonitors"}, v.Name)}
	r := &ValkeyReconciler{Client: c}
	if err := r.reconcilePodMonitor(context.Background(), v); err != nil {
		t.Fatalf("reconcilePodMonitor: %v", err)
	}
	if c.patchCount != 0 {
		t.Errorf("expected no Patch on disabled path; got %d", c.patchCount)
	}
	if c.delCount != 1 {
		t.Errorf("expected exactly 1 Delete on disabled path; got %d", c.delCount)
	}
}

func TestReconcilePodMonitor_DisabledMetrics_NoMatchSwallowed(t *testing.T) {
	// prometheus-operator not installed: Delete returns NoMatchError
	// which must be swallowed (otherwise toggling enabled=false on a
	// cluster without the CRD wedges the reconciler).
	v := newCRWithMetrics(false, false, "")
	c := &pmRecordingClient{deleteErr: &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "monitoring.coreos.com", Kind: "PodMonitor"}}}
	r := &ValkeyReconciler{Client: c}
	if err := r.reconcilePodMonitor(context.Background(), v); err != nil {
		t.Errorf("NoMatchError on disabled path must be swallowed; got %v", err)
	}
}

func TestReconcilePodMonitor_DisabledMetrics_PropagatesOtherErrors(t *testing.T) {
	// Permission denied (or any non-NotFound/non-NoMatch) must
	// propagate so RBAC drift surfaces as a reconcile failure.
	v := newCRWithMetrics(false, false, "")
	c := &pmRecordingClient{deleteErr: apierrors.NewForbidden(schema.GroupResource{Group: "monitoring.coreos.com", Resource: "podmonitors"}, v.Name, errors.New("forbidden"))}
	r := &ValkeyReconciler{Client: c}
	if err := r.reconcilePodMonitor(context.Background(), v); err == nil {
		t.Errorf("Forbidden on disabled path must propagate; got nil")
	}
}

func TestReconcilePodMonitor_Enabled_NoMatchSwallowed(t *testing.T) {
	// On the enabled path, NoMatchError from Patch means the CRD
	// isn't installed. Must be swallowed (logged) so the operator
	// doesn't crash-loop on a cluster without prometheus-operator.
	v := newCRWithMetrics(true, true, "30s")
	c := &pmRecordingClient{patchErr: &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "monitoring.coreos.com", Kind: "PodMonitor"}}}
	r := &ValkeyReconciler{Client: c}
	if err := r.reconcilePodMonitor(context.Background(), v); err != nil {
		t.Errorf("NoMatchError on enabled path must be swallowed; got %v", err)
	}
	if c.patchCount != 1 {
		t.Errorf("expected exactly 1 Patch attempt; got %d", c.patchCount)
	}
	// Verify the Patch was passed the right unstructured shape.
	patched, ok := c.patched.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("Patch target is not *unstructured.Unstructured: %T", c.patched)
	}
	if patched.GetName() != v.Name || patched.GetNamespace() != v.Namespace {
		t.Errorf("Patched object name/namespace = %s/%s; want %s/%s",
			patched.GetNamespace(), patched.GetName(), v.Namespace, v.Name)
	}
}

func TestReconcilePodMonitor_Enabled_PropagatesPatchErrors(t *testing.T) {
	v := newCRWithMetrics(true, true, "30s")
	c := &pmRecordingClient{patchErr: apierrors.NewForbidden(schema.GroupResource{Group: "monitoring.coreos.com", Resource: "podmonitors"}, v.Name, errors.New("forbidden"))}
	r := &ValkeyReconciler{Client: c}
	if err := r.reconcilePodMonitor(context.Background(), v); err == nil {
		t.Errorf("Forbidden on enabled path must propagate; got nil")
	}
}
