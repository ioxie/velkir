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
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	tNS  = "ns"
	tCR  = "cr"
	tLab = "velkir.ioxie.dev/cr"

	// Substate phase-name literals. Held here so the goconst linter
	// doesn't trip on >3 occurrences across the table-shaped per-step
	// tests and the full sequence walk.
	phaseStsOrphaned  = "StsOrphaned"
	phasePVCsPatched  = "PVCsPatched"
	phaseStsRecreated = "StsRecreated"
	phaseVerified     = "Verified"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("appsv1 scheme: %v", err)
	}
	return s
}

// target returns the Target the controller would build for a CR named
// tCR with a 16Gi desired size. All transition tests share the same
// shape; the size is fixed because it's what every test asserts
// against (the only varying field worth exercising in this suite is
// the cluster state, not the desired-size input).
func target() Target {
	return Target{
		Namespace:      tNS,
		STSName:        tCR,
		PVCLabels:      map[string]string{tLab: tCR},
		DesiredSize:    resource.MustParse("16Gi"),
		StsRequeueWait: 2 * time.Second,
		PvcRequeueWait: 5 * time.Second,
		PollInterval:   5 * time.Second,
		StsApplyWait:   1 * time.Second,
	}
}

func makePVC(name, capacity string, status string) corev1.PersistentVolumeClaim {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: tNS,
			Name:      name,
			Labels:    map[string]string{tLab: tCR},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
			},
		},
	}
	if status != "" {
		pvc.Status = corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(status)},
		}
	}
	return pvc
}

// makeSTS builds a 3-replica STS at the given VCT size + ready-replica
// count. Replicas is fixed at 3 because the substate machine's
// transitions don't branch on cluster size; varying ready against 3
// covers the "not ready / partially ready / fully ready" branches.
// Tests that need a different shape (e.g. nil-Replicas edge) build the
// STS inline.
func makeSTS(ready int32, vctSize string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: tNS, Name: tCR},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(3)),
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(vctSize)},
						},
					},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: ready},
	}
}

func TestFromValidated_DeletesSTSWithOrphanPropagation(t *testing.T) {
	scheme := newScheme(t)
	sts := makeSTS(3, "8Gi")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()

	got := FromValidated(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phaseStsOrphaned {
		t.Fatalf("Action=%v NextPhase=%q want Advance/StsOrphaned", got.Action, got.NextPhase)
	}

	check := &appsv1.StatefulSet{}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: tNS, Name: tCR}, check)
	if err == nil {
		t.Errorf("STS should have been deleted, still found in fake client")
	}
}

func TestFromValidated_FastForwardsWhenSTSAlreadyAbsent(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	got := FromValidated(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phaseStsOrphaned {
		t.Fatalf("Action=%v NextPhase=%q want Advance/StsOrphaned", got.Action, got.NextPhase)
	}
}

func TestFromStsOrphaned_HoldsWhileSTSStillPresent(t *testing.T) {
	scheme := newScheme(t)
	sts := makeSTS(3, "8Gi")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()

	got := FromStsOrphaned(context.Background(), c, target())
	if got.Action != ActionRequeue {
		t.Errorf("Action=%v want Requeue (STS still present)", got.Action)
	}
}

func TestFromStsOrphaned_PatchesPVCsToDesired(t *testing.T) {
	scheme := newScheme(t)
	pvcs := []client.Object{
		new(makePVC("data-cr-0", "8Gi", "8Gi")),
		new(makePVC("data-cr-1", "8Gi", "8Gi")),
		new(makePVC("data-cr-2", "8Gi", "8Gi")),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvcs...).Build()

	got := FromStsOrphaned(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phasePVCsPatched {
		t.Fatalf("Action=%v NextPhase=%q want Advance/PVCsPatched", got.Action, got.NextPhase)
	}
	check := &corev1.PersistentVolumeClaimList{}
	if err := c.List(context.Background(), check, client.InNamespace(tNS)); err != nil {
		t.Fatalf("list PVCs: %v", err)
	}
	want := resource.MustParse("16Gi")
	for _, pvc := range check.Items {
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(want) != 0 {
			t.Errorf("PVC %q size = %s, want 16Gi", pvc.Name, got.String())
		}
	}
}

func TestFromStsOrphaned_IdempotentWhenAlreadyAtDesired(t *testing.T) {
	// Re-entry: PVCs were patched on a prior reconcile but the substate
	// write failed and the operator is back at StsOrphaned. The patches
	// must be no-ops.
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		new(makePVC("data-cr-0", "16Gi", "8Gi")),
	).Build()
	got := FromStsOrphaned(context.Background(), c, target())
	if got.Action != ActionAdvance {
		t.Errorf("Action=%v want Advance", got.Action)
	}
}

func TestFromStsOrphaned_SkipsPVCsThatVanishMidPatch(t *testing.T) {
	// A PVC was listed but disappeared before the patch landed (manual
	// delete during the orphan window, recovery procedure mid-resize).
	// The transition should skip the missing PVC and continue, not
	// requeue. interceptor.Funcs lets us inject the NotFound on the
	// second-PVC patch while the first one succeeds.
	scheme := newScheme(t)
	pvcA := new(makePVC("data-cr-0", "8Gi", "8Gi"))
	pvcB := new(makePVC("data-cr-1", "8Gi", "8Gi"))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvcA, pvcB).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if obj.GetName() == "data-cr-1" {
					return apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), obj.GetName())
				}
				return client.Patch(ctx, obj, patch, opts...)
			},
		}).Build()

	got := FromStsOrphaned(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phasePVCsPatched {
		t.Fatalf("Action=%v NextPhase=%q want Advance/PVCsPatched (NotFound skipped)", got.Action, got.NextPhase)
	}
}

func TestFromPVCsPatched_AbortsWhenPVCsVanished(t *testing.T) {
	// Empty PVC list at PVCsPatched substate ⇒ PVCs vanished mid-resize
	// (manual delete, replicas scaled to 0). Aborting with PVCMissing
	// surfaces the stall as a Warning event instead of looping until
	// Phase C's stall guard fires.
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	got := FromPVCsPatched(context.Background(), c, target())
	if got.Action != ActionAbort {
		t.Fatalf("Action=%v want Abort", got.Action)
	}
	if got.EventReason != "PVCMissing" {
		t.Errorf("EventReason=%q want PVCMissing", got.EventReason)
	}
	if !got.EventWarning {
		t.Errorf("EventWarning=false want true (PVCMissing is a stall signal)")
	}
}

func TestFromPVCsPatched_HoldsUntilStatusCapacityCatchesUp(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		new(makePVC("data-cr-0", "16Gi", "16Gi")),
		new(makePVC("data-cr-1", "16Gi", "8Gi")), // CSI still expanding
	).Build()
	got := FromPVCsPatched(context.Background(), c, target())
	if got.Action != ActionRequeue {
		t.Errorf("Action=%v want Requeue (one PVC still expanding)", got.Action)
	}
}

func TestFromPVCsPatched_AdvancesWhenAllStatusCapacityAtDesired(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		new(makePVC("data-cr-0", "16Gi", "16Gi")),
		new(makePVC("data-cr-1", "16Gi", "16Gi")),
	).Build()
	got := FromPVCsPatched(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phaseStsRecreated {
		t.Errorf("Action=%v NextPhase=%q want Advance/StsRecreated", got.Action, got.NextPhase)
	}
}

func TestFromStsRecreated_HoldsWhenSTSAbsent(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	got := FromStsRecreated(context.Background(), c, target())
	if got.Action != ActionRequeue {
		t.Errorf("Action=%v want Requeue (STS not yet recreated)", got.Action)
	}
}

func TestFromStsRecreated_HoldsWhenVCTStillStaleSize(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(makeSTS(0, "8Gi")).Build()
	got := FromStsRecreated(context.Background(), c, target())
	if got.Action != ActionRequeue {
		t.Errorf("Action=%v want Requeue (VCT still at 8Gi)", got.Action)
	}
}

func TestFromStsRecreated_AdvancesWhenVCTAtDesired(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(makeSTS(0, "16Gi")).Build()
	got := FromStsRecreated(context.Background(), c, target())
	if got.Action != ActionAdvance || got.NextPhase != phaseVerified {
		t.Errorf("Action=%v NextPhase=%q want Advance/Verified", got.Action, got.NextPhase)
	}
}

func TestFromVerified_HoldsWhilePodsNotReady(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(makeSTS(1, "16Gi")).Build()
	got := FromVerified(context.Background(), c, target())
	if got.Action != ActionRequeue {
		t.Errorf("Action=%v want Requeue (1/3 ready)", got.Action)
	}
}

func TestFromVerified_CompletesWhenAllPodsReady(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(makeSTS(3, "16Gi")).Build()
	got := FromVerified(context.Background(), c, target())
	if got.Action != ActionComplete {
		t.Errorf("Action=%v want Complete", got.Action)
	}
	if got.EventReason != "PVCResizeComplete" {
		t.Errorf("EventReason=%q want PVCResizeComplete", got.EventReason)
	}
}

func TestFromVerified_TreatsZeroReplicasAsReady(t *testing.T) {
	// Edge case: spec.replicas nil or 0. ReadyReplicas (0) >= desired (0)
	// → Complete. This shouldn't happen in practice (no PVCs to resize on
	// a 0-replica STS) but the guard is part of the readiness contract.
	scheme := newScheme(t)
	sts := makeSTS(0, "16Gi")
	sts.Spec.Replicas = nil
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()
	got := FromVerified(context.Background(), c, target())
	if got.Action != ActionComplete {
		t.Errorf("Action=%v want Complete (replicas=nil treated as 0)", got.Action)
	}
}

// TestTransitions_FullSequenceWalk drives a single fake client through
// every substate the resize machine visits on the happy path:
// Validated → StsOrphaned → PVCsPatched → StsRecreated → Verified →
// Complete. The per-step tests above pin each transition's input/output
// contract in isolation; this walk pins that the contracts compose
// without an external orchestrator dropping a step. Between substates
// the test mirrors what the cluster does outside the resize loop:
// CSI updates PVC status.capacity, Phase 2 recreates the STS, kubelet
// flips ReadyReplicas. The transition functions themselves are not
// re-tested — only the post-condition that lets the next substate
// fire.
func TestTransitions_FullSequenceWalk(t *testing.T) {
	scheme := newScheme(t)
	tgt := target() // DesiredSize=16Gi, STSName=tCR=cr

	// Initial state: STS at 8Gi VCT, 3 PVCs at 8Gi spec/8Gi status.
	sts := makeSTS(3, "8Gi")
	pvcs := []client.Object{
		new(makePVC("data-cr-0", "8Gi", "8Gi")),
		new(makePVC("data-cr-1", "8Gi", "8Gi")),
		new(makePVC("data-cr-2", "8Gi", "8Gi")),
	}
	objs := append([]client.Object{sts}, pvcs...)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	ctx := context.Background()

	// failMsg renders the transition's Reason+Message into the test
	// failure line so a regression debugger sees the rejection cause
	// inline rather than having to re-read the production code.
	failMsg := func(step string, want string, r TransitionResult) string {
		return fmt.Sprintf("%s: Action=%v NextPhase=%q Reason=%q Message=%q, want %s",
			step, r.Action, r.NextPhase, r.Reason, r.Message, want)
	}

	// Step 1: Validated → StsOrphaned. STS is deleted with Orphan
	// propagation; PVCs survive at 8Gi spec.
	r := FromValidated(ctx, c, tgt)
	if r.Action != ActionAdvance || r.NextPhase != phaseStsOrphaned {
		t.Fatal(failMsg("step 1 Validated", "Advance/StsOrphaned", r))
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: tNS, Name: tCR}, &appsv1.StatefulSet{}); err == nil {
		t.Fatal("step 1 post: STS still present after FromValidated")
	}

	// Step 2: StsOrphaned → PVCsPatched. PVCs get patched to 16Gi spec
	// (status.capacity still at 8Gi until CSI catches up).
	r = FromStsOrphaned(ctx, c, tgt)
	if r.Action != ActionAdvance || r.NextPhase != phasePVCsPatched {
		t.Fatal(failMsg("step 2 StsOrphaned", "Advance/PVCsPatched", r))
	}
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(tNS)); err != nil {
		t.Fatalf("step 2 post: list PVCs: %v", err)
	}
	desired := resource.MustParse("16Gi")
	for _, pvc := range pvcList.Items {
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(desired) != 0 {
			t.Fatalf("step 2 post: PVC %q spec=%s, want 16Gi", pvc.Name, got.String())
		}
	}

	// Step 3a: PVCsPatched holds while CSI hasn't updated status.
	r = FromPVCsPatched(ctx, c, tgt)
	if r.Action != ActionRequeue {
		t.Fatal(failMsg("step 3a PVCsPatched (status still 8Gi)", "Requeue", r))
	}

	// Step 3b: CSI catches up — update each PVC's status.capacity to
	// 16Gi. Then the transition advances.
	for i := range pvcList.Items {
		p := &pvcList.Items[i]
		p.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("16Gi")}
		if err := c.Status().Update(ctx, p); err != nil {
			t.Fatalf("step 3b: bump status.capacity on %q: %v", p.Name, err)
		}
	}
	r = FromPVCsPatched(ctx, c, tgt)
	if r.Action != ActionAdvance || r.NextPhase != phaseStsRecreated {
		t.Fatal(failMsg("step 3b PVCsPatched (status 16Gi)", "Advance/StsRecreated", r))
	}

	// Step 4a: StsRecreated holds until Phase 2 recreates the STS.
	r = FromStsRecreated(ctx, c, tgt)
	if r.Action != ActionRequeue {
		t.Fatal(failMsg("step 4a StsRecreated (no STS yet)", "Requeue", r))
	}

	// Step 4b: Phase 2 recreates the STS at 16Gi VCT, ReadyReplicas=0
	// (pods still booting). FromStsRecreated advances because the VCT
	// matches; readiness is checked by FromVerified.
	if err := c.Create(ctx, makeSTS(0, "16Gi")); err != nil {
		t.Fatalf("step 4b: recreate STS at 16Gi: %v", err)
	}
	r = FromStsRecreated(ctx, c, tgt)
	if r.Action != ActionAdvance || r.NextPhase != phaseVerified {
		t.Fatal(failMsg("step 4b StsRecreated (STS at 16Gi, 0 ready)", "Advance/Verified", r))
	}

	// Step 5a: Verified holds while pods aren't ready.
	r = FromVerified(ctx, c, tgt)
	if r.Action != ActionRequeue {
		t.Fatal(failMsg("step 5a Verified (0/3 ready)", "Requeue", r))
	}

	// Step 5b: kubelet flips ReadyReplicas to 3 (all pods ready on
	// the resized VCT). FromVerified completes.
	stsCur := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: tNS, Name: tCR}, stsCur); err != nil {
		t.Fatalf("step 5b: re-read STS: %v", err)
	}
	stsCur.Status.ReadyReplicas = 3
	if err := c.Status().Update(ctx, stsCur); err != nil {
		t.Fatalf("step 5b: bump ReadyReplicas: %v", err)
	}
	r = FromVerified(ctx, c, tgt)
	if r.Action != ActionComplete {
		t.Fatal(failMsg("step 5b Verified (3/3 ready)", "Complete", r))
	}
	if r.EventReason != "PVCResizeComplete" {
		t.Errorf("step 5b: EventReason=%q want PVCResizeComplete", r.EventReason)
	}
}
