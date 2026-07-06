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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sqaggregate"
	"github.com/ioxie/velkir/internal/valkey"
)

// activeAt binds dualMasterActiveFromStamp to a fixed clock — the
// predicate updateStatus injects into dualMasterActiveOrExpire.
func activeAt(now time.Time) func(*dualMasterObservation) bool {
	return func(obs *dualMasterObservation) bool { return dualMasterActiveFromStamp(obs, now) }
}

// TestDualMasterActiveFromStamp pins the pure freshness predicate.
func TestDualMasterActiveFromStamp(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		obs  *dualMasterObservation
		want bool
	}{
		{"nil observation", nil, false},
		{"single pod never active", &dualMasterObservation{observedAt: now, pods: []string{"vk0-0"}}, false},
		{"fresh two-pod stamp active", &dualMasterObservation{observedAt: now.Add(-time.Second), pods: []string{"vk0-0", "vk0-1"}}, true},
		{"exactly at window still active", &dualMasterObservation{observedAt: now.Add(-dualMasterObservedFreshnessWindow), pods: []string{"vk0-0", "vk0-1"}}, true},
		{"past window ages out", &dualMasterObservation{observedAt: now.Add(-dualMasterObservedFreshnessWindow - time.Second), pods: []string{"vk0-0", "vk0-1"}}, false},
	}
	for _, tc := range cases {
		if got := dualMasterActiveFromStamp(tc.obs, now); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDualMasterActiveOrExpire pins the reader used by updateStatus: a
// fresh stamp reads active without mutation; a stale stamp reads
// inactive AND is dropped with the event edge re-armed so the next
// same-pod-set episode fires again.
func TestDualMasterActiveOrExpire(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Fresh: active, stamp preserved.
	s := &perCRState{}
	s.stampDualMasterObserved([]string{"vk0-0", "vk0-1"}, now.Add(-time.Second))
	s.fireDualMasterObservedEdge("vk0-0,vk0-1")
	if !s.dualMasterActiveOrExpire(activeAt(now)) {
		t.Fatalf("fresh stamp read inactive")
	}
	if s.dualMasterObservation() == nil {
		t.Errorf("fresh stamp must be preserved")
	}

	// Stale: inactive, stamp dropped, edge re-armed.
	s.stampDualMasterObserved([]string{"vk0-0", "vk0-1"}, now.Add(-dualMasterObservedFreshnessWindow-time.Second))
	if s.dualMasterActiveOrExpire(activeAt(now)) {
		t.Fatalf("stale stamp read active")
	}
	if s.dualMasterObservation() != nil {
		t.Errorf("stale stamp must be dropped")
	}
	if !s.fireDualMasterObservedEdge("vk0-0,vk0-1") {
		t.Errorf("edge must be re-armed after expiry so a fresh episode fires")
	}
}

// TestSetDualMasterGauge pins the single-writer gauge derivation.
func TestSetDualMasterGauge(t *testing.T) {
	operatormetrics.Register()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "gauge0"}
	g := func() float64 {
		return testutil.ToFloat64(operatormetrics.DualMasterObserved.WithLabelValues(cr.Namespace, cr.Name))
	}
	r.setDualMasterGauge(cr, true)
	if g() != 1 {
		t.Errorf("gauge = %v, want 1", g())
	}
	r.setDualMasterGauge(cr, false)
	if g() != 0 {
		t.Errorf("gauge = %v, want 0", g())
	}
}

// TestDualMasterStampAndEdge pins the stamp/edge/clear lifecycle.
func TestDualMasterStampAndEdge(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	now := time.Now()

	s.stampDualMasterObserved([]string{"vk0-1", "vk0-0"}, now)
	obs := s.dualMasterObservation()
	if obs == nil || len(obs.pods) != 2 || obs.pods[0] != "vk0-0" {
		t.Fatalf("stamp not recorded/sorted: %+v", obs)
	}
	if !s.fireDualMasterObservedEdge("vk0-0,vk0-1") {
		t.Errorf("first edge fire for a new signature must return true")
	}
	if s.fireDualMasterObservedEdge("vk0-0,vk0-1") {
		t.Errorf("same-signature re-fire must return false")
	}
	if !s.fireDualMasterObservedEdge("vk0-0,vk0-2") {
		t.Errorf("changed pod set must re-fire")
	}

	s.clearDualMasterObserved()
	if s.dualMasterObservation() != nil {
		t.Errorf("clear must remove the stamp")
	}
	if !s.fireDualMasterObservedEdge("vk0-0,vk0-2") {
		t.Errorf("edge must re-arm after clear")
	}
}

// TestFoldDualMasterEventUnion_AccumulatesAcrossScans pins that
// consecutive scans accumulate the de-facto-master union (so a role
// permutation keys to the growing union, not the current set),
// independent of the wall-clock gap between them.
func TestFoldDualMasterEventUnion_AccumulatesAcrossScans(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	if got, want := s.foldDualMasterEventUnion([]string{dmPod0, dmPod1}), strings.Join([]string{dmPod0, dmPod1}, ","); got != want {
		t.Fatalf("first key = %q, want %q", got, want)
	}
	if got, want := s.foldDualMasterEventUnion([]string{dmPod1, dmPod2}), strings.Join([]string{dmPod0, dmPod1, dmPod2}, ","); got != want {
		t.Fatalf("accumulated key = %q, want %q", got, want)
	}
}

// TestFoldDualMasterEventUnion_PersistsAcrossSlowScansNoRefire pins the
// core #700 fix: the event episode is time-independent. Two scans of the
// SAME de-facto-master set — however far apart in wall-clock — key to the
// same union and do NOT re-fire, and a slow scan does NOT re-arm the edge.
// A persistent replication split whose scans land slower than the
// freshness window pages exactly once, not once per slow scan.
func TestFoldDualMasterEventUnion_PersistsAcrossSlowScansNoRefire(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	if !s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})) {
		t.Fatalf("first scan of a split must fire")
	}
	// A later same-set scan (any wall-clock gap): the union is unchanged,
	// so the edge is not re-armed and the event does not re-fire.
	if s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})) {
		t.Fatalf("a same-set scan after any gap must NOT re-fire (episode is time-independent)")
	}
	if got, want := strings.Join(s.dualMasterEventUnion, ","), strings.Join([]string{dmPod0, dmPod1}, ","); got != want {
		t.Errorf("union = %q, want %q (persisted, not reset by the gap)", got, want)
	}
	if s.dualMasterObservedEdge == "" {
		t.Errorf("a same-set slow scan must NOT re-arm the edge")
	}
}

// TestDualMasterObservedEdge_ChurnKeyedOnUnionNoRefire drives the
// recovery-survey producer through the churn sequence {a,b}→{a,c}→{b,c}
// and asserts exactly two DualMasterObserved events: fire on {a,b}, fire
// when c joins, NO fire on {b,c} (no genuinely-new pod). A 2-set test
// couldn't distinguish the union key from a last-set key; the 3-set
// sequence does.
func TestDualMasterObservedEdge_ChurnKeyedOnUnionNoRefire(t *testing.T) {
	operatormetrics.Register()
	rec := k8sevents.NewFakeRecorder(32)
	r := &ValkeyReconciler{Recorder: rec}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	survey := func(m ...surveyedPod) *valkeyPodSurvey {
		return &valkeyPodSurvey{dialed: true, masters: m}
	}

	r.observeDualMasterFromSurvey(v, cr, survey(surveyMaster(dmPod0, dmIPLoser, 1), surveyMaster(dmPod1, dmIPSurvivor, 2)))
	r.observeDualMasterFromSurvey(v, cr, survey(surveyMaster(dmPod0, dmIPLoser, 3), surveyMaster(dmPod2, dmIPPod2, 4)))
	r.observeDualMasterFromSurvey(v, cr, survey(surveyMaster(dmPod1, dmIPSurvivor, 5), surveyMaster(dmPod2, dmIPPod2, 6)))

	observed := 0
	for _, e := range drainAllEvents(rec.Events) {
		if strings.Contains(e, "DualMasterObserved") {
			observed++
		}
	}
	if observed != 2 {
		t.Fatalf("union-keyed edge must fire twice ({a,b}, then when c joins) and NOT on {b,c}; got %d", observed)
	}
}

// TestDualMasterObservedEdge_NewEpisodeAfterClearRefires pins that a
// clean clear drops the event union and re-arms the edge, so the same
// pod set fires a fresh event afterward.
func TestDualMasterObservedEdge_NewEpisodeAfterClearRefires(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	if !s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})) {
		t.Fatalf("first fire must return true")
	}
	s.clearDualMasterObserved()
	if s.dualMasterEventUnion != nil {
		t.Errorf("clear must drop the event union")
	}
	if !s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})) {
		t.Errorf("after a clean clear the same pod set must re-fire (new episode)")
	}
}

// TestDualMasterObservedEdge_ExpiryReArmsThenRefires pins that an
// age-out drop (via dualMasterActiveOrExpire) nils the event union and
// re-arms the edge, so a later same-pod-set scan fires again.
func TestDualMasterObservedEdge_ExpiryReArmsThenRefires(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.stampDualMasterObserved([]string{dmPod0, dmPod1}, now)
	s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1}))

	past := now.Add(dualMasterObservedFreshnessWindow + time.Second)
	if s.dualMasterActiveOrExpire(activeAt(past)) {
		t.Fatalf("a stamp past the window must read inactive")
	}
	if s.dualMasterEventUnion != nil {
		t.Errorf("age-out must drop the event union")
	}
	if !s.fireDualMasterObservedEdge(s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})) {
		t.Errorf("after age-out the same pod set must re-fire")
	}
}

// TestDualMasterEventUnion_ClearedByPruneStale pins the prune-sweep
// teardown: the event union is dropped with the rest of the dual-master
// edge state.
func TestDualMasterEventUnion_ClearedByPruneStale(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	s.foldDualMasterEventUnion([]string{dmPod0, dmPod1})
	if s.dualMasterEventUnion == nil {
		t.Fatalf("precondition: the union must be accumulated")
	}
	s.pruneStale()
	if s.dualMasterEventUnion != nil {
		t.Errorf("pruneStale must nil the event union")
	}
}

func surveyMaster(name, ip string, offset int64) surveyedPod {
	return surveyedPod{name: name, ip: ip, state: valkey.LagState{
		Role: valkey.RoleMaster, MasterReplOffset: offset, HaveMasterOffset: true,
	}}
}

// TestUpdateStatus_DualMaster_WiresConditionsAndGauge pins the
// production wiring end-to-end at the controller seam: a fresh
// dual-master stamp drives Ready=False + Degraded=True (both
// DualMasterDivergence) AND the valkey_dual_master_observed gauge to 1
// through a real updateStatus call; once the stamp ages out, the same
// read expires it and all three return to their cleared state. A
// refactor that drops or reorders the updateStatus dual-master block
// (the exact regression a producer-side-only test cannot catch) fails
// here.
func TestUpdateStatus_DualMaster_WiresConditionsAndGauge(t *testing.T) {
	operatormetrics.Register()
	clk := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	cr := sentinelCRForMasterLost()
	c := fake.NewClientBuilder().WithScheme(authRotationScheme(t)).
		WithObjects(cr).WithStatusSubresource(cr).Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(16),
		nowFunc:  func() time.Time { return clk },
	}
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cr)
	gauge := func() float64 {
		return testutil.ToFloat64(operatormetrics.DualMasterObserved.WithLabelValues(key.Namespace, key.Name))
	}
	degraded := func() metav1.Condition {
		got := &valkeyv1beta1.Valkey{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatalf("get cr: %v", err)
		}
		cond := meta.FindStatusCondition(got.Status.Conditions, orchestration.TypeDegraded)
		if cond == nil {
			t.Fatalf("Degraded condition not found")
		}
		return *cond
	}
	callUpdate := func() {
		if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
			orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
	}

	// Fresh stamp → conditions forced + gauge 1.
	r.stateFor(key).stampDualMasterObserved([]string{"vk-0", "vk-1"}, clk.Add(-time.Second))
	callUpdate()
	if got := readyCondition(t, ctx, r.Client, key); got.Status != metav1.ConditionFalse || got.Reason != orchestration.ReasonDualMasterDivergence {
		t.Fatalf("Ready = %s/%s; want False/%s", got.Status, got.Reason, orchestration.ReasonDualMasterDivergence)
	}
	if got := degraded(); got.Status != metav1.ConditionTrue || got.Reason != orchestration.ReasonDualMasterDivergence {
		t.Fatalf("Degraded = %s/%s; want True/%s", got.Status, got.Reason, orchestration.ReasonDualMasterDivergence)
	}
	if gauge() != 1 {
		t.Fatalf("gauge = %v, want 1", gauge())
	}

	// Advance past the freshness window: the same read expires the
	// stamp; conditions clear and the gauge returns to 0.
	clk = clk.Add(dualMasterObservedFreshnessWindow + time.Second)
	callUpdate()
	if got := readyCondition(t, ctx, r.Client, key); got.Reason == orchestration.ReasonDualMasterDivergence {
		t.Errorf("Ready still DualMasterDivergence after the stamp aged out")
	}
	if got := degraded(); got.Reason == orchestration.ReasonDualMasterDivergence {
		t.Errorf("Degraded still DualMasterDivergence after the stamp aged out")
	}
	if gauge() != 0 {
		t.Errorf("gauge = %v, want 0 after expiry", gauge())
	}
	if r.stateFor(key).dualMasterObservation() != nil {
		t.Errorf("expired stamp must be dropped by the updateStatus read")
	}
}

// TestObserveDualMasterFromSurvey_StampsAndEvents pins the Phase 11
// producer's positive path: >=2 masters stamps + fires one edge-gated
// Warning with pod names and offsets; a repeat of the same set does not
// re-fire.
func TestObserveDualMasterFromSurvey_StampsAndEvents(t *testing.T) {
	operatormetrics.Register()
	rec := k8sevents.NewFakeRecorder(32)
	r := &ValkeyReconciler{Recorder: rec}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	// Collect the DualMasterObserved event bodies so the assertion can
	// pin the payload (pod names + offsets) the runbook depends on, not
	// just that the reason fired.
	observedBodies := func() []string {
		var out []string
		for _, e := range drainAllEvents(rec.Events) {
			if strings.Contains(e, "DualMasterObserved") {
				out = append(out, e)
			}
		}
		return out
	}

	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{dialed: true, masters: []surveyedPod{
		surveyMaster(dmPod1, dmIPSurvivor, 2048),
		surveyMaster(dmPod0, dmIPLoser, 1024),
	}})
	if got := r.stateFor(cr).dualMasterObservation(); got == nil || len(got.pods) != 2 {
		t.Fatalf("expected two-pod stamp, got %+v", got)
	}
	bodies := observedBodies()
	if len(bodies) != 1 {
		t.Fatalf("DualMasterObserved events = %d, want 1", len(bodies))
	}
	// The message must name both de-facto masters AND their offsets —
	// the survivor-selection payload the ValkeyDualMasterObserved
	// runbook and the DualMasterObserved catalog entry tell operators
	// to read. A refactor dropping `details` must fail here.
	for _, want := range []string{dmPod0, dmPod1, "master_repl_offset=2048", "master_repl_offset=1024"} {
		if !strings.Contains(bodies[0], want) {
			t.Errorf("DualMasterObserved event missing %q; got %q", want, bodies[0])
		}
	}

	// Same pod set re-observed → no second event.
	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{dialed: true, masters: []surveyedPod{
		surveyMaster(dmPod0, dmIPLoser, 4096),
		surveyMaster(dmPod1, dmIPSurvivor, 8192),
	}})
	if got := observedBodies(); len(got) != 0 {
		t.Errorf("same-set re-observation emitted %d events, want 0", len(got))
	}
}

// TestObserveDualMasterFromSurvey_ClearOnlyOnCompleteSweep pins the
// no-verdict-on-incomplete-coverage rule: a sweep seeing <2 masters
// clears only when it dialed every pod cleanly. A partial sweep (dial
// failure or a pending pod) leaves the stamp to age out, so an
// intermittent dial to a real rogue master can't flap the condition
// off and defeat the alert's for-window.
func TestObserveDualMasterFromSurvey_ClearOnlyOnCompleteSweep(t *testing.T) {
	operatormetrics.Register()
	r := &ValkeyReconciler{Recorder: k8sevents.NewFakeRecorder(8)}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Nil / undialed surveys never carry a verdict.
	r.stateFor(cr).stampDualMasterObserved([]string{dmPod0, dmPod1}, time.Now())
	r.observeDualMasterFromSurvey(v, cr, nil)
	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{dialed: false})
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Fatalf("no-verdict surveys must not clear the stamp")
	}

	// Incomplete sweep (a dial failed) seeing 1 master → no clear.
	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{
		dialed: true, dialFailures: 1,
		masters: []surveyedPod{surveyMaster(dmPod1, dmIPSurvivor, 1)},
	})
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("dial-failure sweep must not clear the stamp")
	}

	// Incomplete sweep (a pod still pending) seeing 0 masters → no clear.
	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{dialed: true, pendingPods: 1})
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("pending-pod sweep must not clear the stamp")
	}

	// Complete, clean sweep seeing 1 master → clear.
	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{
		dialed:  true,
		masters: []surveyedPod{surveyMaster(dmPod1, dmIPSurvivor, 1)},
	})
	if r.stateFor(cr).dualMasterObservation() != nil {
		t.Errorf("complete clean single-master sweep must clear the stamp")
	}
}

// TestObserveDualMasterFromSurvey_NoEventInsideFailover pins that the
// Warning fires only outside a failover section: inside one the bounded
// self-heal's events own the messaging and the "no failover in flight"
// framing would be false — but the stamp is still recorded so the
// condition surfaces the divergence.
func TestObserveDualMasterFromSurvey_NoEventInsideFailover(t *testing.T) {
	operatormetrics.Register()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{Recorder: rec}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	r.failoverLatchSet(cr, "10.9.9.9:6379") // open a failover section

	r.observeDualMasterFromSurvey(v, cr, &valkeyPodSurvey{dialed: true, masters: []surveyedPod{
		surveyMaster(dmPod0, dmIPLoser, 1),
		surveyMaster(dmPod1, dmIPSurvivor, 2),
	}})
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("stamp must be recorded even inside a failover section")
	}
	for _, e := range drainAllEvents(rec.Events) {
		if strings.Contains(e, "DualMasterObserved") {
			t.Errorf("DualMasterObserved event must not fire inside a failover section; got %q", e)
		}
	}
}

// TestSelfHealScanStampsDualMasterObservation pins the Phase 7a
// self-heal producer: the scan stamps before acting (the demotion may
// fail/defer — the condition must not depend on the heal succeeding), a
// clean complete scan clears, and an incomplete scan (a dial failed)
// does not clear.
func TestSelfHealScanStampsDualMasterObservation(t *testing.T) {
	operatormetrics.Register()
	checker := twoMasterChecker(1000, 100000) // unambiguous survivor
	issuer := newFakeReplicaOfIssuer()
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")
	if obs := r.stateFor(cr).dualMasterObservation(); obs == nil || len(obs.pods) != 2 {
		t.Fatalf("self-heal scan did not stamp the two-master observation: %+v", obs)
	}

	// Incomplete scan: the loser's INFO now errors (unreachable). The
	// scan sees 1 master but coverage is incomplete → no clear.
	checker.errAddr[dmAddrLoser] = context.DeadlineExceeded
	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("incomplete self-heal scan (dial failure) must not clear the stamp")
	}

	// Clean complete scan: loser reachable as slave now → clear.
	delete(checker.errAddr, dmAddrLoser)
	checker.byAddr[dmAddrLoser] = valkey.LagState{Role: "slave", LinkUp: true}
	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")
	if got := r.stateFor(cr).dualMasterObservation(); got != nil {
		t.Errorf("clean complete self-heal scan must clear the stamp, got %+v", got)
	}
}

// TestOrphanScanStampsDualMasterObservation pins the labeled-primary
// producer: an elected primary plus a rogue self-reported master stamp
// the observation even though the demotion is attempted the same pass;
// a clean complete post-demotion scan clears; and a scan with an
// unevaluated pod (unlabeled, running as master under relabel
// suppression) does not clear.
func TestOrphanScanStampsDualMasterObservation(t *testing.T) {
	operatormetrics.Register()
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrLoser] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 5000, HaveMasterOffset: true}
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 4000, HaveMasterOffset: true}
	issuer := newFakeReplicaOfIssuer()
	// No client needed: reconcileOrphanMasters consumes the threaded pod
	// slice directly and reaches the labeled-primary path via LagChecker.
	r := &ValkeyReconciler{
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        k8sevents.NewFakeRecorder(16),
	}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	pod := func(name, ip, role string) corev1.Pod { return *labelledValkeyPod(name, ip, role) }

	primaryPlusRogue := []corev1.Pod{
		pod(dmPod0, dmIPLoser, roleValuePrimary),
		pod(dmPod1, dmIPSurvivor, roleValueReplica),
	}
	r.reconcileOrphanMasters(context.Background(), v, primaryPlusRogue, "")
	if obs := r.stateFor(cr).dualMasterObservation(); obs == nil || len(obs.pods) != 2 {
		t.Fatalf("orphan scan did not stamp the primary+rogue observation: %+v", obs)
	}
	if len(issuer.recorded()) == 0 {
		t.Fatalf("orphan demotion should still have been attempted")
	}

	// Rogue demoted (slave), but an unlabeled pod with an IP is present
	// (Phase 7 relabel suppressed): the scan sees 1 labeled master and
	// never dials the unevaluated pod, which could itself be a master →
	// no clear.
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: "slave", LinkUp: true}
	withUnlabeled := []corev1.Pod{
		pod(dmPod0, dmIPLoser, roleValuePrimary),
		pod(dmPod1, dmIPSurvivor, roleValueReplica),
		pod("vk0-2", "10.0.5.7", ""), // unlabeled, has IP, never dialed
	}
	r.reconcileOrphanMasters(context.Background(), v, withUnlabeled, "")
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("scan with an unevaluated pod must not clear the stamp")
	}

	// Complete clean scan (no unevaluated pods, rogue is slave) → clear.
	clean := []corev1.Pod{
		pod(dmPod0, dmIPLoser, roleValuePrimary),
		pod(dmPod1, dmIPSurvivor, roleValueReplica),
	}
	r.reconcileOrphanMasters(context.Background(), v, clean, "")
	if got := r.stateFor(cr).dualMasterObservation(); got != nil {
		t.Errorf("complete clean orphan scan must clear the stamp, got %+v", got)
	}
}
