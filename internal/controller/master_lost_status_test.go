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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

const masterLostDownAfterMs = 1000

// masterLostClock is the frozen "now" all MasterLost tests read so the
// down-after + freshness boundaries are exercised at exact offsets
// rather than against a live wall clock.
var masterLostClock = time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)

func sentinelCRForMasterLost() *valkeyv1beta1.Valkey {
	cr := crForRotation() // sentinel mode (ns/vk)
	cr.Spec.Valkey.Replicas = 3
	cr.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
		MasterName:            "mymaster",
		DownAfterMilliseconds: masterLostDownAfterMs,
	}
	return cr
}

func masterLostReconciler(t *testing.T, cr *valkeyv1beta1.Valkey) *ValkeyReconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(authRotationScheme(t)).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
	return &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(16),
		nowFunc:  func() time.Time { return masterLostClock },
	}
}

func healthySTS3() *appsv1.StatefulSet {
	desired := int32(3)
	return &appsv1.StatefulSet{
		Spec:   appsv1.StatefulSetSpec{Replicas: &desired},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
}

// TestUpdateStatus_MasterLost_ForcesReadyFalseThenClears pins the
// gap #1 wiring end-to-end at the controller seam: once the labelled-
// primary INFO probe has been failing past the down-after window (and
// was observed recently), updateStatus forces Ready=False/MasterLost
// even with a fully-Ready STS, then clears it the moment the probe
// recovers — with no operator-driven failover.
func TestUpdateStatus_MasterLost_ForcesReadyFalseThenClears(t *testing.T) {
	cr := sentinelCRForMasterLost()
	r := masterLostReconciler(t, cr)
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cr)
	st := r.stateFor(key).quorumTracker()

	// Probe failing since 5s ago (>1s down-after), last observed now.
	st.mu.Lock()
	st.observeMasterInfoTimeout(false, masterLostClock.Add(-5*time.Second))
	st.observeMasterInfoTimeout(false, masterLostClock) // latest probe: still failing, observed now
	st.mu.Unlock()
	if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
		orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
		t.Fatalf("updateStatus (master lost): %v", err)
	}
	if got := readyCondition(t, ctx, r.Client, key); got.Status != metav1.ConditionFalse || got.Reason != orchestration.ReasonMasterLost {
		t.Fatalf("Ready = %s/%s; want False/%s (MasterLost must override a healthy STS)",
			got.Status, got.Reason, orchestration.ReasonMasterLost)
	}

	// Probe recovers → latch cleared → Ready True.
	st.mu.Lock()
	st.observeMasterInfoTimeout(true, masterLostClock)
	st.mu.Unlock()
	if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
		orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
		t.Fatalf("updateStatus (recovered): %v", err)
	}
	if got := readyCondition(t, ctx, r.Client, key); got.Status != metav1.ConditionTrue || got.Reason != orchestration.ReasonReady {
		t.Fatalf("after recovery Ready = %s/%s; want True/%s (MasterLost must clear automatically)",
			got.Status, got.Reason, orchestration.ReasonReady)
	}
}

// TestUpdateStatus_MasterLost_SubThresholdNoFlip pins the hysteresis:
// a fresh (sub-down-after) INFO-probe failure must NOT flip Ready —
// otherwise a single slow probe on a live-but-busy master flaps the CR.
func TestUpdateStatus_MasterLost_SubThresholdNoFlip(t *testing.T) {
	cr := sentinelCRForMasterLost()
	r := masterLostReconciler(t, cr)
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cr)
	st := r.stateFor(key).quorumTracker()

	// First failed probe 500ms ago — within the 1s down-after window.
	st.mu.Lock()
	st.observeMasterInfoTimeout(false, masterLostClock.Add(-500*time.Millisecond))
	st.mu.Unlock()
	if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
		orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	if got := readyCondition(t, ctx, r.Client, key); got.Reason == orchestration.ReasonMasterLost {
		t.Fatalf("Ready.Reason = %s; a sub-down-after probe failure must NOT trip MasterLost", got.Reason)
	}
}

// TestUpdateStatus_MasterLost_StaleLatchNotHonored pins finding #6:
// the latch satisfies the hysteresis (failing well past down-after) but
// the last probe is older than the freshness window — i.e. recent
// reconciles early-returned before the probe ran. updateStatus must NOT
// pin Ready=False off the stale latch (the master may have recovered).
func TestUpdateStatus_MasterLost_StaleLatchNotHonored(t *testing.T) {
	cr := sentinelCRForMasterLost()
	r := masterLostReconciler(t, cr)
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cr)
	st := r.stateFor(key).quorumTracker()

	since := masterLostClock.Add(-2 * time.Second) // hysteresis satisfied (2s > 1s)
	st.mu.Lock()
	st.masterInfoTimeoutSince = &since
	// Last probe ran beyond the freshness window — no recent measurement.
	st.masterInfoObservedAt = masterLostClock.Add(-2 * masterInfoProbeFreshnessWindow)
	st.mu.Unlock()
	if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
		orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	if got := readyCondition(t, ctx, r.Client, key); got.Reason == orchestration.ReasonMasterLost {
		t.Fatalf("Ready.Reason = %s; a stale latch (no recent probe) must NOT pin MasterLost", got.Reason)
	}
}

// TestMasterLostFromTimeout pins the pure hysteresis + freshness predicate.
func TestMasterLostFromTimeout(t *testing.T) {
	now := masterLostClock
	old := now.Add(-2 * time.Second)
	recent := now.Add(-500 * time.Millisecond)
	freshObs := now.Add(-time.Second)
	staleObs := now.Add(-2 * masterInfoProbeFreshnessWindow)

	if masterLostFromTimeout(nil, freshObs, 1000, now) {
		t.Errorf("nil since must be not-lost")
	}
	if masterLostFromTimeout(&old, freshObs, 0, now) {
		t.Errorf("non-positive down-after must be not-lost")
	}
	if masterLostFromTimeout(&recent, freshObs, 1000, now) {
		t.Errorf("failure younger than down-after (500ms < 1000ms) must be not-lost")
	}
	if masterLostFromTimeout(&old, staleObs, 1000, now) {
		t.Errorf("stale observation (probe ran long ago) must be not-lost despite hysteresis")
	}
	if masterLostFromTimeout(&old, time.Time{}, 1000, now) {
		t.Errorf("zero observation time must be not-lost")
	}
	if !masterLostFromTimeout(&old, freshObs, 1000, now) {
		t.Errorf("old failure observed recently must be lost")
	}
}

// TestSentinelKeepAliveRequeue pins the keep-alive requeue gate: it
// fires for every sentinel-mode CR (not gated on SQ records) so the
// re-probe cadence stays bounded even on a dead-primary CR with zero
// SQ records (finding #13), and never exceeds the freshness window.
func TestSentinelKeepAliveRequeue(t *testing.T) {
	if got := sentinelKeepAliveRequeue(false); got != 0 {
		t.Errorf("non-sentinel must get no keep-alive requeue, got %v", got)
	}
	got := sentinelKeepAliveRequeue(true)
	if got != sqKeepAliveInterval {
		t.Errorf("sentinel must requeue at sqKeepAliveInterval, got %v", got)
	}
	if got > sentinelQuorumFreshnessWindow {
		t.Errorf("keep-alive requeue %v must not exceed the freshness window %v", got, sentinelQuorumFreshnessWindow)
	}
}

// TestStatusRequeueHint pins the steady-state requeue HELPER only: a
// quiet sentinel CR with a Ready STS yields the keep-alive hint, a
// standalone CR yields none, a not-yet-Ready STS yields the tighter
// ready-converge poll, and a paused CR yields none (relaxed cadence
// preserved). NOTE: this exercises only statusRequeueHint. The
// merge-into-Result mutation (the result.RequeueAfter merge in
// applyStatusRequeue) is pinned by TestApplyStatusRequeue — the test
// that actually fails when the merge is dropped. The through-Reconcile
// envtest only smoke-tests the wired path (requeue positive and <=
// baselineReconcileWatchdog); it cannot isolate the keep-alive value.
func TestStatusRequeueHint(t *testing.T) {
	r := &ValkeyReconciler{}
	sentinel := sentinelCRForMasterLost()
	standalone := crForRotation()
	standalone.Spec.Mode = valkeyv1beta1.ModeStandalone

	// Steady-state (reachedSteadyState=true) sentinel + Ready STS → keep-alive.
	if got := r.statusRequeueHint(sentinel, healthySTS3(), false, true); got != sqKeepAliveInterval {
		t.Fatalf("steady sentinel + Ready STS hint = %v; want %v (keep-alive)", got, sqKeepAliveInterval)
	}
	if got := r.statusRequeueHint(sentinel, healthySTS3(), false, true); got > sentinelQuorumFreshnessWindow {
		t.Fatalf("steady-state hint %v exceeds freshness window %v", got, sentinelQuorumFreshnessWindow)
	}
	if got := r.statusRequeueHint(standalone, healthySTS3(), false, true); got != 0 {
		t.Errorf("standalone hint = %v; want 0", got)
	}
	// Not-yet-Ready STS (steady) → the tighter ready-converge poll wins.
	notReady := healthySTS3()
	notReady.Status.ReadyReplicas = 0
	if got := r.statusRequeueHint(sentinel, notReady, false, true); got != readyConvergeRequeue {
		t.Errorf("not-ready sentinel STS hint = %v; want %v (ready-converge)", got, readyConvergeRequeue)
	}
	// Paused sentinel CR → no contribution (relaxed cadence preserved).
	if got := r.statusRequeueHint(sentinel, healthySTS3(), true, false); got != 0 {
		t.Errorf("paused sentinel hint = %v; want 0 (relaxed cadence preserved)", got)
	}
	// Short-circuit pass (reachedSteadyState=false, !paused) → NO keep-alive
	// even with a Ready STS: the consumers never ran, so tightening a
	// relaxed backoff would be do-nothing churn (#19).
	if got := r.statusRequeueHint(sentinel, healthySTS3(), false, false); got != 0 {
		t.Errorf("short-circuited sentinel hint = %v; want 0 (no keep-alive churn)", got)
	}
}

// TestApplyStatusRequeue pins the load-bearing merge of the keep-alive
// into the reconcile Result (finding #10): seeded at the steady-state
// baseline, a quiet sentinel CR with a Ready STS is tightened to the
// keep-alive interval, while a standalone CR is left untouched. This is
// the assertion that fails if the merge in applyStatusRequeue is dropped
// — the check the through-Reconcile envtest cannot make (the bootstrap
// body requeue dominates the keep-alive in envtest).
func TestApplyStatusRequeue(t *testing.T) {
	r := &ValkeyReconciler{}
	sentinel := sentinelCRForMasterLost()
	standalone := crForRotation()
	standalone.Spec.Mode = valkeyv1beta1.ModeStandalone

	// Steady-state sentinel + Ready STS → keep-alive tightens the baseline.
	res := ctrl.Result{RequeueAfter: baselineReconcileWatchdog}
	r.applyStatusRequeue(&res, sentinel, healthySTS3(), false, true)
	if res.RequeueAfter != sqKeepAliveInterval {
		t.Fatalf("sentinel keep-alive must tighten the %v baseline to %v; got %v (merge dropped?)",
			baselineReconcileWatchdog, sqKeepAliveInterval, res.RequeueAfter)
	}

	res = ctrl.Result{RequeueAfter: baselineReconcileWatchdog}
	r.applyStatusRequeue(&res, standalone, healthySTS3(), false, true)
	if res.RequeueAfter != baselineReconcileWatchdog {
		t.Errorf("standalone must not tighten the baseline requeue; got %v", res.RequeueAfter)
	}

	// Paused sentinel CR → the relaxed baseline is preserved (#15).
	res = ctrl.Result{RequeueAfter: baselineReconcileWatchdog}
	r.applyStatusRequeue(&res, sentinel, healthySTS3(), true, false)
	if res.RequeueAfter != baselineReconcileWatchdog {
		t.Errorf("paused sentinel must keep the relaxed cadence; got %v", res.RequeueAfter)
	}

	// Short-circuit pass (reachedSteadyState=false, !paused): a deliberately
	// relaxed backoff (e.g. auth-missing 5m) must NOT be tightened — #19.
	res = ctrl.Result{RequeueAfter: 5 * time.Minute}
	r.applyStatusRequeue(&res, sentinel, healthySTS3(), false, false)
	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("short-circuited sentinel must keep its relaxed backoff; got %v (#19 churn)", res.RequeueAfter)
	}
}

// TestObserveMasterInfoTimeout_ClearsLatchWhenNoPrimary pins finding
// #3: a label-stripped window (no pod labelled role=primary) clears
// masterInfoTimeoutSince, so MasterLost cannot latch through an
// operator-driven rollout.
func TestObserveMasterInfoTimeout_ClearsLatchWhenNoPrimary(t *testing.T) {
	cr := sentinelCRForMasterLost()
	r := masterLostReconciler(t, cr)
	ctx := context.Background()
	key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	st := r.stateFor(key).quorumTracker()

	// A prior episode stamped the latch.
	st.mu.Lock()
	st.observeMasterInfoTimeout(false, masterLostClock.Add(-5*time.Second))
	st.mu.Unlock()

	// No pods → no labelled primary → the early-return path must clear it.
	r.observeMasterInfoTimeout(ctx, key, "", nil)

	st.mu.Lock()
	since := st.masterInfoTimeoutSince
	st.mu.Unlock()
	if since != nil {
		t.Fatalf("masterInfoTimeoutSince = %v; want nil (label-stripped window must clear the latch)", since)
	}
}

func readyCondition(t *testing.T, ctx context.Context, c client.Client, key client.ObjectKey) metav1.Condition {
	t.Helper()
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get cr: %v", err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, orchestration.TypeReady)
	if cond == nil {
		t.Fatalf("Ready condition not found on %s", key)
	}
	return *cond
}
