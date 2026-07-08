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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

const topoMismatchReason = "SentinelTopologyMismatch"

func topoBaseTime() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// --- pure fold tests (topologyDimState) -------------------------------

// TestFoldTopologyDim_DebounceAccrual pins the debounce + one-event-per-
// episode edge: below-threshold eligible+deficit passes stay inactive;
// crossing the debounce activates and fires exactly once; further
// eligible passes stay active without re-firing.
func TestFoldTopologyDim_DebounceAccrual(t *testing.T) {
	t.Parallel()
	d := &topologyDimState{}
	t0 := topoBaseTime()
	db := topologyMismatchDebounce

	if a, f := d.fold(1, true, t0, db); a || f {
		t.Fatalf("first pass: active=%v fire=%v; want false false", a, f)
	}
	if a, f := d.fold(1, true, t0.Add(db/2), db); a || f {
		t.Fatalf("below-threshold pass: active=%v fire=%v; want false false", a, f)
	}
	if a, f := d.fold(1, true, t0.Add(db), db); !a || !f {
		t.Fatalf("threshold pass: active=%v fire=%v; want true true", a, f)
	}
	if a, f := d.fold(1, true, t0.Add(db+10*time.Second), db); !a || f {
		t.Fatalf("post-fire pass: active=%v fire=%v; want true false", a, f)
	}
}

// TestFoldTopologyDim_PruneOnRecovery pins prune-on-recovery: an eligible
// zero-deficit pass clears the episode and re-arms the edge, so a later
// real deficit accrues and fires again.
func TestFoldTopologyDim_PruneOnRecovery(t *testing.T) {
	t.Parallel()
	d := &topologyDimState{}
	t0 := topoBaseTime()
	db := topologyMismatchDebounce

	d.fold(1, true, t0, db)
	if a, f := d.fold(1, true, t0.Add(db), db); !a || !f {
		t.Fatalf("accrual to active: active=%v fire=%v; want true true", a, f)
	}
	// Recovery: eligible zero-deficit → prune, edge re-armed, since reset.
	if a, f := d.fold(0, true, t0.Add(db+time.Second), db); a || f {
		t.Fatalf("recovery pass: active=%v fire=%v; want false false", a, f)
	}
	if d.since != nil {
		t.Fatalf("recovery must clear since")
	}
	// A fresh deficit re-accrues and fires again.
	d.fold(1, true, t0.Add(db+2*time.Second), db)
	if a, f := d.fold(1, true, t0.Add(2*db+2*time.Second), db); !a || !f {
		t.Fatalf("re-accrual: active=%v fire=%v; want true true", a, f)
	}
}

// TestFoldTopologyDim_YieldResetsAccrual pins yield-to-in-flight: an
// ineligible pass resets `since`, so crossing the debounce needs a fresh
// continuous eligible window (the debounce deliberately exceeds a single
// roll step).
func TestFoldTopologyDim_YieldResetsAccrual(t *testing.T) {
	t.Parallel()
	d := &topologyDimState{}
	t0 := topoBaseTime()
	db := topologyMismatchDebounce

	d.fold(1, true, t0, db)                        // since = t0
	d.fold(1, true, t0.Add(db/2), db)              // still inactive
	d.fold(1, false, t0.Add(db/2+time.Second), db) // ineligible → since reset
	if d.since != nil {
		t.Fatalf("ineligible pass must reset since")
	}
	// New window starts here; a pass one debounce-minus-1s later is NOT
	// yet active because the reset restarted the clock.
	restart := t0.Add(db)
	d.fold(1, true, restart, db)
	if a, _ := d.fold(1, true, restart.Add(db-time.Second), db); a {
		t.Fatalf("must not be active before a fresh continuous window elapses")
	}
	if a, f := d.fold(1, true, restart.Add(db), db); !a || !f {
		t.Fatalf("active after fresh window: active=%v fire=%v; want true true", a, f)
	}
}

// TestTopologyMismatchActiveOrExpire_Freshness pins the output-side latch:
// a fresh read returns the active deficits; a stale read expires both
// dimensions and returns cleared, so the gauge returns to 0 on the next
// updateStatus.
func TestTopologyMismatchActiveOrExpire_Freshness(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	t0 := topoBaseTime()
	db := topologyMismatchDebounce

	s.foldTopologyMismatch(true, 1, true, 2, t0)
	s.foldTopologyMismatch(true, 1, true, 2, t0.Add(db)) // both active; observedAt=t0+db

	active, sDef, rDef := s.topologyMismatchActiveOrExpire(t0.Add(db + time.Second))
	if !active || sDef != 1 || rDef != 2 {
		t.Fatalf("fresh read = active:%v s:%d r:%d; want true 1 2", active, sDef, rDef)
	}

	stale := t0.Add(db).Add(topologyMismatchFreshnessWindow + time.Second)
	active, sDef, rDef = s.topologyMismatchActiveOrExpire(stale)
	if active || sDef != 0 || rDef != 0 {
		t.Fatalf("stale read = active:%v s:%d r:%d; want false 0 0", active, sDef, rDef)
	}
	if s.topologySentinel.since != nil || s.topologyReplica.since != nil {
		t.Fatalf("stale read must expire both dimensions (since reset)")
	}
}

// --- observe-path harness --------------------------------------------

type topoHarness struct {
	r          *ValkeyReconciler
	rec        *k8sevents.FakeRecorder
	cr         *valkeyv1beta1.Valkey
	key        types.NamespacedName
	clk        time.Time
	endpoints  []sentinel.Endpoint
	valkeyPods []corev1.Pod
	obs        []sentinel.EndpointObservation
}

// topoReplicas is the endpoint / valkey-pod count every observe-path
// harness uses (3 = the HA floor); a test that needs a mismatch overrides
// h.endpoints / h.valkeyPods directly.
const topoReplicas = 3

func newTopoHarness(t *testing.T) *topoHarness {
	t.Helper()
	operatormetrics.Register()
	cr := crForRotation()
	cr.Spec.Valkey.Replicas = topoReplicas
	cr.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "mymaster", Replicas: topoReplicas}
	c := fake.NewClientBuilder().WithScheme(authRotationScheme(t)).
		WithObjects(cr).WithStatusSubresource(cr).Build()
	rec := k8sevents.NewFakeRecorder(32)
	h := &topoHarness{
		rec: rec,
		cr:  cr,
		key: client.ObjectKeyFromObject(cr),
		clk: topoBaseTime(),
	}
	h.r = &ValkeyReconciler{
		Client:                 c,
		Recorder:               rec,
		nowFunc:                func() time.Time { return h.clk },
		endpointObservationsFn: func(types.NamespacedName) []sentinel.EndpointObservation { return h.obs },
	}
	h.endpoints = makeTopoEndpoints(topoReplicas)
	h.valkeyPods = makeReadyValkeyPods(topoReplicas)
	return h
}

func makeTopoEndpoints(n int) []sentinel.Endpoint {
	out := make([]sentinel.Endpoint, n)
	for i := range out {
		out[i] = sentinel.Endpoint{Name: fmt.Sprintf("vk-sentinel-%d", i), Addr: fmt.Sprintf("10.0.0.%d:26379", i+10)}
	}
	return out
}

func makeReadyValkeyPods(n int) []corev1.Pod {
	out := make([]corev1.Pod, n)
	for i := range out {
		out[i] = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("vk-%d", i)},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
	}
	return out
}

// setObs stamps the current clock onto each scripted observation so the
// input-freshness gate sees fresh data as the clock advances.
func (h *topoHarness) setObs(valid []bool, sentinels, replicas []int) {
	obs := make([]sentinel.EndpointObservation, len(valid))
	for i := range valid {
		obs[i] = sentinel.EndpointObservation{
			Name:           fmt.Sprintf("vk-sentinel-%d", i),
			Reachable:      true,
			At:             h.clk,
			CountsValid:    valid[i],
			KnownSentinels: sentinels[i],
			KnownReplicas:  replicas[i],
		}
	}
	h.obs = obs
}

func (h *topoHarness) observe(state orchestration.State) {
	h.r.observeSentinelTopology(context.Background(), h.cr, h.key, state, h.endpoints, h.valkeyPods)
}

func (h *topoHarness) topoEvents() []string { return topoEventsFrom(h.rec) }

// topoEventsFrom drains a recorder and keeps only the
// SentinelTopologyMismatch warnings.
func topoEventsFrom(rec *k8sevents.FakeRecorder) []string {
	var out []string
	for _, e := range drainAllEvents(rec.Events) {
		if strings.Contains(e, topoMismatchReason) {
			out = append(out, e)
		}
	}
	return out
}

// cleanCounts returns the healthy per-endpoint count for a
// topoReplicas-sized cluster: every endpoint reports topoReplicas-1
// (= expected), so the dimension carries no deficit.
func cleanCounts() []int {
	out := make([]int, topoReplicas)
	for i := range out {
		out[i] = topoReplicas - 1
	}
	return out
}

// allValid marks every endpoint's counts usable (CountsValid=true).
func allValid() []bool {
	out := make([]bool, topoReplicas)
	for i := range out {
		out[i] = true
	}
	return out
}

// drive runs one debounce episode past the threshold and returns the
// events collected. Each pass re-stamps obs at the current clock.
func (h *topoHarness) driveToActive(state orchestration.State, valid []bool, sentinels, replicas []int) []string {
	h.setObs(valid, sentinels, replicas)
	h.observe(state) // pass 1: since stamped
	h.clk = h.clk.Add(topologyMismatchDebounce + time.Second)
	h.setObs(valid, sentinels, replicas)
	h.observe(state) // pass 2: crosses debounce
	return h.topoEvents()
}

// --- observe-path tests ----------------------------------------------

// TestObserveSentinelTopology_NoValidCounts pins BLOCKER-1: every
// endpoint Reachable+fresh but CountsValid=false (version skew) makes the
// valid set empty → both dimensions ineligible → no event, no false
// full-deficit. (gauge stays 0: the read reports inactive.)
func TestObserveSentinelTopology_NoValidCounts(t *testing.T) {
	h := newTopoHarness(t)
	// valid=false everywhere; the (ignored) counts would read as a huge
	// deficit if consumed.
	events := h.driveToActive(orchestration.StateSteady,
		[]bool{false, false, false}, []int{0, 0, 0}, []int{0, 0, 0})
	if len(events) != 0 {
		t.Fatalf("no-valid-counts must not fire; got %v", events)
	}
	if active, _, _ := h.r.stateFor(h.key).quorumTracker().topologyMismatchActiveOrExpire(h.clk); active {
		t.Fatalf("no-valid-counts must leave the read inactive (gauge 0)")
	}
}

// TestObserveSentinelTopology_PeerDimAnyShort pins BLOCKER-2: one sentinel
// reporting a partial-gossip count (1) among two reporting 2 (expected 2)
// fires the sentinels dimension via ANY-short — the single-node gap MAX
// would mask — while the replicas dimension stays clean.
func TestObserveSentinelTopology_PeerDimAnyShort(t *testing.T) {
	h := newTopoHarness(t)
	// sentinels: [1,2,2] → min=1, expected 2 → deficit 1.
	// replicas:  [2,2,2] → max=2, expected 2 → deficit 0.
	events := h.driveToActive(orchestration.StateSteady,
		allValid(), []int{1, 2, 2}, cleanCounts())
	if len(events) != 1 {
		t.Fatalf("peer any-short must fire exactly once; got %d events %v", len(events), events)
	}
	if !strings.Contains(events[0], "sentinels short by 1") {
		t.Errorf("event must name the sentinels deficit; got %q", events[0])
	}
	if strings.Contains(events[0], "replicas short by") {
		t.Errorf("replicas dimension must stay clean; got %q", events[0])
	}
}

// TestObserveSentinelTopology_ReplicaDimMaxAcrossSentinels pins the
// conservative MAX for replicas: one endpoint full, one lagging-low →
// observed=max=full → no replica mismatch (a single lagging sentinel
// cannot false-fire). Sentinels are held clean too.
func TestObserveSentinelTopology_ReplicaDimMaxAcrossSentinels(t *testing.T) {
	h := newTopoHarness(t)
	// replicas: [2,1,2] → max=2, expected 2 → deficit 0 (MAX ignores the 1).
	// sentinels: all 2 → clean.
	events := h.driveToActive(orchestration.StateSteady,
		allValid(), cleanCounts(), []int{2, 1, 2})
	if len(events) != 0 {
		t.Fatalf("replica MAX must ignore a single lagging-low sentinel; got %v", events)
	}
}

// TestObserveSentinelTopology_ReplicaDimYieldsOnNotReadyValkey pins the
// independent per-dimension gate: with one valkey replica NotReady the
// replicas dimension is ineligible (no replica event) even though its raw
// deficit is present, while the sentinels dimension still fires — a
// NotReady data pod can no longer suppress a real peer-gossip deficit.
func TestObserveSentinelTopology_ReplicaDimYieldsOnNotReadyValkey(t *testing.T) {
	h := newTopoHarness(t)
	// One valkey pod NotReady → readyValkey=2 < 3 → replica dim ineligible.
	h.valkeyPods[2].Status.Conditions[0].Status = corev1.ConditionFalse
	// sentinels: [1,2,2] → deficit 1 (fires). replicas: [0,0,0] → raw
	// deficit 2, but the dimension is ineligible so it must NOT fire.
	events := h.driveToActive(orchestration.StateSteady,
		allValid(), []int{1, 2, 2}, []int{0, 0, 0})
	if len(events) != 1 {
		t.Fatalf("sentinels dim must still fire while replicas yields; got %d %v", len(events), events)
	}
	if strings.Contains(events[0], "replicas short by") {
		t.Errorf("replicas dimension must be silent under NotReady valkey; got %q", events[0])
	}
	if !strings.Contains(events[0], "sentinels short by 1") {
		t.Errorf("event must name the sentinels deficit; got %q", events[0])
	}
}

// TestObserveSentinelTopology_YieldsDuringValkeyRoll pins the FSM-roll
// yield: a data-plane roll makes the common base ineligible, so a raw
// deficit never fires.
func TestObserveSentinelTopology_YieldsDuringValkeyRoll(t *testing.T) {
	h := newTopoHarness(t)
	events := h.driveToActive(orchestration.StateRolloutReplicas,
		allValid(), []int{1, 2, 2}, cleanCounts())
	if len(events) != 0 {
		t.Fatalf("valkey roll must suppress the signal; got %v", events)
	}
}

// TestObserveSentinelTopology_YieldsOnSizeMismatch pins the pod-count
// yield: fewer endpoints than spec.sentinel.replicas (mid sentinel
// roll/scale) makes the base ineligible.
func TestObserveSentinelTopology_YieldsOnSizeMismatch(t *testing.T) {
	h := newTopoHarness(t)
	h.endpoints = makeTopoEndpoints(2) // len(endpoints) != spec.sentinel.replicas
	events := h.driveToActive(orchestration.StateSteady,
		[]bool{true, true}, []int{1, 2}, []int{2, 2})
	if len(events) != 0 {
		t.Fatalf("endpoint-count mismatch must suppress the signal; got %v", events)
	}
}

// TestObserveSentinelTopology_YieldsDuringSuppression pins the
// defer-to-incident-ladder yield: an active quorum-loss suppression gate
// makes the base ineligible.
func TestObserveSentinelTopology_YieldsDuringSuppression(t *testing.T) {
	h := newTopoHarness(t)
	q := h.r.stateFor(h.key).quorumTracker()
	q.mu.Lock()
	q.suppressionActive = true
	q.mu.Unlock()
	events := h.driveToActive(orchestration.StateSteady,
		allValid(), []int{1, 2, 2}, cleanCounts())
	if len(events) != 0 {
		t.Fatalf("suppression must defer the signal to the incident ladder; got %v", events)
	}
}

// TestObserveSentinelTopology_StaleObservationsAgeOut pins input-side
// freshness: a newest observation older than the freshness window makes
// the base ineligible, so both dimensions prune and never latch on stale
// observer data.
func TestObserveSentinelTopology_StaleObservationsAgeOut(t *testing.T) {
	h := newTopoHarness(t)
	// Stamp obs in the past, beyond the freshness window, with a deficit.
	obs := make([]sentinel.EndpointObservation, 3)
	staleAt := h.clk.Add(-(topologyObservationFreshnessWindow + time.Second))
	for i := range obs {
		obs[i] = sentinel.EndpointObservation{
			Name: fmt.Sprintf("vk-sentinel-%d", i), Reachable: true, At: staleAt,
			CountsValid: true, KnownSentinels: 1, KnownReplicas: 0,
		}
	}
	h.obs = obs
	h.observe(orchestration.StateSteady)
	h.clk = h.clk.Add(topologyMismatchDebounce + time.Second)
	h.obs = obs // still stale relative to the advanced clock
	h.observe(orchestration.StateSteady)
	if events := h.topoEvents(); len(events) != 0 {
		t.Fatalf("stale observations must not fire; got %v", events)
	}
	if active, _, _ := h.r.stateFor(h.key).quorumTracker().topologyMismatchActiveOrExpire(h.clk); active {
		t.Fatalf("stale observations must leave the read inactive")
	}
}

// TestObserveSentinelTopology_EmitsMismatchEventOncePerEpisode pins the
// event edge-gating + catalog wiring over multiple passes: exactly one
// Warning per active episode, naming the short dimension.
func TestObserveSentinelTopology_EmitsMismatchEventOncePerEpisode(t *testing.T) {
	h := newTopoHarness(t)
	valid, sent, repl := allValid(), []int{1, 2, 2}, cleanCounts()

	// Pass 1: below threshold, no event.
	h.setObs(valid, sent, repl)
	h.observe(orchestration.StateSteady)
	if got := h.topoEvents(); len(got) != 0 {
		t.Fatalf("pre-debounce pass fired %v", got)
	}
	// Pass 2: cross the debounce → one event.
	h.clk = h.clk.Add(topologyMismatchDebounce + time.Second)
	h.setObs(valid, sent, repl)
	h.observe(orchestration.StateSteady)
	first := h.topoEvents()
	if len(first) != 1 || !strings.Contains(first[0], "sentinels short by 1") {
		t.Fatalf("debounce edge must fire exactly one named event; got %v", first)
	}
	// Pass 3: still active → no re-fire.
	h.clk = h.clk.Add(time.Second)
	h.setObs(valid, sent, repl)
	h.observe(orchestration.StateSteady)
	if got := h.topoEvents(); len(got) != 0 {
		t.Fatalf("active episode must not re-fire; got %v", got)
	}
}

// TestUpdateStatus_SentinelTopology_WiresConditionAndGauge pins the
// production wiring end-to-end at the controller seam: an active mismatch
// drives TypeSentinelTopologyReconciled=False/SentinelTopologyMismatch AND
// the valkey_sentinel_topology_mismatch gauge to the per-dimension
// deficits through a real updateStatus call; once the observation ages
// out, the same read expires it and both return to the cleared state.
func TestUpdateStatus_SentinelTopology_WiresConditionAndGauge(t *testing.T) {
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
	gauge := func(dim string) float64 {
		return testutil.ToFloat64(operatormetrics.SentinelTopologyMismatch.WithLabelValues(key.Namespace, key.Name, dim))
	}
	topoCond := func() metav1.Condition {
		got := &valkeyv1beta1.Valkey{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatalf("get cr: %v", err)
		}
		for i := range got.Status.Conditions {
			if got.Status.Conditions[i].Type == orchestration.TypeSentinelTopologyReconciled {
				return got.Status.Conditions[i]
			}
		}
		t.Fatalf("SentinelTopologyReconciled condition not found")
		return metav1.Condition{}
	}
	callUpdate := func() {
		if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
			orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
	}

	// Fold both dimensions to active, freshly observed just before now.
	q := r.stateFor(key).quorumTracker()
	q.foldTopologyMismatch(true, 1, true, 2, clk.Add(-(topologyMismatchDebounce + 5*time.Second)))
	q.foldTopologyMismatch(true, 1, true, 2, clk.Add(-5*time.Second))

	callUpdate()
	if cnd := topoCond(); cnd.Status != metav1.ConditionFalse || cnd.Reason != orchestration.ReasonSentinelTopologyMismatch {
		t.Fatalf("condition = %s/%s; want False/%s", cnd.Status, cnd.Reason, orchestration.ReasonSentinelTopologyMismatch)
	}
	if gauge("sentinels") != 1 || gauge("replicas") != 2 {
		t.Fatalf("gauge = sentinels:%v replicas:%v; want 1 2", gauge("sentinels"), gauge("replicas"))
	}

	// Advance past the output freshness window: the read expires the
	// latch; condition returns to InSync and both gauge series to 0.
	clk = clk.Add(topologyMismatchFreshnessWindow + time.Second)
	callUpdate()
	if cnd := topoCond(); cnd.Status != metav1.ConditionTrue || cnd.Reason != orchestration.ReasonSentinelTopologyInSync {
		t.Errorf("condition after age-out = %s/%s; want True/%s", cnd.Status, cnd.Reason, orchestration.ReasonSentinelTopologyInSync)
	}
	if gauge("sentinels") != 0 || gauge("replicas") != 0 {
		t.Errorf("gauge after age-out = sentinels:%v replicas:%v; want 0 0", gauge("sentinels"), gauge("replicas"))
	}
}

// TestReconcileSentinelOrchestration_EmitsTopologyMismatch drives the
// PRODUCTION call site — reconcileSentinelOrchestration in sentinel mode
// with a started observer manager, endpoint/pod counts matching
// spec.sentinel.replicas, and a below-spec fresh observation seam — across
// two clock-advanced passes, and asserts the SentinelTopologyMismatch
// Warning fires exactly once with the expected detail. This pins BOTH the
// call site and its exact argument set (state, endpoints, valkeyPods):
// deleting the call, or feeding it the wrong arguments, drops the event
// and fails here — mirroring how the INFO-probe call site is pinned.
func TestReconcileSentinelOrchestration_EmitsTopologyMismatch(t *testing.T) {
	operatormetrics.Register()
	scheme := orphanTestScheme(t)
	cr := orphanTestCR() // sentinel mode, Valkey.Replicas=3
	cr.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "vk0", Replicas: 3, Quorum: 2}
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}

	// Three sentinel pods with PodIPs so sentinelEndpointsFromPods yields
	// len(endpoints) == spec.sentinel.replicas (the base-gate equality).
	objs := make([]client.Object, 0, 3)
	for i := range 3 {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("vk0-sentinel-%d", i),
				Namespace: cr.Namespace,
				Labels:    map[string]string{CRLabel: cr.Name, ComponentLabel: componentSentinel},
			},
			Status: corev1.PodStatus{PodIP: fmt.Sprintf("10.9.0.%d", i+1)},
		})
	}
	// Three ready replica-role valkey pods threaded in as the call
	// argument; no primary label so the INFO probe takes no dial.
	valkeyPods := make([]corev1.Pod, 3)
	for i := range 3 {
		p := labelledValkeyPod(fmt.Sprintf("vk0-%d", i), fmt.Sprintf("10.8.0.%d", i+1), roleValueReplica)
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		valkeyPods[i] = *p
	}

	mgr, _, cancel := startedManagerForReconciler(t)
	defer cancel()

	clk := topoBaseTime()
	rec := k8sevents.NewFakeRecorder(256)
	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		Recorder:         rec,
		SentinelObserver: mgr,
		LagChecker:       newFakeLagChecker(),
		nowFunc:          func() time.Time { return clk },
	}
	// Seam the topology read with fresh, below-spec sentinel counts:
	// min(num-other-sentinels) = 1 < expected 2 → sentinels deficit 1.
	// At tracks the harness clock so the input stays inside the freshness
	// window as the clock advances.
	r.endpointObservationsFn = func(types.NamespacedName) []sentinel.EndpointObservation {
		known := []int{1, 2, 2}
		out := make([]sentinel.EndpointObservation, 3)
		for i := range out {
			out[i] = sentinel.EndpointObservation{
				Name:           fmt.Sprintf("vk0-sentinel-%d", i),
				Reachable:      true,
				At:             clk,
				CountsValid:    true,
				KnownSentinels: known[i],
				KnownReplicas:  2, // matches expected → replicas dimension clean
			}
		}
		return out
	}

	ctx := context.Background()
	// Pass 1: below the debounce threshold — accrues, no event.
	r.reconcileSentinelOrchestration(ctx, cr, orchestration.StateSteady, valkeyPods, "")
	if got := topoEventsFrom(rec); len(got) != 0 {
		t.Fatalf("pre-debounce pass fired %v", got)
	}
	// Pass 2: advance past the debounce → the edge fires exactly one event.
	clk = clk.Add(topologyMismatchDebounce + time.Second)
	r.reconcileSentinelOrchestration(ctx, cr, orchestration.StateSteady, valkeyPods, "")

	events := topoEventsFrom(rec)
	if len(events) != 1 || !strings.Contains(events[0], "sentinels short by 1") {
		t.Fatalf("expected exactly one SentinelTopologyMismatch event naming the deficit; got %v", events)
	}
	if active, sDef, _ := r.stateFor(crKey).quorumTracker().topologyMismatchActiveOrExpire(clk); !active || sDef != 1 {
		t.Fatalf("tracker read = active:%v sDef:%d; want active 1 (gauge/condition path)", active, sDef)
	}
}

// TestObserveSentinelTopology_YieldsDuringFailover pins the
// failover-in-flight yield: a mid-failover sentinel-count dip is a
// legitimate transient (peer gossip churns while the ensemble
// re-elects), so the base gate must hold the accrual while
// IsFailoverInFlight is true — otherwise the hygiene signal accrues a
// false deficit toward SentinelTopologyMismatch during every failover.
func TestObserveSentinelTopology_YieldsDuringFailover(t *testing.T) {
	h := newTopoHarness(t)
	// Stamp the FSM tracker into failover-in-flight — the same idiom the
	// recovery-promotion refusal test uses to drive IsFailoverInFlight.
	h.r.stateFor(h.key).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}
	events := h.driveToActive(orchestration.StateSteady,
		allValid(), []int{1, 2, 2}, cleanCounts())
	if len(events) != 0 {
		t.Fatalf("a failover in flight must suppress the signal; got %v", events)
	}
	if active, _, _ := h.r.stateFor(h.key).quorumTracker().topologyMismatchActiveOrExpire(h.clk); active {
		t.Fatalf("a failover in flight must leave the read inactive (gauge 0)")
	}
}
