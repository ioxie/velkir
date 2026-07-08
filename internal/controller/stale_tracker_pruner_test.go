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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
)

// populatePerCRState fills every tracker (prunable + non-prunable) so a
// sweep's selective reset is observable.
func populatePerCRState(ps *perCRState) {
	// prunable — reset for vanished CRs
	ps.quorum = &crQuorumState{}
	ps.rolloutTrigger = &rolloutTriggerState{}
	ps.replicasRolled = &replicasRolledTracker{}
	ps.fsmTransition = &fsmTransitionTracker{}
	inner := &sync.Map{}
	inner.Store(types.UID("pod"), time.Now())
	ps.staleReplicas = inner
	ps.sqStatusDigest = "digest"
	ps.missingAuthSeen = time.Now()
	ps.dualMasterSelfHeal = &dualMasterSelfHealState{attemptCount: 3, lastAttempt: time.Now()}
	ps.dualMasterDeferEdge = "no-offset"
	ps.dualMasterDeferLastAt = time.Now()
	// not pruned — lifecycle-sensitive / self-clearing
	ps.manualRollout = &manualRolloutState{last: "x"}
	ps.failoverLatch = &failoverInFlightLatch{deadline: time.Now().Add(time.Hour)}
	ps.switchMasterEdge = "10.0.0.42:6379"
	ps.authPassword = &authPasswordCacheEntry{password: "p", hash: "h"}
}

// prunableCleared reports whether every prunable tracker on ps is reset.
func prunableCleared(ps *perCRState) bool {
	return ps.quorum == nil &&
		ps.rolloutTrigger == nil &&
		ps.replicasRolled == nil &&
		ps.fsmTransition == nil &&
		ps.staleReplicas == nil &&
		ps.sqStatusDigest == "" &&
		ps.missingAuthSeen.IsZero() &&
		ps.dualMasterSelfHeal == nil &&
		ps.dualMasterDeferEdge == "" &&
		ps.dualMasterDeferLastAt.IsZero()
}

// nonPrunablePresent reports whether the lifecycle-sensitive trackers on
// ps survive a sweep.
func nonPrunablePresent(ps *perCRState) bool {
	return ps.manualRollout != nil &&
		ps.failoverLatch != nil &&
		ps.switchMasterEdge != "" &&
		ps.authPassword != nil
}

// TestStaleTrackerPruner_resetsStaleAndKeepsLive pins the sweep
// model: a vanished CR's prunable trackers are reset while the entry
// itself (the reconcile mutex) and the lifecycle-sensitive trackers
// survive; a live CR is untouched. The earlier model deleted whole map
// entries from the eight swept maps and left the mutex + the four
// excluded maps alone — the field-level reset preserves that exact set.
func TestStaleTrackerPruner_resetsStaleAndKeepsLive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	live := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "cr-live"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()

	r := &ValkeyReconciler{Client: cli}
	liveKey := types.NamespacedName{Namespace: "ns-a", Name: "cr-live"}
	staleKey := types.NamespacedName{Namespace: "ns-a", Name: "cr-stale"}
	populatePerCRState(r.stateFor(liveKey))
	populatePerCRState(r.stateFor(staleKey))

	p := &StaleTrackerPruner{Client: cli, Reconciler: r}
	if err := p.prune(context.Background()); err != nil {
		t.Fatalf("prune: %v", err)
	}

	// Live CR: nothing reset.
	liveState, ok := r.stateForIfPresent(liveKey)
	if !ok {
		t.Fatal("live entry vanished")
	}
	if prunableCleared(liveState) {
		t.Error("live CR: prunable trackers were wrongly reset")
	}
	if !nonPrunablePresent(liveState) {
		t.Error("live CR: non-prunable trackers were wrongly dropped")
	}

	// Stale CR: entry survives (field-reset, never Delete — the reconcile
	// mutex must not be dropped), prunable reset, lifecycle-sensitive
	// trackers preserved.
	staleState, ok := r.stateForIfPresent(staleKey)
	if !ok {
		t.Fatal("stale entry was deleted entirely; the pruner must field-reset, not Delete (the reconcile mutex must survive)")
	}
	if !prunableCleared(staleState) {
		t.Error("stale CR: prunable trackers were not reset")
	}
	if !nonPrunablePresent(staleState) {
		t.Error("stale CR: lifecycle-sensitive trackers (failover latch / auth cache / switch-master edge / manual-rollout baseline) were wrongly pruned")
	}
}

// TestStaleTrackerPruner_emptyClusterResetsEverything: with no live CRs,
// every entry's prunable trackers reset.
func TestStaleTrackerPruner_emptyClusterResetsEverything(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ValkeyReconciler{Client: cli}
	stale1 := types.NamespacedName{Namespace: "ns-a", Name: "cr-1"}
	stale2 := types.NamespacedName{Namespace: "ns-b", Name: "cr-2"}
	populatePerCRState(r.stateFor(stale1))
	populatePerCRState(r.stateFor(stale2))

	p := &StaleTrackerPruner{Client: cli, Reconciler: r}
	if err := p.prune(context.Background()); err != nil {
		t.Fatalf("prune: %v", err)
	}

	for _, key := range []types.NamespacedName{stale1, stale2} {
		ps, ok := r.stateForIfPresent(key)
		if !ok {
			t.Fatalf("%s: entry was deleted; expected field-reset", key)
		}
		if !prunableCleared(ps) {
			t.Errorf("%s: prunable trackers were not reset", key)
		}
	}
}

func TestStaleTrackerPruner_pruneListErrorReturnsErr(t *testing.T) {
	// Empty scheme produces a List error (no kind registered).
	scheme := runtime.NewScheme()
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ValkeyReconciler{Client: cli}
	p := &StaleTrackerPruner{Client: cli, Reconciler: r}
	if err := p.prune(context.Background()); err == nil {
		t.Fatalf("prune: expected error from unregistered kind, got nil")
	}
}

func TestStaleTrackerPruner_needLeaderElection(t *testing.T) {
	p := &StaleTrackerPruner{}
	if !p.NeedLeaderElection() {
		t.Fatalf("NeedLeaderElection: expected true (leader-gated to avoid duplicate sweeps)")
	}
}

// TestStaleTrackerPruner_reapsGaugesForVanishedCR pins the metric half
// of the sweep: a vanished CR's per-CR gauge series are deleted via the
// full-registry partial-match reap (ResetReconcileGauges — the same
// sweep the Reconcile NotFound path runs), so a swallowed delete cannot
// leave e.g. valkey_dual_master_observed=1 firing its critical alert or
// valkey_resources_paused=1 lingering until operator restart — while a
// live CR's series are untouched.
func TestStaleTrackerPruner_reapsGaugesForVanishedCR(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	live := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-g", Name: "cr-live"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()

	r := &ValkeyReconciler{Client: cli}
	liveKey := types.NamespacedName{Namespace: "ns-g", Name: "cr-live"}
	staleKey := types.NamespacedName{Namespace: "ns-g", Name: "cr-stale"}
	populatePerCRState(r.stateFor(liveKey))
	populatePerCRState(r.stateFor(staleKey))

	operatormetrics.Register()
	for _, key := range []types.NamespacedName{liveKey, staleKey} {
		operatormetrics.DualMasterObserved.WithLabelValues(key.Namespace, key.Name).Set(1)
		operatormetrics.SplitBrainSustainedSeconds.WithLabelValues(key.Namespace, key.Name).Set(120)
		operatormetrics.MasterInfoTimeoutSeconds.WithLabelValues(key.Namespace, key.Name).Set(30)
		operatormetrics.SetSentinelTopologyMismatch(key.Namespace, key.Name, 1, 2)
		// A family OUTSIDE the four dual-master/split-brain names,
		// pinning that the sweep is the full-registry partial match
		// (any registered per-CR collector), not a hand-picked list.
		operatormetrics.SetPaused(key.Namespace, key.Name, true)
	}

	p := &StaleTrackerPruner{Client: cli, Reconciler: r}
	if err := p.prune(context.Background()); err != nil {
		t.Fatalf("prune: %v", err)
	}

	// Stale CR: every series deleted (a re-read materialises at 0).
	if got := testutil.ToFloat64(operatormetrics.DualMasterObserved.WithLabelValues(staleKey.Namespace, staleKey.Name)); got != 0 {
		t.Errorf("stale dual-master series survived the sweep: got %v, want 0 (deleted)", got)
	}
	if got := testutil.ToFloat64(operatormetrics.SplitBrainSustainedSeconds.WithLabelValues(staleKey.Namespace, staleKey.Name)); got != 0 {
		t.Errorf("stale split-brain series survived the sweep: got %v, want 0 (deleted)", got)
	}
	if got := testutil.ToFloat64(operatormetrics.MasterInfoTimeoutSeconds.WithLabelValues(staleKey.Namespace, staleKey.Name)); got != 0 {
		t.Errorf("stale master-info series survived the sweep: got %v, want 0 (deleted)", got)
	}
	for _, dim := range []string{"sentinels", "replicas"} {
		if got := testutil.ToFloat64(operatormetrics.SentinelTopologyMismatch.WithLabelValues(staleKey.Namespace, staleKey.Name, dim)); got != 0 {
			t.Errorf("stale topology-mismatch %s series survived the sweep: got %v, want 0 (deleted)", dim, got)
		}
	}
	if got := testutil.ToFloat64(operatormetrics.ResourcesPaused.WithLabelValues(staleKey.Namespace, staleKey.Name)); got != 0 {
		t.Errorf("stale paused series survived the sweep (full-registry reap regressed to a hand-picked list): got %v, want 0 (deleted)", got)
	}
	// Live CR: series untouched.
	if got := testutil.ToFloat64(operatormetrics.DualMasterObserved.WithLabelValues(liveKey.Namespace, liveKey.Name)); got != 1 {
		t.Errorf("live dual-master series was wrongly reaped: got %v, want 1", got)
	}
	if got := testutil.ToFloat64(operatormetrics.SplitBrainSustainedSeconds.WithLabelValues(liveKey.Namespace, liveKey.Name)); got != 120 {
		t.Errorf("live split-brain series was wrongly reaped: got %v, want 120", got)
	}
	if got := testutil.ToFloat64(operatormetrics.ResourcesPaused.WithLabelValues(liveKey.Namespace, liveKey.Name)); got != 1 {
		t.Errorf("live paused series was wrongly reaped: got %v, want 1", got)
	}
}

// TestStaleTrackerPruner_tearsDownObserverForVanishedCR pins the
// observer half of the sweep: a vanished CR's sentinel observer is
// Removed (Snapshot drops to absent) while a live CR's observer keeps
// publishing — the swallowed-delete counterpart of forgetCR's teardown.
func TestStaleTrackerPruner_tearsDownObserverForVanishedCR(t *testing.T) {
	staleCR := promotionTestCR()
	staleKey := types.NamespacedName{Namespace: staleCR.Namespace, Name: staleCR.Name}
	mgr, stop := promotionTestObserver(t, staleKey, "10.0.0.9")
	defer stop()

	scheme := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	// The cluster list does NOT contain the observer's CR → vanished.
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ValkeyReconciler{Client: cli, SentinelObserver: mgr}
	r.stateFor(staleKey) // the entry every reconcile materialises
	if !mgr.Snapshot(staleKey).Present {
		t.Fatalf("precondition: the observer must have published a snapshot")
	}

	p := &StaleTrackerPruner{Client: cli, Reconciler: r}
	if err := p.prune(context.Background()); err != nil {
		t.Fatalf("prune: %v", err)
	}

	if mgr.Snapshot(staleKey).Present {
		t.Errorf("the sweep must Remove a vanished CR's observer; snapshot still Present")
	}
}
