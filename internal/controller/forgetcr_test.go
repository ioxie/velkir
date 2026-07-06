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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/types"

	operatormetrics "github.com/ioxie/velkir/internal/metrics"
)

// TestForgetCR_ClearsPerCRState pins the dedup: forgetCR is the
// single teardown path, so it must drop the forgotten CR's entire perCR
// state-bag entry while leaving unrelated CRs untouched. After the
// consolidation this is a single map Delete — the type system (one perCR
// sync.Map of *perCRState) supersedes the former reflection count-guard
// that kept N parallel tracker maps in lockstep with forgetCR: a new
// tracker is a field on perCRState and cannot drift out of forgetCR's
// reach.
func TestForgetCR_ClearsPerCRState(t *testing.T) {
	r := &ValkeyReconciler{}
	forget := types.NamespacedName{Namespace: "ns", Name: "going"}
	keep := types.NamespacedName{Namespace: "ns", Name: "staying"}

	// Materialise a populated state bag for each CR — a spread of
	// trackers (pointer sub-trackers and value-typed fields) so the
	// single Delete is shown to drop them all at once.
	for _, key := range []types.NamespacedName{forget, keep} {
		ps := r.stateFor(key)
		ps.quorumTracker()
		ps.fsmTransitionTracker()
		ps.setFailoverLatch(&failoverInFlightLatch{})
		ps.setSQDigest("digest", time.Unix(0, 0))
	}

	// Per-CR metric series must also be torn down, else a deleted CR
	// leaves a permanent series (e.g. valkey_dual_master_observed=1
	// firing the critical alert against a workload that no longer
	// exists). Set the gauge for the forgotten CR before teardown.
	operatormetrics.Register()
	operatormetrics.DualMasterObserved.WithLabelValues(forget.Namespace, forget.Name).Set(1)
	// The topology-mismatch gauge carries a third `dimension` label, so
	// both its per-CR series must clear too — a two-value DeleteLabelValues
	// would miss them, leaving them latched at their last deficit.
	operatormetrics.SetSentinelTopologyMismatch(forget.Namespace, forget.Name, 1, 2)
	// forgetCR reaps the WHOLE per-CR collector registry, not a hand-picked
	// subset: a CR deleted while paused (or mid-resize) must not leave its
	// gauge latched when the terminal-deletion path — which reaches forgetCR
	// via the deferred mutex cleanup, not the NotFound reset — is the only
	// teardown that runs (a swallowed follow-up NotFound reconcile). Pin one
	// family outside the former four so a narrowing regression fails here.
	operatormetrics.SetPaused(forget.Namespace, forget.Name, true)

	r.forgetCR(forget)

	if _, ok := r.perCR.Load(forget); ok {
		t.Error("perCR still holds the forgotten CR after forgetCR")
	}
	if _, ok := r.perCR.Load(keep); !ok {
		t.Error("perCR wrongly evicted an unrelated CR")
	}
	if got := testutil.ToFloat64(operatormetrics.DualMasterObserved.WithLabelValues(forget.Namespace, forget.Name)); got != 0 {
		t.Errorf("dual-master gauge series survived forgetCR: got %v, want 0 (deleted)", got)
	}
	for _, dim := range []string{"sentinels", "replicas"} {
		if got := testutil.ToFloat64(operatormetrics.SentinelTopologyMismatch.WithLabelValues(forget.Namespace, forget.Name, dim)); got != 0 {
			t.Errorf("topology-mismatch %s series survived forgetCR: got %v, want 0 (deleted)", dim, got)
		}
	}
	if got := testutil.ToFloat64(operatormetrics.ResourcesPaused.WithLabelValues(forget.Namespace, forget.Name)); got != 0 {
		t.Errorf("paused series survived forgetCR: got %v, want 0 (full-registry sweep, not a hand-picked subset)", got)
	}
}

// TestPruneStale_ResetsDualMasterFields pins that the stale-list
// pruner drops the dual-master observation and re-arms its event edge —
// so a reclaimed/recreated same-name CR's first split fires
// DualMasterObserved instead of being suppressed by a stale signature.
func TestPruneStale_ResetsDualMasterFields(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	s.stampDualMasterObserved([]string{"vk0-0", "vk0-1"}, time.Unix(1_700_000_000, 0))
	s.fireDualMasterObservedEdge("vk0-0,vk0-1")

	s.pruneStale()

	if s.dualMasterObservation() != nil {
		t.Error("pruneStale must drop the dual-master observation")
	}
	if !s.fireDualMasterObservedEdge("vk0-0,vk0-1") {
		t.Error("pruneStale must re-arm the dual-master event edge")
	}
}

// TestForgetCR_TearsDownSentinelObserver pins the observer half of the
// teardown: forgetCR must Remove the CR's sentinel observer so a deleted
// CR's goroutine tree stops polling freed pod IPs — observable as the
// manager's Snapshot dropping from Present to absent the moment forgetCR
// runs (no reconcile or age-out needed).
func TestForgetCR_TearsDownSentinelObserver(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	r := &ValkeyReconciler{SentinelObserver: mgr}
	r.stateFor(crKey) // materialise the state bag alongside the observer
	if !mgr.Snapshot(crKey).Present {
		t.Fatalf("precondition: the observer must have published a snapshot")
	}

	r.forgetCR(crKey)

	if mgr.Snapshot(crKey).Present {
		t.Errorf("forgetCR must Remove the sentinel observer; snapshot still Present")
	}
}

// TestPruneStale_ResetsDualMasterDeferAndSelfHeal pins that the
// stale-list pruner also drops the self-heal attempt budget and the
// deferral edge + its rate-bound stamp — a reclaimed/recreated
// same-name CR must neither inherit an exhausted attempt budget nor
// have its first DualMasterSelfHealDeferred suppressed by the dead CR's
// latched signature (the deferral constants recur verbatim).
func TestPruneStale_ResetsDualMasterDeferAndSelfHeal(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	now := time.Unix(1_700_000_000, 0)
	for range dualMasterSelfHealMaxAttempts {
		s.recordSelfHealAttempt(now)
	}
	if s.selfHealAttemptAllowed(now.Add(time.Hour), dualMasterSelfHealBaseCooldown, dualMasterSelfHealMaxBackoff, dualMasterSelfHealMaxAttempts) {
		t.Fatalf("precondition: the attempt budget must be exhausted")
	}
	if !s.fireDualMasterDeferEdge("no-offset", now) {
		t.Fatalf("precondition: the deferral edge must latch")
	}

	s.pruneStale()

	if !s.selfHealAttemptAllowed(now, dualMasterSelfHealBaseCooldown, dualMasterSelfHealMaxBackoff, dualMasterSelfHealMaxAttempts) {
		t.Errorf("pruneStale must reset the self-heal attempt budget")
	}
	if !s.fireDualMasterDeferEdge("no-offset", now) {
		t.Errorf("pruneStale must re-arm the deferral edge and its rate-bound stamp")
	}
}
