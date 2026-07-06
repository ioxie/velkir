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
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/sentinel"
)

// TestUpdateQuorumSuppressionGate_SplitBrainEmitsOncePerEpisode pins the
// fix at the REAL emission path: updateQuorumSuppressionGate reads
// the observer snapshot and fires SplitBrainDetected (+counter) exactly
// once per disagreement episode, not once per reconcile. Driven through
// a live observer publishing QuorumStatusLost (reachable sentinels with
// CKQUORUM=NOQUORUM), so a regression that drops the emission OR
// re-emits per reconcile fails here — neither is caught by the
// Unknown-only negative tests elsewhere.
func TestUpdateQuorumSuppressionGate_SplitBrainEmitsOncePerEpisode(t *testing.T) {
	operatormetrics.Register()
	mgr, _, cancel := startedManagerForReconciler(t)
	defer cancel()

	// Two reachable sentinels both reporting CKQUORUM=NOQUORUM → the
	// observer publishes Present + QuorumStatusLost (reachable >=
	// threshold, no quorum agreement).
	s1, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 1: %v", err)
	}
	defer s1.Stop()
	s1.SetQuorumOK(false)
	s2, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 2: %v", err)
	}
	defer s2.Stop()
	s2.SetQuorumOK(false)

	rec := k8sevents.NewFakeRecorder(64)
	r := &ValkeyReconciler{SentinelObserver: mgr, Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{Replicas: 3, Quorum: 2, MasterName: "mymaster"}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	if err := mgr.Ensure(context.Background(), cr, v.Spec.Sentinel.MasterName, "",
		[]sentinel.Endpoint{
			{Name: v.Name + "-sentinel-0", Addr: s1.Addr()},
			{Name: v.Name + "-sentinel-1", Addr: s2.Addr()},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Wait for the first poll-tick Lost snapshot.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap := mgr.Snapshot(cr); snap.Present && snap.Primary.Quorum == sentinel.QuorumStatusLost {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if snap := mgr.Snapshot(cr); !snap.Present || snap.Primary.Quorum != sentinel.QuorumStatusLost {
		t.Fatalf("observer never published Present + QuorumStatusLost; got %+v", snap.Primary)
	}

	countSplitBrain := func(evs []string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, "SplitBrainDetected") {
				n++
			}
		}
		return n
	}

	before := testutil.ToFloat64(operatormetrics.SplitBrainDetectionsTotal.WithLabelValues(cr.Namespace, cr.Name))

	// Three reconciles inside one sustained Lost episode → exactly one
	// event + one counter increment.
	for range 3 {
		r.updateQuorumSuppressionGate(context.Background(), v, cr)
	}
	got := countSplitBrain(drainAllEvents(rec.Events))
	if got != 1 {
		t.Errorf("SplitBrainDetected emitted %d times across one episode, want 1", got)
	}
	delta := testutil.ToFloat64(operatormetrics.SplitBrainDetectionsTotal.WithLabelValues(cr.Namespace, cr.Name)) - before
	if delta != 1 {
		t.Errorf("counter advanced by %v across one episode, want 1", delta)
	}

	// Recover: CKQUORUM=OK → snapshot flips to OK, episode resets.
	s1.SetQuorumOK(true)
	s2.SetQuorumOK(true)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap := mgr.Snapshot(cr); snap.Present && snap.Primary.Quorum == sentinel.QuorumStatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.updateQuorumSuppressionGate(context.Background(), v, cr)
	_ = drainAllEvents(rec.Events)

	// Second episode: CKQUORUM=NOQUORUM again → one more event.
	s1.SetQuorumOK(false)
	s2.SetQuorumOK(false)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap := mgr.Snapshot(cr); snap.Present && snap.Primary.Quorum == sentinel.QuorumStatusLost {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.updateQuorumSuppressionGate(context.Background(), v, cr)
	r.updateQuorumSuppressionGate(context.Background(), v, cr)
	if got := countSplitBrain(drainAllEvents(rec.Events)); got != 1 {
		t.Errorf("second episode emitted %d events, want 1", got)
	}
}
