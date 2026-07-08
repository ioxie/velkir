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

// TestUpdateQuorumSuppressionGate_SamePollWindowDoesNotExit pins the
// gate at the REAL snapshot path: updateQuorumSuppressionGate must
// thread the snapshot's LastPolledAt into the hysteresis so that
// back-to-back reconciles inside one observer poll window count as ONE
// OK poll, not one per pass. The proven window also includes a pubsub
// republish (fake PushEvent → -odown dispatch arm), which advances the
// snapshot's UpdatedAt while carrying LastPolledAt forward unchanged —
// so wiring the gate to UpdatedAt (or to any per-publish stamp) fails
// here, not just dropping the stamp entirely.
//
// A wide 300ms poll interval makes the same-poll window deterministic
// for the microsecond-scale gate-call sequence; a stamp equality check
// around the sequence proves which case ran. Any straddle (a poll tick
// landing mid-window) retries from a force-reset episode — the Lost
// re-entry zeroes the counter and last-counted stamp regardless of
// current gate state, so no attempt inherits residue from a prior one.
func TestUpdateQuorumSuppressionGate_SamePollWindowDoesNotExit(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(256)
	m := sentinel.NewManager(rec, sentinel.Options{
		PollInterval:       300 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	})
	ctx := t.Context()
	go func() { _ = m.Start(ctx) }()
	probe := types.NamespacedName{Namespace: "_probe", Name: "_probe"}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := m.Ensure(ctx, probe, "_probe", "",
			[]sentinel.Endpoint{{Name: "_probe", Addr: "127.0.0.1:1"}}); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.Remove(probe)

	s1, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 1: %v", err)
	}
	defer s1.Stop()
	s2, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 2: %v", err)
	}
	defer s2.Stop()

	// 1ms loss threshold: the two gate calls enter suppression without
	// waiting out the production 60s floor. Entry additionally requires
	// a live poll strictly newer than the episode-opening poll, so
	// cleanEpisode waits (waitForFreshPoll) for a fresh Lost poll
	// between the two gate calls — wall-clock aging alone no longer
	// crosses.
	r := &ValkeyReconciler{
		SentinelObserver: m,
		Recorder:         rec,
		Tunables:         QuorumSuppressionTunables{LossThreshold: time.Millisecond},
	}
	v := newCR(valkeyv1beta1.ModeSentinel)
	v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{Replicas: 3, Quorum: 2, MasterName: "mymaster"}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	if err := m.Ensure(context.Background(), cr, v.Spec.Sentinel.MasterName, "",
		[]sentinel.Endpoint{
			{Name: v.Name + "-sentinel-0", Addr: s1.Addr()},
			{Name: v.Name + "-sentinel-1", Addr: s2.Addr()},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	waitForQuorum := func(want sentinel.QuorumStatus) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if snap := m.Snapshot(cr); snap.Present && snap.Primary.Quorum == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("observer never published Quorum=%v; got %+v", want, m.Snapshot(cr).Primary)
	}

	// waitForFreshPoll blocks until the observer publishes a poll stamp
	// strictly newer than `anchor`, so the entry threshold-cross has a
	// live re-confirmation to satisfy its fresh-poll clause (a wedged
	// observer re-reading one Lost poll must never cross). Mirrors the
	// exit-side distinct-poll wait below.
	waitForFreshPoll := func(anchor time.Time) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if m.Snapshot(cr).Primary.LastPolledAt.After(anchor) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("observer never advanced LastPolledAt past %s", anchor)
	}

	// cleanEpisode force-resets the gate into a freshly-entered
	// suppression with quorum recovered and one OK snapshot published.
	// The Lost observations zero the hysteresis counter and the
	// last-counted stamp unconditionally, so every attempt starts
	// from identical state whether or not the prior attempt exited.
	cleanEpisode := func() {
		t.Helper()
		s1.SetQuorumOK(false)
		s2.SetQuorumOK(false)
		waitForQuorum(sentinel.QuorumStatusLost)
		r.updateQuorumSuppressionGate(context.Background(), v, cr) // opens/resets the loss episode
		// Entry now also requires a live poll strictly newer than the
		// episode-opening poll — wall-clock aging alone (a frozen stamp
		// re-read) can no longer cross. Wait for a fresh Lost poll, then the
		// second gate call crosses (the 1ms LossThreshold is long since met
		// by the >300ms poll wait; sentinels stay not-OK so the poll is Lost).
		openAnchor := m.Snapshot(cr).Primary.LastPolledAt
		waitForFreshPoll(openAnchor)
		r.updateQuorumSuppressionGate(context.Background(), v, cr) // crosses the threshold
		if !r.IsSentinelSuppressed(cr) {
			t.Fatalf("gate did not enter suppression")
		}
		s1.SetQuorumOK(true)
		s2.SetQuorumOK(true)
		waitForQuorum(sentinel.QuorumStatusOK)
	}

	proven := false
	for attempt := range 5 {
		cleanEpisode()
		before := m.Snapshot(cr).Primary

		// Two back-to-back reconciles inside the same poll window.
		r.updateQuorumSuppressionGate(context.Background(), v, cr)
		r.updateQuorumSuppressionGate(context.Background(), v, cr)

		// Pubsub republish: the -odown dispatch arm re-publishes the
		// snapshot with a fresh UpdatedAt while LastPolledAt is
		// carried forward unchanged. A gate wired to UpdatedAt would
		// count this as a fresh poll and exit below.
		s1.PushEvent("-odown", "master mymaster 10.0.0.2 6379")
		pubsubDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(pubsubDeadline) {
			if snap := m.Snapshot(cr); snap.Primary.UpdatedAt.After(before.UpdatedAt) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		r.updateQuorumSuppressionGate(context.Background(), v, cr)

		after := m.Snapshot(cr).Primary
		if after.LastPolledAt.Equal(before.LastPolledAt) {
			// All three gate calls provably consumed the same poll
			// (the third through a republished snapshot).
			if !r.IsSentinelSuppressed(cr) {
				t.Fatalf("gate exited off a single OK poll observed by repeated reconciles")
			}
			proven = true
			break
		}
		// A poll tick landed somewhere in the window — the calls may
		// have legitimately seen distinct stamps. Retry from a fresh
		// force-reset episode.
		t.Logf("attempt %d: poll advanced mid-window, retrying", attempt)
	}
	if !proven {
		t.Fatalf("could not produce a same-poll multi-reconcile window in 5 attempts")
	}

	// Discard everything emitted so far (episode entries, and exits
	// from straddled attempts) — the exactly-once assertion below
	// scopes to the final proven episode only.
	_ = drainAllEvents(rec.Events)

	// Exit path: subsequent polls advance the stamp; the gate exits
	// on the second DISTINCT OK poll (the proven window counted one).
	exitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(exitDeadline) && r.IsSentinelSuppressed(cr) {
		r.updateQuorumSuppressionGate(context.Background(), v, cr)
		time.Sleep(50 * time.Millisecond)
	}
	if r.IsSentinelSuppressed(cr) {
		t.Fatalf("gate never exited after distinct OK polls")
	}

	var quorumReached int
	for _, e := range drainAllEvents(rec.Events) {
		if strings.Contains(e, "QuorumReached") {
			quorumReached++
		}
	}
	if quorumReached != 1 {
		t.Errorf("QuorumReached emitted %d times for the proven episode, want 1", quorumReached)
	}
}

// TestUpdateQuorumSuppressionGate_SplitBrainGaugeFrozenPollStopsGrowing
// pins the freshness cap at the REAL snapshot seam: the gate must
// thread the snapshot's LastPolledAt — not the reconcile wall-clock —
// into the split-brain sustained tracker, so back-to-back reconciles
// consuming ONE Lost poll leave the SplitBrainSustainedSeconds gauge
// unchanged, and a fresh poll resumes its growth. A call site passing
// the wall-clock for both arguments grows the gauge on every reconcile
// against a frozen poll and fails here.
//
// Same proven-window apparatus as _SamePollWindowDoesNotExit: a wide
// poll interval plus a stamp-equality check around the sleep-separated
// gate calls proves both calls consumed the same poll; any straddle
// retries. The frozen window (~a second) sits far below the episode's
// staleness expiry, so the expiry path never interferes.
func TestUpdateQuorumSuppressionGate_SplitBrainGaugeFrozenPollStopsGrowing(t *testing.T) {
	operatormetrics.Register()
	rec := k8sevents.NewFakeRecorder(256)
	m := sentinel.NewManager(rec, sentinel.Options{
		PollInterval:       600 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	})
	ctx := t.Context()
	go func() { _ = m.Start(ctx) }()
	probe := types.NamespacedName{Namespace: "_probe", Name: "_probe"}
	startDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(startDeadline) {
		if err := m.Ensure(ctx, probe, "_probe", "",
			[]sentinel.Endpoint{{Name: "_probe", Addr: "127.0.0.1:1"}}); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.Remove(probe)

	s1, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 1: %v", err)
	}
	defer s1.Stop()
	s2, err := newRecoveringSentinel("10.0.0.2", 100)
	if err != nil {
		t.Fatalf("sentinel 2: %v", err)
	}
	defer s2.Stop()
	s1.SetQuorumOK(false)
	s2.SetQuorumOK(false)

	r := &ValkeyReconciler{SentinelObserver: m, Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	v.Name = "vk0-sbgauge" // own gauge series, isolated from sibling tests
	v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{Replicas: 3, Quorum: 2, MasterName: "mymaster"}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	gauge := func() float64 {
		return testutil.ToFloat64(operatormetrics.SplitBrainSustainedSeconds.WithLabelValues(cr.Namespace, cr.Name))
	}

	if err := m.Ensure(context.Background(), cr, v.Spec.Sentinel.MasterName, "",
		[]sentinel.Endpoint{
			{Name: v.Name + "-sentinel-0", Addr: s1.Addr()},
			{Name: v.Name + "-sentinel-1", Addr: s2.Addr()},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	lostDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(lostDeadline) {
		if snap := m.Snapshot(cr); snap.Present && snap.Primary.Quorum == sentinel.QuorumStatusLost {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if snap := m.Snapshot(cr); !snap.Present || snap.Primary.Quorum != sentinel.QuorumStatusLost {
		t.Fatalf("observer never published Quorum=Lost; got %+v", m.Snapshot(cr).Primary)
	}

	proven := false
	var frozenReading float64
	var frozenStamp time.Time
	for attempt := range 5 {
		before := m.Snapshot(cr).Primary.LastPolledAt
		r.updateQuorumSuppressionGate(context.Background(), v, cr)
		r1 := gauge()
		// Wall-clock advances measurably while — on a proven attempt —
		// the poll stamp does not.
		time.Sleep(150 * time.Millisecond)
		r.updateQuorumSuppressionGate(context.Background(), v, cr)
		r2 := gauge()
		after := m.Snapshot(cr).Primary.LastPolledAt
		if after.Equal(before) {
			// Both gate calls provably consumed the same Lost poll
			// (LastPolledAt is monotone, equal at both ends). The
			// reading must not have grown off wall-clock alone.
			if r2 != r1 {
				t.Fatalf("gauge grew %v -> %v across reconciles consuming one frozen poll", r1, r2)
			}
			frozenReading = r2
			frozenStamp = after
			proven = true
			break
		}
		t.Logf("attempt %d: poll advanced mid-window, retrying", attempt)
	}
	if !proven {
		t.Fatalf("could not produce a same-poll multi-reconcile window in 5 attempts")
	}

	// The observer keeps polling (sentinels still Lost): a strictly-newer
	// poll must resume the gauge's growth.
	freshDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(freshDeadline) {
		if m.Snapshot(cr).Primary.LastPolledAt.After(frozenStamp) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !m.Snapshot(cr).Primary.LastPolledAt.After(frozenStamp) {
		t.Fatalf("observer never advanced LastPolledAt past the proven window")
	}
	r.updateQuorumSuppressionGate(context.Background(), v, cr)
	if resumed := gauge(); resumed <= frozenReading {
		t.Fatalf("a fresh Lost poll must resume the gauge's growth: frozen %v, after fresh poll %v", frozenReading, resumed)
	}
}
