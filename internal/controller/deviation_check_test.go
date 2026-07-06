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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// deviationCR builds a replication-mode CR (no sentinel block) carrying
// exactly two best-practice deviations: a too-permissive valkey PDB
// (minAvailable=0 below the floor of 2 for replicas=3 → PDBTooPermissive)
// and an over-1 rollout maxUnavailable (→ RolloutFragileQuorum). No
// sentinel block keeps RolloutGraceTooTight and the sentinel-side PDB
// check out of the picture, so the active set is exactly those two.
func deviationCR() *valkeyv1beta1.Valkey {
	minAvail := intstr.FromInt32(0)
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-cr", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeReplication,
		},
	}
	v.Spec.Valkey.Replicas = 3
	v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &minAvail}
	v.Spec.Rollout.MaxUnavailable = 2
	return v
}

// TestEmitDeviations_EmitsWarningPerActiveDeviation pins the wiring from
// the webhook's Deviations(o) (single source of truth) through the
// reconciler's DeviationEmitter to durable Warning Events: one
// PDBTooPermissive and one RolloutFragileQuorum, and idempotent within a
// process (the second sweep dedups).
func TestEmitDeviations_EmitsWarningPerActiveDeviation(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{DeviationEmitter: events.NewDeviationEmitter(rec)}
	v := deviationCR()

	r.emitDeviations(v)

	got := drainEvents(rec)
	if len(got) != 2 {
		t.Fatalf("want 2 deviation events, got %d: %v", len(got), got)
	}
	joined := strings.Join(got, " ")
	for _, reason := range []events.Reason{events.PDBTooPermissive, events.RolloutFragileQuorum} {
		if !strings.Contains(joined, string(reason)) {
			t.Errorf("missing %s event; got %v", reason, got)
		}
	}

	// Idempotent within a process lifetime: a second sweep dedups.
	r.emitDeviations(v)
	if again := drainEvents(rec); len(again) != 0 {
		t.Errorf("second sweep should dedup, no new events; got %v", again)
	}
}

// TestEmitDeviations_ForgetReEmits asserts that after Forget (CR delete),
// a recreated CR with the same identity re-emits rather than inheriting
// the prior CR's silenced state.
func TestEmitDeviations_ForgetReEmits(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{DeviationEmitter: events.NewDeviationEmitter(rec)}
	v := deviationCR()

	r.emitDeviations(v)
	_ = drainEvents(rec)

	r.DeviationEmitter.Forget(v)
	r.emitDeviations(v)
	if got := drainEvents(rec); len(got) != 2 {
		t.Errorf("after Forget, recreated CR should re-emit 2 events; got %d: %v", len(got), got)
	}
}

// TestEmitDeviations_NilEmitterNoPanic asserts the sweep is a safe no-op
// when no emitter is wired (minimal reconcilers / tests).
func TestEmitDeviations_NilEmitterNoPanic(t *testing.T) {
	r := &ValkeyReconciler{} // DeviationEmitter nil
	r.emitDeviations(deviationCR())
}

// sentinelAggressiveCR builds a minimal sentinel-mode CR whose only
// best-practice deviation is the in-band down-after (3000ms — the sanctioned
// default, which sits in [1000, 30000)). No explicit PDB and a default
// rollout keep the active deviation set to exactly WarnAggressiveTimeouts.
func sentinelAggressiveCR() *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-aggro-cr", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Sentinel: &valkeyv1beta1.SentinelPodSpec{
				MasterName:            "mymaster",
				Replicas:              3,
				DownAfterMilliseconds: 3000,
				FailoverTimeout:       180000,
			},
		},
	}
	v.Spec.Valkey.Replicas = 3
	return v
}

// TestEmitDeviations_SentinelAggressiveTimeout pins the end-to-end wiring of
// the WarnAggressiveTimeouts deviation from the webhook's Deviations() through
// the reconciler's DeviationEmitter to a durable Warning Event, plus its
// per-process dedup latch.
func TestEmitDeviations_SentinelAggressiveTimeout(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{DeviationEmitter: events.NewDeviationEmitter(rec)}
	v := sentinelAggressiveCR()

	r.emitDeviations(v)

	got := drainEvents(rec)
	if !strings.Contains(strings.Join(got, " "), string(events.WarnAggressiveTimeouts)) {
		t.Fatalf("sentinel CR at the default down-after must emit WarnAggressiveTimeouts; got %v", got)
	}

	// Per-process dedup: a second sweep emits nothing new.
	r.emitDeviations(v)
	if again := drainEvents(rec); len(again) != 0 {
		t.Errorf("second sweep should dedup, no new events; got %v", again)
	}
}
