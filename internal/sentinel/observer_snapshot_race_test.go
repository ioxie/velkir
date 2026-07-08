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

package sentinel

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// TestObserver_SnapshotRMWSerialized pins the serialization: the snapshot
// read-modify-write must be serialized so a pubsub republish carrying a
// stale prior snapshot can't clobber a poll result (or vice versa).
//
// The invariant exploited here is Epoch monotonicity. The poll path only
// ever raises Epoch (epoch = max(prev.Epoch, ...)); the pubsub paths
// carry prev.Epoch forward unchanged. If the load->store pair is not
// atomic, a pubsub writer can read prev at epoch N, a poll writer can
// then store epoch N+k, and the pubsub writer then stores epoch N — an
// observable Epoch regression. With the fix every read-modify-write holds
// o.mu, so the sequence of stored Epochs is monotonically non-decreasing
// and a lock-free reader never sees Epoch go backwards.
//
// Before the fix this test fails (Epoch regresses); after it passes. It
// also runs clean under -race — though the bug is a logical lost update,
// not a memory race (all o.holder access is via atomic.Value), so the
// value assertion below is what catches it.
func TestObserver_SnapshotRMWSerialized(t *testing.T) {
	const masterName = "vk0"
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		masterName, "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: "10.0.0.1:26379"},
			{Name: "vk0-sentinel-1", Addr: "10.0.0.2:26379"},
		},
		Options{}, nil, nil,
	)

	// Baseline snapshot at epoch 0.
	o.mu.Lock()
	o.publishLocked(QuorumStatusOK, "10.0.0.9:6379", 0, SourcePoll, time.Now())
	o.mu.Unlock()

	ctx := context.Background()
	const iters = 50000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Poll-like writer: monotonically raises Epoch under the lock,
	// mirroring pollOnce's `epoch = max(prev.Epoch, maxEpoch)` then
	// store. Closes stop when done so the other goroutines wind down.
	wg.Go(func() {
		defer close(stop)
		for i := int64(1); i <= iters; i++ {
			o.mu.Lock()
			prev, _ := o.holder.load()
			epoch := max(prev.Epoch, i)
			o.publishLocked(QuorumStatusOK, "10.0.0.9:6379", epoch, SourcePoll, time.Now())
			o.mu.Unlock()
		}
	})

	// Pubsub writers: +switch-master swaps the address and +odown
	// marks the sentinel; both carry prev.Epoch forward — the
	// canonical lost-update aggressors. Driven straight through
	// dispatch (no network) from both endpoints concurrently.
	switchMaster := pubsubMessage{Channel: "+switch-master", Payload: "vk0 10.0.0.7 6379 10.0.0.9 6379"}
	odown := pubsubMessage{Channel: "+odown", Payload: "master vk0 10.0.0.9 6379"}
	for _, ep := range o.endpoints {
		wg.Add(1)
		go func(ep Endpoint) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				o.dispatch(ctx, ep, switchMaster)
				o.dispatch(ctx, ep, odown)
			}
		}(ep)
	}

	// Lock-free reader: asserts Epoch never regresses.
	wg.Go(func() {
		var high int64
		for {
			p, _ := o.snapshot()
			if p.Epoch < high {
				t.Errorf("Epoch regressed: observed %d after %d — snapshot read-modify-write not serialized", p.Epoch, high)
				return
			}
			high = p.Epoch
			select {
			case <-stop:
				return
			default:
			}
		}
	})

	wg.Wait()

	// After the writers settle the snapshot must hold the highest poll
	// epoch: pubsub republishes carry it forward, never lower it.
	if p, _ := o.snapshot(); p.Epoch != iters {
		t.Errorf("final Epoch = %d, want %d (highest poll epoch carried forward)", p.Epoch, iters)
	}
}

// TestObserver_PullAndPubsubODownMapsIndependentUnderRace pins
// concurrency safety AND no cross-map contamination between the two
// o_down truth sources: the pull tick reconciles o.odownPull (keyed
// here by sentinel-0) while the pubsub path mutates o.odown (keyed by
// sentinel-1), both under o.mu, and a lock-free reader iterates both
// snapshot maps. The pull-side key must never appear in the pubsub
// ODown map and vice versa, and the whole thing must run clean under
// -race.
func TestObserver_PullAndPubsubODownMapsIndependentUnderRace(t *testing.T) {
	const masterName = "vk0"
	const (
		pullKey   = "vk0-sentinel-0"
		pubsubKey = "vk0-sentinel-1"
	)
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		masterName, "",
		[]Endpoint{
			{Name: pullKey, Addr: "10.0.0.1:26379"},
			{Name: pubsubKey, Addr: "10.0.0.2:26379"},
		},
		Options{}, nil, nil,
	)

	// Baseline snapshot so the reader always loads a Present value.
	o.mu.Lock()
	o.publishLocked(QuorumStatusOK, "10.0.0.9:6379", 0, SourcePoll, time.Now())
	o.mu.Unlock()

	ctx := context.Background()
	const iters = 20000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Pull-side writer: reconcile o.odownPull for sentinel-0 (alternating
	// o_down set / clear) then publish — the exact pollOnce discipline,
	// under o.mu. Closes stop when done so the others wind down.
	wg.Go(func() {
		defer close(stop)
		now := time.Unix(1000, 0)
		for i := range iters {
			obs := []odownPullObs{{name: pullKey, flags: &MasterFlags{ODown: i%2 == 0}}}
			o.mu.Lock()
			reconcileODownPull(o.odownPull, obs, now)
			o.publishLocked(QuorumStatusOK, "10.0.0.9:6379", 0, SourcePoll, now)
			o.mu.Unlock()
			now = now.Add(time.Second)
		}
	})

	// Pubsub writer: +odown / -odown mutate o.odown for sentinel-1 via
	// dispatch — the canonical edge-triggered path, also under o.mu.
	odownMsg := pubsubMessage{Channel: "+odown", Payload: "master vk0 10.0.0.9 6379"}
	clearMsg := pubsubMessage{Channel: "-odown", Payload: "master vk0 10.0.0.9 6379"}
	pubsubEp := o.endpoints[1]
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			o.dispatch(ctx, pubsubEp, odownMsg)
			o.dispatch(ctx, pubsubEp, clearMsg)
		}
	})

	// Lock-free reader: asserts the two maps never cross-contaminate.
	wg.Go(func() {
		for {
			p, _ := o.snapshot()
			if _, bad := p.ODown[pullKey]; bad {
				t.Errorf("pull-side key %q leaked into pubsub ODown map", pullKey)
				return
			}
			if _, bad := p.ODownPull[pubsubKey]; bad {
				t.Errorf("pubsub-side key %q leaked into pull ODownPull map", pubsubKey)
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	})

	wg.Wait()
}
