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
	"errors"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// TestManager_EnsureBeforeStart_SentinelError pins the
// ErrManagerNotStarted contract: Ensure invoked before
// Manager.Start has installed rootCtx returns an error matchable
// via errors.Is, so the reconciler can suppress repeated startup
// log spam and retry on the next reconcile.
func TestManager_EnsureBeforeStart_SentinelError(t *testing.T) {
	m := NewManager(nil, Options{})
	err := m.Ensure(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}},
	)
	if err == nil {
		t.Fatal("expected Ensure to error when manager hasn't started")
	}
	if !errors.Is(err, ErrManagerNotStarted) {
		t.Errorf("expected errors.Is(err, ErrManagerNotStarted) to be true; got %v", err)
	}
}

// TestManager_Ensure_NoOrphanedStartUnderConcurrentSwap pins the
// "start() inside the lock" invariant that prevents an observer's
// goroutines from being spawned AFTER the observer has been
// replaced in the map.
//
// The bug shape, before the fix:
//
//  1. Goroutine A enters Ensure(cr, v1):
//     a. Lock acquired
//     b. observer A installed in m.observers[cr]
//     c. Lock released
//     d. (about to call A.start())
//
//  2. Goroutine B enters Ensure(cr, v2) — different endpoints:
//     a. Lock acquired
//     b. prev = A, observer B replaces A in m.observers[cr]
//     c. Lock released
//     d. A.stop() — no-op (A.cancel is nil; A hasn't started yet)
//     e. B.start() — B's goroutines spawn
//
//  3. Goroutine A continues from 1d:
//     f. A.start() — spawns A's goroutines, but A is no longer
//     reachable from the map. A leaks until rootCtx cancels.
//
// The fix moves start() inside the Ensure lock, before the map
// install. After Ensure returns, every observer reachable from
// m.observers must have had start() called (cancel != nil). This
// test concurrently swaps endpoints on the same cr and asserts
// the in-map observer has a non-nil cancel func — the
// observable trace of start() having run under the lock.
func TestManager_Ensure_NoOrphanedStartUnderConcurrentSwap(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// Twelve fakes pre-loaded with PSUBSCRIBE acks + a few poll
	// replies; concurrent Ensure calls cycle through pairs.
	fakes := make([]*fakeSentinel, 12)
	for i := range fakes {
		fakes[i] = newFakeSentinel(t)
		t.Cleanup(fakes[i].Stop)
		queuePsubscribeAcks(fakes[i])
		for range 5 {
			queuePollReplies(fakes[i], false)
		}
	}

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eps := []Endpoint{
				{Name: "vk0-sentinel-0", Addr: fakes[(i*2)%len(fakes)].Addr()},
				{Name: "vk0-sentinel-1", Addr: fakes[(i*2+1)%len(fakes)].Addr()},
			}
			_ = m.Ensure(context.Background(), cr, "vk0", "", eps)
		}(i)
	}
	wg.Wait()

	m.obsMu.Lock()
	o, ok := m.observers[cr]
	m.obsMu.Unlock()
	if !ok {
		t.Fatal("expected observer in map after concurrent Ensure swaps")
	}
	if o.cancel == nil {
		t.Fatal("observer in map has nil cancel — start() was not called before map install; concurrent Ensure left an orphan observer that the map's drain on Stop will not see")
	}
}
