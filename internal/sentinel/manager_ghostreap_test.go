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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestSelectGhostReapTarget pins the ghost-reap policy applied on top of
// the pure ghost detection: the time-based debounce (a ghost must be
// continuously absent ≥ ghostReapDebounce before it is reap-eligible —
// closing the cache-lag false-positive window), the one-at-a-time cap,
// and the AllowGhostReap veto. Reaping is healthy-state hygiene on
// debounce alone — there is deliberately NO demand-gate (a ghost
// carried into a dead-master incident inflates the failover election
// majority exactly when +odown vetoes reaping). Drives an injected
// clock so no wall-clock sleep is needed.
func TestSelectGhostReapTarget(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	const ghostIP = "10.0.0.9"
	liveIPs := func() map[string]struct{} {
		return map[string]struct{}{"10.0.0.0": {}, "10.0.0.1": {}, "10.0.0.2": {}}
	}
	holder := func(name string, ghosts ...string) GhostHolder {
		return GhostHolder{Endpoint: Endpoint{Name: name, Addr: name + ":26379"}, GhostIPs: ghosts}
	}
	// withClock builds a Manager whose clock is the returned setter's value.
	withClock := func() (*Manager, *time.Time) {
		m := NewManager(nil, Options{})
		now := time.Unix(1000, 0)
		m.clock = func() time.Time { return now }
		return m, &now
	}

	t.Run("absent under debounce is not eligible", func(t *testing.T) {
		m, _ := withClock()
		got := m.selectGhostReapTarget(cr, []GhostHolder{holder("s0", ghostIP)}, liveIPs(), true)
		if got != nil {
			t.Fatalf("expected nil within the debounce window, got %q", got.Name)
		}
	})

	t.Run("absent past debounce is eligible", func(t *testing.T) {
		m, now := withClock()
		holders := []GhostHolder{holder("s0", ghostIP)}
		if got := m.selectGhostReapTarget(cr, holders, liveIPs(), true); got != nil {
			t.Fatalf("pass1 stamps first-seen; expected nil, got %q", got.Name)
		}
		*now = now.Add(ghostReapDebounce + time.Second)
		got := m.selectGhostReapTarget(cr, holders, liveIPs(), true)
		if got == nil || got.Name != "s0" {
			t.Fatalf("expected s0 reap-eligible past debounce, got %v", got)
		}
	})

	t.Run("reappearing IP restarts the debounce", func(t *testing.T) {
		m, now := withClock()
		holders := []GhostHolder{holder("s0", ghostIP)}
		m.selectGhostReapTarget(cr, holders, liveIPs(), true) // stamp at t0
		// The IP reappears in the live set → pruned from the debounce map.
		*now = now.Add(10 * time.Second)
		live := liveIPs()
		live[ghostIP] = struct{}{}
		m.selectGhostReapTarget(cr, nil, live, true)
		// It departs again; the original stamp is gone, so even well past
		// the original window the fresh debounce has not elapsed.
		*now = now.Add(ghostReapDebounce)
		if got := m.selectGhostReapTarget(cr, holders, liveIPs(), true); got != nil {
			t.Fatalf("reappear must restart the debounce; expected nil, got %q", got.Name)
		}
		*now = now.Add(ghostReapDebounce + time.Second)
		if got := m.selectGhostReapTarget(cr, holders, liveIPs(), true); got == nil {
			t.Fatalf("expected eligible after the fresh debounce window elapsed")
		}
	})

	t.Run("sub-majority ghost is still reaped (healthy-state hygiene)", func(t *testing.T) {
		m, now := withClock()
		// The live sentinels could still outvote this single ghost, but
		// it is reaped anyway — the old demand-gate deferred exactly
		// until an incident set +odown, which vetoes reaping via
		// AllowGhostReap. Debounced ghosts go.
		holders := []GhostHolder{holder("s0", ghostIP)}
		m.selectGhostReapTarget(cr, holders, liveIPs(), true)
		*now = now.Add(ghostReapDebounce + time.Second)
		got := m.selectGhostReapTarget(cr, holders, liveIPs(), true)
		if got == nil || got.Name != "s0" {
			t.Fatalf("debounced ghost must be reaped regardless of election math, got %v", got)
		}
	})

	t.Run("caps to one target, lowest name", func(t *testing.T) {
		m, now := withClock()
		holders := []GhostHolder{
			holder("s2", ghostIP),
			holder("s0", "10.0.0.8"),
			holder("s1", "10.0.0.7"),
		}
		m.selectGhostReapTarget(cr, holders, liveIPs(), true)
		*now = now.Add(ghostReapDebounce + time.Second)
		got := m.selectGhostReapTarget(cr, holders, liveIPs(), true)
		if got == nil || got.Name != "s0" {
			t.Fatalf("expected exactly one target (s0, lowest name), got %v", got)
		}
	})

	t.Run("AllowGhostReap=false never returns a target", func(t *testing.T) {
		m, now := withClock()
		holders := []GhostHolder{holder("s0", ghostIP)}
		m.selectGhostReapTarget(cr, holders, liveIPs(), false)
		*now = now.Add(ghostReapDebounce + time.Second)
		if got := m.selectGhostReapTarget(cr, holders, liveIPs(), false); got != nil {
			t.Fatalf("a brewing election (+odown) must veto the reap, got %q", got.Name)
		}
	})
}

// TestRecoverStrandedSentinels_GhostReapDebounceGated drives the full
// per-reconcile path: a gossiping survivor that still knows a dead ghost
// peer is left alone on the first pass (within the debounce window) and
// reaped exactly once (REMOVE + MONITOR) on a later pass past the window.
// The live sentinel-IP set is derived from the endpoint host (loopback
// here), so the 10.0.0.9 peer is correctly a ghost.
func TestRecoverStrandedSentinels_GhostReapDebounceGated(t *testing.T) {
	fs := newFakeSentinel(t) // survivor holding a ghost peer
	defer fs.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	ghost := PeerInfo{Name: "ghost", IP: "10.0.0.9", Port: 26379, RunID: "rid-ghost"}
	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	now := time.Unix(2000, 0)
	m.clock = func() time.Time { return now }

	target := InitialResetTarget{
		CR:             types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:     "vk0",
		MasterIP:       masterHost,
		Port:           masterPort,
		Quorum:         1,
		Endpoints:      []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		AllowGhostReap: true,
	}

	// Pass 1: first sighting of the ghost — inside the debounce window.
	queueSentinelsReply(fs, ghost)
	m.RecoverStrandedSentinels(context.Background(), target, false)
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Fatalf("pass1 within debounce must not REMOVE; sent: %v", fs.Sent())
		}
	}

	// Pass 2: past the debounce window — exactly one REMOVE + MONITOR.
	now = now.Add(ghostReapDebounce + time.Second)
	queueSentinelsReply(fs, ghost)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	out := m.RecoverStrandedSentinels(context.Background(), target, false)

	var sawRemove, sawMonitor bool
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			sawRemove = true
		}
		if strings.HasPrefix(line, "SENTINEL MONITOR") {
			sawMonitor = true
		}
	}
	if !sawRemove || !sawMonitor {
		t.Fatalf("pass2 past debounce must REMOVE+MONITOR the ghost-holder; sent: %v", fs.Sent())
	}
	if len(out.Stranded) != 1 || out.Stranded[0] != "vk0-sentinel-0" {
		t.Fatalf("expected the ghost-holder in out.Stranded, got %v", out.Stranded)
	}
}

// TestRecoverStrandedSentinels_GhostReapVetoedWhenDisallowed pins the
// ODown veto: with AllowGhostReap=false (the controller saw the master
// +odown) a gossiping survivor is never touched, even past the debounce
// window.
func TestRecoverStrandedSentinels_GhostReapVetoedWhenDisallowed(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())
	ghost := PeerInfo{Name: "ghost", IP: "10.0.0.9", Port: 26379, RunID: "rid-ghost"}

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	now := time.Unix(2000, 0)
	m.clock = func() time.Time { return now }
	target := InitialResetTarget{
		CR:             types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:     "vk0",
		MasterIP:       masterHost,
		Port:           masterPort,
		Quorum:         1,
		Endpoints:      []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		AllowGhostReap: false,
	}
	queueSentinelsReply(fs, ghost)
	m.RecoverStrandedSentinels(context.Background(), target, false)
	now = now.Add(ghostReapDebounce + time.Second)
	queueSentinelsReply(fs, ghost)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	out := m.RecoverStrandedSentinels(context.Background(), target, false)
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Fatalf("AllowGhostReap=false must never REMOVE a survivor; sent: %v", fs.Sent())
		}
	}
	if len(out.Stranded) != 0 {
		t.Fatalf("no reap expected when disallowed, got %v", out.Stranded)
	}
}

// TestRemove_ClearsGhostReapDebounce pins the teardown half of the
// ghost-reap debounce: Remove drops the CR's ghostSeen state — even
// when no observer was ever Ensure'd for it (the state is fed by
// surgery passes, not the observer) — so a recreated same-name CR's
// first re-detected ghost restarts the debounce from zero instead of
// inheriting the dead CR's first-seen stamp and being instantly
// reap-eligible.
func TestRemove_ClearsGhostReapDebounce(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	m := NewManager(nil, Options{})
	now := time.Unix(1000, 0)
	m.clock = func() time.Time { return now }
	holders := []GhostHolder{{Endpoint: Endpoint{Name: "s0", Addr: "s0:26379"}, GhostIPs: []string{"10.0.0.9"}}}
	live := map[string]struct{}{"10.0.0.0": {}}

	// A surgery pass stamps the ghost's first-seen time.
	m.selectGhostReapTarget(cr, holders, live, true)
	if _, ok := m.ghostSeen[cr]; !ok {
		t.Fatalf("precondition: the pass must stamp a ghostSeen entry")
	}

	// CR deleted (never Ensure'd — Remove must still clear).
	m.Remove(cr)
	if _, ok := m.ghostSeen[cr]; ok {
		t.Fatalf("Remove must drop the CR's ghostSeen state")
	}

	// Recreated same-name CR: well past the ORIGINAL stamp's window the
	// re-detected ghost must NOT be reap-eligible — the debounce
	// restarts from this pass's fresh stamp.
	now = now.Add(ghostReapDebounce + time.Second)
	if got := m.selectGhostReapTarget(cr, holders, live, true); got != nil {
		t.Fatalf("a recreated CR must restart the ghost debounce from zero; got %q", got.Name)
	}
	now = now.Add(ghostReapDebounce + time.Second)
	if got := m.selectGhostReapTarget(cr, holders, live, true); got == nil {
		t.Fatalf("the fresh debounce window elapsed; the ghost must be reap-eligible")
	}
}
