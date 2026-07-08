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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestBreakAddrTie pins the tie-break contract: a tie between equally-voted
// sentinel addresses resolves deterministically — the incumbent wins
// when present (no spurious relabel), otherwise the lexicographically
// smaller address — and the result is independent of the order the
// tied set is visited (map-iteration order).
func TestBreakAddrTie(t *testing.T) {
	// a < b < c lexicographically; fresh addresses (not reused
	// elsewhere in the package) and referenced by identifier so the
	// literals appear once each.
	const (
		a = "10.0.0.31:6379"
		b = "10.0.0.34:6379"
		c = "10.0.0.39:6379"
	)

	// Incumbent present in the pair always wins (either order).
	if got := breakAddrTie(a, b, b); got != b {
		t.Fatalf("incumbent b should win, got %q", got)
	}
	if got := breakAddrTie(b, a, b); got != b {
		t.Fatalf("incumbent b should win (swapped), got %q", got)
	}
	// No incumbent -> lexicographic min.
	if got := breakAddrTie(b, c, ""); got != b {
		t.Fatalf("no incumbent -> lexicographic min, got %q", got)
	}
	// Incumbent absent from the pair -> lexicographic min.
	if got := breakAddrTie(c, b, a); got != b {
		t.Fatalf("absent incumbent -> lexicographic min, got %q", got)
	}
	// Order-independence across a 3-way tie, incumbent present.
	fwd := breakAddrTie(breakAddrTie(a, b, b), c, b)
	rev := breakAddrTie(breakAddrTie(c, b, b), a, b)
	if fwd != b || rev != b {
		t.Fatalf("3-way tie incumbent b: fwd=%q rev=%q want b", fwd, rev)
	}
	// Order-independence, no incumbent -> lexicographic min a.
	fwd = breakAddrTie(breakAddrTie(a, b, ""), c, "")
	rev = breakAddrTie(breakAddrTie(c, b, ""), a, "")
	if fwd != a || rev != a {
		t.Fatalf("3-way tie no incumbent: fwd=%q rev=%q want a (lexicographic min)", fwd, rev)
	}
}

// queueCustomPoll queues one pull tick's three replies for a single
// sentinel with a tunable master address + quorum signal — used to
// build per-sentinel vote distributions the fixed-address harness
// can't express.
func queueCustomPoll(fs *fakeSentinel, ip, port string, epoch int64, quorumOK bool) {
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", buildArrayReply(ip, port))
	fs.QueueReply("SENTINEL MASTER", buildArrayReply(
		"name", "vk0", "ip", ip, "port", port, "config-epoch", itoa(int(epoch))))
	if quorumOK {
		fs.QueueReply("SENTINEL CKQUORUM", "+OK 3 usable Sentinels\r\n")
	} else {
		fs.QueueReply("SENTINEL CKQUORUM", "-NOQUORUM Quorum not reached\r\n")
	}
}

// TestObserver_QuorumRequiresSameSet pins the contract: quorum is OK
// only when a single threshold-sized set agrees on BOTH the winning
// address AND CKQUORUM. Here N=3 (threshold=2): two sentinels agree on
// address X and two report CKQUORUM-ok, but from DISJOINT subsets — no
// single ≥2 set agrees on both — so the snapshot must report quorum
// NOT-OK. (The all-agree happy path is covered by
// TestObserver_PullTickPublishesQuorumOK.)
func TestObserver_QuorumRequiresSameSet(t *testing.T) {
	fs0 := newFakeSentinel(t)
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs0.Stop(); fs1.Stop(); fs2.Stop() })
	for _, fs := range []*fakeSentinel{fs0, fs1, fs2} {
		queuePsubscribeAcks(fs)
	}
	const addrX, addrY = "10.0.0.41", "10.0.0.42"
	for range 40 {
		queueCustomPoll(fs0, addrX, "6379", 42, true)  // addr X, quorum ok
		queueCustomPoll(fs1, addrX, "6379", 42, false) // addr X, quorum NO
		queueCustomPoll(fs2, addrY, "6379", 42, true)  // addr Y, quorum ok
	}

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs0.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-2", Addr: fs2.Addr()},
		},
		Options{PollInterval: 50 * time.Millisecond, PubsubReadDeadline: 30 * time.Second, PingTimeout: time.Second},
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	got := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		return p.Source == SourcePoll || p.Source == SourceMerge
	})
	if got.QuorumOK {
		t.Fatalf("quorum must be NOT-OK when address-agreement and CKQUORUM-agreement come from disjoint subsets; got QuorumOK=true addr=%q", got.Addr)
	}
}

// TestObserver_SwitchMasterChainGuard pins the chain guard: a
// +switch-master frame is applied only when it chains from the
// currently-believed primary (ev.OldAddr == prev.Addr); a stale or
// replayed frame that switches away from a no-longer-current address
// is dropped, so a delayed pubsub frame can't move the published Addr
// backward between pull ticks.
func TestObserver_SwitchMasterChainGuard(t *testing.T) {
	const (
		addr2 = "10.0.0.22:6379"
		addr3 = "10.0.0.23:6379"
		addr5 = "10.0.0.25:6379"
	)
	opts := Options{PollInterval: 100 * time.Hour, PubsubReadDeadline: 30 * time.Second, PingTimeout: time.Second}
	o, fakes, teardown := startObserverWithFakes(t, opts, 1, false)
	defer teardown()
	waitForSubscriber(t, fakes[0])

	// From an empty primary the first frame is accepted (nothing to
	// chain from) and establishes the primary at addr2.
	pushPmessage(fakes[0], "+switch-master", "vk0 10.0.0.21 6379 10.0.0.22 6379")
	waitForSnapshot(t, o, func(p ObservedPrimary) bool { return p.Addr == addr2 })

	// A frame that chains from the current primary is applied: 2 -> 3.
	pushPmessage(fakes[0], "+switch-master", "vk0 10.0.0.22 6379 10.0.0.23 6379")
	waitForSnapshot(t, o, func(p ObservedPrimary) bool { return p.Addr == addr3 })

	// A stale/replayed frame switches FROM 10.0.0.21 (no longer
	// current) and must be DROPPED. We then push a frame chaining from
	// the real current primary (addr3 -> addr5); it can only land if
	// the stale frame did NOT move the Addr to 10.0.0.29 (which would
	// have broken the chain and stranded the Addr there).
	pushPmessage(fakes[0], "+switch-master", "vk0 10.0.0.21 6379 10.0.0.29 6379") // stale -> dropped
	pushPmessage(fakes[0], "+switch-master", "vk0 10.0.0.23 6379 10.0.0.25 6379") // chains from real current
	got := waitForSnapshot(t, o, func(p ObservedPrimary) bool { return p.Addr == addr5 })
	if got.Addr != addr5 {
		t.Fatalf("expected Addr=%s after chaining frame; the stale frame was likely applied. got %q", addr5, got.Addr)
	}
}
