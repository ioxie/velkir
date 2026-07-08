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

// startObserverWithFakes wires an observer to two fake sentinels
// already pre-loaded with PSUBSCRIBE acks and N poll replies, and
// returns the observer + its teardown closure. opts is used as-is.
func startObserverWithFakes(t *testing.T, opts Options, polls int, quorumOK bool) (*observer, []*fakeSentinel, func()) {
	t.Helper()
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs1.Stop(); fs2.Stop() })

	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		for range polls {
			queuePollReplies(fs, quorumOK)
		}
	}

	rec := k8sevents.NewFakeRecorder(64)
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0",
		"",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		opts,
		rec,
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	o.start(ctx)
	teardown := func() {
		cancel()
		o.stop()
	}
	return o, []*fakeSentinel{fs1, fs2}, teardown
}

// queuePsubscribeAcks bundles the seven psubscribe-ack arrays into
// ONE QueueReply entry under the "PSUBSCRIBE" key — sentinel sends
// all seven back-to-back in response to one PSUBSCRIBE command.
func queuePsubscribeAcks(fs *fakeSentinel) {
	patterns := []string{
		"+switch-master", "+failover-end", "+failover-end-for-timeout",
		"+odown", "-odown", "+tilt", "-tilt",
	}
	var sb strings.Builder
	for i, p := range patterns {
		sb.WriteString(buildPsubscribeAck(p, i+1))
	}
	fs.QueueReply("PSUBSCRIBE", sb.String())
}

// queuePollReplies queues the three replies one pull tick consumes
// from a single sentinel: GET-MASTER-ADDR-BY-NAME (addr),
// SENTINEL MASTER (epoch — config-epoch=42 for happy-path), and
// CKQUORUM (quorum signal). Address is hardcoded to the canonical
// test value since every test uses the same one (any unique stable
// addr works — sentinel doesn't care, the observer compares
// addresses for equality).
func queuePollReplies(fs *fakeSentinel, quorumOK bool) {
	queuePollRepliesWithEpoch(fs, 42, quorumOK)
}

// queuePollRepliesWithEpoch is queuePollReplies with a tunable epoch
// for tests that want to assert ObservedPrimary.Epoch propagation.
func queuePollRepliesWithEpoch(fs *fakeSentinel, epoch int64, quorumOK bool) {
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", buildArrayReply("10.0.0.7", "6379"))
	fs.QueueReply("SENTINEL MASTER", buildArrayReply(
		"name", "vk0",
		"ip", "10.0.0.7",
		"port", "6379",
		"config-epoch", itoa(int(epoch)),
	))
	if quorumOK {
		fs.QueueReply("SENTINEL CKQUORUM", "+OK 3 usable Sentinels\r\n")
	} else {
		fs.QueueReply("SENTINEL CKQUORUM", "-NOQUORUM Quorum not reached\r\n")
	}
}

func buildArrayReply(parts ...string) string {
	var sb strings.Builder
	sb.WriteByte('*')
	sb.WriteString(itoa(len(parts)))
	sb.WriteString("\r\n")
	for _, p := range parts {
		sb.WriteByte('$')
		sb.WriteString(itoa(len(p)))
		sb.WriteString("\r\n")
		sb.WriteString(p)
		sb.WriteString("\r\n")
	}
	return sb.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// queuePollRepliesWithMaster queues one pull tick's three replies with a
// caller-supplied SENTINEL MASTER body — used to script topology-count
// fields (num-slaves / num-other-sentinels) and malformed replies.
func queuePollRepliesWithMaster(fs *fakeSentinel, masterReply string, quorumOK bool) {
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", buildArrayReply("10.0.0.7", "6379"))
	fs.QueueReply("SENTINEL MASTER", masterReply)
	if quorumOK {
		fs.QueueReply("SENTINEL CKQUORUM", "+OK 3 usable Sentinels\r\n")
	} else {
		fs.QueueReply("SENTINEL CKQUORUM", "-NOQUORUM Quorum not reached\r\n")
	}
}

// waitForEndpointObs polls the observer's published per-endpoint
// snapshot until cond holds or 3s elapses.
func waitForEndpointObs(t *testing.T, o *observer, cond func([]EndpointObservation) bool) []EndpointObservation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last []EndpointObservation
	for time.Now().Before(deadline) {
		if v, ok := o.endpointObs.Load().([]EndpointObservation); ok {
			last = v
			if cond(v) {
				return v
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("endpoint-observation condition not satisfied within deadline; last: %+v", last)
	return nil
}

func endpointObsByName(obs []EndpointObservation, name string) *EndpointObservation {
	for i := range obs {
		if obs[i].Name == name {
			return &obs[i]
		}
	}
	return nil
}

// TestQueryOnePublishesTopologyCounts pins the zero-extra-round-trip
// piggyback: the num-slaves / num-other-sentinels counts ride on the
// SENTINEL MASTER reply queryOne already reads. A well-formed reply
// yields CountsValid=true with both counts; a reply missing num-slaves
// yields CountsValid=false with the counts to be ignored; and the pull
// tick issues no SENTINEL REPLICAS command.
func TestQueryOnePublishesTopologyCounts(t *testing.T) {
	fsGood := newFakeSentinel(t)
	fsBad := newFakeSentinel(t)
	t.Cleanup(func() { fsGood.Stop(); fsBad.Stop() })

	queuePsubscribeAcks(fsGood)
	queuePsubscribeAcks(fsBad)
	goodMaster := buildArrayReply(
		"name", "vk0", "ip", "10.0.0.7", "port", "6379",
		"config-epoch", "42", "num-slaves", "2", "num-other-sentinels", "2",
	)
	// Missing num-slaves → ParseSentinelMasterNumSlaves fails → CountsValid=false.
	badMaster := buildArrayReply(
		"name", "vk0", "ip", "10.0.0.7", "port", "6379",
		"config-epoch", "42", "num-other-sentinels", "2",
	)
	for range 20 {
		queuePollRepliesWithMaster(fsGood, goodMaster, true)
		queuePollRepliesWithMaster(fsBad, badMaster, true)
	}

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fsGood.Addr()},
			{Name: "vk0-sentinel-1", Addr: fsBad.Addr()},
		},
		Options{PollInterval: 50 * time.Millisecond, PubsubReadDeadline: 30 * time.Second, PingTimeout: time.Second},
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	obs := waitForEndpointObs(t, o, func(v []EndpointObservation) bool {
		g := endpointObsByName(v, "vk0-sentinel-0")
		b := endpointObsByName(v, "vk0-sentinel-1")
		return g != nil && b != nil && g.CountsValid && !g.At.IsZero() && !b.At.IsZero()
	})

	good := endpointObsByName(obs, "vk0-sentinel-0")
	if !good.CountsValid || good.KnownReplicas != 2 || good.KnownSentinels != 2 {
		t.Errorf("good endpoint = {valid:%v replicas:%d sentinels:%d}; want {true 2 2}",
			good.CountsValid, good.KnownReplicas, good.KnownSentinels)
	}
	bad := endpointObsByName(obs, "vk0-sentinel-1")
	if bad.CountsValid {
		t.Errorf("bad endpoint CountsValid=true; want false (num-slaves missing)")
	}

	// The pull tick must not issue SENTINEL REPLICAS.
	for _, cmd := range fsGood.Sent() {
		if strings.Contains(strings.ToUpper(cmd), "SENTINEL REPLICAS") {
			t.Errorf("pull tick issued %q; topology counts must ride the SENTINEL MASTER reply", cmd)
		}
	}
	sent := strings.Join(fsGood.Sent(), "\n")
	for _, want := range []string{"SENTINEL GET-MASTER-ADDR-BY-NAME", "SENTINEL MASTER", "SENTINEL CKQUORUM"} {
		if !strings.Contains(strings.ToUpper(sent), want) {
			t.Errorf("pull tick did not issue %q; sent=%v", want, fsGood.Sent())
		}
	}
}

func TestObserver_PullTickPublishesQuorumOK(t *testing.T) {
	opts := Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o, _, teardown := startObserverWithFakes(t, opts, 5, true)
	defer teardown()

	got := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		return p.QuorumOK && p.Addr == "10.0.0.7:6379"
	})
	if got.Source == SourceNone {
		t.Errorf("expected Source != none after first poll, got %q", got.Source)
	}
	// queuePollReplies seeds config-epoch=42; pollOnce takes the
	// max across reachable sentinels (both fakes serve 42), so
	// the snapshot must show 42 — pins the contract that
	// ObservedPrimary.Epoch carries a real value rather than the
	// boot-time zero.
	if got.Epoch != 42 {
		t.Errorf("expected Epoch=42 from poll, got %d", got.Epoch)
	}
}

func TestObserver_EpochOnlyAdvances(t *testing.T) {
	// Drive two consecutive ticks where the second sentinel
	// reports a stale epoch; the snapshot must keep the higher
	// value from the first tick — defensive against a single
	// sentinel lagging the cluster after a fresh failover.
	t.Helper()
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs1.Stop(); fs2.Stop() })

	queuePsubscribeAcks(fs1)
	queuePsubscribeAcks(fs2)
	queuePollRepliesWithEpoch(fs1, 7, true)
	queuePollRepliesWithEpoch(fs2, 7, true)
	// Second tick: fs2 regresses to epoch 5.
	queuePollRepliesWithEpoch(fs1, 7, true)
	queuePollRepliesWithEpoch(fs2, 5, true)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		Options{
			PollInterval:       50 * time.Millisecond,
			PubsubReadDeadline: 30 * time.Second,
			PingTimeout:        time.Second,
		},
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	// Wait through at least the second tick; epoch must remain 7.
	deadline := time.Now().Add(2 * time.Second)
	var last ObservedPrimary
	for time.Now().Before(deadline) {
		p, ok := o.snapshot()
		if ok {
			last = p
			if p.Epoch == 7 && p.Source != SourceNone {
				// Sleep one extra tick to catch a regression.
				time.Sleep(120 * time.Millisecond)
				p2, _ := o.snapshot()
				if p2.Epoch < 7 {
					t.Fatalf("epoch regressed: %d → %d", p.Epoch, p2.Epoch)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("did not reach Epoch=7 within deadline; last=%+v", last)
}

func TestObserver_PullTickDegradesOnNoQuorum(t *testing.T) {
	opts := Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o, _, teardown := startObserverWithFakes(t, opts, 5, false)
	defer teardown()

	waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		return p.Source != SourceNone && !p.QuorumOK
	})
}

func TestObserver_PubsubSwitchMasterUpdatesAddr(t *testing.T) {
	opts := Options{
		PollInterval:       100 * time.Hour, // disable pull
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	// One poll fires immediately on observer start (degraded
	// reply lands first); we need at least 1 poll-reply queued
	// so that initial tick doesn't error.
	o, fakes, teardown := startObserverWithFakes(t, opts, 1, false)
	defer teardown()

	// Wait until the observer has registered as a subscriber on
	// fakes[0] before pushing — otherwise the Push fans out to
	// zero connections and the test times out.
	waitForSubscriber(t, fakes[0])

	pushPmessage(fakes[0], "+switch-master", "vk0 10.0.0.7 6379 10.0.0.9 6379")

	waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		return p.Addr == "10.0.0.9:6379" && p.Source == SourcePubsub
	})
}

func TestObserver_ODownMapTracksPerSentinel(t *testing.T) {
	opts := Options{
		PollInterval:       100 * time.Hour, // disable pull
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o, fakes, teardown := startObserverWithFakes(t, opts, 1, false)
	defer teardown()
	waitForSubscriber(t, fakes[0])

	pushPmessage(fakes[0], "+odown", "master vk0 10.0.0.7 6379")

	snap := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		_, ok := p.ODown["vk0-sentinel-0"]
		return ok
	})
	if _, ok := snap.ODown["vk0-sentinel-1"]; ok {
		t.Errorf("expected sentinel-1 NOT in ODown map, got %v", snap.ODown)
	}

	pushPmessage(fakes[0], "-odown", "master vk0 10.0.0.7 6379")
	waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		_, ok := p.ODown["vk0-sentinel-0"]
		return !ok
	})
}

// pushPmessage assembles and sends a +switch-master / +odown / etc.
// pmessage frame to every subscribed connection on fs.
func pushPmessage(fs *fakeSentinel, channel, payload string) {
	fs.Push(buildPmessage(channel, payload))
}

// waitForSubscriber spins until at least one connection has
// completed PSUBSCRIBE on fs (so that subsequent Push calls have
// a target). Bounded to 2s so misconfigured tests fail fast.
func waitForSubscriber(t *testing.T, fs *fakeSentinel) {
	t.Helper()
	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return len(fs.subscribers) > 0
	}, 20*time.Millisecond,
		"no subscriber connection registered on fake")
}

// waitForSnapshot polls o.snapshot() until cond returns true or
// 2s elapses. Bounded to keep test runtimes short under -race.
func waitForSnapshot(t *testing.T, o *observer, cond func(ObservedPrimary) bool) ObservedPrimary {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last ObservedPrimary
	for time.Now().Before(deadline) {
		p, ok := o.snapshot()
		if ok {
			last = p
			if cond(p) {
				return p
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("snapshot condition not satisfied within deadline; last value: %+v", last)
	return ObservedPrimary{}
}

func TestObserver_SubscribeReturnsPromptlyOnCancel(t *testing.T) {
	// Pins the load-bearing invariant that ctx cancellation
	// force-closes the pubsub conn so subscribeOnce returns
	// immediately rather than sleeping the 30s
	// PubsubReadDeadline. Pull is disabled, so the only goroutine
	// the cancel must drain is the subscribe loop sitting in a
	// blocking pubsub read.
	fs := newFakeSentinel(t)
	t.Cleanup(fs.Stop)
	queuePsubscribeAcks(fs)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		Options{
			PollInterval:       100 * time.Hour,
			PubsubReadDeadline: 30 * time.Second,
			PingTimeout:        time.Second,
		},
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	o.start(ctx)
	waitForSubscriber(t, fs)

	cancel()
	done := make(chan struct{})
	go func() {
		o.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("observer.stop did not return within 2s of ctx cancel; the pubsub conn-close-on-cancel mechanism is not unblocking the subscribe read")
	}
}
