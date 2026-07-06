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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// startManager runs Manager.Start in a goroutine and waits for
// rootCtx to be set so subsequent Ensure calls don't race the
// "manager not started" guard. Returns the cancel func to stop
// the manager and a wait func for tests to assert clean shutdown.
func startManager(t *testing.T) (*Manager, context.CancelFunc, func()) {
	t.Helper()
	rec := k8sevents.NewFakeRecorder(64)
	m := NewManager(rec, Options{PollInterval: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = m.Start(ctx)
		close(done)
	}()

	// Wait until rootCtx is set (Start lazily installs it). The
	// Cleanup-registered cancel guarantees the Start goroutine
	// does not leak past the test even when eventually() fails
	// the test before this helper returns the cancel func to its
	// caller — Fatalf inside eventually triggers Goexit, which
	// executes Cleanup but skips the un-deferred return path.
	// Callers also defer cancel() themselves; CancelFunc is
	// idempotent so the second call is a no-op.
	t.Cleanup(cancel)
	rootCtxReady := func() bool {
		m.obsMu.Lock()
		defer m.obsMu.Unlock()
		return m.rootCtx != nil
	}
	eventually(t, rootCtxReady, 5*time.Millisecond,
		"manager Start did not install rootCtx")

	wait := func() {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("manager Start did not return after context cancel")
		}
	}
	return m, cancel, wait
}

func TestManager_NeedLeaderElection(t *testing.T) {
	m := NewManager(nil, Options{})
	if !m.NeedLeaderElection() {
		t.Error("manager must require leader election to avoid double-load on sentinels from non-leader replicas")
	}
}

func TestManager_EnsureBeforeStart(t *testing.T) {
	m := NewManager(nil, Options{})
	err := m.Ensure(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}},
	)
	if err == nil {
		t.Fatal("expected Ensure to error when manager hasn't started")
	}
}

func TestManager_EnsureValidatesArgs(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	if err := m.Ensure(context.Background(), cr, "", "",
		[]Endpoint{{Name: "s0", Addr: "127.0.0.1:1"}}); err == nil {
		t.Error("expected error on empty masterName")
	}
	if err := m.Ensure(context.Background(), cr, "vk0", "", nil); err == nil {
		t.Error("expected error on empty endpoints slice")
	}
}

func TestManager_EnsureIdempotent(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		// Pre-queue plenty of poll replies — the manager test
		// runs the observer for ~200ms (multiple ticks) and we
		// don't care about the details, only that no panic.
		for range 20 {
			queuePollReplies(fs, true)
		}
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}
	if err := m.Ensure(context.Background(), cr, "vk0", "", endpoints); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	if !m.Has(cr) {
		t.Fatal("expected manager to have observer after Ensure")
	}
	// Capture the observer pointer; Ensure must not replace it
	// when called again with identical config.
	m.obsMu.Lock()
	first := m.observers[cr]
	m.obsMu.Unlock()

	if err := m.Ensure(context.Background(), cr, "vk0", "", endpoints); err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	m.obsMu.Lock()
	second := m.observers[cr]
	m.obsMu.Unlock()
	if first != second {
		t.Errorf("Ensure replaced live observer on identical config; want pointer-equal")
	}
}

func TestManager_EnsureReplacesOnEndpointChange(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	fs3 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	defer fs3.Stop()
	for _, fs := range []*fakeSentinel{fs1, fs2, fs3} {
		queuePsubscribeAcks(fs)
		for range 20 {
			queuePollReplies(fs, true)
		}
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := m.Ensure(context.Background(), cr, "vk0", "", []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	m.obsMu.Lock()
	first := m.observers[cr]
	m.obsMu.Unlock()

	// Swap fs2 → fs3 in the endpoint list.
	if err := m.Ensure(context.Background(), cr, "vk0", "", []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-2", Addr: fs3.Addr()},
	}); err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	m.obsMu.Lock()
	second := m.observers[cr]
	m.obsMu.Unlock()
	if first == second {
		t.Error("Ensure must replace observer when endpoints change")
	}
}

func TestManager_RemoveCancelsObserver(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		for range 20 {
			queuePollReplies(fs, true)
		}
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := m.Ensure(context.Background(), cr, "vk0", "", []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Wait until the observer has actually published a snapshot
	// — guarantees its goroutines are live before Remove tries
	// to cancel them.
	eventually(t, func() bool { return m.Snapshot(cr).Present },
		10*time.Millisecond,
		"observer did not publish a snapshot before Remove")

	m.Remove(cr)
	if m.Has(cr) {
		t.Error("expected manager to forget observer after Remove")
	}
	// Remove on a missing CR is idempotent.
	m.Remove(cr)

	s := m.Snapshot(cr)
	if s.Present {
		t.Error("expected !Present snapshot for removed observer")
	}
}
func TestManager_RunInitialReset_FiresInitialEventsPerCR(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	// Each CR needs a stranded (empty peer-list) sentinel paired with
	// a survivor (non-empty peer-list) so the safety-net selectivity
	// rule fires RESET on the stranded one and emits the per-CR
	// InitialSentinelReset event.
	vk0Stranded := newFakeSentinel(t)
	vk0Survivor := newFakeSentinel(t)
	defer vk0Stranded.Stop()
	defer vk0Survivor.Stop()
	queueSentinelsReply(vk0Stranded /* empty */)
	queueSentinelsReply(vk0Survivor, PeerInfo{Name: "p", IP: "10.0.0.99", Port: 26379, RunID: "p1"})
	vk0Stranded.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	vk0Stranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	vk1Stranded := newFakeSentinel(t)
	vk1Survivor := newFakeSentinel(t)
	defer vk1Stranded.Stop()
	defer vk1Survivor.Stop()
	queueSentinelsReply(vk1Stranded /* empty */)
	queueSentinelsReply(vk1Survivor, PeerInfo{Name: "p", IP: "10.0.0.98", Port: 26379, RunID: "p2"})
	vk1Stranded.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	vk1Stranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	targets := []InitialResetTarget{
		{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-0", Addr: vk0Stranded.Addr()},
				{Name: "vk0-sentinel-1", Addr: vk0Survivor.Addr()},
			},
			MasterIP: "10.0.0.5",
			Port:     6379,
			Quorum:   2,
		},
		{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk1"},
			MasterName: "vk1",
			Endpoints: []Endpoint{
				{Name: "vk1-sentinel-0", Addr: vk1Stranded.Addr()},
				{Name: "vk1-sentinel-1", Addr: vk1Survivor.Addr()},
			},
			MasterIP: "10.0.0.6",
			Port:     6379,
			Quorum:   2,
		},
		{
			CR:        types.NamespacedName{Namespace: "ns", Name: "vk-empty"},
			Endpoints: nil, // skipped — no event
		},
	}
	m.RunInitialReset(context.Background(), targets)

	rec := m.recorder.(*k8sevents.FakeRecorder)
	got := drainEvents(rec)
	initialEvents := 0
	for _, e := range got {
		if strings.Contains(e, "InitialSentinelReset") {
			initialEvents++
		}
	}
	if initialEvents != 2 {
		t.Errorf("expected 2 InitialSentinelReset events (vk0+vk1), got %d (events: %v)", initialEvents, got)
	}
}

// TestManager_emitTuningEvents_WarnsOnlyOnFailure pins the contract:
// a failed post-MONITOR tuning restore surfaces a Warning event naming
// the endpoint (rather than being silently dropped), while successful
// restores stay silent to avoid event spam.
func TestManager_emitTuningEvents_WarnsOnlyOnFailure(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	m.emitTuningEvents(cr, []TuningResult{
		{Name: "vk0-sentinel-0", Err: nil},                        // success — silent
		{Name: "vk0-sentinel-1", Err: errors.New("dial timeout")}, // failure — Warning
	})

	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 event (only the failed endpoint), got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "SentinelTuningFailed") {
		t.Errorf("event reason should be SentinelTuningFailed; got %q", got[0])
	}
	if !strings.Contains(got[0], "vk0-sentinel-1") {
		t.Errorf("event should name the failed endpoint; got %q", got[0])
	}
	if !strings.Contains(got[0], "Warning") {
		t.Errorf("tuning-failure event must be a Warning; got %q", got[0])
	}
}

func drainEvents(rec *k8sevents.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestManager_IssueAuthPass_EmptyPasswordIsNoOp(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}}
	results := m.IssueAuthPass(context.Background(), cr, "mymaster", "", endpoints)

	if results != nil {
		t.Errorf("expected nil results for empty password, got %v", results)
	}
	rec := m.recorder.(*k8sevents.FakeRecorder)
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("expected zero events for no-op IssueAuthPass, got %v", got)
	}
}

func TestManager_IssueAuthPass_FiresAppliedOnSuccess(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	queueAuthPassReplies(fs1)
	queueAuthPassReplies(fs2)

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}
	results := m.IssueAuthPass(context.Background(), cr, testMasterName, testPassword, endpoints)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("%s: unexpected err %v", r.Name, r.Err)
		}
	}
	rec := m.recorder.(*k8sevents.FakeRecorder)
	got := drainEvents(rec)
	gotApplied := 0
	for _, e := range got {
		if strings.Contains(e, "SentinelAuthApplied") {
			gotApplied++
		}
		if strings.Contains(e, "SentinelAuthNotApplied") {
			t.Errorf("NotApplied must not fire on full success: %s", e)
		}
	}
	if gotApplied != 2 {
		t.Errorf("expected 2 SentinelAuthApplied events, got %d (events: %v)", gotApplied, got)
	}
}

func TestManager_IssueAuthPass_FiresNotAppliedOnFailure(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}, // unreachable; dial fails on every retry
	}
	results := m.IssueAuthPass(context.Background(), cr, testMasterName, testPassword, endpoints)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected dial-failure error after retries")
	}
	rec := m.recorder.(*k8sevents.FakeRecorder)
	got := drainEvents(rec)
	gotNotApplied := 0
	for _, e := range got {
		if strings.Contains(e, "SentinelAuthNotApplied") {
			gotNotApplied++
		}
	}
	if gotNotApplied != 1 {
		t.Errorf("expected 1 SentinelAuthNotApplied event, got %d (events: %v)", gotNotApplied, got)
	}
}
func TestManager_RunInitialReset_AlsoFiresAuthPassPropagation(t *testing.T) {
	m, cancel, wait := startManager(t)
	defer wait()
	defer cancel()

	// Stranded sentinel triggers the RESET + MONITOR path; auth-pass
	// propagation rides on the same pass against EVERY endpoint of
	// the CR (including the survivor that was not RESET).
	stranded := newFakeSentinel(t)
	survivor := newFakeSentinel(t)
	defer stranded.Stop()
	defer survivor.Stop()
	queueSentinelsReply(stranded /* empty */)
	queueSentinelsReply(survivor, PeerInfo{Name: "p", IP: "10.0.0.99", Port: 26379, RunID: "p1"})
	stranded.QueueReply("SENTINEL RESET", ":1\r\n")
	stranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	// IssueAuthPass hits every endpoint of the CR (stranded + survivor).
	queueAuthPassReplies(stranded)
	queueAuthPassReplies(survivor)

	targets := []InitialResetTarget{
		{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: testMasterName,
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-0", Addr: stranded.Addr()},
				{Name: "vk0-sentinel-1", Addr: survivor.Addr()},
			},
			Password: testPassword,
			MasterIP: "10.0.0.5",
			Port:     6379,
			Quorum:   2,
		},
	}
	m.RunInitialReset(context.Background(), targets)

	rec := m.recorder.(*k8sevents.FakeRecorder)
	got := drainEvents(rec)
	gotInitial, gotApplied := 0, 0
	for _, e := range got {
		if strings.Contains(e, "InitialSentinelReset") {
			gotInitial++
		}
		if strings.Contains(e, "SentinelAuthApplied") {
			gotApplied++
		}
	}
	if gotInitial != 1 {
		t.Errorf("expected 1 InitialSentinelReset event, got %d (events: %v)", gotInitial, got)
	}
	// One SentinelAuthApplied per endpoint that received auth-pass
	// — the survivor + the stranded both get the SET (auth lives on
	// every sentinel of the CR, not just RESET'd ones).
	if gotApplied < 1 {
		t.Errorf("expected ≥1 SentinelAuthApplied event from auto-propagation, got %d (events: %v)", gotApplied, got)
	}
}

func TestManager_StartCancelDrainsObservers(t *testing.T) {
	m, cancel, wait := startManager(t)

	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		for range 20 {
			queuePollReplies(fs, true)
		}
	}

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := m.Ensure(context.Background(), cr, "vk0", "", []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	cancel()
	wait() // Start returns once every observer drains.

	if m.Has(cr) {
		t.Error("Start return must clear the observers map")
	}
}

// TestManager_IssueAuthPass_NotAppliedReportsConfiguredAttempts pins
// that the SentinelAuthNotApplied Warning reports the attempt count the
// retry loop ACTUALLY ran — Options.AuthRetryAttempts — not the package
// default constant. With a tuned single-attempt budget the message must
// say "after 1 attempts"; a message formatter reverted to the
// compatibility constant would report 3 and misdescribe the retry
// budget an operator is debugging against.
func TestManager_IssueAuthPass_NotAppliedReportsConfiguredAttempts(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{AuthRetryAttempts: 1})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	results := m.IssueAuthPass(context.Background(), cr, testMasterName, testPassword,
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}}) // unreachable; the single attempt fails

	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected one failed result, got %+v", results)
	}
	sawNotApplied := false
	for _, e := range drainEvents(rec) {
		if !strings.Contains(e, "SentinelAuthNotApplied") {
			continue
		}
		sawNotApplied = true
		if !strings.Contains(e, "after 1 attempts") {
			t.Errorf("NotApplied must report the CONFIGURED attempt count (after 1 attempts); got %q", e)
		}
	}
	if !sawNotApplied {
		t.Fatalf("expected a SentinelAuthNotApplied Warning")
	}
}
