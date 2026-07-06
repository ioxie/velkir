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
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// captureLogger collects every Info/V emission from logr.FromContext
// so tests can assert that a code path emitted (or did not emit) a
// specific keyed log line. funcr's V-level handling matches
// controller-runtime's: pass MaxLevel=2 so V(1) lines are captured.
type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func newCaptureLogger() (logr.Logger, *captureLogger) {
	cap := &captureLogger{}
	l := funcr.New(func(prefix, args string) {
		cap.mu.Lock()
		cap.lines = append(cap.lines, args)
		cap.mu.Unlock()
	}, funcr.Options{Verbosity: 2})
	return l, cap
}

func (c *captureLogger) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

// TestObserver_DispatchLogsMalformedSwitchMaster pins the contract:
// a malformed +switch-master payload was previously silently dropped
// (no audit trail). dispatch now logs at V(1) with channel + payload
// so a malformed-emitter regression upstream surfaces in operator
// logs.
func TestObserver_DispatchLogsMalformedSwitchMaster(t *testing.T) {
	l, cap := newCaptureLogger()
	ctx := log.IntoContext(context.Background(), l)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:0"}},
		Options{},
		k8sevents.NewFakeRecorder(8),
		nil,
	)

	// Channel matches +switch-master so dispatch routes to
	// ParseSwitchMaster; payload is empty so the parser returns !ok.
	msg := pubsubMessage{Channel: "+switch-master", Payload: ""}
	o.dispatch(ctx, Endpoint{Name: "vk0-sentinel-0"}, msg)

	got := cap.joined()
	if !strings.Contains(got, "malformed sentinel pubsub payload") {
		t.Fatalf("missing malformed-payload log line; captured=%q", got)
	}
	if !strings.Contains(got, "+switch-master") {
		t.Fatalf("log line missing channel field; captured=%q", got)
	}
}

// TestObserver_DispatchLogsMalformedODown pins the same audit-trail
// behavior on the +odown branch (ParseMasterEvent path). The four
// ParseMasterEvent call sites (KindODown / KindODownClear /
// KindFailoverEnd / KindFailoverEndTimeout) share the helper, so one
// branch's coverage suffices to pin the contract.
func TestObserver_DispatchLogsMalformedODown(t *testing.T) {
	l, cap := newCaptureLogger()
	ctx := log.IntoContext(context.Background(), l)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:0"}},
		Options{},
		k8sevents.NewFakeRecorder(8),
		nil,
	)

	msg := pubsubMessage{Channel: "+odown", Payload: ""}
	o.dispatch(ctx, Endpoint{Name: "vk0-sentinel-0"}, msg)

	got := cap.joined()
	if !strings.Contains(got, "malformed sentinel pubsub payload") {
		t.Fatalf("missing malformed-payload log line; captured=%q", got)
	}
	if !strings.Contains(got, "+odown") {
		t.Fatalf("log line missing channel field; captured=%q", got)
	}
}

// TestObserver_DispatchHappyPathSilent confirms the V(1) logging is
// branch-specific: a well-formed payload should NOT trigger the
// malformed-payload log line. Without this pin, a future regression
// could move the log call out of the !ok branch and emit on every
// pubsub message — defeating the V(1) selectivity.
func TestObserver_DispatchHappyPathSilent(t *testing.T) {
	l, cap := newCaptureLogger()
	ctx := log.IntoContext(context.Background(), l)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:0"}},
		Options{},
		k8sevents.NewFakeRecorder(8),
		nil,
	)

	// Well-formed +switch-master: "<masterName> <oldip> <oldport> <newip> <newport>".
	msg := pubsubMessage{Channel: "+switch-master", Payload: "vk0 10.0.0.1 6379 10.0.0.2 6379"}
	o.dispatch(ctx, Endpoint{Name: "vk0-sentinel-0"}, msg)

	if got := cap.joined(); strings.Contains(got, "malformed sentinel pubsub payload") {
		t.Fatalf("log emitted on happy path; captured=%q", got)
	}
}

// TestObserver_RunPollDelaysFirstTick pins the contract: the
// initial pollOnce now waits initialPollBackoff after observer start
// before firing, so a not-yet-Ready sentinel STS at startup doesn't
// stall the first tick on a per-endpoint dial-timeout cascade.
//
// We assert the new behavior by observing the TIME OF FIRST CONNECT
// on the fake sentinel: it MUST be at least ~initialPollBackoff
// after observer.start. A wide tolerance (200ms) accommodates
// scheduler jitter under -race.
func TestObserver_RunPollDelaysFirstTick(t *testing.T) {
	fs := newFakeSentinel(t)
	t.Cleanup(fs.Stop)

	// One pull-tick reply set is enough — we only care about the
	// timing of the first connect, not many ticks.
	queuePollReplies(fs, true)

	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		Options{
			PollInterval:       100 * time.Hour, // pin to first tick only
			PubsubReadDeadline: 30 * time.Second,
			PingTimeout:        time.Second,
		},
		k8sevents.NewFakeRecorder(8),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	startedAt := time.Now()
	o.start(ctx)
	t.Cleanup(o.stop)

	// Wait for the first poll to land — bounded so the test fails
	// fast if initialPollBackoff is removed and behavior reverts.
	_ = waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		return p.Source == SourcePoll
	})
	elapsed := time.Since(startedAt)

	// Tolerate scheduler jitter on slow runners (under -race CI
	// can shave 100-200ms off measured intervals). The pin
	// requires SOME wait, not the exact constant — that level of
	// brittleness invites flakes.
	minExpected := initialPollBackoff - 200*time.Millisecond
	if elapsed < minExpected {
		t.Fatalf("first pollOnce fired too quickly: elapsed=%s, expected >=%s (initialPollBackoff=%s)",
			elapsed, minExpected, initialPollBackoff)
	}
}
