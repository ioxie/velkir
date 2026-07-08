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
	"testing"
	"time"
)

// eventuallyTimeout is the upper bound for every eventually() call
// in this package. Picked at 2s so a contended -race CI runner has
// enough headroom to satisfy any of the conditions used here (the
// slowest path is the manager-Snapshot wait whose first observer
// publish lands ~PollInterval after Ensure returns); shorter values
// flake under load, longer values dilute the fail-fast intent.
const eventuallyTimeout = 2 * time.Second

// eventually polls cond every interval until it returns true or
// eventuallyTimeout elapses; fails the test with msg on timeout.
// Replaces the for-deadline-Sleep readiness pattern that recurs
// across this package's tests so the wait-for-state contract is
// named in one place rather than five inline copies.
//
// Behaviour mirrors testify's require.Eventually: returns
// immediately on first true cond, fails fast on timeout. Does NOT
// guarantee cond is observed continuously after returning — callers
// asserting steady-state must follow up with a separate window check.
//
// MUST be called from the test goroutine, not a child goroutine:
// the timeout path calls testing.T.Fatalf which calls
// runtime.Goexit, and Goexit unwinds only the calling goroutine.
// A child-goroutine call site would Goexit the helper goroutine
// silently, leaving the main test goroutine to hang on whatever
// channel/ctx it was waiting on. Tests that need a concurrent
// readiness probe must hand-roll a poll loop that signals back
// to the test goroutine via a channel and lets the test goroutine
// call Fatalf.
func eventually(t *testing.T, cond func() bool, interval time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(eventuallyTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	if cond() {
		return
	}
	t.Fatalf("eventually: %s (timeout %s, poll interval %s)", msg, eventuallyTimeout, interval)
}
