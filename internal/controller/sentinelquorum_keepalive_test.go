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
	"testing"
	"time"
)

// TestSQStatusWriteSkippable_KeepAlive pins the SentinelQuorum-status
// keep-alive: the per-pod observation digest is the idempotency key,
// but a matching digest may only be skipped while the last write is
// recent enough that the records stay inside the aggregator's
// freshness window. Without the keep-alive, stable-content records'
// LastObservedTime froze and aged out, latching PrimaryConfirmed to
// Unknown on a quiet-but-live cluster (the #680 re-convergence gap).
func TestSQStatusWriteSkippable_KeepAlive(t *testing.T) {
	keepAlive := sqKeepAliveInterval
	within := keepAlive / 3 // comfortably inside the window
	t0 := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)

	s := &perCRState{}

	// First pass: no digest recorded yet → must write (not skippable).
	if s.sqStatusWriteSkippable("digest-A", t0) {
		t.Fatalf("unset digest must never be skippable (first write must happen)")
	}
	s.setSQDigest("digest-A", t0)

	// Same content, well within the keep-alive window → skip (the
	// idempotency optimisation for rapid reconciles).
	if !s.sqStatusWriteSkippable("digest-A", t0.Add(within)) {
		t.Errorf("matching digest within keep-alive must be skippable")
	}

	// Same content, but the last write is now keepAlive old → must
	// re-stamp so LastObservedTime refreshes before records go stale.
	if s.sqStatusWriteSkippable("digest-A", t0.Add(keepAlive)) {
		t.Errorf("matching digest at/after keep-alive boundary must force a re-stamp")
	}

	// Content changed → always write regardless of recency.
	if s.sqStatusWriteSkippable("digest-B", t0.Add(within)) {
		t.Errorf("changed digest must never be skippable")
	}

	// A re-stamp advances the keep-alive baseline: after re-stamping at
	// t0+keepAlive, a write within-window later is skippable again.
	s.setSQDigest("digest-A", t0.Add(keepAlive))
	if !s.sqStatusWriteSkippable("digest-A", t0.Add(keepAlive+within)) {
		t.Errorf("re-stamp must advance the keep-alive baseline")
	}
}
