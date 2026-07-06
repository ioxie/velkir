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

package pvcresize

import "time"

// StallThreshold is the per-substate elapsed-time budget after which
// the dispatcher considers the PVC resize sub-state-machine stuck.
// At this point the dispatcher emits PVCExpansionStuck and pauses
// (rather than auto-aborting) so an operator can intervene without
// losing the partial-progress substate.
//
// 10 minutes covers the worst-case CSI online-expansion window for
// the providers we care about (openebs-hostpath, csi-driver-host-
// path) — a healthy expansion takes seconds; a 10-minute wait is
// "something is wrong, page someone".
const StallThreshold = 10 * time.Minute

// abortRetryBackoffs is the exponential sequence the dispatcher
// applies before re-entering Detected from Aborted. Indexed by the
// pre-retry Attempt counter (the value persisted on the most-recent
// Aborted transition); BackoffForAttempt clamps to the last entry
// so an Attempt past the table caps at 1h rather than overflowing
// the slice.
//
// 1m / 5m / 15m / 1h matches the spec — fast enough that
// a transient-failure retry happens within a normal coffee break,
// slow enough that a structural failure (StorageClass mis-config,
// CSI driver broken) doesn't hammer the apiserver.
var abortRetryBackoffs = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	1 * time.Hour,
}

// StallElapsed returns now - lastTransition. Caller compares to
// StallThreshold to decide whether to emit PVCExpansionStuck.
// A nil-equivalent (zero) lastTransition returns 0 — the substate
// machine just initialised, no stall window to evaluate.
func StallElapsed(lastTransition, now time.Time) time.Duration {
	if lastTransition.IsZero() {
		return 0
	}
	return now.Sub(lastTransition)
}

// IsStalled reports whether the per-substate stall threshold has
// been crossed. Wraps StallElapsed for callers that want a boolean
// rather than the elapsed duration.
func IsStalled(lastTransition, now time.Time) bool {
	return StallElapsed(lastTransition, now) >= StallThreshold
}

// BackoffForAttempt returns the wait time before the dispatcher
// may re-enter Detected from Aborted given the persisted Attempt
// counter on the most-recent Aborted state.
//
// attempt < 1 is treated as attempt=1 (defensive — Attempt is
// stamped to 1 on the first transition into Validated, so an
// Aborted state should always have Attempt >= 1, but a zero value
// shouldn't crash the dispatcher).
//
// The mapping:
//
//	attempt 1 → 1m
//	attempt 2 → 5m
//	attempt 3 → 15m
//	attempt 4+ → 1h (cap)
func BackoffForAttempt(attempt int32) time.Duration {
	idx := max(int(attempt)-1, 0)
	if idx >= len(abortRetryBackoffs) {
		idx = len(abortRetryBackoffs) - 1
	}
	return abortRetryBackoffs[idx]
}

// BackoffRemaining returns how much longer the dispatcher must
// wait before re-entering Detected from Aborted, given when the
// Aborted state was stamped (lastTransition) and the Attempt
// counter that recorded the failed attempt.
//
// A zero or negative result means "backoff window has elapsed,
// re-enter immediately". Caller maps a positive return to
// requeueAfter so controller-runtime wakes us up at the right
// moment without hot-looping.
//
// A nil-equivalent (zero) lastTransition returns 0 — defensive
// against a malformed status; the dispatcher proceeds with
// re-entry rather than waiting forever for a never-set timestamp.
func BackoffRemaining(lastTransition time.Time, attempt int32, now time.Time) time.Duration {
	if lastTransition.IsZero() {
		return 0
	}
	wait := BackoffForAttempt(attempt)
	elapsed := now.Sub(lastTransition)
	if elapsed >= wait {
		return 0
	}
	return wait - elapsed
}
