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

package dynauth

import (
	"crypto/x509"
	"time"
)

// RotationFraction is the fraction of total validity remaining at which we
// pre-emptively reissue. 0.10 means "rotate when ≤10% of the lifetime is
// left" — the cert-manager DynamicAuthority default. Below this the cert is
// still valid but past the point where we want to be sitting on it.
//
// The conservative fraction defends against the "deploy, fall idle, CA
// silently expires" failure mode (the operator restart that triggers a fresh
// cert generation may be days or weeks after the last activity).
const RotationFraction = 0.10

// ShouldRotate returns true when a cert should be reissued. It fires on either:
//
//   - Past expiry (NotAfter < now). Belt-and-braces — by the time we observe
//     this, the apiserver has already started rejecting handshakes, so we
//     reissue immediately.
//   - Within the rotation fraction of remaining lifetime. This is the normal
//     path: predictable, ahead of any production impact, and far enough from
//     expiry that an HA peer rotation race has overlap room.
//
// `now` is injected so callers (Authority loop, tests) can pin the clock.
func ShouldRotate(cert *x509.Certificate, now time.Time, fraction float64) bool {
	if now.After(cert.NotAfter) {
		return true
	}
	total := cert.NotAfter.Sub(cert.NotBefore)
	remaining := cert.NotAfter.Sub(now)
	return float64(remaining) < fraction*float64(total)
}

// minRotationInterval is the lower bound on NextRotationCheck's return value.
// Promoted from a const to a package-level var so tests / `--allow-test-
// overrides` paths can shorten it. Production callers must NEVER lower this
// in real deployments — the 1h floor is what prevents a clock skew or
// future-dated notBefore from spinning the loop hot.
var minRotationInterval = 1 * time.Hour

// maxRotationInterval is the upper bound on NextRotationCheck's return value.
// It is the load-bearing knob for out-of-band annotation responsiveness: a
// long-lived CA (5y) sits far below the rotation-fraction threshold for its
// entire validity, so without this ceiling the Authority loop would sleep
// past every force-rotate annotation between cert rotations.
//
// Production default 24h gives "annotation observed within a day". E2e
// scenarios shorten this via SetMaxRotationInterval so a force-rotate test
// can observe the reconcile inside a sub-minute budget.
var maxRotationInterval = 24 * time.Hour

// SetMinRotationInterval lowers the floor used by NextRotationCheck. The
// `--allow-test-overrides` path in cmd/main.go calls this when the
// VALKEY_OPERATOR_AUTHORITY_MIN_INTERVAL_SEC env var is set. d must be
// positive; non-positive values are ignored to preserve the safety floor.
func SetMinRotationInterval(d time.Duration) {
	if d > 0 {
		minRotationInterval = d
	}
}

// SetMaxRotationInterval lowers the ceiling used by NextRotationCheck. Pairs
// with SetMinRotationInterval on the `--allow-test-overrides` path: shrinking
// only the floor leaves a long-lived CA polled at the 24h ceiling, which
// makes force-rotate-annotation tests time out. Production callers must
// NEVER lower this — the 24h ceiling is what bounds annotation observation
// latency without burning API calls on healthy clusters.
func SetMaxRotationInterval(d time.Duration) {
	if d > 0 {
		maxRotationInterval = d
	}
}

// NextRotationCheck returns a sane sleep interval until the next rotation
// check. Callers should not poll faster than this — the rotation predicate
// is monotone (once true, always true until reissue), so checking more often
// than needed only burns API calls.
//
// The returned interval is bounded:
//   - Lower bound `minRotationInterval` (default 1h; overridable for e2e
//     scenarios that need to observe force-rotate transitions inside a
//     sub-minute budget).
//   - Upper bound `maxRotationInterval` (default 24h; overridable for the
//     same e2e reason — a long-lived CA otherwise sleeps past every force-
//     rotate annotation).
func NextRotationCheck(cert *x509.Certificate, now time.Time, fraction float64) time.Duration {
	total := cert.NotAfter.Sub(cert.NotBefore)
	threshold := cert.NotAfter.Add(-time.Duration(fraction * float64(total)))
	until := threshold.Sub(now)

	switch {
	case until < minRotationInterval:
		return minRotationInterval
	case until > maxRotationInterval:
		return maxRotationInterval
	default:
		return until
	}
}
