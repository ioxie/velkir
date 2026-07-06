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
	"testing"
	"time"
)

func TestShouldRotate(t *testing.T) {
	notBefore := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.Add(365 * 24 * time.Hour) // 1y leaf
	cert := &x509.Certificate{NotBefore: notBefore, NotAfter: notAfter}

	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{
			// Fresh cert, ~99% remaining → no rotation.
			name: "fresh cert",
			now:  notBefore.Add(24 * time.Hour),
			want: false,
		},
		{
			// 80% remaining → no rotation.
			name: "early lifetime",
			now:  notBefore.Add(73 * 24 * time.Hour),
			want: false,
		},
		{
			// Exactly at the 10% threshold (90% elapsed) → still false because
			// the predicate is strict less-than. The case directly below pins
			// "one minute past the threshold → true" so the strict-less-than-
			// only-on-the-rotate-side semantics can't drift to <= without a
			// test failing.
			name: "exactly 10% remaining",
			now:  notAfter.Add(-time.Duration(0.10 * float64(365*24*time.Hour))),
			want: false,
		},
		{
			// One minute past the 10% threshold (so just under 10% remaining)
			// → rotate. Pair-test for "exactly 10% remaining" above.
			name: "just under 10% remaining triggers rotation",
			now:  notAfter.Add(-time.Duration(0.10*float64(365*24*time.Hour)) + time.Minute),
			want: true,
		},
		{
			// 9% remaining → rotate (well past the threshold).
			name: "well below 10% remaining",
			now:  notAfter.Add(-time.Duration(0.09 * float64(365*24*time.Hour))),
			want: true,
		},
		{
			// Past expiry → rotate (belt-and-braces).
			name: "expired",
			now:  notAfter.Add(time.Hour),
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldRotate(cert, tc.now, RotationFraction); got != tc.want {
				t.Errorf("ShouldRotate=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestNextRotationCheck_Bounded(t *testing.T) {
	notBefore := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.Add(365 * 24 * time.Hour)
	cert := &x509.Certificate{NotBefore: notBefore, NotAfter: notAfter}

	tests := []struct {
		name        string
		now         time.Time
		wantAtLeast time.Duration
		wantAtMost  time.Duration
	}{
		{
			// Day 1 → threshold ~10 months out → clamped to 24h ceiling.
			name:        "fresh cert clamps to 24h ceiling",
			now:         notBefore.Add(24 * time.Hour),
			wantAtLeast: 24 * time.Hour,
			wantAtMost:  24 * time.Hour,
		},
		{
			// Past the rotation threshold → clamped to 1h floor (don't spin).
			name:        "past threshold clamps to 1h floor",
			now:         notAfter.Add(-time.Hour),
			wantAtLeast: time.Hour,
			wantAtMost:  time.Hour,
		},
		{
			// 18h before threshold → between bounds, returned as-is.
			name:        "between bounds",
			now:         notAfter.Add(-time.Duration(0.10*float64(365*24*time.Hour)) - 18*time.Hour),
			wantAtLeast: 17*time.Hour + 30*time.Minute,
			wantAtMost:  18*time.Hour + 30*time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NextRotationCheck(cert, tc.now, RotationFraction)
			if got < tc.wantAtLeast || got > tc.wantAtMost {
				t.Errorf("NextRotationCheck=%v, want in [%v, %v]", got, tc.wantAtLeast, tc.wantAtMost)
			}
		})
	}
}
