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
)

// TestPrimaryPodNameFromAddr pins the pod-IP → pod-name resolution.
// The sentinel observer reports the master address as `host:port`
// (built by sentinelEndpointsFromPods); the SQ status writer maps
// that back to a pod name via the operator's pod list. Reviewer
// flagged that the prior strings.LastIndex+strconv.Atoi parser broke
// on IPv6 ([::1]:6379 has multiple colons); switching to
// net.SplitHostPort closes both the IPv4 and IPv6 paths plus
// surfaces malformed inputs cleanly. Pure-function unit test —
// table-driven; no client, no envtest.
func TestPrimaryPodNameFromAddr(t *testing.T) {
	t.Parallel()
	podTable := map[string]string{
		"10.0.0.5": "valkey-shard-0",
		"10.0.0.6": "valkey-shard-1",
		"::1":      "valkey-shard-loopback", // IPv6 row
		"fe80::1":  "valkey-shard-linklocal",
	}
	cases := []struct {
		name string
		addr string
		want string
	}{
		{"ipv4 with port maps to pod", "10.0.0.5:6379", "valkey-shard-0"},
		{"ipv4 second pod", "10.0.0.6:6379", "valkey-shard-1"},
		{"ipv4 not in pod table (lagging cache / deleted)", "10.0.0.99:6379", ""},
		{"empty addr (sentinel hasn't converged)", "", ""},
		{"missing port (malformed sentinel reply)", "10.0.0.5", ""},
		{"only colon", ":", ""},
		{"ipv6 bracketed with port", "[::1]:6379", "valkey-shard-loopback"},
		{"ipv6 link-local with zone", "[fe80::1%eth0]:6379", ""}, // zone-id strips host match; reasonable defense
		{"ipv6 missing brackets", "::1:6379", ""},                // ambiguous, net.SplitHostPort rejects
		{"trailing colon, no port digits", "10.0.0.5:", "valkey-shard-0"},
		{"port-only", ":6379", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := primaryPodNameFromAddr(tc.addr, podTable); got != tc.want {
				t.Errorf("primaryPodNameFromAddr(%q) = %q; want %q", tc.addr, got, tc.want)
			}
		})
	}
}
