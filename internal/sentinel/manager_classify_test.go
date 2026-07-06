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
	"errors"
	"testing"
)

// classifyStrandedSentinels is the shared classification + minority-guard
// chokepoint for the startup safety-net (RunInitialReset, recoverErr=nil)
// and the per-reconcile wedge recovery (RecoverStrandedSentinels,
// recoverErr=isNoSuchMasterErr). These cases pin the two policies'
// behaviour difference and the quorum-threshold boundary so the split-brain
// guard cannot drift between the two callers.
func TestClassifyStrandedSentinels(t *testing.T) {
	ep := func(name string) Endpoint { return Endpoint{Name: name, Addr: name + ":26379"} }
	// A realistic peer carries name/ip/port/runid (the wire parser drops
	// any peer missing them); a live IP set built from these IPs means
	// the holder is NOT a ghost-holder.
	peer := func(ip string) PeerInfo { return PeerInfo{Name: "peer-" + ip, IP: ip, Port: 26379, RunID: "rid-" + ip} }
	nonEmpty := []PeerInfo{peer("10.0.0.1")}
	ipSet := func(ips ...string) map[string]struct{} {
		s := make(map[string]struct{}, len(ips))
		for _, ip := range ips {
			s[ip] = struct{}{}
		}
		return s
	}
	errNoSuchMaster := errors.New("ERR No such master with that name")
	errUnreachable := errors.New("dial tcp: connection refused")

	tests := []struct {
		name            string
		endpoints       []Endpoint
		peerView        []SentinelsResult
		recoverErr      func(error) bool
		liveSentinelIPs map[string]struct{}
		wantTargets     []string
		wantStranded    []string
		wantReachable   int
		wantQuorum      bool
		wantGhosts      []string // ghost-holder endpoint names
	}{
		{
			name:      "startup: only empty-peer sentinels are targets, errors skipped",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: nonEmpty},
				{Name: "s1"}, // empty peer-list → stranded
				{Name: "s2", Err: errUnreachable},
			},
			recoverErr:    nil,
			wantTargets:   []string{"s1"},
			wantStranded:  []string{"s1"},
			wantReachable: 2,    // s0 + s1; s2 unreachable
			wantQuorum:    true, // QuorumThreshold(3)=2, reachable 2 >= 2
		},
		{
			name:      "wedge: no-such-master reply is a reachable stranded target",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: nonEmpty},
				{Name: "", Err: errNoSuchMaster}, // name empty on error → use endpoint name
				{Name: "s2", Err: errUnreachable},
			},
			recoverErr:    isNoSuchMasterErr,
			wantTargets:   []string{"s1"},
			wantStranded:  []string{"s1"}, // endpoints[i].Name, not the empty reply Name
			wantReachable: 2,              // s0 + s1(no-such-master); s2 unreachable
			wantQuorum:    true,
		},
		{
			name:      "startup policy skips a no-such-master reply (behaviour contrast)",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: nonEmpty},
				{Name: "", Err: errNoSuchMaster},
				{Name: "s2", Err: errUnreachable},
			},
			recoverErr:    nil, // same peerView as above, but the startup policy treats every error as unclassifiable
			wantTargets:   nil,
			wantStranded:  nil,
			wantReachable: 1,     // only s0; the no-such-master sentinel is NOT counted
			wantQuorum:    false, // reachable 1 < QuorumThreshold(3)=2
		},
		{
			name:      "minority guard trips when reachable < threshold",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2"), ep("s3"), ep("s4")},
			peerView: []SentinelsResult{
				{Name: "s0"},
				{Name: "s1"},
				{Name: "s2", Err: errUnreachable},
				{Name: "s3", Err: errUnreachable},
				{Name: "s4", Err: errUnreachable},
			},
			recoverErr:    nil,
			wantTargets:   []string{"s0", "s1"},
			wantStranded:  []string{"s0", "s1"},
			wantReachable: 2,     // QuorumThreshold(5)=3
			wantQuorum:    false, // 2 < 3
		},
		{
			name:      "wedge no-such-master lifts reachable over the threshold",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2"), ep("s3"), ep("s4")},
			peerView: []SentinelsResult{
				{Name: "s0"},
				{Name: "s1"},
				{Name: "", Err: errNoSuchMaster}, // counts toward reachable under the wedge policy
				{Name: "s3", Err: errUnreachable},
				{Name: "s4", Err: errUnreachable},
			},
			recoverErr:    isNoSuchMasterErr,
			wantTargets:   []string{"s0", "s1", "s2"},
			wantStranded:  []string{"s0", "s1", "s2"},
			wantReachable: 3,    // without counting the no-such-master reply this would be 2 and wedge the repair
			wantQuorum:    true, // 3 >= QuorumThreshold(5)=3
		},
		{
			name:      "ghost: survivor with all-live peers is NOT a ghost-holder",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: []PeerInfo{peer("10.0.0.1"), peer("10.0.0.2")}},
				{Name: "s1", Peers: []PeerInfo{peer("10.0.0.0"), peer("10.0.0.2")}},
				{Name: "s2", Peers: []PeerInfo{peer("10.0.0.0"), peer("10.0.0.1")}},
			},
			recoverErr:      isNoSuchMasterErr,
			liveSentinelIPs: ipSet("10.0.0.0", "10.0.0.1", "10.0.0.2"),
			wantReachable:   3,
			wantQuorum:      true,
			wantGhosts:      nil, // every peer IP is live
		},
		{
			name:      "ghost: a peer IP in no live pod makes its holder a ghost-holder",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				// s0 still knows 10.0.0.9 — a departed pod's old IP.
				{Name: "s0", Peers: []PeerInfo{peer("10.0.0.1"), peer("10.0.0.9")}},
				{Name: "s1", Peers: []PeerInfo{peer("10.0.0.0"), peer("10.0.0.1")}},
				{Name: "s2", Peers: []PeerInfo{peer("10.0.0.0"), peer("10.0.0.1")}},
			},
			recoverErr:      isNoSuchMasterErr,
			liveSentinelIPs: ipSet("10.0.0.0", "10.0.0.1"), // 10.0.0.9 is gone
			wantReachable:   3,
			wantQuorum:      true,
			wantGhosts:      []string{"s0"},
		},
		{
			name:      "ghost: empty-IP peer is never a ghost",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: []PeerInfo{{Name: "p", IP: "", Port: 26379}}}, // degenerate, in-memory only
				{Name: "s1", Peers: []PeerInfo{peer("10.0.0.0")}},
				{Name: "s2", Peers: []PeerInfo{peer("10.0.0.0")}},
			},
			recoverErr:      isNoSuchMasterErr,
			liveSentinelIPs: ipSet("10.0.0.0"),
			wantReachable:   3,
			wantQuorum:      true,
			wantGhosts:      nil, // empty IP skipped, not flagged a ghost
		},
		{
			name:      "ghost: nil live-IP set disables ghost detection (empty-peer-only fallback)",
			endpoints: []Endpoint{ep("s0"), ep("s1"), ep("s2")},
			peerView: []SentinelsResult{
				{Name: "s0", Peers: []PeerInfo{peer("10.0.0.9")}}, // would be a ghost if detection were on
				{Name: "s1", Peers: []PeerInfo{peer("10.0.0.0")}},
				{Name: "s2"}, // empty-peer stranded
			},
			recoverErr:      isNoSuchMasterErr,
			liveSentinelIPs: nil,
			wantTargets:     []string{"s2"},
			wantStranded:    []string{"s2"},
			wantReachable:   3,
			wantQuorum:      true,
			wantGhosts:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyStrandedSentinels(tc.peerView, tc.endpoints, tc.recoverErr, tc.liveSentinelIPs)
			if !equalNames(endpointNames(got.Targets), tc.wantTargets) {
				t.Errorf("Targets = %v, want %v", endpointNames(got.Targets), tc.wantTargets)
			}
			if !equalNames(got.Stranded, tc.wantStranded) {
				t.Errorf("Stranded = %v, want %v", got.Stranded, tc.wantStranded)
			}
			if got.Reachable != tc.wantReachable {
				t.Errorf("Reachable = %d, want %d", got.Reachable, tc.wantReachable)
			}
			if got.QuorumReachable != tc.wantQuorum {
				t.Errorf("QuorumReachable = %v, want %v", got.QuorumReachable, tc.wantQuorum)
			}
			gotGhosts := make([]string, len(got.GhostHolders))
			for i, h := range got.GhostHolders {
				gotGhosts[i] = h.Endpoint.Name
			}
			if !equalNames(gotGhosts, tc.wantGhosts) {
				t.Errorf("GhostHolders = %v, want %v", gotGhosts, tc.wantGhosts)
			}
		})
	}
}

func endpointNames(eps []Endpoint) []string {
	out := make([]string, len(eps))
	for i, e := range eps {
		out[i] = e.Name
	}
	return out
}

// equalNames treats nil and empty as equal (the helper returns nil
// slices when nothing is appended).
func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
