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
)

// queueSentinelsReply scripts a SENTINEL SENTINELS reply with one
// peer entry. Each peer is encoded as an inner array of alternating
// bulk-string key/value pairs. Field count is intentionally minimal
// (name, ip, port, runid) — real sentinel replies carry more fields
// but the parser only consumes those four.
func queueSentinelsReply(fs *fakeSentinel, peers ...PeerInfo) {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(itoa(len(peers)))
	b.WriteString("\r\n")
	for _, p := range peers {
		// inner array: name, <name>, ip, <ip>, port, <port>, runid, <runid> — 8 elements.
		b.WriteString("*8\r\n")
		writeBulk(&b, "name")
		writeBulk(&b, p.Name)
		writeBulk(&b, "ip")
		writeBulk(&b, p.IP)
		writeBulk(&b, "port")
		writeBulk(&b, itoa(p.Port))
		writeBulk(&b, "runid")
		writeBulk(&b, p.RunID)
	}
	fs.QueueReply("SENTINEL SENTINELS", b.String())
}

func writeBulk(b *strings.Builder, s string) {
	b.WriteString("$")
	b.WriteString(itoa(len(s)))
	b.WriteString("\r\n")
	b.WriteString(s)
	b.WriteString("\r\n")
}

// queueMasterReplyWithPeerCount scripts a SENTINEL MASTER reply with
// the supplied num-other-sentinels value. The minimal reply carries
// just the count field; the wedge-recovery parser only consumes that.
func queueMasterReplyWithPeerCount(fs *fakeSentinel, count int) {
	var b strings.Builder
	b.WriteString("*2\r\n")
	writeBulk(&b, "num-other-sentinels")
	writeBulk(&b, itoa(count))
	fs.QueueReply("SENTINEL MASTER", b.String())
}

func TestParseSentinelsReply_HappyPath(t *testing.T) {
	// Outer array of 2 sentinels; each inner array carries the four
	// fields the parser consumes.
	reply := []any{
		[]any{"name", "s1", "ip", "10.0.0.11", "port", "26379", "runid", "abc123"},
		[]any{"name", "s2", "ip", "10.0.0.12", "port", "26379", "runid", "def456"},
	}
	got := ParseSentinelsReply(reply)
	if len(got) != 2 {
		t.Fatalf("expected 2 peers, got %d (%+v)", len(got), got)
	}
	if got[0].RunID != "abc123" || got[0].IP != "10.0.0.11" || got[0].Port != 26379 {
		t.Errorf("peer 0 misparsed: %+v", got[0])
	}
	if got[1].RunID != "def456" || got[1].IP != "10.0.0.12" {
		t.Errorf("peer 1 misparsed: %+v", got[1])
	}
}

func TestParseSentinelsReply_EmptyOuter(t *testing.T) {
	got := ParseSentinelsReply([]any{})
	if len(got) != 0 {
		t.Errorf("expected empty, got %+v", got)
	}
}

func TestParseSentinelsReply_NonArrayReply(t *testing.T) {
	got := ParseSentinelsReply("+OK")
	if got != nil {
		t.Errorf("expected nil for non-array reply, got %+v", got)
	}
}

func TestParseSentinelsReply_DropsIncompletePeers(t *testing.T) {
	// Peers missing runid/ip/port must be dropped — the wedge-recovery
	// classifier needs all three for reliable counting.
	reply := []any{
		[]any{"name", "good", "ip", "10.0.0.11", "port", "26379", "runid", "abc"},
		[]any{"name", "no-runid", "ip", "10.0.0.12", "port", "26379"},
		[]any{"name", "no-ip", "port", "26379", "runid", "def"},
		[]any{"name", "bad-port", "ip", "10.0.0.13", "port", "not-a-number", "runid", "ghi"},
	}
	got := ParseSentinelsReply(reply)
	if len(got) != 1 || got[0].RunID != "abc" {
		t.Errorf("expected only the complete peer to survive, got %+v", got)
	}
}

func TestSentinelsAll_HappyPath(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs, PeerInfo{Name: "s2", IP: "10.0.0.12", Port: 26379, RunID: "abc"})

	results := SentinelsAll(context.Background(),
		[]Endpoint{{Name: "s1", Addr: fs.Addr()}}, "vk0", "")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("unexpected error: %v", results[0].Err)
	}
	if len(results[0].Peers) != 1 || results[0].Peers[0].RunID != "abc" {
		t.Errorf("expected one peer with runid=abc, got %+v", results[0].Peers)
	}
}

func TestSentinelsAll_ContinuesOnUnreachable(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs, PeerInfo{Name: "s2", IP: "10.0.0.12", Port: 26379, RunID: "abc"})

	results := SentinelsAll(context.Background(), []Endpoint{
		{Name: "s1", Addr: fs.Addr()},
		{Name: "dead", Addr: "127.0.0.1:1"},
	}, "vk0", "")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("reachable result must succeed: %v", results[0].Err)
	}
	if results[1].Err == nil {
		t.Errorf("dead result must error")
	}
}

func TestMasterPeerCountAll_HappyPath(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueMasterReplyWithPeerCount(fs, 2)

	results := MasterPeerCountAll(context.Background(),
		[]Endpoint{{Name: "s1", Addr: fs.Addr()}}, "vk0", "")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("unexpected error: %v", results[0].Err)
	}
	if results[0].Count != 2 {
		t.Errorf("expected count=2, got %d", results[0].Count)
	}
}

func TestMasterPeerCountAll_StrandedSentinel(t *testing.T) {
	// num-other-sentinels=0 is the stranded-sentinel signature the
	// wedge-recovery read-back path checks for.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueMasterReplyWithPeerCount(fs, 0)

	results := MasterPeerCountAll(context.Background(),
		[]Endpoint{{Name: "s-stranded", Addr: fs.Addr()}}, "vk0", "")
	if len(results) != 1 || results[0].Err != nil || results[0].Count != 0 {
		t.Errorf("expected stranded (count=0, nil err), got %+v", results[0])
	}
}

func TestMasterPeerCountAll_ContinuesOnUnreachable(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueMasterReplyWithPeerCount(fs, 1)

	results := MasterPeerCountAll(context.Background(), []Endpoint{
		{Name: "s1", Addr: fs.Addr()},
		{Name: "dead", Addr: "127.0.0.1:1"},
	}, "vk0", "")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("reachable: %v", results[0].Err)
	}
	if results[1].Err == nil {
		t.Errorf("dead must error")
	}
}
