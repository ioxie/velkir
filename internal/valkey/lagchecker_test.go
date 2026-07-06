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

package valkey

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// stubDialer hands a pre-built net.Conn to DialingLagChecker, bypassing
// real network so the post-dial deadline path can be exercised on an
// in-memory pipe.
type stubDialer struct{ conn net.Conn }

func (s stubDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	return s.conn, nil
}

// roleSlave / roleMaster match Valkey's `INFO replication` `role:`
// values. Extracted as consts so the test fixtures don't trip
// goconst — production parsing references the same literals once
// inline, where consts would be over-engineering.
const (
	roleSlave  = "slave"
	roleMaster = "master"
)

func TestParseInfoReplicationMasterRole(t *testing.T) {
	body := `# Replication
role:master
connected_slaves:2
slave0:ip=10.0.0.5,port=6379,state=online,offset=12345,lag=0
master_replid:abcdef
master_repl_offset:12345
`
	got := parseInfoReplication(body)
	if got.Role != roleMaster {
		t.Errorf("Role = %q, want master", got.Role)
	}
	if got.LinkUp {
		t.Errorf("master must not surface LinkUp=true (caller short-circuits on role)")
	}
	if got.LagBytes != 0 {
		t.Errorf("LagBytes = %d, want 0 for master", got.LagBytes)
	}
}

func TestParseInfoReplicationMasterOffsetPresence(t *testing.T) {
	t.Run("present offset sets HaveMasterOffset", func(t *testing.T) {
		got := parseInfoReplication("# Replication\nrole:master\nmaster_repl_offset:12345\n")
		if !got.HaveMasterOffset {
			t.Errorf("HaveMasterOffset = false, want true when master_repl_offset is present")
		}
		if got.MasterReplOffset != 12345 {
			t.Errorf("MasterReplOffset = %d, want 12345", got.MasterReplOffset)
		}
	})
	t.Run("genuine zero offset still sets HaveMasterOffset", func(t *testing.T) {
		got := parseInfoReplication("# Replication\nrole:master\nmaster_repl_offset:0\n")
		if !got.HaveMasterOffset {
			t.Errorf("HaveMasterOffset = false, want true for a genuine master_repl_offset:0")
		}
	})
	t.Run("missing offset leaves HaveMasterOffset false", func(t *testing.T) {
		// Truncated INFO: the master_repl_offset line is omitted entirely.
		got := parseInfoReplication("# Replication\nrole:master\nconnected_slaves:0\n")
		if got.HaveMasterOffset {
			t.Errorf("HaveMasterOffset = true, want false when master_repl_offset is absent")
		}
		if got.MasterReplOffset != 0 {
			t.Errorf("MasterReplOffset = %d, want 0 (zero value) when absent", got.MasterReplOffset)
		}
	})
}

func TestParseInfoReplicationReplicaUpInSync(t *testing.T) {
	body := `# Replication
role:slave
master_host:10.0.0.1
master_port:6379
master_link_status:up
master_last_io_seconds_ago:0
master_sync_in_progress:0
slave_repl_offset:9999
slave_priority:100
slave_read_only:1
master_replid:abcdef
master_repl_offset:9999
`
	got := parseInfoReplication(body)
	if got.Role != roleSlave {
		t.Errorf("Role = %q, want slave", got.Role)
	}
	if !got.LinkUp {
		t.Errorf("master_link_status:up should set LinkUp=true")
	}
	if got.LagBytes != 0 {
		t.Errorf("LagBytes = %d, want 0 (offsets equal)", got.LagBytes)
	}
	// Recovery-election inputs: the corpse check reads MasterHost, the
	// candidate ranking reads SlaveReplOffset/HaveSlaveOffset.
	if got.MasterHost != "10.0.0.1" {
		t.Errorf("MasterHost = %q, want 10.0.0.1", got.MasterHost)
	}
	if !got.HaveSlaveOffset || got.SlaveReplOffset != 9999 {
		t.Errorf("SlaveReplOffset = %d (have=%v), want 9999 (have=true)", got.SlaveReplOffset, got.HaveSlaveOffset)
	}
}

func TestParseInfoReplicationRecoveryFields(t *testing.T) {
	t.Run("master role carries no replica-side recovery fields", func(t *testing.T) {
		got := parseInfoReplication("# Replication\nrole:master\nmaster_repl_offset:42\nconnected_slaves:1\n")
		if got.MasterHost != "" {
			t.Errorf("MasterHost = %q, want empty for role:master", got.MasterHost)
		}
		if got.HaveSlaveOffset || got.SlaveReplOffset != 0 {
			t.Errorf("SlaveReplOffset = %d (have=%v), want 0 (have=false) for role:master", got.SlaveReplOffset, got.HaveSlaveOffset)
		}
	})
	t.Run("truncated replica reply leaves recovery fields unset", func(t *testing.T) {
		// No master_host, no slave_repl_offset: the recovery election
		// must be able to distinguish absence from genuine zero.
		got := parseInfoReplication("# Replication\nrole:slave\nmaster_link_status:down\n")
		if got.MasterHost != "" {
			t.Errorf("MasterHost = %q, want empty when master_host is absent", got.MasterHost)
		}
		if got.HaveSlaveOffset {
			t.Error("HaveSlaveOffset = true, want false when slave_repl_offset is absent")
		}
	})
	t.Run("link-down replica still reports its applied offset", func(t *testing.T) {
		got := parseInfoReplication("# Replication\nrole:slave\nmaster_host:10.0.0.9\nmaster_link_status:down\nslave_repl_offset:1234\nmaster_repl_offset:1300\n")
		if got.MasterHost != "10.0.0.9" {
			t.Errorf("MasterHost = %q, want 10.0.0.9", got.MasterHost)
		}
		if !got.HaveSlaveOffset || got.SlaveReplOffset != 1234 {
			t.Errorf("SlaveReplOffset = %d (have=%v), want 1234 (have=true)", got.SlaveReplOffset, got.HaveSlaveOffset)
		}
		if got.LinkUp {
			t.Error("LinkUp = true, want false")
		}
	})
}

func TestParseInfoReplicationReplicaUpBehind(t *testing.T) {
	body := `# Replication
role:slave
master_link_status:up
slave_repl_offset:1000
master_repl_offset:5000
`
	got := parseInfoReplication(body)
	if got.LagBytes != 4000 {
		t.Errorf("LagBytes = %d, want 4000", got.LagBytes)
	}
	if !got.LinkUp {
		t.Errorf("LinkUp = false, want true")
	}
}

func TestParseInfoReplicationReplicaLinkDown(t *testing.T) {
	body := `# Replication
role:slave
master_link_status:down
master_link_down_since_seconds:42
slave_repl_offset:1000
master_repl_offset:5000
`
	got := parseInfoReplication(body)
	if got.LinkUp {
		t.Errorf("master_link_status:down must not surface LinkUp=true")
	}
	// Lag is still computed even when link is down — useful for
	// dashboards that want to chart "how far behind was it when the
	// link dropped" without the gate ever flipping True.
	if got.LagBytes != 4000 {
		t.Errorf("LagBytes = %d, want 4000 (computed regardless of link state)", got.LagBytes)
	}
}

func TestParseInfoReplicationSlaveOffsetAheadIsClamped(t *testing.T) {
	// Pathological case: replica's offset reports HIGHER than master
	// (e.g. mid-failover, clock-skew on stale data). Clamp to zero
	// rather than emitting a negative LagBytes that downstream gauges
	// would underflow on.
	body := `# Replication
role:slave
master_link_status:up
slave_repl_offset:5000
master_repl_offset:1000
`
	got := parseInfoReplication(body)
	if got.LagBytes != 0 {
		t.Errorf("LagBytes = %d, want 0 (clamped to non-negative)", got.LagBytes)
	}
}

func TestParseInfoReplicationIgnoresUnknownLines(t *testing.T) {
	// A future Valkey version that adds a new INFO field must not
	// break parsing. The unknown line is dropped silently.
	body := `# Replication
role:slave
master_link_status:up
slave_repl_offset:42
master_repl_offset:42
brand_new_field_we_dont_know:42
`
	got := parseInfoReplication(body)
	if got.Role != "slave" || !got.LinkUp || got.LagBytes != 0 {
		t.Errorf("unknown field broke parse: %+v", got)
	}
}

func TestEncodeCommand(t *testing.T) {
	got := resp.EncodeCommand("AUTH", "secret")
	want := "*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n"
	if got != want {
		t.Errorf("encodeCommand AUTH secret = %q, want %q", got, want)
	}
	got = resp.EncodeCommand("INFO", "replication")
	want = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"
	if got != want {
		t.Errorf("encodeCommand INFO replication = %q, want %q", got, want)
	}
}

func TestReadSimpleOrError(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr string
	}{
		{"+OK\r\n", "OK", ""},
		{"+PONG\r\n", "PONG", ""},
		{"-ERR Authentication required\r\n", "", "Authentication required"},
		{"$5\r\nhello\r\n", "", "unexpected reply prefix"},
	} {
		got, err := readSimpleOrError(bufio.NewReader(strings.NewReader(tc.in)))
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("readSimpleOrError(%q) err=%v, want substring %q", tc.in, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("readSimpleOrError(%q) err=%v, want nil", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("readSimpleOrError(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestReadBulkString(t *testing.T) {
	body := `# Replication
role:master
`
	in := "$" + itoa(len(body)) + "\r\n" + body + "\r\n"
	got, err := readBulkString(bufio.NewReader(strings.NewReader(in)))
	if err != nil {
		t.Fatalf("readBulkString err=%v", err)
	}
	if got != body {
		t.Errorf("readBulkString = %q, want %q", got, body)
	}
}

func TestReadBulkStringNil(t *testing.T) {
	got, err := readBulkString(bufio.NewReader(strings.NewReader("$-1\r\n")))
	if err != nil {
		t.Fatalf("readBulkString nil err=%v", err)
	}
	if got != "" {
		t.Errorf("readBulkString nil = %q, want empty", got)
	}
}

func TestReadBulkStringRejectsOversizedLength(t *testing.T) {
	// A compromised replica that responds with a giant length prefix
	// must NOT be allowed to drive a multi-MiB allocation. The cap
	// is checked BEFORE `make`, so the read never starts and no
	// allocation lands. Realistic INFO replication payloads are
	// 1–5 KiB; anything past 1 MiB is the threat model.
	oversize := resp.MaxBulkSize + 1
	in := "$" + itoa(oversize) + "\r\n"
	got, err := readBulkString(bufio.NewReader(strings.NewReader(in)))
	if err == nil {
		t.Fatalf("expected oversize error, got body of len %d", len(got))
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Errorf("err = %q, want 'exceeds cap'", err)
	}
}

func TestReadBulkStringAllowsExactCap(t *testing.T) {
	// Boundary: exactly at the cap is accepted. One byte above is
	// rejected (covered by the test above).
	body := strings.Repeat("a", resp.MaxBulkSize)
	in := "$" + itoa(resp.MaxBulkSize) + "\r\n" + body + "\r\n"
	got, err := readBulkString(bufio.NewReader(strings.NewReader(in)))
	if err != nil {
		t.Fatalf("readBulkString at-cap err=%v", err)
	}
	if len(got) != resp.MaxBulkSize {
		t.Errorf("len(got)=%d, want %d", len(got), resp.MaxBulkSize)
	}
}

func TestReadBulkStringErrorReply(t *testing.T) {
	_, err := readBulkString(bufio.NewReader(strings.NewReader("-ERR no such cmd\r\n")))
	if err == nil || !strings.Contains(err.Error(), "no such cmd") {
		t.Errorf("readBulkString error err=%v, want substring 'no such cmd'", err)
	}
}

// TestDialingLagCheckerEndToEnd wires a fake Valkey listener to the
// dialing checker so the AUTH + INFO round-trip is exercised against
// real net.Conn semantics — covers the read/write deadline plumbing
// the unit tests above can't touch.
func TestDialingLagCheckerEndToEnd(t *testing.T) {
	infoBody := `# Replication
role:slave
master_link_status:up
slave_repl_offset:100
master_repl_offset:150
`
	addr := startFakeValkey(t, infoBody, "secret")

	c := &DialingLagChecker{Timeout: 2 * time.Second}
	got, err := c.CheckLag(context.Background(), addr, "secret")
	if err != nil {
		t.Fatalf("CheckLag err=%v", err)
	}
	if !got.LinkUp || got.LagBytes != 50 || got.Role != "slave" {
		t.Errorf("CheckLag = %+v, want LinkUp=true LagBytes=50 Role=slave", got)
	}
}

func TestDialingLagCheckerNoAuthSkipsAuth(t *testing.T) {
	infoBody := `# Replication
role:master
master_repl_offset:200
`
	addr := startFakeValkey(t, infoBody, "" /* no password expected */)

	c := &DialingLagChecker{Timeout: 2 * time.Second}
	got, err := c.CheckLag(context.Background(), addr, "")
	if err != nil {
		t.Fatalf("CheckLag err=%v", err)
	}
	if got.Role != roleMaster {
		t.Errorf("Role = %q, want master", got.Role)
	}
}

func TestDialingLagCheckerDeadlineFires(t *testing.T) {
	// Pins the load-bearing assertion that the deadline plumbing
	// actually trips when a peer accepts the connection and our
	// request but never replies. A future refactor that drops
	// `conn.SetDeadline` (or resets it per read) would let CheckLag
	// block forever — caught here as a hung test.
	//
	// Deterministic + egress-free: an in-memory net.Pipe stands in for
	// the socket (it honors SetDeadline exactly), and a draining
	// goroutine consumes the INFO write so the *read* is what blocks
	// until the deadline fires — mirroring a real socket whose kernel
	// buffers the request while the peer app stays silent. No real
	// listener, no fixed multi-second sleep.
	clientEnd, serverEnd := net.Pipe()
	go func() {
		buf := make([]byte, 256)
		for {
			if _, err := serverEnd.Read(buf); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = serverEnd.Close(); _ = clientEnd.Close() })

	c := &DialingLagChecker{
		Timeout: 50 * time.Millisecond,
		dialer:  stubDialer{conn: clientEnd},
	}
	start := time.Now()
	_, err := c.CheckLag(context.Background(), "10.0.0.1:6379", "")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a deadline error against a silent peer")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("expected a wrapped deadline-exceeded error, got %v", err)
	}
	// Returned at the ~50ms deadline, not blocked indefinitely. The
	// bound is generous; determinism comes from the in-memory pipe
	// honoring the deadline, not from a tight wall-clock margin.
	if elapsed > time.Second {
		t.Errorf("deadline did not fire within budget: elapsed=%v want <1s", elapsed)
	}
}

func TestDialingLagCheckerDialFailureIsTyped(t *testing.T) {
	c := &DialingLagChecker{Timeout: 200 * time.Millisecond}
	_, err := c.CheckLag(context.Background(), "127.0.0.1:1", "")
	if err == nil {
		t.Fatal("expected dial error against unreachable port")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error %q should mention dial", err)
	}
}

// itoa avoids dragging strconv into the bulk-write fixture.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// startFakeValkey accepts one connection on a random localhost port,
// validates the AUTH (when expectedPassword non-empty), then replies
// to INFO replication with the provided body. Returns the listener
// address; the goroutine self-cleans when the test exits.
func startFakeValkey(t *testing.T, infoBody, expectedPassword string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		rd := bufio.NewReader(conn)

		if expectedPassword != "" {
			if !readArrayCommand(rd, "AUTH", expectedPassword) {
				_, _ = io.WriteString(conn, "-ERR auth\r\n")
				return
			}
			_, _ = io.WriteString(conn, "+OK\r\n")
		}

		if !readArrayCommand(rd, "INFO", "replication") {
			_, _ = io.WriteString(conn, "-ERR cmd\r\n")
			return
		}
		reply := "$" + itoa(len(infoBody)) + "\r\n" + infoBody + "\r\n"
		_, _ = io.WriteString(conn, reply)
	}()

	return ln.Addr().String()
}

// readArrayCommand consumes one RESP array and verifies it carries the
// expected tokens in order. Quick and dirty — sufficient for the
// fixture; the real client is what TestEncodeCommand pins.
func readArrayCommand(rd *bufio.Reader, want ...string) bool {
	header, err := rd.ReadString('\n')
	if err != nil || !strings.HasPrefix(header, "*") {
		return false
	}
	for _, w := range want {
		// $<n>\r\n<n bytes>\r\n
		hdr, err := rd.ReadString('\n')
		if err != nil || !strings.HasPrefix(hdr, "$") {
			return false
		}
		body := make([]byte, len(w))
		if _, err := io.ReadFull(rd, body); err != nil {
			return false
		}
		if string(body) != w {
			return false
		}
		if _, err := rd.Discard(2); err != nil {
			return false
		}
	}
	return true
}

// silence unused import linter: errors is referenced via a sentinel
// only when the conditional compile path is taken in older Go releases.
var _ = errors.New
