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
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// fakeSentinel is a verb-keyed RESP server for tests. Tests queue
// scripted replies per command key (e.g. "PSUBSCRIBE", "SENTINEL
// GET-MASTER-ADDR-BY-NAME", "SENTINEL CKQUORUM") via QueueReply,
// and the fake routes the next inbound command to the matching
// queue. Concurrent connections (subscribe + pull-tick) coexist
// safely because the routing is by verb, not FIFO.
//
// AUTH and PING are auto-handled so tests don't need to script
// the boring path. Once a connection issues PSUBSCRIBE, it is
// registered as a subscriber — Push(msg) writes msg to every
// subscriber concurrently for inline +switch-master injection.
type fakeSentinel struct {
	t        *testing.T
	listener net.Listener
	password string

	mu          sync.Mutex
	queuedReply map[string][]string
	subscribers []net.Conn
	conns       map[net.Conn]struct{}
	sent        []string
	// stopping, set under mu once Stop begins, gates new-conn
	// registration so every connsWG.Add is ordered before Stop's
	// connsWG.Wait — closing the Add-at-zero vs Wait race.
	stopping bool
	connsWG  sync.WaitGroup
}

func newFakeSentinel(t *testing.T) *fakeSentinel {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fs := &fakeSentinel{
		t:           t,
		listener:    l,
		queuedReply: map[string][]string{},
		conns:       make(map[net.Conn]struct{}),
	}
	go fs.acceptLoop()
	return fs
}

func (f *fakeSentinel) Addr() string { return f.listener.Addr().String() }

// QueueReply scripts the next reply for one command key.
// PSUBSCRIBE replies must be the seven concatenated psubscribe-ack
// frames (one per pattern) — the helper queuePsubscribeAcks builds
// that string. SENTINEL subcommands use the key
// "SENTINEL <SUBCMD>" (uppercased).
func (f *fakeSentinel) QueueReply(cmdKey, reply string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queuedReply[cmdKey] = append(f.queuedReply[cmdKey], reply)
}

// Push writes msg directly to every connection currently
// registered as a subscriber. Used to inject pubsub messages
// (+switch-master, +odown, etc.) inline.
func (f *fakeSentinel) Push(msg string) {
	f.mu.Lock()
	subs := append([]net.Conn(nil), f.subscribers...)
	f.mu.Unlock()
	for _, c := range subs {
		_, _ = c.Write([]byte(msg))
	}
}

// Sent returns the rendered space-joined commands the fake
// observed so far. Useful for asserting "the observer sent X".
func (f *fakeSentinel) Sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

func (f *fakeSentinel) Stop() {
	_ = f.listener.Close()
	// Closing the listener stops new accepts but does NOT terminate
	// already-accepted connections. The observer holds a long-lived
	// PSUBSCRIBE conn open per endpoint, and a poll-tick conn briefly
	// sits accepted between dial and the first command (or longer if
	// the observer is cancelled mid-cycle). Closing only `subscribers`
	// — conns that already issued PSUBSCRIBE — leaves any non-subscriber
	// conn open; its serveConn blocks on Read forever and connsWG.Wait
	// deadlocks. Close every live conn so each serveConn's Read returns
	// io.EOF and the goroutine exits.
	f.mu.Lock()
	f.stopping = true
	for c := range f.conns {
		_ = c.Close()
	}
	f.subscribers = nil
	f.mu.Unlock()
	f.connsWG.Wait()
}

func (f *fakeSentinel) acceptLoop() {
	for {
		c, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		if f.stopping {
			f.mu.Unlock()
			_ = c.Close()
			continue
		}
		f.conns[c] = struct{}{}
		f.connsWG.Add(1)
		f.mu.Unlock()
		go f.serveConn(c)
	}
}

func (f *fakeSentinel) serveConn(c net.Conn) {
	defer f.connsWG.Done()
	defer func() {
		_ = c.Close()
		f.mu.Lock()
		delete(f.conns, c)
		f.mu.Unlock()
	}()
	rd := bufio.NewReader(c)
	for {
		req, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.sent = append(f.sent, strings.Join(req, " "))
		f.mu.Unlock()

		key := cmdKey(req)
		switch key {
		case "AUTH":
			if len(req) >= 2 && req[1] == f.password {
				_, _ = c.Write([]byte("+OK\r\n"))
			} else {
				_, _ = c.Write([]byte("-ERR invalid password\r\n"))
			}
			continue
		case "PING":
			_, _ = c.Write([]byte("+PONG\r\n"))
			continue
		case "PSUBSCRIBE":
			f.mu.Lock()
			replies := f.queuedReply[key]
			var reply string
			if len(replies) > 0 {
				reply = replies[0]
				f.queuedReply[key] = replies[1:]
			}
			f.subscribers = append(f.subscribers, c)
			f.mu.Unlock()
			if reply != "" {
				_, _ = c.Write([]byte(reply))
			}
			continue
		}

		f.mu.Lock()
		replies := f.queuedReply[key]
		var reply string
		if len(replies) > 0 {
			reply = replies[0]
			f.queuedReply[key] = replies[1:]
		}
		f.mu.Unlock()
		if reply == "" {
			// No script for this verb — fall back to a -ERR so
			// the client surfaces a controlled failure rather
			// than hanging on the read.
			reply = "-ERR no scripted reply for " + key + "\r\n"
		}
		if _, err := c.Write([]byte(reply)); err != nil {
			return
		}
	}
}

// cmdKey returns the verb key the fake's reply table is indexed
// on. SENTINEL <SUBCMD> collapses to "SENTINEL <SUBCMD>" so
// GET-MASTER-ADDR-BY-NAME and CKQUORUM are routed independently.
func cmdKey(args []string) string {
	if len(args) == 0 {
		return ""
	}
	verb := strings.ToUpper(args[0])
	if verb == "SENTINEL" && len(args) >= 2 {
		return verb + " " + strings.ToUpper(args[1])
	}
	return verb
}

// readRESPCommand parses one RESP-2 array-of-bulk-strings (the
// command form). Returns the command tokens.
func readRESPCommand(rd *bufio.Reader) ([]string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimRight(header, "\r\n")
	if len(header) == 0 || header[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", header)
	}
	var n int
	if _, err := fmt.Sscanf(header[1:], "%d", &n); err != nil {
		return nil, fmt.Errorf("bad array length %q: %w", header, err)
	}
	out := make([]string, 0, n)
	for range n {
		bulkHeader, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimRight(bulkHeader, "\r\n")
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("expected bulk header, got %q", bulkHeader)
		}
		var l int
		if _, err := fmt.Sscanf(bulkHeader[1:], "%d", &l); err != nil {
			return nil, fmt.Errorf("bad bulk length: %w", err)
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		if _, err := rd.Discard(2); err != nil {
			return nil, err
		}
		out = append(out, string(buf))
	}
	return out, nil
}

// ─────────────────────────────────────────────────────────────────────
// Tests for the conn-level helpers (no fake server needed).
// ─────────────────────────────────────────────────────────────────────

func TestEncodeCommand(t *testing.T) {
	got := resp.EncodeCommand("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "vk0")
	want := "*3\r\n$8\r\nSENTINEL\r\n$23\r\nGET-MASTER-ADDR-BY-NAME\r\n$3\r\nvk0\r\n"
	if got != want {
		t.Errorf("encodeCommand mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

func TestReadReply_BulkString(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))
	reply, err := readReply(rd)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if s, _ := reply.(string); s != "hello" {
		t.Errorf("got %q, want hello", s)
	}
}

func TestReadReply_NilBulk(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("$-1\r\n"))
	reply, err := readReply(rd)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if reply != "" {
		t.Errorf("got %v, want empty string for nil bulk", reply)
	}
}

func TestReadReply_Array(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	reply, err := readReply(rd)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	arr, ok := reply.([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("expected []any of len 2, got %T %v", reply, reply)
	}
	if arr[0] != "foo" || arr[1] != "bar" {
		t.Errorf("array = %v, want [foo bar]", arr)
	}
}

func TestReadReply_Integer(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(":42\r\n"))
	reply, err := readReply(rd)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if n, _ := reply.(int64); n != 42 {
		t.Errorf("got %v, want 42", reply)
	}
}

func TestReadReply_ErrorReply(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("-NOQUORUM Quorum not reached\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("expected error reply to surface as Go error")
	}
	if !strings.Contains(err.Error(), "NOQUORUM") {
		t.Errorf("error = %q, want NOQUORUM substring", err)
	}
}

func TestReadReply_BulkSizeCap(t *testing.T) {
	huge := fmt.Sprintf("$%d\r\n", resp.MaxBulkSize+1)
	rd := bufio.NewReader(strings.NewReader(huge))
	if _, err := readReply(rd); err == nil {
		t.Fatal("expected oversized bulk to error")
	}
}

func TestReadPubsubMessage_Pmessage(t *testing.T) {
	body := buildPmessage("+switch-master", "vk0 10.0.0.5 6379 10.0.0.7 6379")
	rd := bufio.NewReader(strings.NewReader(body))
	msg, err := readPubsubMessage(rd)
	if err != nil {
		t.Fatalf("readPubsubMessage: %v", err)
	}
	if msg.Channel != "+switch-master" {
		t.Errorf("Channel=%q, want +switch-master", msg.Channel)
	}
	if msg.Payload != "vk0 10.0.0.5 6379 10.0.0.7 6379" {
		t.Errorf("Payload=%q", msg.Payload)
	}
}

func TestReadPubsubMessage_PsubscribeAck(t *testing.T) {
	body := buildPsubscribeAck("+switch-master", 1)
	rd := bufio.NewReader(strings.NewReader(body))
	msg, err := readPubsubMessage(rd)
	if err != nil {
		t.Fatalf("readPubsubMessage: %v", err)
	}
	if msg.Channel != "" {
		t.Errorf("expected empty Channel for psubscribe ack, got %q", msg.Channel)
	}
}

func TestAuthIfNeeded_NoPassword(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(""))
	if err := authIfNeeded(nopConn{}, rd, ""); err != nil {
		t.Errorf("expected no AUTH when password empty, got %v", err)
	}
}

// TestPingPubsub_BufferedMessageReturnedNotDropped pins that a real
// failover event landing in the buffer ahead of the PONG (the
// read-deadline expired, then sentinel published before the keepalive
// PING) is returned for dispatch, not rejected as an unexpected reply.
// Fails-before: the prior pingPubsub errored on the non-pong array and
// the caller tore the connection down, dropping the event.
func TestPingPubsub_BufferedMessageReturnedNotDropped(t *testing.T) {
	ch := string(KindSwitchMaster)
	stream := buildPmessage(ch, "vk0 10.0.0.5 6379 10.0.0.7 6379") + "+PONG\r\n"
	rd := bufio.NewReader(strings.NewReader(stream))
	msg, err := pingPubsub(nopConn{}, rd, time.Time{})
	if err != nil {
		t.Fatalf("pingPubsub returned error for a buffered pubsub message: %v", err)
	}
	if msg.Channel != ch {
		t.Fatalf("buffered message dropped: Channel=%q, want %q", msg.Channel, ch)
	}
	if msg.Payload != "vk0 10.0.0.5 6379 10.0.0.7 6379" {
		t.Errorf("Payload=%q", msg.Payload)
	}
}

// TestPingPubsub_PongConfirmsLiveness pins the idle path: PING -> PONG
// (flat +PONG and the ["pong", ""] array some builds emit both count)
// yields a zero-value message (no real event) and no error.
func TestPingPubsub_PongConfirmsLiveness(t *testing.T) {
	for _, pong := range []string{"+PONG\r\n", "*2\r\n$4\r\npong\r\n$0\r\n\r\n"} {
		rd := bufio.NewReader(strings.NewReader(pong))
		msg, err := pingPubsub(nopConn{}, rd, time.Time{})
		if err != nil {
			t.Fatalf("pingPubsub(%q): %v", pong, err)
		}
		if msg.Channel != "" {
			t.Errorf("pingPubsub(%q): Channel=%q, want empty", pong, msg.Channel)
		}
	}
}

// TestPingPubsub_DeadConnSurfacesError pins that a read failure (peer
// gone) still surfaces as an error so the caller tears down and
// reconnects.
func TestPingPubsub_DeadConnSurfacesError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("")) // immediate EOF
	if _, err := pingPubsub(nopConn{}, rd, time.Time{}); err == nil {
		t.Fatal("expected error when the connection yields no reply")
	}
}

// nopConn satisfies net.Conn for tests that never read or write.
type nopConn struct{}

func (nopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (nopConn) Write([]byte) (int, error)        { return 0, nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return nil }
func (nopConn) RemoteAddr() net.Addr             { return nil }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

// buildPsubscribeAck returns the wire bytes for one
// `["psubscribe", <pattern>, <count>]` reply array.
func buildPsubscribeAck(pattern string, count int) string {
	return fmt.Sprintf(
		"*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
		len(pattern), pattern, count,
	)
}

// buildPmessage returns the wire bytes for one
// `["pmessage", <channel>, <channel>, <payload>]` array. Pattern
// equals channel because the observer subscribes to literal channel
// names (no glob metas).
func buildPmessage(channel, payload string) string {
	return fmt.Sprintf(
		"*4\r\n$8\r\npmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(channel), channel,
		len(channel), channel,
		len(payload), payload,
	)
}

// ─────────────────────────────────────────────────────────────────────
// Unhappy-path tests for the RESP parser. The happy-path tests above
// cover bulk / nil / array / integer / error / size-cap; these pin
// the failure modes that bite when a sentinel misbehaves or the
// socket goes half-closed mid-frame.
// ─────────────────────────────────────────────────────────────────────

// TestReadReply_EmptyStream_ReturnsEOF pins that a closed socket with
// no bytes surfaces as io.EOF (the caller distinguishes "peer gone"
// from a protocol error by checking errors.Is(err, io.EOF)).
func TestReadReply_EmptyStream_ReturnsEOF(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(""))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("empty stream: expected error, got nil")
	}
	if err != io.EOF {
		t.Errorf("empty stream: err=%v, want io.EOF", err)
	}
}

// TestReadReply_EmptyHeader_ReturnsProtocolError pins that a bare
// "\r\n" with no prefix byte is rejected with a protocol error
// (distinct from io.EOF — the stream is intact, the framing is
// wrong).
func TestReadReply_EmptyHeader_ReturnsProtocolError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("empty header: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "empty reply header") {
		t.Errorf("empty header: err=%v, want 'empty reply header'", err)
	}
}

// TestReadReply_UnknownPrefix_ReturnsProtocolError pins that an
// unrecognised prefix byte (anything other than + - : $ *) surfaces
// as a protocol error naming the offending prefix. Without this, a
// sentinel sending RESP-3 frames (`,` float, `(` big-number, `#`
// bool) would silently 0-byte the caller.
func TestReadReply_UnknownPrefix_ReturnsProtocolError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(",3.14\r\n")) // RESP-3 float
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("unknown prefix: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected reply prefix") {
		t.Errorf("unknown prefix: err=%v, want 'unexpected reply prefix'", err)
	}
}

// TestReadReply_BadBulkLength_ReturnsProtocolError pins the parse
// failure on a non-integer bulk-length header. A garbled `$abc` from
// a stale buffer or a wire-protocol bug must surface, not silently
// produce a zero-length bulk.
func TestReadReply_BadBulkLength_ReturnsProtocolError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("$abc\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("bad bulk length: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bad bulk length") {
		t.Errorf("bad bulk length: err=%v, want 'bad bulk length'", err)
	}
}

// TestReadReply_BadArrayLength_ReturnsProtocolError pins the parse
// failure on a non-integer array-length header. Same shape as bulk
// length, but the array path is its own branch.
func TestReadReply_BadArrayLength_ReturnsProtocolError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("*xyz\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("bad array length: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bad array length") {
		t.Errorf("bad array length: err=%v, want 'bad array length'", err)
	}
}

// TestReadReply_BadInteger_ReturnsProtocolError pins the parse
// failure on the integer-reply payload. A sentinel emitting `:foo`
// instead of `:42` is a wire bug; surface it rather than coerce to
// zero.
func TestReadReply_BadInteger_ReturnsProtocolError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader(":notanumber\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("bad integer: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bad integer") {
		t.Errorf("bad integer: err=%v, want 'bad integer'", err)
	}
}

// TestReadReply_PartialBulkBody_ReturnsError pins the half-closed
// socket case: the header says 10-byte bulk, only 5 bytes arrive,
// EOF. The parser must surface the read failure rather than return
// a truncated 5-byte string masquerading as a complete reply.
func TestReadReply_PartialBulkBody_ReturnsError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("$10\r\nhello")) // claims 10, only 5
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("partial bulk: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "read bulk body") {
		t.Errorf("partial bulk: err=%v, want 'read bulk body'", err)
	}
}

// TestReadReply_MissingBulkTrailer_ReturnsError pins the case where
// the bulk body bytes arrive but the trailing CRLF doesn't (peer
// hung up between the body write and the framing terminator). The
// parser must surface that as a trailer-read failure, distinct from
// a body-read failure.
func TestReadReply_MissingBulkTrailer_ReturnsError(t *testing.T) {
	rd := bufio.NewReader(strings.NewReader("$5\r\nhello")) // body present, no trailing CRLF
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("missing bulk trailer: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bulk trailer") {
		t.Errorf("missing bulk trailer: err=%v, want 'bulk trailer'", err)
	}
}

// TestReadReply_ArrayElementFails_PropagatesError pins that an inner
// parse failure inside an array reply surfaces with the element
// index, not a generic protocol error. The reconciler keys off the
// "array element N" wrapper to localise the bad frame in observer
// logs.
func TestReadReply_ArrayElementFails_PropagatesError(t *testing.T) {
	// 2-element array where the second element has a garbled prefix.
	rd := bufio.NewReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n?bad\r\n"))
	_, err := readReply(rd)
	if err == nil {
		t.Fatal("array element failure: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "array element") {
		t.Errorf("array element failure: err=%v should name the element index", err)
	}
}

// TestReadReply_ReadDeadlineTimeout_Surfaces pins that a real net.Conn
// with a passed read deadline surfaces as a timeout error during
// readReply. The observer's pull-tick relies on this — without a
// surfaced timeout, a wedged sentinel would stall the tick goroutine
// indefinitely instead of being recycled.
func TestReadReply_ReadDeadlineTimeout_Surfaces(t *testing.T) {
	// Dial a listener that accepts but never writes. SetReadDeadline
	// in the past forces an immediate timeout on the next Read.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan struct{})
	done := make(chan struct{})
	// t.Cleanup runs even when a later t.Fatal short-circuits the
	// test body; a bare `defer close(done)` would not, leaking the
	// acceptor goroutine on failure.
	t.Cleanup(func() { close(done) })
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		close(accepted)
		// Block until the test signals it's done, so the deadline is
		// the only signal the read can observe.
		<-done
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	<-accepted
	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set read deadline in past: %v", err)
	}
	rd := bufio.NewReader(c)
	_, err = readReply(rd)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// net.OpError on a tcp read carries Timeout()==true for deadline
	// expiry. The error chain matters more than the message shape
	// (which differs across Go versions).
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Errorf("expected net.Error with Timeout()==true, got %T %v", err, err)
	}
}
