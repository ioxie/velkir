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
	"net"
	"strings"
	"testing"
	"time"
)

// startVerbKeyedFakeValkey accepts one connection, reads RESP
// command arrays, and replies based on the verb-keyed scriptedReplies
// map. The first-token (verb) is uppercased before lookup so callers
// can script `AUTH`, `REPLICAOF`, `CONFIG`, etc. independently. A
// missing scripted reply for a verb falls back to `-ERR no reply
// scripted` so tests fail loudly rather than hang. Returns the
// listener address; the goroutine self-cleans on test exit.
//
// expectedPassword: when non-empty, AUTH replies +OK only if the
// supplied password matches; otherwise -ERR.
func startVerbKeyedFakeValkey(t *testing.T, scriptedReplies map[string]string, expectedPassword string) (string, *[]string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var observed []string

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		rd := bufio.NewReader(conn)
		for {
			args, err := readRESPArray(rd)
			if err != nil {
				return
			}
			observed = append(observed, strings.Join(args, " "))
			verb := strings.ToUpper(args[0])
			if verb == "AUTH" {
				if len(args) >= 2 && args[1] == expectedPassword {
					_, _ = conn.Write([]byte("+OK\r\n"))
				} else {
					_, _ = conn.Write([]byte("-ERR invalid password\r\n"))
				}
				continue
			}
			reply, ok := scriptedReplies[verb]
			if !ok {
				_, _ = conn.Write([]byte("-ERR no reply scripted for " + verb + "\r\n"))
				continue
			}
			_, _ = conn.Write([]byte(reply))
		}
	}()
	return ln.Addr().String(), &observed
}

func readRESPArray(rd *bufio.Reader) ([]string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimRight(header, "\r\n")
	if header == "" || header[0] != '*' {
		return nil, &respErr{want: "array"}
	}
	var n int
	if err := fmtSscanInt(header[1:], &n); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for range n {
		bulkHeader, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimRight(bulkHeader, "\r\n")
		if bulkHeader == "" || bulkHeader[0] != '$' {
			return nil, &respErr{want: "bulk header"}
		}
		var l int
		if err := fmtSscanInt(bulkHeader[1:], &l); err != nil {
			return nil, err
		}
		buf := make([]byte, l)
		if _, err := readFull(rd, buf); err != nil {
			return nil, err
		}
		_, _ = rd.Discard(2)
		out = append(out, string(buf))
	}
	return out, nil
}

type respErr struct{ want string }

func (e *respErr) Error() string { return "expected " + e.want }

func fmtSscanInt(s string, dst *int) error {
	n := 0
	for i, ch := range s {
		if ch < '0' || ch > '9' {
			if i == 0 {
				return &respErr{want: "int"}
			}
			break
		}
		n = n*10 + int(ch-'0')
	}
	*dst = n
	return nil
}

func readFull(rd *bufio.Reader, buf []byte) (int, error) {
	read := 0
	for read < len(buf) {
		n, err := rd.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

func TestIssueReplicaOf_HappyPath_NoAuth(t *testing.T) {
	addr, observed := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "+OK\r\n",
	}, "")

	issuer := &DialingReplicaOfIssuer{}
	if err := issuer.IssueReplicaOf(context.Background(), addr, "", "10.0.0.5", 6379); err != nil {
		t.Fatalf("IssueReplicaOf: %v", err)
	}
	// Wire-format assertion: no AUTH, no CONFIG SET, exactly one
	// REPLICAOF with the correct IP + port arguments.
	if len(*observed) != 1 {
		t.Fatalf("expected exactly 1 command sent (no-auth path), got %d: %v", len(*observed), *observed)
	}
	if got := (*observed)[0]; got != "REPLICAOF 10.0.0.5 6379" {
		t.Errorf("command = %q; want %q", got, "REPLICAOF 10.0.0.5 6379")
	}
}

func TestIssueReplicaOf_HappyPath_WithAuth(t *testing.T) {
	addr, observed := startVerbKeyedFakeValkey(t, map[string]string{
		"CONFIG":    "+OK\r\n",
		"REPLICAOF": "+OK\r\n",
	}, "s3cret")

	issuer := &DialingReplicaOfIssuer{}
	if err := issuer.IssueReplicaOf(context.Background(), addr, "s3cret", "10.0.0.5", 6379); err != nil {
		t.Fatalf("IssueReplicaOf: %v", err)
	}
	// With auth: AUTH → CONFIG SET masterauth → REPLICAOF, in that order.
	if len(*observed) != 3 {
		t.Fatalf("expected 3 commands (AUTH + CONFIG SET + REPLICAOF), got %d: %v", len(*observed), *observed)
	}
	if got := (*observed)[0]; got != "AUTH s3cret" {
		t.Errorf("cmd[0] = %q; want AUTH s3cret", got)
	}
	if got := (*observed)[1]; got != "CONFIG SET masterauth s3cret" {
		t.Errorf("cmd[1] = %q; want CONFIG SET masterauth s3cret", got)
	}
	if got := (*observed)[2]; got != "REPLICAOF 10.0.0.5 6379" {
		t.Errorf("cmd[2] = %q; want REPLICAOF 10.0.0.5 6379", got)
	}
}

func TestIssueReplicaOf_AuthFailureSurfaces(t *testing.T) {
	addr, _ := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "+OK\r\n",
	}, "correct-password")

	issuer := &DialingReplicaOfIssuer{}
	err := issuer.IssueReplicaOf(context.Background(), addr, "wrong-password", "10.0.0.5", 6379)
	if err == nil {
		t.Fatal("expected AUTH failure to surface")
	}
	if !strings.Contains(err.Error(), "AUTH") {
		t.Errorf("error = %q; want AUTH-named failure", err)
	}
}

func TestIssueReplicaOf_REPLICAOFErrorSurfaces(t *testing.T) {
	addr, _ := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "-ERR Can't REPLICAOF: master is the same as self\r\n",
	}, "")

	issuer := &DialingReplicaOfIssuer{}
	err := issuer.IssueReplicaOf(context.Background(), addr, "", "10.0.0.5", 6379)
	if err == nil {
		t.Fatal("expected REPLICAOF error to surface")
	}
	if !strings.Contains(err.Error(), "REPLICAOF") {
		t.Errorf("error = %q; want REPLICAOF-named failure", err)
	}
}

func TestIssueReplicaOf_DialFailureSurfaces(t *testing.T) {
	issuer := &DialingReplicaOfIssuer{Timeout: 200 * time.Millisecond}
	err := issuer.IssueReplicaOf(context.Background(), "127.0.0.1:1", "", "10.0.0.5", 6379)
	if err == nil {
		t.Fatal("expected dial error against unreachable port")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error = %q; want dial-named failure", err)
	}
}

func TestIssuePromote_HappyPath_NoAuth(t *testing.T) {
	addr, observed := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "+OK\r\n",
	}, "")

	issuer := &DialingReplicaOfIssuer{}
	if err := issuer.IssuePromote(context.Background(), addr, ""); err != nil {
		t.Fatalf("IssuePromote: %v", err)
	}
	// Wire-format assertion: exactly one REPLICAOF NO ONE, no AUTH,
	// no CONFIG SET masterauth (the target stops replicating).
	if len(*observed) != 1 {
		t.Fatalf("expected exactly 1 command sent (no-auth path), got %d: %v", len(*observed), *observed)
	}
	if got := (*observed)[0]; got != "REPLICAOF NO ONE" {
		t.Errorf("command = %q; want %q", got, "REPLICAOF NO ONE")
	}
}

func TestIssuePromote_HappyPath_WithAuth(t *testing.T) {
	addr, observed := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "+OK\r\n",
	}, "s3cret")

	issuer := &DialingReplicaOfIssuer{}
	if err := issuer.IssuePromote(context.Background(), addr, "s3cret"); err != nil {
		t.Fatalf("IssuePromote: %v", err)
	}
	// With auth: AUTH → REPLICAOF NO ONE — and NO masterauth tail.
	if len(*observed) != 2 {
		t.Fatalf("expected 2 commands (AUTH + REPLICAOF NO ONE), got %d: %v", len(*observed), *observed)
	}
	if got := (*observed)[0]; got != "AUTH s3cret" {
		t.Errorf("cmd[0] = %q; want AUTH s3cret", got)
	}
	if got := (*observed)[1]; got != "REPLICAOF NO ONE" {
		t.Errorf("cmd[1] = %q; want REPLICAOF NO ONE", got)
	}
}

func TestIssuePromote_ErrorReplySurfaces(t *testing.T) {
	addr, _ := startVerbKeyedFakeValkey(t, map[string]string{
		"REPLICAOF": "-ERR unable to promote\r\n",
	}, "")

	issuer := &DialingReplicaOfIssuer{}
	err := issuer.IssuePromote(context.Background(), addr, "")
	if err == nil {
		t.Fatal("expected REPLICAOF NO ONE error to surface")
	}
	if !strings.Contains(err.Error(), "REPLICAOF NO ONE") {
		t.Errorf("error = %q; want REPLICAOF NO ONE-named failure", err)
	}
}

func TestIssuePromote_DialFailureSurfaces(t *testing.T) {
	issuer := &DialingReplicaOfIssuer{Timeout: 200 * time.Millisecond}
	err := issuer.IssuePromote(context.Background(), "127.0.0.1:1", "")
	if err == nil {
		t.Fatal("expected dial error against unreachable port")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error = %q; want dial-named failure", err)
	}
}
