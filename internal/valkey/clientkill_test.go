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

// TestDialingClientKillIssuer_NoAuth_HappyPath drives the issuer
// against a fake TCP server that responds `:42\r\n` and asserts (a)
// the issuer wrote `CLIENT KILL TYPE normal SKIPME yes` with no AUTH
// preamble, (b) the integer return is the parsed count.
func TestDialingClientKillIssuer_NoAuth_HappyPath(t *testing.T) {
	addr, stop, recv := startFakeRESP(t, func(_ *bufio.Reader, w *bufio.Writer) {
		_, _ = w.WriteString(":42\r\n")
		_ = w.Flush()
	})
	defer stop()

	d := &DialingClientKillIssuer{Timeout: 2 * time.Second}
	n, err := d.KillNormalClients(context.Background(), addr, "")
	if err != nil {
		t.Fatalf("KillNormalClients: %v", err)
	}
	if n != 42 {
		t.Errorf("count: got %d, want 42", n)
	}
	got := <-recv
	want := "*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n$6\r\nSKIPME\r\n$3\r\nyes\r\n"
	if got != want {
		t.Errorf("wire bytes: got %q want %q", got, want)
	}
}

// TestDialingClientKillIssuer_Auth_PreflightsBeforeKill drives the
// issuer with a password and asserts the AUTH preamble is sent +
// AUTH's +OK is consumed before CLIENT KILL is written.
func TestDialingClientKillIssuer_Auth_PreflightsBeforeKill(t *testing.T) {
	addr, stop, recv := startFakeRESP(t, func(_ *bufio.Reader, w *bufio.Writer) {
		// First reply: AUTH +OK.
		_, _ = w.WriteString("+OK\r\n")
		_ = w.Flush()
		// Second reply: CLIENT KILL :7.
		_, _ = w.WriteString(":7\r\n")
		_ = w.Flush()
	})
	defer stop()

	d := &DialingClientKillIssuer{Timeout: 2 * time.Second}
	n, err := d.KillNormalClients(context.Background(), addr, "s3cret")
	if err != nil {
		t.Fatalf("KillNormalClients: %v", err)
	}
	if n != 7 {
		t.Errorf("count: got %d, want 7", n)
	}
	got := <-recv
	if !strings.Contains(got, "AUTH") || !strings.Contains(got, "s3cret") {
		t.Errorf("wire bytes missing AUTH/secret: %q", got)
	}
	if !strings.Contains(got, "CLIENT") || !strings.Contains(got, "KILL") {
		t.Errorf("wire bytes missing CLIENT KILL: %q", got)
	}
	if strings.Index(got, "AUTH") > strings.Index(got, "CLIENT") {
		t.Errorf("AUTH must precede CLIENT KILL on the wire: %q", got)
	}
}

// TestDialingClientKillIssuer_ServerErrorPropagates asserts a
// `-NOPERM\r\n` server reply surfaces as an error.
func TestDialingClientKillIssuer_ServerErrorPropagates(t *testing.T) {
	addr, stop, _ := startFakeRESP(t, func(_ *bufio.Reader, w *bufio.Writer) {
		_, _ = w.WriteString("-NOPERM CLIENT command requires admin\r\n")
		_ = w.Flush()
	})
	defer stop()

	d := &DialingClientKillIssuer{Timeout: 2 * time.Second}
	_, err := d.KillNormalClients(context.Background(), addr, "")
	if err == nil {
		t.Fatal("expected error from -NOPERM reply, got nil")
	}
	if !strings.Contains(err.Error(), "NOPERM") {
		t.Errorf("error must mention NOPERM, got: %v", err)
	}
}

// TestReadInteger_HappyAndErrorPaths pins the RESP integer parser
// the issuer relies on for the CLIENT KILL reply.
func TestReadInteger_HappyAndErrorPaths(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{"positive", ":12345\r\n", 12345, false},
		{"zero", ":0\r\n", 0, false},
		{"negative", ":-5\r\n", -5, false},
		{"error-reply", "-WRONGTYPE\r\n", 0, true},
		{"bad-prefix", "+OK\r\n", 0, true},
		{"empty", "\r\n", 0, true},
		{"unparseable", ":notanum\r\n", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rd := bufio.NewReader(strings.NewReader(c.input))
			got, err := readInteger(rd)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil (n=%d)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %d want %d", got, c.want)
			}
		})
	}
}

// startFakeRESP spins up a TCP listener that calls handler on the
// first accepted connection, records every byte read off that
// connection, and pushes the accumulated bytes onto recv when the
// client closes. Returns the dial addr + a stop func + the recv
// channel.
func startFakeRESP(t *testing.T, handler func(rd *bufio.Reader, w *bufio.Writer)) (string, func(), chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	recv := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		rd := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		// Capture every byte the client writes. We run handler
		// concurrently with the read loop because the handler writes
		// reply bytes that the client expects in lock-step.
		var buf strings.Builder
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				b, err := rd.ReadByte()
				if err != nil {
					return
				}
				buf.WriteByte(b)
			}
		}()
		handler(rd, w)
		// Give the client time to consume + close.
		<-time.After(50 * time.Millisecond)
		_ = conn.Close()
		<-done
		recv <- buf.String()
	}()
	stop := func() {
		_ = ln.Close()
	}
	return ln.Addr().String(), stop, recv
}
