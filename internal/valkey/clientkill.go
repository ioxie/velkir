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
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// ClientKillTimeout caps a single CLIENT KILL round-trip. 5s — the
// command is local state work on the target Valkey (close FDs for
// matching client connections) and returns an integer count.
const ClientKillTimeout = 5 * time.Second

// ClientKillIssuer is the test-injection seam for issuing CLIENT
// KILL against a Valkey pod that has just been demoted from primary
// to replica. The production implementation is
// DialingClientKillIssuer; tests inject a fake that records the
// commands without opening sockets.
type ClientKillIssuer interface {
	// KillNormalClients issues `CLIENT KILL TYPE normal SKIPME yes`
	// at addr after AUTH-ing if password != "". Returns the number
	// of clients dropped (the integer reply from CLIENT KILL) on
	// success, or an error on transport / protocol failure.
	//
	// "TYPE normal" excludes replicas, monitors, master, and the
	// pubsub stream — so a primary→replica transition only drops
	// stale write-pool connections, not the operator's own observers
	// or the replication link about to be re-established by the
	// orphan-master demote path.
	//
	// "SKIPME yes" excludes the issuer's own connection from the
	// kill set (defensive: this connection is closed by the deferred
	// conn.Close anyway).
	KillNormalClients(ctx context.Context, addr, password string) (int, error)
}

// DialingClientKillIssuer is the production ClientKillIssuer. Same
// transport / auth shape as DialingReplicaOfIssuer; the difference
// is the command + reply parsing (integer reply, not simple string).
// Stateless — safe to share across goroutines.
type DialingClientKillIssuer struct {
	// Timeout caps the entire issue (dial + AUTH + CLIENT KILL +
	// read). Zero defaults to ClientKillTimeout; callers can tighten
	// via the context.
	Timeout time.Duration

	// dialer, when non-nil, replaces the per-call net.Dialer (the shared
	// contextDialer seam, dial.go). Tests inject an in-memory fake; nil in
	// production.
	dialer contextDialer
}

// KillNormalClients implements ClientKillIssuer.
func (d *DialingClientKillIssuer) KillNormalClients(ctx context.Context, addr, password string) (int, error) {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = ClientKillTimeout
	}
	conn, rd, err := dialAndAuth(ctx, addr, password, timeout, d.dialer)
	if err != nil {
		return 0, err
	}
	defer closeConn(ctx, conn)

	if _, err := io.WriteString(conn, resp.EncodeCommand("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")); err != nil {
		return 0, fmt.Errorf("write CLIENT KILL: %w", err)
	}
	n, err := readInteger(rd)
	if err != nil {
		return 0, fmt.Errorf("CLIENT KILL: %w", err)
	}
	return n, nil
}

// readInteger parses a `:<n>\r\n` (integer) RESP reply. `-…\r\n`
// (error) becomes an error. Other prefixes are an error.
func readInteger(rd *bufio.Reader) (int, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return 0, fmt.Errorf("empty reply")
	}
	switch line[0] {
	case ':':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return 0, fmt.Errorf("parse integer %q: %w", line[1:], err)
		}
		return n, nil
	case '-':
		return 0, fmt.Errorf("server: %s", line[1:])
	default:
		return 0, fmt.Errorf("unexpected reply prefix %q (line=%q)", line[0], line)
	}
}
