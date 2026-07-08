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

// Package valkey hosts the operator's data-plane client surfaces.
// LagChecker is the first one — Phase 8 calls it to learn whether a
// replica pod is caught up enough to flip its
// `velkir.ioxie.dev/replication-ready` readiness gate.
//
// The interface is the only injection point reconciler tests rely on,
// so the dialing default (RESP over a plain net.Conn) can be swapped
// for a fake without any test infrastructure beyond the Go standard
// library. Sentinel-observer code lives alongside (PSUBSCRIBE for
// `+switch-master`, INFO sentinel polling) so the data-plane client
// concerns sit in one place.
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

// DefaultPort is the canonical Valkey client-plane port. Mandatory in
// the operator's rendered valkey.conf (see internal/valkeyconf); kept
// here too so the LagChecker default doesn't have to import a
// reconciler-only constant.
const DefaultPort = 6379

// RoleMaster is the literal value Valkey reports in `INFO
// replication` under the `role:` key for a primary server. Callers
// compare LagState.Role against this constant to detect whether a
// pod is currently serving writes.
const RoleMaster = "master"

// LagState is the result of one CheckLag call.
//
//   - LinkUp == false: the replica reports `master_link_status:down`.
//     Its readiness gate must be False.
//   - LinkUp == true && LagBytes < threshold: replica is caught up;
//     gate may flip to True.
//   - LinkUp == true && LagBytes >= threshold: replica is connected
//     but behind. Caller decides (gate stays False or flips False).
//
// The role field surfaces what valkey reported for `role` in INFO so
// callers can short-circuit when a pod they thought was a replica
// has been promoted (a `role:master` pod under the
// LagChecker's gaze means the operator's view is stale; treat as
// "replication-ready by definition" and flip True).
//
// MasterReplOffset is the raw `master_repl_offset` value from INFO:
// the current write-position the master is at, OR (for replicas)
// the last-seen master offset the replica is tracking against. The
// orphan-master detector compares this between a role=master orphan
// and the elected master to surface data divergence — when the
// orphan's offset exceeds the elected master's, REPLICAOF will
// discard those writes and the audit event must surface the byte
// count.
type LagState struct {
	Role             string
	LinkUp           bool
	LagBytes         int64
	MasterReplOffset int64
	// HaveMasterOffset distinguishes a genuine master_repl_offset=0
	// from a truncated INFO reply that omitted the field entirely
	// (which would otherwise leave MasterReplOffset at its zero
	// value). The orphan-master divergence check compares offsets
	// only when both sides report HaveMasterOffset.
	HaveMasterOffset bool
	// MasterHost is the `master_host` a replica points at (empty for
	// masters and truncated replies). The zero-master recovery path
	// checks it against live pod IPs — a replica whose master host
	// matches no current pod is replicating from a corpse.
	MasterHost string
	// SlaveReplOffset is the raw `slave_repl_offset` (the replica's
	// applied position in the replication stream). The zero-master
	// recovery path ranks promotion candidates by it — the same
	// criterion Sentinel's leader uses to pick a promotion target.
	// HaveSlaveOffset mirrors HaveMasterOffset's present-vs-zero
	// disambiguation.
	SlaveReplOffset int64
	HaveSlaveOffset bool
}

// LagChecker queries one Valkey pod and reports its replication
// freshness. Implementations must be context-respecting and never
// block past the context deadline; the dialing default uses a
// per-step DialTimeout + read deadlines.
type LagChecker interface {
	CheckLag(ctx context.Context, addr, password string) (LagState, error)
}

// DialingLagChecker is the production LagChecker. Connects via plain
// TCP, optionally AUTH-s, runs `INFO replication`, and parses the
// reply. Stateless — safe to share across goroutines.
//
// TLS support lives in a future field (post-v1 backlog); the
// data-plane is plaintext-on-cluster-network for v1, matching the
// rendered valkey.conf's `protected-mode no` and the operator's
// IP-only peer-addressing rule.
type DialingLagChecker struct {
	// Timeout caps the entire check (dial + AUTH + INFO + read).
	// Zero defaults to 5s; callers can tighten via the context as
	// well — whichever expires first wins.
	Timeout time.Duration

	// dialer, when non-nil, replaces the per-call net.Dialer. Tests
	// inject an in-memory fake to drive the deadline path
	// deterministically; nil in production. The shared contextDialer
	// seam type lives in dial.go.
	dialer contextDialer
}

// CheckLag implements LagChecker. addr must be `host:port` (the
// caller is expected to assemble it from pod.Status.PodIP and
// DefaultPort). password may be empty when the CR has no auth.
func (d *DialingLagChecker) CheckLag(ctx context.Context, addr, password string) (LagState, error) {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, rd, err := dialAndAuth(ctx, addr, password, timeout, d.dialer)
	if err != nil {
		return LagState{}, err
	}
	defer closeConn(ctx, conn)

	if _, err := io.WriteString(conn, resp.EncodeCommand("INFO", "replication")); err != nil {
		return LagState{}, fmt.Errorf("write INFO: %w", err)
	}
	body, err := readBulkString(rd)
	if err != nil {
		return LagState{}, fmt.Errorf("read INFO reply: %w", err)
	}
	return parseInfoReplication(body), nil
}

// readSimpleOrError parses a `+OK\r\n` (simple string) or `-ERR...\r\n`
// (error) reply. Returns the simple-string body on success, an error
// otherwise. The bulk-string and array forms aren't needed for AUTH.
func readSimpleOrError(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return "", fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("server: %s", line[1:])
	default:
		return "", fmt.Errorf("unexpected reply prefix %q (line=%q)", line[0], line)
	}
}

// readBulkString parses a `$<n>\r\n<n bytes>\r\n` (bulk string) reply.
// `$-1\r\n` (nil) becomes "". Other prefixes are an error.
func readBulkString(rd *bufio.Reader) (string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	header = strings.TrimRight(header, "\r\n")
	if len(header) == 0 {
		return "", fmt.Errorf("empty bulk header")
	}
	if header[0] == '-' {
		return "", fmt.Errorf("server: %s", header[1:])
	}
	if header[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got %q", header[0])
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil {
		return "", fmt.Errorf("bad bulk length %q: %w", header[1:], err)
	}
	if n < 0 {
		return "", nil
	}
	if n > resp.MaxBulkSize {
		// Cap before make() — see resp.MaxBulkSize for the
		// DoS-amplification rationale. Realistic INFO replication
		// payloads are 1–5 KiB; anything larger is either a bug in
		// the server or a hostile actor.
		return "", fmt.Errorf("bulk string length %d exceeds cap %d", n, resp.MaxBulkSize)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return "", fmt.Errorf("read bulk body: %w", err)
	}
	// Consume the trailing CRLF.
	if _, err := rd.Discard(2); err != nil {
		return "", fmt.Errorf("read bulk trailer: %w", err)
	}
	return string(buf), nil
}

// parseInfoReplication walks the `INFO replication` text response and
// extracts the fields Phase 8 cares about. The text is a series of
// `key:value\r\n` lines with a `# Replication\r\n` header; unknown
// fields are ignored so a future Valkey version that adds new lines
// won't break us.
//
// Lag is `master_repl_offset - slave_repl_offset` for replicas; for
// pods reporting `role:master` we return LagBytes=0 (the master is by
// definition replication-ready, and no slave_repl_offset field is
// present). LinkUp follows `master_link_status:up`.
func parseInfoReplication(body string) LagState {
	var (
		role          string
		linkUp        bool
		masterHost    string
		masterOff     int64
		slaveOff      int64
		haveMasterOff bool
		haveSlave     bool
	)
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch k {
		case "role":
			role = v
		case "master_host":
			masterHost = v
		case "master_link_status":
			linkUp = v == "up"
		case "master_repl_offset":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				masterOff = n
				haveMasterOff = true
			}
		case "slave_repl_offset":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				slaveOff = n
				haveSlave = true
			}
		}
	}

	st := LagState{Role: role, MasterReplOffset: masterOff, HaveMasterOffset: haveMasterOff}
	if role == RoleMaster {
		// Master is replication-ready by definition; no link status to
		// inspect. Leaving LinkUp false here would force the caller's
		// gate-flip logic to special-case the role; surfacing role
		// directly lets the caller short-circuit cleanly.
		return st
	}
	st.LinkUp = linkUp
	st.MasterHost = masterHost
	st.SlaveReplOffset = slaveOff
	st.HaveSlaveOffset = haveSlave
	if haveSlave && masterOff >= slaveOff {
		st.LagBytes = masterOff - slaveOff
	}
	return st
}
