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

// Package valkey — shared connection preamble for the single-password
// data-plane issuers. CheckLag, KillNormalClients, and IssueReplicaOf
// all open a plain-TCP connection, clamp one overall deadline, and AUTH
// (when a password is set) the same way; this file hosts that preamble
// so the three issuers no longer carry a copy each.
package valkey

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-logr/logr"

	"github.com/ioxie/velkir/internal/resp"
)

// contextDialer abstracts net.Dialer.DialContext so the dial path can be
// driven by an in-memory fake in tests, without real network egress or a
// wall-clock sleep. It is the single shared seam for all four data-plane
// issuers (CheckLag, IssueReplicaOf, KillNormalClients, rotateOne); each
// leaves its dialer nil in production, where dialAndAuth builds a
// net.Dialer per call.
type contextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// dialAndAuth opens the data-plane connection to addr and runs the
// shared preamble: clamp timeout to the context's remaining deadline,
// dial (via dialer when non-nil — test injection — else a per-call
// net.Dialer bounded by timeout), set one overall connection deadline
// for the whole exchange, and AUTH when password is non-empty. timeout
// is the issuer's already-resolved value (its field-or-default), so each
// issuer keeps its own default.
//
// It stops at AUTH: each issuer's post-AUTH command tail (and any
// CONFIG SET) stays in the caller, so no issuer pays an RTT it did not
// before. On success it returns the open connection — the CALLER owns
// Close via closeConn — and a buffered reader positioned after the AUTH
// reply. On any preamble error it closes the connection itself (the
// caller has not yet installed its deferred close) and returns the same
// error the issuers returned inline before the extraction.
func dialAndAuth(ctx context.Context, addr, password string, timeout time.Duration, dialer contextDialer) (net.Conn, *bufio.Reader, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return nil, nil, fmt.Errorf("context deadline exceeded before dial")
	}

	var (
		conn net.Conn
		err  error
	)
	if dialer != nil {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	} else {
		nd := net.Dialer{Timeout: timeout}
		conn, err = nd.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	// One overall deadline for the whole exchange — simpler and tighter
	// than per-op deadlines; the protocol is two short round-trips at
	// most.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		closeConn(ctx, conn)
		return nil, nil, fmt.Errorf("set deadline: %w", err)
	}

	rd := bufio.NewReader(conn)

	if password != "" {
		if _, err := io.WriteString(conn, resp.EncodeCommand("AUTH", password)); err != nil {
			closeConn(ctx, conn)
			return nil, nil, fmt.Errorf("write AUTH: %w", err)
		}
		if _, err := readSimpleOrError(rd); err != nil {
			closeConn(ctx, conn)
			return nil, nil, fmt.Errorf("AUTH: %w", err)
		}
	}

	return conn, rd, nil
}

// closeConn closes conn, logging a close failure at V(1) — the shared
// form of the deferred close the issuers install once dialAndAuth hands
// the connection back.
func closeConn(ctx context.Context, conn net.Conn) {
	if cerr := conn.Close(); cerr != nil {
		logr.FromContextOrDiscard(ctx).V(1).Info("close conn failed", "err", cerr)
	}
}
