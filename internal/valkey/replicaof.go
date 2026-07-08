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
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// ReplicaOfTimeout caps a single REPLICAOF round-trip (including
// any AUTH + CONFIG SET masterauth preamble). 5s — REPLICAOF is
// local state work (the target writes its in-memory state +
// schedules an async sync to the new master); no fan-out happens
// synchronously in the command path.
const ReplicaOfTimeout = 5 * time.Second

// ReplicaOfIssuer is the test-injection seam for issuing REPLICAOF
// against a Valkey pod. The production implementation is
// DialingReplicaOfIssuer; tests inject a fake that records the
// commands without opening sockets.
type ReplicaOfIssuer interface {
	// IssueReplicaOf instructs the Valkey instance at addr to
	// replicate from masterIP:masterPort. The password authenticates
	// the issuer's connection AND is propagated to the target's
	// `masterauth` config so the subsequent replication handshake
	// against the master can authenticate. password=="" is the
	// no-AUTH path (no CONFIG SET, no AUTH).
	IssueReplicaOf(ctx context.Context, addr, password, masterIP string, masterPort int) error
}

// PromoteIssuer is the test-injection seam for promoting a Valkey
// replica to master via `REPLICAOF NO ONE`. Kept as a separate
// interface (rather than a second ReplicaOfIssuer method) so
// existing ReplicaOfIssuer fakes keep compiling; the production
// DialingReplicaOfIssuer implements both.
//
// Promotion is reserved for the zero-master recovery path: every
// address the sentinel quorum knows is dead, no live pod
// self-reports master, and Sentinel's own election cannot succeed
// because its candidate set is entirely dead. The caller owns ALL
// safety gating; this issuer only speaks the wire form.
type PromoteIssuer interface {
	IssuePromote(ctx context.Context, addr, password string) error
}

// DialingReplicaOfIssuer is the production ReplicaOfIssuer. Connects
// via plain TCP (same TLS-future-work as DialingLagChecker), AUTH-s,
// optionally sets `masterauth`, then issues REPLICAOF. Stateless —
// safe to share across goroutines.
type DialingReplicaOfIssuer struct {
	// Timeout caps the entire issue (dial + AUTH + CONFIG SET +
	// REPLICAOF + read). Zero defaults to ReplicaOfTimeout; callers
	// can tighten via the context.
	Timeout time.Duration

	// dialer, when non-nil, replaces the per-call net.Dialer (the shared
	// contextDialer seam, dial.go). Tests inject an in-memory fake; nil in
	// production.
	dialer contextDialer
}

// IssueReplicaOf implements ReplicaOfIssuer.
//
// Sequence (with auth):
//
//	AUTH <password>                                            → +OK
//	CONFIG SET masterauth <password>                            → +OK
//	REPLICAOF <masterIP> <masterPort>                          → +OK
//
// Without auth, the AUTH + CONFIG SET pair is skipped.
//
// CONFIG SET masterauth is load-bearing on a freshly-recreated pod
// whose in-memory masterauth was lost — without it the replication
// handshake against the master fails with NOAUTH and the orphan
// stays disconnected. The valkey.conf rendered at pod init does
// stamp masterauth too, but a runtime CONFIG GET masterauth on the
// orphan can return empty if the config was reloaded without it.
// Always-set is cheap (one extra RTT) and idempotent.
//
// REPLICAOF (formerly SLAVEOF) is the modern Valkey/Redis command
// and is the only form the operator emits. Use a "real" IP +
// port, never DNS — sentinel-cluster IP rules require this.
func (d *DialingReplicaOfIssuer) IssueReplicaOf(ctx context.Context, addr, password, masterIP string, masterPort int) error {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = ReplicaOfTimeout
	}
	conn, rd, err := dialAndAuth(ctx, addr, password, timeout, d.dialer)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	// CONFIG SET masterauth is part of this issuer's post-AUTH tail (it
	// is the credential the replica will use to AUTH to its new master),
	// so it stays here rather than in the shared preamble — the other
	// issuers must not pay this RTT.
	if password != "" {
		if _, err := io.WriteString(conn, resp.EncodeCommand("CONFIG", "SET", "masterauth", password)); err != nil {
			return fmt.Errorf("write CONFIG SET masterauth: %w", err)
		}
		if _, err := readSimpleOrError(rd); err != nil {
			return fmt.Errorf("CONFIG SET masterauth: %w", err)
		}
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("REPLICAOF", masterIP, strconv.Itoa(masterPort))); err != nil {
		return fmt.Errorf("write REPLICAOF: %w", err)
	}
	if _, err := readSimpleOrError(rd); err != nil {
		return fmt.Errorf("REPLICAOF: %w", err)
	}
	return nil
}

// IssuePromote implements PromoteIssuer: `REPLICAOF NO ONE` after the
// AUTH preamble. No `CONFIG SET masterauth` tail — the target stops
// replicating, so the replica-side credential is irrelevant (and the
// pod's rendered config still carries it for a later demotion).
func (d *DialingReplicaOfIssuer) IssuePromote(ctx context.Context, addr, password string) error {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = ReplicaOfTimeout
	}
	conn, rd, err := dialAndAuth(ctx, addr, password, timeout, d.dialer)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	if _, err := io.WriteString(conn, resp.EncodeCommand("REPLICAOF", "NO", "ONE")); err != nil {
		return fmt.Errorf("write REPLICAOF NO ONE: %w", err)
	}
	if _, err := readSimpleOrError(rd); err != nil {
		return fmt.Errorf("REPLICAOF NO ONE: %w", err)
	}
	return nil
}
