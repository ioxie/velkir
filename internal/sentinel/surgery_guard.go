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
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/ioxie/velkir/internal/resp"
)

// masterReachable dials the resolved master address and issues PING.
// Gate for destructive sentinel surgery: a freshly REMOVE+MONITORed
// sentinel learns replicas and peer sentinels ONLY from the master it
// is registered at — re-registering an empty sentinel against a dead
// master strands it permanently (no replicas to promote, no peers to
// vote with). The password is the data-plane requirepass, the same
// credential the operator's INFO probes use against valkey pods.
func masterReachable(ctx context.Context, addr, password string) error {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	rd := bufio.NewReaderSize(conn, readBufSize)
	if err := authIfNeeded(conn, rd, password); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("PING")); err != nil {
		return fmt.Errorf("write PING: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("read PING: %w", err)
	}
	if s, ok := reply.(string); !ok || s != "PONG" {
		return fmt.Errorf("PING: unexpected reply %v", reply)
	}
	return nil
}

// anyFailoverInProgress queries `SENTINEL MASTER <name>` on each
// endpoint and reports whether ANY reachable one carries
// failover_in_progress in its master flags. Used to keep destructive
// surgery (REMOVE+MONITOR on stranded sentinels) from racing an
// election a healthy survivor is mid-driving — the survivor's promoted
// candidate would be re-pointed at the pre-election master.
//
// Endpoints that fail to answer cannot veto (the surgery gate must not
// wedge on an unreachable sentinel — the minority guard upstream
// already bounds how few survivors are acceptable). s_down/o_down on a
// survivor does NOT veto: the survivor's master entry may point at the
// dead pre-incident address, and that state is exactly what the
// recovery exists to repair.
func anyFailoverInProgress(ctx context.Context, endpoints []Endpoint, masterName, password string) bool {
	if len(endpoints) == 0 {
		return false
	}
	results := make([]bool, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			inProgress, err := failoverInProgressOne(ctx, ep, masterName, password)
			results[i] = err == nil && inProgress
		}(i, ep)
	}
	wg.Wait()
	for _, r := range results {
		if r {
			return true
		}
	}
	return false
}

// failoverInProgressOne fetches one sentinel's `SENTINEL MASTER <name>`
// record and checks the flags field for failover_in_progress.
func failoverInProgressOne(ctx context.Context, ep Endpoint, masterName, password string) (bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	rd := bufio.NewReaderSize(conn, readBufSize)
	if err := authIfNeeded(conn, rd, password); err != nil {
		return false, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "master", masterName)); err != nil {
		return false, fmt.Errorf("write SENTINEL master: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return false, fmt.Errorf("read SENTINEL master: %w", err)
	}
	flags, ok := masterReplyFlags(reply)
	if !ok {
		// No flags field (or no such master) — nothing in progress to
		// veto on.
		return false, nil
	}
	return strings.Contains(flags, "failover_in_progress"), nil
}

// flagsIndicateElection reports whether a comma-joined SENTINEL MASTER
// `flags` value carries any token that means an election is brewing or
// running on that master: failover_in_progress (mid-failover), o_down
// (quorum-agreed objectively down — vote-gathering), or s_down (this
// sentinel's subjective down — the pre-election window). Substring
// matching is the same shape failoverInProgressOne already uses; none
// of the three tokens is a substring of a benign flag.
func flagsIndicateElection(flags string) bool {
	return strings.Contains(flags, "failover_in_progress") ||
		strings.Contains(flags, "o_down") ||
		strings.Contains(flags, "s_down")
}

// recheckElectionQuiet re-reads SENTINEL MASTER <name> on each target
// immediately before destructive surgery and returns only those whose
// reply parses AND whose flags are NOT flagsIndicateElection. It is the
// pre-surgery fresh re-check that shrinks the classify→REMOVE window to
// ~0 for the live-but-different re-point sub-class (those targets bypass
// the doomed filter and are excluded from the survivor election veto,
// so a fresh read is the only time-independent guarantee neither the
// target nor its own master has begun an election since classification).
// Fail-safe: a target that errors or cannot be confirmed quiet is
// dropped (cannot prove quiet ⇒ do not wipe). Same fresh-conn pattern
// as failoverInProgressOne.
func recheckElectionQuiet(ctx context.Context, targets []Endpoint, masterName, password string) []Endpoint {
	if len(targets) == 0 {
		return nil
	}
	quiet := make([]bool, len(targets))
	var wg sync.WaitGroup
	for i, ep := range targets {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			flags, ok, err := masterFlagsOne(ctx, ep, masterName, password)
			quiet[i] = err == nil && ok && !flagsIndicateElection(flags)
		}(i, ep)
	}
	wg.Wait()
	out := make([]Endpoint, 0, len(targets))
	for i, ep := range targets {
		if quiet[i] {
			out = append(out, ep)
		}
	}
	return out
}

// cohortMasterRead is one masterIP-cohort member's fresh SENTINEL MASTER
// re-read, captured per goroutine index for the destination pre-surgery
// re-check (written by exactly one goroutine, read after wg.Wait — race-free).
type cohortMasterRead struct {
	flags   string
	flagsOK bool
	epoch   int64
	epochOK bool
	err     error
}

// destinationCohortQuiet re-reads SENTINEL MASTER <name> on the masterIP
// cohort (the endpoints monitoring the resolved master, captured at classify)
// immediately before destructive surgery, and reports whether the destination
// is still safe to re-point ONTO. This is the destination-side twin of
// recheckElectionQuiet: the classify-time destination guards (frontier Guard A;
// dest-election guard) are otherwise never re-validated across the
// classify→RemoveAll window, so a failover that deposed the destination — or
// moved the frontier past it — in that window would slip through.
//
// It disarms (returns false ⇒ the caller drops the whole surviving stale-epoch
// subset) only when:
//
//   - Flags fail-safe (per member): ANY cohort member's fresh read ERRORS,
//     carries no flags field, or flagsIndicateElection — a member we cannot
//     prove is not-electing must disarm. This is the load-bearing
//     dest-election guard; it stays per-member.
//   - Frontier (max-based, symmetric with classify Guard A): the MAX config-epoch
//     over the members that returned a readable epoch has fallen below the
//     classify-time agreeEpoch — the cohort as a whole no longer holds the
//     frontier. If NO member returned a readable epoch, disarm (the frontier
//     can't be established → fail-safe).
//
// The frontier is intentionally max-based: classify's Guard A only requires the
// cohort MAX to hold the frontier (agreeEpoch is itself that max), so a single
// below-max member — a sentinel lagging or freshly SENTINEL MONITOR-reset onto
// masterIP, which sits at a lower config-epoch until monotonic gossip
// convergence catches it up — is legitimate and must NOT disarm as long as some
// member still holds >= agreeEpoch. An all-members >= agreeEpoch test would be
// asymmetric with classify and self-defeat the re-point in exactly the
// post-failover / post-surgery convergence window the class exists to repair.
//
// An empty cohort is treated as unconfirmable (never quiet); classification
// only produces a cohort alongside selected targets, so this only bites a
// degraded caller. Same fresh-conn pattern as recheckElectionQuiet; each
// goroutine writes its own slice index, read after wg.Wait.
func destinationCohortQuiet(ctx context.Context, cohort []Endpoint, masterName, password string, agreeEpoch int64) bool {
	if len(cohort) == 0 {
		return false
	}
	reads := make([]cohortMasterRead, len(cohort))
	var wg sync.WaitGroup
	for i, ep := range cohort {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			r := &reads[i]
			r.flags, r.flagsOK, r.epoch, r.epochOK, r.err = masterFlagsEpochOne(ctx, ep, masterName, password)
		}(i, ep)
	}
	wg.Wait()

	var maxEpoch int64
	frontierKnown := false
	for i := range reads {
		r := reads[i]
		// Flags fail-safe (per member): cannot prove this member is
		// not-electing ⇒ disarm.
		if r.err != nil || !r.flagsOK || flagsIndicateElection(r.flags) {
			return false
		}
		if r.epochOK && (!frontierKnown || r.epoch > maxEpoch) {
			maxEpoch = r.epoch
			frontierKnown = true
		}
	}
	// Frontier (max-based): the cohort as a whole must still hold the
	// classify-time frontier; a below-max member does not disarm.
	return frontierKnown && maxEpoch >= agreeEpoch
}

// masterFlagsEpochOne fetches one sentinel's SENTINEL MASTER <name> record
// and returns its `flags` field, its `config-epoch`, and a per-field OK bit
// for each. flagsOK / epochOK are false when the reply carries no such field
// (or no such master); err is non-nil only on a wire-side failure (dial /
// auth / read). The epoch-carrying sibling of masterFlagsOne — the
// destination re-check must compare the frontier as well as the flags.
func masterFlagsEpochOne(ctx context.Context, ep Endpoint, masterName, password string) (string, bool, int64, bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return "", false, 0, false, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	rd := bufio.NewReaderSize(conn, readBufSize)
	if err := authIfNeeded(conn, rd, password); err != nil {
		return "", false, 0, false, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "master", masterName)); err != nil {
		return "", false, 0, false, fmt.Errorf("write SENTINEL master: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return "", false, 0, false, fmt.Errorf("read SENTINEL master: %w", err)
	}
	flags, flagsOK := masterReplyFlags(reply)
	epoch, epochOK := ParseSentinelMasterEpoch(reply)
	return flags, flagsOK, epoch, epochOK, nil
}

// masterFlagsOne fetches one sentinel's SENTINEL MASTER <name> record
// and returns its `flags` field. ok is false when the reply has no
// flags field (or no such master); err is non-nil only on a wire-side
// failure (dial / auth / read).
func masterFlagsOne(ctx context.Context, ep Endpoint, masterName, password string) (string, bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return "", false, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	rd := bufio.NewReaderSize(conn, readBufSize)
	if err := authIfNeeded(conn, rd, password); err != nil {
		return "", false, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "master", masterName)); err != nil {
		return "", false, fmt.Errorf("write SENTINEL master: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return "", false, fmt.Errorf("read SENTINEL master: %w", err)
	}
	flags, ok := masterReplyFlags(reply)
	return flags, ok, nil
}

// isNoSuchMasterErr reports whether err is the canonical sentinel
// "no such master with that name" reply — a sentinel that answered but
// carries no entry for the monitored master (e.g. a recovery pass
// interrupted after REMOVE, before MONITOR). Matched on the canonical
// substring, tight enough not to swallow unrelated errors.
func isNoSuchMasterErr(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "no such master with that name")
}

// masterReplyFlags extracts the `flags` value from the flat
// key/value-pair array reply `SENTINEL MASTER <name>` returns. ok is
// false when the reply is not an array or carries no flags field.
func masterReplyFlags(reply any) (string, bool) {
	arr, ok := reply.([]any)
	if !ok {
		return "", false
	}
	for i := 0; i+1 < len(arr); i += 2 {
		k, ok := arr[i].(string)
		if !ok || k != flagsField {
			continue
		}
		v, ok := arr[i+1].(string)
		return v, ok
	}
	return "", false
}
