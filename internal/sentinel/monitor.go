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
	"net"
	"strconv"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ioxie/velkir/internal/resp"
)

// MonitorTimeout caps a single SENTINEL MONITOR round-trip per pod.
// 5s — MONITOR is local-state work (sentinel writes the new master
// pointer into its in-memory state and persists to sentinel.conf);
// no fan-out to peers happens synchronously in the command path.
const MonitorTimeout = 5 * time.Second

// MonitorResult is one per-pod outcome from MonitorAll. Name is
// the sentinel pod name (matches Endpoint.Name from the input).
// Err is nil on success; non-nil errors are dial-time, AUTH-time,
// or MONITOR-reply failures.
type MonitorResult struct {
	Name string
	Err  error
}

// ProbeResult captures what a single sentinel pod reports as its
// current master via SENTINEL get-master-addr-by-name <name>.
// Addr is "host:port" (or empty when the sentinel reports a nil
// reply, which Redis Sentinel returns when the master is not
// monitored — typically post-RESET before MONITOR completes).
// Err is non-nil on any wire-side failure.
type ProbeResult struct {
	Name string
	Addr string
	Err  error
	// Epoch is the `config-epoch` pulled from a best-effort SENTINEL
	// MASTER <name> read issued on the same connection immediately after
	// GET-MASTER-ADDR-BY-NAME. It supplies the per-sentinel config-epoch
	// total order the live-but-different re-point sub-class is gated on
	// (the aggregated snapshot max-clamps epoch, so it cannot supply
	// per-sentinel order). EpochOK is false when the SENTINEL MASTER read
	// or parse failed; consumers treat a non-OK epoch as "unknown".
	Epoch   int64
	EpochOK bool
	// Flags is the comma-joined `flags` field of the same SENTINEL MASTER
	// reply (its tokens include s_down / o_down / failover_in_progress) —
	// the pull-side election signal the re-point guards read. Empty when
	// the read or parse failed. Populated best-effort: a SENTINEL MASTER
	// failure never sets Err (Err reflects ONLY the GET-MASTER-ADDR path,
	// so a flaky second read never masquerades as an unreachable sentinel).
	Flags string
}

// MonitorAll issues `SENTINEL MONITOR <masterName> <ip> <port>
// <quorum>` against every endpoint in parallel with a per-pod
// timeout. Returns one MonitorResult per endpoint in the same
// order. Never short-circuits — every pod gets the command so
// the caller can emit one event per pod.
//
// Sentinel rejects MONITOR for an already-monitored master with
// "ERR Duplicated master name"; the caller should issue RESET
// first (which clears state) or REMOVE (which only clears the
// pointer, leaves epoch). This function does NOT chain RESET
// internally — the Manager does, so it can decide based on
// probe results whether RESET is warranted.
func MonitorAll(ctx context.Context, endpoints []Endpoint, masterName, masterIP, password string, port int, quorum int) []MonitorResult {
	results := make([]MonitorResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			results[i] = MonitorResult{
				Name: ep.Name,
				Err:  monitorOne(ctx, ep, masterName, masterIP, password, port, quorum),
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// monitorOne dials, authenticates, and issues `SENTINEL MONITOR
// <masterName> <masterIP> <port> <quorum>` against one sentinel.
// Returns nil on success; non-nil error on any step.
//
// Sentinel's MONITOR reply on success is +OK; on conflict
// ("Duplicated master name") it's a -ERR line which readReply
// surfaces as a non-nil error.
func monitorOne(ctx context.Context, ep Endpoint, masterName, masterIP, password string, port int, quorum int) error {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			log.FromContext(ctx).V(1).Info("close conn failed", "err", cerr)
		}
	}()

	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	rd := bufio.NewReaderSize(conn, readBufSize)

	if err := authIfNeeded(conn, rd, password); err != nil {
		return err
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand(
		"SENTINEL", "MONITOR", masterName, masterIP, strconv.Itoa(port), strconv.Itoa(quorum),
	)); err != nil {
		return fmt.Errorf("write SENTINEL MONITOR: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("read SENTINEL MONITOR: %w", err)
	}
	if s, ok := reply.(string); !ok || s != "OK" {
		return fmt.Errorf("SENTINEL MONITOR: unexpected reply %v", reply)
	}
	return nil
}

// ProbeAll fans out SENTINEL get-master-addr-by-name <name> to
// every endpoint in parallel, piggybacking a best-effort SENTINEL
// MASTER read on the same connection for the config-epoch + flags.
// Returns one ProbeResult per endpoint in input order. Its sole
// production caller is the armed re-point classification pass
// (selectRepointTargets), which runs only when the caller supplies
// a live-pod set — so the two-command probe fan-out is armed-pass-
// only, never a startup or steady-state cost (the startup safety-net
// classifies via SentinelsAll, not this probe).
//
// Probe round-trip is bounded by MonitorTimeout (sharing the budget
// keeps the pass predictable).
func ProbeAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []ProbeResult {
	results := make([]ProbeResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			addr, epoch, epochOK, flags, err := probeOne(ctx, ep, masterName, password)
			results[i] = ProbeResult{
				Name:    ep.Name,
				Addr:    addr,
				Err:     err,
				Epoch:   epoch,
				EpochOK: epochOK,
				Flags:   flags,
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// probeOne queries SENTINEL get-master-addr-by-name <name> on one
// sentinel, then best-effort SENTINEL MASTER <name> on the SAME
// connection for the config-epoch + flags. Returns
// ("host:port", epoch, epochOK, flags, nil) on success;
// ("", 0, false, "", nil) when the sentinel reports a nil reply
// (master not monitored — e.g. post-RESET before MONITOR);
// ("", 0, false, "", err) on GET-MASTER-ADDR wire failure.
//
// Err isolation is load-bearing: the SENTINEL MASTER round-trip is
// best-effort — any write / read / parse failure there leaves
// epoch=0, epochOK=false, flags="" and does NOT set err. A flaky
// second read must never masquerade as an unreachable sentinel and
// vanish from the config-epoch frontier the re-point guards compute.
func probeOne(ctx context.Context, ep Endpoint, masterName, password string) (string, int64, bool, string, error) {
	callCtx, cancel := context.WithTimeout(ctx, MonitorTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return "", 0, false, "", fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			log.FromContext(ctx).V(1).Info("close conn failed", "err", cerr)
		}
	}()
	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	rd := bufio.NewReaderSize(conn, readBufSize)
	if err := authIfNeeded(conn, rd, password); err != nil {
		return "", 0, false, "", err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand(
		"SENTINEL", "get-master-addr-by-name", masterName,
	)); err != nil {
		return "", 0, false, "", fmt.Errorf("write SENTINEL get-master-addr-by-name: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return "", 0, false, "", fmt.Errorf("read SENTINEL get-master-addr-by-name: %w", err)
	}
	addr, ok := ParseGetMasterAddr(reply)
	if !ok {
		// Nil reply or malformed — sentinel reports "no monitored
		// master with this name". Not an error per se; the caller
		// uses an empty Addr as a positive signal that this sentinel
		// has nothing to RESET to. No epoch/flags to pull.
		return "", 0, false, "", nil
	}
	epoch, epochOK, flags := probeMasterEpochFlags(conn, rd, masterName)
	return addr, epoch, epochOK, flags, nil
}

// probeMasterEpochFlags issues SENTINEL MASTER <name> on an
// already-dialled+authed connection and extracts (config-epoch, flags).
// Best-effort: any write / read / parse failure returns (0, false, "")
// so the caller can keep the addr it already resolved while treating
// the epoch as unknown (see probeOne's Err-isolation contract).
func probeMasterEpochFlags(conn net.Conn, rd *bufio.Reader, masterName string) (int64, bool, string) {
	if _, err := io.WriteString(conn, resp.EncodeCommand(
		"SENTINEL", "master", masterName,
	)); err != nil {
		return 0, false, ""
	}
	reply, err := readReply(rd)
	if err != nil {
		return 0, false, ""
	}
	epoch, epochOK := ParseSentinelMasterEpoch(reply)
	flags, _ := masterReplyFlags(reply)
	return epoch, epochOK, flags
}
