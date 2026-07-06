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
	"strconv"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ioxie/velkir/internal/resp"
)

// PeerInfo is one entry parsed from `SENTINEL SENTINELS <master-name>`.
// The wedge-recovery path uses the peer count to decide whether a
// sentinel is "stranded" (empty peer-list = boot-from-fresh pod) vs.
// healthy (non-empty peer-list = participating in gossip).
//
// Historical note: an earlier rev of the operator passed (RunID, IP, Port)
// to `SENTINEL SET <master> known-sentinel <ip> <port> <runid>` to
// re-register peers after a startup RESET. That command is NOT a valid
// runtime option of SENTINEL SET — Valkey's sentinelSetCommand whitelist
// rejects it with "Unknown option" (`known-sentinel` is a config-file
// directive only, parsed at sentinel boot from sentinel.conf). The
// operator now drops the RESET on healthy survivors entirely, and the
// rebuilt sentinel's peer-list is rebuilt via Pub/Sub gossip on the
// `__sentinel__:hello` channel once it is MONITORing the same master
// as its peers.
type PeerInfo struct {
	Name  string
	RunID string
	IP    string
	Port  int
}

// SentinelsResult is one per-pod outcome from SentinelsAll. Name is
// the sentinel pod whose peer-list was queried; Peers is the parsed
// peer list with self excluded by Sentinel (the SENTINEL SENTINELS
// reply intentionally omits the responder). Err is non-nil on any
// wire failure or unparseable reply.
type SentinelsResult struct {
	Name  string
	Peers []PeerInfo
	Err   error
}

// SentinelsTimeout caps a single SENTINEL SENTINELS round-trip per
// pod. 5s mirrors the other sentinel-side helpers; sentinel composes
// the reply from in-memory state so latency is bounded.
const SentinelsTimeout = 5 * time.Second

// SentinelsAll fans out `SENTINEL SENTINELS <master-name>` to every
// endpoint in parallel. Returns one SentinelsResult per endpoint in
// input order. Never short-circuits — the wedge-recovery path needs
// the per-sentinel peer count to classify "stranded" vs "healthy".
func SentinelsAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []SentinelsResult {
	results := make([]SentinelsResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			peers, err := sentinelsOne(ctx, ep, masterName, password)
			results[i] = SentinelsResult{Name: ep.Name, Peers: peers, Err: err}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// sentinelsOne queries `SENTINEL SENTINELS <master-name>` on one
// sentinel and parses the multi-bulk reply. Returns (nil, nil) when
// the sentinel reports an empty peer-list (its own peer-list was
// already wiped, e.g. by a prior RESET); ([…], nil) on a successful
// non-empty reply; (nil, err) on wire failure.
func sentinelsOne(ctx context.Context, ep Endpoint, masterName, password string) ([]PeerInfo, error) {
	callCtx, cancel := context.WithTimeout(ctx, SentinelsTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
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
		return nil, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "SENTINELS", masterName)); err != nil {
		return nil, fmt.Errorf("write SENTINEL SENTINELS: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return nil, fmt.Errorf("read SENTINEL SENTINELS: %w", err)
	}
	return ParseSentinelsReply(reply), nil
}

// ParseSentinelsReply decodes the SENTINEL SENTINELS <name> reply
// shape: an outer array whose elements are inner flat arrays of
// alternating key/value bulk-strings ("name", <name>, "ip", <ip>,
// "port", <port>, "runid", <runid>, …). Each inner array describes
// one peer sentinel. Anything malformed is silently dropped — the
// caller treats a missing peer as "we don't know it yet" rather
// than a hard error.
func ParseSentinelsReply(reply any) []PeerInfo {
	outer, ok := reply.([]any)
	if !ok {
		return nil
	}
	out := make([]PeerInfo, 0, len(outer))
	for _, item := range outer {
		inner, ok := item.([]any)
		if !ok {
			continue
		}
		p := PeerInfo{}
		for i := 0; i+1 < len(inner); i += 2 {
			k, _ := inner[i].(string)
			v, _ := inner[i+1].(string)
			switch k {
			case "name":
				p.Name = v
			case "runid":
				p.RunID = v
			case "ip":
				p.IP = v
			case "port":
				if n, err := strconv.Atoi(v); err == nil {
					p.Port = n
				}
			}
		}
		// Reject incomplete entries — a peer with missing fields
		// can't be counted reliably toward the stranded-detection
		// heuristic.
		if p.RunID == "" || p.IP == "" || p.Port <= 0 {
			continue
		}
		out = append(out, p)
	}
	return out
}

// ReplicaInfo is one entry parsed from `SENTINEL REPLICAS
// <master-name>` — a replica the queried sentinel knows for that
// master. The dead-master recovery paths use the IP against the
// live-pod set to decide whether the sentinel's OWN failover election
// can still succeed: a sentinel that knows at least one live replica
// has a viable promotion candidate and must be left to elect; a
// sentinel whose entire known-replica set is dead can never complete
// an election, so operator intervention races nothing.
type ReplicaInfo struct {
	IP    string
	Port  int
	Flags string
}

// ReplicasResult is one per-pod outcome from ReplicasAll. Name is the
// sentinel pod whose replica table was queried; Replicas is the parsed
// table. Err is non-nil on any wire failure or unparseable reply —
// callers must treat an errored table as UNKNOWN (never as "doomed").
type ReplicasResult struct {
	Name     string
	Replicas []ReplicaInfo
	Err      error
}

// ReplicasAll fans out `SENTINEL REPLICAS <master-name>` to every
// endpoint in parallel. Returns one ReplicasResult per endpoint in
// input order; never short-circuits. Round-trips share
// SentinelsTimeout (in-memory state read on the sentinel).
func ReplicasAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []ReplicasResult {
	results := make([]ReplicasResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			replicas, err := replicasOne(ctx, ep, masterName, password)
			results[i] = ReplicasResult{Name: ep.Name, Replicas: replicas, Err: err}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// replicasOne queries `SENTINEL REPLICAS <master-name>` on one
// sentinel and parses the multi-bulk reply. (nil, nil) means the
// sentinel knows no replicas — a valid answer, not an error.
func replicasOne(ctx context.Context, ep Endpoint, masterName, password string) ([]ReplicaInfo, error) {
	callCtx, cancel := context.WithTimeout(ctx, SentinelsTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
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
		return nil, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "REPLICAS", masterName)); err != nil {
		return nil, fmt.Errorf("write SENTINEL REPLICAS: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return nil, fmt.Errorf("read SENTINEL REPLICAS: %w", err)
	}
	return ParseReplicasReply(reply), nil
}

// ParseReplicasReply decodes the SENTINEL REPLICAS <name> reply —
// the same outer-array-of-flat-key/value-arrays shape as SENTINEL
// SENTINELS. Entries without an ip are dropped (an addr-less table
// row carries no liveness evidence either way).
func ParseReplicasReply(reply any) []ReplicaInfo {
	outer, ok := reply.([]any)
	if !ok {
		return nil
	}
	out := make([]ReplicaInfo, 0, len(outer))
	for _, item := range outer {
		inner, ok := item.([]any)
		if !ok {
			continue
		}
		r := ReplicaInfo{}
		for i := 0; i+1 < len(inner); i += 2 {
			k, _ := inner[i].(string)
			v, _ := inner[i+1].(string)
			switch k {
			case "ip":
				r.IP = v
			case "port":
				if n, err := strconv.Atoi(v); err == nil {
					r.Port = n
				}
			case flagsField:
				r.Flags = v
			}
		}
		if r.IP == "" {
			continue
		}
		out = append(out, r)
	}
	return out
}

// MasterPeerCountResult is one per-pod outcome from
// MasterPeerCountAll. Name is the sentinel pod queried; Count is
// the `num-other-sentinels` field from its SENTINEL MASTER reply
// (0 when the sentinel sees no peers — the canonical post-RESET
// stranded signature). Err is non-nil on any wire failure or
// unparseable reply; Count is meaningless when Err != nil.
type MasterPeerCountResult struct {
	Name  string
	Count int
	Err   error
}

// MasterPeerCountTimeout caps a single SENTINEL MASTER round-trip
// per pod. 5s — local-state read on the sentinel, bounded.
const MasterPeerCountTimeout = 5 * time.Second

// MasterPeerCountAll fans out `SENTINEL MASTER <master-name>` to
// every endpoint in parallel and extracts the `num-other-sentinels`
// field. Returns one MasterPeerCountResult per endpoint in input
// order. Used by the wedge-recovery read-back path to confirm a
// freshly-RESET'd sentinel has rebuilt its peer-list via gossip
// (peer count > 0 once gossip kicks in on the new master's
// __sentinel__:hello PubSub channel).
func MasterPeerCountAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []MasterPeerCountResult {
	results := make([]MasterPeerCountResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			count, err := masterPeerCountOne(ctx, ep, masterName, password)
			results[i] = MasterPeerCountResult{Name: ep.Name, Count: count, Err: err}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// masterPeerCountOne issues `SENTINEL MASTER <master-name>` against
// one sentinel and extracts the `num-other-sentinels` field.
// Returns (n, nil) on success; (0, err) on dial / AUTH / non-array
// reply / missing-field. The two failure cases (wire vs. parse) are
// merged because the caller's response is identical: treat as
// "couldn't read peer count, skip this pod for this verification
// round, retry next poll".
func masterPeerCountOne(ctx context.Context, ep Endpoint, masterName, password string) (int, error) {
	callCtx, cancel := context.WithTimeout(ctx, MasterPeerCountTimeout)
	defer cancel()
	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
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
		return 0, err
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "MASTER", masterName)); err != nil {
		return 0, fmt.Errorf("write SENTINEL MASTER: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return 0, fmt.Errorf("read SENTINEL MASTER: %w", err)
	}
	n, ok := ParseSentinelMasterNumOtherSentinels(reply)
	if !ok {
		return 0, fmt.Errorf("SENTINEL MASTER: num-other-sentinels field absent or malformed")
	}
	return n, nil
}
