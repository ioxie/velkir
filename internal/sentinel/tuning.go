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

// TuningTimeout caps a single per-pod tuning round-trip (dial +
// AUTH + three SENTINEL SET commands). 5s mirrors the other
// sentinel-side helpers; SET is local synchronous state work.
const TuningTimeout = 5 * time.Second

// MasterTuning carries the per-master timing knobs that the
// stranded-recovery path needs to re-propagate after REMOVE +
// MONITOR. Plain MONITOR re-registers the master with hardcoded
// Sentinel defaults (down-after-milliseconds=30000,
// failover-timeout=180000, parallel-syncs=1) — losing whatever
// the operator's CR set via sentinel.conf at boot. Zero values
// for any field skip that SET so a caller passing a partially-
// populated struct can re-propagate a subset.
type MasterTuning struct {
	DownAfterMilliseconds int32
	FailoverTimeout       int32
	ParallelSyncs         int32
}

// TuningResult is one per-pod outcome from SetMasterTuningAll.
// Name matches Endpoint.Name; Err is nil on success.
type TuningResult struct {
	Name string
	Err  error
}

// SetMasterTuningAll issues `SENTINEL SET <masterName> <field>
// <value>` for each non-zero field in t, against every endpoint
// in parallel. Returns one TuningResult per endpoint in input
// order. Used after the stranded-recovery REMOVE + MONITOR pass
// to restore the operator's configured per-master tuning that
// MONITOR's default-population erased.
//
// All-zero tuning is a no-op (returns nil).
func SetMasterTuningAll(ctx context.Context, endpoints []Endpoint, masterName string, t MasterTuning, password string) []TuningResult {
	if t.DownAfterMilliseconds == 0 && t.FailoverTimeout == 0 && t.ParallelSyncs == 0 {
		return nil
	}
	if len(endpoints) == 0 {
		return nil
	}
	results := make([]TuningResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			results[i] = TuningResult{
				Name: ep.Name,
				Err:  setTuningOne(ctx, ep, masterName, t, password),
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// setTuningOne dials, authenticates, and issues the SENTINEL SET
// commands for each populated tuning field in order: down-after,
// failover-timeout, parallel-syncs. Returns the first error
// encountered; subsequent fields are skipped on error (the
// caller's next reconcile retries).
func setTuningOne(ctx context.Context, ep Endpoint, masterName string, t MasterTuning, password string) error {
	callCtx, cancel := context.WithTimeout(ctx, TuningTimeout)
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

	for _, kv := range tuningKVs(t) {
		if err := writeAndExpectOK(conn, rd, "SENTINEL", "SET", masterName, kv.field, kv.value); err != nil {
			return fmt.Errorf("SET %s: %w", kv.field, err)
		}
	}
	return nil
}

type tuningKV struct {
	field string
	value string
}

func tuningKVs(t MasterTuning) []tuningKV {
	out := make([]tuningKV, 0, 3)
	if t.DownAfterMilliseconds > 0 {
		out = append(out, tuningKV{"down-after-milliseconds", strconv.FormatInt(int64(t.DownAfterMilliseconds), 10)})
	}
	if t.FailoverTimeout > 0 {
		out = append(out, tuningKV{"failover-timeout", strconv.FormatInt(int64(t.FailoverTimeout), 10)})
	}
	if t.ParallelSyncs > 0 {
		out = append(out, tuningKV{"parallel-syncs", strconv.FormatInt(int64(t.ParallelSyncs), 10)})
	}
	return out
}

// writeAndExpectOK writes a RESP-2 command and asserts the reply
// is "+OK" (simple string). Returns wire / reply errors verbatim;
// unexpected non-"+OK" replies surface as a controlled error.
func writeAndExpectOK(conn net.Conn, rd *bufio.Reader, args ...string) error {
	if _, err := io.WriteString(conn, resp.EncodeCommand(args...)); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if s, ok := reply.(string); !ok || s != "OK" {
		return fmt.Errorf("unexpected reply %v", reply)
	}
	return nil
}
