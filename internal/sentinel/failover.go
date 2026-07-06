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
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ioxie/velkir/internal/resp"
)

// FailoverTimeout caps a single SENTINEL FAILOVER round-trip per pod.
// 10s — sentinel parses the command synchronously, validates quorum,
// and either accepts (returns +OK) or rejects (-IDONTKNOW, -INPROG,
// -NOGOODSLAVE, etc.) within milliseconds. Anything slower implies a
// wedged sentinel; the next reconcile picks a different one.
const FailoverTimeout = 10 * time.Second

// ErrFailoverInProgress is returned by FailoverOne when the chosen
// sentinel reports an in-flight failover via -INPROG. Caller should
// treat this as a non-error and transition to FailoverInFlight (the
// failover IS happening, we just didn't initiate it on this round).
var ErrFailoverInProgress = errors.New("sentinel failover already in progress")

// FailoverOne dials the supplied sentinel, authenticates, and issues
// `SENTINEL FAILOVER <masterName>`. Returns nil on +OK,
// ErrFailoverInProgress on -INPROG, or a wrapped error on any other
// failure (dial, AUTH, -NOGOODSLAVE, unexpected reply).
//
// The FAILOVER command is fire-and-forget from the operator's
// perspective: success here means the sentinel accepted the request
// and will drive the failover; the operator's observer goroutine
// catches the resulting `+switch-master` / `+failover-end` pubsub
// messages independently.
func FailoverOne(ctx context.Context, ep Endpoint, masterName, password string) error {
	if masterName == "" {
		return fmt.Errorf("masterName required")
	}
	callCtx, cancel := context.WithTimeout(ctx, FailoverTimeout)
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

	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "FAILOVER", masterName)); err != nil {
		return fmt.Errorf("write SENTINEL FAILOVER: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		// Distinguish -INPROG (benign — a failover is already
		// running) from other server errors. readReply wraps the
		// reply body as `server: <body>`; the body's first token is
		// the sentinel reply code (INPROG, NOGOODSLAVE, ERR, etc.).
		// HasPrefix on the full `server: INPROG` string avoids the
		// false-positive case where a hostile or misconfigured
		// sentinel emits a different code containing the substring
		// "INPROG" anywhere in the body.
		if strings.HasPrefix(err.Error(), "server: INPROG") {
			return ErrFailoverInProgress
		}
		return fmt.Errorf("SENTINEL FAILOVER: %w", err)
	}
	// Expected reply is +OK (simple-string). Other shapes are
	// unexpected and we surface them so a future sentinel-protocol
	// change doesn't slip through silently.
	if s, ok := reply.(string); ok && s == "OK" {
		return nil
	}
	return fmt.Errorf("SENTINEL FAILOVER: unexpected reply %v", reply)
}
