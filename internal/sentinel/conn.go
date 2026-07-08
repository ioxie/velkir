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
	"strings"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// readBufSize is the bufio.Reader size for sentinel connections.
// Sentinel pubsub messages are small (~80 bytes for +switch-master,
// up to ~150 for +odown with the master name + addr); the default
// 4 KiB is fine but a slightly larger buffer absorbs back-to-back
// bursts during failovers without forcing a read syscall per
// message.
const readBufSize = 8192

// dialer is overridable in tests so the observer can be wired to
// a net.Listen fake without depending on real network. Production
// uses a default 5s DialTimeout.
type dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// defaultDialer is the production net.Dialer used by the observer.
// Per-call deadlines (read/write) are set on the returned Conn by
// the caller; this struct only owns the dial-time budget.
var defaultDialer dialer = &net.Dialer{Timeout: 5 * time.Second}

// dialSentinel opens a TCP connection to a sentinel pod's client
// port. Caller is responsible for setting per-op deadlines and
// closing the connection.
func dialSentinel(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := defaultDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return conn, nil
}

// pubsubMessage is one decoded RESP-2 array reply from a
// PSUBSCRIBE channel. Sentinel publishes:
//
//	*4\r\n$8\r\npmessage\r\n$<len>\r\n<pattern>\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n
//
// We discard the pattern (we know what we subscribed to) and
// surface (Channel, Payload) for the parser. Keep-alive
// "subscribe" / "psubscribe" reply arrays produce a zero-value
// pubsubMessage with Channel == "" — the caller treats that as
// "ignore, keep reading".
type pubsubMessage struct {
	Channel string
	Payload string
}

// readReply reads one RESP-2 top-level reply from the buffered
// reader. The minimal subset we need:
//
//   - simple string  +OK\r\n
//   - error          -ERR ...\r\n
//   - integer        :<n>\r\n
//   - bulk string    $<n>\r\n<n bytes>\r\n  ($-1 for nil)
//   - array          *<n>\r\n<n elements>
//
// Returns the reply as an `any`: string for +/$/-, int64 for :,
// []any for *. RESP-3 typed maps / sets are not used by sentinel.
func readReply(rd *bufio.Reader) (any, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimRight(header, "\r\n")
	if len(header) == 0 {
		return nil, fmt.Errorf("empty reply header")
	}
	prefix, body := header[0], header[1:]
	switch prefix {
	case '+':
		return body, nil
	case '-':
		return nil, fmt.Errorf("server: %s", body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad integer %q: %w", body, err)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, fmt.Errorf("bad bulk length %q: %w", body, err)
		}
		if n < 0 {
			return "", nil
		}
		if n > resp.MaxBulkSize {
			return nil, fmt.Errorf("bulk string length %d exceeds cap %d", n, resp.MaxBulkSize)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, fmt.Errorf("read bulk body: %w", err)
		}
		if _, err := rd.Discard(2); err != nil {
			return nil, fmt.Errorf("read bulk trailer: %w", err)
		}
		return string(buf), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, fmt.Errorf("bad array length %q: %w", body, err)
		}
		if n < 0 {
			return []any(nil), nil
		}
		out := make([]any, n)
		for i := range n {
			el, err := readReply(rd)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", i, err)
			}
			out[i] = el
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected reply prefix %q (line=%q)", prefix, header)
	}
}

// readPubsubMessage waits for the next pmessage / message reply on
// the subscription socket and returns it. The PSUBSCRIBE-ack
// arrays sentinel sends right after the SUBSCRIBE / PSUBSCRIBE
// command (`["subscribe", "<channel>", <count>]`,
// `["psubscribe", "<pattern>", <count>]`) are absorbed silently —
// the caller doesn't see them.
//
// On a non-array reply (a stray simple-string PING reply, an
// error, etc.), returns an empty pubsubMessage with no error so
// the caller can keep reading without treating it as a fatal.
func readPubsubMessage(rd *bufio.Reader) (pubsubMessage, error) {
	reply, err := readReply(rd)
	if err != nil {
		return pubsubMessage{}, err
	}
	arr, ok := reply.([]any)
	if !ok {
		// Non-array (e.g., +PONG to an in-band PING) — caller
		// loops and re-reads.
		return pubsubMessage{}, nil
	}
	if len(arr) == 0 {
		return pubsubMessage{}, nil
	}
	kind, _ := arr[0].(string)
	switch kind {
	case "pmessage":
		// ["pmessage", <pattern>, <channel>, <payload>]
		if len(arr) < 4 {
			return pubsubMessage{}, fmt.Errorf("short pmessage reply (len=%d)", len(arr))
		}
		channel, _ := arr[2].(string)
		payload, _ := arr[3].(string)
		return pubsubMessage{Channel: channel, Payload: payload}, nil
	case "message":
		// ["message", <channel>, <payload>]
		if len(arr) < 3 {
			return pubsubMessage{}, fmt.Errorf("short message reply (len=%d)", len(arr))
		}
		channel, _ := arr[1].(string)
		payload, _ := arr[2].(string)
		return pubsubMessage{Channel: channel, Payload: payload}, nil
	case "subscribe", "psubscribe", "unsubscribe", "punsubscribe":
		// Subscription-state acks; benign keep-alive shape.
		return pubsubMessage{}, nil
	default:
		// Unknown reply shape; treat as benign keep-alive so a
		// future Sentinel version that emits new metadata frames
		// doesn't kill the subscription.
		return pubsubMessage{}, nil
	}
}

// authIfNeeded sends AUTH on `conn` when password is non-empty and
// reads the reply. Used by both pubsub and command paths.
func authIfNeeded(conn net.Conn, rd *bufio.Reader, password string) error {
	if password == "" {
		return nil
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("AUTH", password)); err != nil {
		return fmt.Errorf("write AUTH: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}
	if s, ok := reply.(string); ok && s == "OK" {
		return nil
	}
	return fmt.Errorf("AUTH: unexpected reply %v", reply)
}

// pingPubsub sends an in-band PING on the subscription socket and
// reads one reply, used by the observer's read-deadline branch to
// distinguish "idle channel" from "dead connection". A reply read
// without error proves the link is alive.
//
// A real pub/sub message (+switch-master / +odown) can land in the
// buffer between the read-deadline expiry and this read. Rather than
// rejecting it as an unexpected reply and tearing the connection
// down — which would silently drop a failover-observation event —
// the parsed message is returned to the caller for dispatch; its
// arrival is itself proof the link is alive, which is what the PING
// tests for. The returned pubsubMessage has Channel set only when a
// real message was read; a PONG (flat +PONG or the ["pong", ""]
// array some builds emit) collapses to a zero-value message.
func pingPubsub(conn net.Conn, rd *bufio.Reader, deadline time.Time) (pubsubMessage, error) {
	if err := conn.SetDeadline(deadline); err != nil {
		return pubsubMessage{}, fmt.Errorf("set ping deadline: %w", err)
	}
	if _, err := io.WriteString(conn, resp.EncodeCommand("PING")); err != nil {
		return pubsubMessage{}, fmt.Errorf("write PING: %w", err)
	}
	msg, err := readPubsubMessage(rd)
	if err != nil {
		return pubsubMessage{}, fmt.Errorf("read PING reply: %w", err)
	}
	return msg, nil
}
