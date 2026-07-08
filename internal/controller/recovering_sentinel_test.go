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

package controller

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/ioxie/velkir/internal/valkey"
)

// recoveringSentinel is a stripped-down RESP server that lets the
// controller-package envtest exercise the sentinel observer's
// quorum-recovery path without standing up real valkey-sentinel
// processes. It speaks just enough of the protocol the observer
// drives — AUTH (any password), PING, PSUBSCRIBE (for the seven
// subscribed channels), SENTINEL GET-MASTER-ADDR-BY-NAME, SENTINEL
// MASTER, SENTINEL CKQUORUM — and answers with a fixed master
// address + epoch, plus a controllable CKQUORUM verdict. No reply
// queues; replies repeat across pull ticks so the observer keeps
// publishing the desired snapshot for the lifetime of the test.
//
// The sentinel package owns a richer fakeSentinel keyed on scripted
// reply queues (internal/sentinel/conn_test.go) used by its observer
// and manager tests. That fake is package-private to sentinel; this
// helper is its smaller controller-side cousin: same wire shape,
// minimum viable behaviour for envtest assertions.
type recoveringSentinel struct {
	listener net.Listener

	masterIP string
	epoch    int64

	mu        sync.Mutex
	quorumOK  bool
	conns     map[net.Conn]struct{}
	subs      map[net.Conn]struct{}
	connsWG   sync.WaitGroup
	closeOnce sync.Once
}

// newRecoveringSentinel binds a TCP listener on a random localhost
// port and starts the accept loop. The fake starts answering CKQUORUM
// affirmatively; SetQuorumOK flips the answer mid-test. The reported
// master always uses the canonical valkey client port
// (valkey.DefaultPort). The caller must invoke Stop (typically via
// DeferCleanup).
func newRecoveringSentinel(masterIP string, epoch int64) (*recoveringSentinel, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("recoveringSentinel listen: %w", err)
	}
	rs := &recoveringSentinel{
		listener: l,
		masterIP: masterIP,
		epoch:    epoch,
		quorumOK: true,
		conns:    make(map[net.Conn]struct{}),
		subs:     make(map[net.Conn]struct{}),
	}
	go rs.acceptLoop()
	return rs, nil
}

func (rs *recoveringSentinel) Addr() string { return rs.listener.Addr().String() }

func (rs *recoveringSentinel) SetQuorumOK(ok bool) {
	rs.mu.Lock()
	rs.quorumOK = ok
	rs.mu.Unlock()
}

// PushEvent writes one pmessage frame (pattern == channel) with the
// given payload on every live PSUBSCRIBE connection — the fake-side
// equivalent of sentinel publishing a notification. Lets tests drive
// the observer's pubsub dispatch arms (which republish the snapshot
// with a fresh UpdatedAt while carrying LastPolledAt forward
// unchanged) independently of the poll tick.
func (rs *recoveringSentinel) PushEvent(channel, payload string) {
	frame := fmt.Sprintf("*4\r\n$8\r\npmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(channel), channel, len(channel), channel, len(payload), payload)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for c := range rs.subs {
		_, _ = c.Write([]byte(frame))
	}
}

func (rs *recoveringSentinel) Stop() {
	rs.closeOnce.Do(func() {
		_ = rs.listener.Close()
		// Closing the listener stops new accepts but does NOT
		// terminate already-accepted connections (the observer
		// holds its PSUBSCRIBE conn open indefinitely). Close every
		// live conn so serveConn's blocking Read returns and the
		// goroutine exits — otherwise connsWG.Wait deadlocks.
		rs.mu.Lock()
		for c := range rs.conns {
			_ = c.Close()
		}
		rs.mu.Unlock()
		rs.connsWG.Wait()
	})
}

func (rs *recoveringSentinel) acceptLoop() {
	for {
		c, err := rs.listener.Accept()
		if err != nil {
			return
		}
		rs.mu.Lock()
		rs.conns[c] = struct{}{}
		rs.mu.Unlock()
		rs.connsWG.Add(1)
		go rs.serveConn(c)
	}
}

func (rs *recoveringSentinel) serveConn(c net.Conn) {
	defer rs.connsWG.Done()
	defer func() {
		_ = c.Close()
		rs.mu.Lock()
		delete(rs.conns, c)
		delete(rs.subs, c)
		rs.mu.Unlock()
	}()
	rd := bufio.NewReader(c)
	for {
		req, err := readRESPArrayCommand(rd)
		if err != nil {
			return
		}
		if len(req) == 0 {
			return
		}
		verb := strings.ToUpper(req[0])
		switch verb {
		case "AUTH":
			_, _ = c.Write([]byte("+OK\r\n"))
			continue
		case "PING":
			_, _ = c.Write([]byte("+PONG\r\n"))
			continue
		case "PSUBSCRIBE":
			// The observer subscribes to seven channels in one
			// PSUBSCRIBE invocation; sentinel replies with seven
			// `["psubscribe", <pattern>, <count>]` array frames.
			patterns := []string{
				"+switch-master",
				"+failover-end",
				"+failover-end-for-timeout",
				"+odown",
				"-odown",
				"+tilt",
				"-tilt",
			}
			var sb strings.Builder
			for i, p := range patterns {
				sb.WriteString(buildRESPPsubscribeAck(p, i+1))
			}
			if _, err := c.Write([]byte(sb.String())); err != nil {
				return
			}
			rs.mu.Lock()
			rs.subs[c] = struct{}{}
			rs.mu.Unlock()
			continue
		case "SENTINEL":
			if len(req) < 2 {
				_, _ = c.Write([]byte("-ERR malformed SENTINEL\r\n"))
				continue
			}
			sub := strings.ToUpper(req[1])
			switch sub {
			case "GET-MASTER-ADDR-BY-NAME":
				_, _ = c.Write([]byte(buildRESPArray(rs.masterIP, strconv.Itoa(valkey.DefaultPort))))
			case "MASTER":
				_, _ = c.Write([]byte(buildRESPArray(
					"name", "mymaster",
					"ip", rs.masterIP,
					"port", strconv.Itoa(valkey.DefaultPort),
					"config-epoch", strconv.FormatInt(rs.epoch, 10),
				)))
			case "CKQUORUM":
				rs.mu.Lock()
				ok := rs.quorumOK
				rs.mu.Unlock()
				if ok {
					_, _ = c.Write([]byte("+OK 3 usable Sentinels\r\n"))
				} else {
					_, _ = c.Write([]byte("-NOQUORUM Quorum not reached\r\n"))
				}
			default:
				_, _ = c.Write([]byte("-ERR unsupported SENTINEL subcommand\r\n"))
			}
			continue
		default:
			_, _ = c.Write([]byte("-ERR unsupported\r\n"))
		}
	}
}

// readRESPArrayCommand parses one RESP-2 array-of-bulk-strings
// command (the form sentinel clients always emit). Returns the
// command tokens.
func readRESPArrayCommand(rd *bufio.Reader) ([]string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	header = strings.TrimRight(header, "\r\n")
	if len(header) == 0 || header[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", header)
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil {
		return nil, fmt.Errorf("bad array length %q: %w", header, err)
	}
	out := make([]string, 0, n)
	for range n {
		bulkHeader, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimRight(bulkHeader, "\r\n")
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("expected bulk header, got %q", bulkHeader)
		}
		l, err := strconv.Atoi(bulkHeader[1:])
		if err != nil {
			return nil, fmt.Errorf("bad bulk length: %w", err)
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		if _, err := rd.Discard(2); err != nil {
			return nil, err
		}
		out = append(out, string(buf))
	}
	return out, nil
}

// buildRESPArray renders a RESP-2 array of bulk strings.
func buildRESPArray(parts ...string) string {
	var sb strings.Builder
	sb.WriteByte('*')
	sb.WriteString(strconv.Itoa(len(parts)))
	sb.WriteString("\r\n")
	for _, p := range parts {
		sb.WriteByte('$')
		sb.WriteString(strconv.Itoa(len(p)))
		sb.WriteString("\r\n")
		sb.WriteString(p)
		sb.WriteString("\r\n")
	}
	return sb.String()
}

// buildRESPPsubscribeAck renders one `["psubscribe", <pattern>, <count>]`
// reply array — the per-pattern frame sentinel emits in response to
// PSUBSCRIBE.
func buildRESPPsubscribeAck(pattern string, count int) string {
	return fmt.Sprintf(
		"*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
		len(pattern), pattern, count,
	)
}
