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
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// queueFailoverOK scripts the standard +OK reply that real sentinel
// returns when SENTINEL FAILOVER is accepted.
func queueFailoverOK(fs *fakeSentinel) {
	fs.QueueReply("SENTINEL FAILOVER", "+OK\r\n")
}

func TestFailoverOne_HappyPath(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueFailoverOK(fs)

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "")
	if err != nil {
		t.Fatalf("FailoverOne: %v", err)
	}
	sent := fs.Sent()
	found := false
	for _, s := range sent {
		if strings.HasPrefix(s, "SENTINEL FAILOVER vk0") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected SENTINEL FAILOVER vk0 in sent; got %v", sent)
	}
}

func TestFailoverOne_INPROGSurfacedAsErrFailoverInProgress(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	// Sentinel returns -INPROG when a failover is already running for
	// the named master. The caller treats this as "failover IS
	// happening, just not initiated by us this round" and transitions
	// to FailoverInFlight cleanly.
	fs.QueueReply("SENTINEL FAILOVER", "-INPROG failover already in progress\r\n")

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "")
	if !errors.Is(err, ErrFailoverInProgress) {
		t.Fatalf("expected ErrFailoverInProgress, got %v", err)
	}
}

func TestFailoverOne_INPROGOnlyMatchesAsExactReplyCode(t *testing.T) {
	// A sentinel reply whose body contains the substring "INPROG" but
	// whose reply code is something else (here -ERR with a free-form
	// message) must NOT be treated as in-progress. The previous
	// substring-match detection would have false-positived; the
	// HasPrefix("server: INPROG") form pins the reply code as the
	// first token after the "server: " prefix readReply prepends.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL FAILOVER", "-ERR sentinel rejected: candidate state INPROG_LIKE\r\n")

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "")
	if err == nil {
		t.Fatal("expected -ERR, got nil")
	}
	if errors.Is(err, ErrFailoverInProgress) {
		t.Fatalf("INPROG substring in -ERR body must not be classified as in-progress; got %v", err)
	}
}

func TestFailoverOne_NOGOODSLAVESurfacedAsError(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	// -NOGOODSLAVE is the sentinel-side "I have no replica I can
	// promote" reply — distinct from our operator-side
	// offset-tolerance preflight, which catches the same condition
	// earlier. Surface it as a wrapped error so the caller can log
	// and stay in RolloutPrimary for the next reconcile.
	fs.QueueReply("SENTINEL FAILOVER", "-NOGOODSLAVE No suitable replica\r\n")

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "")
	if err == nil {
		t.Fatalf("expected NOGOODSLAVE error, got nil")
	}
	if errors.Is(err, ErrFailoverInProgress) {
		t.Fatalf("NOGOODSLAVE was misclassified as in-progress: %v", err)
	}
	if !strings.Contains(err.Error(), "NOGOODSLAVE") {
		t.Errorf("error should mention NOGOODSLAVE; got %v", err)
	}
}

func TestFailoverOne_AuthRequired(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = "secret"
	queueFailoverOK(fs)

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "secret")
	if err != nil {
		t.Fatalf("FailoverOne with AUTH: %v", err)
	}
}

func TestFailoverOne_AuthMismatchSurfaced(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = "secret"
	// No queued FAILOVER reply needed — AUTH fails first.

	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: fs.Addr()}, "vk0", "wrong")
	if err == nil {
		t.Fatal("expected AUTH error, got nil")
	}
}

func TestFailoverOne_DialFailureWrapped(t *testing.T) {
	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}, "vk0", "")
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("error should mention dial; got %v", err)
	}
}

func TestFailoverOne_RejectsEmptyMasterName(t *testing.T) {
	// Defensive: an empty masterName would be silently encoded as
	// "SENTINEL FAILOVER " and sentinel would reject with -WRONGFORMAT
	// or similar. Catch it client-side so the failure is clear.
	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}, "", "")
	if err == nil {
		t.Fatal("expected masterName validation error")
	}
	if !strings.Contains(err.Error(), "masterName") {
		t.Errorf("error should mention masterName; got %v", err)
	}
}

func TestFailoverOne_TimeoutBounded(t *testing.T) {
	// Same RFC-5737 unroutable address pattern as the RESET test.
	start := time.Now()
	err := FailoverOne(context.Background(), Endpoint{Name: "vk0-sentinel-0", Addr: "198.51.100.1:26379"}, "vk0", "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 2*FailoverTimeout+time.Second {
		t.Errorf("FailoverOne took %s — expected ≤ %s", elapsed, 2*FailoverTimeout+time.Second)
	}
}
