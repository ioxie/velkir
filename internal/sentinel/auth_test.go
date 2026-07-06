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

// Test fixtures shared across this file (and the manager-side
// IssueAuthPass tests). Extracted to constants so unparam +
// goconst stop flagging the success-path helpers for "always
// receives the same value".
const (
	testMasterName = "mymaster"
	testPassword   = "s3cret"
	freshPassword  = "fresh"
)

// queueAuthPassReplies scripts the success path: SET → +OK,
// MASTER → multi-bulk reply with auth-pass echo. The fake's
// per-verb keying lets a single QueueReply per verb satisfy
// both round-trips on a single connection. Also primes
// fs.password so AUTH on the propagation connection succeeds —
// IssueAuthPass uses the same password for sentinel-client AUTH
// AND the value being SET as auth-pass. Hard-coded to
// (testMasterName, testPassword); tests with non-default
// passwords (verify-mismatch, retry-success) script the fake
// inline instead of using this helper.
func queueAuthPassReplies(fs *fakeSentinel) {
	fs.password = testPassword
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", buildMasterReply(testPassword))
}

// buildMasterReply renders a SENTINEL MASTER reply with
// `auth-pass` echoing the supplied value. Real sentinel returns
// many more keys; the parser only consumes auth-pass so a
// minimal reply is fine. masterName is fixed at testMasterName
// — tests use a single master across the whole file.
func buildMasterReply(authPass string) string {
	pairs := []string{"name", testMasterName, "auth-pass", authPass}
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(itoa(len(pairs)))
	b.WriteString("\r\n")
	for _, p := range pairs {
		b.WriteString("$")
		b.WriteString(itoa(len(p)))
		b.WriteString("\r\n")
		b.WriteString(p)
		b.WriteString("\r\n")
	}
	return b.String()
}

// (itoa lives in observer_test.go in this package; reuse it.)

func TestSetAuthPassAll_AllSucceed(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	queueAuthPassReplies(fs1)
	queueAuthPassReplies(fs2)

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}, testMasterName, testPassword)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("result[%d] (%s): unexpected error %v", i, r.Name, r.Err)
		}
	}
}

func TestSetAuthPassAll_EmptyPasswordIsNoOp(t *testing.T) {
	// Endpoint addr is unreachable on purpose — an empty password
	// must short-circuit before any dial attempt.
	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"},
	}, testMasterName, "")

	if results != nil {
		t.Errorf("expected nil results for empty password, got %v", results)
	}
}

func TestSetAuthPassAll_ZeroEndpoints(t *testing.T) {
	results := SetAuthPassAll(context.Background(), nil, testMasterName, testPassword)
	if results != nil {
		t.Errorf("expected nil results for empty endpoints, got %v", results)
	}
}

func TestSetAuthPassAll_ContinuesOnPartialFailure(t *testing.T) {
	fs1 := newFakeSentinel(t)
	defer fs1.Stop()
	queueAuthPassReplies(fs1)
	// fs2 unreachable — dial fails on every retry.

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: "127.0.0.1:1"},
	}, testMasterName, testPassword)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected sentinel-0 success, got %v", results[0].Err)
	}
	if results[1].Err == nil {
		t.Error("expected sentinel-1 dial failure after retries")
	}
}

func TestSetAuthPassAll_VerifyMismatchSurfacesAsErr(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = freshPassword
	// Queue enough replies that every retry attempt sees a
	// mismatching auth-pass — drives the retry loop to exhaustion.
	for range AuthRetryAttempts {
		fs.QueueReply("SENTINEL SET", "+OK\r\n")
		fs.QueueReply("SENTINEL MASTER", buildMasterReply("stale"))
	}

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, freshPassword)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("expected verify-mismatch error")
	}
	if !errors.Is(results[0].Err, errAuthPassMismatch) {
		t.Errorf("expected errAuthPassMismatch, got %v", results[0].Err)
	}
}

func TestSetAuthPassAll_SetReplyShape(t *testing.T) {
	// Real sentinel returns +OK; a future version returning an
	// integer would be unexpected and surface a controlled error.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = testPassword
	for range AuthRetryAttempts {
		fs.QueueReply("SENTINEL SET", ":1\r\n") // bogus int reply for SET
	}

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, testPassword)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected unexpected-reply error for SET")
	}
}

func TestSetAuthPassAll_MasterReplyMissingAuthPassSoftPass(t *testing.T) {
	// Some Valkey/Redis versions mask auth-pass in SENTINEL MASTER
	// output for security — the field is absent from an otherwise
	// well-formed reply. With the fix, setAndVerifyOne trusts
	// the preceding SET +OK and returns nil rather than aborting
	// the recovery path on every attempt.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = testPassword
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", "*2\r\n$4\r\nname\r\n$8\r\nmymaster\r\n")

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, testPassword)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected soft-pass on auth-pass omitted, got %v", results[0].Err)
	}
}

func TestSetAuthPassAll_MasterReplyMalformedStillErrors(t *testing.T) {
	// A reply that isn't a well-formed multi-bulk pair sequence
	// (odd length here) is a structural protocol error, not a
	// version-masking case — must still surface as an error so
	// the retry loop has a chance to recover from a transient
	// wire glitch.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = testPassword
	for range AuthRetryAttempts {
		fs.QueueReply("SENTINEL SET", "+OK\r\n")
		fs.QueueReply("SENTINEL MASTER", "*1\r\n$4\r\nname\r\n")
	}

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, testPassword)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected error on malformed MASTER reply")
	}
}

func TestSetAuthPassAll_RetrySucceedsAfterTransientFailure(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = freshPassword
	// First attempt: SET reply OK, MASTER reply with WRONG auth-pass
	// (transient mid-failover-like read of stale master state).
	// Second attempt: SET reply OK, MASTER reply with correct auth-pass.
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", buildMasterReply("stale"))
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", buildMasterReply(freshPassword))

	results := SetAuthPassAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, freshPassword)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected retry to succeed on attempt 2, got %v", results[0].Err)
	}
}

func TestSetAuthPassAll_RespectsContextCancellation(t *testing.T) {
	// The retry-loop's backoff select must observe ctx.Done and
	// return ctx.Err immediately on the second attempt (not wait
	// the 250ms backoff first). Cancellation is signalled the
	// moment the fake has served the first attempt's two commands
	// (SENTINEL SET + SENTINEL MASTER) — this places the cancel
	// deterministically during the retry-loop's backoff window
	// rather than at a fixed 50ms wall-clock that races the
	// in-flight first attempt under a contended CI runner.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = freshPassword
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", buildMasterReply("stale"))

	ctx, cancel := context.WithCancel(context.Background())
	// Goroutine polls fs.Sent() for the first-attempt commands; on
	// match it closes sawCommands and cancels ctx. On poll-deadline
	// expiry it cancels ctx anyway (so the main goroutine doesn't
	// block forever in SetAuthPassAll) but leaves sawCommands open
	// — the post-call assert turns that into a Fatalf in the test
	// goroutine. testing.T.Fatalf is documented as test-goroutine-
	// only because of runtime.Goexit, so the polling goroutine
	// strictly avoids it.
	sawCommands := make(chan struct{})
	go func() {
		deadline := time.NewTimer(eventuallyTimeout)
		defer deadline.Stop()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-deadline.C:
				cancel()
				return
			case <-tick.C:
				var sawSet, sawMaster bool
				for _, s := range fs.Sent() {
					if strings.HasPrefix(s, "SENTINEL SET") {
						sawSet = true
					}
					if strings.HasPrefix(s, "SENTINEL MASTER") {
						sawMaster = true
					}
				}
				if sawSet && sawMaster {
					close(sawCommands)
					cancel()
					return
				}
			}
		}
	}()

	results := SetAuthPassAll(ctx, []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, testMasterName, freshPassword)

	select {
	case <-sawCommands:
		// OK — cancel fired after the fake observed both commands.
	default:
		t.Fatalf("fake did not observe SENTINEL SET + SENTINEL MASTER from first attempt within %s; sent=%v", eventuallyTimeout, fs.Sent())
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected non-nil error on cancelled context")
	}
}
