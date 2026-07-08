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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

const vkReadbackSentinel = "vk0-sentinel-0"

// TestAuthFailureNames_OnlyDefinitiveMismatch pins that only a
// verification MISMATCH (errAuthPassMismatch — SENTINEL MASTER did not
// echo the value we SET) is reported. A transient transport error must
// NOT be surfaced, so a single dial blip can't seed a recoverable
// sentinel straight to the caller's stuck threshold.
func TestAuthFailureNames_OnlyDefinitiveMismatch(t *testing.T) {
	got := authFailureNames([]AuthResult{
		{Name: "ok", Err: nil},                                                // success — excluded
		{Name: "mismatch", Err: errAuthPassMismatch},                          // definitive — included
		{Name: "transient", Err: fmt.Errorf("dial tcp: refused")},             // transport — excluded
		{Name: "wrapped", Err: fmt.Errorf("verify: %w", errAuthPassMismatch)}, // wrapped — included
	})
	want := map[string]bool{"mismatch": true, "wrapped": true}
	if len(got) != len(want) {
		t.Fatalf("authFailureNames = %v; want exactly the definitive mismatches %v", got, want)
	}
	for _, n := range got {
		if !want[n] {
			t.Errorf("authFailureNames wrongly included %q", n)
		}
	}
}

// TestRecoverStrandedSentinels_SubQuorumNotHealthy pins that a pass
// which sees no stranded targets but only a reachable MINORITY is NOT
// reported Healthy — the unreachable majority may be stranded, so the
// caller must not clear its no-progress state.
func TestRecoverStrandedSentinels_SubQuorumNotHealthy(t *testing.T) {
	fs := newFakeSentinel(t) // reachable, has peers (not stranded)
	defer fs.Stop()
	queueSentinelsReply(fs, PeerInfo{Name: "peer-1", IP: "10.0.0.9", Port: 26379, RunID: "r1"})

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
		// Three endpoints, only the first reachable → reachable
		// minority (1 of 3, quorum 2).
		Endpoints: []Endpoint{
			{Name: vkReadbackSentinel, Addr: fs.Addr()},
			{Name: "vk0-sentinel-1", Addr: "127.0.0.1:1"}, // closed
			{Name: "vk0-sentinel-2", Addr: "127.0.0.1:2"}, // closed
		},
	}, false)

	if out.Healthy {
		t.Errorf("a reachable-minority no-targets pass must NOT report Healthy=true; got %+v", out)
	}
}

// TestRecoverStrandedSentinels_HealthyClassification pins the Healthy
// signal: a full classification that finds no stranded / re-point /
// ghost target returns Healthy=true (distinct from a gate-deferred
// pass) so the caller can clear its no-progress tracking.
func TestRecoverStrandedSentinels_HealthyClassification(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	// Non-empty peer-list → not stranded → healthy classification.
	queueSentinelsReply(fs, PeerInfo{Name: "peer-1", IP: "10.0.0.9", Port: 26379, RunID: "r1"})

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
		Endpoints:  []Endpoint{{Name: vkReadbackSentinel, Addr: fs.Addr()}},
	}, false)

	if !out.Healthy {
		t.Errorf("non-stranded classification must set Healthy=true; got %+v", out)
	}
	if len(out.Stranded) != 0 {
		t.Errorf("Stranded must be empty on a healthy pass; got %v", out.Stranded)
	}
}

// TestRecoverStrandedSentinels_GateDeferNotHealthy pins that a
// gate-deferred pass (here: the deferral predicate) returns
// Healthy=false — the caller must not mistake a defer for recovery and
// clear its no-progress state.
func TestRecoverStrandedSentinels_GateDeferNotHealthy(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	m.SetDeferralPredicate(func(types.NamespacedName) bool { return true })
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
		Endpoints:  []Endpoint{{Name: vkReadbackSentinel, Addr: fs.Addr()}},
	}, false /* do not bypass */)

	if out.Healthy {
		t.Errorf("a gate-deferred pass must not report Healthy=true; got %+v", out)
	}
}

// TestRecoverStrandedSentinels_AuthFailureSurfaced pins that a wiped
// sentinel whose post-MONITOR auth-pass re-propagation fails
// verification is reported in AuthFailures (the caller treats it as an
// immediate no-progress cause — a sentinel that can't AUTH can never
// gossip its peer-list back).
func TestRecoverStrandedSentinels_AuthFailureSurfaced(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.password = testPassword
	fm := newFakeSentinel(t) // fake master; auto-answers AUTH + PING
	defer fm.Stop()
	fm.password = testPassword
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	queueSentinelsReply(fs /* empty — stranded */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	// Auth verify (1 attempt): SET ok, but MASTER echoes a WRONG
	// auth-pass → verification fails.
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL MASTER", buildMasterReply("wrong-pass"))
	// Tuning SETs fired after auth (down-after / failover-timeout /
	// parallel-syncs) — three +OK.
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(32), Options{AuthRetryAttempts: 1})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     2,
		Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
		Endpoints:  []Endpoint{{Name: vkReadbackSentinel, Addr: fs.Addr()}},
		Password:   testPassword,
	}, false)

	if len(out.Stranded) != 1 {
		t.Fatalf("expected 1 stranded sentinel, got %v", out.Stranded)
	}
	if len(out.AuthFailures) != 1 || out.AuthFailures[0] != vkReadbackSentinel {
		t.Errorf("expected AuthFailures=[%s]; got %v", vkReadbackSentinel, out.AuthFailures)
	}
}
