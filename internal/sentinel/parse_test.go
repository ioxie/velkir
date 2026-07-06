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
	"testing"
)

// testPrimaryAddr is the canonical primary "ip:port" used in
// happy-path parser tests so the assertions don't repeat the
// literal — keeps the goconst linter quiet too.
const testPrimaryAddr = "10.0.0.7:6379"

func TestParseEventKind(t *testing.T) {
	cases := []struct {
		channel string
		want    EventKind
	}{
		{"+switch-master", KindSwitchMaster},
		{"+failover-end", KindFailoverEnd},
		{"+failover-end-for-timeout", KindFailoverEndTimeout},
		{"+odown", KindODown},
		{"-odown", KindODownClear},
		{"+tilt", KindTilt},
		{"-tilt", KindTiltClear},
		{"+sdown", KindOther},
		{"unknown", KindOther},
		{"", KindOther},
	}
	for _, tc := range cases {
		if got := ParseEventKind(tc.channel); got != tc.want {
			t.Errorf("ParseEventKind(%q) = %q, want %q", tc.channel, got, tc.want)
		}
	}
}

func TestParseSwitchMaster(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		// Real sentinel emits this on +switch-master:
		//   "<masterName> <oldIP> <oldPort> <newIP> <newPort>"
		ev, ok := ParseSwitchMaster("vk0 10.0.0.5 6379 10.0.0.7 6379")
		if !ok {
			t.Fatal("expected ok=true")
		}
		if ev.MasterName != "vk0" {
			t.Errorf("MasterName=%q, want vk0", ev.MasterName)
		}
		if ev.OldAddr != "10.0.0.5:6379" {
			t.Errorf("OldAddr=%q, want 10.0.0.5:6379", ev.OldAddr)
		}
		if ev.NewAddr != testPrimaryAddr {
			t.Errorf("NewAddr=%q, want 10.0.0.7:6379", ev.NewAddr)
		}
	})

	t.Run("rejects short payload", func(t *testing.T) {
		if _, ok := ParseSwitchMaster("vk0 10.0.0.5 6379"); ok {
			t.Error("expected ok=false on short payload")
		}
	})

	t.Run("rejects bad port", func(t *testing.T) {
		if _, ok := ParseSwitchMaster("vk0 10.0.0.5 abc 10.0.0.7 6379"); ok {
			t.Error("expected ok=false on non-numeric old port")
		}
		if _, ok := ParseSwitchMaster("vk0 10.0.0.5 6379 10.0.0.7 0"); ok {
			t.Error("expected ok=false on zero new port")
		}
	})

	t.Run("ipv6 address with brackets", func(t *testing.T) {
		ev, ok := ParseSwitchMaster("vk0 fd00::1 6379 fd00::2 6379")
		if !ok {
			t.Fatal("expected ok=true on IPv6 address")
		}
		if ev.NewAddr != "[fd00::2]:6379" {
			t.Errorf("NewAddr=%q, want [fd00::2]:6379", ev.NewAddr)
		}
	})
}

func TestParseMasterEvent(t *testing.T) {
	t.Run("failover-end shape", func(t *testing.T) {
		// Real shape: "master <name> <ip> <port>"
		ev, ok := ParseMasterEvent("master vk0 10.0.0.7 6379")
		if !ok {
			t.Fatal("expected ok=true")
		}
		if ev.MasterName != "vk0" {
			t.Errorf("MasterName=%q, want vk0", ev.MasterName)
		}
		if ev.Addr != testPrimaryAddr {
			t.Errorf("Addr=%q, want 10.0.0.7:6379", ev.Addr)
		}
	})

	t.Run("odown trailing-fields shape", func(t *testing.T) {
		// +odown sometimes appends "@ <ip> <port>" for slave context.
		// Parser ignores trailing fields.
		ev, ok := ParseMasterEvent("master vk0 10.0.0.7 6379 # quorum 2/2")
		if !ok {
			t.Fatal("expected ok=true on extended shape")
		}
		if ev.Addr != testPrimaryAddr {
			t.Errorf("Addr=%q, want 10.0.0.7:6379", ev.Addr)
		}
	})

	t.Run("rejects non-master prefix", func(t *testing.T) {
		if _, ok := ParseMasterEvent("slave vk0 10.0.0.7 6379"); ok {
			t.Error("expected ok=false; observer should not accept slave-prefix events as primary signals")
		}
	})

	t.Run("rejects short payload", func(t *testing.T) {
		if _, ok := ParseMasterEvent("master vk0 10.0.0.7"); ok {
			t.Error("expected ok=false on missing port")
		}
	})

	t.Run("rejects bad port", func(t *testing.T) {
		if _, ok := ParseMasterEvent("master vk0 10.0.0.7 -1"); ok {
			t.Error("expected ok=false on negative port")
		}
	})
}

func TestParseGetMasterAddr(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		// SENTINEL GET-MASTER-ADDR-BY-NAME returns ["<ip>","<port>"]
		// (each is a RESP bulk string).
		addr, ok := ParseGetMasterAddr([]any{"10.0.0.7", "6379"})
		if !ok {
			t.Fatal("expected ok=true")
		}
		if addr != testPrimaryAddr {
			t.Errorf("addr=%q, want 10.0.0.7:6379", addr)
		}
	})

	t.Run("rejects nil reply", func(t *testing.T) {
		if _, ok := ParseGetMasterAddr(nil); ok {
			t.Error("expected ok=false on nil")
		}
	})

	t.Run("rejects too-short reply", func(t *testing.T) {
		if _, ok := ParseGetMasterAddr([]any{"10.0.0.7"}); ok {
			t.Error("expected ok=false on single-element array")
		}
	})

	t.Run("rejects empty host", func(t *testing.T) {
		if _, ok := ParseGetMasterAddr([]any{"", "6379"}); ok {
			t.Error("expected ok=false on empty host")
		}
	})

	t.Run("rejects non-numeric port", func(t *testing.T) {
		if _, ok := ParseGetMasterAddr([]any{"10.0.0.7", "abc"}); ok {
			t.Error("expected ok=false on non-numeric port")
		}
	})
}

func TestParseSentinelMasterEpoch(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
		ok   bool
	}{
		{"happy path", []any{"name", "vk0", "ip", "10.0.0.7", "port", "6379", "config-epoch", "42"}, 42, true},
		{"epoch first", []any{"config-epoch", "7", "name", "vk0"}, 7, true},
		{"missing config-epoch", []any{"name", "vk0", "ip", "10.0.0.7"}, 0, false},
		{"non-numeric epoch", []any{"config-epoch", "abc"}, 0, false},
		{"nil reply", nil, 0, false},
		{"too short", []any{"name"}, 0, false},
		{"odd-length array (key without value)", []any{"name", "vk0", "config-epoch"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSentinelMasterEpoch(tc.in)
			if ok != tc.ok {
				t.Errorf("ok=%v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("epoch=%d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseSentinelMasterFlags(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want MasterFlags
		ok   bool
	}{
		{"both down", []any{"flags", "master,s_down,o_down"}, MasterFlags{SDown: true, ODown: true}, true},
		{"o_down only", []any{"flags", "master,o_down"}, MasterFlags{ODown: true}, true},
		{"s_down only", []any{"flags", "master,s_down"}, MasterFlags{SDown: true}, true},
		{"healthy master (present and clear)", []any{"flags", "master"}, MasterFlags{}, true},
		{"flags first, then other fields", []any{"flags", "master,o_down", "name", "vk0"}, MasterFlags{ODown: true}, true},
		{"empty flags value", []any{"flags", ""}, MasterFlags{}, true},
		{"missing flags key", []any{"name", "vk0", "ip", "10.0.0.7"}, MasterFlags{}, false},
		{"non-string flags value", []any{"flags", 123}, MasterFlags{}, false},
		{"nil reply", nil, MasterFlags{}, false},
		{"too short", []any{"flags"}, MasterFlags{}, false},
		{"odd-length array (key without value)", []any{"name", "vk0", "flags"}, MasterFlags{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSentinelMasterFlags(tc.in)
			if ok != tc.ok {
				t.Errorf("ok=%v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("flags=%+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseSentinelMasterNumOtherSentinels(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int
		ok   bool
	}{
		{"happy path", []any{"name", "vk0", "ip", "10.0.0.7", "port", "6379", "num-other-sentinels", "2"}, 2, true},
		{"zero peers (stranded signature)", []any{"name", "vk0", "num-other-sentinels", "0"}, 0, true},
		{"field first", []any{"num-other-sentinels", "5", "name", "vk0"}, 5, true},
		{"missing key", []any{"name", "vk0", "ip", "10.0.0.7"}, 0, false},
		{"non-numeric value", []any{"num-other-sentinels", "abc"}, 0, false},
		{"nil reply", nil, 0, false},
		{"too short", []any{"name"}, 0, false},
		{"odd-length array (key without value)", []any{"name", "vk0", "num-other-sentinels"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSentinelMasterNumOtherSentinels(tc.in)
			if ok != tc.ok {
				t.Errorf("ok=%v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("count=%d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseSentinelMasterNumSlaves(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int
		ok   bool
	}{
		{"happy path", []any{"name", "vk0", "ip", "10.0.0.7", "port", "6379", "num-slaves", "2"}, 2, true},
		{"zero replicas", []any{"name", "vk0", "num-slaves", "0"}, 0, true},
		{"field first", []any{"num-slaves", "5", "name", "vk0"}, 5, true},
		{"missing key", []any{"name", "vk0", "ip", "10.0.0.7"}, 0, false},
		{"non-numeric value", []any{"num-slaves", "abc"}, 0, false},
		{"nil reply", nil, 0, false},
		{"too short", []any{"name"}, 0, false},
		{"odd-length array (key without value)", []any{"name", "vk0", "num-slaves"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSentinelMasterNumSlaves(tc.in)
			if ok != tc.ok {
				t.Errorf("ok=%v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("count=%d, want %d", got, tc.want)
			}
		})
	}
}

// TestParseSentinelMaster_SharedReplyBothCounts pins the shared-reply
// piggyback: one SENTINEL MASTER reply yields BOTH num-slaves and
// num-other-sentinels, so the topology-hygiene read costs no extra
// round-trip on top of the epoch / flags reads.
func TestParseSentinelMaster_SharedReplyBothCounts(t *testing.T) {
	reply := []any{
		"name", "vk0",
		"ip", "10.0.0.7",
		"port", "6379",
		"config-epoch", "42",
		"num-slaves", "2",
		"num-other-sentinels", "2",
	}
	nSlaves, slavesOK := ParseSentinelMasterNumSlaves(reply)
	if !slavesOK || nSlaves != 2 {
		t.Errorf("num-slaves = %d,%v; want 2,true", nSlaves, slavesOK)
	}
	nSent, sentOK := ParseSentinelMasterNumOtherSentinels(reply)
	if !sentOK || nSent != 2 {
		t.Errorf("num-other-sentinels = %d,%v; want 2,true", nSent, sentOK)
	}
}

func TestParseSentinelMasterAuthPass(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
		ok   bool
	}{
		{"happy path", []any{"name", "vk0", "ip", "10.0.0.7", "port", "6379", "auth-pass", "s3cret"}, "s3cret", true},
		{"auth-pass first", []any{"auth-pass", "p1", "name", "vk0"}, "p1", true},
		{"empty auth-pass present", []any{"name", "vk0", "auth-pass", ""}, "", true},
		{"missing auth-pass key", []any{"name", "vk0", "ip", "10.0.0.7"}, "", false},
		{"nil reply", nil, "", false},
		{"too short", []any{"name"}, "", false},
		{"odd-length array (key without value)", []any{"name", "vk0", "auth-pass"}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSentinelMasterAuthPass(tc.in)
			if ok != tc.ok {
				t.Errorf("ok=%v, want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("auth-pass=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseCKQuorum(t *testing.T) {
	cases := []struct {
		name  string
		reply any
		want  bool
	}{
		{"OK with detail", "OK 3 usable Sentinels. Quorum and failover authorization can be reached", true},
		{"plain OK", "OK", true},
		{"NOQUORUM error", nil, false},
		{"non-string", int64(1), false},
		{"unrelated string", "+PONG", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseCKQuorum(tc.reply); got != tc.want {
				t.Errorf("ParseCKQuorum(%v) = %v, want %v", tc.reply, got, tc.want)
			}
		})
	}
}

func TestQuorumThreshold(t *testing.T) {
	// 1 sentinel: degenerate (webhook should reject) — return 1 so
	// the observer's reachable<threshold guard still fires in
	// practice. 2 sentinels: require both. 3+: strict majority.
	cases := []struct {
		n, want int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
		{7, 4},
	}
	for _, tc := range cases {
		if got := QuorumThreshold(tc.n); got != tc.want {
			t.Errorf("QuorumThreshold(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}
