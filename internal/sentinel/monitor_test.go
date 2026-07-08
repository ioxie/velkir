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
	"strings"
	"testing"
)

func TestMonitorAll_AllSucceed(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	fs1.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fs2.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	results := MonitorAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}, "vk0", "10.0.0.5", "", 6379, 2)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("result[%d] (%s): unexpected error %v", i, r.Name, r.Err)
		}
	}
	// Wire-format assertion: the MONITOR command must carry the
	// master IP, port, and quorum as separate args so sentinel
	// parses them correctly.
	sent := fs1.Sent()
	found := false
	for _, line := range sent {
		// `SENTINEL MONITOR vk0 10.0.0.5 6379 2`
		if strings.Contains(line, "SENTINEL MONITOR vk0 10.0.0.5 6379 2") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected `SENTINEL MONITOR vk0 10.0.0.5 6379 2` in sent commands; got %v", sent)
	}
}

func TestMonitorAll_SentinelErrSurfaces(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL MONITOR", "-ERR Duplicated master name\r\n")

	results := MonitorAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, "vk0", "10.0.0.5", "", 6379, 2)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected -ERR Duplicated master name to surface")
	}
	if !strings.Contains(results[0].Err.Error(), "Duplicated") {
		t.Errorf("error doesn't mention duplicate: %v", results[0].Err)
	}
}

func TestMonitorAll_DialFailureSurfaces(t *testing.T) {
	results := MonitorAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-dead", Addr: "127.0.0.1:1"},
	}, "vk0", "10.0.0.5", "", 6379, 2)
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected dial error, got %+v", results)
	}
}

func TestProbeAll_HappyPath(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	// SENTINEL get-master-addr-by-name returns [ip, port] as a
	// 2-element array of bulk strings.
	fs1.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME",
		"*2\r\n$8\r\n10.0.0.5\r\n$4\r\n6379\r\n")
	fs2.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME",
		"*2\r\n$8\r\n10.0.0.5\r\n$4\r\n6379\r\n")

	results := ProbeAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
		{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
	}, "vk0", "")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("%s: unexpected error %v", r.Name, r.Err)
		}
		if r.Addr != "10.0.0.5:6379" {
			t.Errorf("%s: addr = %q; want 10.0.0.5:6379", r.Name, r.Addr)
		}
	}
}

func TestProbeAll_NilReplyMappsToEmptyAddr(t *testing.T) {
	// Sentinel returns nil array when the master isn't monitored
	// (post-RESET before MONITOR). The probe must report ""
	// + nil error so the caller treats it as "needs MONITOR".
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", "*-1\r\n")

	results := ProbeAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}, "vk0", "")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("nil reply should not be an error, got %v", results[0].Err)
	}
	if results[0].Addr != "" {
		t.Errorf("nil reply should map to empty Addr, got %q", results[0].Addr)
	}
}

func TestProbeAll_DialFailureSurfaces(t *testing.T) {
	results := ProbeAll(context.Background(), []Endpoint{
		{Name: "vk0-sentinel-dead", Addr: "127.0.0.1:1"},
	}, "vk0", "")
	if results[0].Err == nil {
		t.Error("expected dial error")
	}
}

// TestProbeAll_CapturesEpochAndFlags pins the best-effort epoch/flags
// capture and the Err isolation: a good SENTINEL MASTER read populates
// Epoch/EpochOK/Flags; a failed SENTINEL MASTER read leaves them zero but
// does NOT set Err (GET-MASTER-ADDR still succeeded); a failed
// GET-MASTER-ADDR read sets Err.
func TestProbeAll_CapturesEpochAndFlags(t *testing.T) {
	good := newFakeSentinel(t)
	masterErr := newFakeSentinel(t)
	addrErr := newFakeSentinel(t)
	t.Cleanup(func() { good.Stop(); masterErr.Stop(); addrErr.Stop() })

	good.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", "*2\r\n$8\r\n10.0.0.5\r\n$4\r\n6379\r\n")
	good.QueueReply("SENTINEL MASTER", buildArrayReply("config-epoch", "7", "flags", "master,s_down"))

	// GET-MASTER-ADDR succeeds; SENTINEL MASTER errors → epoch unknown,
	// flags empty, but Err stays nil.
	masterErr.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", "*2\r\n$8\r\n10.0.0.5\r\n$4\r\n6379\r\n")
	masterErr.QueueReply("SENTINEL MASTER", "-ERR no such master\r\n")

	// GET-MASTER-ADDR itself errors → Err set.
	addrErr.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", "-ERR boom\r\n")

	results := ProbeAll(context.Background(), []Endpoint{
		{Name: "good", Addr: good.Addr()},
		{Name: "master-err", Addr: masterErr.Addr()},
		{Name: "addr-err", Addr: addrErr.Addr()},
	}, "vk0", "")

	byName := map[string]ProbeResult{}
	for _, r := range results {
		byName[r.Name] = r
	}
	if g := byName["good"]; g.Err != nil || !g.EpochOK || g.Epoch != 7 || g.Flags != "master,s_down" {
		t.Errorf("good probe = %+v; want epoch 7, epochOK, flags master,s_down, no err", g)
	}
	if m := byName["master-err"]; m.Err != nil || m.EpochOK || m.Epoch != 0 || m.Flags != "" {
		t.Errorf("master-err probe = %+v; want epochOK=false, empty flags, nil err (Err isolation)", m)
	}
	if a := byName["addr-err"]; a.Err == nil {
		t.Errorf("addr-err probe = %+v; want a GET-MASTER-ADDR error", a)
	}
}
