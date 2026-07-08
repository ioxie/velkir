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

func TestSetMasterTuningAll_AllFieldsSet(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	results := SetMasterTuningAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		"vk0",
		MasterTuning{
			DownAfterMilliseconds: 3000,
			FailoverTimeout:       60000,
			ParallelSyncs:         1,
		},
		"")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected success, got %v", results[0].Err)
	}
	var sawDownAfter, sawFailover, sawParallel bool
	for _, s := range fs.Sent() {
		if strings.Contains(s, "SENTINEL SET vk0 down-after-milliseconds 3000") {
			sawDownAfter = true
		}
		if strings.Contains(s, "SENTINEL SET vk0 failover-timeout 60000") {
			sawFailover = true
		}
		if strings.Contains(s, "SENTINEL SET vk0 parallel-syncs 1") {
			sawParallel = true
		}
	}
	if !sawDownAfter {
		t.Errorf("expected SET down-after-milliseconds; sent: %v", fs.Sent())
	}
	if !sawFailover {
		t.Errorf("expected SET failover-timeout; sent: %v", fs.Sent())
	}
	if !sawParallel {
		t.Errorf("expected SET parallel-syncs; sent: %v", fs.Sent())
	}
}

func TestSetMasterTuningAll_AllZeroIsNoOp(t *testing.T) {
	results := SetMasterTuningAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}},
		"vk0", MasterTuning{}, "")
	if results != nil {
		t.Errorf("expected nil for all-zero tuning, got %v", results)
	}
}

func TestSetMasterTuningAll_PartialSubsetSkipsZero(t *testing.T) {
	// Only DownAfterMilliseconds set — must emit exactly one SET
	// (no failover-timeout / parallel-syncs).
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	results := SetMasterTuningAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		"vk0",
		MasterTuning{DownAfterMilliseconds: 5000},
		"")

	if results[0].Err != nil {
		t.Errorf("unexpected err: %v", results[0].Err)
	}
	setCount := 0
	for _, s := range fs.Sent() {
		if strings.HasPrefix(s, "SENTINEL SET") {
			setCount++
		}
	}
	if setCount != 1 {
		t.Errorf("expected exactly 1 SET (only DownAfterMilliseconds populated), got %d; sent: %v", setCount, fs.Sent())
	}
}

func TestSetMasterTuningAll_EmptyEndpointsIsNoOp(t *testing.T) {
	results := SetMasterTuningAll(context.Background(), nil, "vk0",
		MasterTuning{DownAfterMilliseconds: 3000}, "")
	if results != nil {
		t.Errorf("expected nil for empty endpoints, got %v", results)
	}
}

func TestSetMasterTuningAll_AbortsOnFirstFieldError(t *testing.T) {
	// First SET errors → second + third are skipped (caller's next
	// reconcile retries).
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL SET", "-ERR wrong\r\n")

	results := SetMasterTuningAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		"vk0",
		MasterTuning{
			DownAfterMilliseconds: 3000,
			FailoverTimeout:       60000,
			ParallelSyncs:         1,
		},
		"")

	if results[0].Err == nil {
		t.Error("expected error from -ERR reply")
	}
	setCount := 0
	for _, s := range fs.Sent() {
		if strings.HasPrefix(s, "SENTINEL SET") {
			setCount++
		}
	}
	if setCount != 1 {
		t.Errorf("expected exactly 1 SET (abort on first error), got %d; sent: %v", setCount, fs.Sent())
	}
}
