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

func TestRemoveAll_Success(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")

	results := RemoveAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}, "vk0", "")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected success, got %v", results[0].Err)
	}
	var sawRemove bool
	for _, s := range fs.Sent() {
		if strings.HasPrefix(s, "SENTINEL REMOVE vk0") {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Errorf("expected SENTINEL REMOVE vk0 on the wire; sent: %v", fs.Sent())
	}
}

func TestRemoveAll_NoSuchMasterIsSuccess(t *testing.T) {
	// "No such master with that name" means the entry is already
	// gone — the operator's target state. RemoveAll must surface
	// this as success so the caller's follow-up MONITOR runs.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL REMOVE", "-ERR No such master with that name\r\n")

	results := RemoveAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}, "vk0", "")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected nil err (no-such-master is target state), got %v", results[0].Err)
	}
}

func TestRemoveAll_OtherErrSurfacesAsErr(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL REMOVE", "-ERR Some other failure\r\n")

	results := RemoveAll(context.Background(),
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}}, "vk0", "")

	if results[0].Err == nil {
		t.Error("expected error on non-no-such-master -ERR reply")
	}
}

func TestRemoveAll_EmptyEndpointsReturnsEmpty(t *testing.T) {
	results := RemoveAll(context.Background(), nil, "vk0", "")
	if len(results) != 0 {
		t.Errorf("expected empty results for empty endpoints, got %d", len(results))
	}
}
