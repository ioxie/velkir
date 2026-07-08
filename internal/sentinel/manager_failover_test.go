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

	"k8s.io/apimachinery/pkg/types"
)

func TestManager_IssueFailover_FirstReachableWins(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueFailoverOK(fs)

	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: fs.Addr()},
	}
	if err := m.IssueFailover(context.Background(), cr, "vk0", "", endpoints); err != nil {
		t.Fatalf("IssueFailover: %v", err)
	}
}

func TestManager_IssueFailover_INPROGTreatedAsSuccessSentinel(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fs.QueueReply("SENTINEL FAILOVER", "-INPROG already running\r\n")

	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	err := m.IssueFailover(context.Background(), cr, "vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}})
	if !errors.Is(err, ErrFailoverInProgress) {
		t.Fatalf("expected ErrFailoverInProgress, got %v", err)
	}
}

func TestManager_IssueFailover_FallsThroughDialFailure(t *testing.T) {
	// First endpoint unreachable, second accepts. The dispatcher
	// must not give up after the first dial failure.
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueFailoverOK(fs)

	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}, // closed port
		{Name: "vk0-sentinel-1", Addr: fs.Addr()},
	}
	if err := m.IssueFailover(context.Background(), cr, "vk0", "", endpoints); err != nil {
		t.Fatalf("IssueFailover: %v", err)
	}
}

func TestManager_IssueFailover_AllFailedAggregatesError(t *testing.T) {
	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{
		{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"},
		{Name: "vk0-sentinel-1", Addr: "127.0.0.1:2"},
	}
	err := m.IssueFailover(context.Background(), cr, "vk0", "", endpoints)
	if err == nil {
		t.Fatal("expected aggregated error when every endpoint fails")
	}
	if !strings.Contains(err.Error(), "all 2 sentinel endpoint(s) failed") {
		t.Errorf("error should mention aggregate count; got %v", err)
	}
}

func TestManager_IssueFailover_RejectsZeroEndpoints(t *testing.T) {
	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if err := m.IssueFailover(context.Background(), cr, "vk0", "", nil); err == nil {
		t.Fatal("expected validation error for empty endpoints")
	}
}

func TestManager_IssueFailover_RejectsEmptyMasterName(t *testing.T) {
	m := NewManager(nil, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	endpoints := []Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}}
	if err := m.IssueFailover(context.Background(), cr, "", "", endpoints); err == nil {
		t.Fatal("expected validation error for empty masterName")
	}
}
