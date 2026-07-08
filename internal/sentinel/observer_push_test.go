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
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// pushTestMaster is the master/CR name used by the push tests.
// Consolidated into one const so the literal isn't repeated (goconst).
const pushTestMaster = "pushmaster"

// newPushObserver builds an observer wired to the supplied push
// channel. start() is NOT called — dispatch is exercised directly, as
// the other observer dispatch tests do.
func newPushObserver(eventCh chan<- event.GenericEvent) *observer {
	return newObserver(
		types.NamespacedName{Namespace: "ns", Name: pushTestMaster},
		pushTestMaster, "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "10.0.0.1:26379"}},
		Options{},
		k8sevents.NewFakeRecorder(8),
		eventCh,
	)
}

// recvPush returns the single GenericEvent buffered on ch, failing if
// none is present (the send is synchronous within dispatch, so it has
// already landed by the time dispatch returns).
func recvPush(t *testing.T, ch <-chan event.GenericEvent) event.GenericEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	default:
		t.Fatal("expected a reconcile-trigger GenericEvent, got none")
		return event.GenericEvent{}
	}
}

// TestObserver_DispatchPushesGenericEvent pins the push half: each of
// the four topology-changing pubsub events enqueues exactly one
// GenericEvent carrying the observer's CR identity.
func TestObserver_DispatchPushesGenericEvent(t *testing.T) {
	cases := []struct {
		name    string
		channel string
		payload string
	}{
		{"switch-master", "+switch-master", fmt.Sprintf("%s 10.0.0.1 6379 10.0.0.2 6379", pushTestMaster)},
		{"failover-end", "+failover-end", fmt.Sprintf("master %s 10.0.0.2 6379", pushTestMaster)},
		{"odown", "+odown", fmt.Sprintf("master %s 10.0.0.1 6379", pushTestMaster)},
		{"odown-clear", "-odown", fmt.Sprintf("master %s 10.0.0.1 6379", pushTestMaster)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := make(chan event.GenericEvent, 2)
			o := newPushObserver(events)
			o.dispatch(context.Background(), Endpoint{Name: "vk0-sentinel-0"}, pubsubMessage{Channel: tc.channel, Payload: tc.payload})

			ev := recvPush(t, events)
			if ev.Object == nil {
				t.Fatal("GenericEvent.Object is nil")
			}
			if got := ev.Object.GetNamespace(); got != "ns" {
				t.Errorf("pushed event namespace = %q; want ns", got)
			}
			if got := ev.Object.GetName(); got != pushTestMaster {
				t.Errorf("pushed event name = %q; want %q", got, pushTestMaster)
			}
			// Exactly one push per event — nothing else queued.
			select {
			case extra := <-events:
				t.Errorf("expected exactly one push, got a second: %+v", extra)
			default:
			}
		})
	}
}

// TestObserver_DispatchNoPushForOtherMaster confirms a pubsub event
// for a different master name is filtered out before publish/notify —
// no spurious reconcile for an unrelated CR sharing a sentinel pool.
func TestObserver_DispatchNoPushForOtherMaster(t *testing.T) {
	events := make(chan event.GenericEvent, 2)
	o := newPushObserver(events)
	// Payload's master name differs from the observer's.
	o.dispatch(context.Background(), Endpoint{Name: "vk0-sentinel-0"},
		pubsubMessage{Channel: "+switch-master", Payload: "othermaster 10.0.0.1 6379 10.0.0.2 6379"})

	select {
	case ev := <-events:
		t.Errorf("expected no push for a non-matching master, got %+v", ev.Object)
	default:
	}
}

// TestObserver_NotifyNonBlockingWhenChannelFull pins the non-blocking
// contract: a full buffer drops the push rather than stalling the
// pubsub goroutine (the pull tick is the safety net). A blocking send
// here would deadlock the test.
func TestObserver_NotifyNonBlockingWhenChannelFull(t *testing.T) {
	events := make(chan event.GenericEvent, 1)
	events <- event.GenericEvent{} // pre-fill so the buffer is full
	o := newPushObserver(events)

	done := make(chan struct{})
	go func() {
		o.dispatch(context.Background(), Endpoint{Name: "vk0-sentinel-0"},
			pubsubMessage{Channel: "+switch-master", Payload: fmt.Sprintf("%s 10.0.0.1 6379 10.0.0.2 6379", pushTestMaster)})
		close(done)
	}()
	// If notify blocked on the full channel, close(done) never runs and
	// this receive hangs until the test deadline — the deadlock the
	// non-blocking send exists to prevent.
	<-done

	// The pre-filled placeholder is still there; the dropped push did
	// not displace it.
	if len(events) != 1 {
		t.Errorf("expected the full buffer to hold its single placeholder; len=%d", len(events))
	}
}

// TestObserver_NilChannelNotifyIsNoOp confirms an observer constructed
// without a push channel (test / alternate wiring) dispatches without
// panicking.
func TestObserver_NilChannelNotifyIsNoOp(t *testing.T) {
	o := newPushObserver(nil)
	o.dispatch(context.Background(), Endpoint{Name: "vk0-sentinel-0"},
		pubsubMessage{Channel: "+switch-master", Payload: fmt.Sprintf("%s 10.0.0.1 6379 10.0.0.2 6379", pushTestMaster)})
	// Reaching here without a panic is the assertion.
}
