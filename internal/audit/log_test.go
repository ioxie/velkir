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

package audit

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func TestAllEventsSortedNoDuplicates(t *testing.T) {
	// init() sorts AllEvents and panics on duplicates. Running the
	// test exercises init; the explicit checks here pin the post-
	// condition so a future contributor sees the test fail rather
	// than just an init-time panic in some unrelated suite.
	if !sort.StringsAreSorted(AllEvents) {
		t.Fatalf("AllEvents must be sorted after init; got %v", AllEvents)
	}
	for i := 1; i < len(AllEvents); i++ {
		if AllEvents[i] == AllEvents[i-1] {
			t.Errorf("duplicate event in AllEvents at index %d: %q", i, AllEvents[i])
		}
	}
	if len(AllEvents) == 0 {
		t.Fatal("AllEvents must not be empty")
	}
}

func TestIsKnown(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{EventReconciliationPaused, true},
		{EventSentinelFailoverIssued, true},
		{EventDeviationAccepted, true},
		{"", false},
		{"unknown_event", false},
		{"sentinel_failover", false}, // close-but-not-equal substring
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsKnown(tc.name); got != tc.want {
				t.Errorf("IsKnown(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// captureLog runs fn with a logr.Logger that writes every call into
// the returned slice. Used to assert Log's emitted shape without
// depending on stdout interception.
func captureLog(t *testing.T, fn func(ctx context.Context)) []string {
	t.Helper()
	var lines []string
	captured := funcr.New(func(prefix, args string) {
		lines = append(lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 1})
	ctx := log.IntoContext(context.Background(), logr.New(captured.GetSink()))
	fn(ctx)
	return lines
}

func TestLog_EmitsKnownEvent(t *testing.T) {
	lines := captureLog(t, func(ctx context.Context) {
		Log(ctx, Event{
			Name:      EventSentinelResetIssued,
			CR:        types.NamespacedName{Namespace: "ns", Name: "vk0"},
			Requestor: "user@example.com",
			Attrs: map[string]string{
				"targets": "[s0,s1]",
				"reason":  "pod_replaced",
			},
		})
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d: %v", len(lines), lines)
	}
	got := lines[0]
	for _, want := range []string{
		`"event"="sentinel_reset_issued"`,
		`"cr"="ns/vk0"`,
		`"requestor"="user@example.com"`,
		`"reason"="pod_replaced"`,
		`"targets"="[s0,s1]"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("log line missing %q in: %s", want, got)
		}
	}
}

func TestLog_UnknownEventDropped(t *testing.T) {
	lines := captureLog(t, func(ctx context.Context) {
		Log(ctx, Event{
			Name: "made_up_event",
			CR:   types.NamespacedName{Namespace: "ns", Name: "vk0"},
		})
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 dropped-event log line, got %d: %v", len(lines), lines)
	}
	got := lines[0]
	if !strings.Contains(got, "audit: dropping unknown event") {
		t.Errorf("unknown-event drop line not emitted; got: %s", got)
	}
	if !strings.Contains(got, `"name"="made_up_event"`) {
		t.Errorf("dropped-event line should carry the offending name; got: %s", got)
	}
}

func TestLog_EmptyRequestorBecomesOperator(t *testing.T) {
	lines := captureLog(t, func(ctx context.Context) {
		Log(ctx, Event{
			Name: EventPodLabelReconciled,
			CR:   types.NamespacedName{Namespace: "ns", Name: "vk0"},
		})
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], `"requestor"="operator:reconciler"`) {
		t.Errorf("empty Requestor must default to 'operator:reconciler'; got: %s", lines[0])
	}
}

func TestLog_AttrsSorted(t *testing.T) {
	// The Attrs map iteration order is randomised by the Go
	// runtime; Log sorts the keys so the rendered line is
	// deterministic. Assert by emitting two events with the same
	// Attrs and comparing the rendered lines.
	emit := func() string {
		lines := captureLog(t, func(ctx context.Context) {
			Log(ctx, Event{
				Name: EventSentinelFailoverIssued,
				CR:   types.NamespacedName{Namespace: "ns", Name: "vk0"},
				Attrs: map[string]string{
					"z_last":      "z",
					"a_first":     "a",
					"m_middle":    "m",
					"old_primary": "vk0-0",
				},
			})
		})
		if len(lines) != 1 {
			t.Fatalf("expected 1 line, got %d", len(lines))
		}
		return lines[0]
	}
	first := emit()
	second := emit()
	if first != second {
		t.Errorf("Log output must be deterministic across runs; got\nfirst:  %s\nsecond: %s", first, second)
	}
	// Check ordering: a_first should appear before m_middle before
	// old_primary before z_last.
	for _, pair := range [][2]string{
		{`"a_first"="a"`, `"m_middle"="m"`},
		{`"m_middle"="m"`, `"old_primary"="vk0-0"`},
		{`"old_primary"="vk0-0"`, `"z_last"="z"`},
	} {
		if strings.Index(first, pair[0]) > strings.Index(first, pair[1]) {
			t.Errorf("Attrs not sorted: expected %q before %q in: %s", pair[0], pair[1], first)
		}
	}
}
