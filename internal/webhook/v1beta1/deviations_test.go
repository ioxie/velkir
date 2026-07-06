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

package v1beta1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// TestDeviations_StructuredReasonAndField pins the structured contract
// the controller's emitDeviations relies on: each deviation carries the
// catalog Reason and the JSON-path field it concerns. The field is the
// DeviationEmitter's dedup discriminator and is NOT present in the
// rendered admission-warning string, so it needs its own assertion.
func TestDeviations_StructuredReasonAndField(t *testing.T) {
	minAvail := intstr.FromInt32(0) // floor for replicas=3 is 2 → too permissive
	v := validStandalone("dev-struct", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &minAvail}
		v.Spec.Rollout.MaxUnavailable = 2 // > 1 → RolloutFragileQuorum
	})

	want := map[events.Reason]string{
		events.PDBTooPermissive:     "spec.valkey.pdb",
		events.RolloutFragileQuorum: "spec.rollout.maxUnavailable",
	}

	got := map[events.Reason]string{}
	for _, d := range Deviations(v) {
		got[d.Reason] = d.Field
		if d.Message == "" {
			t.Errorf("deviation %s has an empty message", d.Reason)
		}
	}
	for reason, field := range want {
		if got[reason] != field {
			t.Errorf("Deviations: reason %s → field %q, want %q (all: %+v)",
				reason, got[reason], field, Deviations(v))
		}
	}
}

// TestDeviations_RenderMatchesReasonPrefix asserts the admission-warning
// rendering is exactly "<Reason>: <Message>" — the format admission
// clients (and the existing string-asserting validator tests) depend on.
func TestDeviations_RenderMatchesReasonPrefix(t *testing.T) {
	minAvail := intstr.FromInt32(0)
	v := validStandalone("dev-render", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &minAvail}
	})

	devs := Deviations(v)
	warns := renderDeviationWarnings(v)
	if len(warns) != len(devs) {
		t.Fatalf("render produced %d warnings for %d deviations", len(warns), len(devs))
	}
	for i, d := range devs {
		want := string(d.Reason) + ": " + d.Message
		if warns[i] != want {
			t.Errorf("warning %d = %q, want %q", i, warns[i], want)
		}
	}
}

// TestDeviations_NilForCleanCR confirms a best-practice CR yields no
// deviations (so the reconcile sweep is a no-op on a clean cluster).
func TestDeviations_NilForCleanCR(t *testing.T) {
	if devs := Deviations(validStandalone("dev-clean", nil)); len(devs) != 0 {
		t.Errorf("clean CR should have no deviations; got %+v", devs)
	}
}

// TestDeviations_SentinelAggressiveTimeout pins the WarnAggressiveTimeouts
// deviation: a sentinel-mode CR whose downAfterMilliseconds sits in the
// accepted-but-aggressive band [1000, 30000) carries the deviation (the
// durable twin of the below-recommended admission warning, single source of
// truth for both surfaces). Values at or above the recommended 30000ms, and
// sub-1000ms values (rejected by the hard floor in validateSentinelTimingFloors),
// do not.
func TestDeviations_SentinelAggressiveTimeout(t *testing.T) {
	find := func(devs []Deviation, r events.Reason) *Deviation {
		for i := range devs {
			if devs[i].Reason == r {
				return &devs[i]
			}
		}
		return nil
	}

	cases := []struct {
		name      string
		downAfter int32
		want      bool
	}{
		{"sanctioned default 3000 is in-band", 3000, true},
		{"at hard floor 1000 is in-band", 1000, true},
		{"one below recommended", 29999, true},
		{"at recommended 30000 is clean", 30000, false},
		{"above recommended", 45000, false},
		{"sub-floor is rejected, not a deviation", 500, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := sentinelCR("dev-aggro", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.DownAfterMilliseconds = tc.downAfter
			})
			d := find(Deviations(v), events.WarnAggressiveTimeouts)
			switch {
			case tc.want && d == nil:
				t.Fatalf("downAfterMilliseconds=%d must carry WarnAggressiveTimeouts; got none (all: %+v)", tc.downAfter, Deviations(v))
			case !tc.want && d != nil:
				t.Fatalf("downAfterMilliseconds=%d must not carry WarnAggressiveTimeouts; got %q", tc.downAfter, d.Message)
			}
			if tc.want {
				if d.Field != "spec.sentinel.downAfterMilliseconds" {
					t.Errorf("field = %q, want spec.sentinel.downAfterMilliseconds", d.Field)
				}
				if !strings.Contains(d.Message, "30000") {
					t.Errorf("message should name the recommended 30000ms threshold; got %q", d.Message)
				}
			}
		})
	}
}

// TestDeviations_SentinelTimingNotForOtherModes confirms the
// WarnAggressiveTimeouts band only applies to mode=sentinel CRs — other modes
// have no sentinel sub-spec, so an aggressive down-after can't be read.
func TestDeviations_SentinelTimingNotForOtherModes(t *testing.T) {
	v := validStandalone("dev-nonsentinel", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 2
	})
	for _, d := range Deviations(v) {
		if d.Reason == events.WarnAggressiveTimeouts {
			t.Errorf("non-sentinel CR must not carry WarnAggressiveTimeouts; got %q", d.Message)
		}
	}
}
