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

package controller

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyReconcileError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil err returns empty", nil, ""},
		{"phase 1 wrap", fmt.Errorf("phase 1: %w", errors.New("cm apply")), "ConfigMapPhaseError"},
		{"phase 2 wrap", fmt.Errorf("phase 2: %w", errors.New("sts apply")), "STSPhaseError"},
		{"phase 3 wrap", fmt.Errorf("phase 3: %w", errors.New("sentinel infra")), "SentinelInfraPhaseError"},
		{"phase 4 wrap", fmt.Errorf("phase 4: %w", errors.New("pvc resize")), "PVCResizePhaseError"},
		{"phase 5 wrap", fmt.Errorf("phase 5: %w", errors.New("svc apply")), "ServicePhaseError"},
		{"phase 6 wrap", fmt.Errorf("phase 6: %w", errors.New("pdb apply")), "PDBPhaseError"},
		{"phase 7 wrap", fmt.Errorf("phase 7: %w", errors.New("role label")), "RoleLabelPhaseError"},
		{"phase 8 wrap", fmt.Errorf("phase 8: %w", errors.New("readiness gate")), "ReadinessGatePhaseError"},
		{"phase 9 wrap", fmt.Errorf("phase 9: %w", errors.New("pod rollout")), "PodRolloutPhaseError"},
		{"auth secret get failure", errors.New("getting auth secret: nope"), "AuthSecretError"},
		{"finalizer-add failure", errors.New("ensuring pvc-retention finalizer: timeout"), "FinalizerError"},
		{"PVC resize aborted (matches before phase prefix)", errors.New("PVC resize aborted: shrink rejected"), "PVCResizeAborted"},
		{"unknown error catches all", errors.New("something else"), "ReconcileError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyReconcileError(tc.err); got != tc.want {
				t.Errorf("classifyReconcileError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestClassifyReconcileError_BoundedCardinality is the regression
// guard against accidentally letting err.Error() contents leak into
// the failure-counter's `reason` label. The metric's cardinality is
// part of the Prometheus contract — any unbounded label value (like
// a wrapped err message containing namespace / pod / IP) would blow
// up the time series count under churn.
//
// The classifier today returns one of a fixed enum; this test
// asserts that invariant by feeding a random-shape error and
// checking the result is in the known set. If a future change
// adds a `default: return "Error_" + msg` fallback (or any other
// path that incorporates input bytes into the output), this test
// catches it before merge.
func TestClassifyReconcileError_BoundedCardinality(t *testing.T) {
	allowed := map[string]struct{}{
		"":                        {}, // nil err
		"ConfigMapPhaseError":     {},
		"STSPhaseError":           {},
		"SentinelInfraPhaseError": {},
		"PVCResizePhaseError":     {},
		"ServicePhaseError":       {},
		"PDBPhaseError":           {},
		"RoleLabelPhaseError":     {},
		"ReadinessGatePhaseError": {},
		"PodRolloutPhaseError":    {},
		"AuthSecretError":         {},
		"FinalizerError":          {},
		"PVCResizeAborted":        {},
		"ReconcileError":          {},
	}

	// A spread of error shapes that have, at various times, surfaced
	// from real reconcile paths — plus shapes a future contributor
	// might unwittingly inject. None of these should leak into the
	// returned label.
	//
	// Coverage focus: the function uses both `strings.HasPrefix` and
	// `strings.Contains` against fixed literals. The Contains paths
	// (`"auth secret"`, `"finalizer"`, `"PVC resize aborted"`) match
	// errors that may carry user-supplied bytes around the matching
	// substring. The probes below interleave matching substrings with
	// arbitrary user-supplied tokens (namespaces, pod names, secret
	// names, IPs, binary garbage) so that a future change which
	// extends a Contains arm to incorporate the matched bytes into
	// the returned label still trips this test.
	probes := []error{
		nil,
		errors.New(""),
		errors.New("ns=production pod=valkey-prod-0 ip=10.0.0.42 connect: timeout"),
		errors.New("connection refused: 192.168.1.1:6379"),
		fmt.Errorf("phase 99: %w", errors.New("future-phase still wraps but unrecognized prefix")),
		errors.New("phase 1"),                             // matches phase-1 prefix even without colon
		errors.New("auth secret \"my-secret\" not found"), // user-supplied secret name
		errors.New("finalizer add: timeout"),
		errors.New("PVC resize aborted"),
		errors.New("\x00\x01\x02 binary garbage"),
		// User-supplied bytes embedded around / inside the
		// Contains-matched substrings — guards against a future
		// "smarter" classifier that wraps the matched span into the
		// returned label.
		errors.New("getting auth secret \"prod-creds-aws-us-east-2\" from ns/payments: timeout"),
		errors.New("ensuring pvc-retention finalizer on valkey-team-data-0: forbidden by webhook \"deny.policy.example.com\""),
		errors.New("PVC resize aborted: name=team-data ip=10.244.13.42 namespace=user-with-dashes-and-numbers-9999"),
		errors.New("phase 4: %!(NOVERB)pod-prod-0 IP=2001:db8::1 unbounded-token-1234567890abcdef"),
		// Long unbounded-cardinality probe — even if a future change
		// truncates an err.Error() into the label, length alone would
		// still exceed any expected enum value. Test should reject
		// this with the same guard.
		errors.New(string(make([]byte, 8192)) + " auth secret leak"),
	}
	for i, e := range probes {
		got := classifyReconcileError(e)
		if _, ok := allowed[got]; !ok {
			t.Errorf("probe %d: classifyReconcileError(%v) returned %q which is not in the bounded label set; cardinality invariant violated",
				i, e, got)
		}
	}
}
