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

package valkeyconf

import (
	"strings"
	"testing"
)

func TestRenderSentinel_BasicShape(t *testing.T) {
	out := RenderSentinel(SentinelInputs{
		MasterName:            "mymaster",
		Quorum:                2,
		DownAfterMilliseconds: 30000,
		FailoverTimeout:       180000,
		ParallelSyncs:         1,
	})

	for _, want := range []string{
		"port 26379",
		"sentinel announce-hostnames no",
		"sentinel resolve-hostnames no",
		"sentinel announce-ip _POD_IP_",
		"sentinel announce-port 26379",
		"sentinel monitor mymaster _SEED_MASTER_IP_ 6379 2",
		"sentinel down-after-milliseconds mymaster 30000",
		"sentinel failover-timeout mymaster 180000",
		"sentinel parallel-syncs mymaster 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered config missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestRenderSentinel_Deterministic(t *testing.T) {
	in := SentinelInputs{MasterName: "x", Quorum: 3, DownAfterMilliseconds: 30000, FailoverTimeout: 180000, ParallelSyncs: 1}
	a := RenderSentinel(in)
	b := RenderSentinel(in)
	if a != b {
		t.Fatalf("RenderSentinel must be deterministic")
	}
}

func TestRenderSentinel_QuorumFolded(t *testing.T) {
	// Quorum is fold-time, not init-time — the literal 5 must appear
	// after the SEED_MASTER_IP placeholder.
	out := RenderSentinel(SentinelInputs{MasterName: "m", Quorum: 5})
	if !strings.Contains(out, "_SEED_MASTER_IP_ 6379 5") {
		t.Errorf("quorum 5 not folded into monitor line: %q", out)
	}
}

func TestRenderSentinel_MasterNameFolded(t *testing.T) {
	// MasterName is fold-time — it must appear literally in every
	// directive that references the master, not as a placeholder. A
	// regression that re-introduced placeholder substitution would
	// leak an unsubstituted token into the running config.
	out := RenderSentinel(SentinelInputs{
		MasterName:            "production-cache",
		Quorum:                3,
		DownAfterMilliseconds: 30000,
		FailoverTimeout:       180000,
		ParallelSyncs:         1,
	})
	for _, want := range []string{
		"sentinel monitor production-cache ",
		"sentinel down-after-milliseconds production-cache ",
		"sentinel failover-timeout production-cache ",
		"sentinel parallel-syncs production-cache ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("master name not folded into %q\nfull output:\n%s", want, out)
		}
	}
	// Defense-in-depth: the old _MASTER_NAME_ token must NOT appear
	// anywhere in the output.
	if strings.Contains(out, "_MASTER_NAME_") {
		t.Errorf("renderer leaked _MASTER_NAME_ placeholder into output:\n%s", out)
	}
}

func TestRenderSentinel_NoHostnameLeak(t *testing.T) {
	// Webhook rejects user-set sentinel announce-hostnames, but the
	// renderer is the second line of defense: render must never emit
	// the string `announce-hostnames yes` regardless of inputs.
	out := RenderSentinel(SentinelInputs{MasterName: "m", Quorum: 2})
	if strings.Contains(out, "announce-hostnames yes") {
		t.Fatal("renderer must never emit announce-hostnames=yes")
	}
	if strings.Contains(out, "resolve-hostnames yes") {
		t.Fatal("renderer must never emit resolve-hostnames=yes")
	}
}

func TestRenderSentinel_PlaceholderConstantsUsed(t *testing.T) {
	// The renderer must reference the package-level placeholder
	// constants so a rename in one place breaks compile here. Verify
	// every emitted placeholder appears in the output.
	out := RenderSentinel(SentinelInputs{MasterName: "m", Quorum: 2})
	for _, p := range []string{
		SentinelSeedMasterIPPlaceholder,
		SentinelAnnounceIPPlaceholder,
	} {
		if !strings.Contains(out, p) {
			t.Errorf("placeholder %q not present in output", p)
		}
	}
}
