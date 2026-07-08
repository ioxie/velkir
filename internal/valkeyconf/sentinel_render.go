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
	"fmt"
	"strings"
)

// Sentinel placeholders the init container substitutes from the
// pod's env / Downward API + the per-CR sentinel-bootstrap mount
// before sentinel starts. The constants are the canonical source
// of truth — referenced from both this renderer (where they're
// emitted into the template) and the controller's init-script
// builder (where the substitution happens). A rename in one place
// breaks the unit-test cross-reference, surfacing as a compile or
// test failure rather than a runtime CrashLoopBackOff.
//
// Only the two values that genuinely come from per-pod runtime
// state get a placeholder: the seed master IP (from a live valkey
// pod-0 lookup, written to the per-CR sentinel-bootstrap ConfigMap)
// and the announce-ip (from the Downward API). Master name, quorum,
// and timing values are template-time constants and get folded
// directly into the rendered bytes.
const (
	SentinelSeedMasterIPPlaceholder = "_SEED_MASTER_IP_"
	SentinelAnnounceIPPlaceholder   = "_POD_IP_"
)

// SentinelPort is the canonical sentinel listener port. Mandatory
// in the rendered config; not user-tunable.
const SentinelPort = 26379

// SentinelInputs is the renderer surface for the sentinel.conf
// template. None of the fields carry per-pod runtime values — those
// come from the env-substitution layer at init time. The renderer
// holds the per-CR static shape (master name, quorum, and timing
// values that come from spec.sentinel) and folds them directly into
// the output bytes.
//
// SeedMasterIP is intentionally NOT in this struct: the seed comes
// from the bootstrap ConfigMap that the operator updates per
// reconcile based on a live valkey pod-0 lookup. The renderer emits
// the placeholder; the controller writes the value into a separate
// ConfigMap and the init container substitutes at start time.
type SentinelInputs struct {
	// All fields are template-time constants folded into the literal
	// sentinel.conf bytes the operator writes to <cr>-sentinel-conf.
	// Per-pod runtime values (announce-ip, seed-master-ip) substitute
	// later, at init time.
	MasterName            string
	Quorum                int32
	DownAfterMilliseconds int32
	FailoverTimeout       int32
	ParallelSyncs         int32
}

// RenderSentinel returns the sentinel.conf bytes the operator writes
// to the per-CR <cr>-sentinel-conf ConfigMap. The output contains
// two placeholders the init container must substitute at pod start:
//
//   - _SEED_MASTER_IP_ — read from the per-CR sentinel-bootstrap
//     ConfigMap (operator-maintained from a live valkey pod-0 lookup).
//   - _POD_IP_ — read from the Downward API ($POD_IP).
//
// Static values (master name, quorum, timing) are folded in by the
// renderer so the same bytes apply to every sentinel pod for this CR.
//
// Output is deterministic: same Inputs produce byte-identical bytes.
// The hash that drives sentinel pod-template rollout triggers is
// stable across reconciles when only runtime substitutions change.
func RenderSentinel(in SentinelInputs) string {
	var b strings.Builder
	b.WriteString(sentinelHeaderComment)
	b.WriteString(fmt.Sprintf("port %d\n", SentinelPort))
	// IP-only peer addressing — the resolve-/announce- pair forces
	// sentinel to publish IPs so the operator's pod-IP discovery
	// flow stays load-bearing. (Hostname-mode peer addressing makes
	// announce-ip a no-op and breaks the operator's failover-time
	// label-flip contract.)
	b.WriteString("sentinel announce-hostnames no\n")
	b.WriteString("sentinel resolve-hostnames no\n")
	b.WriteString(fmt.Sprintf("sentinel announce-ip %s\n", SentinelAnnounceIPPlaceholder))
	b.WriteString(fmt.Sprintf("sentinel announce-port %d\n", SentinelPort))

	// SENTINEL MONITOR — the seed IP is filled at init time from the
	// bootstrap ConfigMap. Subsequent sentinels learn the real
	// primary via pubsub; the seed is only the bootstrap pointer.
	b.WriteString(fmt.Sprintf("sentinel monitor %s %s %d %d\n",
		in.MasterName,
		SentinelSeedMasterIPPlaceholder,
		ValkeyPort,
		in.Quorum))

	// Per-master tuning. Each line uses in.MasterName so a future
	// rename of the CR's masterName flows through to every directive
	// without changes here.
	b.WriteString(fmt.Sprintf("sentinel down-after-milliseconds %s %d\n",
		in.MasterName, in.DownAfterMilliseconds))
	b.WriteString(fmt.Sprintf("sentinel failover-timeout %s %d\n",
		in.MasterName, in.FailoverTimeout))
	b.WriteString(fmt.Sprintf("sentinel parallel-syncs %s %d\n",
		in.MasterName, in.ParallelSyncs))

	return b.String()
}

const sentinelHeaderComment = `# Managed by velkir. Do not edit by hand; this ConfigMap is
# overwritten on every reconcile.
#
# Master name, quorum, and timing values are folded by the renderer.
# _SEED_MASTER_IP_ is substituted at pod start from the per-CR
# <cr>-sentinel-bootstrap ConfigMap. _POD_IP_ is substituted from
# the Downward API ($POD_IP) on the init container.
`
