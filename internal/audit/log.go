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

// Package audit emits structured audit-log lines for operator
// actions that change Valkey data-plane state for reasons OTHER
// than normal spec reconciliation. Stream consumers (cluster log
// shippers, compliance auditors, incident-review queries) can
// filter on `event=<name>` and `requestor=<user>` to reconstruct
// the chain of who-did-what at any moment.
//
// The kube-apiserver's audit log already captures who initiated an
// admission request; this package covers what the operator did as
// a consequence — closing the gap between "user set annotation X"
// and "operator did Y because of it".
//
// Emission is at V(0) on the controller-runtime logger (always
// emitted regardless of verbosity). Callers MUST use one of the
// `Event*` constants for the Name field — the catalog is the
// canonical list of allowed event names; unknown names are dropped
// at log time with an Info-level emission tagged
// `audit: dropping unknown event` so stream consumers can grep
// for typos at run time. (controller-runtime's logr API has Info
// and Error tiers only — no separate Warning level — and a typo
// in the Name argument is not an error condition the calling
// goroutine should react to.)
//
// Per-line integrity is NOT signed: lines are not HMAC'd or signed
// by the operator. The audit stream is a detective control whose
// integrity rests on a write-once log shipper plus cross-checks
// against the apiserver's own audit log. See
// docs/security/audit-log-integrity.md for the threat model and
// the keyed-HMAC retrofit guidance for compliance regimes that
// require per-emission non-repudiation.
package audit

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Event names — every audit.Log call MUST use one of these. The
// `AllEvents` slice below carries the full set so tests + future
// lint analyzers can cross-check call sites against this canonical
// list, and so the init-time sanity check below catches accidental
// duplicate constants.
const (
	// EventReconciliationPaused fires when a CR carries the
	// `velkir.ioxie.dev/paused=true` annotation and the reconciler
	// short-circuits. Attrs: generation.
	EventReconciliationPaused = "reconciliation_paused"

	// EventManualRolloutTriggered fires when a user sets
	// `velkir.ioxie.dev/rollout-generation=N` to force a rollout
	// outside the spec-driven path. Attrs: generation,
	// previous_generation.
	EventManualRolloutTriggered = "manual_rollout_triggered"

	// EventPVCLossAccepted fires when the operator consumes the
	// `velkir.ioxie.dev/accept-pvc-loss=true` annotation that opts
	// into STS+PVC-loss recovery. Attrs: (none beyond the base set).
	EventPVCLossAccepted = "pvc_loss_accepted"

	// EventAggressiveTimeoutsAccepted fires when the webhook admits
	// a CR carrying `velkir.ioxie.dev/allow-aggressive-timeouts=true`
	// with sub-floor down-after / failover-timeout. Attrs:
	// down_after_ms, failover_timeout_ms.
	EventAggressiveTimeoutsAccepted = "aggressive_timeouts_accepted"

	// EventForceScaleAccepted fires when v1beta1+ admits a
	// `velkir.ioxie.dev/force-scale=true` scale-down past the safety
	// floor. Attrs: from, to.
	EventForceScaleAccepted = "force_scale_accepted"

	// EventSentinelFailoverIssued fires when the operator issues
	// `SENTINEL FAILOVER` (rollout / preStop / manual paths).
	// Attrs: old_primary, reason (rollout|preStop|manual),
	// rollout_id.
	EventSentinelFailoverIssued = "sentinel_failover_issued"

	// EventSentinelResetIssued fires when the operator issues
	// `SENTINEL RESET *`. Attrs: targets, reason
	// (pod_replaced|cold_start|degraded).
	EventSentinelResetIssued = "sentinel_reset_issued"

	// EventAuthSecretRotated fires when an auth-Secret hot-rotation
	// orchestration reaches a terminal state — succeeded, failed (all
	// pods reverted to the old credential), or partial (cluster left
	// in mixed-credential state). One emission per terminal
	// transition, mirroring the SecretRotated / SecretRotationFailed /
	// SecretRotationPartial Kubernetes Events.
	//
	// Attrs:
	//
	//   secret  — namespace/name of the auth Secret
	//   pre_hash, post_hash  — 12-char hex prefix of the SHA-256 of
	//     the password content before/after the rotation. Matches the
	//     Status.Rollout.AuthRotation.ObservedSecretHash prefix. On
	//     outcome=failed and outcome=partial, post_hash equals
	//     pre_hash because the cluster's ObservedSecretHash does not
	//     advance — for failed the cluster is back on the old
	//     credential; for partial the operator cannot speak for the
	//     mixed state.
	//   outcome  — succeeded | failed | partial
	//   pods_on_new_credential  — count of data-plane pods carrying
	//     the NEW credential at terminal time
	//   pods_on_old_credential  — count of data-plane pods carrying
	//     the OLD credential at terminal time (zero unless the revert
	//     path ran)
	//
	// The attr names are state-at-terminal-time, NOT "operations
	// performed". A compliance query like
	//   outcome != succeeded AND pods_on_new_credential > 0
	// finds the partial-state clusters that need operator
	// intervention; that shape is exactly what the names invite.
	//
	// Per-outcome state-table (data-plane only; sentinel plane
	// out-of-scope, see below):
	//
	//   succeeded → pods_on_new_credential = N (every replica + master),
	//               pods_on_old_credential = 0
	//   failed    → pods_on_new_credential = 0,
	//               pods_on_old_credential = K (revert moved them all back)
	//   partial   → pods_on_new_credential = R (revert failed on these,
	//                 stuck on new),
	//               pods_on_old_credential = K - R (revert succeeded,
	//                 back on old)
	//
	// Sentinel-plane rotation lives outside this event today
	// (delegated to the existing SENTINEL SET auth-pass flow);
	// the attrs cover the data-plane orchestrator only.
	EventAuthSecretRotated = "auth_secret_rotated"

	// EventFinalizerRemovedExternally fires when a reconcile
	// observes a finalizer the operator owned has been stripped
	// outside the operator's flow (human force-flow via kubectl
	// patch). Attrs: last_observed_finalizer.
	EventFinalizerRemovedExternally = "finalizer_removed_externally"

	// EventPodLabelReconciled fires on every `velkir.ioxie.dev/role`
	// label flip the operator drives — bootstrap-stamping, observer-
	// driven failover, or role transition during rolling updates.
	// Attrs: pod, from, to, trigger (+switch-master|steady_state).
	EventPodLabelReconciled = "pod_label_reconciled"

	// EventAdmissionRejected fires from the validating webhook
	// itself (not the reconciler) on every CR rejection. Attrs:
	// field, reason.
	EventAdmissionRejected = "admission_rejected"

	// EventDeviationAccepted fires when the webhook admits a CR
	// whose value for a soft-warn field deviates from the operator-
	// derived default (PDBTooPermissive, RolloutFragileQuorum,
	// etc.). Attrs: reason, value, default.
	EventDeviationAccepted = "deviation_accepted"

	// EventClientsKilled fires when the operator issues
	// `CLIENT KILL TYPE normal SKIPME yes` against a pod it just
	// demoted from primary to replica, severing live write-pool
	// connections so clients reconnect through the Service and land
	// on the new primary. It is a destructive data-plane action — it
	// terminates connections the operator did not open — so it belongs
	// in the audit trail alongside failover/RESET/rotation, not only in
	// the Kubernetes Event stream. One emission per pod actually acted
	// on (count > 0). Attrs: pod, count (connections dropped), reason
	// (today always primary_demotion).
	EventClientsKilled = "clients_killed"
)

// AllEvents is the canonical catalog of allowed event names. Every
// emission site MUST use a constant from this slice; the future
// lint plugin walks `internal/` AST and rejects calls whose Name
// argument is a foreign string literal. The slice is sorted at
// init() so binary-search lookup in `IsKnown` is constant-stable
// across additions.
var AllEvents = []string{
	EventReconciliationPaused,
	EventManualRolloutTriggered,
	EventPVCLossAccepted,
	EventAggressiveTimeoutsAccepted,
	EventForceScaleAccepted,
	EventSentinelFailoverIssued,
	EventSentinelResetIssued,
	EventAuthSecretRotated,
	EventFinalizerRemovedExternally,
	EventPodLabelReconciled,
	EventAdmissionRejected,
	EventDeviationAccepted,
	EventClientsKilled,
}

func init() {
	sort.Strings(AllEvents)
	// Sanity-check: no duplicate event constants. A duplicate would
	// silently shadow one entry in the catalog and break the lint
	// plugin's "set of emitted names matches catalog 1:1" property.
	for i := 1; i < len(AllEvents); i++ {
		if AllEvents[i] == AllEvents[i-1] {
			panic("audit: duplicate event constant in AllEvents: " + AllEvents[i])
		}
	}
}

// IsKnown reports whether name is in the canonical catalog. O(log
// n) via sort.SearchStrings on the init-sorted slice.
func IsKnown(name string) bool {
	i := sort.SearchStrings(AllEvents, name)
	return i < len(AllEvents) && AllEvents[i] == name
}

// Event captures one audit-trail entry. The required-fields
// contract is enforced by Log:
//
//   - Name must be one of the Event* constants (IsKnown returns true).
//   - CR must carry both Namespace and Name (the audit row is
//     keyed on the CR identity for compliance review).
//   - Requestor may be empty when the operator-side actor is the
//     reconciler itself rather than a user-driven admission flow
//     (e.g., `sentinel_reset_issued` from the stranded-sentinel
//     recovery pass); in that case Requestor is logged as
//     "operator:reconciler".
//   - Attrs is free-form key/value attached to the log line.
//     Order-stable: the keys are sorted at log time so the rendered
//     line is deterministic across runs (helpful for log-pipeline
//     dedup and golden-test comparisons).
type Event struct {
	Name      string
	CR        types.NamespacedName
	Requestor string
	Attrs     map[string]string
}

// Log writes evt to the operator log stream at V(0). The shape is
// always:
//
//	event=<name> cr=<ns>/<name> requestor=<user> <attr-key>=<attr-value>...
//
// rendered via controller-runtime's logr.Logger so the stream
// consumer (kubernetes-mixin loki query, ElasticSearch ingest
// pipeline, etc.) sees a structured key/value document with a
// stable schema.
//
// Unknown event names (Name not in AllEvents) are dropped at V(0)
// with the message `audit: dropping unknown event` and a `name=`
// key carrying the offending string. The behaviour is fail-open
// rather than panic so a typo in a fresh emission site doesn't
// crash the operator on first hit; the lint plugin (or the
// package's own go-vet equivalent — TBD) catches the typo at
// build time.
func Log(ctx context.Context, evt Event) {
	logger := log.FromContext(ctx).WithName("audit")
	if !IsKnown(evt.Name) {
		logger.Info("audit: dropping unknown event", "name", evt.Name, "cr", evt.CR.String())
		return
	}
	requestor := evt.Requestor
	if requestor == "" {
		requestor = "operator:reconciler"
	}
	keysAndValues := []any{
		"event", evt.Name,
		"cr", evt.CR.String(),
		"requestor", requestor,
	}
	// Sorted attr keys so the log line is deterministic across
	// runs (the `Attrs` map iteration order is randomised by the
	// Go runtime).
	if len(evt.Attrs) > 0 {
		keys := make([]string, 0, len(evt.Attrs))
		for k := range evt.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			keysAndValues = append(keysAndValues, k, evt.Attrs[k])
		}
	}
	logger.Info("audit", keysAndValues...)
}
