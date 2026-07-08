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
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/audit"
)

// auditCR is the NamespacedName form used as the audit-row key for a CR.
func auditCR(v *valkeyv1beta1.Valkey) types.NamespacedName {
	return types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
}

// auditRoleLabel emits one EventPodLabelReconciled audit entry for a
// `velkir.ioxie.dev/role` label write the operator just applied. trigger
// classifies the cause: "steady_state" for Phase-7 reconcile stamps,
// "switch-master" for the rollout strip/restore around a failover.
func auditRoleLabel(ctx context.Context, cr types.NamespacedName, pod, from, to, trigger string) {
	audit.Log(ctx, audit.Event{
		Name: audit.EventPodLabelReconciled,
		CR:   cr,
		Attrs: map[string]string{
			"pod":     pod,
			"from":    from,
			"to":      to,
			"trigger": trigger,
		},
	})
}

// auditFailover emits one EventSentinelFailoverIssued audit entry for an
// operator-driven SENTINEL FAILOVER on the primary-rollout path.
// oldPrimary is the stripped (outgoing) primary pod; newPrimary is the
// promotion target; rolloutID is the target STS revision the rollout
// converges to (omitted when empty).
func auditFailover(ctx context.Context, cr types.NamespacedName, oldPrimary, newPrimary, rolloutID string) {
	attrs := map[string]string{
		"old_primary": oldPrimary,
		"new_primary": newPrimary,
		"reason":      "rollout",
	}
	if rolloutID != "" {
		attrs["rollout_id"] = rolloutID
	}
	audit.Log(ctx, audit.Event{
		Name:  audit.EventSentinelFailoverIssued,
		CR:    cr,
		Attrs: attrs,
	})
}

// auditReconciliationPaused emits one EventReconciliationPaused audit
// entry on the transition into the paused state. The caller owns the
// edge guard (only the first paused reconcile reaches here).
func auditReconciliationPaused(ctx context.Context, cr types.NamespacedName, generation int64) {
	audit.Log(ctx, audit.Event{
		Name: audit.EventReconciliationPaused,
		CR:   cr,
		Attrs: map[string]string{
			"generation": strconv.FormatInt(generation, 10),
		},
	})
}

// auditSentinelReset emits one EventSentinelResetIssued audit entry for a
// SENTINEL RESET / REMOVE the operator dispatched. reason is one of
// pod_replaced | cold_start | degraded. targets is the set of sentinel
// pods the reset acted on, rendered as a sorted comma-joined list so the
// audit line is deterministic across runs.
func auditSentinelReset(ctx context.Context, cr types.NamespacedName, targets []string, reason string) {
	audit.Log(ctx, audit.Event{
		Name: audit.EventSentinelResetIssued,
		CR:   cr,
		Attrs: map[string]string{
			"targets": joinSortedTargets(targets),
			"reason":  reason,
		},
	})
}

// joinSortedTargets renders a target set deterministically (sorted, then
// comma-joined) so identical reset dispatches produce byte-identical
// audit lines regardless of slice ordering.
func joinSortedTargets(targets []string) string {
	out := append([]string(nil), targets...)
	sort.Strings(out)
	return strings.Join(out, ",")
}

// manualRolloutState carries the per-CR last-observed value of the
// manual-rollout annotation, used to fire EventManualRolloutTriggered
// exactly once per value change rather than on every reconcile that
// merely observes the annotation present.
type manualRolloutState struct {
	mu   sync.Mutex
	last string
}

// maybeAuditManualRollout emits EventManualRolloutTriggered when the
// `velkir.ioxie.dev/rollout-generation` annotation value changes from the
// value this process last observed for the CR.
//
// The first observation of a CR seeds the baseline WITHOUT emitting: a CR
// created with the annotation already set — or the first reconcile after
// an operator restart — is not itself a fresh manual-rollout trigger.
// This matches the seed-on-first-observation semantics of the other
// per-CR edge trackers in this reconciler (rolloutTriggerStates,
// quorumState), at the cost of not auditing a bump that lands entirely
// within an operator-downtime window — an acceptable gap for a detective
// control. Clearing the annotation (non-empty → empty) is not a trigger.
func (r *ValkeyReconciler) maybeAuditManualRollout(ctx context.Context, v *valkeyv1beta1.Valkey) {
	cur := v.Annotations[ManualRolloutAnnotation]
	key := auditCR(v)
	st, loaded := r.stateFor(key).manualRolloutTracker(cur)
	if !loaded {
		return // first observation — baseline seeded, no emission
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if cur == st.last {
		return
	}
	prev := st.last
	st.last = cur
	if cur == "" {
		return // annotation cleared — not a rollout trigger
	}
	audit.Log(ctx, audit.Event{
		Name: audit.EventManualRolloutTriggered,
		CR:   key,
		Attrs: map[string]string{
			"generation":          cur,
			"previous_generation": prev,
		},
	})
}
