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

package orchestration

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Result is the watchdog verdict. Active=false means no watchdog is
// armed (no pod-delete in flight); the reconciler skips the stall
// check entirely. Active=true + Expired=true means the deadline has
// passed without the replacement reaching Ready — the reconciler must
// emit RolloutStalled and transition to Degraded. Active=true +
// Expired=false means the watchdog is armed and the deadline hasn't
// passed yet; the reconciler waits for the next pod-watch event.
type Result struct {
	// Active reflects whether a watchdog is currently armed (the
	// MasterAware substate has both WaitingForPod and Deadline set).
	Active bool

	// Expired reflects whether the deadline has passed. Only meaningful
	// when Active is true; false when Active is false.
	Expired bool

	// PodName names the pod the watchdog is waiting on, for log /
	// event-message context. Empty when Active is false.
	PodName string

	// Deadline is the wall-clock cutoff (copy of status.Deadline) for
	// log / event-message context. Zero-time when Active is false.
	Deadline time.Time
}

// Check evaluates the watchdog substate against now and returns the
// verdict. Pass status=nil to indicate the substate isn't set; the
// helper returns Active=false in that case.
//
// "Active" requires BOTH WaitingForPod (non-empty) AND Deadline (non-nil).
// A status with one but not the other is treated as inactive — defensive
// against a partial-write race during status patch.
func Check(now time.Time, status *valkeyv1beta1.MasterAwareRolloutStatus) Result {
	if status == nil {
		return Result{}
	}
	if status.WaitingForPod == "" || status.Deadline == nil {
		return Result{}
	}
	return Result{
		Active:   true,
		Expired:  !now.Before(status.Deadline.Time),
		PodName:  status.WaitingForPod,
		Deadline: status.Deadline.Time,
	}
}

// Arm builds a MasterAwareRolloutStatus stamped at now for the supplied
// pod name and timeout. The reconciler calls Arm right after it deletes
// a data-plane pod during a rollout step; the returned substate is the
// value to write into status.rollout.masterAware.
//
// Arm is idempotent for the same victim: when existing is already armed
// for podName (same WaitingForPod, deadline set), Arm returns it unchanged
// so a re-arm does NOT push the deadline out. Without this, a reconcile
// storm while the replacement pod is still pending would re-stamp a fresh
// now+timeout deadline every tick and the watchdog could never elapse —
// stall detection would be silently defeated. A nil existing, or one armed
// for a different victim, produces a fresh deadline.
//
// The timeout is clamped to the same bounds the validating webhook
// enforces on spec.rollout.replicaReadyTimeoutSeconds (60s ≤ t ≤ 3600s)
// — defensive against a status-patch path that bypasses the webhook
// (the operator's own writes don't go through validation).
func Arm(now time.Time, podName string, timeoutSeconds int32, existing *valkeyv1beta1.MasterAwareRolloutStatus) *valkeyv1beta1.MasterAwareRolloutStatus {
	if existing != nil && existing.WaitingForPod == podName && existing.Deadline != nil {
		return existing
	}
	const (
		minTimeoutSec = int32(60)
		maxTimeoutSec = int32(3600)
	)
	clamped := min(maxTimeoutSec, max(minTimeoutSec, timeoutSeconds))
	deadline := metav1.NewTime(now.Add(time.Duration(clamped) * time.Second))
	deletedAt := metav1.NewTime(now)
	return &valkeyv1beta1.MasterAwareRolloutStatus{
		WaitingForPod: podName,
		Deadline:      &deadline,
		DeletedAt:     &deletedAt,
	}
}

// Disarm returns the inactive zero-value substate. The reconciler
// writes this when the replacement pod has been observed Ready (so
// the watchdog stops firing) — semantically equivalent to setting
// status.rollout.masterAware = nil, but returning a struct here lets
// the caller distinguish "watchdog ran and was cleared cleanly" from
// "watchdog never armed".
func Disarm() *valkeyv1beta1.MasterAwareRolloutStatus {
	return &valkeyv1beta1.MasterAwareRolloutStatus{}
}
