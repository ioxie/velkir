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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

// TestUpdateStatus_LinkupStuck_SurfacesThenAgesOut pins the controller
// seam for the new Degraded condition: a stuck per-CR no-progress
// tracker drives Degraded=True/SentinelPeerLinkupStuck through a real
// updateStatus call, and once the freshness-gated read stales, the same
// call expires it and the reason clears. Mirrors the MasterLost seam
// test — a freshness-gate mis-wire or a dropped field would fail here.
func TestUpdateStatus_LinkupStuck_SurfacesThenAgesOut(t *testing.T) {
	clk := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	cr := sentinelCRForMasterLost()
	c := fake.NewClientBuilder().WithScheme(authRotationScheme(t)).
		WithObjects(cr).WithStatusSubresource(cr).Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(16),
		nowFunc:  func() time.Time { return clk },
	}
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cr)

	// Drive the per-CR tracker to stuck at clk.
	st := r.stateFor(key).quorumTracker()
	for range strandedSurgeryStuckThreshold {
		st.detectStrandedPeerLinkupStuck([]string{"10.0.0.1:26379"}, nil, nil, clk)
	}

	degraded := func() metav1.Condition {
		got := &valkeyv1beta1.Valkey{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatalf("get cr: %v", err)
		}
		cond := meta.FindStatusCondition(got.Status.Conditions, orchestration.TypeDegraded)
		if cond == nil {
			t.Fatalf("Degraded condition not found")
		}
		return *cond
	}
	callUpdate := func() {
		if err := r.updateStatus(ctx, cr, healthySTS3(), nil, false,
			orchestration.Result{}, sqaggregate.Result{}, false, false); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}
	}

	callUpdate()
	if got := degraded(); got.Status != metav1.ConditionTrue || got.Reason != orchestration.ReasonSentinelPeerLinkupStuck {
		t.Fatalf("Degraded = %s/%s; want True/%s", got.Status, got.Reason, orchestration.ReasonSentinelPeerLinkupStuck)
	}

	// Advance past the freshness window with no fresh surgery: the read
	// expires the flag and the reason clears.
	clk = clk.Add(strandedLinkupStuckFreshnessWindow + time.Second)
	callUpdate()
	if got := degraded(); got.Reason == orchestration.ReasonSentinelPeerLinkupStuck {
		t.Errorf("Degraded still SentinelPeerLinkupStuck after the flag aged out")
	}
	if st.strandedLinkupStuck {
		t.Errorf("the updateStatus read must have expired the stale flag")
	}
}
