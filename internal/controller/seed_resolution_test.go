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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Seed-resolution pins: the sentinel-bootstrap seed must follow
// quorum-backed knowledge of the primary and never regress to the
// pod-0 fallback mid-incident (which seeded fresh sentinels — and a
// replacement pod-0 — at a brand-new empty master).

const seedTestCR = "vk-seed"

func seedScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 AddToScheme: %v", err)
	}
	return s
}

func seedCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: seedTestCR, Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Sentinel: &valkeyv1beta1.SentinelPodSpec{
				Replicas:   3,
				Quorum:     2,
				MasterName: "mymaster",
			},
		},
	}
}

func seedPod(ordinal int, ip, role string) *corev1.Pod {
	labels := map[string]string{
		CRLabel:        seedTestCR,
		ComponentLabel: componentValkey,
	}
	if role != "" {
		labels[RoleLabel] = role
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", seedTestCR, ordinal),
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "valkey", Image: "x"}}},
		Status: corev1.PodStatus{
			PodIP: ip,
		},
	}
}

func seedSQ(ordinal int, observedPrimary string) *valkeyv1beta1.SentinelQuorum {
	now := metav1.Now()
	reachable := true
	return &valkeyv1beta1.SentinelQuorum{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-sentinel-%d", seedTestCR, ordinal),
			Namespace: "default",
			Labels: map[string]string{
				CRLabel:        seedTestCR,
				ComponentLabel: componentSentinel,
			},
		},
		Status: valkeyv1beta1.SentinelQuorumStatus{
			ObservedPrimary:  observedPrimary,
			QuorumReachable:  &reachable,
			LastObservedTime: &now,
		},
	}
}

func TestSeedMasterIPForCR_LabelledPrimaryWins(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		seedPod(0, "10.1.0.10", ""),
		seedPod(1, "10.1.0.11", roleValuePrimary),
		// SQ majority disagrees — the label is authoritative.
		seedSQ(0, seedTestCR+"-0"),
		seedSQ(1, seedTestCR+"-0"),
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}

	if got := r.seedMasterIPForCR(context.Background(), seedCR()); got != "10.1.0.11" {
		t.Errorf("seed = %q, want the labelled primary's IP 10.1.0.11", got)
	}
}

func TestSeedMasterIPForCR_SQMajorityWhenLabelLost(t *testing.T) {
	// The primary label died with its pod; 2 of 3 fresh sentinel
	// records still name pod-1. The seed must follow the quorum.
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		seedPod(0, "10.1.0.20", ""),
		seedPod(1, "10.1.0.21", ""),
		seedPod(2, "10.1.0.22", ""),
		seedSQ(0, seedTestCR+"-1"),
		seedSQ(1, seedTestCR+"-1"),
		seedSQ(2, ""),
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}

	if got := r.seedMasterIPForCR(context.Background(), seedCR()); got != "10.1.0.21" {
		t.Errorf("seed = %q, want the SQ-majority primary's IP 10.1.0.21", got)
	}
}

func TestSeedMasterIPForCR_NoMajorityMidIncident_Undetermined(t *testing.T) {
	// The mid-incident end state: records exist and are fresh, but two carry
	// empty observations and one names a pod that no longer exists.
	// The resolver must return "" — NOT the pod-0 fallback that would
	// seed an empty replacement master.
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		seedPod(0, "10.1.0.30", ""), // recreated pod-0, no label
		seedPod(1, "10.1.0.31", ""),
		seedSQ(0, ""),
		seedSQ(1, ""),
		seedSQ(2, seedTestCR+"-9"), // names a pod that doesn't exist
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}

	if got := r.seedMasterIPForCR(context.Background(), seedCR()); got != "" {
		t.Errorf("seed = %q, want \"\" (mid-incident must never regress to the pod-0 fallback)", got)
	}
}

func TestSeedMasterIPForCR_TrueBootstrap_Pod0Fallback(t *testing.T) {
	// No SentinelQuorum records at all — the CR is bootstrapping for
	// the first time; the pod-0 fallback applies.
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		seedPod(0, "10.1.0.40", ""),
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}

	if got := r.seedMasterIPForCR(context.Background(), seedCR()); got != "10.1.0.40" {
		t.Errorf("seed = %q, want pod-0's IP 10.1.0.40 at true bootstrap", got)
	}
}

func TestReconcileSentinelBootstrapCM_StickyOnUndetermined(t *testing.T) {
	v := seedCR()
	// A live CR-owned pod still holds the seed IP — the identity check
	// keeps the sticky value only in this case.
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		v, seedPod(1, "10.1.0.50", ""),
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}
	ctx := context.Background()
	cmKey := types.NamespacedName{Namespace: "default", Name: seedTestCR + suffixSentinelBootstrap}

	// Establish a known-good seed.
	if err := r.reconcileSentinelBootstrapCM(ctx, v, "10.1.0.50"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}

	// Undetermined resolution must keep the last-known-good value while
	// it still names a live CR pod.
	if err := r.reconcileSentinelBootstrapCM(ctx, v, ""); err != nil {
		t.Fatalf("sticky apply: %v", err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, cmKey, cm); err != nil {
		t.Fatalf("get CM: %v", err)
	}
	if got := cm.Data[bootstrapSeedMasterIPKey]; got != "10.1.0.50" {
		t.Errorf("seed CM = %q after undetermined pass, want sticky 10.1.0.50", got)
	}

	// A new determination still overwrites.
	if err := r.reconcileSentinelBootstrapCM(ctx, v, "10.1.0.51"); err != nil {
		t.Fatalf("update apply: %v", err)
	}
	if err := c.Get(ctx, cmKey, cm); err != nil {
		t.Fatalf("get CM: %v", err)
	}
	if got := cm.Data[bootstrapSeedMasterIPKey]; got != "10.1.0.51" {
		t.Errorf("seed CM = %q after new determination, want 10.1.0.51", got)
	}
}

// TestReconcileSentinelBootstrapCM_ClearsStaleSeedNotOwnedByCR pins the
// IP-reuse guard: when an undetermined pass finds the existing seed no
// longer maps to any live pod of this CR (the CNI may have recycled the
// IP to a foreign pod), the seed is cleared rather than kept. Keeping it
// could make a replacement pod-0 `replicaof` a foreign pod and flush its
// dataset on sync.
func TestReconcileSentinelBootstrapCM_ClearsStaleSeedNotOwnedByCR(t *testing.T) {
	v := seedCR()
	// No CR pod holds 10.1.0.50 — it's stale / reused elsewhere.
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(
		v, seedPod(0, "10.1.0.99", ""),
	).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}
	ctx := context.Background()
	cmKey := types.NamespacedName{Namespace: "default", Name: seedTestCR + suffixSentinelBootstrap}

	if err := r.reconcileSentinelBootstrapCM(ctx, v, "10.1.0.50"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	if err := r.reconcileSentinelBootstrapCM(ctx, v, ""); err != nil {
		t.Fatalf("undetermined apply: %v", err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, cmKey, cm); err != nil {
		t.Fatalf("get CM: %v", err)
	}
	if got := cm.Data[bootstrapSeedMasterIPKey]; got != "" {
		t.Errorf("seed CM = %q, want cleared (stale IP not owned by this CR)", got)
	}
}

func TestReconcileSentinelBootstrapCM_CreatesEmptyWhenAbsent(t *testing.T) {
	v := seedCR()
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).WithObjects(v).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}
	ctx := context.Background()

	if err := r.reconcileSentinelBootstrapCM(ctx, v, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: seedTestCR + suffixSentinelBootstrap}, cm); err != nil {
		t.Fatalf("the CM must still be created (empty) at first pass: %v", err)
	}
	if got, ok := cm.Data[bootstrapSeedMasterIPKey]; !ok || got != "" {
		t.Errorf("seed CM data = %q (present=%v), want empty value present", got, ok)
	}
}

// --- durable failover-dispatch seed fallback (the un-fixed half) ---

const (
	seedDurableIP   = "10.1.0.30"
	seedDurableAddr = "10.1.0.30:6379"
)

// midIncidentSeedObjects builds the mid-incident topology — records
// exist (two empty, one naming a vanished pod) so no quorum majority
// resolves — plus a live CR-owned pod at seedDurableIP, so
// seedMasterIPForCR reaches the durable-fallback case.
func midIncidentSeedObjects() []client.Object {
	return []client.Object{
		seedPod(0, seedDurableIP, ""),
		seedPod(1, "10.1.0.31", ""),
		seedSQ(0, ""),
		seedSQ(1, ""),
		seedSQ(2, seedTestCR+"-9"), // names a pod that doesn't exist
	}
}

// seedCRWithDispatch returns a sentinel-mode CR carrying a durable
// FailoverDispatch marker (the mid-failover strip record).
func seedCRWithDispatch(preStripAddr string, preStripEpoch int64) *valkeyv1beta1.Valkey {
	v := seedCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
			PreStripAddr:  preStripAddr,
			PreStripEpoch: preStripEpoch,
		},
	}
	return v
}

func stubEpoch(n int64) func(types.NamespacedName) int64 {
	return func(types.NamespacedName) int64 { return n }
}

// TestSeedMasterIP_MidFailoverDurableFallback: with no labelled primary
// and no quorum majority, but a FailoverDispatch marker whose PreStripAddr
// still names a live CR-owned pod and whose epoch is current, the seed is
// that durable address — so a pod rebuilt in the election window boots as
// its replica, not a second master.
func TestSeedMasterIP_MidFailoverDurableFallback(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
		WithObjects(midIncidentSeedObjects()...).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(9)}

	v := seedCRWithDispatch(seedDurableAddr, 9) // epoch == current
	if got := r.seedMasterIPForCR(context.Background(), v); got != seedDurableIP {
		t.Errorf("seed = %q, want the durable PreStripAddr IP %q (marker present, owned, epoch current)", got, seedDurableIP)
	}
}

// TestSeedMasterIP_StaleEpochReResolvesFromQuorum: a marker from a
// superseded failover generation (PreStripEpoch < current) must NOT seed
// its now-stale primary — it falls through to "" so the sticky-CM /
// quorum path re-resolves.
func TestSeedMasterIP_StaleEpochReResolvesFromQuorum(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
		WithObjects(midIncidentSeedObjects()...).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(9)}

	v := seedCRWithDispatch(seedDurableAddr, 5) // 5 < current 9 → superseded
	if got := r.seedMasterIPForCR(context.Background(), v); got != "" {
		t.Errorf("seed = %q, want \"\" (a stale-epoch marker must re-resolve from quorum, not seed a pre-failover primary)", got)
	}
}

// TestSeedMasterIP_DurableFallbackRefusesUnownedAddr: a marker whose
// PreStripAddr names no live CR-owned pod (current epoch) must not be
// seeded — the IP may have been recycled to a foreign pod.
func TestSeedMasterIP_DurableFallbackRefusesUnownedAddr(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
		WithObjects(midIncidentSeedObjects()...).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(9)}

	v := seedCRWithDispatch("10.9.9.9:6379", 9) // epoch current, addr owned by no CR pod
	if got := r.seedMasterIPForCR(context.Background(), v); got != "" {
		t.Errorf("seed = %q, want \"\" (a PreStripAddr that names no live CR pod must not be seeded)", got)
	}
}

// TestSeedMasterIP_EpochFenceEdgeCases pins the epoch fence's
// monotonic-token semantics (matching the in-memory latch): it bites
// ONLY when both epochs are non-zero and the marker's is strictly lower
// (a newer election advanced past it). A zero PreStripEpoch (inert fence)
// or a zero current epoch (observer not republished post-restart — the
// crash window the durable marker exists for) must NOT suppress the seed,
// or split-brain protection would be silently off exactly when needed.
func TestSeedMasterIP_EpochFenceEdgeCases(t *testing.T) {
	cases := []struct {
		name          string
		preStripEpoch int64
		currentEpoch  int64
		wantSeed      bool
	}{
		{"current generation seeds", 9, 9, true},
		{"marker ahead of a regressed observer seeds", 9, 5, true},
		{"strictly-superseded marker re-resolves", 5, 9, false},
		{"inert zero marker epoch seeds (observer present)", 0, 9, true},
		{"observer not yet reseeded post-restart seeds", 9, 0, true},
		{"both epochs unknown seeds off marker+ownership", 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
				WithObjects(midIncidentSeedObjects()...).Build()
			r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(tc.currentEpoch)}
			v := seedCRWithDispatch(seedDurableAddr, tc.preStripEpoch)
			want := ""
			if tc.wantSeed {
				want = seedDurableIP
			}
			if got := r.seedMasterIPForCR(context.Background(), v); got != want {
				t.Errorf("seed = %q, want %q (PreStripEpoch=%d, current=%d)", got, want, tc.preStripEpoch, tc.currentEpoch)
			}
		})
	}
}

// TestSeedMasterIP_DurableFallbackRefusesMalformedAddr: an empty or
// unparseable PreStripAddr must not be seeded (no host to verify).
func TestSeedMasterIP_DurableFallbackRefusesMalformedAddr(t *testing.T) {
	for _, addr := range []string{"", "garbage", "10.1.0.30"} { // empty, no colon, no port
		t.Run("addr="+addr, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
				WithObjects(midIncidentSeedObjects()...).Build()
			r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(9)}
			v := seedCRWithDispatch(addr, 9)
			if got := r.seedMasterIPForCR(context.Background(), v); got != "" {
				t.Errorf("seed = %q, want \"\" (a malformed/empty PreStripAddr must not be seeded)", got)
			}
		})
	}
}

// TestSeedFromFailoverDispatch_FailsClosedOnListError: a pod-list error
// during the ownership check must decline to seed (fail-closed) — the
// address must be positively verified before a pod boots replicaof it.
func TestSeedFromFailoverDispatch_FailsClosedOnListError(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
		WithObjects(seedPod(0, seedDurableIP, "")).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("simulated apiserver list flake")
			},
		}).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme(), currentEpochFn: stubEpoch(9)}

	v := seedCRWithDispatch(seedDurableAddr, 9)
	if got := r.seedFromFailoverDispatch(context.Background(), v); got != "" {
		t.Errorf("got %q, want \"\" (a pod-list error must fail closed — never seed an unverified address)", got)
	}
}

// TestReconcileSentinelBootstrapCM_KeepsSeedOnListError pins the
// fail-OPEN half of seedIPOwnedByCR's split error contract: when the
// pod-ownership list errors during an undetermined pass, the sticky CM
// must KEEP its known-good seed, not wipe it — a transient apiserver
// flake must never reintroduce the empty-seed regression.
func TestReconcileSentinelBootstrapCM_KeepsSeedOnListError(t *testing.T) {
	const keptSeed = "10.1.0.60"
	v := seedCR()
	c := fake.NewClientBuilder().WithScheme(seedScheme(t)).
		WithObjects(v, seedPod(1, keptSeed, "")).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("simulated apiserver list flake")
			},
		}).Build()
	r := &ValkeyReconciler{Client: c, Scheme: c.Scheme()}
	ctx := context.Background()
	cmKey := types.NamespacedName{Namespace: "default", Name: seedTestCR + suffixSentinelBootstrap}

	// Establish a known-good seed (a determined pass — applies, no list).
	if err := r.reconcileSentinelBootstrapCM(ctx, v, keptSeed); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	// Undetermined pass while the pod list errors: the sticky seed must be
	// KEPT (fail-open), not cleared.
	if err := r.reconcileSentinelBootstrapCM(ctx, v, ""); err != nil {
		t.Fatalf("sticky apply under list error: %v", err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, cmKey, cm); err != nil {
		t.Fatalf("get CM: %v", err)
	}
	if got := cm.Data[bootstrapSeedMasterIPKey]; got != keptSeed {
		t.Errorf("seed CM = %q after list-error undetermined pass, want kept %q (a transient flake must not wipe the seed)", got, keptSeed)
	}
}
