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
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
)

// DefaultStaleTrackerPruneInterval is the cadence the manager wires
// into StaleTrackerPruner unless an explicit Interval is set. 24h is
// long enough to amortize the cluster-wide list cost yet short enough
// that a missed-delete leak is reclaimed within a working day.
const DefaultStaleTrackerPruneInterval = 24 * time.Hour

// StaleTrackerPruner is a leader-gated periodic sweep that reclaims
// the per-CR tracker state of CRs that no longer exist. The reconciler
// clears its perCR state bag on the observed CR-delete code path
// (Phase 0a NotFound + Phase 0c terminal-deletion); a missed delete
// (operator restart between Get and the cleanup write, watch
// interruption that swallows the delete event) leaves state pinned to
// a CR the operator will never reconcile again. Bounded growth by
// sweep, no per-reconcile cost.
//
// The sweep resets the prunable fields of each vanished CR's
// perCRState (perCRState.pruneStale), tears down its sentinel observer
// (nil-guarded), and reaps every per-CR gauge series via the same
// full-registry ResetReconcileGauges sweep the Reconcile NotFound path
// runs — so a swallowed delete leaks neither the observer goroutine
// tree nor a latched alert-driving series. The prunable fields are the
// edge detectors and digests
// whose loss is harmless because they re-seed / recompute. The
// reconcile mutex and the lifecycle-sensitive trackers (manual-rollout
// audit baseline, failover latch, switch-master edge, auth-password
// cache) are deliberately preserved — dropping a held mutex would race
// a still-running reconcile past serialization, and dropping the
// others on a transient list-staleness could re-fire an audit, re-open
// a failover-strip window, or lose the rotation OLD-password. The
// retained per-entry footprint (the struct shell + those few fields)
// is small, matching the original tolerance for leaked mutex entries.
type StaleTrackerPruner struct {
	Client     client.Client
	Reconciler *ValkeyReconciler
	Interval   time.Duration
}

// Compile-time enforcement.
var _ manager.Runnable = (*StaleTrackerPruner)(nil)
var _ manager.LeaderElectionRunnable = (*StaleTrackerPruner)(nil)

// NeedLeaderElection scopes the sweep to one replica. Each replica
// keeps its own tracker maps in memory, so running on every replica
// would not be incorrect — but it would multiply the cluster-wide
// list cost by the replica count. Leader-only is the cheaper shape;
// the leak window on follower replicas closes when they win the
// lease.
func (p *StaleTrackerPruner) NeedLeaderElection() bool { return true }

// Start runs the periodic sweep until ctx is cancelled. Per the
// controller-runtime Runnable contract, returning nil from Start on
// context cancellation is the expected shutdown signal. A failed
// sweep is logged and retried on the next tick rather than
// propagated — a transient apiserver flake should not stop the
// runnable for the rest of the operator's lifetime.
func (p *StaleTrackerPruner) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("stale-tracker-pruner")
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultStaleTrackerPruneInterval
	}
	logger.Info("starting periodic sweep", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.prune(ctx); err != nil {
				logger.Error(err, "sweep failed (will retry next interval)")
			}
		}
	}
}

// prune lists every Valkey CR cluster-wide, builds a set of live keys,
// and resets the prunable trackers of every perCR entry whose key is
// not in the set (perCRState.pruneStale — see the type docstring for
// what is and isn't reset).
func (p *StaleTrackerPruner) prune(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("stale-tracker-pruner")

	crs := &valkeyv1beta1.ValkeyList{}
	// Cluster-wide list is intentional: the sweep must see every
	// CR in every watched namespace to distinguish "vanished CR"
	// from "CR in a namespace not yet observed". Bounded by the
	// Interval cadence.
	//nolint:velkir-lints // intentional cluster-wide scan, fires once per Interval
	if err := p.Client.List(ctx, crs); err != nil {
		return fmt.Errorf("listing Valkey CRs: %w", err)
	}

	live := make(map[types.NamespacedName]struct{}, len(crs.Items))
	for i := range crs.Items {
		live[types.NamespacedName{Namespace: crs.Items[i].Namespace, Name: crs.Items[i].Name}] = struct{}{}
	}

	pruned := 0
	p.Reconciler.perCR.Range(func(key, val any) bool {
		nn, ok := key.(types.NamespacedName)
		if !ok {
			// Defensive — every entry is keyed by NamespacedName by
			// construction. A non-matching key would be a Go bug;
			// leave it alone rather than corrupt the map.
			return true
		}
		if _, alive := live[nn]; alive {
			return true
		}
		ps, ok := val.(*perCRState)
		if !ok {
			return true
		}
		ps.pruneStale()
		// A swallowed delete also leaves the observer goroutine tree
		// polling freed pod IPs and every per-CR gauge series pinned at
		// its last value (nothing re-Sets them once reconciles stop —
		// a latched valkey_dual_master_observed=1 keeps its critical
		// alert firing against a workload that no longer exists, and
		// paused/resize/observer-connected/exporter-up series linger the
		// same way). ResetReconcileGauges is the same full-registry
		// partial-match sweep the Reconcile NotFound path runs, so a new
		// per-CR metric is reaped here automatically with nothing to
		// keep in sync. Both teardowns are idempotent and
		// self-correcting on a wrongful prune of a live CR: the next
		// reconcile re-Ensures the observer and re-Sets every series.
		p.Reconciler.forgetSentinelObserver(nn)
		operatormetrics.ResetReconcileGauges(nn.Namespace, nn.Name)
		pruned++
		logger.V(1).Info("pruned stale tracker entry", "cr", nn.String())
		return true
	})

	logger.Info("sweep complete", "live_crs", len(live), "pruned", pruned)
	return nil
}
