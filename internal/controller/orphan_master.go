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
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/valkey"
)

// reconcileOrphanMasters demotes any pod whose `velkir.ioxie.dev/role`
// label says `replica` but whose `INFO replication` reports
// `role:master`. The canonical trigger is the dual-kill scenario
// where the operator pod and the active master pod die at the same
// instant: the master is recreated by the StatefulSet with its PVC
// intact, so it starts back up as a master at boot. Phase 7
// labels it `replica` (matching the sentinel-elected new master)
// but never sends `REPLICAOF` to actually demote it. The pod sits
// as an isolated master with divergent data — invisible to
// sentinels, invisible to the Service, but a data-loss hazard if
// it's later elected via failover.
//
// This phase runs after Phase 7 (so role labels are current) and
// before Phase 8 (Phase 8 would otherwise observe the orphan via
// CheckLag, see role=master, treat it as replication-ready, and
// flip its gate True — masking the bug). The detection cost is
// (1 + N) `INFO replication` queries per reconcile, where N =
// replica-labeled pod count (typically 2-4). Bounded.
//
// Behaviour matrix:
//
//   - No elected master (no pod labelled primary): hand off to the
//     bounded dual-master self-heal (reconcileDualMasterSelfHeal),
//     which only acts inside the failover critical section when the
//     sentinel quorum view is unusable and two or more pods report
//     role=master; otherwise it is a no-op and the wedge is left for
//     the Phase 7 NoMasterAgreement guard / observer to recover.
//   - Elected master's INFO does NOT report role=master: SKIP. The
//     primary label is stale or Phase 7's snapshot is wrong; both
//     are deeper bugs that Phase 7 / observeNoMasterAgreement
//     already surface.
//   - Replica's INFO unreachable or fails: SKIP this replica; retry
//     on the next reconcile. The error is logged at V(1); it doesn't
//     fail the reconcile pass (the rest of the cluster is healthy).
//   - Replica's INFO reports role=master AND its MasterReplOffset
//     exceeds the elected master's: emit OrphanMasterDataDivergence
//     Warning event with the byte diff BEFORE issuing REPLICAOF —
//     the diverged writes will be discarded by the resync, and the
//     event is the forensic audit trail.
//   - Replica's INFO reports role=master: issue REPLICAOF
//     <master-ip> <port>. On success, emit OrphanMasterDemoted
//     (Normal); on failure, emit OrphanMasterDemotionFailed
//     (Warning) and continue with the next replica.
//
// Mode-gated: standalone has no replicas, so this is a no-op.
// Replication mode is also covered — the same orphan-master shape
// can occur there (less likely without sentinel-driven failover,
// but not impossible).
func (r *ValkeyReconciler) reconcileOrphanMasters(ctx context.Context, v *valkeyv1beta1.Valkey, pods []corev1.Pod, password string) {
	if v.Spec.Mode == valkeyv1beta1.ModeStandalone {
		return
	}
	logger := log.FromContext(ctx).WithName("orphan-master")

	// pods is the read-only valkey-pod snapshot threaded in from the
	// once-per-reconcile fetch. This phase only reads the role
	// label + PodIP and dials each pod, so the shared snapshot is safe.
	var primary *corev1.Pod
	var replicas []*corev1.Pod
	podsWithIP, pendingPods := 0, 0
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP == "" {
			// A listed pod with no IP (Pending / recreating) cannot be
			// dialed, so any scan below never evaluated it — counted so
			// the coverage verdict treats the sweep as incomplete.
			pendingPods++
			continue
		}
		podsWithIP++
		switch p.Labels[RoleLabel] {
		case roleValuePrimary:
			if primary == nil {
				primary = p
			}
		case roleValueReplica:
			replicas = append(replicas, p)
		}
	}

	if primary == nil {
		if v.Spec.Mode == valkeyv1beta1.ModeReplication {
			// Replication has no sentinel-driven failover, so the sentinel
			// self-heal's failover-in-flight gate is never satisfied here.
			// Surface a no-labeled-primary dual-master split instead —
			// condition + gauge + edge-gated event, no demotion.
			r.observeDualMasterNoPrimary(ctx, v, pods, password)
			return
		}
		// No labeled primary: the orphan-master demotion path has no
		// target. Hand off to the bounded dual-master self-heal, which
		// resolves a genuine dual-master split (≥2 pods running as master,
		// quorum view unusable, failover in flight) and is otherwise a
		// no-op.
		r.reconcileDualMasterSelfHeal(ctx, v, pods, password)
		return
	}
	// A labeled primary exists; any prior split is resolved. Clear the
	// self-heal attempt budget and re-arm the deferral edge so a future
	// split starts with a full budget rather than inheriting a stale
	// cooldown or a suppressed Deferred event.
	if ps, ok := r.stateForIfPresent(types.NamespacedName{Namespace: v.Namespace, Name: v.Name}); ok {
		ps.resetSelfHeal()
		ps.resetDualMasterDeferEdge()
	}
	if len(replicas) == 0 {
		return
	}

	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	issuer := r.ReplicaOfIssuer
	if issuer == nil {
		issuer = &valkey.DialingReplicaOfIssuer{}
	}

	primaryAddr := net.JoinHostPort(primary.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
	primaryState, err := checker.CheckLag(ctx, primaryAddr, password)
	if err != nil {
		logger.V(1).Info("elected primary unreachable; skipping orphan-master scan",
			"cr", v.Namespace+"/"+v.Name, "primary", primary.Name, "err", err.Error())
		return
	}
	if primaryState.Role != valkey.RoleMaster {
		// Elected primary isn't running as master. observeNoMasterAgreement
		// surfaces this elsewhere — skip the orphan scan rather than
		// point orphans at a non-master.
		logger.V(1).Info("elected primary does not report role=master; skipping orphan-master scan",
			"cr", v.Namespace+"/"+v.Name, "primary", primary.Name, "role", primaryState.Role)
		return
	}

	// This scan is a dual-master condition producer too: the labeled
	// primary plus every orphan observed below form the self-reported
	// master set. Demotion is attempted in the same pass, so the stamp
	// usually clears on the next scan; a persistently failing demotion
	// keeps it stamped — which is exactly the state that must surface.
	observedMasters := []string{primary.Name}
	// Coverage: the scan may CLEAR the observation only when it dialed
	// every listed pod cleanly. A replica whose INFO timed out, any pod
	// with an IP this scan never evaluated (unlabeled, or a second
	// primary-labeled pod — Phase 7 relabel can be suppressed during a
	// split), or a listed pod still Pending without an IP (it may boot
	// off an intact PVC as role:master) could itself be a rogue master;
	// clearing on an incomplete scan would flap the condition off and
	// re-fire the event. An incomplete scan that sees <2 masters records
	// no verdict and lets the stamp age out via the freshness window
	// instead.
	coverageComplete := podsWithIP == 1+len(replicas) && pendingPods == 0

	for _, replica := range replicas {
		replicaAddr := net.JoinHostPort(replica.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		state, err := checker.CheckLag(ctx, replicaAddr, password)
		if err != nil {
			logger.V(1).Info("replica unreachable; skipping orphan check this pass",
				"cr", v.Namespace+"/"+v.Name, "pod", replica.Name, "err", err.Error())
			coverageComplete = false
			continue
		}
		if state.Role != valkey.RoleMaster {
			continue
		}
		observedMasters = append(observedMasters, replica.Name)

		// ORPHAN. The pod is labelled replica but is running as
		// master. Surface the data-divergence first, then demote.
		// Compare offsets only when BOTH INFO replies actually carried
		// master_repl_offset: a truncated reply leaves the offset at 0,
		// which would otherwise mask a real divergence (orphan offset
		// missing) or fabricate one (primary offset missing).
		switch {
		case !state.HaveMasterOffset || !primaryState.HaveMasterOffset:
			logger.V(1).Info("orphan divergence not assessed: master_repl_offset absent from an INFO reply",
				"cr", v.Namespace+"/"+v.Name, "orphan", replica.Name,
				"orphanHasOffset", state.HaveMasterOffset, "primaryHasOffset", primaryState.HaveMasterOffset)
		case state.MasterReplOffset > primaryState.MasterReplOffset:
			diff := state.MasterReplOffset - primaryState.MasterReplOffset
			r.recordEventf(v, corev1.EventTypeWarning, string(events.OrphanMasterDataDivergence),
				"OrphanDataDivergence",
				"pod %s (labelled role=replica) reports role=master with master_repl_offset=%d, exceeds elected primary %s offset=%d by %d bytes — these writes will be discarded by the upcoming REPLICAOF resync",
				replica.Name, state.MasterReplOffset, primary.Name, primaryState.MasterReplOffset, diff)
		}

		if err := issuer.IssueReplicaOf(ctx, replicaAddr, password, primary.Status.PodIP, valkey.DefaultPort); err != nil {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.OrphanMasterDemotionFailed),
				"OrphanDemotionFail",
				"REPLICAOF on orphan pod %s failed: %s; will retry next reconcile",
				replica.Name, err.Error())
			logger.Info("REPLICAOF on orphan pod failed",
				"cr", v.Namespace+"/"+v.Name, "pod", replica.Name, "err", err.Error())
			continue
		}
		r.recordEventf(v, corev1.EventTypeNormal, string(events.OrphanMasterDemoted),
			"OrphanDemoted",
			"pod %s (labelled role=replica but reporting role=master) demoted via REPLICAOF %s %d",
			replica.Name, primary.Status.PodIP, valkey.DefaultPort)
	}

	// Dual-master condition producer: >=2 observed self-reported masters
	// stamps the observation (demotion was attempted above — the stamp
	// clears on the next scan once it sticks); a clean, complete scan
	// clears it. Event messaging stays with the OrphanMaster* events
	// emitted above; the gauge is written from updateStatus.
	crKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	ps := r.stateFor(crKey)
	switch {
	case len(observedMasters) >= 2:
		ps.stampDualMasterObserved(observedMasters, time.Now())
	case coverageComplete:
		ps.clearDualMasterObserved()
	}
}

// observeDualMasterNoPrimary surfaces a replication-mode no-labeled-primary
// dual-master split: when no pod carries the role=primary label and two or
// more live pods self-report role=master, it stamps the dual-master
// observation (driving Ready=False + Degraded=DualMasterDivergence + the
// valkey_dual_master_observed gauge) and fires an edge-gated
// DualMasterObserved Warning. It is the replication counterpart of the
// sentinel recovery survey and self-heal scan for the primary==nil case.
//
// Surface-only, no demotion: replication mode has no failover section and
// no fencing epoch, so there is no elected primary to fence a demotion
// against. Auto-demoting a de-facto master here could discard the
// more-advanced data, so the split is surfaced and left to resolve once
// pod-0 rejoins and is labeled primary (the pre-existing labeled-primary
// orphan path, which emits its own divergence event before REPLICAOF) or
// an operator intervenes. No REPLICAOF, no election, no label writes.
//
// Reachability: the bootstrap init points every non-pod-0 pod at pod-0 via
// replicaof, so replicas normally boot role=slave and an ordinary
// no-primary window (pod-0 pending or absent) sees at most one
// self-reported master and never fires — no false positives. Two or more
// self-reported masters with no labeled primary is an externally- or
// manually-induced state (a hand-run REPLICAOF NO ONE, a manually promoted
// replica, a partition survivor) with no other producer in replication
// mode.
//
// Cost: 0..N INFO dials (N = live pods), zero sentinel round-trips, run
// only in the non-steady-state no-labeled-primary window at the normal
// reconcile cadence — the same fan-out the labeled-primary orphan scan
// already makes, not a per-reconcile steady-state cost.
func (r *ValkeyReconciler) observeDualMasterNoPrimary(ctx context.Context, v *valkeyv1beta1.Valkey, pods []corev1.Pod, password string) {
	logger := log.FromContext(ctx).WithName("dual-master-no-primary")
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}

	var masterNames []string
	var details []string
	// Coverage: false when any listed pod went unevaluated — a failed
	// INFO dial, or an IP-less (Pending / recreating) pod the sweep
	// cannot dial at all. Either could itself be a rogue master (a
	// recreated pod-0 boots off its intact PVC as role:master), so an
	// incomplete <2-master sweep records no clear verdict below and the
	// stamp ages out via the freshness window instead of flapping the
	// condition and re-firing the event — mirroring the recovery
	// survey's pendingPods guard.
	coverageComplete := true
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP == "" {
			coverageComplete = false
			continue
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		state, err := checker.CheckLag(ctx, addr, password)
		if err != nil {
			logger.V(1).Info("pod unreachable during no-primary dual-master scan; skipping",
				"cr", cr.String(), "pod", p.Name, "err", err.Error())
			coverageComplete = false
			continue
		}
		if state.Role != valkey.RoleMaster {
			continue
		}
		masterNames = append(masterNames, p.Name)
		offset := "master_repl_offset=unknown"
		if state.HaveMasterOffset {
			offset = fmt.Sprintf("master_repl_offset=%d", state.MasterReplOffset)
		}
		details = append(details, fmt.Sprintf("%s(%s)", p.Name, offset))
	}

	now := time.Now()
	ps := r.stateFor(cr)
	switch {
	case len(masterNames) >= 2:
		ps.stampDualMasterObserved(masterNames, now)
		sort.Strings(details)
		if ps.fireDualMasterObservedEdge(ps.foldDualMasterEventUnion(masterNames)) {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.DualMasterObserved), "DualMasterObserve",
				"pods %s all self-report role:master with no elected primary labelled — active data divergence in replication mode; no demotion is admitted without an elected primary to fence against, so the split persists until pod-0 rejoins as primary or it is resolved manually",
				strings.Join(details, ", "))
		}
	case coverageComplete:
		ps.clearDualMasterObserved()
	}
}

// dualMasterOffsetEpsilon is the master_repl_offset window within which
// two de-facto masters are treated as an ambiguous tie. The two offsets
// are read from two pods in sequence (not atomically), so a difference
// this small can't be trusted to mean one genuinely holds more data — and
// the self-heal must never demote the more-advanced master on a
// coin-flip, so a within-epsilon gap refuses to act.
const dualMasterOffsetEpsilon int64 = 4096

const (
	// dualMasterSelfHealBaseCooldown is the wait before the first retry;
	// it doubles each subsequent attempt (capped at the max backoff) so a
	// split the self-heal can't unwedge doesn't thrash REPLICAOF/CLIENT
	// KILL every reconcile.
	dualMasterSelfHealBaseCooldown = 30 * time.Second
	dualMasterSelfHealMaxBackoff   = 5 * time.Minute
	// dualMasterSelfHealMaxAttempts caps total attempts before the
	// self-heal gives up until a labeled primary reappears (resetSelfHeal).
	dualMasterSelfHealMaxAttempts = 5
	// dualMasterDeferMinInterval is the minimum interval between
	// DualMasterSelfHealDeferred emissions when the deferral SIGNATURE
	// changes (a same-signature deferral never re-fires within an
	// episode regardless). Reconciles in a wedged split run at the 5s
	// readiness cadence, so two alternating deferral reasons (an
	// intermittently-offset-less pod flipping the no-offset and
	// epsilon-tie defers, or an advancing observer epoch varying the
	// epoch fence signature) would otherwise re-fire the Warning every
	// pass — this bounds the producer to ~1 event/min like its
	// siblings.
	dualMasterDeferMinInterval = 60 * time.Second
)

// deFactoMaster is one pod observed reporting role=master during the
// dual-master scan, paired with its dial address and replication offset.
type deFactoMaster struct {
	pod    *corev1.Pod
	addr   string
	offset int64
}

// dualMasterScan is one self-heal pass's pod-scan verdict: the rankable
// de-facto masters, the full observed master-name set (offset or not),
// and the coverage signals the caller's clear/act gates consume.
type dualMasterScan struct {
	masters     []deFactoMaster
	masterNames []string
	sawNoOffset bool
	// coverageComplete is false when any listed pod went unevaluated —
	// a failed INFO dial, or an IP-less (Pending / recreating) pod the
	// scan cannot dial at all. Either could itself be a de-facto master
	// (a recreated pod boots off its intact PVC as role:master), so an
	// incomplete scan records no clear verdict — matching the posture
	// of the other three dual-master producers.
	coverageComplete bool
	// pendingPods counts the listed IP-less pods; any makes the pass
	// refuse to elect a survivor (the un-dialed pod may hold the
	// most-advanced data).
	pendingPods int
}

// scanDeFactoMasters dials every listed pod for the self-heal scan and
// buckets the role:master reports into the scan verdict above. A
// master without a readable master_repl_offset flags sawNoOffset (the
// caller refuses to rank) and never joins the rankable set.
func scanDeFactoMasters(ctx context.Context, checker valkey.LagChecker, pods []corev1.Pod, password string, cr types.NamespacedName) dualMasterScan {
	logger := log.FromContext(ctx).WithName("dual-master-self-heal")
	scan := dualMasterScan{coverageComplete: true}
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP == "" {
			scan.pendingPods++
			scan.coverageComplete = false
			continue
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		state, err := checker.CheckLag(ctx, addr, password)
		if err != nil {
			logger.V(1).Info("pod unreachable during dual-master scan; skipping",
				"cr", cr.String(), "pod", p.Name, "err", err.Error())
			scan.coverageComplete = false
			continue
		}
		if state.Role != valkey.RoleMaster {
			continue
		}
		scan.masterNames = append(scan.masterNames, p.Name)
		if !state.HaveMasterOffset {
			// Without a trustworthy offset the survivor can't be ranked
			// safely — refuse the whole pass rather than risk demoting the
			// more-advanced master.
			scan.sawNoOffset = true
			continue
		}
		scan.masters = append(scan.masters, deFactoMaster{pod: p, addr: addr, offset: state.MasterReplOffset})
	}
	return scan
}

// reconcileDualMasterSelfHeal is Phase 7a's bounded last-resort recovery
// for the dual-master split: no pod carries role=primary, the sentinel
// quorum view is unusable (no fresh agreement, or agreement on a dead
// IP), a failover is in flight, and two or more pods are running as
// master. It elects the unambiguous highest-master_repl_offset survivor
// (refusing a within-epsilon tie), fences the action against the
// failover's PreStripEpoch, and demotes every loser onto the survivor via
// REPLICAOF + CLIENT KILL. It is bounded by a cooldown + exponential
// backoff + max-attempts so an unresolvable split doesn't thrash.
//
// Promote-side boundary: the self-heal ONLY demotes losers. It never
// stamps role=primary on the survivor — that promotion is the job of
// desiredRolesForCR once quorum recovers, gated by the relabel-guard
// floor. Removing the split is safe without quorum; promoting is not.
func (r *ValkeyReconciler) reconcileDualMasterSelfHeal(ctx context.Context, v *valkeyv1beta1.Valkey, pods []corev1.Pod, password string) {
	logger := log.FromContext(ctx).WithName("dual-master-self-heal")
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Gate 1 — only inside the failover critical section. Outside it, a
	// missing primary label is the ordinary transient (STS recreate,
	// pre-bootstrap) the old skip handled; leave it to the observer.
	if !r.IsFailoverInFlight(cr) {
		logger.V(1).Info("no elected primary and no failover in flight; skipping dual-master self-heal",
			"cr", cr.String())
		return
	}
	// Gate 2 — only when sentinels cannot resolve the primary themselves.
	// If the quorum view is usable the observer-driven relabel recovers;
	// the self-heal must not race it.
	if !r.quorumViewUnusable(ctx, v) {
		logger.V(1).Info("no elected primary but sentinel quorum view is usable; deferring to observer relabel",
			"cr", cr.String())
		return
	}

	ps := r.stateFor(cr)
	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}

	// Scan every pod for de-facto masters (INFO reports role=master).
	scan := scanDeFactoMasters(ctx, checker, pods, password, cr)
	// Surface the observation regardless of whether the heal below is
	// admitted: this scan and the Phase 11 recovery survey are the two
	// dual-master condition producers (Ready/Degraded read the stamp
	// freshness-gated in updateStatus). Messaging inside the failover
	// section stays with the Initiated/Deferred events — the stamp
	// deliberately does not consume the DualMasterObserved event edge.
	// Clear only on a complete sweep: a pod whose INFO timed out — or a
	// listed pod with no IP yet — could itself be a rogue master, so an
	// incomplete scan that saw <2 masters records no verdict and lets
	// the stamp age out.
	switch {
	case len(scan.masterNames) >= 2:
		ps.stampDualMasterObserved(scan.masterNames, time.Now())
	case scan.coverageComplete:
		ps.clearDualMasterObserved()
	}
	if scan.pendingPods > 0 {
		// An un-dialed listed pod may itself boot as a de-facto master
		// holding the most-advanced data — a survivor elected without it
		// could demote the wrong master. Refuse to act until every listed
		// pod is dialable; the deferral event fires only when a split is
		// actually visible (an ordinary single-master failover window with
		// a pod still coming up must not page).
		if len(scan.masterNames) >= 2 {
			r.emitDualMasterDeferred(ps, v, "pending",
				"dual-master self-heal deferred: %d listed pod(s) have no IP yet (Pending / recreating); cannot prove the full de-facto master set, refusing to elect a survivor",
				scan.pendingPods)
		}
		return
	}
	if scan.sawNoOffset {
		r.emitDualMasterDeferred(ps, v, "no-offset",
			"dual-master self-heal deferred: a pod reports role=master but INFO carried no master_repl_offset; cannot rank survivors safely")
		return
	}

	// Need at least two de-facto masters for there to be a split to heal.
	masters := scan.masters
	if len(masters) < 2 {
		logger.V(1).Info("fewer than two de-facto masters; no dual-master split to heal",
			"cr", cr.String(), "count", len(masters))
		return
	}

	// Bound: cooldown + exponential backoff + max-attempts.
	now := time.Now()
	if !ps.selfHealAttemptAllowed(now, dualMasterSelfHealBaseCooldown, dualMasterSelfHealMaxBackoff, dualMasterSelfHealMaxAttempts) {
		r.emitDualMasterDeferred(ps, v, "cooldown",
			"dual-master self-heal deferred: attempt budget exhausted or cooling down (max %d attempts, base cooldown %s)",
			dualMasterSelfHealMaxAttempts, dualMasterSelfHealBaseCooldown)
		return
	}

	// Survivor election. Primary key: highest master_repl_offset.
	// Lineage cross-check: on an otherwise-equal key prefer the pod still
	// at the failover's PreStripAddr (the pre-strip primary). Pod name is
	// the final deterministic tie-break.
	preStripAddr, preStripEpoch := failoverDispatchFence(v)
	sort.SliceStable(masters, func(i, j int) bool {
		if masters[i].offset != masters[j].offset {
			return masters[i].offset > masters[j].offset
		}
		li, lj := masters[i].addr == preStripAddr, masters[j].addr == preStripAddr
		if li != lj {
			return li
		}
		return masters[i].pod.Name < masters[j].pod.Name
	})
	survivor, runnerUp := masters[0], masters[1]

	// Refuse a within-epsilon offset tie — never demote the more-advanced
	// master when we can't tell which one that is.
	if survivor.offset-runnerUp.offset <= dualMasterOffsetEpsilon {
		// Edge-gate on the tied pod PAIR, not the live offsets: a steady
		// epsilon-tie split keeps the same two pods while their absolute
		// master_repl_offsets drift upward every reconcile (writes, or the
		// periodic replication PING even when idle). Keying on the offsets
		// would change the signature every pass and re-fire the Warning —
		// the very spam this gate suppresses. Normalize the pair order
		// lexically too: within the epsilon window the two offsets can
		// cross between passes and swap which pod sorts first (survivor vs
		// runnerUp), so an ordered key would still flip on a cross. The
		// live offsets stay as diagnostic detail in the message.
		loName, hiName := survivor.pod.Name, runnerUp.pod.Name
		if loName > hiName {
			loName, hiName = hiName, loName
		}
		r.emitDualMasterDeferred(ps, v, fmt.Sprintf("epsilon:%s:%s", loName, hiName),
			"dual-master self-heal deferred: top two master_repl_offsets within %d bytes (%s=%d, %s=%d) — no unambiguous survivor, refusing to demote the more-advanced master",
			dualMasterOffsetEpsilon, survivor.pod.Name, survivor.offset, runnerUp.pod.Name, runnerUp.offset)
		return
	}

	// Epoch fence — only meaningful when the observer snapshot is itself
	// present + QuorumOK (sentinels agree on a — dead — IP): there
	// snap.Primary.Epoch is a trustworthy config-epoch, and a value below
	// the failover's PreStripEpoch is a stale view of an election this
	// dispatch already superseded. In a genuine quorum-lost split the
	// observer epoch is 0/stale and carries no fencing signal, so applying
	// it would defer the self-heal in exactly the scenario it targets;
	// there the offset election + lineage + the failover-in-flight bound
	// are the safety, not a meaningless epoch.
	if preStripEpoch > 0 && r.SentinelObserver != nil {
		if snap := r.SentinelObserver.Snapshot(cr); snap.Present && snap.Primary.QuorumOK && snap.Primary.Epoch < preStripEpoch {
			r.emitDualMasterDeferred(ps, v, fmt.Sprintf("epoch:%d:%d", snap.Primary.Epoch, preStripEpoch),
				"dual-master self-heal deferred: observer config-epoch %d is below the failover fence %d (stale view)",
				snap.Primary.Epoch, preStripEpoch)
			return
		}
	}

	// Commit. Record the attempt so the cooldown/backoff applies from here,
	// and clear the deferral edge so a later deferral re-emits cleanly.
	ps.recordSelfHealAttempt(now)
	ps.resetDualMasterDeferEdge()
	lineage := "no-prestrip-addr"
	if preStripAddr != "" {
		if survivor.addr == preStripAddr {
			lineage = "survivor-matches-prestrip-addr"
		} else {
			lineage = "survivor-differs-from-prestrip-addr"
		}
	}
	r.recordEventf(v, corev1.EventTypeWarning, string(events.DualMasterSelfHealInitiated),
		"DualMasterSelfHealInitiated",
		"dual-master split: %d de-facto masters; electing survivor %s (master_repl_offset=%d, lineage=%s) and demoting the rest",
		len(masters), survivor.pod.Name, survivor.offset, lineage)

	issuer := r.ReplicaOfIssuer
	if issuer == nil {
		issuer = &valkey.DialingReplicaOfIssuer{}
	}
	killer := r.ClientKillIssuer
	if killer == nil {
		killer = &valkey.DialingClientKillIssuer{}
	}

	// Demote each loser onto the survivor. The survivor is NOT relabeled
	// here; role=primary follows from desiredRolesForCR once quorum
	// recovers.
	for _, loser := range masters[1:] {
		if err := issuer.IssueReplicaOf(ctx, loser.addr, password, survivor.pod.Status.PodIP, valkey.DefaultPort); err != nil {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.OrphanMasterDemotionFailed),
				"OrphanDemotionFail",
				"dual-master self-heal: REPLICAOF on loser %s → survivor %s failed: %s; will retry next reconcile",
				loser.pod.Name, survivor.pod.Name, err.Error())
			logger.Info("REPLICAOF on dual-master loser failed",
				"cr", cr.String(), "loser", loser.pod.Name, "err", err.Error())
			continue
		}
		// Drop pooled writers on the demoted pod so they reconnect via the
		// Service and land on the survivor. Best-effort — a kill failure
		// does not undo the demotion.
		if _, err := killer.KillNormalClients(ctx, loser.addr, password); err != nil {
			logger.V(1).Info("CLIENT KILL on demoted dual-master loser failed",
				"cr", cr.String(), "loser", loser.pod.Name, "err", err.Error())
		}
		r.recordEventf(v, corev1.EventTypeNormal, string(events.DualMasterSelfHealDemoted),
			"DualMasterSelfHealDemoted",
			"dual-master self-heal: demoted loser %s (master_repl_offset=%d) via REPLICAOF onto survivor %s %d",
			loser.pod.Name, loser.offset, survivor.pod.Status.PodIP, valkey.DefaultPort)
	}
}

// failoverDispatchFence returns the durable failover fence
// (PreStripAddr, PreStripEpoch) from the CR status, or ("", 0) when no
// dispatch marker is recorded.
func failoverDispatchFence(v *valkeyv1beta1.Valkey) (string, int64) {
	if v.Status.Rollout == nil || v.Status.Rollout.FailoverDispatch == nil {
		return "", 0
	}
	fd := v.Status.Rollout.FailoverDispatch
	return fd.PreStripAddr, fd.PreStripEpoch
}

// quorumViewUnusable reports whether the sentinel observer cannot give a
// usable live primary: no observer wired, no snapshot, quorum not OK
// (Unknown/Lost), or quorum-OK on an Addr that maps to no live pod
// (NoMasterAgreement). It is the precondition that distinguishes a
// genuine wedge the self-heal must resolve from a transient the observer
// will recover on its own.
func (r *ValkeyReconciler) quorumViewUnusable(ctx context.Context, v *valkeyv1beta1.Valkey) bool {
	if r.SentinelObserver == nil {
		return true
	}
	snap := r.SentinelObserver.Snapshot(types.NamespacedName{Namespace: v.Namespace, Name: v.Name})
	if !snap.Present || !snap.Primary.QuorumOK {
		return true
	}
	return r.observeNoMasterAgreement(ctx, v)
}

// emitDualMasterDeferred emits a DualMasterSelfHealDeferred Warning event
// edge-gated by sig: a sustained identical deferral (a steady
// within-epsilon tie or epoch fence that recurs every reconcile while the
// split persists) emits once per episode rather than re-firing each pass
// and exhausting the Kubernetes per-source event budget. The edge re-arms
// when the deferral reason/signature changes — rate-bounded to one
// emission per dualMasterDeferMinInterval so alternating signatures
// cannot re-fire every pass — when the self-heal acts
// (resetDualMasterDeferEdge on commit), or when a labeled primary
// reappears (resetSelfHeal path).
func (r *ValkeyReconciler) emitDualMasterDeferred(ps *perCRState, v *valkeyv1beta1.Valkey, sig, format string, args ...any) {
	if !ps.fireDualMasterDeferEdge(sig, time.Now()) {
		return
	}
	r.recordEventf(v, corev1.EventTypeWarning, string(events.DualMasterSelfHealDeferred),
		"DualMasterSelfHealDeferred", format, args...)
}
