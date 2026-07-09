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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"
	k8sevents "k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/audit"
	"github.com/ioxie/velkir/internal/defaults"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/logging"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/sqaggregate"
	"github.com/ioxie/velkir/internal/util/ssa"
	"github.com/ioxie/velkir/internal/valkey"
	"github.com/ioxie/velkir/internal/valkeyconf"
	"github.com/ioxie/velkir/internal/version"
)

const (
	// PauseAnnotation halts reconciliation for the CR while present and set
	// to "true". The reconciler still updates the per-CR paused gauge so an
	// operator can spot stuck pauses on a dashboard, but no spec or status
	// mutations happen.
	PauseAnnotation = "velkir.ioxie.dev/paused"

	// AcceptPVCLossAnnotation is consumed once by the reconciler when a CR
	// recovery path needs an explicit user opt-in to accept missing PVCs.
	// Single-shot: stripped after a successful reconcile.
	AcceptPVCLossAnnotation = "velkir.ioxie.dev/accept-pvc-loss"

	// ForceRotateAnnotation triggers webhook-cert rotation; consumed by the
	// cert subsystem and listed here so Phase 0 strips it after success.
	ForceRotateAnnotation = "velkir.ioxie.dev/force-rotate"

	// ManualRolloutAnnotation lets a user trigger a rollout without a
	// spec change (e.g. to pick up a refreshed sidecar image whose tag
	// hasn't changed, or to recover from a stalled bootstrap). The
	// value is opaque — bumping it (any non-empty string differing
	// from the previously-projected value) drives the STS pod-
	// template hash forward, which trips the rollout-trigger edge
	// through the standard pathway. The annotation persists on the CR
	// (NOT in singleShotAnnotations); re-bumping the value triggers
	// another rollout, matching kubectl-rollout-restart semantics.
	//
	// Compliance audit-log emission uses
	// `audit.EventManualRolloutTriggered`; see
	// internal/audit/log.go for the canonical event name.
	ManualRolloutAnnotation = "velkir.ioxie.dev/rollout-generation"

	// PVCRetentionFinalizer makes the apiserver block GC of a deleting CR
	// until the operator runs reconcileDeletion and removes the finalizer.
	// Without it the policy in spec.pvcRetentionPolicy was best-effort —
	// honoured only when the operator was reachable at the moment K8s
	// processed the deletionTimestamp. The finalizer turns the policy into
	// a hard guarantee: K8s won't drop the CR (and won't fire owner-ref
	// cascade GC on the PVCs) until the operator has applied the policy
	// and signalled completion by stripping the finalizer.
	PVCRetentionFinalizer = "velkir.ioxie.dev/pvc-retention"

	// CRLabel ties an owned resource back to its CR. Aliased to the
	// canonical key in internal/defaults so the anti-affinity selector
	// stamped there (against ownedLabels pods) can't drift from the
	// labels the controller writes.
	CRLabel = defaults.CRLabelKey
	// ComponentLabel marks the component (`valkey` / `sentinel`).
	ComponentLabel = defaults.ComponentLabelKey
	// RoleLabel marks the data-plane role of a valkey pod. Two values:
	// `primary` (the writeable pod, exactly one in any healthy
	// replication / sentinel cluster) and `replica` (read-only
	// follower). The label is operator-owned — the validating webhook
	// rejects user attempts to set it via spec.valkey.podLabels — and
	// feeds the `<cr>-ro` Service selector.
	RoleLabel        = "velkir.ioxie.dev/role"
	roleValuePrimary = "primary"
	roleValueReplica = "replica"
	// roleLabelUnset is the display value used in logs/events/audit when a
	// pod carries no role label (pre-bootstrap, or the strip window during
	// a failover).
	roleLabelUnset = "<unset>"

	// podIndexLabel is the standard Kubernetes label the StatefulSet
	// controller stamps on every pod it creates (the pod-index label,
	// on by default since K8s 1.28). The value is the pod's ordinal as
	// a decimal string — `"0"` for ordinal-0, `"1"` for ordinal-1, etc.
	// Bootstrap-rule role assignment and the rollout victim sort prefer
	// this label over name-suffix matching so a future StatefulSet
	// improvement that decouples pod names from ordinals doesn't
	// silently break ordinal detection. The K8s 1.30 floor guarantees
	// the label's presence in every supported version.
	podIndexLabel = "apps.kubernetes.io/pod-index"

	// ReplicationReadyGate is the readiness-gate condition the operator
	// stamps on non-standalone valkey pods (when ReadinessGate.Enabled
	// is true, default). Phase 8 patches the matching condition on
	// pods/status: True for the primary (ready by definition), True
	// for replicas whose `master_link_status:up` AND `lag <
	// ReadinessGate.MaxLagBytes`, False otherwise. The gate keeps
	// not-yet-caught-up replicas out of the `<cr>-ro` Service's
	// endpoint set so reads don't hit a stale view.
	ReplicationReadyGate corev1.PodConditionType = "velkir.ioxie.dev/replication-ready"
	// ManagedByLabel and ManagedByValue feed the operator's informer cache
	// label-selector filter.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value of the managed-by label.
	ManagedByValue = "velkir"
	// AppNameLabel is the standard k8s app.kubernetes.io/name.
	AppNameLabel = "app.kubernetes.io/name"
	// AppInstanceLabel is the standard k8s app.kubernetes.io/instance.
	AppInstanceLabel = "app.kubernetes.io/instance"
	// AppComponentLabel is the standard k8s app.kubernetes.io/component.
	AppComponentLabel = "app.kubernetes.io/component"
	// AppPartOfLabel is the standard k8s app.kubernetes.io/part-of.
	AppPartOfLabel = "app.kubernetes.io/part-of"

	// ConfigHashAnnotation lets pod-template hashing pick up ConfigMap changes.
	ConfigHashAnnotation = "velkir.ioxie.dev/config-hash"

	componentValkey = "valkey"
	// componentSentinel labels every sentinel pod the operator owns.
	// Reconciler triggers + the startup safety-net runnable list pods
	// by this selector; trigger paths no-op until pods exist.
	componentSentinel = "sentinel"

	// stsRevisionLabel is the StatefulSet controller-revision-hash label
	// the controller reads to tell a pod's revision from its STS's target
	// revision. Single source for the whole package (derive_state,
	// primary_rollout, the sentinel-roll partition walk).
	stsRevisionLabel = appsv1.StatefulSetRevisionLabel

	defaultValkeyPort   int32 = 6379
	defaultExporterPort int32 = 9121
	// defaultSentinelPort is the canonical sentinel client port used
	// by the reconciler triggers and the startup safety-net runnable
	// to compute Endpoint.Addr from pod IPs.
	defaultSentinelPort int32 = 26379

	pausedRequeueAfter        = 5 * time.Minute
	authSecretMissingRequeue  = 30 * time.Second
	pvcLossGateRequeue        = 30 * time.Second
	terminationGracePeriodVal = int64(60)
	// sentinelTerminationGracePeriodVal is shorter than the valkey
	// budget: sentinels run no master-failover preStop, so the 60s the
	// valkey pods need to drain a failover does not apply — a stuck
	// sentinel should be removed promptly instead.
	sentinelTerminationGracePeriodVal = int64(30)

	// rolloutQuorumDeferRequeue paces the retry when a sentinel-mode
	// replica rollout is held back because the observed sentinel quorum
	// is not OK. Deleting a replica mid-roll while quorum is fragile can
	// tip the sentinel pool below quorum, leaving no failover authority
	// during the disruption window — so the roll waits and re-checks on
	// this cadence until quorum recovers. Short enough to resume promptly
	// once the pool heals, long enough not to thrash the apiserver while
	// it stays degraded.
	rolloutQuorumDeferRequeue = 10 * time.Second

	// baselineReconcileWatchdog re-arms a Reconcile at least every
	// few minutes even after a fully-converged steady-state pass set
	// no other RequeueAfter hint. Without this, the operator depends
	// entirely on watch events to learn about spec changes — and a
	// missed Update event (informer cache resync gap, watch-reconnect
	// bookmark drift, predicate filter racing a stale-cache compare)
	// strands the CR until the operator pod restarts. Bounding
	// the gap to a few minutes turns "stuck until restart" into
	// "stuck for at most this interval"; mergeRequeue keeps any
	// tighter hint from a phase / substate machine, so this only
	// fires on the otherwise-empty steady-state path.
	baselineReconcileWatchdog = 5 * time.Minute

	// minRequeueFloor caps how aggressively a phase / substate /
	// edge-detection can drag the next reconcile forward via a
	// "tighter cadence wins" merge. Without the floor, a hint of
	// (say) 5ms would rearm controller-runtime to re-Reconcile in
	// 5ms — a 200/sec apiserver thrash on the watch + status patch
	// path under any churn that re-suggests the small interval. The
	// floor is forgiving enough to absorb legitimate sub-second
	// hints (e.g. 250ms substate cadences) without round-tripping
	// the apiserver more than ~10 times/sec per CR.
	minRequeueFloor = 100 * time.Millisecond

	// lockContendedRequeue paces the retry when the per-CR reconcile
	// mutex is already held by a concurrent reconcile of the same CR
	// (TryLock miss). Acquisition is non-blocking by design — a busy CR
	// must never park a workqueue worker on the lock. Contention is rare
	// and transient (controller-runtime already serialises same-key
	// reconciles; this mutex only guards the safety-net runnable and
	// cross-controller callers) and the holder finishes well under a
	// second, so the contending worker requeues on the smallest cadence
	// the thrash guard permits — a fixed floor, not exponential backoff
	// (the workqueue dedups the requeued key, so this never busy-spins).
	lockContendedRequeue = minRequeueFloor

	// ConfigMap suffixes for the per-CR template + script set. The
	// `<cr>-valkey-conf` ConfigMap holds the rendered template (with the
	// _POD_IP_ placeholder); the `<cr>-init-scripts` ConfigMap holds the
	// short shell script that the init container executes to substitute
	// the placeholder and conditionally append the `replicaof` line.
	suffixValkeyConf  = "-valkey-conf"
	suffixInitScripts = "-init-scripts"

	// suffixSentinelBootstrap is the per-CR ConfigMap that the operator
	// populates with `seedMasterIP` so sentinel pods — and replicas
	// during cold-start — know which IP to point at before hellos
	// converge. The valkey pod mounts it `optional: true` so
	// standalone / replication CRs without sentinels still schedule
	// cleanly (the init script falls back to Service DNS).
	suffixSentinelBootstrap = "-sentinel-bootstrap"

	// Mount paths inside the valkey pod. Kept in this file (not in
	// valkeyconf) because they're a property of the pod-template
	// contract, not the renderer's output. The init container reads from
	// /config-template (RO ConfigMap) and writes to /config (shared
	// emptyDir); the main container reads /config/valkey.conf at start.
	mountConfigTemplate    = "/config-template"
	mountConfig            = "/config"
	mountInitScripts       = "/init-scripts"
	mountSentinelBootstrap = "/bootstrap"

	// renderScriptPath is the in-pod path of the render-config script.
	// Owned by this package; the init container's args reference it
	// verbatim.
	renderScriptPath = "/init-scripts/render-valkey-conf.sh"

	// defaultAuthSecretKey is the Secret key the operator reads from
	// when spec.Auth.SecretKey is empty. Matches the field-level
	// default documented on the CRD; centralised here so callers
	// (env-var injection, sentinel REDISCLI_AUTH wiring) share one
	// fallback string.
	defaultAuthSecretKey = "password"
)

// singleShotAnnotations enumerates the operator-trigger annotations that
// Phase 0 strips from the CR after a successful reconcile. The set is
// closed; new triggers must be added here AND validated by the
// validating webhook.
var singleShotAnnotations = []string{
	AcceptPVCLossAnnotation,
	ForceRotateAnnotation,
}

// ValkeyReconciler reconciles a Valkey object. The per-CR mutex is
// kept here so concurrent reconciles for the same CR serialise even
// when MaxConcurrentReconciles > 1.
type ValkeyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader bypasses the manager's cache. User-supplied auth Secrets
	// don't carry the operator's `app.kubernetes.io/managed-by=
	// velkir` label, so the cluster's label-narrowed informer
	// cache (cmd/main.go::buildCacheOptions) excludes them. The cached
	// client returns NotFound for any unlabeled Secret; the APIReader
	// goes straight to the apiserver. Nil-safe — falls back to the
	// cached Client (used by tests with fake clients that have no
	// label filter).
	APIReader client.Reader

	// Recorder emits Kubernetes Events on notable transitions
	// (DegradedResolved is the first consumer; rollout / failover
	// events join later). Nil-safe — the reconciler skips event
	// emission if no recorder is set.
	Recorder k8sevents.EventRecorder

	// ShortAuthPasswordReporter emits a one-shot AuthSecretShortPassword
	// warning event per (CR, secretName) tuple when the auth Secret's
	// `password` data falls below the redaction registry's MinTokenLen
	// floor. Nil-safe — falls back to no emission. SetupWithManager
	// seeds a Recorder-backed instance if the field is left empty so
	// production wiring stays minimal.
	ShortAuthPasswordReporter *events.ShortAuthPasswordReporter

	// Deprecator emits FieldDeprecated events for CR fields whose
	// Predicate in the Deprecations registry matches the live CR.
	// Per-(namespace/name/Path) tuple emits at most one event per
	// process lifetime; the in-memory dedup set resets on operator
	// restart. Nil-safe — checkDeprecations short-circuits when nil.
	// SetupWithManager seeds a Recorder-backed instance if the field
	// is left empty so production wiring stays minimal.
	Deprecator *events.Deprecator

	// Deprecations is the registry walked by checkDeprecations each
	// reconcile. Production wiring assigns ProductionDeprecations
	// (empty today — v1beta1 is additive-only since v0.1.0); tests
	// override with synthetic entries to exercise the emission path.
	// Nil/empty = no-op sweep.
	Deprecations []FieldDeprecation

	// DeviationEmitter re-surfaces the best-practice deviations the
	// validating webhook reports as ephemeral admission warnings as
	// durable Warning Events (PDBTooPermissive, AntiAffinityTooPermissive,
	// …). Per-(namespace/name/reason/field) tuple emits at most one event
	// per process lifetime; the in-memory dedup set resets on operator
	// restart. Nil-safe — emitDeviations short-circuits when nil.
	// SetupWithManager seeds a Recorder-backed instance if the field is
	// left empty so production wiring stays minimal.
	DeviationEmitter *events.DeviationEmitter

	// MaxConcurrentReconciles caps parallelism for this reconciler. Zero
	// defers to controller-runtime's default of 1.
	MaxConcurrentReconciles int

	// WatchNamespaces mirrors the operator's configured watch scope onto
	// the dedicated auth-Secret metadata informer (authSecretWatchSource).
	// Empty = cluster scope, matching cmd/main.go::buildCacheOptions.
	WatchNamespaces []string

	// LagChecker is the data-plane probe used by Phase 8 to query
	// `INFO replication` on each replica pod. Nil falls back to the
	// dialing default; tests inject a fake to exercise the gate-flip
	// logic without spinning up Valkey instances under envtest.
	LagChecker valkey.LagChecker

	// sentinelPeerCountFn reads `num-other-sentinels` per sentinel pod,
	// used by the Phase-3 one-pod-at-a-time roll to gate each advance on
	// the previously-rolled sentinel re-joining the quorum. Nil falls
	// back to sentinel.MasterPeerCountAll; tests inject a stub to drive
	// the re-join gate without live sentinels under envtest.
	sentinelPeerCountFn func(ctx context.Context, endpoints []sentinel.Endpoint, masterName, password string) []sentinel.MasterPeerCountResult

	// currentEpochFn returns the current observed Sentinel config-epoch
	// for a CR, used by the durable failover-dispatch seed fallback to
	// fence a stale marker. Nil falls back to the observer snapshot
	// (observedPrimaryAddrEpoch); tests inject a stub to drive the
	// epoch-fence branch without seeding a live observer snapshot.
	currentEpochFn func(cr types.NamespacedName) int64

	// nowFunc is the wall-clock source for the MasterLost hysteresis +
	// probe-freshness gate (the master-info latch reads). Nil falls back
	// to time.Now; tests inject a frozen clock so the down-after and
	// freshness boundaries are exercised at exact offsets rather than
	// against a live wall clock.
	nowFunc func() time.Time

	// endpointObservationsFn returns the sentinel observer's in-memory
	// per-endpoint observation snapshot for a CR, used by
	// observeSentinelTopology to read the topology counts without a live
	// observer. Nil falls back to r.SentinelObserver.EndpointObservations;
	// tests inject a stub to drive the topology-hygiene fold with scripted
	// KnownReplicas / KnownSentinels / CountsValid values.
	endpointObservationsFn func(cr types.NamespacedName) []sentinel.EndpointObservation

	// ReplicaOfIssuer issues `REPLICAOF <ip> <port>` against a
	// Valkey pod. Phase 7a (reconcileOrphanMasters) uses it to
	// demote pods whose role=replica label disagrees with their
	// in-process `INFO replication` role=master output. Nil-safe;
	// defaults to &valkey.DialingReplicaOfIssuer{} at first use.
	ReplicaOfIssuer valkey.ReplicaOfIssuer

	// PromoteIssuer issues `REPLICAOF NO ONE` against a Valkey pod.
	// Used ONLY by the zero-master recovery election in
	// detectAndRecoverStrandedSentinels — the state where every
	// address the sentinel quorum knows is dead, no live pod
	// self-reports master, and Sentinel's own election cannot succeed
	// because its candidate set died with the master. Nil-safe;
	// defaults to &valkey.DialingReplicaOfIssuer{} at first use.
	PromoteIssuer valkey.PromoteIssuer

	// ElectionDoomedFn evaluates the recovery election's doomed-election
	// evidence (see Manager.QuorumElectionDoomed — the single owner of
	// the rule). Nil-safe; defaults to
	// r.SentinelObserver.QuorumElectionDoomed. Test-injection seam only.
	ElectionDoomedFn func(ctx context.Context, endpoints []sentinel.Endpoint, masterName, password string, liveValkeyIPs map[string]struct{}) bool

	// observedMasterFn resolves the per-CR master IP + pod survey.
	// Nil-safe; defaults to r.observedMasterIPForCR. Test-injection
	// seam only — lets a test drive detectAndRecoverStrandedSentinels
	// with a zero-master survey (no live dials) so the recovery
	// election's independence from the stranded-surgery backoff is
	// unit-assertable.
	observedMasterFn func(ctx context.Context, v *valkeyv1beta1.Valkey, password string) (string, *valkeyPodSurvey)

	// recoverStrandedFn dispatches one stranded-sentinel recovery pass.
	// Nil-safe; defaults to r.SentinelObserver.RecoverStrandedSentinels.
	// Test-injection seam only — lets a test record the computed
	// InitialResetTarget (in particular SkipStrandedAddrs) the dispatcher
	// passes, without a live sentinel manager on the wire.
	recoverStrandedFn func(ctx context.Context, target sentinel.InitialResetTarget, bypassQuorumDeferral bool) sentinel.StrandedRecoveryResult

	// ClientKillIssuer issues `CLIENT KILL TYPE normal SKIPME yes`
	// against a Valkey pod that just demoted from primary to replica.
	// Closes pooled write-connections so clients reconnect
	// via the Service abstraction and land on the new primary,
	// rather than getting `-READONLY` on every write against the
	// demoted pod's ESTABLISHED socket. Nil-safe; defaults to
	// &valkey.DialingClientKillIssuer{} at first use.
	ClientKillIssuer valkey.ClientKillIssuer

	// SentinelObserver is the per-process singleton that owns per-CR
	// observer goroutines. The reconciler consults it via
	// Snapshot(cr) to drive sentinel-mode role labelling. Nil-safe:
	// when nil (test injection or pre-startup race) the reconciler
	// falls back to the bootstrap topology rule (pod-0 = primary)
	// for sentinel-mode CRs — same shape as standalone/replication.
	SentinelObserver *sentinel.Manager

	// FSM is the rollout state machine (internal/orchestration). Pure —
	// no clock, no recorder, no client. Initialised by cmd/main.go via
	// orchestration.NewMachine(). Nil-safe: applyFSM treats a nil FSM as
	// "no rollout machine wired" and short-circuits without emitting
	// events.
	FSM *orchestration.Machine

	// perCR is the single per-CR in-memory state bag, keyed by
	// types.NamespacedName. It replaces the former parallel sync.Map
	// trackers (the reconcile mutex, sentinel-UID set, quorum gate,
	// rollout/FSM edge detectors, failover latch, auth-password cache,
	// stale-replica timers, missing-Secret backoff clock, and SQ status
	// digest). See perCRState (percrstate.go) for the per-field
	// semantics and the forgetCR / StaleTrackerPruner teardown rules.
	perCR sync.Map // namespacedName → *perCRState

	// RotateAuthFn is an optional override for valkey.RotateAuth.
	// Production wiring leaves this nil — the dispatcher in
	// (r *ValkeyReconciler).rotateAuth falls back to the real package
	// function. Tests inject a stub returning deterministic PodResult
	// slices without spinning up real fake valkey TCP servers.
	RotateAuthFn rotateAuthFunc

	// Tunables overrides the load-bearing suppression-gate floors and
	// the Phase 11 orchestration deadline. Zero-valued in production;
	// cmd/main.go's `--allow-test-overrides` flag populates the
	// struct from env vars when explicitly opted in. See
	// `QuorumSuppressionTunables` for the field-level semantics.
	Tunables QuorumSuppressionTunables
}

// crQuorumState holds the per-CR last-observed Quorum plus the
// suppression-gate and stranded-recovery bookkeeping.
//
// The suppression-gate fields below model the operator-side
// equivalent of `orchestration.StateDegradedQuorumLost` — after the
// configured loss threshold of continuous Lost observations
// (`QuorumSuppressionTunables.lossThreshold()`), the operator flips
// `suppressionActive` and the sentinel manager's deferral predicate
// (wired from cmd/main.go) defers the stranded-sentinel REMOVE +
// MONITOR surgery. Exit requires the configured recovery hysteresis
// (`QuorumSuppressionTunables.recoveryPolls()`) consecutive OK
// observations backed by distinct observer polls, so neither a
// transient quorum-recovery flicker nor reconcile churn re-reading
// one OK poll releases the gate. Entry is likewise poll-backed: the
// sustained-loss threshold crosses only when at least one live
// observer poll newer than the episode-opening poll has re-confirmed
// the loss, so a wedged observer re-publishing a single stale Lost
// reading cannot arm the gate off reconcile churn.
//
// QuorumStatusUnknown observations are a no-op on the gate: the
// observer publishes them when it cannot reach a quorum of sentinel
// peers, which is the expected steady-state during the operator's
// own master-aware recovery rollouts (transient per-pod
// unreachability windows). Without this no-op, those windows
// accumulate spurious loss-time and re-arm the gate even when the
// cluster has quorum among reachable sentinels.
type crQuorumState struct {
	mu sync.Mutex

	// lastQuorum is the most recent tri-state Quorum value the gate
	// has consumed. Surfaced for diagnostic logging / metric
	// labelling on transition.
	lastQuorum sentinel.QuorumStatus

	// quorumLostSince is the wall-clock time of the first observed
	// QuorumStatusLost in the current loss episode. Nil while quorum
	// is OK / Unknown (or has never been observed). The sustained-
	// loss threshold fires when
	// `now - *quorumLostSince >= lossThreshold`.
	quorumLostSince *time.Time

	// quorumLostSincePoll is the observer poll stamp
	// (ObservedPrimary.LastPolledAt) captured when the current loss
	// episode opened. The entry threshold-cross additionally requires the
	// crossing observation's poll stamp to be strictly newer than this
	// anchor, so a wedged observer re-publishing one Lost poll (its stamp
	// frozen, carried forward unchanged by pub/sub republishes) can never
	// enter suppression off reconcile churn — the aging must be backed by
	// a live re-confirmation. Entry-side mirror of quorumOKLastCountedPoll.
	// Zeroed alongside quorumLostSince on quorum recovery.
	quorumLostSincePoll time.Time

	// quorumOKConsecutivePolls counts consecutive OK observations
	// backed by distinct observer polls after suppressionActive
	// flipped on. Reset to 0 on any Lost observation; preserved
	// unchanged on Unknown and on OK observations whose poll stamp
	// has already been counted. The exit transition fires when the
	// counter reaches `recoveryPolls`.
	quorumOKConsecutivePolls int

	// quorumOKLastCountedPoll is the snapshot poll stamp
	// (ObservedPrimary.LastPolledAt) of the most recent OK
	// observation that advanced quorumOKConsecutivePolls. Reconciles
	// fire far more often than observer polls (watch churn vs the
	// poll tick), so back-to-back passes routinely re-read the SAME
	// poll; advancing the counter only on a strictly-newer stamp
	// keeps the exit hysteresis a count of polls, not reconcile
	// passes. Zeroed alongside the counter on Lost and on gate exit.
	quorumOKLastCountedPoll time.Time

	// suppressionActive is the gate state read by the deferral
	// predicate. Flips on at the threshold-cross, off at the
	// hysteresis-cross.
	suppressionActive bool

	// splitBrainSince is the wall-clock time when the observer
	// first reported `Present && Primary.Quorum == QuorumStatusLost`
	// in the current disagreement episode. Nil while sentinels agree
	// or the observer can't decide (Unknown). The
	// derived sustained-seconds duration is exposed via the
	// SplitBrainSustainedSeconds gauge so the chart-shipped
	// ValkeySplitBrainDetected alert can require sustained
	// disagreement before paging, instead of firing on every
	// bootstrap-race blip.
	splitBrainSince *time.Time

	// splitBrainPollAt / splitBrainConfirmedAt freshness-gate the
	// sustained reading: splitBrainPollAt is the newest observer
	// LastPolledAt folded into the current episode, and
	// splitBrainConfirmedAt is the reconcile wall-clock at which that
	// poll advance was observed. The gauge reports
	// confirmedAt-since, not now-since, so reconcile churn against a
	// wedged observer (final snapshot replayed with a frozen
	// LastPolledAt) freezes the reading at its last live-poll-confirmed
	// value instead of growing it without bound off data the operator
	// cannot trust. Zero while no episode is in flight.
	splitBrainPollAt      time.Time
	splitBrainConfirmedAt time.Time
	// splitBrainExpiredPollAt is the frozen poll stamp at which the last
	// episode expired for staleness. It gates the episode re-arm: a
	// permanently-wedged observer replays that same LastPolledAt forever,
	// so re-stamping a "new" episode off it would re-fire SplitBrainDetected
	// (and re-Inc its counter) once per staleness window against data the
	// expiry already deemed unverifiable. Only a strictly-newer poll — a
	// genuine re-measurement — re-arms. Cleared on a clean reset (OK /
	// !Present); zero when no episode has ever expired.
	splitBrainExpiredPollAt time.Time

	// strandedRecoveryLastFired is the wall-clock time of the most
	// recent stranded-surgery classification probe. Repurposed as the
	// coarse probe-cadence stamp: it debounces the SentinelsAll
	// classification probe to base cadence (strandedRecoveryCooldown)
	// for BOTH fresh-strand pickup and wedged-sentinel recovery-
	// detection, regardless of any address's per-address backoff depth.
	// Stamped once per actual probe (healthy, wipe, skip-only, or a
	// post-classification minority/PING/failover defer). Per-address
	// re-wipe pacing lives in strandedNoProgress, not here.
	strandedRecoveryLastFired time.Time

	// strandedNoProgress tracks, per stranded sentinel ADDRESS, the
	// consecutive REMOVE + MONITOR surgeries fired against it without
	// its peer-list rebuilding (the no-progress count) plus the
	// wall-clock of its last actual re-wipe. Keyed by Endpoint.Addr, so
	// a recreated pod — same name, new IP — starts a fresh count and a
	// recovered sentinel drops out. The derived per-address backoff
	// level (strandedAddrBackoffLevel) plus lastWiped drive the
	// per-address re-wipe pace: a wedged sentinel is re-probed every base
	// window but re-wiped only on its lengthened cadence, so a
	// permanently-wedged sentinel (auth broken, or a NetworkPolicy
	// blocking `__sentinel__:hello`) never blocks a clean surgery on a
	// different freshly-stranded sentinel of the same CR. Auth-failed
	// targets are seeded to the stuck threshold (a sentinel that can't
	// AUTH can never gossip). nil until the first dispatch.
	strandedNoProgress map[string]strandedAddrState

	// strandedLinkupStuck is true while >=1 tracked sentinel has hit
	// the no-progress threshold. Read (freshness-gated) by updateStatus
	// to drive the SentinelPeerLinkupStuck Degraded reason. Cleared on
	// a healthy classification or a dispatch where no sentinel is stuck.
	strandedLinkupStuck     bool
	strandedLinkupStuckAt   time.Time
	strandedLinkupStuckEdge string // last emitted stuck-addr signature; "" = re-armed

	// recoveryPromotionLastFired is the wall-clock time of the most
	// recent zero-master recovery election (REPLICAOF NO ONE
	// issuance, successful or not). Its cooldown is deliberately
	// separate from strandedRecoveryLastFired: promotion runs on the
	// pass where no master resolves, and the very next pass must be
	// free to fire the re-point surgery at the newly-promoted master
	// without waiting out a shared cooldown.
	recoveryPromotionLastFired time.Time

	// recoveryDetectionLastProbed is the wall-clock time of the most recent
	// sustained-quorum-loss arming probe (the forced dial-sweep). A monotonic
	// rate-limiter stamp read/written under state.mu — not a latched condition
	// or gauge, so the freshness age-out rule does not apply to it.
	recoveryDetectionLastProbed time.Time

	// lastOKAddr is the Snapshot.Primary.Addr captured the last
	// reconcile QuorumOK was true. Compared against the next-OK
	// Addr to detect a failover (Addr changed from value A → value
	// B across a disagreement episode) and observe the
	// FailoverDurationSeconds histogram (observability gap).
	// Empty string means "no prior OK observation yet" — bootstrap
	// vs. failover are distinguished by this field's emptiness.
	lastOKAddr string

	// masterInfoTimeoutSince is the wall-clock time the operator's
	// INFO-replication probe of the labelled primary first started
	// timing out / returning malformed in the current episode. Nil
	// while the primary responds normally. Drives the
	// MasterInfoTimeoutSeconds gauge (tier-3) so frozen-process
	// signatures (SIGSTOP'd valkey-server, cgroup-frozen, kernel
	// stall) surface as a passive observability signal even when the
	// chart's exec liveness probe restart loop is in flight.
	masterInfoTimeoutSince *time.Time

	// masterInfoObservedAt is the wall-clock of the most recent
	// observeMasterInfoTimeout run (a probe pass, or a no-labelled-
	// primary clear). The status defer reads masterInfoTimeoutSince on
	// every reconcile, but the probe runs only on passes that reach
	// Phase 11; this timestamp lets the MasterLost read ignore a stale
	// latch from an early-return pass where no measurement was taken.
	// Zero until the first probe run.
	masterInfoObservedAt time.Time

	// masterInfoRoleDisclaimed records whether the most recent
	// labeled-primary INFO probe ANSWERED but self-reported a
	// non-master role — a demoted-under-the-label pod (post-failover,
	// relabel pending). observedMasterIPForCR's same-pass reuse treats
	// it like a probe failure: the label must not resolve as master.
	// Meaningful only alongside a fresh masterInfoObservedAt; reset on
	// the no-labelled-primary path.
	masterInfoRoleDisclaimed bool

	// topologySentinel / topologyReplica are the per-dimension debounce
	// trackers for the sentinel-topology-mismatch hygiene signal (peer
	// count vs spec, replica count vs spec). Each latches a `since`
	// stamp on the first eligible+deficit pass and declares `active`
	// once it has held for topologyMismatchDebounce. topologyMismatchObservedAt
	// stamps the last observeSentinelTopology run so the status read can
	// expire a stale latch (observer stopped running) instead of pinning
	// the condition/gauge forever — same posture as the dual-master and
	// linkup-stuck freshness gates.
	topologySentinel           topologyDimState
	topologyReplica            topologyDimState
	topologyMismatchObservedAt time.Time
}

// topologyDimState is one dimension's debounce state for the
// sentinel-topology-mismatch hygiene signal. The `since`/`active`/`edge`
// triple mirrors the splitBrainSince / masterInfoTimeoutSince trackers:
// `since` is stamped on the first eligible+deficit pass and KEPT across
// subsequent passes (never refreshed), `active` flips once the deficit
// has held for the debounce window, and `edge` latches one event per
// active episode (re-armed on prune). `deficit` carries the latest
// observed deficit for the gauge read.
type topologyDimState struct {
	since   *time.Time
	active  bool
	edge    bool
	deficit int
}

// fold advances one dimension's debounce state by one observe pass and
// reports whether the dimension is active and whether the caller should
// fire its once-per-episode event. Pure (no lock); the caller holds
// crQuorumState.mu.
//
//   - ineligible OR non-positive deficit → prune (clear since/active/edge/
//     deficit) and return (false, false). Recovery and any ineligible
//     pass both re-arm the event edge.
//   - eligible + positive deficit, still inside the debounce window →
//     record the deficit, stamp `since` on the nil→set edge, stay inactive.
//   - eligible + positive deficit, debounce window crossed → active; fire
//     exactly once (on the first pass past the threshold) via the edge latch.
func (d *topologyDimState) fold(deficit int, eligible bool, now time.Time, debounce time.Duration) (active, fire bool) {
	if !eligible || deficit <= 0 {
		d.since = nil
		d.active = false
		d.edge = false
		d.deficit = 0
		return false, false
	}
	d.deficit = deficit
	if d.since == nil {
		t := now
		d.since = &t
	}
	if now.Sub(*d.since) >= debounce {
		d.active = true
		if !d.edge {
			d.edge = true
			return true, true
		}
		return true, false
	}
	d.active = false
	return false, false
}

// strandedAddrState is the per-address stranded-surgery record: the
// consecutive no-progress surgery count plus the wall-clock of the last
// actual re-wipe. Keyed by Endpoint.Addr in strandedNoProgress. The
// derived backoff level (strandedAddrBackoffLevel) times the base
// cooldown gives the per-address re-wipe cadence measured from lastWiped.
type strandedAddrState struct {
	noProgress int
	lastWiped  time.Time
}

// strandedRecoveryCooldown is the minimum interval between
// consecutive SentinelStrandedRecovery dispatches for the same CR.
// Sized to cover the typical Sentinel gossip-bootstrap window
// (~2s gossip-interval × a couple of cycles) so a freshly-MONITORed
// sentinel has a chance to populate its peer-list before the next
// reconcile evaluates it again.
const strandedRecoveryCooldown = 30 * time.Second

const (
	// strandedSurgeryStuckThreshold is the number of consecutive
	// no-progress REMOVE + MONITOR surgeries against one sentinel
	// address before the operator declares its peer-link stuck: it
	// backs off and surfaces SentinelPeerLinkupStuck instead of
	// re-wiping at fixed cadence. Three surgeries (≥90s apart at the
	// base cooldown) is well past the ~2-5s gossip-bootstrap window, so
	// a still-empty peer-list is a real failure (auth broken,
	// NetworkPolicy blocking `__sentinel__:hello`), not gossip in
	// flight — the destructive-surgery livelock the read-back defends.
	strandedSurgeryStuckThreshold = 3

	// strandedSurgeryMaxBackoffLevel caps the exponential cooldown at
	// strandedRecoveryCooldown << level. Level 3 = 8× = 4m: long enough
	// to stop hammering a wedged ensemble, short enough that an
	// operator's NetworkPolicy / auth fix is retried promptly. Surgery
	// is never latched off — it stays the only repair path during
	// sustained quorum loss — only paced.
	strandedSurgeryMaxBackoffLevel = 3
)

// strandedLinkupStuckFreshnessWindow bounds how long a linkup-stuck
// observation drives the Degraded condition without a fresh surgery
// pass refreshing it. Larger than the maximum backed-off cooldown
// (strandedRecoveryCooldown << maxLevel) so a genuinely-stuck CR whose
// surgeries are paced out to 4m keeps the condition, while a CR whose
// dispatcher stops running entirely (an out-of-contract state) ages the
// stale condition out instead of latching it forever — the
// gauge-latch lesson from the dual-master surfacing work.
var strandedLinkupStuckFreshnessWindow = 2 * (strandedRecoveryCooldown << strandedSurgeryMaxBackoffLevel)

// strandedAddrBackoffLevel derives one address's re-wipe backoff level
// from its consecutive no-progress surgery count: 0 below the stuck
// threshold (base cadence — a fresh strand fires immediately), then
// climbing one level per further no-progress surgery, capped at
// strandedSurgeryMaxBackoffLevel. Pure; reproduces the old per-CR
// single-sentinel pacing exactly (count 3→level 1, 4→2, 5→3, capped)
// while a count-0 fresh strand stays at level 0 = base.
func strandedAddrBackoffLevel(noProgress int) int {
	if noProgress < strandedSurgeryStuckThreshold {
		return 0
	}
	return min(noProgress-strandedSurgeryStuckThreshold+1, strandedSurgeryMaxBackoffLevel)
}

// strandedProbeCoolingDown reports whether the stranded-surgery
// classification probe is still within its BASE cooldown at `now`. This
// is the coarse gate that debounces the SentinelsAll classification
// fan-out to base cadence regardless of any address's backoff depth;
// per-address re-wipe pacing is applied inside the manager via the
// skip-set, not here. So a fresh strand is picked up at base cadence even
// during a deep-backoff episode on a different wedged sentinel. Caller
// holds no lock.
func (state *crQuorumState) strandedProbeCoolingDown(now time.Time) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return now.Sub(state.strandedRecoveryLastFired) < strandedRecoveryCooldown
}

// strandedSkipSet returns the set of tracked addresses (keyed by
// Endpoint.Addr) that are stuck (derived level >= 1) AND not yet due for
// re-wipe — their per-address cooldown (strandedRecoveryCooldown <<
// level, measured from lastWiped) has not elapsed at `now`. The
// dispatcher passes it to the manager as SkipStrandedAddrs so those
// wedged sentinels are re-probed but not re-wiped this pass, while a
// fresh strand (absent from the map) or a below-threshold address (level
// 0) is never skipped. A stuck address whose window HAS elapsed is not in
// the set — the backoff paces surgery, it never latches it off. Returns
// nil when nothing qualifies. Caller holds no lock.
func (state *crQuorumState) strandedSkipSet(now time.Time) map[string]struct{} {
	state.mu.Lock()
	defer state.mu.Unlock()
	var skip map[string]struct{}
	for addr, rec := range state.strandedNoProgress {
		level := strandedAddrBackoffLevel(rec.noProgress)
		if level < 1 {
			continue
		}
		if now.Sub(rec.lastWiped) >= strandedRecoveryCooldown<<level {
			continue
		}
		if skip == nil {
			skip = make(map[string]struct{})
		}
		skip[addr] = struct{}{}
	}
	return skip
}

// detectStrandedPeerLinkupStuck folds one dispatched surgery's outcome
// into the per-CR per-address no-progress tracker. A wiped sentinel
// re-classified as empty-peer after a prior wipe made no progress: its
// count advances and its last-wipe clock re-stamps. One whose auth-pass
// re-propagation failed can never gossip and is seeded straight to the
// threshold. A SKIPPED sentinel (deliberately paced this pass) carries
// its prior record forward unchanged — neither advancing the count nor
// re-stamping the clock IS the per-address pace. Addresses absent from
// both sets recovered (or the pod was replaced — the map is keyed by IP
// and rebuilt each pass) and drop out. Sets the strandedLinkupStuck flag
// + its event edge, then returns the sorted stuck set (addresses at
// derived level >= 1), whether the caller should emit the
// once-per-episode SentinelPeerLinkupStuck event, and the max per-address
// backoff level among the stuck set. Caller holds no lock.
func (state *crQuorumState) detectStrandedPeerLinkupStuck(wipedAddrs, skippedAddrs, authFailedAddrs []string, now time.Time) (stuck []string, fireEvent bool, backoffLevel int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	next := make(map[string]strandedAddrState, len(wipedAddrs)+len(skippedAddrs))
	for _, a := range wipedAddrs {
		rec := state.strandedNoProgress[a]
		// "Consecutive" has a bound: a record whose last wipe is older
		// than the linkup-stuck freshness window survived a fold-free
		// stretch (masterIP unresolvable, empty endpoints, sustained
		// defers) during which the map is never rebuilt. Its count is no
		// longer consecutive evidence — and a replaced pod stranding at a
		// REUSED IP would inherit it, declaring linkup-stuck after one
		// wipe and skipping the threshold's base-cadence wipes — so the
		// count restarts. Also covers the absent-record case (zero
		// lastWiped), where the prior count is already 0.
		if now.Sub(rec.lastWiped) > strandedLinkupStuckFreshnessWindow {
			rec.noProgress = 0
		}
		next[a] = strandedAddrState{noProgress: rec.noProgress + 1, lastWiped: now}
	}
	for _, a := range authFailedAddrs {
		// Only sentinels this pass actually wiped can be no-progress;
		// an auth failure on such a target is an immediate wedge.
		if rec, wiped := next[a]; wiped && rec.noProgress < strandedSurgeryStuckThreshold {
			rec.noProgress = strandedSurgeryStuckThreshold
			next[a] = rec
		}
	}
	for _, a := range skippedAddrs {
		// A paced sentinel carries its prior record forward untouched — no
		// count advance, no re-stamp. (Absent-from-old is impossible: the
		// skip set is derived from tracked >=1-level entries; guard anyway.)
		if rec, ok := state.strandedNoProgress[a]; ok {
			next[a] = rec
		}
	}
	state.strandedNoProgress = next
	for a, rec := range next {
		if strandedAddrBackoffLevel(rec.noProgress) >= 1 {
			stuck = append(stuck, a)
		}
	}
	sort.Strings(stuck)
	if len(stuck) == 0 {
		state.strandedLinkupStuck = false
		state.strandedLinkupStuckEdge = ""
		return nil, false, 0
	}
	for _, a := range stuck {
		if lvl := strandedAddrBackoffLevel(next[a].noProgress); lvl > backoffLevel {
			backoffLevel = lvl
		}
	}
	state.strandedLinkupStuck = true
	state.strandedLinkupStuckAt = now
	sig := strings.Join(stuck, ",")
	fireEvent = state.strandedLinkupStuckEdge != sig
	state.strandedLinkupStuckEdge = sig
	return stuck, fireEvent, backoffLevel
}

// clearStrandedLinkupStuck resets the no-progress tracker after a
// healthy classification (the ensemble's peer-lists are intact). Caller
// holds no lock.
func (state *crQuorumState) clearStrandedLinkupStuck() {
	state.mu.Lock()
	state.strandedNoProgress = nil
	state.strandedLinkupStuck = false
	state.strandedLinkupStuckEdge = ""
	state.mu.Unlock()
}

// recoveryPromotionCooldownActive reports whether the most recent
// zero-master recovery election fired within recoveryPromotionCooldown
// of `now`, read under the state mutex. Returns false when no promotion
// has ever fired. Lets the Phase-8 escape honour the same cooldown
// maybeRecoveryPromote writes under state.mu, without a lock-free read
// of recoveryPromotionLastFired.
func (state *crQuorumState) recoveryPromotionCooldownActive(now time.Time) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	last := state.recoveryPromotionLastFired
	return !last.IsZero() && now.Sub(last) < recoveryPromotionCooldown
}

// strandedLinkupStuckActiveOrExpire reports whether a linkup-stuck
// observation is fresh enough to drive the Degraded condition, and — on
// the stale path — EXPIRES it (drops the flag + re-arms the event edge
// so a fresh episode surfaces again). The OrExpire name signals the
// read-side mutation, mirroring the sibling dualMasterActiveOrExpire.
// Caller holds no lock.
func (state *crQuorumState) strandedLinkupStuckActiveOrExpire(now time.Time) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.strandedLinkupStuck && now.Sub(state.strandedLinkupStuckAt) <= strandedLinkupStuckFreshnessWindow {
		return true
	}
	if state.strandedLinkupStuck {
		// Stale: no surgery pass refreshed it within the window — drop
		// it and re-arm the event edge so a fresh stuck episode
		// surfaces again.
		state.strandedLinkupStuck = false
		state.strandedLinkupStuckEdge = ""
	}
	return false
}

// Sentinel-topology-mismatch hygiene tuning. The debounce sits above
// the ~10-15s post-roll / sentinel-replacement gossip-reconverge window
// and above the observer's 10s pull tick, so a transient partition
// re-converges before the signal fires. The input freshness window is
// ~3× the tick so a couple of missed polls don't flap the accrual, and
// the output latch expires at 2× the debounce so a stale observation
// (observer stopped running) can never pin the condition/gauge.
var (
	topologyMismatchDebounce           = 60 * time.Second
	topologyObservationFreshnessWindow = 30 * time.Second
	topologyMismatchFreshnessWindow    = 2 * topologyMismatchDebounce
)

// foldTopologyMismatch folds one observe pass into both per-dimension
// debounce trackers under the state mutex (mirrors
// detectStrandedPeerLinkupStuck's single-lock fold) and stamps
// topologyMismatchObservedAt so the status read can expire a stale
// latch. Returns whether each dimension should fire its once-per-episode
// event; the active state is read back by topologyMismatchActiveOrExpire
// (which the status defer + gauge use), so it is not surfaced here.
// Caller holds no lock.
func (state *crQuorumState) foldTopologyMismatch(sentinelEligible bool, sentinelDeficit int, replicaEligible bool, replicaDeficit int, now time.Time) (sentFire, replFire bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	_, sentFire = state.topologySentinel.fold(sentinelDeficit, sentinelEligible, now, topologyMismatchDebounce)
	_, replFire = state.topologyReplica.fold(replicaDeficit, replicaEligible, now, topologyMismatchDebounce)
	state.topologyMismatchObservedAt = now
	return sentFire, replFire
}

// topologyMismatchActiveOrExpire reports the current active per-dimension
// deficits, and — when the last observe pass is stale (or never ran) —
// EXPIRES both dimensions (resets since/active/edge/deficit, re-arming
// the event edge) so a fresh episode surfaces again. The OrExpire name
// signals the read-side mutation, mirroring strandedLinkupStuckActiveOrExpire.
// A dimension contributes its deficit only while active; `active` is true
// when either dimension has a positive deficit. Caller holds no lock.
func (state *crQuorumState) topologyMismatchActiveOrExpire(now time.Time) (active bool, sentinelDeficit, replicaDeficit int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.topologyMismatchObservedAt.IsZero() ||
		now.Sub(state.topologyMismatchObservedAt) > topologyMismatchFreshnessWindow {
		state.topologySentinel = topologyDimState{}
		state.topologyReplica = topologyDimState{}
		return false, 0, 0
	}
	if state.topologySentinel.active {
		sentinelDeficit = state.topologySentinel.deficit
	}
	if state.topologyReplica.active {
		replicaDeficit = state.topologyReplica.deficit
	}
	active = sentinelDeficit > 0 || replicaDeficit > 0
	return active, sentinelDeficit, replicaDeficit
}

// recoveryPromotionCooldown is the minimum interval between
// zero-master recovery elections for the same CR. Longer than the
// stranded cooldown: a promotion should have converged the cluster
// (promote → re-point → relabel) well within it, so a re-fire means
// the prior attempt failed and hammering REPLICAOF NO ONE at
// reconcile cadence would only churn the data plane.
const recoveryPromotionCooldown = 60 * time.Second

// recoveryDetectionCooldown paces the sustained-quorum-loss arming path's
// forced survey (a full dial-sweep plus the doomed-election fan-out inside
// maybeRecoveryPromote) so it runs periodically rather than every
// reconcile. Sized to three observer poll cycles (3 x sentinel.DefaultPollInterval
// = 3 x 10s), roughly the rate at which fresh CKQUORUM evidence arrives.
// Distinct from recoveryPromotionCooldown (post-promotion, 60s) and
// strandedRecoveryCooldown (surgery, 30s) — this bounds detection cost,
// not promotion or surgery cadence.
const recoveryDetectionCooldown = 30 * time.Second

// dualMasterObservedFreshnessWindow bounds how long a dual-master
// observation drives Ready/Degraded without a fresh scan. The producers
// run behind strandedRecoveryCooldown (the Phase 11 survey), once per
// failover-section pass (the Phase 7a self-heal scan), or once per
// no-labeled-primary reconcile (the replication orphan scan and
// no-primary observation scan); two cooldown periods of silence means no
// scan has re-confirmed the split — the stamp ages out rather than
// pinning conditions from a pass that never re-measured. The event-edge
// union (foldDualMasterEventUnion) accumulates independently of this
// window and ends its episode only on a clear / age-out / prune.
const dualMasterObservedFreshnessWindow = 2 * strandedRecoveryCooldown

// dualMasterActiveFromStamp reports whether a dual-master observation
// is fresh enough to drive the Ready/Degraded conditions and the
// valkey_dual_master_observed gauge. Pure (observation + clock passed
// in) for table-testing.
func dualMasterActiveFromStamp(obs *dualMasterObservation, now time.Time) bool {
	if obs == nil || len(obs.pods) < 2 {
		return false
	}
	return now.Sub(obs.observedAt) <= dualMasterObservedFreshnessWindow
}

// setDualMasterGauge writes the valkey_dual_master_observed gauge to
// match `active`. The gauge is a single-writer derivation of the same
// freshness-gated observation the Ready/Degraded conditions read, so it
// can never diverge from the conditions — in particular it can't latch
// at 1 after the observation ages out or after a producer stops running.
func (r *ValkeyReconciler) setDualMasterGauge(cr types.NamespacedName, active bool) {
	val := 0.0
	if active {
		val = 1
	}
	operatormetrics.DualMasterObserved.WithLabelValues(cr.Namespace, cr.Name).Set(val)
}

// masterInfoSamePassReuseWindow bounds how old this reconcile's
// observeMasterInfoTimeout reading may be for observedMasterIPForCR to
// reuse it instead of re-dialing the labeled primary. Both run in the
// same Reconcile pass, so anything beyond a few seconds means the
// probe did not run this pass (an early-return path) and the resolver
// must dial for itself. Distinct from masterInfoProbeFreshnessWindow,
// which bounds the CROSS-pass trust of the same timestamp for the
// MasterLost Ready read — the two thresholds deliberately differ.
const masterInfoSamePassReuseWindow = 15 * time.Second

// splitBrainConfirmationStaleness bounds how long a split-brain episode
// may drive the sustained gauge without a fresh observer poll
// re-confirming it. The confirmation producer is the observer pull tick
// (10s default); topologyObservationFreshnessWindow is that producer's
// input-freshness tolerance (3× the tick, so a couple of missed polls
// never flap a reading), and this output latch doubles it — the same 2×
// freshness shape as the sibling OrExpire reads (dual-master 2×30s,
// linkup-stuck 2×(base<<max), topology 2×debounce). Past it the episode
// is unverifiable and expires (gauge back to 0) instead of pinning the
// critical sustained-disagreement alert until operator restart.
var splitBrainConfirmationStaleness = 2 * topologyObservationFreshnessWindow

// updateSplitBrainSustained advances the per-CR split-brain
// duration tracker from the tri-state Quorum observation. Returns
// the sustained-seconds value the caller stamps onto the
// SplitBrainSustainedSeconds gauge for this reconcile, plus
// episodeStarted=true exactly on the nil→set edge of
// splitBrainSince — the one reconcile where a new disagreement
// episode begins. That edge is what gates the SplitBrainDetected
// event + counter to once per episode instead of once per reconcile
// (a sustained loss otherwise re-emits hundreds of warning events
// whose varying text defeats recorder aggregation). Caller MUST
// hold state.mu.
//
// polledAt is the snapshot's LastPolledAt: the elapsed reading is
// capped at the last reconcile whose polledAt strictly advanced
// (advanceSplitBrainConfirmation), so a wedged observer replaying a
// frozen snapshot cannot grow the gauge on reconcile churn alone —
// only fresh polls advance it. And the cap itself expires: once no
// fresh poll has re-confirmed the episode within
// splitBrainConfirmationStaleness, the episode is unverifiable and is
// dropped (splitBrainEpisodeExpired) — gauge back to 0 rather than a
// frozen non-zero reading holding the alert forever. After the
// observer recovers, a fresh Lost starts a NEW episode, deliberately
// re-firing SplitBrainDetected once: continuity across the wedge could
// not be verified, so the new episode is a new page. The episode edge
// and reset semantics are otherwise unaffected by the cap.
//
//   - Present && QuorumStatusUnknown → the observer can't reach a
//     quorum of peers (the restart placeholder / a transient
//     unreachable window during a recovery rollout). Neither start
//     nor reset the episode; report the current sustained value (0
//     when none is in flight), advancing it only on a fresh poll.
//     Mirrors updateQuorumSuppression's Unknown branch and the
//     QuorumStatus godoc contract — the placeholder's zero
//     LastPolledAt never advances anything, so an operator restart of
//     a healthy cluster cannot false-page the
//     SplitBrainSustainedSeconds alert.
//   - !Present OR QuorumStatusOK → reset (gauge=0). New disagreement
//     episodes start fresh; the gauge is the "current sustained"
//     reading, not a max.
//   - Present && QuorumStatusLost, first observation → stamp now,
//     gauge=0, episodeStarted=true — UNLESS a prior episode expired for
//     staleness and the observer has not produced a newer poll since
//     (still wedged): then stay quiet (gauge=0, episodeStarted=false) so
//     the event/counter does not churn once per staleness window.
//   - Present && QuorumStatusLost, continuing → elapsed up to the
//     last fresh-poll confirmation.
func (state *crQuorumState) updateSplitBrainSustained(present bool, q sentinel.QuorumStatus, polledAt, now time.Time) (float64, bool) {
	if present && q == sentinel.QuorumStatusUnknown {
		if state.splitBrainSince == nil {
			return 0, false
		}
		state.advanceSplitBrainConfirmation(polledAt, now)
		if state.splitBrainEpisodeExpired(now) {
			return 0, false
		}
		return state.splitBrainConfirmedAt.Sub(*state.splitBrainSince).Seconds(), false
	}
	if !present || q != sentinel.QuorumStatusLost {
		state.splitBrainSince = nil
		state.splitBrainPollAt = time.Time{}
		state.splitBrainConfirmedAt = time.Time{}
		state.splitBrainExpiredPollAt = time.Time{}
		return 0, false
	}
	if state.splitBrainSince == nil {
		// After a staleness expiry, only re-arm (and re-fire
		// SplitBrainDetected) on a poll strictly newer than the one that
		// expired: a permanently-wedged observer replays the same frozen
		// LastPolledAt, and re-stamping here would churn the event +
		// counter every staleness window. The IsZero guard keeps a genuine
		// first observation (no prior expiry) unaffected.
		if !state.splitBrainExpiredPollAt.IsZero() && !polledAt.After(state.splitBrainExpiredPollAt) {
			return 0, false
		}
		t := now
		state.splitBrainSince = &t
		state.splitBrainPollAt = polledAt
		state.splitBrainConfirmedAt = now
		return 0, true
	}
	state.advanceSplitBrainConfirmation(polledAt, now)
	if state.splitBrainEpisodeExpired(now) {
		return 0, false
	}
	return state.splitBrainConfirmedAt.Sub(*state.splitBrainSince).Seconds(), false
}

// advanceSplitBrainConfirmation re-confirms the in-flight split-brain
// episode when polledAt is strictly newer than the episode's recorded
// poll stamp — the "the observer actually re-measured" freshness signal.
// A replayed snapshot (pub/sub carries LastPolledAt forward unchanged)
// leaves both stamps alone, freezing the reported elapsed. Caller MUST
// hold state.mu and have splitBrainSince set.
func (state *crQuorumState) advanceSplitBrainConfirmation(polledAt, now time.Time) {
	if polledAt.After(state.splitBrainPollAt) {
		state.splitBrainPollAt = polledAt
		state.splitBrainConfirmedAt = now
	}
}

// splitBrainEpisodeExpired applies the freshness expiry to the in-flight
// episode: when no fresh observer poll has re-confirmed it within
// splitBrainConfirmationStaleness, the episode is unverifiable — the
// tracker drops it (since/pollAt/confirmedAt reset) and the gauge reads
// 0, mirroring the sibling OrExpire reads instead of holding a frozen
// non-zero value against a wedged observer. The frozen poll stamp is
// remembered in splitBrainExpiredPollAt so the re-arm branch can tell a
// still-wedged observer (same replayed poll — stays quiet) from a genuine
// re-measurement (strictly-newer poll — starts a NEW episode and re-fires
// SplitBrainDetected once, intended, continuity across the wedge being
// unverifiable). Called AFTER advanceSplitBrainConfirmation, so a live
// observer observed by a slow reconcile re-confirms first and never
// expires. Caller MUST hold state.mu and have splitBrainSince set.
func (state *crQuorumState) splitBrainEpisodeExpired(now time.Time) bool {
	if now.Sub(state.splitBrainConfirmedAt) <= splitBrainConfirmationStaleness {
		return false
	}
	state.splitBrainExpiredPollAt = state.splitBrainPollAt
	state.splitBrainSince = nil
	state.splitBrainPollAt = time.Time{}
	state.splitBrainConfirmedAt = time.Time{}
	return true
}

// observeFailoverIfAddrChanged detects a completed failover and
// returns (duration, trigger, true) when one is observed. Returns
// zeros + false when the snapshot represents bootstrap (no prior
// OK Addr), no-change steady state, or a still-in-flight disagreement.
//
// Detection rule: only fires on the transition `!QuorumOK with
// splitBrainSince stamped → QuorumOK with snap.Addr != lastOKAddr`.
// That filters out: bootstrap (lastOKAddr empty), spurious snapshot
// flips where Addr stayed the same, and re-entry into disagreement
// from disagreement (no OK transition).
//
// Caller MUST hold state.mu and pass `now` consistent with the
// updateSplitBrainSustained call in the same reconcile so the
// histogram observation matches the gauge that was just reset.
func (state *crQuorumState) observeFailoverIfAddrChanged(present, quorumOK bool, addr string, now time.Time) (time.Duration, string, bool) {
	if !present || !quorumOK {
		// Disagreement window — nothing to observe yet.
		return 0, "", false
	}
	// QuorumOK branch: candidate transition. Two short-circuits:
	//   - splitBrainSince empty → no prior disagreement → not a failover
	//   - lastOKAddr empty → first OK observation, bootstrap not failover
	if state.splitBrainSince == nil || state.lastOKAddr == "" {
		state.lastOKAddr = addr
		return 0, "", false
	}
	if state.lastOKAddr == addr {
		// QuorumOK reattached to the same master. Could be a brief
		// bootstrap-race blip that auto-resolved, OR a sentinel
		// hiccup. Not a failover — same primary throughout.
		state.lastOKAddr = addr
		return 0, "", false
	}
	// Real failover: prior OK Addr != current OK Addr, and we passed
	// through at least one !QuorumOK reconcile (splitBrainSince was
	// set). Duration is the time from the start of the disagreement
	// episode to now; trigger label is "sentinel_elected" because the
	// operator's reconcile path never directly drives Addr changes —
	// SENTINEL emits +switch-master, observer picks it up, snap.Addr
	// flips. Manual-failover and master-aware-rollout paths set
	// failoverInFlightLatches first; those callers can still
	// re-classify if a future refactor unifies the observation point.
	dur := now.Sub(*state.splitBrainSince)
	state.lastOKAddr = addr
	return dur, "sentinel_elected", true
}

// observeMasterInfoTimeout advances the per-CR master-info-timeout
// tracker. Returns the sustained-seconds value the caller stamps
// onto the MasterInfoTimeoutSeconds gauge. Caller MUST hold state.mu.
//
// Semantics mirror updateSplitBrainSustained: a successful INFO
// probe (`ok=true`) resets the tracker; a failed probe (`ok=false`)
// stamps the start timestamp on first occurrence and returns
// elapsed seconds on subsequent occurrences.
//
// The "frozen master" detection signature is `ok=false sustained
// for >liveness-grace-window` — kubelet's exec liveness probe
// catches genuine freezes within ~60s; the gauge surfaces cases
// that slip past (e.g. liveness disabled, CrashLoopBackOff-limited
// restart cadence too slow).
func (state *crQuorumState) observeMasterInfoTimeout(ok bool, now time.Time) float64 {
	// Every call is a fresh observation (probe ran or no-primary clear);
	// stamp the observed-at so the status read can tell a current
	// measurement from a stale latch left by an early-return pass.
	state.masterInfoObservedAt = now
	if ok {
		state.masterInfoTimeoutSince = nil
		return 0
	}
	if state.masterInfoTimeoutSince == nil {
		t := now
		state.masterInfoTimeoutSince = &t
		return 0
	}
	return now.Sub(*state.masterInfoTimeoutSince).Seconds()
}

const (
	// quorumLossSuppressionThreshold is the wall-clock duration of
	// continuous CKQUORUM=NOQUORUM observations required before the
	// operator suppresses SENTINEL command issuance for a CR
	// ("Degraded+QuorumLost" entry threshold).
	quorumLossSuppressionThreshold = 60 * time.Second

	// quorumRecoveryHysteresisPolls is the number of distinct
	// CKQUORUM=OK observer polls required to clear the suppression
	// flag — two-poll hysteresis to reject transient recovery
	// flickers. Distinct means the snapshot's poll stamp advanced:
	// reconcile passes re-reading the same poll count once, so churn-
	// driven back-to-back reconciles cannot clear the gate off a
	// single transient OK poll.
	quorumRecoveryHysteresisPolls = 2

	// quorumSuppressedRequeue paces the reconcile loop while the
	// quorum-suppression gate is active. Gate exit counts distinct
	// observer polls, but poll ticks do not trigger reconciles on
	// their own — on an otherwise-quiet steady-state cluster the
	// SentinelQuorum keep-alive bounds passes at its ~30s cadence,
	// so gate exit would take ~recoveryPolls keep-alive periods.
	// Pacing at the observer's poll cadence bounds it at roughly
	// recoveryPolls poll intervals after quorum actually recovers.
	// The value tracks the production poll interval; a test-override
	// poll interval below this does not tighten the pace — gate-exit
	// latency in such runs is bounded by this const.
	quorumSuppressedRequeue = sentinel.DefaultPollInterval

	// phase11DefaultTimeout caps Phase 11 sentinel-orchestration
	// network calls (TCP to every sentinel + the primary). Without
	// the cap a single unresponsive sentinel can stall the whole
	// reconcile and freeze the requeue cadence.
	phase11DefaultTimeout = 30 * time.Second
)

// QuorumSuppressionTunables overrides the suppression-gate floor
// values. Zero-valued fields fall back to the load-bearing safety
// defaults (`quorumLossSuppressionThreshold`, `quorumRecoveryHysteresisPolls`,
// `phase11DefaultTimeout`). Production callers leave the struct
// zero; the `--allow-test-overrides` flag (cmd/main.go) opens an
// env-driven path that lets shared-cluster e2e scenarios shorten
// these for the gate-entry / gate-exit scenarios where the
// production floors dominate wall time.
type QuorumSuppressionTunables struct {
	// LossThreshold overrides `quorumLossSuppressionThreshold`.
	// Zero (default) → use the const.
	LossThreshold time.Duration
	// RecoveryPolls overrides `quorumRecoveryHysteresisPolls`. Zero
	// (default) → use the const. Negative or zero after override
	// is rejected as invalid by parseTestOverrides.
	RecoveryPolls int
	// Phase11Timeout overrides Phase 11's per-reconcile orchestration
	// deadline. Zero (default) → use the const.
	Phase11Timeout time.Duration
}

// lossThreshold returns the effective gate-entry threshold.
func (t QuorumSuppressionTunables) lossThreshold() time.Duration {
	if t.LossThreshold > 0 {
		return t.LossThreshold
	}
	return quorumLossSuppressionThreshold
}

// recoveryPolls returns the effective hysteresis poll count.
func (t QuorumSuppressionTunables) recoveryPolls() int {
	if t.RecoveryPolls > 0 {
		return t.RecoveryPolls
	}
	return quorumRecoveryHysteresisPolls
}

// phase11Timeout returns the effective Phase 11 deadline.
func (t QuorumSuppressionTunables) phase11Timeout() time.Duration {
	if t.Phase11Timeout > 0 {
		return t.Phase11Timeout
	}
	return phase11DefaultTimeout
}

// updateQuorumSuppression mutates the per-CR suppression tracker for
// the given tri-state Quorum observation at wall-clock `now` and
// returns transition signals so the caller can emit one-shot
// QuorumLost / QuorumReached events. The caller MUST hold state.mu.
//
// polledAt is the snapshot's poll stamp (ObservedPrimary.LastPolledAt
// — advances only on live observer polls; pub/sub republishes carry
// it forward unchanged). The OK branch counts an observation toward
// the exit hysteresis only when this stamp is strictly newer than the
// last counted one, so the hysteresis counts distinct polls rather
// than reconcile passes. A zero stamp (no live poll yet) is never
// counted.
//
// The Lost branch opens a loss episode on the first observation
// (stamping quorumLostSince and quorumLostSincePoll) and crosses into
// suppression only when the wall-clock loss duration reaches
// lossThreshold AND the crossing observation's polledAt is strictly
// newer than the episode-opening poll — so aging alone (a frozen
// stamp re-read by reconcile churn while the observer is wedged)
// cannot cross without a live re-confirmation.
//
// Pure-computation: the function does not consult external state and
// may be unit-tested with synthetic now/polledAt sequences. Tunables
// overrides the load-bearing safety floors; pass a zero-valued
// tunables struct for the production defaults.
func (state *crQuorumState) updateQuorumSuppression(q sentinel.QuorumStatus, polledAt, now time.Time, tunables QuorumSuppressionTunables) (justEnteredSuppression, justExitedSuppression bool) {
	state.lastQuorum = q
	switch q {
	case sentinel.QuorumStatusUnknown:
		// Observer cannot reach enough peers to decide.
		// Preserve gate state across the window: neither advance
		// recovery hysteresis nor start loss-time. Without this
		// branch, transient pod-recreation windows during a
		// recovery rollout re-arm the gate even when the cluster
		// has quorum among reachable sentinels.
		return false, false
	case sentinel.QuorumStatusOK:
		// Clear the loss-onset stamp (a new loss episode starts
		// fresh) and advance the hysteresis counter only while
		// suppression is active.
		state.quorumLostSince = nil
		state.quorumLostSincePoll = time.Time{}
		if !state.suppressionActive {
			return false, false
		}
		// Count only OK observations backed by a not-yet-counted
		// poll: reconciles outrun the observer's poll cadence, so
		// back-to-back passes reading the same poll must advance
		// the hysteresis once, not once per pass. Strictly-newer
		// also rejects the zero stamp (no live poll yet).
		if !polledAt.After(state.quorumOKLastCountedPoll) {
			return false, false
		}
		state.quorumOKLastCountedPoll = polledAt
		state.quorumOKConsecutivePolls++
		if state.quorumOKConsecutivePolls >= tunables.recoveryPolls() {
			state.suppressionActive = false
			state.quorumOKConsecutivePolls = 0
			state.quorumOKLastCountedPoll = time.Time{}
			return false, true
		}
		return false, false
	default: // QuorumStatusLost
		// Reset the recovery counter (any OK streak is broken —
		// regardless of the observation's poll stamp, since a Lost
		// signal can also arrive via a pub/sub republish carrying
		// an older stamp) and advance the loss-duration tracker.
		state.quorumOKConsecutivePolls = 0
		state.quorumOKLastCountedPoll = time.Time{}
		if state.suppressionActive {
			// Already suppressed; no transition.
			return false, false
		}
		if state.quorumLostSince == nil {
			t := now
			state.quorumLostSince = &t
			state.quorumLostSincePoll = polledAt
			return false, false
		}
		if now.Sub(*state.quorumLostSince) >= tunables.lossThreshold() &&
			polledAt.After(state.quorumLostSincePoll) {
			// Cross into suppression only when the wall-clock loss floor is
			// met AND a live poll newer than the episode-opening poll has
			// re-confirmed the loss — a wedged observer re-reading one frozen
			// Lost stamp can never satisfy the second clause.
			state.suppressionActive = true
			return true, false
		}
		return false, false
	}
}

// +kubebuilder:rbac:groups=velkir.ioxie.dev,resources=valkeys,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velkir.ioxie.dev,resources=valkeys/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=velkir.ioxie.dev,resources=valkeys/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// serviceaccounts: per-CR data-plane SAs are SSA-applied and reaped by
// owner-reference GC, so the operator never issues delete itself.
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// Secrets are read cluster-wide (user-supplied auth.existingSecret /
// sentinelAuthSecret values live alongside the Valkey CR, which can
// be in any namespace), but WRITES are namespaced to the operator's
// own namespace via config/rbac/cert_writer_role.yaml — only the
// dynauth cert injector needs to write Secrets, and only its own
// webhook-cert Secret. Cluster-wide Secret WRITE is the canonical
// operator-CVE pattern (cf. CVE-2025-55196, CVE-2025-59303); the
// split is load-bearing.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch;delete
// pods/status patch is the ONLY subresource verb the operator needs;
// drives the replication-lag readiness gate.
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// Events for EventRecorder. The new client-go EventBroadcaster
// migrated to events.k8s.io/v1 — we keep both API groups so older
// and newer broadcaster paths work.
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// StorageClass is read by the PVC-resize detector to decide whether
// the backing class allows online volume expansion. Cluster-scoped
// because StorageClass is a cluster resource. Without the watch verb,
// the controller-runtime cache cannot start the informer and
// `r.Get(StorageClass)` blocks indefinitely on cache sync.
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// Webhook cert injector — manages caBundle on the configurations it
// owns via the velkir.ioxie.dev/inject-ca label selector.
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;patch;update
// PodMonitor lifecycle for per-CR exporter scraping. The CRD is
// optional infrastructure — when prometheus-operator isn't
// installed reconcilePodMonitor short-circuits on the NoMatchError,
// so these verbs are dormant until the CRD lands. The operator
// writes (create/update/patch/delete) and reads (get) PodMonitors
// it owns; list/watch are deliberately omitted since the operator
// doesn't run an informer for this type (per-CR write is
// reconciler-driven by spec.metrics.podMonitor.enabled, not by an
// external trigger).
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;create;update;patch;delete

// Reconcile drives a single CR through phases (in execution order):
// 0a (fetch) → 0b' (PVC-retention finalizer) → 0d' (PVC-loss gate)
// → 0c (terminal deletion) → 1 (ConfigMaps) → 4 (PVC resize) →
// 2 (data-plane STS) → 3 (sentinel infra: STS + Services +
// bootstrap CM, sentinel mode only) → 5 (Services) → 6 (PDB) →
// 7 (role labels) → 8 (replication-ready gates) → 9 (master-aware
// pod rollout) → 11 (sentinel orchestration, sentinel mode only,
// 30s timeout-wrapped) → 0e (single-shot annotation strip).
//
// Phase 3 + 11 are no-ops in standalone / replication mode; phases
// 7-9 short-circuit to bootstrap defaults in non-sentinel modes.
//
// Two-channel error model: controller-runtime sees `err` (drives the
// reconcile_errors_total metric and exponential backoff). The deferred
// status-update closure reads `statusErr` (drives Reconciled /
// Degraded / phase). They diverge at user-config-issue paths
// (e.g. auth Secret not yet created) where requeue is right but
// counting the reconcile as a runtime error would wrongly inflate the
// error metric. Phase failures set BOTH so controller-runtime retries
// AND the conditions surface flips.
//
// The status update fires from a deferred closure that wraps the whole
// body, so the conditions surface reflects current reconciler state on
// every termination path — not just the all-success path. A paused CR
// gets `phase=Paused`; a missing auth Secret surfaces as
// `Reconciled=False, Reason=ReconcileError` while controller-runtime
// sees `(RequeueAfter, nil)`.
//
// Reconcile is the orchestrator entry point — its cyclomatic
// complexity is fundamentally a function of the number of phases
// (each phase contributes one phase call + one err-check branch) plus
// the orchestrator-level early-returns (NotFound, paused, deleting,
// auth-secret-missing). Each phase is already its own helper; further
// extraction would split logic that belongs together. Suppress the
// gocyclo warning rather than chase it with synthetic decomposition.
//
//nolint:gocyclo
func (r *ValkeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	log := logf.FromContext(ctx).WithValues("valkey", req.NamespacedName)

	// `valkey_reconciliations_failed_total` — increments on
	// any err return from this Reconcile, with a coarse classified
	// reason. NotFound on the initial Get is intentionally not counted
	// (the CR is gone, no failure to attribute); everything else that
	// surfaces an err to controller-runtime ticks the counter.
	defer func() {
		if err == nil {
			return
		}
		operatormetrics.ReconciliationsFailedTotal.WithLabelValues(
			"Valkey", req.Namespace, req.Name, classifyReconcileError(err)).Inc()
	}()

	// Phase 0a — fetch.
	v := &valkeyv1beta1.Valkey{}
	if getErr := r.Get(ctx, req.NamespacedName, v); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			// The CR is gone from the cache; forgetCR drops its state bag
			// (incl. the reconcile mutex — dead weight otherwise, bounded only
			// by the unique-NamespacedName count over the process lifetime),
			// tears down the sentinel observer, and reaps its gauge series.
			// Safe here: no concurrent reconcile can hold the lock (the CR is
			// gone; any later reconcile under the same name LoadOrStores a
			// fresh mutex).
			r.forgetCR(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, getErr
	}

	// Phase 0a' — in-memory spec normalization. Rendered output
	// (valkey.conf / sentinel.conf bytes, config hashes, pod templates,
	// derived PDBs) must never depend on admission state: a CR admitted
	// while the defaulting webhook was unreachable (failurePolicy=Ignore),
	// or defaulted by an older operator version missing newer stamps,
	// would otherwise render differently than a fully-defaulted one and
	// config-roll its pods when the defaults later arrive. The
	// normalized view lives only in this reconcile's memory: every CR
	// write below is a metadata-only merge-patch whose spec sides cancel
	// out, so defaults are never persisted from here.
	defaults.ApplySpecDefaults(v)

	paused := v.Annotations[PauseAnnotation] == "true"
	deleting := v.DeletionTimestamp != nil
	// reachedSteadyState flips true only once the full apply sequence
	// (Phases 1-11) has run — i.e. NOT on any early-return that
	// short-circuits before Phase 11 (auth-missing backoff, PVC-loss
	// gate, list errors, paused). The status defer reads it so the
	// keep-alive requeue (whose consumers, the SQ re-stamp + master-info
	// probe, live in Phase 11) only tightens the cadence on passes where
	// those consumers actually ran — never defeating a deliberately
	// relaxed backoff on a short-circuit path.
	reachedSteadyState := false
	// Capture the PVC-retention finalizer presence pre-Phase-0b'.
	// Phase 0b' adds the finalizer when missing, so reading v.Finalizers
	// after that step would always show the finalizer present and
	// would erase the bootstrap-vs-disaster signal Phase 0d' (the
	// pvc-loss gate) depends on. The capture must happen here,
	// immediately after the fetch, before any in-memory modification
	// of Finalizers in this reconcile pass.
	hadPVCRetentionFinalizer := controllerutil.ContainsFinalizer(v, PVCRetentionFinalizer)

	// statusErr is the channel the deferred update reads. Distinct
	// from `err` so user-config-issue early-returns (auth secret
	// missing) can flip Reconciled/Degraded WITHOUT registering as
	// a runtime error in controller-runtime's metrics.
	var statusErr error

	// lockContended is set true only on the per-CR mutex TryLock-miss
	// early-return below. It gates the status-update defer to a no-op on
	// that one path: the reconcile that holds the mutex owns this round's
	// status write, so re-evaluating status here would race its Patch and
	// duplicate the STS/quorum reads + observer events the mutex serialises.
	lockContended := false

	// Status-update defer: fires on every reconcile termination after
	// the CR is in hand. Skipped only when the CR is gone (NotFound
	// returned above) or being deleted (status patches on a
	// terminating object can race the apiserver's delete sweep).
	defer func() {
		if deleting || lockContended {
			return
		}
		var observedSTS *appsv1.StatefulSet
		// Best-effort STS observation; nil on NotFound feeds the
		// `StatefulSetMissing` reason path in the Ready / Available
		// evaluators.
		sts := &appsv1.StatefulSet{}
		if getErr := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, sts); getErr == nil {
			observedSTS = sts
		} else if !apierrors.IsNotFound(getErr) {
			log.V(1).Info("status-update STS observation failed", "error", getErr.Error())
		}
		// Replica-readiness watchdog evaluation. Skip when paused —
		// the user has stopped the rollout loop intentionally, so a
		// stale Arm shouldn't fire events. The Check + emit + disarm
		// side wires defensively here so the contract is in place the
		// moment Arm starts stamping substates.
		watchdog, requeueIn := r.evaluateRolloutWatchdog(v, paused)
		result.RequeueAfter = mergeRequeue(result.RequeueAfter, requeueIn)
		// Per-CR SentinelQuorum aggregation. List error surfaces
		// as the zero Result, which the status evaluators treat as
		// Unknown — the next reconcile retries the list. Skipped
		// implicitly when the CR isn't in sentinel mode (the helper
		// short-circuits on Spec.Mode).
		sqResult := r.aggregateSentinelQuorum(ctx, v)
		// Steady-state requeue: the ready-converge active poll plus the
		// sentinel keep-alive (guarantees a reconcile within the SQ
		// freshness window so reconcileSentinelQuorumStatus re-stamps and
		// the master-info probe re-runs on a quiet-but-live cluster — so
		// PrimaryConfirmed doesn't latch Unknown and MasterLost keeps
		// clearing/confirming). mergeRequeue keeps any tighter hint, so
		// this only sets the steady-state floor.
		r.applyStatusRequeue(&result, v, observedSTS, paused, reachedSteadyState)
		result.RequeueAfter = r.suppressionPaceHint(result.RequeueAfter, v, reachedSteadyState)
		// Mirror Phase 7's split-brain guard into the Degraded
		// condition: a fresh observer snapshot reporting
		// QuorumOK=false on a sentinel-mode CR is the same signal
		// that suppresses role-label writes in `desiredRolesForCR`.
		// Surfacing it on the condition keeps `kubectl describe`
		// readers in sync with the event + metric pair without
		// requiring a Phase 7 round-trip.
		splitBrainActive := r.observeSplitBrain(v)
		noMasterAgreementActive := r.observeNoMasterAgreement(ctx, v)
		if updateErr := r.updateStatus(ctx, v, observedSTS, statusErr, paused, watchdog, sqResult, splitBrainActive, noMasterAgreementActive); updateErr != nil {
			// Don't shadow the original err with a status-update
			// failure — log it instead. The next reconcile will
			// re-attempt the status patch.
			//
			// `deleting` is captured at fetch time; the apiserver can
			// still set DeletionTimestamp between our fetch and this
			// patch, in which case the patch returns NotFound. That's
			// the documented race window — log at V(1) so it doesn't
			// surface as an error in the operational log; there's
			// nothing to retry, the CR is gone.
			//
			// Cleanup ownership: per-CR map entries (quorumState,
			// rolloutTriggerStates, replicasRolledTrackers,
			// fsmTransitionTrackers, authPasswordCache) are owned by the
			// initial-Get NotFound branch above (lines ~488-508). On the
			// NEXT reconcile
			// after a mid-reconcile delete, that branch fires, drops
			// every map entry for the CR, and returns nil. The V(1)
			// downgrade here is purely a log-level adjustment for the
			// races we previously misclassified as errors; the cleanup
			// invariant is unchanged. Other status-patch failure types
			// (Conflict on a live CR, Forbidden, apiserver auth)
			// continue to log at log.Error so genuine RBAC / contention
			// regressions stay visible.
			if apierrors.IsNotFound(updateErr) {
				log.V(1).Info("status update skipped: CR deleted mid-reconcile",
					"error", updateErr.Error())
			} else {
				log.Error(updateErr, "status update failed (original reconcile error preserved)")
			}
		}
	}()

	// Phase 0b — pause-annotation short-circuit. Returns handled=true to
	// short-circuit the reconcile when the CR is paused.
	if pauseResult, handled := r.handlePause(ctx, log, v, paused); handled {
		return pauseResult, nil
	}

	// Phase 0b' — finalizer add (non-deleting only).
	if finalizerErr := r.ensurePVCRetentionFinalizer(ctx, v, deleting); finalizerErr != nil {
		statusErr = finalizerErr
		err = finalizerErr
		return ctrl.Result{}, err
	}

	// Phase 0b'' — deprecation observer. Walks the registry and emits
	// one FieldDeprecated event per matching (CR, field) tuple per
	// process lifetime. Production registry is empty today (v1beta1
	// is additive-only); no-op in that case. Best-effort: emission
	// failures are absorbed by the Deprecator and never block reconcile.
	r.checkDeprecations(v)
	r.emitDeviations(v)

	// Per-CR mutex keeps concurrent reconciles of the same CR from
	// overlapping even when controller-runtime's MaxConcurrentReconciles
	// > 1. Acquisition is non-blocking (TryLock): a contended pass
	// requeues instead of parking the workqueue worker on the lock. The
	// `cleanupMutex` flag below tells the deferred unlock to also drop
	// the map entry once the CR's terminal-deletion path has run; we
	// hold the lock during that drop so a concurrent reconcile can't
	// observe the half-cleaned-up state (LoadOrStore-ing a fresh mutex
	// while the deletion path is mid-Apply).
	mu := r.lockFor(req.NamespacedName)
	if !mu.TryLock() {
		// A concurrent reconcile of the same CR holds the mutex. The
		// mutex is non-blocking by design — never park a workqueue worker
		// waiting on it. Record the contention and requeue on a short
		// floored cadence; the status-update defer no-ops via
		// lockContended so the holder's status write isn't raced.
		operatormetrics.ReconciliationsLockedTotal.WithLabelValues("Valkey", req.Namespace, req.Name).Inc()
		lockContended = true
		return ctrl.Result{RequeueAfter: lockContendedRequeue}, nil
	}
	cleanupMutex := false
	defer func() {
		mu.Unlock()
		if cleanupMutex {
			r.forgetCR(req.NamespacedName)
		}
	}()

	// Phase 0c — terminal deletion handling honours pvcRetentionPolicy.
	if deleting {
		if delErr := r.reconcileDeletion(ctx, v); delErr != nil {
			statusErr = delErr
			err = delErr
			return ctrl.Result{}, err
		}
		// Successful deletion — owner-refs are now in their final shape
		// for the GC cascade; mark the mutex for cleanup so the deferred
		// unlock drops it.
		cleanupMutex = true
		return ctrl.Result{}, nil
	}

	// Phase 0d — auth Secret resolution + content-hash rotation driver.
	// A missing Secret short-circuits (handled) so we don't materialise
	// pods that would crash-loop on auth; the resolved password is
	// threaded into the apply phases below. The redaction cleanup is
	// deferred HERE (not inside resolveAuth) so it fires at Reconcile's
	// end — covering every phase's log surface — and before the
	// status-update defer (LIFO), matching the prior inline ordering.
	authPassword, authCleanup, authResult, authHandled, authStatusErr, authErr := r.resolveAuth(ctx, log, v)
	if authCleanup != nil {
		defer authCleanup()
	}
	if authHandled {
		statusErr = authStatusErr
		err = authErr
		return authResult, err
	}

	// Phase 0d' — STS+PVC-absence safety gate. On a previously-
	// bootstrapped CR (finalizer was already present at fetch time)
	// with `pvcRetentionPolicy=Retain`, refuse silent recovery when
	// both the StatefulSet and every PVC matching the CR's selector
	// are gone. The user must opt in via
	// `velkir.ioxie.dev/accept-pvc-loss=true`; on consume, emit the
	// `pvc_loss_accepted` audit event with the requestor stamped by
	// the defaulter. The annotation is stripped by the existing
	// Phase 0e single-shot path on successful reconcile completion.
	proceed, gateErr := r.detectPVCLossAndGate(ctx, v, hadPVCRetentionFinalizer)
	if !proceed {
		statusErr = gateErr
		log.Info("pvc-loss gate refused recovery; awaiting accept-pvc-loss annotation",
			"err", gateErr.Error())
		return ctrl.Result{RequeueAfter: pvcLossGateRequeue}, nil
	}

	// Fetch the valkey data-plane pod list and the StatefulSet
	// once per reconcile and thread the read-only snapshots into the
	// observation + read phases below (deriveState, orphan-master scan,
	// pod rollout, sentinel orchestration). Previously each re-Listed
	// the same cache-backed pod set, DeepCopying it per pass. The two
	// phases that PATCH pod objects in place (Phase 7 role labels,
	// Phase 8 readiness gates) keep their own List — they need a
	// private mutable copy. Placed after the pvc-loss gate so the
	// fetch isn't wasted on the early-return recovery-refused path. A
	// List/Get failure here is defensive only: the cached reader
	// serves from a synced informer, so post-startup it returns cached
	// state (including empty) rather than erroring.
	valkeyPods, podsErr := r.listValkeyPods(ctx, v)
	if podsErr != nil {
		statusErr = podsErr
		err = podsErr
		return ctrl.Result{}, err
	}
	valkeySTS, stsErr := r.getValkeySTS(ctx, v)
	if stsErr != nil {
		statusErr = stsErr
		err = stsErr
		return ctrl.Result{}, err
	}

	// FSM observation + dispatch pass — derives the rollout state from the
	// once-per-reconcile pod/STS snapshot and fires the edge-triggered FSM
	// dispatches. Returns the state + facts the apply phases consume plus
	// any requeue hint from an FSM transition.
	state, facts, observeResult := r.observeRolloutState(ctx, log, v, req, valkeyPods, valkeySTS)
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, observeResult.RequeueAfter)

	// Phases 1-11 — the ordered apply sequence. Any phase failure sets
	// BOTH channels: controller-runtime sees the error (retries with
	// backoff, ticks the error metric) and the deferred update flips
	// Reconciled / Degraded. On error the accumulated requeue hint is
	// dropped (empty Result returned), matching the prior inline behaviour.
	applyResult, applyErr := r.applyDesiredState(ctx, log, v, state, facts, valkeyPods, valkeySTS, authPassword)
	if applyErr != nil {
		statusErr = applyErr
		err = applyErr
		return ctrl.Result{}, err
	}
	// Phases 1-11 all ran (incl. the keep-alive's consumers) — the
	// status defer may now contribute the keep-alive cadence.
	reachedSteadyState = true
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, applyResult.RequeueAfter)

	// Phase 0e — single-shot annotation self-clear. Strip after a
	// successful reconcile; failure paths above leave the annotation in
	// place so the next reconcile retries.
	if clrErr := r.clearSingleShotAnnotations(ctx, v); clrErr != nil {
		statusErr = fmt.Errorf("clearing single-shot annotations: %w", clrErr)
		err = statusErr
		return ctrl.Result{}, err
	}
	// Baseline steady-state watchdog: mergeRequeue keeps any
	// tighter hint from a substate machine; only fires when nothing
	// else has set one. Closes the missed-watch-event hole — without
	// this, a fully-converged reconcile returns RequeueAfter=0 and
	// the operator idles until the next informer event, which may
	// never arrive after a watch-reconnect bookmark drift.
	// Placed after every phase has had a chance to contribute its
	// own RequeueAfter hint (e.g. Phase 3 sentinel-infra deferral,
	// Phase 4 PVC resize, Phase 8 readiness-gate), so the baseline
	// only takes effect on otherwise-empty success paths.
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, baselineReconcileWatchdog)
	log.V(1).Info("phases: exit 0e (Reconcile returning)", "requeueAfter", result.RequeueAfter.String())

	// Return the accumulated `result` (not a fresh ctrl.Result{}) so the
	// requeue hints merged by the substate machines and phases along the
	// success path (Phase 4 PVC resize, Phase 3 sentinel-infra deferral)
	// actually fire. controller-runtime's workqueue honours
	// `result.RequeueAfter`; an explicit `ctrl.Result{}` would silently
	// drop every accumulated hint.
	return result, nil
}

// applyDesiredState runs the ordered apply sequence (Phases 1-11) that
// materialises the CR's desired state: ConfigMaps, PVC resize, ServiceAccounts
// + StatefulSet, sentinel infra, Services, PDB, PodMonitor, role labels,
// orphan-master scan, readiness gates, pod rollout, and sentinel orchestration.
// Returns the accumulated requeue hint on success; on any phase failure it
// returns an empty Result and the wrapped error (the caller mirrors it into
// both the runtime error and the status-update channel). Phase ordering is
// load-bearing — Phase 4 (PVC resize) runs before Phase 2 because the STS
// volumeClaimTemplates are immutable on Update.
func (r *ValkeyReconciler) applyDesiredState(
	ctx context.Context,
	log logr.Logger,
	v *valkeyv1beta1.Valkey,
	state orchestration.State,
	facts observedFacts,
	valkeyPods []corev1.Pod,
	valkeySTS *appsv1.StatefulSet,
	authPassword string,
) (ctrl.Result, error) {
	var result ctrl.Result

	// traceLog at V(1): per-phase enter/exit lines emitted only when
	// --zap-log-level >= 1 (or the operator's Pod is annotated to
	// raise verbosity). Useful for diagnosing reconcile-stall bugs
	// like the rc.9 Phase 11 hang without producing a log entry per
	// 5s reconcile in steady state.
	traceLog := log.V(1)
	traceLog.Info("phases: entering 1")
	cmHash, cmErr := r.reconcileConfigMaps(ctx, v)
	if cmErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 1: %w", cmErr)
	}
	traceLog.Info("phases: exit 1 enter 4")
	// Phase 4 runs BEFORE Phase 2: the substate machine needs to
	// orphan-delete the STS before Phase 2 attempts to reapply it,
	// because K8s makes StatefulSet.spec.volumeClaimTemplates
	// immutable on Update. Running Phase 2 first would either fail
	// the apply on the size change or silently keep the old PVC
	// capacity in the template.
	pvcRequeue, pvcErr := r.reconcilePVCResize(ctx, v)
	if pvcErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 4: %w", pvcErr)
	}
	// Substate machine wants the next reconcile at a specific cadence;
	// fold into ctrl.Result so controller-runtime polls at the requested
	// interval. Tighter cadence wins, but never below minRequeueFloor.
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, pvcRequeue)
	traceLog.Info("phases: exit 4 enter 2")
	if saErr := r.reconcileServiceAccounts(ctx, v); saErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 2: service accounts: %w", saErr)
	}
	if stsErr := r.reconcileStatefulSet(ctx, v, cmHash, state); stsErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 2: %w", stsErr)
	}
	traceLog.Info("phases: exit 2 enter 3")
	sentinelRequeue, sentinelErr := r.reconcileSentinelInfra(ctx, v, state, authPassword)
	if sentinelErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 3: %w", sentinelErr)
	}
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, sentinelRequeue)
	traceLog.Info("phases: exit 3 enter 5")
	if svcErr := r.reconcileServices(ctx, v); svcErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 5: %w", svcErr)
	}
	traceLog.Info("phases: exit 5 enter 6")
	if pdbErr := r.reconcilePDB(ctx, v); pdbErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 6: %w", pdbErr)
	}
	traceLog.Info("phases: exit 6 enter 6a")
	if pmErr := r.reconcilePodMonitor(ctx, v); pmErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 6a: %w", pmErr)
	}
	traceLog.Info("phases: exit 6a enter 7")
	if labelErr := r.reconcileRoleLabels(ctx, v, authPassword); labelErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 7: %w", labelErr)
	}
	traceLog.Info("phases: exit 7 enter 7a")
	// Phase 7a — orphan-master scan. Best-effort (per-pod failures log
	// + continue); never fails the reconcile, so no error to propagate.
	r.reconcileOrphanMasters(ctx, v, valkeyPods, authPassword)
	traceLog.Info("phases: exit 7a enter 8")
	gateRequeue, gateErr := r.reconcileReadinessGates(ctx, v, authPassword)
	if gateErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 8: %w", gateErr)
	}
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, gateRequeue)
	traceLog.Info("phases: exit 8 enter 9")
	rolloutRequeue, rolloutErr := r.reconcilePodRollout(ctx, v, facts.QuorumOK, valkeyPods, valkeySTS)
	if rolloutErr != nil {
		return ctrl.Result{}, fmt.Errorf("phase 9: %w", rolloutErr)
	}
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, rolloutRequeue)
	traceLog.Info("phases: exit 9 enter 11")

	// Phase 11 — sentinel orchestration. Mode-gated to `ModeSentinel`;
	// no-op for standalone / replication and for any sentinel-mode CR
	// whose sentinel pods haven't been created yet (the trigger path
	// tolerates the empty pod list as a clean no-op). Best-effort:
	// errors are logged via the per-trigger Event emission paths and
	// don't fail the reconcile. `state` is the just-derived FSM state
	// — passed in so the master-aware primary-rollout dispatcher
	// (offset-tolerance preflight + label-strip-before-FAILOVER)
	// can fire on StateRolloutPrimary without re-deriving.
	//
	// Wrapped with `r.Tunables.phase11Timeout()` (default 30s) because
	// Phase 11 talks to all sentinel pods + the primary via TCP — without
	// this cap a single unresponsive sentinel can block the entire
	// Reconcile and stall the requeue cadence (no fresh ctrl.Result
	// returned to controller-runtime). The default is the production
	// safety floor; cmd/main.go's `--allow-test-overrides` flag opens
	// an env-driven path so e2e scenarios can tighten the cap when
	// the floor dominates scenario wall time.
	//
	// Block scope so `defer orchCancel()` fires immediately on Phase 11
	// exit (panic-safe) — without the IIFE the cancel would only run on
	// the outer Reconcile return, leaking the timer goroutine across
	// the rest of the reconcile.
	func() {
		orchCtx, orchCancel := context.WithTimeout(ctx, r.Tunables.phase11Timeout())
		defer orchCancel()
		r.reconcileSentinelOrchestration(orchCtx, v, state, valkeyPods, authPassword)
	}()
	traceLog.Info("phases: exit 11 enter 0e")

	return result, nil
}

// observeRolloutState derives the rollout FSM state once per reconcile from
// the pod/STS snapshot and runs the edge-triggered FSM dispatches
// (rollout-trigger, replicas-rolled, abort/recovery, switch-master) plus the
// same-node co-location warning. deriveState is a pure function of its inputs
// and cannot fail, so this pass has no error return — it returns the derived
// state + facts the apply phases consume and the accumulated requeue hint from
// any FSM transition.
func (r *ValkeyReconciler) observeRolloutState(
	ctx context.Context,
	log logr.Logger,
	v *valkeyv1beta1.Valkey,
	req ctrl.Request,
	valkeyPods []corev1.Pod,
	valkeySTS *appsv1.StatefulSet,
) (orchestration.State, observedFacts, ctrl.Result) {
	var result ctrl.Result

	// FSM observation pass. Derive the rollout state once per reconcile
	// from observable cluster state (sentinel snapshot + the pod list +
	// STS revision fetched just above). deriveState is a pure function
	// of those inputs — it cannot fail, so the downstream FSM dispatch
	// is no longer gated on a derive error (the only prior error
	// sources, the pod List and STS Get, now fail the reconcile at the
	// fetch above before deriveState runs).
	state, facts := r.deriveState(v, valkeyPods, valkeySTS)
	log.V(1).Info("derive_state observation",
		"state", state,
		"podCount", facts.PodCount,
		"quorumOK", facts.QuorumOK,
		"failoverInFlight", facts.FailoverInFlight,
		"primaryAtTarget", facts.PrimaryAtTargetRevision,
		"replicasAtTarget", facts.AllReplicasAtTargetRevision,
	)

	// Rollout-trigger detection. Edge-detect on the STS UpdateRevision
	// != CurrentRevision transition (a fresh spec change just landed).
	// When the edge fires AND deriveState says StateSteady, hand the
	// trigger to the FSM via applyFSM(EventRolloutTrigger). When a
	// mid-rollout target swap fires (user edited spec while a previous
	// rollout was still rolling), dispatch EventSpecChanged so the
	// FSM emits T13 SpecChangeDeferred from StateRolloutReplicas.
	// The trigger detector is non-blocking — a detection error logs
	// at V(1) and the reconcile continues. `state` is reliable here:
	// deriveState is a pure function of inputs the fetch above already
	// validated, so there is no derive-error path left to suppress on.
	// Edge-triggered audit for a user-driven manual rollout: emit once
	// per `velkir.ioxie.dev/rollout-generation` value change. Independent
	// of the STS-revision rollout edge below (the annotation bump is the
	// user action; the revision change is its mechanism).
	r.maybeAuditManualRollout(ctx, v)

	sig, edgeErr := r.rolloutTriggerEdge(ctx, v)
	if edgeErr != nil {
		log.V(1).Info("rollout_trigger detection failed (non-blocking)", "error", edgeErr)
	} else {
		// Mutually exclusive by construction: sig.edge requires
		// !lastWasPending while sig.midRolloutChange requires
		// lastWasPending. At most one applyFSM call runs per
		// reconcile from this dispatch.
		if sig.edge {
			_, requeue, _ := r.applyFSM(v, state, orchestration.EventRolloutTrigger, orchestration.GuardCtx{QuorumOK: facts.QuorumOK})
			result.RequeueAfter = mergeRequeue(result.RequeueAfter, requeue)
		}
		if sig.midRolloutChange {
			_, requeue, _ := r.applyFSM(v, state, orchestration.EventSpecChanged, orchestration.GuardCtx{QuorumOK: facts.QuorumOK})
			result.RequeueAfter = mergeRequeue(result.RequeueAfter, requeue)
		}
	}

	// Replicas-rolled edge — detects the transition into
	// StateRolloutPrimary (every replica at target revision,
	// primary still stale). When the edge fires,
	// applyFSM(StateRolloutReplicas, EventAllReplicasRolled, ...)
	// emits ReplicasRolled once. The source state passed is
	// StateRolloutReplicas (where the FSM was conceptually before
	// the transition), NOT the just-derived StateRolloutPrimary —
	// the transition fires on the exit edge from RolloutReplicas.
	// Re-arms when the rollout completes and state returns to
	// Steady.
	if r.replicasRolledEdge(req.NamespacedName, state) {
		r.applyFSM(v, orchestration.StateRolloutReplicas, orchestration.EventAllReplicasRolled, orchestration.GuardCtx{QuorumOK: facts.QuorumOK})
	}

	r.fsmAbortAndRecoveryDispatch(ctx, v, req.NamespacedName, state, facts.QuorumOK)

	// T5 (UnexpectedFailover): an externally-driven primary switch
	// observed while the FSM is in Steady — sentinel promoted a new
	// primary the operator did not initiate, observed before Phase 7
	// relabels. Reads the same once-per-reconcile snapshot + pod list;
	// no-op outside Steady and bounded to one event per failover
	// episode by the per-CR edge tracker.
	if r.SentinelObserver != nil {
		r.fsmSwitchMasterDispatch(v, req.NamespacedName, state,
			r.SentinelObserver.Snapshot(req.NamespacedName), valkeyPods)
	}

	// Runtime same-node co-location warning. The soft anti-affinity
	// default can be overridden by the scheduler, so observe the actual
	// placement and warn when 2+ valkey pods share a node.
	r.warnOnSameNodeColocation(v, valkeyPods, componentValkey)

	return state, facts, result
}

// handlePause implements the Phase 0b pause-annotation short-circuit. When the
// CR is paused it bumps the gauge so a long-paused CR is visible on dashboards,
// emits the edge-triggered audit once on the transition into Paused, and
// returns (requeue, true) to short-circuit the reconcile — status writes still
// fire from Reconcile's deferred closure so `phase=Paused` is visible on
// `kubectl get`. When not paused it clears the gauge and returns (_, false).
func (r *ValkeyReconciler) handlePause(ctx context.Context, log logr.Logger, v *valkeyv1beta1.Valkey, paused bool) (ctrl.Result, bool) {
	if paused {
		operatormetrics.SetPaused(v.Namespace, v.Name, true)
		// Edge-triggered audit: emit once on the transition INTO the
		// paused state, not on every paused requeue. The prior persisted
		// Phase is the baseline; the deferred updateStatus stamps
		// Phase=Paused for subsequent reconciles, suppressing re-emission.
		if v.Status.Phase != orchestration.PhasePaused {
			auditReconciliationPaused(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, v.Generation)
		}
		log.V(1).Info("paused; skipping reconcile")
		return ctrl.Result{RequeueAfter: mergeRequeue(pausedRequeueAfter, baselineReconcileWatchdog)}, true
	}
	operatormetrics.SetPaused(v.Namespace, v.Name, false)
	return ctrl.Result{}, false
}

// resolveAuth implements Phase 0d: resolve the user's auth Secret, derive the
// master password once, register log redaction for it, and run the content-hash
// rotation driver. A missing Secret blocks the apply path so we don't
// materialise pods that would crash-loop on auth — a user-config issue, not a
// runtime error: the returned statusErr flips Reconciled/Degraded while err
// stays nil so reconcile_errors_total doesn't tick. Returns handled=true to
// short-circuit the reconcile (Secret missing or unreadable). On the resolved
// path it returns the password plus a redaction-cleanup func the caller MUST
// defer (so it fires at reconcile end, covering every phase's log surface).
// The rotation driver runs on the no-auth and resolved paths alike — best
// effort, never fatal.
func (r *ValkeyReconciler) resolveAuth(ctx context.Context, log logr.Logger, v *valkeyv1beta1.Valkey) (authPassword string, cleanup func(), result ctrl.Result, handled bool, statusErr error, err error) {
	var authSecret *corev1.Secret
	crKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	if v.Spec.Auth != nil && v.Spec.Auth.SecretName != "" {
		secret := &corev1.Secret{}
		if getErr := r.userSecretReader().Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.Auth.SecretName}, secret); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				// Requeue cadence backs off the longer the Secret stays
				// missing so the apiserver isn't hammered on long-running
				// user-config issues, while still polling at 30s during the
				// first few minutes when the user is likely about to apply it.
				requeue := r.observeMissingAuthSecret(crKey, time.Now())
				log.Info("auth secret not found, requeueing", "secret", v.Spec.Auth.SecretName, "after", requeue)
				return "", nil, ctrl.Result{RequeueAfter: requeue}, true, fmt.Errorf("auth secret %q not found", v.Spec.Auth.SecretName), nil
			}
			wrapped := fmt.Errorf("getting auth secret: %w", getErr)
			return "", nil, ctrl.Result{}, true, wrapped, wrapped
		}
		// Successful resolve clears the backoff clock so a future
		// recurrence starts fresh at 30s.
		r.stateFor(crKey).clearMissingAuthSeen()
		authSecret = secret
		// Earliest read site of the user's auth Secret in the reconcile
		// loop. The lookupAuthPassword paths surface the same warning,
		// but they run only on sentinel orchestration / readiness-gate
		// branches — a replication-mode CR with no readiness gate
		// would otherwise never trip the report. Dedup makes the
		// duplicate-call later in the same reconcile a no-op.
		reportShortAuthPassword(ctx, r.ShortAuthPasswordReporter, v, v.Spec.Auth.SecretName, string(authSecret.Data["password"]))
		// Resolve the password once and register redaction once; the
		// phases below receive authPassword instead of re-reading the
		// Secret. The returned cleanup deregisters at reconcile end.
		authPassword = string(authSecret.Data["password"])
		cleanup = r.registerAuthRedaction(ctx, v, authPassword)
	}

	// Auth-Secret content-hash hot-rotation driver. Compares the password-
	// content hash of the current Secret against
	// Status.Rollout.AuthRotation.ObservedSecretHash and either records
	// the first observation, defers (mid-failover), or drives RotateAuth
	// + classify-into-Succeeded/Failed/Partial. A Patch error is
	// non-fatal — the rotation driver will retry on the next reconcile.
	if rotateErr := r.maybeRotateAuth(ctx, v, authSecret); rotateErr != nil {
		log.V(1).Info("auth rotation driver returned error (non-fatal, will retry)",
			"err", rotateErr.Error())
	}
	return authPassword, cleanup, ctrl.Result{}, false, nil, nil
}

// forgetCR drops the per-CR in-memory state for a Valkey that no longer
// exists: the single perCR state-bag entry (which carries the reconcile
// mutex), the sentinel observer (with its ghost-reap debounce state),
// and the per-CR gauge series (full-registry sweep). It is the single
// teardown path shared by the cache-miss (NotFound) and terminal-
// deletion code paths; a new per-CR tracker is a field on perCRState and
// is torn down by the one Delete below, and a new per-CR gauge collector
// is reaped by the registry sweep — no second drifting site to wire.
// Idempotent: Delete, Remove, and the gauge sweep are all no-ops on a
// missing key. Callers must hold no lock a concurrent reconcile under the
// same key could re-acquire mid-teardown.
func (r *ValkeyReconciler) forgetCR(key types.NamespacedName) {
	r.perCR.Delete(key)
	r.forgetSentinelObserver(key)
	// Full-registry per-CR gauge sweep — the same call the StaleTrackerPruner
	// makes. The terminal-deletion path (Phase 0c, after the finalizer drops)
	// reaches forgetCR via the deferred mutex cleanup and is NOT followed by a
	// full reset in the same reconcile; it relies on the subsequent NotFound
	// reconcile, which a watch interruption can swallow. Reaping the whole
	// per-CR collector set here (not a hand-picked subset) closes that leak and
	// keeps forgetCR from being narrower than the pruner — a future per-CR
	// collector, incl. an alert-driving one, is covered automatically.
	// DeletePartialMatch keys on {namespace,name} only, so multi-label series
	// (topology `dimension`, per-pod exporter/observer) are reaped too.
	operatormetrics.ResetReconcileGauges(key.Namespace, key.Name)
}

// forgetSentinelObserver tears down the CR's sentinel observer
// goroutine tree (nil-guarded — non-sentinel deployments and most tests
// wire no observer manager). Without this, a deleted sentinel-mode CR's
// observer keeps polling the freed pod IPs for the leader-process
// lifetime, its password is never evicted from the redaction registry,
// and a recreated same-name CR inherits the stale ghost-reap debounce
// stamps Remove also clears.
func (r *ValkeyReconciler) forgetSentinelObserver(key types.NamespacedName) {
	if r.SentinelObserver == nil {
		return
	}
	r.SentinelObserver.Remove(key)
}

// observeMissingAuthSecret records the time this operator instance
// first saw the auth Secret missing for the given CR, then returns
// the next requeue duration per the backoff schedule. now is passed
// explicitly so tests can drive the clock without monkey-patching.
func (r *ValkeyReconciler) observeMissingAuthSecret(key types.NamespacedName, now time.Time) time.Duration {
	first := r.stateFor(key).observeMissingAuthFirstSeen(now)
	return backoffForMissingAuthSecret(now.Sub(first))
}

// backoffForMissingAuthSecret returns the requeue duration appropriate
// for an auth Secret that has been missing for the given elapsed
// time. Three tiers: 30s for the first 5 minutes (cover the
// "operator races user's kubectl apply" case), 1 minute for the next
// 25, then 5 minutes indefinitely. Bounded so a never-resolved
// missing Secret stops thrashing the apiserver after half an hour.
func backoffForMissingAuthSecret(elapsed time.Duration) time.Duration {
	switch {
	case elapsed < 5*time.Minute:
		return authSecretMissingRequeue
	case elapsed < 30*time.Minute:
		return time.Minute
	default:
		return 5 * time.Minute
	}
}

// recordEventf is a nil-safe wrapper around r.Recorder.Eventf. Tests
// often construct ValkeyReconciler without a Recorder (the
// controller-runtime EventRecorder is a manager-bound singleton, and
// most non-envtest unit tests don't run a manager). Centralising the
// nil-check here lets emit sites drop the per-call
// `if r.Recorder != nil { ... }` boilerplate that the linter can't
// enforce uniformly. `related` is always nil at our call sites today;
// we don't emit causal links across resources.
func (r *ValkeyReconciler) recordEventf(obj runtime.Object, eventType, reason, action, noteFmt string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, action, noteFmt, args...)
}

// reconcileSentinelOrchestration runs the sentinel-orchestration
// triggers + ensures the per-CR observer is registered with the
// sentinel.Manager.
//
// Two observable side effects, in order:
//
//  1. **Observer Ensure** — call `Manager.Ensure(ctx, cr,
//     masterName, password, endpoints)` so the per-CR observer
//     goroutine starts (or re-starts on endpoint change). Without
//     this the split-brain guard never fires (Snapshot stays
//     Present=false), and Trigger 3 has no signal.
//
//  2. **Stranded-sentinel recovery**: probe each sentinel's peer-list
//     and fire a targeted `RecoverStrandedSentinels` REMOVE + MONITOR
//     against any sentinel whose peer-list is empty — the rebuilt-pod
//     signature (a replaced sentinel boots with a fresh emptyDir
//     sentinel.conf and therefore no peers). Surviving sentinels with
//     intact peer-lists are left untouched. The deferral predicate
//     gates this naturally — when FailoverInFlight or under sustained
//     quorum-loss suppression, the pass defers and retries on the next
//     reconcile (no queue). Repeated no-progress surgeries against the
//     same sentinel back off exponentially and surface
//     `SentinelPeerLinkupStuck`.
//
// Both are no-ops in non-sentinel modes, when SentinelObserver
// is nil (test injection), when masterName is empty (defensive — the
// webhook rejects this), or when the sentinel pod list is empty
// (STS doesn't exist yet). Errors surface as Events via the
// package's emission paths and don't fail the outer Reconcile pass.

// applyRoleTransitionGate is fired immediately after a pod's role
// label flips. The two directions:
//
//   - `desired == primary`: strip `ReplicationReadyGate` from
//     `pod.Spec.ReadinessGates` (so kube-scheduler's
//     gate-aggregation removes the blocker) AND clear the
//     matching entry from `pod.Status.Conditions` (otherwise the
//     gate's last-True condition would still aggregate into Ready
//     after the spec.gate is gone — `containers/Ready=True` AND
//     `gate.condition=True` is what makes Ready=True today, so
//     dropping the spec.gate without clearing the condition
//     leaves a stale phantom condition the next gate-add would
//     re-pick-up).
//   - `desired == replica`: add `ReplicationReadyGate` to
//     `pod.Spec.ReadinessGates` if not already present. The
//     demoted pod re-enters the standard sync-gating lifecycle:
//     stays Unready until `master_link_status=up` AND lag is
//     below the threshold, per Phase 8's existing logic.
//
// Idempotent: re-running on a steady-state pod is a no-op (the
// spec/status set-membership checks short-circuit when the pod
// is already in the desired shape).
func (r *ValkeyReconciler) applyRoleTransitionGate(ctx context.Context, p *corev1.Pod, desired string) error {
	switch desired {
	case roleValuePrimary:
		// Strip from spec.readinessGates if present.
		if podHasReadinessGate(p, ReplicationReadyGate) {
			old := p.DeepCopy()
			p.Spec.ReadinessGates = stripReadinessGate(p.Spec.ReadinessGates, ReplicationReadyGate)
			if err := r.Patch(ctx, p, client.StrategicMergeFrom(old)); err != nil {
				return fmt.Errorf("strip gate from pod.spec: %w", err)
			}
		}
		// Clear matching condition from .status.conditions via
		// pods/status patch. The condition stays in the slice
		// as a potential ghost otherwise — present-and-True
		// from the replica era — and a future re-add of the
		// gate would aggregate the stale True into Ready. The
		// patch is unconditional (Patch is a no-op when the
		// condition is already absent).
		statusOld := p.DeepCopy()
		p.Status.Conditions = stripPodCondition(p.Status.Conditions, ReplicationReadyGate)
		if err := r.Status().Patch(ctx, p, client.StrategicMergeFrom(statusOld)); err != nil {
			return fmt.Errorf("clear gate condition from pod.status: %w", err)
		}
	case roleValueReplica:
		if podHasReadinessGate(p, ReplicationReadyGate) {
			return nil // already gated
		}
		old := p.DeepCopy()
		p.Spec.ReadinessGates = append(p.Spec.ReadinessGates, corev1.PodReadinessGate{
			ConditionType: ReplicationReadyGate,
		})
		if err := r.Patch(ctx, p, client.StrategicMergeFrom(old)); err != nil {
			return fmt.Errorf("add gate to pod.spec: %w", err)
		}
		// Don't pre-stamp the condition — Phase 8 owns its
		// True/False evaluation against actual replication state.
	}
	return nil
}

// podHasReadinessGate reports whether pod.Spec.ReadinessGates
// already contains the named condition type.
func podHasReadinessGate(p *corev1.Pod, ct corev1.PodConditionType) bool {
	for _, g := range p.Spec.ReadinessGates {
		if g.ConditionType == ct {
			return true
		}
	}
	return false
}

// stripReadinessGate returns a new slice with every entry whose
// ConditionType matches ct removed. Returns the input unchanged
// if no entries match (zero allocation common case).
func stripReadinessGate(gates []corev1.PodReadinessGate, ct corev1.PodConditionType) []corev1.PodReadinessGate {
	out := gates[:0:0]
	for _, g := range gates {
		if g.ConditionType != ct {
			out = append(out, g)
		}
	}
	return out
}

// stripPodCondition returns a new slice with every entry whose
// Type matches ct removed.
func stripPodCondition(conds []corev1.PodCondition, ct corev1.PodConditionType) []corev1.PodCondition {
	out := conds[:0:0]
	for _, c := range conds {
		if c.Type != ct {
			out = append(out, c)
		}
	}
	return out
}

func (r *ValkeyReconciler) reconcileSentinelOrchestration(ctx context.Context, v *valkeyv1beta1.Valkey, state orchestration.State, valkeyPods []corev1.Pod, password string) {
	if v.Spec.Mode != valkeyv1beta1.ModeSentinel || r.SentinelObserver == nil {
		return
	}
	if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" {
		return
	}

	pods, err := r.listSentinelPods(ctx, v)
	if err != nil {
		// Listing failure is not fatal — apiserver flakes happen.
		// Log + skip the trigger pass; the next reconcile retries.
		// Not surfaced as an Event to avoid catalog noise; the
		// reconciler's per-CR error log already captures it.
		logf.FromContext(ctx).V(1).Info("sentinel orchestration: listing sentinel pods failed",
			"err", err.Error())
		return
	}
	if len(pods) == 0 {
		// Pre-STS state OR transient post-deletion gap. No observer
		// to ensure, no UIDs to track, no quorum to read.
		return
	}

	// Runtime same-node co-location warning for the sentinel set.
	r.warnOnSameNodeColocation(v, pods, componentSentinel)

	// Per-pod SentinelQuorum resource creation. Best-effort: apply
	// errors are logged + ignored because the next reconcile retries,
	// and a missing SQ only delays quorum aggregation — it doesn't
	// break sentinel-mode operation. Surfaced as Event emission would
	// risk catalog noise on transient apiserver flakes; the per-CR
	// error log is enough.
	if sqErr := r.reconcileSentinelQuorums(ctx, v, pods); sqErr != nil {
		logf.FromContext(ctx).V(1).Info("sentinel orchestration: SentinelQuorum apply failed",
			"err", sqErr.Error())
	}

	endpoints := r.sentinelEndpointsFromPods(pods)
	if len(endpoints) == 0 {
		// Every pod missing a PodIP — likely just-scheduled, not yet
		// running. The next reconcile will see the IPs.
		return
	}

	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Ensure the observer is running for this CR.
	if ensureErr := r.SentinelObserver.Ensure(ctx, cr,
		v.Spec.Sentinel.MasterName, password, endpoints); ensureErr != nil {
		// ErrManagerNotStarted is a transient race with leader
		// election — controller-runtime starts Runnables in
		// parallel, so the reconciler may run before the sentinel
		// manager's Start has installed rootCtx. Skip the trigger
		// pass; the next reconcile retries. Any other Ensure error
		// (validation, etc.) is real and warrants a log so the
		// operator can spot misconfiguration without grovelling
		// the CR status.
		if !errors.Is(ensureErr, sentinel.ErrManagerNotStarted) {
			logf.FromContext(ctx).V(1).Info("sentinel observer Ensure failed",
				"cr", cr.String(), "err", ensureErr.Error())
		}
		return
	}

	// Suppression gate: track sustained CKQUORUM=NOQUORUM and flip
	// the per-CR suppressionActive flag the sentinel manager's
	// deferral predicate consults to defer stranded-sentinel
	// REMOVE + MONITOR surgery. Runs BEFORE the trigger helpers so
	// the in-flight gate is up-to-date
	// for this reconcile pass.
	r.updateQuorumSuppressionGate(ctx, v, cr)

	// Stamp the MasterInfoTimeoutSeconds gauge from a quick INFO probe
	// of the labelled-primary pod (tier-3). A non-zero sustained value
	// indicates "TCP up, valkey-server unresponsive" (the frozen-process
	// signature). The chart's exec liveness probe is the active recovery
	// path; this gauge surfaces cases that slip past the liveness restart
	// window. Side effect (since #680): the per-CR masterInfoTimeoutSince
	// this stamps is also read in updateStatus to drive Ready=False/
	// MasterLost once it exceeds the down-after window — do not move,
	// skip, or throttle this probe without accounting for that signal.
	// The probe scans for the role=primary label, which lives on the
	// VALKEY data-plane pods — passing the sentinel pod set here made
	// the probe a permanent no-op (primaryIP always empty), silently
	// disabling both the MasterLost signal and the resolver's
	// same-pass probe reuse.
	r.observeMasterInfoTimeout(ctx, cr, password, valkeyPods)

	// Wedge recovery for stranded sentinels. A replaced sentinel pod
	// (node loss, eviction, or operator-downtime recreation) boots
	// with a fresh emptyDir sentinel.conf and therefore an empty
	// peer-list — it cannot gossip its way back into the ring on its
	// own. The operator-driven REMOVE + MONITOR re-attaches only such
	// stranded sentinels, gated on a confirmed-live master and a
	// reachable quorum so a survivor quorum's own +odown → failover
	// election is never disturbed. Surviving sentinels with intact
	// peer-lists are left untouched — touching them is what wedges the
	// cluster during a master-down window.
	r.detectAndRecoverStrandedSentinels(ctx, v, cr, password, endpoints)

	// Master-aware primary-rollout dispatcher: offset-tolerance
	// preflight + label-strip-before-FAILOVER. No-op unless the FSM-
	// derived state is StateRolloutPrimary AND the observer reports
	// QuorumOK; runs after the bootstrap-reset trigger so a fresh
	// cluster bootstrap has had a chance to land its initial primary
	// label before this path inspects role labels.
	r.runPrimaryRolloutDispatch(ctx, v, cr, state, v.Spec.Sentinel.MasterName, password, endpoints)

	// Per-pod SentinelQuorum status writer. Reads the
	// observer's latest per-endpoint observation set and stamps
	// each SQ.Status with what its corresponding sentinel pod
	// reported. Pre-first-poll race window: EndpointObservations
	// returns nil and the writer is a no-op (SQ.Status stays
	// empty until the next reconcile). Best-effort: an Apply
	// failure here is logged but doesn't fail the orchestration
	// pass, same posture as the SQ create path above. valkeyPods is
	// the read-only pod snapshot threaded in from the once-per-
	// reconcile fetch.
	if statusErr := r.reconcileSentinelQuorumStatus(ctx, v, pods, valkeyPods); statusErr != nil {
		logf.FromContext(ctx).V(1).Info("sentinel orchestration: SentinelQuorum status apply failed",
			"err", statusErr.Error())
	}

	// Topology-mismatch hygiene: fold the sentinel-known peer / replica
	// counts (read from the same pull-tick reply, no new round-trip) into
	// the per-dimension debounce and emit the once-per-episode event. The
	// gauge + condition are written from the same read in updateStatus.
	r.observeSentinelTopology(ctx, v, cr, state, endpoints, valkeyPods)
}

// endpointObservations returns the sentinel observer's per-endpoint
// observation snapshot for cr, applying the test-injection seam. Nil
// SentinelObserver (test / pre-startup) yields nil, which every consumer
// treats as no-data.
func (r *ValkeyReconciler) endpointObservations(cr types.NamespacedName) []sentinel.EndpointObservation {
	if r.endpointObservationsFn != nil {
		return r.endpointObservationsFn(cr)
	}
	if r.SentinelObserver == nil {
		return nil
	}
	return r.SentinelObserver.EndpointObservations(cr)
}

// observeSentinelTopology folds this pass's sentinel-known peer / replica
// counts (already fetched by the observer's 10s pull tick — no new
// round-trip) into the per-dimension mismatch-hygiene debounce and emits
// the once-per-episode SentinelTopologyMismatch Warning event on the
// debounce edge. Observation-only: it issues no sentinel or data-plane
// command. The gauge and the SentinelTopologyReconciled condition are
// written from the same freshness-gated read in updateStatus.
//
// The two dimensions aggregate and gate differently. The peer dimension
// uses ANY-short (min KnownSentinels over the CountsValid endpoints < expected)
// so a single stranded or partial-gossip sentinel is surfaced; the replica
// dimension uses MAX (max KnownReplicas) so one lagging sentinel can't
// false-fire, and adds a readyValkeyPods==spec.valkey.replicas eligibility
// clause the peer dimension does not (peer gossip has no data-plane
// dependency). Aggregation runs ONLY over CountsValid endpoints, and an
// empty valid set makes both dimensions ineligible (no-data → prune, never
// a false full-deficit).
func (r *ValkeyReconciler) observeSentinelTopology(ctx context.Context, v *valkeyv1beta1.Valkey, cr types.NamespacedName, state orchestration.State, endpoints []sentinel.Endpoint, valkeyPods []corev1.Pod) {
	obs := r.endpointObservations(cr)
	var newestAt time.Time
	validCount := 0
	minSentinels, maxReplicas := 0, 0
	for i := range obs {
		if obs[i].At.After(newestAt) {
			newestAt = obs[i].At
		}
		if !obs[i].CountsValid {
			continue
		}
		if validCount == 0 {
			minSentinels = obs[i].KnownSentinels
			maxReplicas = obs[i].KnownReplicas
		} else {
			if obs[i].KnownSentinels < minSentinels {
				minSentinels = obs[i].KnownSentinels
			}
			if obs[i].KnownReplicas > maxReplicas {
				maxReplicas = obs[i].KnownReplicas
			}
		}
		validCount++
	}

	now := r.now()
	fresh := !newestAt.IsZero() && now.Sub(newestAt) <= topologyObservationFreshnessWindow
	base := !valkeyRollActive(state) &&
		!r.IsFailoverInFlight(cr) &&
		!r.IsSentinelSuppressed(cr) &&
		len(endpoints) == int(v.Spec.Sentinel.Replicas) &&
		fresh &&
		validCount >= 1

	// Sentinels dimension: ANY-short (min over valid < expected). No
	// data-plane-readiness clause — peer gossip is independent of it.
	expS := int(v.Spec.Sentinel.Replicas) - 1
	sentEligible, sDef := false, 0
	if expS >= 1 {
		sDef = max(0, expS-minSentinels)
		sentEligible = base
	}

	// Replicas dimension: MAX (max over valid < expected), plus the
	// data-plane readiness clause so a still-converging valkey roll
	// doesn't false-fire a replica deficit.
	expR := int(v.Spec.Valkey.Replicas) - 1
	replEligible, rDef := false, 0
	if expR >= 1 {
		rDef = max(0, expR-maxReplicas)
		readyValkey := 0
		for i := range valkeyPods {
			if podReady(&valkeyPods[i]) {
				readyValkey++
			}
		}
		replEligible = base && readyValkey == int(v.Spec.Valkey.Replicas)
	}

	q := r.stateFor(cr).quorumTracker()
	sentFire, replFire := q.foldTopologyMismatch(sentEligible, sDef, replEligible, rDef, now)
	if sentFire || replFire {
		if active, aSDef, aRDef := q.topologyMismatchActiveOrExpire(now); active {
			detail := orchestration.SentinelTopologyDetail(aSDef, aRDef)
			r.recordEventf(v, corev1.EventTypeWarning, string(events.SentinelTopologyMismatch),
				"SentinelTopologyMismatch", "sentinel topology below spec: %s", detail)
			logf.FromContext(ctx).V(2).Info("sentinel topology below spec",
				"cr", cr.String(), "detail", detail)
		}
	}
}

// listSentinelPods returns every sentinel pod the operator owns for
// this CR, identified by the CR-name + componentSentinel labels.
// Returns an empty slice (no error) when the sentinel STS hasn't
// been created yet.
func (r *ValkeyReconciler) listSentinelPods(ctx context.Context, v *valkeyv1beta1.Valkey) ([]corev1.Pod, error) {
	return listSentinelPodsFor(ctx, r, v)
}

// listValkeyPods returns every valkey data-plane pod the operator
// owns for this CR, identified by the CR-name + componentValkey
// labels. Returns an empty slice (no error) when the StatefulSet
// hasn't created any pods yet.
//
// Fetched once per reconcile and threaded read-only into the
// observation + read phases (deriveState, orphan-master scan, pod
// rollout, sentinel orchestration) that previously each re-Listed —
// and DeepCopied — the same cache-backed set. Callers MUST treat the
// result as immutable: the phases that PATCH pod objects in place
// (Phase 7 role labels, Phase 8 readiness gates) keep their own List
// for a private mutable copy, since sharing this snapshot would leak
// their in-place edits into later phases.
func (r *ValkeyReconciler) listValkeyPods(ctx context.Context, v *valkeyv1beta1.Valkey) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return nil, fmt.Errorf("listing valkey pods: %w", err)
	}
	return pods.Items, nil
}

// getValkeySTS fetches the CR's StatefulSet once per reconcile,
// returning (nil, nil) when it doesn't exist yet so callers treat
// absence as a first-class state without re-checking IsNotFound. The
// returned object is threaded read-only into deriveState and
// reconcilePodRollout, which only read Status.{Update,Current}Revision
// — fields the kube StatefulSet controller updates asynchronously, so
// a single top-of-pass snapshot is consistent for every read in the
// pass.
func (r *ValkeyReconciler) getValkeySTS(ctx context.Context, v *valkeyv1beta1.Valkey) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting statefulset: %w", err)
	}
	return sts, nil
}

// sentinelEndpointsFromPods builds the Endpoint slice the
// sentinel.Manager surface consumes — one per pod with a non-empty
// PodIP. Pod name + IP:port is enough; the package never queries
// kube state itself.
func (r *ValkeyReconciler) sentinelEndpointsFromPods(pods []corev1.Pod) []sentinel.Endpoint {
	return sentinelEndpointsFromPodList(pods)
}

// observeMasterInfoTimeout runs one bounded INFO-replication probe
// against the labelled-primary pod and stamps the
// MasterInfoTimeoutSeconds gauge with the contiguous-failure
// duration. Purely passive observability (tier-3) — the only
// active recovery path for a frozen master is kubelet restarting
// the container via the exec liveness probe.
//
// Resolution:
//   - No pod labelled role=primary: gauge=0 (no measurement target;
//     not a frozen-master signal, the wedge / bootstrap paths own
//     this state).
//   - Labelled primary pod present + INFO probe succeeds: gauge=0
//     (master responsive).
//   - Labelled primary pod present + INFO probe fails (timeout,
//     wire error, malformed reply): increment the sustained-time
//     tracker; gauge reports elapsed.
//
// Probe timeout is bounded to 2s per pod so a frozen master can't
// stall the reconcile budget. The reconcile cadence (~5s) bounds
// the gauge's update frequency; the gauge represents "seconds
// since the last successful INFO probe" with reconcile-interval
// granularity.
func (r *ValkeyReconciler) observeMasterInfoTimeout(ctx context.Context, cr types.NamespacedName, password string, pods []corev1.Pod) {
	var primaryIP string
	for i := range pods {
		p := &pods[i]
		if p.Labels[RoleLabel] != roleValuePrimary {
			continue
		}
		if p.Status.PodIP == "" {
			continue
		}
		primaryIP = p.Status.PodIP
		break
	}
	if primaryIP == "" {
		// No labelled primary pod (bootstrap / wedge / an operator-driven
		// rollout that stripped the label). Reset the gauge AND clear the
		// per-CR timeout latch: "no labelled primary" is "no measurement",
		// not "still failing", so MasterLost must not persist across a
		// label-stripped window. A genuinely dead master keeps its
		// role=primary label, so this only resets the intentional
		// no-primary windows.
		operatormetrics.MasterInfoTimeoutSeconds.WithLabelValues(cr.Namespace, cr.Name).Set(0)
		state := r.stateFor(cr).quorumTracker()
		state.mu.Lock()
		state.observeMasterInfoTimeout(true, r.now())
		state.masterInfoRoleDisclaimed = false
		state.mu.Unlock()
		return
	}

	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	addr := net.JoinHostPort(primaryIP, strconv.Itoa(valkey.DefaultPort))
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	probeState, probeErr := checker.CheckLag(probeCtx, addr, password)
	cancel()

	now := r.now()
	state := r.stateFor(cr).quorumTracker()
	state.mu.Lock()
	sustained := state.observeMasterInfoTimeout(probeErr == nil, now)
	// Role capture for the same-pass reuse in observedMasterIPForCR:
	// a labeled primary that answers INFO but self-reports a
	// non-master role has been demoted under the label (post-failover,
	// relabel pending) and must not be resolved as the master.
	state.masterInfoRoleDisclaimed = probeErr == nil && roleDisclaimsMastership(probeState.Role)
	state.mu.Unlock()
	operatormetrics.MasterInfoTimeoutSeconds.WithLabelValues(cr.Namespace, cr.Name).Set(sustained)
}

// detectAndRecoverStrandedSentinels probes each sentinel endpoint
// for peer-list state and dispatches a targeted RESET + MONITOR at
// any sentinel reporting `num-other-sentinels = 0` (empty peer-
// list — the canonical "rebuilt pod with no preserved peer state"
// signature). Surviving sentinels with non-empty peer-lists are
// left alone; wiping their state is what caused the rc.10 / rc.11
// cascading wedge.
//
// Runs every reconcile. Cheap when the cluster is healthy (one
// SENTINEL SENTINELS round-trip per endpoint; sentinel composes
// the reply from in-memory state). The minority gate inside
// Manager.RecoverStrandedSentinels also refuses to fire when fewer
// than a quorum of sentinels are reachable, so a reachable minority
// cannot point sentinels at a possibly-stale master during a partition.
//
// Gate: only runs when observedMasterIP is non-empty AND at least
// one sentinel pod is observed in the endpoint set. Operator-side
// observability for the SentinelStrandedRecovery / SentinelPeerLinkupStuck
// events comes from Manager.RecoverStrandedSentinels and the read-
// back follow-up in detectStrandedPeerLinkupStuck below.
// shouldBypassQuorumDeferral reports whether stranded-sentinel recovery
// may run despite an active deferral predicate. True only when the gate
// is held by quorum-loss suppression ALONE (the state this recovery
// repairs — deferring it deadlocks: the gate waits for quorum, quorum
// waits for the repair) and NOT by an in-flight failover (whose
// config-epoch propagation the surgery must never race). The manager's
// PING + failover-in-progress surgery gates apply regardless of this
// flag — this only governs the suppression-gate deferral.
func (r *ValkeyReconciler) shouldBypassQuorumDeferral(cr types.NamespacedName) bool {
	return r.IsSentinelSuppressed(cr) && !r.IsFailoverInFlight(cr)
}

func (r *ValkeyReconciler) detectAndRecoverStrandedSentinels(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	password string,
	endpoints []sentinel.Endpoint,
) {
	if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" || len(endpoints) == 0 {
		return
	}
	// Resolve the master + survey and surface observation ABOVE the
	// stranded-surgery cooldown gate below: the survey, dual-master
	// surfacing, and the zero-master recovery election each carry their
	// own independent gating (the election its own recoveryPromotionCooldown)
	// and must NOT be paced by the backed-off stranded interval. A
	// compound failure — a sentinel linkup-stuck AND the cluster's
	// master lost — must still recover write-availability on the
	// election's cadence, not wait out the (up to base<<max) stranded
	// backoff.
	resolveMaster := r.observedMasterFn
	if resolveMaster == nil {
		resolveMaster = r.observedMasterIPForCR
	}
	masterIP, survey := resolveMaster(ctx, v, password)
	// Dual-master surfacing: when the survey's dial sweep ran, it is
	// the operator's only observation point for >=2 self-reported
	// masters outside a failover section — no path below acts on that
	// shape (the resolver returns "" on it by design), so surface it
	// here: condition stamp + gauge + edge-gated Warning event.
	r.observeDualMasterFromSurvey(v, cr, survey)
	if masterIP == "" {
		// No safe RESET target. When the survey shows the total-wedge
		// signature (zero self-reported masters, every replica and the
		// sentinel quorum pointing at corpses), the recovery election
		// promotes a replica so the NEXT pass has a target; otherwise
		// the per-reconcile detector simply retries.
		r.maybeRecoveryPromote(ctx, v, cr, password, survey, endpoints)
		return
	}

	// Second arming path for the zero-master recovery election: sustained
	// CKQUORUM=NOQUORUM (the suppression gate) forces a fully-dialed survey
	// through the same promotion guard chain even when a masterIP resolved via
	// a cheap short-circuit, so detection no longer depends on the single
	// masterIP=="" survey verdict. Placed above the stranded-surgery cooldown
	// so it runs on the election's own cadence, never paced by the surgery
	// probe interval. Its own arm plus recoveryDetectionCooldown bound the
	// cost; it authorizes no promotion the guard chain would not already permit.
	r.maybeRecoveryPromoteOnQuorumLoss(ctx, v, cr, password, survey, endpoints)

	// Base-cooldown probe gate: prevent back-to-back RESET while the
	// prior pass's gossip-bootstrap is still in flight. Without this gate,
	// a 5s reconcile cadence vs a 2-5s gossip window can fire RESET on the
	// same sentinel multiple times before its peer-list repopulates,
	// returning -ERR Duplicated master name on MONITOR. This gate is now
	// BASE cadence (no per-CR backoff): the SentinelsAll classification
	// probe fans out at most once per base window regardless of any
	// address's backoff depth, so a fresh strand fires at base cadence
	// even while a different sentinel is deeply backed off. Per-address
	// re-wipe pacing is applied inside the manager via the skip-set
	// (computed below). Gates ONLY the surgery below — never the
	// survey/election above.
	state := r.stateFor(cr).quorumTracker()
	if state.strandedProbeCoolingDown(r.now()) {
		return
	}

	bypassQuorumDeferral := r.shouldBypassQuorumDeferral(cr)

	// o_down gate for the ghost-reap class: the manager's PING +
	// failover-in-progress surgery gates do not cover the
	// +sdown→+odown vote-gathering window before a sentinel-driven
	// election formally starts (the operator's PING is a different
	// network path; failover_in_progress is set only after a leader is
	// elected). The observer's o_down view IS that signal — a FRESH
	// o_down means an election may be brewing, so disallow reaping a
	// gossiping survivor this pass. The veto reads BOTH truth sources:
	// the pubsub +odown/-odown last-seen map AND the pull-tick
	// rising-edge first-seen map (ODownPull), so a lost -odown frame is
	// reconciled away by the next poll instead of lingering. Each source
	// ages out at one failover-timeout — an entry older than that no
	// longer vetoes, because past one failover-timeout any genuine
	// election has provably aborted at least once. Empty-peer stranded
	// recovery still proceeds. A boot-race (!Present) snapshot is treated
	// as unknown → disallow.
	snap := r.SentinelObserver.Snapshot(cr)
	allowGhostReap := ghostReapAllowed(snap, v.Spec.Sentinel.FailoverTimeout, time.Now())

	// The re-point class (and its per-pass probe fan-out) arms only
	// when the snapshot suggests corpse-monitoring; a nil live set
	// disables it in the manager.
	var repointLiveIPs map[string]struct{}
	if repointClassArmed(snap, liveValkeyIPs(survey)) {
		repointLiveIPs = liveValkeyIPs(survey)
	}

	// Per-address re-wipe pacing: skip the wedged empty-peer sentinels
	// whose lengthened per-address cadence has not elapsed. They are still
	// probed (cheap classify read, so a NetworkPolicy-fix / recovery is
	// detected within one base cycle) but not re-wiped, while a fresh
	// strand on the same CR is wiped in the same pass.
	skip := state.strandedSkipSet(r.now())

	// Dispatch via the injectable seam (nil-safe) so a controller test can
	// assert the computed skip-set is passed without a live manager.
	recover := r.recoverStrandedFn
	if recover == nil {
		recover = r.SentinelObserver.RecoverStrandedSentinels
	}
	out := recover(
		ctx,
		sentinel.InitialResetTarget{
			CR:         cr,
			MasterName: v.Spec.Sentinel.MasterName,
			MasterIP:   masterIP,
			Port:       int(defaultValkeyPort),
			Quorum:     int(v.Spec.Sentinel.Quorum),
			Tuning: sentinel.MasterTuning{
				DownAfterMilliseconds: v.Spec.Sentinel.DownAfterMilliseconds,
				FailoverTimeout:       v.Spec.Sentinel.FailoverTimeout,
				ParallelSyncs:         v.Spec.Sentinel.ParallelSyncs,
			},
			Endpoints:      endpoints,
			Password:       password,
			AllowGhostReap: allowGhostReap,
			// StaleEpoch's destination-election veto gates on the
			// identical "no fresh +odown on the CR primary within one
			// failover-timeout" signal ghost-reap already computed.
			AllowStaleEpochRepoint: allowGhostReap,
			LiveValkeyIPs:          repointLiveIPs,
			SkipStrandedAddrs:      skip,
		},
		bypassQuorumDeferral,
	)
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, out, endpoints, masterIP)
}

// applyStrandedRecoveryOutcome folds one RecoverStrandedSentinels result
// into the per-CR no-progress tracker + events. Separated from the I/O
// dispatch above so the outcome wiring — the probe-cadence stamp (gated
// on out.Probed), Healthy → clear, gate-defer → leave the no-progress
// state untouched (NO freshness refresh, so a persistent defer ages the
// stuck flag out), the EmptyPeerStranded (wiped) + SkippedStranded
// (paced) feed, and the SentinelPeerLinkupStuck / stranded / re-point
// events — is unit-testable with a synthetic result and no live sentinel
// manager.
func (r *ValkeyReconciler) applyStrandedRecoveryOutcome(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	state *crQuorumState,
	out sentinel.StrandedRecoveryResult,
	endpoints []sentinel.Endpoint,
	masterIP string,
) {
	now := r.now()
	// Stamp the base-cadence probe clock whenever classification actually
	// ran (Probed) — including healthy, wipe, skip-only, and the three
	// post-classification defers — so the SentinelsAll probe debounces to
	// base cadence. The two pre-classification early-returns leave
	// Probed=false → no wasted stamp → surgery resumes on the next
	// reconcile once the pre-classification condition clears.
	if out.Probed {
		state.mu.Lock()
		state.strandedRecoveryLastFired = now
		state.mu.Unlock()
	}
	if out.Healthy {
		// Authoritative healthy classification (peer-lists intact):
		// clear any linkup-stuck state we were tracking.
		state.clearStrandedLinkupStuck()
		return
	}
	if len(out.Stranded) == 0 && len(out.Repointed) == 0 && len(out.SkippedStranded) == 0 && len(out.StaleEpochRepointed) == 0 {
		// Gate-deferred (minority / PING / failover / deferral
		// predicate): no wipe fired, nothing was paced, and no
		// authoritative verdict — leave the no-progress state entirely
		// alone; the next pass re-evaluates. The stuck flag is NOT
		// refreshed here: a genuine stuck episode's own surgeries (or a
		// skip-only pass, below) refresh it every base probe, while a
		// persistent defer that stops surgery from re-confirming the wedge
		// correctly ages the flag out (the operator can't assert
		// linkup-stuck it can no longer verify — the current condition,
		// e.g. QuorumLost, should surface instead). The SkippedStranded==0
		// term keeps a skip-only pass (which DID re-confirm the wedge, just
		// without wiping) out of this branch.
		return
	}

	// Read-back / no-progress tracking for the empty-peer stranded class
	// only (out.EmptyPeerStranded = the WIPED subset, plus SkippedStranded
	// = the paced subset carried forward; NOT out.Stranded which also
	// carries the ghost-reap target): the re-point and ghost-reap classes
	// had intact peer-lists and converge on a different signal, so a "peer
	// count still 0" check does not apply to them. A wiped stranded
	// sentinel whose peer-list did not rebuild — or whose auth-pass
	// verification failed — is making no progress; the detector advances
	// its per-address count, backs off its re-wipe cadence, and surfaces
	// SentinelPeerLinkupStuck past the threshold. A skipped sentinel is
	// held in place (count + clock preserved) so its pace holds while
	// still refreshing freshness. Called on every dispatch (even a
	// repoint-only pass) so a recovered stranded class resets the flag.
	addrByName := make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		addrByName[ep.Name] = ep.Addr
	}
	stuck, fireStuck, backoff := state.detectStrandedPeerLinkupStuck(
		sentinelNamesToAddrs(out.EmptyPeerStranded, addrByName),
		sentinelNamesToAddrs(out.SkippedStranded, addrByName),
		sentinelNamesToAddrs(out.AuthFailures, addrByName),
		now,
	)
	if fireStuck {
		r.recordEventf(v, corev1.EventTypeWarning,
			string(events.SentinelPeerLinkupStuck), "SentinelPeerLinkupStuck",
			"sentinel(s) at %v still report an empty peer-list after %d consecutive REMOVE + MONITOR surgeries; gossip cannot rebuild (auth failure verifying, or a NetworkPolicy blocking __sentinel__:hello). Backing off surgery (level %d); manual intervention is likely required.",
			stuck, strandedSurgeryStuckThreshold, backoff)
		auditSentinelReset(ctx, cr, stuck, "linkup-stuck")
	}

	if len(out.Stranded) > 0 {
		r.recordEventf(v, corev1.EventTypeNormal,
			string(events.SentinelStrandedRecovery), "SentinelStrandedRecovery",
			"stranded sentinel(s) %v detected; fired REMOVE + MONITOR (masterIP=%s)",
			out.Stranded, masterIP)
		auditSentinelReset(ctx, cr, out.Stranded, "degraded")
	}
	if len(out.Repointed) > 0 {
		r.recordEventf(v, corev1.EventTypeWarning,
			string(events.SentinelDeadMasterRepoint), "SentinelDeadMasterRepoint",
			"sentinel(s) %v were monitoring a master address matching no live pod; re-pointed at %s via REMOVE + MONITOR",
			out.Repointed, masterIP)
		auditSentinelReset(ctx, cr, out.Repointed, "dead-master-repoint")
	}
	if len(out.StaleEpochRepointed) > 0 {
		r.recordEventf(v, corev1.EventTypeWarning,
			string(events.SentinelStaleEpochRepoint), "SentinelStaleEpochRepoint",
			"sentinel(s) %v were monitoring a live pod behind the config-epoch frontier; re-pointed at %s via REMOVE + MONITOR",
			out.StaleEpochRepointed, masterIP)
		auditSentinelReset(ctx, cr, out.StaleEpochRepointed, "stale-epoch-repoint")
	}
}

// sentinelNamesToAddrs maps sentinel pod names to their current
// Endpoint.Addr, dropping any name absent from the set (defensive —
// the surgery operates on the same endpoint slice, so every returned
// name should resolve).
func sentinelNamesToAddrs(names []string, addrByName map[string]string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if a, ok := addrByName[n]; ok {
			out = append(out, a)
		}
	}
	return out
}

// liveValkeyIPs unwraps the survey's live-pod IP set, tolerating a nil
// survey (failed pod list) — the manager treats nil as "re-point class
// disabled".
func liveValkeyIPs(survey *valkeyPodSurvey) map[string]struct{} {
	if survey == nil {
		return nil
	}
	return survey.livePodIPs
}

// observeDualMasterFromSurvey folds one recovery-survey verdict into
// the per-CR dual-master observation. Only a survey whose dial sweep
// ran carries a verdict: >=2 self-reported masters stamps the
// observation (Ready/Degraded and the valkey_dual_master_observed gauge
// read it freshness-gated in updateStatus). A sweep seeing <=1 masters
// clears the stamp ONLY when it was complete and clean (every pod
// dialed, none pending): a partial sweep — a rogue master's INFO dial
// timed out, or a pod is still coming up — cannot prove the split
// resolved, so the stamp is left to age out via the freshness window
// rather than flapping the condition. Surveys that resolved the master
// before dialing (label or snapshot short-circuit) carry no verdict.
//
// The DualMasterObserved Warning fires only OUTSIDE a failover section:
// there no demotion path is admitted (no fencing epoch), so the event
// is the only actor-facing signal. Inside a section the bounded
// self-heal's Initiated/Deferred events own the messaging — firing here
// too would double-message and the "no failover in flight" framing
// would be false. The stamp is recorded either way so the condition
// surfaces the divergence regardless of the failover state.
func (r *ValkeyReconciler) observeDualMasterFromSurvey(v *valkeyv1beta1.Valkey, cr types.NamespacedName, survey *valkeyPodSurvey) {
	if survey == nil || !survey.dialed {
		return
	}
	complete := survey.dialFailures == 0 && survey.pendingPods == 0
	if len(survey.masters) < 2 {
		if complete {
			r.stateFor(cr).clearDualMasterObserved()
		}
		return
	}
	ps := r.stateFor(cr)
	pods := make([]string, 0, len(survey.masters))
	details := make([]string, 0, len(survey.masters))
	for _, m := range survey.masters {
		pods = append(pods, m.name)
		offset := "master_repl_offset=unknown"
		if m.state.HaveMasterOffset {
			offset = fmt.Sprintf("master_repl_offset=%d", m.state.MasterReplOffset)
		}
		details = append(details, fmt.Sprintf("%s(%s)", m.name, offset))
	}
	now := time.Now()
	ps.stampDualMasterObserved(pods, now)
	if r.IsFailoverInFlight(cr) {
		return
	}
	sort.Strings(details)
	// Key the event edge on the accumulated episode union (fed only here
	// and by the replication no-primary scan, which are mode-exclusive per
	// CR), not this pass's exact pod-set: role churn within one split
	// re-fires once per genuinely-new pod, not once per permutation. The
	// union accumulates only on fire-eligible passes (after the failover
	// early-return above).
	if ps.fireDualMasterObservedEdge(ps.foldDualMasterEventUnion(pods)) {
		r.recordEventf(v, corev1.EventTypeWarning, string(events.DualMasterObserved), "DualMasterObserve",
			"pods %s all self-report role:master with no operator failover in flight — active data divergence; no demotion is admitted without a fencing epoch, so the split persists until resolved manually or a failover section opens",
			strings.Join(details, ", "))
	}
}

// roleDisclaimsMastership is the single acceptance predicate for a
// labeled primary's self-reported role: an explicit non-master reply
// disclaims (demoted under the label); empty means the reply carried
// no role line and the label is trusted. Shared by the probe capture
// site and the confirmation dial so the fast and slow resolution
// branches cannot drift apart.
func roleDisclaimsMastership(role string) bool {
	return role != "" && role != valkey.RoleMaster
}

// repointClassArmed decides whether this pass should run the
// dead-master re-point classification (a full SENTINEL fan-out): only
// when the observer gives reason to suspect corpse-monitoring — the
// quorum-agreed primary addr maps to no live pod, quorum agreement is
// absent, or the agreed addr is unparseable. A healthy snapshot naming
// a live master skips the class (and its per-pass probe fan-out)
// entirely, keeping the steady-state sentinel round-trip count
// unchanged.
//
// Accepted tradeoff: the arm signal is the QUORUM view, not any
// per-sentinel monitored addr, so a divergent-minority sentinel
// monitoring a corpse while the majority names a live master stays
// unaddressed until the next quorum disruption re-arms the class.
// That state is degraded-but-available (the majority keeps serving
// and failing over) and self-correcting on re-arm; scanning for it
// every pass would reintroduce the per-reconcile fan-out this gate
// exists to avoid.
func repointClassArmed(snap sentinel.Snapshot, liveIPs map[string]struct{}) bool {
	if !snap.Present {
		return false
	}
	if !snap.Primary.QuorumOK {
		return true
	}
	host, _, err := net.SplitHostPort(snap.Primary.Addr)
	if err != nil || host == "" {
		return true
	}
	_, live := liveIPs[host]
	return !live
}

// ghostReapAllowed derives the ghost-reap veto from the snapshot's
// two o_down maps, each bounded by one failover-timeout. Within one
// failover-timeout of a fresh o_down a sentinel election may genuinely
// be progressing — veto. Past it, any election has provably aborted at
// least once, so operator hygiene may proceed. A non-positive
// failoverTimeoutMS (defaulting webhook not yet run) falls back to the
// 180s floor the webhook enforces.
//
// Two sources feed the veto, and either one within the bound blocks:
//   - ODown is edge-triggered by +odown / -odown pubsub frames; an
//     entry whose -odown clear frame was lost would otherwise linger,
//     but the freshness bound ages it out.
//   - ODownPull is level-reconciled by the pull tick with a rising-edge
//     first-seen stamp, so a lost -odown frame is corroborated (or
//     cleared) by the next poll. Because the stamp is never refreshed
//     while o_down persists, a stuck o_down still ages out one
//     failover-timeout after it FIRST appeared — the escape valve holds.
//
// The veto only ever suppresses hygiene; it never authorises a relabel
// or promotion, so adding the pull-side source only makes the gate more
// conservative.
func ghostReapAllowed(snap sentinel.Snapshot, failoverTimeoutMS int32, now time.Time) bool {
	if !snap.Present {
		return false
	}
	ttl := time.Duration(failoverTimeoutMS) * time.Millisecond
	if ttl <= 0 {
		ttl = 180 * time.Second
	}
	for _, ts := range snap.Primary.ODown {
		if now.Sub(ts) < ttl {
			return false
		}
	}
	for _, ts := range snap.Primary.ODownPull {
		if now.Sub(ts) < ttl {
			return false
		}
	}
	return true
}

// maybeRecoveryPromote is the zero-master recovery election — the
// last-resort repair for the total wedge where every address the
// sentinel quorum knows is dead and no live pod serves writes.
// Sentinel's own failover cannot resolve it (its candidate set died
// with the master), the re-point surgery cannot fire (no master IP to
// re-point at), and Phase 7 correctly refuses to label without quorum
// agreement — so nothing in the system would ever mint a master again.
//
// Promotion preconditions (ALL required — each independently blocks a
// split-brain path):
//
//   - The dial sweep ran and reached every pod that has an IP
//     (dialFailures==0 — an unreachable pod could be a live master),
//     no listed pod is still IP-less (pendingPods==0 — a recreated
//     master pod carries the freshest data and must not be promoted
//     over while coming up), and it found ZERO self-reported masters
//     and ≥1 replica.
//   - Every reachable replica reports master_link down AND a
//     master_host matching no live pod — all of them replicate from
//     corpses. A link-up replica means its master exists somewhere;
//     defer to the normal paths. Replicas pointing at DIFFERENT dead
//     masters defer too (divergent lineages have incomparable
//     offsets).
//   - A quorum of sentinels is reachable and every reachable
//     sentinel's SENTINEL REPLICAS table is provably doomed (no known
//     candidate maps to a live pod) — the discriminator that keeps
//     this election from racing Sentinel's own during an ordinary
//     master death, whose first seconds are otherwise
//     indistinguishable from the total wedge.
//   - The observer snapshot is Present and shows no POSITIVE evidence
//     of a live master: a fresh quorum-backed snapshot naming a LIVE
//     pod blocks promotion (Phase 7 / re-point own that state); a
//     quorum-named dead address proceeds (the total wedge), as does a
//     snapshot with no quorum agreement at all (the hybrid wedge —
//     sentinels disagree because every address any of them knows is
//     dead). The API-server evidence carries the decision: no master
//     pod OBJECT exists — a partitioned-but-alive master keeps its pod
//     object and is caught by the live-master and MasterHost checks,
//     which is a strictly stronger guard than Sentinel's own
//     o_down-based election applies.
//   - No failover is in flight and the per-CR promotion cooldown is
//     clear.
//
// The candidate is the replica with the highest applied replication
// offset (slave_repl_offset — the criterion Sentinel's leader uses),
// ties broken by lowest pod name. Only the data plane is touched
// (REPLICAOF NO ONE): the role label still waits for sentinel quorum
// agreement in Phase 7 after the next pass re-points the sentinels at
// the new master — the split-brain guard's invariant is preserved.
//
// Accepted residual: a force-deleted master pod on a partitioned node
// can leave a zombie valkey-server process whose pod OBJECT is gone.
// The promotion cannot see it; the bound on zombie-side write loss is
// the rendered min-replicas-to-write floor — with its replicas
// re-pointed away, the zombie refuses writes within
// min-replicas-max-lag seconds of isolation.
func (r *ValkeyReconciler) maybeRecoveryPromote(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	password string,
	survey *valkeyPodSurvey,
	endpoints []sentinel.Endpoint,
) {
	if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" {
		return
	}
	if !promotionSurveyAdmits(ctx, cr, survey, endpoints) {
		return
	}
	if r.SentinelObserver == nil {
		return
	}
	// Sentinel-side evidence gate: block only on POSITIVE evidence of
	// a live master — a fresh quorum-backed snapshot naming a live
	// pod's IP (Phase 7 / the re-point class own that state). A
	// quorum-named DEAD address proceeds (the observed total wedge),
	// and so does a Present snapshot with no quorum agreement at all
	// (the hybrid wedge: sentinels disagree because every address any
	// of them knows is dead). The load-bearing safety is the survey:
	// the API server says no master pod object exists and every live
	// replica is orphaned by a corpse. !Present still blocks — a
	// boot-racing observer must not authorize a promotion.
	snap := r.SentinelObserver.Snapshot(cr)
	if !snap.Present {
		return
	}
	snapIP := freshSnapshotPrimaryIP(snap, time.Now())
	if snapIP != "" {
		if _, live := survey.livePodIPs[snapIP]; live {
			return
		}
	}
	if r.IsFailoverInFlight(cr) {
		return
	}

	// Cooldown pre-check BEFORE any wire fan-out: during the cooldown
	// window every reconcile in the wedge would otherwise pay N
	// sentinel round-trips only to discard the result. Pure read; the
	// stamp happens atomically just before issuance.
	state := r.stateFor(cr).quorumTracker()
	state.mu.Lock()
	cooldownActive := time.Since(state.recoveryPromotionLastFired) < recoveryPromotionCooldown
	state.mu.Unlock()
	if cooldownActive {
		return
	}

	// Candidate selection before the fan-out — an unrankable set (no
	// replica reports slave_repl_offset) defers without paying wire
	// costs.
	best := choosePromotionCandidate(survey.replicas)
	if best == -1 {
		return
	}

	// Doomed-election discriminator — the gate that keeps this election
	// from racing Sentinel's own. The zero-master survey signature is
	// indistinguishable from the first seconds of an ORDINARY master
	// death (replacement pod not yet listed, replica links just
	// dropped), where the sentinels' replica tables still name the
	// live replicas and their election WILL succeed; IsFailoverInFlight
	// cannot see that window (it tracks only operator-dispatched
	// failovers, and Sentinel sets failover_in_progress only after a
	// leader is elected). SENTINEL REPLICAS is the discriminating
	// evidence, owned by the sentinel seam (Manager.QuorumElectionDoomed):
	// a quorum of sentinels must be reachable and EVERY reachable table
	// must be provably doomed (all known candidates dead) — one live
	// entry means Sentinel has a viable candidate and the operator
	// defers to it.
	doomedFn := r.ElectionDoomedFn
	if doomedFn == nil {
		doomedFn = r.SentinelObserver.QuorumElectionDoomed
	}
	if !doomedFn(ctx, endpoints, v.Spec.Sentinel.MasterName, password, survey.livePodIPs) {
		return
	}

	// Strong-consistency confirm: the survey's pod view came from the
	// informer cache, and the discriminator treats any sentinel-known
	// replica absent from it as a corpse. Re-read through the API
	// server so a watch-lagged live pod cannot be misjudged into an
	// irreversible promotion; any drift from the surveyed view defers
	// to the next pass.
	if !r.promotionPodViewConfirmed(ctx, v, survey) {
		return
	}

	state.mu.Lock()
	cooldownActive = time.Since(state.recoveryPromotionLastFired) < recoveryPromotionCooldown
	if !cooldownActive {
		// Stamp before issuing so an erroring candidate can't be
		// hot-looped at reconcile cadence.
		state.recoveryPromotionLastFired = time.Now()
	}
	state.mu.Unlock()
	if cooldownActive {
		return
	}

	quorumEvidence := "quorum-named dead address " + snapIP
	if snapIP == "" {
		quorumEvidence = "no quorum-agreed primary address"
	}
	r.issueRecoveryPromotion(ctx, v, cr, password, survey.replicas[best], quorumEvidence)
}

// maybeRecoveryPromoteOnQuorumLoss is the sustained-quorum-loss arming path
// for the zero-master recovery election. It arms only while the per-CR
// quorum-suppression gate is active (CKQUORUM=NOQUORUM sustained past the
// loss dwell) and paces itself with recoveryDetectionCooldown, so the forced
// work runs at roughly the rate fresh quorum evidence arrives, never once
// per reconcile.
//
// When the resolver reached its answer via a cheap short-circuit (the survey
// was not dialed), this forces a fully-dialed survey so the promotion guard
// chain is exercised against directly-observed pod state rather than the
// short-circuit's verdict. The forced dial is deliberately unconditional
// under the arm: independence from the resolver's verdict is the point, so
// it is not skipped on an already-"known" live master. An already-dialed
// survey is reused as-is (no second sweep). The dialed survey is handed to
// the unchanged maybeRecoveryPromote, which still gates every REPLICAOF NO
// ONE behind the full guard chain (survey admittance, live-snapshot block,
// failover-in-flight, promotion cooldown, doomed-election, strong-read pod
// confirm); this path authorizes no promotion the chain would not otherwise
// permit.
func (r *ValkeyReconciler) maybeRecoveryPromoteOnQuorumLoss(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	password string,
	survey *valkeyPodSurvey,
	endpoints []sentinel.Endpoint,
) {
	// Arm: only under a sustained-quorum-loss suppression gate. In steady
	// state this is false, so the forced sweep costs nothing.
	if !r.IsSentinelSuppressed(cr) {
		return
	}
	// Debounce: one probe per recoveryDetectionCooldown. Stamp on commit so a
	// List/dial error does not hot-loop the sweep at reconcile cadence. Held
	// only for the timestamp check/stamp — released before any I/O, so the
	// delegate's own state.mu use below is not re-entrant.
	state := r.stateFor(cr).quorumTracker()
	now := r.now()
	state.mu.Lock()
	cooling := now.Sub(state.recoveryDetectionLastProbed) < recoveryDetectionCooldown
	if !cooling {
		state.recoveryDetectionLastProbed = now
	}
	state.mu.Unlock()
	if cooling {
		return
	}
	// Force a fully-dialed survey when the resolver short-circuited, so the
	// guard chain sees directly-observed pod state. Reuse an already-dialed
	// survey unchanged.
	if survey == nil || !survey.dialed {
		pods, err := r.listValkeyPods(ctx, v)
		if err != nil {
			return
		}
		survey = newSurveyFromPods(pods)
		r.dialValkeySurvey(ctx, pods, survey, password)
		// The forced dial is the only observation point for a data-plane
		// split hiding behind a labeled-primary/snapshot short-circuit
		// (the dispatcher's surfacing call saw only the non-dialed
		// resolver survey and early-returned). Fold this sweep's verdict
		// too — freshness-gated, union-edge-debounced, and paced by the
		// detection cooldown above — so >=2 self-reported masters stamp
		// the condition/gauge and page instead of only refusing promotion
		// silently; a complete clean sweep symmetrically clears a stale
		// stamp early. An already-dialed survey was surfaced at the
		// dispatcher, so only the forced branch folds.
		r.observeDualMasterFromSurvey(v, cr, survey)
	}
	r.maybeRecoveryPromote(ctx, v, cr, password, survey, endpoints)
}

// promotionSurveyAdmits evaluates the survey-level promotion gates:
// complete dial coverage (no failures, no IP-less pods), zero
// self-reported masters, ≥1 replica, sentinel endpoints present, every
// replica link-down and orphaned by a corpse, and a single dead
// lineage (replicas pointing at DIFFERENT dead masters have
// incomparable replication histories — ranking offsets across
// lineages can promote the wrong data; that compound state needs a
// human).
func promotionSurveyAdmits(ctx context.Context, cr types.NamespacedName, survey *valkeyPodSurvey, endpoints []sentinel.Endpoint) bool {
	if survey == nil || !survey.dialed || survey.dialFailures > 0 || survey.pendingPods > 0 {
		return false
	}
	if len(survey.masters) != 0 || len(survey.replicas) == 0 || len(endpoints) == 0 {
		return false
	}
	lineageHost := ""
	for i := range survey.replicas {
		st := survey.replicas[i].state
		if st.LinkUp {
			return false
		}
		if st.MasterHost == "" {
			// A role:slave INFO always carries master_host; an empty
			// value is a truncated/anomalous reply. The replica would
			// still be offset-rankable with unconfirmable lineage, so
			// be conservative: no promotion this pass.
			return false
		}
		if _, live := survey.livePodIPs[st.MasterHost]; live {
			return false
		}
		if lineageHost != "" && st.MasterHost != lineageHost {
			logf.FromContext(ctx).WithName("recovery-promotion").Info(
				"recovery promotion: replicas point at different dead masters (divergent lineages); refusing to rank offsets across lineages",
				"cr", cr.String(), "hostA", lineageHost, "hostB", st.MasterHost)
			return false
		}
		lineageHost = st.MasterHost
	}
	return true
}

// promotionPodViewConfirmed re-reads the CR's valkey pods through the
// uncached APIReader and confirms the strongly-consistent view matches
// the informer-cache survey the promotion gates evaluated: same pod
// count, no IP-less pods, and every pod IP already in the surveyed
// live set. Any drift (a pod the cache missed, a pod coming up, a
// changed IP) defers the irreversible REPLICAOF NO ONE to the next
// pass. Nil-safe on APIReader (falls back to the cached client, which
// preserves the pre-confirm behavior in tests wired without a
// reader); a List error defers.
func (r *ValkeyReconciler) promotionPodViewConfirmed(ctx context.Context, v *valkeyv1beta1.Valkey, survey *valkeyPodSurvey) bool {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return false
	}
	if len(pods.Items) != len(survey.livePodIPs) {
		return false
	}
	for i := range pods.Items {
		ip := pods.Items[i].Status.PodIP
		if ip == "" {
			return false
		}
		if _, ok := survey.livePodIPs[ip]; !ok {
			return false
		}
	}
	return true
}

// choosePromotionCandidate returns the index of the replica with the
// highest applied replication offset (ties: lowest pod name), or -1
// when no replica reports slave_repl_offset — an unrankable set.
func choosePromotionCandidate(replicas []surveyedPod) int {
	best := -1
	for i := range replicas {
		st := replicas[i].state
		if !st.HaveSlaveOffset {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		bs := replicas[best].state
		if st.SlaveReplOffset > bs.SlaveReplOffset ||
			(st.SlaveReplOffset == bs.SlaveReplOffset && replicas[i].name < replicas[best].name) {
			best = i
		}
	}
	return best
}

// issueRecoveryPromotion performs the REPLICAOF NO ONE issuance plus
// its event/log tail for a chosen recovery-election candidate.
func (r *ValkeyReconciler) issueRecoveryPromotion(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	password string,
	chosen surveyedPod,
	quorumEvidence string,
) {
	logger := logf.FromContext(ctx).WithName("recovery-promotion")
	issuer := r.PromoteIssuer
	if issuer == nil {
		issuer = &valkey.DialingReplicaOfIssuer{}
	}
	addr := net.JoinHostPort(chosen.ip, strconv.Itoa(valkey.DefaultPort))
	if err := issuer.IssuePromote(ctx, addr, password); err != nil {
		logger.Info("recovery promotion: REPLICAOF NO ONE failed",
			"cr", cr.String(), "pod", chosen.name, "err", err.Error())
		r.recordEventf(v, corev1.EventTypeWarning,
			string(events.RecoveryPromotionFailed), "RecoveryPromotionFailed",
			"REPLICAOF NO ONE on %s failed: %v", chosen.name, err)
		return
	}
	logger.Info("recovery promotion: promoted replica via REPLICAOF NO ONE",
		"cr", cr.String(), "pod", chosen.name, "ip", chosen.ip,
		"slaveReplOffset", chosen.state.SlaveReplOffset, "quorumEvidence", quorumEvidence)
	r.recordEventf(v, corev1.EventTypeWarning,
		string(events.RecoveryPromotionInitiated), "RecoveryPromotionInitiated",
		"zero live masters (%s): promoted %s (applied offset %d) via REPLICAOF NO ONE; sentinels re-point next pass",
		quorumEvidence, chosen.name, chosen.state.SlaveReplOffset)
}

// valkeyPodSurvey is the by-product of one observedMasterIPForCR
// resolution: the live valkey-pod IP set (populated whenever the pod
// list succeeded) plus — when the dial fallback ran — the per-pod
// INFO replication readings. The stranded-recovery dispatcher threads
// livePodIPs into the dead-master re-point class; the zero-master
// recovery election reads masters/replicas/dialFailures.
type valkeyPodSurvey struct {
	livePodIPs map[string]struct{}
	// pendingPods counts listed pods that have no PodIP yet
	// (Pending / ContainerCreating). The recovery election treats
	// any of them as a potential master-in-waiting — a recreated
	// master pod carries the freshest data on its PVC and must not
	// be promoted over while it is still coming up.
	pendingPods int
	// dialed reports whether the INFO fallback sweep ran (it is
	// skipped when a primary label or the snapshot resolved the
	// master first); masters/replicas/dialFailures are meaningful
	// only when true.
	dialed       bool
	masters      []surveyedPod
	replicas     []surveyedPod
	dialFailures int
}

// surveyedPod is one reachable pod's INFO replication reading.
type surveyedPod struct {
	name  string
	ip    string
	state valkey.LagState
}

// freshSnapshotPrimaryIP returns the primary's IP as named by the
// surviving sentinels' quorum (the observer snapshot) when that snapshot
// is safe to act on — present, quorum-backed, and fresh (a live pull tick
// confirmed it within maxRolloutSnapshotAge) — otherwise "" so the caller
// falls back to dialing. The host is split from the snapshot's host:port
// Addr; an empty or malformed Addr also yields "".
//
// QuorumOK is required because Addr is only meaningful under quorum
// (ObservedPrimary.Addr is empty when QuorumOK is false). The freshness
// gate keys off LastPolledAt — the last live poll — so a pub/sub replay
// carrying a stale quorum forward can't feed a stale address into
// stranded-recovery RESET dispatch, the same rule the rollout dispatcher
// applies before stripping the primary label. Pure (snapshot + clock
// passed in) so the gate is table-testable without driving the observer.
func freshSnapshotPrimaryIP(snap sentinel.Snapshot, now time.Time) string {
	if !snap.Present || !snap.Primary.QuorumOK {
		return ""
	}
	if snapshotStale(snap, now, maxRolloutSnapshotAge) {
		return ""
	}
	host, _, err := net.SplitHostPort(snap.Primary.Addr)
	if err != nil || host == "" {
		return ""
	}
	return host
}

// newSurveyFromPods builds the live-IP set and pending-pod count for a CR's
// valkey pods. Extracted so both observedMasterIPForCR and the sustained-
// quorum-loss arming path build a survey the same way.
func newSurveyFromPods(pods []corev1.Pod) *valkeyPodSurvey {
	survey := &valkeyPodSurvey{livePodIPs: make(map[string]struct{}, len(pods))}
	for i := range pods {
		if ip := pods[i].Status.PodIP; ip != "" {
			survey.livePodIPs[ip] = struct{}{}
		} else {
			survey.pendingPods++
		}
	}
	return survey
}

// dialValkeySurvey runs the INFO-replication fan-out over every live pod,
// marks the survey dialed, and fills masters/replicas/dialFailures. Each
// pod gets an independent 2s timeout so a single hung pod cannot stall the
// whole dispatch. Extracted so the arming path can force a dialed survey
// through the exact sweep observedMasterIPForCR uses.
func (r *ValkeyReconciler) dialValkeySurvey(ctx context.Context, allPods []corev1.Pod, survey *valkeyPodSurvey, password string) {
	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	survey.dialed = true
	for i := range allPods {
		p := &allPods[i]
		if p.Status.PodIP == "" {
			continue
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		state, err := checker.CheckLag(probeCtx, addr, password)
		cancel()
		if err != nil {
			survey.dialFailures++
			continue
		}
		sp := surveyedPod{name: p.Name, ip: p.Status.PodIP, state: state}
		if state.Role == valkey.RoleMaster {
			survey.masters = append(survey.masters, sp)
		} else {
			survey.replicas = append(survey.replicas, sp)
		}
	}
}

// observedMasterIPForCR mirrors SentinelStartupReset.observedMasterIP
// for the per-reconcile path. Resolution order:
//
//  1. Any valkey pod labelled role=primary with a non-empty PodIP.
//  2. The observer snapshot's quorum-named primary, when present and
//     fresh — answers "who is the master" without re-dialing.
//  3. INFO replication fallback against every valkey pod, picking
//     the single pod reporting role=master.
//  4. Empty when none resolves (wedge in flight, observer observing
//     stale state) — caller treats empty as "skip this reconcile's
//     stranded-detection".
//
// The snapshot path is additionally cross-checked against live pod
// IPs: after a chaotic roll the rebuilt sentinels can unanimously
// (quorum-backed, freshly polled) name a primary IP whose pod no
// longer exists — acting on it re-MONITORs every stranded sentinel at
// the dead address and cements the wedge the recovery exists to fix.
// An IP matching no current pod falls through to the dial fallback,
// whose single-master requirement is the safe arbiter.
//
// The returned survey carries the live-pod IP set (nil only when the
// pod list failed) and, when the dial fallback ran, the per-pod INFO
// states — the inputs for the dead-master re-point class and the
// zero-master recovery election.
func (r *ValkeyReconciler) observedMasterIPForCR(ctx context.Context, v *valkeyv1beta1.Valkey, password string) (string, *valkeyPodSurvey) {
	logger := logf.FromContext(ctx).WithName("stranded-recovery")

	allPods := &corev1.PodList{}
	if err := r.List(ctx, allPods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		logger.V(1).Info("valkey-pod list err during master-IP resolution",
			"cr", v.Namespace+"/"+v.Name, "err", err.Error())
		return "", nil
	}
	survey := newSurveyFromPods(allPods.Items)

	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}

	// Same-pass probe signal: observeMasterInfoTimeout already dialed
	// the labeled primary's INFO earlier in this reconcile and recorded
	// the outcome in the per-CR tracker. Reusing it keeps the healthy
	// hot path at ONE primary INFO round-trip per reconcile.
	crKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	qstate := r.stateFor(crKey).quorumTracker()
	qstate.mu.Lock()
	probeFresh := !qstate.masterInfoObservedAt.IsZero() &&
		time.Since(qstate.masterInfoObservedAt) < masterInfoSamePassReuseWindow
	probeFailing := qstate.masterInfoTimeoutSince != nil
	probeDisclaimed := qstate.masterInfoRoleDisclaimed
	qstate.mu.Unlock()

	// Step 1 — primary-labeled pod, filtered in memory from the same
	// cache read the live-IP set came from. The label alone is NOT
	// trusted: a dead-but-undeleted pod (node loss, stuck Terminating)
	// keeps both its label and its PodIP, and Phase 7 cannot strip the
	// label while the quorum guard suppresses relabels — returning that
	// IP would feed a corpse into the PING-gated surgery, which then
	// defers forever. The labeled pod must answer INFO (per this pass's
	// probe when fresh, else one confirmation dial) and not explicitly
	// disclaim mastership; a failure falls through to the
	// snapshot / dial-sweep arbiters.
	for i := range allPods.Items {
		p := &allPods.Items[i]
		if p.Labels[RoleLabel] != roleValuePrimary || p.Status.PodIP == "" {
			continue
		}
		if probeFresh {
			// Same acceptance criteria as the confirmation dial below:
			// the pod must have answered INFO AND not disclaimed
			// mastership on this pass's probe.
			if !probeFailing && !probeDisclaimed {
				return p.Status.PodIP, survey
			}
			logger.V(0).Info("stranded-recovery: labeled primary failed or disclaimed this pass's INFO probe; falling through to snapshot/dial resolution",
				"cr", v.Namespace+"/"+v.Name, "pod", p.Name,
				"probeFailing", probeFailing, "roleDisclaimed", probeDisclaimed)
			break
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		state, err := checker.CheckLag(probeCtx, addr, password)
		cancel()
		if err != nil {
			logger.V(0).Info("stranded-recovery: labeled primary did not answer INFO; falling through to snapshot/dial resolution",
				"cr", v.Namespace+"/"+v.Name, "pod", p.Name, "err", err.Error())
			break
		}
		if roleDisclaimsMastership(state.Role) {
			logger.V(0).Info("stranded-recovery: labeled primary self-reports a non-master role; falling through to snapshot/dial resolution",
				"cr", v.Namespace+"/"+v.Name, "pod", p.Name, "role", state.Role)
			break
		}
		return p.Status.PodIP, survey
	}

	// Step 2 — before re-dialing every valkey pod's INFO replication,
	// prefer the observer's snapshot: the surviving sentinels' quorum
	// already names the primary (GET-MASTER-ADDR-BY-NAME), so a fresh,
	// quorum-backed snapshot answers "who is the master" authoritatively
	// without N fresh TCP connections. Falls through to the dial
	// fallback when the observer is absent, its snapshot is stale /
	// quorum-lost, or the named IP belongs to no live pod.
	if r.SentinelObserver != nil {
		cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
		if ip := freshSnapshotPrimaryIP(r.SentinelObserver.Snapshot(cr), time.Now()); ip != "" {
			if _, live := survey.livePodIPs[ip]; live {
				return ip, survey
			}
			logger.V(0).Info("stranded-recovery: snapshot primary IP matches no live valkey pod; ignoring snapshot",
				"cr", v.Namespace+"/"+v.Name, "snapshotIP", ip)
		}
	}
	r.dialValkeySurvey(ctx, allPods.Items, survey, password)
	if len(survey.masters) != 1 {
		// Zero masters (early bootstrap / wedge) OR >1 masters
		// (data-plane split-brain) — operator can't pick a safe
		// MasterIP; skip RESET this pass. The zero-master case hands
		// the survey to the recovery election, whose own gates decide
		// whether promotion is safe.
		return "", survey
	}
	return survey.masters[0].ip, survey
}

// updateQuorumSuppressionGate observes the latest tri-state Quorum
// signal from the sentinel snapshot, advances the per-CR threshold +
// hysteresis tracker, and emits one-shot QuorumLost / QuorumReached
// events on transition. The boolean state is read by IsSentinelSuppressed
// (wired into the sentinel manager's deferral predicate by main.go)
// so RecoverStrandedSentinels defers its REMOVE + MONITOR surgery while
// the gate is active — preventing operator-driven SENTINEL command
// issuance during sustained quorum loss.
//
// No-op when the snapshot is not yet Present (pre-Ensure or just
// after a CR-create — there's no signal to integrate). The tri-state
// Quorum field is consumed directly so a QuorumStatusUnknown
// observation (observer can't reach a quorum of peers) preserves the
// gate's prior state instead of accumulating loss-time as if it were
// a Lost observation. See QuorumStatus godoc for the design rationale.
// Snapshot reads are atomic.Value loads and effectively free.
func (r *ValkeyReconciler) updateQuorumSuppressionGate(ctx context.Context, v *valkeyv1beta1.Valkey, cr types.NamespacedName) {
	snap := r.SentinelObserver.Snapshot(cr)
	state := r.stateFor(cr).quorumTracker()

	now := time.Now()
	state.mu.Lock()
	// Split-brain sustained-duration tracking runs even when the
	// observer has not yet published a snapshot — pre-Ensure / pre-
	// first-poll windows are treated as agreement (gauge=0) so a
	// re-created CR starts from zero rather than inheriting the
	// previous episode's elapsed time.
	sustained, episodeStarted := state.updateSplitBrainSustained(snap.Present, snap.Primary.Quorum, snap.Primary.LastPolledAt, now)
	// Failover observation: detects the !QuorumOK → QuorumOK with
	// changed Addr transition that marks a completed failover.
	// Runs under the same lock as updateSplitBrainSustained so the
	// histogram + gauge readings are consistent for this reconcile.
	failoverDur, failoverTrigger, didFailover := state.observeFailoverIfAddrChanged(snap.Present, snap.Primary.QuorumOK, snap.Primary.Addr, now)
	state.mu.Unlock()
	operatormetrics.SplitBrainSustainedSeconds.WithLabelValues(cr.Namespace, cr.Name).Set(sustained)
	// SplitBrainDetected fires exactly once per disagreement episode
	// (the nil→set edge above). The relabel suppression in Phase 7 is
	// level-based and unaffected; only the warning event + counter are
	// edge-gated, so a sustained loss pages once instead of storming
	// the event stream on every reconcile. Same-reconcile ordering with
	// the first suppression pass is preserved: Phase 7 and this gate
	// run in the same Reconcile.
	if episodeStarted {
		operatormetrics.SplitBrainDetectionsTotal.WithLabelValues(cr.Namespace, cr.Name).Inc()
		r.recordEventf(v, corev1.EventTypeWarning, string(events.SplitBrainDetected), "SplitBrainObserve",
			"sentinel observer reports quorum lost (source=%s) — suppressing role-label relabel until quorum recovers",
			snap.Primary.Source)
	}
	if didFailover {
		operatormetrics.FailoversTotal.WithLabelValues(cr.Namespace, cr.Name, failoverTrigger).Inc()
		operatormetrics.FailoverDurationSeconds.WithLabelValues(cr.Namespace, cr.Name, failoverTrigger).Observe(failoverDur.Seconds())
		logf.FromContext(ctx).Info("failover completed",
			"cr", cr.String(),
			"trigger", failoverTrigger,
			"newPrimaryAddr", snap.Primary.Addr,
			"elapsed", failoverDur.Round(time.Second).String())
	}

	if !snap.Present {
		return
	}
	state.mu.Lock()
	priorActive := state.suppressionActive
	justEntered, justExited := state.updateQuorumSuppression(snap.Primary.Quorum, snap.Primary.LastPolledAt, time.Now(), r.Tunables)
	nextActive := state.suppressionActive
	state.mu.Unlock()

	if justEntered || justExited {
		operatormetrics.QuorumSuppressionTransitionsTotal.WithLabelValues(
			cr.Namespace, cr.Name,
			suppressionLabel(priorActive),
			suppressionLabel(nextActive),
		).Inc()
	}

	r.onSuppressionTransition(v, justEntered, justExited)
}

// suppressionLabel maps the gate's boolean state to the
// `active|inactive` label values used by the
// QuorumSuppressionTransitionsTotal counter. Kept in one place so
// future renames stay consistent across emit sites.
func suppressionLabel(active bool) string {
	if active {
		return "active"
	}
	return "inactive"
}

// onSuppressionTransition runs the side effects for a quorum-suppression
// gate edge: emit the one-shot QuorumLost / QuorumReached event.
// Extracted from updateQuorumSuppressionGate so the side-effect wiring
// can be exercised without driving a real observer snapshot.
func (r *ValkeyReconciler) onSuppressionTransition(v *valkeyv1beta1.Valkey, justEntered, justExited bool) {
	switch {
	case justEntered:
		r.recordEventf(v, corev1.EventTypeWarning, string(events.QuorumLost), "QuorumLossObserve",
			"sentinel CKQUORUM has reported NOQUORUM continuously for ≥%s; suppressing operator-issued SENTINEL MONITOR/REMOVE/SET until %d consecutive CKQUORUM=OK polls observed",
			r.Tunables.lossThreshold(), r.Tunables.recoveryPolls())
	case justExited:
		r.recordEventf(v, corev1.EventTypeNormal, string(events.QuorumReached), "QuorumReachObserve",
			"sentinel CKQUORUM recovered: %d consecutive CKQUORUM=OK polls observed; releasing the SENTINEL command-suppression gate",
			r.Tunables.recoveryPolls())
	}
}

// IsSentinelSuppressed reports whether the operator is currently
// suppressing SENTINEL MONITOR/REMOVE/SET issuance for the given CR
// due to sustained CKQUORUM=NOQUORUM. Wired into
// sentinel.Manager.SetDeferralPredicate by cmd/main.go so
// RecoverStrandedSentinels defers while the gate is active.
//
// Concurrency-safe: takes the per-CR mutex for the duration of the
// flag read; never blocks on any I/O.
func (r *ValkeyReconciler) IsSentinelSuppressed(cr types.NamespacedName) bool {
	ps, ok := r.stateForIfPresent(cr)
	if !ok {
		return false
	}
	state := ps.quorumIfPresent()
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.suppressionActive
}

// suppressionPaceHint tightens the requeue to the observer's poll
// cadence while the quorum-suppression gate is active — but only on
// passes that actually ran the Phase 11 gate driver
// (reachedSteadyState). Short-circuit passes (paused, auth-missing
// backoff, PVC-loss gate) never reach Phase 11 and so cannot advance
// the gate; pacing them would only tighten their deliberately relaxed
// requeues into do-nothing churn, mirroring the reachedSteadyState
// gating on the SentinelQuorum keep-alive.
func (r *ValkeyReconciler) suppressionPaceHint(current time.Duration, v *valkeyv1beta1.Valkey, reachedSteadyState bool) time.Duration {
	if !reachedSteadyState {
		return current
	}
	if !r.IsSentinelSuppressed(types.NamespacedName{Namespace: v.Namespace, Name: v.Name}) {
		return current
	}
	return mergeRequeue(current, quorumSuppressedRequeue)
}

// ensurePVCRetentionFinalizer stamps the pvc-retention finalizer on a
// non-deleting CR if it isn't already present. Stamping the finalizer
// before any apply-side work makes the spec.pvcRetentionPolicy a hard
// guarantee: from the next CRUD on this CR, the apiserver will not
// drop it until reconcileDeletion has run and removed the finalizer.
//
// Idempotent — controllerutil.AddFinalizer is a no-op when the entry
// already exists. Existing CRs gain the finalizer on their first
// reconcile after upgrade. Skipped when the CR is already deleting:
// stamping a finalizer on a terminating CR is pointless work
// (reconcileDeletion's Remove block would self-heal it on the same
// or next reconcile) and opens a small race window where a concurrent
// observer might see the CR re-blocked between the stamp and the
// removal.
func (r *ValkeyReconciler) ensurePVCRetentionFinalizer(ctx context.Context, v *valkeyv1beta1.Valkey, deleting bool) error {
	if deleting || controllerutil.ContainsFinalizer(v, PVCRetentionFinalizer) {
		return nil
	}
	patched := v.DeepCopy()
	controllerutil.AddFinalizer(patched, PVCRetentionFinalizer)
	if err := r.Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		return fmt.Errorf("adding pvc-retention finalizer: %w", err)
	}
	// Patch refreshed `patched` with the server's response — the STORED
	// object, whose spec may be less defaulted than the in-memory
	// normalized view Reconcile is working with. Re-normalize after the
	// assignment so the rest of this reconcile keeps rendering from the
	// admission-independent view.
	*v = *patched
	defaults.ApplySpecDefaults(v)
	return nil
}

// reconcileDeletion handles CR deletion per spec.pvcRetentionPolicy. For
// `Retain` (default), make sure no PVC owner-ref points at the CR (so
// owner-ref GC won't cascade). For `Delete`, patch owner-refs onto each
// matching PVC so owner-ref GC cascades cleanly.
//
// Hard guarantee via the PVCRetentionFinalizer: the apiserver blocks GC
// of the deleting CR until this function returns nil and removes the
// finalizer below. So the policy fires deterministically at deletion
// time even if the operator was offline when the user issued the
// delete — the CR sits in `Terminating` until the operator wakes up,
// applies the policy, and strips the finalizer.
//
// Per-PVC patches are idempotent (re-applying the same owner-ref shape
// is a no-op) and independent (a transient failure on one PVC doesn't
// block patching the rest). The loop accumulates per-PVC errors and
// continues so a single transient apiserver hiccup on one PVC doesn't
// indefinitely defer ownerref patches on the remaining PVCs — every
// reconcile makes maximum progress. The finalizer is only removed when
// the loop completes without errors, leaving the CR in `Terminating`
// (and the caller free to requeue) when any PVC patch failed.
func (r *ValkeyReconciler) reconcileDeletion(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(v.Namespace), client.MatchingLabels{CRLabel: v.Name}); err != nil {
		return fmt.Errorf("listing PVCs on delete: %w", err)
	}

	policy := v.Spec.PVCRetentionPolicy
	if policy == "" {
		policy = valkeyv1beta1.PVCRetentionRetain
	}

	var errs error
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		switch policy {
		case valkeyv1beta1.PVCRetentionDelete:
			if !hasCROwnerRef(pvc, v) {
				patched := pvc.DeepCopy()
				patched.OwnerReferences = append(patched.OwnerReferences, *metav1.NewControllerRef(v, valkeyv1beta1.GroupVersion.WithKind("Valkey")))
				if err := r.Patch(ctx, patched, client.MergeFrom(pvc)); err != nil {
					errs = errors.Join(errs, fmt.Errorf("patching PVC %q owner-ref: %w", pvc.Name, err))
				}
			}
		default: // Retain
			if hasCROwnerRef(pvc, v) {
				patched := pvc.DeepCopy()
				patched.OwnerReferences = stripCROwnerRef(patched.OwnerReferences, v)
				if err := r.Patch(ctx, patched, client.MergeFrom(pvc)); err != nil {
					errs = errors.Join(errs, fmt.Errorf("stripping PVC %q owner-ref: %w", pvc.Name, err))
				}
			}
		}
	}
	if errs != nil {
		return errs
	}

	// Policy applied successfully — remove the finalizer so the
	// apiserver can drop the CR. Skipped (no-op) when the finalizer is
	// already gone: covers the case where a previous reconcileDeletion
	// reached this line, removed the finalizer, but the apiserver hadn't
	// yet sent the NotFound on the next watch event so a stale Reconcile
	// is still in flight.
	if controllerutil.ContainsFinalizer(v, PVCRetentionFinalizer) {
		patched := v.DeepCopy()
		controllerutil.RemoveFinalizer(patched, PVCRetentionFinalizer)
		if err := r.Patch(ctx, patched, client.MergeFrom(v)); err != nil {
			return fmt.Errorf("removing pvc-retention finalizer: %w", err)
		}
	}
	// Drop process-lifetime dedup state for this CR's auth-Secret
	// short-password reporter. Without this, a CR recreated with the
	// same name + namespace would inherit the prior CR's silenced
	// state and a fresh short-password Secret would NOT re-emit the
	// warning until the operator restarts.
	r.ShortAuthPasswordReporter.Forget(v)
	// Same dedup contract for the FieldDeprecated reporter: a recreated
	// CR with the same identity must re-emit its first deprecation
	// observation rather than inherit silenced state from the prior CR.
	r.Deprecator.Forget(v)
	// Same dedup contract for the deviation emitter: a recreated CR with
	// the same identity must re-emit its first deviation observation
	// rather than inherit silenced state from the prior CR.
	r.DeviationEmitter.Forget(v)
	return nil
}

func hasCROwnerRef(pvc *corev1.PersistentVolumeClaim, v *valkeyv1beta1.Valkey) bool {
	for _, o := range pvc.OwnerReferences {
		if o.UID == v.UID {
			return true
		}
	}
	return false
}

func stripCROwnerRef(refs []metav1.OwnerReference, v *valkeyv1beta1.Valkey) []metav1.OwnerReference {
	out := refs[:0]
	for _, o := range refs {
		if o.UID != v.UID {
			out = append(out, o)
		}
	}
	return out
}

// clearSingleShotAnnotations strips the consumed operator-trigger
// annotations after a successful reconcile. Uses a merge-patch (not
// SSA) because we're operating on metadata only and want the change
// to be a single round-trip whether or not other field managers also
// touch annotations.
func (r *ValkeyReconciler) clearSingleShotAnnotations(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	if v.Annotations == nil {
		return nil
	}
	patched := v.DeepCopy()
	mutated := false
	for _, key := range singleShotAnnotations {
		if _, ok := patched.Annotations[key]; ok {
			delete(patched.Annotations, key)
			mutated = true
		}
	}
	if !mutated {
		return nil
	}
	return r.Patch(ctx, patched, client.MergeFrom(v))
}

// reconcileConfigMaps applies the per-CR ConfigMaps that back the
// pod's render-config init container:
//
//   - <cr>-valkey-conf — the rendered valkey.conf template, with the
//     _POD_IP_ placeholder the script substitutes at pod start.
//   - <cr>-init-scripts — the short shell script the init container
//     executes (substitution + conditional replicaof appendix).
//
// Returns the combined SHA-256 of (template || script) so the STS pod
// template hash flips when either changes. The combined hash drives
// the config-change rolling update; deriving it from the rendered
// output (not the raw spec fields) means a `configurationOverrides`
// edit correctly rolls the pods even though the underlying CR field
// can't drift on its own.
func (r *ValkeyReconciler) reconcileConfigMaps(ctx context.Context, v *valkeyv1beta1.Valkey) (string, error) {
	conf := valkeyconf.Render(valkeyconf.Inputs{
		Persistent:         v.Spec.Valkey.Persistence != nil,
		Configuration:      v.Spec.Valkey.Configuration,
		Overrides:          v.Spec.Valkey.ConfigurationOverrides,
		MinReplicasToWrite: v.Spec.Valkey.MinReplicasToWrite,
		MinReplicasMaxLag:  v.Spec.Valkey.MinReplicasMaxLag,
	})
	script := renderInitScript()
	hash := sha256Hex(conf + "\x00" + script)

	confName := v.Name + suffixValkeyConf
	confCM := corev1ac.ConfigMap(confName, v.Namespace).
		WithLabels(ownedLabels(v, componentValkey)).
		WithAnnotations(map[string]string{ConfigHashAnnotation: hash}).
		WithOwnerReferences(crOwnerRef(v)).
		WithData(map[string]string{"valkey.conf": conf})
	if err := ssa.ApplyAC(ctx, r.Client, confCM); err != nil {
		return "", fmt.Errorf("applying ConfigMap %q: %w", confName, err)
	}

	scriptName := v.Name + suffixInitScripts
	scriptCM := corev1ac.ConfigMap(scriptName, v.Namespace).
		WithLabels(ownedLabels(v, componentValkey)).
		WithAnnotations(map[string]string{ConfigHashAnnotation: hash}).
		WithOwnerReferences(crOwnerRef(v)).
		WithData(map[string]string{"render-valkey-conf.sh": script})
	if err := ssa.ApplyAC(ctx, r.Client, scriptCM); err != nil {
		return "", fmt.Errorf("applying ConfigMap %q: %w", scriptName, err)
	}

	return hash, nil
}

// crOwnerRef builds the controller owner-ref apply-config that points at
// the Valkey CR — the apply-config equivalent of metav1.NewControllerRef
// (which doesn't exist in the apply-config builder library). Centralised
// so per-resource builders don't repeat the field stamping.
func crOwnerRef(v *valkeyv1beta1.Valkey) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(valkeyv1beta1.GroupVersion.String()).
		WithKind("Valkey").
		WithName(v.Name).
		WithUID(v.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// renderInitScript is the shell script the render-config init container
// executes. Kept inline (not parameterised) so the rendered output is
// independent of CR-specific values — every CR's init-scripts ConfigMap
// holds the same bytes, and the hash only flips when the script itself
// changes.
//
// The script:
//
//   - Substitutes the literal `_POD_IP_` token for $POD_IP from the
//     Downward API (matches valkeyconf.PodIPPlaceholder so a rename in
//     the renderer surfaces as a unit-test failure, not a runtime
//     CrashLoopBackOff).
//   - Resolves a candidate primary IP from the optional sentinel-mode
//     bootstrap mount first, then falls back to the Service DNS lookup.
//     In standalone the bootstrap mount is absent and the Service has
//     no endpoint at first reconcile → MASTER_IP stays empty → no
//     `replicaof` line appended.
//   - Appends `replicaof <ip> 6379` on non-pod-0 pods whenever
//     MASTER_TARGET is non-empty. Pod-0 ALSO gets a `replicaof` line
//     when the operator seed names a different address that answers a
//     liveness PING (via `valkey-cli` + `timeout` in the valkey image)
//     — a replacement pod-0 rejoining mid-incident joins the live
//     elected primary as a replica instead of booting a second, empty
//     master. A dead seed fails the PING so pod-0 falls back to
//     booting as the bootstrap master, preserving total-restart
//     recovery. The pod-0 check uses POD_NAME and APP_NAME so the same
//     script works in standalone (no append), replication, and
//     sentinel.
func renderInitScript() string {
	return `#!/bin/sh
# Managed by velkir. Do not edit by hand; this ConfigMap is
# overwritten on every reconcile.
set -eu

# Substitute the pod-local IP placeholder. POD_IP is sourced from
# the Downward API on the init container.
sed -e "s|_POD_IP_|${POD_IP}|g" \
    /config-template/valkey.conf > /config/valkey.conf

# Resolve a candidate primary TARGET. Source preference (highest first):
#
#   1. Sentinel-mode bootstrap seed (mounted optional from the
#      <cr>-sentinel-bootstrap ConfigMap). Present only when the
#      operator has populated the seed; absent in standalone /
#      replication mode.
#   2. The StatefulSet's pod-0 stable headless DNS name. The headless
#      Service publishes per-pod DNS at
#      <pod-name>.<cr>-headless.<ns>.svc.cluster.local and is set
#      to publishNotReadyAddresses=true so DNS resolves even before
#      the operator labels pod-0 role=primary. valkey-server resolves
#      the hostname at startup and retries the connection on transient
#      failures, so a one-shot lookup window is not on the critical
#      path. This replaces the older getent-hosts-on-the-writer-
#      Service approach which both required getent in the runtime
#      image (not always present on Alpine builds) AND lost to a
#      CoreDNS NXDOMAIN window when the writer Service had no
#      endpoints during early bootstrap.
SEED_IP=""
if [ -r /bootstrap/seedMasterIP ]; then
    SEED_IP="$(cat /bootstrap/seedMasterIP 2>/dev/null || true)"
fi
MASTER_TARGET="${SEED_IP}"
if [ -z "${MASTER_TARGET}" ]; then
    MASTER_TARGET="${APP_NAME}-0.${APP_NAME}-headless.${POD_NAMESPACE}.svc.cluster.local"
fi

# Append replicaof for every secondary (non-pod-0) pod. Pod-0 also
# gets a replicaof line when the operator's seed names a DIFFERENT
# address AND that address answers as a live valkey — a replacement
# pod-0 booting while the elected primary lives elsewhere must join
# as its replica, never come up as a second, empty master. The
# liveness probe is what keeps total-restart recovery working: after
# a full cluster wipe the seed points at a dead address, the probe
# fails, and pod-0 boots as the bootstrap master exactly as before.
# Any RESP reply (PONG, NOAUTH before auth, LOADING during restore)
# counts as live; connect failures and timeouts do not. Standalone
# falls through (single pod, seed equals its own IP or is empty).
if [ "${POD_NAME:-}" != "${APP_NAME}-0" ]; then
    if [ -n "${MASTER_TARGET}" ]; then
        printf '\nreplicaof %s 6379\n' "${MASTER_TARGET}" >> /config/valkey.conf
    fi
elif [ -n "${SEED_IP}" ] && [ "${SEED_IP}" != "${POD_IP}" ]; then
    SEED_REPLY="$(timeout 3 valkey-cli -h "${SEED_IP}" -p 6379 ping 2>&1 || true)"
    case "${SEED_REPLY}" in
        *PONG*|*NOAUTH*|*LOADING*)
            printf '\nreplicaof %s 6379\n' "${SEED_IP}" >> /config/valkey.conf
            ;;
    esac
fi
`
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// reconcileStatefulSet applies the data-plane STS. PSA-restricted
// compatible: runAsNonRoot, allowPrivilegeEscalation=false, drop ALL
// capabilities, seccomp RuntimeDefault on both pod and containers.
//
// Scale safety has two layers:
//   - Structural: if the desired replica count would remove the
//     primary pod, the STS apply preserves the prior count and
//     emits `ScaleRefused`. Fires regardless of FSM state.
//   - Temporal: if the FSM is mid-rollout (Pending / RolloutReplicas /
//     RolloutPrimary / FailoverInFlight / RolloutComplete) and the
//     desired count differs from the current count in EITHER direction,
//     the STS apply preserves the prior count and emits `ScaleDeferred`.
//     The next reconcile after the FSM returns to Steady picks the
//     deferred count up via the normal path.
//
// Structural wins on collision (a primary-removing scale during a
// rollout fires ScaleRefused, not ScaleDeferred) — the data-plane
// safety property survives every transition, while the temporal
// deferral is a politeness gate.
func (r *ValkeyReconciler) reconcileStatefulSet(ctx context.Context, v *valkeyv1beta1.Valkey, cmHash string, state orchestration.State) error {
	// PVC resize substate guard: skip STS reconcile while the substate
	// machine has the STS orphan-deleted but PVCs not yet expanded.
	// Recreating the STS during this window would re-bind it to the
	// not-yet-expanded PVCs and lose the resize. The StsRecreated
	// phase is NOT guarded — it expects the STS to be recreated at the
	// new size from the (now expanded) PVCs.
	if isPVCResizeGuardingPhase2(currentPVCResizePhase(v)) {
		return nil
	}

	sts := buildValkeySTS(v, cmHash)

	existing := &appsv1.StatefulSet{}
	getErr := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, existing)
	switch {
	case getErr == nil:
		// Version-compat runtime check: reject major-version
		// downgrades outright, warn on skip-minor. Runs only when an
		// STS already exists (a transition observable; bootstrap
		// creates have no `from` to compare against). Lives in the
		// reconciler — not the admission webhook — because Flux
		// re-apply patterns lose the `oldObj` continuity admission
		// needs to see the transition. See
		// `internal/version/compat.go` package doc for the full
		// split rationale.
		if !r.checkValkeyImageTransition(v, existing) {
			// Hard reject: keep the existing STS image; drop this
			// reconcile pass without erroring (return nil so we
			// don't requeue churn — the user must edit the spec).
			return nil
		}
		currentReplicas := ptr.Deref(existing.Spec.Replicas, 0)
		desiredReplicas := v.Spec.Valkey.Replicas

		// Structural primary-removal guard runs first (it's the data-
		// plane safety property — must hold across every FSM state).
		structuralRefuse := false
		if desiredReplicas < currentReplicas {
			primaryOrd, primaryErr := r.findPrimaryOrdinal(ctx, v)
			if scaleDownPrimaryRefusal(desiredReplicas, currentReplicas, primaryOrd, primaryErr) {
				if primaryErr != nil {
					r.recordEventf(v, corev1.EventTypeWarning, string(events.ScaleRefused), "ScaleRefuse",
						"scale-down to %d replicas refused: could not determine primary ordinal (%v); retry once the apiserver is reachable",
						desiredReplicas, primaryErr)
				} else {
					r.recordEventf(v, corev1.EventTypeWarning, string(events.ScaleRefused), "ScaleRefuse",
						"scale-down to %d replicas would remove primary at ordinal %d; failover first or restore %d",
						desiredReplicas, primaryOrd, currentReplicas)
				}
				sts.Spec.WithReplicas(currentReplicas)
				structuralRefuse = true
			}
		}

		// Temporal deferral: any scale change while the FSM is
		// rolling. Skipped when structural refuse already preserved
		// the prior count (no need to double-emit).
		if !structuralRefuse && desiredReplicas != currentReplicas && isInRolloutState(state) {
			r.recordEventf(v, corev1.EventTypeNormal, string(events.ScaleDeferred), "ScaleDeferred",
				"scale from %d to %d replicas deferred while state=%s; applies on return to Steady",
				currentReplicas, desiredReplicas, state)
			// Preserve current replica count so SSA doesn't churn the
			// field while the rollout completes. Same WithReplicas
			// override mechanic as the structural branch.
			sts.Spec.WithReplicas(currentReplicas)
		}
	case apierrors.IsNotFound(getErr):
		// Bootstrap path — no STS yet, nothing to scale. Fall through
		// to the apply with the desired replicas.
	default:
		// Non-NotFound read failure: we can't tell whether the user's
		// requested replica count is a safe scale-up, a safe scale-
		// down, or a primary-removing one. Refuse to apply *any* STS
		// shape this pass — applying with the user's desired count
		// would silently bypass the primary-removal guard during the
		// blind window. Return the read error so controller-runtime
		// backs off and retries; emit ScalePrecheckFailed so the
		// on-call operator sees the blind window without grepping
		// logs.
		r.recordEventf(v, corev1.EventTypeWarning, string(events.ScalePrecheckFailed), "ScalePrecheckFail",
			"could not read existing StatefulSet for scale-down safety check: %v",
			getErr)
		return fmt.Errorf("scale-down precheck: STS read failed: %w", getErr)
	}

	if err := ssa.ApplyAC(ctx, r.Client, sts); err != nil {
		return fmt.Errorf("applying STS %q: %w", v.Name, err)
	}
	return nil
}

// FeatureGateUpgradePreflight is the per-CR feature-gate key that
// governs whether checkValkeyImageTransition's hard-rejection arm
// fires. Default behaviour (gate absent or explicitly true) keeps
// the preflight enforced: major-version downgrades are rejected,
// skip-minor transitions emit a Warning, everything else is silent.
// Users that explicitly set the gate to false opt out of the
// downgrade rejection — the operator records the override on the
// audit trail via ValkeyImageTransitionOverridden and lets the STS
// apply proceed. Intended for testbed / disaster-recovery scenarios
// where the operator-of-the-operator has accepted the data-format
// risk; not a path the user should leave enabled on a production
// cluster.
const FeatureGateUpgradePreflight = "UpgradePreflight"

// checkValkeyImageTransition is the runtime half of the version-
// compat split. Compares the desired Valkey image (CR spec) against
// the StatefulSet's currently-running Valkey image; returns false to
// reject the transition (the STS apply skips this pass and the
// cluster keeps running the existing image), true to allow the apply
// to proceed (with or without a soft warning event).
//
// Rules:
//
//   - Tag parse failure on either side: allow with no event. The
//     admission webhook's tag-shape rule already rejects
//     malformed tags on CR write; if a malformed tag reaches the
//     reconciler (custom registry path the admission accepts as
//     valid shape but the version parser doesn't recognize as a
//     Valkey major.minor), we don't second-guess the user.
//   - Major-version downgrade (`to.Major < from.Major`): default
//     emit `ValkeyImageTransitionRejected` Warning, return false.
//     Cross-major data-format compatibility is not an upstream
//     Valkey guarantee; the operator cannot safely roll back the
//     on-disk RDB / AOF. Bypassed when
//     `Spec.FeatureGates["UpgradePreflight"] == false`, in which
//     case `ValkeyImageTransitionOverridden` is emitted and the
//     apply proceeds.
//   - Skip-minor (`to.Major == from.Major && to.Minor - from.Minor > 1`):
//     emit `ValkeyImageTransitionWarning` Warning, return true. Per
//     `docs/versions.md` skip-version policy, two-or-more-minor
//     skips are best-effort during alpha. Not gated by
//     UpgradePreflight — the warning is informational, not a refusal.
//   - Otherwise (equal-or-supported transition): silent, return
//     true.
//
// The check operates on the StatefulSet's Pod template (the
// declarative target), not on currently-running Pod images — the
// rule concerns the spec transition, not the rolling-update
// progress.
func (r *ValkeyReconciler) checkValkeyImageTransition(v *valkeyv1beta1.Valkey, existing *appsv1.StatefulSet) bool {
	currentImage := findValkeyContainerImage(existing)
	if currentImage == "" {
		return true // STS exists but has no valkey container image (shouldn't happen post-bootstrap; allow).
	}
	desiredImage := v.Spec.Image.Valkey.Repository + ":" + v.Spec.Image.Valkey.Tag

	from, fromErr := version.ParseValkeyTag(currentImage)
	to, toErr := version.ParseValkeyTag(desiredImage)
	if fromErr != nil || toErr != nil {
		// Custom-registry / non-Valkey-shape image — the operator
		// declines to enforce major/minor rules on something it
		// can't reason about. Allow and stay silent.
		return true
	}
	if from == to {
		return true
	}
	if version.IsDowngrade(from, to) {
		// Comma-ok lookup distinguishes "absent" (default behaviour:
		// reject) from "explicitly false" (user opt-out: allow with
		// override emission). A missing key on a nil map returns the
		// zero value (false) but with ok==false, so the override
		// only fires when the user wrote the key.
		if enabled, set := v.Spec.FeatureGates[FeatureGateUpgradePreflight]; set && !enabled {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.ValkeyImageTransitionOverridden), "ValkeyImageTransitionOverride",
				"major-version downgrade %s → %s would be rejected by the version-compat preflight, "+
					"but spec.featureGates.UpgradePreflight=false bypasses the rejection. "+
					"Continuing with the apply; cross-major data-format compatibility is the user's responsibility.",
				from, to)
			return true
		}
		r.recordEventf(v, corev1.EventTypeWarning, string(events.ValkeyImageTransitionRejected), "ValkeyImageTransitionReject",
			"refusing major-version downgrade %s → %s (Valkey does not guarantee cross-major data-format compatibility); "+
				"keeping existing image %q. Revert spec.image.valkey.tag to a tag with major %d to clear this rejection, "+
				"or set spec.featureGates.UpgradePreflight=false to bypass the preflight (testbed only).",
			from, to, currentImage, from.Major)
		return false
	}
	if version.IsSkipMinor(from, to) {
		r.recordEventf(v, corev1.EventTypeWarning, string(events.ValkeyImageTransitionWarning), "ValkeyImageTransitionWarn",
			"transition %s → %s skips minor versions; per docs/versions.md skip-version policy, "+
				"two-or-more-minor skips are best-effort during alpha (validated only on current-1 → current). "+
				"Continuing with the apply.",
			from, to)
	}
	return true
}

// findValkeyContainerImage extracts the `image` of the container
// named "valkey" from the StatefulSet's Pod template, or "" when
// no such container exists (shouldn't happen post-bootstrap; the
// caller treats "" as "allow, nothing to compare").
func findValkeyContainerImage(sts *appsv1.StatefulSet) string {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "valkey" {
			return c.Image
		}
	}
	return ""
}

// findPrimaryOrdinal returns the ordinal of the pod currently
// labelled `velkir.ioxie.dev/role=primary`, or -1 when no such pod
// exists (bootstrap before Phase 7 stamps the label, or the
// observer has stripped the label during a failover window).
//
// A List failure is returned as a non-nil error rather than
// collapsed into the -1 "no primary" sentinel: the two are
// indistinguishable at the value level, and the scale-down guard
// must refuse-by-default on an apiserver flake instead of mistaking
// a transient List error for "no primary labelled" and waving a
// primary-removing scale-down through.
//
// When more than one pod carries role=primary (transient split-
// label state mid-failover), returns the LOWEST ordinal — that's
// the conservative choice for the scale-down refusal: the lowest
// primary-labelled pod's ordinal becomes the floor below which
// scale-down is unsafe.
func (r *ValkeyReconciler) findPrimaryOrdinal(ctx context.Context, v *valkeyv1beta1.Valkey) (int32, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
			RoleLabel:      roleValuePrimary,
		},
	); err != nil {
		return -1, err
	}
	if len(pods.Items) == 0 {
		return -1, nil
	}
	prefix := v.Name + "-"
	minOrd := int32(-1)
	for i := range pods.Items {
		name := pods.Items[i].Name
		suffix, ok := strings.CutPrefix(name, prefix)
		if !ok {
			continue
		}
		ord := parseOrdinal(suffix)
		if ord < 0 {
			continue
		}
		if minOrd < 0 || ord < minOrd {
			minOrd = ord
		}
	}
	return minOrd, nil
}

// scaleDownPrimaryRefusal decides whether a scale-down must be
// refused to protect the primary. It refuses when the primary
// ordinal could not be determined (primaryErr != nil — an apiserver
// flake must never silently wave a primary-removing scale-down
// through) or when the target replica count would delete the
// primary's pod (desired <= primaryOrd). A non-scale-down (desired
// >= current) is never refused here.
func scaleDownPrimaryRefusal(desired, current, primaryOrd int32, primaryErr error) bool {
	if desired >= current {
		return false
	}
	if primaryErr != nil {
		return true
	}
	return primaryOrd >= 0 && desired <= primaryOrd
}

// reconcilePodRollout drives the config-change rolling update: when
// the StatefulSet's UpdateRevision differs from CurrentRevision, the
// operator deletes stale-revision REPLICA pods one at a time so the
// STS controller (under our `OnDelete` strategy) recreates them
// against the new pod-template revision. The primary is left alone
// — its master-aware recreation is handled separately, gated by
// `SENTINEL FAILOVER` first.
//
// Sequencing:
//   - Skip in standalone mode (single pod; no rolling).
//   - Skip when STS revisions agree (no rollout needed).
//   - Skip when any pod is currently NotReady (a delete is
//     already in flight; wait for the recreate to finish before
//     deleting the next).
//   - In sentinel mode, defer (no delete) when the observed
//     sentinel quorum is not OK — deleting a replica while quorum
//     is fragile can drop the sentinel pool below quorum mid-roll;
//     requeue and re-check until it recovers.
//   - Pick the highest-ordinal stale REPLICA pod, delete it.
//     Highest-ordinal-first matches the STS controller's own
//     rollout direction so the operator's choice and the
//     controller's expectation agree.
//   - When the only remaining stale pod is the primary, emit
//     `RolloutDeferred` once and return. Suppressed on
//     subsequent reconciles (same-revision means we already
//     deferred for this rollout; a fresh hash change resets the
//     state).
//
// quorumOK is the observer-derived sentinel quorum verdict for this CR
// (facts.QuorumOK), already folded with snapshot presence: true only
// when a fresh observer snapshot reports QuorumOK. It is consulted for
// the sentinel-mode replica-delete gate below.
//
// Returns a RequeueAfter hint (0 = none) so a quorum-deferred roll
// retries on a short cadence; the caller folds it via mergeRequeue.
func (r *ValkeyReconciler) reconcilePodRollout(ctx context.Context, v *valkeyv1beta1.Valkey, quorumOK bool, valkeyPods []corev1.Pod, sts *appsv1.StatefulSet) (time.Duration, error) {
	if v.Spec.Mode == valkeyv1beta1.ModeStandalone {
		return 0, nil
	}

	// sts + valkeyPods are the once-per-reconcile snapshots threaded in.
	// nil sts means the StatefulSet doesn't exist yet. The
	// revision fields read below are set by the kube StatefulSet
	// controller asynchronously, so the top-of-pass snapshot is the
	// same value a fresh Get would return this pass.
	if sts == nil {
		return 0, nil
	}
	target := sts.Status.UpdateRevision
	current := sts.Status.CurrentRevision
	if target == "" || target == current {
		return 0, nil
	}

	// In-flight check: a stale pod still terminating (DeletionTimestamp
	// set) OR a fresh pod not yet Ready means an earlier delete from
	// the previous reconcile is still being absorbed. Wait.
	//
	// Also gate on ReplicationHealthy (the
	// `velkir.ioxie.dev/replication-ready` PodCondition stamped by
	// Phase 8). The replacement pod is "Ready" the moment its TCP
	// readiness probe passes, but the master_link_status / lag check
	// runs in Phase 8; advancing to the next replica before that
	// gate flips True risks rolling a replica that's still catching
	// up. The gate is only meaningful for non-standalone modes and
	// only when the CR has the readiness gate enabled —
	// podReplicationHealthy returns true when the gate is absent so
	// opt-out CRs aren't blocked.
	for i := range valkeyPods {
		p := &valkeyPods[i]
		if p.DeletionTimestamp != nil {
			return 0, nil
		}
		if !podReady(p) {
			return 0, nil
		}
		if !podReplicationHealthy(p) {
			return 0, nil
		}
	}

	// Partition the pod set into stale primary (skipped this milestone)
	// and stale replicas (the rollout queue).
	var (
		staleReplicas []*corev1.Pod
		staleHasPrim  bool
	)
	for i := range valkeyPods {
		p := &valkeyPods[i]
		if p.Labels[stsRevisionLabel] == target {
			continue
		}
		if p.Labels[RoleLabel] == roleValuePrimary {
			staleHasPrim = true
			continue
		}
		staleReplicas = append(staleReplicas, p)
	}

	if len(staleReplicas) == 0 {
		// All replicas are at target. If the primary is still on the
		// old revision, this is the master-aware-recreation hand-off
		// point.
		if staleHasPrim {
			r.recordEventf(v, corev1.EventTypeNormal, string(events.RolloutDeferred), "RolloutDefer",
				"primary pod still at old revision; master-aware recreation deferred")
		}
		return 0, nil
	}

	// Quorum preflight before deleting a replica (sentinel mode only).
	// A delete triggers an OnDelete recreate; while the pod is gone the
	// sentinel pool is one member lighter. If the observed quorum is
	// already not OK, removing a replica now can tip the pool below
	// quorum and leave no failover authority during the disruption
	// window — so hold the roll and re-check on a short cadence until
	// quorum recovers. Standalone returned above; replication has no
	// sentinel pool (its quorumOK is always false), so the gate is
	// sentinel-only — otherwise it would wedge every replication-mode
	// rollout.
	if v.Spec.Mode == valkeyv1beta1.ModeSentinel && !quorumOK {
		r.recordEventf(v, corev1.EventTypeNormal, string(events.RolloutDeferred), "RolloutDeferQuorum",
			"replica rollout deferred: sentinel quorum not OK; deleting a replica now risks dropping the pool below quorum mid-roll")
		return rolloutQuorumDeferRequeue, nil
	}

	// Highest-ordinal first — matches the STS controller's own rollout
	// shape so the operator's choice and the controller's expectation
	// converge.
	sort.Slice(staleReplicas, func(i, j int) bool {
		return ordinalFromPod(staleReplicas[i], v.Name) >
			ordinalFromPod(staleReplicas[j], v.Name)
	})

	victim := staleReplicas[0]
	if err := r.Delete(ctx, victim); err != nil && !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("deleting stale pod %q: %w", victim.Name, err)
	}
	// Arm the readiness watchdog for the just-deleted pod. The Check
	// + emit + disarm side fires every reconcile against this
	// substate; once the replacement pod is Ready +
	// replication-healthy the in-flight check above defers the next
	// delete, the substate is disarmed by updateStatus's Check pass,
	// and the next reconcile proceeds to the next replica. On stall,
	// watchdog.Check expires and the controller emits RolloutStalled
	// + Degraded.
	timeout := v.Spec.Rollout.ReplicaReadyTimeoutSeconds
	if timeout == 0 {
		// Defensive default — the validating webhook stamps 300s on
		// admission, but a status-patch path that bypasses validation
		// could see a zero value. The watchdog Arm clamps to [60,
		// 3600] regardless; substituting the spec default here keeps
		// log/event messages consistent with the documented contract.
		timeout = 300
	}
	if v.Status.Rollout == nil {
		v.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
	}
	v.Status.Rollout.MasterAware = orchestration.Arm(time.Now(), victim.Name, timeout, v.Status.Rollout.MasterAware)

	r.recordEventf(v, corev1.EventTypeNormal, string(events.PodRolledForConfig), "PodRollForConfig",
		"recreating pod %s for STS revision %s (was %s)",
		victim.Name, target, current)
	return 0, nil
}

// podReplicationHealthy reports whether the pod carries the
// `velkir.ioxie.dev/replication-ready` PodCondition with status=True.
// Returns true if the condition is absent (the CR has the readiness
// gate disabled), so opt-out callers aren't blocked. Used by Phase 9
// to gate replica-rollout advance on replication catch-up, not just
// on the stock PodReady (which can flip True before the gate's
// master_link_status check completes).
func podReplicationHealthy(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == ReplicationReadyGate {
			return c.Status == corev1.ConditionTrue
		}
	}
	return true
}

// podReady returns true when the pod has the standard `Ready`
// PodCondition with `status: True`. Used by Phase 9 to gate the
// next stale-pod deletion behind the previously-deleted pod
// finishing its restart cycle.
func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// parseOrdinal converts a decimal StatefulSet ordinal string to an
// int32, returning -1 when it is not a non-negative value that fits
// int32. The explicit upper bound keeps the narrowing conversion
// provably safe (a pod ordinal is always small in practice).
func parseOrdinal(s string) int32 {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > math.MaxInt32 {
		return -1
	}
	return int32(n)
}

// ordinalFromPodName returns the ordinal suffix of a StatefulSet
// pod, or -1 when the name doesn't match the canonical `<sts>-<N>`
// shape.
func ordinalFromPodName(podName, crName string) int32 {
	suffix, ok := strings.CutPrefix(podName, crName+"-")
	if !ok {
		return -1
	}
	return parseOrdinal(suffix)
}

// ordinalFromPod returns a StatefulSet pod's ordinal, preferring the
// standard `apps.kubernetes.io/pod-index` label the StatefulSet
// controller stamps since K8s 1.28 (guaranteed by the 1.30 floor) over
// a name-suffix parse. The label is authoritative: a future K8s
// release that decouples pod names from ordinals would silently break a
// pure-name match, so name parsing is only a fallback for the boot-race
// window before the label has propagated (controller-runtime cache
// lag). Returns -1 when neither source yields a valid non-negative
// ordinal. Mirrors the label-first detection in desiredRoleForPod.
func ordinalFromPod(pod *corev1.Pod, crName string) int32 {
	if idx, ok := pod.Labels[podIndexLabel]; ok {
		if o := parseOrdinal(idx); o >= 0 {
			return o
		}
	}
	return ordinalFromPodName(pod.Name, crName)
}

// reconcileServices applies the client-facing `<cr>` Service, the
// read-only `<cr>-ro` Service, and the STS-governing headless
// `<cr>-headless`.
//
// `<cr>` selector requires `role=primary` so writes only land on
// the pod the operator has stamped as the writeable primary. Empty
// endpoints during failover are intentional — the writes-pause
// window is bounded by the operator's failover machinery.
//
// `<cr>-ro` already requires `role=replica` — empty-endpoint state
// in standalone is intentional and harmless.
func (r *ValkeyReconciler) reconcileServices(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	labels := ownedLabels(v, componentValkey)

	port := corev1ac.ServicePort().
		WithName("valkey").
		WithPort(defaultValkeyPort).
		WithTargetPort(intstr.FromInt32(defaultValkeyPort)).
		WithProtocol(corev1.ProtocolTCP)

	clientSelector := mergeMaps(labels, map[string]string{RoleLabel: roleValuePrimary})
	clientSpec := corev1ac.ServiceSpec().
		WithType(serviceType(v.Spec.Service.Client.Type)).
		WithSelector(clientSelector).
		WithPorts(port)
	if len(v.Spec.Service.Client.LoadBalancerSourceRanges) > 0 {
		clientSpec.WithLoadBalancerSourceRanges(v.Spec.Service.Client.LoadBalancerSourceRanges...)
	}
	clientSvc := corev1ac.Service(v.Name, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(clientSpec)
	if err := ssa.ApplyAC(ctx, r.Client, clientSvc); err != nil {
		return fmt.Errorf("applying client Service: %w", err)
	}

	roSelector := mergeMaps(labels, map[string]string{RoleLabel: roleValueReplica})
	roSvc := corev1ac.Service(v.Name+"-ro", v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(corev1ac.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithSelector(roSelector).
			WithPorts(port))
	if err := ssa.ApplyAC(ctx, r.Client, roSvc); err != nil {
		return fmt.Errorf("applying read-only Service: %w", err)
	}

	headless := corev1ac.Service(v.Name+"-headless", v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(corev1ac.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithClusterIP(corev1.ClusterIPNone).
			WithSelector(labels).
			WithPublishNotReadyAddresses(true).
			WithPorts(port))
	if err := ssa.ApplyAC(ctx, r.Client, headless); err != nil {
		return fmt.Errorf("applying headless Service: %w", err)
	}
	return nil
}

func serviceType(t string) corev1.ServiceType {
	switch t {
	case "LoadBalancer":
		return corev1.ServiceTypeLoadBalancer
	case "NodePort":
		return corev1.ServiceTypeNodePort
	case "Headless":
		// The schema accepts "Headless" as an operator-level abstraction;
		// it maps to ClusterIP with clusterIP=None at the K8s level.
		return corev1.ServiceTypeClusterIP
	default:
		return corev1.ServiceTypeClusterIP
	}
}

// suffixClusterPDB is the per-CR name suffix for the role-agnostic
// cluster-wide PodDisruptionBudget. Its selector is intentionally
// role-free (CR + component only), so the match count stays invariant
// across role-label strip-then-restamp transitions —
// closing the zero-match window that role-scoped PDBs expose during
// failover and during the manual-relabel path. Stays the source of
// truth for "at least N pods of this CR's valkey side must remain
// healthy" while role-scoped PDBs come and go alongside the role
// label.
const suffixClusterPDB = "-cluster-pdb"

// reconcilePDB applies the role-agnostic cluster PodDisruptionBudget
// for the CR's valkey side. Standalone (replicas <= 1) short-circuits
// because a one-pod cluster has no useful disruption budget.
//
// The selector spans CRLabel + componentValkey only — never RoleLabel.
// That invariance is the load-bearing property: role-scoped PDBs
// briefly have zero selector matches during a role-strip-then-restamp
// (the failover-initiated and manual-relabel paths both produce a
// transient unlabeled state); the cluster PDB plugs that gap.
//
// minAvailable derives to `replicas - 1` (one disruption budget left
// for the operator's own rolling-update). When `v.Spec.Valkey.PDB` is
// set, the user's MinAvailable or MaxUnavailable wins — the webhook
// CEL guarantees the two are mutually exclusive, so the switch lands
// at most one populated field.
func (r *ValkeyReconciler) reconcilePDB(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	if v.Spec.Valkey.Replicas <= 1 {
		return nil
	}
	labels := ownedLabels(v, componentValkey)
	selector := metav1ac.LabelSelector().WithMatchLabels(map[string]string{
		CRLabel:        v.Name,
		ComponentLabel: componentValkey,
	})
	spec := policyv1ac.PodDisruptionBudgetSpec().WithSelector(selector)
	switch {
	case v.Spec.Valkey.PDB != nil && v.Spec.Valkey.PDB.MaxUnavailable != nil:
		spec.WithMaxUnavailable(*v.Spec.Valkey.PDB.MaxUnavailable)
	case v.Spec.Valkey.PDB != nil && v.Spec.Valkey.PDB.MinAvailable != nil:
		spec.WithMinAvailable(*v.Spec.Valkey.PDB.MinAvailable)
	default:
		spec.WithMinAvailable(intstr.FromInt32(v.Spec.Valkey.Replicas - 1))
	}
	pdb := policyv1ac.PodDisruptionBudget(v.Name+suffixClusterPDB, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(spec)
	if err := ssa.ApplyAC(ctx, r.Client, pdb); err != nil {
		return fmt.Errorf("applying cluster PDB: %w", err)
	}
	return nil
}

// reconcileRoleLabels stamps `velkir.ioxie.dev/role` on each valkey pod.
//
// For non-sentinel modes (and for sentinel mode before the observer
// has produced its first quorum-OK snapshot), the bootstrap topology
// rule applies: pod-0 is `primary`, all others are `replica`. The
// label feeds the `<cr>-ro` Service selector.
//
// For sentinel mode with a quorum-OK observer snapshot, the desired
// primary is the pod whose `Status.PodIP` matches the observer's
// reported `Primary.Addr`; everyone else is a replica. When the
// snapshot exists but reports `QuorumOK=false`, Phase 7 suppresses
// the relabel entirely (split-brain guard) — the prior label set is
// left in place to avoid flapping the `<cr>` Service selector
// mid-election.
//
// Idempotent — pods that already carry the desired value are
// skipped. The patch is a strategic-merge so other field managers'
// labels survive untouched.
func (r *ValkeyReconciler) reconcileRoleLabels(ctx context.Context, v *valkeyv1beta1.Valkey, password string) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return fmt.Errorf("listing valkey pods: %w", err)
	}

	// Rehydrate (post-restart) or clear the durable failover-dispatch
	// marker before deriving roles, so a strip+FAILOVER that was in
	// flight when the operator crashed still suppresses re-stamping the
	// pre-strip primary mid-election (the marker is the durable mirror of
	// the in-memory failover latch). No-op when no failover is in flight.
	r.maintainFailoverDispatchMarker(ctx, v, types.NamespacedName{Namespace: v.Namespace, Name: v.Name})

	desired, suppress := r.desiredRolesForCR(v, pods.Items)
	if suppress {
		// Split-brain guard fired in desiredRolesForCR (event +
		// metric already emitted there). Leave the prior label set
		// in place; the next reconcile retries.
		return nil
	}

	// add-before-remove ordering. Stamp the new role=primary
	// on its target pod FIRST, then strip role=primary off the old
	// primary (which is becoming role=replica). Without this, a
	// transition that stripped first would briefly leave 0 pods
	// matching the `<cr>` Service selector — the slice would empty
	// and fresh client connections would land on the wrong pod or
	// hit ConnectionRefused. The add-first ordering leaves a brief
	// 2-primary window instead; the Service selector matches both,
	// so a small fraction of fresh connections may transiently land
	// on the old (becoming-replica) pod and get READONLY, but the
	// majority land on the new primary. The subsequent demotion
	// patch in pass 2 closes the window.
	//
	// Pass 1: pods becoming role=primary (or unset → primary).
	// Pass 2: pods becoming role=replica (or unset → replica).
	// `unset → primary` is grouped with primary so a first-bootstrap
	// pass stamps the primary before flipping ordinals 1..N to
	// replica.
	log := logf.FromContext(ctx)
	patchOne := func(p *corev1.Pod, want string) error {
		current := p.Labels[RoleLabel]
		if current == want {
			return nil
		}
		old := p.DeepCopy()
		if p.Labels == nil {
			p.Labels = map[string]string{}
		}
		p.Labels[RoleLabel] = want
		if err := r.Patch(ctx, p, client.StrategicMergeFrom(old)); err != nil {
			return fmt.Errorf("patching pod %q role label: %w", p.Name, err)
		}
		fromText := current
		if fromText == "" {
			fromText = roleLabelUnset
		}
		// info-level log per label change so the
		// observability gap during a kill-cascade window closes
		// (Loki queries on the operator pod surface every
		// label-change decision with the source CR + before/after
		// role + the reconcileID propagated via ctx).
		log.Info("phase 7: role label patched",
			"cr", v.Namespace+"/"+v.Name,
			"pod", p.Name,
			"from", fromText,
			"to", want)
		r.recordEventf(v, corev1.EventTypeNormal, string(events.PodLabelReconciled), "PodLabelReconcile",
			"pod %s role %s -> %s", p.Name, fromText, want)
		auditRoleLabel(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name},
			p.Name, fromText, want, "steady_state")
		// Dynamic readiness-gate lifecycle. The replication-readiness
		// gate is added to replica pods only; on a `replica → primary`
		// transition we strip it from `pod.spec.readinessGates` AND
		// clear the matching condition from `.status.conditions`
		// (otherwise the gate's last-True condition would block the
		// new primary's Ready status, deadlocking the failover). On
		// the reverse transition we re-add the gate so the demoted
		// pod re-enters the normal sync-gating lifecycle.
		//
		// Best-effort: pod-spec mutation can fail under some K8s
		// admission policies (the project's 1.30 floor allows it
		// per the upstream pod-mutability matrix). Errors here are
		// logged but don't fail the reconcile pass — the role
		// label is already patched, and the next reconcile retries
		// the gate mutation.
		if !replicationGateEnabled(v) {
			return nil
		}
		if gateErr := r.applyRoleTransitionGate(ctx, p, want); gateErr != nil {
			log.V(1).Info("role-transition gate mutation failed",
				"pod", p.Name, "to", want, "err", gateErr.Error())
		}
		return nil
	}

	// Pass 1: primary stamps.
	for i := range pods.Items {
		p := &pods.Items[i]
		want, ok := desired[p.Name]
		if !ok || want != roleValuePrimary {
			continue
		}
		if err := patchOne(p, want); err != nil {
			return err
		}
	}
	// Pass 2: replica stamps (demotions land AFTER promotions so
	// the Service selector never empties mid-transition). Track
	// pods that flipped primary → replica so we can issue
	// CLIENT KILL on them in pass 3.
	var demoted []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		want, ok := desired[p.Name]
		if !ok || want == roleValuePrimary {
			// Either unclassified or already handled in pass 1.
			continue
		}
		// Capture pre-patch label BEFORE patchOne — patchOne mutates
		// p.Labels[RoleLabel] in place (via the in-memory write
		// before client.Patch), so reading p.Labels[RoleLabel]
		// AFTER the call would always return the new value. The
		// pass-3 CLIENT KILL pass only fires for true demotions
		// (primary → replica), so this pre-patch read is load-
		// bearing.
		wasPrimary := p.Labels[RoleLabel] == roleValuePrimary
		if err := patchOne(p, want); err != nil {
			return err
		}
		if wasPrimary {
			demoted = append(demoted, p)
		}
	}
	// Pass 3: CLIENT KILL TYPE normal on every
	// pod that just flipped primary → replica, so pooled write-
	// clients still ESTABLISHED on the (now-replica) pod's socket
	// drop and reconnect through the Service. Best-effort —
	// failure emits a Warning event but does not fail the reconcile
	// pass (the label patch already landed; new client connections
	// route correctly via the slice; pooled clients self-heal on
	// their library's reconnect cycle when this falls back).
	if len(demoted) > 0 {
		if killErr := r.issueClientKillOnDemoted(ctx, v, demoted, password); killErr != nil {
			// issueClientKillOnDemoted already logged + emitted per
			// pod; surface the aggregate failure at info level for
			// the reconcile trace and continue (do not fail).
			log.V(1).Info("phase 7: CLIENT KILL on demoted pods returned aggregate error",
				"cr", v.Namespace+"/"+v.Name, "demoted", len(demoted), "err", killErr.Error())
		}
	}

	// AllValkeyPodsDown detection — emit when the cluster has at
	// least one valkey pod present but no pod carries any role
	// label after the patch loop ran. This catches the "Phase 7
	// produced no assignments AND prior labels are gone" pathological
	// state (typical during a same-pass STS recreate, or after a
	// snapshot-suppress when the prior labels had already been
	// stripped). Only fires when r.Recorder is set.
	if len(pods.Items) > 0 {
		anyLabelled := false
		for i := range pods.Items {
			role := pods.Items[i].Labels[RoleLabel]
			if role == roleValuePrimary || role == roleValueReplica {
				anyLabelled = true
				break
			}
		}
		if !anyLabelled {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.AllValkeyPodsDown), "AllPodsDownObserve",
				"%d valkey pod(s) present but none carry a role=primary|replica label",
				len(pods.Items))
		}
	}

	// Primary-loss detection. Replication mode is the manual-
	// failover shape: when the pod labelled role=primary is gone
	// the operator does NOT auto-promote — the user is responsible
	// for restoring the primary, or for migrating to mode=sentinel
	// to gain HA. We re-list with the role=primary selector so the
	// post-patch label state is observed; the controller-runtime
	// informer cache may briefly lag the patches the loop just
	// issued, in which case the worst case is one false-positive
	// event that the EventRecorder server-side dedup absorbs and
	// the next reconcile clears.
	if v.Spec.Mode != valkeyv1beta1.ModeStandalone && len(pods.Items) > 0 {
		primaries := &corev1.PodList{}
		if err := r.List(ctx, primaries,
			client.InNamespace(v.Namespace),
			client.MatchingLabels{
				CRLabel:        v.Name,
				ComponentLabel: componentValkey,
				RoleLabel:      roleValuePrimary,
			},
		); err == nil && len(primaries.Items) == 0 {
			r.recordEventf(v, corev1.EventTypeWarning, string(events.ReplicationPrimaryLost), "PrimaryLostObserve",
				"no pod labelled role=primary across %d valkey pod(s); replication mode does not auto-promote — restore the primary or migrate to mode=sentinel for HA",
				len(pods.Items))
		}
	}
	return nil
}

// desiredRolesForCR returns the role each valkey pod should carry,
// driven by either the bootstrap topology rule (non-sentinel modes,
// or sentinel mode pre-snapshot) or the observer snapshot (sentinel
// mode with `QuorumOK=true`).
//
// The boolean second return is `suppressRelabel`: true when the
// observer snapshot is present but reports `QuorumOK=false`, in
// which case Phase 7 must NOT touch any pod label (split-brain
// guard). The `SplitBrainDetected` event + metric are emitted by
// updateQuorumSuppressionGate, edge-gated to once per disagreement
// episode and only on a real quorum loss
// (Quorum==QuorumStatusLost), never on Unknown (the restart
// placeholder / transient observer-unreachable window); the
// reconciler treats `suppressRelabel=true` as a no-op pass either way.
func (r *ValkeyReconciler) desiredRolesForCR(v *valkeyv1beta1.Valkey, pods []corev1.Pod) (map[string]string, bool) {
	if v.Spec.Mode != valkeyv1beta1.ModeSentinel || r.SentinelObserver == nil {
		return r.bootstrapRoles(v, pods), false
	}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	snap := r.SentinelObserver.Snapshot(cr)
	// !Present is the boot-race / observer-not-yet-published case.
	// Two sub-cases:
	//   - No pod currently carries a role label (true first-bootstrap):
	//     fall back to the bootstrap topology rule so the ro-Service
	//     selector has SOMETHING to match while the observer comes up.
	//   - At least one pod already carries a role label (operator
	//     restart with existing post-failover settled state): leave
	//     the label set alone. The bootstrap rule would
	//     blindly stamp pod-0=primary even when sentinels have
	//     promoted a different pod — and the `<cr>` Service selector
	//     would then route writes to a replica until the observer
	//     reconverged (~10-55s on a real cluster), producing a
	//     READONLY storm at the client. Trusting the existing labels
	//     preserves correctness until the observer publishes the
	//     authoritative primary IP.
	if !snap.Present {
		if anyValkeyPodLabelled(pods) {
			// Trust existing labels; the observer's next snapshot
			// will correct them. If a failover happened during the
			// operator-down window, the still-stale primary label
			// briefly mis-routes — but only the few pods the prior
			// settled state pointed at, not the bootstrap rule's
			// blindly-wrong pod-0.
			return nil, false
		}
		return r.bootstrapRoles(v, pods), false
	}
	if !snap.Primary.QuorumOK {
		// Suppress relabel whenever quorum isn't affirmatively OK
		// (Unknown OR Lost) — refuse to act on incomplete agreement.
		// The SplitBrainDetected event + counter live in
		// updateQuorumSuppressionGate, edge-gated to the start of a
		// Lost episode, so a sustained loss doesn't re-emit on every
		// reconcile and Unknown (the restart placeholder / transient
		// observer-unreachable window) never emits at all.
		return nil, true
	}
	// Failover-in-flight latch suppression. After
	// runPrimaryRolloutDispatch strips role=primary and dispatches
	// SENTINEL FAILOVER, the observer's snapshot still points at the
	// old primary's Addr until +switch-master arrives — re-stamping
	// role=primary now would undo the strip and reopen the write-
	// loss window. The latch auto-clears (inside failoverLatchActive)
	// when snap.Primary.Addr changes off the pre-strip Addr with an
	// epoch ≥ the strip's (a lower-epoch move is a stale view and the
	// suppression holds) or the deadline expires; on those edges we fall
	// through to the normal snapshot-driven assignment below and the new
	// primary gets its label.
	if r.failoverLatchActive(cr, snap.Primary.Addr, snap.Primary.Epoch) {
		return nil, true
	}
	// QuorumOK=true: map snapshot.Addr → pod by IP.
	primaryHost, _, err := net.SplitHostPort(snap.Primary.Addr)
	if err != nil {
		// Malformed snapshot Addr — fall back to bootstrap rule
		// rather than wedging Phase 7. The next reconcile after
		// the observer republishes will retry.
		return r.bootstrapRoles(v, pods), false
	}
	// NoMasterAgreement guard: if the observer reports a primary
	// Addr that doesn't match ANY current valkey pod, sentinels
	// have agreed on a defunct IP — typically a pod that was
	// replaced during operator downtime, so its old IP is gone.
	// Stamping every pod as "replica" (the else branch below)
	// would silently demote the actual prior primary; falling back
	// to the bootstrap rule would silently re-promote pod-0 even
	// when pod-0 is a slave. Both shapes mis-route writes via the
	// `<cr>` Service. Instead, suppress the relabel so the
	// existing label set holds while the deferred status closure
	// surfaces Degraded=NoMasterAgreement + Ready=False via
	// observeNoMasterAgreement. Per-CR event surfaces the same
	// signal for `kubectl describe`.
	if !podIPMatchesAny(primaryHost, pods) {
		r.recordEventf(v, corev1.EventTypeWarning, string(events.NoMasterAgreement), "NoMasterAgreementObserve",
			"sentinel observer reports primary=%s but no current valkey pod has that IP; suppressing role-label relabel until sentinels recover",
			snap.Primary.Addr)
		return nil, true
	}
	// Settling damp: a primary MOVE — relabeling role=primary from
	// one pod to a different one — only commits once the newly-observed
	// primary Addr has been named by ≥2 consecutive fresh
	// (LastPolledAt-advancing) observer polls, suppressing the post-Ready
	// label flap where the observer briefly oscillates between addresses
	// before the election settles. Initial stamps (no pod currently
	// labeled primary) and no-change reconciles pass through immediately,
	// so the damp adds no latency to first-bootstrap or steady state.
	// freshPolls accrues over the sentinel poll cadence (itself sized
	// relative to down-after-milliseconds), so two fresh polls is a
	// down-after-multiple dwell without a separate wall-clock gate. This
	// is the two-snapshot stability the orchestration NewPrimaryStable
	// concept names, computed here against the live observer cursor.
	const newPrimaryStableMinPolls = 2
	freshPolls, _ := r.stateFor(cr).observePrimaryStability(snap.Primary.Addr, snap.Primary.LastPolledAt)
	currentPrimaryPod, desiredPrimaryPod := "", ""
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP != "" && p.Status.PodIP == primaryHost {
			desiredPrimaryPod = p.Name
		}
		if p.Labels[RoleLabel] == roleValuePrimary {
			currentPrimaryPod = p.Name
		}
	}
	if currentPrimaryPod != "" && desiredPrimaryPod != "" &&
		currentPrimaryPod != desiredPrimaryPod && freshPolls < newPrimaryStableMinPolls {
		return nil, true
	}
	out := make(map[string]string, len(pods))
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP != "" && p.Status.PodIP == primaryHost {
			out[p.Name] = roleValuePrimary
		} else {
			out[p.Name] = roleValueReplica
		}
	}
	return out, false
}

// issueClientKillOnDemoted runs CLIENT KILL TYPE normal SKIPME yes
// on each pod that just flipped role=primary → role=replica.
// Best-effort: per-pod failures emit a Warning event + continue;
// the function returns the first transport error encountered
// (only for the reconcile-trace log line — the caller does not
// propagate this error to controller-runtime). password is the auth
// password resolved once at Phase 0d and threaded in; a missing
// Secret is already handled by Phase 0d before any phase runs.
func (r *ValkeyReconciler) issueClientKillOnDemoted(ctx context.Context, v *valkeyv1beta1.Valkey, demoted []*corev1.Pod, password string) error {
	log := logf.FromContext(ctx)

	issuer := r.ClientKillIssuer
	if issuer == nil {
		issuer = &valkey.DialingClientKillIssuer{}
	}

	var firstErr error
	for _, p := range demoted {
		if p == nil {
			// Defensive: pass-2 only appends non-nil pods, but a
			// future refactor of the patch loop shouldn't be able
			// to crash this best-effort pass with a nil deref.
			continue
		}
		if p.Status.PodIP == "" {
			// Pod has no IP (deleted, terminating, never scheduled).
			// Skip — no socket to kill against; the next reconcile
			// catches up if the pod reappears.
			continue
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		killed, err := issuer.KillNormalClients(ctx, addr, password)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			r.recordEventf(v, corev1.EventTypeWarning, string(events.PrimaryClientsDropFailed),
				"ClientKillFail",
				"CLIENT KILL on demoted pod %s failed: %s; pooled clients will see -READONLY until they reconnect on their own",
				p.Name, err.Error())
			log.Info("phase 7: CLIENT KILL on demoted pod failed",
				"cr", v.Namespace+"/"+v.Name, "pod", p.Name, "err", err.Error())
			continue
		}
		if killed > 0 {
			// Destructive data-plane action (the operator severs client
			// connections it did not open) — record it in the audit trail
			// alongside failover/RESET/rotation, not only as a K8s Event.
			// Only when something was actually dropped: a zero-count kill
			// changed no live state and would just be audit noise.
			audit.Log(ctx, audit.Event{
				Name: audit.EventClientsKilled,
				CR:   types.NamespacedName{Namespace: v.Namespace, Name: v.Name},
				Attrs: map[string]string{
					"pod":    p.Name,
					"count":  strconv.Itoa(killed),
					"reason": "primary_demotion",
				},
			})
		}
		r.recordEventf(v, corev1.EventTypeNormal, string(events.PrimaryClientsDropped),
			"ClientKillIssued",
			"CLIENT KILL on demoted pod %s dropped %d client connection(s)",
			p.Name, killed)
		log.Info("phase 7: CLIENT KILL on demoted pod",
			"cr", v.Namespace+"/"+v.Name, "pod", p.Name, "dropped", killed)
	}
	return firstErr
}

// anyValkeyPodLabelled returns true when at least one pod carries
// either `role=primary` or `role=replica`. Used by the sentinel-mode
// pre-snapshot path to distinguish first-bootstrap (no labels yet,
// safe to apply the topology rule) from operator-restart (existing
// labels reflect the prior settled state, trust them until the
// observer reconverges).
func anyValkeyPodLabelled(pods []corev1.Pod) bool {
	for i := range pods {
		role := pods[i].Labels[RoleLabel]
		if role == roleValuePrimary || role == roleValueReplica {
			return true
		}
	}
	return false
}

// bootstrapRoles returns the bootstrap topology assignment: ordinal-0
// is primary, all others are replica. Used pre-quorum-snapshot in
// sentinel mode and for every reconcile pass in standalone /
// replication modes.
func (r *ValkeyReconciler) bootstrapRoles(v *valkeyv1beta1.Valkey, pods []corev1.Pod) map[string]string {
	out := make(map[string]string, len(pods))
	for i := range pods {
		out[pods[i].Name] = desiredRoleForPod(&pods[i], v.Name)
	}
	return out
}

// desiredRoleForPod returns the role the operator wants stamped on
// pod at the *bootstrap* topology stage — the StatefulSet's
// ordinal-0 pod is the canonical primary; sentinel-driven failover
// relabelling consults the observer snapshot via desiredRolesForCR.
//
// Ordinal detection prefers the standard
// `apps.kubernetes.io/pod-index` label that the StatefulSet
// controller stamps on every pod since K8s 1.28 (the project's
// 1.30 floor guarantees presence). Name-suffix matching is a
// fallback for the boot-race window where the label may not
// have propagated yet (controller-runtime cache lag) and for any
// future K8s version that decouples pod names from ordinals — a
// long-discussed StatefulSet improvement that would silently
// break a pure-name match without the label-first path.
func desiredRoleForPod(pod *corev1.Pod, crName string) string {
	if idx, ok := pod.Labels[podIndexLabel]; ok {
		if idx == "0" {
			return roleValuePrimary
		}
		return roleValueReplica
	}
	if pod.Name == crName+"-0" {
		return roleValuePrimary
	}
	return roleValueReplica
}

// replicationGateEnabled returns true when the operator should stamp
// the `velkir.ioxie.dev/replication-ready` gate on this CR's pods.
// Standalone is always off (the gate is meaningless — there are no
// replicas); replication / sentinel default to on with explicit
// opt-out via spec.valkey.readinessGate.enabled=false.
func replicationGateEnabled(v *valkeyv1beta1.Valkey) bool {
	if v.Spec.Mode == valkeyv1beta1.ModeStandalone {
		return false
	}
	if v.Spec.Valkey.ReadinessGate.Enabled == nil {
		return true
	}
	return *v.Spec.Valkey.ReadinessGate.Enabled
}

// readinessGateMaxLagBytes is the nil-safe accessor for
// `spec.valkey.readinessGate.maxLagBytes`. The defaulter stamps 1 MiB
// when the field is unset, so post-admission the pointer should never
// be nil — but this function still returns the same default for the
// envtest paths that build a CR without exercising the defaulter.
func readinessGateMaxLagBytes(v *valkeyv1beta1.Valkey) int64 {
	if v.Spec.Valkey.ReadinessGate.MaxLagBytes == nil {
		return 1 << 20 // 1 MiB
	}
	return *v.Spec.Valkey.ReadinessGate.MaxLagBytes
}

// buildReadinessGates returns the pod-template's readinessGates
// list. Empty in standalone or with the gate explicitly disabled —
// kube-scheduler treats an empty list as "no extra gates", same as
// nil. Otherwise the single ReplicationReadyGate entry is stamped;
// Phase 8 patches the matching condition on each pod's status as it
// becomes ready.
func buildReadinessGates(v *valkeyv1beta1.Valkey) []*corev1ac.PodReadinessGateApplyConfiguration {
	if !replicationGateEnabled(v) {
		return nil
	}
	return []*corev1ac.PodReadinessGateApplyConfiguration{
		corev1ac.PodReadinessGate().WithConditionType(ReplicationReadyGate),
	}
}

// reconcileReadinessGates implements Phase 8. For each valkey pod
// with a PodIP and the ReplicationReadyGate present in spec:
//
//   - role=primary → patch the gate condition to True (the primary
//     is replication-ready by definition; without this the pod
//     stays Unready forever, breaking bootstrap).
//   - role=replica → call the LagChecker. LinkUp + lag below the
//     CR's MaxLagBytes → True; everything else → False (or skip if
//     the connection failed and we'd churn the condition).
//
// Standalone short-circuits — the gate isn't stamped on the
// template (replicationGateEnabled returns false), so there are no
// pods to patch. This phase costs nothing in the standalone path.
//
// Patches go through the `pods/status` subresource (the only
// subresource verb the operator holds — see RBAC marker block
// above).
func (r *ValkeyReconciler) reconcileReadinessGates(ctx context.Context, v *valkeyv1beta1.Valkey, password string) (time.Duration, error) {
	log := logf.FromContext(ctx)
	if !replicationGateEnabled(v) {
		return 0, nil
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return 0, fmt.Errorf("listing valkey pods: %w", err)
	}
	log.V(1).Info("gate-trace: listed valkey pods", "count", len(pods.Items))

	checker := r.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	maxLag := readinessGateMaxLagBytes(v)

	// Drive the reconcile cadence off the gate state. Replication lag is
	// a dynamic property only observable by polling the pod over TCP —
	// neither the apiserver nor the controller-runtime cache surface lag
	// changes as events — so the gate is re-evaluated on a timer:
	//   - gate not yet True (bootstrap / catching up): poll fast at
	//     readinessGateRequeue so it converges quickly.
	//   - steady replica (gate already True): re-check at the relaxed
	//     replicaSteadyRecheck cadence — still catches a dropped link and
	//     flips the gate back to False (dropping the replica from the read
	//     Service), but without the 5s steady-state churn that scaled
	//     linearly with CR count.
	// Aggregated via mergeRequeue so the shortest needed cadence wins: a
	// single converging pod keeps the whole CR at 5s; once every gate is
	// steady the CR settles to the relaxed cadence. (Primary pods don't
	// drive the cadence; their gate is True-by-definition and stable.)
	var requeue time.Duration
	// Look up (or create) the per-CR stale-replica tracker. Tracks
	// when THIS operator instance first observed each replica's gate
	// as False, keyed by pod UID. Phase 8 measures staleness against
	// this in-memory timestamp instead of the apiserver-side
	// LastTransitionTime — the apiserver clock keeps running while
	// the operator is down, so the post-operator-restart window must
	// start fresh.
	crKey := client.ObjectKeyFromObject(v)
	crState := r.stateFor(crKey)
	tracker := crState.staleReplicaTracker()
	// Cap deletions per reconcile pass — even when multiple replicas
	// cross the staleness threshold, only one delete fires before the
	// 5s requeue. The next pass re-evaluates the (now-recreated) pod
	// before considering the next stale candidate. Prevents
	// "delete-all-replicas-at-once" on an operator returning from a
	// long downtime where every replica's gate was already False.
	deletedThisPass := false
	now := time.Now()

	// Gate stale-replica deletion behind "we have an agreed master."
	// Zero primary-labeled pods is the "active recovery" state —
	// sentinel-driven failover hasn't landed a primary, OR Phase 7
	// stripped/suppressed primary labels because the observer
	// reports a master IP not matching any pod (the NoMasterAgreement
	// wedge). In either case, deleting a replica now removes a
	// future failover candidate; the cluster needs the replicas
	// around so sentinel can promote one. Skip deletions until at
	// least one pod is labeled primary.
	//
	// Why count from the same pods list rather than a fresh
	// MatchingLabels query: the pods list above is already filtered
	// to (CRLabel + ComponentLabel=valkey), so the role check is
	// in-memory and free. A fresh apiserver round-trip would race
	// the patches Phase 7 just issued; the in-memory snapshot is
	// strictly more consistent with the rest of this reconcile
	// pass.
	primaryLabeledCount := countPrimaryLabeledPods(pods.Items)
	suppressStaleDelete := primaryLabeledCount == 0 && len(pods.Items) > 0

	// Phase-8 bounded-escape state. When stale-delete is suppressed (no
	// primary-labeled pod), a deferred recovery promotion that never
	// lands would otherwise block the only self-heal for a replica
	// wedged on a dead master IP forever. The escape (after the loop)
	// may, past a long sustained no-primary dwell, delete ONE least-
	// fresh stale replica — but only in a state the recovery-promotion
	// path itself would admit. Track the dwell here and accumulate the
	// survey the escape reuses from the CheckLag readings this loop
	// already performs (zero extra sentinel round-trips).
	var noPrimarySince time.Time
	var livePodIPs map[string]struct{}
	escapeArmed := false
	if suppressStaleDelete {
		noPrimarySince = crState.observeNoPrimarySince(now)
		// Allocated only on the suppressed path — the escape survey is
		// consulted only there, so the healthy steady state never pays for
		// (or populates) this map.
		livePodIPs = make(map[string]struct{}, len(pods.Items))
		// The escape's dwell + cooldown guards are loop-independent, so
		// hoist them: while either would disarm the escape anyway, the
		// gate-less classification below must not pay its per-pass
		// CheckLag dial (an ordinary failover window strips the
		// ex-primary's gate and holds suppression at the 5s cadence for
		// minutes — every dial in that window would be discarded by the
		// dwell guard). Gated-pod readings stay free reuse either way.
		escapeArmed = crState.staleReplicaEscapeArmedAt(noPrimarySince, now)
	} else {
		crState.clearNoPrimarySince()
	}
	var escapeMasters []surveyedPod
	var escapeObs []replicaGateObs
	var dialFailures, pendingPods int

	for i := range pods.Items {
		p := &pods.Items[i]
		hasGate := podHasReplicationGate(p)
		role := p.Labels[RoleLabel]
		log.V(1).Info("gate-trace: candidate pod",
			"pod", p.Name, "role", role, "hasGate", hasGate, "podIP", p.Status.PodIP)

		// Phase-8 escape survey: fold EVERY pod's live IP (or its IP-less
		// pending state) into the survey — gate-less pods included. A
		// just-stripped ex-primary keeps its PodIP after its role label and
		// ReplicationReadyGate are removed; promotionSurveyAdmits' live-
		// lineage check keys off this IP set, so omitting a gate-less pod
		// would let a replica pointing at that still-live address read as a
		// dead lineage and wrongly admit the escape. Accumulated only when
		// stale-delete is suppressed — the escape's sole precondition; on
		// the healthy path the survey is never consulted, so building it
		// would just churn allocations.
		if suppressStaleDelete {
			if p.Status.PodIP == "" {
				pendingPods++
			} else {
				livePodIPs[p.Status.PodIP] = struct{}{}
			}
		}

		if !hasGate {
			// Gate-less pod (e.g. a stripped ex-primary still running). It has
			// no ReplicationReadyGate to reconcile, but when the escape is
			// armed it must still be dialed and classified into the escape
			// survey: a gate-less live de-facto master must land in
			// escapeMasters so promotionSurveyAdmits' len(masters)!=0 disarm
			// trips even when the surveyed replicas point at a DIFFERENT dead
			// master (a corpse absent from livePodIPs, which the live-lineage
			// check alone would miss). A gate-less pod is never a stale-replica
			// victim (eligible false). Gated on escapeArmed — not bare
			// suppression — because this dial is the survey's only NEW wire
			// call: while the dwell/cooldown guards would discard the reading
			// anyway, the dial must not run.
			if escapeArmed {
				r.classifyGatelessEscapePod(ctx, p, role, password, checker,
					&escapeMasters, &escapeObs, &dialFailures)
			}
			continue
		}
		// Any gated pod whose condition is not yet True drives the
		// fast requeue cadence. The owned-Pod watch wakes the
		// operator on PodIP/phase/condition changes, but a gate flips
		// True only when a lag poll observes the replica caught up, and
		// lag is invisible to every watch — so a converging gate cannot
		// be event-driven: without the fast poll the operator never
		// re-checks lag, never patches the gate True, and the watch that
		// would fire on that patch never sees it. Cover both roles — a
		// mode=replication pod-0 primary whose gate hasn't flipped True
		// yet relies on this too, so the canonical single-live-pod
		// bootstrap converges instead of stranding with no requeue.
		current := findReplicationCondition(p)
		gateReady := current != nil && current.Status == corev1.ConditionTrue
		switch {
		case !gateReady:
			// Gate not yet True (bootstrap / catching up): poll fast
			// until it converges. Covers both roles — a mode=replication
			// pod-0 primary whose gate hasn't flipped True yet relies on
			// this too.
			requeue = mergeRequeue(requeue, readinessGateRequeue)
		case role == roleValueReplica:
			// Steady replica (gate already True): lag is poll-only and
			// invisible to every watch, so re-check at the relaxed
			// cadence — still catches a dropped link, without the 5s
			// steady-state churn that scaled with CR count.
			requeue = mergeRequeue(requeue, replicaSteadyRecheck)
		}
		if p.Status.PodIP == "" {
			// Gated pod with no IP yet (Pending / ContainerCreating): a
			// potential master-in-waiting the escape must defer to. Its
			// pending state is already folded into the survey above; skip
			// the dial.
			continue
		}

		desiredStatus, message, lagState, gateErr := r.evaluateReplicationGate(ctx, p, password, maxLag, checker, v)
		log.V(1).Info("gate-trace: evaluated", "pod", p.Name, "desired", desiredStatus, "message", message)
		patched := patchReplicationCondition(ctx, r.Client, p, desiredStatus, message)
		log.V(1).Info("gate-trace: patch outcome", "pod", p.Name, "patched", patched)
		if patched {
			r.recordEventf(v, corev1.EventTypeNormal, string(events.ReplicationGatePatched), "ReplicationGatePatch",
				"pod %s gate -> %s (%s)", p.Name, desiredStatus, message)
		}

		// A role=replica pod whose gate reads False has a stale-replica
		// timer. Load its first-seen time ONCE here and derive BOTH the
		// escape eligibility and the stale-delete staleness below from this
		// single read (LoadOrStore is idempotent — it writes only when the
		// entry is absent).
		staleReplicaFalse := role == roleValueReplica && desiredStatus == corev1.ConditionFalse
		var firstSeen time.Time
		if staleReplicaFalse {
			fs, _ := tracker.LoadOrStore(p.UID, now)
			firstSeen = fs.(time.Time)
		}

		// Classify this pod into the Phase-8 escape survey, reusing the
		// reading just taken (no extra dial). A dial error (topology
		// uncertainty), a reachable de-facto master, or any link-up
		// replica each disarms the escape via promotionSurveyAdmits. Only
		// when stale-delete is suppressed — the survey is consulted only on
		// that path, so the healthy path skips the accumulation entirely.
		if suppressStaleDelete {
			eligible := staleReplicaFalse && now.Sub(firstSeen) >= staleReplicaEscapeVictimDwell
			appendEscapeReading(&escapeMasters, &escapeObs, &dialFailures, p, role, lagState, gateErr, eligible)
		}

		// Update the per-pod tracker against the live evaluation: any
		// True observation clears the timer (recovery wiped it); any
		// False observation already recorded its first-seen time above.
		if desiredStatus == corev1.ConditionTrue {
			tracker.Delete(p.UID)
			continue
		}
		if role != roleValueReplica {
			continue
		}

		// Stuck-replica recovery. A replica whose gate has been False
		// for longer than staleReplicaThreshold has lost its
		// replication link (typically pointing at a primary IP that
		// no longer exists after a multi-step rolling update). The
		// kubelet won't restart the pod (the gate is False, not the
		// container probe), sentinel can't reconfigure it (its
		// SLAVEOF arrives at an old IP or arrives during a rolling
		// restart and doesn't stick), and the operator's primary-
		// rollout dispatch refuses to fail over (no healthy replica
		// for sentinel to promote → NOGOODSLAVE). Deleting the pod
		// lets the STS controller re-create it; the init container
		// then renders valkey.conf with the current `seedMasterIP`
		// (which `seedMasterIPForCR` keeps in sync with the elected
		// primary), and the fresh pod replicates from the live master.
		if deletedThisPass {
			continue
		}
		staleFor := now.Sub(firstSeen)
		if staleFor < staleReplicaThreshold {
			continue
		}
		if suppressStaleDelete {
			// Cluster is in active recovery (no role=primary). A
			// future sentinel-driven failover will need this
			// replica around to promote. Deletion would shrink the
			// promotion candidate set during the exact window where
			// the cluster most needs them.
			log.V(1).Info("gate-trace: stale-replica delete suppressed (no primary-labeled pod)",
				"pod", p.Name, "staleFor", staleFor.String(), "primaryLabeledCount", primaryLabeledCount)
			continue
		}
		if delErr := r.Delete(ctx, p); delErr != nil && !apierrors.IsNotFound(delErr) {
			log.Info("gate-trace: stale-replica delete failed",
				"pod", p.Name, "err", delErr.Error())
			continue
		}
		log.Info("gate-trace: deleted stale replica; STS controller will re-create",
			"pod", p.Name, "staleFor", staleFor.String())
		r.recordEventf(v, corev1.EventTypeWarning, string(events.ReplicationGatePatched), "StaleReplicaRecreated",
			"pod %s gate stuck False for %s (observed by this operator instance); deleted to force reinit",
			p.Name, staleFor.Truncate(time.Second))
		tracker.Delete(p.UID)
		deletedThisPass = true
	}

	// Phase-8 bounded escape: a last-resort, rate-limited delete of ONE
	// least-fresh stale replica when a deferred recovery promotion has
	// failed to land across a long sustained no-primary window. Runs only
	// when stale-delete was suppressed this pass and no inline delete
	// already fired (preserves the one-delete-per-pass cap).
	if suppressStaleDelete && !deletedThisPass {
		r.maybeEscapeStaleReplica(ctx, v, crKey, crState, livePodIPs,
			escapeMasters, escapeObs, dialFailures, pendingPods, noPrimarySince, now, tracker)
	}
	return requeue, nil
}

// replicaGateObs is one gated pod's Phase-8 reading, retained so the
// bounded escape can rebuild the recovery-promotion survey without
// re-dialing.
type replicaGateObs struct {
	pod       *corev1.Pod
	uid       types.UID
	name      string
	ip        string
	labelRole string // p.Labels[RoleLabel] at read time
	state     valkey.LagState
	// eligible is true only for a role=replica-labelled pod whose own
	// gate-False dwell has reached staleReplicaEscapeVictimDwell — the
	// per-victim guard against thrashing a just-recreated pod (a new UID
	// resets its firstSeen, so a freshly-recreated pod is never a victim).
	eligible bool
}

// appendEscapeReading buckets one surveyed pod's INFO reading into the
// Phase-8 escape survey accumulators: a dial error → dialFailures (any
// topology uncertainty disarms the escape); a self-reported master →
// escapeMasters (a reachable master disarms it); otherwise → escapeObs.
// eligible marks a stale-replica victim candidate — always false for a
// gate-less pod, which is never a victim.
func appendEscapeReading(
	masters *[]surveyedPod,
	obs *[]replicaGateObs,
	dialFailures *int,
	p *corev1.Pod,
	role string,
	state valkey.LagState,
	dialErr error,
	eligible bool,
) {
	switch {
	case dialErr != nil:
		*dialFailures++
	case state.Role == valkey.RoleMaster:
		*masters = append(*masters, surveyedPod{name: p.Name, ip: p.Status.PodIP, state: state})
	default:
		*obs = append(*obs, replicaGateObs{
			pod: p, uid: p.UID, name: p.Name, ip: p.Status.PodIP,
			labelRole: role, state: state, eligible: eligible,
		})
	}
}

// classifyGatelessEscapePod dials a gate-less pod and folds its reading
// into the escape survey. Gate-less pods carry no ReplicationReadyGate,
// so the Phase-8 loop does not otherwise dial them; but a stripped
// ex-primary still running as a de-facto master must be classified into
// escapeMasters so the escape disarms (promotionSurveyAdmits' zero-master
// requirement) regardless of where the surveyed replicas point — the
// live-lineage IP check alone misses it when the replicas follow a
// DIFFERENT dead master. A gate-less pod is never a stale-replica victim
// (eligible false). No-op when the pod has no PodIP (the caller already
// counted it as pending).
func (r *ValkeyReconciler) classifyGatelessEscapePod(
	ctx context.Context,
	p *corev1.Pod,
	role, password string,
	checker valkey.LagChecker,
	masters *[]surveyedPod,
	obs *[]replicaGateObs,
	dialFailures *int,
) {
	if p.Status.PodIP == "" {
		return
	}
	addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
	state, err := checker.CheckLag(ctx, addr, password)
	appendEscapeReading(masters, obs, dialFailures, p, role, state, err, false)
}

// chooseEscapeVictim picks, among eligible non-protected replicas, the
// one with the LOWEST applied replication offset (ties: lexically
// HIGHEST name, so the promotion path's lowest-name tie-break winner is
// never the victim). A replica with no readable offset
// (HaveSlaveOffset false) has an unknown true offset and is never chosen
// — treated as +inf so deleting it can never discard the freshest data;
// the fully-unrankable set is disarmed upstream (maybeEscapeStaleReplica
// guard 9). Returns -1 when no rankable eligible non-protected replica
// exists.
func chooseEscapeVictim(replicas []surveyedPod, eligible []bool, protectedIdx int) int {
	victim := -1
	for i := range replicas {
		if !eligible[i] || i == protectedIdx {
			continue
		}
		if !replicas[i].state.HaveSlaveOffset {
			// Unknown offset — treat as +inf so it is never the
			// lowest-offset victim.
			continue
		}
		if victim == -1 {
			victim = i
			continue
		}
		cand, cur := replicas[i], replicas[victim]
		if cand.state.SlaveReplOffset < cur.state.SlaveReplOffset ||
			(cand.state.SlaveReplOffset == cur.state.SlaveReplOffset && cand.name > cur.name) {
			victim = i
		}
	}
	return victim
}

// maybeEscapeStaleReplica is Phase 8's bounded, last-resort escape from
// the no-primary stale-delete suppression. Past a long sustained
// no-primary dwell it deletes ONE least-fresh stale replica to force STS
// re-creation (re-rendering valkey.conf at the current seedMasterIP) so
// a recovery that a deferred promotion never unblocked can proceed. It
// fires ONLY in a state promotionSurveyAdmits accepts — reusing that
// exact gate means it never fires where a reachable master exists, never
// spans divergent lineages, and (via choosePromotionCandidate) never
// deletes the replica the recovery election would promote. Returns
// whether it deleted a pod.
//
// Unlike the fast recovery-promotion path it does NOT re-read the pod
// view through the strong-consistency APIReader: the sustained dwell
// (staleReplicaEscapeDwell) that gates this escape IS the consistency
// window — a pod the informer cache transiently missed would have
// surfaced across the many reconciles spanning that dwell — and the
// UID precondition on the delete closes the same-name-recreate race the
// re-read would otherwise catch.
func (r *ValkeyReconciler) maybeEscapeStaleReplica(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	cr types.NamespacedName,
	crState *perCRState,
	livePodIPs map[string]struct{},
	masters []surveyedPod,
	obs []replicaGateObs,
	dialFailures, pendingPods int,
	noPrimarySince, now time.Time,
	tracker *sync.Map,
) bool {
	log := logf.FromContext(ctx)
	// Guard 1: sustained-no-primary dwell.
	if noPrimarySince.IsZero() || now.Sub(noPrimarySince) < staleReplicaEscapeDwell {
		return false
	}
	// Guard 2: sentinel mode only. In replication mode "no primary
	// label" is the documented manual-failover contract — there is no
	// recovery promotion to unblock, so the escape must not fire.
	if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" {
		return false
	}
	// Guard 3: per-CR escape rate bound (at most one per threshold).
	if !crState.staleReplicaEscapeAllowed(now) {
		return false
	}
	// Guard 4: defer to an in-flight operator-driven failover.
	if r.IsFailoverInFlight(cr) {
		return false
	}
	// Guard 5: defer to a landing recovery promotion. Phase 8 precedes
	// Phase 11 in-pass, so a REPLICAOF NO ONE issued last pass must be
	// given its cooldown to take effect before we disturb the pod set.
	// Read under the quorum mutex (maybeRecoveryPromote writes the stamp
	// under the same lock).
	if q := crState.quorumIfPresent(); q != nil && q.recoveryPromotionCooldownActive(now) {
		return false
	}
	// Guard 6: endpoints (gated behind 1-5, so not a per-reconcile
	// cost). No sentinels or a List error → cannot admit.
	sentinelPods, err := r.listSentinelPods(ctx, v)
	if err != nil {
		return false
	}
	endpoints := r.sentinelEndpointsFromPods(sentinelPods)
	if len(endpoints) == 0 {
		return false
	}
	// Rebuild the survey from the readings the loop already took.
	replicas := make([]surveyedPod, len(obs))
	eligible := make([]bool, len(obs))
	for i := range obs {
		replicas[i] = surveyedPod{name: obs[i].name, ip: obs[i].ip, state: obs[i].state}
		eligible[i] = obs[i].eligible
	}
	survey := &valkeyPodSurvey{
		livePodIPs:   livePodIPs,
		pendingPods:  pendingPods,
		dialed:       true,
		masters:      masters,
		replicas:     replicas,
		dialFailures: dialFailures,
	}
	// Guard 7: the whole recovery-promotion admission conjunction —
	// dialFailures==0, no reachable master, no link-up replica, non-empty
	// master_host, single dead lineage, no pending pods.
	if !promotionSurveyAdmits(ctx, cr, survey, endpoints) {
		return false
	}
	// Guard 8: never the last replica.
	if len(replicas) < 2 {
		return false
	}
	// Guard 9: victim selection — never the promotion candidate. A fully
	// unrankable set (no replica reports slave_repl_offset) disarms,
	// mirroring the recovery election's defer: with no comparable offsets
	// there is no provably-safe least-fresh victim to delete.
	protectedIdx := choosePromotionCandidate(replicas)
	if protectedIdx == -1 {
		return false
	}
	victim := chooseEscapeVictim(replicas, eligible, protectedIdx)
	if victim < 0 {
		return false
	}
	target := obs[victim].pod
	// Delete by name is guarded with a UID precondition: if the STS
	// controller recreated a same-name pod (new UID) between the cache
	// snapshot this survey was built from and now, the precondition fails
	// and the fresh pod is spared instead of wrongly deleted.
	if delErr := r.Delete(ctx, target, client.Preconditions{UID: &target.UID}); delErr != nil && !apierrors.IsNotFound(delErr) {
		log.Info("gate-trace: stale-replica escape delete failed",
			"pod", target.Name, "err", delErr.Error())
		return false
	}
	crState.recordStaleReplicaEscape(now)
	tracker.Delete(obs[victim].uid)
	dwell := now.Sub(noPrimarySince).Truncate(time.Second)
	log.Info("gate-trace: stale-replica escape deleted least-fresh replica",
		"pod", target.Name, "noPrimaryFor", dwell.String())
	r.recordEventf(v, corev1.EventTypeWarning, string(events.StaleReplicaEscapeDeleted), "StaleReplicaEscapeDeleted",
		"no primary-labeled pod for %s; deleted least-fresh stale replica %s to force reinit (highest-offset replica preserved)",
		dwell, target.Name)
	return true
}

// staleReplicaThreshold is the gate-False dwell time after which
// Phase 8 deletes a replica pod for STS controller re-creation.
// Longer than the typical bootstrap/sync window (60-90s on shared
// cluster) plus a safety margin so transient sync states never
// trigger a delete; shorter than the e2e Ready-recheck assertions
// (3 min) so scenario 2's post-rolling reconverge wins.
const staleReplicaThreshold = 90 * time.Second

// The Phase-8 bounded escape is bounded by three distinct durations,
// all set to the same value but kept separate so each can be tuned
// independently. Set far above staleReplicaThreshold (90s) and the e2e
// Ready-recheck windows so ordinary bootstrap, failover, and rolling
// flows always resolve first; the escape only fires when a deferred
// promotion has provably failed to land for this long.
const (
	// staleReplicaEscapeDwell is the sustained-no-primary dwell (guard 1)
	// after which the escape may delete ONE least-fresh stale replica
	// even while stale-delete is otherwise suppressed (no primary-labeled
	// pod).
	staleReplicaEscapeDwell = 10 * time.Minute
	// staleReplicaEscapeCooldown bounds the per-CR escape rate (at most
	// one escape per cooldown per CR).
	staleReplicaEscapeCooldown = 10 * time.Minute
	// staleReplicaEscapeVictimDwell is the per-victim gate-False dwell a
	// replica must sustain before it is escape-eligible — the guard
	// against thrashing a just-recreated pod (a new UID resets its
	// firstSeen).
	staleReplicaEscapeVictimDwell = 10 * time.Minute
)

// readinessGateRequeue is the fast cadence at which Phase 8 re-evaluates
// a replica whose replication-ready gate has not yet converged to True
// (bootstrap / catching up). Lag is not visible to the apiserver — only
// polling reveals it — so a converging replica is re-checked aggressively
// until its gate flips True.
const readinessGateRequeue = 5 * time.Second

// replicaSteadyRecheck is the relaxed cadence for a replica whose gate is
// already True. Lag stays invisible to every watch — neither the owned-Pod
// watch (PodIP/phase/condition changes) nor the observer push (sentinel
// topology events) surface a caught-up replica silently falling behind — so
// a dedicated poll remains the only lag-drift detector; this bounds that
// detection window (the gate flips back to False, dropping the replica from
// the read Service). With the Pod watch and observer push now covering every
// non-lag wake-up, this poll relaxes toward baselineReconcileWatchdog: 2
// minutes trades a longer steady-state lag-detection window for far fewer
// no-op reconciles than the prior 30s (which itself replaced the original 5s
// per-replica churn). Converging replicas keep the faster
// readinessGateRequeue — their gate flips True only when a lag poll observes
// catch-up, so bootstrap convergence must not be slowed.
const replicaSteadyRecheck = 2 * time.Minute

// readyConvergeRequeue is the cadence at which the status-write
// defer re-fires reconcile while the STS hasn't reached its desired
// ReadyReplicas. STS-watch events fire when StatefulSet.Status
// changes, but on a loaded cluster informer-cache propagation +
// work-queue depth can delay event delivery by tens of seconds. The
// active poll guarantees Ready transitions True within a bounded
// window once the underlying pods are healthy.
const readyConvergeRequeue = 5 * time.Second

// evaluateReplicationGate decides what the gate condition should be
// for one pod. Returns the desired status and a short reason string
// for the event message / condition message.
func (r *ValkeyReconciler) evaluateReplicationGate(
	ctx context.Context,
	p *corev1.Pod,
	password string,
	maxLag int64,
	checker valkey.LagChecker,
	v *valkeyv1beta1.Valkey,
) (corev1.ConditionStatus, string, valkey.LagState, error) {
	role := p.Labels[RoleLabel]
	if role == roleValuePrimary {
		return corev1.ConditionTrue, "primary; replication-ready by definition", valkey.LagState{}, nil
	}
	addr := fmt.Sprintf("%s:%d", p.Status.PodIP, valkey.DefaultPort)
	state, err := checker.CheckLag(ctx, addr, password)
	if err != nil {
		r.recordEventf(v, corev1.EventTypeWarning, string(events.ReplicationGateCheckFailed), "ReplicationGateCheckFail",
			"pod %s lag check failed: %v", p.Name, err)
		// Return the error so the Phase-8 escape can count this pod as a
		// dial failure (any uncertainty about the replication topology
		// disarms the escape).
		return corev1.ConditionFalse, fmt.Sprintf("lag check failed: %v", err), state, err
	}
	if state.Role == valkey.RoleMaster {
		// Operator's view (role=replica label) disagrees with the
		// pod's runtime view (`role:master` from INFO). Trust the
		// pod — a sentinel-driven flip happened that the observer
		// hasn't relabelled yet.
		return corev1.ConditionTrue, "pod self-reports as master", state, nil
	}
	if !state.LinkUp {
		return corev1.ConditionFalse, "master_link_status:down", state, nil
	}
	if state.LagBytes > maxLag {
		return corev1.ConditionFalse, fmt.Sprintf("lag %d > %d bytes", state.LagBytes, maxLag), state, nil
	}
	return corev1.ConditionTrue, fmt.Sprintf("link up; lag %d bytes", state.LagBytes), state, nil
}

// podHasReplicationGate returns true when the pod's spec carries the
// ReplicationReadyGate entry. Without the gate in spec, patching
// the condition is a no-op for kube-scheduler (the condition has no
// gate to gate readiness on).
func podHasReplicationGate(p *corev1.Pod) bool {
	for _, g := range p.Spec.ReadinessGates {
		if g.ConditionType == ReplicationReadyGate {
			return true
		}
	}
	return false
}

// patchReplicationCondition writes the desired condition status to
// the pod's status subresource. Returns true when a patch was issued
// (current condition differed); false when the condition was
// already at the desired status (no-op).
func patchReplicationCondition(ctx context.Context, c client.Client, p *corev1.Pod, desired corev1.ConditionStatus, message string) bool {
	current := findReplicationCondition(p)
	if current != nil && current.Status == desired && current.Message == message {
		return false
	}
	old := p.DeepCopy()
	now := metav1.Now()
	updated := corev1.PodCondition{
		Type:               ReplicationReadyGate,
		Status:             desired,
		Message:            message,
		LastProbeTime:      now,
		LastTransitionTime: now,
		ObservedGeneration: p.Generation,
	}
	if current != nil && current.Status == desired {
		// Status unchanged but message did — preserve the original
		// transition time so dashboards charting "time in status"
		// don't reset on a cosmetic message edit.
		updated.LastTransitionTime = current.LastTransitionTime
	}
	replaced := false
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == ReplicationReadyGate {
			p.Status.Conditions[i] = updated
			replaced = true
			break
		}
	}
	if !replaced {
		p.Status.Conditions = append(p.Status.Conditions, updated)
	}
	if err := c.Status().Patch(ctx, p, client.MergeFrom(old)); err != nil {
		// Patch failures are surfaced through the next reconcile
		// (the condition stays at its prior value on this pod;
		// next tick re-attempts).
		logf.FromContext(ctx).Info("gate-trace: pods/status patch failed",
			"pod", p.Name, "err", err.Error())
		return false
	}
	return true
}

func findReplicationCondition(p *corev1.Pod) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == ReplicationReadyGate {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

// lookupAuthPassword reads the password key from the auth Secret
// referenced by spec.auth.secretName. Convention: the Secret has a
// `password` data key. Returns empty string when no auth is
// configured (the LagChecker will skip AUTH).
//
// The returned cleanup func is always non-nil and MUST be invoked
// (typically via defer) once the password is no longer in scope. It
// releases the redaction-registry registration the function makes on
// the success path; on the no-auth, error, and short-password paths
// it is a no-op closure, so callers can `defer cleanup()`
// unconditionally without checking err first.
//
// Reads via APIReader (uncached) so unlabeled user-supplied Secrets
// resolve correctly — the cluster's label-narrowed informer cache
// excludes them.
//
// Side-effect: when the password is non-empty but shorter than
// `logging.MinTokenLen`, fires the AuthSecretShortPassword warning
// event (deduped per CR-Secret tuple). The redaction registry
// silently drops sub-floor tokens, so without this surfacing an
// operator running with a short password gets no signal that their
// log surface won't be scrubbed.
func (r *ValkeyReconciler) lookupAuthPassword(ctx context.Context, v *valkeyv1beta1.Valkey) (string, func(), error) {
	return lookupAuthPasswordWithRedaction(ctx, r.userSecretReader(), r.ShortAuthPasswordReporter, v)
}

// lookupAuthPasswordWithRedaction resolves the master auth password from
// spec.auth.secretName via reader, fires the short-password warning, and
// registers it — plus the sentinel-auth password when
// spec.auth.sentinelAuthSecretName is set — with the redaction registry.
// Returns the master password, a cleanup that deregisters every token it
// added (non-nil on every path), and any Secret-read error. Shared by the
// reconciler's per-reconcile resolution and the sentinel startup-reset
// safety net so both scrub identically (the latter previously registered
// only the master password, leaving the sentinel-auth value unredacted).
func lookupAuthPasswordWithRedaction(ctx context.Context, reader client.Reader, reporter *events.ShortAuthPasswordReporter, v *valkeyv1beta1.Valkey) (string, func(), error) {
	if v.Spec.Auth == nil || v.Spec.Auth.SecretName == "" {
		return "", func() {}, nil
	}
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.Auth.SecretName}, secret); err != nil {
		return "", func() {}, err
	}
	password := string(secret.Data["password"])
	reportShortAuthPassword(ctx, reporter, v, v.Spec.Auth.SecretName, password)
	return password, registerAuthRedaction(ctx, reader, v, password), nil
}

// registerAuthRedaction (method) delegates to the package-level helper
// using the reconciler's user-Secret reader, preserving the Phase-0d and
// lookupAuthPassword call sites unchanged.
func (r *ValkeyReconciler) registerAuthRedaction(ctx context.Context, v *valkeyv1beta1.Valkey, password string) func() {
	return registerAuthRedaction(ctx, r.userSecretReader(), v, password)
}

// registerAuthRedaction registers the master auth password — and, when
// spec.auth.sentinelAuthSecretName is set, the sentinel-auth password —
// with the redaction registry, returning a cleanup that deregisters
// every token it added. reader fetches the sentinel-auth Secret. Shared
// by the reconciler (per-reconcile Phase-0d resolution + lookupAuthPassword)
// and the sentinel startup-reset path so both scrub identically; threading
// the resolved password into the phases means only this single
// registration runs per reconcile instead of one per phase that re-read
// the Secret.
func registerAuthRedaction(ctx context.Context, reader client.Reader, v *valkeyv1beta1.Valkey, password string) func() {
	cleanup := logging.DefaultRegistry.RegisterScoped(password)

	// When spec.auth.sentinelAuthSecretName is set, its
	// resolved value lands in the sentinel pod's SENTINEL_AUTH_PASS
	// env var and (via the init script) the sentinel.conf auth-pass
	// directive. The value never flows through the operator's own
	// observer (which still uses the master password) but COULD
	// surface in log lines if init-container errors or sentinel
	// startup errors include the env value. Register it for
	// redaction here so any future surfacing is automatically
	// scrubbed — matches the security posture of the master
	// password's registration above.
	if v.Spec.Auth != nil && v.Spec.Auth.SentinelAuthSecretName != "" {
		sentinelSecret := &corev1.Secret{}
		if err := reader.Get(ctx,
			types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.Auth.SentinelAuthSecretName},
			sentinelSecret); err == nil {
			sentinelKey := v.Spec.Auth.SentinelAuthSecretKey
			if sentinelKey == "" {
				sentinelKey = defaultAuthSecretKey
			}
			if sentinelPw := string(sentinelSecret.Data[sentinelKey]); sentinelPw != "" && sentinelPw != password {
				sentinelCleanup := logging.DefaultRegistry.RegisterScoped(sentinelPw)
				prev := cleanup
				cleanup = func() { prev(); sentinelCleanup() }
			}
		}
		// Best-effort: a missing sentinelAuthSecretName Secret is a
		// real configuration error that surfaces elsewhere (chart
		// install fails or sentinel pod fails to start due to env
		// resolution); we don't want to fail the entire reconcile
		// just because the redaction registration couldn't fire.
	}
	return cleanup
}

// reportShortAuthPassword fires the AuthSecretShortPassword event +
// paired structured INFO log when the resolved password sits in the
// (0, logging.MinTokenLen) range. Empty passwords are no-ops (the CR
// either has no auth or the Secret has no `password` key — neither is
// a configuration smell of *this* kind). Lengths ≥ MinTokenLen are
// the steady state and equally a no-op. The reporter handles dedup;
// the caller branches on its return so the log fires exactly once
// per dedup boundary.
func reportShortAuthPassword(ctx context.Context, reporter *events.ShortAuthPasswordReporter, v *valkeyv1beta1.Valkey, secretName, password string) {
	pwdLen := len(password)
	if pwdLen == 0 || pwdLen >= logging.MinTokenLen {
		return
	}
	if !reporter.Emit(v, secretName, pwdLen, logging.MinTokenLen) {
		return
	}
	logf.FromContext(ctx).Info("auth Secret password is shorter than redaction registry MinTokenLen; the configuration smell will not be redacted from operator logs",
		"cr", v.Namespace+"/"+v.Name,
		"secretName", secretName,
		"passwordLen", pwdLen,
		"minLen", logging.MinTokenLen)
}

// userSecretReader returns the reader to use for user-supplied Secret
// reads (auth Secrets referenced by spec.auth.secretName). Prefers the
// uncached APIReader; falls back to the cached Client when APIReader
// is nil (tests with fake clients).
func (r *ValkeyReconciler) userSecretReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// observeReplicationHealthy folds the per-pod ReplicationReadyGate
// conditions into the aggregate that drives the ReplicationHealthy
// condition to a definite value. Standalone returns the zero
// value — evalReplicationHealthy short-circuits to NotApplicable
// before reading it. When the readiness gate is disabled
// (spec.valkey.readinessGate.enabled=false) the operator can't track
// master_link/lag, so it reports GateEnabled false and the evaluator
// surfaces True/NotApplicable rather than hanging at Unknown. A transient pod-list failure yields zero ready
// replicas (the condition reads WaitingForReplicas, never Unknown);
// the next reconcile retries.
func (r *ValkeyReconciler) observeReplicationHealthy(ctx context.Context, v *valkeyv1beta1.Valkey) orchestration.ReplicationObservation {
	if v == nil || v.Spec.Mode == valkeyv1beta1.ModeStandalone {
		return orchestration.ReplicationObservation{}
	}
	obs := orchestration.ReplicationObservation{
		GateEnabled: v.Spec.Valkey.ReadinessGate.Enabled != nil && *v.Spec.Valkey.ReadinessGate.Enabled,
	}
	if !obs.GateEnabled {
		return obs
	}
	pods, err := r.listValkeyPods(ctx, v)
	if err != nil {
		return obs
	}
	for i := range pods {
		p := &pods[i]
		if p.Labels[RoleLabel] != roleValueReplica {
			continue
		}
		if cond := findReplicationCondition(p); cond != nil && cond.Status == corev1.ConditionTrue {
			obs.ReadyReplicas++
		}
	}
	return obs
}

// updateStatus runs the per-condition evaluators against the
// observation, derives the cosmetic `phase`, and patches the status
// subresource. Emits a `DegradedResolved` event when Degraded just
// flipped True → False so dashboards / alert silencers see the
// recovery moment as a discrete event rather than only as a
// disappearing alert.
//
// Called from the deferred closure in Reconcile, so it runs on every
// termination path — `reconcileErr` carries the actual phase failure
// (or nil at success), `paused` reflects the pause-annotation state
// observed at the top of the reconcile, `sts` is a best-effort
// snapshot (nil when the STS isn't yet created or we couldn't read
// it), and `watchdog` is the readiness-watchdog verdict the closure
// computed from the prior reconcile's substate. When the watchdog
// has expired the closure has already emitted RolloutStalled; this
// function disarms the substate on the patched copy so the next
// reconcile sees a fresh (inactive) watchdog and the event fires
// exactly once per expiry.
func (r *ValkeyReconciler) updateStatus(ctx context.Context, v *valkeyv1beta1.Valkey, sts *appsv1.StatefulSet, reconcileErr error, paused bool, watchdog orchestration.Result, sqResult sqaggregate.Result, splitBrainActive bool, noMasterAgreementActive bool) error {
	prior := append([]metav1.Condition{}, v.Status.Conditions...)
	// Read the per-CR suppression-gate state so the QuorumLost
	// condition tracks the same hysteresis as the QuorumLost /
	// QuorumReached events. LoadOrStore returns a fresh zero state
	// for CRs we haven't observed yet (gate inactive → condition
	// follows FreshCount path).
	suppressionActive := false
	masterLostActive := false
	linkupStuckActive := false
	topologyMismatchActive := false
	topologySentinelDeficit := 0
	topologyReplicaDeficit := 0
	if v.Spec.Mode == valkeyv1beta1.ModeSentinel {
		// Stranded-sentinel repair stuck: freshness-gated so a stale
		// flag (dispatcher stopped running) ages out rather than
		// latching the Degraded condition — same posture as
		// masterLost / dual-master.
		linkupStuckActive = r.stateFor(client.ObjectKeyFromObject(v)).quorumTracker().strandedLinkupStuckActiveOrExpire(r.now())
		state := r.stateFor(client.ObjectKeyFromObject(v)).quorumTracker()
		state.mu.Lock()
		suppressionActive = state.suppressionActive
		// masterInfoTimeoutSince / masterInfoObservedAt are stamped by
		// observeMasterInfoTimeout, which runs only on passes that reach
		// Phase 11. The status defer runs on every pass, so read both:
		// `since` drives the dead-master-still-labelled signal, and
		// `observedAt` gates it on a recent measurement so an early-return
		// pass (where the probe never ran) can't pin Ready off a stale
		// latch. Same liveness source as the MasterInfoTimeoutSeconds
		// gauge, no extra dial.
		since := state.masterInfoTimeoutSince
		observedAt := state.masterInfoObservedAt
		state.mu.Unlock()
		// Hysteresis + freshness: declare MasterLost only once the
		// contiguous INFO failure has lasted at least the CR's down-after
		// window AND a probe observed it recently. The down-after gate
		// stops a single slow probe (a >2s fork-for-RDB / GC pause /
		// kubelet-probe restart) from flapping Ready and keeps the
		// operator from calling the master lost before Sentinel's own
		// death-detection window — so the "awaiting Sentinel-driven
		// promotion" message stays accurate.
		downAfter := int32(0)
		if v.Spec.Sentinel != nil {
			downAfter = v.Spec.Sentinel.DownAfterMilliseconds
		}
		masterLostActive = masterLostFromTimeout(since, observedAt, downAfter, r.now())

		// Topology-mismatch hygiene: single-writer of the
		// valkey_sentinel_topology_mismatch gauge, derived from the same
		// freshness-gated read that drives the SentinelTopologyReconciled
		// condition — so the gauge can't latch after the observation ages
		// out. A stale stamp is expired here so the gauge returns to 0 and
		// the next episode re-fires its event. Only sentinel-mode CRs emit
		// a series.
		topologyMismatchActive, topologySentinelDeficit, topologyReplicaDeficit =
			r.stateFor(client.ObjectKeyFromObject(v)).quorumTracker().topologyMismatchActiveOrExpire(r.now())
		operatormetrics.SetSentinelTopologyMismatch(v.Namespace, v.Name, topologySentinelDeficit, topologyReplicaDeficit)
	}
	// Dual-master observation: stamped by any of the four scans — the
	// sentinel Phase 7a self-heal scan (inside a failover section) and
	// Phase 11 recovery survey (outside one), and the replication
	// labeled-primary orphan scan and no-labeled-primary observation
	// scan — on passes that reach those phases only; the freshness gate
	// keeps an early-return pass from pinning the condition off a stale
	// stamp — same posture as masterLost above.
	// This is the single writer of the valkey_dual_master_observed
	// gauge: driving it from the same freshness-gated read the
	// conditions use keeps metric and condition consistent by
	// construction (the gauge can't latch at 1 after the observation
	// ages out). A stale stamp is expired here so the gauge returns to
	// 0 and the next episode re-fires its event. Standalone CRs can't
	// have dual masters, so no producer ever stamps them and no series
	// is emitted.
	dualMasterActive := false
	if v.Spec.Mode != valkeyv1beta1.ModeStandalone {
		now := r.now()
		ps := r.stateFor(client.ObjectKeyFromObject(v))
		dualMasterActive = ps.dualMasterActiveOrExpire(func(obs *dualMasterObservation) bool {
			return dualMasterActiveFromStamp(obs, now)
		})
		r.setDualMasterGauge(client.ObjectKeyFromObject(v), dualMasterActive)
	}
	conds, phase := orchestration.Evaluate(orchestration.Observation{
		CR:                            v,
		STS:                           sts,
		ReconcileError:                reconcileErr,
		Paused:                        paused,
		RolloutWatchdog:               watchdog,
		SentinelQuorum:                sqResult,
		SplitBrainActive:              splitBrainActive,
		NoMasterAgreementActive:       noMasterAgreementActive,
		MasterLostActive:              masterLostActive,
		DualMasterActive:              dualMasterActive,
		SentinelPeerLinkupStuckActive: linkupStuckActive,
		QuorumSuppressionActive:       suppressionActive,
		Replication:                   r.observeReplicationHealthy(ctx, v),

		SentinelTopologyMismatchActive:  topologyMismatchActive,
		SentinelTopologySentinelDeficit: topologySentinelDeficit,
		SentinelTopologyReplicaDeficit:  topologyReplicaDeficit,
	})

	patched := v.DeepCopy()
	for _, c := range conds {
		meta.SetStatusCondition(&patched.Status.Conditions, c)
	}
	patched.Status.Phase = phase
	if watchdog.Active && watchdog.Expired {
		// Disarm on the patched copy (post-DeepCopy) so the
		// MergeFrom diff captures the substate clear. Mutating
		// `v` here would zero both sides of the diff and the
		// patch would skip the field.
		if patched.Status.Rollout == nil {
			patched.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
		}
		patched.Status.Rollout.MasterAware = orchestration.Disarm()
	}
	// Mirror the aggregator's PrimaryPod into the patched status.
	// Empty string is fine (clears a stale value when no majority
	// is reached) — the field is +optional.
	patched.Status.PrimaryPod = sqResult.PrimaryPod

	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}

	if orchestration.DegradedFlippedFalse(prior, patched.Status.Conditions) {
		r.recordEventf(patched, corev1.EventTypeNormal, string(events.DegradedResolved), "DegradedResolve", "Degraded condition flipped True->False")
	}
	return nil
}

// masterAwareSubstate returns v.Status.Rollout.MasterAware or nil
// when either parent is unset. Inlining the nil-walk in callers
// would clutter the read site.
func masterAwareSubstate(v *valkeyv1beta1.Valkey) *valkeyv1beta1.MasterAwareRolloutStatus {
	if v == nil || v.Status.Rollout == nil {
		return nil
	}
	return v.Status.Rollout.MasterAware
}

// evaluateRolloutWatchdog runs the readiness-watchdog Check against
// the CR's current MasterAware substate and returns the verdict + a
// suggested RequeueAfter for an active-not-yet-expired watchdog.
// Emits the RolloutStalled event when expired so the caller doesn't
// have to know the event-emission shape; the substate disarm itself
// happens in updateStatus where the status patch is built.
//
// `paused` short-circuits the whole evaluation — a paused CR has had
// its reconcile loop intentionally stopped by the user, and a stale
// Arm shouldn't fire events under that hold.
func (r *ValkeyReconciler) evaluateRolloutWatchdog(v *valkeyv1beta1.Valkey, paused bool) (orchestration.Result, time.Duration) {
	if paused {
		return orchestration.Result{}, 0
	}
	wd := orchestration.Check(time.Now(), masterAwareSubstate(v))
	if !wd.Active {
		return wd, 0
	}
	if wd.Expired {
		r.recordEventf(v, corev1.EventTypeWarning, string(events.RolloutStalled), "RolloutStallObserve",
			"replica-readiness watchdog expired waiting for pod %q (deadline %s)",
			wd.PodName,
			wd.Deadline.UTC().Format("2006-01-02T15:04:05Z"))
		return wd, 0
	}
	// Active and still pending — schedule a wake at the deadline so
	// expiry detection doesn't depend on an external pod-watch event
	// firing at exactly the right moment.
	if until := time.Until(wd.Deadline); until > 0 {
		return wd, until
	}
	return wd, 0
}

// component is currently always componentValkey; the second sentinel
// component value lands when the sentinel STS path starts calling
// this with `componentSentinel`.
//
//nolint:unparam // component widens to "sentinel" once sentinel STS path lands
func ownedLabels(v *valkeyv1beta1.Valkey, component string) map[string]string {
	return map[string]string{
		ManagedByLabel:    ManagedByValue,
		AppNameLabel:      "valkey",
		AppInstanceLabel:  v.Name,
		AppComponentLabel: component,
		AppPartOfLabel:    "velkir",
		CRLabel:           v.Name,
		ComponentLabel:    component,
	}
}

func mergeMaps(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	keys := make([]string, 0, len(base)+len(overlay))
	for k := range base {
		keys = append(keys, k)
	}
	for k := range overlay {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if v, ok := overlay[k]; ok {
			out[k] = v
		} else {
			out[k] = base[k]
		}
	}
	return out
}

// SetupWithManager wires the reconciler with predicate filters that drop
// status-only updates so the workqueue isn't paged on every status write.
func (r *ValkeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("valkey-controller")
	}
	// Fallback for tests + alternate wiring paths that construct the
	// reconciler without going through cmd/main.go. Production wiring
	// in main.go injects a SHARED reporter into both the reconciler
	// and the sentinel startup safety net so cross-component dedup
	// stays consistent on a cold leader-acquire; this fallback only
	// fires when that injection didn't happen.
	if r.ShortAuthPasswordReporter == nil {
		r.ShortAuthPasswordReporter = events.NewShortAuthPasswordReporter(r.Recorder)
	}
	if r.Deprecator == nil {
		r.Deprecator = events.NewDeprecator(r.Recorder)
	}
	if r.DeviationEmitter == nil {
		r.DeviationEmitter = events.NewDeviationEmitter(r.Recorder)
	}
	b := ctrl.NewControllerManagedBy(mgr).
		// Operational annotations (pause/unpause, single-shot opt-ins,
		// manual rollout) change observable behaviour without bumping
		// .metadata.generation, so GenerationChangedPredicate alone
		// would leave them waiting for the multi-minute baseline
		// watchdog. OR in a predicate that wakes the reconciler on
		// those annotation edits. (Auth-Secret rotation responsiveness is
		// handled by a dedicated metadata-only Secret watch — see
		// authSecretWatchSource — since the user-owned auth Secret sits
		// outside this label-filtered informer cache.)
		For(&valkeyv1beta1.Valkey{}, builder.WithPredicates(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				operationalAnnotationChangePredicate(),
			),
		)).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		// Watch owned pods so PodIP assignment / containerStatuses /
		// readiness-condition flips reach the reconciler directly. The
		// StatefulSet-only watch misses these: STS.status fields
		// (readyReplicas, …) don't bump when a pod goes Pending →
		// Running unless it becomes Ready — which a gated pod can't
		// until the operator patches its gate condition. That deadlock
		// (pod never Ready → STS never updates → operator never
		// reconciles → gate never patched) was the canonical
		// replication-bootstrap-timeout failure. The predicate filters
		// the spec/label/annotation churn controller-runtime delivers
		// verbatim down to the status transitions Phase 8 acts on;
		// GenerationChangedPredicate is wrong here (a pod's generation
		// rarely bumps).
		Owns(&corev1.Pod{}, builder.WithPredicates(podStatusChangePredicate())).
		Named("valkey").
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles})
	// Push half of hybrid push+pull observation: the sentinel observer
	// enqueues the owning CR on +switch-master / +failover-end /
	// +odown / -odown so the operator reacts within seconds instead of
	// waiting for the 10s observer poll or the multi-minute baseline
	// watchdog. Nil-guarded for the test / alternate-wiring paths that
	// build the reconciler without a sentinel manager.
	if r.SentinelObserver != nil {
		b = b.WatchesRawSource(source.Channel(
			r.SentinelObserver.Events(),
			handler.EnqueueRequestsFromMapFunc(r.mapObserverEventToCR),
		))
	}
	// Auth-Secret rotation: a dedicated metadata-only Secret informer
	// (outside the manager's label-filtered cache, which excludes the
	// user-owned auth Secret) enqueues the owning CR when its auth Secret
	// changes, so a rotation reconciles immediately instead of waiting for
	// the baseline watchdog.
	authSrc, err := r.authSecretWatchSource(mgr)
	if err != nil {
		return err
	}
	b = b.WatchesRawSource(authSrc)
	return b.Complete(r)
}

// mapObserverEventToCR maps a sentinel-observer push GenericEvent to a
// reconcile request for the CR the observer watches. The observer
// stamps the event object's namespace/name, so we enqueue exactly that
// CR. Returns nil for an object missing identity (defensive — the
// observer always sets both).
func (r *ValkeyReconciler) mapObserverEventToCR(_ context.Context, obj client.Object) []reconcile.Request {
	if obj == nil || obj.GetName() == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}}}
}

// podStatusChangePredicate filters owned-pod events down to the status
// transitions the reconciler can act on: creation, deletion, and updates
// whose status changed (PodIP, phase, or conditions). Spec-only,
// label-only, and annotation-only updates — which controller-runtime
// delivers for every kubelet-side patch — are dropped so they don't
// churn the workqueue.
func podStatusChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return true },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, ok := e.ObjectOld.(*corev1.Pod)
			if !ok {
				return true
			}
			newPod, ok := e.ObjectNew.(*corev1.Pod)
			if !ok {
				return true
			}
			if oldPod.Status.PodIP != newPod.Status.PodIP {
				return true
			}
			if oldPod.Status.Phase != newPod.Status.Phase {
				return true
			}
			return !equalPodConditions(oldPod.Status.Conditions, newPod.Status.Conditions)
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// operationalAnnotations are the CR annotations whose edits must wake
// the reconciler promptly. They change observable behaviour (pause,
// single-shot recovery opt-ins, manual rollout) without bumping
// .metadata.generation, so a GenerationChangedPredicate filters them
// out and the change would otherwise wait for the baseline watchdog.
// ConfigHashAnnotation is deliberately excluded — it lives on the STS
// pod template, not the CR, and is operator-written.
var operationalAnnotations = []string{
	PauseAnnotation,
	AcceptPVCLossAnnotation,
	ForceRotateAnnotation,
	ManualRolloutAnnotation,
}

// operationalAnnotationsChanged reports whether any operational
// annotation differs between two annotation maps.
func operationalAnnotationsChanged(oldAnn, newAnn map[string]string) bool {
	for _, k := range operationalAnnotations {
		if oldAnn[k] != newAnn[k] {
			return true
		}
	}
	return false
}

// operationalAnnotationChangePredicate fires on updates that change an
// operational annotation. Composed via predicate.Or with
// GenerationChangedPredicate on the primary Valkey watch so that
// annotation-only edits (which leave .metadata.generation untouched)
// still trigger a reconcile. Create/Delete/Generic intentionally fall
// through to the OR-composed GenerationChangedPredicate.
func operationalAnnotationChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return operationalAnnotationsChanged(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations())
		},
	}
}

// equalPodConditions compares two pod-condition slices by (type, status)
// tuples — message and timestamps don't affect what the reconciler can
// act on. Returns true iff both carry the same set of type→status
// entries.
func equalPodConditions(a, b []corev1.PodCondition) bool {
	if len(a) != len(b) {
		return false
	}
	idx := make(map[corev1.PodConditionType]corev1.ConditionStatus, len(a))
	for i := range a {
		idx[a[i].Type] = a[i].Status
	}
	for i := range b {
		if idx[b[i].Type] != b[i].Status {
			return false
		}
	}
	return true
}

// classifyReconcileError maps a Reconcile-return error to a coarse
// label value for the `valkey_reconciliations_failed_total{reason=…}`
// counter. The label set is bounded — every classifier must return
// one of a small fixed enum so the metric's cardinality stays
// predictable. Each phase that returns to controller-runtime wraps
// its error with a known prefix (`phase 1: …`, `phase 4: …`); the
// classifier matches on those prefixes plus a few well-known
// sentinel paths (auth secret get failure, finalizer add failure).
// Unrecognised errors collapse to ReconcileError so the counter
// still ticks — the rate-of-failures alert needs a non-zero series
// to evaluate against.
//
// Refining the taxonomy (per-error subclasses, structured-error
// errors.As) is out of scope here; the alert only needs a usable
// signal that any reconcile failed. A follow-up issue can promote
// classification to use typed errors once the reconciler grows
// dedicated error types.
func classifyReconcileError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "phase 1:"):
		return "ConfigMapPhaseError"
	case strings.HasPrefix(msg, "phase 2:"):
		return "STSPhaseError"
	case strings.HasPrefix(msg, "phase 3:"):
		return "SentinelInfraPhaseError"
	case strings.HasPrefix(msg, "phase 4:"):
		return "PVCResizePhaseError"
	case strings.HasPrefix(msg, "phase 5:"):
		return "ServicePhaseError"
	case strings.HasPrefix(msg, "phase 6:"):
		return "PDBPhaseError"
	case strings.HasPrefix(msg, "phase 7:"):
		return "RoleLabelPhaseError"
	case strings.HasPrefix(msg, "phase 8:"):
		return "ReadinessGatePhaseError"
	case strings.HasPrefix(msg, "phase 9:"):
		return "PodRolloutPhaseError"
	case strings.Contains(msg, "auth secret"):
		return "AuthSecretError"
	case strings.Contains(msg, "finalizer"):
		return "FinalizerError"
	case strings.Contains(msg, "PVC resize aborted"):
		return "PVCResizeAborted"
	}
	return "ReconcileError"
}

// mergeRequeue merges a phase / substate / edge-detection cadence
// hint into the running result.RequeueAfter:
//
//   - hint <= 0 means "no hint" — current is preserved.
//   - hint > 0 but below minRequeueFloor is raised to the floor
//     (the foot-gun guard against an aggressive sub-floor hint
//     thrashing controller-runtime's reconcile loop).
//   - the (floored) hint wins when it's tighter than current OR
//     when current is unset; otherwise current is preserved.
//
// Centralising the merge here pins the "tighter wins, with floor"
// invariant across every call site (Phase 0d watchdog, rollout-
// trigger edge, Phase 4 PVC resize substate). Without the floor,
// a hint of e.g. 5ms would rearm controller-runtime to re-Reconcile
// at ~200/sec per CR — apiserver thrash under any churn that
// re-suggests the small interval.
func mergeRequeue(current, hint time.Duration) time.Duration {
	if hint <= 0 {
		return current
	}
	if hint < minRequeueFloor {
		hint = minRequeueFloor
	}
	if current == 0 || hint < current {
		return hint
	}
	return current
}

// now is the reconciler's wall-clock source — time.Now in production,
// a frozen clock under test (nowFunc).
func (r *ValkeyReconciler) now() time.Time {
	if r.nowFunc != nil {
		return r.nowFunc()
	}
	return time.Now()
}

// masterInfoProbeFreshnessWindow bounds how stale the master-info probe
// observation may be and still drive the MasterLost Ready signal. The
// probe runs only on reconcile passes that reach Phase 11; passes that
// early-return before it (PVC gate, list/apply errors, the sentinel
// orchestration guards) leave the latch untouched. Reading a stale
// latch from the every-pass status defer would pin Ready=False after
// the master recovered, so the read is gated on a recent observation.
// Set to 2× the keep-alive requeue: a healthy sentinel CR reconciles
// (and re-probes) at least every sqKeepAliveInterval, so a window of
// 2× always covers a live cluster while still clearing a latch whose
// probe has stopped running. Distinct from
// masterInfoSamePassReuseWindow, which bounds the SAME-pass reuse of
// this timestamp inside observedMasterIPForCR — the two thresholds
// deliberately differ.
const masterInfoProbeFreshnessWindow = 2 * sqKeepAliveInterval

// masterLostFromTimeout reports whether the contiguous INFO-probe
// failure recorded in `since` has lasted at least the down-after
// window (downAfterMillis ms) AND was observed by a probe that ran
// recently (observedAt within masterInfoProbeFreshnessWindow of now).
// The down-after gate is the hysteresis — a single slow probe can't
// flap Ready and the operator never declares the master lost before
// Sentinel's own death-detection window. The freshness gate prevents a
// stale latch (no probe this pass — an early-return reconcile) from
// pinning Ready=False after the master recovered: no recent
// measurement → not lost.
func masterLostFromTimeout(since *time.Time, observedAt time.Time, downAfterMillis int32, now time.Time) bool {
	if since == nil || downAfterMillis <= 0 {
		return false
	}
	if observedAt.IsZero() || now.Sub(observedAt) > masterInfoProbeFreshnessWindow {
		return false
	}
	return now.Sub(*since) >= time.Duration(downAfterMillis)*time.Millisecond
}

// sentinelKeepAliveRequeue returns the requeue hint that guarantees a
// reconcile within the SentinelQuorum freshness window for every
// sentinel-mode CR. It serves two consumers, both of which need a
// bounded re-probe cadence regardless of SQ-record presence:
//   - reconcileSentinelQuorumStatus re-stamps LastObservedTime so a
//     quiet-but-live cluster's PrimaryConfirmed doesn't latch Unknown;
//   - the master-info probe re-runs so the MasterLost freshness gate
//     (masterInfoProbeFreshnessWindow) keeps clearing/confirming the
//     Ready signal even on a CR with a dead primary and zero SQ records
//     (all sentinels unreachable) and a Ready STS — the path where a
//     records-gated keep-alive would have left only the 5-minute
//     baseline, aging the probe out of the freshness window.
//
// Gating on sentinel mode alone (not SQ-record count) is the bounded,
// sentinel-scoped hint; a sentinel CR before its first SQ write simply
// reconciles at the keep-alive cadence, which the tighter bootstrap
// hints dominate anyway. The caller (statusRequeueHint) suppresses this
// on any pass that short-circuited before Phase 11 (reachedSteadyState
// false), where neither consumer runs.
func sentinelKeepAliveRequeue(isSentinel bool) time.Duration {
	if isSentinel {
		return sqKeepAliveInterval
	}
	return 0
}

// statusRequeueHint is the realized steady-state requeue contribution
// from the status defer: the ready-converge active poll plus the
// sentinel keep-alive, merged (tighter wins). The ready-converge poll
// is gated on !paused; the keep-alive is gated on reachedSteadyState
// (Phase 11 ran this pass). Gating the keep-alive on reachedSteadyState
// — not merely sentinel mode + !paused — is what stops it tightening a
// deliberately relaxed requeue on a short-circuit pass (auth-missing
// backoff, PVC-loss gate, paused) where its consumers (the SQ re-stamp
// + master-info probe) never ran: that would be do-nothing churn.
// Extracted so the helper is unit-testable; the merge-into-Result
// mutation is killed by TestApplyStatusRequeue. The through-Reconcile
// envtest only smoke-tests the wired path (requeue positive and <=
// baselineReconcileWatchdog) — it cannot isolate the keep-alive value,
// since a bootstrap body requeue dominates the keep-alive in envtest (no
// converged sentinel cluster without a kubelet).
func (r *ValkeyReconciler) statusRequeueHint(v *valkeyv1beta1.Valkey, observedSTS *appsv1.StatefulSet, paused, reachedSteadyState bool) time.Duration {
	var hint time.Duration
	// Active-poll cadence while the STS hasn't reached desired
	// ReadyReplicas — covers the post-orphan-delete + STS-recreate path
	// where an STS-watch event may lag under load. Skipped when paused.
	if !paused && observedSTS != nil && observedSTS.Spec.Replicas != nil {
		desired := *observedSTS.Spec.Replicas
		if desired > 0 && observedSTS.Status.ReadyReplicas < desired {
			hint = mergeRequeue(hint, readyConvergeRequeue)
		}
	}
	// Keep-alive only when the reconcile actually reached Phase 11 this
	// pass (its consumers — the SQ re-stamp + master-info probe — live
	// there). On a short-circuit pass (auth-missing backoff, PVC-loss
	// gate, paused) the consumers never run, so contributing the
	// keep-alive would only tighten a deliberately relaxed requeue into
	// do-nothing churn.
	if reachedSteadyState {
		hint = mergeRequeue(hint, sentinelKeepAliveRequeue(v.Spec.Mode == valkeyv1beta1.ModeSentinel))
	}
	return hint
}

// applyStatusRequeue merges the statusRequeueHint into the reconcile
// Result — the load-bearing wiring that puts a quiet sentinel CR's
// requeue inside the SQ freshness window. Kept as its own method (not
// inlined in the status defer) so the merge-into-Result is unit-testable:
// seeding result.RequeueAfter at the steady-state baseline and asserting
// the keep-alive tightens it fails if this merge is dropped — a check the
// through-Reconcile path cannot make in envtest, where the bootstrap body
// requeue always dominates the keep-alive (no converged sentinel cluster
// without a kubelet).
func (r *ValkeyReconciler) applyStatusRequeue(result *ctrl.Result, v *valkeyv1beta1.Valkey, observedSTS *appsv1.StatefulSet, paused, reachedSteadyState bool) {
	result.RequeueAfter = mergeRequeue(result.RequeueAfter, r.statusRequeueHint(v, observedSTS, paused, reachedSteadyState))
}

// Compile-time interface assertion.
var _ reconcile.Reconciler = (*ValkeyReconciler)(nil)
