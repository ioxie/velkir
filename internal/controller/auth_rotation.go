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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/audit"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/logging"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/valkey"
)

// authRotationOutcome is the terminal outcome of one rotation drive,
// stamped on the audit-log line as the `outcome` attr so compliance
// queries can filter "show me all failed rotations" without parsing
// the message string. Typed string so the compiler rejects raw-string
// args at the emitAuthRotationAudit boundary; downstream parsers
// MUST treat any other value as "unknown" and surface a hard error.
type authRotationOutcome string

const (
	authRotationOutcomeSucceeded authRotationOutcome = "succeeded"
	authRotationOutcomeFailed    authRotationOutcome = "failed"
	authRotationOutcomePartial   authRotationOutcome = "partial"
)

// hashPrefix returns the first 12 chars of a hex hash — enough to
// disambiguate within one CR's rotation history without echoing the
// full SHA-256 of the password content into the audit stream. Empty
// in → empty out (so the first-observation path emits "" for
// pre_hash without a special case at the call site).
func hashPrefix(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

// authRotationAuditEvent collects the per-emission attrs that
// emitAuthRotationAudit stamps on the audit-log line. Pulled into a
// struct so the helper signature stays at four args (ctx + cr +
// secret + this struct) and the per-attr semantics live with the
// struct field names rather than a positional arg list.
//
// The PodsOnNewCredential / PodsOnOldCredential fields name the
// compliance-relevant cluster state at terminal time:
//
//	outcome=succeeded → PodsOnNewCredential = N (every replica + master), PodsOnOldCredential = 0
//	outcome=failed    → PodsOnNewCredential = 0, PodsOnOldCredential = K (revert moved them all back)
//	outcome=partial   → PodsOnNewCredential = R (revert failed on these, stuck on new),
//	                    PodsOnOldCredential = K - R (revert succeeded, back on old)
//
// Compliance queries can filter `PodsOnNewCredential > 0 AND
// outcome != succeeded` to find the partial-state cluster the
// operator left behind. The earlier names `pods_updated` /
// `pods_reverted` would have invited misreading as
// "pods the operation modified" rather than "pods carrying each
// credential at terminal time".
type authRotationAuditEvent struct {
	Outcome             authRotationOutcome
	PreHash             string
	PostHash            string
	PodsOnNewCredential int
	PodsOnOldCredential int
}

// emitAuthRotationAudit is the single audit-emission site for the
// data-plane rotation orchestrator. Called from the three terminal
// branches in driveAuthRotation. The secretName is the user-provided
// auth Secret (v.Spec.Auth.SecretName resolved against the CR's
// namespace).
//
// Helper-vs-inline convention: this package wraps the audit.Log call
// in a helper because the same audit event Reason fires from THREE
// branches with shared attribute layout — DRY plus a single place to
// evolve the attribute schema. Single-shot audit emission sites
// (e.g. valkey_controller_pvcloss.go's pvc_loss_accepted) inline
// audit.Log directly. When introducing a fresh audit Reason, default
// to inline; promote to a helper once the second emission site lands.
func emitAuthRotationAudit(ctx context.Context, v *valkeyv1beta1.Valkey, secretName string, evt authRotationAuditEvent) {
	audit.Log(ctx, audit.Event{
		Name: audit.EventAuthSecretRotated,
		CR:   types.NamespacedName{Namespace: v.Namespace, Name: v.Name},
		Attrs: map[string]string{
			"secret":                 v.Namespace + "/" + secretName,
			"outcome":                string(evt.Outcome),
			"pre_hash":               hashPrefix(evt.PreHash),
			"post_hash":              hashPrefix(evt.PostHash),
			"pods_on_new_credential": fmt.Sprintf("%d", evt.PodsOnNewCredential),
			"pods_on_old_credential": fmt.Sprintf("%d", evt.PodsOnOldCredential),
		},
	})
}

// authPasswordCacheEntry is the in-memory record of the most recently
// observed auth password value for one CR. The reconciler caches the
// password whenever it observes a steady-state Secret (hash matches
// Status.Rollout.AuthRotation.ObservedSecretHash) so that on the next
// content-change reconcile it can pass the OLD password to RotateAuth's
// AUTH-with-old + SET-new step. The hash is mirrored from the Status
// field so a stale cache (operator restart with a Status that survived)
// is detected by hash mismatch and treated as a missed-window rather
// than a successful seed.
type authPasswordCacheEntry struct {
	password string
	hash     string
}

// rotateAuthFunc is the function signature of valkey.RotateAuth. The
// reconciler holds a settable field of this type so tests can inject a
// stub returning deterministic PodResult slices without spinning up
// real fake valkey TCP servers per test case. Production callers leave
// the field nil; the dispatcher in (r *ValkeyReconciler).rotateAuth
// falls back to the real package function.
type rotateAuthFunc func(ctx context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, oldPassword, newPassword string) []valkey.PodResult

// rotateAuth dispatches to RotateAuthFn when set (test injection),
// otherwise to the real valkey.RotateAuth.
func (r *ValkeyReconciler) rotateAuth(ctx context.Context, replicas []valkey.Endpoint, master valkey.Endpoint, oldPassword, newPassword string) []valkey.PodResult {
	if r.RotateAuthFn != nil {
		return r.RotateAuthFn(ctx, replicas, master, oldPassword, newPassword)
	}
	return valkey.RotateAuth(ctx, replicas, master, oldPassword, newPassword)
}

// hashAuthSecret returns the hex-encoded SHA-256 of the password key
// bytes inside the auth Secret. Returns "" when secret is nil, has
// no `password` data key, OR has a `password` key whose bytes are
// empty — all three shapes are equivalent to "no auth" from the
// operator's perspective and should not drive a rotation. Hashing
// the password content (not the Secret bytes / resourceVersion)
// avoids false triggers on metadata-only edits.
func hashAuthSecret(secret *corev1.Secret) string {
	if secret == nil {
		return ""
	}
	pwd, ok := secret.Data["password"]
	if !ok || len(pwd) == 0 {
		return ""
	}
	sum := sha256.Sum256(pwd)
	return hex.EncodeToString(sum[:])
}

// currentAuthRotationPhase returns the current AuthRotation substate
// phase. Empty Status / nil Rollout / nil AuthRotation all map to
// Idle for ergonomic equality checks.
func currentAuthRotationPhase(v *valkeyv1beta1.Valkey) valkeyv1beta1.AuthRotationPhase {
	if v.Status.Rollout == nil || v.Status.Rollout.AuthRotation == nil {
		return valkeyv1beta1.AuthRotationPhaseIdle
	}
	p := valkeyv1beta1.AuthRotationPhase(v.Status.Rollout.AuthRotation.Phase)
	if p == "" {
		return valkeyv1beta1.AuthRotationPhaseIdle
	}
	return p
}

// observedAuthSecretHash returns the persisted hash, or "" when none
// has been recorded yet.
func observedAuthSecretHash(v *valkeyv1beta1.Valkey) string {
	if v.Status.Rollout == nil || v.Status.Rollout.AuthRotation == nil {
		return ""
	}
	return v.Status.Rollout.AuthRotation.ObservedSecretHash
}

// classifyRotateResults splits a RotateAuth result slice into failed
// (Err != nil) and succeeded entries. Order within each output slice
// is preserved relative to the input.
//
// ErrRewriteFailed is treated as a success: the in-memory CONFIG SET
// succeeded (auth requirement on the pod is updated for any live
// connection), so the rotation is observable end-to-end via valkey-cli.
// The on-disk REWRITE failure is harmless when the data-plane container
// sources `--requirepass`/`--masterauth` from a Secret-mounted env var:
// kubelet re-resolves the Secret on every container start, so any pod
// restart (CrashLoopBackOff recovery, kubelet reboot, image bump) picks
// up the new credential without depending on the rewritten config file.
// Without this carve-out the partial-failure path classifies every pod
// as failed (REWRITE always fails when `/config` is mounted read-only),
// reverts a no-op succeeded set, and writes Status=Failed despite the
// cluster being on the new credential. With the carve-out the same
// run records Status=Succeeded — matching reality.
func classifyRotateResults(results []valkey.PodResult) (failed, succeeded []valkey.PodResult) {
	for _, r := range results {
		if r.Err != nil && !errors.Is(r.Err, valkey.ErrRewriteFailed) {
			failed = append(failed, r)
		} else {
			succeeded = append(succeeded, r)
		}
	}
	return failed, succeeded
}

// splitDataPlaneEndpoints partitions data-plane pods into a
// (replicas, master, ok) triple. Replica order is name-sorted so
// RotateAuth's per-pod result slice is deterministic and easy to
// assert against.
//
// ok=false in three cases — all refuse rotation:
//   - No pod is labelled role=primary. The cluster is mid-failover
//     or pre-bootstrap; we cannot rotate without knowing which pod
//     to send the master leg to.
//   - More than one pod is labelled role=primary (transient split-
//     brain during failover). Pod-list iteration order is not stable,
//     so silently picking one risks rotating against the wrong
//     primary, or applying the new password to a pod that's about to
//     be demoted by the sentinel quorum. The M3.x split-brain detector
//     already surfaces this state on the Degraded condition; the
//     rotation driver refuses to act inside that window and waits for
//     the cluster to converge on a single primary.
//   - Any input pod is missing a PodIP OR is unlabelled (role neither
//     primary nor replica). On a fresh sentinel cluster the secret-
//     edit reconcile can race the async role-labeler; silently
//     skipping the unobservable pods and then flipping Status to
//     Succeeded would leave them on the old password indefinitely.
//     Refusing rotation defers the driver until every data-plane pod
//     is observable, at which point the next reconcile re-detects the
//     change and walks the full set.
func splitDataPlaneEndpoints(pods []corev1.Pod) (replicas []valkey.Endpoint, master valkey.Endpoint, ok bool) {
	primaryCount := 0
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP == "" {
			return nil, valkey.Endpoint{}, false
		}
		ep := valkey.Endpoint{
			Name: p.Name,
			Addr: fmt.Sprintf("%s:%d", p.Status.PodIP, valkey.DefaultPort),
		}
		switch p.Labels[RoleLabel] {
		case roleValuePrimary:
			primaryCount++
			master = ep
		case roleValueReplica:
			replicas = append(replicas, ep)
		default:
			return nil, valkey.Endpoint{}, false
		}
	}
	if primaryCount != 1 {
		return nil, valkey.Endpoint{}, false
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].Name < replicas[j].Name })
	return replicas, master, true
}

// splitRevertEndpoints rebuilds (replicas, master) from a flat slice
// of PodResults to revert. The Phase tag set by RotateAuth identifies
// which leg each result belongs to; we mirror that split when calling
// RotateAuth(new, old) on the successfully-updated subset. Implicit
// contract: every PodResult must carry RotationPhaseReplica or
// RotationPhaseMaster; results with an unset Phase are silently
// dropped. RotateAuth's contract guarantees the tag is always set on
// every emitted PodResult, so the dropped-on-empty branch only fires
// when this helper is called with a hand-constructed slice in tests.
func splitRevertEndpoints(succeeded []valkey.PodResult) (replicas []valkey.Endpoint, master valkey.Endpoint) {
	for _, r := range succeeded {
		switch r.Phase {
		case valkey.RotationPhaseMaster:
			master = r.Endpoint
		case valkey.RotationPhaseReplica:
			replicas = append(replicas, r.Endpoint)
		}
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].Name < replicas[j].Name })
	return replicas, master
}

// endpointNames returns a sorted comma-joined list of pod names from
// a PodResult slice. Used for human-readable event messages and
// Status.Message text.
func endpointNames(results []valkey.PodResult) string {
	if len(results) == 0 {
		return ""
	}
	names := make([]string, 0, len(results))
	for _, r := range results {
		names = append(names, r.Endpoint.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// cacheAuthPassword stamps the password+hash pair into the per-CR
// in-memory cache. Called whenever the operator observes a steady-
// state Secret (initial observation, hash-match short-circuit, or
// successful rotation).
func (r *ValkeyReconciler) cacheAuthPassword(key client.ObjectKey, password, hash string) {
	r.stateFor(key).storeAuthPassword(&authPasswordCacheEntry{
		password: password,
		hash:     hash,
	})
}

// lookupAuthPasswordCache returns the cached entry for the CR or
// (nil, false) when the cache is cold.
func (r *ValkeyReconciler) lookupAuthPasswordCache(key client.ObjectKey) (*authPasswordCacheEntry, bool) {
	return r.stateFor(key).loadAuthPassword()
}

// maybeRotateAuth is the auth-Secret hot-rotation driver. Called from
// Reconcile after Phase 0d's Secret fetch. The driver:
//
//   - Returns early when the CR has no auth (secret nil) and clears
//     any stale Status.Rollout.AuthRotation.
//   - Stamps an initial ObservedSecretHash on first observation; no
//     rotation drives until the user changes the Secret content.
//   - Settles a stale Succeeded → Idle transition.
//   - Leaves a Partial substate alone (operator-of-the-operator must
//     intervene).
//   - Edge-detects a content change by comparing the current password
//     hash against ObservedSecretHash. On change, defers if
//     FailoverInFlight; otherwise calls driveAuthRotation.
//
// Caller logs and ignores any non-nil error — the next reconcile retries.
func (r *ValkeyReconciler) maybeRotateAuth(ctx context.Context, v *valkeyv1beta1.Valkey, secret *corev1.Secret) error {
	log := logf.FromContext(ctx)
	key := client.ObjectKeyFromObject(v)

	currentHash := hashAuthSecret(secret)
	if currentHash == "" {
		// CR has no auth (or Secret has no `password` key). Clear the
		// substate if it exists; nothing else to do.
		r.stateFor(key).clearAuthPassword()
		return r.clearAuthRotationStatus(ctx, v)
	}

	// Register the password value with the redaction registry for the
	// duration of this driver invocation. Defensive: no log lines in
	// this function carry the password value today, but a future log
	// added here without thinking about redaction would leak — early
	// registration closes that window. The Forget cleanup runs on
	// every return path via defer.
	pwdBytes := secret.Data["password"]
	if len(pwdBytes) > 0 {
		defer logging.DefaultRegistry.RegisterScoped(string(pwdBytes))()
	}

	cur := currentAuthRotationPhase(v)
	observed := observedAuthSecretHash(v)

	// Partial state is sticky; only human intervention clears it (the
	// admin removes Status.Rollout.AuthRotation, or fixes the cluster
	// and waits for the next reconcile to re-observe).
	if cur == valkeyv1beta1.AuthRotationPhasePartial {
		return nil
	}

	// First-ever observation: stamp the hash, then advance the cache.
	// On Conflict (persisted=false), skip the cache advance — the
	// next reconcile will retry from a fresh fetch and re-stamp.
	if observed == "" {
		persisted, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseIdle, currentHash, "initial observation")
		if err != nil {
			return err
		}
		if persisted {
			r.cacheAuthPassword(key, string(secret.Data["password"]), currentHash)
		}
		return nil
	}

	// Settle Succeeded → Idle on the next reconcile after a successful
	// rotation. The cache is already up to date with the new password.
	if cur == valkeyv1beta1.AuthRotationPhaseSucceeded {
		_, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseIdle, observed, "")
		return err
	}

	// Hash matches: steady state. Refresh the in-memory cache (no-op
	// when already populated, but seeds it on operator restart with a
	// pre-existing Status). Steady-state cache refresh has no Status
	// dependency to gate against — the Status is already in agreement
	// with the cluster.
	if currentHash == observed {
		r.cacheAuthPassword(key, string(secret.Data["password"]), currentHash)
		return nil
	}

	// Hash differs → user-driven content change. Defer if a failover
	// is in flight; the next reconcile after the FailoverInFlight-exit
	// edge will retry (the deferral predicate is symmetric with the
	// one gating SentinelObserver.RecoverStrandedSentinels).
	if r.IsFailoverInFlight(key) {
		log.V(1).Info("auth rotation deferred: failover in flight", "cr", key.String())
		return nil
	}

	// Cache miss (operator restart) or stale-hash entry: we do not
	// know the OLD password and cannot drive AUTH-with-old + SET-new
	// safely. Stamp the new hash so we treat the current Secret as the
	// authoritative cluster credential going forward; any legitimate
	// rotation that was in flight at restart will surface as auth
	// failures from clients (we cannot recover it from this side
	// without the old password). Cache advance is gated on Status
	// persistence so a Patch Conflict doesn't desync.
	cached, ok := r.lookupAuthPasswordCache(key)
	if !ok || cached.hash != observed {
		log.Info("auth rotation: missed rotation window (no cached old password); recording new hash without rotation",
			"cr", key.String())
		persisted, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseIdle, currentHash,
			"rotation window missed; current Secret content adopted as observed credential")
		if err != nil {
			return err
		}
		if persisted {
			r.cacheAuthPassword(key, string(secret.Data["password"]), currentHash)
		}
		return nil
	}

	return r.driveAuthRotation(ctx, v, cached.password, string(secret.Data["password"]), currentHash, observed)
}

// driveAuthRotation lists the data-plane pods, derives endpoints,
// stamps InProgress, calls RotateAuth, classifies the result, and
// either emits SecretRotated (all-success) or runs the partial-failure
// revert path (Failed / Partial). currentHash is the post-change hash
// (the user's NEW Secret content); observedHash is the pre-change
// hash (the one already on the cluster).
func (r *ValkeyReconciler) driveAuthRotation(ctx context.Context, v *valkeyv1beta1.Valkey, oldPwd, newPwd, currentHash, observedHash string) error {
	log := logf.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		}); err != nil {
		return fmt.Errorf("listing valkey pods for rotation: %w", err)
	}
	replicas, master, ok := splitDataPlaneEndpoints(pods.Items)
	if !ok {
		// Either no pod is labelled role=primary (mid-bootstrap or
		// post-failover settle window), or more than one pod is
		// labelled role=primary (transient split-brain). In both
		// cases we cannot rotate safely; defer until the cluster
		// converges on exactly one primary. ObservedSecretHash is
		// NOT advanced so the next reconcile re-detects the change.
		log.V(1).Info("auth rotation deferred: no single role=primary pod observed", "cr", v.Namespace+"/"+v.Name)
		return nil
	}

	// Register the OLD password with the redaction registry for the
	// duration of this drive (the NEW password is already registered
	// by the maybeRotateAuth caller). Ensures any logged AUTH-error
	// string from rotateOne carrying the old credential is scrubbed
	// in the operator logs even though the per-pod retry already
	// swallows the bytes most of the time.
	defer logging.DefaultRegistry.RegisterScoped(oldPwd)()

	// Stamp InProgress before the synchronous RotateAuth call. The
	// stamp is best-effort — a Patch error or Conflict here logs and
	// continues so we don't double-emit on the next reconcile. The
	// terminal-state patch below is the load-bearing one.
	if _, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseInProgress, observedHash,
		fmt.Sprintf("rotating %d replica(s) + 1 master", len(replicas))); err != nil {
		log.V(1).Info("auth rotation: status InProgress patch failed (non-fatal)", "err", err.Error())
	}

	results := r.rotateAuth(ctx, replicas, master, oldPwd, newPwd)
	failed, succeeded := classifyRotateResults(results)

	if len(failed) == 0 {
		// All-success path. Status patch first, then cache update, then
		// event emission. Order matters: if the Status patch fails to
		// persist (apiserver error OR Conflict), the cache is NOT
		// advanced, so the next reconcile re-detects the change
		// against the still-old observedHash and re-attempts the
		// rotation (idempotent: pods already on newPwd will error on
		// AUTH-with-old, the per-pod retry will surface them as
		// failures, the revert path will run, and the cluster lands
		// on a consistent old credential — net result is a
		// Failed/Partial transition rather than a silent split
		// between cache and Status). The event fires only after
		// Status is durable so dashboards reading events alongside
		// Status see consistent signal.
		key := client.ObjectKeyFromObject(v)
		persisted, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseSucceeded, currentHash,
			fmt.Sprintf("rotation succeeded across %d replica(s) + 1 master", len(replicas)))
		if err != nil {
			return err
		}
		if !persisted {
			// Conflict: another writer touched the CR between our
			// fetch and the patch. Don't advance dependent in-memory
			// state — the next reconcile re-evaluates from a fresh
			// fetch and the rotation is idempotent.
			return nil
		}
		r.cacheAuthPassword(key, newPwd, currentHash)
		r.recordEventf(v, corev1.EventTypeNormal, string(events.SecretRotated), "SecretRotation",
			"auth password rotated across %d replica(s) + 1 master", len(replicas))
		operatormetrics.SecretRotationTotal.WithLabelValues(v.Namespace, v.Name, "success").Inc()
		// Audit-log emission gated on durable Status persistence so the
		// audit stream and the cluster Status agree. PodsOnNewCredential
		// counts every data-plane endpoint that received the new
		// credential (replicas + master); PodsOnOldCredential is zero on
		// the all-success path. v.Spec.Auth is non-nil here
		// (maybeRotateAuth's hash short-circuit ruled out the no-auth
		// case before reaching the driver).
		emitAuthRotationAudit(ctx, v, v.Spec.Auth.SecretName, authRotationAuditEvent{
			Outcome:             authRotationOutcomeSucceeded,
			PreHash:             observedHash,
			PostHash:            currentHash,
			PodsOnNewCredential: len(succeeded),
			PodsOnOldCredential: 0,
		})
		return nil
	}

	// Partial failure: revert the successfully-updated subset back to
	// the old password. The successfully-rotated set might be empty
	// (every pod failed), in which case the revert call is a no-op
	// (RotateAuth returns nil for empty replicas + zero-value master).
	revertReplicas, revertMaster := splitRevertEndpoints(succeeded)
	revertResults := r.rotateAuth(ctx, revertReplicas, revertMaster, newPwd, oldPwd)
	revertFailed, _ := classifyRotateResults(revertResults)

	failedNames := endpointNames(failed)
	if len(revertFailed) == 0 {
		// Failed: cluster back on old password. ObservedSecretHash
		// stays at observedHash so a fresh content edit (different
		// from currentHash) re-triggers; if the user re-applies the
		// SAME content, the hash matches observed and we stay Idle
		// (no infinite-retry loop on poison content). Status patch
		// first; the event fires only when Status is durable so a
		// dashboard reading events alongside Status sees consistent
		// signal — and on Conflict the next reconcile re-runs the
		// classification rather than leaving a phantom event behind.
		msg := fmt.Sprintf("rotation failed on %d endpoint(s) [%s]; cluster reverted to previous credential",
			len(failed), failedNames)
		persisted, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhaseFailed, observedHash, msg)
		if err != nil {
			return err
		}
		if persisted {
			r.recordEventf(v, corev1.EventTypeWarning,
				string(events.SecretRotationFailed), "SecretRotationFailed", "%s", msg)
			// result="reverted": rotation failed but the cluster rolled
			// back cleanly to the prior credential (audit outcome=failed).
			operatormetrics.SecretRotationTotal.WithLabelValues(v.Namespace, v.Name, "reverted").Inc()
			// PostHash mirrors the persisted ObservedSecretHash
			// (observedHash, the pre-rotation value) — the cluster is
			// back on the old credential, so the post-state hash IS the
			// old hash. PodsOnNewCredential = 0 because every
			// successfully-rotated pod was reverted; PodsOnOldCredential
			// = the count of those pods (revert all-success path).
			emitAuthRotationAudit(ctx, v, v.Spec.Auth.SecretName, authRotationAuditEvent{
				Outcome:             authRotationOutcomeFailed,
				PreHash:             observedHash,
				PostHash:            observedHash,
				PodsOnNewCredential: 0,
				PodsOnOldCredential: len(succeeded),
			})
		}
		return nil
	}

	// Partial: cluster in mixed-credential state. Sticky until human
	// intervention. Status patch first; event fires only when Status
	// is durable.
	revertFailedNames := endpointNames(revertFailed)
	msg := fmt.Sprintf(
		"rotation failed on %d endpoint(s) [%s] AND revert failed on %d endpoint(s) [%s]; cluster is in mixed-credential state — operator intervention required",
		len(failed), failedNames, len(revertFailed), revertFailedNames)
	persisted, err := r.writeAuthRotationStatus(ctx, v, valkeyv1beta1.AuthRotationPhasePartial, observedHash, msg)
	if err != nil {
		return err
	}
	if persisted {
		r.recordEventf(v, corev1.EventTypeWarning,
			string(events.SecretRotationPartial), "SecretRotationPartial", "%s", msg)
		// result="failed" coarsens the audit `partial` outcome: the
		// metric enum is {success,reverted,failed}, so a partial
		// (revert also failed → mixed-credential, the worst case)
		// counts as failed here, distinct from a clean reverted.
		operatormetrics.SecretRotationTotal.WithLabelValues(v.Namespace, v.Name, "failed").Inc()
		// Mixed-credential state: PodsOnNewCredential counts pods that
		// successfully rotated AND failed to revert (still on new);
		// PodsOnOldCredential counts pods that successfully rotated
		// AND successfully reverted (back on old). PostHash is
		// observedHash (the cluster's ObservedSecretHash stays at the
		// pre-rotation value because we don't know which half is "the
		// cluster credential" anymore).
		revertSucceeded := len(succeeded) - len(revertFailed)
		emitAuthRotationAudit(ctx, v, v.Spec.Auth.SecretName, authRotationAuditEvent{
			Outcome:             authRotationOutcomePartial,
			PreHash:             observedHash,
			PostHash:            observedHash,
			PodsOnNewCredential: len(revertFailed),
			PodsOnOldCredential: revertSucceeded,
		})
	}
	return nil
}

// writeAuthRotationStatus stamps the AuthRotation substate and patches
// the status subresource. The first transition into a non-Idle phase
// stamps StartedAt; every transition refreshes LastTransitionAt. A
// re-write of the same phase with the same hash + message is a no-op
// (avoids hot-loop status churn).
//
// Returns (persisted, err):
//   - persisted=true, err=nil → patch landed durably (or was a no-op
//     idempotent re-write of an already-stamped substate).
//   - persisted=false, err=nil → patch lost a race (Conflict). The
//     next reconcile re-evaluates from a fresh fetch; the caller MUST
//     NOT advance dependent in-memory state (e.g., the password
//     cache) because Status didn't durably advance.
//   - persisted=false, err non-nil → unexpected apiserver error;
//     caller should propagate.
func (r *ValkeyReconciler) writeAuthRotationStatus(ctx context.Context, v *valkeyv1beta1.Valkey, phase valkeyv1beta1.AuthRotationPhase, hash, message string) (bool, error) {
	prior := v.Status.Rollout
	if prior != nil && prior.AuthRotation != nil &&
		prior.AuthRotation.Phase == string(phase) &&
		prior.AuthRotation.ObservedSecretHash == hash &&
		prior.AuthRotation.Message == message {
		// Idempotent: the substate already matches the desired shape;
		// the prior reconcile already persisted it. Treat as
		// persisted=true so callers in the all-success path can safely
		// advance dependent state (the cache, etc.) — the durable
		// Status is what they need to anchor against.
		return true, nil
	}
	now := metav1.Now()
	patched := v.DeepCopy()
	if patched.Status.Rollout == nil {
		patched.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
	}
	priorAR := patched.Status.Rollout.AuthRotation
	next := &valkeyv1beta1.AuthRotationStatus{
		Phase:              string(phase),
		ObservedSecretHash: hash,
		Message:            message,
		LastTransitionAt:   &now,
	}
	switch phase {
	case valkeyv1beta1.AuthRotationPhaseInProgress:
		// New active rotation: stamp StartedAt fresh.
		next.StartedAt = &now
	default:
		// Carry StartedAt forward when transitioning out of an active
		// rotation (Succeeded / Failed / Partial) so dashboards can
		// compute total rotation duration. Idle clears StartedAt to
		// signal "no rotation in flight".
		if phase != valkeyv1beta1.AuthRotationPhaseIdle && priorAR != nil && priorAR.StartedAt != nil {
			next.StartedAt = priorAR.StartedAt
		}
	}
	patched.Status.Rollout.AuthRotation = next
	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			// Lost the race; next reconcile re-evaluates from a fresh
			// fetch. Caller is signalled via persisted=false so it
			// doesn't advance dependent state out from under the
			// non-persisted Status.
			return false, nil
		}
		return false, err
	}
	v.Status = patched.Status
	return true, nil
}

// clearAuthRotationStatus drops Status.Rollout.AuthRotation when the
// CR has no auth Secret. Idempotent; no-op when the substate is
// already absent.
func (r *ValkeyReconciler) clearAuthRotationStatus(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	if v.Status.Rollout == nil || v.Status.Rollout.AuthRotation == nil {
		return nil
	}
	patched := v.DeepCopy()
	patched.Status.Rollout.AuthRotation = nil
	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	v.Status = patched.Status
	return nil
}
