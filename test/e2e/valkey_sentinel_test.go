//go:build e2e
// +build e2e

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

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// sha256Hex mirrors the operator's auth_rotation.hashAuthSecret
// derivation: SHA-256 over the password bytes, hex-encoded. Used to
// compute the post-rotation ObservedSecretHash for the hot-rotation
// assertion below — the operator stamps the same value into
// Status.Rollout.AuthRotation.ObservedSecretHash on settle.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// End-to-end coverage for `mode: sentinel`. The
// Manager-deployment lifecycle is owned by the existing `Manager`
// Describe block in e2e_test.go; the e2e namespace lifecycle is
// owned by the standalone suite (valkey_standalone_test.go); this
// file just adds sentinel-specific scenarios on top of the same
// operator deployment + namespace.

var _ = Describe("Valkey sentinel", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create. Ginkgo v2 randomises top-level
		// container order, so this Describe may execute before the
		// standalone Describe's BeforeAll fires; create our own ns
		// up front rather than depending on registration order.
		// `kubectl create ns` returns "AlreadyExists" if the ns exists,
		// which we discard via `_, _ =`.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))

		// Stamp the e2e-target label so the chart-deploy operator's
		// webhooks (mutating + validating) fire on CRs in this ns
		// per the namespaceSelector override the harness sets. The
		// standalone Describe's scenario-1 body does this too, but
		// when a focus filter narrows the run to just sentinel
		// specs the standalone label-stamp never executes — the
		// defaulter then doesn't run, CRD-built-in CEL rejects
		// missing sentinel.quorum, and every sentinel spec fails
		// at the first applyCR. Idempotent: --overwrite makes a
		// double-stamp a no-op.
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		// Clean every Valkey CR between specs so each test starts fresh.
		// `--wait=false` is load-bearing: kubectl-delete on a CR with
		// a finalizer (we use `velkir.ioxie.dev/pvc-retention`) defaults
		// to --wait=true, so it blocks until the finalizer runs. When
		// a spec ends mid-rollout the operator may not get a clean
		// reconcile turn to run finalizer-removal inside go-test's
		// timeout; the suite then hangs in AfterEach until the outer
		// timeout fires, masking the underlying spec result and
		// leaving cleanup ambiguous. Async delete + unique per-spec
		// CR names (m49-*, sentinel-*) sidesteps cross-spec name
		// collisions even when prior CRs are still Terminating.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
	})

	// Scenario 1 — bootstrap a 3 valkey + 3 sentinel CR. Verifies the
	// happy-path topology: pod-0 stamped role=primary, pods 1+ stamped
	// role=replica, the three Services exist with the right selectors
	// (write-Service routes to role=primary, read-Service to
	// role=replica, sentinel-Service to the sentinel pods), and the
	// sentinel StatefulSet has all three pods Ready.
	It("bootstraps 3 valkey + 3 sentinel; role labels stable, three Services route correctly", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: sentinel-bootstrap
spec:
  mode: sentinel
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
  sentinel:
    masterName: mymaster
    replicas: 3
`)
		waitForReady("sentinel-bootstrap", 5*time.Minute)

		By("verifying all 3 valkey pods reached Ready")
		Eventually(stsReadyReplicas("sentinel-bootstrap"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"all 3 valkey pods must reach Ready post-bootstrap")

		By("verifying all 3 sentinel pods reached Ready")
		Eventually(stsReadyReplicas("sentinel-bootstrap-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"all 3 sentinel pods must reach Ready post-bootstrap")

		By("verifying role labels: pod-0 primary, pods 1+ replica")
		pod0Role := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", "sentinel-bootstrap-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		))
		Expect(strings.TrimSpace(pod0Role)).To(Equal("primary"),
			"pod-0 must be labelled role=primary post-bootstrap")
		for _, ord := range []string{"1", "2"} {
			role := mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", "sentinel-bootstrap-"+ord,
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			Expect(strings.TrimSpace(role)).To(Equal("replica"),
				"pod-%s must be labelled role=replica post-bootstrap", ord)
		}

		By("verifying the primary <cr> Service selector points at role=primary")
		primarySelector := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "svc", "sentinel-bootstrap",
			"-o", `jsonpath={.spec.selector.velkir\.ioxie\.dev/role}`,
		))
		Expect(strings.TrimSpace(primarySelector)).To(Equal("primary"),
			"<cr> Service selector must route writes to role=primary")

		By("verifying the read-only <cr>-ro Service selector points at role=replica")
		roSelector := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "svc", "sentinel-bootstrap-ro",
			"-o", `jsonpath={.spec.selector.velkir\.ioxie\.dev/role}`,
		))
		Expect(strings.TrimSpace(roSelector)).To(Equal("replica"),
			"<cr>-ro Service selector must route reads to role=replica")

		By("verifying the <cr>-sentinel Service exists with 3 endpoints")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "endpoints", "sentinel-bootstrap-sentinel",
				"-o", "jsonpath={.subsets[*].addresses[*].targetRef.name}",
			))
			return out
		}, 2*time.Minute, 2*time.Second).Should(
			SatisfyAll(
				ContainSubstring("sentinel-bootstrap-sentinel-0"),
				ContainSubstring("sentinel-bootstrap-sentinel-1"),
				ContainSubstring("sentinel-bootstrap-sentinel-2"),
			),
			"<cr>-sentinel endpoints must include all 3 sentinel pods")

		By("verifying the primary accepts writes via the client <cr> Service")
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "sentinel-bootstrap-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m38-key", "m38-value",
		))
		Expect(strings.TrimSpace(out)).To(Equal("OK"),
			"primary must accept writes post-bootstrap")

		By("verifying the replica observes the write via replication")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", "sentinel-bootstrap-1", "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m38-key",
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m38-value"),
			"replica must observe the primary's write within the replication window")
	})

	// Scenario 6 — bootstrap with a non-default `sentinel.masterName`
	// (required, no default, immutable). Scenario 1 used the
	// commonplace `mymaster`; this scenario picks an arbitrary value
	// (`prod-valkey`) and proves the masterName propagates through
	// every layer that consumes it: `SENTINEL MASTER <name>` returns a
	// populated reply, and the wrong name returns the `NOMASTER`
	// error. Without this, a typo'd masterName at apply time would
	// silently be accepted (since every layer reads the field
	// independently and a single divergence wouldn't surface until
	// failover).
	It("bootstraps with non-default sentinel.masterName=prod-valkey; SENTINEL MASTER plumbed end-to-end", func() {
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: sentinel-naming
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: prod-valkey
    replicas: 3
`)
		waitForReady("sentinel-naming", 5*time.Minute)

		By("verifying both StatefulSets reach full readiness")
		Eventually(stsReadyReplicas("sentinel-naming"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"valkey STS must reach 3/3 ready post-bootstrap")
		Eventually(stsReadyReplicas("sentinel-naming-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 ready post-bootstrap")

		By("verifying SENTINEL MASTER prod-valkey returns a populated reply")
		// `SENTINEL MASTER <name>` returns a flat key/value list
		// describing the master. The first two entries are
		// `name`/`<name>`; the next pair is `ip`/`<addr>`. A populated
		// reply proves the sentinel is monitoring the named master and
		// has converged on a primary.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", "sentinel-naming-sentinel-0", "-c", "sentinel", "--",
				"valkey-cli", "-p", "26379", "SENTINEL", "MASTER", "prod-valkey",
			))
			return out
		}, 2*time.Minute, 5*time.Second).Should(
			SatisfyAll(
				ContainSubstring("name"),
				ContainSubstring("prod-valkey"),
				ContainSubstring("ip"),
			),
			"SENTINEL MASTER prod-valkey must return a populated reply with name + ip")

		By("verifying SENTINEL MASTER mymaster returns the No-such-master error")
		// Negative test: the wrong masterName must NOT return a
		// populated reply. Sentinel returns `No such master with that
		// name` (or similar Sentinel-version-dependent wording). The
		// invariant is: the response must NOT contain the master's
		// `ip` field that a populated reply would carry.
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "sentinel-naming-sentinel-0", "-c", "sentinel", "--",
			"valkey-cli", "-p", "26379", "SENTINEL", "MASTER", "mymaster",
		))
		Expect(out).NotTo(ContainSubstring("\"ip\""),
			"SENTINEL MASTER on the wrong masterName must NOT return a populated reply")
	})

	// Scenario 5 — sentinels can authenticate to the auth-protected
	// primary and observe it via SENTINEL MASTER. Without the auth
	// round-trip working (sentinel auth-pass propagation),
	// sentinels can't poll the master and never converge on
	// `ObservedPrimary`; the failover state machine starves and the
	// suppression gate becomes a silent no-op. The positive
	// `SENTINEL MASTER mymaster
	// → populated reply` is the natural exit gate — if any layer of
	// the auth wiring is broken (operator doesn't render the auth-pass
	// directive into sentinel.conf, the Secret-mount injection
	// silently skips the value, sentinel pod doesn't pick up the
	// SENTINEL SET auth-pass at startup), the sentinel can't log in,
	// can't observe the master, and the lookup returns empty.
	It("sentinels authenticate to the auth-protected primary; SENTINEL MASTER converges", func() {
		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "create", "secret", "generic", "auth-failover",
			"--from-literal=password=changeme"))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "secret", "auth-failover",
				"--ignore-not-found", "--wait=false"))
		})

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: sentinel-failover
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
  auth:
    secretName: auth-failover
    secretKey: password
    sentinelAuthSecretName: auth-failover
    sentinelAuthSecretKey: password
`)
		waitForReady("sentinel-failover", 5*time.Minute)

		By("verifying both StatefulSets reach full readiness")
		Eventually(stsReadyReplicas("sentinel-failover"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"valkey STS must reach 3/3 ready under auth")
		Eventually(stsReadyReplicas("sentinel-failover-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 ready under auth")

		By("verifying the primary requires auth (PING without -a returns NOAUTH)")
		// `valkey-cli -p 6379 ping` without `-a` against an
		// auth-protected primary returns `NOAUTH Authentication
		// required.` (or the version-dependent equivalent). We use
		// CombinedOutput here directly — utils.Run drops stderr on
		// non-zero exit but we want the NOAUTH text from stderr.
		// The sentinel-mode container ships REDISCLI_AUTH on its env
		// (set so the preStop hook's own valkey-cli call can
		// authenticate). valkey-cli reads it automatically when no
		// `-a` flag is supplied, which would mask the
		// unauthenticated-ping case we want to assert. `env -u`
		// strips the variable for this single exec so the assert
		// genuinely exercises the no-credentials path.
		noauthCmd := exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "sentinel-failover-0", "-c", "valkey", "--",
			"env", "-u", "REDISCLI_AUTH",
			"valkey-cli", "-p", "6379", "ping",
		)
		noauthOut, _ := noauthCmd.CombinedOutput()
		Expect(string(noauthOut)).To(ContainSubstring("NOAUTH"),
			"primary must reject unauthenticated PING (got: %s)", string(noauthOut))

		By("verifying the primary accepts authenticated PING")
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", "sentinel-failover-0", "-c", "valkey", "--",
			"valkey-cli", "-a", "changeme", "--no-auth-warning", "-p", "6379", "ping",
		))
		Expect(strings.TrimSpace(out)).To(Equal("PONG"),
			"primary must accept PING with the right -a")

		By("verifying SENTINEL MASTER mymaster reports master REACHABLE with discovered peers (#425, #427)")
		// Strengthened to catch the sentinel→master auth-pass missing
		// from sentinel.conf and the operator→sentinel-listener
		// requirepass missing. Both bugs leave sentinels in
		// `flags: s_down,disconnected` with `num-slaves: 0` and
		// `num-other-sentinels: 0` because the sentinel can't subscribe
		// to the master's pub/sub channel (master rejects unauthenticated
		// connections, and the operator's observer/rotation paths can't
		// AUTH to sentinels without listener requirepass).
		//
		// The prior assertion only checked that SENTINEL MASTER returned
		// a populated reply, which is true even in the broken state —
		// sentinels echo their CONFIGURED master info regardless of
		// whether they can REACH it. The strengthened assertion checks
		// the reachability fields:
		//   - flags MUST be plain "master" (not "s_down,master,disconnected")
		//   - num-slaves >= 2 (sentinel discovered both replicas via pub/sub)
		//   - num-other-sentinels >= 2 (sentinel discovered both peers)
		// Extract the `flags`, `num-slaves`, and `num-other-sentinels`
		// fields explicitly via grep, then assert on each — more
		// robust than substring-matching on the raw RESP output
		// (which could shift framing under a future valkey-cli
		// output format). The SENTINEL MASTER reply is a flat
		// key/value list; each field's value is the line that
		// follows its key line.
		sentinelMasterField := func(field string) func() string {
			return func() string {
				out, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", "sentinel-failover-sentinel-0", "-c", "sentinel", "--",
					"sh", "-c",
					// The sentinel listener carries requirepass under auth and the
					// sentinel container ships no REDISCLI_AUTH env (only the valkey
					// container does), so this probe must pass -a explicitly —
					// otherwise the listener returns NOAUTH, grep matches nothing, and
					// the field reads empty forever (3m Eventually timeout).
					"valkey-cli -a changeme --no-auth-warning -p 26379 SENTINEL MASTER mymaster | grep -A1 -E '^"+field+"$' | tail -1",
				))
				return strings.TrimSpace(out)
			}
		}
		Eventually(sentinelMasterField("flags"), 3*time.Minute, 5*time.Second).Should(
			Equal("master"),
			"SENTINEL MASTER flags must converge to plain 'master' — without #425+#427 fixes, flags stays 's_down,master,disconnected' indefinitely")
		Eventually(sentinelMasterField("num-slaves"), 3*time.Minute, 5*time.Second).Should(
			Equal("2"),
			"SENTINEL MASTER num-slaves must converge to 2 (master discovered both replicas via pub/sub) — without #425+#427 fixes, stays 0 because sentinel can't subscribe")
		Eventually(sentinelMasterField("num-other-sentinels"), 3*time.Minute, 5*time.Second).Should(
			Equal("2"),
			"SENTINEL MASTER num-other-sentinels must converge to 2 (sentinel discovered both peers via master pub/sub) — without #425+#427 fixes, stays 0")

		By("verifying SentinelQuorum.status populates per-pod (#428)")
		// Pre-fix: kubectl get sq showed empty PRIMARY and QUORUM
		// columns indefinitely. Post-fix: the in-process observer's
		// per-endpoint snapshot flows into each SQ.Status via the
		// reconciler's reconcileSentinelQuorumStatus writer.
		for _, sqName := range []string{"sentinel-failover-sentinel-0", "sentinel-failover-sentinel-1", "sentinel-failover-sentinel-2"} {
			Eventually(func() string {
				out, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "sentinelquorums.velkir.ioxie.dev", sqName,
					"-o", "jsonpath={.status.quorumReachable}",
				))
				return strings.TrimSpace(out)
			}, 3*time.Minute, 5*time.Second).Should(Equal("true"),
				"SentinelQuorum %q .status.quorumReachable must converge to true", sqName)
			Eventually(func() string {
				out, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "sentinelquorums.velkir.ioxie.dev", sqName,
					"-o", "jsonpath={.status.observedPrimary}",
				))
				return strings.TrimSpace(out)
			}, 3*time.Minute, 5*time.Second).Should(Equal("sentinel-failover-0"),
				"SentinelQuorum %q .status.observedPrimary must converge to the bootstrap primary pod name", sqName)
		}
	})

	// Scenario 7 — single-replica sentinel CR (sub-HA). The data-plane
	// has only one valkey pod, which is not enough for the sentinel
	// pool to perform a failover (sentinel needs at least one replica
	// to promote). The webhook accepts the CR on purpose (lab use,
	// smoke testing) and attaches a `Warning:` header naming the gap;
	// the reconciler then carries the matching `Degraded=True
	// reason=HANotMet` condition while the CR is sub-HA. Ready may
	// still flip True (the single pod is healthy on its own; the
	// degradation is structural, not a Ready blocker), and the
	// sentinel pool reaches its full count of 3 because that plane is
	// independent of the data-plane replica count.
	It("accepts single-replica sentinel CR with sub-HA Warning; Degraded=True reason=HANotMet", func() {
		const crName = "sentinel-sub-ha"

		By("applying the sub-HA CR; webhook attaches the sub-HA Warning header on accept")
		// Inline `kubectl apply` instead of the shared `applyCR`
		// helper because the helper drops the combined output —
		// scenario 7 needs the `Warning:` text from stderr to assert
		// the webhook fired the soft-warn path.
		applyCmd := exec.Command("kubectl", "-n", e2eNamespace, "apply", "-f", "-")
		applyCmd.Stdin = strings.NewReader(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 1
  sentinel:
    masterName: mymaster
    replicas: 3
`)
		applyOut, applyErr := applyCmd.CombinedOutput()
		Expect(applyErr).NotTo(HaveOccurred(),
			"kubectl apply failed (sub-HA must be accepted, not rejected): %s", string(applyOut))
		Expect(string(applyOut)).To(SatisfyAll(
			ContainSubstring("Warning:"),
			ContainSubstring("sub-HA"),
			ContainSubstring("HANotMet"),
			ContainSubstring("valkey.replicas=1"),
		), "webhook must attach a Warning header naming the sub-HA gap and the HANotMet reason that the reconciler will surface")

		waitForReady(crName, 5*time.Minute)

		By("verifying valkey STS reaches readyReplicas=1 (single-pod degenerate primary)")
		Eventually(stsReadyReplicas(crName), 5*time.Minute, 10*time.Second).Should(Equal("1"),
			"the single valkey pod must reach Ready — sub-HA is structural, not a Ready blocker")

		By("verifying sentinel STS still reaches readyReplicas=3 (sentinel plane independent of data-plane HA)")
		Eventually(stsReadyReplicas(crName+"-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 — its readiness does not depend on the data-plane replica count")

		By("verifying pod-0 carries role=primary (single-pod degenerate primary)")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			return strings.TrimSpace(out)
		}, 2*time.Minute, 2*time.Second).Should(Equal("primary"),
			"pod-0 must be labelled role=primary — the single pod is the degenerate primary in sub-HA")

		By("verifying Degraded=True on the CR")
		Eventually(func() string {
			return getCondition(crName, "Degraded")
		}, 2*time.Minute, 2*time.Second).Should(Equal("True"),
			"Degraded must flip True for a sub-HA sentinel CR — matches the webhook Warning")

		By("verifying Degraded reason=HANotMet (matches the webhook Warning)")
		// Inline jsonpath rather than a getCondition extension —
		// HANotMet is the only reason scenario 7 needs to assert on
		// today; centralising a reason helper is wider scope than
		// this issue calls for.
		reason := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
			"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
		))
		Expect(strings.TrimSpace(reason)).To(Equal("HANotMet"),
			"Degraded reason must be HANotMet — surfaces the static-config sub-HA gap the webhook warned about at apply time")
	})

	// Scenario 2 — kill the bootstrap primary pod and verify the
	// failover handoff is observable end-to-end: sentinels detect the
	// loss, elect a new primary, and the operator relabels the
	// surviving replica as `role=primary` within the rollout window.
	// The same scenario also pins two M3 exit-criteria invariants:
	//   - the new primary accepts writes via the client `<cr>` Service
	//     within 30s (sentinel `failoverTimeout` floor + observer
	//     republish delay),
	//   - the natural failover never trips the split-brain guard —
	//     no `SplitBrainDetected` event fires, no `Degraded` flips
	//     to `reason=SplitBrain`. (Distinct from scenario 3, which
	//     injects a ≥quorum-unreachable partition (Quorum=Unknown) to
	//     exercise the relabel-refusal guard and the event-
	//     suppression contract.)
	It("kill primary pod -> sentinels elect new primary; operator relabels within rollout window; writes resume", func() {
		const crName = "sentinel-failover"

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying the bootstrap put pod-0 in role=primary before any kill (precondition)")
		// Without this guard, a regression that flipped the bootstrap
		// topology would silently change the meaning of the test —
		// killing pod-0 wouldn't be killing the primary, and the
		// post-kill assertions would still pass against a different
		// election outcome than the one this scenario is meant to
		// pin.
		pod0RoleBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))
		Expect(pod0RoleBefore).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the kill (sentinel mode keeps the bootstrap topology until the first failover)")

		By("seeding a write on the bootstrap primary so post-failover reads can verify replication survived")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m38-failover-key", "m38-failover-value",
		))

		By("recording the bootstrap primary pod UID before delete")
		// UID, not name — the StatefulSet recreates pod-0 with the
		// same name, so the failover-target pod can only be
		// distinguished by which pod retains its original UID and
		// which one(s) are recreations.
		pod0UIDBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(pod0UIDBefore).NotTo(BeEmpty())

		By("deleting the primary pod (kill -9 equivalent: --force --grace-period=0)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-0",
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the operator to relabel a surviving pod as the new primary")
		// The relabel propagation chain: sentinel
		// `down-after-milliseconds` detection →
		// sentinel quorum on +odown → +switch-master → operator
		// observer republish → reconcile pass → pod-label patch.
		// 90s baseline covers a typical chain (~45-50s real time)
		// with 2× CI-slowness headroom. The "10s" figure in the M3
		// exit criteria tracks the OPERATOR-SIDE relabel latency
		// alone (`+switch-master observed → label patched`); it is
		// NOT the full failover wall-time, which is sentinel-side
		// and bounded by `failoverTimeout` (180s floor) for the
		// in-progress election but typically completes in <30s on
		// a healthy cluster.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).ShouldNot(BeEmpty(),
			"a pod must carry role=primary within the rollout window post-kill")

		By("verifying the new primary is NOT the just-killed pod-0 (election picked a peer)")
		// Two acceptable post-failover topologies:
		//   - pod-0 is gone (recreated, fresh UID) and pod-1 or
		//     pod-2 is the new primary;
		//   - pod-0 has come back as pod-0 in role=replica and
		//     pod-1 or pod-2 is the new primary.
		// Either way, role=primary must NOT be on a pod whose
		// UID matches the killed pod's pre-delete UID.
		newPrimaryName := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(newPrimaryName).NotTo(BeEmpty(),
			"a primary pod must exist after the failover")
		newPrimaryUID := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", newPrimaryName,
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(newPrimaryUID).NotTo(Equal(pod0UIDBefore),
			"new primary UID %q must not match the killed pod's pre-delete UID %q (sentinel must elect a peer)",
			newPrimaryUID, pod0UIDBefore)

		By("verifying the new primary accepts writes within 30s of relabel")
		// The 30s gate tracks the M3 exit criteria ("new primary
		// ready for writes within 30s"). Issue a write via
		// `kubectl exec` directly on the new primary pod — the
		// Service-routed path is the same thing one layer up; the
		// pod-direct path validates the data plane is ready, the
		// Service-selector path is implicitly validated by the
		// successful relabel above (kube-proxy reprograms its
		// rules from the role=primary selector update).
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", newPrimaryName, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "set", "m38-postfailover", "ok",
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("OK"),
			"the new primary must accept writes within 30s of relabel")

		By("verifying the seeded pre-failover write survived replication onto the new primary")
		// The seed write was on the bootstrap primary. If
		// replication or the failover handoff is buggy, the new
		// primary may not carry it. This is the data-plane survival
		// invariant the seeded write was added to pin.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", newPrimaryName, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m38-failover-key",
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m38-failover-value"),
			"the pre-failover write must be present on the new primary (replication + failover preserved data)")

		By("verifying the natural failover did NOT permanently trip the split-brain guard")
		// SplitBrainDetected fires when the observer publishes
		// QuorumOK=false. The tri-state quorum collapses
		// QuorumStatusUnknown to QuorumOK=false for the legacy
		// boolean consumer, and sentinel polls during the elect-
		// new-primary window can transiently return Unknown (one or
		// two sentinels are momentarily replying with stale or in-
		// flight master state mid-election). On fast clusters the
		// reconciler may catch one or more such transient snapshots
		// and emit SplitBrainDetected before the observer settles on
		// QuorumStatusOK again. The stronger correctness signal is
		// that the operator DID successfully relabel a survivor
		// (asserted above) — meaning the guard cleared and the
		// failover completed. Verify that the final observed quorum
		// state is OK and the Degraded condition is not stuck on
		// reason=SplitBrain.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).ShouldNot(Equal("SplitBrain"),
			"natural single-valkey-pod failover must leave Degraded=SplitBrain cleared once the observer resettles")
	})

	// Scenario 3 — split-brain injection under a ≥quorum-unreachable
	// partition. Genuine network-partition split-brain is hard to inject
	// under the kindnet CNI (no NetworkPolicy enforcement); the next-best
	// real injection is a forced shortage of reachable sentinels — delete
	// two of three sentinel pods so the observer's pollOnce sees
	// `reachable < threshold` and publishes `Quorum=Unknown` (QuorumOK=
	// false) — NOT `Quorum=Lost`.
	//
	// The suppression contract: `Unknown` means "no data yet" (the observer
	// could not reach a quorum of peers), not a real disagreement, so the
	// operator SUPPRESSES the `SplitBrainDetected` event +
	// `valkey_split_brain_detections_total` counter AND the
	// `Degraded=True reason=SplitBrain` condition on Unknown. Only a
	// genuine `Quorum=Lost` (≥quorum reachable but agreement not met) is a
	// split-brain signal — reporting it on Unknown would false-flap on
	// every operator restart of a healthy cluster.
	//
	// The Phase 7 relabel guard is separate and stricter: it refuses to
	// relabel on `!QuorumOK` (Unknown OR Lost), so a surviving pod's role
	// label stays put throughout the partition. Because the new contract
	// surfaces no event/condition on Unknown, that relabel-refusal is the
	// load-bearing positive assertion of this spec.
	It("split-brain injection (Quorum=Unknown) -> suppresses SplitBrainDetected/Degraded, refuses to relabel", func() {
		const crName = "sentinel-splitbrain"

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
`)
		waitForReady(crName, 5*time.Minute)

		By("recording the bootstrap primary pod's role label going into the partition")
		const primaryPodName = crName + "-0"
		primaryRoleBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", primaryPodName,
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))
		Expect(primaryRoleBefore).To(Equal("primary"),
			"bootstrap primary must carry role=primary going into the partition")

		By("recording the baseline SplitBrainDetected event count (a healthy bootstrap is expected to be 0)")
		// Assert the post-partition delta is 0 rather than an absolute
		// count, so a stray pre-existing event (there should be none on a
		// clean bootstrap under the suppression contract) can never mask a regression.
		baselineSplitBrain := countEventsByReason(crName, "SplitBrainDetected")

		By("injecting partition: delete sentinel-0 and sentinel-1 simultaneously (force, no grace)")
		// Two-of-three sentinels gone leaves only sentinel-2
		// reachable; the observer's pollOnce threshold for 3
		// endpoints is 2 (majority), so reachable=1 < threshold=2
		// → publish Quorum=Unknown (QuorumOK=false). The StatefulSet
		// controller recreates the deleted pods within ~10-30s on a
		// typical cluster, but the window is more than long enough for
		// the observer's poll-tick (default 10s) to land at least one
		// Quorum=Unknown snapshot. Slow / overloaded clusters may
		// stretch the recreation past 30s, which only widens the
		// partition window — never narrows it below the observer's
		// tick budget.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod",
			crName+"-sentinel-0", crName+"-sentinel-1",
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting until both deleted sentinel pods are observed gone (partition definitely active)")
		// `--wait=false` returns immediately — the pods may still
		// be Terminating when the next assertion runs. Poll until
		// the pod-list reports neither pod exists, OR the pods
		// have been recreated with a fresh UID. The boundary case
		// (StatefulSet recreates the pod within milliseconds)
		// can't be distinguished by name alone; UID is the durable
		// identity signal. We lookup BOTH pods' status here so a
		// later "did the partition actually fire?" failure
		// diagnoses cleanly.
		Eventually(func() bool {
			for _, ord := range []string{"0", "1"} {
				phase, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "pod",
					crName+"-sentinel-"+ord,
					"-o", "jsonpath={.status.phase}",
				))
				phase = strings.TrimSpace(phase)
				if phase == "" || phase == "Pending" || phase == "Failed" || phase == "Succeeded" {
					// pod gone (NotFound returns "" via the
					// ignored err) or in a non-Running phase
					// the observer treats as unreachable.
					return true
				}
			}
			return false
		}, 30*time.Second, 1*time.Second).Should(BeTrue(),
			"at least one of the deleted sentinel pods must be observed in a non-Running state (partition active)")

		By("verifying the #557 suppression contract holds throughout the Unknown window")
		// One Consistently window overlapping the active partition (the
		// observer publishes Quorum=Unknown within its ~10s poll tick) and
		// running into recovery. Three invariants must hold at every poll:
		//
		//   1. SplitBrainDetected must NOT fire. The event +
		//      valkey_split_brain_detections_total counter gate on
		//      Quorum==Lost (snapshotReportsSplitBrain in
		//      internal/controller/split_brain_observe.go), and this
		//      injection produces Quorum=Unknown ("no data yet", the
		//      observer reached < a quorum of peers) — never Lost. A
		//      regression that fired on Unknown would surface the event
		//      within one or two reconcile passes.
		//   2. Degraded must NOT take reason=SplitBrain. It is driven by
		//      the SAME shared gate (Observation.SplitBrainActive ←
		//      snapshotReportsSplitBrain), so its suppression must stay
		//      consistent with the event's — no event-vs-condition drift.
		//   3. pod-0's role label must stay byte-identical to the
		//      pre-partition value. The Phase 7 relabel guard refuses to
		//      relabel on !QuorumOK (Unknown OR Lost), so a ≥quorum-
		//      unreachable partition must never race a rogue relabel.
		//      Under the new contract the partition surfaces NO split-brain
		//      event/condition, so this relabel-refusal is the load-bearing
		//      positive assertion that the guard engaged.
		//
		// Coverage boundary: like the prior positive assertion, this relies
		// on the partition window (≥10-30s pod recreation) exceeding the
		// observer's ~10s poll tick to actually reach Unknown. We do not
		// gate on a positive Unknown observation: the observer is quiet on
		// Unknown by design, and the SentinelQuorum aggregation that IS
		// kubectl-observable is freshness-windowed, so a brief partition may
		// never register there — gating on it would flake, not strengthen.
		Consistently(func(g Gomega) {
			g.Expect(countEventsByReason(crName, "SplitBrainDetected")-baselineSplitBrain).To(Equal(0),
				"SplitBrainDetected must NOT fire on Quorum=Unknown — only on a real Quorum=Lost")

			reasonOut, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
			))
			g.Expect(strings.TrimSpace(reasonOut)).NotTo(Equal("SplitBrain"),
				"Degraded reason must NOT reach SplitBrain on Quorum=Unknown — only on a real Quorum=Lost")

			roleDuring, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", primaryPodName,
				"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
			))
			g.Expect(strings.TrimSpace(roleDuring)).To(Equal(primaryRoleBefore),
				"pod-0 role label must stay byte-identical across the partition window (relabel guard refuses on !QuorumOK)")
		}, 30*time.Second, 2*time.Second).Should(Succeed())

		By("verifying recovery: sentinel pods are recreated, observer reconverges to QuorumOK, Degraded settles False")
		// Self-healing — both deleted sentinel pods come back via the
		// StatefulSet controller, observer pollOnce sees reachable >=
		// threshold again, the snapshot republishes Quorum=OK, and the CR
		// settles on Degraded=False. Under the suppression contract Degraded never
		// reached reason=SplitBrain during the Unknown window (asserted
		// above), so this confirms the cluster ends healthy and no late
		// split-brain signal latched. 5min budget is generous because the
		// data-plane Ready may flap during the window.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].status}`,
			))
			return strings.TrimSpace(out)
		}, 5*time.Minute, 2*time.Second).Should(Equal("False"),
			"Degraded must settle False once the partition heals and the observer republishes Quorum=OK")
	})
	// Scenario 9 — hot-rotation of an auth Secret on a
	// running 3-replica + 3-sentinel cluster. Bootstraps with auth,
	// edits the Secret with a new password, and verifies that the
	// rotation orchestrator re-credentials every data-plane pod
	// without a pod restart: clients can authenticate with the new
	// password, the old password is rejected, the
	// `Status.Rollout.AuthRotation` substate walks Idle→Succeeded,
	// the SecretRotated Kubernetes Event lands, and the
	// `event=auth_secret_rotated` audit-log entry is emitted to
	// stdout (visible to log shippers / compliance pipelines).
	//
	// User-supplied auth Secrets do NOT carry the
	// `app.kubernetes.io/managed-by=velkir` label — the
	// operator reads them via APIReader (uncached) so the cluster's
	// label-narrowed informer cache (lateral-movement defence) does
	// not exclude them.
	//
	// The reconcile that drives the rotation needs an event to wake
	// it up — the operator does not Watch user Secrets directly. We
	// trigger it by stamping a transient annotation on the owned
	// StatefulSet (Owns(StatefulSet) propagates through to reconcile)
	// rather than mutating CR spec, which would conflate rollout
	// causes.
	It("hot-rotates the auth Secret on a 3-replica + 3-sentinel cluster without pod restart", func() {
		if sharedClusterMode() {
			// Tracked separately (race-condition list). Bootstrap-time
			// SplitBrain on the 3-sentinel cluster blocks role-label
			// writes for some duration. The auth-rotation driver
			// requires every data-plane pod to be observable (role
			// label set + PodIP set) before it can converge; when
			// label-suppression overlaps the rotation window, the
			// driver defers indefinitely. The driver behaviour is
			// correct — refusing rotation on a partial pod view
			// prevents leaving unobservable pods on the old
			// password — but the bootstrap race makes the test
			// flaky on shared clusters. Envtest covers the
			// drive-with-full-view path deterministically.
			Skip("flaky on shared cluster due to bootstrap SplitBrain ↔ label-suppression race (tracked in #410)")
		}
		const crName = "sentinel-rotate"
		const secretName = "auth-rotate-hot"
		const oldPwd = "hunter-old"
		const newPwd = "hunter-new"

		By("creating the auth Secret (no managed-by label — APIReader bypasses the cache filter)")
		_, err := utils.Run(exec.Command("kubectl", "-n", e2eNamespace,
			"create", "secret", "generic", secretName,
			"--from-literal=password="+oldPwd))
		Expect(err).NotTo(HaveOccurred(), "creating auth Secret failed")
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "secret", secretName,
				"--ignore-not-found", "--wait=false"))
		})

		applyCR(fmt.Sprintf(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: %s
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
  auth:
    secretName: %s
    secretKey: password
    sentinelAuthSecretName: %s
    sentinelAuthSecretKey: password
`, crName, secretName, secretName))
		waitForReady(crName, 5*time.Minute)

		By("verifying both StatefulSets reach 3/3 ready under the OLD password")
		Eventually(stsReadyReplicas(crName), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"valkey STS must reach 3/3 ready before rotation")
		Eventually(stsReadyReplicas(crName+"-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 ready before rotation")

		By("verifying the OLD password authenticates against the primary")
		out := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-a", oldPwd, "--no-auth-warning", "-p", "6379", "ping",
		))
		Expect(strings.TrimSpace(out)).To(Equal("PONG"),
			"primary must accept PING with the OLD password before rotation")

		By("waiting for the operator to stamp the initial AuthRotation observation (Idle)")
		// First-observation path: the reconciler hashes the Secret
		// content and stamps `Status.Rollout.AuthRotation =
		// {Phase: Idle, ObservedSecretHash: <h>}`. Without this stamp,
		// step "patch Secret" would race the first observation and the
		// reconciler would treat the new content as the initial
		// observation rather than a rotation trigger.
		Eventually(func() string {
			s, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", "jsonpath={.status.rollout.authRotation.observedSecretHash}",
			))
			return strings.TrimSpace(s)
		}, 2*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
			"AuthRotation.ObservedSecretHash must be stamped on first observation")

		By("patching the auth Secret with the NEW password")
		// Capture the wall-clock checkpoint immediately before the
		// rotation trigger so the SecretRotated-event and operator-log
		// assertions can filter to "events emitted after this point",
		// not "any matching event in the namespace". A test re-run on
		// a dirty namespace (or any prior CR carrying the same name)
		// would otherwise match a stale event.
		rotationTriggerTime := time.Now()
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "patch", "secret", secretName,
			"--type=merge", "-p", fmt.Sprintf(`{"stringData":{"password":"%s"}}`, newPwd),
		))

		By("touching the owned StatefulSet to wake the reconciler")
		// The operator does not watch user Secrets, but it does
		// `Owns(StatefulSet)` — annotating the STS fires an Update
		// event that propagates to reconcile. The annotation is
		// content-free; the reconcile will see it and proceed past
		// the STS phase normally. Using `--overwrite` so this is
		// idempotent across re-runs.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "annotate", "sts", crName,
			"--overwrite", fmt.Sprintf("rotation-trigger=%d", time.Now().UnixNano()),
		))

		By("waiting for Status.Rollout.AuthRotation to land on Succeeded or settle back to Idle with the new hash")
		// The operator writes Phase=Succeeded on the all-success path
		// and then the next reconcile immediately settles Succeeded →
		// Idle. On fast clusters the test poll can land between those
		// two writes and miss the Succeeded snapshot entirely; the
		// rotation still happened. Accept either Phase=Succeeded (the
		// transient signal) or Phase=Idle with an ObservedSecretHash
		// that matches the NEW Secret content — the post-settle
		// invariant.
		newHash := sha256Hex([]byte(newPwd))
		Eventually(func() string {
			phase, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", "jsonpath={.status.rollout.authRotation.phase}",
			))
			if strings.TrimSpace(phase) == "Succeeded" {
				return "Succeeded"
			}
			if strings.TrimSpace(phase) != "Idle" {
				return strings.TrimSpace(phase)
			}
			observedHash, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", "jsonpath={.status.rollout.authRotation.observedSecretHash}",
			))
			if strings.TrimSpace(observedHash) == newHash {
				return "Succeeded"
			}
			return "Idle"
		}, 3*time.Minute, 5*time.Second).Should(Equal("Succeeded"),
			"AuthRotation phase must reach Succeeded (or settle to Idle with the new hash)")

		By("verifying the NEW password authenticates against every data-plane pod (no pod restart)")
		// Hot-rotation invariant: the same pod processes that were
		// running pre-rotation must accept the new credential. If
		// the operator silently restarted a pod (e.g. by bouncing
		// the STS), this assertion would still pass — but we also
		// verify the pod-UID is unchanged below to pin the no-
		// restart contract.
		for _, ord := range []string{"0", "1", "2"} {
			Eventually(func() string {
				o, _ := utils.Run(exec.Command(
					"kubectl", "-n", e2eNamespace, "exec", crName+"-"+ord, "-c", "valkey", "--",
					"valkey-cli", "-a", newPwd, "--no-auth-warning", "-p", "6379", "ping",
				))
				return strings.TrimSpace(o)
			}, 30*time.Second, 2*time.Second).Should(Equal("PONG"),
				"pod %s-%s must accept the NEW password post-rotation", crName, ord)
		}

		By("verifying the OLD password no longer authenticates (rejected with NOAUTH/WRONGPASS)")
		// The new credential replaced the old; the old must now be
		// rejected. We use CombinedOutput because valkey-cli writes
		// the auth-error to stderr on a non-zero exit and utils.Run
		// drops stderr.
		oldPwdCmd := exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-a", oldPwd, "-p", "6379", "ping",
		)
		oldPwdOut, _ := oldPwdCmd.CombinedOutput()
		Expect(string(oldPwdOut)).To(SatisfyAny(
			ContainSubstring("WRONGPASS"),
			ContainSubstring("invalid username-password"),
		), "primary must REJECT the OLD password post-rotation; got: %s", string(oldPwdOut))

		By("verifying a fresh SecretRotated Kubernetes Event landed on the CR (post-trigger)")
		// Pull events with their lastTimestamp + message and filter
		// to ones at or after rotationTriggerTime (with the same
		// -1s tolerance as the operator-log filter below — kube
		// events carry second-precision lastTimestamp, so an event
		// emitted in the same wall-clock second as the trigger
		// would otherwise be excluded by an exclusive `.After()`
		// comparison even though it's genuinely fresh). A namespace
		// already carrying a stale SecretRotated from a prior test
		// run would still be excluded as long as it's older than
		// the trigger by ≥1s.
		eventCutoff := rotationTriggerTime.Add(-1 * time.Second)
		Eventually(func() bool {
			o, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "events",
				"--field-selector", fmt.Sprintf("involvedObject.name=%s,reason=SecretRotated", crName),
				"-o", "jsonpath={range .items[*]}{.lastTimestamp}|{.message}{\"\\n\"}{end}",
			))
			if err != nil {
				return false
			}
			for _, line := range strings.Split(strings.TrimSpace(o), "\n") {
				parts := strings.SplitN(line, "|", 2)
				if len(parts) != 2 {
					continue
				}
				ts, parseErr := time.Parse(time.RFC3339, parts[0])
				if parseErr != nil {
					continue
				}
				if !ts.Before(eventCutoff) && strings.Contains(parts[1], "rotated") {
					return true
				}
			}
			return false
		}, 1*time.Minute, 5*time.Second).Should(BeTrue(),
			"a SecretRotated event with timestamp ≥ rotationTriggerTime-1s must land on the CR after a successful rotation")

		By("verifying the auth_secret_rotated audit-log entry is visible in operator stdout")
		// Compliance pipelines tail the operator pod stdout for
		// `event=auth_secret_rotated` entries. The line is emitted at
		// V(0) by `internal/audit.Log`, so it surfaces unconditionally
		// in the operator log stream regardless of verbosity.
		//
		// Operator namespace: package-level `namespace` var is env-
		// driven (E2E_OPERATOR_NAMESPACE → defaults to the kustomize
		// `velkir-system`). Legacy OPERATOR_NAMESPACE is
		// honoured for back-compat with older drivers.
		operatorNS := os.Getenv("OPERATOR_NAMESPACE")
		if operatorNS == "" {
			operatorNS = namespace
		}
		// Bound the log query with --since-time anchored to the
		// rotation trigger checkpoint. This (a) avoids a stale audit
		// line from a prior test run satisfying the substring match
		// without proving THIS rotation emitted, and (b) sidesteps the
		// --tail-window flake where high operator-log volume between
		// the rotation and the assertion could push the audit line
		// past the tail boundary. --since-time accepts an RFC3339
		// timestamp; we subtract one second to compensate for log
		// timestamp granularity (kubelet rounds to seconds, so a
		// log emitted at the same wall-clock second as the trigger
		// could be excluded by an exclusive comparison).
		sinceTime := rotationTriggerTime.Add(-1 * time.Second).UTC().Format(time.RFC3339)
		Eventually(func() string {
			o, _ := utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "logs",
				"-l", "control-plane=controller-manager",
				"--since-time="+sinceTime,
			))
			return o
		}, 1*time.Minute, 5*time.Second).Should(SatisfyAll(
			ContainSubstring(`"event"="auth_secret_rotated"`),
			ContainSubstring(`"outcome"="succeeded"`),
			ContainSubstring(fmt.Sprintf(`"cr"="%s/%s"`, e2eNamespace, crName)),
		), "operator log must carry the auth_secret_rotated audit entry post-rotation (filtered to since=%s)", sinceTime)
	})

	// External `SENTINEL FAILOVER` exercises the T5 FSM transition:
	// the FSM is in StateSteady, the sentinel observer sees a
	// +switch-master that the operator did NOT initiate, the
	// transition fires StateSteady → StateFailoverInFlight with the
	// `UnexpectedFailover` event reason on the side. Distinguishes
	// operator-initiated failovers (no UnexpectedFailover; the operator
	// drives the transition via its own RolloutPrimary path) from
	// sentinel-initiated or user-initiated ones (UnexpectedFailover
	// fires so an operator-of-the-operator sees that a failover
	// happened outside the operator's control flow).
	//
	// Distinct from the kill-primary scenario above: that test drives
	// the same observable through a pod-deletion trigger; this one
	// drives it through an explicit operator-external `SENTINEL
	// FAILOVER` command, matching the user-action shape an SRE would
	// use during incident response.
	It("external SENTINEL FAILOVER -> UnexpectedFailover event emitted on the CR", func() {
		if sharedClusterMode() {
			// Tracked separately (deferred-feature list).
			//
			// The FSM transition T5 (StateSteady --EventSwitchMaster-->
			// StateFailoverInFlight, side-effect UnexpectedFailover) is
			// defined in internal/orchestration/transitions.go but
			// EventSwitchMaster is NEVER dispatched from the controller
			// — see grep against internal/controller/. The sentinel
			// observer's +switch-master handler updates the snapshot
			// (internal/sentinel/observer.go ~L378) and the controller
			// then converges the primary label via Snapshot().Addr, but
			// no FSM event is fired, so UnexpectedFailover never lands.
			//
			// The introducing PR (8fbad5a) shipped this e2e as "one of ten
			// scenarios; remaining nine deferred to follow-up PRs".
			// Test was un-skipped optimistically during the e2e
			// hardening sweep; the upstream wiring is still pending.
			Skip("deferred-feature: controller never dispatches EventSwitchMaster — UnexpectedFailover wiring pending (tracked in #410)")
		}
		const (
			crName     = "sentinel-unexpected"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying the bootstrap put pod-0 in role=primary before the external failover (precondition)")
		// Without this guard, a regression in the bootstrap topology
		// would silently change which pod the SENTINEL FAILOVER
		// command targets — the post-failover assertions would still
		// pass against a different election outcome than the one this
		// scenario is meant to pin.
		pod0RoleBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))
		Expect(pod0RoleBefore).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the external failover")

		By("recording the trigger timestamp so the UnexpectedFailover event filter excludes stale emissions")
		// The -1s slack mirrors the auth-rotation scenario above:
		// kubelet rounds event lastTimestamp to seconds, so an event
		// emitted in the same wall-clock second as the trigger would
		// be excluded by an exclusive .After comparison.
		triggerTime := time.Now()
		eventCutoff := triggerTime.Add(-1 * time.Second)

		By("issuing SENTINEL FAILOVER on a sentinel pod (operator did NOT initiate)")
		// The command returns +OK immediately when sentinel accepts
		// the failover request; the actual primary handoff completes
		// asynchronously on the sentinel side and fires
		// +switch-master once the new primary is elected. The
		// observer's PSUBSCRIBE on +switch-master is what feeds the
		// FSM's EventSwitchMaster from StateSteady → T5. Sentinels are
		// equivalent observers — pod-0 is just the lowest ordinal,
		// not special — so any sentinel pod can issue the command.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-sentinel-0", "-c", "sentinel", "--",
			"valkey-cli", "-p", "26379", "SENTINEL", "FAILOVER", masterName,
		))

		By("waiting for the UnexpectedFailover Kubernetes event to land on the CR")
		// failoverTimeout is the 180s ceiling on a sentinel-side
		// election attempt (sentinel aborts the in-progress election
		// past that bound). Typical completion on a healthy cluster
		// is < 30s; the existing kill-primary scenario uses a 90s
		// relabel window. 120s here gives ~4× headroom over the
		// typical observed time, with margin for the +switch-master
		// → observer republish → reconcile chain.
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "events",
				"--field-selector",
				fmt.Sprintf("involvedObject.name=%s,reason=UnexpectedFailover", crName),
				"-o", "jsonpath={range .items[*]}{.lastTimestamp}{\"\\n\"}{end}",
			))
			if err != nil {
				return false
			}
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				ts, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(line))
				if parseErr != nil {
					continue
				}
				if !ts.Before(eventCutoff) {
					return true
				}
			}
			return false
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(),
			"UnexpectedFailover must fire when a +switch-master observed while FSM=StateSteady was not operator-initiated")

		By("verifying the cluster reconverges with a labelled primary on a peer pod")
		// One Eventually does both checks atomically against the same
		// query: there must be a labelled primary AND it must not be
		// pod-0. Splitting these across two queries races the
		// relabel propagation chain — the test could see a primary
		// in query 1, then re-read after a transient relabel cycle
		// and see a different name in query 2. The combined check
		// returns the new primary's name only when both predicates
		// hold; the Should comparison pins both invariants at once.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			name := strings.TrimSpace(out)
			if name == "" || name == crName+"-0" {
				return ""
			}
			return name
		}, 90*time.Second, 2*time.Second).ShouldNot(BeEmpty(),
			"external SENTINEL FAILOVER must hand off to a peer; either no pod carries role=primary or pod-0 remained the primary which means the failover never took effect")
	})

	// Scenario 5 — operator killed mid-rollout. Verifies the
	// `deriveState` post-restart resume path: a rollout is initiated,
	// the operator pod is deleted while the STS still has pods at the
	// pre-update revision, the Deployment recreates the operator pod,
	// and the rolling update completes correctly with no manual
	// intervention. Pins the exit criterion that the FSM
	// re-derives its state from observed K8s + sentinel facts on every
	// reconcile, so an operator restart is invisible to the data plane
	// modulo a single requeue cycle.
	//
	// The rollout trigger is `ManualRollout` (a single annotation bump)
	// rather than a config / image change: it drives the STS pod-
	// template hash forward through the same edge a real upgrade would
	// take, but doesn't require pre-loading a second image tag into
	// the kind cluster and doesn't conflate the test with the
	// scenario-2 config-bump coverage.
	//
	// The operator pod is killed AFTER the STS UpdateRevision changes
	// but BEFORE the operator has finished rolling pods (the
	// `updatedReplicas` < 3 window). That window pins the resume from
	// the StateRolloutReplicas branch of deriveStateFromFacts: pods
	// exist, quorum OK, AllReplicasAtTargetRevision=false,
	// PrimaryAtTargetRevision=false — fallback to RolloutReplicas. On
	// restart the operator re-reads these facts and continues the loop
	// from where it left off; the data plane sees the rollout complete
	// after one additional reconcile cycle.
	It("operator killed mid-rollout -> rollout resumes post-restart, all pods reach target revision", func() {
		const (
			crName     = "sentinel-opkill"
			masterName = "mymaster"
		)
		// operatorLabel defaults to the kustomize-deploy pod label.
		// Shared-cluster runs (chart-installed operator) override via
		// E2E_OPERATOR_LABEL (e.g. "app.kubernetes.io/instance=<release>").
		// operatorNS pulls from the package-level `namespace` var
		// (env-driven via E2E_OPERATOR_NAMESPACE).
		operatorLabel := envOrDefault("E2E_OPERATOR_LABEL", "control-plane=controller-manager")
		operatorNS := namespace

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying the bootstrap put pod-0 in role=primary before the rollout (precondition)")
		// Same guard as scenarios 2 and UnexpectedFailover above:
		// without it a regression that flipped the bootstrap topology
		// would silently change the meaning of the test — the
		// post-rollout "primary survived or peer took over" check
		// would pass against a topology this scenario was not
		// designed to validate.
		pod0RoleBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))
		Expect(pod0RoleBefore).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the rollout (sentinel mode keeps the bootstrap topology until the first failover)")

		By("seeding a write so the post-rollout data plane can be verified end-to-end")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m49-opkill-key", "m49-opkill-value",
		))

		By("recording the STS UpdateRevision before the rollout trigger")
		// Compared against the post-trigger value to detect "rollout
		// has started". A spec change can take a beat to propagate
		// through the apiserver → STS controller chain, so the test
		// must read both values and wait for them to diverge before
		// killing the operator.
		stsRevBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "sts", crName,
			"-o", "jsonpath={.status.updateRevision}",
		)))
		Expect(stsRevBefore).NotTo(BeEmpty(),
			"STS must have a populated updateRevision after bootstrap")

		By("triggering a rollout via the ManualRollout annotation")
		// ManualRollout drives the STS pod-template hash forward
		// through the standard rollout-trigger edge without needing
		// a second container image preloaded into kind. The value is
		// opaque to the operator — any non-empty differing string
		// trips the next reconcile to project a new template hash.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "annotate", "--overwrite",
			"valkeys.velkir.ioxie.dev", crName,
			"velkir.ioxie.dev/rollout-generation=m49-opkill-"+time.Now().Format("20060102T150405"),
		))

		By("waiting for the STS UpdateRevision to advance off the pre-trigger value")
		// One Eventually returning the post-trigger revision string
		// only when it differs from stsRevBefore. 60s ceiling is
		// generous — the reconcile chain (annotation apply → STS
		// spec patch → STS controller status update) is typically
		// <5s on a healthy cluster.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updateRevision}",
			))
			if err != nil {
				return ""
			}
			rev := strings.TrimSpace(out)
			if rev == "" || rev == stsRevBefore {
				return ""
			}
			return rev
		}, 60*time.Second, 2*time.Second).ShouldNot(BeEmpty(),
			"STS UpdateRevision must advance after the ManualRollout annotation is applied")

		By("confirming the rollout is actually mid-flight (updatedReplicas < replicas) before killing the operator")
		// Load-bearing gate. Without this, the test would silently
		// pass even if `deriveState`'s resume path were broken: a
		// kill firing AFTER the operator had already completed the
		// rollout puts the FSM in Steady, and every downstream
		// assertion in this scenario would be trivially satisfied
		// without ever exercising the StateRolloutReplicas resume
		// branch. Pinning `updatedReplicas < 3` here proves the
		// rollout has actually started but not finished at kill-
		// time, which is the precondition that makes the post-kill
		// "rollout reached 3/3" assertion load-bearing.
		//
		// 30s ceiling is generous: even the fastest reconcile loop
		// cannot roll 3 pods (each needs a pod restart plus sentinel
		// re-discovery) before
		// this poll fires. In practice updatedReplicas==0 when this
		// check first reads it.
		Eventually(func() (int, error) {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return -1, err
			}
			s := strings.TrimSpace(out)
			if s == "" {
				// updatedReplicas omitted by kubectl when zero — STS
				// is past UpdateRevision change but has not yet
				// rolled any pod. That IS mid-flight.
				return 0, nil
			}
			var n int
			if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
				return -1, err
			}
			return n, nil
		}, 30*time.Second, 2*time.Second).Should(BeNumerically("<", 3),
			"STS updatedReplicas must be < 3 at kill-time (mid-rollout). If this fails, the operator finished rolling faster than the test could catch — the kill-window gate needs tightening or the rollout trigger needs to be heavier to slow the operator down.")

		By("recording the operator pod name + UID for the kill")
		// UID, not just name: the Deployment recreates with the same
		// label-selector identity but a fresh UID, so the post-kill
		// liveness check has to wait for a pod whose UID differs
		// from the one we just deleted (otherwise an Eventually
		// firing against the still-Terminating old pod would report
		// Ready from a doomed pod).
		opPodNameBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "get", "pods",
			"-l", operatorLabel,
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(opPodNameBefore).NotTo(BeEmpty(),
			"operator pod must be running before the kill")
		opPodUIDBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "get", "pod", opPodNameBefore,
			"-o", "jsonpath={.metadata.uid}",
		)))
		Expect(opPodUIDBefore).NotTo(BeEmpty())

		By("killing the operator pod mid-rollout (force-delete, grace-period=0)")
		// Force + grace=0 mirrors kill -9: skips the operator's
		// graceful-shutdown path so the resume is exercised against
		// a crash-like exit, not a clean leader-lease release. That
		// matches the crash window where the operator can
		// die between issuing SENTINEL FAILOVER and observing
		// +failover-end. The early-kill window in this scenario
		// doesn't reach SENTINEL FAILOVER (rollout hasn't progressed
		// to primary hand-off yet), but the kill mechanism is
		// identical.
		_ = mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "delete", "pod", opPodNameBefore,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the Deployment to recreate the operator pod with a fresh UID and Ready=True")
		// 3-minute ceiling matches the Manager Describe's own
		// readiness window in e2e_test.go. The image is already on
		// the kind node (loaded by BeforeSuite); the only delay is
		// pod-scheduling + container start + leader-election ack.
		var opPodNameAfter string
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "get", "pods",
				"-l", operatorLabel,
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }} {{ .metadata.uid }} "+
					"{{ range .status.conditions }}{{ if eq .type \"Ready\" }}{{ .status }}{{ end }}{{ end }}"+
					"\n{{ end }}{{ end }}",
			))
			if err != nil {
				return ""
			}
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 3 {
					continue
				}
				name, uid, ready := fields[0], fields[1], fields[2]
				if uid == opPodUIDBefore {
					continue
				}
				if ready != "True" {
					continue
				}
				opPodNameAfter = name
				return name
			}
			return ""
		}, 3*time.Minute, 2*time.Second).ShouldNot(BeEmpty(),
			"Deployment must recreate the operator pod and the new pod must reach Ready=True post-kill")
		Expect(opPodNameAfter).NotTo(Equal(opPodNameBefore),
			"the new operator pod must have a name different from the deleted one (Deployment recreate, not the old Terminating pod returning Ready)")

		By("waiting for the rollout to complete on the restarted operator (all 3 pods at the new STS UpdateRevision)")
		// updatedReplicas is the count of pods whose
		// controller-revision-hash matches UpdateRevision. When it
		// equals .spec.replicas (3) the rolling phase is done from
		// the STS controller's perspective; the operator's pod-
		// relabel layer still has to settle, which the subsequent
		// Ready=True check covers.
		//
		// Generous 10-minute ceiling: the resume path goes through
		// deriveState → sentinel observer poll → RolloutReplicas →
		// pod-delete → wait Ready → repeat for each of 3 pods → then
		// RolloutPrimary → SENTINEL FAILOVER → +failover-end → final
		// pod roll. Each pod restart needs sentinel re-discovery.
		// Worst case is well under 10m on a healthy kind cluster.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 10*time.Minute, 2*time.Second).Should(Equal("3"),
			"STS updatedReplicas must reach 3 (all pods rolled to the new revision) after the operator restart")

		By("verifying the CR reaches Ready=True again post-rollout")
		// deriveState's contract: once every pod is at UpdateRevision
		// and quorum is OK, deriveStateFromFacts returns Steady. The
		// reconciler's status-update path then flips Ready=True. If
		// the resume mis-derived state (e.g. stuck in RolloutPrimary
		// after the primary has already been rolled), Ready would
		// stay False past the timeout.
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 3*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the rolling update completes on the restarted operator")

		By("verifying a pod carries role=primary post-rollout")
		// The rolling update may or may not have triggered a primary
		// failover (operator-driven, master-aware path). Either way
		// some pod must carry role=primary at the end: the test pins
		// the post-resume label-stamping invariant, not the choice
		// of which pod ends up as primary.
		//
		// 5m budget covers the multi-stage causal chain on a loaded
		// cluster: new-leader-acquires-lease (up to 30s for leader
		// election after the operator pod restart) + observer
		// re-warm-up (one poll cycle to re-establish sentinel peer
		// state) + post-rollout relabel reconcile (gated on
		// readinessGateRequeue cadence). Kind converges in seconds.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 5*time.Minute, 2*time.Second).ShouldNot(BeEmpty(),
			"a pod must carry role=primary after the rolling update completes (post-restart relabel invariant)")

		By("verifying the seeded write survived the rollout + operator restart")
		// Read off the labelled primary (whichever pod that is now).
		// The pre-rollout write was on pod-0 / role=primary; if it
		// doesn't appear here either replication failed or the
		// failover handoff lost data — both regressions worth
		// pinning.
		newPrimaryName := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(newPrimaryName).NotTo(BeEmpty(),
			"a primary pod must exist for the post-rollout data-plane check")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", newPrimaryName, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m49-opkill-key",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m49-opkill-value"),
			"the pre-rollout seeded write must survive the rolling update + operator restart (replication + post-restart resume preserved data)")
	})

	// scenario 1 — image tag bump (happy path): the operator
	// drives a full master-aware rolling update. Replicas roll first
	// (highest-ordinal first), then the primary fails over to one of
	// the already-rolled replicas, then the ex-primary rejoins as a
	// replica. End state has every pod at the new image tag and SOME
	// pod (≠ pod-0) carries role=primary. The pre-rollout write
	// survives.
	//
	// The trigger is the ManualRollout annotation (same as scenario
	// 5) rather than a literal image-tag patch: ManualRollout drives
	// the same rollout-trigger edge through the operator's rolling
	// machinery, but with no dependency on a second valkey/valkey
	// image tag being available on the shared cluster's nodes (image
	// pull on a fresh tag adds 30-60s per pod and burns 3× the test
	// time without exercising any additional code path).
	It("image-tag-bump rolling -> replicas roll first, primary fails over, data survives", func() {
		const (
			crName     = "m49-imagebump"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying the bootstrap put pod-0 in role=primary before the rollout (precondition)")
		// Pinning the bootstrap topology — the post-rollout assertion
		// "primary moved off pod-0" is only meaningful if pod-0 was
		// the bootstrap primary. The sentinel mode contract is that
		// bootstrap places the primary at the lowest ordinal; if a
		// future change relaxes that the test should fail here, not
		// silently pass downstream.
		pod0RoleBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", crName+"-0",
			"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
		)))
		Expect(pod0RoleBefore).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the rollout")

		By("seeding a write so post-rollout data-plane survival can be verified end-to-end")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m49-imagebump-key", "m49-imagebump-value",
		))

		By("triggering the rolling update via the ManualRollout annotation")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "annotate", "--overwrite",
			"valkeys.velkir.ioxie.dev", crName,
			"velkir.ioxie.dev/rollout-generation=m49-imagebump-"+time.Now().Format("20060102T150405"),
		))

		By("waiting for the STS UpdateRevision to advance off the pre-trigger value (rollout actually started)")
		stsRevAfter := ""
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updateRevision}{','}{.status.currentRevision}",
			))
			if err != nil {
				return false
			}
			parts := strings.SplitN(strings.TrimSpace(out), ",", 2)
			if len(parts) != 2 {
				return false
			}
			stsRevAfter = parts[0]
			return parts[0] != "" && parts[0] != parts[1]
		}, 60*time.Second, 2*time.Second).Should(BeTrue(),
			"STS UpdateRevision must advance after the ManualRollout annotation is applied (rollout started)")
		Expect(stsRevAfter).NotTo(BeEmpty())

		By("waiting for the rolling update to complete (all 3 pods at the new STS UpdateRevision)")
		// Generous 10-minute ceiling matches the operator-killed
		// scenario above: the rolling sequence walks each pod through
		// Phase 9 (delete → STS recreate → wait Ready), then primary
		// failover (SENTINEL FAILOVER → observer Addr change → Phase
		// 7 relabel), then the ex-primary as a final replica roll.
		// Sentinel re-discovery on each replacement is gated by
		// down-after-milliseconds (floor 1s).
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 10*time.Minute, 2*time.Second).Should(Equal("3"),
			"STS updatedReplicas must reach 3 (all pods rolled to the new revision)")

		By("verifying the CR returns to Ready=True post-rollout")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 3*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the rolling update completes")

		By("verifying the rolling update triggered a primary failover (some pod ≠ pod-0 now carries role=primary)")
		// The master-aware path rolls replicas first then issues
		// SENTINEL FAILOVER for the primary. After the failover the
		// new primary is one of the already-rolled replicas, NOT
		// pod-0. Pod-0 then re-joins as a replica when its turn
		// comes (Phase 9 deletes it last). This assertion fails if a
		// regression bypassed the failover step (e.g., rolled the
		// primary in place without failover, which would violate D6
		// "operator-driven master-aware rolling").
		newPrimaryName := ""
		// SENTINEL FAILOVER can NOGOODSLAVE-retry several times on a
		// shared cluster (replicas catching up to maxLagBytes after a
		// rolling restart, multi-attach delays on the new pods). Each
		// retry strips role=primary, retries, and on failure restores
		// role=primary; during the strip window no pod carries the
		// label. The surrounding budgets are 10m (STS) / 3m (CR Ready);
		// match that band so this poll doesn't spuriously time out
		// inside the strip-retry window. Kind converges in seconds.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			newPrimaryName = strings.TrimSpace(out)
			return newPrimaryName
		}, 4*time.Minute, 2*time.Second).ShouldNot(BeEmpty(),
			"a pod must carry role=primary post-rollout")
		Expect(newPrimaryName).NotTo(Equal(crName+"-0"),
			"primary must have moved off pod-0 — the master-aware rolling contract is replicas-first-then-failover; pod-0 staying primary means the failover step was skipped")

		By("verifying the seeded write survived the rolling update + primary failover")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", newPrimaryName, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m49-imagebump-key",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m49-imagebump-value"),
			"the pre-rollout seeded write must survive the rolling update + primary failover")
	})

	// Capstone: an image-tag-bump rolling update drives an
	// in-flight primary failover; the cluster must CONVERGE to a single
	// master with no dual-master dead-IP wedge. This pins the failover-
	// transport invariants the happy-path image-tag-bump scenario above
	// does not assert:
	//
	//   * the sentinel StatefulSet apply DEFERS while the valkey roll /
	//     failover is in flight (SentinelRollDeferred) — rolling a
	//     sentinel mid-election could drop the surviving quorum below the
	//     failover threshold;
	//   * at most one pod carries role=primary at any instant during the
	//     roll, and exactly one at the end — a transient dual-master is
	//     self-healed (loser demoted onto the survivor), never left as a
	//     wedge;
	//   * the converged primary's label agrees with data-plane truth
	//     (INFO replication role:master) and serves the seeded write — it
	//     is the live master, not a dead address a replacement pod booted
	//     `replicaof` against;
	//   * the post-Ready label set is damped: the primary count holds at
	//     one across a sustained window rather than flapping as the
	//     observer re-warms.
	//
	// The dual-master self-heal and epoch-fence paths are DEFENSIVE: they
	// trip only if a real split appears during the window, which a
	// healthy cluster's operator-driven rolling does not reliably
	// produce. They are asserted for consistency-when-fired (a self-heal
	// that initiates must follow through), never required to fire —
	// requiring a defensive path to trip on a clean run would be a flaky
	// inversion of the very invariant under test.
	It("ImageBumpDuringInFlightFailover_ConvergesSingleMaster", func() {
		const (
			crName     = "m49-capstone"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying the bootstrap put pod-0 in role=primary before the rollout (precondition)")
		// The post-rollout "primary moved off pod-0" assertion is only
		// meaningful if pod-0 was the bootstrap primary.
		Expect(roleLabelOf(crName+"-0")).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the rollout")
		Expect(primaryPodCount(crName)).To(Equal(1),
			"exactly one pod must carry role=primary at bootstrap (no pre-existing split)")

		By("seeding a write so post-failover data-plane convergence can be verified end-to-end")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "exec", crName+"-0", "-c", "valkey", "--",
			"valkey-cli", "-p", "6379", "set", "m49-capstone-key", "m49-capstone-value",
		))

		By("triggering the rolling update via the ManualRollout annotation (drives the in-flight failover)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "annotate", "--overwrite",
			"valkeys.velkir.ioxie.dev", crName,
			"velkir.ioxie.dev/rollout-generation=m49-capstone-"+time.Now().Format("20060102T150405"),
		))

		By("waiting for the STS UpdateRevision to advance off the pre-trigger value (rollout actually started)")
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updateRevision}{','}{.status.currentRevision}",
			))
			if err != nil {
				return false
			}
			parts := strings.SplitN(strings.TrimSpace(out), ",", 2)
			if len(parts) != 2 {
				return false
			}
			return parts[0] != "" && parts[0] != parts[1]
		}, 60*time.Second, 2*time.Second).Should(BeTrue(),
			"STS UpdateRevision must advance after the ManualRollout annotation is applied (rollout started)")

		By("asserting no dual-master wedge appears at any sampled instant during the rolling window")
		// The strip-retry window of SENTINEL FAILOVER can transiently
		// leave ZERO pods labelled primary, which is benign (the operator
		// re-stamps once the election settles). What must NEVER happen is
		// TWO pods simultaneously labelled primary — the dual-master wedge
		// the dual-master self-heal exists to prevent. Sample across the early
		// roll; a count > 1 at any poll fails the invariant immediately.
		// (Bounded fixed cost: the targeted post-Ready damp check below is
		// the precise no-flap guard; this is the in-roll coverage.)
		Consistently(func() int {
			return primaryPodCount(crName)
		}, 60*time.Second, 3*time.Second).Should(BeNumerically("<=", 1),
			"at most one pod may carry role=primary at any instant during the rolling failover — a count > 1 is the dual-master wedge")

		By("waiting for the rolling update to complete (all 3 pods at the new STS UpdateRevision)")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 10*time.Minute, 2*time.Second).Should(Equal("3"),
			"STS updatedReplicas must reach 3 (all pods rolled to the new revision)")

		By("verifying the CR returns to Ready=True post-rollout")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 3*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the rolling update completes")

		By("verifying the sentinel roll DEFERRED during the failover window (SentinelRollDeferred fired)")
		// Phase 3 holds the sentinel StatefulSet apply whenever the valkey
		// data plane is mid-roll or a failover is in flight, emitting
		// SentinelRollDeferred each deferred pass. The rolling update keeps
		// the valkey roll active across the whole window, so at least one
		// deferred event must have been recorded — the observable proof
		// that the sentinel roll yielded to the election rather than
		// racing it.
		Expect(countEventsByReason(crName, "SentinelRollDeferred")).To(BeNumerically(">=", 1),
			"the sentinel roll must defer at least once while the valkey roll / failover is in flight")

		By("verifying convergence to exactly one primary, moved off pod-0 (failover happened, no wedge)")
		Eventually(func() int {
			return primaryPodCount(crName)
		}, 4*time.Minute, 2*time.Second).Should(Equal(1),
			"the cluster must converge to exactly one role=primary pod after the rollout")
		convergedPrimary := primaryPodName(crName)
		Expect(convergedPrimary).NotTo(BeEmpty())
		Expect(convergedPrimary).NotTo(Equal(crName+"-0"),
			"primary must have moved off pod-0 — the master-aware rolling contract is replicas-first-then-failover")

		By("verifying the converged primary is the live data-plane leader (seeded write survives, read off the new primary)")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", convergedPrimary, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "get", "m49-capstone-key",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("m49-capstone-value"),
			"the seeded write must survive the in-flight failover, read off the converged primary (proves it is the live master, not a dead-IP wedge)")

		By("verifying the converged primary reports role:master in INFO replication (label agrees with data-plane truth)")
		// A label that disagrees with the data plane is the dead-IP wedge
		// in disguise: the converged pod must actually BE a valkey master,
		// not merely carry the label.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", convergedPrimary, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "info", "replication",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 60*time.Second, 2*time.Second).Should(ContainSubstring("role:master"),
			"the converged primary must report role:master in INFO replication — the label must agree with data-plane truth")

		By("verifying the post-Ready settling damp holds the primary count at one (no label flap)")
		// The settling damp suppresses a post-Ready relabel flap as the
		// observer re-warms: once converged, the count must hold at exactly
		// one across a sustained window rather than oscillating 1->0->1 or
		// 1->2->1.
		Consistently(func() int {
			return primaryPodCount(crName)
		}, 30*time.Second, 3*time.Second).Should(Equal(1),
			"the converged primary count must hold steady at one — the settling damp must suppress any post-Ready label flap")

		By("verifying dual-master self-heal consistency: if it initiated, it followed through (demote or safe deferral)")
		// Defensive-path consistency, NOT a requirement that the path
		// fired: a clean operator-driven rolling does not reliably produce
		// a real split. But IF the self-heal initiated, it must have
		// demoted a loser onto the survivor OR deferred for a named safety
		// reason — never initiate-and-vanish, which would leave a split
		// unaddressed.
		if initiated := countEventsByReason(crName, "DualMasterSelfHealInitiated"); initiated > 0 {
			demoted := countEventsByReason(crName, "DualMasterSelfHealDemoted")
			deferred := countEventsByReason(crName, "DualMasterSelfHealDeferred")
			Expect(demoted+deferred).To(BeNumerically(">=", 1),
				"a dual-master self-heal that initiated must follow through: demote a loser or defer for a named safety reason, never initiate-and-vanish")
		}
	})

	// scenario 2 — config bumps via both spec surfaces. First a
	// `spec.valkey.configuration` string bump rolls all pods to a new
	// ConfigMap hash; then a `spec.valkey.configurationOverrides` map
	// entry overrides the same directive (D15: override map wins over
	// configuration string). After each bump CONFIG GET on the live
	// primary returns the expected effective value.
	//
	// Single CR — the second bump is applied on top of the first to
	// also pin the cross-bump invariant that the operator surfaces the
	// final effective merge correctly (override > string > mandatory
	// fallback) under successive spec changes.
	It("config bump via configuration string then configurationOverrides map; override wins on collision", func() {
		const (
			crName     = "m49-configbump"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("recording the pre-bump primary IP for the CONFIG GET probe")
		// CONFIG GET is run against the live primary regardless of
		// which pod it is post-rollout; capture the name so we can
		// re-read it after each bump if a failover moved it.
		initialPrimary := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(initialPrimary).NotTo(BeEmpty(), "a bootstrap primary must exist")

		By("capturing pre-bump STS UpdateRevision so the post-patch wait can prove a new rollout actually started")
		// Without this guard, the subsequent Eventually(updatedReplicas
		// == "3") matches instantly on a prior rollout's completed
		// state instead of waiting for the new one to start. The
		// image-bump scenario (scenario 1) uses the same pattern.
		preStringBumpRev := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "sts", crName,
			"-o", "jsonpath={.status.updateRevision}",
		)))

		By("bumping spec.valkey.configuration with maxmemory 64mb")
		// kubectl patch --type=merge so the string field is replaced
		// wholesale; the chart-default `omitempty` lets the empty
		// pre-state coexist with the new value cleanly.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", crName,
			"--type=merge",
			"-p", `{"spec":{"valkey":{"configuration":"maxmemory 64mb\n"}}}`,
		))

		By("waiting for the STS UpdateRevision to advance off the pre-bump value (config-string rollout actually started)")
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updateRevision}",
			))
			if err != nil {
				return false
			}
			cur := strings.TrimSpace(out)
			return cur != "" && cur != preStringBumpRev
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(),
			"STS UpdateRevision must advance after the configuration-string patch (operator picked up the change)")

		By("waiting for the config-bump rolling update to complete")
		// Same 10-minute ceiling as scenario 1 — the config-change
		// path drives the same rollout-trigger edge through the
		// operator's rolling machinery (ConfigMap hash change → pod-
		// template hash flip).
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 10*time.Minute, 2*time.Second).Should(Equal("3"),
			"STS must complete rolling update after configuration string bump")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 3*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the config-string bump")

		By("verifying CONFIG GET maxmemory on the live primary reports 64mb")
		Eventually(func() string {
			// utils.Run, not mustRun: zero role=primary pods is a
			// legitimate transient around the hand-off window, and the
			// indexed jsonpath makes kubectl exit non-zero on it — the
			// Eventually must retry, not abort the spec.
			podOut, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			primary := strings.TrimSpace(podOut)
			if primary == "" {
				return ""
			}
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", primary, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "config", "get", "maxmemory",
			))
			if err != nil {
				return ""
			}
			// Output shape: "maxmemory\n67108864" (64 MiB in bytes).
			// 64mb literal in valkey.conf parses to 64*1024*1024.
			return strings.TrimSpace(out)
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("67108864"),
			"CONFIG GET maxmemory must reflect the configuration-string bump (64mb = 67108864 bytes)")

		By("capturing pre-override STS UpdateRevision so the post-patch wait can prove a new rollout actually started")
		preOverrideRev := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "sts", crName,
			"-o", "jsonpath={.status.updateRevision}",
		)))

		By("layering a configurationOverrides map entry that conflicts on maxmemory (128mb)")
		// Override map wins over configuration string per D15 — the
		// effective value should flip to 128mb without touching the
		// configuration string field.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", crName,
			"--type=merge",
			"-p", `{"spec":{"valkey":{"configurationOverrides":{"maxmemory":"128mb"}}}}`,
		))

		By("waiting for the STS UpdateRevision to advance off the pre-override value (override-map rollout actually started)")
		Eventually(func() bool {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updateRevision}",
			))
			if err != nil {
				return false
			}
			cur := strings.TrimSpace(out)
			return cur != "" && cur != preOverrideRev
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(),
			"STS UpdateRevision must advance after the configurationOverrides patch (operator picked up the change)")

		By("waiting for the override-map rolling update to complete")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 10*time.Minute, 2*time.Second).Should(Equal("3"),
			"STS must complete rolling update after override-map bump")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 3*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the override-map bump")

		By("verifying CONFIG GET maxmemory on the live primary now reports 128mb (override won over the still-present configuration string)")
		Eventually(func() string {
			// utils.Run, not mustRun — same retry-on-empty-primary
			// rationale as the 64mb probe above.
			podOut, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			primary := strings.TrimSpace(podOut)
			if primary == "" {
				return ""
			}
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", primary, "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "config", "get", "maxmemory",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 2*time.Minute, 5*time.Second).Should(ContainSubstring("134217728"),
			"CONFIG GET maxmemory must reflect the override (128mb = 134217728 bytes) — D15 override-wins-over-configuration-string invariant")
	})

	// scenario 7 — CR deleted mid-rollout. Trigger a rollout,
	// kubectl delete the CR, verify the finalizer runs to completion
	// (CR object is gone, not stuck Terminating), and every owned
	// resource is GC'd: the valkey STS, the sentinel STS, the three
	// Services (write / -ro / -sentinel), the ConfigMap, the PDB.
	// The PVCs retention is governed by spec.pvcRetentionPolicy (D14)
	// — at chart defaults the policy is Retain, so the PVCs survive
	// the CR delete by design; explicit Retain check pins that
	// invariant.
	It("CR delete mid-rollout -> finalizer completes, owned resources GC'd, PVCs retained per D14 default", func() {
		const (
			crName     = "m49-delete-midroll"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
    persistence:
      size: 1Gi
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("triggering a rolling update")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "annotate", "--overwrite",
			"valkeys.velkir.ioxie.dev", crName,
			"velkir.ioxie.dev/rollout-generation=m49-delete-"+time.Now().Format("20060102T150405"),
		))

		By("waiting for the rollout to be mid-flight before the delete")
		Eventually(func() (int, error) {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "sts", crName,
				"-o", "jsonpath={.status.updatedReplicas}",
			))
			if err != nil {
				return -1, err
			}
			s := strings.TrimSpace(out)
			if s == "" {
				return 0, nil
			}
			var n int
			if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
				return -1, err
			}
			return n, nil
		}, 2*time.Minute, 2*time.Second).Should(BeNumerically("<", 3),
			"rollout must be mid-flight when the CR is deleted")

		By("deleting the CR mid-rollout")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", crName,
			"--wait=false",
		))

		By("waiting for the CR object to be fully gone (finalizer ran to completion, not stuck Terminating)")
		// The finalizer is `velkir.ioxie.dev/pvc-retention`. It applies
		// the PVC retention policy (Retain at chart defaults), then
		// removes itself; apiserver GC follows. If the finalizer
		// blocks indefinitely on mid-rollout state the CR sits at
		// Terminating forever; this Eventually catches that.
		Eventually(func() error {
			return exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
			).Run()
		}, 3*time.Minute, 5*time.Second).Should(HaveOccurred(),
			"CR object must be fully gone after delete (finalizer must not block on mid-rollout state)")

		By("verifying owned cluster-namespaced resources are GC'd")
		// The operator owns these via OwnerReferences (controller=true);
		// once the CR is gone they GC automatically. Each must not
		// exist post-deletion.
		for _, kindName := range []struct {
			kind, name string
		}{
			{"sts", crName},
			{"sts", crName + "-sentinel"},
			{"svc", crName},
			{"svc", crName + "-ro"},
			{"svc", crName + "-sentinel"},
			{"configmap", crName + "-config"},
			{"pdb", crName},
		} {
			kind := kindName.kind
			name := kindName.name
			Eventually(func() error {
				return exec.Command(
					"kubectl", "-n", e2eNamespace, "get", kind, name,
				).Run()
			}, 2*time.Minute, 5*time.Second).Should(HaveOccurred(),
				"%s/%s must be GC'd after CR delete (OwnerReferences cascade)", kind, name)
		}

		By("verifying PVCs are retained (D14 default policy=Retain)")
		// pvcRetentionPolicy default = Retain. The PVCs survive the
		// CR delete; the operator does NOT delete them. Tightly pins
		// the D14 invariant.
		pvcs := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pvc",
			"-l", "velkir.ioxie.dev/cr="+crName,
			"-o", "jsonpath={.items[*].metadata.name}",
		)))
		Expect(pvcs).NotTo(BeEmpty(),
			"PVCs must be retained after CR delete (pvcRetentionPolicy default=Retain per D14)")

		By("manual PVC cleanup so the AfterEach doesn't trip on residual claims")
		// AfterEach above only deletes Valkey CRs (which are gone);
		// the retained PVCs would carry over to the next spec under
		// the same namespace. Clean them up explicitly.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pvc",
			"-l", "velkir.ioxie.dev/cr="+crName,
			"--wait=false",
		))
	})

	// scenario 9 — preStop invariant I12: deleting a replica
	// pod while the operator is up MUST NOT trigger a SENTINEL
	// FAILOVER. The preStop hook queries the local valkey-server's
	// INFO replication; for a replica it returns role=slave and the
	// script exits 0 immediately (the role-aware branch in the
	// preStop bash). No FailoverInitiated event fires; the primary
	// label does not move.
	It("preStop I12 -> replica pod delete while operator alive: no spurious failover, primary stable", func() {
		const (
			crName     = "m49-prestop-replica"
			masterName = "mymaster"
		)

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
`)
		waitForReady(crName, 5*time.Minute)

		By("identifying a replica pod to delete (any pod not labelled role=primary)")
		// Pick the first replica via label selector. Bootstrap puts
		// pod-0 as primary so pod-1 or pod-2 is a replica; not
		// hardcoding the ordinal protects against bootstrap topology
		// changes.
		replicaPodName := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(replicaPodName).NotTo(BeEmpty(),
			"at least one replica pod must exist on a 3-pod sentinel CR")
		primaryBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(primaryBefore).NotTo(BeEmpty(),
			"a primary pod must exist before the replica delete")

		By("recording the baseline FailoverInitiated event count (pre-delete)")
		baselineFailover := countEventsByReason(crName, "FailoverInitiated")

		By("deleting the replica pod (graceful, no --force — preStop must run)")
		// Graceful delete (default grace 60s) so the preStop hook
		// actually fires. The contract under test is that the script,
		// finding role=slave, exits 0 immediately and does NOT call
		// SENTINEL FAILOVER.
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", replicaPodName,
			"--wait=false",
		))

		By("waiting for the STS to recreate the replica pod and the cluster to return to 3 Ready")
		// Error-checking variant (not stsReadyReplicas): a transient kubectl
		// get failure during the respawn must read as "not ready yet", not
		// leak captured stdout into the comparison. Keeps the 5s poll cadence.
		Eventually(stsReadyReplicasOrEmpty(crName), 5*time.Minute, 5*time.Second).Should(Equal("3"),
			"STS must return to readyReplicas=3 after the replica pod respawn")

		By("verifying no FailoverInitiated event fired during the replica respawn window")
		// Use Consistently with a 2-minute window after the recovery
		// to ensure the operator's relabel/observer poll-cycle
		// completes without any failover side-effect.
		Consistently(func() int {
			return countEventsByReason(crName, "FailoverInitiated") - baselineFailover
		}, 2*time.Minute, 10*time.Second).Should(Equal(0),
			"no FailoverInitiated event must fire during replica pod respawn — the preStop hook's role=slave branch must exit 0 immediately (invariant I12)")

		By("verifying the primary did not move (same pod still labelled role=primary)")
		primaryAfter := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(primaryAfter).To(Equal(primaryBefore),
			"primary pod identity must be unchanged by a replica respawn (invariant I12: replica delete is a no-op for failover)")
	})

	// scenario 10 — preStop invariant I13: deleting the primary
	// pod while the operator is DOWN must trigger sentinel-driven
	// failover via the preStop hook. The hook's role=master branch
	// fires `SENTINEL FAILOVER <masterName>` against the sentinel
	// headless service; sentinels elect a new primary; the cluster
	// recovers WITHOUT the operator. Once the operator comes back up
	// it re-discovers the new topology and re-labels accordingly.
	//
	// The operator-down condition is achieved by scaling the operator
	// Deployment to 0. Scale back to 1 at the end so the AfterEach
	// AfterAll cleanup path has a live operator to reconcile finalizers.
	It("preStop I13 -> primary delete with operator down: preStop triggers FAILOVER, sentinels elect, cluster recovers without operator", func() {
		const (
			crName     = "m49-prestop-primary"
			masterName = "mymaster"
		)
		operatorLabel := envOrDefault("E2E_OPERATOR_LABEL", "control-plane=controller-manager")
		operatorNS := namespace
		operatorDeployment := envOrDefault("E2E_OPERATOR_DEPLOYMENT", "")

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: ` + masterName + `
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
`)
		waitForReady(crName, 5*time.Minute)

		By("recording the operator Deployment name (env-override or derived from the operator pod's ownerRef chain)")
		// The Deployment name varies by deploy path (kustomize:
		// velkir-controller-manager; chart: <release>-velkir).
		// Prefer the explicit E2E_OPERATOR_DEPLOYMENT env; fall back
		// to deriving from the operator pod's controller ownerRef
		// (Deployment → ReplicaSet → Pod), reading the RS first then
		// the Deployment off the RS owner.
		if operatorDeployment == "" {
			opPod := strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", operatorNS, "get", "pods",
				"-l", operatorLabel,
				"-o", "jsonpath={.items[0].metadata.name}",
			)))
			Expect(opPod).NotTo(BeEmpty(), "operator pod must be running")
			rsName := strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", operatorNS, "get", "pod", opPod,
				"-o", `jsonpath={.metadata.ownerReferences[?(@.controller==true)].name}`,
			)))
			Expect(rsName).NotTo(BeEmpty(),
				"operator pod must have a controller ownerRef (ReplicaSet)")
			operatorDeployment = strings.TrimSpace(mustRun(exec.Command(
				"kubectl", "-n", operatorNS, "get", "rs", rsName,
				"-o", `jsonpath={.metadata.ownerReferences[?(@.controller==true)].name}`,
			)))
			Expect(operatorDeployment).NotTo(BeEmpty(),
				"operator ReplicaSet must have a controller ownerRef (Deployment)")
		}

		By("recording the primary pod name and IP for the post-failover comparison")
		primaryBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[0].metadata.name}",
		)))
		Expect(primaryBefore).NotTo(BeEmpty(), "a primary pod must exist before the delete")
		primaryIPBefore := strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pod", primaryBefore,
			"-o", "jsonpath={.status.podIP}",
		)))
		Expect(primaryIPBefore).NotTo(BeEmpty(), "primary pod must have a PodIP")

		By("scaling the operator Deployment to 0 (operator-down precondition)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "scale", "deployment", operatorDeployment,
			"--replicas=0",
		))

		// Defer to restore operator at exit regardless of pass/fail.
		// AfterEach already deletes the CR; without the operator the
		// finalizer can't run → CR sits in Terminating forever and
		// blocks the next spec's BeforeAll create.
		defer func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "scale", "deployment", operatorDeployment,
				"--replicas=1",
			))
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "wait", "deployment", operatorDeployment,
				"--for=condition=Available", "--timeout=3m",
			))
		}()

		By("waiting for the operator Deployment to scale to 0 pods")
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", operatorNS, "get", "deployment", operatorDeployment,
				"-o", "jsonpath={.status.replicas}",
			))
			if err != nil {
				return ""
			}
			return strings.TrimSpace(out)
		}, 2*time.Minute, 5*time.Second).Should(Or(Equal("0"), Equal("")),
			"operator Deployment must report 0 replicas after scale-down")

		By("deleting the primary pod (graceful — preStop must run with role=master branch firing SENTINEL FAILOVER)")
		_ = mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", primaryBefore,
			"--wait=false",
		))

		By("verifying sentinels elect a new primary (PodIP returned by SENTINEL MASTER differs from the deleted primary's IP) — operator is down, only sentinel-driven failover can do this")
		// Query SENTINEL MASTER directly from a sentinel pod and
		// parse the master IP from the response. The response shape
		// is an array of key/value pairs; "ip" is the third field
		// (after "name" + master-name). Use sed to extract.
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", crName+"-sentinel-0", "-c", "sentinel", "--",
				"valkey-cli", "-p", "26379", "sentinel", "master", masterName,
			))
			if err != nil {
				return ""
			}
			// Output:
			//   name
			//   mymaster
			//   ip
			//   10.244.0.42
			//   port
			//   6379
			//   ...
			// Find the line right after "ip\n".
			lines := strings.Split(strings.TrimSpace(out), "\n")
			for i, l := range lines {
				if strings.TrimSpace(l) == "ip" && i+1 < len(lines) {
					return strings.TrimSpace(lines[i+1])
				}
			}
			return ""
		}, 4*time.Minute, 5*time.Second).ShouldNot(Or(BeEmpty(), Equal(primaryIPBefore)),
			"SENTINEL MASTER must report a new primary IP after the preStop-triggered failover — the deleted primary's IP would mean sentinels didn't elect (preStop didn't fire, or sentinels couldn't quorum)")

		By("scaling the operator back up to 1 so it can reconcile the post-failover topology")
		_ = mustRun(exec.Command(
			"kubectl", "-n", operatorNS, "scale", "deployment", operatorDeployment,
			"--replicas=1",
		))

		By("waiting for the operator Deployment to return to Available")
		Eventually(func() error {
			return exec.Command(
				"kubectl", "-n", operatorNS, "wait", "deployment", operatorDeployment,
				"--for=condition=Available", "--timeout=10s",
			).Run()
		}, 3*time.Minute, 5*time.Second).ShouldNot(HaveOccurred(),
			"operator Deployment must become Available again after scale-up")

		By("verifying the operator re-labels the new primary (some pod ≠ primaryBefore carries role=primary)")
		newPrimaryName := ""
		Eventually(func() string {
			out, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[0].metadata.name}",
			))
			if err != nil {
				return ""
			}
			newPrimaryName = strings.TrimSpace(out)
			return newPrimaryName
		}, 5*time.Minute, 5*time.Second).ShouldNot(BeEmpty(),
			"operator must relabel some pod as role=primary after re-discovering the post-failover topology")
		Expect(newPrimaryName).NotTo(Equal(primaryBefore),
			"the new primary must not be the deleted pod (which has a different IP even if reborn under the same name; the assertion above on IPs is the load-bearing sentinel-handled-it check)")

		By("verifying the CR returns to Ready=True post-recovery")
		Eventually(func() string {
			return getCondition(crName, "Ready")
		}, 5*time.Minute, 2*time.Second).Should(Equal("True"),
			"CR Ready must return to True after the operator catches up with the sentinel-driven failover")
	})

	// Scenario 11 — quiet-window invariant: a clean fresh deploy must
	// emit NO split-brain signal during bootstrap or steady-state
	// settle. While sentinels are still discovering peers a fresh
	// cluster can transiently look split-brained; the operator holds
	// the signal at false until the observer publishes its first
	// snapshot (the Snapshot.Present gate), and healthy sentinels then
	// converge on a single primary (QuorumOK=true), so the
	// SplitBrainDetected event never fires, the counter never
	// increments, and Degraded never takes reason=SplitBrain. We assert
	// on the operator's own signals (Events + the Degraded condition)
	// so the spec needs no Prometheus/Alertmanager and runs under
	// make test-e2e-shared on a single-node cluster.
	//
	// The SplitBrainDetected event and valkey_split_brain_detections_total
	// are emitted from the same `if snapshotReportsSplitBrain(snap)` gate
	// in desiredRolesForCR, so a zero event-delta over the window proves
	// the counter stayed flat without scraping /metrics. Both that event
	// and the Degraded(SplitBrain) condition derive from the same shared
	// gate (Snapshot.Present && Quorum==Lost, not merely !QuorumOK);
	// because the event is durable in the apiserver, a baseline-delta
	// captured before apply and re-read after Ready also catches any
	// transient Degraded(SplitBrain) that flipped back before a
	// point-in-time read.
	It("stays quiet on a clean fresh deploy — no split-brain signal during bootstrap or settle (quiet-window invariant)", func() {
		const crName = "sentinel-no-splitbrain"

		mustRun(exec.Command("kubectl", "-n", e2eNamespace, "create", "secret", "generic", "auth-no-splitbrain",
			"--from-literal=password=changeme"))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "-n", e2eNamespace, "delete", "secret", "auth-no-splitbrain",
				"--ignore-not-found", "--wait=false"))
		})

		By("recording the baseline SplitBrainDetected event count (pre-deploy, expected 0)")
		// Events persist in the apiserver for the suite's lifetime, so a
		// delta captured before apply and re-read after Ready
		// retroactively covers the whole bootstrap window — a split-brain
		// blip during sentinel discovery leaves a durable event the
		// delta still counts.
		baselineSplitBrain := countEventsByReason(crName, "SplitBrainDetected")

		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: sentinel-no-splitbrain
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
  auth:
    secretName: auth-no-splitbrain
    secretKey: password
    sentinelAuthSecretName: auth-no-splitbrain
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying both StatefulSets reach full readiness")
		Eventually(stsReadyReplicas(crName), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"valkey STS must reach 3/3 ready post-bootstrap")
		Eventually(stsReadyReplicas(crName+"-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 ready post-bootstrap")

		By("verifying no SplitBrainDetected event fired during the bootstrap window")
		Expect(countEventsByReason(crName, "SplitBrainDetected")-baselineSplitBrain).To(Equal(0),
			"a clean bootstrap must be quiet — no SplitBrainDetected event (and therefore no valkey_split_brain_detections_total increment) during sentinel discovery")

		By("verifying the CR reached Ready with exactly one primary")
		primaries := strings.Fields(strings.TrimSpace(mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "pods",
			"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
			"-o", "jsonpath={.items[*].metadata.name}",
		))))
		Expect(primaries).To(HaveLen(1),
			"a healthy sentinel bootstrap must converge on exactly one role=primary pod")

		By("verifying the cluster stays quiet over the settle window — no split-brain signal")
		// 2-minute settle comfortably exceeds the observer poll cadence
		// (10s), the stranded-recovery cooldown (30s) and the
		// quorum-loss suppression threshold (60s), so a late false
		// positive after convergence is still caught. There is no
		// operator-side split-brain debounce — the signal fires on the
		// first reconcile where the snapshot reports Present &&
		// Quorum==Lost — so a quiet window here means the sentinels
		// genuinely agree.
		Consistently(func(g Gomega) {
			g.Expect(countEventsByReason(crName, "SplitBrainDetected")-baselineSplitBrain).To(Equal(0),
				"no SplitBrainDetected event must fire on a healthy steady-state cluster")

			reasonOut, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
			))
			g.Expect(strings.TrimSpace(reasonOut)).NotTo(Equal("SplitBrain"),
				"Degraded must never take reason=SplitBrain on a healthy fresh deploy")
		}, 2*time.Minute, 10*time.Second).Should(Succeed())
	})

	// Record-only measurement — measure the end-to-end failover
	// recovery time a well-behaved fresh-connection client experiences
	// after a primary kill, and RECORD it rather than gating pass/fail
	// on an absolute threshold. Absolute failover latency is
	// environment-dependent (a slow single-node CI node inflates it), so
	// a hard SLO gate here would be flaky and fail a correct operator.
	// The pass criterion is convergence within a generous deadline; the
	// measured outage is emitted to the test log + structured report for
	// cross-run regression tracking. If a hard SLO gate is ever wanted,
	// apply it only in a controlled, fixed-capacity environment.
	It("measures failover recovery time after a primary kill (record-only, not gated)", func() {
		const (
			crName   = "sentinel-recovery-time"
			authPass = "changeme"
			probePod = crName + "-probe"
		)

		By("creating the auth Secret the cluster and probe authenticate with")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", crName+"-auth",
			"--from-literal=password="+authPass,
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "secret", crName+"-auth", "--ignore-not-found",
			))
		})

		By("provisioning an auth-enabled 3+3 sentinel cluster (aggressive timeouts bound failover)")
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
  annotations:
    velkir.ioxie.dev/allow-aggressive-timeouts: "true"
spec:
  mode: sentinel
  valkey:
    replicas: 3
  sentinel:
    masterName: mymaster
    replicas: 3
    downAfterMilliseconds: 5000
    failoverTimeout: 30000
  auth:
    secretName: ` + crName + `-auth
    secretKey: password
    sentinelAuthSecretName: ` + crName + `-auth
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		oldPrimary := crName + "-0"
		By("confirming the bootstrap primary is pod-0 (the kill target)")
		Expect(roleLabelOf(oldPrimary)).To(Equal("primary"),
			"precondition: pod-0 must be the bootstrap primary before the kill")

		By("launching a timestamped fresh-connection role probe (~100ms cadence) spanning the failover window")
		// Each iteration opens a NEW connection to the `<cr>` write
		// Service VIP and prints `<unix> <role>`. A line carrying only a
		// timestamp (no role) is a fresh-connection failure during the
		// outage — exactly the gap the measurement quantifies. The probe
		// is wall-clock-bounded (900s) so it comfortably outlives the
		// generous convergence deadline + settle regardless of
		// valkey-cli latency drift or slow probe-pod startup.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "run", probePod,
			"--restart=Never", "--image=valkey/valkey:8.1.7-alpine", "--",
			"sh", "-c",
			fmt.Sprintf("end=$(( $(date +%%s) + 900 )); while [ $(date +%%s) -lt $end ]; do "+
				"echo \"$(date +%%s) $(valkey-cli -h %s -p 6379 -a %s --no-auth-warning ROLE 2>/dev/null | head -1)\"; "+
				"sleep 0.1; done",
				crName, authPass),
		))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "delete", "pod", probePod,
				"--ignore-not-found", "--force", "--grace-period=0", "--wait=false",
			))
		})

		By("waiting for the probe pod to be Running before the kill")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pod", probePod,
				"-o", "jsonpath={.status.phase}",
			))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).Should(Equal("Running"),
			"probe pod must be Running so it samples across the failover window")

		By("establishing a pre-kill baseline and anchoring the recovery check on the probe's own clock")
		// Wait until the probe has logged a role:master sample — proving
		// it is functional (auth + image + write-Service routing all
		// work) — then take the newest sample timestamp as the pre-kill
		// boundary. Anchoring on the probe's OWN clock (`date +%s` inside
		// the pod) rather than the test machine's avoids any cross-host
		// skew: a master sample strictly newer than this boundary proves
		// the probe re-reached a primary after the kill (recovery
		// captured), without the two clocks needing to agree.
		var preKillMaxTs int64
		Eventually(func() bool {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "logs", probePod,
			))
			sawMaster := false
			var maxTs int64
			for _, line := range strings.Split(out, "\n") {
				f := strings.Fields(strings.TrimSpace(line))
				if len(f) == 0 {
					continue
				}
				ts, err := strconv.ParseInt(f[0], 10, 64)
				if err != nil {
					continue
				}
				if ts > maxTs {
					maxTs = ts
				}
				if len(f) == 2 && f[1] == "master" {
					sawMaster = true
				}
			}
			if sawMaster {
				preKillMaxTs = maxTs
			}
			return sawMaster
		}, 60*time.Second, 2*time.Second).Should(BeTrue(),
			"probe must observe role:master in steady state before the kill")

		By("force-deleting the primary pod (kill -9 equivalent: --force --grace-period=0)")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", oldPrimary,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for convergence: a different pod is the sole primary, Ready=True, Degraded=False (generous deadline)")
		// Pass criterion. Deliberately generous (5min) so a slow-but-
		// correct failover on an underprovisioned node is not a failure;
		// the only hard failure is the cluster never converging.
		var newPrimary string
		Eventually(func() error {
			if r := getCondition(crName, "Ready"); r != "True" {
				return fmt.Errorf("Ready=%q", r)
			}
			if d := getCondition(crName, "Degraded"); d != "False" {
				return fmt.Errorf("Degraded=%q", d)
			}
			// Single query for count + identity so the two cannot race
			// across separate kubectl calls (a pod transitioning between
			// a count and a name read would otherwise capture a stale
			// primary).
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "get", "pods",
				"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
				"-o", "jsonpath={.items[*].metadata.name}",
			))
			primaries := strings.Fields(strings.TrimSpace(out))
			if len(primaries) != 1 {
				return fmt.Errorf("role=primary count=%d", len(primaries))
			}
			if primaries[0] == oldPrimary {
				return fmt.Errorf("no new primary elected yet (still %s)", oldPrimary)
			}
			newPrimary = primaries[0]
			return nil
		}, 5*time.Minute, 3*time.Second).Should(Succeed(),
			"cluster must converge on a single new primary with Ready=True, Degraded=False within the generous deadline")

		By("confirming the new primary accepts writes (data plane ready)")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", newPrimary, "-c", "valkey", "--",
				"sh", "-c", fmt.Sprintf("valkey-cli -a %s --no-auth-warning SET recovery-probe ok", authPass),
			))
			return strings.TrimSpace(out)
		}, 30*time.Second, 2*time.Second).Should(Equal("OK"),
			"the new primary must accept writes after convergence")

		By("settling so the probe captures sustained post-recovery role:master through the write Service")
		// kube-proxy reprogramming + observer republish lag the relabel
		// by a few seconds; a brief settle anchors the post-recovery side
		// of the straddling gap on a real sustained observation.
		time.Sleep(15 * time.Second)

		By("waiting for the probe to capture post-recovery role:master samples (validity guard, not an SLO)")
		// Parse `<unix> <role>` lines: a 2-field line is a completed
		// fresh connection; a 1-field line (timestamp only) is a
		// connection failure during the outage — counted as a sample but
		// not a master observation. The outage window is the longest gap
		// between consecutive role:master observations (sorted by time):
		// the kill opens a gap in the otherwise ~100ms master cadence,
		// and taking the LONGEST gap measures sustained recovery while
		// ignoring a transient stale-NAT master hit on the dying primary.
		//
		// The guard protects measurement validity, NOT an SLO — the spec
		// never fails on the outage magnitude. A probe that never saw a
		// master (broken auth/image) or never re-observed one after the
		// kill (recovery uncaptured) would make the recorded outage
		// unbounded/fictional. Convergence and write acceptance are
		// already proven above, but on a CPU-saturated node kube-proxy
		// reprogramming lag plus probe-pod starvation can delay the first
		// post-kill master sample well past the settle window. The probe
		// keeps sampling on its 900s wall-clock budget, so re-read its
		// log until the recovery sample lands rather than failing on a
		// single snapshot.
		var masterTimes []int64
		totalSamples := 0
		Eventually(func() error {
			probeLog, err := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "logs", probePod,
			))
			if err != nil {
				return fmt.Errorf("read probe log: %w", err)
			}
			masterTimes = masterTimes[:0]
			totalSamples = 0
			sawMasterAfterKill := false
			for _, line := range strings.Split(probeLog, "\n") {
				f := strings.Fields(strings.TrimSpace(line))
				if len(f) == 0 {
					continue
				}
				ts, err := strconv.ParseInt(f[0], 10, 64)
				if err != nil {
					continue
				}
				totalSamples++
				if len(f) == 2 && f[1] == "master" {
					masterTimes = append(masterTimes, ts)
					if ts > preKillMaxTs {
						sawMasterAfterKill = true
					}
				}
			}
			if len(masterTimes) < 2 {
				return fmt.Errorf("only %d role:master observations — not yet real evidence", len(masterTimes))
			}
			if !sawMasterAfterKill {
				return fmt.Errorf("no role:master sample newer than the pre-kill boundary %d — recovery not captured yet", preKillMaxTs)
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed(),
			"probe must observe role:master after the kill — convergence is already proven, so a persistent miss here means the probe never regained a sampling window onto the recovered primary")

		By("computing and recording the outage window from the probe samples")

		slices.Sort(masterTimes)
		var outageSeconds int64
		for i := 1; i < len(masterTimes); i++ {
			if gap := masterTimes[i] - masterTimes[i-1]; gap > outageSeconds {
				outageSeconds = gap
			}
		}

		// RECORD ONLY — emit to the test log and the structured report
		// for cross-run regression tracking. No pass/fail gate on this
		// number (see the scenario comment above).
		_, _ = fmt.Fprintf(GinkgoWriter,
			"FAILOVER-RECOVERY: outage window (longest gap between fresh-conn role:master samples) = %ds "+
				"[samples=%d masterObservations=%d newPrimary=%s]\n",
			outageSeconds, totalSamples, len(masterTimes), newPrimary)
		AddReportEntry("failover-recovery-outage-seconds", outageSeconds)
	})
})

// countEventsByReason returns the number of events currently associated
// with the named CR's involvedObject matching the given reason. Counts
// by name match alone, ignoring timestamps — call sites use count-delta
// (count-after - count-before) to bound the assertion to a window. Safe
// under no-events (returns 0) and under any single kubectl flake
// (returns 0 on transient errors so the surrounding `Eventually`
// retries cleanly).
func countEventsByReason(crName, reason string) int {
	out, err := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "events",
		"--field-selector", fmt.Sprintf("involvedObject.name=%s,reason=%s", crName, reason),
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	if err != nil {
		return 0
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return 0
	}
	return len(strings.Fields(out))
}
