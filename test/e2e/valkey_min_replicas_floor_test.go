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
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// End-to-end coverage for the operator-owned write-loss floor: the
// mandatory `min-replicas-to-write` / `min-replicas-max-lag` directives
// the renderer prepends onto every replication/sentinel CR. Unit and
// envtest already pin that the defaulter stamps 1/10 and that the
// renderer emits those lines; only a live valkey-server can prove the
// RUNTIME consequence — the primary actually refusing a write with
// NOREPLICAS while its sole replica is down, resuming on resync, and a
// config rollout confining that refusal window to a single replica
// restart.
//
// Replication mode (not sentinel) is used deliberately: it gives a
// single fixed primary (pod-0, no failover) so the ONLY thing that can
// refuse a write is the floor — no election/relabel machinery to confound
// a sole-replica-down scenario. The suite never touches the cluster-scoped
// operator pod, so it is safe under the per-process e2eNamespace in
// parallel shared-cluster runs.
var _ = Describe("Valkey replication - min-replicas write-loss floor", Ordered, func() {

	const crName = "min-replicas-floor"

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label. Ginkgo v2 randomises
		// top-level Describe order, so this may run before any sibling
		// Describe's BeforeAll and cannot assume the namespace exists.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))

		// replicas: 2 -> pods min-replicas-floor-0 (role=primary at
		// bootstrap, stable — no failover in replication mode) and
		// min-replicas-floor-1 (the sole role=replica). The floor is
		// stamped identically for replication and sentinel; replication
		// reproduces the runtime behaviour without failover confounders.
		applyCR(`
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: ` + crName + `
spec:
  mode: replication
  valkey:
    replicas: 2
    persistence:
      size: 1Gi
`)
		waitForReady(crName, 5*time.Minute)
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterAll(func() {
		// Async delete so teardown doesn't block on finalizer settle;
		// the per-process namespace is owned by the shared-cluster harness.
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", crName,
			"--ignore-not-found", "--wait=false",
		))
	})

	// It #1 pins properties (a) and (b): the primary refuses writes with
	// NOREPLICAS once its sole replica is down, and resumes once the
	// recreated replica resyncs within the lag window.
	It("refuses writes with NOREPLICAS while its sole replica is down and resumes on resync", func() {
		By("verifying the rendered ConfigMap carries the mandatory floor directives")
		// The floor lines are prepended by the renderer and are not
		// user-overridable; asserting them here anchors the runtime
		// behaviour that follows to the config the pod actually loaded.
		conf := mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "configmap", crName+"-valkey-conf",
			"-o", `jsonpath={.data.valkey\.conf}`,
		))
		Expect(conf).To(ContainSubstring("min-replicas-to-write 1"))
		Expect(conf).To(ContainSubstring("min-replicas-max-lag 10"))

		By("deriving the primary and its sole replica from role labels")
		primary := primaryPodName(crName)
		Expect(primary).To(Equal(crName+"-0"),
			"replication bootstrap must stamp role=primary on pod-0")
		replica := aReplicaPod(crName)
		Expect(replica).To(Equal(crName+"-1"),
			"the sole replica must be pod-1")
		Expect(roleLabelOf(replica)).To(Equal("replica"))

		By("baseline: the SET succeeds while the replica is up and in-lag")
		// ContainSubstring, not Equal: writeToPrimary returns combined
		// stdout+stderr, and a NOREPLICAS refusal cannot contain "OK".
		// Eventually-wrapped like its NOREPLICAS/resume siblings so a
		// transient exec-channel hiccup on a loaded shared cluster retries
		// rather than hard-failing the Ordered container on infra noise.
		Eventually(func() string { return writeToPrimary(primary, "floor-k", "v0") },
			30*time.Second, 1*time.Second).Should(ContainSubstring("OK"),
			"the SET must succeed while the floor is satisfied")

		By("force-deleting the sole replica so the replication link drops")
		// --force --grace-period=0 closes the replication socket
		// near-instantly, driving connected_slaves to 0.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", replica,
			"--force", "--grace-period=0", "--wait=false",
		))

		By("(a) the primary refuses writes with NOREPLICAS once the replica is down")
		Eventually(func() string { return writeToPrimary(primary, "floor-k", "v1") },
			60*time.Second, 1*time.Second).Should(ContainSubstring("NOREPLICAS"),
			"primary must refuse writes once its sole replica is down")

		By("(b) writes resume once the recreated replica resyncs within the lag window")
		// The StatefulSet recreates pod-1 (a deleted pod is recreated
		// regardless of the update strategy, which governs only update
		// rolls, not replica count); once its ack offset is within the
		// lag window the primary counts it good again.
		Eventually(func() string { return writeToPrimary(primary, "floor-k", "v2") },
			3*time.Minute, 3*time.Second).Should(ContainSubstring("OK"),
			"writes resume once the recreated replica resyncs within min-replicas-max-lag")

		By("restoring clean state for the next spec")
		waitForReady(crName, 5*time.Minute)
	})

	// It #2 pins property (c): across a config bump, the master-aware
	// rollout leaves the primary in place (UID stable, never sustained
	// NotReady), so the write-refusal window is bounded by the single
	// replica restart.
	It("a master-aware config rollout confines the write-refusal window to a single replica restart", func() {
		By("establishing a clean baseline independent of the previous spec")
		waitForReady(crName, 5*time.Minute)
		Eventually(stsReadyReplicas(crName), 2*time.Minute, 5*time.Second).Should(Equal("2"),
			"both valkey pods must be ready before the bump")

		By("recording primary and replica UIDs before the bump")
		primary := primaryPodName(crName)
		Expect(primary).To(Equal(crName + "-0"))
		primaryUIDBefore := podUIDOf(primary)
		Expect(primaryUIDBefore).NotTo(BeEmpty())
		replica := crName + "-1"
		Expect(roleLabelOf(replica)).To(Equal("replica"))
		replicaUIDBefore := podUIDOf(replica)
		Expect(replicaUIDBefore).NotTo(BeEmpty())

		By("starting the readiness sampler BEFORE the bump so it never clips the leading edge")
		stop := make(chan struct{})
		obsCh := sampleValkeyReadiness(crName, primary, stop)
		// Tear the sampler down on EVERY exit path. A Ginkgo Fail panic in
		// any assertion below would otherwise leave the goroutine shelling
		// kubectl every 300ms for the rest of the run, contending with
		// dumpDiagnosticsOnFailure and later Ordered specs. sync.Once makes
		// the stopper idempotent so the happy-path stop (before the receive)
		// and the DeferCleanup safety net can't double-close and panic.
		var stopOnce sync.Once
		stopSampler := func() { stopOnce.Do(func() { close(stop) }) }
		DeferCleanup(stopSampler)

		By("applying a configurationOverrides bump to flip the rendered hash")
		// The bump changes the rendered config hash and rolls non-primary
		// pods one at a time, leaving the primary in place.
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", crName,
			"--type=merge", "-p", `{"spec":{"valkey":{"configurationOverrides":{"maxmemory":"128mb"}}}}`,
		))

		By("waiting for the sole replica to be recreated (UID changes) and the CR to recover")
		Eventually(func() bool { return podUIDOf(replica) != "" && podUIDOf(replica) != replicaUIDBefore },
			5*time.Minute, 5*time.Second).Should(BeTrue(),
			"the sole replica must be recreated (UID changes)")
		waitForReady(crName, 5*time.Minute)
		Eventually(stsReadyReplicas(crName), 2*time.Minute, 5*time.Second).Should(Equal("2"),
			"both valkey pods must be ready again after the roll")

		By("stopping the sampler and reading its aggregate")
		// Stop then receive establishes a clean happens-before: the
		// goroutine's single buffered send carries all its writes to the
		// main goroutine with no shared mutable state. Idempotent via Once,
		// so the DeferCleanup stopper is a no-op after this.
		stopSampler()
		obs := <-obsCh

		By("verifying the master was untouched: primary UID is stable")
		Expect(podUIDOf(primary)).To(Equal(primaryUIDBefore),
			"the master-aware roll must leave the primary in place")

		By("verifying the primary never sustained NotReady across the roll")
		// A genuine primary flip needs three consecutive failed loopback
		// readiness probes (~30s) to set Ready=False and then stays False
		// for dozens of 300ms samples; a lone slow/transient sample —
		// already filtered by skip-on-kubectl-error — cannot exceed 1
		// consecutive. So the ONLY source of write-refusal is the sole
		// replica restart.
		Expect(obs.primaryMaxConsecutiveNotReady).To(BeNumerically("<=", 1),
			"the primary must never sustain NotReady while the sole replica rolls")

		By("verifying the primary did not also go down while the replica was down (defense-in-depth)")
		// At replicas=2 the rollout partitions off the one non-primary
		// pod, so a simultaneous both-down reading (which would be 2)
		// cannot occur unless a regression takes the primary down too.
		Expect(obs.maxSimultaneousNotReady).To(BeNumerically("<=", 1),
			"at most one valkey pod may be not-ready at any sampled instant")

		By("verifying the roll restarted the replica exactly once (single down-window)")
		// The sampler counts rising edges of the replica's not-ready
		// state (debounced, so a lone glitch sample cannot split one
		// window in two). Exactly one window pins BOTH that the sampler
		// positively observed the restart (non-vacuous — it started
		// before the bump and stopped after full recovery) AND that the
		// roll did not re-mark the recreated pod stale and roll it again
		// — a repeated-roll regression extends the write-refusal window
		// across N restarts while every point-in-time assertion above
		// still passes.
		Expect(obs.samples).To(BeNumerically(">", 0),
			"the sampler must have recorded at least one non-skipped tick")
		Expect(obs.replicaDownWindows).To(Equal(1),
			"the sole replica must go through exactly one down-window across the roll")
	})
})

// writeToPrimary runs `valkey-cli set <key> <val>` on the primary pod and
// returns the TRIMMED combined stdout+stderr. It uses CombinedOutput and
// tolerates a non-zero exit because a NOREPLICAS reply is an error reply:
// valkey-cli prints it and exits non-zero, and utils.Run would discard the
// stderr text and fail the spec. Returns "OK" on success or a string
// containing "NOREPLICAS" on refusal.
func writeToPrimary(pod, key, val string) string {
	out, _ := exec.Command(
		"kubectl", "-n", e2eNamespace, "exec", pod, "-c", "valkey", "--",
		"valkey-cli", "-p", "6379", "set", key, val,
	).CombinedOutput()
	return strings.TrimSpace(string(out))
}

// rolloutObservation is the aggregate a single sampler goroutine emits
// once, on its result channel, after stop. All fields are owned by the
// goroutine; the main goroutine reads them only after the channel receive
// (clean happens-before, no shared mutable state — go test -race clean).
type rolloutObservation struct {
	primaryMaxConsecutiveNotReady int // longest run of consecutive samples where the primary was absent-or-NotReady
	maxSimultaneousNotReady       int // max over samples of (2 - readyValkeyPodCount), absent pod counted not-ready
	replicaDownWindows            int // rising edges of the replica's not-ready state (debounced; 1 = single restart)
	samples                       int // non-skipped ticks recorded
}

// replicaDownCloseDebounce is how many consecutive ready samples close a
// replica down-window. Two samples (~600ms) absorb a lone flapping
// ready reading mid-restart, so one physical restart cannot be counted
// as two windows; a genuine second roll holds not-ready for the whole
// pod recreate (many samples) and is always a fresh rising edge.
const replicaDownCloseDebounce = 2

// sampleValkeyReadiness polls pod Ready-conditions until stop is closed,
// then sends exactly one rolloutObservation. primaryPod is the stable
// primary name (pod-0; its UID is stable across the roll). Each ~300ms
// tick lists the CR's valkey pods with a per-pod name+Ready jsonpath. A
// kubectl ERROR skips the tick entirely (a transient get failure is not
// "pod not ready"); an ABSENT replica pod (fewer items listed) IS counted
// not-ready.
func sampleValkeyReadiness(crName, primaryPod string, stop <-chan struct{}) <-chan rolloutObservation {
	result := make(chan rolloutObservation, 1)
	go func() {
		var obs rolloutObservation
		curRun := 0
		replicaDown := false
		replicaReadyRun := 0
		replicaPod := crName + "-1"
		selector := fmt.Sprintf("velkir.ioxie.dev/cr=%s,velkir.ioxie.dev/component=valkey", crName)
		const readyJSONPath = `jsonpath={range .items[*]}{.metadata.name}{"="}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}`

		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				result <- obs
				return
			case <-ticker.C:
				// Raw exec (not utils.Run): a background goroutine must not
				// mutate the process CWD (utils.Run does os.Chdir) or race
				// GinkgoWriter with the main spec goroutine's utils.Run
				// calls — the same reason writeToPrimary avoids utils.Run.
				outBytes, err := exec.Command(
					"kubectl", "-n", e2eNamespace, "get", "pods",
					"-l", selector, "-o", readyJSONPath,
				).Output()
				if err != nil {
					// A transient get failure is not evidence a pod is
					// not ready; skip the tick without touching any
					// counter or the consecutive-run tracker.
					continue
				}
				out := string(outBytes)

				readyByName := map[string]string{}
				listed := 0
				readyCount := 0
				for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					parts := strings.SplitN(line, "=", 2)
					if len(parts) != 2 {
						continue
					}
					name, status := parts[0], parts[1]
					readyByName[name] = status
					listed++
					if status == "True" {
						readyCount++
					}
				}

				obs.samples++
				// An absent pod is a not-ready pod: (2 - listed) absent +
				// (listed - readyCount) listed-but-not-ready.
				notReady := (2 - listed) + (listed - readyCount)
				if notReady > obs.maxSimultaneousNotReady {
					obs.maxSimultaneousNotReady = notReady
				}

				// Primary readiness: listed AND status True. Track the
				// longest consecutive run of not-ready samples.
				if readyByName[primaryPod] == "True" {
					curRun = 0
				} else {
					curRun++
					if curRun > obs.primaryMaxConsecutiveNotReady {
						obs.primaryMaxConsecutiveNotReady = curRun
					}
				}

				// Replica down-windows: keyed on the stable ordinal name so
				// the tracking spans delete+recreate (only its UID changes).
				// Absent or listed-not-True both count as down. Count rising
				// edges into the down state; the window closes only after
				// replicaDownCloseDebounce consecutive ready samples, so a
				// lone glitch reading mid-restart cannot split one restart
				// into two windows.
				if status, ok := readyByName[replicaPod]; !ok || status != "True" {
					replicaReadyRun = 0
					if !replicaDown {
						replicaDown = true
						obs.replicaDownWindows++
					}
				} else if replicaDown {
					replicaReadyRun++
					if replicaReadyRun >= replicaDownCloseDebounce {
						replicaDown = false
						replicaReadyRun = 0
					}
				}
			}
		}
	}()
	return result
}
