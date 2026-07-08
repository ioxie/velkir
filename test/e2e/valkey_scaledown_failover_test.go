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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ioxie/velkir/test/utils"
)

// End-to-end coverage for a user-intent scale-down that arrives while
// a failover is in flight. It pins two correctness properties that the
// independent scale and failover specs never exercise together:
//
//  1. Scale-DOWN propagation — there is otherwise no e2e coverage for
//     shrinking a cluster (only scale-up). A scale-down that silently
//     fails to reconcile (StatefulSet stuck at the old count until an
//     operator restart) would go unnoticed.
//  2. Serialization of spec mutation against recovery — patching
//     `spec.valkey.replicas` during the disruption window must not act
//     on a half-recovered cluster. The operator holds the count change
//     while it is in a rollout state and re-evaluates it once recovery
//     completes, so the StatefulSet never drifts from spec and the
//     PodDisruptionBudget never deadlocks the concurrent shrink.
//
// The post-recovery primary ordinal decides which of two correct
// outcomes applies, so the spec branches rather than asserting a single
// fixed count:
//
//   - primary on ordinal <= 1 → the deferred shrink is applied: the
//     StatefulSet settles at 2 with no operator restart.
//   - primary on ordinal 2 → shrinking to 2 would delete the primary's
//     own pod, so the operator refuses the change (ScaleRefused) and
//     holds the StatefulSet at 3.
//
// Sentinel elects the new primary nondeterministically, so a spec that
// unconditionally demanded `readyReplicas == 2` would be flaky on the
// ~50% of runs that elect the ordinal-2 pod. Both branches assert the
// same underlying invariant (the mid-recovery patch is serialized, then
// honoured-or-refused per the structural rule), and all assertions are
// on settled state rather than on catching the in-flight instant.
var _ = Describe("Valkey sentinel — scale-down during failover", Ordered, func() {

	BeforeAll(func() {
		// Idempotent ns create + e2e-target label. Ginkgo v2 randomises
		// top-level Describe order, so this may run before the other
		// sentinel Describes' BeforeAll fires.
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", e2eNamespace))
		_, _ = utils.Run(exec.Command(
			"kubectl", "label", "--overwrite", "ns", e2eNamespace,
			"velkir.ioxie.dev/e2e-target=true",
		))
	})

	JustAfterEach(dumpDiagnosticsOnFailure)

	AfterEach(func() {
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "valkeys.velkir.ioxie.dev", "--all",
			"--ignore-not-found", "--grace-period=0", "--force", "--wait=false",
		))
		_, _ = utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "secret", "sdf-auth",
			"--ignore-not-found", "--wait=false",
		))
	})

	It("scale-down (3->2) patched mid-failover is serialized against recovery; no spec/STS drift or PDB deadlock", func() {
		const crName = "sdf-failover"

		By("creating the auth secret")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "create", "secret", "generic", "sdf-auth",
			"--from-literal=password=changeme",
		))

		By("applying a 3 valkey + 3 sentinel CR with auth and aggressive failover timing")
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
    secretName: sdf-auth
    secretKey: password
    sentinelAuthSecretName: sdf-auth
    sentinelAuthSecretKey: password
`)
		waitForReady(crName, 5*time.Minute)

		By("verifying both StatefulSets reach full readiness (3/3)")
		Eventually(stsReadyReplicas(crName), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"valkey STS must reach 3/3 ready before the kill")
		Eventually(stsReadyReplicas(crName+"-sentinel"), 5*time.Minute, 10*time.Second).Should(Equal("3"),
			"sentinel STS must reach 3/3 ready before the kill")

		By("verifying pod-0 is the bootstrap primary before the kill (precondition)")
		// Guarantees the kill below actually removes the primary and
		// forces a real election; the bootstrap topology holds until the
		// first failover.
		Expect(roleLabelOf(crName+"-0")).To(Equal("primary"),
			"pod-0 must be the bootstrap primary before the kill")
		pod0UID := podUIDOf(crName + "-0")
		Expect(pod0UID).NotTo(BeEmpty())

		By("force-deleting the primary pod to trigger a failover")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "delete", "pod", crName+"-0",
			"--force", "--grace-period=0", "--wait=false",
		))

		By("waiting for the kill to register (readyReplicas drops to 2) so the patch lands during recovery")
		// Gate on the observable disruption rather than a fixed sleep: a
		// freshly-deleted primary drops the valkey StatefulSet to 2/3
		// ready, which puts the operator in a rollout state. Patching at
		// this point forces the scale-down to be serialized behind the
		// in-flight recovery instead of acting on a half-recovered cluster.
		Eventually(stsReadyReplicas(crName), 60*time.Second, time.Second).Should(Equal("2"),
			"the force-deleted primary must drop the valkey STS to 2/3 ready, confirming recovery is in flight")

		// Baseline the ScaleRefused count before introducing the scale-down
		// intent. countEventsByReason matches by CR name only (not UID or
		// timestamp), so on a shared cluster a refusal left by a prior run
		// of this fixed CR name can still be within the Event TTL; the
		// ord==2 branch asserts the delta this run produces, not an
		// absolute count.
		baselineRefused := countEventsByReason(crName, "ScaleRefused")

		By("patching spec.valkey.replicas 3->2 while recovery is in flight")
		mustRun(exec.Command(
			"kubectl", "-n", e2eNamespace, "patch", "valkeys.velkir.ioxie.dev", crName,
			"--type=merge", "-p", `{"spec":{"valkey":{"replicas":2}}}`,
		))

		By("verifying the spec carries the patched replica count")
		Eventually(specValkeyReplicas(crName), 30*time.Second, 2*time.Second).Should(Equal("2"),
			"the patch must land on spec.valkey.replicas regardless of when it is applied to the StatefulSet")

		By("waiting for a surviving peer to be relabeled as the new primary")
		// Capture inside the poll so the peer check below reads the last
		// non-empty value, not a separate re-read that could catch a
		// sub-second label gap and see "".
		var electedPrimary string
		Eventually(func() string {
			electedPrimary = primaryPodName(crName)
			return electedPrimary
		}, 120*time.Second, 2*time.Second).ShouldNot(BeEmpty(),
			"a surviving pod must be relabeled role=primary within the failover window")
		Expect(podUIDOf(electedPrimary)).NotTo(Equal(pod0UID),
			"the new primary must be a peer, not the just-killed pod")

		By("waiting for the cluster to re-settle (Ready=True implies the shrink did not deadlock the PDB)")
		waitForReady(crName, 5*time.Minute)

		By("verifying Degraded did not latch on SplitBrain and is not stuck True")
		Eventually(degradedReason(crName), 60*time.Second, 2*time.Second).ShouldNot(Equal("SplitBrain"),
			"a natural failover must leave Degraded=SplitBrain cleared once the observer resettles")
		Eventually(func() string { return getCondition(crName, "Degraded") }, 60*time.Second, 2*time.Second).ShouldNot(Equal("True"),
			"Degraded must not be stuck True at settle")

		// The outcome is legitimately bistable: it depends on which
		// ordinal the election (and any post-recovery label correction)
		// finally lands the primary on. Branching on a single label
		// snapshot races that settling — the label can move within
		// seconds of Ready=True while the quorum view converges. Wait
		// for the system to reach ONE of the two consistent end states
		// instead:
		//   propagate: primary at ordinal <=1, STS shrunk to 2
		//   refuse:    primary at ordinal 2, fresh ScaleRefused, STS held at 3
		By("waiting for one consistent scale-down outcome: propagate (STS=2, primary <=1) or refuse (ScaleRefused, STS=3, primary ==2)")
		var outcome string
		Eventually(func() error {
			primary := primaryPodName(crName)
			if primary == "" {
				return fmt.Errorf("no labeled primary yet")
			}
			ord := podOrdinal(primary)
			if ord < 0 {
				return fmt.Errorf("primary %q has no parseable ordinal", primary)
			}
			ready := stsReadyReplicas(crName)()
			refusedDelta := countEventsByReason(crName, "ScaleRefused") - baselineRefused
			switch {
			case ord <= 1 && ready == "2":
				outcome = "propagate"
				return nil
			case ord == 2 && refusedDelta >= 1 && ready == "3":
				outcome = "refuse"
				return nil
			}
			return fmt.Errorf("not settled: primary=%s ready=%s scaleRefusedΔ=%d", primary, ready, refusedDelta)
		}, 5*time.Minute, 5*time.Second).Should(Succeed(),
			"the cluster must settle into either the propagated scale-down (STS=2, primary ordinal <=1) or the refused one (ScaleRefused event, STS=3, primary ordinal 2)")
		By("scale-down outcome: " + outcome)
		if outcome == "refuse" {
			Consistently(stsReadyReplicas(crName), 30*time.Second, 5*time.Second).Should(Equal("3"),
				"a refused scale-down must hold the StatefulSet at its current count")
		}

		By("verifying exactly one pod is labeled primary at settle")
		Expect(primaryPodCount(crName)).To(Equal(1),
			"exactly one pod must carry role=primary after the dust settles")

		By("verifying the new primary accepts writes (data plane healthy post-settle)")
		Eventually(func() string {
			out, _ := utils.Run(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", primaryPodName(crName), "-c", "valkey", "--",
				"valkey-cli", "-p", "6379", "set", "sdf-postsettle", "ok",
			))
			return strings.TrimSpace(out)
		}, 60*time.Second, 2*time.Second).Should(Equal("OK"),
			"the settled primary must accept writes")

		By("verifying the <cr> write Service converges to exactly the primary endpoint")
		primaryIP := podIPOf(primaryPodName(crName))
		Expect(primaryIP).NotTo(BeEmpty())
		Eventually(func() []string { return endpointIPs(crName) }, 60*time.Second, 2*time.Second).Should(
			ConsistOf(primaryIP),
			"the <cr> write Service must route only to the primary pod")

		By("verifying no replica IP ever enters the <cr> write Service endpoints across a quiet window")
		// The write Service endpoints are exactly what new client
		// connections are routed to. If a replica were even transiently
		// labeled primary during recovery, its IP would surface here and a
		// fresh write would land on a read-only backend. Capture the
		// replica IPs at settle, then assert none appears across the quiet
		// window. An empty read is not a violation of this correctness
		// invariant (nothing is routed to a replica), so the assertion
		// reports the first offending replica IP rather than demanding a
		// fixed endpoint set.
		replicas := replicaPodIPs(crName)
		Expect(replicas).NotTo(BeEmpty(), "at least one replica must exist after a 3->2 (or refused) settle")
		Consistently(func() string {
			for _, ip := range endpointIPs(crName) {
				if replicas[ip] {
					return ip
				}
			}
			return ""
		}, 15*time.Second, 1*time.Second).Should(BeEmpty(),
			"no replica IP may appear in the <cr> write Service endpoints")

		By("verifying fresh connections through the <cr> write Service report role:master")
		// Each exec opens a new connection from a replica pod to the write
		// Service VIP, so the connection traverses kube-proxy's routing
		// rather than looping back on the primary. Every fresh connection
		// must reach the primary. REDISCLI_AUTH is set on the valkey
		// container, so valkey-cli authenticates without an explicit -a.
		replicaPod := aReplicaPod(crName)
		Expect(replicaPod).NotTo(BeEmpty())
		for i := 0; i < 10; i++ {
			out := mustRun(exec.Command(
				"kubectl", "-n", e2eNamespace, "exec", replicaPod, "-c", "valkey", "--",
				"valkey-cli", "-h", crName, "-p", "6379", "info", "replication",
			))
			Expect(out).To(ContainSubstring("role:master"),
				"fresh connection #%d through the <cr> write Service must reach the primary", i)
			Expect(out).NotTo(ContainSubstring("role:slave"),
				"fresh connection #%d through the <cr> write Service must never reach a read-only replica", i)
		}
	})
})

// stsReadyReplicas returns a thunk reading a StatefulSet's
// .status.readyReplicas, for use inside Eventually/Consistently.
// Swallow variant: a kubectl error yields TrimSpace(stdout) (typically
// "" on NotFound). Use stsReadyReplicasOrEmpty when the probe must treat
// any kubectl error as "not ready yet" regardless of captured stdout.
func stsReadyReplicas(name string) func() string {
	return func() string {
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "sts", name,
			"-o", "jsonpath={.status.readyReplicas}",
		))
		return strings.TrimSpace(out)
	}
}

// stsReadyReplicasOrEmpty is the error-checking counterpart of
// stsReadyReplicas: a non-nil kubectl error yields "" (not the captured
// stdout), so a transient get failure during a pod respawn reads as "not
// ready yet" rather than letting partial stdout leak into the comparison.
func stsReadyReplicasOrEmpty(name string) func() string {
	return func() string {
		out, err := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "sts", name,
			"-o", "jsonpath={.status.readyReplicas}",
		))
		if err != nil {
			return ""
		}
		return strings.TrimSpace(out)
	}
}

// specValkeyReplicas returns a thunk reading the CR's
// .spec.valkey.replicas.
func specValkeyReplicas(crName string) func() string {
	return func() string {
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
			"-o", "jsonpath={.spec.valkey.replicas}",
		))
		return strings.TrimSpace(out)
	}
}

// degradedReason returns a thunk reading the CR's Degraded condition
// reason.
func degradedReason(crName string) func() string {
	return func() string {
		out, _ := utils.Run(exec.Command(
			"kubectl", "-n", e2eNamespace, "get", "valkeys.velkir.ioxie.dev", crName,
			"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`,
		))
		return strings.TrimSpace(out)
	}
}

// primaryPodName returns the name of the (first) pod labeled
// role=primary for the CR, or "" if none.
func primaryPodName(crName string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
		"-o", "jsonpath={.items[0].metadata.name}",
	))
	return strings.TrimSpace(out)
}

// primaryPodCount returns how many pods carry role=primary for the CR.
func primaryPodCount(crName string) int {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=primary",
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	return len(strings.Fields(strings.TrimSpace(out)))
}

// aReplicaPod returns the name of any pod labeled role=replica for the
// CR, or "" if none.
func aReplicaPod(crName string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
		"-o", "jsonpath={.items[0].metadata.name}",
	))
	return strings.TrimSpace(out)
}

// roleLabelOf reads a single pod's role label.
func roleLabelOf(podName string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pod", podName,
		"-o", `jsonpath={.metadata.labels.velkir\.ioxie\.dev/role}`,
	))
	return strings.TrimSpace(out)
}

// podUIDOf reads a pod's metadata.uid.
func podUIDOf(podName string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pod", podName,
		"-o", "jsonpath={.metadata.uid}",
	))
	return strings.TrimSpace(out)
}

// podIPOf reads a pod's status.podIP.
func podIPOf(podName string) string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pod", podName,
		"-o", "jsonpath={.status.podIP}",
	))
	return strings.TrimSpace(out)
}

// endpointIPs returns the ready endpoint IPs of a Service (legacy
// Endpoints object, which the rest of the suite reads too).
func endpointIPs(svc string) []string {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "endpoints", svc,
		"-o", "jsonpath={.subsets[*].addresses[*].ip}",
	))
	return strings.Fields(strings.TrimSpace(out))
}

// replicaPodIPs returns the set of pod IPs labeled role=replica for the
// CR.
func replicaPodIPs(crName string) map[string]bool {
	out, _ := utils.Run(exec.Command(
		"kubectl", "-n", e2eNamespace, "get", "pods",
		"-l", "velkir.ioxie.dev/cr="+crName+",velkir.ioxie.dev/role=replica",
		"-o", "jsonpath={range .items[*]}{.status.podIP}{\"\\n\"}{end}",
	))
	set := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if ip := strings.TrimSpace(line); ip != "" {
			set[ip] = true
		}
	}
	return set
}

// podOrdinal parses the trailing ordinal off a StatefulSet pod name
// (e.g. "sdf-failover-2" -> 2). Returns -1 when unparseable.
func podOrdinal(podName string) int {
	i := strings.LastIndex(podName, "-")
	if i < 0 {
		return -1
	}
	n, err := strconv.Atoi(podName[i+1:])
	if err != nil {
		return -1
	}
	return n
}
