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

// Package metrics: exporter_watcher.go
//
// Operator-side maintenance of the `valkey_exporter_sidecar_up` gauge
// declared in metrics.go. The exporter doesn't emit this gauge itself —
// the operator does, based on its own reading of pod status.
//
// Gauge contract:
//
//   ExporterSidecarUp == 1 iff
//     the pod's spec.containers[name="exporter"] exists  AND
//     the pod's status.conditions[type=Ready] is True
//
//   ExporterSidecarUp == 0 otherwise — sidecar absent, pod not Ready,
//   pod terminating, or pod observed but not yet seen by the watcher.
//
// Pod-status-based readiness rather than a real HTTP scrape: the
// operator-side scrape would need its own dial-and-probe path with
// timeout / retry semantics and a separate failure-domain story; the
// kubelet's Ready gate already tracks the exporter container's own
// readiness probe, so reading it from pod status carries the same
// signal at no extra cost. A real HTTP probe is a viable extension
// (would tighten the contract from "kubelet says Ready" to "operator
// just talked to the exporter"); deferred.
//
// Why operator-side, not exporter-emitted: a pod whose exporter is
// crashlooping wouldn't be able to emit a "I'm down" metric. The
// gauge needs an external observer that's *NOT* on the same pod as
// the thing it's reporting on. The operator runs in its own
// Deployment, watches every valkey pod, and writes the gauge from
// outside the failure domain.
//
// Why polling, not Pod-watch reconciler: this gauge is a periodic
// probe, not an event-driven update. A Pod-watcher would fire a
// reconcile on every status update of every pod the operator owns
// — many of which don't change exporter readiness. The 30s polling
// loop is below the 60s default Prometheus scrape interval, so the
// gauge is fresh by the time Prometheus reads it. Avoids coupling to
// the Valkey reconciler's Phase 7 (which has its own Pod watch for
// role-label maintenance).

package metrics

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// exporterContainerName is the well-known container name that the
// chart's pod template stamps on the exporter sidecar. Hard-coded
// here rather than threaded through config — every chart-rendered
// valkey pod uses this exact name; a chart edit that renames the
// container is a breaking change.
const exporterContainerName = "exporter"

// managedByLabelKey / managedByLabelValue are the operator-stamped
// label every pod we want to scan carries. Used as the scoping
// option on the cache list call — typed client.MatchingLabels{}
// rather than a parsed string selector keeps the project's
// no_unbounded_client_call analyzer happy and lets the cache
// indexer short-circuit on the label.
const (
	managedByLabelKey   = "app.kubernetes.io/managed-by"
	managedByLabelValue = "velkir"
)

// podKey is the identity of a stamped series: every gauge sample
// belongs to exactly one (namespace, cr-name, pod-name) triple.
type podKey struct {
	namespace string
	crName    string
	podName   string
}

// ExporterWatcher implements manager.Runnable. Each run iteration
// lists every operator-owned pod, classifies its exporter readiness,
// and stamps the per-pod ExporterSidecarUp gauge. Runs leader-only —
// standby replicas would race the leader and produce flapping series.
//
// Cross-scan tracking: lastStamped holds the keys we set in the
// previous scan, so this scan can evict any series whose pod has
// disappeared. Without this, a CR-deletion or pod-eviction leaves
// the gauge series with its last observed value forever (cardinality
// bomb on long-lived clusters).
type ExporterWatcher struct {
	Client client.Client
	// Interval is the time between full pod-list passes. Default
	// 30 * time.Second when zero. Must stay below the chart's
	// default Prometheus scrape interval (60s) so the gauge is
	// always fresh by the time a scrape reads it.
	Interval time.Duration
	Log      logr.Logger

	// lastStamped is the set of series this watcher set on the
	// previous scan. Single-goroutine state — Start runs the
	// scan loop sequentially and is the only writer.
	lastStamped map[podKey]struct{}
}

// NeedLeaderElection makes the manager skip Start on standby
// replicas. The gauge has a single canonical owner per cluster.
func (w *ExporterWatcher) NeedLeaderElection() bool { return true }

// Start blocks until ctx is cancelled. Runs an initial pass
// immediately so the gauge is non-empty by the time the first
// Prometheus scrape arrives, then loops on the configured interval.
// Errors are logged but never abort the loop — a transient cache
// miss shouldn't take down the gauge.
func (w *ExporterWatcher) Start(ctx context.Context) error {
	w.applyDefaults()

	// First pass before sleep — populates gauges immediately on
	// operator-startup rather than after the first interval delay.
	w.scanOnce(ctx)

	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.scanOnce(ctx)
		}
	}
}

func (w *ExporterWatcher) applyDefaults() {
	if w.Interval == 0 {
		w.Interval = 30 * time.Second
	}
}

// scanOnce lists every operator-managed pod and stamps the gauge.
// Cache-only read (controller-runtime's client.List uses the cached
// indexer when available); list latency is bounded by the local
// in-memory cache, not the apiserver.
//
// At the end of the pass, evicts any series whose pod was stamped
// last pass but didn't appear this pass — covers pod deletion,
// CR deletion, namespace deletion, and the operator losing its
// owner-label on a pod (the cache stops serving the row, so we
// drop the series).
//
// Failure handling: on a pod-list error we keep `lastStamped` and
// skip the eviction sweep. Otherwise a transient apiserver outage
// would clear every series and re-emit them on the next pass —
// alerting downstream sees a fake "exporter went away" event.
func (w *ExporterWatcher) scanOnce(ctx context.Context) {
	var pods corev1.PodList
	if err := w.Client.List(ctx, &pods, client.MatchingLabels{managedByLabelKey: managedByLabelValue}); err != nil {
		w.Log.V(1).Info("exporter watcher: pod list failed; gauge stamps deferred to next pass",
			"err", err.Error())
		return
	}

	seen := make(map[podKey]struct{}, len(pods.Items))
	for i := range pods.Items {
		if k, ok := w.stampPod(&pods.Items[i]); ok {
			seen[k] = struct{}{}
		}
	}

	for k := range w.lastStamped {
		if _, still := seen[k]; still {
			continue
		}
		ExporterSidecarUp.DeletePartialMatch(prometheus.Labels{
			"namespace": k.namespace,
			"name":      k.crName,
			"pod":       k.podName,
		})
	}
	w.lastStamped = seen
}

// stampPod evaluates the exporter readiness for a single pod and
// sets the corresponding gauge. The valkey CR name is read from
// the pod's `app.kubernetes.io/instance` label (chart-set) so the
// watcher doesn't need to look up owner references. Returns the
// key that was stamped, or ok=false when the pod was skipped
// (so scanOnce can keep the bookkeeping accurate).
func (w *ExporterWatcher) stampPod(pod *corev1.Pod) (podKey, bool) {
	crName := pod.Labels["app.kubernetes.io/instance"]
	if crName == "" {
		// Pod claims to be operator-managed but doesn't carry the
		// instance label — chart misconfiguration or a hand-applied
		// pod. Skip rather than emit a series with empty `name`.
		return podKey{}, false
	}

	value := 0.0
	if hasExporterContainer(pod) && podIsReady(pod) {
		value = 1.0
	}
	ExporterSidecarUp.WithLabelValues(pod.Namespace, crName, pod.Name).Set(value)
	return podKey{namespace: pod.Namespace, crName: crName, podName: pod.Name}, true
}

// hasExporterContainer reports whether the pod template includes a
// container named "exporter". A pod whose CR has
// `spec.metrics.enabled=false` won't have the sidecar; the gauge
// reports 0 for those pods (correct — there's nothing to scrape).
func hasExporterContainer(pod *corev1.Pod) bool {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == exporterContainerName {
			return true
		}
	}
	return false
}

// podIsReady reads the standard "Ready" condition. Pods in the
// Pending phase don't have the condition yet → not ready.
// Terminating pods have Ready=False → not ready.
func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
