# Monitoring samples

Copy-paste scrape configs and alerts for the velkir and the
managed Valkey/sentinel data-plane. Apply them yourself — the operator
chart does **not** render `PodMonitor` / `ServiceMonitor` / `PrometheusRule`
by default, and the operator has **no hard dependency** on the
`monitoring.coreos.com` CRDs (the `PodMonitor` / `ServiceMonitor` /
`PrometheusRule` kinds): a cluster without those CRDs installed still
installs and runs the chart cleanly.

## What's here

- **`operator-podmonitor.yaml`** — scrapes the operator's own controller
  metrics. The operator endpoint is HTTPS + bearer-token authenticated,
  so this sample carries the `scheme: https`, `tlsConfig`, and
  `bearerTokenSecret` the secure endpoint requires.
- **`valkey-podmonitor.yaml`** — scrapes the `redis_exporter` sidecar on
  managed Valkey data-plane pods (plain HTTP `:9121`).
- **`sentinel-podmonitor.yaml`** — scrapes the exporter on managed
  sentinel pods. The only sample that covers the sentinel plane.
- **`servicemonitor.yaml`** — a `ServiceMonitor` equivalent (plus a
  companion headless metrics Service) for clusters whose Prometheus
  discovers ServiceMonitors but not PodMonitors.
- **`prometheusrule.yaml`** — the alert pack (data-plane, sentinel, and
  operator alerts). Consumes the labels the monitors above stamp.

## Prerequisites

The data-plane and sentinel monitors scrape the `redis_exporter` sidecar,
which is added to a pod only when the CR sets `spec.metrics.enabled=true`.
The sidecar exposes its `/metrics` on a container port named `metrics`
(`:9121`) — the monitors select that port by name, so they keep working
across any future port renumber.

The alert pack also assumes `kube-state-metrics` is running (the
`ValkeyReplicaDown` alert correlates pod readiness against the operator's
pod labels).

## Label conventions

The monitors lift the operator-stamped pod labels onto the scraped series
so the alert pack can group and filter:

| Series label | Source pod label | Meaning |
|---|---|---|
| `valkey_instance` | `app.kubernetes.io/instance` | CR name |
| `namespace` | (scrape namespace) | CR namespace |
| `valkey_role` | `velkir.ioxie.dev/role` | `primary` / `replica` (data-plane); stamped `sentinel` for sentinel pods |

The selectors key off `app.kubernetes.io/managed-by=velkir` plus
`velkir.ioxie.dev/component` (`valkey` or `sentinel`).

## PodMonitor vs ServiceMonitor

`PodMonitor` is the canonical path: it selects managed pods directly,
cluster-wide, with no extra Service to maintain, and it lifts per-pod
labels (primary vs replica) that a Service-level scrape can't see.

`servicemonitor.yaml` is the alternative for Prometheus deployments
configured to scrape ServiceMonitors only. It covers the Valkey
data-plane only (mirroring `valkey-podmonitor.yaml`); sentinel scraping
stays on `sentinel-podmonitor.yaml`. It needs a companion Service
(shipped in the same file) because the operator's per-CR Services don't
publish the exporter port — and that Service must be applied in each
namespace holding Valkey CRs. **Apply either the PodMonitors or the
ServiceMonitor, never both**, or every series is scraped twice.

## Built-in alternatives

If you install via Helm you may not need these samples at all:

- **Operator-self**: set `metrics.serviceMonitor.enabled=true` — the chart
  ships the operator-self ServiceMonitor with the TLS/bearer config
  templated and the release name/namespace tracked automatically. Prefer
  it over `operator-podmonitor.yaml`.
- **Per-CR data-plane**: set `spec.metrics.podMonitor.enabled=true` on a
  Valkey resource and the operator renders a PodMonitor scoped to that
  CR's pods. Use `valkey-podmonitor.yaml` only when you want a single
  cluster-wide monitor instead.

The sentinel plane has no built-in equivalent — `sentinel-podmonitor.yaml`
is the way to scrape it.

## Adapting

Every sample names the operator release `velkir` in namespace
`velkir-system`, and carries a `release:` label as the Prometheus
monitor-discovery selector. Adjust the release/namespace, and set the
`release:` label to whatever your Prometheus's `podMonitorSelector` /
`serviceMonitorSelector` matches, before applying.

See also [`../networkpolicy/`](../networkpolicy/) for the matching
network-policy samples that allow Prometheus to reach the exporter ports.
