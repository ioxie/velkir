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

package events

// OperatorStarted is emitted once per process on the operator manager's
// OnStart callback.
//
// Informational; not alertable.
const OperatorStarted Reason = "OperatorStarted"

// WebhookCertRotated is emitted by the dynauth Authority when it has
// successfully reissued the webhook CA or leaf cert. The event message
// names which Secret was reissued so admins reading the event log can
// distinguish CA-rotation events (which cascade to a leaf reissue) from
// stand-alone leaf rotations.
//
// Informational; not alertable on its own.
const WebhookCertRotated Reason = "WebhookCertRotated"

// WebhookCertRotationFailed is emitted when the Authority's reconcile
// pass errors out — Secret CRUD failure, parse failure, or signing
// failure. The Authority retries on a 1h floor after errors, so a
// transient blip surfaces as a single event and resolves itself; a
// sustained failure fires repeatedly and trips the cert-rotation
// failing alert.
//
// Alertable.
const WebhookCertRotationFailed Reason = "WebhookCertRotationFailed"

// BootstrapCompleted is emitted once per CR per cold bootstrap when
// the operator has finished writing the per-CR ConfigMaps and seen
// the data-plane STS reach its desired pod count. Marks the
// transition from initial rollout into steady-state reconciliation.
//
// Informational; not alertable.
const BootstrapCompleted Reason = "BootstrapCompleted"

// BootstrapConfigMapUpdated is emitted when the operator rewrites the
// `<cr>-sentinel-bootstrap` seed ConfigMap — most commonly during a
// whole-STS cold start where pod-0 returns at a different IP than the
// value the prior generation of sentinels were seeded against.
// Sentinel-mode-specific; informational.
const BootstrapConfigMapUpdated Reason = "BootstrapConfigMapUpdated"

// ReplicaAnnounceIPUpdated is emitted when the operator notices a
// pod's status.podIP no longer matches the announce-ip the
// downstream peers have on file. The CLI flag is sourced from the
// Downward API so the announce value tracks the pod IP automatically;
// this event records the moment the operator observed the drift,
// distinct from any operator-driven correction.
// Informational.
const ReplicaAnnounceIPUpdated Reason = "ReplicaAnnounceIPUpdated"

// ConfigDrift is emitted when the rendered valkey.conf hash for a CR
// changes between reconciles, signalling that a `spec.valkey.configuration`
// or `spec.valkey.configurationOverrides` edit (or a mandatory-layer
// version bump) will roll the data-plane pods on the next pod-template
// apply.
// Informational.
const ConfigDrift Reason = "ConfigDrift"

// ConfigMapRecreated is emitted when the operator has to recreate (not
// merely patch) one of its owned ConfigMaps — e.g. the
// `<cr>-init-scripts` ConfigMap was deleted out-of-band. Distinct
// from ConfigDrift: this reflects a destructive external mutation,
// not a spec edit. Surfaced so admins reading the event log can see
// when something deleted operator-owned state.
// Informational; investigate if frequent.
const ConfigMapRecreated Reason = "ConfigMapRecreated"

// DegradedResolved is emitted when the operator observes the CR's
// Degraded condition flipping True -> False during status finalisation.
// Pairs with whatever event flipped Degraded True in the first place
// (a previous Warning event recorded the cause); this event marks the
// recovery edge so an alert keying on `Degraded=True` cleanly clears
// without needing to poll status.
//
// Informational; not alertable on its own — the recovery is the
// resolution of a previously-fired alert.
const DegradedResolved Reason = "DegradedResolved"

// FieldDeprecated is emitted by the per-CR deprecation watcher when a
// CR's spec exercises a field currently in the alpha-deprecation
// window. Emitted once per (namespace/name/field) tuple per process
// lifetime — the in-memory dedup set resets on operator restart, so
// the first reconcile after restart re-emits.
//
// Pair with an Alertmanager rule on
// `rate(kube_event_count{reason="FieldDeprecated"}[1h]) > 0` to surface
// deprecation pressure ahead of a removal landing. Informational on
// its own.
const FieldDeprecated Reason = "FieldDeprecated"
