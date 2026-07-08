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

// SecretRotated is emitted when the operator successfully hot-rotates
// the auth password across the data plane and the sentinel plane.
// Rotation order: replicas first, then master, then sentinels —
// surviving connections cycle to AUTH-with-new without a pod
// restart, and clients see at most a ~1s write blip during the
// master leg. One emission per CR per successful rotation; the
// message body names the per-pod outcome counts so a
// `kubectl get events` reader sees rotation health without
// consulting status.
//
// Informational; sustained absence (Secret content moved but no
// SecretRotated event fired) is the alert-worthy signal, surfaced
// by a PrometheusRule that pairs the Secret resourceVersion
// with this event.
const SecretRotated Reason = "SecretRotated"

// SecretRotationFailed is emitted when at least one data-plane pod
// did not accept the new credential AND the operator successfully
// reverted every successfully-updated pod back to the prior
// credential. The cluster is on the old password; the user can
// re-drive the rotation by re-applying a fresh Secret content edit.
// One emission per failed rotation attempt.
//
// Alertable: a single emission is operator-visible — the rotation
// the user requested did not land. Pair with a runbook entry that
// points at the affected endpoints in the message body and the
// AuthRotation substate's Message field.
const SecretRotationFailed Reason = "SecretRotationFailed"

// SecretRotationPartial is emitted when at least one data-plane pod
// did not accept the new credential AND the revert also failed on
// at least one of the successfully-updated pods. The cluster is in
// mixed-credential state — at least one pod is on the new password
// and at least one is on the old — so client connections will
// succeed or fail depending on which pod they reach. The affected
// endpoints are named in the message body so the
// operator-of-the-operator can re-align them manually.
//
// Page-worthy: the cluster is degraded for client auth until the
// operator intervenes.
const SecretRotationPartial Reason = "SecretRotationPartial"

// AuthSecretShortPassword is emitted when the operator reads an auth
// Secret whose `password` data key is shorter than the redaction
// registry's MinTokenLen floor (currently 8 chars; the canonical value
// lives at `internal/logging.MinTokenLen` — keep this doc in sync if
// that constant moves). The registry silently drops tokens below that
// floor because substring-redacting 7-char-or-shorter strings collides
// with ordinary log content (UUIDs, short identifiers, base64
// fragments) and over-redacts operational signal. Operators running
// genuinely-short Secret values get an unredacted log surface for
// those values; this event is the documented escape hatch that
// surfaces the configuration smell so operator-of-the-operator audit
// trails can spot the gap. Emitted at most once per
// (namespace/name/secretName) per process lifetime — the in-memory
// dedup set resets on operator restart, so the first reconcile after a
// restart re-emits.
//
// Distinct from runtime AUTH errors (which surface as
// ReplicationGateCheckFailed / sentinel reset failures); this Reason
// reports a configuration condition that affects redaction, not a
// runtime auth failure.
//
// Alertable on persistence — sustained firing points at an operator
// running with sub-MinTokenLen credentials.
const AuthSecretShortPassword Reason = "AuthSecretShortPassword"
