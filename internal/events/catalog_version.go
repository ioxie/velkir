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

// ValkeyImageTransitionRejected is emitted when the reconciler
// observes a desired Valkey image-tag transition that the
// version-compat rules block — currently only major-version
// downgrades (e.g., `valkey:9.0` → `valkey:8.5`). Data-format
// compatibility across majors is not an upstream guarantee and
// the operator cannot safely roll back the on-disk RDB / AOF, so
// the rejection is hard: the StatefulSet apply is skipped this
// pass and the cluster keeps running the old image until the user
// reverts the spec.
//
// This is the runtime half of the version-compat split — admission
// cannot enforce it because Flux re-apply patterns lose the
// `oldObj` continuity needed to observe the transition. Distinct
// from a static admission rejection (which would surface as a
// webhook error, not an event); this Reason fires only at
// reconcile time on a real transition observed against a live
// StatefulSet.
//
// Alertable: a sustained rejection means the user has a spec the
// operator will not converge on; the on-call should be paged.
const ValkeyImageTransitionRejected Reason = "ValkeyImageTransitionRejected"

// ValkeyImageTransitionWarning is emitted when the reconciler
// observes a Valkey image-tag transition that skips one or more
// minor versions within the same major (e.g., `valkey:8.0` →
// `valkey:8.3`). Per `docs/versions.md` skip-version policy,
// one-minor-skip is supported; two-or-more-minor skips are
// best-effort during alpha and validated only on the current-1 →
// current path in the upgrade matrix. Reconciliation continues
// — the warning is informational, not blocking.
//
// Distinct from ValkeyImageTransitionRejected: this Reason fires
// on a transition that proceeds; the rejected Reason fires on a
// transition that is blocked outright.
//
// Non-alertable on individual emissions; the rate would be a
// useful dashboard panel.
const ValkeyImageTransitionWarning Reason = "ValkeyImageTransitionWarning"

// ValkeyImageTransitionOverridden is emitted when a transition the
// version-compat preflight would otherwise reject proceeds because
// the user explicitly disabled the preflight via
// `Spec.FeatureGates["UpgradePreflight"] = false`. The
// data-format risk is the user's to carry; the operator records
// the override on the audit trail so a post-incident review can
// see why the unsafe transition was allowed.
//
// Page-worthy: a sustained emission means a cluster is running
// with the safety check disabled, which the on-call should know
// about even if the cluster is otherwise healthy.
const ValkeyImageTransitionOverridden Reason = "ValkeyImageTransitionOverridden"
