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

// PodsCoLocated is emitted (Warning) when two or more pods of the same
// set (2+ valkey, or 2+ sentinel) are observed scheduled on the same
// node. The operator stamps a SOFT (preferred) cross-node anti-affinity
// by default, which the scheduler may override under node scarcity or
// pressure — so co-location remains possible even with the default in
// place. Surfaces the residual single-node-failure risk. A valkey and a
// sentinel pod sharing a node is permitted and never triggers this.
//
// Alertable on a sustained non-zero count: persistent co-location means
// a single node failure can take out a quorum-relevant fraction of a set.
const PodsCoLocated Reason = "PodsCoLocated"

// PDBTooPermissive is emitted (Warning) when a CR's valkey or sentinel
// PodDisruptionBudget sets an explicit `minAvailable` below the
// best-practice floor of max(1, replicas-1) — values below the floor
// bypass node-drain protection during rolling updates. This is the
// durable, queryable twin of the same-named admission warning
// (warn-on-deviation): admission warnings are shown once at apply time
// and then gone, so the operator re-surfaces the deviation as an Event
// on reconcile. Emitted at most once per (namespace/name) per process
// lifetime; the in-memory dedup set resets on operator restart.
//
// Alertable on persistence — a CR running with a too-permissive PDB
// carries weakened disruption protection the operator-of-the-operator
// should know about even though the CR is accepted.
const PDBTooPermissive Reason = "PDBTooPermissive"

// MaxUnavailableInsteadOfMin is emitted (Warning) when a CR's valkey
// PDB sets `maxUnavailable` but not `minAvailable`; the operator's
// rolling-update math reads `minAvailable` directly and falls back to
// `maxUnavailable` only when `minAvailable` is unset, so the shape
// works but is less expressive. Durable twin of the admission warning;
// per-(namespace/name)-per-process dedup like PDBTooPermissive.
//
// Informational; the shape is accepted and functional.
const MaxUnavailableInsteadOfMin Reason = "MaxUnavailableInsteadOfMin"

// AntiAffinityTooPermissive is emitted (Warning) when a ≥2-replica pod
// set carries a user-supplied PodAntiAffinity with no same-pod-set
// cross-node term, so two pods of the set may share a node and a single
// node failure can take down the set. Distinct from PodsCoLocated,
// which keys on observed runtime co-location; this Reason keys on the
// CR's declared anti-affinity shape at reconcile time. Durable twin of
// the admission warning; per-(namespace/name)-per-process dedup.
//
// Alertable on persistence — declared loss of cross-node spread is a
// single-node-failure risk.
const AntiAffinityTooPermissive Reason = "AntiAffinityTooPermissive"

// RolloutFragileQuorum is emitted (Warning) when `spec.rollout.maxUnavailable`
// exceeds the best-practice value 1; concurrent multi-pod rolls force
// the operator to re-derive CKQUORUM with a higher down-count, fragile
// on small (3-replica) sentinel pools. Durable twin of the admission
// warning; per-(namespace/name)-per-process dedup.
//
// Informational; alertable on persistence for sentinel-mode CRs.
const RolloutFragileQuorum Reason = "RolloutFragileQuorum"

// RolloutGraceTooTight is emitted (Warning) when
// `spec.rollout.failoverGracePeriodSeconds` is non-zero and below
// `spec.sentinel.failoverTimeout` (converted to seconds); the operator
// may declare FailoverStalled before the sentinels themselves would
// have given up. Durable twin of the admission warning;
// per-(namespace/name)-per-process dedup.
//
// Informational; the shape is accepted.
const RolloutGraceTooTight Reason = "RolloutGraceTooTight"

// WarnAggressiveTimeouts is emitted (Warning) when a sentinel-mode CR's
// spec.sentinel.downAfterMilliseconds sits at or above the hard floor
// (1000ms) but below the recommended production-safe value (30000ms): on a
// CPU-throttled or oversubscribed node a transient stall can exceed the
// aggressive down-after and look like a crash, tripping a spurious
// +sdown/failover. Durable twin of the same-band admission warning
// (warn-on-deviation): admission warnings are shown once at apply time and
// then gone, so the operator re-surfaces the deviation as an Event on
// reconcile. The sanctioned default (3000ms) falls in this band, so every
// default sentinel CR carries one — the per-(namespace/name/reason/field)-
// per-process latch keeps it to a single Event per CR per operator lifetime
// rather than one per reconcile.
//
// Informational; alertable on persistence for sentinel-mode CRs running
// below the recommended down-after.
const WarnAggressiveTimeouts Reason = "WarnAggressiveTimeouts"
