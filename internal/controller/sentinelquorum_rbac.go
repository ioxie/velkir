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

// kubebuilder:rbac markers for the operator's access to SentinelQuorum.
//
// Verb shape:
//   - resources=sentinelquorums: get;list;watch + create;update;patch;delete.
//     `patch` is required for SSA (the operator applies SQ resources via
//     server-side apply, which is a PATCH operation even when the desired
//     shape already matches the server's view). `update` is included for
//     symmetry with other operator-owned resources; `delete` covers manual
//     cleanup paths even though owner-ref cascade GC is the primary teardown
//     mechanism. Spec stays immutable via the type-level CEL — re-applying
//     the same {Valkey, PodName} shape produces an empty diff that doesn't
//     trip the CEL rule.
//   - resources=sentinelquorums/status: get;patch. The operator reads
//     SQ status via its aggregator (internal/sqaggregate) to derive the
//     primary-pod majority view, and writes it back through server-side
//     apply (reconcileSentinelQuorumStatus -> ssa.ApplyACStatus), which
//     is a PATCH even when the desired shape already matches the server.
//     `update` is intentionally omitted: the writer only ever SSA-
//     applies, never a typed Status().Update().
//
// Pull-model architecture: sentinel pods are not K8s API clients;
// they speak only the Valkey protocol. The operator is the sole
// SentinelQuorum writer, so per-pod Role + RoleBinding +
// ServiceAccount scaffolding (push model — sentinel pods writing
// their own SQ status via RBAC narrowed by resourceNames) is
// unnecessary and intentionally absent. Re-evaluate only if a future
// compliance requirement demands per-pod attestation that the
// operator's single-writer identity cannot satisfy.

// +kubebuilder:rbac:groups=velkir.ioxie.dev,resources=sentinelquorums,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velkir.ioxie.dev,resources=sentinelquorums/status,verbs=get;patch
