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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SentinelQuorumSpec is operator-set, immutable bookkeeping that binds
// a SentinelQuorum record to the sentinel pod it reports for and its
// parent Valkey CR. The operator creates one SentinelQuorum per
// sentinel pod at bootstrap and never updates the spec afterwards.
//
// Pull-model architecture: sentinel pods are not K8s API clients —
// they speak only the Valkey protocol. The operator observes them
// in-process (internal/sentinel/observer.go) and is the sole intended
// writer of SentinelQuorum status. There is no peer-impersonation
// path because the sentinel pods cannot reach the apiserver at all.
type SentinelQuorumSpec struct {
	// Valkey is the name of the parent Valkey CR this record reports for.
	// Set by the operator at create time and immutable afterwards (the
	// CEL rule on the type root enforces). Cross-Valkey reuse of a
	// SentinelQuorum record would break the aggregation invariant
	// (operator aggregates per-Valkey).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`
	Valkey string `json:"valkey"`

	// PodName is the name of the sentinel pod this record reports
	// for. Set by the operator at create time and immutable
	// afterwards. The 1:1 SQ-name-to-pod-name mapping lets a reader
	// correlate each record with its pod via `kubectl get
	// sentinelquorums` without a separate derivation step.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`
	PodName string `json:"podName"`
}

// SentinelQuorumStatus carries one sentinel pod's view of the
// topology — which valkey pod it sees as primary, which replicas it
// can reach, and whether it has enough peer-sentinels online to form
// quorum. The operator reads + aggregates these per-Valkey records to
// drive the failover state machine and `Valkey.Status` updates.
//
// Writer: the operator is the sole intended writer (pull model — see
// the type-level godoc above). Status writer wiring from the operator
// observer is forward-compat; aggregation reads tolerate empty/stale
// status by reporting Unknown, so unwired records do not surface as
// a failure.
type SentinelQuorumStatus struct {
	// ObservedPrimary is the pod name the sentinel currently believes
	// is the primary. Empty before the sentinel has converged on a
	// view; non-empty thereafter (refreshed on each observation pass).
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ObservedPrimary string `json:"observedPrimary,omitempty"`

	// ObservedReplicas is the set of pod names the sentinel currently
	// sees as connected, non-primary replicas. Order is not
	// significant; the operator sorts on aggregation. Empty list is
	// legal (single-replica lab clusters).
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=10
	ObservedReplicas []string `json:"observedReplicas,omitempty"`

	// QuorumReachable indicates whether this sentinel pod can see at
	// least `spec.sentinel.quorum` peer-sentinels online. The operator
	// requires every aggregated record to report `true` before the
	// triple-check failover rule will fire.
	// +optional
	QuorumReachable *bool `json:"quorumReachable,omitempty"`

	// LastObservedTime is the wall-clock timestamp of the most
	// recent observation pass that produced this status. Stale
	// records (older than the operator's freshness threshold) are
	// excluded from aggregation — the operator treats them as a
	// missing pod, not a stale observation, to avoid acting on data
	// the observer may have since corrected.
	// +optional
	LastObservedTime *metav1.Time `json:"lastObservedTime,omitempty"`

	// Conditions reports the standard observation-pass status
	// conditions for this sentinel. Reserved condition types:
	//   - "Reachable": the sentinel pod responds to operator probes
	//   - "QuorumAchieved": peer-sentinel quorum is met
	//   - "PrimaryConfirmed": ObservedPrimary has been seen by a
	//     majority of sentinels (set by the operator post-aggregation
	//     on the corresponding Valkey CR — NOT here)
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=sq;sentinelq
// +kubebuilder:printcolumn:name="Valkey",type=string,JSONPath=`.spec.valkey`
// +kubebuilder:printcolumn:name="Pod",type=string,JSONPath=`.spec.podName`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.observedPrimary`
// +kubebuilder:printcolumn:name="Quorum",type=boolean,JSONPath=`.status.quorumReachable`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.spec.valkey == oldSelf.spec.valkey",message="spec.valkey is immutable"
// +kubebuilder:validation:XValidation:rule="self.spec.podName == oldSelf.spec.podName",message="spec.podName is immutable"
// +genclient

// SentinelQuorum is a per-sentinel-pod, per-Valkey record of one
// sentinel's observation of the topology. The operator creates one
// per sentinel pod at bootstrap and aggregates the per-Valkey set
// into `Valkey.Status`. Pull-model architecture: sentinel pods
// don't speak to the apiserver — the operator's in-process observer
// (internal/sentinel/observer.go) is the sole intended writer of
// SQ status (writer wiring forward-compat; reads tolerate empty
// status by reporting Unknown).
type SentinelQuorum struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the operator-set binding (parent Valkey, owner pod).
	// Immutable after creation — see the type-level CEL rules.
	// +required
	Spec SentinelQuorumSpec `json:"spec"`

	// status reports the sentinel pod's observation of the topology.
	// Sentinel-pod-set; operator reads only.
	// +optional
	Status SentinelQuorumStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SentinelQuorumList contains a list of SentinelQuorum
type SentinelQuorumList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SentinelQuorum `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SentinelQuorum{}, &SentinelQuorumList{})
}
