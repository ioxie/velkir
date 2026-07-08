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
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// Deviation is a single best-practice deviation: a catalog Reason,
// the JSON-path field it concerns, and a human-readable explanation. The
// validating webhook renders these as admission warnings
// ("<Reason>: <Message>"); the controller emits them as durable Events
// via internal/events.DeviationEmitter. Single source of truth for both
// surfaces — admission warnings are shown once at apply time and then
// gone, the Events are durable and queryable.
type Deviation struct {
	Reason  events.Reason
	Field   string
	Message string
}

// Deviations returns every active warn-on-deviation for the CR as
// structured (reason, field, message) triples. Returns nil when the CR's
// PDB, anti-affinity, and rollout shapes all match the best-practice
// defaults. Exported so the controller can re-derive the same deviations
// at reconcile time without duplicating the detection logic.
func Deviations(o *valkeyv1beta1.Valkey) []Deviation {
	pdb := pdbDeviations(o)
	aff := antiAffinityDeviations(o)
	rollout := rolloutDeviations(o)
	timing := sentinelTimingDeviations(o)
	out := make([]Deviation, 0, len(pdb)+len(aff)+len(rollout)+len(timing))
	out = append(out, pdb...)
	out = append(out, aff...)
	out = append(out, rollout...)
	out = append(out, timing...)
	return out
}

// sentinelTimingDeviations returns the WarnAggressiveTimeouts deviation when a
// sentinel-mode CR's down-after sits in the accepted-but-aggressive band
// [downAfterFloor, downAfterRecommended). Sub-floor values are rejected (or
// floor-warned under the allow-aggressive-timeouts annotation) in
// validateSentinelTimingFloors, so they never reach this band. The message is
// the single source of truth for both the admission warning
// (renderDeviationWarnings) and the durable reconciler Event (emitDeviations).
func sentinelTimingDeviations(o *valkeyv1beta1.Valkey) []Deviation {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel || o.Spec.Sentinel == nil {
		return nil
	}
	da := o.Spec.Sentinel.DownAfterMilliseconds
	if da < downAfterFloor || da >= downAfterRecommended {
		return nil
	}
	return []Deviation{{
		Reason: events.WarnAggressiveTimeouts,
		Field:  "spec.sentinel.downAfterMilliseconds",
		Message: fmt.Sprintf("spec.sentinel.downAfterMilliseconds=%d is below the recommended %d ms; accepted, but increases spurious-failover risk on CPU-throttled or oversubscribed nodes where a transient stall can look like a crash",
			da, downAfterRecommended),
	}}
}

// renderDeviationWarnings renders Deviations(o) as admission warnings,
// one per deviation, formatted "<Reason>: <Message>" — byte-for-byte the
// strings the webhook surfaced before the detection was lifted into
// Deviations. The CR is always accepted; these are user-overridable
// knobs, not invariants.
func renderDeviationWarnings(o *valkeyv1beta1.Valkey) admission.Warnings {
	devs := Deviations(o)
	if len(devs) == 0 {
		return nil
	}
	warnings := make(admission.Warnings, 0, len(devs))
	for _, d := range devs {
		warnings = append(warnings, string(d.Reason)+": "+d.Message)
	}
	return warnings
}

// pdbDeviations returns the PDB-shape deviations: PDBTooPermissive for a
// valkey or sentinel PDB whose explicit minAvailable is below the
// best-practice floor max(1, replicas-1), and MaxUnavailableInsteadOfMin
// when the valkey PDB sets maxUnavailable but not minAvailable (the
// operator prefers minAvailable; maxUnavailable-only works but is less
// expressive). Returns nil for any CR whose PDB shapes match the
// best-practice default.
func pdbDeviations(o *valkeyv1beta1.Valkey) []Deviation {
	var out []Deviation

	out = append(out, pdbBelowDefault(
		"spec.valkey.pdb",
		o.Spec.Valkey.PDB,
		o.Spec.Valkey.Replicas,
	)...)
	if o.Spec.Sentinel != nil {
		out = append(out, pdbBelowDefault(
			"spec.sentinel.pdb",
			o.Spec.Sentinel.PDB,
			o.Spec.Sentinel.Replicas,
		)...)
	}
	if pdb := o.Spec.Valkey.PDB; pdb != nil && pdb.MinAvailable == nil && pdb.MaxUnavailable != nil {
		out = append(out, Deviation{
			Reason: events.MaxUnavailableInsteadOfMin,
			Field:  "spec.valkey.pdb",
			Message: fmt.Sprintf("spec.valkey.pdb sets maxUnavailable=%s but not minAvailable; minAvailable is preferred — the operator's rolling-update math reads minAvailable directly and falls back to maxUnavailable only when minAvailable is unset.",
				pdb.MaxUnavailable.String()),
		})
	}
	return out
}

// pdbBelowDefault returns a PDBTooPermissive deviation when the supplied
// PDB has an explicit minAvailable below the best-practice floor of
// max(1, replicas-1). Returns nil when PDB is unset (the defaulter
// stamps the floor in that case), when minAvailable is unset (covered
// by MaxUnavailableInsteadOfMin separately), or when the value matches
// or exceeds the floor.
//
// Only IntOrString=Int form is compared against the floor — percentage
// minAvailable is intentionally not flagged because the conversion
// depends on replicas-at-runtime which the validator can't compute
// against a future scale-up.
func pdbBelowDefault(fieldRef string, pdb *valkeyv1beta1.PDBSpec, replicas int32) []Deviation {
	if pdb == nil || pdb.MinAvailable == nil {
		return nil
	}
	if pdb.MinAvailable.Type != intstr.Int {
		return nil
	}
	floor := max(int32(1), replicas-1)
	if pdb.MinAvailable.IntVal >= floor {
		return nil
	}
	return []Deviation{{
		Reason: events.PDBTooPermissive,
		Field:  fieldRef,
		Message: fmt.Sprintf("%s.minAvailable=%d is below the best-practice floor of max(1, replicas-1)=%d for replicas=%d; values below the floor bypass node-drain protection during rolling updates.",
			fieldRef, pdb.MinAvailable.IntVal, floor, replicas),
	}}
}

// antiAffinityDeviations returns an AntiAffinityTooPermissive deviation
// when a pod set with ≥2 replicas carries a user-supplied PodAntiAffinity
// that no longer spreads the set across nodes — i.e. the user replaced
// the soft same-pod-set default the defaulter would otherwise stamp with
// terms that target only the other set (or none). The CR is always
// accepted (mirror PDBTooPermissive).
//
// Never flags valkey↔sentinel terms — co-location of the two sets is
// allowed.
func antiAffinityDeviations(o *valkeyv1beta1.Valkey) []Deviation {
	var out []Deviation
	out = append(out, antiAffinityBelowDefault(
		"spec.valkey.affinity",
		o.Spec.Valkey.Affinity,
		o.Spec.Valkey.Replicas,
		componentValkey,
	)...)
	if o.Spec.Sentinel != nil {
		out = append(out, antiAffinityBelowDefault(
			"spec.sentinel.affinity",
			o.Spec.Sentinel.Affinity,
			o.Spec.Sentinel.Replicas,
			componentSentinel,
		)...)
	}
	return out
}

// antiAffinityBelowDefault returns an AntiAffinityTooPermissive deviation
// when a ≥2-replica pod set carries a user-set PodAntiAffinity with no
// same-pod-set cross-node term. Returns nil when:
//   - replicas < 2 (single-pod sets need no spread);
//   - PodAntiAffinity is unset (the defaulter stamps the soft default —
//     mirrors pdbBelowDefault returning nil for an unset PDB);
//   - a same-pod-set term exists, soft (matches the default) or hard
//     (required — stricter than the default, never flagged).
func antiAffinityBelowDefault(fieldRef string, aff *corev1.Affinity, replicas int32, component string) []Deviation {
	if replicas < 2 {
		return nil
	}
	if aff == nil || aff.PodAntiAffinity == nil {
		return nil
	}
	if hasSamePodSetAntiAffinity(aff.PodAntiAffinity, component) {
		return nil
	}
	return []Deviation{{
		Reason: events.AntiAffinityTooPermissive,
		Field:  fieldRef,
		Message: fmt.Sprintf("%s.podAntiAffinity has no same-pod-set (component=%s) term on topologyKey %q, so two %s pods may share a node and a single node failure can take down the set; the defaulter stamps a soft preferred anti-affinity by default — restore a same-pod-set term (soft or hard) to keep cross-node spread.",
			fieldRef, component, antiAffinityTopologyKey, component),
	}}
}

// hasSamePodSetAntiAffinity reports whether the PodAntiAffinity carries
// at least one term (required or preferred) that targets the same pod
// set, identified by the component label. Cross-set terms (targeting
// only the other component) do not count. A nil LabelSelector is
// treated as not set-specific.
func hasSamePodSetAntiAffinity(paa *corev1.PodAntiAffinity, component string) bool {
	for i := range paa.RequiredDuringSchedulingIgnoredDuringExecution {
		if termTargetsComponent(&paa.RequiredDuringSchedulingIgnoredDuringExecution[i], component) {
			return true
		}
	}
	for i := range paa.PreferredDuringSchedulingIgnoredDuringExecution {
		if termTargetsComponent(&paa.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm, component) {
			return true
		}
	}
	return false
}

// termTargetsComponent reports whether term's LabelSelector selects the
// given pod-set component, via either a MatchLabels entry or a
// MatchExpressions In/Exists on the component label.
func termTargetsComponent(term *corev1.PodAffinityTerm, component string) bool {
	if term.LabelSelector == nil {
		return false
	}
	if term.LabelSelector.MatchLabels[componentLabelKey] == component {
		return true
	}
	for _, req := range term.LabelSelector.MatchExpressions {
		if req.Key != componentLabelKey {
			continue
		}
		switch req.Operator {
		case metav1.LabelSelectorOpExists:
			return true
		case metav1.LabelSelectorOpIn:
			if slices.Contains(req.Values, component) {
				return true
			}
		}
	}
	return false
}

// rolloutDeviations returns warn-on-deviation entries for rollout knobs
// that deviate from the best-practice baseline. The CR is always
// accepted.
//
// Coverage:
//   - spec.rollout.maxUnavailable > 1 → RolloutFragileQuorum (multi-pod
//     concurrent rolls require the reconciler to re-derive CKQUORUM
//     projections with a higher down-count; sentinel-mode CRs are
//     particularly exposed because the maxUnavailable count comes
//     directly out of the sentinel-side quorum math).
//   - spec.rollout.failoverGracePeriodSeconds non-zero AND below
//     spec.sentinel.failoverTimeout/1000 → RolloutGraceTooTight (the
//     grace period is the operator's wall-clock backstop on
//     +failover-end; setting it below failoverTimeout means the
//     operator declares FailoverStalled before sentinels themselves
//     would have given up).
//
// Returns nil for any CR whose rollout shape matches the baseline.
func rolloutDeviations(o *valkeyv1beta1.Valkey) []Deviation {
	var out []Deviation

	if o.Spec.Rollout.MaxUnavailable > 1 {
		out = append(out, Deviation{
			Reason: events.RolloutFragileQuorum,
			Field:  "spec.rollout.maxUnavailable",
			Message: fmt.Sprintf("spec.rollout.maxUnavailable=%d exceeds the best-practice value 1; concurrent multi-pod rolls force the operator to re-derive CKQUORUM with a higher down-count, which is fragile on small sentinel pools (3-replica). The CR is accepted, but consider pinning to 1 unless the data plane carries enough headroom.",
				o.Spec.Rollout.MaxUnavailable),
		})
	}

	// RolloutGraceTooTight: only meaningful when the user set a
	// non-zero grace AND a sentinel block exists to compare against.
	// failoverTimeout is in milliseconds; failoverGracePeriodSeconds in
	// seconds — convert before comparing.
	if o.Spec.Sentinel != nil && o.Spec.Rollout.FailoverGracePeriodSeconds > 0 {
		failoverTimeoutSec := o.Spec.Sentinel.FailoverTimeout / 1000
		if o.Spec.Rollout.FailoverGracePeriodSeconds < failoverTimeoutSec {
			out = append(out, Deviation{
				Reason: events.RolloutGraceTooTight,
				Field:  "spec.rollout.failoverGracePeriodSeconds",
				Message: fmt.Sprintf("spec.rollout.failoverGracePeriodSeconds=%d is below spec.sentinel.failoverTimeout=%dms (%ds); the operator may declare FailoverStalled before sentinels themselves would have given up. The CR is accepted, but consider %ds (failoverTimeout + a 30s margin) or 0 (operator computes the grace at reconcile time).",
					o.Spec.Rollout.FailoverGracePeriodSeconds,
					o.Spec.Sentinel.FailoverTimeout,
					failoverTimeoutSec,
					failoverTimeoutSec+30),
			})
		}
	}

	return out
}
