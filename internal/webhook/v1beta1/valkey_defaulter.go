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
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/defaults"
)

const (
	// ManagedByLabel is the well-known Kubernetes label that marks every
	// operator-owned object. The reconciler's informer cache is filtered
	// on this label; without it, the operator can't see the resources it
	// just created.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value the operator stamps under ManagedByLabel.
	ManagedByValue = "velkir"
)

// Pod-set identity label aliases — the canonical values live in
// internal/defaults (shared with the reconciler's spec normalization);
// the validator and tests in this package keep their local names.
const (
	crLabelKey              = defaults.CRLabelKey
	componentLabelKey       = defaults.ComponentLabelKey
	componentValkey         = defaults.ComponentValkey
	componentSentinel       = defaults.ComponentSentinel
	antiAffinityTopologyKey = defaults.AntiAffinityTopologyKey

	defaultValkeyImageRepo   = defaults.DefaultValkeyImageRepo
	defaultValkeyImageTag    = defaults.DefaultValkeyImageTag
	defaultExporterImageRepo = defaults.DefaultExporterImageRepo
	defaultExporterImageTag  = defaults.DefaultExporterImageTag
)

// SetupValkeyWebhookWithManager registers the defaulting and validating
// webhooks. Both handlers are wrapped with admissionSizeLimitHandler so
// an oversized payload is refused at handler entry — before the typed
// wrapper's Decode allocates the corresponding Go struct, which is the
// allocation-free shape the defense-in-depth bound depends on. Manual
// registration replaces the ctrl.NewWebhookManagedBy builder shortcut
// because that builder's Complete() registers the inner Webhook with
// no middleware extension point.
//
// Path strings must stay in lockstep with the +kubebuilder:webhook
// `path=` markers; if those drift, the manifest is generated against a
// path the server doesn't serve.
func SetupValkeyWebhookWithManager(mgr ctrl.Manager) error {
	scheme := mgr.GetScheme()
	server := mgr.GetWebhookServer()

	defaulterWebhook := admission.WithDefaulter[*valkeyv1beta1.Valkey](scheme, &ValkeyCustomDefaulter{})
	defaulterWebhook.Handler = admissionSizeLimitHandler(defaulterWebhook.Handler)
	server.Register("/mutate-velkir-ioxie-dev-v1beta1-valkey", defaulterWebhook)

	validatorWebhook := admission.WithValidator[*valkeyv1beta1.Valkey](scheme, &ValkeyCustomValidator{})
	validatorWebhook.Handler = admissionSizeLimitHandler(validatorWebhook.Handler)
	server.Register("/validate-velkir-ioxie-dev-v1beta1-valkey", validatorWebhook)

	return nil
}

// failurePolicy=ignore matches the convention for defaulting webhooks: a
// momentarily-unreachable defaulter must not block CR CRUD. Schema-level
// OpenAPI defaults still fire on the apiserver, so users get a usable
// baseline without the webhook; the reconciler is expected to be
// defensive about missing defaulter-stamped fields. The validating
// webhook uses failurePolicy=fail because validation enforces contracts,
// not nice-to-have fills.
// +kubebuilder:webhook:path=/mutate-velkir-ioxie-dev-v1beta1-valkey,mutating=true,failurePolicy=ignore,sideEffects=None,groups=velkir.ioxie.dev,resources=valkeys,verbs=create;update,versions=v1beta1,name=mvalkey-v1beta1.kb.io,admissionReviewVersions=v1

// ValkeyCustomDefaulter fills in fields a user omitted on a Valkey CR.
//
// Contract:
//   - Idempotent: applying the defaulter to an already-defaulted spec is a
//     no-op (CR-level deep equality before/after must hold).
//   - Never overrides user-provided values; only fills nils/zeros.
//   - Pure function on the AdmissionReview — no API calls, no cluster
//     lookups (the latency budget for admission is tight).
type ValkeyCustomDefaulter struct{}

// Default implements admission.Defaulter.
func (d *ValkeyCustomDefaulter) Default(ctx context.Context, v *valkeyv1beta1.Valkey) (retErr error) {
	log := logf.FromContext(ctx).WithValues("valkey", v.Name, "namespace", v.Namespace)
	log.V(1).Info("defaulting Valkey")

	// operation is "*" because the typed CustomDefaulter signature
	// doesn't carry the AdmissionRequest.Operation through; CREATE and
	// UPDATE share the same Default body so a finer split adds no signal.
	defer recordWebhookDuration(time.Now(), "valkey-defaulter", "*", retErr)

	stampManagedByLabel(v)
	// Spec-shaping defaults live in internal/defaults, shared with the
	// reconciler's in-memory normalization so rendered output can never
	// depend on whether this webhook ran at admission time.
	defaults.ApplySpecDefaults(v)
	stampOperatorTriggerRequestor(ctx, v)

	return nil
}

// requestorAnnotationSuffix is appended to each operator-trigger
// annotation key to form the per-trigger sibling that records who set
// the trigger. The full key is, e.g.,
// `velkir.ioxie.dev/accept-pvc-loss-requestor`. Centralised so the
// reconciler reads back via the same suffix.
const requestorAnnotationSuffix = "-requestor"

// stampOperatorTriggerRequestor populates per-trigger sibling annotations
// recording the userInfo.username from the admission request whenever an
// operator-trigger annotation is set to literal "true". The reconciler
// reads these siblings when emitting audit events so the compliance
// trail names a real user (or service account), not the
// "operator:reconciler" fallback.
//
// Status is a subresource on Valkey, so a webhook cannot persist this
// information into Status (the apiserver strips status writes from
// admission responses on the main resource path). Per-trigger metadata
// annotations are the only path that survives.
//
// Semantics:
//   - On every admission where a trigger annotation is "true": stamp the
//     <trigger>-requestor sibling to the current userInfo.username,
//     overwriting any prior value. The last user to touch the CR while
//     the trigger is active is recorded — they could have removed the
//     annotation but did not, so they are an effective re-authorizer.
//     Overwriting (rather than preserve-first) also closes the spoofing
//     path where a user pre-sets the requestor sibling themselves.
//   - When the trigger annotation is absent or its value is not "true":
//     strip the sibling. Leftover requestor data on a stale or removed
//     annotation would be misleading at audit-emission time.
//
// Errors from RequestFromContext (e.g., test paths that call Default
// without going through the admission pipeline) are silently skipped —
// the production webhook always plumbs the request, and tests that need
// to exercise this path build a context via NewContextWithRequest.
func stampOperatorTriggerRequestor(ctx context.Context, v *valkeyv1beta1.Valkey) {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return
	}
	username := req.UserInfo.Username

	for _, trigger := range operatorTriggerAnnotations {
		siblingKey := trigger + requestorAnnotationSuffix
		if v.Annotations[trigger] == triggerTrueValue {
			if username == "" {
				// Anonymous request — drop any stale sibling rather than
				// stamping an empty username (which would lose the audit
				// signal entirely).
				delete(v.Annotations, siblingKey)
				continue
			}
			if v.Annotations == nil {
				v.Annotations = map[string]string{}
			}
			v.Annotations[siblingKey] = username
		} else {
			delete(v.Annotations, siblingKey)
		}
	}
	// Don't leave an empty (but non-nil) annotations map — fixtures and
	// the round-trip tests pin the nil-map case for CRs without any
	// annotations.
	if len(v.Annotations) == 0 {
		v.Annotations = nil
	}
}

// stampManagedByLabel ensures the CR carries the operator's
// managed-by label. The controller's informer cache filter applies to
// owned types (Pods, Services, ConfigMaps, etc.) — NOT to the Valkey
// CR itself, which is read with an unfiltered cache so users don't
// have to know about operator labels to author one. The label on the
// CR is for: `kubectl get valkeys -l app.kubernetes.io/managed-by=...`
// inventory queries, multi-operator coexistence (where another
// operator manages similarly-shaped CRs), and consistency with the
// labels stamped on owned resources. Users may add their own labels;
// we only stamp the managed-by key when missing.
func stampManagedByLabel(v *valkeyv1beta1.Valkey) {
	if v.Labels == nil {
		v.Labels = map[string]string{}
	}
	if _, ok := v.Labels[ManagedByLabel]; !ok {
		v.Labels[ManagedByLabel] = ManagedByValue
	}
}

// Compile-time interface assertion: the v0.23 typed Defaulter contract.
var _ admission.Defaulter[*valkeyv1beta1.Valkey] = (*ValkeyCustomDefaulter)(nil)
