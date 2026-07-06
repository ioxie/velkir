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
	"cmp"
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/audit"
	"github.com/ioxie/velkir/internal/valkeyconf"
	"github.com/ioxie/velkir/internal/version"
)

// +kubebuilder:webhook:path=/validate-velkir-ioxie-dev-v1beta1-valkey,mutating=false,failurePolicy=fail,sideEffects=None,groups=velkir.ioxie.dev,resources=valkeys,verbs=create;update,versions=v1beta1,name=vvalkey-v1beta1.kb.io,admissionReviewVersions=v1

// ValkeyCustomValidator enforces the validating-webhook contract that CEL
// cannot express: parser-level config-snippet bans, image-banlist string
// matching, probe-handler restriction, reserved-label/annotation
// protection, image-tag format checks, and the operator-trigger annotation
// literal-"true" rule. failurePolicy=fail (vs the defaulter's ignore):
// validation enforces invariants that could corrupt cluster state if
// bypassed. When the webhook is unreachable, blocking CRUD is the right
// failure mode.
type ValkeyCustomValidator struct{}

// Reserved label / annotation keys that the operator owns. Users editing
// pod-level overlay maps that contain any of these collide with the
// operator's cache filter, Service selectors, and quorum-aggregation
// status logic. Sorted for determinism in error messages.
var reservedLabels = []string{
	"app.kubernetes.io/component",
	"app.kubernetes.io/instance",
	"app.kubernetes.io/managed-by",
	"app.kubernetes.io/name",
	"app.kubernetes.io/part-of",
	"velkir.ioxie.dev/component",
	"velkir.ioxie.dev/cr",
	"velkir.ioxie.dev/instance-name",
	"velkir.ioxie.dev/role",
}

// reservedAnnotationPrefixes captures the keyspace the operator may
// stamp on pods at any time. User-set annotations sharing this namespace
// would silently lose to the operator's writes during reconcile, which
// is worse than a hard reject at admission.
var reservedAnnotationPrefixes = []string{
	"velkir.ioxie.dev/",
}

// reservedAnnotationKeys are exact-match keys (in addition to the
// prefix-namespace above) that the operator owns on pod metadata.
var reservedAnnotationKeys = []string{
	"app.kubernetes.io/managed-by",
}

// AllowAggressiveTimeoutsAnnotation is the operator-trigger annotation
// that bypasses the sentinel timing floors (downAfterMilliseconds >= 1s,
// failoverTimeout >= 180s) at admission time. With the annotation set to
// "true" the validator emits a Warning instead of rejecting; without it
// sub-floor values are rejected with a field.Invalid error.
const AllowAggressiveTimeoutsAnnotation = "velkir.ioxie.dev/allow-aggressive-timeouts"

// triggerTrueValue is the literal string the four operator-trigger
// annotations must carry to fire. The validator rejects every truthy
// synonym ("True", "1", "yes", etc.) on purpose — typos must fail loudly,
// not silently no-op a privileged behaviour. Centralised so the
// defaulter, validator, and any future reader stay on the same string
// literal without goconst noise.
const triggerTrueValue = "true"

// operatorTriggerAnnotations are the four annotations on the Valkey CR
// itself (not pod overlays) that flip a privileged operator behaviour.
// The literal-"true" rule on these is load-bearing against typo-induced
// silent no-ops: a user typing "True" or "1" expecting the trigger to
// fire would get nothing, in the worst possible way.
var operatorTriggerAnnotations = []string{
	"velkir.ioxie.dev/accept-pvc-loss",
	AllowAggressiveTimeoutsAnnotation,
	"velkir.ioxie.dev/force-rotate",
	"velkir.ioxie.dev/paused",
}

// bannedConfigDirectives are the valkey.conf directive names the operator
// owns and forbids in user-supplied config. Listed as canonical lower-case
// prefixes; matched case-insensitive. Multi-word entries (`sentinel
// monitor`) match as the whole prefix on a single line. The canonical list
// lives in internal/valkeyconf (OperatorOwnedDirectives) so the renderer's
// defense-in-depth strip and this admission ban share one source of truth
// and cannot drift apart.
var bannedConfigDirectives = valkeyconf.OperatorOwnedDirectives

// configLineRegexp captures the directive name on a single line. We do
// not parse arguments; the directive name is enough to tag the line as
// banned. Comments and blanks are filtered before this matches. The
// regex captures the directive token (group 1) and any subsequent
// whitespace/argument suffix (ignored).
var configLineRegexp = regexp.MustCompile(`^\s*([A-Za-z][A-Za-z0-9-]*(?:\s+[A-Za-z][A-Za-z0-9-]*)?)`)

// imageTagRegexp validates the shape of a container image tag (no
// whitespace, no shell metacharacters, no empty). Conservative: a
// stricter image-reference parse lives in container/image/v5 but
// pulling that as a dependency for a six-rule webhook is overkill.
var imageTagRegexp = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

// floatingTags are non-empty tags we still warn about — they identify a
// moving target rather than a pinned release.
var floatingTags = map[string]bool{
	"latest": true,
	"main":   true,
	"master": true,
	"stable": true,
	"edge":   true,
}

// ValidateCreate fires on every admission CREATE.
func (v *ValkeyCustomValidator) ValidateCreate(ctx context.Context, obj *valkeyv1beta1.Valkey) (warnings admission.Warnings, retErr error) {
	defer recordWebhookDuration(time.Now(), "valkey-validator", "CREATE", retErr)
	logf.FromContext(ctx).V(1).Info("validating Valkey on create", "valkey", obj.Name, "namespace", obj.Namespace)
	return v.validate(ctx, obj)
}

// ValidateUpdate fires on every admission UPDATE. The defaulter already
// gates immutability via CEL on `spec.mode` and `spec.sentinel.masterName`;
// the rules in this validator are insensitive to old/new — they evaluate
// the new object as a standalone artifact.
func (v *ValkeyCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *valkeyv1beta1.Valkey) (warnings admission.Warnings, retErr error) {
	defer recordWebhookDuration(time.Now(), "valkey-validator", "UPDATE", retErr)
	logf.FromContext(ctx).V(1).Info("validating Valkey on update", "valkey", newObj.Name, "namespace", newObj.Namespace)
	return v.validate(ctx, newObj)
}

// ValidateDelete is a no-op — there's no shape to enforce on the way out.
func (v *ValkeyCustomValidator) ValidateDelete(_ context.Context, _ *valkeyv1beta1.Valkey) (warnings admission.Warnings, retErr error) {
	defer recordWebhookDuration(time.Now(), "valkey-validator", "DELETE", retErr)
	return nil, nil
}

func (v *ValkeyCustomValidator) validate(ctx context.Context, o *valkeyv1beta1.Valkey) (admission.Warnings, error) {
	var (
		warnings admission.Warnings
		errs     field.ErrorList
	)

	errs = append(errs, validateOperatorTriggerAnnotations(o.Annotations)...)
	errs = append(errs, validateReservedLabels(field.NewPath("spec", "valkey", "podLabels"), o.Spec.Valkey.PodLabels)...)
	errs = append(errs, validateReservedAnnotations(field.NewPath("spec", "valkey", "podAnnotations"), o.Spec.Valkey.PodAnnotations)...)
	if o.Spec.Sentinel != nil {
		errs = append(errs, validateReservedLabels(field.NewPath("spec", "sentinel", "podLabels"), o.Spec.Sentinel.PodLabels)...)
		errs = append(errs, validateReservedAnnotations(field.NewPath("spec", "sentinel", "podAnnotations"), o.Spec.Sentinel.PodAnnotations)...)
	}

	errs = append(errs, validateConfigBanlist(o.Spec.Valkey.Configuration, o.Spec.Valkey.ConfigurationOverrides)...)
	errs = append(errs, validateConfigControlChars(o.Spec.Valkey.Configuration, o.Spec.Valkey.ConfigurationOverrides)...)
	errs = append(errs, validateReservedRenderTokens(o.Spec.Valkey.Configuration, o.Spec.Valkey.ConfigurationOverrides)...)
	errs = append(errs, validateImageBanlist(o.Spec.Image)...)

	tagWarn, tagErrs := validateImageTagFormat(o.Spec.Image)
	warnings = append(warnings, tagWarn...)
	errs = append(errs, tagErrs...)

	errs = append(errs, validateValkeyImageMajorSupported(o.Spec.Image.Valkey)...)

	timingWarn, timingErrs, aggressiveAccepted := validateSentinelTimingFloors(o)
	warnings = append(warnings, timingWarn...)
	errs = append(errs, timingErrs...)

	errs = append(errs, validateProbeHandler(field.NewPath("spec", "valkey", "customLivenessProbe"), o.Spec.Valkey.CustomLivenessProbe)...)

	warnings = append(warnings, validateFeatureGates(o.Spec.FeatureGates)...)
	warnings = append(warnings, validateSentinelHA(o)...)
	warnings = append(warnings, validateSentinelReplicaParity(o)...)
	warnings = append(warnings, validateSentinelQuorumSubMajority(o)...)
	warnings = append(warnings, validateSentinelQoS(o)...)
	warnings = append(warnings, renderDeviationWarnings(o)...)
	warnings = append(warnings, validateReadinessGateMaxLagBytes(o)...)

	if len(errs) > 0 {
		// Audit every rejected field. The kube-apiserver audit log records
		// who attempted the admission; this records what the operator
		// rejected, one entry per field error so a compliance query can
		// filter on the offending field. `reason` is the stable
		// field.ErrorType code (FieldValueInvalid / FieldValueForbidden /
		// …) rather than the free-text detail — the detail can interpolate
		// user-supplied config and would risk leaking secret values (e.g.
		// a requirepass override) into the audit stream, whereas the type
		// code is a fixed, queryable enum. The full detail stays in the
		// admission response and the apiserver audit log.
		cr := types.NamespacedName{Namespace: o.Namespace, Name: o.Name}
		requestor := requestorFromContext(ctx)
		for _, e := range errs {
			audit.Log(ctx, audit.Event{
				Name:      audit.EventAdmissionRejected,
				CR:        cr,
				Requestor: requestor,
				Attrs: map[string]string{
					"field":  e.Field,
					"reason": string(e.Type),
				},
			})
		}
		return warnings, errs.ToAggregate()
	}
	// Admitted. If the incident-critical allow-aggressive-timeouts escape
	// hatch waved a sub-floor down-after / failover-timeout through, audit
	// the accept — this admission path has no Event-layer fallback, so the
	// audit stream is the only durable record that the safety floor was
	// bypassed.
	if aggressiveAccepted {
		audit.Log(ctx, audit.Event{
			Name:      audit.EventAggressiveTimeoutsAccepted,
			CR:        types.NamespacedName{Namespace: o.Namespace, Name: o.Name},
			Requestor: requestorFromContext(ctx),
			Attrs: map[string]string{
				"down_after_ms":       strconv.Itoa(int(o.Spec.Sentinel.DownAfterMilliseconds)),
				"failover_timeout_ms": strconv.Itoa(int(o.Spec.Sentinel.FailoverTimeout)),
			},
		})
	}
	return warnings, nil
}

// requestorFromContext extracts the admitting user's name from the
// admission request on ctx. Empty (→ audit.Log defaults it to
// "operator:reconciler") when no request is plumbed, e.g. unit tests
// that call the validator directly without an admission context.
func requestorFromContext(ctx context.Context) string {
	if req, err := admission.RequestFromContext(ctx); err == nil {
		return req.UserInfo.Username
	}
	return ""
}

// validateOperatorTriggerAnnotations enforces that each of the four
// privileged annotations may only carry the literal string "true". A
// typo'd value silently no-ops the trigger, which is the worst kind of
// failure for behaviour with this much blast radius.
func validateOperatorTriggerAnnotations(ann map[string]string) field.ErrorList {
	var errs field.ErrorList
	annPath := field.NewPath("metadata", "annotations")
	for _, key := range operatorTriggerAnnotations {
		val, ok := ann[key]
		if !ok {
			continue
		}
		if val != triggerTrueValue {
			errs = append(errs, field.Invalid(annPath.Key(key), val,
				fmt.Sprintf(`operator-trigger annotation %q must be the literal string "true" (got %q); "True", "1", "yes", and other truthy synonyms are rejected on purpose`, key, val)))
		}
	}
	return errs
}

// validateReservedLabels rejects pod-overlay labels colliding with the
// operator-managed key space. Pod labels feed Service selectors and the
// cache filter — a user-pinned `velkir.ioxie.dev/role=primary` on a
// replica pod would route writes to the wrong pod.
func validateReservedLabels(parent *field.Path, labels map[string]string) field.ErrorList {
	var errs field.ErrorList
	if labels == nil {
		return nil
	}
	reserved := setOf(reservedLabels)
	for k, v := range labels {
		if reserved[k] {
			errs = append(errs, field.Forbidden(parent.Key(k),
				fmt.Sprintf("label key %q is reserved for the operator (got value %q); user-supplied values here would collide with operator-managed pod labels", k, v)))
		}
	}
	return errs
}

// validateReservedAnnotations rejects pod-overlay annotations colliding
// with the operator-managed key space. The `velkir.ioxie.dev/*` prefix is
// reserved as a whole; specific exact-match keys add to the cover.
func validateReservedAnnotations(parent *field.Path, ann map[string]string) field.ErrorList {
	var errs field.ErrorList
	if ann == nil {
		return nil
	}
	exact := setOf(reservedAnnotationKeys)
	for k, v := range ann {
		if exact[k] {
			errs = append(errs, field.Forbidden(parent.Key(k),
				fmt.Sprintf("annotation key %q is reserved for the operator (got value %q)", k, v)))
			continue
		}
		for _, p := range reservedAnnotationPrefixes {
			if strings.HasPrefix(k, p) {
				errs = append(errs, field.Forbidden(parent.Key(k),
					fmt.Sprintf("annotation prefix %q is reserved for the operator (got key %q)", p, k)))
				break
			}
		}
	}
	return errs
}

// validateConfigBanlist applies the bannedConfigDirectives keyset to:
//   - the raw `Configuration` snippet (line-by-line scan)
//   - the keys of `ConfigurationOverrides` (direct match)
//
// The match is by directive prefix rather than full-line match so a user
// who writes `replica-announce-ip 10.0.0.1` is rejected even though the
// argument follows the directive name.
func validateConfigBanlist(raw string, overrides map[string]string) field.ErrorList {
	var errs field.ErrorList
	banned := setOf(bannedConfigDirectives)

	// Raw config snippet: scan non-comment, non-blank lines.
	if raw != "" {
		rawPath := field.NewPath("spec", "valkey", "configuration")
		for i, line := range strings.Split(raw, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			directive := extractDirective(trimmed)
			if directive == "" {
				continue
			}
			directiveKey := strings.ToLower(directive)
			if banned[directiveKey] {
				errs = append(errs, field.Invalid(rawPath, fmt.Sprintf("line %d", i+1),
					fmt.Sprintf(`directive %q is operator-owned and may not be set in spec.valkey.configuration`, directiveKey)))
			}
		}
	}

	// Overrides map keys: normalise to the directive's first token and
	// match the renderer's banned-key set, so admission rejects exactly
	// what the renderer's filterOverrides would otherwise drop silently.
	// A whole-string match here was narrower than the renderer — a
	// multi-token key like `requirepass evil` slipped admission yet was
	// still stripped at render, leaving the tenant a silent drop instead
	// of an explicit rejection. Sharing the first-token keyset closes that
	// gap (a padded ` requirepass` and every `sentinel *` key collapse the
	// same way the renderer collapses them).
	if len(overrides) > 0 {
		ovPath := field.NewPath("spec", "valkey", "configurationOverrides")
		bannedFirstToken := valkeyconf.BannedDirectiveKeys()
		keys := slices.Sorted(maps.Keys(overrides)) // deterministic error ordering across map walks
		for _, k := range keys {
			if bannedFirstToken[valkeyconf.NormalizeDirectiveKey(k)] {
				errs = append(errs, field.Forbidden(ovPath.Key(k),
					fmt.Sprintf(`directive %q is operator-owned and may not be set as a configuration override (key %q normalises to it)`,
						valkeyconf.NormalizeDirectiveKey(k), k)))
			}
		}
	}

	return errs
}

// validateConfigControlChars rejects user-supplied config carrying
// characters that can split a value across lines or terminate it early.
// Both the key and the value of an override are single-line by contract:
// the renderer emits each override as `<key> <value>`, so a newline in
// EITHER half renders as a second, live directive (e.g. value
// `{appendonly: "yes\nrequirepass x"}`, or key `{"x\nrequirepass y": "z"}`),
// smuggling an operator-owned directive past validateConfigBanlist (which
// matches the key's first token, not embedded control chars). A NUL in a
// key can likewise truncate the rendered directive. The raw Configuration
// is legitimately multi-line, so only NUL is rejected there — newlines are
// the separator the banlist scan already splits on, and a stray CR is
// normalised away by directive extraction.
func validateConfigControlChars(raw string, overrides map[string]string) field.ErrorList {
	var errs field.ErrorList
	if valkeyconf.ContainsNUL(raw) {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "valkey", "configuration"),
			"<control-char>",
			"must not contain a NUL byte"))
	}
	if len(overrides) > 0 {
		ovPath := field.NewPath("spec", "valkey", "configurationOverrides")
		keys := slices.Sorted(maps.Keys(overrides)) // deterministic error ordering across map walks
		for _, k := range keys {
			if valkeyconf.ContainsInjectionChars(k) {
				errs = append(errs, field.Invalid(ovPath.Key(k), "<control-char>",
					"key must not contain a newline, carriage return, or NUL byte (the renderer emits `<key> <value>`; a control char in the key would inject or truncate a directive in the rendered config)"))
			}
			if valkeyconf.ContainsInjectionChars(overrides[k]) {
				errs = append(errs, field.Invalid(ovPath.Key(k), "<control-char>",
					"value must not contain a newline, carriage return, or NUL byte (config values are single-line; a newline here would inject a second directive)"))
			}
		}
	}
	return errs
}

// reservedRenderTokens lists init-container substitution placeholders.
// The render-valkey-conf.sh and sentinel-bootstrap scripts run a
// process-wide `sed s|<token>|<value>|g` over the rendered config to
// inject the pod IP. Any user-supplied value containing one of these
// tokens would be silently rewritten — surprising at best, semantically
// wrong at worst (a Valkey directive like `proc-title-template` accepts
// arbitrary template strings; a user setting it with the literal token
// would have it substituted with the actual address).
//
// Sourced from internal/valkeyconf so a placeholder rename in either
// upstream const auto-propagates here. The init helper dedupes — both
// PodIPPlaceholder and SentinelAnnounceIPPlaceholder are "_POD_IP_"
// today, but adding both makes the lockstep structural rather than
// textual; if they ever diverge both entries land in the scan with
// no edit to this validator.
var reservedRenderTokens = dedupStrings(
	valkeyconf.PodIPPlaceholder,
	valkeyconf.SentinelAnnounceIPPlaceholder,
)

func dedupStrings(in ...string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// validateReservedRenderTokens rejects any user-supplied configuration
// value that contains an init-container reserved token. Scans:
//   - the raw `Configuration` snippet as a single string (the substring
//     check is enough — line-level granularity isn't needed because the
//     init-container sed pass also operates on the raw rendered text)
//   - each value of the `ConfigurationOverrides` map (keys are directive
//     names; only values can carry the placeholder)
func validateReservedRenderTokens(raw string, overrides map[string]string) field.ErrorList {
	var errs field.ErrorList
	for _, tok := range reservedRenderTokens {
		if raw != "" && strings.Contains(raw, tok) {
			errs = append(errs, field.Invalid(
				field.NewPath("spec", "valkey", "configuration"),
				"<reserved-token>",
				fmt.Sprintf("contains reserved init-container placeholder %q (substituted with the pod IP at runtime); choose a different token", tok)))
		}
		for k, v := range overrides {
			if strings.Contains(v, tok) {
				errs = append(errs, field.Invalid(
					field.NewPath("spec", "valkey", "configurationOverrides").Key(k),
					"<reserved-token>",
					fmt.Sprintf("value contains reserved init-container placeholder %q (substituted with the pod IP at runtime); choose a different token", tok)))
			}
		}
	}
	return errs
}

// extractDirective pulls the canonical directive token from a config
// line. For multi-word directives (`sentinel monitor mymaster ...`) the
// first two whitespace-separated tokens are taken (where the second
// looks directive-shaped); otherwise the first token alone.
func extractDirective(line string) string {
	m := configLineRegexp.FindStringSubmatch(line)
	if len(m) < 2 {
		return ""
	}
	first := strings.Fields(m[1])
	if len(first) == 0 {
		return ""
	}
	if len(first) >= 2 && first[0] == "sentinel" {
		return first[0] + " " + first[1]
	}
	return first[0]
}

// validateImageBanlist rejects upstream image references known to ship
// a different supply chain. The substring check is intentionally narrow:
// it stops a copy-pasted image reference, not a determined adversary.
func validateImageBanlist(img valkeyv1beta1.ImageSpec) field.ErrorList {
	var errs field.ErrorList
	imgPath := field.NewPath("spec", "image")
	for name, ci := range map[string]valkeyv1beta1.ContainerImage{
		"valkey":   img.Valkey,
		"sentinel": img.Sentinel,
		"exporter": img.Exporter,
	} {
		if strings.Contains(strings.ToLower(ci.Repository), "bitnami") {
			errs = append(errs, field.Invalid(imgPath.Child(name, "repository"), ci.Repository,
				`image repository may not contain the substring "bitnami"; pull the upstream image directly`))
		}
	}
	return errs
}

// validateImageTagFormat checks per-component image tags for shape and
// floating-target sanity. Empty / whitespace tags reject; floating tags
// (`latest`, `main`, etc.) warn. Repository-as-digest references
// (`repo@sha256:...`) carry their pin in repository, not tag, so an
// empty tag is permitted only when the repo contains an `@sha256:` pin.
func validateImageTagFormat(img valkeyv1beta1.ImageSpec) (admission.Warnings, field.ErrorList) {
	var (
		warnings admission.Warnings
		errs     field.ErrorList
	)
	imgPath := field.NewPath("spec", "image")
	checks := []struct {
		name string
		ci   valkeyv1beta1.ContainerImage
	}{
		{"valkey", img.Valkey},
		{"sentinel", img.Sentinel},
		{"exporter", img.Exporter},
	}
	// Iterate alphabetically for deterministic error ordering.
	slices.SortFunc(checks, func(a, b struct {
		name string
		ci   valkeyv1beta1.ContainerImage
	}) int {
		return cmp.Compare(a.name, b.name)
	})

	for _, c := range checks {
		hasDigestPin := strings.Contains(c.ci.Repository, "@sha256:")
		switch {
		case c.ci.Tag == "" && !hasDigestPin:
			errs = append(errs, field.Required(imgPath.Child(c.name, "tag"),
				fmt.Sprintf("image tag for %q is required (or pin the digest via `repo@sha256:...` in repository)", c.name)))
		case c.ci.Tag != "" && !imageTagRegexp.MatchString(c.ci.Tag):
			errs = append(errs, field.Invalid(imgPath.Child(c.name, "tag"), c.ci.Tag,
				`image tag must match [A-Za-z0-9_][A-Za-z0-9_.-]{0,127}`))
		case floatingTags[strings.ToLower(c.ci.Tag)]:
			warnings = append(warnings,
				fmt.Sprintf("image %q tag %q is a floating reference; pin a versioned tag for reproducibility", c.name, c.ci.Tag))
		}
	}
	return warnings, errs
}

// validateValkeyImageMajorSupported is the static admission half of
// the version-compat split: rejects a Valkey image-tag whose parsed
// major version is not in `internal/version.SupportedMajors`.
// Tag-shape errors from `validateImageTagFormat` already cover the
// "no tag" / "malformed shape" cases — the version package's
// parser-side errors here are silently ignored to avoid
// double-reporting; the static check surfaces only as a
// "supported-major" rule violation when the tag is otherwise
// well-formed.
//
// The companion runtime checks (no major-downgrade, warn-on-skip-
// minor) live in the reconciler — admission cannot enforce them
// without `oldObj` continuity, which GitOps re-apply patterns
// silently lose. See `internal/version/compat.go` package doc for
// the full split rationale.
func validateValkeyImageMajorSupported(ci valkeyv1beta1.ContainerImage) field.ErrorList {
	if ci.Tag == "" {
		return nil // tag-required error already raised by validateImageTagFormat
	}
	parsed, err := version.ParseValkeyTag(ci.Repository + ":" + ci.Tag)
	if err != nil {
		// Tag does not parse as a Valkey major.minor shape (e.g.,
		// `latest`, `main`, custom-build tags). Stay silent here —
		// the existing `validateImageTagFormat` already warns on
		// floating tags and rejects empty/malformed-regex shapes.
		// We don't second-guess users running custom builds whose
		// tags don't follow the upstream Valkey shape.
		return nil
	}
	if !version.IsSupportedMajor(parsed) {
		imgPath := field.NewPath("spec", "image", "valkey", "tag")
		return field.ErrorList{field.NotSupported(imgPath, parsed.String(),
			majorStrings(version.SupportedMajors))}
	}
	return nil
}

// majorStrings renders an int slice as a string slice for use with
// field.NotSupported (which takes []string-shaped allowed values).
func majorStrings(majors []int) []string {
	out := make([]string, len(majors))
	for i, m := range majors {
		out[i] = strconv.Itoa(m) + ".x"
	}
	return out
}

// validateProbeHandler accepts tcpSocket and loopback-only exec
// handlers for the valkey liveness probe. The defaulter now stamps an
// `exec: valkey-cli -h 127.0.0.1 ping` probe so the kubelet can
// detect frozen-process states (SIGSTOP'd, cgroup-frozen, kernel-
// stalled) that a tcpSocket probe — which only verifies the kernel TCP
// stack — would miss. httpGet and grpc are still rejected because they
// reintroduce HTTP / DNS dependencies the operator's probe invariants
// exclude.
func validateProbeHandler(path *field.Path, p *corev1.Probe) field.ErrorList {
	if p == nil {
		return nil
	}
	if p.HTTPGet == nil && p.GRPC == nil && (p.TCPSocket != nil || p.Exec != nil) {
		return nil
	}
	return field.ErrorList{
		field.Invalid(path.Child("handler"), describeProbeHandler(p),
			"valkey liveness probe handler must be tcpSocket or exec; httpGet / grpc handlers reintroduce HTTP / DNS dependencies the operator's probe invariants exclude"),
	}
}

func describeProbeHandler(p *corev1.Probe) string {
	switch {
	case p.Exec != nil:
		return "exec"
	case p.HTTPGet != nil:
		return "httpGet"
	case p.GRPC != nil:
		return "grpc"
	case p.TCPSocket != nil:
		return "tcpSocket"
	}
	return "unset"
}

// validateFeatureGates emits a Warning for every feature-gate key the
// operator does not yet recognise. Unknown keys are accepted (forward
// compatibility) so a user-set future gate doesn't admission-error
// against an older operator running in the cluster mid-upgrade.
//
// The known-gate allowlist names every feature-gate key the
// operator currently recognises; new gates land here as they ship.
// Every user-supplied key not in the allowlist emits a Warning so
// typos surface in `kubectl apply` output without blocking the
// request.
//
// A second pass emits a per-gate Warning for known keys whose
// effect is safety-critical (e.g. `UpgradePreflight: false` opts
// out of the major-version-downgrade rejection). The bypass stays
// functional, but a `kubectl apply` user gets an explicit signal
// that they wrote it — typo'd-to-false and copy-pasted-from-
// testbed cases both surface at admission time instead of silently
// taking effect on the next reconcile.
func validateFeatureGates(gates map[string]bool) admission.Warnings {
	if len(gates) == 0 {
		return nil
	}
	var known = map[string]bool{
		// UpgradePreflight: when explicitly set to false on a CR,
		// bypasses the reconciler's major-version-downgrade rejection
		// in checkValkeyImageTransition. Default (absent or true)
		// keeps the preflight enforced. See docs/FEATURE-GATES.md.
		"UpgradePreflight": true,
	}
	var warnings admission.Warnings

	// Surface explicit safety-bypass writes as their own Warning so
	// admission output names the bypass; the reconciler-side
	// ValkeyImageTransitionOverridden event only fires after the next
	// real downgrade attempt, which can be hours later.
	if disabled, set := gates["UpgradePreflight"]; set && !disabled {
		warnings = append(warnings,
			"spec.featureGates.UpgradePreflight=false disables the major-version-downgrade preflight in the reconciler; "+
				"cross-major data-format compatibility is not guaranteed. Intended for testbed / disaster-recovery only.")
	}

	keys := make([]string, 0, len(gates))
	for k := range gates {
		if !known[k] {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		slices.Sort(keys)
		warnings = append(warnings,
			fmt.Sprintf("featureGates: unknown keys %v; the operator will log a warning for each at startup", keys))
	}
	return warnings
}

func setOf(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, s := range items {
		m[s] = true
	}
	return m
}

// validateReadinessGateMaxLagBytes warns when a user explicitly sets
// `readinessGate.maxLagBytes: 0`. The defaulter preserves an explicit
// zero (the *int64 shape lets it distinguish unset from zero), but
// zero lag tolerance means a replica's `master_repl_offset -
// slave_repl_offset` would have to be exactly zero to mark Ready —
// which never happens in practice on a steadily-written primary.
// Accepting the value lets a user demand it for, e.g., a paused
// workload smoke test, but the warning surfaces the consequence so
// it's not a silent foot-gun.
func validateReadinessGateMaxLagBytes(o *valkeyv1beta1.Valkey) admission.Warnings {
	mlb := o.Spec.Valkey.ReadinessGate.MaxLagBytes
	if mlb == nil || *mlb != 0 {
		return nil
	}
	return admission.Warnings{
		"spec.valkey.readinessGate.maxLagBytes=0 means a replica is only Ready when its replication offset matches the primary's exactly. On a steadily-written primary this never happens, so replicas would never join the read-only Service endpoints. The value is accepted (lab use), but you almost certainly want a small positive byte budget instead.",
	}
}

// The sentinel timing-floor bounds are package-level so both the hard-floor
// reject (validateSentinelTimingFloors) and the accepted-but-aggressive
// soft-warn band (sentinelTimingDeviations) read the same numbers. The two
// bands are complementary and must stay adjacent — a single source prevents
// a gap or overlap opening between them.
const (
	// downAfterFloor is the hard floor for spec.sentinel.downAfterMilliseconds,
	// lowered from 30000 → 1000 — the 30s floor (Sentinel's documented
	// "production safe" value) made sub-15s recovery SLOs mathematically
	// unreachable. 1s is conservative against false-positive failovers on
	// stable in-cluster networks; values below 1s still require the
	// AllowAggressiveTimeoutsAnnotation escape.
	downAfterFloor = int32(1000)
	// failoverTimeoutFloor is the hard floor for spec.sentinel.failoverTimeout.
	failoverTimeoutFloor = int32(180000)
	// downAfterRecommended is Sentinel's documented production-safe down-after
	// value. Values in [downAfterFloor, downAfterRecommended) are accepted but
	// flagged (WarnAggressiveTimeouts): on a CPU-throttled or oversubscribed
	// node a transient stall can exceed an aggressive down-after and look like
	// a crash, tripping a spurious +sdown/failover. Recommendation line only —
	// the hard floor stays downAfterFloor.
	downAfterRecommended = int32(30000)
)

// validateSentinelTimingFloors enforces the sentinel timing floors
// (downAfterMilliseconds >= 1000, failoverTimeout >= 180000) at the
// validating-webhook layer rather than at CEL. CEL bound to a
// SentinelPodSpec/ValkeySpec `self` cannot read `metadata.annotations`,
// so a CR carrying `velkir.ioxie.dev/allow-aggressive-timeouts=true` and
// sub-floor timing would be rejected by CEL before the annotation
// could grant a bypass. The webhook can read both, so the floor
// becomes "reject sub-floor without the annotation; emit Warning when
// the annotation is set to "true"".
//
// The floors only apply to mode=sentinel CRs (other modes don't run
// sentinel monitors). The defaulter stamps the floor values when the
// fields are omitted, so the only way to hit the sub-floor branch is
// for a user to explicitly set lower values — opt-in territory.
// validateSentinelTimingFloors returns the timing warnings, the hard-floor
// rejection errors, and whether a sub-floor value was waved through under
// the allow-aggressive-timeouts bypass annotation (the third return drives
// the EventAggressiveTimeoutsAccepted audit entry).
func validateSentinelTimingFloors(o *valkeyv1beta1.Valkey) (admission.Warnings, field.ErrorList, bool) {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel || o.Spec.Sentinel == nil {
		return nil, nil, false
	}
	bypass := o.Annotations[AllowAggressiveTimeoutsAnnotation] == triggerTrueValue

	var (
		warnings admission.Warnings
		errs     field.ErrorList
	)
	sentinelPath := field.NewPath("spec", "sentinel")

	// aggressiveAccepted records that at least one sub-floor timing value
	// was waved through under the bypass annotation, so the caller can
	// audit the escape-hatch accept.
	aggressiveAccepted := false
	check := func(value, floor int32, fieldName, friendlyName string) {
		if value >= floor {
			return
		}
		path := sentinelPath.Child(fieldName)
		if bypass {
			aggressiveAccepted = true
			warnings = append(warnings,
				fmt.Sprintf("spec.sentinel.%s=%d is below the recommended floor of %d (%s); accepting because %s=%q is set, but the operator may take longer to detect a failed primary or may abandon a failover prematurely",
					fieldName, value, floor, friendlyName, AllowAggressiveTimeoutsAnnotation, triggerTrueValue))
			return
		}
		errs = append(errs, field.Invalid(path, value,
			fmt.Sprintf("%s must be >= %d (set %s=\"true\" annotation to bypass at your own risk)",
				friendlyName, floor, AllowAggressiveTimeoutsAnnotation)))
	}
	check(o.Spec.Sentinel.DownAfterMilliseconds, downAfterFloor, "downAfterMilliseconds", "down-after-milliseconds")
	check(o.Spec.Sentinel.FailoverTimeout, failoverTimeoutFloor, "failoverTimeout", "failover-timeout")

	// The accepted-but-aggressive band [downAfterFloor, downAfterRecommended)
	// is reported as the WarnAggressiveTimeouts deviation (Deviations()) — the
	// single source of truth for both the admission warning and the durable
	// reconciler Event. Sub-floor values are handled above (rejected, or
	// floor-warned under the bypass annotation).
	return warnings, errs, aggressiveAccepted
}

// validateSentinelQoS warns when a sentinel-mode CR's resources won't
// land the Guaranteed QoS class. Sentinel reliability is load-bearing
// for the whole topology, so the defaulter mirrors a one-sided
// requests/limits spec into Guaranteed shape (see stampSentinelDefaults).
// It cannot, however, override a user who pinned BOTH sides to a
// Burstable shape, nor complete a partial spec missing a cpu/memory
// limit — those reach here and warn. Warn-only: Burstable sentinels run,
// they're just more eviction-prone under node pressure. Guaranteed
// requires, per container, a cpu+memory limit with limit == request (an
// absent request defaults to the limit, so that case is still Guaranteed).
func validateSentinelQoS(o *valkeyv1beta1.Valkey) admission.Warnings {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel || o.Spec.Sentinel == nil {
		return nil
	}
	r := o.Spec.Sentinel.Resources
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		lim, hasLim := r.Limits[name]
		if !hasLim || lim.IsZero() {
			return admission.Warnings{fmt.Sprintf(
				"spec.sentinel.resources has no %s limit; sentinel pods will not be Guaranteed QoS and are more likely to be evicted under node pressure — set requests==limits on cpu and memory, or omit both to accept the operator's Guaranteed defaults",
				name)}
		}
		if req, hasReq := r.Requests[name]; hasReq && !req.Equal(lim) {
			return admission.Warnings{fmt.Sprintf(
				"spec.sentinel.resources %s request (%s) != limit (%s); sentinel pods will be Burstable rather than Guaranteed QoS and are more likely to be evicted under node pressure",
				name, req.String(), lim.String())}
		}
	}
	return nil
}

// validateSentinelHA emits the sub-HA soft-warn: sentinel mode is
// designed for HA topology (≥2 valkey replicas so the sentinel pool
// has something to watch + fail over to). A single-replica sentinel CR
// is accepted on purpose (lab use, smoke testing) but admission emits
// a Warning so the user knows what they signed up for. The reconciler
// carries the matching `Degraded=True reason=HANotMet` status
// condition; webhook only owns the admission-time signal.
//
// Returns nil for any non-sentinel mode or when valkey.replicas is at
// least 2 — the warning channel stays quiet for HA-shaped CRs.
func validateSentinelHA(o *valkeyv1beta1.Valkey) admission.Warnings {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel {
		return nil
	}
	if o.Spec.Valkey.Replicas >= 2 {
		return nil
	}
	return admission.Warnings{
		fmt.Sprintf("mode=sentinel with spec.valkey.replicas=%d is sub-HA: at least 2 valkey replicas are required for sentinel to perform a failover. The CR is accepted (lab use), but the reconciler will mark Degraded=True with reason=HANotMet.",
			o.Spec.Valkey.Replicas),
	}
}

// validateSentinelReplicaParity emits a Warning when the sentinel pool
// has an even number of pods. Valkey's quorum semantics work with any
// count ≥ 2, but odd counts are operationally preferred:
//
//   - even counts allow split-vote scenarios where two equal-sized
//     factions can each form a "majority" depending on which sentinel
//     drops first, slowing failover decisions
//   - the typical 3 / 5 / 7 progression matches the sentinel pool sizes
//     production deployments actually use
//
// Warn-only: operators running deliberately-even pools (lab smoke,
// infra constraints) can disregard. Returning admission.Warnings with
// no error is the right shape — escalating to a Deny would be an
// over-reach for a split-vote risk that's not a certainty.
//
// Returns nil for non-sentinel modes, missing sentinel block, replicas
// below 4 (3 is odd; 1 and 2 are CEL-rejected anyway), and odd counts.
func validateSentinelReplicaParity(o *valkeyv1beta1.Valkey) admission.Warnings {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel || o.Spec.Sentinel == nil {
		return nil
	}
	if o.Spec.Sentinel.Replicas < 4 || o.Spec.Sentinel.Replicas%2 != 0 {
		return nil
	}
	return admission.Warnings{
		fmt.Sprintf("spec.sentinel.replicas=%d is even; odd counts are preferred for sentinel quorum stability (split-vote risk on equal-sized factions). The CR is accepted, but consider 3, 5, or 7.",
			o.Spec.Sentinel.Replicas),
	}
}

// validateSentinelQuorumSubMajority emits a Warning when
// spec.sentinel.quorum is below the sentinel pool majority
// (ceil((replicas+1)/2), i.e. (replicas+2)/2 in integer math — the same
// value the defaulter stamps when quorum is unset). The existing
// even-parity warn only fires on even pool sizes; this surfaces the
// odd-replica sub-majority shapes it misses (e.g. 5/2, 7/2, 7/3) as
// well as even ones (4/2, 6/3).
//
// A sub-majority quorum is admitted on purpose — a hard reject would
// forbid legitimate fast-+odown configs and remove operator agency. It
// flows verbatim into sentinel.conf's `SENTINEL MONITOR … <quorum>`
// line, lowering the +odown declaration threshold so a minority faction
// can escalate a transient stall to objective-down faster. The
// operator's own split-brain relabel guard is unaffected — it clamps to
// pool-majority regardless of this value — so warn-only is the right
// shape: surface the intent at admission without rejecting.
//
// Returns nil for non-sentinel modes, a missing sentinel block, an
// unset/zero quorum (the defaulter stamps the majority in that case),
// and any quorum at or above majority.
func validateSentinelQuorumSubMajority(o *valkeyv1beta1.Valkey) admission.Warnings {
	if o.Spec.Mode != valkeyv1beta1.ModeSentinel || o.Spec.Sentinel == nil {
		return nil
	}
	quorum := o.Spec.Sentinel.Quorum
	if quorum <= 0 {
		return nil
	}
	majority := (o.Spec.Sentinel.Replicas + 2) / 2
	if quorum >= majority {
		return nil
	}
	return admission.Warnings{
		fmt.Sprintf("spec.sentinel.quorum=%d is below the sentinel pool majority of %d (replicas=%d): a sub-majority quorum lowers the +odown declaration threshold, letting a minority faction escalate a transient stall to objective-down faster. The CR is accepted, but consider quorum >= %d.",
			quorum, majority, o.Spec.Sentinel.Replicas, majority),
	}
}

// Compile-time interface assertion: typed Validator contract from
// controller-runtime v0.23.
var _ admission.Validator[*valkeyv1beta1.Valkey] = (*ValkeyCustomValidator)(nil)

// _ unused import asserts; kept for future webhook scaffolding.
var _ = webhook.Admission{}
