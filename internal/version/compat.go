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

// Package version implements the Valkey image-tag compatibility rules.
//
// The operator constrains which Valkey image-tag transitions it will
// reconcile, with two enforcement points:
//
//   - Static admission checks (cheap, oldObj-free): semver shape +
//     supported-major. Lives in the validating webhook
//     (internal/webhook/v1beta1).
//   - Runtime transition checks (need oldObj continuity): no major
//     downgrade, warn-on-skip-minor. Lives in this package, called
//     from the reconciler (internal/controller).
//
// # Why the split exists
//
// The no-skip-minor / no-major-downgrade rules require comparing the
// new image against what the cluster is currently running. Admission
// cannot reliably observe that transition because Flux re-apply
// patterns silently lose `oldObj` continuity: `kubectl apply
// --server-side` re-applying a manifest that matches the live state
// byte-for-byte produces an admission request where `oldObj == newObj`
// (no transition signal) or, on certain create-shaped re-applies, no
// `oldObj` at all. A future reviewer hardening admission with
// stateful checks would either:
//
//   - Generate false negatives during normal Flux operation (the
//     cluster's actual image transition is invisible to admission), or
//   - Generate false positives by reading state out-of-band (e.g.,
//     listing the StatefulSet from admission), which contradicts the
//     "admission is a pure function of the request" contract and
//     introduces a race window the validating webhook is not
//     supposed to occupy.
//
// Runtime enforcement at reconcile time observes the real transition
// (CR's spec.valkey.image vs the existing StatefulSet's currently-
// running image) and is the only place these rules can fire correctly.
//
// # Compatibility table source of truth
//
// `SupportedMajors` below is the Go-literal source of truth derived
// from `docs/versions.md`. The two MUST stay in sync (a future CI lint
// can verify; today this is operator-discipline). When a new Valkey
// major lands, both the markdown table and `SupportedMajors` get
// updated in the same PR.
package version

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ValkeyVersion is the parsed major.minor of a Valkey image tag.
// Patch is intentionally not tracked — the version-compat rules
// concern major and minor only; patch updates are always allowed.
type ValkeyVersion struct {
	Major int
	Minor int
}

// String renders a ValkeyVersion in the canonical "major.minor" form.
func (v ValkeyVersion) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor)
}

// SupportedMajors is the set of Valkey major versions the operator
// permits at the validating webhook (admission-only gate). Runtime
// reconciliation logic is major-agnostic — IsSupportedMajor has no
// production callers beyond the webhook. Source of truth:
// docs/versions.md "Compatibility matrix" row(s); update both files
// in the same PR.
//
//   - Valkey 8 is the GA-tested major (e2e + upgrade matrix run against it).
//   - Valkey 9 is admitted as of operator v1.0; since the runtime is
//     major-agnostic, the operator's state machines work the same
//     against 9.x. Treat 9.x as best-effort during the v1.0 alpha —
//     the e2e matrix has not yet added a 9.x cohort; deploys are at
//     the operator-of-the-operator's discretion.
var SupportedMajors = []int{8, 9}

// IsSupportedMajor reports whether v.Major is in SupportedMajors.
// Callable from both admission and runtime — the static-check
// portion of the version-compat rules.
func IsSupportedMajor(v ValkeyVersion) bool {
	return slices.Contains(SupportedMajors, v.Major)
}

// IsDowngrade reports whether `to` is a major-version downgrade
// relative to `from` (i.e., to.Major < from.Major). Major-version
// downgrades are unconditionally rejected at the runtime layer:
// data-format compatibility across majors is not a guarantee
// upstream Valkey makes, and the operator cannot safely roll back
// the on-disk RDB / AOF.
//
// Equal-major or major-upgrade transitions return false (allowed
// from this rule's perspective; the skip-minor rule applies
// separately).
func IsDowngrade(from, to ValkeyVersion) bool {
	return to.Major < from.Major
}

// IsSkipMinor reports whether `to` skips a minor version within the
// same major (i.e., to.Major == from.Major && to.Minor - from.Minor > 1).
// Skip-minor transitions are allowed but trigger a Warning: per
// docs/versions.md "Skip-version policy", one-minor-skip is
// supported; two-or-more-minor skips are best-effort during alpha
// and validated only on the current-1 → current path in the
// upgrade matrix.
//
// Equal-major-but-decreasing-minor returns false (that's a
// minor-downgrade, which is not gated by this rule today — it would
// be a separate runtime check if added).
func IsSkipMinor(from, to ValkeyVersion) bool {
	return to.Major == from.Major && to.Minor-from.Minor > 1
}

// ErrNoTag indicates the input string had no `:tag` suffix to parse.
var ErrNoTag = errors.New("image reference has no tag")

// ErrMalformedTag indicates the tag was present but did not parse
// as a Valkey major.minor[.patch] form.
var ErrMalformedTag = errors.New("tag is not a Valkey semver shape")

// ParseValkeyTag extracts the (major, minor) version from a
// container-image reference of the form `[registry/]repo:tag` or
// `[registry/]repo:tag@sha256:...`. Strips the registry/repo prefix
// and any digest suffix; parses the tag as a Valkey major.minor or
// major.minor.patch.
//
// Variant suffixes after a dash (e.g., `8.1.6-alpine`, `8.1-rc1`) are
// allowed and ignored — only the leading numeric major.minor is
// load-bearing for the version-compat rules. The patch component
// (when present) is parsed for shape but discarded; patch updates are
// always allowed by the rules.
//
// Returned errors:
//
//   - ErrNoTag: input has no `:` (or `:` only inside a registry-port
//     prefix with no tag after the repo path)
//   - ErrMalformedTag: tag's leading numeric portion has fewer than 2
//     dot-separated numeric components, or any leading component does
//     not parse as a non-negative integer
//
// Examples:
//
//	ParseValkeyTag("valkey:8.0")              -> ValkeyVersion{8, 0}, nil
//	ParseValkeyTag("valkey:8.0.1")            -> ValkeyVersion{8, 0}, nil  (patch dropped)
//	ParseValkeyTag("valkey:8.1.6-alpine")     -> ValkeyVersion{8, 1}, nil  (variant suffix dropped)
//	ParseValkeyTag("valkey:8.1-rc1")          -> ValkeyVersion{8, 1}, nil  (pre-release suffix dropped)
//	ParseValkeyTag("docker.io/valkey/valkey:8.1") -> ValkeyVersion{8, 1}, nil
//	ParseValkeyTag("registry.local:5000/valkey/valkey:8.2") -> ValkeyVersion{8, 2}, nil
//	ParseValkeyTag("valkey:8.0@sha256:abc...")              -> ValkeyVersion{8, 0}, nil  (digest stripped)
//	ParseValkeyTag("valkey")                  -> {}, ErrNoTag
//	ParseValkeyTag("valkey:8")                -> {}, ErrMalformedTag (need at least major.minor)
//	ParseValkeyTag("valkey:latest")           -> {}, ErrMalformedTag
func ParseValkeyTag(ref string) (ValkeyVersion, error) {
	// Strip a digest suffix first — it can contain `@sha256:...`
	// where the `:` would otherwise confuse the tag-extractor.
	if at := strings.Index(ref, "@"); at >= 0 {
		ref = ref[:at]
	}
	// Find the LAST `:` — earlier `:` may belong to a registry-port
	// (`registry.local:5000/repo:tag`) which we don't want to split on.
	colon := strings.LastIndex(ref, ":")
	if colon < 0 {
		return ValkeyVersion{}, ErrNoTag
	}
	// If the last `:` is followed by a `/` somewhere, it was a
	// registry-port colon and the repo has no tag.
	tag := ref[colon+1:]
	if strings.Contains(tag, "/") {
		return ValkeyVersion{}, ErrNoTag
	}
	// Strip a variant suffix (e.g., `-alpine`, `-rc1`, `-debian`) —
	// only the leading numeric portion drives the version rules.
	if dash := strings.Index(tag, "-"); dash >= 0 {
		tag = tag[:dash]
	}
	parts := strings.Split(tag, ".")
	if len(parts) < 2 {
		return ValkeyVersion{}, fmt.Errorf("%w: need at least major.minor, got %q", ErrMalformedTag, tag)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 0 {
		return ValkeyVersion{}, fmt.Errorf("%w: major %q is not a non-negative integer", ErrMalformedTag, parts[0])
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < 0 {
		return ValkeyVersion{}, fmt.Errorf("%w: minor %q is not a non-negative integer", ErrMalformedTag, parts[1])
	}
	// Optional patch is parsed only to validate shape; the value is
	// discarded per the package contract (patch updates are always
	// allowed; the rules concern only major + minor).
	if len(parts) >= 3 {
		if _, err := strconv.Atoi(parts[2]); err != nil {
			return ValkeyVersion{}, fmt.Errorf("%w: patch %q is not a non-negative integer", ErrMalformedTag, parts[2])
		}
	}
	return ValkeyVersion{Major: major, Minor: minor}, nil
}
