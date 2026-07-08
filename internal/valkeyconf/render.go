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

// Package valkeyconf renders the valkey.conf the operator stamps into
// the per-CR <cr>-valkey-conf ConfigMap. The pod's render-config init
// container substitutes _POD_IP_ (and, in replication mode, appends a
// `replicaof` line) before valkey-server reads the file from emptyDir.
//
// Three layers, highest precedence first:
//
//  1. Mandatory directives — operator-owned, never overridable. Cover
//     the floor required for IP-only peer addressing and the protected-
//     mode / port pins velkir commits to.
//  2. ConfigurationOverrides map — directive→value pairs. A line in the
//     user-supplied `Configuration` whose first token matches an override
//     key is dropped before the overrides block is appended.
//  3. Configuration string — raw user snippet, pass-through for any line
//     whose first token does not collide with the mandatory or override
//     layers.
//
// The validating webhook rejects banned directives upstream; the strip
// here is defense-in-depth so a future regressing webhook cannot leak
// operator-owned config to valkey-server.
package valkeyconf

import (
	"fmt"
	"sort"
	"strings"
)

// PodIPPlaceholder is the substring the init container replaces with the
// pod's actual status.podIP from the Downward API. Exported so the
// controller's init-script ConfigMap can reference the same literal.
const PodIPPlaceholder = "_POD_IP_"

// ValkeyPort is the canonical client-plane port. Mandatory in the
// rendered config; not user-tunable.
const ValkeyPort = 6379

// OperatorOwnedDirectives are the valkey.conf directives the operator
// owns and forbids in user-supplied config (spec.valkey.configuration and
// spec.valkey.configurationOverrides). It is the single source of truth
// shared by two layers:
//
//   - the validating webhook bans them at admission (the primary
//     control); and
//   - this renderer strips them defense-in-depth, so a regressed or
//     bypassed webhook cannot leak an operator-owned directive into the
//     rendered valkey.conf.
//
// Matched case-insensitively on the directive's first token. The
// multi-word `sentinel *` entries are sentinel.conf directives (never
// valid in valkey.conf); they collapse to the `sentinel` first token for
// the renderer's strip, which is harmless since no sentinel directive
// belongs in valkey.conf. min-replicas-* have typed-field equivalents
// (spec.valkey.minReplicasToWrite, spec.valkey.minReplicasMaxLag) the
// operator validates and renders; banning the raw form prevents the
// "user sets both, undefined precedence" failure mode.
var OperatorOwnedDirectives = []string{
	"bind",
	"daemonize",
	"masterauth",
	"min-replicas-max-lag",
	"min-replicas-to-write",
	"port",
	"protected-mode",
	"replica-announce-ip",
	"replica-announce-port",
	"replicaof",
	"requirepass",
	"sentinel announce-hostnames",
	"sentinel monitor",
	"sentinel resolve-hostnames",
	"slaveof",
	"unixsocket",
}

// ContainsInjectionChars reports whether s carries a character that can
// break a single config value across lines or terminate it early —
// newline, carriage return, or NUL. A config value is single-line by
// contract; a newline in a user override value renders as a second, live
// directive (e.g. `appendonly "yes\nrequirepass x"` → two lines),
// smuggling an operator-owned directive past the key-only banlist. The
// webhook rejects such values at admission; the renderer drops them as a
// backstop.
//
// The set is exactly newline/CR/NUL. Valkey splits its config into lines
// on '\n' alone, so '\n' is the only directive-injection vector; '\r' and
// NUL are rejected conservatively (CR rides a '\r\n' pair; NUL corrupts
// the line). Other whitespace valkey treats as in-line argument separators
// (space, tab, '\v', '\f') — like space/tab they cannot start a new line,
// so a banned word after one is only an argument to the line's first
// directive, never a directive of its own. Adding them here would reject
// harmless keys/values without closing any injection path.
func ContainsInjectionChars(s string) bool {
	return strings.ContainsAny(s, "\n\r") || ContainsNUL(s)
}

// ContainsNUL reports whether s carries a NUL byte. NUL is illegal in
// any rendered config — a single-line override value and the multi-line
// raw Configuration snippet alike — so the webhook rejects it at
// admission for both inputs. It is the NUL-only half of
// ContainsInjectionChars: the raw snippet legitimately spans lines
// (newline/CR are separators there, not injection), so only this half of
// the control-char policy applies to it. Defining it here keeps the
// webhook and renderer sharing one notion of "illegal byte".
func ContainsNUL(s string) bool {
	return strings.ContainsRune(s, '\x00')
}

// Inputs is the renderer's surface. Persistent flips the `dir` directive
// between /data (PVC-backed) and /tmp (emptyDir). Configuration is the
// raw user snippet (spec.valkey.configuration). Overrides is the keyed
// override map (spec.valkey.configurationOverrides).
//
// MinReplicasToWrite / MinReplicasMaxLag carry the resolved
// spec.valkey.minReplicas* values. The defaulter stamps them (1 / 10) for
// replication and sentinel modes and leaves them nil for standalone, so a
// non-nil pointer here means "render the operator-owned write-loss floor"
// and nil means "omit it" (standalone has no replicas — a
// `min-replicas-to-write 1` there would wedge every write). They render in
// the mandatory layer so user config can never clobber the floor.
type Inputs struct {
	Persistent         bool
	Configuration      string
	Overrides          map[string]string
	MinReplicasToWrite *int32
	MinReplicasMaxLag  *int32
}

// Render returns the valkey.conf bytes the operator writes to the
// <cr>-valkey-conf ConfigMap. Output is deterministic — same Inputs
// produce byte-identical output, so the SHA-256 hash that drives
// pod-template rollout triggers is stable across reconciles.
func Render(in Inputs) string {
	mandatory := mandatoryDirectives(in)
	mandatoryKeys := mandatoryKeyset(mandatory)
	banned := bannedDirectiveKeys()
	overrides := filterOverrides(in.Overrides, mandatoryKeys, banned)
	skip := mergeKeysets(mandatoryKeys, overrides)
	for k := range banned {
		skip[k] = true
	}

	var b strings.Builder
	b.WriteString(headerComment)
	for _, line := range mandatory {
		b.WriteString(line)
		b.WriteByte('\n')
	}

	user := stripUserDirectives(in.Configuration, skip)
	if user != "" {
		b.WriteByte('\n')
		b.WriteString("# spec.valkey.configuration\n")
		b.WriteString(user)
	}

	if len(overrides) > 0 {
		b.WriteByte('\n')
		b.WriteString("# spec.valkey.configurationOverrides\n")
		keys := make([]string, 0, len(overrides))
		for k := range overrides {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("%s %s\n", k, overrides[k]))
		}
	}

	return b.String()
}

const headerComment = `# Managed by velkir. Do not edit by hand; this ConfigMap is
# overwritten on every reconcile.
#
# _POD_IP_ is substituted by the render-config init container at pod
# start using $(POD_IP) from the Downward API.
`

// mandatoryDirectives returns the operator-owned floor in deterministic
// order. The order is part of the contract — changing it changes the
// rendered hash and rolls every existing pod, so additions go at the
// end.
func mandatoryDirectives(in Inputs) []string {
	dir := "/tmp"
	if in.Persistent {
		dir = "/data"
	}
	out := []string{
		fmt.Sprintf("dir %s", dir),
		fmt.Sprintf("port %d", ValkeyPort),
		"protected-mode no",
		fmt.Sprintf("replica-announce-ip %s", PodIPPlaceholder),
		fmt.Sprintf("replica-announce-port %d", ValkeyPort),
	}
	// Write-loss floor. Emitted operator-owned (and stripped
	// from the user layers via the banlist) so a primary partitioned from
	// all its replicas stops accepting writes that can never replicate and
	// would be lost on the next failover. Present only when the defaulter
	// stamped the typed fields — i.e. replication/sentinel modes; nil
	// (standalone) omits them. Appended last to keep the rollout hash
	// stable for the existing directives.
	if in.MinReplicasToWrite != nil {
		out = append(out, fmt.Sprintf("min-replicas-to-write %d", *in.MinReplicasToWrite))
	}
	if in.MinReplicasMaxLag != nil {
		out = append(out, fmt.Sprintf("min-replicas-max-lag %d", *in.MinReplicasMaxLag))
	}
	return out
}

// mandatoryKeyset returns the lower-cased directive first-tokens of the
// mandatory layer. Used both for stripping the user string AND for
// filtering an override map that tries to redefine a mandatory key.
func mandatoryKeyset(mandatory []string) map[string]bool {
	out := make(map[string]bool, len(mandatory))
	for _, line := range mandatory {
		if d := directive(line); d != "" {
			out[d] = true
		}
	}
	return out
}

// filterOverrides drops any override the renderer must not emit:
//   - keys OR values carrying a newline/CR/NUL: each override renders as
//     `<key> <value>`, so a control char in either half would split the
//     line into a second live directive (or truncate it at a NUL),
//     smuggling operator-owned config past the first-token key banlist;
//   - keys colliding with a mandatory or operator-owned directive
//     (matched on the normalised first token, so a whitespace-padded
//     ` requirepass` is caught the same as `requirepass`).
//
// Mandatory and operator-owned directives win outright — neither the
// override layer nor a smuggled key/value may rewrite them. Returns a
// copy keyed on the user-supplied (case-preserving) string so the
// rendered output reflects what the user wrote.
func filterOverrides(overrides map[string]string, mandatoryKeys, bannedKeys map[string]bool) map[string]string {
	if len(overrides) == 0 {
		return nil
	}
	out := make(map[string]string, len(overrides))
	for k, v := range overrides {
		if ContainsInjectionChars(k) || ContainsInjectionChars(v) {
			continue
		}
		key := directive(k)
		if mandatoryKeys[key] || bannedKeys[key] {
			continue
		}
		out[k] = v
	}
	return out
}

// bannedDirectiveKeys returns the lower-cased first tokens of
// OperatorOwnedDirectives — the form stripUserDirectives and
// filterOverrides compare against. Multi-word entries collapse to their
// first token (`sentinel monitor` → `sentinel`).
func bannedDirectiveKeys() map[string]bool {
	out := make(map[string]bool, len(OperatorOwnedDirectives))
	for _, d := range OperatorOwnedDirectives {
		if k := directive(d); k != "" {
			out[k] = true
		}
	}
	return out
}

// BannedDirectiveKeys returns the set of operator-owned directive
// first-tokens the renderer strips from user override keys — the same
// keyset filterOverrides consults. Exported so the validating webhook
// rejects at admission exactly what the renderer would otherwise drop
// silently, sharing one normalisation rather than re-deriving a narrower
// (whole-string) match. The map is freshly built per call; callers may
// keep and mutate it.
func BannedDirectiveKeys() map[string]bool {
	return bannedDirectiveKeys()
}

// NormalizeDirectiveKey returns the lower-cased first whitespace-
// delimited token of an override key — the form BannedDirectiveKeys is
// keyed on. A padded ` requirepass` and a multi-token `requirepass evil`
// both normalise to `requirepass`; every `sentinel *` key collapses to
// `sentinel`, matching the renderer's strip. Empty for a blank or
// comment-prefixed key (which the renderer emits inert).
func NormalizeDirectiveKey(key string) string {
	return directive(key)
}

// mergeKeysets unions the mandatory-key set with the lower-cased keys of
// the (already-filtered) overrides map. The result is the skip-set the
// user-string filter consults.
func mergeKeysets(mandatoryKeys map[string]bool, overrides map[string]string) map[string]bool {
	out := make(map[string]bool, len(mandatoryKeys)+len(overrides))
	for k := range mandatoryKeys {
		out[k] = true
	}
	for k := range overrides {
		out[strings.ToLower(strings.TrimSpace(k))] = true
	}
	return out
}

// directive returns the lower-cased first whitespace-delimited token of
// a non-blank, non-comment line, or "" if the line carries no
// directive. Multi-word directives (`sentinel monitor`) are not the
// renderer's concern — the webhook bans them in the user-supplied
// layers, and sentinel configs render through their own template
// elsewhere.
func directive(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return ""
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToLower(fields[0])
}

// stripUserDirectives walks the raw user snippet line-by-line and drops
// any line whose first token collides with the skip set. Comments and
// blank lines are preserved verbatim. Returns "" for an empty result so
// the caller can elide the section header entirely.
func stripUserDirectives(raw string, skip map[string]bool) string {
	if raw == "" {
		return ""
	}
	var out strings.Builder
	for line := range strings.SplitSeq(raw, "\n") {
		if d := directive(line); d != "" && skip[d] {
			continue
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	s := out.String()
	// Trim down to a single trailing newline so the section's whitespace
	// is well-formed regardless of whether the user terminated their
	// snippet with one. An all-stripped snippet collapses to "".
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return ""
	}
	return s + "\n"
}
