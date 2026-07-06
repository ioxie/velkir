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
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/valkeyconf"
)

func validStandalone(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			Image: valkeyv1beta1.ImageSpec{
				Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
				Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
				Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
			},
		},
	}
	if mutate != nil {
		mutate(v)
	}
	return v
}

func runValidate(t *testing.T, v *valkeyv1beta1.Valkey) (warnings []string, err error) {
	t.Helper()
	w, err := (&ValkeyCustomValidator{}).ValidateCreate(context.Background(), v)
	return w, err
}

func mustReject(t *testing.T, v *valkeyv1beta1.Valkey, wantSubstr string) {
	t.Helper()
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatalf("expected rejection containing %q, got nil error", wantSubstr)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("error %q does not contain expected substring %q", err.Error(), wantSubstr)
	}
}

func mustAccept(t *testing.T, v *valkeyv1beta1.Valkey) {
	t.Helper()
	if _, err := runValidate(t, v); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
}

// --- Operator-trigger annotation rule ---

func TestValidator_OperatorTriggerAnnotation_AcceptsLiteralTrue(t *testing.T) {
	v := validStandalone("trig-true", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{"velkir.ioxie.dev/paused": "true"}
	})
	mustAccept(t, v)
}

func TestValidator_OperatorTriggerAnnotation_RejectsTrueWithCapital(t *testing.T) {
	v := validStandalone("trig-True", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{"velkir.ioxie.dev/paused": "True"}
	})
	mustReject(t, v, `must be the literal string "true"`)
}

func TestValidator_OperatorTriggerAnnotation_RejectsOne(t *testing.T) {
	v := validStandalone("trig-one", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{"velkir.ioxie.dev/force-rotate": "1"}
	})
	mustReject(t, v, `must be the literal string "true"`)
}

func TestValidator_OperatorTriggerAnnotation_RejectsEmpty(t *testing.T) {
	v := validStandalone("trig-empty", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{"velkir.ioxie.dev/accept-pvc-loss": ""}
	})
	mustReject(t, v, `must be the literal string "true"`)
}

func TestValidator_OperatorTriggerAnnotation_IgnoresUnrelatedAnnotations(t *testing.T) {
	v := validStandalone("trig-unrelated", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{"team": "platform", "owner": "sre"}
	})
	mustAccept(t, v)
}

// --- Reserved-label protection ---

func TestValidator_ReservedLabel_RejectsManagedBy(t *testing.T) {
	v := validStandalone("label-managed-by", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodLabels = map[string]string{"app.kubernetes.io/managed-by": "user"}
	})
	mustReject(t, v, "reserved for the operator")
}

func TestValidator_ReservedLabel_RejectsRolePin(t *testing.T) {
	v := validStandalone("label-role", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodLabels = map[string]string{"velkir.ioxie.dev/role": "primary"}
	})
	mustReject(t, v, "reserved for the operator")
}

func TestValidator_ReservedLabel_AcceptsUserLabels(t *testing.T) {
	v := validStandalone("label-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodLabels = map[string]string{"team": "platform", "tier": "hot"}
	})
	mustAccept(t, v)
}

func TestValidator_ReservedAnnotation_RejectsValkeyXiesIoPrefix(t *testing.T) {
	v := validStandalone("ann-prefix", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodAnnotations = map[string]string{"velkir.ioxie.dev/anything": "x"}
	})
	mustReject(t, v, "reserved for the operator")
}

func TestValidator_ReservedAnnotation_RejectsManagedByExact(t *testing.T) {
	v := validStandalone("ann-managed-by", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodAnnotations = map[string]string{"app.kubernetes.io/managed-by": "x"}
	})
	mustReject(t, v, "reserved for the operator")
}

func TestValidator_ReservedAnnotation_AcceptsUserPrefix(t *testing.T) {
	v := validStandalone("ann-user", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodAnnotations = map[string]string{"app.kubernetes.io/component": "x"}
	})
	mustAccept(t, v)
}

// --- Config banlist ---

func TestValidator_ConfigBanlist_RejectsReplicaAnnounceIPInRaw(t *testing.T) {
	v := validStandalone("conf-replica-ip", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "maxmemory 100mb\nreplica-announce-ip 10.0.0.1\n"
	})
	mustReject(t, v, "replica-announce-ip")
}

func TestValidator_ConfigBanlist_RejectsBitnamiCommentNoFalsePositive(t *testing.T) {
	v := validStandalone("conf-comment", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "# replica-announce-ip 10.0.0.1 (do not enable)\nmaxmemory 100mb\n"
	})
	mustAccept(t, v)
}

func TestValidator_ConfigBanlist_RejectsSentinelMonitorMultiword(t *testing.T) {
	v := validStandalone("conf-sentinel-monitor", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "sentinel monitor mymaster 10.0.0.1 6379 2\n"
	})
	mustReject(t, v, "sentinel monitor")
}

func TestValidator_ConfigBanlist_RejectsBindInOverrideMap(t *testing.T) {
	v := validStandalone("conf-bind-map", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"bind": "0.0.0.0"}
	})
	mustReject(t, v, "bind")
}

func TestValidator_ConfigBanlist_RejectsSlaveofCaseInsensitive(t *testing.T) {
	v := validStandalone("conf-slaveof", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "SlaveOf 10.0.0.1 6379\n"
	})
	mustReject(t, v, "slaveof")
}

func TestValidator_ConfigBanlist_RejectsMinReplicasToWriteInRaw(t *testing.T) {
	// min-replicas-to-write is a typed field on the spec; the raw
	// config-string form must be rejected so a user can't set both
	// and hit undefined precedence.
	v := validStandalone("conf-min-replicas-write", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "min-replicas-to-write 1\n"
	})
	mustReject(t, v, "min-replicas-to-write")
}

func TestValidator_ConfigBanlist_RejectsMinReplicasMaxLagInOverrideMap(t *testing.T) {
	v := validStandalone("conf-min-replicas-lag", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"min-replicas-max-lag": "10"}
	})
	mustReject(t, v, "min-replicas-max-lag")
}

// The init container's render-valkey-conf.sh runs `sed s|_POD_IP_|...|g`
// over the rendered config. A user value carrying the placeholder
// substring would be silently rewritten (e.g. proc-title-template
// "valkey-_POD_IP_-instance" → "valkey-10.0.0.1-instance"), which
// surprises at runtime. Reject at admission with a message that names
// the token explicitly.

func TestValidator_ReservedRenderTokens_RejectsPodIPPlaceholderInRaw(t *testing.T) {
	v := validStandalone("conf-pod-ip-raw", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "proc-title-template \"valkey-_POD_IP_-instance\"\n"
	})
	// Pin the user-facing rationale, not just the token name — a future
	// refactor that drops the "init container" hint from the message
	// would otherwise still pass with a token-only assertion.
	mustReject(t, v, "reserved init-container placeholder")
}

func TestValidator_ReservedRenderTokens_RejectsPodIPPlaceholderInOverrideValue(t *testing.T) {
	v := validStandalone("conf-pod-ip-override", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{
			"proc-title-template": "valkey-_POD_IP_-instance",
		}
	})
	mustReject(t, v, "reserved init-container placeholder")
}

func TestValidator_ReservedRenderTokens_MessageNamesTokenAndSubstitutionMechanism(t *testing.T) {
	// Belt-and-suspenders: the message must (a) name the offending token
	// verbatim so a user can grep for it and (b) explain the runtime
	// substitution so the user knows why it's reserved. Pinning both
	// shapes prevents a "summarising" rewrite from dropping either.
	v := validStandalone("conf-pod-ip-msg", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "_POD_IP_"
	})
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatal("expected rejection, got nil error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "_POD_IP_") {
		t.Errorf("rejection must name the offending token; got: %v", msg)
	}
	if !strings.Contains(msg, "substituted with the pod IP") {
		t.Errorf("rejection must explain the init-container substitution; got: %v", msg)
	}
}

func TestValidator_ReservedRenderTokens_AcceptsValueWithoutPlaceholder(t *testing.T) {
	v := validStandalone("conf-pod-ip-clean", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "proc-title-template \"valkey-instance\"\nmaxmemory 100mb\n"
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{
			"proc-title-template": "valkey-instance",
		}
	})
	mustAccept(t, v)
}

func TestValidator_ConfigBanlist_AcceptsAllowedDirectives(t *testing.T) {
	v := validStandalone("conf-ok", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "maxmemory 100mb\nmaxmemory-policy allkeys-lru\nappendonly yes\n"
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"timeout": "0", "tcp-keepalive": "300"}
	})
	mustAccept(t, v)
}

// --- config-override injection: control chars + key normalisation ---

func TestValidator_ConfigInjection_RejectsNewlineInOverrideValue(t *testing.T) {
	// Mechanism #1: a newline in an override value renders as a second,
	// live directive — `appendonly` passes the key banlist, the value
	// smuggles a `requirepass`.
	v := validStandalone("conf-inject-newline", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"appendonly": "yes\nrequirepass attacker"}
	})
	mustReject(t, v, "must not contain a newline")
}

func TestValidator_ConfigInjection_RejectsCarriageReturnInOverrideValue(t *testing.T) {
	v := validStandalone("conf-inject-cr", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"appendonly": "yes\rrequirepass attacker"}
	})
	mustReject(t, v, "carriage return")
}

func TestValidator_ConfigInjection_RejectsNulInOverrideValue(t *testing.T) {
	v := validStandalone("conf-inject-nul", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"appendonly": "yes\x00"}
	})
	mustReject(t, v, "NUL")
}

func TestValidator_ConfigInjection_RejectsNulInRawConfig(t *testing.T) {
	v := validStandalone("conf-inject-nul-raw", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "maxmemory 1gb\x00\n"
	})
	mustReject(t, v, "NUL")
}

func TestValidator_ConfigBanlist_RejectsWhitespacePaddedOverrideKey(t *testing.T) {
	// Mechanism #2: a leading space dodges the exact-match banlist but the
	// renderer/Valkey trim it, so it would render as a live directive. The
	// key is normalised (ToLower(TrimSpace)) before the banlist check.
	v := validStandalone("conf-padded-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{" requirepass": "attacker"}
	})
	mustReject(t, v, "requirepass")
}

func TestValidator_ConfigBanlist_RejectsUppercasePaddedOverrideKey(t *testing.T) {
	// Combined case + whitespace: normalisation must lower-case after
	// trimming.
	v := validStandalone("conf-padded-upper-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"  ReplicaOf  ": "attacker 6379"}
	})
	mustReject(t, v, "operator-owned")
}

// --- override-key banlist parity with the renderer ---

func TestValidator_ConfigBanlist_RejectsMultiTokenOverrideKey(t *testing.T) {
	// The headline gap: a multi-token override key whose first token
	// is operator-owned (`requirepass evil`) dodged the old whole-string
	// match yet was stripped by the renderer's first-token filter — a
	// silent drop where the tenant should have seen an explicit rejection.
	v := validStandalone("conf-multitoken-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"requirepass evil": "x"}
	})
	mustReject(t, v, "requirepass evil")
}

func TestValidator_ConfigBanlist_RejectsNonCanonicalSentinelOverrideKey(t *testing.T) {
	// Every `sentinel *` key collapses to the `sentinel` first token in the
	// renderer's strip, so a non-canonical `sentinel foobar` is dropped at
	// render. Admission must reject it the same way rather than admit it.
	v := validStandalone("conf-sentinel-noncanon-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"sentinel foobar": "x"}
	})
	mustReject(t, v, "operator-owned")
}

func TestValidator_ConfigBanlist_OverrideKeyRendererParity(t *testing.T) {
	// Pins the contract directly: every override key the renderer
	// silently strips must be rejected at admission. Exercises both sides
	// so future drift in either the validator's normalisation or the
	// renderer's filter fails this test.
	const marker = "PARITY_DROP_MARKER"
	for _, key := range []string{
		"requirepass evil", // multi-token: first token is operator-owned
		"sentinel foobar",  // non-canonical sentinel: collapses to `sentinel`
		" requirepass",     // whitespace-padded
		"ReplicaOf",        // case-insensitive
	} {
		t.Run(key, func(t *testing.T) {
			// Renderer side: the key is stripped, so its value never renders.
			rendered := valkeyconf.Render(valkeyconf.Inputs{
				Overrides: map[string]string{key: marker},
			})
			if strings.Contains(rendered, marker) {
				t.Fatalf("precondition: renderer should drop override key %q but its value rendered:\n%s", key, rendered)
			}
			// Validator side: admission must reject it (not silently admit)
			// and the error must name the offending key.
			name := "parity-" + strings.ToLower(strings.ReplaceAll(strings.TrimSpace(key), " ", "-"))
			v := validStandalone(name, func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.ConfigurationOverrides = map[string]string{key: marker}
			})
			_, err := runValidate(t, v)
			if err == nil {
				t.Fatalf("override key %q must be rejected at admission, got nil error", key)
			}
			if !strings.Contains(err.Error(), "operator-owned") {
				t.Errorf("rejection of %q must cite operator-owned; got %q", key, err.Error())
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("rejection must name the offending key %q; got %q", key, err.Error())
			}
		})
	}
}

func TestValidator_ConfigInjection_RejectsNewlineInOverrideKey(t *testing.T) {
	// The key side of the override injection class: the renderer emits
	// `<key> <value>`, so a newline in the KEY splits the line — the second
	// physical line `requirepass attacker` is a live directive even though
	// the key's first token (`x`) is not operator-owned, so the banlist
	// never sees it. The control-char check must reject it.
	v := validStandalone("conf-inject-newline-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"x\nrequirepass attacker": "y"}
	})
	mustReject(t, v, "key must not contain a newline")
}

func TestValidator_ConfigInjection_RejectsNulInOverrideKey(t *testing.T) {
	// A NUL in the key can truncate the rendered directive at its first
	// token, smuggling an operator-owned `requirepass` that the first-token
	// banlist did not match on the full `requirepass\x00evil` key.
	v := validStandalone("conf-inject-nul-key", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"requirepass\x00evil": "y"}
	})
	mustReject(t, v, "key must not contain")
}

func TestValidator_ConfigInjection_AcceptsMultilineRawConfig(t *testing.T) {
	// Newlines are the legitimate line separator in the raw Configuration;
	// only NUL is rejected there.
	v := validStandalone("conf-multiline-ok", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "maxmemory 1gb\nmaxmemory-policy allkeys-lru\nappendonly yes\n"
	})
	mustAccept(t, v)
}

func TestValidator_ConfigInjection_AcceptsCleanOverrideValue(t *testing.T) {
	v := validStandalone("conf-clean-value", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ConfigurationOverrides = map[string]string{"maxmemory": "1gb", "appendonly": "yes"}
	})
	mustAccept(t, v)
}

// --- Image banlist + tag format ---

func TestValidator_ImageBanlist_RejectsBitnami(t *testing.T) {
	v := validStandalone("img-bitnami", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Repository = "docker.io/bitnami/redis"
	})
	mustReject(t, v, "bitnami")
}

func TestValidator_ImageBanlist_RejectsBitnamiSubstringInExporter(t *testing.T) {
	v := validStandalone("img-bitnami-exp", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Exporter.Repository = "registry.example.com/bitnami-mirror/redis_exporter"
	})
	mustReject(t, v, "bitnami")
}

func TestValidator_ImageTag_RejectsEmptyTagWithoutDigest(t *testing.T) {
	v := validStandalone("img-empty-tag", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = ""
	})
	mustReject(t, v, "is required")
}

func TestValidator_ImageTag_AcceptsEmptyTagWithDigestPin(t *testing.T) {
	v := validStandalone("img-digest", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Repository = "valkey/valkey@sha256:" + strings.Repeat("a", 64)
		v.Spec.Image.Valkey.Tag = ""
	})
	mustAccept(t, v)
}

func TestValidator_ImageTag_RejectsTagWithSpace(t *testing.T) {
	v := validStandalone("img-bad-tag", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "8.1.6 alpine"
	})
	mustReject(t, v, "must match")
}

func TestValidator_ImageTag_WarnsOnLatest(t *testing.T) {
	v := validStandalone("img-latest", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "latest"
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if len(w) == 0 {
		t.Fatal("expected a Warning for floating tag, got none")
	}
	if !strings.Contains(strings.Join(w, " "), "floating reference") {
		t.Errorf("warnings %v do not mention floating reference", w)
	}
}

// --- Valkey image major-version supported ---

func TestValidator_ImageMajor_AcceptsSupportedMajor8(t *testing.T) {
	v := validStandalone("img-major-8", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "8.0"
	})
	mustAccept(t, v)
}

func TestValidator_ImageMajor_AcceptsAlpineVariant(t *testing.T) {
	// The default fixture uses 8.1.6-alpine; mustAccept the variant
	// suffix path explicitly so a future parser regression on
	// `-variant` tags doesn't sneak past the existing fixtures.
	v := validStandalone("img-major-alpine", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "8.2.1-alpine"
	})
	mustAccept(t, v)
}

func TestValidator_ImageMajor_AcceptsValkey9(t *testing.T) {
	// Valkey 9 is admitted as of operator v1.0 (best-effort during
	// alpha; runtime reconcile paths do not branch on major). The
	// inverse assertion that previously pinned this rejection moved
	// to TestValidator_ImageMajor_RejectsUnsupportedFutureMajor.
	v := validStandalone("img-major-9", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "9.0"
	})
	mustAccept(t, v)
}

func TestValidator_ImageMajor_RejectsUnsupportedFutureMajor(t *testing.T) {
	// Pins that majors beyond SupportedMajors still reject. Pick a
	// value far enough ahead that a future scope expansion (adding
	// 9, 10) does not silently turn this test into a tautology.
	v := validStandalone("img-major-99", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "99.0"
	})
	mustReject(t, v, `Unsupported value: "99.0"`)
}

func TestValidator_ImageMajor_RejectsPreValkey8Major7(t *testing.T) {
	v := validStandalone("img-major-7", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "7.4"
	})
	mustReject(t, v, `Unsupported value: "7.4"`)
}

func TestValidator_ImageMajor_LatestStaysAsFloatingWarning(t *testing.T) {
	// The supported-major check stays silent on tags that don't parse
	// as Valkey major.minor (custom-build escape hatch); the existing
	// floating-tag warning still fires.
	v := validStandalone("img-major-latest", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Image.Valkey.Tag = "latest"
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("supported-major check should NOT reject `latest` (defers to tag-format warning): %v", err)
	}
	if len(w) == 0 || !strings.Contains(strings.Join(w, " "), "floating reference") {
		t.Errorf("expected the floating-reference warning to still fire on `latest`; got warnings %v", w)
	}
}

// Pins the canonical floating-tag set so adding/removing an entry is an
// explicit, reviewed change rather than a silent edit. Without this, a
// typo'd removal or a casual addition (e.g. "nightly", "dev") slips
// through code review unless someone greps the validator.
func TestValidator_FloatingTags_PinsCanonicalSet(t *testing.T) {
	want := []string{"edge", "latest", "main", "master", "stable"}
	got := slices.Sorted(maps.Keys(floatingTags))
	if !slices.Equal(got, want) {
		t.Errorf("floatingTags drifted from canonical set\n  got:  %v\n  want: %v", got, want)
	}
}

// TestValidator_ImageMajor_Matrix walks every (major, minor, variant)
// shape the validator's supported-major rule should accept or reject,
// keyed off internal/version.SupportedMajors. The per-case tests
// above pin specific shapes (8.0 accept, 9.0 reject, 7.4 reject,
// 8.2.1-alpine accept, latest defers); this matrix asserts the same
// decisions but as a single table so adding a new supported major
// (e.g. 9 lands in SupportedMajors) touches one row, not five tests.
//
// `wantReject == true` means the validator should reject with the
// canonical "Unsupported value:" substring. `wantReject == false`
// means accept. The `wantFloatingWarn` column pins the warning-only
// branch for tags whose major.minor doesn't parse (latest, edge, …) —
// the supported-major rule is silent in that case and the floating-
// tag warning takes over.
func TestValidator_ImageMajor_Matrix(t *testing.T) {
	cases := []struct {
		name             string
		tag              string
		wantReject       bool
		wantFloatingWarn bool
	}{
		// Supported (Valkey 8.x — GA-tested).
		{name: "8.0 plain", tag: "8.0"},
		{name: "8.0.1 patch", tag: "8.0.1"},
		{name: "8.1.6-alpine variant", tag: "8.1.6-alpine"},
		{name: "8.2.1-alpine variant", tag: "8.2.1-alpine"},
		{name: "8.9 future minor (still major=8)", tag: "8.9"},

		// Supported (Valkey 9.x — admitted, best-effort during alpha).
		{name: "9.0 plain", tag: "9.0"},
		{name: "9.0.4-alpine variant", tag: "9.0.4-alpine"},

		// Unsupported pre-Valkey (Redis-7 lineage).
		{name: "7.4 rejected", tag: "7.4", wantReject: true},
		{name: "7.0.0 rejected", tag: "7.0.0", wantReject: true},

		// Unsupported future major (will flip to accept once
		// SupportedMajors grows; updating this row is the load-bearing
		// signal for that scope expansion).
		{name: "10.0 rejected", tag: "10.0", wantReject: true},
		{name: "99.0 rejected", tag: "99.0", wantReject: true},

		// Floating tags — supported-major rule is silent, the
		// floating-tag warning fires instead. These must NOT reject.
		{name: "latest defers to floating warning", tag: "latest", wantFloatingWarn: true},
		{name: "edge defers to floating warning", tag: "edge", wantFloatingWarn: true},
		{name: "stable defers to floating warning", tag: "stable", wantFloatingWarn: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := validStandalone("img-matrix-"+strings.ReplaceAll(tc.tag, ".", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Image.Valkey.Tag = tc.tag
			})
			warnings, err := runValidate(t, v)
			switch {
			case tc.wantReject:
				if err == nil {
					t.Fatalf("tag %q: expected rejection, got accept", tc.tag)
				}
				// The validator canonicalizes patch versions to
				// "major.minor" in the error message — so 7.0.0 surfaces
				// as Unsupported value: "7.0". Assert on the prefix
				// rather than the raw tag.
				if !strings.Contains(err.Error(), "Unsupported value:") {
					t.Errorf("tag %q: error %q missing canonical Unsupported-value prefix", tc.tag, err.Error())
				}
			case tc.wantFloatingWarn:
				if err != nil {
					t.Fatalf("tag %q: supported-major rule should defer to floating-warn, got rejection %v", tc.tag, err)
				}
				if len(warnings) == 0 || !strings.Contains(strings.Join(warnings, " "), "floating reference") {
					t.Errorf("tag %q: expected floating-reference warning, got %v", tc.tag, warnings)
				}
			default:
				if err != nil {
					t.Fatalf("tag %q: expected accept, got rejection %v", tc.tag, err)
				}
				// Happy-path rows must not emit a floating-tag
				// warning either (8.0 / 8.1.6-alpine / etc. are
				// pinned versions, not floating refs). Without
				// this assertion an accidental addition of one of
				// these tags to floatingTags would slip past the
				// matrix.
				if len(warnings) > 0 {
					t.Errorf("tag %q: expected no warnings on supported pinned tag, got %v", tc.tag, warnings)
				}
			}
		})
	}
}

// --- Probe handler restriction ---

func TestValidator_LivenessProbe_AcceptsTcpSocket(t *testing.T) {
	v := validStandalone("probe-tcp", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(6379)}},
		}
	})
	mustAccept(t, v)
}

// TestValidator_LivenessProbe_AcceptsExec — the validator was
// relaxed to accept exec liveness probes so the defaulter can
// stamp a `valkey-cli ping` exec probe that catches frozen-process
// states a tcpSocket probe is blind to.
func TestValidator_LivenessProbe_AcceptsExec(t *testing.T) {
	v := validStandalone("probe-exec", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"valkey-cli", "ping"}}},
		}
	})
	mustAccept(t, v)
}

func TestValidator_LivenessProbe_RejectsHttpGet(t *testing.T) {
	v := validStandalone("probe-http", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromInt(8080)}},
		}
	})
	mustReject(t, v, "must be tcpSocket or exec")
}

func TestValidator_LivenessProbe_RejectsGRPC(t *testing.T) {
	v := validStandalone("probe-grpc", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 9000}},
		}
	})
	mustReject(t, v, "must be tcpSocket or exec")
}

// --- Feature gates (warn-only) ---

func TestValidator_FeatureGates_WarnsOnUnknownKey(t *testing.T) {
	v := validStandalone("fg-unknown", func(v *valkeyv1beta1.Valkey) {
		v.Spec.FeatureGates = map[string]bool{"experimentalFastFailover": true}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if len(w) == 0 || !strings.Contains(strings.Join(w, " "), "unknown") {
		t.Errorf("expected unknown-feature-gate warning, got %v", w)
	}
}

// --- Baseline ---

func TestValidator_AcceptsMinimalValidStandalone(t *testing.T) {
	mustAccept(t, validStandalone("baseline", nil))
}

// --- Sentinel HA soft-warn ---

func sentinelCR(name string, mutate func(*valkeyv1beta1.Valkey)) *valkeyv1beta1.Valkey {
	v := validStandalone(name, func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeSentinel
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
			Size: resource.MustParse("1Gi"),
		}
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
			MasterName:            "test-master",
			Replicas:              3,
			Quorum:                2,
			DownAfterMilliseconds: 30000,
			FailoverTimeout:       180000,
			ParallelSyncs:         1,
		}
	})
	if mutate != nil {
		mutate(v)
	}
	return v
}

func TestValidator_SentinelQoS_WarnsOnBurstableMismatch(t *testing.T) {
	v := sentinelCR("sent-qos-burstable", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"), // != request → Burstable
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("Burstable sentinel resources must warn, not reject: %v", err)
	}
	if !strings.Contains(strings.Join(w, " "), "Guaranteed") {
		t.Errorf("expected a Guaranteed-QoS warning for mismatched cpu request/limit; got %v", w)
	}
}

func TestValidator_SentinelQoS_WarnsOnMissingLimit(t *testing.T) {
	// The defaulter would mirror this into Guaranteed in production; the
	// validator runs in isolation here and sees the raw (limit-less) object.
	v := sentinelCR("sent-qos-no-limit", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("missing sentinel limits must warn, not reject: %v", err)
	}
	if !strings.Contains(strings.Join(w, " "), "Guaranteed") {
		t.Errorf("expected a Guaranteed-QoS warning when limits are absent; got %v", w)
	}
}

func TestValidator_SentinelQoS_NoWarnWhenGuaranteed(t *testing.T) {
	v := sentinelCR("sent-qos-guaranteed", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("150m"),
				corev1.ResourceMemory: resource.MustParse("192Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("150m"),
				corev1.ResourceMemory: resource.MustParse("192Mi"),
			},
		}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("Guaranteed sentinel resources must validate cleanly: %v", err)
	}
	if strings.Contains(strings.Join(w, " "), "Guaranteed QoS") {
		t.Errorf("did not expect a QoS warning for Guaranteed resources; got %v", w)
	}
}

func TestValidator_SentinelHA_AcceptsHAReplicas(t *testing.T) {
	v := sentinelCR("sentinel-ha", nil) // valkey.replicas=3 from helper
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	for _, msg := range w {
		if strings.Contains(msg, "sub-HA") {
			t.Errorf("HA-shaped sentinel CR triggered sub-HA warning: %q", msg)
		}
	}
}

func TestValidator_SentinelHA_WarnsOnSingleValkeyReplica(t *testing.T) {
	v := sentinelCR("sentinel-lab", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Replicas = 1
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("sub-HA sentinel should soft-warn, not reject; got error: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "sub-HA") || !strings.Contains(joined, "HANotMet") {
		t.Errorf("expected sub-HA + HANotMet warning, got warnings %v", w)
	}
}

func TestValidator_SentinelHA_DoesNotWarnForReplicationMode(t *testing.T) {
	// Replication mode with the minimum (2) replicas should not trigger
	// the sentinel-only HA warning even though it shares the field path.
	v := validStandalone("rep-min", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 2
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
			Size: resource.MustParse("1Gi"),
		}
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "sub-HA") {
			t.Errorf("replication-mode CR triggered sentinel-only sub-HA warning: %q", msg)
		}
	}
}

// --- Even-sentinel-replicas warn ---

func TestValidator_SentinelReplicaParity_AcceptsOddCounts(t *testing.T) {
	for _, n := range []int32{3, 5, 7} {
		t.Run(fmt.Sprintf("replicas=%d", n), func(t *testing.T) {
			v := sentinelCR(fmt.Sprintf("sent-odd-%d", n), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.Replicas = n
				v.Spec.Sentinel.Quorum = (n + 2) / 2
			})
			w, err := runValidate(t, v)
			if err != nil {
				t.Fatalf("unexpected rejection: %v", err)
			}
			for _, msg := range w {
				if strings.Contains(msg, "even") {
					t.Errorf("odd sentinel.replicas=%d triggered even-count warning: %q", n, msg)
				}
			}
		})
	}
}

func TestValidator_SentinelReplicaParity_WarnsOnEvenCount(t *testing.T) {
	v := sentinelCR("sent-even", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.Replicas = 4
		v.Spec.Sentinel.Quorum = 3
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("even sentinel.replicas should be warn-not-reject; got error: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "even") || !strings.Contains(joined, "split-vote") {
		t.Errorf("expected split-vote warning naming 'even', got warnings %v", w)
	}
}

func TestValidator_SentinelReplicaParity_DoesNotWarnForReplicationMode(t *testing.T) {
	// Even valkey.replicas under replication mode is unrelated to
	// sentinel quorum stability; the parity warn must not fire.
	v := validStandalone("rep-even", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 4
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
			Size: resource.MustParse("1Gi"),
		}
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "split-vote") {
			t.Errorf("replication-mode CR triggered sentinel parity warning: %q", msg)
		}
	}
}

// --- Sentinel sub-majority quorum warn ---

func TestValidator_SentinelQuorumSubMajority_WarnsBelowMajority(t *testing.T) {
	cases := []struct {
		replicas, quorum, majority int32
	}{
		{replicas: 4, quorum: 2, majority: 3},
		{replicas: 5, quorum: 2, majority: 3},
		{replicas: 6, quorum: 3, majority: 4},
		{replicas: 7, quorum: 2, majority: 4},
		{replicas: 7, quorum: 3, majority: 4},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("replicas=%d/quorum=%d", tc.replicas, tc.quorum), func(t *testing.T) {
			v := sentinelCR(fmt.Sprintf("sent-subq-%d-%d", tc.replicas, tc.quorum), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.Replicas = tc.replicas
				v.Spec.Sentinel.Quorum = tc.quorum
			})
			w, err := runValidate(t, v)
			if err != nil {
				t.Fatalf("sub-majority quorum should warn-not-reject; got error: %v", err)
			}
			joined := strings.Join(w, " ")
			if !strings.Contains(joined, "below the sentinel pool majority") {
				t.Errorf("expected sub-majority quorum warning, got warnings %v", w)
			}
			if !strings.Contains(joined, fmt.Sprintf("majority of %d", tc.majority)) {
				t.Errorf("expected warning naming majority=%d, got %v", tc.majority, w)
			}
		})
	}
}

func TestValidator_SentinelQuorumSubMajority_AcceptsAtOrAboveMajority(t *testing.T) {
	cases := []struct{ replicas, quorum int32 }{
		{replicas: 3, quorum: 2},
		{replicas: 4, quorum: 3},
		{replicas: 5, quorum: 3},
		{replicas: 5, quorum: 4},
		{replicas: 7, quorum: 4},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("replicas=%d/quorum=%d", tc.replicas, tc.quorum), func(t *testing.T) {
			v := sentinelCR(fmt.Sprintf("sent-okq-%d-%d", tc.replicas, tc.quorum), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.Replicas = tc.replicas
				v.Spec.Sentinel.Quorum = tc.quorum
			})
			w, err := runValidate(t, v)
			if err != nil {
				t.Fatalf("unexpected rejection: %v", err)
			}
			for _, msg := range w {
				if strings.Contains(msg, "below the sentinel pool majority") {
					t.Errorf("majority-or-above quorum (%d/%d) triggered sub-majority warning: %q", tc.replicas, tc.quorum, msg)
				}
			}
		})
	}
}

func TestValidator_SentinelQuorumSubMajority_DoesNotWarnForReplicationMode(t *testing.T) {
	v := validStandalone("rep-subq", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 5
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
			Size: resource.MustParse("1Gi"),
		}
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "below the sentinel pool majority") {
			t.Errorf("replication-mode CR triggered sentinel quorum warning: %q", msg)
		}
	}
}

// --- Sentinel timing floors + allow-aggressive-timeouts bypass ---

func TestValidator_SentinelTimingFloors_AcceptsAtFloor(t *testing.T) {
	// sentinelCR helper sets DownAfterMilliseconds=30000 and
	// FailoverTimeout=180000 — the floor values exactly. Must accept
	// without warning or error.
	v := sentinelCR("sent-tf-floor", nil)
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("at-floor sentinel timing should accept; got: %v", err)
	}
	for _, msg := range w {
		if strings.Contains(msg, "downAfterMilliseconds") || strings.Contains(msg, "failoverTimeout") {
			t.Errorf("at-floor sentinel timing should not warn; got %q", msg)
		}
	}
}

func TestValidator_SentinelTimingFloors_RejectsSubFloorWithoutAnnotation(t *testing.T) {
	v := sentinelCR("sent-tf-rej", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.DownAfterMilliseconds = 500 // sub-floor (floor lowered to 1000)
		v.Spec.Sentinel.FailoverTimeout = 60000     // sub-floor
	})
	w, err := runValidate(t, v)
	if err == nil {
		t.Fatal("sub-floor sentinel timing without bypass annotation should reject")
	}
	msg := err.Error()
	if !strings.Contains(msg, "down-after-milliseconds") || !strings.Contains(msg, "failover-timeout") {
		t.Errorf("rejection should name both sub-floor fields; got %q", msg)
	}
	if !strings.Contains(msg, AllowAggressiveTimeoutsAnnotation) {
		t.Errorf("rejection should name the bypass annotation so the user knows the escape hatch; got %q", msg)
	}
	for _, w := range w {
		if strings.Contains(w, "downAfterMilliseconds") || strings.Contains(w, "failoverTimeout") {
			t.Errorf("rejection path should not also emit warnings on the same fields; got warning %q", w)
		}
	}
}

func TestValidator_SentinelTimingFloors_AcceptsSubFloorWithAnnotationAndWarns(t *testing.T) {
	v := sentinelCR("sent-tf-bypass", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{AllowAggressiveTimeoutsAnnotation: "true"}
		v.Spec.Sentinel.DownAfterMilliseconds = 500
		v.Spec.Sentinel.FailoverTimeout = 60000
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("sub-floor with bypass annotation should accept; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "downAfterMilliseconds=500") {
		t.Errorf("warning should name the sub-floor downAfterMilliseconds value; got warnings %v", w)
	}
	if !strings.Contains(joined, "failoverTimeout=60000") {
		t.Errorf("warning should name the sub-floor failoverTimeout value; got warnings %v", w)
	}
	if !strings.Contains(joined, "AllowAggressiveTimeouts") && !strings.Contains(joined, AllowAggressiveTimeoutsAnnotation) {
		t.Errorf("warning should reference the bypass annotation that granted acceptance; got warnings %v", w)
	}
}

func TestValidator_SentinelTimingFloors_WarnsBelowRecommended(t *testing.T) {
	// A value in [hard floor, recommended) — here 5000 — is accepted with
	// no annotation, but must emit a soft-warn that it is below the
	// recommended 30000ms. This is the signal the floor lowering
	// silently removed for the accepted-but-aggressive band.
	v := sentinelCR("sent-tf-belowrec", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Sentinel.DownAfterMilliseconds = 5000
	})
	if _, ok := v.Annotations[AllowAggressiveTimeoutsAnnotation]; ok {
		t.Fatal("precondition: CR must carry no bypass annotation — the warning must fire without it")
	}
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("downAfterMilliseconds=5000 is above the hard floor and must be accepted; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "downAfterMilliseconds=5000") {
		t.Errorf("expected a below-recommended warning naming the value 5000; got warnings %v", w)
	}
	if !strings.Contains(joined, "30000") {
		t.Errorf("warning should name the recommended 30000ms threshold; got warnings %v", w)
	}
	// The below-recommended warning is rendered from the Deviations() SoT
	// (folded so the admission warning and the durable Event share
	// one string), so it carries the standard "<Reason>: " prefix like every
	// other deviation warning.
	if !strings.Contains(joined, "WarnAggressiveTimeouts") {
		t.Errorf("below-recommended warning should carry the WarnAggressiveTimeouts reason prefix; got warnings %v", w)
	}
}

func TestValidator_SentinelTimingFloors_DoesNotApplyToReplicationMode(t *testing.T) {
	// The floors only apply when mode=sentinel — replication CRs have
	// no sentinel sub-spec, so the sub-floor check has nothing to read.
	v := validStandalone("rep-tf", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 2
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{
			Size: resource.MustParse("1Gi"),
		}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("replication-mode CR should not hit sentinel timing checks; got: %v", err)
	}
	for _, msg := range w {
		if strings.Contains(msg, "downAfterMilliseconds") || strings.Contains(msg, "failoverTimeout") {
			t.Errorf("replication-mode CR triggered sentinel-only timing warning: %q", msg)
		}
	}
}

func TestValidator_SentinelTimingFloors_RejectsBypassAnnotationTypoes(t *testing.T) {
	// Annotation value MUST be the literal string "true" — the
	// operator-trigger annotation rule rejects "True", "1", etc.
	// with a separate field.Invalid error. So a sub-floor CR
	// carrying `allow-aggressive-timeouts: True` (capital T) doesn't
	// get the bypass — the annotation rule rejects it first AND the
	// timing-floor check sees no bypass, so both errors land.
	v := sentinelCR("sent-tf-typo", func(v *valkeyv1beta1.Valkey) {
		v.Annotations = map[string]string{AllowAggressiveTimeoutsAnnotation: "True"}
		v.Spec.Sentinel.DownAfterMilliseconds = 500
	})
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatal("typo'd bypass annotation must NOT grant the floor bypass")
	}
	msg := err.Error()
	if !strings.Contains(msg, "down-after-milliseconds") {
		t.Errorf("error should still flag the sub-floor field even with a typo'd bypass; got %q", msg)
	}
	if !strings.Contains(msg, `"true"`) {
		t.Errorf("error should also flag the typo'd annotation value; got %q", msg)
	}
}

func TestValidator_SentinelTimingFloors_OffByOneBoundary(t *testing.T) {
	// Pin the `>=` comparator on each floor against a `>` off-by-one
	// regression. Floors: down-after-milliseconds=1000 (lowered from
	// 30000), failover-timeout=180000. Each field is exercised
	// in isolation (the other held at a valid value) so a regression on
	// one comparator can't be masked by the other field's rejection.
	cases := []struct {
		name      string
		downAfter int32
		failover  int32
		// reject names the substring required in the rejection;
		// "" means the case must be accepted.
		reject string
	}{
		{"down-after one below floor rejects", 999, 180000, "down-after-milliseconds"},
		{"failover-timeout one below floor rejects", 1000, 179999, "failover-timeout"},
		{"both exactly at floor accept", 1000, 180000, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := sentinelCR("sent-tf-boundary", func(v *valkeyv1beta1.Valkey) {
				v.Spec.Sentinel.DownAfterMilliseconds = tc.downAfter
				v.Spec.Sentinel.FailoverTimeout = tc.failover
			})
			w, err := runValidate(t, v)
			if tc.reject == "" {
				if err != nil {
					t.Fatalf("exactly-at-floor (downAfter=%d, failover=%d) should accept; got %v", tc.downAfter, tc.failover, err)
				}
				// down-after at the hard floor (1000) is accepted but sits
				// below the recommended 30000ms, so it carries the soft
				// below-recommended warning (pinned by WarnsBelowRecommended).
				// failover-timeout has no recommended band — it must not warn.
				for _, msg := range w {
					if strings.Contains(msg, "failoverTimeout") {
						t.Errorf("at-floor failoverTimeout should not warn; got %q", msg)
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("floor-minus-one (downAfter=%d, failover=%d) without bypass annotation should reject", tc.downAfter, tc.failover)
			}
			if !strings.Contains(err.Error(), tc.reject) {
				t.Errorf("rejection should name %q; got %q", tc.reject, err.Error())
			}
		})
	}
}

// --- ReadinessGate.MaxLagBytes warn-on-zero ---

func TestValidator_ReadinessGateMaxLagBytes_QuietWhenUnset(t *testing.T) {
	// Standalone leaves MaxLagBytes nil; the validator must not warn
	// (the defaulter stamps the 1 MiB default later).
	v := validStandalone("rg-lag-unset", nil)
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "readinessGate.maxLagBytes") {
			t.Errorf("unset MaxLagBytes triggered the zero warning: %q", msg)
		}
	}
}

func TestValidator_ReadinessGateMaxLagBytes_QuietWhenPositive(t *testing.T) {
	v := validStandalone("rg-lag-positive", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(524288))
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "readinessGate.maxLagBytes") {
			t.Errorf("positive MaxLagBytes triggered the zero warning: %q", msg)
		}
	}
}

func TestValidator_ReadinessGateMaxLagBytes_WarnsOnExplicitZero(t *testing.T) {
	v := validStandalone("rg-lag-zero", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(0))
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("explicit zero must be warn-not-reject; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "readinessGate.maxLagBytes=0") {
		t.Errorf("expected zero-lag warning citing the field path; got %v", w)
	}
}

// --- Rollout / PDB warn-on-deviation ---

func TestValidator_PDBTooPermissive_FlagsBelowFloor(t *testing.T) {
	min := intstr.FromInt32(0)
	v := validStandalone("pdb-perm", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("PDBTooPermissive should be warn-not-reject; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "PDBTooPermissive") {
		t.Errorf("expected PDBTooPermissive in warnings; got %v", w)
	}
}

func TestValidator_PDBTooPermissive_QuietAtFloor(t *testing.T) {
	min := intstr.FromInt32(2) // floor for replicas=3 is max(1, 2) = 2
	v := validStandalone("pdb-floor", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "PDBTooPermissive") {
			t.Errorf("at-floor minAvailable should not warn; got %q", msg)
		}
	}
}

func TestValidator_PDBTooPermissive_SkipsPercentageMin(t *testing.T) {
	min := intstr.FromString("50%")
	v := validStandalone("pdb-pct", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "PDBTooPermissive") {
			t.Errorf("percentage minAvailable must be skipped (replicas-at-runtime unknown); got %q", msg)
		}
	}
}

func TestValidator_MaxUnavailableInsteadOfMin_Warns(t *testing.T) {
	maxUn := intstr.FromInt32(1)
	v := validStandalone("pdb-maxun", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeReplication
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MaxUnavailable: &maxUn}
	})
	w, _ := runValidate(t, v)
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "MaxUnavailableInsteadOfMin") {
		t.Errorf("expected MaxUnavailableInsteadOfMin warning; got %v", w)
	}
}

func TestValidator_RolloutFragileQuorum_WarnsAboveOne(t *testing.T) {
	v := validStandalone("ro-fragile", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Rollout.MaxUnavailable = 2
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("RolloutFragileQuorum should be warn-not-reject; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "RolloutFragileQuorum") {
		t.Errorf("expected RolloutFragileQuorum warning; got %v", w)
	}
}

func TestValidator_RolloutFragileQuorum_QuietAtOne(t *testing.T) {
	v := validStandalone("ro-quiet", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Rollout.MaxUnavailable = 1
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "RolloutFragileQuorum") {
			t.Errorf("MaxUnavailable=1 must not warn; got %q", msg)
		}
	}
}

func TestValidator_RolloutGraceTooTight_WarnsBelowFailoverTimeout(t *testing.T) {
	v := validStandalone("ro-grace", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeSentinel
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
			MasterName:            "mymaster",
			Replicas:              3,
			DownAfterMilliseconds: 30000,
			FailoverTimeout:       180000, // 180s
		}
		v.Spec.Rollout.FailoverGracePeriodSeconds = 60 // < 180
	})
	w, err := runValidate(t, v)
	if err != nil {
		t.Fatalf("RolloutGraceTooTight should be warn-not-reject; got: %v", err)
	}
	joined := strings.Join(w, " ")
	if !strings.Contains(joined, "RolloutGraceTooTight") {
		t.Errorf("expected RolloutGraceTooTight warning; got %v", w)
	}
}

func TestValidator_RolloutGraceTooTight_QuietForZero(t *testing.T) {
	// Zero is the "compute at reconcile time" sentinel; never warns.
	v := validStandalone("ro-grace-zero", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeSentinel
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
			MasterName:            "mymaster",
			Replicas:              3,
			DownAfterMilliseconds: 30000,
			FailoverTimeout:       180000,
		}
		v.Spec.Rollout.FailoverGracePeriodSeconds = 0
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "RolloutGraceTooTight") {
			t.Errorf("FailoverGracePeriodSeconds=0 (sentinel) must not warn; got %q", msg)
		}
	}
}

func TestValidator_RolloutGraceTooTight_QuietAtFailoverTimeout(t *testing.T) {
	v := validStandalone("ro-grace-eq", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Mode = valkeyv1beta1.ModeSentinel
		v.Spec.Valkey.Replicas = 3
		v.Spec.Valkey.Persistence = &valkeyv1beta1.PersistenceSpec{Size: resource.MustParse("1Gi")}
		v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{
			MasterName:            "mymaster",
			Replicas:              3,
			DownAfterMilliseconds: 30000,
			FailoverTimeout:       180000,
		}
		v.Spec.Rollout.FailoverGracePeriodSeconds = 180 // == failoverTimeout
	})
	w, _ := runValidate(t, v)
	for _, msg := range w {
		if strings.Contains(msg, "RolloutGraceTooTight") {
			t.Errorf("FailoverGracePeriodSeconds at failoverTimeout floor must not warn; got %q", msg)
		}
	}
}

// --- Comprehensive negative-permutation matrices ---
//
// The hand-rolled rejection tests above cover representative cases per
// rule but leave gaps: a `||`-vs-`&&` typo in the validator could survive,
// and a future addition to bannedConfigDirectives / reservedLabels /
// reservedAnnotation* would land without a corresponding negative case.
// The tables below iterate the validator's own keysets so any new entry
// added to those slices is automatically covered. The multi-violation
// tests pin that the validator accumulates errors rather than
// short-circuiting on the first miss.

func TestValidator_ConfigBanlist_AllDirectivesRejectedInRaw_Table(t *testing.T) {
	// Iterates bannedConfigDirectives and asserts each is rejected when
	// placed in the raw Configuration snippet. Adding a new entry to the
	// slice automatically extends the matrix; if the new directive can't
	// be validated by simply appending an argument string, this test
	// fails loudly rather than silently skipping.
	for _, directive := range bannedConfigDirectives {
		t.Run(directive, func(t *testing.T) {
			// Multi-word directives (e.g. "sentinel monitor") need the
			// whole phrase verbatim; single-word directives need a
			// trailing argument so configLineRegexp captures the
			// directive token correctly.
			snippet := directive + " somearg\n"
			v := validStandalone("conf-rej-"+strings.ReplaceAll(directive, " ", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.Configuration = snippet
			})
			mustReject(t, v, directive)
		})
	}
}

func TestValidator_ConfigBanlist_AllDirectivesRejectedInOverrideMap_Table(t *testing.T) {
	// Iterates bannedConfigDirectives and asserts each is rejected when
	// used as a ConfigurationOverrides map key. Mirror of the raw-snippet
	// table above; covers the validateConfigBanlist override-path branch.
	for _, directive := range bannedConfigDirectives {
		t.Run(directive, func(t *testing.T) {
			v := validStandalone("conf-ov-"+strings.ReplaceAll(directive, " ", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.ConfigurationOverrides = map[string]string{directive: "somearg"}
			})
			mustReject(t, v, directive)
		})
	}
}

func TestValidator_ConfigBanlist_RejectsMultipleViolationsAtOnce(t *testing.T) {
	// A `||`-vs-`&&` typo in validateConfigBanlist (or an early-return
	// after the first hit) would silently pass with one of the two
	// violations surfaced. Pin the accumulation contract: both
	// directives must appear in the rejection error.
	v := validStandalone("conf-multi", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.Configuration = "bind 0.0.0.0\nrequirepass hunter2\n"
	})
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatalf("expected rejection containing both 'bind' and 'requirepass'; got nil error")
	}
	if !strings.Contains(err.Error(), "bind") {
		t.Errorf("error must surface 'bind' violation; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "requirepass") {
		t.Errorf("error must surface 'requirepass' violation; got %q", err.Error())
	}
}

func TestValidator_ReservedLabel_AllReservedKeysRejected_Table(t *testing.T) {
	// Iterates reservedLabels and asserts each is rejected when placed
	// in the spec.valkey.podLabels map. New entries in the slice
	// auto-extend the matrix.
	for _, key := range reservedLabels {
		t.Run(key, func(t *testing.T) {
			v := validStandalone("label-rej-"+strings.ReplaceAll(key, "/", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.PodLabels = map[string]string{key: "user-value"}
			})
			mustReject(t, v, "reserved for the operator")
		})
	}
}

func TestValidator_ReservedLabel_RejectsMultipleAtOnce(t *testing.T) {
	// Two reserved labels in one PodLabels map — both must surface.
	// The keys carry distinct values so the rejection messages are
	// distinguishable in the error output, catching a regression where
	// the validator returns after the first hit.
	v := validStandalone("label-multi", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodLabels = map[string]string{
			"app.kubernetes.io/managed-by": "user-A",
			"velkir.ioxie.dev/role":        "user-B",
		}
	})
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatalf("expected rejection naming both reserved labels; got nil error")
	}
	if !strings.Contains(err.Error(), "app.kubernetes.io/managed-by") {
		t.Errorf("error must name app.kubernetes.io/managed-by; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "velkir.ioxie.dev/role") {
		t.Errorf("error must name velkir.ioxie.dev/role; got %q", err.Error())
	}
}

func TestValidator_ReservedAnnotation_AllExactKeysRejected_Table(t *testing.T) {
	// Iterates reservedAnnotationKeys (exact-match keys) and asserts
	// each is rejected when placed in spec.valkey.podAnnotations.
	for _, key := range reservedAnnotationKeys {
		t.Run(key, func(t *testing.T) {
			v := validStandalone("ann-key-"+strings.ReplaceAll(key, "/", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.PodAnnotations = map[string]string{key: "user-value"}
			})
			mustReject(t, v, "reserved for the operator")
		})
	}
}

func TestValidator_ReservedAnnotation_AllPrefixesRejected_Table(t *testing.T) {
	// Iterates reservedAnnotationPrefixes and asserts each is rejected
	// when placed in spec.valkey.podAnnotations as a prefix-match. The
	// suffix "-future-key" is appended so the test exercises the
	// HasPrefix path rather than an exact-match collision.
	for _, prefix := range reservedAnnotationPrefixes {
		t.Run(prefix, func(t *testing.T) {
			key := prefix + "future-key"
			v := validStandalone("ann-pre-"+strings.ReplaceAll(prefix, "/", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Spec.Valkey.PodAnnotations = map[string]string{key: "user-value"}
			})
			mustReject(t, v, "reserved for the operator")
		})
	}
}

func TestValidator_ReservedAnnotation_RejectsPrefixAndExactKeyAtOnce(t *testing.T) {
	// One annotation collides on the exact-key path (managed-by); a
	// second collides on the prefix path (velkir.ioxie.dev/anything).
	// The validator must surface both rather than short-circuit on the
	// first hit. Without this guard, the for-loop's `break` after a
	// prefix hit could combine with a future early-return on the exact
	// path to silently mask one of the two violations.
	v := validStandalone("ann-multi", func(v *valkeyv1beta1.Valkey) {
		v.Spec.Valkey.PodAnnotations = map[string]string{
			"app.kubernetes.io/managed-by":     "user-A",
			"velkir.ioxie.dev/something-novel": "user-B",
		}
	})
	_, err := runValidate(t, v)
	if err == nil {
		t.Fatalf("expected rejection naming both reserved annotations; got nil error")
	}
	if !strings.Contains(err.Error(), "app.kubernetes.io/managed-by") {
		t.Errorf("error must name app.kubernetes.io/managed-by; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "velkir.ioxie.dev/something-novel") {
		t.Errorf("error must name velkir.ioxie.dev/something-novel; got %q", err.Error())
	}
}

func TestValidator_OperatorTriggerAnnotation_AllTriggersRejectNonLiteralTrue_Table(t *testing.T) {
	// Iterates operatorTriggerAnnotations and asserts each rejects a
	// non-literal-"true" value. New entries to the slice are
	// automatically covered. Pairs the trigger annotation contract
	// (literal "true" only) with the reserved-annotation prefix rule —
	// these annotations are inside the reserved velkir.ioxie.dev/ prefix
	// but legal on the CR itself (operatorTriggerAnnotations is read
	// from o.Annotations, not o.Spec.Valkey.PodAnnotations).
	for _, key := range operatorTriggerAnnotations {
		t.Run(key, func(t *testing.T) {
			v := validStandalone("trig-tbl-"+strings.ReplaceAll(key, "/", "-"), func(v *valkeyv1beta1.Valkey) {
				v.Annotations = map[string]string{key: "True"}
			})
			mustReject(t, v, `must be the literal string "true"`)
		})
	}
}

// --- featureGates: UpgradePreflight bypass Warning ---

func TestValidateFeatureGates_UpgradePreflightFalse_EmitsSafetyBypassWarning(t *testing.T) {
	w := validateFeatureGates(map[string]bool{"UpgradePreflight": false})
	if len(w) != 1 {
		t.Fatalf("expected exactly one Warning for explicit UpgradePreflight=false, got %d: %v", len(w), w)
	}
	if !strings.Contains(w[0], "UpgradePreflight=false") {
		t.Errorf("Warning should name the gate; got %q", w[0])
	}
	if !strings.Contains(w[0], "testbed") {
		t.Errorf("Warning should signpost testbed-only intent so a copy-paste from prod gets flagged; got %q", w[0])
	}
}

func TestValidateFeatureGates_UpgradePreflightTrue_NoWarning(t *testing.T) {
	// Explicit-true is equivalent to absent — no Warning. The known-
	// allowlist swallows it as "recognised key" and the bypass branch
	// only fires when the value is false. Pins that a regression
	// inverting the comma-ok check would surface as an unexpected
	// Warning here.
	if w := validateFeatureGates(map[string]bool{"UpgradePreflight": true}); len(w) != 0 {
		t.Fatalf("explicit UpgradePreflight=true must not emit a Warning; got %v", w)
	}
}

func TestValidateFeatureGates_AbsentGate_NoWarning(t *testing.T) {
	// Empty map (and a nil map per the len-zero short-circuit at the
	// top of validateFeatureGates) is the default-safe state — no
	// Warning. This is the case where the validator's behaviour must
	// stay silent to avoid pestering every CR that doesn't touch
	// feature gates.
	if w := validateFeatureGates(map[string]bool{}); len(w) != 0 {
		t.Fatalf("empty featureGates must not emit a Warning; got %v", w)
	}
	if w := validateFeatureGates(nil); len(w) != 0 {
		t.Fatalf("nil featureGates must not emit a Warning; got %v", w)
	}
}

func TestValidateFeatureGates_UnknownKey_EmitsTypoWarning(t *testing.T) {
	// Unknown keys are accepted (forward-compatibility for future
	// gates), but emit a Warning so typos surface in `kubectl apply`
	// output. Pairs with the bypass-Warning test: when the user wrote
	// `UpgradePreflghtt: false` (typo), the unknown-key Warning fires,
	// not the bypass Warning — they get to see the typo, not the
	// (accidentally) safe behaviour.
	w := validateFeatureGates(map[string]bool{"UpgradePreflghtt": false})
	if len(w) != 1 {
		t.Fatalf("expected one Warning for unknown gate key, got %d: %v", len(w), w)
	}
	if !strings.Contains(w[0], "UpgradePreflghtt") || !strings.Contains(w[0], "unknown") {
		t.Errorf("Warning should name the typo and signal unknown-key; got %q", w[0])
	}
}

func TestValidateFeatureGates_BypassAndUnknownKey_BothWarnings(t *testing.T) {
	// Mixed input: explicit bypass + a typo. Both Warnings must fire
	// independently — the bypass Warning is its own admission signal,
	// the unknown-key Warning surfaces the typo separately. Pins that
	// a future refactor making the function return on the first
	// Warning would regress (typo would silently slip through).
	w := validateFeatureGates(map[string]bool{
		"UpgradePreflight": false,
		"FuturGate":        true,
	})
	if len(w) != 2 {
		t.Fatalf("expected two Warnings (bypass + unknown), got %d: %v", len(w), w)
	}
	joined := strings.Join(w, "\n")
	if !strings.Contains(joined, "UpgradePreflight=false") {
		t.Errorf("expected bypass Warning naming UpgradePreflight=false; got %v", w)
	}
	if !strings.Contains(joined, "FuturGate") {
		t.Errorf("expected unknown-key Warning naming FuturGate; got %v", w)
	}
}
