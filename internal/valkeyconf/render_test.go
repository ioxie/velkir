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

package valkeyconf

import (
	"strings"
	"testing"
)

func TestRenderEmpty(t *testing.T) {
	out := Render(Inputs{Persistent: true})
	for _, want := range []string{
		"dir /data\n",
		"port 6379\n",
		"protected-mode no\n",
		"replica-announce-ip _POD_IP_\n",
		"replica-announce-port 6379\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing mandatory directive %q in output:\n%s", want, out)
		}
	}
	if strings.Contains(out, "# spec.valkey.configuration") {
		t.Errorf("user-config section header should be omitted when input is empty:\n%s", out)
	}
	if strings.Contains(out, "# spec.valkey.configurationOverrides") {
		t.Errorf("overrides section header should be omitted when overrides map is empty:\n%s", out)
	}
}

func TestRenderPersistenceFlipsDir(t *testing.T) {
	persistent := Render(Inputs{Persistent: true})
	emptyDir := Render(Inputs{Persistent: false})
	if !strings.Contains(persistent, "dir /data\n") {
		t.Errorf("persistent=true should set dir=/data; got:\n%s", persistent)
	}
	if !strings.Contains(emptyDir, "dir /tmp\n") {
		t.Errorf("persistent=false should set dir=/tmp; got:\n%s", emptyDir)
	}
	if strings.Contains(persistent, "dir /tmp") {
		t.Errorf("persistent=true must not also write dir /tmp")
	}
}

func TestRenderUserStringPassesThrough(t *testing.T) {
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "maxmemory 1gb\nmaxmemory-policy allkeys-lru\n",
	})
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("user maxmemory should pass through; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory-policy allkeys-lru\n") {
		t.Errorf("user maxmemory-policy should pass through; got:\n%s", out)
	}
	if !strings.Contains(out, "# spec.valkey.configuration\n") {
		t.Errorf("user-config section header should be present; got:\n%s", out)
	}
}

func TestRenderOverrideWinsOverUserString(t *testing.T) {
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "maxmemory 1gb\nappendonly yes\n",
		Overrides:     map[string]string{"maxmemory": "2gb"},
	})
	if strings.Contains(out, "maxmemory 1gb") {
		t.Errorf("override must displace user-string maxmemory; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 2gb\n") {
		t.Errorf("override value should appear; got:\n%s", out)
	}
	if !strings.Contains(out, "appendonly yes\n") {
		t.Errorf("non-colliding user line should survive; got:\n%s", out)
	}
}

func TestRenderMandatoryStripsUserString(t *testing.T) {
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "port 7000\nprotected-mode yes\nmaxmemory 1gb\n",
	})
	if strings.Contains(out, "port 7000") {
		t.Errorf("mandatory `port` must displace user override; got:\n%s", out)
	}
	if strings.Contains(out, "protected-mode yes") {
		t.Errorf("mandatory `protected-mode` must displace user override; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("non-colliding user line should survive; got:\n%s", out)
	}
}

func TestRenderMandatoryStripsOverride(t *testing.T) {
	// Overrides matching mandatory directive names are dropped from
	// the override layer too — the user can't smuggle a replacement
	// past the mandatory floor via either path.
	out := Render(Inputs{
		Persistent: true,
		Overrides: map[string]string{
			"port":                "7000",
			"replica-announce-ip": "1.2.3.4",
		},
	})
	if strings.Contains(out, "port 7000") {
		t.Errorf("override `port` must not surface; got:\n%s", out)
	}
	if strings.Contains(out, "replica-announce-ip 1.2.3.4") {
		t.Errorf("override `replica-announce-ip` must not surface; got:\n%s", out)
	}
	// The mandatory layer's own values still hold.
	if !strings.Contains(out, "port 6379\n") {
		t.Errorf("mandatory port should still appear; got:\n%s", out)
	}
}

func TestRenderDirectiveMatchIsCaseInsensitive(t *testing.T) {
	// User upper-cases a directive, override is lower-case. Override
	// still wins via case-insensitive directive matching.
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "MAXMEMORY 1gb\n",
		Overrides:     map[string]string{"maxmemory": "2gb"},
	})
	if strings.Contains(out, "MAXMEMORY 1gb") {
		t.Errorf("upper-case user directive must be displaced by override; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 2gb\n") {
		t.Errorf("override value should appear; got:\n%s", out)
	}
}

func TestRenderPreservesCommentsAndBlanks(t *testing.T) {
	in := "# user comment\n\nappendonly yes\n# another comment\n"
	out := Render(Inputs{Persistent: true, Configuration: in})
	if !strings.Contains(out, "# user comment\n") {
		t.Errorf("user comments should survive; got:\n%s", out)
	}
	if !strings.Contains(out, "# another comment\n") {
		t.Errorf("trailing user comment should survive; got:\n%s", out)
	}
	if !strings.Contains(out, "appendonly yes\n") {
		t.Errorf("user directive should survive; got:\n%s", out)
	}
}

func TestRenderMultipleSavesReplacedTogether(t *testing.T) {
	// `save` is a directive that legitimately repeats. If overrides
	// names it, every user-string `save` line is dropped (key-level
	// replace, applied per line — the override wins outright).
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "save 900 1\nsave 300 10\nsave 60 10000\nappendonly yes\n",
		Overrides:     map[string]string{"save": ""},
	})
	if strings.Contains(out, "save 900 1") || strings.Contains(out, "save 300 10") || strings.Contains(out, "save 60 10000") {
		t.Errorf("override must drop every user `save` line; got:\n%s", out)
	}
	if !strings.Contains(out, "save \n") {
		t.Errorf("override `save: \"\"` should render `save ` line; got:\n%s", out)
	}
	if !strings.Contains(out, "appendonly yes\n") {
		t.Errorf("non-colliding user line should survive; got:\n%s", out)
	}
}

func TestRenderDeterministicOrder(t *testing.T) {
	// Map iteration is non-deterministic; the renderer must sort to
	// keep the SHA-256 hash stable so the pod template doesn't roll
	// pods on a cold reconcile that happened to walk the map in a
	// different order.
	in := Inputs{
		Persistent: true,
		Overrides: map[string]string{
			"maxmemory":       "2gb",
			"appendonly":      "yes",
			"timeout":         "0",
			"tcp-keepalive":   "300",
			"databases":       "16",
			"hz":              "10",
			"slowlog-max-len": "128",
		},
	}
	first := Render(in)
	for i := range 10 {
		if got := Render(in); got != first {
			t.Fatalf("Render output not deterministic across calls (iteration %d):\n--- first ---\n%s\n--- got ---\n%s", i, first, got)
		}
	}
}

func TestRenderEmitsAnnounceIPPlaceholder(t *testing.T) {
	// The init container relies on the literal `_POD_IP_` token to do
	// its sed substitution. Hard-code the assertion so a rename
	// surfaces here, not in a runtime CrashLoopBackOff.
	out := Render(Inputs{Persistent: true})
	if !strings.Contains(out, "replica-announce-ip _POD_IP_\n") {
		t.Errorf("rendered template must carry the literal _POD_IP_ placeholder; got:\n%s", out)
	}
}

func TestRenderUserStringTrailingNewlineNormalized(t *testing.T) {
	// A user snippet without a trailing newline still produces a
	// well-formed section; the renderer normalises to exactly one
	// trailing newline so the overrides-section header (if any)
	// starts on its own line.
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "appendonly yes",
		Overrides:     map[string]string{"timeout": "30"},
	})
	if !strings.Contains(out, "appendonly yes\n\n# spec.valkey.configurationOverrides\n") {
		t.Errorf("user section should terminate with a single newline before the next section; got:\n%s", out)
	}
}

func TestRenderAllUserLinesStrippedCollapsesSection(t *testing.T) {
	// If every user-string line gets stripped (mandatory or override
	// collision) the section header must disappear too — otherwise the
	// rendered file shows a header followed by nothing, which is
	// confusing and bumps the hash without behavioural reason.
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "port 7000\nprotected-mode yes\n",
	})
	if strings.Contains(out, "# spec.valkey.configuration") {
		t.Errorf("section header should collapse when user input is fully stripped; got:\n%s", out)
	}
}

// --- defense-in-depth: renderer strips the operator-owned banlist
// even when the webhook is bypassed/regressed. ---

func TestRenderStripsBannedDirectiveFromUserString(t *testing.T) {
	// requirepass is operator-owned but NOT a mandatory directive, so the
	// pre-fix renderer (which stripped only the 5 mandatory keys) leaked
	// it. It must now be stripped from the raw Configuration.
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "requirepass attacker\nmaxmemory 1gb\n",
	})
	if strings.Contains(out, "requirepass") {
		t.Errorf("operator-owned `requirepass` must be stripped from the user string; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("non-banned user line should survive; got:\n%s", out)
	}
}

func TestRenderStripsReplicaofFromUserString(t *testing.T) {
	// replicaof is the highest-risk leak (flush + re-sync from an attacker
	// host). Must not survive into the rendered valkey.conf.
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "replicaof attacker-host 6379\n",
	})
	if strings.Contains(out, "replicaof") {
		t.Errorf("operator-owned `replicaof` must be stripped from the user string; got:\n%s", out)
	}
}

func TestRenderDropsBannedOverrideKey(t *testing.T) {
	out := Render(Inputs{
		Persistent: true,
		Overrides:  map[string]string{"requirepass": "attacker", "maxmemory": "1gb"},
	})
	if strings.Contains(out, "requirepass") {
		t.Errorf("operator-owned `requirepass` override must be dropped; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("non-banned override should survive; got:\n%s", out)
	}
}

func TestRenderDropsWhitespacePaddedBannedOverrideKey(t *testing.T) {
	// Mechanism #2: a leading space on the key dodges an exact-match
	// banlist but the renderer (and Valkey) trim it, so it would render as
	// a live directive. The normalised first-token match drops it.
	out := Render(Inputs{
		Persistent: true,
		Overrides:  map[string]string{" requirepass": "attacker"},
	})
	if strings.Contains(out, "requirepass") {
		t.Errorf("whitespace-padded `requirepass` override must be dropped; got:\n%s", out)
	}
}

func TestRenderDropsOverrideValueWithNewline(t *testing.T) {
	// Mechanism #1: a newline in a non-banned override value would render
	// as a second, live directive. The whole override is dropped.
	out := Render(Inputs{
		Persistent: true,
		Overrides:  map[string]string{"appendonly": "yes\nrequirepass attacker"},
	})
	if strings.Contains(out, "requirepass") {
		t.Errorf("newline-injected `requirepass` must not render; got:\n%s", out)
	}
	if strings.Contains(out, "appendonly yes\nrequirepass") {
		t.Errorf("override value newline must not produce two directives; got:\n%s", out)
	}
}

func TestRenderStripsSentinelDirectiveFromUserString(t *testing.T) {
	// Multi-word `sentinel *` entries collapse to the `sentinel` first
	// token; any sentinel directive in valkey.conf is stripped (it belongs
	// only in sentinel.conf).
	out := Render(Inputs{
		Persistent:    true,
		Configuration: "sentinel monitor mymaster 10.0.0.1 6379 2\nmaxmemory 1gb\n",
	})
	if strings.Contains(out, "sentinel monitor") {
		t.Errorf("`sentinel monitor` must be stripped from valkey.conf; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("non-banned user line should survive; got:\n%s", out)
	}
}

func TestRenderDropsMultiTokenBannedOverrideKey(t *testing.T) {
	// a multi-token override key whose first token is operator-owned
	// (`requirepass evil`) normalises to `requirepass` and is dropped — the
	// renderer behaviour the validating webhook must reject at admission.
	out := Render(Inputs{
		Persistent: true,
		Overrides:  map[string]string{"requirepass evil": "x", "maxmemory": "1gb"},
	})
	if strings.Contains(out, "requirepass") {
		t.Errorf("multi-token `requirepass evil` override must be dropped; got:\n%s", out)
	}
	if !strings.Contains(out, "maxmemory 1gb\n") {
		t.Errorf("non-banned override should survive; got:\n%s", out)
	}
}

func TestRenderDropsOverrideKeyWithControlChar(t *testing.T) {
	// The key side of injection: a newline or NUL in an override KEY would
	// render `<key> <value>` as a second live directive (or truncate at a
	// NUL). filterOverrides drops the whole override as a backstop.
	for _, key := range []string{"x\nrequirepass attacker", "requirepass\x00evil"} {
		out := Render(Inputs{
			Persistent: true,
			Overrides:  map[string]string{key: "MARK", "maxmemory": "1gb"},
		})
		if strings.Contains(out, "MARK") || strings.Contains(out, "requirepass") {
			t.Errorf("override key %q with a control char must be dropped; got:\n%s", key, out)
		}
		if !strings.Contains(out, "maxmemory 1gb\n") {
			t.Errorf("clean sibling override should survive; got:\n%s", out)
		}
	}
}

// --- exported normalisation helpers shared with the webhook ---

func TestContainsNUL(t *testing.T) {
	for _, s := range []string{"a\x00b", "\x00", "trailing\x00"} {
		if !ContainsNUL(s) {
			t.Errorf("ContainsNUL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "clean", "line1\nline2", "cr\ronly"} {
		if ContainsNUL(s) {
			t.Errorf("ContainsNUL(%q) = true, want false", s)
		}
	}
	// ContainsInjectionChars still covers all three injection bytes,
	// including the NUL half now routed through ContainsNUL.
	for _, s := range []string{"a\nb", "a\rb", "a\x00b"} {
		if !ContainsInjectionChars(s) {
			t.Errorf("ContainsInjectionChars(%q) = false, want true", s)
		}
	}
	if ContainsInjectionChars("clean value") {
		t.Errorf("ContainsInjectionChars(clean) = true, want false")
	}
}

func TestBannedDirectiveKeysFirstTokenCollapse(t *testing.T) {
	keys := BannedDirectiveKeys()
	// Single-word operator-owned directives are present verbatim.
	for _, want := range []string{"requirepass", "bind", "replicaof", "min-replicas-to-write"} {
		if !keys[want] {
			t.Errorf("BannedDirectiveKeys missing %q", want)
		}
	}
	// Every multi-word `sentinel *` entry collapses to the `sentinel`
	// first token — the keyset is first-token, not whole-string.
	if !keys["sentinel"] {
		t.Errorf("BannedDirectiveKeys missing collapsed `sentinel` token")
	}
	if keys["sentinel monitor"] {
		t.Errorf("BannedDirectiveKeys must key on first tokens, not whole strings; found `sentinel monitor`")
	}
	// Common legitimate user directives must NOT be in the keyset — guards
	// against the banned set accidentally widening and over-rejecting.
	for _, notBanned := range []string{"maxmemory", "appendonly", "timeout", "save"} {
		if keys[notBanned] {
			t.Errorf("BannedDirectiveKeys must not include non-operator-owned %q", notBanned)
		}
	}
}

func TestNormalizeDirectiveKey(t *testing.T) {
	cases := map[string]string{
		"requirepass":      "requirepass",
		"requirepass evil": "requirepass", // multi-token → first token
		" requirepass ":    "requirepass", // padded
		"ReplicaOf":        "replicaof",   // case-folded
		"sentinel monitor": "sentinel",    // sentinel collapse
		"sentinel foobar":  "sentinel",
		"":                 "", // blank
		"# commented":      "", // comment line is inert
	}
	for in, want := range cases {
		if got := NormalizeDirectiveKey(in); got != want {
			t.Errorf("NormalizeDirectiveKey(%q) = %q, want %q", in, got, want)
		}
	}
}

// --- write-loss floor: min-replicas-* render in the
// operator-owned mandatory layer when the resolved fields are set
// (replication/sentinel) and are omitted when nil (standalone). ---

func TestRenderEmitsMinReplicasWhenSet(t *testing.T) {
	out := Render(Inputs{
		Persistent:         true,
		MinReplicasToWrite: new(int32(1)),
		MinReplicasMaxLag:  new(int32(10)),
	})
	if !strings.Contains(out, "min-replicas-to-write 1\n") {
		t.Errorf("min-replicas-to-write directive missing; got:\n%s", out)
	}
	if !strings.Contains(out, "min-replicas-max-lag 10\n") {
		t.Errorf("min-replicas-max-lag directive missing; got:\n%s", out)
	}
}

func TestRenderOmitsMinReplicasWhenNil(t *testing.T) {
	// Standalone resolves both fields to nil → directives must be absent;
	// `min-replicas-to-write 1` on a replica-less primary wedges writes.
	out := Render(Inputs{Persistent: true})
	if strings.Contains(out, "min-replicas-to-write") {
		t.Errorf("min-replicas-to-write must be omitted when unset; got:\n%s", out)
	}
	if strings.Contains(out, "min-replicas-max-lag") {
		t.Errorf("min-replicas-max-lag must be omitted when unset; got:\n%s", out)
	}
}

func TestRenderMinReplicasOperatorOwnedNotClobbered(t *testing.T) {
	// A regressed/bypassed webhook can't smuggle a weaker floor through
	// the raw config: the operator-owned directives win and the user's
	// `min-replicas-* 0` lines are stripped.
	out := Render(Inputs{
		Persistent:         true,
		MinReplicasToWrite: new(int32(1)),
		MinReplicasMaxLag:  new(int32(10)),
		Configuration:      "min-replicas-to-write 0\nmin-replicas-max-lag 0\n",
	})
	if strings.Contains(out, "min-replicas-to-write 0") {
		t.Errorf("user min-replicas-to-write 0 must be stripped; got:\n%s", out)
	}
	if strings.Contains(out, "min-replicas-max-lag 0") {
		t.Errorf("user min-replicas-max-lag 0 must be stripped; got:\n%s", out)
	}
	if !strings.Contains(out, "min-replicas-to-write 1\n") {
		t.Errorf("operator-owned min-replicas-to-write 1 must survive; got:\n%s", out)
	}
}

func TestRenderMinReplicasResolvedValuePreserved(t *testing.T) {
	// A stricter resolved value (user set the typed field above the
	// stamped floor) renders verbatim.
	out := Render(Inputs{
		Persistent:         true,
		MinReplicasToWrite: new(int32(2)),
		MinReplicasMaxLag:  new(int32(15)),
	})
	if !strings.Contains(out, "min-replicas-to-write 2\n") {
		t.Errorf("resolved min-replicas-to-write 2 should render; got:\n%s", out)
	}
	if !strings.Contains(out, "min-replicas-max-lag 15\n") {
		t.Errorf("resolved min-replicas-max-lag 15 should render; got:\n%s", out)
	}
}
