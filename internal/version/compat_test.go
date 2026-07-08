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

package version

import (
	"errors"
	"testing"
)

func TestParseValkeyTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		ref     string
		want    ValkeyVersion
		wantErr error
	}{
		{name: "bare repo:major.minor", ref: "valkey:8.0", want: ValkeyVersion{8, 0}},
		{name: "repo:major.minor.patch (patch dropped)", ref: "valkey:8.0.1", want: ValkeyVersion{8, 0}},
		{name: "registry/repo:major.minor", ref: "docker.io/valkey/valkey:8.1", want: ValkeyVersion{8, 1}},
		{name: "registry/repo:major.minor.patch", ref: "docker.io/valkey/valkey:8.1.3", want: ValkeyVersion{8, 1}},
		{name: "registry-with-port/repo:tag", ref: "registry.local:5000/valkey/valkey:8.2", want: ValkeyVersion{8, 2}},
		{name: "registry-with-port/repo:major.minor.patch", ref: "registry.local:5000/valkey/valkey:8.2.7", want: ValkeyVersion{8, 2}},
		{name: "digest-pinned tag", ref: "valkey:8.0@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", want: ValkeyVersion{8, 0}},
		{name: "registry/repo:tag with digest", ref: "docker.io/valkey/valkey:8.1.2@sha256:abc", want: ValkeyVersion{8, 1}},
		{name: "9.x major (parses, IsSupportedMajor handles policy)", ref: "valkey:9.0", want: ValkeyVersion{9, 0}},
		{name: "high minor", ref: "valkey:8.42", want: ValkeyVersion{8, 42}},
		{name: "alpine variant", ref: "valkey/valkey:8.1.6-alpine", want: ValkeyVersion{8, 1}},
		{name: "debian variant", ref: "valkey/valkey:8.0.3-debian", want: ValkeyVersion{8, 0}},
		{name: "rc pre-release", ref: "valkey:8.1-rc1", want: ValkeyVersion{8, 1}},
		{name: "alpine variant with digest", ref: "valkey:8.1.6-alpine@sha256:abc", want: ValkeyVersion{8, 1}},

		// Error cases.
		{name: "no tag at all", ref: "valkey", wantErr: ErrNoTag},
		{name: "registry-port-only no repo tag", ref: "registry.local:5000/valkey/valkey", wantErr: ErrNoTag},
		{name: "tag is just major", ref: "valkey:8", wantErr: ErrMalformedTag},
		{name: "tag is non-numeric", ref: "valkey:latest", wantErr: ErrMalformedTag},
		{name: "tag has non-numeric minor", ref: "valkey:8.x", wantErr: ErrMalformedTag},
		{name: "tag has negative minor (parsed via leading dash)", ref: "valkey:8.-1", wantErr: ErrMalformedTag},
		{name: "tag has non-numeric patch", ref: "valkey:8.0.x", wantErr: ErrMalformedTag},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseValkeyTag(tc.ref)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v want=%v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err=%v", err)
			}
			if got != tc.want {
				t.Errorf("got=%+v want=%+v", got, tc.want)
			}
		})
	}
}

func TestValkeyVersion_String(t *testing.T) {
	t.Parallel()
	cases := []struct {
		v    ValkeyVersion
		want string
	}{
		{ValkeyVersion{8, 0}, "8.0"},
		{ValkeyVersion{8, 42}, "8.42"},
		{ValkeyVersion{9, 1}, "9.1"},
	}
	for _, tc := range cases {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("%+v.String() = %q, want %q", tc.v, got, tc.want)
		}
	}
}

func TestIsSupportedMajor(t *testing.T) {
	t.Parallel()
	if !IsSupportedMajor(ValkeyVersion{8, 0}) {
		t.Errorf("major 8 must be in SupportedMajors (GA-tested)")
	}
	if !IsSupportedMajor(ValkeyVersion{9, 0}) {
		t.Errorf("major 9 must be in SupportedMajors (admitted as of v1.0; best-effort during alpha)")
	}
	if IsSupportedMajor(ValkeyVersion{7, 4}) {
		t.Errorf("major 7 (pre-Valkey-8 era) must NOT be supported")
	}
	if IsSupportedMajor(ValkeyVersion{10, 0}) {
		t.Errorf("major 10 must NOT be supported until docs/versions.md adds it")
	}
}

func TestIsDowngrade(t *testing.T) {
	t.Parallel()
	cases := []struct {
		from, to ValkeyVersion
		want     bool
		desc     string
	}{
		{ValkeyVersion{8, 0}, ValkeyVersion{7, 9}, true, "8 → 7 is a downgrade"},
		{ValkeyVersion{9, 0}, ValkeyVersion{8, 99}, true, "9 → 8 is a downgrade (regardless of minor)"},
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 0}, false, "same is not a downgrade"},
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 1}, false, "minor-up within major is not a downgrade"},
		{ValkeyVersion{8, 1}, ValkeyVersion{8, 0}, false, "minor-down within major is not gated by the major-downgrade rule"},
		{ValkeyVersion{8, 0}, ValkeyVersion{9, 0}, false, "major-up is not a downgrade"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			if got := IsDowngrade(tc.from, tc.to); got != tc.want {
				t.Errorf("IsDowngrade(%+v, %+v) = %v, want %v", tc.from, tc.to, got, tc.want)
			}
		})
	}
}

func TestIsSkipMinor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		from, to ValkeyVersion
		want     bool
		desc     string
	}{
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 0}, false, "same is not a skip"},
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 1}, false, "+1 minor is the supported step"},
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 2}, true, "+2 minor is a skip"},
		{ValkeyVersion{8, 0}, ValkeyVersion{8, 5}, true, "+5 minor is a skip"},
		{ValkeyVersion{8, 1}, ValkeyVersion{8, 0}, false, "minor-down is not a skip"},
		{ValkeyVersion{8, 5}, ValkeyVersion{9, 0}, false, "major-bump is not gated by the skip-minor rule"},
		{ValkeyVersion{8, 0}, ValkeyVersion{9, 5}, false, "cross-major skip is not gated here either"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			if got := IsSkipMinor(tc.from, tc.to); got != tc.want {
				t.Errorf("IsSkipMinor(%+v, %+v) = %v, want %v", tc.from, tc.to, got, tc.want)
			}
		})
	}
}
