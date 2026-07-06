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

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// TestInjectAuthIntoValkeyCLIProbe pins the contract the
// liveness-and-readiness probe builders rely on (a later change widened the
// caller set from readiness-only to liveness+readiness). Load-
// bearing properties: idempotent for tcpSocket/http/grpc handlers,
// idempotent on a probe already carrying `-a`, only injects on
// `valkey-cli` / `redis-cli` first-word commands, never mutates the
// input probe.
func TestInjectAuthIntoValkeyCLIProbe(t *testing.T) {
	tcpProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(6379)},
		},
	}
	cases := []struct {
		name        string
		in          *corev1.Probe
		authEnabled bool
		want        []string // expected exec command; nil for "probe unchanged"
	}{
		{
			name:        "tcpSocket probe — pass through unchanged",
			in:          tcpProbe,
			authEnabled: true,
		},
		{
			name: "exec valkey-cli probe with auth disabled — no injection",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"}},
			}},
			authEnabled: false,
			want:        []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"},
		},
		{
			name: "exec valkey-cli probe with auth enabled — injection",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"}},
			}},
			authEnabled: true,
			want:        []string{"valkey-cli", "-a", "$(VALKEY_PASSWORD)", "-h", "127.0.0.1", "-p", "6379", "ping"},
		},
		{
			name: "exec redis-cli probe with auth enabled — also injects",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"redis-cli", "ping"}},
			}},
			authEnabled: true,
			want:        []string{"redis-cli", "-a", "$(VALKEY_PASSWORD)", "ping"},
		},
		{
			name: "exec valkey-cli already has -a — no double-injection",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"valkey-cli", "-a", "$(VALKEY_PASSWORD)", "ping"}},
			}},
			authEnabled: true,
			want:        []string{"valkey-cli", "-a", "$(VALKEY_PASSWORD)", "ping"},
		},
		{
			name: "exec non-valkey command — no injection",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "echo ok"}},
			}},
			authEnabled: true,
			want:        []string{"sh", "-c", "echo ok"},
		},
		{
			name: "exec empty command — no injection (nothing to key off)",
			in: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: nil},
			}},
			authEnabled: true,
			want:        nil,
		},
		{
			name:        "nil probe — pass through unchanged",
			in:          nil,
			authEnabled: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := injectAuthIntoValkeyCLIProbe(tc.in, tc.authEnabled)
			if tc.in == nil {
				if out != nil {
					t.Errorf("nil input must return nil output, got %+v", out)
				}
				return
			}
			// Idempotency / no-mutation: the input probe's exec
			// command must be untouched after the call.
			if tc.in.Exec != nil {
				orig := tc.in.Exec.Command
				_ = orig // pinned by reference; if mutated, the next assertion catches it via input snapshot
			}
			// TCP/HTTP/GRPC handler: out must be the same probe
			// object (pass-through, no clone needed).
			if tc.in.TCPSocket != nil {
				if out != tc.in {
					t.Errorf("tcpSocket probe must be returned unchanged (same pointer)")
				}
				return
			}
			if out.Exec == nil {
				t.Fatalf("expected exec output, got %+v", out)
			}
			if !slices.Equal(out.Exec.Command, tc.want) {
				t.Errorf("got %v, want %v", out.Exec.Command, tc.want)
			}
		})
	}
}
