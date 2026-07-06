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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runInitScript renders the production init script, rewrites its three
// absolute mount paths to a temp root, stubs `valkey-cli` (returns
// cliReply, or a non-zero exit when cliReply is "") and `timeout` on
// PATH, then executes it under /bin/sh with the given env. Returns the
// rendered /config/valkey.conf. This exercises the script's real
// replicaof branching — the data-plane half that has no other
// coverage.
func runInitScript(t *testing.T, env map[string]string, seedFileContent *string, cliReply string) string {
	t.Helper()
	root := t.TempDir()
	for _, d := range []string{"config-template", "config", "bootstrap", "bin"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	// Template the init script consumes (with the _POD_IP_ placeholder).
	if err := os.WriteFile(filepath.Join(root, "config-template", "valkey.conf"),
		[]byte("bind _POD_IP_\nport 6379\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if seedFileContent != nil {
		if err := os.WriteFile(filepath.Join(root, "bootstrap", "seedMasterIP"),
			[]byte(*seedFileContent), 0o644); err != nil {
			t.Fatalf("write seed: %v", err)
		}
	}

	// Stub valkey-cli: echo the scripted reply and exit 0; empty reply
	// simulates connection-refused (non-zero exit, no output).
	var cli string
	if cliReply == "" {
		cli = "#!/bin/sh\nexit 1\n"
	} else {
		cli = "#!/bin/sh\necho '" + cliReply + "'\n"
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "valkey-cli"), []byte(cli), 0o755); err != nil {
		t.Fatalf("write valkey-cli stub: %v", err)
	}
	// Stub timeout: drop the duration arg, exec the rest.
	if err := os.WriteFile(filepath.Join(root, "bin", "timeout"),
		[]byte("#!/bin/sh\nshift\nexec \"$@\"\n"), 0o755); err != nil {
		t.Fatalf("write timeout stub: %v", err)
	}

	script := renderInitScript()
	// Rewrite the three absolute mount roots to the temp tree.
	script = strings.ReplaceAll(script, "/config-template/", filepath.Join(root, "config-template")+"/")
	script = strings.ReplaceAll(script, "/config/", filepath.Join(root, "config")+"/")
	script = strings.ReplaceAll(script, "/bootstrap/", filepath.Join(root, "bootstrap")+"/")
	scriptPath := filepath.Join(root, "render.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	cmd := exec.Command("/bin/sh", scriptPath) //nolint:gosec // fixed test script
	cmd.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"))
	for k, val := range env {
		cmd.Env = append(cmd.Env, k+"="+val)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init script failed: %v\noutput: %s\nscript:\n%s", err, out, script)
	}
	got, err := os.ReadFile(filepath.Join(root, "config", "valkey.conf"))
	if err != nil {
		t.Fatalf("read rendered conf: %v", err)
	}
	return string(got)
}

func TestRenderInitScript_ReplicaofMatrix(t *testing.T) {
	const (
		appName   = "vk0"
		seedIP    = "10.0.0.5"
		ownIP     = "10.0.0.9"
		pod0      = "vk0-0"
		pod1      = "vk0-1"
		seedLine  = "replicaof 10.0.0.5 6379"
		dnsTarget = "replicaof vk0-0.vk0-headless.ns.svc.cluster.local 6379"
	)
	str := func(s string) *string { return &s }

	cases := []struct {
		name       string
		podName    string
		podIP      string
		seedFile   *string
		cliReply   string
		wantSubstr string // "" means assert NO replicaof line
	}{
		{"pod-0 live seed PONG joins as replica", pod0, ownIP, str(seedIP), "PONG", seedLine},
		{"pod-0 seed NOAUTH counts as live", pod0, ownIP, str(seedIP), "NOAUTH Authentication required", seedLine},
		{"pod-0 seed LOADING counts as live", pod0, ownIP, str(seedIP), "LOADING dataset", seedLine},
		{"pod-0 dead seed boots as master", pod0, ownIP, str(seedIP), "", ""},
		{"pod-0 seed equals own IP", pod0, seedIP, str(seedIP), "PONG", ""},
		{"pod-0 empty seed boots as master", pod0, ownIP, str(""), "PONG", ""},
		{"non-pod-0 with seed joins", pod1, "10.0.0.10", str(seedIP), "", seedLine},
		{"non-pod-0 no seed uses DNS fallback", pod1, "10.0.0.10", str(""), "", dnsTarget},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{
				"POD_IP":        tc.podIP,
				"POD_NAME":      tc.podName,
				"APP_NAME":      appName,
				"POD_NAMESPACE": "ns",
			}
			out := runInitScript(t, env, tc.seedFile, tc.cliReply)
			has := strings.Contains(out, "replicaof ")
			if tc.wantSubstr == "" {
				if has {
					t.Errorf("expected NO replicaof line; got:\n%s", out)
				}
				return
			}
			if !strings.Contains(out, tc.wantSubstr) {
				t.Errorf("expected %q in output; got:\n%s", tc.wantSubstr, out)
			}
		})
	}
}
