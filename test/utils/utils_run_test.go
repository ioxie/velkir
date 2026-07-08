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

package utils

import (
	"os/exec"
	"strings"
	"testing"
)

// Run must return only the command's stdout. Previously it used
// cmd.CombinedOutput(), merging stderr into stdout, so a kubectl
// deprecation warning (k8s >= 1.33) leaked into parsed output and broke
// token/jsonpath assertions.
func TestRunReturnsStdoutOnly(t *testing.T) {
	cmd := exec.Command("sh", "-c", "echo out-line; echo err-line 1>&2")
	out, err := Run(cmd)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if got := strings.TrimSpace(out); got != "out-line" {
		t.Fatalf("Run must return only stdout; got %q, want %q", got, "out-line")
	}
	if strings.Contains(out, "err-line") {
		t.Fatalf("stderr leaked into Run's stdout output: %q", out)
	}
}

// On a non-zero exit, Run must surface stderr in the returned error so a
// failing command stays debuggable even though stderr is excluded from the
// success-path output.
func TestRunErrorContainsStderr(t *testing.T) {
	cmd := exec.Command("sh", "-c", "echo diag-to-stderr 1>&2; exit 7")
	out, err := Run(cmd)
	if err == nil {
		t.Fatalf("Run must return an error for a non-zero exit; out=%q", out)
	}
	if !strings.Contains(err.Error(), "diag-to-stderr") {
		t.Fatalf("Run error must surface stderr for debuggability; got %v", err)
	}
}
