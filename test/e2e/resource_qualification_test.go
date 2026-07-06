//go:build e2e
// +build e2e

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

package e2e

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

// selfFile is this file's basename; the scan skips it because the
// checker necessarily spells out the bare forms it forbids.
const selfFile = "resource_qualification_test.go"

// bareCRLiteral matches a Go interpreted string literal whose entire
// content is a bare custom-resource type — singular, plural, or
// shortname — or a comma-list of such types (e.g. "valkey",
// "sentinelquorums", "valkey,sentinelquorum").
var bareCRLiteral = regexp.MustCompile(
	`"(?:valkeys?|vk|sentinelquorums?|sq)(?:,(?:valkeys?|vk|sentinelquorums?|sq))*"`)

// TestNoBareCRResourceTypes pins the convention that every kubectl
// invocation in this package references the custom resources by their
// group-qualified plural (valkeys.velkir.ioxie.dev,
// sentinelquorums.velkir.ioxie.dev), never by a bare type. A bare
// "valkey"/"sentinelquorum" is ambiguous on any cluster that carries a
// same-named CRD from another API group: kubectl fails with "must
// specify only one resource", the Run helpers swallow the error, and
// the suite wedges silently.
//
// The check is a static file scan (no cluster needed) with a
// documented heuristic. A string literal on a line is a violation
// only when ALL of the following hold:
//
//  1. the line is not a `//` comment line;
//  2. the literal's full content is a bare CR type or comma-list of
//     them (bareCRLiteral above);
//  3. the literal is not immediately followed by ':' — that shape is
//     a JSON object key inside a raw-string merge-patch payload
//     (`{"spec":{"valkey":{...}}}`), i.e. the CRD's spec field, not a
//     resource-type argument;
//  4. the literal is not immediately preceded by `"-c",` — that shape
//     is the container-name argument to `kubectl exec -c`, where the
//     container really is named "valkey";
//  5. `"kubectl"` appears on the same line or within the 10 preceding
//     lines — restricting the scan to kubectl exec.Command slices, so
//     unrelated literals elsewhere in a file cannot trip it.
func TestNoBareCRResourceTypes(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading test directory: %v", err)
	}

	var violations []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == selfFile || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			for _, loc := range bareCRLiteral.FindAllStringIndex(line, -1) {
				start, end := loc[0], loc[1]
				// (3) JSON object key inside a raw-string patch payload.
				if end < len(line) && line[end] == ':' {
					continue
				}
				// (4) container-name argument to `kubectl exec -c`.
				before := strings.TrimRight(line[:start], " \t")
				if strings.HasSuffix(before, `"-c",`) {
					continue
				}
				if before == "" && i > 0 &&
					strings.HasSuffix(strings.TrimRight(lines[i-1], " \t"), `"-c",`) {
					continue
				}
				// (5) only flag kubectl-adjacent literals.
				if !nearKubectl(lines, i) {
					continue
				}
				violations = append(violations, fmt.Sprintf(
					"%s:%d: bare CR resource type %s — use the group-qualified plural",
					entry.Name(), i+1, line[start:end]))
			}
		}
	}

	if len(violations) > 0 {
		t.Errorf("kubectl must reference custom resources by group-qualified plural "+
			"(valkeys.velkir.ioxie.dev / sentinelquorums.velkir.ioxie.dev); "+
			"found %d bare reference(s):\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}

// nearKubectl reports whether the line at idx, or any of the 10 lines
// above it, mentions the kubectl binary as a string literal.
func nearKubectl(lines []string, idx int) bool {
	lo := idx - 10
	if lo < 0 {
		lo = 0
	}
	for _, l := range lines[lo : idx+1] {
		if strings.Contains(l, `"kubectl"`) {
			return true
		}
	}
	return false
}
