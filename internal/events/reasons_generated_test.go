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

package events

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestAllReasonsMatchesDeclaredConsts pins the single-source-of-truth
// invariant for the generated AllReasons(): its membership equals exactly the
// set of exported Reason-typed consts declared in the package. The declared
// set is re-derived straight from source here, so a hand-edited generated
// file or a new Reason const added without `make generate` fails at unit-test
// time — not only in CI's generate-diff gate. It also rejects duplicates.
func TestAllReasonsMatchesDeclaredConsts(t *testing.T) {
	t.Parallel()

	declared := declaredReasonValues(t)

	got := make(map[string]bool, len(AllReasons()))
	for _, r := range AllReasons() {
		if got[string(r)] {
			t.Errorf("AllReasons() lists duplicate reason %q", r)
		}
		got[string(r)] = true
	}

	for v := range declared {
		if !got[v] {
			t.Errorf("declared Reason %q is missing from AllReasons() — run `make generate`", v)
		}
	}
	for v := range got {
		if !declared[v] {
			t.Errorf("AllReasons() lists %q which is not a declared Reason const — run `make generate`", v)
		}
	}
}

// declaredReasonValues parses the package's own non-test source and returns
// the string value of every exported const whose declared type is Reason —
// independently of the generated AllReasons(), so the two can be compared.
func declaredReasonValues(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				id, ok := vs.Type.(*ast.Ident)
				if !ok || id.Name != "Reason" {
					continue
				}
				for i, n := range vs.Names {
					if !n.IsExported() {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("Reason const %s has a non-string-literal value; "+
							"generator and this test assume string literals", n.Name)
					}
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatalf("unquote value of %s: %v", n.Name, err)
					}
					out[v] = true
				}
			}
		}
	}
	return out
}
