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

package analyzers

import (
	"go/ast"
	"go/constant"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// EventsCatalogMembership rejects record.EventRecorder Event/Eventf/AnnotatedEventf
// calls whose reason argument is a string literal (or const) that isn't
// declared in internal/events as a Reason-typed constant.
//
// "Reason-typed" is literal: only exported constants whose Go type is the
// `internal/events.Reason` named type contribute to the catalog. Plain
// untyped string consts in the events package are ignored — the analyzer
// is the single source of truth, and tightening to a specific type stops
// unrelated string consts (e.g. internal helper names) from silently
// expanding the accept set.
//
// Enforcement grows with the catalog as features ship.
//
// Escape hatches (intentional):
//
//   - reason passed as a non-constant expression (runtime variable) — the
//     analyzer can't resolve dynamic values. Rare in practice; reviewers
//     catch these at PR review.
//   - calls inside the internal/events package itself — the catalog owns
//     its own reasons.
var EventsCatalogMembership = &analysis.Analyzer{
	Name:     "events_catalog_membership",
	Doc:      "rejects EventRecorder Event/Eventf calls whose reason is not declared as a Reason-typed const in internal/events",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runEventsCatalogMembership,
}

const (
	clientGoRecordPath = "k8s.io/client-go/tools/record"
	eventsPath         = "github.com/ioxie/velkir/internal/events"
)

func runEventsCatalogMembership(pass *analysis.Pass) (any, error) {
	// Skip the catalog package itself.
	if pass.Pkg != nil && pass.Pkg.Path() == eventsPath {
		return nil, nil
	}

	// Build the set of known reasons from internal/events.
	// Imported packages are accessible via pass.Pkg.Imports().
	known := knownReasons(pass)
	// If the catalog is empty (expected for early stages), accept everything;
	// as reasons land, the set grows and enforcement tightens automatically.
	enforce := len(known) > 0

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		reasonArgIdx, matched := eventRecorderReasonArgIndex(pass, call)
		if !matched {
			return
		}
		if reasonArgIdx >= len(call.Args) {
			return
		}
		reason, okLit := stringConstValue(pass, call.Args[reasonArgIdx])
		if !okLit {
			// Non-literal reason; skip (see doc comment on escape hatches).
			return
		}
		if !enforce {
			return
		}
		if _, ok := known[reason]; !ok {
			pass.Report(analysis.Diagnostic{
				Pos:     call.Args[reasonArgIdx].Pos(),
				End:     call.Args[reasonArgIdx].End(),
				Message: "event reason " + reason + " is not declared in " + eventsPath + " catalog",
			})
		}
	})
	return nil, nil
}

// knownReasons walks the imported internal/events package and collects every
// exported const whose Go type is `Reason`. Plain string consts are
// intentionally ignored — the analyzer's accept set is defined by the
// Reason type, so only deliberate catalog entries expand it.
func knownReasons(pass *analysis.Pass) map[string]struct{} {
	out := map[string]struct{}{}
	var eventsPkg *types.Package
	for _, imp := range pass.Pkg.Imports() {
		if imp.Path() == eventsPath {
			eventsPkg = imp
			break
		}
	}
	if eventsPkg == nil {
		return out
	}
	scope := eventsPkg.Scope()
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		c, ok := obj.(*types.Const)
		if !ok || !c.Exported() {
			continue
		}
		// Require the const's static type to be `Reason` (defined in the
		// events package). Rejects untyped string consts / helpers / etc.
		named, ok := c.Type().(*types.Named)
		if !ok {
			continue
		}
		if named.Obj().Name() != "Reason" || named.Obj().Pkg() == nil ||
			named.Obj().Pkg().Path() != eventsPath {
			continue
		}
		if c.Val().Kind() != constant.String {
			continue
		}
		out[constant.StringVal(c.Val())] = struct{}{}
	}
	return out
}

// eventRecorderReasonArgIndex reports which arg of the call is the `reason`
// string, for the three EventRecorder method shapes. Returns -1 / false when
// the call doesn't match.
func eventRecorderReasonArgIndex(pass *analysis.Pass, call *ast.CallExpr) (int, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil {
		return -1, false
	}
	obj := pass.TypesInfo.Uses[sel.Sel]
	fn, ok := obj.(*types.Func)
	if !ok {
		return -1, false
	}
	recv := fn.Signature().Recv()
	if recv == nil {
		return -1, false
	}
	recvType := recv.Type()
	if ptr, ok := recvType.(*types.Pointer); ok {
		recvType = ptr.Elem()
	}
	// record.EventRecorder is an interface — method calls on it have an
	// Interface receiver type with an object whose package is
	// k8s.io/client-go/tools/record.
	pkg := fn.Pkg()
	if pkg == nil || pkg.Path() != clientGoRecordPath {
		return -1, false
	}
	switch sel.Sel.Name {
	case "Event":
		// Event(obj runtime.Object, eventtype, reason, message string) — reason at 2
		return 2, true
	case "Eventf":
		// Eventf(obj, eventtype, reason, messageFmt string, args ...any) — reason at 2
		return 2, true
	case "AnnotatedEventf":
		// AnnotatedEventf(obj, annotations, eventtype, reason, messageFmt, args...) — reason at 3
		return 3, true
	}
	return -1, false
}

func stringConstValue(pass *analysis.Pass, arg ast.Expr) (string, bool) {
	tv, ok := pass.TypesInfo.Types[arg]
	if !ok || tv.Value == nil {
		return "", false
	}
	if tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}
