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
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// NoUnboundedClientCall rejects controller-runtime client.List calls that
// don't carry at least one SCOPING option. An unbounded List in a cluster-
// scoped reconciler walks every object of the kind in the cluster — a
// ready-made hot loop / apiserver DOS.
//
// Accepted scoping patterns:
//
//   - client.InNamespace(...)            — inline call
//   - client.MatchingLabels{...}         — non-empty composite literal
//   - client.MatchingFields{...}         — non-empty composite literal
//   - client.MatchingLabelsSelector{...} — non-empty composite literal
//   - client.MatchingFieldsSelector{...} — non-empty composite literal
//   - any arg (ident / selector / spread) whose TYPE implements
//     client.ListOption — optimistic acceptance for variable-held options
//     and `opts...` spread. Trades strictness for ergonomics: a dev who
//     factors out a defaultListOpts() helper still gets their code
//     accepted. Off-label use (a local typed as `client.Limit`) slips
//     through by design; PR review is the backstop.
//
// Notable rejections:
//
//   - client.Limit(n) ALONE  — Limit is a pagination hint, not a scope.
//     Records after the first page are silently missed without a Continue
//     loop. Reject when Limit is the sole option so the bug doesn't hide
//     behind a fake-bounded shape.
//   - empty composite literals (client.MatchingLabels{}) — functionally
//     equivalent to no option at all.
//
// Escape hatch: `//nolint:no_unbounded_client_call` for the rare site that
// needs a genuinely unbounded List.
var NoUnboundedClientCall = &analysis.Analyzer{
	Name:     "no_unbounded_client_call",
	Doc:      "rejects controller-runtime client.List calls that are not scoped by namespace, selector, or similar",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runNoUnboundedClientCall,
}

func runNoUnboundedClientCall(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	listOption := findListOptionInterface(pass)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		if !isControllerRuntimeClientListCall(pass, call) {
			return
		}
		// List(ctx, list, opts...) — opts start at index 2.
		if len(call.Args) < 3 {
			reportUnboundedList(pass, call)
			return
		}
		for _, arg := range call.Args[2:] {
			if isScopingArg(pass, arg, listOption) {
				return
			}
		}
		reportUnboundedList(pass, call)
	})
	return nil, nil
}

func reportUnboundedList(pass *analysis.Pass, call *ast.CallExpr) {
	pass.Report(analysis.Diagnostic{
		Pos:     call.Pos(),
		End:     call.End(),
		Message: "client.List without scoping options is unbounded; add client.InNamespace / MatchingLabels / MatchingFields (Limit alone does not scope)",
	})
}

// isControllerRuntimeClientListCall matches c.List(ctx, list, opts...) where
// c's type is controller-runtime's client.Client / client.Reader.
func isControllerRuntimeClientListCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil || sel.Sel.Name != "List" {
		return false
	}
	obj := pass.TypesInfo.Uses[sel.Sel]
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	recv := fn.Signature().Recv()
	if recv == nil {
		return false
	}
	recvType := recv.Type()
	if ptr, ok := recvType.(*types.Pointer); ok {
		recvType = ptr.Elem()
	}
	named, ok := recvType.(*types.Named)
	if !ok {
		return false
	}
	pkg := named.Obj().Pkg()
	if pkg == nil || pkg.Path() != controllerRuntimeClientPath {
		return false
	}
	switch named.Obj().Name() {
	case "Client", "Reader":
		return true
	}
	return false
}

// isScopingArg determines whether a single arg to c.List is scope-bounding.
// Returns true only for the specific accepted patterns in the doc-comment.
func isScopingArg(pass *analysis.Pass, arg ast.Expr, listOption *types.Interface) bool {
	// Strip &foo{...}.
	if u, ok := arg.(*ast.UnaryExpr); ok && u.Op == token.AND {
		arg = u.X
	}

	// Inline composite literals are classified precisely — empty {} is a
	// runtime no-op, so we can't fall through to the types.Implements
	// branch (which would wrongly accept it).
	if composite, ok := arg.(*ast.CompositeLit); ok {
		return isScopingComposite(pass, composite)
	}
	// Inline calls: recognize client.InNamespace(...) as scoping. If the
	// call doesn't match that whitelisted shape and isn't one of the
	// known-non-scoping client-package calls (currently just client.Limit,
	// which returns a ListOption by construction but bounds only one
	// page), fall through to the types.Implements check.
	//
	// Why a targeted block instead of "any client.* call"? It's narrower
	// by design: client.MatchingLabels(mapVar) and client.MatchingFields(mapVar)
	// as type-conversion calls genuinely scope the List, and rejecting
	// them would break a realistic helper-building pattern. Only Limit is
	// structurally non-scoping; add new client-package calls here as the
	// API grows and any show up that shouldn't scope (Limit, Continue, ...).
	if call, ok := arg.(*ast.CallExpr); ok {
		if isScopingCall(pass, call) {
			return true
		}
		if isPaginationOnlyCall(pass, call) {
			return false
		}
		// fall through to the types.Implements(...) check below
	}

	// Indirect shapes (ident, selector, helper-call, spread):
	// optimistically accept if the arg's type implements client.ListOption.
	// Can't resolve the runtime value; trust the dev + PR review. See
	// doc-comment for the Limit-via-var escape caveat.
	if listOption == nil {
		return false
	}
	tv, ok := pass.TypesInfo.Types[arg]
	if !ok || tv.Type == nil {
		return false
	}
	t := tv.Type
	// Spread: c.List(ctx, list, opts...) — the arg is the []ListOption slice.
	if slice, ok := t.Underlying().(*types.Slice); ok {
		t = slice.Elem()
	}
	return types.Implements(t, listOption) ||
		types.Implements(types.NewPointer(t), listOption)
}

func isScopingComposite(pass *analysis.Pass, composite *ast.CompositeLit) bool {
	sel, ok := composite.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil {
		return false
	}
	if !identIsInControllerRuntimeClientPkg(pass, sel.Sel) {
		return false
	}
	switch sel.Sel.Name {
	case "MatchingLabels", "MatchingFields",
		"MatchingLabelsSelector", "MatchingFieldsSelector":
		// Empty composite literal is functionally no-op; require at least
		// one element to claim scope.
		return len(composite.Elts) > 0
	}
	return false
}

func isScopingCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil {
		return false
	}
	if !identIsInControllerRuntimeClientPkg(pass, sel.Sel) {
		return false
	}
	// Limit is explicitly NOT scoping — see doc comment.
	// MatchingLabels / MatchingFields as calls (converting from a map) are
	// accepted only as composite literals (handled in isScopingComposite).
	if sel.Sel.Name == "InNamespace" {
		return true
	}
	return false
}

// isPaginationOnlyCall reports whether call is a known-non-scoping call
// into the controller-runtime client package — currently only client.Limit.
// Limit bounds a single page; records after the first page are silently
// missed without a Continue-driven loop. Used to block the fall-through
// accept so `client.Limit(100)` alone still flags as unbounded even though
// its return type implements client.ListOption.
//
// Other client-package calls (MatchingLabels(mapVar), MatchingFields(mapVar))
// genuinely scope and are accepted by the fall-through's types.Implements
// check. Grow this set only for structurally non-scoping client symbols
// (e.g. Continue, if it's ever added to the API).
func isPaginationOnlyCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil {
		return false
	}
	if !identIsInControllerRuntimeClientPkg(pass, sel.Sel) {
		return false
	}
	return sel.Sel.Name == "Limit"
}

func identIsInControllerRuntimeClientPkg(pass *analysis.Pass, id *ast.Ident) bool {
	obj := pass.TypesInfo.Uses[id]
	if obj == nil {
		return false
	}
	pkg := obj.Pkg()
	return pkg != nil && pkg.Path() == controllerRuntimeClientPath
}

// findListOptionInterface returns the *types.Interface for
// sigs.k8s.io/controller-runtime/pkg/client.ListOption, or nil if the
// package isn't imported by pass.Pkg (in which case no List call can be
// happening anyway and the analyzer's List check won't fire).
func findListOptionInterface(pass *analysis.Pass) *types.Interface {
	for _, imp := range pass.Pkg.Imports() {
		if imp.Path() != controllerRuntimeClientPath {
			continue
		}
		obj := imp.Scope().Lookup("ListOption")
		if obj == nil {
			return nil
		}
		tn, ok := obj.(*types.TypeName)
		if !ok {
			return nil
		}
		iface, _ := tn.Type().Underlying().(*types.Interface)
		return iface
	}
	return nil
}
