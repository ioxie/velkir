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

// Package analyzers implements the operator's three custom golangci-lint
// analyzers. Each one pairs with a specific invariant the project's design
// relies on; the analyzer's doc-comment names the invariant.
package analyzers

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// SSAUseHelper rejects direct references to controller-runtime's deprecated
// client.Apply patch constant outside internal/util/ssa. All operator SSA
// must flow through the helper so the DeepCopy + SetManagedFields(nil) +
// FieldOwner dance can't be forgotten.
//
// The analyzer walks three AST node kinds to catch the common bypass
// patterns:
//
//   - *ast.CallExpr — any arg that resolves to client.Apply
//     (e.g. c.Patch(ctx, obj, client.Apply, ...))
//   - *ast.AssignStmt — any RHS that resolves to client.Apply
//     (e.g. p := client.Apply; c.Patch(ctx, obj, p))
//   - *ast.ValueSpec — any initializer that resolves to client.Apply
//     (e.g. var p = client.Apply)
//
// Invariant: the only package permitted to reference client.Apply is
// internal/util/ssa (the helper itself). Exact path match — no accidental
// allowlisting of vendor'd forks that share the suffix.
var SSAUseHelper = &analysis.Analyzer{
	Name:     "ssa_use_helper",
	Doc:      "rejects direct client.Apply references outside internal/util/ssa (covers call args, assignments, and var decls)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runSSAUseHelper,
}

const (
	controllerRuntimeClientPath = "sigs.k8s.io/controller-runtime/pkg/client"
	ssaHelperPath               = "github.com/ioxie/velkir/internal/util/ssa"
)

func runSSAUseHelper(pass *analysis.Pass) (any, error) {
	if pass.Pkg != nil && pass.Pkg.Path() == ssaHelperPath {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	filter := []ast.Node{
		(*ast.CallExpr)(nil),
		(*ast.AssignStmt)(nil),
		(*ast.ValueSpec)(nil),
	}
	insp.Preorder(filter, func(n ast.Node) {
		switch x := n.(type) {
		case *ast.CallExpr:
			for _, arg := range x.Args {
				reportIfClientApplyRef(pass, arg)
			}
		case *ast.AssignStmt:
			for _, rhs := range x.Rhs {
				reportIfClientApplyRef(pass, rhs)
			}
		case *ast.ValueSpec:
			for _, val := range x.Values {
				reportIfClientApplyRef(pass, val)
			}
		}
	})
	return nil, nil
}

func reportIfClientApplyRef(pass *analysis.Pass, expr ast.Expr) {
	if !isClientApplyRef(pass, expr) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:     expr.Pos(),
		End:     expr.End(),
		Message: "direct client.Apply is forbidden outside " + ssaHelperPath + "; use ssa.Apply or ssa.ApplyStatus",
	})
}

// isClientApplyRef reports whether expr references the client.Apply variable
// from sigs.k8s.io/controller-runtime/pkg/client.
func isClientApplyRef(pass *analysis.Pass, expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil || sel.Sel.Name != "Apply" {
		return false
	}
	obj := pass.TypesInfo.Uses[sel.Sel]
	if obj == nil {
		return false
	}
	pkg := obj.Pkg()
	if pkg == nil {
		return false
	}
	if pkg.Path() != controllerRuntimeClientPath {
		return false
	}
	return isExportedVar(obj)
}

func isExportedVar(obj types.Object) bool {
	v, ok := obj.(*types.Var)
	if !ok {
		return false
	}
	return v.Exported()
}
