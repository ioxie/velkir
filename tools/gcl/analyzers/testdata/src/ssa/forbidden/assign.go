package forbidden

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Package-level var initializer.
var packageLevel = client.Apply // want `direct client\.Apply is forbidden`

// Local variable assignment.
func ShortVarDecl(ctx context.Context, c client.Client, obj client.Object) error {
	q := client.Apply // want `direct client\.Apply is forbidden`
	return c.Patch(ctx, obj, q)
}

// var statement inside a function.
func VarStmt(ctx context.Context, c client.Client, obj client.Object) error {
	var p = client.Apply // want `direct client\.Apply is forbidden`
	return c.Patch(ctx, obj, p)
}
