// Package ssa simulates internal/util/ssa for the analyzer's allowlist.
// Calls here to client.Apply must NOT be flagged.
package ssa

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Apply(ctx context.Context, c client.Client, obj client.Object) error {
	return c.Patch(ctx, obj, client.Apply) // OK — inside the helper
}
