// Package helper contains NON-forbidden patch call shapes — regular Patch
// with a non-Apply marker. The analyzer must leave these alone.
package helper

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func AcceptableMergePatch(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
	return c.Patch(ctx, obj, p) // OK — the patch is not client.Apply
}
