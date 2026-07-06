// Package forbidden contains patterns the SSAUseHelper analyzer must flag.
package forbidden

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func DirectApplyIsForbidden(ctx context.Context, c client.Client, obj client.Object) error {
	return c.Patch(ctx, obj, client.Apply) // want `direct client\.Apply is forbidden`
}

func DirectStatusApplyIsForbidden(ctx context.Context, c client.Client, obj client.Object) error {
	return c.Status().Patch(ctx, obj, client.Apply) // want `direct client\.Apply is forbidden`
}
