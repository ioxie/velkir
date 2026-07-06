package bad

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NoOptsAtAll(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list) // want `client\.List without scoping options is unbounded`
}

// Limit alone is NOT scoping — pagination hint, not a bound.
func LimitAlone(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.Limit(100)) // want `client\.List without scoping options is unbounded`
}

// Empty MatchingLabels composite is functionally no-op.
func EmptyMatchingLabels(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.MatchingLabels{}) // want `client\.List without scoping options is unbounded`
}

// Empty MatchingLabelsSelector is unbounded at runtime.
func EmptyMatchingLabelsSelector(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.MatchingLabelsSelector{}) // want `client\.List without scoping options is unbounded`
}

// Empty MatchingFieldsSelector is unbounded at runtime.
func EmptyMatchingFieldsSelector(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.MatchingFieldsSelector{}) // want `client\.List without scoping options is unbounded`
}
