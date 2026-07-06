package ok

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func WithInNamespace(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.InNamespace("default"))
}

func WithMatchingLabels(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.MatchingLabels{"app": "v"})
}

func WithMatchingFields(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, client.MatchingFields{"spec.node": "n"})
}

// Variable-held scoping option — accepted optimistically via the
// client.ListOption type check.
func VariableHeldInNamespace(ctx context.Context, c client.Client, list client.ObjectList) error {
	ns := client.InNamespace("default")
	return c.List(ctx, list, ns)
}

// Spread — c.List(ctx, list, opts...) — accepted via the slice-elem-implements
// check on client.ListOption.
func SpreadOpts(ctx context.Context, c client.Client, list client.ObjectList) error {
	opts := []client.ListOption{client.InNamespace("x")}
	return c.List(ctx, list, opts...)
}

// Helper function that returns a ListOption — realistic "factor out the
// defaults" pattern. The analyzer's CallExpr branch falls through to the
// types.Implements(client.ListOption) check when the call isn't an inline
// client.InNamespace.
func defaultScope(ns string) client.ListOption {
	return client.InNamespace(ns)
}

func HelperReturningListOption(ctx context.Context, c client.Client, list client.ObjectList) error {
	return c.List(ctx, list, defaultScope("x"))
}

// Map-to-selector type conversion — client.MatchingLabels(mapVar) — accepted
// via the types.Implements fall-through. Only client.Limit is blocked from
// the fall-through; other client-package calls are passed through.
func MatchingLabelsFromMap(ctx context.Context, c client.Client, list client.ObjectList) error {
	labels := map[string]string{"app": "v"}
	return c.List(ctx, list, client.MatchingLabels(labels))
}
