// Package client is a minimal stub of sigs.k8s.io/controller-runtime/pkg/client
// used by the analysistest suite in analyzers/. Real imports are replaced by
// this skeleton so analysistest doesn't need the full dependency.
package client

import "context"

type Object interface{}
type ObjectList interface{}
type Patch interface{}
type PatchOption interface{}

// ListOption is an interface so types.Implements(T, ListOption) can succeed
// for each of the option types below. The marker method mirrors the real
// controller-runtime API shape.
type ListOption interface {
	ApplyToList(*ListOptions)
}

type ListOptions struct{}

type Client interface {
	Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error
	List(ctx context.Context, list ObjectList, opts ...ListOption) error
	Status() StatusWriter
}

type Reader interface {
	List(ctx context.Context, list ObjectList, opts ...ListOption) error
}

type StatusWriter interface {
	Patch(ctx context.Context, obj Object, patch Patch, opts ...SubResourcePatchOption) error
}

type SubResourcePatchOption interface{}

// Apply is the deprecated patch-type marker; the analyzer flags any reference
// to this variable outside internal/util/ssa.
var Apply Patch = nil

type InNamespace string

func (n InNamespace) ApplyToList(*ListOptions) {}

type MatchingLabels map[string]string

func (m MatchingLabels) ApplyToList(*ListOptions) {}

type MatchingFields map[string]string

func (m MatchingFields) ApplyToList(*ListOptions) {}

type MatchingLabelsSelector struct{}

func (m MatchingLabelsSelector) ApplyToList(*ListOptions) {}

type MatchingFieldsSelector struct{}

func (m MatchingFieldsSelector) ApplyToList(*ListOptions) {}

type Limit int64

func (l Limit) ApplyToList(*ListOptions) {}

type FieldOwner string

func (f FieldOwner) ApplyToPatch(any) {}
