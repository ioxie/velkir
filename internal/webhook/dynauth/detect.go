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

package dynauth

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client" //nolint:revive // referenced via client.Reader in the public signature
)

// CertManagerCertificateGVK is the GVK of cert-manager's Certificate
// resource. Probed via the typed client's unstructured path so the
// operator binary doesn't drag in cert-manager's API types as a build
// dependency just to check for its presence.
var CertManagerCertificateGVK = schema.GroupVersionKind{
	Group:   "cert-manager.io",
	Version: "v1",
	Kind:    "Certificate",
}

// CertManagerOptedIn reports whether cert-manager is the authoritative
// cert provisioner in this cluster. True iff:
//
//  1. The cert-manager.io/v1 API group is installed (no NoMatchError on
//     the GVK).
//  2. A Certificate named LeafSecretName exists in the operator
//     namespace (cert-manager will populate the matching leaf Secret).
//
// When true, the caller MUST skip starting Authority + Injector — both
// would race cert-manager's controllers on the same Secret + the same
// caBundle field.
//
// The two-step shape (CRD-installed AND Certificate-present) is
// intentional: the cluster might run cert-manager generally without
// using it for this operator. A bare CRD presence isn't enough to
// transfer ownership.
//
// Takes client.Reader (Get + List only) so callers can pass an
// uninitialised manager's APIReader before mgr.Start — the typed
// cache hasn't been started yet at wire-up time.
func CertManagerOptedIn(ctx context.Context, c client.Reader, namespace string) (bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(CertManagerCertificateGVK)

	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: LeafSecretName}, obj)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err):
		// cert-manager CRD installed, but no Certificate of our name —
		// dynamic-authority owns the lifecycle.
		return false, nil
	case meta.IsNoMatchError(err):
		// cert-manager CRD not installed at all.
		return false, nil
	default:
		return false, err
	}
}
