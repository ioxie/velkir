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

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// authSecretNameField indexes Valkey CRs by the auth Secret they reference
// (spec.auth.secretName). The Secret→CR reverse map (mapAuthSecretToCRs)
// lists by this index so a changed auth Secret enqueues only the CR(s) that
// actually reference it, never every CR.
const authSecretNameField = ".spec.auth.secretName"

// indexValkeyByAuthSecretName is the index extractor for authSecretNameField.
// A CR with no auth block, or an empty secretName, contributes no entry.
func indexValkeyByAuthSecretName(obj client.Object) []string {
	v, ok := obj.(*valkeyv1beta1.Valkey)
	if !ok || v.Spec.Auth == nil || v.Spec.Auth.SecretName == "" {
		return nil
	}
	return []string{v.Spec.Auth.SecretName}
}

// authSecretMetaObject is the typed handle for the metadata-only Secret
// informer: a PartialObjectMetadata stamped with the Secret GVK. Watching
// via PartialObjectMetadata keeps the informer metadata-only, so the
// user-owned auth Secret's `data` is never cached — only the change signal
// reaches the workqueue and the reconciler re-reads the Secret via APIReader
// exactly as before.
func authSecretMetaObject() *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{}
	m.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return m
}

// mapAuthSecretToCRs maps a changed auth Secret to reconcile requests for
// every Valkey CR in the Secret's namespace that references it by name,
// resolved through the authSecretNameField index. A Secret no CR references
// — the overwhelming majority of cluster Secrets the unfiltered informer
// sees — resolves to zero requests, so the watch never translates unrelated
// Secret churn into reconciles.
func (r *ValkeyReconciler) mapAuthSecretToCRs(ctx context.Context, obj *metav1.PartialObjectMetadata) []reconcile.Request {
	if obj == nil || obj.GetName() == "" {
		return nil
	}
	var list valkeyv1beta1.ValkeyList
	if err := r.List(ctx, &list,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{authSecretNameField: obj.GetName()},
	); err != nil {
		// A sustained index/List failure would silently revert auth-rotation
		// responsiveness to the baseline watchdog; log so it is visible rather
		// than dropping the enqueue without a trace.
		logf.FromContext(ctx).Error(err, "auth-secret watch: listing CRs by auth-secret index failed",
			"secret", obj.GetNamespace()+"/"+obj.GetName())
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: list.Items[i].Namespace,
			Name:      list.Items[i].Name,
		}})
	}
	return reqs
}

// authSecretCacheNamespaces mirrors the operator's configured watch scope
// onto the dedicated Secret-metadata cache: nil (cluster scope) when the
// operator watches the whole cluster, or the configured set when scoped —
// matching cmd/main.go::buildCacheOptions.
func (r *ValkeyReconciler) authSecretCacheNamespaces() map[string]cache.Config {
	if len(r.WatchNamespaces) == 0 {
		return nil
	}
	ns := make(map[string]cache.Config, len(r.WatchNamespaces))
	for _, n := range r.WatchNamespaces {
		ns[n] = cache.Config{}
	}
	return ns
}

// authSecretWatchSource registers the spec.auth.secretName field index on
// the manager's main cache and returns a raw watch source backed by a
// dedicated, metadata-only Secret informer.
//
// Why a dedicated cache: the manager's main informer cache narrows Secret to
// `app.kubernetes.io/managed-by=velkir` (cmd/main.go::
// buildCacheOptions), and that selector is resolved per-GVK — a metadata
// watch routed through the main cache would inherit the label filter and
// never observe the unlabeled, user-owned auth Secret. A separate cache with
// no label selector observes those Secrets, while PartialObjectMetadata keeps
// it metadata-only so no Secret `data` is ever cached (the read-path security
// boundary documented in docs/security/ stays intact). The field-index map
// keeps the unfiltered informer from generating reconciles for Secrets no CR
// references.
func (r *ValkeyReconciler) authSecretWatchSource(mgr ctrl.Manager) (source.Source, error) {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(), &valkeyv1beta1.Valkey{}, authSecretNameField, indexValkeyByAuthSecretName,
	); err != nil {
		return nil, err
	}

	secretCache, err := cache.New(mgr.GetConfig(), cache.Options{
		Scheme:            mgr.GetScheme(),
		Mapper:            mgr.GetRESTMapper(),
		DefaultNamespaces: r.authSecretCacheNamespaces(),
	})
	if err != nil {
		return nil, err
	}
	// Start/stop the dedicated cache with the manager. source.Kind waits
	// for this cache to sync before the controller processes events.
	if err := mgr.Add(secretCache); err != nil {
		return nil, err
	}

	return source.Kind(
		secretCache,
		authSecretMetaObject(),
		handler.TypedEnqueueRequestsFromMapFunc(r.mapAuthSecretToCRs),
		predicate.TypedResourceVersionChangedPredicate[*metav1.PartialObjectMetadata]{},
	), nil
}
