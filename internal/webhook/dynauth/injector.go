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
	"bytes"
	"context"
	"fmt"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	admissionv1ac "k8s.io/client-go/applyconfigurations/admissionregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ioxie/velkir/internal/util/ssa"
)

// Injector reconciles Mutating + Validating WebhookConfigurations carrying
// the inject-ca=true label, patching their per-webhook clientConfig.CABundle
// to match the current CA Secret. Reads the CA Secret on every reconcile
// (cheap; cached client) so a freshly rotated CA flows out to every webhook
// in the next reconcile pass.
//
// Only webhook entries whose clientConfig.service lives in the injector's
// own namespace are patched: the bundle is the trust anchor for endpoints
// THIS operator instance serves, so an entry pointing at another namespace
// (a second operator install, or a URL-based webhook) belongs to someone
// else's CA. Without that scoping, two installs on one cluster force-stamp
// each other's configs in a watch-triggered loop and exactly one of the two
// webhook endpoints is unverifiable at any instant.
//
// Distinct FieldOwner (CABundleFieldOwner) keeps SSA conflict resolution
// honest: a user manually editing caBundle hits a clean SSA conflict the
// next reconcile, instead of getting silently overwritten.
type Injector struct {
	Client    client.Client
	Namespace string
}

// Reconcile patches both webhook-config kinds. The trigger source (Secret
// vs MutatingWebhookConfiguration vs ValidatingWebhookConfiguration) is
// flattened in setup via a custom EnqueueRequestsFromMapFunc that maps any
// trigger to a single sentinel request, so Reconcile is parameter-shape-
// agnostic and always re-fans-out to all labelled configs.
func (in *Injector) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	caSec, err := in.getCASecret(ctx)
	if err != nil {
		// Pre-CA-creation race: the Secret doesn't exist yet because the
		// Authority loop hasn't run. Requeue so the next observation
		// (caused by the Authority's create event) re-triggers us.
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("CA Secret not found yet; deferring caBundle patch")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get CA Secret: %w", err)
	}
	caBundle := caSec.Data[corev1.TLSCertKey]
	if len(caBundle) == 0 {
		// Shell Secret with no payload yet — wait for Authority to populate.
		logger.V(1).Info("CA Secret payload empty; deferring caBundle patch")
		return ctrl.Result{}, nil
	}

	if err := in.patchMutating(ctx, caBundle); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch mutating webhooks: %w", err)
	}
	if err := in.patchValidating(ctx, caBundle); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch validating webhooks: %w", err)
	}
	return ctrl.Result{}, nil
}

func (in *Injector) getCASecret(ctx context.Context) (*corev1.Secret, error) {
	var sec corev1.Secret
	err := in.Client.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: CASecretName}, &sec)
	return &sec, err
}

func (in *Injector) patchMutating(ctx context.Context, caBundle []byte) error {
	logger := log.FromContext(ctx)
	var list admissionv1.MutatingWebhookConfigurationList
	if err := in.Client.List(ctx, &list, client.MatchingLabels{InjectCALabel: InjectCALabelTrue}); err != nil {
		return err
	}
	for i := range list.Items {
		wh := &list.Items[i]
		var owned []string
		var bundles [][]byte
		for _, w := range wh.Webhooks {
			if !in.ownsClientConfig(w.ClientConfig) {
				continue
			}
			owned = append(owned, w.Name)
			bundles = append(bundles, w.ClientConfig.CABundle)
		}
		if len(owned) == 0 {
			logger.V(1).Info("skipping webhook config served from another namespace",
				"kind", "MutatingWebhookConfiguration", "name", wh.Name)
			continue
		}
		if !needsPatch(bundles, caBundle) {
			continue
		}
		ac := admissionv1ac.MutatingWebhookConfiguration(wh.Name)
		for _, name := range owned {
			ac.WithWebhooks(admissionv1ac.MutatingWebhook().
				WithName(name).
				WithClientConfig(admissionv1ac.WebhookClientConfig().
					WithCABundle(caBundle...)))
		}
		if err := ssa.ApplyAC(ctx, in.Client, ac,
			client.FieldOwner(CABundleFieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("SSA mutating %s: %w", wh.Name, err)
		}
		logger.Info("patched webhook caBundle",
			"kind", "MutatingWebhookConfiguration", "name", wh.Name, "webhooks", len(owned))
	}
	return nil
}

func (in *Injector) patchValidating(ctx context.Context, caBundle []byte) error {
	logger := log.FromContext(ctx)
	var list admissionv1.ValidatingWebhookConfigurationList
	if err := in.Client.List(ctx, &list, client.MatchingLabels{InjectCALabel: InjectCALabelTrue}); err != nil {
		return err
	}
	for i := range list.Items {
		wh := &list.Items[i]
		var owned []string
		var bundles [][]byte
		for _, w := range wh.Webhooks {
			if !in.ownsClientConfig(w.ClientConfig) {
				continue
			}
			owned = append(owned, w.Name)
			bundles = append(bundles, w.ClientConfig.CABundle)
		}
		if len(owned) == 0 {
			logger.V(1).Info("skipping webhook config served from another namespace",
				"kind", "ValidatingWebhookConfiguration", "name", wh.Name)
			continue
		}
		if !needsPatch(bundles, caBundle) {
			continue
		}
		ac := admissionv1ac.ValidatingWebhookConfiguration(wh.Name)
		for _, name := range owned {
			ac.WithWebhooks(admissionv1ac.ValidatingWebhook().
				WithName(name).
				WithClientConfig(admissionv1ac.WebhookClientConfig().
					WithCABundle(caBundle...)))
		}
		if err := ssa.ApplyAC(ctx, in.Client, ac,
			client.FieldOwner(CABundleFieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("SSA validating %s: %w", wh.Name, err)
		}
		logger.Info("patched webhook caBundle",
			"kind", "ValidatingWebhookConfiguration", "name", wh.Name, "webhooks", len(owned))
	}
	return nil
}

// ownsClientConfig reports whether a webhook entry's endpoint is served by
// this operator instance: a Service reference in the injector's namespace.
// URL-based entries and Services in other namespaces are someone else's
// trust domain — their caBundle is never touched.
func (in *Injector) ownsClientConfig(cc admissionv1.WebhookClientConfig) bool {
	return cc.Service != nil && cc.Service.Namespace == in.Namespace
}

// needsPatch returns true if any of the per-webhook caBundle slices
// disagrees with the current trust bundle. Skipping the SSA call when
// every bundle is already correct keeps audit logs quiet and avoids
// triggering the apiserver's webhook-rotation watcher on no-op writes.
func needsPatch(current [][]byte, want []byte) bool {
	if len(current) == 0 {
		return true
	}
	for _, b := range current {
		if !bytes.Equal(b, want) {
			return true
		}
	}
	return false
}

// SetupWithManager wires the injector controller into the manager. Watches
// the CA Secret (any change refans to all webhook configs) and both
// admission-config kinds (so a chart upgrade or `kubectl edit` that drops
// the caBundle gets re-injected on the next reconcile).
//
// All sources collapse onto a single sentinel request because Reconcile is
// idempotent over the whole label-matched set — there's no benefit to
// per-object enqueueing, and a flat fan-in keeps the controller's queue
// trivially bounded.
func (in *Injector) SetupWithManager(mgr ctrl.Manager) error {
	caSecretPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		sec, ok := obj.(*corev1.Secret)
		if !ok {
			return false
		}
		return sec.Namespace == in.Namespace && sec.Name == CASecretName
	})

	injectLabel := labels.SelectorFromSet(map[string]string{
		InjectCALabel: InjectCALabelTrue,
	})
	labelPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return injectLabel.Matches(labels.Set(obj.GetLabels()))
	})

	sentinel := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, _ client.Object) []reconcile.Request {
		// Single sentinel request — Reconcile re-lists every labelled
		// config + the CA Secret on each pass, so per-object detail is
		// lost on purpose.
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "ca-bundle"}}}
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("ca-bundle-injector").
		For(&corev1.Secret{}, builder.WithPredicates(caSecretPredicate)).
		Watches(&admissionv1.MutatingWebhookConfiguration{}, sentinel,
			builder.WithPredicates(labelPredicate)).
		Watches(&admissionv1.ValidatingWebhookConfiguration{}, sentinel,
			builder.WithPredicates(labelPredicate)).
		Complete(in)
}
