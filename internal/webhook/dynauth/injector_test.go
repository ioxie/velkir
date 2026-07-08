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
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newInjector(t *testing.T, objs ...client.Object) (*Injector, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := admissionv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add admissionv1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Injector{Client: c, Namespace: "velkir"}, c
}

func caSecret(payload []byte) *corev1.Secret {
	return caSecretInNamespace("velkir", payload)
}

func caSecretInNamespace(namespace string, payload []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      CASecretName,
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				CertRoleLabel:  CertRoleCA,
			},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: payload},
	}
}

func serviceRef(namespace string) *admissionv1.ServiceReference {
	return &admissionv1.ServiceReference{Namespace: namespace, Name: "velkir-webhook"}
}

func mutatingConfig(name, serviceNS string, currentBundle []byte, labelled bool) *admissionv1.MutatingWebhookConfiguration {
	wh := &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionv1.MutatingWebhook{{
			Name: "x.example.com",
			ClientConfig: admissionv1.WebhookClientConfig{
				Service:  serviceRef(serviceNS),
				CABundle: currentBundle,
			},
		}},
	}
	if labelled {
		wh.Labels = map[string]string{InjectCALabel: InjectCALabelTrue}
	}
	return wh
}

// validatingConfig builds a labelled ValidatingWebhookConfiguration with
// a single Service-based webhook in serviceNS. (Every caller needs the
// inject-ca label; the unlabelled-skip path is covered on the mutating
// side by TestInjector_Reconcile_SkipsUnlabelledConfigs.)
func validatingConfig(name, serviceNS string, currentBundle []byte) *admissionv1.ValidatingWebhookConfiguration {
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{InjectCALabel: InjectCALabelTrue},
		},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "y.example.com",
			ClientConfig: admissionv1.WebhookClientConfig{
				Service:  serviceRef(serviceNS),
				CABundle: currentBundle,
			},
		}},
	}
}

func TestInjector_Reconcile_PatchesLabelledConfigs(t *testing.T) {
	wantBundle := []byte("CA-PEM-BYTES")
	in, c := newInjector(t,
		caSecret(wantBundle),
		mutatingConfig("velkir-defaulter", "velkir", []byte("stale"), true),
		validatingConfig("velkir-validator", "velkir", nil),
	)

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "velkir-defaulter"}, &got)
	if !bytes.Equal(got.Webhooks[0].ClientConfig.CABundle, wantBundle) {
		t.Errorf("mutating CABundle = %q, want %q", got.Webhooks[0].ClientConfig.CABundle, wantBundle)
	}

	var gotV admissionv1.ValidatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "velkir-validator"}, &gotV)
	if !bytes.Equal(gotV.Webhooks[0].ClientConfig.CABundle, wantBundle) {
		t.Errorf("validating CABundle = %q, want %q", gotV.Webhooks[0].ClientConfig.CABundle, wantBundle)
	}
}

func TestInjector_Reconcile_SkipsUnlabelledConfigs(t *testing.T) {
	wantBundle := []byte("CA-PEM-BYTES")
	in, c := newInjector(t,
		caSecret(wantBundle),
		mutatingConfig("third-party-webhook", "third-party-ns", []byte("foreign"), false),
	)

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "third-party-webhook"}, &got)
	if !bytes.Equal(got.Webhooks[0].ClientConfig.CABundle, []byte("foreign")) {
		t.Errorf("third-party webhook caBundle was modified; injector overreached")
	}
}

func TestInjector_Reconcile_DefersWhenCASecretMissing(t *testing.T) {
	in, _ := newInjector(t,
		mutatingConfig("velkir-defaulter", "velkir", nil, true),
	)
	// No CA Secret in store → reconcile should soft-defer (no error).
	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Errorf("Reconcile errored on missing CA Secret instead of deferring: %v", err)
	}
}

func TestInjector_Reconcile_NoOpWhenAlreadyCorrect(t *testing.T) {
	wantBundle := []byte("CA-PEM-BYTES")
	in, c := newInjector(t,
		caSecret(wantBundle),
		mutatingConfig("velkir-defaulter", "velkir", wantBundle, true),
	)

	var before admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "velkir-defaulter"}, &before)
	rvBefore := before.ResourceVersion

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var after admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "velkir-defaulter"}, &after)
	if after.ResourceVersion != rvBefore {
		t.Errorf("ResourceVersion changed (%s → %s) — injector wrote a no-op", rvBefore, after.ResourceVersion)
	}
}

func TestInjector_Reconcile_SkipsForeignServiceNamespace(t *testing.T) {
	ownBundle := []byte("OWN-CA")
	foreignBundle := []byte("FOREIGN-CA")
	in, c := newInjector(t,
		caSecret(ownBundle),
		mutatingConfig("own-defaulter", "velkir", []byte("stale"), true),
		mutatingConfig("foreign-defaulter", "other-operator-ns", foreignBundle, true),
		validatingConfig("foreign-validator", "other-operator-ns", foreignBundle),
	)

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var own admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "own-defaulter"}, &own)
	if !bytes.Equal(own.Webhooks[0].ClientConfig.CABundle, ownBundle) {
		t.Errorf("own config not patched: got %q", own.Webhooks[0].ClientConfig.CABundle)
	}

	var foreignM admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "foreign-defaulter"}, &foreignM)
	if !bytes.Equal(foreignM.Webhooks[0].ClientConfig.CABundle, foreignBundle) {
		t.Errorf("foreign mutating config caBundle was modified despite inject-ca label: got %q",
			foreignM.Webhooks[0].ClientConfig.CABundle)
	}

	var foreignV admissionv1.ValidatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "foreign-validator"}, &foreignV)
	if !bytes.Equal(foreignV.Webhooks[0].ClientConfig.CABundle, foreignBundle) {
		t.Errorf("foreign validating config caBundle was modified despite inject-ca label: got %q",
			foreignV.Webhooks[0].ClientConfig.CABundle)
	}
}

// TestInjector_Reconcile_MixedConfig_PatchesOnlyOwnedEntry pins the
// per-entry filtering: a single labelled config carrying one own-ns
// service entry and one foreign-ns entry must get its OWNED entry's
// caBundle stamped while the foreign entry is left untouched (the SSA
// apply lists only the owned entry; list-merge-by-name leaves the
// other alone).
func TestInjector_Reconcile_MixedConfig_PatchesOnlyOwnedEntry(t *testing.T) {
	wantBundle := []byte("OWN-CA")
	foreignBundle := []byte("FOREIGN-CA")
	mixed := &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "mixed-defaulter",
			Labels: map[string]string{InjectCALabel: InjectCALabelTrue},
		},
		Webhooks: []admissionv1.MutatingWebhook{
			{
				Name: "owned.example.com",
				ClientConfig: admissionv1.WebhookClientConfig{
					Service:  serviceRef("velkir"),
					CABundle: []byte("stale"),
				},
			},
			{
				Name: "foreign.example.com",
				ClientConfig: admissionv1.WebhookClientConfig{
					Service:  serviceRef("other-operator-ns"),
					CABundle: foreignBundle,
				},
			},
		},
	}
	in, c := newInjector(t, caSecret(wantBundle), mixed)

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "mixed-defaulter"}, &got)
	byName := map[string][]byte{}
	for _, w := range got.Webhooks {
		byName[w.Name] = w.ClientConfig.CABundle
	}
	if !bytes.Equal(byName["owned.example.com"], wantBundle) {
		t.Errorf("owned entry caBundle = %q, want %q", byName["owned.example.com"], wantBundle)
	}
	if !bytes.Equal(byName["foreign.example.com"], foreignBundle) {
		t.Errorf("foreign entry caBundle = %q, want it untouched %q", byName["foreign.example.com"], foreignBundle)
	}
}

func TestInjector_Reconcile_SkipsURLClientConfig(t *testing.T) {
	ownBundle := []byte("OWN-CA")
	url := "https://example.com/validate"
	wh := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "url-validator",
			Labels: map[string]string{InjectCALabel: InjectCALabelTrue},
		},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "y.example.com",
			ClientConfig: admissionv1.WebhookClientConfig{
				URL:      &url,
				CABundle: []byte("external-trust"),
			},
		}},
	}
	in, c := newInjector(t, caSecret(ownBundle), wh)

	if _, err := in.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got admissionv1.ValidatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "url-validator"}, &got)
	if !bytes.Equal(got.Webhooks[0].ClientConfig.CABundle, []byte("external-trust")) {
		t.Errorf("URL-based webhook caBundle was modified: got %q", got.Webhooks[0].ClientConfig.CABundle)
	}
}

// Two operator installs on one cluster must converge each config to its own
// release's CA and reach quiescence — the rc.20 external-run failure mode was
// both injectors force-stamping the other's configs in a watch-triggered war.
func TestInjector_TwoInjectors_NoWar(t *testing.T) {
	bundleA := []byte("CA-OF-A")
	bundleB := []byte("CA-OF-B")

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := admissionv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add admissionv1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		caSecretInNamespace("ns-a", bundleA),
		caSecretInNamespace("ns-b", bundleB),
		validatingConfig("release-a-validator", "ns-a", nil),
		validatingConfig("release-b-validator", "ns-b", nil),
	).Build()

	injA := &Injector{Client: c, Namespace: "ns-a"}
	injB := &Injector{Client: c, Namespace: "ns-b"}

	// Interleave passes the way dueling watches would fire them.
	for range 3 {
		if _, err := injA.Reconcile(context.Background(), ctrl.Request{}); err != nil {
			t.Fatalf("injector A: %v", err)
		}
		if _, err := injB.Reconcile(context.Background(), ctrl.Request{}); err != nil {
			t.Fatalf("injector B: %v", err)
		}
	}

	var cfgA, cfgB admissionv1.ValidatingWebhookConfiguration
	_ = c.Get(context.Background(), types.NamespacedName{Name: "release-a-validator"}, &cfgA)
	_ = c.Get(context.Background(), types.NamespacedName{Name: "release-b-validator"}, &cfgB)
	if !bytes.Equal(cfgA.Webhooks[0].ClientConfig.CABundle, bundleA) {
		t.Errorf("release A validator holds %q, want its own CA", cfgA.Webhooks[0].ClientConfig.CABundle)
	}
	if !bytes.Equal(cfgB.Webhooks[0].ClientConfig.CABundle, bundleB) {
		t.Errorf("release B validator holds %q, want its own CA", cfgB.Webhooks[0].ClientConfig.CABundle)
	}

	// Steady state: another pass of each must not write.
	rvA, rvB := cfgA.ResourceVersion, cfgB.ResourceVersion
	if _, err := injA.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("injector A steady-state: %v", err)
	}
	if _, err := injB.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("injector B steady-state: %v", err)
	}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "release-a-validator"}, &cfgA)
	_ = c.Get(context.Background(), types.NamespacedName{Name: "release-b-validator"}, &cfgB)
	if cfgA.ResourceVersion != rvA || cfgB.ResourceVersion != rvB {
		t.Errorf("steady state not reached: A %s→%s, B %s→%s",
			rvA, cfgA.ResourceVersion, rvB, cfgB.ResourceVersion)
	}
}

func TestNeedsPatch(t *testing.T) {
	want := []byte("CA")

	tests := []struct {
		name    string
		current [][]byte
		need    bool
	}{
		{"all match", [][]byte{want, want}, false},
		{"one stale", [][]byte{want, []byte("STALE")}, true},
		{"all empty", [][]byte{nil, nil}, true},
		{"no webhooks", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsPatch(tc.current, want); got != tc.need {
				t.Errorf("needsPatch=%v, want %v", got, tc.need)
			}
		})
	}
}
