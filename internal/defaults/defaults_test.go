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

package defaults_test

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/defaults"
	webhookv1beta1 "github.com/ioxie/velkir/internal/webhook/v1beta1"
)

// minimalCRs returns one minimally-specified CR per mode — only the
// fields CEL validation forces a user to set, mirroring what lands in
// etcd when the defaulting webhook is unreachable at admission time.
func minimalCRs() map[string]*valkeyv1beta1.Valkey {
	return map[string]*valkeyv1beta1.Valkey{
		"standalone": {
			ObjectMeta: metav1.ObjectMeta{Name: "min-standalone", Namespace: "default"},
			Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeStandalone},
		},
		"replication": {
			ObjectMeta: metav1.ObjectMeta{Name: "min-replication", Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeReplication,
				Valkey: valkeyv1beta1.ValkeyPodSpec{
					Persistence: &valkeyv1beta1.PersistenceSpec{},
				},
			},
		},
		"sentinel": {
			ObjectMeta: metav1.ObjectMeta{Name: "min-sentinel", Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeSentinel,
				Sentinel: &valkeyv1beta1.SentinelPodSpec{
					Replicas:   3,
					Quorum:     2,
					MasterName: "mymaster",
				},
			},
		},
	}
}

func TestApplySpecDefaults_Idempotent(t *testing.T) {
	for mode, cr := range minimalCRs() {
		t.Run(mode, func(t *testing.T) {
			once := cr.DeepCopy()
			defaults.ApplySpecDefaults(once)
			twice := once.DeepCopy()
			defaults.ApplySpecDefaults(twice)
			if !reflect.DeepEqual(once.Spec, twice.Spec) {
				t.Errorf("ApplySpecDefaults is not idempotent for mode %s", mode)
			}
		})
	}
}

// TestApplySpecDefaults_MatchesWebhookDefaulterSpec is the
// single-source-of-truth pin: the reconciler's in-memory
// normalization and the admission defaulter must produce the SAME spec
// from the same input, so rendered output can never depend on whether
// the webhook ran. A spec-shaping stamp added only to the webhook (or
// only to ApplySpecDefaults) fails this test.
func TestApplySpecDefaults_MatchesWebhookDefaulterSpec(t *testing.T) {
	for mode, cr := range minimalCRs() {
		t.Run(mode, func(t *testing.T) {
			normalized := cr.DeepCopy()
			defaults.ApplySpecDefaults(normalized)

			admitted := cr.DeepCopy()
			d := &webhookv1beta1.ValkeyCustomDefaulter{}
			if err := d.Default(context.Background(), admitted); err != nil {
				t.Fatalf("webhook Default: %v", err)
			}

			if !reflect.DeepEqual(normalized.Spec, admitted.Spec) {
				t.Errorf("normalized spec diverges from webhook-defaulted spec for mode %s:\nnormalized: %+v\nadmitted:   %+v",
					mode, normalized.Spec, admitted.Spec)
			}
		})
	}
}
