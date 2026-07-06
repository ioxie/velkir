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

// This file holds the pure StatefulSet / pod-template builders extracted
// from the reconcile loop: buildValkeySTS and the apply-configuration
// converters it composes. These functions carry no reconcile state,
// client, or clock — given a Valkey CR (and rendered-config hash) they
// return the desired server-side-apply configuration.

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/utils/ptr"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func buildValkeySTS(v *valkeyv1beta1.Valkey, cmHash string) *appsv1ac.StatefulSetApplyConfiguration {
	labels := ownedLabels(v, componentValkey)
	podLabels := mergeMaps(labels, v.Spec.Valkey.PodLabels)
	baseAnnotations := map[string]string{ConfigHashAnnotation: cmHash}
	// Project the CR-level manual-rollout annotation onto the pod
	// template so a value change bumps the STS UpdateRevision, which
	// trips the standard rolloutTriggerEdge path. Empty value (the
	// annotation absent or empty) leaves the annotation off the pod
	// template entirely — every bump from empty → non-empty (or
	// across distinct non-empty values) triggers a new rollout.
	if gen := v.Annotations[ManualRolloutAnnotation]; gen != "" {
		baseAnnotations[ManualRolloutAnnotation] = gen
	}
	annotations := mergeMaps(baseAnnotations, v.Spec.Valkey.PodAnnotations)

	containers := []*corev1ac.ContainerApplyConfiguration{buildValkeyContainer(v)}
	if v.Spec.Metrics.Enabled != nil && *v.Spec.Metrics.Enabled {
		containers = append(containers, buildExporterContainer(v))
	}

	podSpec := corev1ac.PodSpec().
		WithServiceAccountName(valkeyServiceAccountName(v)).
		WithAutomountServiceAccountToken(false).
		WithTerminationGracePeriodSeconds(terminationGracePeriodVal).
		WithSecurityContext(restrictedPodSecurityContext(v.Spec.Valkey.SecurityContext)).
		WithInitContainers(buildRenderConfigInitContainer(v)).
		WithContainers(containers...).
		WithVolumes(buildValkeyVolumes(v)...)
	if gates := buildReadinessGates(v); len(gates) > 0 {
		podSpec.WithReadinessGates(gates...)
	}
	if aff := acAffinity(v.Spec.Valkey.Affinity); aff != nil {
		podSpec.WithAffinity(aff)
	}
	if tols := acTolerations(v.Spec.Valkey.Tolerations); len(tols) > 0 {
		podSpec.WithTolerations(tols...)
	}
	if len(v.Spec.Valkey.NodeSelector) > 0 {
		podSpec.WithNodeSelector(v.Spec.Valkey.NodeSelector)
	}
	if tsc := acTopologySpreadConstraints(v.Spec.Valkey.TopologySpreadConstraints); len(tsc) > 0 {
		podSpec.WithTopologySpreadConstraints(tsc...)
	}
	if v.Spec.Valkey.PriorityClassName != "" {
		podSpec.WithPriorityClassName(v.Spec.Valkey.PriorityClassName)
	}
	if dns := acDNSConfig(v.Spec.Valkey.DNSConfig); dns != nil {
		podSpec.WithDNSConfig(dns)
	}

	template := corev1ac.PodTemplateSpec().
		WithLabels(podLabels).
		WithAnnotations(annotations).
		WithSpec(podSpec)

	stsSpec := appsv1ac.StatefulSetSpec().
		WithServiceName(v.Name + "-headless").
		WithReplicas(v.Spec.Valkey.Replicas).
		WithUpdateStrategy(appsv1ac.StatefulSetUpdateStrategy().
			WithType(appsv1.OnDeleteStatefulSetStrategyType)).
		WithSelector(metav1ac.LabelSelector().WithMatchLabels(labels)).
		WithTemplate(template)
	if v.Spec.Valkey.Persistence != nil {
		stsSpec.WithVolumeClaimTemplates(buildValkeyDataPVC(v, v.Spec.Valkey.Persistence))
	}

	return appsv1ac.StatefulSet(v.Name, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(stsSpec)
}

// acAffinity, acTolerations, acTopologySpreadConstraints, acDNSConfig
// convert user-supplied legacy types from `spec.valkey.*` into their
// apply-config equivalents via a JSON roundtrip. The two shapes share
// JSON tags, so the marshal/unmarshal is lossless. Used only for fields
// the operator does not synthesise itself — for those, a builder chain
// is preferred.
// acAffinity builds the apply-config. The three sub-aggregates carry
// `omitempty`; each is converted only when the source pointer is non-nil.
func acAffinity(src *corev1.Affinity) *corev1ac.AffinityApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.Affinity()
	if src.NodeAffinity != nil {
		out.WithNodeAffinity(acNodeAffinity(src.NodeAffinity))
	}
	if src.PodAffinity != nil {
		out.WithPodAffinity(acPodAffinity(src.PodAffinity))
	}
	if src.PodAntiAffinity != nil {
		out.WithPodAntiAffinity(acPodAntiAffinity(src.PodAntiAffinity))
	}
	return out
}

func acNodeAffinity(src *corev1.NodeAffinity) *corev1ac.NodeAffinityApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.NodeAffinity()
	if src.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		out.WithRequiredDuringSchedulingIgnoredDuringExecution(
			acNodeSelector(src.RequiredDuringSchedulingIgnoredDuringExecution))
	}
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		out.WithPreferredDuringSchedulingIgnoredDuringExecution(
			acPreferredSchedulingTerm(&src.PreferredDuringSchedulingIgnoredDuringExecution[i]))
	}
	return out
}

func acPodAffinity(src *corev1.PodAffinity) *corev1ac.PodAffinityApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.PodAffinity()
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		out.WithRequiredDuringSchedulingIgnoredDuringExecution(
			acPodAffinityTerm(&src.RequiredDuringSchedulingIgnoredDuringExecution[i]))
	}
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		out.WithPreferredDuringSchedulingIgnoredDuringExecution(
			acWeightedPodAffinityTerm(&src.PreferredDuringSchedulingIgnoredDuringExecution[i]))
	}
	return out
}

func acPodAntiAffinity(src *corev1.PodAntiAffinity) *corev1ac.PodAntiAffinityApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.PodAntiAffinity()
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		out.WithRequiredDuringSchedulingIgnoredDuringExecution(
			acPodAffinityTerm(&src.RequiredDuringSchedulingIgnoredDuringExecution[i]))
	}
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		out.WithPreferredDuringSchedulingIgnoredDuringExecution(
			acWeightedPodAffinityTerm(&src.PreferredDuringSchedulingIgnoredDuringExecution[i]))
	}
	return out
}

func acNodeSelector(src *corev1.NodeSelector) *corev1ac.NodeSelectorApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.NodeSelector()
	for i := range src.NodeSelectorTerms {
		out.WithNodeSelectorTerms(acNodeSelectorTerm(&src.NodeSelectorTerms[i]))
	}
	return out
}

func acNodeSelectorTerm(src *corev1.NodeSelectorTerm) *corev1ac.NodeSelectorTermApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.NodeSelectorTerm()
	for i := range src.MatchExpressions {
		out.WithMatchExpressions(acNodeSelectorRequirement(&src.MatchExpressions[i]))
	}
	for i := range src.MatchFields {
		out.WithMatchFields(acNodeSelectorRequirement(&src.MatchFields[i]))
	}
	return out
}

// acNodeSelectorRequirement: Key + Operator required (always stamped),
// Values is `omitempty`.
func acNodeSelectorRequirement(src *corev1.NodeSelectorRequirement) *corev1ac.NodeSelectorRequirementApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.NodeSelectorRequirement().
		WithKey(src.Key).
		WithOperator(src.Operator)
	if len(src.Values) > 0 {
		out.WithValues(src.Values...)
	}
	return out
}

// acPreferredSchedulingTerm: Weight + Preference both required.
func acPreferredSchedulingTerm(src *corev1.PreferredSchedulingTerm) *corev1ac.PreferredSchedulingTermApplyConfiguration {
	if src == nil {
		return nil
	}
	return corev1ac.PreferredSchedulingTerm().
		WithWeight(src.Weight).
		WithPreference(acNodeSelectorTerm(&src.Preference))
}

// acPodAffinityTerm: TopologyKey required (always stamped); rest are
// `omitempty`.
func acPodAffinityTerm(src *corev1.PodAffinityTerm) *corev1ac.PodAffinityTermApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.PodAffinityTerm().WithTopologyKey(src.TopologyKey)
	if src.LabelSelector != nil {
		out.WithLabelSelector(acLabelSelector(src.LabelSelector))
	}
	if len(src.Namespaces) > 0 {
		out.WithNamespaces(src.Namespaces...)
	}
	if src.NamespaceSelector != nil {
		out.WithNamespaceSelector(acLabelSelector(src.NamespaceSelector))
	}
	if len(src.MatchLabelKeys) > 0 {
		out.WithMatchLabelKeys(src.MatchLabelKeys...)
	}
	if len(src.MismatchLabelKeys) > 0 {
		out.WithMismatchLabelKeys(src.MismatchLabelKeys...)
	}
	return out
}

// acWeightedPodAffinityTerm: Weight + PodAffinityTerm both required.
func acWeightedPodAffinityTerm(src *corev1.WeightedPodAffinityTerm) *corev1ac.WeightedPodAffinityTermApplyConfiguration {
	if src == nil {
		return nil
	}
	return corev1ac.WeightedPodAffinityTerm().
		WithWeight(src.Weight).
		WithPodAffinityTerm(acPodAffinityTerm(&src.PodAffinityTerm))
}

// acTolerations converts user-supplied Tolerations into AC builders
// directly. Toleration's surface is small (5 fields, all leaf
// scalars or pointers) and stable, so the explicit field-copy is
// cheaper than the JSON roundtrip without giving up correctness:
// the AC type's `omitempty` semantics match the JSON tags on the
// legacy type, so skipping `WithX` calls on zero-value source
// fields produces an AC equivalent to what the round-trip emits.
func acTolerations(src []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.TolerationApplyConfiguration, len(src))
	for i := range src {
		ac := corev1ac.Toleration()
		if src[i].Key != "" {
			ac.WithKey(src[i].Key)
		}
		if src[i].Operator != "" {
			ac.WithOperator(src[i].Operator)
		}
		if src[i].Value != "" {
			ac.WithValue(src[i].Value)
		}
		if src[i].Effect != "" {
			ac.WithEffect(src[i].Effect)
		}
		if src[i].TolerationSeconds != nil {
			ac.WithTolerationSeconds(*src[i].TolerationSeconds)
		}
		out[i] = ac
	}
	return out
}

// acTopologySpreadConstraints builds the apply-config slice. MaxSkew,
// TopologyKey, WhenUnsatisfiable have no `omitempty` on the source and
// are always stamped (zero values appear in the JSON-roundtrip too);
// the remaining fields follow the source's `omitempty` tags.
func acTopologySpreadConstraints(src []corev1.TopologySpreadConstraint) []*corev1ac.TopologySpreadConstraintApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.TopologySpreadConstraintApplyConfiguration, len(src))
	for i := range src {
		c := &src[i]
		ac := corev1ac.TopologySpreadConstraint().
			WithMaxSkew(c.MaxSkew).
			WithTopologyKey(c.TopologyKey).
			WithWhenUnsatisfiable(c.WhenUnsatisfiable)
		if c.LabelSelector != nil {
			ac.WithLabelSelector(acLabelSelector(c.LabelSelector))
		}
		if c.MinDomains != nil {
			ac.WithMinDomains(*c.MinDomains)
		}
		if c.NodeAffinityPolicy != nil {
			ac.WithNodeAffinityPolicy(*c.NodeAffinityPolicy)
		}
		if c.NodeTaintsPolicy != nil {
			ac.WithNodeTaintsPolicy(*c.NodeTaintsPolicy)
		}
		if len(c.MatchLabelKeys) > 0 {
			ac.WithMatchLabelKeys(c.MatchLabelKeys...)
		}
		out[i] = ac
	}
	return out
}

// acLabelSelector builds a LabelSelector apply-config (nil-safe).
// MatchLabels + MatchExpressions both carry `omitempty`.
func acLabelSelector(src *metav1.LabelSelector) *metav1ac.LabelSelectorApplyConfiguration {
	if src == nil {
		return nil
	}
	out := metav1ac.LabelSelector()
	if len(src.MatchLabels) > 0 {
		out.WithMatchLabels(src.MatchLabels)
	}
	for i := range src.MatchExpressions {
		req := &src.MatchExpressions[i]
		r := metav1ac.LabelSelectorRequirement().
			WithKey(req.Key).
			WithOperator(req.Operator)
		if len(req.Values) > 0 {
			r.WithValues(req.Values...)
		}
		out.WithMatchExpressions(r)
	}
	return out
}

// acDNSConfig builds a PodDNSConfig apply-config. All three fields are
// `omitempty` on the source; each is appended only when non-empty so
// the AC's JSON output stays equivalent to the prior JSON-roundtrip.
func acDNSConfig(src *corev1.PodDNSConfig) *corev1ac.PodDNSConfigApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.PodDNSConfig()
	if len(src.Nameservers) > 0 {
		out.WithNameservers(src.Nameservers...)
	}
	if len(src.Searches) > 0 {
		out.WithSearches(src.Searches...)
	}
	for i := range src.Options {
		opt := corev1ac.PodDNSConfigOption()
		if src.Options[i].Name != "" {
			opt.WithName(src.Options[i].Name)
		}
		if src.Options[i].Value != nil {
			opt.WithValue(*src.Options[i].Value)
		}
		out.WithOptions(opt)
	}
	return out
}

// buildValkeyVolumes returns the pod's volume set in deterministic order:
//   - config-template (RO ConfigMap with the rendered valkey.conf
//     template) — input to the init container.
//   - init-scripts (RO ConfigMap with render-valkey-conf.sh) — input
//     to the init container.
//   - config (emptyDir shared between init and main) — the init
//     container writes the substituted valkey.conf here, the main
//     container reads it.
//   - bootstrap (RO sentinel-bootstrap ConfigMap, optional) — absent
//     in standalone / replication, which the init script handles via
//     fall-through to Service DNS.
//   - data (emptyDir when Persistence is unset, otherwise projected from
//     the StatefulSet volumeClaimTemplate of the same name).
func buildValkeyVolumes(v *valkeyv1beta1.Valkey) []*corev1ac.VolumeApplyConfiguration {
	base := []*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().
			WithName("config-template").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixValkeyConf).
				WithDefaultMode(0o444)),
		corev1ac.Volume().
			WithName("init-scripts").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixInitScripts).
				WithDefaultMode(0o555)),
		corev1ac.Volume().
			WithName("config").
			WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
		// Writable scratch for readOnlyRootFilesystem containers: the
		// valkey main + exporter mount this at /tmp. Non-persistent
		// standalone also writes its data here (`dir /tmp`).
		corev1ac.Volume().
			WithName("tmp").
			WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
		corev1ac.Volume().
			WithName("bootstrap").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixSentinelBootstrap).
				WithDefaultMode(0o444).
				WithOptional(true)),
	}
	if v.Spec.Valkey.Persistence != nil {
		// STS controller injects the "data" volume from the
		// volumeClaimTemplate keyed on the matching name; no
		// manual entry needed here.
		return base
	}
	return append(base, corev1ac.Volume().
		WithName("data").
		WithEmptyDir(corev1ac.EmptyDirVolumeSource()))
}

// buildValkeyDataPVC constructs the volumeClaimTemplate that backs the
// "data" volume when the CR opts into persistent storage. Each pod
// ordinal receives its own PVC named "data-<sts>-<ord>"; pod-ordinal
// preservation across STS recreates is what lets the PVC resize sub-
// state-machine orphan-delete the STS and reattach without data loss.
//
// Labels mirror ownedLabels so the operator's existing CR-keyed
// selectors (reconcileDeletion for pvcRetentionPolicy, reconcilePVC
// Resize for the sub-state-machine) match the PVCs the STS controller
// creates from this template. Takes the PersistenceSpec sub-struct
// directly (rather than the full CR) to match sibling builders such
// as acAffinity / acTolerations and keep the nil-guard at the single
// call site in buildValkeySTS.
func buildValkeyDataPVC(v *valkeyv1beta1.Valkey, p *valkeyv1beta1.PersistenceSpec) *corev1ac.PersistentVolumeClaimApplyConfiguration {
	spec := corev1ac.PersistentVolumeClaimSpec().
		WithResources(corev1ac.VolumeResourceRequirements().
			WithRequests(corev1.ResourceList{corev1.ResourceStorage: p.Size}))
	if len(p.AccessModes) > 0 {
		spec.WithAccessModes(p.AccessModes...)
	}
	if p.StorageClass != nil && *p.StorageClass != "" {
		spec.WithStorageClassName(*p.StorageClass)
	}
	return corev1ac.PersistentVolumeClaim("data", v.Namespace).
		WithLabels(ownedLabels(v, componentValkey)).
		WithSpec(spec)
}

// buildRenderConfigInitContainer assembles the render-config init
// container. The container reuses the valkey image so we don't pull a
// second registry path; the renderer / replicaof script is POSIX sh
// (alpine busybox included) and, on pod-0, additionally shells out to
// `valkey-cli` + `timeout` for the seed-liveness probe — both present
// in the valkey image, so a swapped init image must still provide them.
func buildRenderConfigInitContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	return corev1ac.Container().
		WithName("render-config").
		WithImage(v.Spec.Image.Valkey.Repository+":"+v.Spec.Image.Valkey.Tag).
		WithCommand("/bin/sh", renderScriptPath).
		WithEnv(
			downwardEnvVar("POD_IP", "status.podIP"),
			downwardEnvVar("POD_NAME", "metadata.name"),
			downwardEnvVar("POD_NAMESPACE", "metadata.namespace"),
			corev1ac.EnvVar().WithName("APP_NAME").WithValue(v.Name),
			// MASTER_SERVICE is the FQDN of the writer Service. The
			// init script does a one-time getent lookup; if the
			// Service has no endpoints (early bootstrap) the lookup
			// fails harmlessly and the pod starts as primary-or-
			// orphan, which is what we want — the operator labels
			// pod-0 role=primary on the next reconcile and the next
			// pod restart picks up the resolved IP.
			corev1ac.EnvVar().
				WithName("MASTER_SERVICE").
				WithValue(fmt.Sprintf("%s.%s.svc.cluster.local", v.Name, v.Namespace)),
		).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName("config-template").WithMountPath(mountConfigTemplate).WithReadOnly(true),
			corev1ac.VolumeMount().WithName("init-scripts").WithMountPath(mountInitScripts).WithReadOnly(true),
			corev1ac.VolumeMount().WithName("config").WithMountPath(mountConfig),
			corev1ac.VolumeMount().WithName("bootstrap").WithMountPath(mountSentinelBootstrap).WithReadOnly(true),
		).
		WithResources(corev1ac.ResourceRequirements().
			WithRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			}).
			WithLimits(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			})).
		WithSecurityContext(restrictedContainerSecurityContext())
}

// downwardEnvVar builds an EnvVar apply-config that sources its value
// from the Downward API at `fieldPath` (e.g. "status.podIP",
// "metadata.name"). Centralised so the four common downward refs in
// the valkey + render-config containers share one shape.
func downwardEnvVar(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
	return corev1ac.EnvVar().
		WithName(name).
		WithValueFrom(corev1ac.EnvVarSource().
			WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath(fieldPath)))
}

func buildValkeyContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	args := []string{"--replica-announce-ip", "$(POD_IP)"}
	env := []*corev1ac.EnvVarApplyConfiguration{downwardEnvVar("POD_IP", "status.podIP")}
	// When auth is enabled, source the password from the user-supplied
	// Secret into a container env var, then pass it as
	// `--requirepass`/`--masterauth` CLI flags. The flags survive pod
	// restart (kubelet re-resolves the Secret on each container start),
	// which is what the manual-rotation flow at v1beta1 relies on; the
	// hot-rotation path layers CONFIG SET on top for the live
	// pod's in-memory state without waiting for a restart.
	if v.Spec.Auth != nil && v.Spec.Auth.SecretName != "" {
		key := v.Spec.Auth.SecretKey
		if key == "" {
			key = defaultAuthSecretKey
		}
		env = append(env, corev1ac.EnvVar().
			WithName("VALKEY_PASSWORD").
			WithValueFrom(corev1ac.EnvVarSource().
				WithSecretKeyRef(corev1ac.SecretKeySelector().
					WithName(v.Spec.Auth.SecretName).
					WithKey(key))))
		args = append(args, "--requirepass", "$(VALKEY_PASSWORD)", "--masterauth", "$(VALKEY_PASSWORD)")
	}
	c := corev1ac.Container().
		WithName("valkey").
		WithImage(v.Spec.Image.Valkey.Repository+":"+v.Spec.Image.Valkey.Tag).
		WithPorts(corev1ac.ContainerPort().
			WithName("valkey").
			WithContainerPort(defaultValkeyPort).
			WithProtocol(corev1.ProtocolTCP)).
		// The init container substitutes the _POD_IP_ placeholder and
		// drops the result into the shared `config` emptyDir. The main
		// container reads from there. The CLI flag `--replica-announce-ip
		// $(POD_IP)` is set both for defence-in-depth (it overrides the
		// rendered value if anything went wrong with substitution) and
		// because valkey-server treats the flag as authoritative — a
		// CONFIG GET reflecting the same value helps debugging.
		WithCommand("valkey-server", mountConfig+"/valkey.conf").
		WithArgs(args...).
		WithEnv(env...).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName("config").WithMountPath(mountConfig).WithReadOnly(true),
			corev1ac.VolumeMount().WithName("data").WithMountPath("/data"),
			corev1ac.VolumeMount().WithName("tmp").WithMountPath("/tmp"),
		).
		WithResources(valkeyContainerResources(v.Spec.Valkey.Resources)).
		WithSecurityContext(restrictedContainerSecurityContextReadOnly())
	authEnabled := v.Spec.Auth != nil && v.Spec.Auth.SecretName != ""
	if v.Spec.Valkey.CustomLivenessProbe != nil {
		// Auth injection applies to liveness too: the defaulter
		// now stamps an exec `valkey-cli ping` liveness probe so a
		// frozen valkey-server (SIGSTOP'd, cgroup-frozen) is detected.
		// Without `-a $(VALKEY_PASSWORD)`, the probe returns
		// `NOAUTH Authentication required` against an auth-enabled
		// cluster, kubelet declares liveness failed, container loops in
		// CrashLoopBackOff. injectAuthIntoValkeyCLIProbe is a no-op for
		// tcpSocket/http/grpc handlers so user-supplied overrides
		// passing those shapes are unaffected.
		c.WithLivenessProbe(acProbe(injectAuthIntoValkeyCLIProbe(v.Spec.Valkey.CustomLivenessProbe, authEnabled)))
	}
	if v.Spec.Valkey.CustomReadinessProbe != nil {
		c.WithReadinessProbe(acProbe(injectAuthIntoValkeyCLIProbe(v.Spec.Valkey.CustomReadinessProbe, authEnabled)))
	}
	addPreStopHookIfSentinelMode(c, v)
	return c
}

// injectAuthIntoValkeyCLIProbe returns a probe whose Exec command has
// `-a $(VALKEY_PASSWORD)` inserted before any subcommand when auth is
// configured AND the command begins with `valkey-cli` (or `redis-cli`).
// HTTP / TCP / gRPC probes pass through unchanged. The defaulter's
// readiness probe is the canonical caller: the operator stamps the
// VALKEY_PASSWORD env on the container (sourced from the auth Secret);
// without `-a` the probe receives `NOAUTH Authentication required` and
// pods never go Ready. The probe input is treated as read-only — the
// caller's `*corev1.Probe` is never mutated.
func injectAuthIntoValkeyCLIProbe(p *corev1.Probe, authEnabled bool) *corev1.Probe {
	if p == nil || !authEnabled || p.Exec == nil || len(p.Exec.Command) == 0 {
		return p
	}
	cmd := p.Exec.Command[0]
	if cmd != "valkey-cli" && cmd != "redis-cli" {
		return p
	}
	for i := 1; i < len(p.Exec.Command); i++ {
		if p.Exec.Command[i] == "-a" {
			return p
		}
	}
	out := p.DeepCopy()
	out.Exec.Command = append([]string{out.Exec.Command[0], "-a", "$(VALKEY_PASSWORD)"}, out.Exec.Command[1:]...)
	return out
}

// valkeyContainerResources stamps the operator's default CPU/memory
// requests when the user-supplied `spec.valkey.resources.requests` is
// unset (nil). An explicitly-empty map preserves the "no requests"
// shape so the kubelet falls back to its own LimitRange / cluster
// defaults verbatim. A partial or full map is honoured as-is. Limits
// fall through unmodified.
func valkeyContainerResources(user corev1.ResourceRequirements) *corev1ac.ResourceRequirementsApplyConfiguration {
	out := corev1ac.ResourceRequirements()
	requests := user.Requests
	if requests == nil {
		requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		}
	}
	if len(requests) > 0 {
		out.WithRequests(requests)
	}
	if len(user.Limits) > 0 {
		out.WithLimits(user.Limits)
	}
	if claims := acResourceClaims(user.Claims); len(claims) > 0 {
		out.WithClaims(claims...)
	}
	return out
}

// acResourceClaims builds ResourceClaim apply-configs. Name has no
// `omitempty` on the source so it is always stamped (even at "");
// Request is `omitempty` and is gated.
func acResourceClaims(src []corev1.ResourceClaim) []*corev1ac.ResourceClaimApplyConfiguration {
	if len(src) == 0 {
		return nil
	}
	out := make([]*corev1ac.ResourceClaimApplyConfiguration, len(src))
	for i := range src {
		ac := corev1ac.ResourceClaim().WithName(src[i].Name)
		if src[i].Request != "" {
			ac.WithRequest(src[i].Request)
		}
		out[i] = ac
	}
	return out
}

// acProbe builds a Probe apply-config. Probe inlines a ProbeHandler
// union (Exec/HTTPGet/TCPSocket/GRPC); each handler and each of the
// six numeric scheduling knobs are `omitempty` on the source and
// stamped only when present/non-zero.
func acProbe(src *corev1.Probe) *corev1ac.ProbeApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.Probe()
	if src.Exec != nil {
		out.WithExec(acExecAction(src.Exec))
	}
	if src.HTTPGet != nil {
		out.WithHTTPGet(acHTTPGetAction(src.HTTPGet))
	}
	if src.TCPSocket != nil {
		out.WithTCPSocket(acTCPSocketAction(src.TCPSocket))
	}
	if src.GRPC != nil {
		out.WithGRPC(acGRPCAction(src.GRPC))
	}
	if src.InitialDelaySeconds != 0 {
		out.WithInitialDelaySeconds(src.InitialDelaySeconds)
	}
	if src.TimeoutSeconds != 0 {
		out.WithTimeoutSeconds(src.TimeoutSeconds)
	}
	if src.PeriodSeconds != 0 {
		out.WithPeriodSeconds(src.PeriodSeconds)
	}
	if src.SuccessThreshold != 0 {
		out.WithSuccessThreshold(src.SuccessThreshold)
	}
	if src.FailureThreshold != 0 {
		out.WithFailureThreshold(src.FailureThreshold)
	}
	if src.TerminationGracePeriodSeconds != nil {
		out.WithTerminationGracePeriodSeconds(*src.TerminationGracePeriodSeconds)
	}
	return out
}

// acExecAction: Command is `omitempty`, appended only when non-empty.
func acExecAction(src *corev1.ExecAction) *corev1ac.ExecActionApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.ExecAction()
	if len(src.Command) > 0 {
		out.WithCommand(src.Command...)
	}
	return out
}

// acHTTPGetAction: Port is required on the source (no `omitempty`) so
// it is always stamped; the rest follow the source's `omitempty` tags.
func acHTTPGetAction(src *corev1.HTTPGetAction) *corev1ac.HTTPGetActionApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.HTTPGetAction().WithPort(src.Port)
	if src.Path != "" {
		out.WithPath(src.Path)
	}
	if src.Host != "" {
		out.WithHost(src.Host)
	}
	if src.Scheme != "" {
		out.WithScheme(src.Scheme)
	}
	for i := range src.HTTPHeaders {
		h := corev1ac.HTTPHeader().
			WithName(src.HTTPHeaders[i].Name).
			WithValue(src.HTTPHeaders[i].Value)
		out.WithHTTPHeaders(h)
	}
	return out
}

// acTCPSocketAction: Port is required (no `omitempty`).
func acTCPSocketAction(src *corev1.TCPSocketAction) *corev1ac.TCPSocketActionApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.TCPSocketAction().WithPort(src.Port)
	if src.Host != "" {
		out.WithHost(src.Host)
	}
	return out
}

// acGRPCAction: Port is required. Service is `*string` on the source
// with no `omitempty`, but the AC's tag carries `omitempty` so a nil
// source's JSON-roundtrip output omits the field — gate on `!= nil`.
func acGRPCAction(src *corev1.GRPCAction) *corev1ac.GRPCActionApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.GRPCAction().WithPort(src.Port)
	if src.Service != nil {
		out.WithService(*src.Service)
	}
	return out
}

// addPreStopHookIfSentinelMode stamps the preStop safety net onto
// the valkey container — the in-pod failover trigger that works
// even when the operator is down — sentinels would otherwise have
// to wait for their own down-after-milliseconds timer (typically
// 30s+) to detect the dead primary. No-op in standalone mode (no
// sentinel quorum exists; nothing to call).
//
// Mechanism: at pod termination, the script asks valkey-server's own
// INFO replication for its role.
//
//   - role:slave → exit 0. Replicas are safe to terminate; sentinels
//     don't care, the operator's per-replica rollout handles the
//     orderly recreate. Replica deletes do not trigger failover —
//     only primaries call SENTINEL FAILOVER on their way out, so a
//     routine replica-pod replacement during a rolling update doesn't
//     churn the cluster's primary identity.
//   - role:master → call SENTINEL FAILOVER <masterName> against the
//     sentinel headless service; exit 0 even on failure. The happy
//     path is the operator catching the rollout (master-aware
//     rolling) and issuing the failover itself; this preStop is the
//     safety net for "kubelet killed the pod while operator was
//     unavailable". The 60s terminationGracePeriodSeconds bounds the
//     wait so a stuck sentinel call cannot stall pod termination
//     indefinitely.
//
// Auth: REDISCLI_AUTH is sourced from spec.auth.secretName (Valkey
// default-user password). The local INFO replication call needs
// it; the SENTINEL FAILOVER call uses the same env var, which
// works for the common case where sentinel auth matches the
// Valkey password. If sentinelAuthSecretName diverges, the SENTINEL
// FAILOVER call will fail with an auth error and the script still
// exits 0 — sentinels then drive the failover themselves once their
// own down-after-milliseconds expires (typically faster than the
// pod's terminationGracePeriod). Documented limitation; revisit if
// users actually run divergent sentinel auth.
func addPreStopHookIfSentinelMode(c *corev1ac.ContainerApplyConfiguration, v *valkeyv1beta1.Valkey) {
	if v.Spec.Mode != valkeyv1beta1.ModeSentinel {
		return
	}
	c.WithLifecycle(corev1ac.Lifecycle().
		WithPreStop(corev1ac.LifecycleHandler().
			WithExec(corev1ac.ExecAction().
				WithCommand("/bin/sh", "-c", valkeyPreStopScript))))
	c.WithEnv(
		corev1ac.EnvVar().WithName("MASTER_NAME").WithValue(v.Spec.Sentinel.MasterName),
		corev1ac.EnvVar().
			WithName("SENTINEL_HOST").
			WithValue(fmt.Sprintf("%s-sentinel-headless.%s.svc.cluster.local", v.Name, v.Namespace)),
	)
	if v.Spec.Auth != nil && v.Spec.Auth.SecretName != "" {
		key := v.Spec.Auth.SecretKey
		if key == "" {
			key = defaultAuthSecretKey
		}
		c.WithEnv(corev1ac.EnvVar().
			WithName("REDISCLI_AUTH").
			WithValueFrom(corev1ac.EnvVarSource().
				WithSecretKeyRef(corev1ac.SecretKeySelector().
					WithName(v.Spec.Auth.SecretName).
					WithKey(key))))
	}
}

// valkeyPreStopScript is the shell stamped into the valkey
// container's lifecycle.preStop.exec.command. See
// `addPreStopHookIfSentinelMode` for the per-role behaviour
// rationale.
//
// Always exits 0 — the 60s terminationGracePeriodSeconds bounds the
// wait, and blocking pod termination on sentinel reachability would
// convert a partial failure (sentinel unreachable) into a stuck-pod
// incident (kubelet times out and SIGKILLs anyway, but with a noisy
// event trail).
//
// `valkey-cli` reads the auth password from REDISCLI_AUTH so the
// password never appears in `ps` output (the env var is only
// readable by the container's own UID).
const valkeyPreStopScript = `set -u
ROLE=$(valkey-cli -p 6379 INFO replication 2>/dev/null | awk -F: '$1=="role"{print $2}' | tr -d '\r')
case "$ROLE" in
  slave)
    exit 0
    ;;
  master)
    valkey-cli -h "$SENTINEL_HOST" -p 26379 SENTINEL FAILOVER "$MASTER_NAME" >/dev/null 2>&1 || true
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
`

// buildExporterContainer builds the data-plane exporter sidecar,
// pointed at the local Valkey (localhost:6379).
func buildExporterContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	return buildExporterContainerForPort(v, defaultValkeyPort)
}

// buildSentinelExporterContainer builds the sentinel-plane exporter
// sidecar, pointed at the local Sentinel (localhost:26379).
// oliver006/redis_exporter auto-detects a Sentinel target and emits
// the redis_sentinel_* series, giving the sentinel alert pack and the
// sentinel PodMonitor a metric producer (data-plane sidecar covers only
// 6379 and never produces sentinel series).
func buildSentinelExporterContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	return buildExporterContainerForPort(v, defaultSentinelPort)
}

// buildExporterContainerForPort builds a redis_exporter sidecar pointed
// at redis://localhost:<redisPort>. The data-plane (6379) and sentinel
// (26379) sidecars are identical except for that target port — same
// image, "metrics" port, auth wiring, /tmp scratch mount, resources, and
// restricted read-only security context.
func buildExporterContainerForPort(v *valkeyv1beta1.Valkey, redisPort int32) *corev1ac.ContainerApplyConfiguration {
	env := []*corev1ac.EnvVarApplyConfiguration{
		corev1ac.EnvVar().
			WithName("REDIS_ADDR").
			WithValue(fmt.Sprintf("redis://localhost:%d", redisPort)),
	}
	// When the CR carries auth, the local Valkey/Sentinel rejects every
	// command from the exporter with `NOAUTH Authentication
	// required` and the /metrics endpoint serves only Go-runtime
	// metrics — no `redis_*` series, no telemetry, every per-pod
	// dashboard panel blank. oliver006/redis_exporter reads
	// REDIS_PASSWORD as its connection password (per upstream
	// README); inject it from the same Secret the valkey + sentinel
	// containers consume, so the operator-managed auth pipeline is
	// the single source of truth. The sentinel listener's requirepass
	// matches the master, so the same Secret authenticates both ports.
	if v.Spec.Auth != nil && v.Spec.Auth.SecretName != "" {
		key := v.Spec.Auth.SecretKey
		if key == "" {
			key = defaultAuthSecretKey
		}
		env = append(env,
			corev1ac.EnvVar().
				WithName("REDIS_PASSWORD").
				WithValueFrom(corev1ac.EnvVarSource().
					WithSecretKeyRef(corev1ac.SecretKeySelector().
						WithName(v.Spec.Auth.SecretName).
						WithKey(key))),
		)
	}
	return corev1ac.Container().
		WithName("exporter").
		WithImage(v.Spec.Image.Exporter.Repository + ":" + v.Spec.Image.Exporter.Tag).
		WithPorts(corev1ac.ContainerPort().
			WithName("metrics").
			WithContainerPort(defaultExporterPort).
			WithProtocol(corev1.ProtocolTCP)).
		WithEnv(env...).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName("tmp").WithMountPath("/tmp"),
		).
		WithResources(exporterResources(v)).
		WithSecurityContext(restrictedContainerSecurityContextReadOnly())
}

// exporterResources honours spec.metrics.resources when the user sets
// it. Otherwise it stamps requests=10m/32Mi and a 64Mi memory limit
// with no CPU limit — a 100m CPU limit on the bursty redis_exporter
// trips CFS-quota bucketing (every 30s scrape needing >10ms in one
// 100ms period counts as "throttled") and lights up
// CPUThrottlingHighAlert on a sidecar whose long-run usage is ~1m.
func exporterResources(v *valkeyv1beta1.Valkey) *corev1ac.ResourceRequirementsApplyConfiguration {
	if v.Spec.Metrics.Resources.Requests != nil || v.Spec.Metrics.Resources.Limits != nil {
		out := corev1ac.ResourceRequirements()
		if v.Spec.Metrics.Resources.Requests != nil {
			out = out.WithRequests(v.Spec.Metrics.Resources.Requests)
		}
		if v.Spec.Metrics.Resources.Limits != nil {
			out = out.WithLimits(v.Spec.Metrics.Resources.Limits)
		}
		return out
	}
	return corev1ac.ResourceRequirements().
		WithRequests(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		}).
		WithLimits(corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		})
}

// restrictedPodSecurityContext returns a pod-level SecurityContext that
// the Pod Security Admission "restricted" profile accepts. User-supplied
// fields override the operator's default; load-bearing fields like
// runAsNonRoot=true are stamped only when missing.
func restrictedPodSecurityContext(user *corev1.PodSecurityContext) *corev1ac.PodSecurityContextApplyConfiguration {
	merged := corev1.PodSecurityContext{}
	if user != nil {
		merged = *user.DeepCopy()
	}
	if merged.RunAsNonRoot == nil {
		merged.RunAsNonRoot = new(true)
	}
	if merged.RunAsUser == nil {
		merged.RunAsUser = new(int64(1000))
	}
	if merged.RunAsGroup == nil {
		merged.RunAsGroup = new(int64(1000))
	}
	if merged.FSGroup == nil {
		merged.FSGroup = new(int64(1000))
	}
	// Without this, kubelet defaults to FSGroupChangePolicy=Always and
	// recursively chowns the whole data PVC on every pod (re)start —
	// material startup latency on large volumes. OnRootMismatch skips the
	// walk when the volume's top-level ownership already matches fsGroup.
	if merged.FSGroupChangePolicy == nil {
		merged.FSGroupChangePolicy = ptr.To(corev1.FSGroupChangeOnRootMismatch)
	}
	if merged.SeccompProfile == nil {
		merged.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
	return acPodSecurityContext(&merged)
}

// acPodSecurityContext builds the apply-config. Every source field
// carries `omitempty`; every WithX is gated on non-nil/non-empty.
func acPodSecurityContext(src *corev1.PodSecurityContext) *corev1ac.PodSecurityContextApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.PodSecurityContext()
	if src.SELinuxOptions != nil {
		out.WithSELinuxOptions(acSELinuxOptions(src.SELinuxOptions))
	}
	if src.WindowsOptions != nil {
		out.WithWindowsOptions(acWindowsSecurityContextOptions(src.WindowsOptions))
	}
	if src.RunAsUser != nil {
		out.WithRunAsUser(*src.RunAsUser)
	}
	if src.RunAsGroup != nil {
		out.WithRunAsGroup(*src.RunAsGroup)
	}
	if src.RunAsNonRoot != nil {
		out.WithRunAsNonRoot(*src.RunAsNonRoot)
	}
	if len(src.SupplementalGroups) > 0 {
		out.WithSupplementalGroups(src.SupplementalGroups...)
	}
	if src.SupplementalGroupsPolicy != nil {
		out.WithSupplementalGroupsPolicy(*src.SupplementalGroupsPolicy)
	}
	if src.FSGroup != nil {
		out.WithFSGroup(*src.FSGroup)
	}
	for i := range src.Sysctls {
		out.WithSysctls(corev1ac.Sysctl().
			WithName(src.Sysctls[i].Name).
			WithValue(src.Sysctls[i].Value))
	}
	if src.FSGroupChangePolicy != nil {
		out.WithFSGroupChangePolicy(*src.FSGroupChangePolicy)
	}
	if src.SeccompProfile != nil {
		out.WithSeccompProfile(acSeccompProfile(src.SeccompProfile))
	}
	if src.AppArmorProfile != nil {
		out.WithAppArmorProfile(acAppArmorProfile(src.AppArmorProfile))
	}
	if src.SELinuxChangePolicy != nil {
		out.WithSELinuxChangePolicy(*src.SELinuxChangePolicy)
	}
	return out
}

// acSELinuxOptions: all four fields are `omitempty` — skip empty.
func acSELinuxOptions(src *corev1.SELinuxOptions) *corev1ac.SELinuxOptionsApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.SELinuxOptions()
	if src.User != "" {
		out.WithUser(src.User)
	}
	if src.Role != "" {
		out.WithRole(src.Role)
	}
	if src.Type != "" {
		out.WithType(src.Type)
	}
	if src.Level != "" {
		out.WithLevel(src.Level)
	}
	return out
}

// acWindowsSecurityContextOptions: every field is `*string`/`*bool`
// with `omitempty` — nil sources skip.
func acWindowsSecurityContextOptions(src *corev1.WindowsSecurityContextOptions) *corev1ac.WindowsSecurityContextOptionsApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.WindowsSecurityContextOptions()
	if src.GMSACredentialSpecName != nil {
		out.WithGMSACredentialSpecName(*src.GMSACredentialSpecName)
	}
	if src.GMSACredentialSpec != nil {
		out.WithGMSACredentialSpec(*src.GMSACredentialSpec)
	}
	if src.RunAsUserName != nil {
		out.WithRunAsUserName(*src.RunAsUserName)
	}
	if src.HostProcess != nil {
		out.WithHostProcess(*src.HostProcess)
	}
	return out
}

// acSeccompProfile: Type has no `omitempty` (always stamped).
func acSeccompProfile(src *corev1.SeccompProfile) *corev1ac.SeccompProfileApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.SeccompProfile().WithType(src.Type)
	if src.LocalhostProfile != nil {
		out.WithLocalhostProfile(*src.LocalhostProfile)
	}
	return out
}

// acAppArmorProfile: Type has no `omitempty` (always stamped).
func acAppArmorProfile(src *corev1.AppArmorProfile) *corev1ac.AppArmorProfileApplyConfiguration {
	if src == nil {
		return nil
	}
	out := corev1ac.AppArmorProfile().WithType(src.Type)
	if src.LocalhostProfile != nil {
		out.WithLocalhostProfile(*src.LocalhostProfile)
	}
	return out
}

func restrictedContainerSecurityContext() *corev1ac.SecurityContextApplyConfiguration {
	return corev1ac.SecurityContext().
		WithAllowPrivilegeEscalation(false).
		WithRunAsNonRoot(true).
		WithCapabilities(corev1ac.Capabilities().WithDrop("ALL")).
		WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault))
}

// restrictedContainerSecurityContextReadOnly is the restricted context
// hardened with readOnlyRootFilesystem=true. Used by the long-running
// data-plane containers (valkey, sentinel, exporter): all their writes
// land on mounted volumes — the data PVC/emptyDir, the config /
// sentinel-conf emptyDirs, and the `tmp` emptyDir at /tmp (which also
// backs the non-persistent `dir /tmp`). Init containers keep the
// writable-root base: their setup scripts may stage files on the root fs.
func restrictedContainerSecurityContextReadOnly() *corev1ac.SecurityContextApplyConfiguration {
	return restrictedContainerSecurityContext().WithReadOnlyRootFilesystem(true)
}
