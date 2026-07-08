{{/*
Full chart name. Truncated to 63 chars to fit DNS-label limits; strips a
trailing hyphen.
*/}}
{{- define "velkir.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "velkir.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "velkir.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every operator resource.
*/}}
{{- define "velkir.labels" -}}
helm.sh/chart: {{ include "velkir.chart" . }}
{{ include "velkir.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels — stable subset of the label set. Never include anything that
changes across upgrades (image version, chart version).
*/}}
{{- define "velkir.selectorLabels" -}}
app.kubernetes.io/name: {{ include "velkir.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
ServiceAccount name: user override wins; else "<fullname>" when creation is on,
else "default".
*/}}
{{- define "velkir.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "velkir.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Metrics Service name: "<fullname>-metrics". Single-sources the derivation
shared by the metrics Service, the operator's --metrics-service-name flag, the
ServiceMonitor serverName, and the alert pack's PromQL `job=` selectors, so
they cannot drift from the rendered Service name.
*/}}
{{- define "velkir.metricsServiceName" -}}
{{- printf "%s-metrics" (include "velkir.fullname" .) -}}
{{- end -}}

{{/*
Effective image reference. If tag is empty, fall back to Chart.AppVersion.
When .Values.image.digest is set, append "@<digest>" so the cluster pulls
the exact bytes the operator was signed against (required for digest-
binding image-policy verification, e.g. Kyverno verifyImages with
sigstore-signed digests).
*/}}
{{- define "velkir.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- $ref := printf "%s:%s" .Values.image.repository $tag -}}
{{- with .Values.image.digest -}}
{{- printf "%s@%s" $ref . -}}
{{- else -}}
{{- $ref -}}
{{- end -}}
{{- end -}}

{{/*
Fail-closed value validation, invoked from the always-rendered Deployment.
The admission webhook is mandatory: the manager serves :9443 unconditionally
and the webhook enforces the timing-floor, IP-only addressing, and
masterName-immutability invariants. webhook.enabled=false only drops the
WebhookConfigurations, leaving a half-configured install (cert minted, port
bound, no admission routing) — reject it at render time rather than ship it.
*/}}
{{- define "velkir.validateValues" -}}
{{- if not .Values.webhook.enabled -}}
{{- fail "webhook.enabled=false is not supported: the operator always serves its admission webhook on :9443 (it enforces the timing-floor, IP-only addressing, and sentinel.masterName-immutability invariants) and cannot run without it. Leave webhook.enabled=true; use webhook.certManager.enabled to choose the certificate source." -}}
{{- end -}}
{{- end -}}

{{/*
namespaceSelector body shared by the mutating + validating WebhookConfigurations.

Default (no webhook.namespaceSelector override): exclude kube-system and the
operator's own release namespace, so a webhook-down window (cert rotation,
leader handover, dynauth bootstrap) can never lock admission for CRs the
operator itself might own.

Override set: the user's selector is honoured for scoped installs (e.g.
shared-cluster e2e targeting one labelled namespace), but the kube-system
exclusion is merged in unconditionally. kube-system is the control plane's
namespace; a failurePolicy=Fail webhook must never be able to block admission
there regardless of how the selector is scoped. The release-namespace
exclusion is NOT forced on an override — a scoped install may legitimately
target it, and an override is an explicit opt-in to a narrower surface.
*/}}
{{- define "velkir.webhookNamespaceSelector" -}}
{{- if .Values.webhook.namespaceSelector }}
{{- $sel := deepCopy .Values.webhook.namespaceSelector }}
{{- $exprs := append (default (list) $sel.matchExpressions) (dict "key" "kubernetes.io/metadata.name" "operator" "NotIn" "values" (list "kube-system")) }}
{{- $_ := set $sel "matchExpressions" $exprs }}
{{- toYaml $sel }}
{{- else -}}
matchExpressions:
  - key: kubernetes.io/metadata.name
    operator: NotIn
    values:
      - kube-system
      - {{ .Release.Namespace }}
{{- end }}
{{- end -}}
