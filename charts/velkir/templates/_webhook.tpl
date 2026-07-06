{{/*
Webhook helpers. Actual ValidatingWebhookConfiguration / MutatingWebhookConfiguration
resources and the cert provisioning Job (self-signed path) or Certificate
(cert-manager path).

These helpers stabilise the names used in both the webhook Service and the
webhook configurations, so they stay in sync once the resources exist.
*/}}

{{- define "velkir.webhook.serviceName" -}}
{{- printf "%s-webhook" (include "velkir.fullname" .) -}}
{{- end -}}

{{- define "velkir.webhook.secretName" -}}
{{- printf "%s-webhook-cert" (include "velkir.fullname" .) -}}
{{- end -}}

{{- define "velkir.webhook.caSecretName" -}}
{{- printf "%s-webhook-ca" (include "velkir.fullname" .) -}}
{{- end -}}

{{- define "velkir.webhook.certManagerIssuerName" -}}
{{- printf "%s-selfsigned" (include "velkir.fullname" .) -}}
{{- end -}}

{{- define "velkir.webhook.certManagerCertName" -}}
{{- printf "%s-webhook-cert" (include "velkir.fullname" .) -}}
{{- end -}}
