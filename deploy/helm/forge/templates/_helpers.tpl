{{/* The chart is NAMED forge-chart only because the OCI repo sits beside the
     mancube/forge image repo; every in-cluster name stays plain "forge". */}}
{{- define "forge.name" -}}
forge
{{- end -}}

{{- define "forge.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "forge.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "forge.labels" -}}
app.kubernetes.io/name: {{ include "forge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "forge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "forge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "forge.image" -}}
{{- required "image.repository is required — build the repo-root Dockerfile and push it to your registry" .Values.image.repository -}}:{{- .Values.image.tag -}}
{{- end -}}

{{/* Name of the Secret carrying the serve-batch bearer key, or "" when auth is off. */}}
{{- define "forge.apiKeySecretName" -}}
{{- if .Values.serveBatch.existingApiKeySecret -}}
{{- .Values.serveBatch.existingApiKeySecret -}}
{{- else if .Values.serveBatch.apiKey -}}
{{- include "forge.fullname" . -}}-api-key
{{- end -}}
{{- end -}}
