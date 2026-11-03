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

{{/* "true" when serve-batch must also be given its key as `--api-key`, else "".
     Images before v0.3.0 ignore FORGE_API_KEY and would serve with no key at all.
     `helm upgrade --reuse-values` keeps an old release's image tag, so this cannot
     assume the chart's default image. Every forge version accepts `--api-key`, so a
     tag this cannot read as v0.3.0 or later gets it too: fail safe, not open. */}}
{{- define "forge.apiKeyAsArg" -}}
{{- $mode := .Values.serveBatch.apiKeyArg | default "auto" -}}
{{- if eq $mode "always" -}}
true
{{- else if eq $mode "auto" -}}
{{- $tag := .Values.image.tag | toString | trimPrefix "v" -}}
{{- if regexMatch `^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$` $tag -}}
{{- if not (semverCompare ">=0.3.0-0" $tag) -}}
true
{{- end -}}
{{- else -}}
true
{{- end -}}
{{- else if ne $mode "never" -}}
{{- fail (printf "serveBatch.apiKeyArg must be auto, always or never, not %q" $mode) -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret carrying the serve-batch bearer key, or "" when auth is off. */}}
{{- define "forge.apiKeySecretName" -}}
{{- if .Values.serveBatch.existingApiKeySecret -}}
{{- .Values.serveBatch.existingApiKeySecret -}}
{{- else if .Values.serveBatch.apiKey -}}
{{- include "forge.fullname" . -}}-api-key
{{- end -}}
{{- end -}}
