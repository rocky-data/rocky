{{- define "rocky.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rocky.fullname" -}}
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

{{- define "rocky.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "rocky.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "rocky.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rocky.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rocky.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "rocky.claimName" -}}
{{- if .Values.persistence.existingClaim -}}
{{- .Values.persistence.existingClaim -}}
{{- else -}}
{{- include "rocky.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
The serve arguments, defined once so the Deployment and the guide cannot drift.
*/}}
{{- define "rocky.serveArgs" -}}
- serve
- --host
- 0.0.0.0
{{- if .Values.serve.ui.enabled }}
- --ui
- --token-scope
- {{ .Values.serve.ui.tokenScope | quote }}
{{- end }}
{{- if eq .Values.scheduling.mode "resident" }}
- --scheduler
- --poll-interval-seconds
- {{ .Values.scheduling.resident.pollIntervalSeconds | quote }}
- --drain-timeout-seconds
- {{ .Values.scheduling.resident.drainTimeoutSeconds | quote }}
{{- end }}
{{- if .Values.serve.ui.enabled }}
{{- if and .Values.ingress.enabled .Values.ingress.host }}
- --allowed-host
- {{ .Values.ingress.host | quote }}
{{- end }}
{{- range .Values.serve.extraAllowedHosts }}
- --allowed-host
- {{ . | quote }}
{{- end }}
{{- end }}
{{- range .Values.serve.allowedOrigins }}
- --allowed-origin
- {{ . | quote }}
{{- end }}
{{- end -}}
