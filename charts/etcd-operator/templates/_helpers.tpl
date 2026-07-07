{{/*
Expand the name of the chart.
*/}}
{{- define "etcd-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name, truncated at 63 chars.
*/}}
{{- define "etcd-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "etcd-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "etcd-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "etcd-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "etcd-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Manager image ref: digest pin wins over tag; tag defaults to appVersion.
*/}}
{{- define "etcd-operator.image" -}}
{{- if .Values.controller.image.digest -}}
{{ .Values.controller.image.repository }}@{{ .Values.controller.image.digest }}
{{- else -}}
{{ .Values.controller.image.repository }}:{{ .Values.controller.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end }}

{{- define "etcd-operator.serviceAccountName" -}}
{{ include "etcd-operator.fullname" . }}
{{- end }}
