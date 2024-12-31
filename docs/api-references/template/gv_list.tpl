{{- define "gvList" -}}
{{- $groupVersions := . -}}

# ETCD Operator API References

## Packages
{{- range $groupVersions }}
- {{ markdownRenderGVLink . }}
{{- end }}

{{ range $groupVersions }}
{{ template "gvDetails" . }}
{{ end }}

{{- end -}}
