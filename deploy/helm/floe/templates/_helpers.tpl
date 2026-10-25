{{- define "floe.name" -}}{{ .Release.Name }}{{- end -}}

{{- define "floe.labels" -}}
app.kubernetes.io/part-of: floe
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "floe.selector" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "floe.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}

{{/* Environment shared by floe workloads: non-secret config + S3 credentials from the Secret. */}}
{{- define "floe.env" -}}
- name: FLOE_S3_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: {{ include "floe.name" . }}-credentials, key: s3-access-key}}
- name: FLOE_S3_SECRET_KEY
  valueFrom: {secretKeyRef: {name: {{ include "floe.name" . }}-credentials, key: s3-secret-key}}
{{- end -}}

{{- define "floe.envFrom" -}}
- configMapRef:
    name: {{ include "floe.name" . }}-config
{{- end -}}

{{- define "floe.tmpVolume" -}}
- name: tmp
  emptyDir: {sizeLimit: 4Gi}
{{- end -}}

{{- define "floe.tmpMount" -}}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{/* initContainer: block until bootstrap has created the tables and topic. */}}
{{- define "floe.waitReady" -}}
{{- $r := .root -}}
- name: wait-ready
  image: {{ include "floe.image" $r }}
  imagePullPolicy: {{ $r.Values.image.pullPolicy }}
  args: ["wait-ready", "--timeout", "600"{{ if not .kafka }}, "--no-kafka"{{ end }}{{ if .noCatalog }}, "--no-catalog"{{ end }}]
  envFrom: {{- include "floe.envFrom" $r | nindent 4 }}
  securityContext: {{- toYaml $r.Values.containerSecurityContext | nindent 4 }}
  resources:
    requests: {cpu: 10m, memory: 64Mi}
    limits: {cpu: 500m, memory: 256Mi}
  volumeMounts: {{- include "floe.tmpMount" $r | nindent 4 }}
{{- end -}}
