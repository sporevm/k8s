{{- define "sporevm-k8s.labels" -}}
app.kubernetes.io/part-of: sporevm-k8s
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "sporevm-k8s.agentServiceAccountName" -}}
{{- if .Values.agent.serviceAccount.create -}}
{{ .Values.agent.serviceAccount.name }}
{{- else -}}
{{ .Values.agent.serviceAccount.name }}
{{- end -}}
{{- end -}}
