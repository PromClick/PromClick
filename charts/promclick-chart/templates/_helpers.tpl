{{- define "promclick.clickhouseSecretName" -}}
{{- $clickhouse := default dict .Values.secretRefs.clickhouse -}}
{{- if $clickhouse.existingSecret -}}
{{- $clickhouse.existingSecret -}}
{{- else -}}
{{- printf "%s-clickhouse" .Release.Name -}}
{{- end -}}
{{- end -}}

{{- define "promclick.depName" -}}
{{- default .key .dep.name -}}
{{- end -}}

{{- define "promclick.configMapName" -}}
{{- printf "%s-config" (include "promclick.depName" .) -}}
{{- end -}}

{{- define "promclick.configFileName" -}}
{{- $cfg := default dict .dep.config -}}
{{- default (printf "%s.yaml" $cfg.kind) $cfg.filename -}}
{{- end -}}

{{- define "promclick.defaults.shared" -}}
clickhouse:
  http_addr: "http://clickhouse:8123"
  native_addr: "clickhouse:9000"
  database: "metrics"
  user: "${CLICKHOUSE_USER}"
  password: "${CLICKHOUSE_PASSWORD}"

schema:
  samples_table: "samples"
  time_series_table: "time_series"
  columns:
    metric_name: "metric_name"
    timestamp: "unix_milli"
    value: "value"
    fingerprint: "fingerprint"
    labels: "labels"
  labels_type: "json"
{{- end -}}

{{- define "promclick.defaults.proxy" -}}
listen_addr: ":9099"
query_timeout: "2m"

labels:
  cache_enabled: true
  cache_ttl: "60s"
  cache_max_series: 50000

cors:
  allow_origin: "*"

downsampling:
  enabled: true
  raw_retention: "7d"
  tiers:
    - name: "5m"
      resolution: "5m"
      table: "samples_5m"
      compact_after: "40h"
      retention: "90d"
      min_step: "60s"
    - name: "1h"
      resolution: "1h"
      table: "samples_1h"
      compact_after: "240h"
      retention: "730d"
      min_step: "3600s"
{{- end -}}

{{- define "promclick.defaults.writer" -}}
listen_addr: ":9091"

write:
  batch_size: 10000
  queue_size: 100000
  flush_interval: "5s"
{{- end -}}

{{- define "promclick.defaults.downsampler" -}}
daemon: true

downsampling:
  enabled: true
  raw_retention: "7d"
  tiers:
    - name: "5m"
      resolution: "5m"
      table: "samples_5m"
      compact_after: "40h"
      retention: "90d"
      min_step: "60s"
    - name: "1h"
      resolution: "1h"
      table: "samples_1h"
      compact_after: "240h"
      retention: "730d"
      min_step: "3600s"
{{- end -}}

{{- define "promclick.renderConfig" -}}
{{- $root := .root -}}
{{- $kind := .kind -}}
{{- $overrides := default dict .overrides -}}

{{- $sharedHard := include "promclick.defaults.shared" $root | fromYaml -}}
{{- $kindHard := include (printf "promclick.defaults.%s" $kind) $root | fromYaml -}}

{{- $sharedUser := default dict $root.Values.configDefaults.shared -}}
{{- $kindUser := default dict (index $root.Values.configDefaults $kind) -}}

{{- $merged := mergeOverwrite $sharedHard $sharedUser $kindHard $kindUser $overrides -}}

{{- if eq $kind "downsampler" }}
  {{- $daemon := default false $merged.daemon -}}
  {{- $downsampling := default dict $merged.downsampling -}}

  {{- if $daemon }}
    {{- if not (hasKey $downsampling "interval") }}
      {{- $_ := set $downsampling "interval" "1h" -}}
    {{- end }}
  {{- else }}
    {{- if hasKey $downsampling "interval" }}
      {{- $_ := unset $downsampling "interval" -}}
    {{- end }}
  {{- end }}

  {{- $_ := set $merged "downsampling" $downsampling -}}
{{- end }}

{{- toYaml $merged -}}
{{- end -}}