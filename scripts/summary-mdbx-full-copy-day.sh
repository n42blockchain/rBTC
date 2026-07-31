#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  cat >&2 <<'EOF'
usage: summary-mdbx-full-copy-day.sh EVAL_DIR [REPORT_JSON] [TRACE_TSV] [OUTPUT_JSON]

  EVAL_DIR: directory created by eval-mdbx-full-copy-day.sh
  REPORT_JSON: default mdbx-migration-report.json
  TRACE_TSV: default size-trace.tsv
  OUTPUT_JSON: default mdbx-migration-summary.json
EOF
  exit 2
fi

eval_dir=$1
report_json=${2:-"$eval_dir/mdbx-migration-report.json"}
trace_tsv=${3:-"$eval_dir/size-trace.tsv"}
output_json=${4:-"$eval_dir/mdbx-migration-summary.json"}

if [[ ! -f "$report_json" || -L "$report_json" ]]; then
  echo "missing report: $report_json" >&2
  exit 1
fi
if [[ ! -f "$trace_tsv" || -L "$trace_tsv" ]]; then
  echo "missing trace: $trace_tsv" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for summary generation" >&2
  exit 1
fi

read -r source_redb_bytes target_mdbx_bytes target_hot target_cold elapsed_seconds < <(
  jq -r '
    [
      .source_redb_bytes,
      .target_mdbx_bytes,
      .target_mdbx_hot,
      .target_mdbx_cold,
      .elapsed_seconds
    ] | @tsv
  ' "$report_json"
)

if [[ -z "$source_redb_bytes" || "$source_redb_bytes" == "null" ]]; then
  source_redb_bytes=0
fi
if [[ -z "$target_mdbx_bytes" || "$target_mdbx_bytes" == "null" ]]; then
  target_mdbx_bytes=0
fi

sample_count=$(($(wc -l <"$trace_tsv") - 1))
if (( sample_count < 1 )); then
  echo "trace has no samples: $trace_tsv" >&2
  exit 1
fi

start_bytes=$(awk 'NR==2 {print $3}' "$trace_tsv")
end_bytes=$(awk 'END {print $3}' "$trace_tsv")
max_bytes=$(awk 'NR>1 {if ($3+0>m) m=$3; c++; if(c==1)s=$3; e=$3} END{print m}' "$trace_tsv")
min_bytes=$(awk 'NR>1 {print $3}' "$trace_tsv" | awk 'NR==1{m=$1} $1+0 < m {m=$1} END{print m}')
max_rss=$(awk 'NR>1 {if ($6+0>m) m=$6} END{print m+0}' "$trace_tsv")
target_kib_end=$(awk 'END {print $5}' "$trace_tsv")
duration_seconds=$(awk 'NR>1 {end=$2} END{print end+0}' "$trace_tsv")

if [[ -z "$start_bytes" || -z "$end_bytes" || -z "$max_bytes" ]]; then
  echo "invalid trace format: $trace_tsv" >&2
  exit 1
fi

size_growth=$((end_bytes - start_bytes))
if (( source_redb_bytes > 0 )); then
  # shell arithmetic cannot do floats; keep ratio as basis points to avoid precision issues.
  ratio_bp=$(( (target_mdbx_bytes * 10000) / source_redb_bytes ))
else
  ratio_bp=0
fi
ratio_percent=$(awk -v bp="$ratio_bp" 'BEGIN { printf "%.2f", bp / 100.0 }')

cat >"$output_json" <<JSON
{
  "evaluation_dir": "$eval_dir",
  "report_json": "$report_json",
  "trace_tsv": "$trace_tsv",
  "report": {
    "source_redb_bytes": $source_redb_bytes,
    "target_mdbx_bytes": $target_mdbx_bytes,
    "target_hot": $target_hot,
    "target_cold": $target_cold,
    "migration_elapsed_seconds": $elapsed_seconds
  },
  "size_trace": {
    "sample_count": $sample_count,
    "start_bytes": $start_bytes,
    "end_bytes": $end_bytes,
    "max_bytes": $max_bytes,
    "min_bytes": $min_bytes,
    "target_kib_end": $target_kib_end,
    "max_rss_kib": $max_rss,
    "duration_seconds": $duration_seconds
  },
  "comparison": {
    "size_growth_bytes": $size_growth,
    "compact_ratio_bps": $ratio_bp,
    "compact_ratio_percent": $ratio_percent
  }
}
JSON

jq . "$output_json"
