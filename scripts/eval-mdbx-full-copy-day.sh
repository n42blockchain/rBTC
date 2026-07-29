#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 5 ]]; then
  cat >&2 <<'EOF'
usage: eval-mdbx-full-copy-day.sh SOURCE_DATA_DIR TARGET_DIR [BATCH_SIZE] [DURATION_SECONDS] [SAMPLE_SECONDS]

  SOURCE_DATA_DIR: existing node data directory containing chainstate.redb
  TARGET_DIR: directory where MDBX data is written
  BATCH_SIZE: migration batch rows (default 20000)
  DURATION_SECONDS: default 86400 (24h)
  SAMPLE_SECONDS: default 30

Optional env:
  MDBX_EVAL_RUN_CMD: workload command to run against TARGET_DIR while monitoring.
EOF
  exit 2
fi

source_data_dir=$1
target_dir=$2
batch_size=${3:-20000}
duration_seconds=${4:-86400}
sample_seconds=${5:-30}
report_file="${target_dir}/mdbx-migration-report.json"
monitor_file="${target_dir}/size-trace.tsv"
run_cmd="${MDBX_EVAL_RUN_CMD:-}"

if [[ ! -d "$source_data_dir" || -L "$source_data_dir" ]]; then
  echo "source data directory must be an existing non-symlink directory: $source_data_dir" >&2
  exit 1
fi
if [[ ! -f "$source_data_dir/chainstate.redb" || -L "$source_data_dir/chainstate.redb" ]]; then
  echo "expected source chainstate redb at: $source_data_dir/chainstate.redb" >&2
  exit 1
fi
if ! [[ "$batch_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "batch size must be a positive integer" >&2
  exit 1
fi
if ! [[ "$duration_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "duration must be a positive integer" >&2
  exit 1
fi
if ! [[ "$sample_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "sample seconds must be a positive integer" >&2
  exit 1
fi

mkdir -p "$target_dir"

echo "start migration: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee "$monitor_file"
echo "migrating into $target_dir" | tee -a "$monitor_file"

cargo run --locked --release --example redb_to_mdbx_migrate --features mdbx -- \
  --source "$source_data_dir/chainstate.redb" \
  --target "$target_dir/mdbx-chainstate" \
  --batch-size "$batch_size" \
  --report "$report_file" \
  --overwrite \
  --verify

if [[ -n "$run_cmd" ]]; then
  echo "starting workload: $run_cmd" | tee -a "$monitor_file"
  bash -lc "$run_cmd" >"${target_dir}/workload.out" 2>&1 &
  workload_pid=$!
else
  workload_pid=
fi

echo "timestamp_utc\telapsed_seconds\ttarget_mdbx_bytes\ttarget_mdbx_kib\ttarget_dir_kib\trss_kib" | tee -a "$monitor_file"

start_epoch=$(date +%s)
end_epoch=$((start_epoch + duration_seconds))
while (( $(date +%s) < end_epoch )); do
  now=$(date +%s)
  elapsed=$((now - start_epoch))
  if [[ -d "$target_dir/mdbx-chainstate" ]]; then
    mdbx_bytes=$(du -sb "$target_dir/mdbx-chainstate" | awk '{print $1}')
  else
    mdbx_bytes=0
  fi
  mdbx_kib=$(( mdbx_bytes / 1024 ))
  target_kib=$(du -sk "$target_dir" | awk '{print $1}')
  if [[ -n "$workload_pid" ]] && kill -0 "$workload_pid" 2>/dev/null; then
    rssi=$(ps -p "$workload_pid" -o rss= 2>/dev/null | tr -d ' ' || echo 0)
  else
    rssi=0
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "$elapsed" \
    "$mdbx_bytes" \
    "$mdbx_kib" \
    "$target_kib" \
    "$rssi" >>"$monitor_file"
  sleep "$sample_seconds"
done

if [[ -n "$workload_pid" ]] && kill -0 "$workload_pid" 2>/dev/null; then
  kill "$workload_pid" 2>/dev/null || true
  wait "$workload_pid" 2>/dev/null || true
fi

echo "completed after ${duration_seconds}s: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee -a "$monitor_file"
