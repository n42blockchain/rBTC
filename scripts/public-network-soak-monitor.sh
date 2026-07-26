#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 || $# -gt 6 ]]; then
  echo "usage: $0 OUTPUT_DIR BITCOIN_PID BITCOIN_DATA_DIR TESTNET4_PID TESTNET4_DATA_DIR [INTERVAL_SECONDS]" >&2
  exit 2
fi

output_dir=$1
bitcoin_pid=$2
bitcoin_data_dir=$3
testnet4_pid=$4
testnet4_data_dir=$5
interval_seconds=${6:-60}

for value in "$bitcoin_pid" "$testnet4_pid" "$interval_seconds"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "PID and interval values must be positive integers" >&2
    exit 2
  fi
done
for directory in "$output_dir" "$bitcoin_data_dir" "$testnet4_data_dir"; do
  if [[ ! -d "$directory" || -L "$directory" ]]; then
    echo "directory must exist and must not be a symlink: $directory" >&2
    exit 2
  fi
done

metrics_dir="$output_dir/metrics"
mkdir -p "$metrics_dir"
process_metrics="$metrics_dir/process.tsv"
disk_metrics="$metrics_dir/disk.tsv"
events="$metrics_dir/events.log"

if [[ ! -e "$process_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tpid\telapsed\trss_kib\tcpu_percent\n' >"$process_metrics"
fi
if [[ ! -e "$disk_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tdata_kib\tfilesystem_free_kib\n' >"$disk_metrics"
fi

sample_process() {
  local timestamp=$1
  local network=$2
  local pid=$3
  if ! kill -0 "$pid" 2>/dev/null; then
    printf '%s\t%s\tpid=%s exited\n' "$timestamp" "$network" "$pid" >>"$events"
    return 1
  fi
  local values
  values=$(ps -p "$pid" -o etime=,rss=,%cpu= | awk '{$1=$1; print}')
  if [[ -z "$values" ]]; then
    printf '%s\t%s\tpid=%s unavailable\n' "$timestamp" "$network" "$pid" >>"$events"
    return 1
  fi
  local elapsed rss cpu
  read -r elapsed rss cpu <<<"$values"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$timestamp" "$network" "$pid" "$elapsed" "$rss" "$cpu" >>"$process_metrics"
}

sample_disk() {
  local timestamp=$1
  local network=$2
  local directory=$3
  local data_kib free_kib
  data_kib=$(du -sk "$directory" | awk '{print $1}')
  free_kib=$(df -k "$directory" | awk 'NR == 2 {print $4}')
  printf '%s\t%s\t%s\t%s\n' \
    "$timestamp" "$network" "$data_kib" "$free_kib" >>"$disk_metrics"
}

sample=0
while true; do
  timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  sample_process "$timestamp" bitcoin "$bitcoin_pid" || exit 1
  sample_process "$timestamp" testnet4 "$testnet4_pid" || exit 1
  if (( sample % 60 == 0 )); then
    sample_disk "$timestamp" bitcoin "$bitcoin_data_dir"
    sample_disk "$timestamp" testnet4 "$testnet4_data_dir"
  fi
  sample=$((sample + 1))
  sleep "$interval_seconds"
done
