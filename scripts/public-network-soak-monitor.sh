#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 || $# -gt 6 ]]; then
  echo "usage: $0 OUTPUT_DIR BITCOIN_PID_OR_FILE BITCOIN_DATA_DIR TESTNET4_PID_OR_FILE TESTNET4_DATA_DIR [INTERVAL_SECONDS]" >&2
  exit 2
fi

output_dir=$1
bitcoin_pid_source=$2
bitcoin_data_dir=$3
testnet4_pid_source=$4
testnet4_data_dir=$5
interval_seconds=${6:-60}

if [[ ! "$interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "interval must be a positive integer" >&2
  exit 2
fi
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
peer_metrics="$metrics_dir/peers.tsv"
tip_metrics="$metrics_dir/tips.tsv"
freezer_metrics="$metrics_dir/freezer.tsv"
persistent_metrics="$metrics_dir/persistent.tsv"
events="$metrics_dir/events.log"

if [[ ! -e "$process_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tpid\telapsed\trss_kib\tcpu_percent\n' >"$process_metrics"
fi
if [[ ! -e "$disk_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tdata_kib\tfilesystem_free_kib\n' >"$disk_metrics"
fi
if [[ ! -e "$peer_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tpid\tremote_endpoint\n' >"$peer_metrics"
fi
if [[ ! -e "$tip_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\theader_height\theader_hash\texecution_height\n' >"$tip_metrics"
fi
if [[ ! -e "$freezer_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tnext_slot\tsegments\tfirst_height\tlast_height\tbytes\n' >"$freezer_metrics"
fi
if [[ ! -e "$persistent_metrics" ]]; then
  printf 'timestamp_utc\tnetwork\tmempool_bytes\tpeer_store_bytes\n' >"$persistent_metrics"
fi

resolve_pid() {
  local source=$1
  local value
  if [[ "$source" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s\n' "$source"
    return 0
  fi
  if [[ ! -f "$source" || -L "$source" ]]; then
    return 1
  fi
  value=$(<"$source")
  if [[ ! "$value" =~ ^[[:space:]]*([1-9][0-9]*)[[:space:]]*$ ]]; then
    return 1
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

for source in "$bitcoin_pid_source" "$testnet4_pid_source"; do
  if ! resolve_pid "$source" >/dev/null; then
    echo "PID source must be a positive integer or a regular PID file: $source" >&2
    exit 2
  fi
done

bitcoin_observed_state=
testnet4_observed_state=

record_process_state() {
  local timestamp=$1
  local network=$2
  local state=$3
  local previous
  if [[ "$network" == bitcoin ]]; then
    previous=$bitcoin_observed_state
    bitcoin_observed_state=$state
  else
    previous=$testnet4_observed_state
    testnet4_observed_state=$state
  fi
  if [[ "$state" != "$previous" ]]; then
    printf '%s\t%s\tprocess_state=%s\n' "$timestamp" "$network" "$state" >>"$events"
  fi
}

sample_process() {
  local timestamp=$1
  local network=$2
  local pid_source=$3
  local pid
  if ! pid=$(resolve_pid "$pid_source"); then
    record_process_state "$timestamp" "$network" "pid-source-unavailable"
    return 1
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    record_process_state "$timestamp" "$network" "pid=$pid-exited"
    return 1
  fi
  local values
  values=$(ps -p "$pid" -o etime=,rss=,%cpu= | awk '{$1=$1; print}')
  if [[ -z "$values" ]]; then
    record_process_state "$timestamp" "$network" "pid=$pid-unavailable"
    return 1
  fi
  record_process_state "$timestamp" "$network" "pid=$pid-running"
  local elapsed rss cpu
  read -r elapsed rss cpu <<<"$values"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$timestamp" "$network" "$pid" "$elapsed" "$rss" "$cpu" >>"$process_metrics"
}

sample_peers() {
  local timestamp=$1
  local network=$2
  local pid_source=$3
  local pid endpoint
  if ! pid=$(resolve_pid "$pid_source") || ! kill -0 "$pid" 2>/dev/null; then
    return
  fi
  if ! command -v lsof >/dev/null 2>&1; then
    return
  fi
  while IFS= read -r endpoint; do
    [[ -n "$endpoint" ]] || continue
    printf '%s\t%s\t%s\t%s\n' \
      "$timestamp" "$network" "$pid" "$endpoint" >>"$peer_metrics"
  done < <(
    lsof -nP -a -p "$pid" -iTCP -sTCP:ESTABLISHED 2>/dev/null |
      awk 'NR > 1 {
        for (field = 1; field <= NF; field++) {
          if (index($field, "->") != 0) {
            endpoint=$field
            sub(/^.*->/, "", endpoint)
            print endpoint
          }
        }
      }' |
      LC_ALL=C sort -u
  )
}

sample_tip() {
  local timestamp=$1
  local network=$2
  local log_file="$output_dir/logs/$network.log"
  local header execution height hash
  [[ -f "$log_file" && ! -L "$log_file" ]] || return
  header=$(
    tail -n 2000 "$log_file" |
      sed -n 's/^peer returned no more headers at \([0-9][0-9]*\):\([0-9a-f][0-9a-f]*\)$/\1\t\2/p' |
      tail -n 1
  )
  execution=$(
    tail -n 2000 "$log_file" |
      sed -n 's/^block execution caught up at height \([0-9][0-9]*\)$/\1/p' |
      tail -n 1
  )
  [[ -n "$header" && -n "$execution" ]] || return
  IFS=$'\t' read -r height hash <<<"$header"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$timestamp" "$network" "$height" "$hash" "$execution" >>"$tip_metrics"
}

sample_freezer() {
  local timestamp=$1
  local network=$2
  local data_dir=$3
  local index="$data_dir/blocks/ledger-index.json"
  local values
  [[ -f "$index" && ! -L "$index" ]] || return
  command -v jq >/dev/null 2>&1 || return
  if ! values=$(
    jq -er '
      (.segments | length) as $count |
      [
        .next_slot,
        $count,
        (if $count == 0 then 0 else .segments[0].first_height end),
        (if $count == 0 then 0
         else (.segments[-1].first_height + .segments[-1].block_count - 1)
         end),
        ([.segments[].bytes] | add // 0)
      ] | @tsv
    ' "$index" 2>/dev/null
  ); then
    return
  fi
  printf '%s\t%s\t%s\n' "$timestamp" "$network" "$values" >>"$freezer_metrics"
}

sample_persistent_stores() {
  local timestamp=$1
  local network=$2
  local data_dir=$3
  local mempool="$data_dir/mempool.redb"
  local peers="$data_dir/peers.redb"
  local mempool_bytes=0
  local peer_bytes=0
  if [[ -f "$mempool" && ! -L "$mempool" ]]; then
    mempool_bytes=$(wc -c <"$mempool" | awk '{print $1}')
  fi
  if [[ -f "$peers" && ! -L "$peers" ]]; then
    peer_bytes=$(wc -c <"$peers" | awk '{print $1}')
  fi
  printf '%s\t%s\t%s\t%s\n' \
    "$timestamp" "$network" "$mempool_bytes" "$peer_bytes" >>"$persistent_metrics"
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
  sample_process "$timestamp" bitcoin "$bitcoin_pid_source" || true
  sample_process "$timestamp" testnet4 "$testnet4_pid_source" || true
  sample_peers "$timestamp" bitcoin "$bitcoin_pid_source"
  sample_peers "$timestamp" testnet4 "$testnet4_pid_source"
  if (( sample % 5 == 0 )); then
    sample_tip "$timestamp" bitcoin
    sample_tip "$timestamp" testnet4
    sample_freezer "$timestamp" bitcoin "$bitcoin_data_dir"
    sample_freezer "$timestamp" testnet4 "$testnet4_data_dir"
    sample_persistent_stores "$timestamp" bitcoin "$bitcoin_data_dir"
    sample_persistent_stores "$timestamp" testnet4 "$testnet4_data_dir"
  fi
  if (( sample % 60 == 0 )); then
    sample_disk "$timestamp" bitcoin "$bitcoin_data_dir"
    sample_disk "$timestamp" testnet4 "$testnet4_data_dir"
  fi
  sample=$((sample + 1))
  sleep "$interval_seconds"
done
