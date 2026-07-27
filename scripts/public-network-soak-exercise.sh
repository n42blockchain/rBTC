#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "usage: $0 SOAK_DIR NETWORK DATA_DIR graceful|abrupt [TIMEOUT_SECONDS]" >&2
  exit 2
fi

soak_dir=$1
network=$2
data_dir=$3
mode=$4
timeout_seconds=${5:-900}
minimum_age_seconds=${RBTC_SOAK_EXERCISE_MINIMUM_AGE_SECONDS:-86400}

if [[ "$network" != bitcoin && "$network" != testnet4 ]]; then
  echo "network must be bitcoin or testnet4" >&2
  exit 2
fi
if [[ "$mode" != graceful && "$mode" != abrupt ]]; then
  echo "exercise mode must be graceful or abrupt" >&2
  exit 2
fi
for value in "$timeout_seconds" "$minimum_age_seconds"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout and minimum age must be positive integers" >&2
    exit 2
  fi
done
if (( timeout_seconds > 3600 || minimum_age_seconds > 604800 )); then
  echo "timeout cannot exceed one hour and minimum age cannot exceed seven days" >&2
  exit 2
fi
for directory in "$soak_dir" "$data_dir"; do
  if [[ ! -d "$directory" || -L "$directory" ]]; then
    echo "directory must exist and must not be a symlink: $directory" >&2
    exit 1
  fi
done

state_dir="$soak_dir/state"
metrics_dir="$soak_dir/metrics"
baseline="$state_dir/baseline.txt"
binary="$state_dir/rbtcd-soak-start"
pid_file="$state_dir/$network.pid"
log_file="$soak_dir/logs/$network.log"
events="$metrics_dir/events.log"
tip_metrics="$metrics_dir/tips.tsv"
persistent_metrics="$metrics_dir/persistent.tsv"
pid_temp="$pid_file.next.$$"
trap 'rm -f "$pid_temp"' EXIT

for path in "$baseline" "$pid_file" "$log_file" "$events" "$tip_metrics" \
  "$persistent_metrics"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required soak input is missing or unsafe: $path" >&2
    exit 1
  fi
done
if [[ ! -f "$binary" || -L "$binary" || ! -x "$binary" ]]; then
  echo "immutable soak binary is missing, unsafe, or not executable" >&2
  exit 1
fi

to_epoch() {
  local timestamp=$1
  if date -u -d "$timestamp" '+%s' >/dev/null 2>&1; then
    date -u -d "$timestamp" '+%s'
  else
    date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$timestamp" '+%s'
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

persistent_sizes() {
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
  printf '%s,%s\n' "$mempool_bytes" "$peer_bytes"
}

started_utc=$(sed -n 's/^started_utc=//p' "$baseline")
expected_sha256=$(sed -n 's/^binary_sha256=//p' "$baseline")
if [[ ! "$started_utc" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] \
  || [[ ! "$expected_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid soak baseline identity" >&2
  exit 1
fi
if [[ "$(sha256_file "$binary")" != "$expected_sha256" ]]; then
  echo "immutable soak binary SHA-256 does not match the baseline" >&2
  exit 1
fi
age_seconds=$(( $(date -u '+%s') - $(to_epoch "$started_utc") ))
if (( age_seconds < minimum_age_seconds )); then
  echo "soak must run for ${minimum_age_seconds}s before a recovery exercise; age is ${age_seconds}s" >&2
  exit 1
fi

old_pid=$(<"$pid_file")
if [[ ! "$old_pid" =~ ^[[:space:]]*([1-9][0-9]*)[[:space:]]*$ ]]; then
  echo "invalid PID file: $pid_file" >&2
  exit 1
fi
old_pid=${BASH_REMATCH[1]}
if ! kill -0 "$old_pid" 2>/dev/null; then
  echo "recorded process is not running: $old_pid" >&2
  exit 1
fi
old_command=$(ps -ww -p "$old_pid" -o command=)
if [[ "$old_command" != *rbtcd* || "$old_command" != *"$data_dir"* ]]; then
  echo "refusing to signal PID $old_pid because its command does not match rbtcd and the data directory" >&2
  exit 1
fi

tip_before=$(
  awk -F '\t' -v network="$network" '
    NR > 1 && $2 == network { height=$3; hash=$4; execution=$5 }
    END { printf "%s\t%s\t%s", height, hash, execution }
  ' "$tip_metrics"
)
IFS=$'\t' read -r height_before hash_before execution_before <<<"$tip_before"
if [[ ! "$height_before" =~ ^[0-9]+$ || "$height_before" != "$execution_before" ]] \
  || [[ ! "$hash_before" =~ ^[0-9a-f]{64}$ ]]; then
  echo "latest pre-exercise header and execution evidence is absent or inconsistent" >&2
  exit 1
fi
persistent_sample_count=$(
  awk -F '\t' -v network="$network" 'NR > 1 && $2 == network { count++ } END { print count + 0 }' \
    "$persistent_metrics"
)
persistent_before=$(persistent_sizes)
IFS=, read -r mempool_before peer_store_before <<<"$persistent_before"
if (( persistent_sample_count < 1 || mempool_before < 1 || peer_store_before < 1 )); then
  echo "pre-exercise persistent mempool/peer-store evidence is absent or empty" >&2
  exit 1
fi

exercise_started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
started_epoch=$(date -u '+%s')
printf '%s\t%s\tscenario=controlled-restart mode=%s status=started old_pid=%s height=%s hash=%s persistent_bytes=%s\n' \
  "$exercise_started" "$network" "$mode" "$old_pid" "$height_before" \
  "$hash_before" "$persistent_before" >>"$events"

if [[ "$mode" == graceful ]]; then
  kill -TERM "$old_pid"
else
  kill -KILL "$old_pid"
fi
deadline=$(( $(date -u '+%s') + timeout_seconds ))
while kill -0 "$old_pid" 2>/dev/null; do
  if (( $(date -u '+%s') >= deadline )); then
    printf '%s\t%s\tscenario=controlled-restart mode=%s status=failed reason=old-process-timeout old_pid=%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$network" "$mode" "$old_pid" >>"$events"
    echo "old process did not exit within ${timeout_seconds}s" >&2
    exit 1
  fi
  sleep 1
done

first_new_log_line=$(( $(wc -l <"$log_file") + 1 ))
nohup "$binary" --network "$network" --data-dir "$data_dir" \
  >>"$log_file" 2>&1 </dev/null &
new_pid=$!
printf '%s\n' "$new_pid" >"$pid_temp"
mv -f "$pid_temp" "$pid_file"

deadline=$(( $(date -u '+%s') + timeout_seconds ))
height_after=
hash_after=
execution_after=
while true; do
  if ! kill -0 "$new_pid" 2>/dev/null; then
    printf '%s\t%s\tscenario=controlled-restart mode=%s status=failed reason=new-process-exited new_pid=%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$network" "$mode" "$new_pid" >>"$events"
    echo "replacement process exited before reaching a consistent tip" >&2
    exit 1
  fi
  new_log=$(tail -n +"$first_new_log_line" "$log_file")
  header_after=$(
    sed -n 's/^peer returned no more headers at \([0-9][0-9]*\):\([0-9a-f][0-9a-f]*\)$/\1\t\2/p' \
      <<<"$new_log" | tail -n 1
  )
  execution_after=$(
    sed -n 's/^block execution caught up at height \([0-9][0-9]*\)$/\1/p' \
      <<<"$new_log" | tail -n 1
  )
  if [[ -n "$header_after" && -n "$execution_after" ]]; then
    IFS=$'\t' read -r height_after hash_after <<<"$header_after"
    if [[ "$height_after" == "$execution_after" ]] \
      && (( height_after >= height_before )); then
      break
    fi
  fi
  if (( $(date -u '+%s') >= deadline )); then
    printf '%s\t%s\tscenario=controlled-restart mode=%s status=failed reason=tip-timeout new_pid=%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$network" "$mode" "$new_pid" >>"$events"
    echo "replacement process did not reach a consistent tip within ${timeout_seconds}s" >&2
    exit 1
  fi
  sleep 1
done

persistent_after=$(persistent_sizes)
IFS=, read -r mempool_after peer_store_after <<<"$persistent_after"
if (( mempool_after < 1 || peer_store_after < 1 )); then
  printf '%s\t%s\tscenario=controlled-restart mode=%s status=failed reason=persistent-store-empty new_pid=%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$network" "$mode" "$new_pid" >>"$events"
  echo "replacement process did not reopen non-empty persistent stores" >&2
  exit 1
fi
elapsed_seconds=$(( $(date -u '+%s') - started_epoch ))
completed_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
printf '%s\t%s\tscenario=controlled-restart mode=%s status=completed old_pid=%s new_pid=%s duration_seconds=%s height=%s hash=%s persistent_before=%s persistent_after=%s\n' \
  "$completed_utc" "$network" "$mode" "$old_pid" "$new_pid" \
  "$elapsed_seconds" "$height_after" "$hash_after" "$persistent_before" \
  "$persistent_after" >>"$events"
if [[ "$mode" == abrupt ]]; then
  printf '%s\t%s\tscenario=fault-abrupt-kill status=completed old_pid=%s new_pid=%s duration_seconds=%s height=%s hash=%s\n' \
    "$completed_utc" "$network" "$old_pid" "$new_pid" "$elapsed_seconds" \
    "$height_after" "$hash_after" >>"$events"
fi

echo "$network $mode recovery completed in ${elapsed_seconds}s at ${height_after}:${hash_after}; new PID $new_pid"
