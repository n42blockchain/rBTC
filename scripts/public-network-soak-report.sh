#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 SOAK_DIR [MINIMUM_SECONDS]" >&2
  exit 2
fi

soak_dir=$1
minimum_seconds=${2:-604800}
baseline="$soak_dir/state/baseline.txt"
metrics_dir="$soak_dir/metrics"

if [[ ! "$minimum_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "minimum duration must be a positive integer" >&2
  exit 2
fi
for path in "$soak_dir" "$metrics_dir"; do
  if [[ ! -d "$path" || -L "$path" ]]; then
    echo "soak path must be an existing non-symlink directory: $path" >&2
    exit 1
  fi
done
if [[ ! -f "$baseline" || -L "$baseline" ]]; then
  echo "soak baseline is missing or unsafe" >&2
  exit 1
fi

started_utc=$(sed -n 's/^started_utc=//p' "$baseline")
commit=$(sed -n 's/^commit=//p' "$baseline")
binary_sha256=$(sed -n 's/^binary_sha256=//p' "$baseline")
baseline_binary="$soak_dir/state/rbtcd-soak-start"
[[ "$started_utc" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]
[[ "$commit" =~ ^[0-9a-f]{40}$ ]]
[[ "$binary_sha256" =~ ^[0-9a-f]{64}$ ]]
if [[ ! -f "$baseline_binary" || -L "$baseline_binary" || ! -x "$baseline_binary" ]]; then
  echo "immutable soak binary is missing, unsafe, or not executable" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  actual_binary_sha256=$(sha256sum "$baseline_binary" | awk '{print $1}')
else
  actual_binary_sha256=$(shasum -a 256 "$baseline_binary" | awk '{print $1}')
fi
if [[ "$actual_binary_sha256" != "$binary_sha256" ]]; then
  echo "immutable soak binary SHA-256 does not match the baseline" >&2
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

ended_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
started_epoch=$(to_epoch "$started_utc")
ended_epoch=$(to_epoch "$ended_utc")
duration_seconds=$((ended_epoch - started_epoch))
if (( duration_seconds < 0 )); then
  echo "soak start time is in the future" >&2
  exit 1
fi

required_files=(
  process.tsv disk.tsv peers.tsv tips.tsv freezer.tsv persistent.tsv events.log
)
for name in "${required_files[@]}"; do
  path="$metrics_dir/$name"
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "required soak evidence is missing or unsafe: $name" >&2
    exit 1
  fi
done

status=PASS
failures=()
if (( duration_seconds < minimum_seconds )); then
  status=IN_PROGRESS
  failures+=("duration ${duration_seconds}s is below ${minimum_seconds}s")
fi

printf '# rBTC public-network soak report\n\n'
printf -- '- Duration status: `%s`\n' "$status"
printf -- '- Window: `%s` through `%s` (%s seconds)\n' \
  "$started_utc" "$ended_utc" "$duration_seconds"
printf -- '- Commit: `%s`\n' "$commit"
printf -- '- Binary SHA-256: `%s`\n\n' "$binary_sha256"
printf '| Network | Samples | PIDs | Max RSS KiB | Peer endpoints | Peer groups | Tip range | Final hash | Freezer slots | Freezer bytes | Max freezer bytes | Data KiB | Min free KiB | Mempool bytes | Restart seconds |\n'
printf '| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | ---: | --- | ---: | ---: | ---: |\n'

for network in bitcoin testnet4; do
  process_values=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network {
        samples++
        pids[$3]=1
        if (($5 + 0) > max_rss) max_rss=$5 + 0
      }
      END {
        for (pid in pids) pid_count++
        printf "%d\t%d\t%d", samples, pid_count, max_rss
      }
    ' "$metrics_dir/process.tsv"
  )
  IFS=$'\t' read -r samples pid_count max_rss <<<"$process_values"

  peer_values=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network {
        endpoint=$4
        endpoints[endpoint]=1
        host=endpoint
        sub(/:[0-9]+$/, "", host)
        gsub(/^\[/, "", host)
        gsub(/\]$/, "", host)
        if (host ~ /^[0-9]+\.[0-9]+\./) {
          split(host, octets, ".")
          groups[octets[1] "." octets[2]]=1
        } else {
          split(host, hextets, ":")
          groups[hextets[1] ":" hextets[2] ":" hextets[3] ":" hextets[4]]=1
        }
      }
      END {
        for (endpoint in endpoints) endpoint_count++
        for (group in groups) group_count++
        printf "%d\t%d", endpoint_count, group_count
      }
    ' "$metrics_dir/peers.tsv"
  )
  IFS=$'\t' read -r peer_count peer_groups <<<"$peer_values"

  tip_values=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network {
        if (!seen++) {
          first_height=$3
          first_hash=$4
        }
        last_height=$3
        last_hash=$4
        last_execution=$5
      }
      END {
        printf "%s\t%s\t%s\t%s\t%s", first_height, first_hash,
          last_height, last_hash, last_execution
      }
    ' "$metrics_dir/tips.tsv"
  )
  IFS=$'\t' read -r first_height first_hash last_height last_hash last_execution \
    <<<"$tip_values"

  freezer_values=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network {
        if (!seen++) {
          first_slot=$3
          first_bytes=$7
        }
        last_slot=$3
        last_bytes=$7
        if (($7 + 0) > max_bytes) max_bytes=$7 + 0
      }
      END {
        printf "%s\t%s\t%s\t%s\t%d",
          first_slot, last_slot, first_bytes, last_bytes, max_bytes
      }
    ' "$metrics_dir/freezer.tsv"
  )
  IFS=$'\t' read -r first_slot last_slot first_freezer_bytes \
    last_freezer_bytes max_freezer_bytes <<<"$freezer_values"

  disk_values=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network {
        if (!seen++) first_data=$3
        last_data=$3
        if (min_free == "" || ($4 + 0) < min_free) min_free=$4 + 0
      }
      END { printf "%s\t%s\t%s", first_data, last_data, min_free }
    ' "$metrics_dir/disk.tsv"
  )
  IFS=$'\t' read -r first_data_kib last_data_kib min_free_kib <<<"$disk_values"

  mempool_bytes=$(
    awk -F '\t' -v network="$network" '
      NR > 1 && $2 == network { bytes=$3 }
      END { printf "%d", bytes }
    ' "$metrics_dir/persistent.tsv"
  )
  restart_seconds=$(
    awk -F '\t' -v network="$network" '
      $2 == network &&
        $3 ~ /scenario=controlled-restart/ &&
        $3 ~ /status=completed/ {
        count=split($3, fields, " ")
        for (field=1; field <= count; field++) {
          if (fields[field] ~ /^duration_seconds=[0-9]+$/) {
            sub(/^duration_seconds=/, "", fields[field])
            duration=fields[field]
          }
        }
      }
      END { printf "%s", duration }
    ' "$metrics_dir/events.log"
  )

  if [[ -z "$samples" || "$samples" == 0 ]]; then
    failures+=("$network has no process samples")
  fi
  if (( pid_count < 2 )); then
    failures+=("$network has not yet completed a process restart")
  fi
  if [[ -z "$last_height" || "$last_height" != "$last_execution" ]]; then
    failures+=("$network header/execution tips are absent or inconsistent")
  fi
  if [[ ! "$last_hash" =~ ^[0-9a-f]{64}$ ]]; then
    failures+=("$network final header hash is absent or malformed")
  fi
  if (( peer_groups < 4 )); then
    failures+=("$network observed fewer than four peer network groups")
  fi
  if [[ -z "$first_slot" || -z "$last_slot" ]]; then
    failures+=("$network has no freezer samples")
    freezer_range=unavailable
    freezer_bytes_range=unavailable
  else
    freezer_range="${first_slot}→${last_slot}"
    freezer_bytes_range="${first_freezer_bytes}→${last_freezer_bytes}"
    if (( last_slot <= first_slot )); then
      failures+=("$network has not yet demonstrated freezer rotation")
    fi
  fi
  if [[ -z "$first_height" || -z "$last_height" ]]; then
    tip_range=unavailable
  else
    tip_range="${first_height}→${last_height}"
    if (( last_height <= first_height )); then
      failures+=("$network has not yet demonstrated a natural tip advance")
    fi
  fi
  if [[ -z "$first_data_kib" || -z "$last_data_kib" || -z "$min_free_kib" ]]; then
    failures+=("$network has no disk-capacity samples")
    data_range=unavailable
    min_free_kib=unavailable
  else
    data_range="${first_data_kib}→${last_data_kib}"
  fi
  if [[ -z "$mempool_bytes" ]]; then
    failures+=("$network has no persistent-store samples")
    mempool_bytes=unavailable
  fi
  if [[ -z "$restart_seconds" ]]; then
    failures+=("$network has no measured completed restart duration")
    restart_seconds=unavailable
  fi

  printf '| %s | %s | %s | %s | %s | %s | `%s` | `%s` | `%s` | `%s` | %s | `%s` | %s | %s | %s |\n' \
    "$network" "$samples" "$pid_count" "$max_rss" "$peer_count" "$peer_groups" \
    "$tip_range" "$last_hash" "$freezer_range" "$freezer_bytes_range" \
    "$max_freezer_bytes" "$data_range" "$min_free_kib" "$mempool_bytes" \
    "$restart_seconds"
done

bitcoin_restarts=$(
  grep -c $'\tbitcoin\t.*scenario=controlled-restart.*status=completed' \
    "$metrics_dir/events.log" || true
)
testnet4_restarts=$(
  grep -c $'\ttestnet4\t.*scenario=controlled-restart.*status=completed' \
    "$metrics_dir/events.log" || true
)
fault_scenarios=$(
  grep -c 'scenario=fault-.*status=completed' "$metrics_dir/events.log" || true
)
if (( bitcoin_restarts < 1 )); then
  failures+=("bitcoin has no completed controlled-restart record")
fi
if (( testnet4_restarts < 1 )); then
  failures+=("testnet4 has no completed controlled-restart record")
fi
if (( fault_scenarios < 1 )); then
  failures+=("no recorded fault scenario has completed")
fi

printf '\n- Bitcoin controlled restart completions: `%s`\n' "$bitcoin_restarts"
printf -- '- Testnet4 controlled restart completions: `%s`\n' "$testnet4_restarts"
printf -- '- Fault scenarios completed: `%s`\n' "$fault_scenarios"

if (( ${#failures[@]} != 0 )); then
  printf '\n## Open gates\n\n'
  for failure in "${failures[@]}"; do
    printf -- '- %s\n' "$failure"
  done
  if [[ "${RBTC_SOAK_ALLOW_INCOMPLETE:-0}" != 1 ]]; then
    exit 1
  fi
fi
