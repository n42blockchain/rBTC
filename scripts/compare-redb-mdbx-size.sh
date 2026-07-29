#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  cat >&2 <<'EOF'
usage: compare-redb-mdbx-size.sh REDB_PATH MDBX_PATH [REDB_TARGET_BYTES] [OUTPUT_JSON]

  REDB_PATH: source redb file (e.g., chainstate.redb)
  MDBX_PATH: target mdbx directory
  REDB_TARGET_BYTES: target size budget in bytes (default 10,000,000,000)
  OUTPUT_JSON: output summary path (default /tmp/mdbx_redb_compare.json)
EOF
  exit 2
fi

redb_path=$1
mdbx_path=$2
redb_target_bytes=${3:-10000000000}
output_json=${4:-/tmp/mdbx_redb_compare.json}

if [[ ! -f "$redb_path" || -L "$redb_path" ]]; then
  echo "missing redb path: $redb_path" >&2
  exit 1
fi
if [[ ! -d "$mdbx_path" || -L "$mdbx_path" ]]; then
  echo "missing mdbx directory: $mdbx_path" >&2
  exit 1
fi
if ! [[ "$redb_target_bytes" =~ ^[0-9]+$ ]]; then
  echo "target bytes must be an integer" >&2
  exit 1
fi

redb_bytes=$(stat -f '%z' "$redb_path" 2>/dev/null || stat -c '%s' "$redb_path")
mdbx_bytes=$(du -sb "$mdbx_path" | awk '{print $1}')

if (( mdbx_bytes <= 0 )); then
  echo "mdbx path is empty: $mdbx_path" >&2
  exit 1
fi

ratio_times_10000=$(( mdbx_bytes * 10000 / redb_bytes ))
ratio_percent=$(awk -v x="$ratio_times_10000" 'BEGIN { printf "%.2f", x / 100.0 }')
delta_bytes=$(( mdbx_bytes - redb_bytes ))
margin_bytes=$(( redb_target_bytes - mdbx_bytes ))
status="pass"
if (( mdbx_bytes > redb_target_bytes )); then
  status="fail"
fi

cat >"$output_json" <<JSON
{
  "redb_path": "$redb_path",
  "mdbx_path": "$mdbx_path",
  "redb_bytes": $redb_bytes,
  "mdbx_bytes": $mdbx_bytes,
  "delta_bytes": $delta_bytes,
  "target_bytes": $redb_target_bytes,
  "margin_bytes": $margin_bytes,
  "size_ratio_bps": $ratio_times_10000,
  "size_ratio_percent": $ratio_percent,
  "status": "$status",
  "notes": [
    "negative margin_bytes means over target",
    "use this for static upper-bound sizing policy",
    "mdbx directory includes all files inside the selected path"
  ]
}
JSON

jq . "$output_json"
