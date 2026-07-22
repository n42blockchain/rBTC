#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
max_bytes="${RBTC_SYNC_MAX_BYTES:-1073741824}"
timeout_seconds="${RBTC_SYNC_TIMEOUT_SECONDS:-600}"
reserve_bytes="${RBTC_SYNC_FREE_RESERVE_BYTES:-2147483648}"
target_hash="00000086d6b2636cb2a392d45edc4ec544a10024d30141c9adf4bfd9de533b53"

for value in "$max_bytes" "$timeout_seconds" "$reserve_bytes"; do
    if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value == 0 )); then
        echo "sync resource limits must be positive integers" >&2
        exit 1
    fi
done

available_bytes="$(df -Pk "$repo_root" | awk 'NR == 2 { print $4 * 1024 }')"
required_bytes=$((max_bytes + reserve_bytes))
if (( available_bytes < required_bytes )); then
    echo "sync smoke test requires ${required_bytes} free bytes; found ${available_bytes}" >&2
    exit 1
fi

run_root="$(mktemp -d "${TMPDIR:-/tmp}/rbtc-signet-smoke.XXXXXX")"
data_dir="$run_root/data"
log_file="$run_root/rbtcd.log"
mkdir -p "$data_dir"
child_pid=""

cleanup() {
    if [[ -n "$child_pid" ]] && kill -0 "$child_pid" 2>/dev/null; then
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
    fi
    if [[ "${RBTC_KEEP_SYNC_DATA:-0}" == "1" ]]; then
        echo "kept sync smoke data at $run_root"
    else
        rm -rf -- "$run_root"
    fi
}
handle_signal() {
    cleanup
    trap - EXIT
    exit 130
}
trap cleanup EXIT
trap handle_signal INT TERM

cargo build --manifest-path "$repo_root/Cargo.toml" --locked --release
"$repo_root/target/release/rbtcd" \
    --network signet \
    --data-dir "$data_dir" \
    --validate-until-height 1 \
    --validate-until-blockhash "$target_hash" \
    --validation-batch-size 1 \
    --once >"$log_file" 2>&1 &
child_pid=$!
started_at="$(date +%s)"

while kill -0 "$child_pid" 2>/dev/null; do
    used_bytes=$(( $(du -sk "$run_root" | awk '{ print $1 }') * 1024 ))
    elapsed=$(( $(date +%s) - started_at ))
    if (( used_bytes > max_bytes )); then
        echo "sync smoke test exceeded ${max_bytes} bytes" >&2
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
        child_pid=""
        tail -n 40 "$log_file" >&2
        exit 1
    fi
    if (( elapsed > timeout_seconds )); then
        echo "sync smoke test exceeded ${timeout_seconds} seconds" >&2
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
        child_pid=""
        tail -n 40 "$log_file" >&2
        exit 1
    fi
    sleep 1
done

if ! wait "$child_pid"; then
    child_pid=""
    tail -n 80 "$log_file" >&2
    exit 1
fi
child_pid=""

expected="independent genesis validation stopped exactly at 1:$target_hash"
if ! grep -Fq "$expected" "$log_file"; then
    echo "sync smoke test exited without reaching the authenticated target" >&2
    tail -n 80 "$log_file" >&2
    exit 1
fi

used_bytes=$(( $(du -sk "$run_root" | awk '{ print $1 }') * 1024 ))
echo "$expected"
echo "sync smoke test used ${used_bytes} bytes"
