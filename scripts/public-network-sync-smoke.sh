#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
network="${RBTC_SYNC_NETWORK:-signet}"
case "$network" in
    signet)
        default_max_bytes=1073741824
        default_timeout_seconds=600
        default_target_height=1000
        default_target_hash=0000010ebfa3c6193793701c198392e21bdb8bc9fb2032f0d74a628d36e9a75e
        default_batch_size=1
        default_restart_height=0
        ;;
    bitcoin)
        default_max_bytes=2147483648
        default_timeout_seconds=1800
        default_target_height=11111
        default_target_hash=0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d
        default_batch_size=16
        default_restart_height=1000
        ;;
    *)
        echo "RBTC_SYNC_NETWORK must be signet or bitcoin" >&2
        exit 1
        ;;
esac

max_bytes="${RBTC_SYNC_MAX_BYTES:-$default_max_bytes}"
timeout_seconds="${RBTC_SYNC_TIMEOUT_SECONDS:-$default_timeout_seconds}"
reserve_bytes="${RBTC_SYNC_FREE_RESERVE_BYTES:-2147483648}"
batch_size="${RBTC_SYNC_BATCH_SIZE:-$default_batch_size}"
restart_height="${RBTC_SYNC_RESTART_HEIGHT:-$default_restart_height}"
if [[ "${RBTC_SYNC_TARGET_HEIGHT+x}" != "${RBTC_SYNC_TARGET_HASH+x}" ]]; then
    echo "RBTC_SYNC_TARGET_HEIGHT and RBTC_SYNC_TARGET_HASH must be supplied together" >&2
    exit 1
fi
target_height="${RBTC_SYNC_TARGET_HEIGHT:-$default_target_height}"
target_hash="${RBTC_SYNC_TARGET_HASH:-$default_target_hash}"

for value in "$max_bytes" "$timeout_seconds" "$reserve_bytes" "$target_height" "$batch_size"; do
    if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value == 0 )); then
        echo "sync resource limits and target height must be positive integers" >&2
        exit 1
    fi
done
if (( max_bytes > 1099511627776 || reserve_bytes > 1099511627776 )); then
    echo "sync data ceiling and free-space reserve cannot exceed 1 TiB each" >&2
    exit 1
fi
if (( timeout_seconds > 86400 )); then
    echo "sync timeout cannot exceed 86,400 seconds" >&2
    exit 1
fi
if (( target_height > 10000000 )); then
    echo "sync target height cannot exceed 10,000,000" >&2
    exit 1
fi
if (( batch_size > 16 )); then
    echo "sync batch size cannot exceed 16 blocks" >&2
    exit 1
fi
if [[ ! "$restart_height" =~ ^[0-9]+$ ]] || (( restart_height >= target_height )); then
    echo "sync restart height must be zero (disabled) or below the target height" >&2
    exit 1
fi
if [[ ! "$target_hash" =~ ^[0-9a-f]{64}$ ]]; then
    echo "sync target hash must be 64 lowercase hexadecimal characters" >&2
    exit 1
fi

available_bytes="$(df -Pk "$repo_root" | awk 'NR == 2 { print $4 * 1024 }')"
required_bytes=$((max_bytes + reserve_bytes))
if (( available_bytes < required_bytes )); then
    echo "sync smoke test requires ${required_bytes} free bytes; found ${available_bytes}" >&2
    exit 1
fi

run_root="$(mktemp -d "${TMPDIR:-/tmp}/rbtc-${network}-smoke.XXXXXX")"
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
execution_options=()
if [[ "$network" == "bitcoin" ]]; then
    execution_options+=(--experimental-network-execution)
fi

launch_node() {
    "$repo_root/target/release/rbtcd" \
        --network "$network" \
        --data-dir "$data_dir" \
        "${execution_options[@]}" \
        --validate-until-height "$target_height" \
        --validate-until-blockhash "$target_hash" \
        --validation-batch-size "$batch_size" \
        --once >>"$log_file" 2>&1 &
    child_pid=$!
}

launch_node
started_at="$(date +%s)"
restarted=0

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
    if (( restart_height > 0 && restarted == 0 )) \
        && grep -Fq "validated and executed block ${restart_height}:" "$log_file"; then
        echo "interrupting sync after observing block $restart_height to verify durable restart"
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
        child_pid=""
        restarted=1
        launch_node
        continue
    fi
    sleep 1
done

if ! wait "$child_pid"; then
    child_pid=""
    tail -n 80 "$log_file" >&2
    exit 1
fi
child_pid=""

expected="independent genesis validation stopped exactly at $target_height:$target_hash"
if ! grep -Fq "$expected" "$log_file"; then
    echo "sync smoke test exited without reaching the authenticated target" >&2
    tail -n 80 "$log_file" >&2
    exit 1
fi
if (( restart_height > 0 && restarted == 0 )); then
    echo "sync smoke test reached its target without exercising the requested restart" >&2
    exit 1
fi

used_bytes=$(( $(du -sk "$run_root" | awk '{ print $1 }') * 1024 ))
elapsed=$(( $(date +%s) - started_at ))
echo "$expected"
if (( restarted > 0 )); then
    echo "sync smoke test resumed durable state after observing block $restart_height"
fi
echo "sync smoke test used ${used_bytes} bytes in ${elapsed} seconds"
