#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/rbtc-soak-monitor-test.XXXXXX")
monitor_pid=

cleanup() {
  if [[ -n "$monitor_pid" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
  rm -rf -- "$test_root"
}
trap cleanup EXIT

soak_dir="$test_root/soak"
bitcoin_dir="$test_root/bitcoin"
testnet4_dir="$test_root/testnet4"
mkdir -p "$soak_dir/logs/bitcoin" "$soak_dir/logs/testnet4" \
  "$bitcoin_dir" "$testnet4_dir"
: >"$soak_dir/logs/bitcoin.log"
: >"$soak_dir/logs/testnet4.log"
printf '%s\n%s\n' \
  '{"message":"peer returned no more headers at 42:0000000000000000000000000000000000000000000000000000000000000001"}' \
  '{"message":"block execution caught up at height 42"}' \
  >"$soak_dir/logs/bitcoin/rbtc.log"
printf '%s\n%s\n' \
  '{"message":"peer returned no more headers at 84:0000000000000000000000000000000000000000000000000000000000000002"}' \
  '{"message":"block execution caught up at height 84"}' \
  >"$soak_dir/logs/testnet4/rbtc.log"
printf '%s\n' "$$" >"$test_root/bitcoin.pid"
printf '%s\n' "$$" >"$test_root/testnet4.pid"

"$repo_root/scripts/public-network-soak-monitor.sh" \
  "$soak_dir" \
  "$test_root/bitcoin.pid" "$bitcoin_dir" \
  "$test_root/testnet4.pid" "$testnet4_dir" \
  1 &
monitor_pid=$!

sleep 2
if ! kill -0 "$monitor_pid" 2>/dev/null; then
  wait "$monitor_pid"
  echo "monitor exited while optional tip/freezer evidence was unavailable" >&2
  exit 1
fi
kill -TERM "$monitor_pid"
wait "$monitor_pid" 2>/dev/null || true
monitor_pid=

for evidence in process.tsv disk.tsv peers.tsv tips.tsv freezer.tsv persistent.tsv events.log; do
  if [[ ! -f "$soak_dir/metrics/$evidence" ]]; then
    echo "monitor did not initialize $evidence" >&2
    exit 1
  fi
done
if [[ $(wc -l <"$soak_dir/metrics/process.tsv") -lt 3 ]]; then
  echo "monitor did not sample both live PID sources" >&2
  exit 1
fi
if ! grep -Fq $'\tbitcoin\t42\t' "$soak_dir/metrics/tips.tsv" \
  || ! grep -Fq $'\ttestnet4\t84\t' "$soak_dir/metrics/tips.tsv"; then
  echo "monitor did not parse structured rotating-log tips" >&2
  exit 1
fi

echo "public-network soak monitor tests passed"
