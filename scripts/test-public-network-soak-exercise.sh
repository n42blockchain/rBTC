#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exercise="$repo_root/scripts/public-network-soak-exercise.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/rbtc-soak-exercise.XXXXXX")
old_pid=
new_pid=

cleanup() {
  if [[ -n "$old_pid" ]]; then
    kill -KILL "$old_pid" 2>/dev/null || true
  fi
  if [[ -n "$new_pid" ]]; then
    kill -KILL "$new_pid" 2>/dev/null || true
  fi
  rm -rf "$fixture"
}
trap cleanup EXIT

mkdir -p "$fixture/state" "$fixture/metrics" "$fixture/logs" "$fixture/data"
printf 'mempool fixture\n' >"$fixture/data/mempool.redb"
printf 'peer fixture\n' >"$fixture/data/peers.redb"
cat >"$fixture/state/rbtcd-soak-start" <<'SH'
#!/usr/bin/env bash
trap 'exit 0' TERM
log_dir=
while (( $# > 0 )); do
  if [[ "$1" == --log-dir ]]; then
    log_dir=$2
    shift 2
  else
    shift
  fi
done
if [[ -n "$log_dir" ]]; then
  mkdir -p "$log_dir"
  printf '%s\n' \
    '{"level":"info","message":"peer returned no more headers at 101:0000000000000000000000000000000000000000000000000000000000000001"}' \
    '{"level":"info","message":"block execution caught up at height 101"}' \
    >>"$log_dir/rbtc.log"
else
  echo "peer returned no more headers at 101:0000000000000000000000000000000000000000000000000000000000000001"
  echo "block execution caught up at height 101"
fi
while true; do
  sleep 1
done
SH
chmod +x "$fixture/state/rbtcd-soak-start"
if command -v sha256sum >/dev/null 2>&1; then
  binary_sha256=$(sha256sum "$fixture/state/rbtcd-soak-start" | awk '{print $1}')
else
  binary_sha256=$(shasum -a 256 "$fixture/state/rbtcd-soak-start" | awk '{print $1}')
fi
printf '%s\n' \
  'started_utc=2000-01-01T00:00:00Z' \
  'commit=0123456789abcdef0123456789abcdef01234567' \
  "binary_sha256=$binary_sha256" >"$fixture/state/baseline.txt"

printf 'timestamp_utc\tnetwork\theader_height\theader_hash\texecution_height\n' \
  >"$fixture/metrics/tips.tsv"
printf '2026-01-01T00:00:00Z\tbitcoin\t100\t%064d\t100\n' 0 \
  >>"$fixture/metrics/tips.tsv"
printf 'timestamp_utc\tnetwork\tmempool_bytes\tpeer_store_bytes\n' \
  >"$fixture/metrics/persistent.tsv"
printf '2026-01-01T00:00:00Z\tbitcoin\t4096\t8192\n' \
  >>"$fixture/metrics/persistent.tsv"
: >"$fixture/metrics/events.log"
: >"$fixture/logs/bitcoin.log"

"$fixture/state/rbtcd-soak-start" \
  --network bitcoin --data-dir "$fixture/data" \
  >>"$fixture/logs/bitcoin.log" 2>&1 &
old_pid=$!
disown "$old_pid" 2>/dev/null || true
printf '%s\n' "$old_pid" >"$fixture/state/bitcoin.pid"

RBTC_SOAK_EXERCISE_MINIMUM_AGE_SECONDS=1 \
  "$exercise" "$fixture" bitcoin "$fixture/data" graceful 10 >/dev/null
new_pid=$(<"$fixture/state/bitcoin.pid")
grep -q $'\tbitcoin\tscenario=controlled-restart mode=graceful status=completed' \
  "$fixture/metrics/events.log"
if [[ "$new_pid" == "$old_pid" ]] || ! kill -0 "$new_pid" 2>/dev/null; then
  echo "exercise did not leave a distinct replacement process running" >&2
  exit 1
fi
old_pid=$new_pid
new_pid=

kill -TERM "$old_pid"
wait "$old_pid" 2>/dev/null || true
mkdir -p "$fixture/logs/bitcoin"
: >"$fixture/logs/bitcoin/rbtc.log"
"$fixture/state/rbtcd-soak-start" \
  --network bitcoin --data-dir "$fixture/data" \
  --log-dir "$fixture/logs/bitcoin" >/dev/null 2>&1 &
old_pid=$!
disown "$old_pid" 2>/dev/null || true
printf '%s\n' "$old_pid" >"$fixture/state/bitcoin.pid"

RBTC_SOAK_EXERCISE_MINIMUM_AGE_SECONDS=1 \
  "$exercise" "$fixture" bitcoin "$fixture/data" abrupt 10 >/dev/null
new_pid=$(<"$fixture/state/bitcoin.pid")
grep -q $'\tbitcoin\tscenario=controlled-restart mode=abrupt status=completed' \
  "$fixture/metrics/events.log"
grep -q $'\tbitcoin\tscenario=fault-abrupt-kill status=completed' \
  "$fixture/metrics/events.log"
if [[ "$new_pid" == "$old_pid" ]] || ! kill -0 "$new_pid" 2>/dev/null; then
  echo "structured-log abrupt exercise did not leave a distinct replacement process running" >&2
  exit 1
fi
old_pid=

echo "public-network soak exercise tests passed"
