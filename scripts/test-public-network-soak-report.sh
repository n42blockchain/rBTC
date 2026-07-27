#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report="$repo_root/scripts/public-network-soak-report.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/rbtc-soak-report.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/state" "$fixture/metrics"
: >"$fixture/state/rbtcd-soak-start"
chmod +x "$fixture/state/rbtcd-soak-start"
printf '%s\n' \
  'started_utc=2000-01-01T00:00:00Z' \
  'commit=0123456789abcdef0123456789abcdef01234567' \
  'binary_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' \
  >"$fixture/state/baseline.txt"

printf 'timestamp_utc\tnetwork\tpid\telapsed\trss_kib\tcpu_percent\n' \
  >"$fixture/metrics/process.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t101\t1:00\t1000\t1.0\n' "$network"
  printf '2026-01-01T01:00:00Z\t%s\t202\t2:00\t2000\t2.0\n' "$network"
done >>"$fixture/metrics/process.tsv"

printf 'timestamp_utc\tnetwork\tdata_kib\tfilesystem_free_kib\n' \
  >"$fixture/metrics/disk.tsv"
printf 'timestamp_utc\tnetwork\tpid\tremote_endpoint\n' \
  >"$fixture/metrics/peers.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t101\t10.1.0.1:8333\n' "$network"
  printf '2026-01-01T00:00:00Z\t%s\t101\t10.2.0.1:8333\n' "$network"
  printf '2026-01-01T00:00:00Z\t%s\t101\t10.3.0.1:8333\n' "$network"
  printf '2026-01-01T00:00:00Z\t%s\t101\t10.4.0.1:8333\n' "$network"
done >>"$fixture/metrics/peers.tsv"

printf 'timestamp_utc\tnetwork\theader_height\theader_hash\texecution_height\n' \
  >"$fixture/metrics/tips.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t100\t%064d\t100\n' "$network" 0
  printf '2026-01-01T01:00:00Z\t%s\t101\t%064d\t101\n' "$network" 1
done >>"$fixture/metrics/tips.tsv"

printf 'timestamp_utc\tnetwork\tnext_slot\tsegments\tfirst_height\tlast_height\tbytes\n' \
  >"$fixture/metrics/freezer.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t10\t3\t1\t99\t1000\n' "$network"
  printf '2026-01-01T01:00:00Z\t%s\t11\t3\t2\t100\t2000\n' "$network"
done >>"$fixture/metrics/freezer.tsv"

printf 'timestamp_utc\tnetwork\tmempool_bytes\tpeer_store_bytes\n' \
  >"$fixture/metrics/persistent.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t4096\t8192\n' "$network"
done >>"$fixture/metrics/persistent.tsv"

printf '%s\n' \
  $'2026-01-01T01:00:00Z\tbitcoin\tscenario=controlled-restart status=completed' \
  $'2026-01-01T01:00:00Z\ttestnet4\tscenario=controlled-restart status=completed' \
  $'2026-01-01T01:00:00Z\ttestnet4\tscenario=fault-abrupt-kill status=completed' \
  >"$fixture/metrics/events.log"

"$report" "$fixture" 1 >"$fixture/report.md"
grep -q -- '- Duration status: `PASS`' "$fixture/report.md"
grep -q -- '- Fault scenarios completed: `1`' "$fixture/report.md"

printf 'tampered\n' >"$fixture/state/rbtcd-soak-start"
if "$report" "$fixture" 1 >/dev/null 2>&1; then
  echo "soak report accepted a modified baseline binary" >&2
  exit 1
fi
: >"$fixture/state/rbtcd-soak-start"

printf 'timestamp_utc\tnetwork\theader_height\theader_hash\texecution_height\n' \
  >"$fixture/metrics/tips.tsv"
for network in bitcoin testnet4; do
  printf '2026-01-01T00:00:00Z\t%s\t100\t%064d\t100\n' "$network" 0
  printf '2026-01-01T01:00:00Z\t%s\t101\t%064d\t99\n' "$network" 1
done >>"$fixture/metrics/tips.tsv"
if "$report" "$fixture" 1 >/dev/null 2>&1; then
  echo "soak report accepted an inconsistent execution tip" >&2
  exit 1
fi

echo "public-network soak report tests passed"
