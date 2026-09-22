#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || -e "$1" ]]; then
  echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
  exit 2
fi
if [[ "$(uname -s)" != Linux || ! -r /proc/self/status ]]; then
  echo "canonical resource acceptance requires a Linux host with /proc RSS" >&2
  exit 2
fi
for name in RBTC_CORE_LINEARIZE RBTC_CORE_POSTLINEARIZE; do
  if [[ -z "${!name:-}" || ! -x "${!name}" ]]; then
    echo "$name must name an executable pinned Core 31 oracle" >&2
    exit 2
  fi
done

output=$1
commit=$(git rev-parse HEAD)
source_digest=$(python3 scripts/verify-release-readiness.py --print-source-digest)
working_tree_status=$(git status --porcelain=v1 --untracked-files=all -- . \
  ':(exclude)release/acceptance/**' ':(exclude)docs/**' ':(exclude)README.md')
if [[ -n "$working_tree_status" ]]; then
  echo "release-relevant working tree differs from HEAD" >&2
  git status --short --untracked-files=all -- . \
    ':(exclude)release/acceptance/**' ':(exclude)docs/**' ':(exclude)README.md' >&2
  exit 2
fi
mkdir -p "$output"
output=$(cd "$output" && pwd -P)

run() {
  local name=$1
  shift
  printf '%q ' "$@" >"$output/$name.command"
  printf '\n' >>"$output/$name.command"
  "$@" >"$output/$name.log" 2>&1
  printf '0\n' >"$output/$name.exit"
}

run admission-tests cargo test --locked --all-features --lib \
  transaction_admission::tests -- --nocapture
run admission-node-tests cargo test --locked --all-features --lib \
  node::tests::admission_resources -- --nocapture
mkdir "$output/oracle-linearize" "$output/oracle-postlinearize"
run optimizer-core env RBTC_CORE_LINEARIZE_REPORT_DIR="$output/oracle-linearize" \
  cargo test --locked --release --all-features --lib \
  feerate_diagram::optimizer::tests::full_optimizer_matches_core_optimal_diagrams_and_orders \
  -- --ignored --exact --nocapture
run postlinearize-core env RBTC_CORE_POSTLINEARIZE_REPORT_DIR="$output/oracle-postlinearize" \
  cargo test --locked --release --all-features --lib \
  feerate_diagram::refinement_tests::core_31_postlinearization_matches_generated_orders_exactly \
  -- --ignored --exact --nocapture

: >"$output/admission-probe.jsonl"
for attempt in 1 2 3; do
  cargo run --locked --release --all-features --example admission_resource_probe -- 256 8 \
    >"$output/admission-probe-$attempt.json" 2>"$output/admission-probe-$attempt.stderr"
  cat "$output/admission-probe-$attempt.json" >>"$output/admission-probe.jsonl"
done
python3 scripts/check-admission-resource-probe.py "$output/admission-probe.jsonl" \
  >"$output/admission-check.json"

run header-tests cargo test --locked --all-features --lib \
  node::tests::header_resync -- --nocapture
run header-store-tests cargo test --locked --all-features --lib \
  header_store::tests::retention -- --nocapture
RBTC_HEADER_PROBE_SECONDS=3600 \
  cargo run --locked --release --all-features --example header_resource_probe -- \
  retained 2500 1000000 >"$output/header-probe.jsonl" 2>"$output/header-probe.stderr"
python3 scripts/check-header-resource-probe.py "$output/header-probe.jsonl" \
  --minimum-seconds 3600 --max-rss-mib 512 --max-disk-mib 256 \
  >"$output/header-check.json"

cat >"$output/admission-resources.md" <<EOF
# Default-redb admission resource acceptance
- Gate: \`admission-resources\`
- Commit: \`$commit\`
- Source SHA-256: \`$source_digest\`
- Functional test status: \`PASS\`
- Optimizer differential status: \`PASS\`
- Resource probe status: \`PASS\`
- Recovery/fault status: \`PASS\`
- Acceptance status: \`PASS\`
EOF

cat >"$output/header-resources.md" <<EOF
# Default-redb header resource acceptance
- Gate: \`header-resources\`
- Commit: \`$commit\`
- Source SHA-256: \`$source_digest\`
- Functional test status: \`PASS\`
- Semantic comparison status: \`PASS\`
- Sustained probe status: \`PASS\`
- Restart/fault status: \`PASS\`
- Acceptance status: \`PASS\`
EOF

(cd "$output" && find . -type f ! -name SHA256SUMS -print0 | sort -z | \
  xargs -0 sha256sum >SHA256SUMS)
echo "resource acceptance passed for $commit; review $output before committing reports"
