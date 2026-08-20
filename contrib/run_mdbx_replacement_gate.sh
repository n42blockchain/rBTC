#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 OUTPUT_DIRECTORY" >&2
    exit 64
fi

output=$1
if [ -e "$output" ]; then
    echo "refusing existing output directory: $output" >&2
    exit 73
fi
mkdir -p "$output"

for tool in cargo git jq uname df shasum /usr/bin/time; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "required tool not found: $tool" >&2
        exit 69
    fi
done

git rev-parse HEAD >"$output/revision.txt"
git status --porcelain --untracked-files=no >"$output/worktree.txt"
uname -a >"$output/uname.txt"
df -k "$output" >"$output/filesystem-before.txt"
cargo test --release --all-features --test mdbx_mainnet_scale_gate --no-run \
    >"$output/build.log" 2>&1

run_lane() {
    batch=$1
    lane=$output/batch-$batch
    mkdir -p "$lane"
    report=$lane/report.json
    timing=$lane/time.txt
    log=$lane/test.log

    if [ "$(uname -s)" = "Darwin" ]; then
        /usr/bin/time -l env \
            RBTC_MDBX_GATE_DIR="$lane/chainstate.mdbx" \
            RBTC_MDBX_GATE_REPORT="$report" \
            RBTC_MDBX_GATE_COMMIT_BATCH="$batch" \
            cargo test --release --all-features \
                --test mdbx_mainnet_scale_gate -- --ignored --nocapture \
                >"$log" 2>"$timing"
        peak_rss_bytes=$(awk '/maximum resident set size/ {value=$1} END {print value+0}' "$timing")
    else
        /usr/bin/time -v env \
            RBTC_MDBX_GATE_DIR="$lane/chainstate.mdbx" \
            RBTC_MDBX_GATE_REPORT="$report" \
            RBTC_MDBX_GATE_COMMIT_BATCH="$batch" \
            cargo test --release --all-features \
                --test mdbx_mainnet_scale_gate -- --ignored --nocapture \
                >"$log" 2>"$timing"
        peak_rss_kib=$(awk -F: '/Maximum resident set size/ {value=$2} END {gsub(/ /, "", value); print value+0}' "$timing")
        peak_rss_bytes=$((peak_rss_kib * 1024))
    fi
    jq --argjson peak_rss_bytes "$peak_rss_bytes" \
        '. + {observed_peak_rss_bytes: $peak_rss_bytes}' \
        "$report" >"$lane/report-with-rss.json"
}

run_lane 64
run_lane 256

jq -s '{schema: 1, lanes: .}' \
    "$output/batch-64/report-with-rss.json" \
    "$output/batch-256/report-with-rss.json" >"$output/matrix.json"
df -k "$output" >"$output/filesystem-after.txt"
shasum -a 256 "$output/matrix.json" >"$output/SHA256SUMS"
echo "$output/matrix.json"
