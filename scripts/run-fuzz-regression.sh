#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
runs="${RBTC_FUZZ_RUNS:-10000}"
if [[ ! "$runs" =~ ^[0-9]+$ ]] || (( runs == 0 || runs > 10000000 )); then
    echo "RBTC_FUZZ_RUNS must be between 1 and 10,000,000" >&2
    exit 1
fi

cd "$repo_root"
cargo +nightly fuzz run p2p_decode_v1 fuzz/corpus/p2p_decode_v1 -- \
    -runs="$runs" -max_len=65536 -dict=fuzz/dictionaries/p2p_v1.dict
cargo +nightly fuzz run merkle_proof fuzz/corpus/merkle_proof -- \
    -runs="$runs" -max_len=1124
cargo +nightly fuzz run signet_block fuzz/corpus/signet_block -- \
    -runs="$runs" -max_len=131072
