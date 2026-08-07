#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
runs="${RBTC_FUZZ_RUNS:-10000}"
toolchain="${RBTC_FUZZ_TOOLCHAIN:-nightly-2026-07-13}"
if [[ ! "$runs" =~ ^[0-9]+$ ]] || (( runs == 0 || runs > 10000000 )); then
    echo "RBTC_FUZZ_RUNS must be between 1 and 10,000,000" >&2
    exit 1
fi
if [[ ! "$toolchain" =~ ^nightly-[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "RBTC_FUZZ_TOOLCHAIN must be a dated nightly toolchain" >&2
    exit 1
fi

cd "$repo_root"
cargo +"$toolchain" fuzz run p2p_decode_v1 fuzz/corpus/p2p_decode_v1 -- \
    -runs="$runs" -max_len=65536 -dict=fuzz/dictionaries/p2p_v1.dict
cargo +"$toolchain" fuzz run p2p_v2_handshake fuzz/corpus/p2p_v2_handshake -- \
    -runs="$runs" -max_len=65536
cargo +"$toolchain" fuzz run merkle_proof fuzz/corpus/merkle_proof -- \
    -runs="$runs" -max_len=1124
cargo +"$toolchain" fuzz run signet_block fuzz/corpus/signet_block -- \
    -runs="$runs" -max_len=131072
cargo +"$toolchain" fuzz run archive_decode fuzz/corpus/archive_decode -- \
    -runs="$runs" -max_len=1048576 -dict=fuzz/dictionaries/container.dict
cargo +"$toolchain" fuzz run snapshot_decode fuzz/corpus/snapshot_decode -- \
    -runs="$runs" -max_len=1048576 -dict=fuzz/dictionaries/container.dict
cargo +"$toolchain" fuzz run explorer_request fuzz/corpus/explorer_request -- \
    -runs="$runs" -max_len=4096
cargo +"$toolchain" fuzz run local_rpc fuzz/corpus/local_rpc -- \
    -runs="$runs" -max_len=65537
cargo +"$toolchain" fuzz run config_parsers fuzz/corpus/config_parsers -- \
    -runs="$runs" -max_len=4096
cargo +"$toolchain" fuzz run wallet_auth fuzz/corpus/wallet_auth -- \
    -runs="$runs" -max_len=4096
cargo +"$toolchain" fuzz run wallet_descriptor fuzz/corpus/wallet_descriptor -- \
    -runs="$runs" -max_len=131072
cargo +"$toolchain" fuzz run wallet_psbt fuzz/corpus/wallet_psbt -- \
    -runs="$runs" -max_len=786433
cargo +"$toolchain" fuzz run persisted_metadata fuzz/corpus/persisted_metadata -- \
    -runs="$runs" -max_len=8193
