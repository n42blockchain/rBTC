#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
minimum_free_bytes="${RBTC_RELEASE_MIN_FREE_BYTES:-6442450944}"
available_bytes="$(df -Pk "$repo_root" | awk 'NR == 2 { print $4 * 1024 }')"

if (( available_bytes < minimum_free_bytes )); then
    echo "reproducible build requires at least ${minimum_free_bytes} free bytes; found ${available_bytes}" >&2
    exit 1
fi

build_root="$(mktemp -d "${TMPDIR:-/tmp}/rbtc-repro.XXXXXX")"
cleanup() {
    rm -rf -- "$build_root"
}
trap cleanup EXIT

source_date_epoch="${SOURCE_DATE_EPOCH:-$(git -C "$repo_root" log -1 --pretty=%ct)}"
export CARGO_INCREMENTAL=0
export SOURCE_DATE_EPOCH="$source_date_epoch"
export RUSTFLAGS="-C strip=symbols -C debuginfo=0"

cargo build --manifest-path "$repo_root/Cargo.toml" --locked --release --all-features --target-dir "$build_root/first"
cargo build --manifest-path "$repo_root/Cargo.toml" --locked --release --all-features --target-dir "$build_root/second"

first="$build_root/first/release/rbtcd"
second="$build_root/second/release/rbtcd"
if ! cmp -s "$first" "$second"; then
    echo "release binaries differ across clean builds" >&2
    exit 1
fi

mkdir -p "$repo_root/target/release"
cp "$first" "$repo_root/target/release/rbtcd"
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$first"
else
    shasum -a 256 "$first"
fi
