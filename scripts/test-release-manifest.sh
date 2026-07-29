#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d "${TMPDIR:-/tmp}/rbtc-release-manifest-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

assets=(
    rbtcd-x86_64-unknown-linux-gnu
    rbtcd-aarch64-unknown-linux-gnu
    rbtcd-x86_64-apple-darwin
    rbtcd-x86_64-apple-darwin.zip
    rbtcd-x86_64-apple-darwin.notary.json
    rbtcd-aarch64-apple-darwin
    rbtcd-aarch64-apple-darwin.zip
    rbtcd-aarch64-apple-darwin.notary.json
    rbtcd-x86_64-pc-windows-msvc.exe
    rbtc.cdx.json
)
for asset in "${assets[@]}"; do
    printf 'fixture:%s\n' "$asset" >"$fixture/$asset"
done

"$repo_root/scripts/generate-release-manifest.sh" \
    "$fixture" "$fixture/RELEASE-MANIFEST.tsv" v0.0.0-test 0.0.0-test \
    0123456789abcdef0123456789abcdef01234567 \
    "rustc 1.85.0 (fixture)" 1
"$repo_root/scripts/verify-release-manifest.sh" \
    "$fixture/RELEASE-MANIFEST.tsv" "$fixture"

printf 'tampered\n' >>"$fixture/rbtc.cdx.json"
if "$repo_root/scripts/verify-release-manifest.sh" \
    "$fixture/RELEASE-MANIFEST.tsv" "$fixture" >/dev/null 2>&1; then
    echo "tampered release asset was accepted" >&2
    exit 1
fi

if "$repo_root/scripts/generate-release-manifest.sh" \
    "$fixture" "$fixture/mismatched.tsv" v9.9.9 0.0.0 \
    0123456789abcdef0123456789abcdef01234567 \
    "rustc 1.85.0 (fixture)" 1 >/dev/null 2>&1; then
    echo "release tag/package version mismatch was accepted" >&2
    exit 1
fi

echo "release manifest negative test passed"
