#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 RBTC_BINARY EXPECTED_VERSION" >&2
    exit 2
fi

binary=$1
expected_version=$2

if [[ ! "$expected_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
    echo "expected version is not valid semantic version text: $expected_version" >&2
    exit 2
fi
if [[ ! -f "$binary" || -L "$binary" || ! -x "$binary" ]]; then
    echo "release binary is missing, unsafe, or not executable: $binary" >&2
    exit 1
fi

actual_version=$("$binary" --version)
if [[ "$actual_version" != "rbtcd $expected_version" ]]; then
    echo "release binary version mismatch: expected 'rbtcd $expected_version', got '$actual_version'" >&2
    exit 1
fi

"$binary" --help >/dev/null
echo "release binary verified: $actual_version"
