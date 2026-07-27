#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 DIST_DIR OUTPUT TAG COMMIT RUSTC SOURCE_DATE_EPOCH" >&2
    exit 2
}

[[ $# -eq 6 ]] || usage

dist_dir=$1
output=$2
release_tag=$3
commit=$4
rustc_version=$5
source_date_epoch=$6

[[ -d "$dist_dir" ]] || {
    echo "release directory does not exist: $dist_dir" >&2
    exit 1
}
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "commit must be a lowercase 40-character Git object id" >&2
    exit 1
}
[[ "$source_date_epoch" =~ ^[0-9]+$ ]] || {
    echo "SOURCE_DATE_EPOCH must be an unsigned integer" >&2
    exit 1
}

for value in "$release_tag" "$rustc_version"; do
    [[ "$value" != *$'\t'* && "$value" != *$'\n'* && -n "$value" ]] || {
        echo "manifest metadata must be non-empty single-line text without tabs" >&2
        exit 1
    }
done

if find "$dist_dir" -type l -print -quit | grep -q .; then
    echo "release input must not contain symbolic links" >&2
    exit 1
fi

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

declare -a expected=(
    "rbtcd-x86_64-unknown-linux-gnu|executable|x86_64-unknown-linux-gnu|sigstore"
    "rbtcd-aarch64-unknown-linux-gnu|executable|aarch64-unknown-linux-gnu|sigstore"
    "rbtcd-x86_64-apple-darwin|executable|x86_64-apple-darwin|developer-id+notarized"
    "rbtcd-x86_64-apple-darwin.zip|archive|x86_64-apple-darwin|developer-id+notarized"
    "rbtcd-x86_64-apple-darwin.notary.json|notary-log|x86_64-apple-darwin|apple-notary"
    "rbtcd-aarch64-apple-darwin|executable|aarch64-apple-darwin|developer-id+notarized"
    "rbtcd-aarch64-apple-darwin.zip|archive|aarch64-apple-darwin|developer-id+notarized"
    "rbtcd-aarch64-apple-darwin.notary.json|notary-log|aarch64-apple-darwin|apple-notary"
    "rbtcd-x86_64-pc-windows-msvc.exe|executable|x86_64-pc-windows-msvc|authenticode"
    "rbtc.cdx.json|sbom|all|cyclonedx-1.5"
)

tmp_output=$(mktemp "${TMPDIR:-/tmp}/rbtc-release-manifest.XXXXXX")
trap 'rm -f "$tmp_output"' EXIT

{
    printf 'rbtc-release-manifest-v1\n'
    printf 'tag\t%s\n' "$release_tag"
    printf 'commit\t%s\n' "$commit"
    printf 'rustc\t%s\n' "$rustc_version"
    printf 'source_date_epoch\t%s\n' "$source_date_epoch"
    printf 'data_schema\t3\n'

    for specification in "${expected[@]}"; do
        IFS='|' read -r relative kind target trust <<<"$specification"
        path="$dist_dir/$relative"
        [[ -f "$path" && ! -L "$path" ]] || {
            echo "required release asset is missing or unsafe: $relative" >&2
            exit 1
        }
        digest=$(sha256_file "$path")
        bytes=$(wc -c <"$path" | awk '{print $1}')
        printf 'file\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$digest" "$bytes" "$kind" "$target" "$trust" "$relative"
    done
} >"$tmp_output"

mkdir -p "$(dirname "$output")"
mv "$tmp_output" "$output"
trap - EXIT
