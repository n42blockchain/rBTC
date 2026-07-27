#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 MANIFEST [ASSET_DIR]" >&2
    exit 2
}

[[ $# -ge 1 && $# -le 2 ]] || usage

manifest=$1
asset_dir=${2:-$(dirname "$manifest")}

[[ -f "$manifest" && ! -L "$manifest" ]] || {
    echo "manifest is missing or is not a regular file: $manifest" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

expected_metadata=(
    "rbtc-release-manifest-v1"
    $'tag\t'
    $'commit\t'
    $'rustc\t'
    $'source_date_epoch\t'
    $'data_schema\t3'
)

lines=()
while IFS= read -r line || [[ -n "$line" ]]; do
    lines[${#lines[@]}]=$line
done <"$manifest"
[[ ${#lines[@]} -eq 16 ]] || {
    echo "manifest must contain exactly 6 metadata and 10 file records" >&2
    exit 1
}
[[ "${lines[0]}" == "${expected_metadata[0]}" ]] || {
    echo "unsupported release manifest format" >&2
    exit 1
}
for index in 1 2 3 4; do
    [[ "${lines[$index]}" == "${expected_metadata[$index]}"* ]] || {
        echo "invalid metadata record at line $((index + 1))" >&2
        exit 1
    }
done
[[ "${lines[5]}" == "${expected_metadata[5]}" ]] || {
    echo "unsupported data schema" >&2
    exit 1
}

IFS=$'\t' read -r tag_key tag tag_extra <<<"${lines[1]}"
IFS=$'\t' read -r commit_key commit commit_extra <<<"${lines[2]}"
IFS=$'\t' read -r rustc_key rustc_version rustc_extra <<<"${lines[3]}"
IFS=$'\t' read -r epoch_key source_date_epoch epoch_extra <<<"${lines[4]}"
[[ "$tag_key" == tag && "$commit_key" == commit && "$rustc_key" == rustc \
    && "$epoch_key" == source_date_epoch && -z "${tag_extra:-}" \
    && -z "${commit_extra:-}" && -z "${rustc_extra:-}" \
    && -z "${epoch_extra:-}" ]] || {
    echo "manifest metadata records must contain exactly two fields" >&2
    exit 1
}
[[ -n "$tag" && -n "$rustc_version" ]] || {
    echo "release metadata must not be empty" >&2
    exit 1
}
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "invalid manifest commit" >&2
    exit 1
}
[[ "$source_date_epoch" =~ ^[0-9]+$ ]] || {
    echo "invalid manifest SOURCE_DATE_EPOCH" >&2
    exit 1
}

expected_paths=(
    "rbtcd-x86_64-unknown-linux-gnu"
    "rbtcd-aarch64-unknown-linux-gnu"
    "rbtcd-x86_64-apple-darwin"
    "rbtcd-x86_64-apple-darwin.zip"
    "rbtcd-x86_64-apple-darwin.notary.json"
    "rbtcd-aarch64-apple-darwin"
    "rbtcd-aarch64-apple-darwin.zip"
    "rbtcd-aarch64-apple-darwin.notary.json"
    "rbtcd-x86_64-pc-windows-msvc.exe"
    "rbtc.cdx.json"
)

for offset in "${!expected_paths[@]}"; do
    line=${lines[$((offset + 6))]}
    IFS=$'\t' read -r record digest bytes kind target trust relative extra <<<"$line"
    [[ "$record" == "file" && -z "${extra:-}" ]] || {
        echo "invalid file record at line $((offset + 7))" >&2
        exit 1
    }
    [[ "$digest" =~ ^[0-9a-f]{64}$ && "$bytes" =~ ^[0-9]+$ ]] || {
        echo "invalid digest or size for $relative" >&2
        exit 1
    }
    [[ "$relative" == "${expected_paths[$offset]}" ]] || {
        echo "unexpected or unsorted release asset: $relative" >&2
        exit 1
    }
    [[ "$relative" != */* && "$relative" != "." && "$relative" != ".." ]] || {
        echo "unsafe release asset path: $relative" >&2
        exit 1
    }
    path="$asset_dir/$relative"
    [[ -f "$path" && ! -L "$path" ]] || {
        echo "release asset is missing or unsafe: $relative" >&2
        exit 1
    }
    actual_bytes=$(wc -c <"$path" | awk '{print $1}')
    actual_digest=$(sha256_file "$path")
    [[ "$actual_bytes" == "$bytes" && "$actual_digest" == "$digest" ]] || {
        echo "release asset does not match manifest: $relative" >&2
        exit 1
    }

    case "$relative" in
        rbtcd-x86_64-unknown-linux-gnu)
            [[ "$kind|$target|$trust" == \
                "executable|x86_64-unknown-linux-gnu|sigstore" ]]
            ;;
        rbtcd-aarch64-unknown-linux-gnu)
            [[ "$kind|$target|$trust" == \
                "executable|aarch64-unknown-linux-gnu|sigstore" ]]
            ;;
        rbtcd-x86_64-apple-darwin)
            [[ "$kind|$target|$trust" == \
                "executable|x86_64-apple-darwin|developer-id+notarized" ]]
            ;;
        rbtcd-x86_64-apple-darwin.zip)
            [[ "$kind|$target|$trust" == \
                "archive|x86_64-apple-darwin|developer-id+notarized" ]]
            ;;
        rbtcd-x86_64-apple-darwin.notary.json)
            [[ "$kind|$target|$trust" == \
                "notary-log|x86_64-apple-darwin|apple-notary" ]]
            ;;
        rbtcd-aarch64-apple-darwin)
            [[ "$kind|$target|$trust" == \
                "executable|aarch64-apple-darwin|developer-id+notarized" ]]
            ;;
        rbtcd-aarch64-apple-darwin.zip)
            [[ "$kind|$target|$trust" == \
                "archive|aarch64-apple-darwin|developer-id+notarized" ]]
            ;;
        rbtcd-aarch64-apple-darwin.notary.json)
            [[ "$kind|$target|$trust" == \
                "notary-log|aarch64-apple-darwin|apple-notary" ]]
            ;;
        rbtcd-x86_64-pc-windows-msvc.exe)
            [[ "$kind|$target|$trust" == \
                "executable|x86_64-pc-windows-msvc|authenticode" ]]
            ;;
        rbtc.cdx.json)
            [[ "$kind|$target|$trust" == "sbom|all|cyclonedx-1.5" ]]
            ;;
    esac || {
        echo "incorrect target or trust declaration for $relative" >&2
        exit 1
    }
done

echo "release manifest verified: $tag ($commit)"
