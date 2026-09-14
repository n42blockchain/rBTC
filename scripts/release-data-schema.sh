#!/usr/bin/env bash
set -euo pipefail

# Read the daemon's declaration instead of maintaining a second schema version.
# Refuse an absent/ambiguous declaration when the Rust source is reorganized.
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
awk '
  { sub(/\r$/, "") }
  /^const DATA_FORMAT_SCHEMA_VERSION: u32 = [1-9][0-9]*;$/ {
    count++
    value=$5
    sub(/;$/, "", value)
  }
  END {
    if (count != 1) exit 1
    print value
  }
' "$repo_root/src/node.rs" || {
    echo "cannot resolve the daemon data-format schema" >&2
    exit 1
}
