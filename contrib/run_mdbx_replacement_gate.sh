#!/bin/sh
# Linux/macOS runner; Python owns frozen binaries, resume identity and evidence.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$script_dir/mdbx_gate_runner.py" "$@"
