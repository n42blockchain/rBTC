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

printf 'fixture:rbtc.cdx.json\n' >"$fixture/rbtc.cdx.json"
python3 - "$repo_root" "$fixture" <<'PY'
from pathlib import Path
import shutil
import subprocess
import sys

repo, fixture = map(Path, sys.argv[1:])
original = (fixture / 'RELEASE-MANIFEST.tsv').read_text()
verify = repo / 'scripts/verify-release-manifest.sh'
schema = subprocess.check_output(['bash', str(repo / 'scripts/release-data-schema.sh')], text=True).strip()
assert original.splitlines()[6] == 'data_schema\t' + schema

def rejected(text, label):
    path = fixture / 'invalid.tsv'
    path.write_text(text)
    result = subprocess.run(['bash', str(verify), str(path), str(fixture)], capture_output=True)
    assert result.returncode != 0, label

rejected(original.replace('data_schema\t' + schema, 'data_schema\t0'), 'stale schema')
rejected(original.replace('tag\t', 'tag\t\t'), 'empty metadata field')
rejected(original.replace('version\t0.0.0-test', 'version\t0.0.0-test\t'), 'extra metadata field')
rejected(original.replace('rustc\t', 'rustc\tbad\r'), 'carriage return')
rejected(original.replace('file\t', 'file\t\t', 1), 'empty asset field')
rejected(original.replace('\tsigstore\t', '\tsigstore\t\t', 1), 'extra asset field')
rejected(original.replace('rbtc.cdx.json\n', 'rbtc.cdx.json\t\n'), 'trailing field')
rejected(original.replace('rbtc.cdx.json\n', '../rbtc.cdx.json\n'), 'path traversal')
rejected(original.replace('sigstore', 'unsigned', 1), 'incorrect trust')
rejected(original + '\n', 'extra record')
asset = fixture / 'rbtc.cdx.json'
asset.unlink()
asset.symlink_to(fixture / 'rbtcd-x86_64-unknown-linux-gnu')
rejected(original, 'symlink asset')
asset.unlink()
asset.write_text('fixture:rbtc.cdx.json\n')

# A future daemon schema must automatically reach both producer and verifier.
# Copy only three scripts and a tiny source declaration, not a working tree.
checkout = fixture / 'tooling'
(checkout / 'scripts').mkdir(parents=True)
(checkout / 'src').mkdir()
for name in ('generate-release-manifest.sh', 'verify-release-manifest.sh', 'release-data-schema.sh'):
    shutil.copyfile(repo / 'scripts' / name, checkout / 'scripts' / name)
source = checkout / 'src/node.rs'
source.write_text('const DATA_FORMAT_SCHEMA_VERSION: u32 = 123;\n')
manifest = fixture / 'future.tsv'
command = ['bash', str(checkout / 'scripts/generate-release-manifest.sh'), str(fixture),
           str(manifest), 'v0.0.0-test', '0.0.0-test', '0123456789abcdef0123456789abcdef01234567',
           'rustc fixture', '1']
subprocess.run(command, check=True)
assert manifest.read_text().splitlines()[6] == 'data_schema\t123'
subprocess.run(['bash', str(checkout / 'scripts/verify-release-manifest.sh'),
                str(manifest), str(fixture)], check=True)
rejected(manifest.read_text(), 'schema from a different source revision')
for declaration in ('', 'const DATA_FORMAT_SCHEMA_VERSION: u32 = 123;\n' * 2):
    source.write_text(declaration)
    assert subprocess.run(command, capture_output=True).returncode != 0
PY

echo "release manifest positive and negative tests passed"
