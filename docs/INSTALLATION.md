# Installation and first start

Status date: 2026-09-14.

rBTC is pre-release. Build from the reviewed source commit for development, or
use a future signed release artifact only after completing every verification
step below. Do not install an unsigned binary as a production service.

## Build from source

The repository pins Rust 1.85 and its dependency graph:

```text
git clone https://github.com/n42blockchain/rBTC.git
cd rBTC
cargo test --locked --all-features
cargo build --locked --release
target/release/rbtcd --version
target/release/rbtcd --help
```

The reported version must equal `Cargo.toml`; the Bitcoin P2P subversion and
authenticated `getnetworkinfo` response derive from the same value.

## Verify a signed release

Use verification scripts from the reviewed release tag's checkout. The schema
check reads that checkout's daemon declaration; scripts from different releases
are not interchangeable. Download **all release assets** into a separate empty
directory. The canonical manifest verifies all ten required assets, while
`SHA256SUMS` also covers provenance bundles. Downloading one platform binary
is insufficient for these full-release checks. Once a real tag is published,
`gh release download TAG -R n42blockchain/rBTC --dir ASSET_DIR` fetches the full
set. From that directory, using the reviewed checkout path:

```text
gh attestation verify RELEASE-MANIFEST.tsv -R n42blockchain/rBTC --bundle RELEASE-MANIFEST.sigstore.json --signer-workflow n42blockchain/rBTC/.github/workflows/release.yml
/path/to/reviewed-checkout/scripts/verify-release-manifest.sh RELEASE-MANIFEST.tsv .
sha256sum -c SHA256SUMS
./rbtcd-PLATFORM --version
./rbtcd-PLATFORM --help
```

The manifest's `tag` must equal `v` plus its `version`, and its commit must be
the reviewed release commit. On macOS also require `codesign --verify --deep
--strict` and Gatekeeper; on Windows require `signtool verify /pa /all`.
On macOS, use `shasum -a 256 -c SHA256SUMS` when `sha256sum` is unavailable.
The current root schema is 4; review minimum-reader and migration rules in
[DISASTER_RECOVERY.md](DISASTER_RECOVERY.md) before opening an existing directory.

## Prepare an operator account

Use a dedicated unprivileged account. Its data, log, authentication-token, and
watch-only descriptor paths must not be writable by unrelated users. Allow at
least the configured `minimum_free_bytes` plus the startup forecast; a pruned
mainnet chainstate still requires substantially more space than the bounded
block freezer.

Create one data directory per network. Never share a directory between rBTC
versions running concurrently, networks, custom Signet challenges, or embedded
instances.

## Validate configuration without starting

Create a strict configuration based on
[OPERATOR_CONFIG.md](OPERATOR_CONFIG.md), then run:

```text
rbtcd --check-config --config /etc/rbtc/bitcoin.conf
```

This parses and bounds the complete effective configuration, prints the same
secret-free startup summary as the daemon, and exits without creating the data
directory, opening a database, installing diagnostics, or connecting a peer.
A nonzero exit is a deployment failure.

## First start and readiness

Start in the foreground under the platform service manager:

```text
rbtcd --config /etc/rbtc/bitcoin.conf
```

Use `SIGTERM`, Ctrl-C, or authenticated RPC `stop`; do not use an unconditional
kill during an ordinary upgrade. If the loopback API is enabled,
`/api/v1/health` proves process liveness while `/api/v1/ready` becomes HTTP 200
only after headers, execution, projections, minimum chainwork, and disk safety
agree. Monitor `/metrics`, structured logs, free space, peer diversity, and
header/execution lag.

Before upgrading, backing up, restoring, moving a data directory, changing
indexes, or rolling back, follow
[DISASTER_RECOVERY.md](DISASTER_RECOVERY.md). Storage verification and reindex
commands require a stopped node and are intentionally separate from service
startup.
