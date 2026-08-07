# Changelog

All notable user-visible changes will be recorded here. Versions follow
Semantic Versioning; a release tag must exactly equal `v` plus the version in
`Cargo.toml`.

## [Unreleased]

### Added

- Bitcoin, legacy testnet, Testnet4, Signet, and regtest full-node validation.
- Core 31-compatible AssumeUTXO activation with independent background
  validation.
- Bounded outbound and optional inbound P2P services, persistent mempool,
  pruned freezer, optional indexes, authenticated operator APIs, and a
  watch-only external-signer wallet.
- Cross-platform signed-release, SBOM, provenance, reproducibility, audit,
  fuzzing, and public-network soak gates.
- Opt-in BIP324 v2 encrypted transport for outbound peers (`--v2-transport`),
  with the specified one-shot v1 retry when a peer closes the v2 attempt.

### Safety

- rBTC remains pre-release until the seven-day public-network soak and first
  native signed release complete. It must not hold mainnet private keys.

### Productization

- Added runtime version reporting, side-effect-free configuration validation,
  tag/package/binary version binding, native release artifact smoke tests, and
  a private vulnerability-reporting policy.

[Unreleased]: https://github.com/n42blockchain/rBTC/compare/v0.1.0...HEAD
