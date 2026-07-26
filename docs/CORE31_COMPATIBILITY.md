# Bitcoin Core 31 compatibility and dependency decision

Status date: 2026-07-25.

Bitcoin Core 31 is rBTC's current public-network reference. This document
separates consensus compatibility, which is a release trust requirement, from
relay policy and operator feature parity, which are independently versioned
P1/P2 work.

## Decision

rBTC owns its narrow vendored script-engine boundary. It does not pretend that
the removed `libbitcoinconsensus` ABI is maintained by Bitcoin Core:

- The engine is pinned to the final Taproot-capable `libbitcoinconsensus`
  semantics and exposes one local transaction-batch entry point. Bitcoin Core
  27 explicitly states that the underlying functionality does not change and
  that users may continue using the final library; Core 28 removed the public
  library rather than replacing its script semantics.
- The complete vendored source, compiler flags, Rust FFI, and local batch patch
  are built by rBTC CI. Rust owns input byte lifetimes for the complete call,
  the C++ adapter decodes the transaction once, and no Core validation queue or
  detached script-check object crosses the FFI boundary.
- Security advisories are classified against this actual call graph. In
  particular CVE-2024-52911 was a lifetime error between Core's
  `CCheckQueueControl` and `PrecomputedTransactionData`; neither class nor the
  affected asynchronous ownership pattern exists in rBTC's adapter.
- Replacing this boundary with `libbitcoinkernel` remains an engineering option
  only after its API is stable and it can preserve rBTC's atomic chainstate
  boundary. It is not required merely to rename an unchanged script
  interpreter.

The dependency is therefore maintained by this repository, not silently
inherited from an end-of-life binary release. A future consensus deployment or
applicable script-engine security fix is a mandatory update and release
blocker.

Primary references:

- [Bitcoin Core 27 release notes](https://bitcoincore.org/en/releases/27.0/)
- [Bitcoin Core 28 release notes](https://bitcoincore.org/en/releases/28.0/)
- [Bitcoin Core 29 release notes](https://bitcoincore.org/en/releases/29.0/)
- [Bitcoin Core 30 release notes](https://bitcoincore.org/en/releases/30.0/)
- [Bitcoin Core 31 release notes](https://bitcoincore.org/en/releases/31.0/)
- [CVE-2024-52911 disclosure](https://bitcoincore.org/en/2026/05/05/disclose-cve-2024-52911/)

## Core 27–31 classification

| Area | Upstream change | rBTC disposition |
| --- | --- | --- |
| Consensus time | Core 27 removed network-adjusted time from consensus checks. | Implemented: header future-time validation uses the local system clock; peer time is not a consensus input. |
| Script engine | Core 27 deprecated and Core 28 removed `libbitcoinconsensus`; no post-Taproot script rule was added. | Repository-owned boundary described above; existing Core vectors, historical blocks/spends, sanitizers, and the Core 31 live matrix remain mandatory. |
| Testnet4/BIP94 | Core 28 introduced Testnet4 behavior; Core 29 removed BIP94 from regtest. | Implemented for Testnet4 only, including retarget base and timewarp boundary; regtest retains its independent rules. |
| AssumeUTXO | Chainparams identities, minimum work, assume-valid data, and snapshot tooling evolved. | Core 31 identities are pinned; v2 parsing, exact `hash_serialized`, maximum-work membership, base-to-live execution, and independent genesis replay are implemented. |
| P2P transport | BIP324 became Core's default in 27. | Bounded v1 remains interoperable and secure; BIP324 is P2 because it does not alter validation trust. |
| Mempool policy | TRUC, full-RBF defaults, ephemeral dust, 1p1c package relay, orphan accounting, 0.1 sat/vB defaults, multiple data carriers, and the 2,500 legacy-sigop standard limit changed across 27–31. | Consensus remains unaffected. Current-policy parity is tracked as one explicit P1 gate; rBTC's documented conservative policy must not be labeled Core 31 policy until that gate closes. |
| Storage/indexes | Pruning cadence, dbcache defaults, coinstats index format, and tx-output-spender index changed. | rBTC's freezer and cache have independent bounded invariants. Configurable pruning/reindex and optional indexes remain explicit P1 work. |
| RPC/wallet/mining | JSON-RPC, wallet, descriptor, fee, mining IPC, and response fields changed. | Only rBTC's documented bounded API is supported; exact Core RPC/wallet/mining parity is not a validating-node release requirement. |
| Security | Later releases fixed addr, block-download/storage, and Core queue-lifetime issues. | Each advisory is checked against rBTC's independent bounded transports, pre-validation staging, freezer deletion, and synchronous FFI ownership. Applicable findings must gain a regression before release. |

## Core 31 acceptance evidence

The live differential test refuses any daemon whose `getnetworkinfo.version` is
not exactly `310000`. On 2026-07-25 the official
`bitcoin-31.0-arm64-apple-darwin.tar.gz` artifact (SHA-256
`a2d7a13b4da53d4a3e4c517f3a0269e2429813417bb320d3b268993cfdc545d0`)
passed all seven matrices:

- end-to-end valid blocks and twelve atomic rejection classes;
- BIP34, BIP66, BIP65, SegWit, CSV/BIP68, BIP113, and BIP147 activation
  boundaries;
- identical accepted/rejected outcomes, with rejected candidates leaving no
  execution tip, undo, or UTXO residue.

Core 31 renamed two rejection strings (`bad-blk-length` and `bad-cb-missing`);
the validity results did not change. The immutable Core 26 JSON and historical
fixtures remain useful cross-version evidence and are intentionally not
relabeled as Core 31 files.

Run the maintained gate with:

```sh
RBTC_BITCOIND=/path/to/bitcoin-core-31/bin/bitcoind \
  cargo test --release --test core_block_differential -- --ignored --nocapture
```
