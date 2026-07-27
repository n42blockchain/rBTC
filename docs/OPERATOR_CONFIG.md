# Operator configuration

Status date: 2026-07-27.

`rbtcd --config PATH` loads a strict, bounded `key=value` file before applying
command-line options. The file must be a regular non-symlink file and may not
exceed 64 KiB. Unknown keys, unknown network sections, malformed booleans, and
duplicate scalar keys fail before storage or network startup.

Global values apply to every network. A matching `[bitcoin]`, `[testnet]`,
`[testnet4]`, `[signet]`, or `[regtest]` section replaces global scalar values
and replaces a global repeatable list when that section supplies the same key.
An explicit CLI option replaces the complete corresponding file option group;
for example, any CLI `--connect` replaces every configured `connect`, and
`--no-once` overrides `once=true`.

```ini
# rbtc.conf
network=bitcoin
data_dir=/var/lib/rbtc/bitcoin
dns_seeds=true
once=false
mempool_full_rbf=false
prune_blocks=1008
prune_max_bytes=1073741824
chainstate_cache_bytes=1073741824
background_chainstate_cache_bytes=8589934592
bulk_validation_cache_bytes=17179869184

[bitcoin]
connect=203.0.113.10:8333
connect=203.0.113.11:8333

[testnet4]
data_dir=/var/lib/rbtc/testnet4
connect=203.0.113.12:48333
```

The supported scalar keys are:

- `network`, `data_dir`, `explorer_listen`, `rpc_auth_token_file`,
  `wallet_descriptors`, `wallet_auth_token_file`
- `minimum_chainwork`, `assumevalid`, `signetchallenge`
- `complete_assumeutxo`, `background_assumeutxo`,
  `validation_batch_size`, `validation_pause_ms`
- `prune_blocks`, `prune_max_bytes`, `chainstate_cache_bytes`,
  `background_chainstate_cache_bytes`, `bulk_validation_cache_bytes`
- Boolean `dns_seeds`, `once`, `mempool_full_rbf`,
  `cleanup_validation_dir`, and `validation_deferred_repair`

The repeatable keys are `connect`, `dns_seed`, `signetseednode`, `vbparams`,
and `testactivationheight`. Boolean values are exactly `true`, `false`, `1`,
or `0`. Authentication credentials and wallet descriptors remain in their
existing owner-only files; the config contains paths, not secrets.

Snapshot installation/download, repair, UTXO reporting/re-tiering, and other
one-shot offline maintenance commands deliberately remain CLI-only. This
prevents a persistent service configuration from unexpectedly selecting a
destructive or exceptional operating mode.

Before opening durable state or connecting peers, a persistent launch prints
one bounded effective-configuration summary covering the selected network,
data directory, peer/DNS counts, active/background/bulk cache bytes, freezer
targets, validation resources, and enabled API surfaces. It reports only
booleans for RPC/wallet enablement and never prints authentication paths,
descriptor paths, or credential contents.
