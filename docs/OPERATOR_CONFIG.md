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
automatic_hot_standbys=8
mempool_max_transactions=64
mempool_max_bytes=4000000
log_level=info
log_max_bytes=16777216
log_max_files=5
prune_blocks=1008
prune_max_bytes=1073741824
minimum_free_bytes=5368709120
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
- `prune_blocks`, `prune_max_bytes`, `minimum_free_bytes`
  (512 MiB–1 TiB), `chainstate_cache_bytes`,
  `background_chainstate_cache_bytes`, `bulk_validation_cache_bytes`
- `automatic_hot_standbys` (0–16), `mempool_max_transactions`
  (1–300,000), and `mempool_max_bytes` (4,000,000–1,073,741,824)
- `log_level` (`error`, `warn`, `info`, or `debug`), `log_dir`,
  `log_max_bytes` (1 MiB–1 GiB), and `log_max_files` (2–20)
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
targets, hot-standby and mempool ceilings, validation resources, and enabled API surfaces. It reports only
booleans for RPC/wallet enablement and never prints authentication paths,
descriptor paths, or credential contents.

The daemon also takes an exclusive advisory lock on the owner-only
`DATA_DIR/.rbtc.lock` before opening any database. A conflicting process fails
immediately with the recorded PID, network, and start time instead of surfacing
an arbitrary redb open error or connecting peers. The lock path rejects
symlinks, non-regular files, and (on Unix) hard links; the OS releases the lock
after crashes, while the retained marker improves the next conflict
diagnostic. Distinct embedded node instances therefore require distinct data
directories.

`.rbtc-data-format.json` binds the directory to one network and inventories the
schema version of headers, chainstate, freezer, peers, mempool, fee estimates,
explorer, wallet, and rebroadcast state. It is checked after taking the lock but
before any mutable database open. A legacy directory without the manifest is
migrated to v1 only after its existing preflight succeeds; a future root or
minimum-reader version and any component mismatch fail without rewriting it.
Do not edit or delete this file to force a downgrade. See
[`DISASTER_RECOVERY.md`](DISASTER_RECOVERY.md).

Stop the node before running a freezer audit:

```text
rbtcd --network bitcoin --data-dir /srv/rbtc/bitcoin --verify-storage
```

The command takes a shared lock on the existing `.rbtc.lock` without rewriting
its marker and fails if the daemon still owns the exclusive lock. Defaults
bound verification to 1,008 archives and 2 GiB of compressed input;
`--verify-storage-max-segments` and `--verify-storage-max-bytes` can change
those ceilings within the documented CLI bounds. It verifies compressed piece
hashes and the complete decompressed record stream with fixed memory, prints a
JSON dry-run repair plan, and returns failure for any issue or incomplete
budget. It never opens chainstate, creates a database, writes a file log, or
executes a repair.

Manual freezer pruning uses a mandatory two-phase plan:

```text
rbtcd --network bitcoin --data-dir /srv/rbtc/bitcoin \
  --prune-through-height 950000
rbtcd --network bitcoin --data-dir /srv/rbtc/bitcoin \
  --prune-through-height 950000 --apply-prune-token PLAN_TOKEN
```

The first command is read-only. The token commits to the durable ledger index,
request, exact complete segments, retained range, and reclaimed bytes. The
second command takes the exclusive lock, runs a complete freezer audit, and
refuses a stale token before creating a versioned prune intent. It never splits
an immutable archive and always preserves at least 288 retained-tip blocks, so
the effective height can be below the requested height. Index publication
precedes deletion; startup resumes a durable intent after interruption.

Every persistent data directory is checked before database open and before
each atomic validation checkpoint or live transaction-persistence cycle.
`minimum_free_bytes` is the operator reserve (default 5 GiB). The enforced
threshold additionally forecasts two consensus-maximum serialized copies of
the configured block batch, one complete bounded mempool image, one rotating
log file, and 512 MiB of database commit headroom. Falling below the threshold
is a local-resource failure: the daemon does not blame or retry peers, does not
begin another checkpoint, and leaves the last atomic state resumable.
Authenticated `getdiskinfo`, `/api/v1/status`, and `rbtc_disk_bytes` metrics
report total/available/required/reserve bytes and the bounded freezer,
mempool, and log storage ceiling.

The standalone daemon writes newline-delimited JSON diagnostics to
`DATA_DIR/logs/rbtc.log` by default. A 4,096-record non-blocking queue and a
500-record-per-second limiter bound hostile log production; excess records are
dropped and counted. Files rotate before `log_max_bytes` and retain at most
`log_max_files`, including the active file. The log directory is owner-only and
must not be a symlink. Authenticated `getloginfo` reports the effective level
and dropped count; `setloglevel ["error"|"warn"|"info"|"debug"]` changes the
level without restart. Embedded hosts do not install this process-global sink
and instead consume the bounded typed status/event receivers.
