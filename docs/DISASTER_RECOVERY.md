# Backup, restore, and disaster recovery

Status date: 2026-07-27.

This procedure applies to the standalone daemon and to an embedded rBTC node.
Each network must have its own data directory. The directory contains consensus
state, headers, bounded block archives, peer state, optional projections, and
the version inventory in `.rbtc-data-format.json`. Descriptor and
authentication files may live elsewhere and must be backed up separately.
rBTC does not store private signing keys.

## Consistent backup

1. Request graceful shutdown and wait for the node task/process to exit.
2. Confirm that an exclusive lock on `DATA_DIR/.rbtc.lock` can be obtained by
   the backup process. Never copy a live redb file or a freezer transition in
   progress.
3. Copy the complete data directory as one filesystem snapshot or one
   recursive copy. Do not omit `.rbtc-data-format.json`, `blocks/`, or a
   recovery intent. Preserve file names, permissions, and sparse-file
   allocation.
4. Copy external public descriptor configuration and auth-token files through
   the deployment's secret backup channel. Do not place bearer tokens in the
   node-data archive or its manifest.
5. Record the rBTC release, network, directory byte count, and a cryptographic
   digest of the resulting backup artifact. Keep at least one prior known-good
   generation before upgrading.

Filesystem snapshots are suitable only when they cover the entire directory at
one instant after shutdown. Per-file cloud synchronization is not an atomic
backup mechanism.

## Restore

1. Restore into a new empty directory on a filesystem with enough free space.
   Do not merge files from different backup generations or networks.
2. Restore owner-only permissions for `.rbtc.lock`,
   `.rbtc-data-format.json`, policy/intent files, wallet databases, and
   externally stored token/descriptor files.
3. Check the backup artifact digest, then run:

   ```text
   rbtcd --network NETWORK --data-dir RESTORED_DIR --verify-storage
   ```

   A non-clean or incomplete report is a hard stop. The command is read-only.
4. Start the same rBTC release with networking disabled or controlled first.
   The data-format manifest is checked before any database opens. Confirm
   network, header/execution/freezer tips, AssumeUTXO trust state, index lag,
   and disk forecast before restoring ordinary peer/API access.
   With the node stopped again, run `--verify-chain` at the intended reorg
   depth. This second command is exclusive and recovery-capable rather than
   read-only; preserve the pre-open backup generation until it reports clean.
5. After a clean checkpoint and restart, retire the previous directory only
   under the deployment's retention policy.

## Upgrade and rollback

`.rbtc-data-format.json` is a strict, owner-only inventory of the root schema,
minimum reader, network, and every persistent subsystem schema. A directory
without it is legacy v0; rBTC publishes v1 only after the existing database
preflight succeeds. An unknown future version, higher minimum reader, different
network, unknown field, or component mismatch fails before mutable database
open and is never rewritten.

The current root inventory is v3. Version 2 used basic-filter component schema
1, whose filter-header chain omitted the genesis filter. A v2 directory without
that optional database migrates forward normally. A directory containing the
old filter database fails closed and must rebuild that projection from complete
freezer history or with `--reindex-chainstate`; deleting or editing the
manifest is not a supported migration.

Before an upgrade, take a stopped consistent backup. A rollback is supported
only when the older binary accepts the manifest and all component versions.
Never delete or edit the manifest to force a rollback. Restore the prior
complete backup instead. A migration is complete only after its new manifest
is durable; a crash before publication remains the old readable generation.

## Failure decisions

- **Freezer checksum/index issue:** keep the node stopped, retain the audit JSON,
  reacquire the exact archive from an authenticated source, rerun the complete
  audit, and rebuild an index only after every selected archive verifies. Never
  delete a corrupt latest segment merely to make the report green.
- **Interrupted manual prune:** ordinary startup resumes the versioned intent.
  The reduced index is durable before physical deletion, so restart exposes
  either the old complete prefix or the planned retained suffix.
- **Chainstate corruption with complete local history:** preserve the failed
  directory for analysis and run `--reindex-from-freezer OUTPUT`. The command
  refuses incomplete or dirty history, never opens the failed source
  chainstate, and promotes only a separately verified output. Do not overwrite
  the only copy during diagnosis.
- **Chainstate corruption with a pruned freezer:** local reindex is impossible.
  Run `--reindex-chainstate OUTPUT` to pin the fully replayed maximum-work
  source header tip and revalidate from genesis using full-history peers into
  a fresh directory, or activate a release-pinned Bitcoin AssumeUTXO snapshot
  and complete independent background validation. A transport checksum alone
  does not authenticate UTXO state.
- **Header corruption:** reacquire and fully validate headers/PoW/fork work in a
  fresh directory before accepting any snapshot base or block stream.
- **Lost wallet projection:** restore public descriptors and replay from the
  configured birthday using authenticated active-chain blocks. Private signing
  material remains the external signer's responsibility.
- **Lost auth token:** replace it atomically with owner-only permissions and
  update the authorized client; do not recover it from logs or audit records.

Never silently create a new chainstate inside a damaged pruned directory.
Preserve evidence, build replacement state separately, compare authenticated
tips, and switch directories only after validation.
