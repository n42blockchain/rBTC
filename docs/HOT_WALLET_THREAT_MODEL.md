# Daemon-held key threat model

Status date: 2026-08-11.

This document is a product-security gate, not authorization to add private
keys to rBTC. The supported wallet remains watch-only with an external signer.
Any daemon-held-key implementation is a separate P2 product and must receive an
explicit go decision after the choices and acceptance gates below are reviewed.

## Decision summary

The current architecture has the safer default boundary:

- descriptor configuration is size-bounded and rejects secret descriptors;
- the wallet database contains public descriptors, revealed scripts, chain
  state, transactions, and PSBT state, but no signing key;
- PSBT creation is bounded and persists change derivation before returning;
- signatures arrive from an external signer, then current wallet prevouts,
  sighash policy, fee, final scripts, and consensus scripts are revalidated;
- every wallet API route is loopback-only, bearer-authenticated, rate-limited
  where it mutates state, `no-store`, and backed by a fail-closed audit log.

That boundary means compromise of the running node or its wallet bearer token
can disclose wallet history and propose transactions, but cannot by itself
produce a signature. A hot wallet removes that property. Encryption at rest
does not restore it while the wallet is unlocked.

**Recommendation:** do not implement daemon-held keys without a concrete
deployment that cannot use the existing PSBT/external-signer flow. If approved,
ship it as a separately enabled custody profile, never as an upgrade-side
change to the watch-only default.

The current boundary was revalidated at `b39f75d` with
`cargo test --locked --lib wallet`: 37 wallet, API, persistence, and node
integration tests passed, including private-descriptor rejection, owner-only
files, authentication/audit failure behavior, PSBT tamper rejection, external
signature consensus verification, restart-safe address derivation, and durable
broadcast retry.

## Assets and security objectives

Assets, in descending consequence, are:

1. seed/private-key material and any passphrase that unlocks it;
2. authority to sign a transaction;
3. the exact recipients, amounts, fee, selected inputs, and change policy a
   human or policy engine authorized;
4. wallet privacy: descriptors, balances, transaction history, and address
   linkage;
5. durable wallet availability and the ability to recover from backup.

The primary objective is that no input controlled by the wallet can be signed
without the configured authorization boundary. Secondary objectives are to
avoid silent policy weakening, secret disclosure, address reuse, rollback,
and unrecoverable key loss.

## Trust boundaries

The existing process crosses these boundaries:

- local wallet API client to the loopback HTTP service;
- descriptor JSON and bearer-token files to the daemon;
- validated chainstate to the BDK wallet projection;
- unsigned PSBT from rBTC to an external signer;
- signed PSBT back to rBTC and then to the bounded relay queue.

A hot wallet adds three more sensitive boundaries:

- an encrypted key container on disk to an in-memory signing service;
- an unlock authority to that service;
- the signing service to the existing PSBT finalization and broadcast path.

The node's P2P peers, DNS/proxy services, explorer clients, and ordinary
read-only RPC token must never become signing authorities.

## Adversaries

| Adversary | Required protection |
| --- | --- |
| Remote unauthenticated network peer | Cannot reach key, unlock, or signing operations; malformed P2P data cannot influence a signing request. |
| Local process without wallet authorization | Cannot read the key container, scrape an unlock secret through process arguments/environment, or call signing routes. |
| Holder of the existing wallet bearer token | Can retain existing watch-only powers only; it must not automatically gain spending authority. |
| Malicious authenticated wallet client | Cannot substitute recipients, prevouts, fee, change, sighash, network, or PSBT bytes between approval and signing. |
| Offline disk or backup thief | Cannot recover private keys without the independent unlock factor. Public wallet history may still be exposed unless separately encrypted. |
| Crash, rollback, or filesystem failure | Cannot reuse an address reservation, replace the key identity, weaken KDF parameters, or leave an ambiguous partial key rotation. |
| Dependency or update compromise | Cannot silently enable hot-wallet mode or migrate a public descriptor file into a secret-bearing file. Release/audit gates remain independent. |
| Administrator/root while unlocked | Out of scope for key secrecy: a fully privileged live-host attacker can read process memory or invoke signing. The product must state this plainly. |

## Non-negotiable invariants

1. Watch-only remains the default build and runtime behavior. Existing
   descriptor files continue to reject every private descriptor.
2. Enabling custody requires a new explicit configuration profile and a new
   data-format inventory entry. Absence, ambiguity, or downgrade fails closed.
3. The encrypted key container is separate from `wallet.sqlite`, descriptors,
   bearer tokens, logs, diagnostics, crash reports, and backups unless the
   operator explicitly includes it.
4. The existing wallet token never authorizes unlock or signing. Spending uses
   a separate least-privilege authorization channel.
5. Unlock material is never accepted in CLI arguments, ordinary configuration,
   environment variables, HTTP JSON, or logs.
6. The signer signs only a canonical transaction/PSBT whose network, current
   wallet prevouts, outputs, change, fee, locktime, sequence, and sighash policy
   were checked immediately before signing. It returns through the existing
   finalization and consensus-verification path.
7. No `SIGHASH_NONE`, `SIGHASH_SINGLE`, or `ANYONECANPAY` is introduced by the
   hot-wallet path. The current `SIGHASH_ALL`/Taproot-default restriction is
   retained unless a separately reviewed product requires more.
8. A key is locked by default after startup, after the configured short TTL or
   signing-use limit, and before orderly shutdown. A failed memory-protection
   requirement fails closed on platforms where custody is declared supported.
9. Key creation, import, unlock failure, signing intent, signing result,
   relock, rotation, and backup verification are durably audited without
   secret, passphrase, PSBT, address, or transaction payloads.
10. Secure deletion is not claimed. Copy-on-write filesystems, SSD wear
    leveling, swap, hibernation, and backups make overwrite claims unreliable.

## Candidate architecture if approved

### Encrypted key container

Use one versioned, owner-only, non-symlink, non-hardlinked file with an atomic
create/rotate protocol and parent-directory synchronization. Bind the network,
public master fingerprint, descriptor identity, schema version, KDF parameters,
and encryption parameters as authenticated metadata. Use a reviewed
memory-hard password KDF with per-container random salt and an authenticated
encryption construction from maintained RustCrypto crates; exact algorithms
and minimum parameters require a cryptography review before implementation.

Do not put secret descriptors into the existing descriptor JSON. The public
descriptor remains the durable wallet identity and lets a locked daemon keep
watching the chain.

### Unlock channel and memory lifetime

Accept unlock material only from a controlling terminal, inherited file
descriptor/pipe, or an OS credential service selected for supported platforms.
The input must be non-echoing and bounded. Store decrypted secrets in a
zeroizing, non-cloneable object owned by a small signing service rather than the
general wallet/API state. Avoid formatted errors, debug output, heap copies,
and async task fan-out containing secrets.

The product decision must choose both a maximum unlock TTL and a maximum number
of signatures per unlock. Relock erases best-effort memory immediately. Core
dumps and swap/hibernation policy must be documented and tested on every
platform that claims hot-wallet support.

### Authorization and signing

Keep PSBT construction watch-only. A separate signing endpoint accepts an
identifier for a server-created, durably recorded intent—not arbitrary hidden
replacement bytes—and returns the canonical transaction summary that was
authorized. At minimum enforce:

- network and wallet fingerprint match;
- exact recipient and change classification;
- current unspent prevouts and coinbase maturity;
- recipient count, input count, transaction weight, fee-rate, and absolute-fee
  ceilings;
- optional per-transaction and rolling spend ceilings chosen by deployment;
- no signing while chainstate or wallet projection is stale, assumed-only, or
  not independently validated from genesis;
- one durable terminal outcome per intent so crash/retry cannot ambiguously
  sign altered content.

The signer hands its result back to `finalize_psbt_with_transaction`; it does
not bypass the existing fee, sighash, prevout, script, persistence, or relay
checks.

### Backup and recovery

Key generation/import must require a verified recovery procedure before funds
are accepted. Specify whether recovery is a mnemonic, descriptor backup,
encrypted container copy, hardware-backed export, or threshold scheme; these
are different products and must not be mixed implicitly. Rotation must prove
the new public identity before retiring the old container. Restore tests must
cover wrong network, wrong key, stale wallet database, lost metadata, and a
watch-only rescan from the public descriptors.

## Required decisions

No implementation should start until an owner answers:

1. What deployment cannot use an external signer, and what maximum value may
   this daemon custody?
2. Is the key source a generated BIP39/BIP32 seed, imported descriptor, OS
   keystore key, hardware-backed key, or threshold signer?
3. What unlock channel, TTL, signature-use limit, and supported OS matrix are
   required?
4. Is a second human/policy approval required, and what amount/fee/rate limits
   are mandatory?
5. What backup form and recovery drill are acceptable?

## Acceptance gates

Before the roadmap checkbox can change:

- an independent cryptography/security review accepts the container format,
  KDF/AEAD choices, memory lifecycle, and authorization protocol;
- unit tests cover parser bounds, wrong keys/networks, KDF floors, tampering,
  rollback, TTL/use relock, policy bypasses, PSBT substitution, and audit
  redaction;
- abrupt-kill tests cover create, rotate, unlock-state transition, intent
  publication, signing, and broadcast boundaries;
- supported-platform tests cover permissions, links, keychain/credential
  integration, swap/core-dump guidance, backup, restore, and cold restart;
- fuzzing covers the encrypted container and signing request parsers;
- a testnet-only soak exercises unlock/relock, restart, backup restore, fee
  spikes, reorgs, and rejected malicious signing requests;
- documentation continues to say that root or same-user compromise while
  unlocked can steal funds.

Until all gates pass, rBTC must keep advertising only the watch-only and
external-signer wallet as supported.
