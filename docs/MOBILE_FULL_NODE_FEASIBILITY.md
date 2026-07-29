# Mobile Bitcoin full-node feasibility

Status date: 2026-07-29.

## Decision

A fully validating Bitcoin node on a mobile device is technically feasible as
an opt-in, resource-bounded mode, but the current rBTC redb layout is not
mobile-ready. A practical mobile implementation should target 15–25 GB of
reserved storage, 512 MiB–1 GiB of working memory, Wi-Fi and charging-aware
operation, and a compact random-access UTXO backend.

A 60-day UTXO subset is useful as a fast cache but is not a complete
chainstate. A fully validating node must retain every unspent output, either in
local hot/warm/cold tiers or behind an authenticated accumulator/proof design.
Bitcoin block headers do not commit to the current UTXO set, so fetching
unproven cold coins from a remote server would weaken the node to a trusted
client.

The recommended product split is:

- default mobile mode: maximum-work headers, compact filters, wallet state,
  and remote miner monitoring;
- advanced full-node mode: Core 31 AssumeUTXO bootstrap, the complete compact
  UTXO set, 288 recent blocks, no historical indexes or inbound serving by
  default, and background validation only while charging on unmetered Wi-Fi.

## Measurement scope

Unless stated otherwise, sizes use decimal bytes. The data was captured from
the independent public-mainnet soak directory
`rBTC-mainnet-assumeutxo-20260725`.

The UTXO population measurements came from:

- a complete genesis-to-height-935,000 replay with 3,257,609,051 observed
  spends;
- a separate height-935,001-to-959,730 live sample with 179,211,528 observed
  spends;
- an exact current-population scan at height 959,730;
- exact parsing of 556 retained consensus blocks from heights 959,514 through
  960,069.

The soak continued advancing after capture, reaching height 960,071 while this
document was written. The difference is immaterial to the storage and traffic
conclusions, but the report heights are retained so every number remains
reproducible rather than being presented as a moving live value.

An rBTC logical UTXO record estimate includes:

```text
36-byte outpoint key
+ 29-byte rBTC value metadata
+ serialized scriptPubKey
```

It excludes B-tree pages, transaction journals, free space, WAL/checkpoint
headroom, and filesystem allocation.

## Current UTXO and node footprint

At height 959,730 the active UTXO set contained 166,291,695 entries with an
estimated logical record size of 15,515,964,135 bytes. Applying the exact net
changes through height 960,070 produced approximately 166.34 million entries;
the logical size remains approximately 15.5 GB.

The captured on-disk files were:

| Item | Captured size |
| --- | ---: |
| Core 31 height-935,000 `dumptxoutset` v2 snapshot | 9,387,990,306 bytes |
| rBTC logical UTXO records at height 935,000 | 15,334,473,795 bytes |
| Live `chainstate.redb` | 73,023,500,288 bytes |
| `headers.redb` | 286,273,536 bytes |
| Retained compressed block archives | about 604 MB |
| Complete node directory reported by `du` | about 76 GiB |

The Core snapshot is compactly serialized using Bitcoin amount, script, and
integer encodings. Its size is 61.22% of rBTC's logical record estimate at the
same height, a 38.78% reduction. It is an import artifact rather than a
random-access database and can be deleted after successful activation.

The 73 GB redb chainstate is suitable for the current correctness and soak
work, but is not a mobile storage target. A mobile backend needs compact keys,
prefix-aware blocks, bounded write amplification, explicit compaction, and a
small mutable overlay over immutable cold data.

## UTXO activity and the hot/cold boundary

An address is not a consensus chainstate object. Several UTXOs may share one
address or script, and an unspent output is not updated when an address is
reused. The measurements therefore classify UTXOs by creation height and
historical spends by the number of blocks between creation and spend. They do
not claim to count unique active addresses.

The post-base live sample produced:

| Candidate window | Current UTXOs | Logical bytes | Current-set share | Historical spend-hit rate |
| --- | ---: | ---: | ---: | ---: |
| 60 days / 8,640 blocks | 5,873,863 | 530,785,912 | 3.53226% | 94.10195% |
| 180 days / 25,920 blocks | 14,010,944 | 1,271,049,204 | 8.42552% | 96.05636% |
| 1 year / 52,560 blocks | 26,167,447 | 2,401,443,575 | 15.73587% | 97.19957% |
| 2 years / 105,120 blocks | 54,941,098 | 5,146,812,929 | 33.03899% | 98.43638% |
| 3 years / 157,680 blocks | 97,862,624 | 9,277,301,849 | 58.84997% | 99.42139% |
| Complete set | 166,291,695 | 15,515,964,135 | 100% | 100% |

The complete genesis replay independently selected 157,680 blocks as the
smallest evaluated window reaching the 99% target, at a 99.38467% historical
spend-hit rate. The later live sample confirmed 99.42139%.

The data-backed storage policy is therefore:

1. **L0, very hot:** 60 days, about 531 MB, for the fastest mobile cache.
2. **L1, performance hot:** 3 years, about 9.28 GB including L0, expected to
   satisfy about 99.42% of spends without a cold lookup.
3. **L2, cold:** 68,429,071 older UTXOs, about 6.24 GB logically, stored in
   compact immutable shards.

All three tiers are part of the same consensus UTXO set. Tier placement is a
performance policy and is deliberately excluded from the snapshot and
chainstate identity. The implemented activity report and restart-safe
re-tiering procedure are described in
[ARCHITECTURE.md](ARCHITECTURE.md#utxo-layout).

## Compression and the txid entropy floor

The 32-byte transaction identifier in every outpoint behaves like uniformly
distributed hash output. For approximately 166.34 million UTXOs, txids alone
occupy:

```text
166,344,266 × 32 bytes = 5,323,016,512 bytes
```

That approximately 5.32 GB is effectively incompressible. Output indexes add
more key material. Sorting improves lookup locality and permits delta or prefix
encoding of structural fields, but should not be expected to create long
shared prefixes among cryptographic hashes.

Useful compression remains available in:

- Bitcoin Core amount compression;
- templates for P2PKH, P2SH, P2WPKH, P2WSH, and P2TR scripts;
- height deltas and combined height/coinbase encodings;
- repeated record framing;
- immutable-shard compression and dictionaries.

The measured 9.39 GB Core snapshot demonstrates that the complete record is
not incompressible even though its txid component is. A compact random-access
mobile UTXO store will necessarily be larger than the entropy floor and is
expected to require roughly 11–16 GB before block retention, undo, journals,
and compaction headroom. This range is an engineering target, not yet a
measured rBTC mobile backend result.

## Block traffic and UTXO mutation rate

Exact parsing of the retained 556-block mainnet sample produced:

| Metric per block | Mean | P50 | P99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Consensus block bytes | 1,586,770 | 1,590,738 | 1,957,853 | 3,970,193 |
| Transactions | 4,928 | 4,768 | 7,187 | 7,305 |
| Spent UTXOs | 7,671 | 7,759 | 9,580 | 10,845 |
| Created spendable UTXOs | 7,858 | 7,621 | 12,917 | 14,321 |
| Logical UTXO mutation bytes | 968,153 | 955,384 | 1,385,955 | 1,489,552 |

The mutation estimate contains 36 bytes for each deletion key and
`36 + 29 + script length` bytes for each created spendable output. It excludes
database page and journal amplification.

Projected from the complete retained sample:

| Window | Full block download | Local logical UTXO mutation |
| --- | ---: | ---: |
| 1 block | 1.587 MB | 0.968 MB |
| 6 blocks / 1 hour | 9.52 MB | 5.81 MB |
| 144 blocks / 1 day | 228.5 MB | 139.4 MB |
| 1,008 blocks / 1 week | 1.60 GB | 0.976 GB |
| 4,320 blocks / 30 days | 6.85 GB | 4.18 GB |

The last six observed blocks happened to total 11.79 MB of block payload and
4.87 MB of logical UTXO mutations, illustrating short-window variance.

The sample grew by a net 103,972 UTXOs across 556 blocks, exactly 187 entries
per block on average. At the current mean record size, net durable-set growth
is only about 17 KB per block or 75 MB per 30 days. Local write churn is much
larger than net growth, so flash suitability depends primarily on WAL,
copy-on-write, and compaction amplification.

## Mobile networking assessment

An average 1.587 MB block every ten minutes is only about 21.2 kbit/s averaged
over time. Even a 10 Mbit/s Wi-Fi link transfers an average block in roughly
1.3 seconds. Wi-Fi bandwidth is therefore not the limiting resource.

The full-block lower bound is about 6.85 GB per 30 days. Mempool transaction
relay, replacements, rejected traffic, peer negotiation, and inventory add to
that total. Compact blocks avoid retransmitting transactions already present
in the mempool but do not remove the need for the node to receive those
transactions at least once. A product traffic budget must be based on
end-to-end peer byte counters rather than block bytes alone.

Recommended mobile defaults are:

- unmetered Wi-Fi only for background synchronization and historical
  validation;
- no inbound listener;
- no historical upload service;
- bounded outbound peers with network-group diversity;
- a small mempool;
- 288 retained blocks unless the operator selects a larger window;
- pause background validation on battery or thermal pressure.

Cellular operation is possible but should be explicit because the monthly
traffic lower bound is already material. iOS background suspension also makes
an always-on full node unsuitable as a normal background application. Android
requires a foreground service and power-management exemptions for reliable
continuous operation.

## Maximum-work security and snapshot authentication

Bitcoin's 80-byte block header commits to the previous header and the Merkle
root of the block's transactions. Changing a historical transaction changes
the Merkle root and requires redoing that block's proof of work and every
descendant's work. The Bitcoin developer reference documents these header
commitments and the proof-of-work target:

<https://developer.bitcoin.org/reference/block_chain.html>

A counterfeit history must exceed the honest chain's cumulative work to become
the selected maximum-work chain. This is not unconditional mathematical
unforgeability: it relies on SHA-256 security, honest hashpower dominance, and
the node learning about the honest chain. An eclipsed fresh node can be shown a
valid but stale lower-work chain and cannot know that an unseen heavier chain
exists. Peer diversity, a compiled minimum-chainwork floor, time sanity, and
independent network paths reduce that information-isolation risk.

Maximum-work headers do **not** authenticate an arbitrary UTXO snapshot.
Headers commit to block transactions, not to a current UTXO root. Bitcoin Core
31 instead hard-codes an AssumeUTXO tuple containing the base height, block
hash, serialized UTXO-set hash, and chain transaction count. Mainnet height
935,000 is one of those compiled identities:

<https://github.com/bitcoin/bitcoin/blob/v31.0/src/kernel/chainparams.cpp#L1995-L2043>

Core accepts a downloaded snapshot only when it matches a compiled hash,
promotes it for fast synchronization, and simultaneously validates the
historical chain from genesis. When background validation reaches the snapshot
base height, Core hashes the independently reconstructed UTXO set and compares
it with the compiled value:

<https://github.com/bitcoin/bitcoin/blob/v31.0/doc/design/assumeutxo.md#background-chainstate-hits-snapshot-base-block>

rBTC follows this existing Bitcoin mechanism rather than adding an MPT:

1. download and validate the maximum-work header chain;
2. require the Core 31 height/blockhash/count/UTXO-set identity;
3. atomically load the complete snapshot;
4. validate and serve new blocks against that complete state;
5. independently replay genesis to the snapshot base in the background;
6. finalize only after the reconstructed state agrees.

Before background completion, operation inherits the compiled Core release's
snapshot assumption. After completion, the node has restored ordinary
genesis-to-tip full-validation assurance.

## Techniques that can and cannot bound UTXO storage

### Compatible with existing Bitcoin consensus

- omit provably unspendable outputs such as `OP_RETURN`;
- delete spent outputs immediately;
- prune old block payloads while retaining headers and the complete UTXO set;
- compact amounts, scripts, heights, and flags;
- keep sorted outpoint batches and merge create-then-spend changes before disk;
- use bounded write overlays and immutable cold shards;
- cache the 60-day tier and keep a 3-year performance tier;
- coalesce and sort deletion and insertion keys before an atomic transaction;
- use Bloom filters or learned cache admission to avoid unnecessary cold reads;
- retain only a bounded block/undo suffix;
- bootstrap through Core AssumeUTXO and complete background validation.

Bitcoin relay dust policy and the fee market discourage uneconomic UTXO
creation, but they are not consensus restrictions. A miner can include outputs
that ordinary peers would not relay.

### Requires additional protocol or consensus work

- deleting old unspent coins by age;
- UTXO rent, demurrage, or expiry;
- treating inactive addresses as abandoned;
- relying on remote cold records without an authenticated proof;
- assuming that the maximum-work header authenticates a UTXO snapshot.

An accumulator design such as Utreexo can reduce local UTXO storage by requiring
proof-carrying spends and proof-serving peers. It can be implemented as a node
protocol without making an MPT part of Bitcoin, but it still needs a securely
derived bootstrap accumulator state, proof availability, new peer behavior,
and extensive consensus-equivalence validation. It is not the current Bitcoin
AssumeUTXO mechanism.

## Mobile product acceptance criteria

The current desktop implementation should not be described as a mobile full
node until all of the following are measured on supported devices:

1. compact random-access UTXO storage no larger than 16 GB at the measured
   height;
2. total steady-state allocation no larger than 25 GB including blocks, undo,
   journals, and one compaction window;
3. steady live memory below 1 GiB and a 512 MiB target configuration;
4. bounded recovery after process kill and device reboot;
5. no unbounded rewrite or compaction after every block;
6. monthly flash writes measured, including filesystem amplification;
7. background validation resumable across mobile suspension;
8. thermal and battery tests while validating worst-case blocks;
9. metered-network controls and end-to-end peer byte accounting;
10. the same consensus, AssumeUTXO, crash-safety, and differential gates as the
    desktop node.

On a 128 GB or 256 GB Android device, an optimized 15–25 GB opt-in node is
reasonable while charging on Wi-Fi. It is not a suitable default for a 64 GB
device. A headers-and-filters mode remains the correct default for broad mobile
deployment.

## Remote ASIC mining boundary

ASIC miners such as Antminer devices perform SHA-256 proof-of-work. They do not
replace an independently validating node. A mobile device can monitor hashrate,
temperature, pool/Stratum state, payouts, and the independently verified chain,
but it should not be the mine's only block-template, Stratum coordination, or
consensus service.

The reliable deployment is:

- an always-on full node and template/Stratum coordinator near the miners;
- dedicated ASICs for proof-of-work;
- the mobile node or light mode for independent verification, monitoring, and
  operator control;
- no mining or wallet hot keys stored in the rBTC mobile node process.
