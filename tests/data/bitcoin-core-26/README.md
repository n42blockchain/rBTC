# Bitcoin Core 26 consensus fixtures

These JSON files are copied byte-for-byte from the Bitcoin Core `v26.0` tag:

- `src/test/data/tx_valid.json`
- `src/test/data/tx_invalid.json`
- `src/test/data/script_tests.json`

Upstream: <https://github.com/bitcoin/bitcoin/tree/v26.0/src/test/data>

They are licensed under Bitcoin Core's MIT license. The pinned SHA-256 digests are:

```text
d24984eb33d5b05a85574fddaa4eee63b2490b6c5c48921355e50c1474372114  tx_valid.json
62205c293d2c98f53676dae1101017b8a609bfa67dab1392918cd29564a4b42c  tx_invalid.json
195d1ae4c1701ffa4e4b0ac14ba2b451da0e73fb22292656cd0f2196a78010db  script_tests.json
```

`signet-block-1.hex` is the consensus serialization of default global Signet
block 1, hash
`00000086d6b2636cb2a392d45edc4ec544a10024d30141c9adf4bfd9de533b53`,
retrieved from the public mempool.space Signet block API. Its SHA-256 is:

```text
f32129863bddc391dce28f83a546079fa7fa14ed590269eee55033985e52bb6f  signet-block-1.hex
```

The historical mainnet blocks were retrieved through public Esplora-compatible
APIs operated by [Blockstream](https://github.com/Blockstream/esplora/blob/master/API.md)
and [mempool.space](https://github.com/mempool/mempool/tree/master/backend).
The decoded consensus serialization is checked against the block hash embedded
in each filename, its claimed proof of work, and its Merkle root. `.zst.hex` is
hex-encoded zstd data; `.zst` is binary zstd data.
The committed-file SHA-256 digests are:

```text
95330c43be182d28e5f5871a53cc5691a6669d6cd8310d0e847b8681ec5285b2  mainnet-00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec.hex
b4183de01a2a05afd6e25ef0d85ff84cce88d5cf721fb07efaf4b35f631327f7  mainnet-00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721.hex
a7d5215251bc8cf0a9a4b755f0106b24ad4d9e848ce534bea49836a7a69fbd44  mainnet-00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22.hex
0ee234e3b6050157424d5d19d696098cd05213241ff13264eb8af8570031a81c  mainnet-000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8.zst.hex
cd1ad1ec6abcd8b8dcedbe69c8c59745bfff0a3dd0bb514feefae3998c0b45d0  mainnet-00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931.hex
e8676ddf7e5750d00851a2811f8ad3410f6fece9d4778d88e3039a95eaf3a4d7  mainnet-000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0.zst
8313e74bb995a8db3cbcb6d6c844560bd747c902eb0c9e5f4e5498461bc486b9  mainnet-000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5.zst
879350abe1ba10d15ef832abb7d470f3ad5e1a5bfc6b97a1a68893f3c80bcab4  mainnet-0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893.zst
2ab7b8e86995c1893be8dd9add535b654f5705eb9e60316743df9f0d3d9b4c1d  mainnet-0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad.zst
5a30cda5c9a6a0f2b21c9d4061c0af55a8186c112720774111c2d3e86836bec3  mainnet-0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244.zst
```

The `.utxos.json.zst` files are compact minimal external UTXO views generated
from the same Esplora block transaction responses. Tests prove that their keys
are exactly the non-coinbase inputs not created earlier in the same block. They
provide every amount and script needed for full script, fee, sigop, lock-time,
and block-undo execution. Every entry carries its exact origin height, the
origin block's parent median-time-past, and whether its creating transaction was
coinbase. `scripts/enrich-historical-utxos.py` reconstructs those fields from
Esplora Merkle proofs and block metadata, verifies every proof against its
origin block's Merkle root, reconstructs every referenced header to verify its
hash and claimed proof of work, verifies the 11-header ancestry window,
recomputes the parent median-time-past, and refuses to change any previously
pinned BIP68 height. The ignored
`target/historical-utxo-metadata` cache makes rebuilds resumable:

```sh
scripts/enrich-historical-utxos.py --api https://mempool.space/api --jobs 16
scripts/enrich-historical-utxos.py --api https://mempool.space/api --verify-only
```

These compact views are activation regression corpora, not substitutes for
prior chainstate. The three smaller authenticated transaction fixtures below
provide raw-prev-tx/Merkle evidence for representative amounts and scripts.
Snapshot digests are:

```text
c4a61807a51026482582eae639b4ea6d6336acfbeefe3759fd5f527d523f0eb1  mainnet-000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0.utxos.json.zst
2644f0d29a419b9301413ef20c14bd50da30859ba1fd257af3e8ae0d6627b649  mainnet-000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5.utxos.json.zst
a0498cb52b230da6a3e9fc2f4c8125a0cb06b5a4a577cd33915ef9cf32564280  mainnet-0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893.utxos.json.zst
d970beddc7492a90ae73a3c2bfb336cab544918c1ff0f943e503c26f8f2492fa  mainnet-0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad.utxos.json.zst
61b2d71641a93a43718efaf98aa8588c6ab8805abbbff1dbf8a7ff4bb17d19b0  mainnet-0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244.utxos.json.zst
```

`authenticated-historical-transactions.json` contains a real SegWit activation
spend, a real BIP68 spend accepted at its exact 144-block relative-height
boundary, and the first mainnet Taproot key-path spend. Every spending and
previous transaction is included as raw consensus bytes together with its
containing block header, transaction position, and Merkle branch. The tests
validate both headers' claimed proof of work, both inclusion proofs, and derive
each UTXO amount and script directly from the txid-authenticated previous
transaction. Its SHA-256 is:

```text
74609a29b34dd9d2073a2777f22a0f487fbfb46ccdada907cb479cf150e583a2  authenticated-historical-transactions.json
```

The rBTC harness executes every valid transaction vector and every invalid transaction vector expressible through the public `libbitcoinconsensus` consensus-flag API. It also parses all 1,207 script vectors and executes the 230 cases whose complete flag set is exposed by that API. It reports `BADTX` structure cases and policy-only script-flag cases separately instead of silently changing their expected result. The Signet fixture exercises Core-compatible BIP325 commitment extraction and challenge execution through the production block-connection path.
