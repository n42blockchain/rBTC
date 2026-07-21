# Bitcoin Core 26 transaction vectors

These files are copied byte-for-byte from the Bitcoin Core `v26.0` tag:

- `src/test/data/tx_valid.json`
- `src/test/data/tx_invalid.json`

Upstream: <https://github.com/bitcoin/bitcoin/tree/v26.0/src/test/data>

They are licensed under Bitcoin Core's MIT license. The pinned SHA-256 digests are:

```text
d24984eb33d5b05a85574fddaa4eee63b2490b6c5c48921355e50c1474372114  tx_valid.json
62205c293d2c98f53676dae1101017b8a609bfa67dab1392918cd29564a4b42c  tx_invalid.json
```

The rBTC harness executes every valid vector and every invalid vector expressible through the public `libbitcoinconsensus` consensus-flag API. It reports `BADTX` structure cases and policy-only script-flag cases separately instead of silently changing their expected result.
