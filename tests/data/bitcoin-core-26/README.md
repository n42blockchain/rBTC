# Bitcoin Core 26 transaction vectors

These files are copied byte-for-byte from the Bitcoin Core `v26.0` tag:

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

The rBTC harness executes every valid transaction vector and every invalid transaction vector expressible through the public `libbitcoinconsensus` consensus-flag API. It also parses all 1,207 script vectors and executes the 230 cases whose complete flag set is exposed by that API. It reports `BADTX` structure cases and policy-only script-flag cases separately instead of silently changing their expected result.
