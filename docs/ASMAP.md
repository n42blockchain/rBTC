# ASN-based address diversity (asmap)

Status date: 2026-08-13.

rBTC buckets peer addresses by the autonomous system announcing them, using
the same compressed-trie asmap format Bitcoin Core has shipped since 0.20 and
has embedded by default since 31.0. The interpreter, the IPv4-in-IPv6 lookup
form, and the load-time structural validation in `src/asmap.rs` are ports of
Core's `util/asmap.cpp`, so one data file answers identically in both
implementations.

## Why

Address-manager diversity used to be derived from IP prefixes (`/16` for
IPv4). Address space is announced by autonomous systems, not by prefix
arithmetic: one operator routinely announces dozens of unrelated-looking
prefixes, so a prefix-derived grouping overstates the diversity of an
eclipse attempt mounted from one network. Grouping by ASN prices that attack
at the number of *networks* an attacker controls, not the number of prefixes.
`peer_store::tests::a_single_asn_flood_is_capped_by_one_group_quota` holds
the resulting bound: a flood from 32 prefixes inside one AS is capped at one
source group's quota where the prefix derivation would have accepted all of
it.

## Data provenance

The embedded map is compiled into the binary by `src/asmap.rs`:

| Field | Value |
| --- | --- |
| File | `vendor/asmap/1786032000_asmap.dat` |
| Source | [`bitcoin-core/asmap-data`](https://github.com/bitcoin-core/asmap-data), path `2026/1786032000_asmap.dat` |
| Map epoch | 1786032000 (2026-08-06T16:00:00Z) |
| Upstream commit | `508156eb35a0c12e6fd14edd33c35db53bc1eea3` (published 2026-08-09) |
| SHA-256 | `5691b328295cd4266f86e85f84316eea049896049246646b918edec67348eb99` |
| Size | 1,561,497 bytes |

The file is the upstream "filled" variant, the same shape Core embeds. It is
validated at first use by the complete structural sanity check; a corrupted
build refuses to serve the map rather than answering from it.
`asmap::tests::the_embedded_map_validates_and_diversifies_a_real_address_set`
pins known-stable assignments (AS15169, AS13335, AS19281), so a silently
regenerated or truncated data file fails the suite instead of passing as
equivalent.

## Operator surface

| Configuration | Effect |
| --- | --- |
| default / `--asmap embedded` | Use the compiled-in map |
| `--asmap <path>` | Use an operator-supplied asmap file, validated fail-closed at startup; a file literally named `embedded` or `off` must be addressed as a path (`./off`) |
| `--asmap off` | No ASN mapping; groups stay IP-prefix derived |
| config file `asmap=<value>` | Same values; the command line wins on conflict |

Group derivation with a map: an address inside a known AS gets the group
`as:<asn>:0`, shared across IPv4 and IPv6 exactly as Core groups them; an
address the map does not know falls back to the prefix-derived group, so a
lookup miss never collapses unrelated prefixes into one group. The
onion/I2P/name-proxy marker groups are untouched. Records persisted under a
previous derivation keep their stored groups and remain valid; replacing or
removing the map never invalidates the peer store.

## Updating the embedded map

1. Pick the newest `<epoch>_asmap.dat` (filled variant) from
   `bitcoin-core/asmap-data`, and record its upstream commit hash.
2. Replace the file under `vendor/asmap/`, update the constant's provenance
   comment in `src/asmap.rs` (`EMBEDDED_ASMAP`) and the table above.
3. Re-verify the pinned ASNs in
   `the_embedded_map_validates_and_diversifies_a_real_address_set` still
   hold; if an anycast operator genuinely renumbered, update the pin with a
   note, not silently.
4. The map is diversity data, not consensus data: a stale map degrades
   gracefully toward prefix grouping as unmapped space grows, so updates
   ride ordinary releases (the upstream cadence is roughly monthly).

## Boundaries

- The interpreter never trusts its input: malformed bytes terminate a lookup
  with "unknown" (ASN 0), never panic and never loop. The
  `asmap_interpret` fuzz target drives exactly that property, and
  validation-accepted data must agree with the unvalidated walk.
- `MAX_ASMAP_BYTES` (16 MiB) bounds any file this node will stage, embedded
  or operator-supplied.
- ASN 0 is reserved (RFC 7607) and is this module's "unknown"; it is
  unencodable in the format's ASN operand and never becomes a group.

## macOS fuzz acceptance

On 2026-08-14, the `asmap_interpret` target completed 50,000 executions under
`cargo-fuzz` 0.13.2 and `nightly-2026-07-13` with no crash:

```bash
cd fuzz
cargo +nightly-2026-07-13 fuzz run asmap_interpret \
  corpus/asmap_interpret -- -runs=50000 -max_len=65552
```

This is a bounded smoke run, not a replacement for continuous fuzzing.
