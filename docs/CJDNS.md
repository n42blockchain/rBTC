# CJDNS overlay reachability

Status date: 2026-08-13.

CJDNS is an encrypted IPv6 overlay network. Node addresses are derived from
public keys, always lie in `fc00::/8`, and are reachable only through a
local `cjdroute` daemon, which presents the overlay as an ordinary network
interface. Unlike onion and I2P there is no proxy or bridge protocol: a
CJDNS peer is dialed as a plain TCP connection to its `fc00::/8` socket, and
the operating system routes it through the tunnel interface.

## Operator surface

| Configuration | Effect |
| --- | --- |
| default | `fc00::/8` is unroutable; overlay addresses are refused everywhere |
| `--cjdns-reachable` (config key `cjdns_reachable=true\|false`) | Declares a local `cjdroute` interface: overlay addresses become storable, dialable, and advertisable |
| `--onlynet cjdns` | Restricts outbound peers to the overlay; requires `--cjdns-reachable` |

Fail-closed refusals, each with a test:

- `--onlynet cjdns` without `--cjdns-reachable` — no permitted destination
  could exist.
- `--cjdns-reachable` with `--proxy` — overlay sockets are dialed through
  the local interface, never a SOCKS5 proxy. Refusing the combination keeps
  the documented invariant that a configured proxy carries *every* outbound
  peer socket, instead of quietly excepting the overlay.
- `--connect` to an `fc00::/8` peer without `--cjdns-reachable`.
- `--name-proxy` with an anonymity-only `--onlynet` (already refused; CJDNS
  joins that set).

## Network classification

- `fc00::/8` (first byte `0xfc`) is CJDNS. `fd00::/8`, the other half of
  the RFC 4193 unique-local block, is not, and stays unroutable under every
  policy.
- `--onlynet ipv6` excludes the overlay; `--onlynet cjdns` excludes
  everything else. The overlay is its own network, exactly as Core
  classifies it.
- BIP155: overlay addresses travel as the dedicated CJDNS network ID
  (`0x06`), never as plain IPv6. The legacy `addr` encoding cannot express
  them, so a peer that did not negotiate `sendaddrv2` is served nothing
  rather than a misclassified IPv6 entry. Inbound `addrv2` entries whose
  CJDNS network ID carries a non-`fc00::/8` address fail consensus decoding
  outright, and `receive_addresses` guards the same invariant for any
  payload constructed without that decoder.

## Diversity accounting

Every overlay address shares the single marker group `cjdns:0:0`, taking
precedence over both the asmap and prefix derivations: a key-derived
address carries no prefix or ASN structure to diversify over, and
fabricating fresh ones is free. One group means the entire overlay gets one
source-group quota in the new table and one address-group's tried slots —
the same conservative bound the onion and I2P books use.
`an_overlay_flood_from_many_fabricated_sources_shares_one_quota` holds the
bound: 128 candidates taught by 8 fabricated overlay sources are capped at
one group's 64 records.

Records persisted while the overlay was reachable are pruned on the first
mutation after the policy turns it off, exactly as any other
no-longer-acceptable address.

## Acceptance residue

The deterministic gates — policy-gated storage, marker-group quotas,
BIP155-only encoding, onlynet isolation, and the fail-closed configuration
refusals — all run in this repository's suite. The remaining acceptance
item is a live handshake with a real overlay peer through an independently
running `cjdroute`, which requires the daemon and an overlay route and is
therefore an operational exercise in the spirit of
[NAME_PROXY_DISCOVERY.md](NAME_PROXY_DISCOVERY.md)'s external-Tor run: dial
a known-good public CJDNS Bitcoin peer with `--cjdns-reachable --connect
[fc00…]:8333` on a host running `cjdroute`, and record the handshake and
that no non-overlay socket was opened for it. The roadmap item stays open
until that run is recorded.
