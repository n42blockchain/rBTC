# Private transaction broadcast

Status date: 2026-08-13.

`--private-broadcast` makes every locally originated wallet transaction
travel exclusively over anonymity networks — onion peers via the Tor SOCKS5
proxy, I2P peers via the SAM bridge — so the transaction is never linked to
this node's routable address at broadcast time. It is the per-transaction
analogue of Bitcoin Core 31's `-privatebroadcast`, built on the proxy,
`onlynet`, and name-proxy isolation already in place.

## Operator surface

| Configuration | Effect |
| --- | --- |
| default | Wallet transactions broadcast over the ordinary outbound peer session |
| `--private-broadcast` (config key `private_broadcast=true\|false`) | Wallet transactions broadcast only over anonymity-network waves; requires at least one anonymity path |

Fail-closed: `--private-broadcast` with neither `--proxy` (Tor SOCKS5, for
onion peers) nor `--i2psam` (I2P SAM bridge) is refused at startup — there
would be no anonymity path on which to keep the promise, and the option
must never degrade silently to clearnet.

## The broadcast wave

When the option is active, the wallet broadcast path is replaced, not
supplemented:

1. The clearnet peer session continues to drive *scheduling* only (it is
   what wakes the broadcast loop), but the transaction bytes never touch it.
2. Up to `MAX_PRIVATE_BROADCAST_TARGETS` (4) distinct anonymity-network
   peers are selected from the onion and I2P address books.
3. Each is dialed as a short-lived session — a proxied SOCKS5 connect for
   onion, a transient SAM session for I2P — the transaction is written, and
   the session is dropped. Wave sessions use the v1 transport deliberately:
   both carriers already encrypt end to end, and a one-message connection
   gains nothing from a v2-with-retry negotiation.
4. The I2P wave creates a *fresh, keyless* SAM session per wave rather than
   reusing the node's persistent destination, because that destination is
   public identity and linking the transaction to it would defeat the
   option.
5. At least one accepting peer counts as success: the transaction is
   recorded in the durable rebroadcast store and becomes a compact-block
   candidate, exactly as a clearnet broadcast would.
6. Zero accepting peers is a **bounded failure**: the transaction stays
   queued for a later wave and the poll ends rather than busy-spinning. It
   is never written to a clearnet peer and never handed to the hot-standby
   relay fan-out, which is a set of clearnet sessions.

## Leak boundary

The security-critical property — a privately broadcast transaction is never
observable on a clearnet peer — is held by
`private_broadcast_never_writes_the_transaction_to_a_clearnet_session`: with
the option active and no reachable anonymity peer, a connected clearnet
session is drained to end-of-stream and asserted never to have read a `Tx`,
the transaction is confirmed still queued, and the hot-standby relay channel
is confirmed empty. The positive path is held by
`private_broadcast_delivers_over_the_proxied_onion_path`, which runs the
whole proxied onion route in process: a mock endpoint answers the real
SOCKS5 CONNECT that `connect_proxied_target` issues, then completes the
Bitcoin inbound handshake on the same stream and reads the transaction —
the true path minus the Tor circuit.

## Acceptance residue

The deterministic gates — the fail-closed configuration refusal, the
clearnet leak gate, and the proxied-delivery path — all run in this
repository's suite. Delivery over *live* Tor and I2P circuits is exercised
by the `--ignored` tests in `tests/anonymity_network_interop.rs`, which
need real daemons; running `--private-broadcast` end to end against those
daemons and recording that no clearnet transaction relay occurred is the
one operational residue, in the same spirit as the CJDNS live-handshake and
name-proxy external-Tor runs.
