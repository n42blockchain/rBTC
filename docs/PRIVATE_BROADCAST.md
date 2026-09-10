# Private transaction broadcast

Status date: 2026-09-10.

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

With private broadcast enabled, `rbtc.submitrawtransaction` returns error
`-32041` before enqueueing a transaction. That operator RPC uses the ordinary
mempool relay queue, so callers must use the wallet PSBT broadcast endpoint
for private submission. The refusal also applies when no wallet is configured.
Peer-originated transactions continue through ordinary mempool relay.

## The broadcast wave

When the option is active, the wallet broadcast path is replaced, not
supplemented:

1. The clearnet peer session continues to drive *scheduling* only (it is
   what wakes the broadcast loop), but the transaction bytes never touch it.
2. Up to `MAX_PRIVATE_BROADCAST_TARGETS` (4) distinct anonymity-network
   peers are selected from the onion and I2P address books.
3. Each is dialed as a short-lived session — a proxied SOCKS5 connect for
   onion, a transient SAM session for I2P. After writing the transaction, the
   sender waits for a matching ping/pong exchange before dropping the session.
   This keeps a transient proxy destination alive through remote receipt;
   writing to the local proxy alone is insufficient. Wave sessions use v1:
   both carriers already encrypt end to end, and a one-message connection
   gains nothing from a v2-with-retry negotiation.
4. The I2P wave creates a *fresh, keyless* SAM session per wave rather than
   reusing the node's persistent destination, because that destination is
   public identity and linking the transaction to it would defeat the
   option.
5. At least one peer answering the post-transaction ping counts as delivery:
   the durable rebroadcast record is updated and the transaction becomes a
   compact-block candidate. A pong proves receipt, not mempool acceptance.
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

## Live acceptance

The deterministic configuration, clearnet isolation, raw-RPC refusal and proxy
regressions run in the ordinary suite. Real-daemon transport gates live in
`tests/anonymity_network_interop.rs`; wallet-queue/private-wave gates are in
`src/node/tests/private_broadcast_interop.rs`. The latter retain a clearnet
observer until EOF, require an empty standby relay and verify real receiver
payloads. Two I2P waves from one wallet runtime must use different destinations.
They start at the wallet broadcast queue, without HTTP PSBT signing.

The [September operational report](UPSTREAM_OPERATIONAL_ACCEPTANCE_2026-09-10.md)
records the real Tor/I2P runs, the transient-session delivery defect they found,
the bounded ping/pong correction, reproduction commands and remaining scope.

## September ingress and fallback audit

Static inspection traced the wallet PSBT endpoint through
`WalletBroadcastSink` to the private wave and durable retry path. It also
found that the raw operator RPC shared `InboundTransactionQueue` with P2P
transactions, which the execution loop drains into ordinary admission and
relay. The RPC now refuses submission in private mode; its flag comes from
startup configuration independently of wallet availability.

`connect_proxied_target` opens both its preferred v2 connection and any v1
retry through `open_socks5_stream` with the same proxy and target. Private
waves request v1 directly. The I2P wave opens streams through a transient
SAM session; failed waves return to retry handling.

The `private_broadcast_refuses_raw_rpc_before_transaction_queueing`
regression checks the RPC error and an unchanged submission queue. It passed
on 2026-09-09, together with `private_broadcast_never_writes_the_transaction_to_a_clearnet_session`
and `private_broadcast_delivers_over_the_proxied_onion_path` in the full default
library suite. Dependencies were downloaded using a writable temporary Cargo
cache; the earlier dependency blocker is resolved for this run.

That run exposed an intermittent v2 fallback failure: closing a TCP stream
with unread handshake garbage can return a connection reset rather than EOF.
Outbound v2 handshake I/O now treats reset/abort/broken-pipe/EOF errors as
peer closure, allowing the existing single v1 retry. Other I/O errors and
protocol errors retain their failure paths. Deterministic stream-fault tests
cover read, write and flush. The new
`onion_v2_fallback_reconnects_through_the_same_proxy_and_target` test passed,
checking both SOCKS5 requests and the unspecified version receiver address.
These fallback checks use local mocks. Subsequent real Tor/I2P and private-wave
acceptance is recorded in the operational report linked above.
