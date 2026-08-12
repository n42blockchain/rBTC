# Name-proxy discovery boundary

Status date: 2026-08-11.

This note turns the open name-proxy roadmap item into an implementable decision.
It does not change current behavior: configuring `--proxy` still requires DNS
seeds to be disabled, so seed names never reach the host resolver.

## Current boundary

Today rBTC has two separate operations:

- `resolve_dns_candidates` sends bounded seed lookups through
  `tokio::net::lookup_host`, filters the returned IP addresses, then schedules
  ordinary socket targets;
- `open_socks5_stream` connects to a configured no-authentication SOCKS5 proxy
  and encodes routable peers as IP literals or onion services as domain names.

Allowing those paths together without a new design would leak seed hostnames to
the host resolver before the resulting peer socket entered the proxy. Startup
therefore rejects `--proxy` with enabled DNS seeds. Operators bootstrap with
explicit IP peers or their persisted verified peer database.

At `b39f75d`, both existing SOCKS5 encoding/handshake tests passed, as did the
exact `proxy_and_onlynet_prevent_direct_or_resolver_leaks` configuration test.
Those three tests preserve today's fail-closed boundary; they are not evidence
that the proposed name-discovery path exists.

## Goal and non-goals

The goal is to bootstrap a routable Bitcoin peer through a proxy without any
local DNS request and without weakening existing `onlynet` or candidate bounds.

It is not a goal to:

- resolve `.onion` or `.b32.i2p` through public DNS;
- add proxy credentials to configuration or logs;
- trust DNS seed data as more than untrusted peer candidates;
- persist seed authority names as verified peers;
- promise the destination IP when the proxy deliberately hides it;
- make name discovery a P0 release requirement.

## Options considered

### A. SOCKS5 CONNECT with a domain target — recommended baseline

Send the DNS seed hostname directly in the standard SOCKS5 `CONNECT` request
with `ATYP=DOMAIN` and the network's P2P port. The proxy resolves the hostname
and selects one returned peer. rBTC completes the normal Bitcoin handshake on
that stream, requests peer addresses through the existing bounded discovery
path, and persists only independently validated IP entries learned from the
peer—not the seed hostname.

Advantages:

- portable standard SOCKS5 behavior already used for onion domain targets;
- no local resolver call or new DNS/HTTP dependency;
- one successful bootstrap is enough to enter existing `getaddr` discovery;
- the peer remains subject to ordinary handshake, service, protocol, retry,
  and chain validation.

Limits:

- one connection yields one proxy-selected address rather than the seed's full
  answer set;
- the actual peer IP may remain unknown, so IP-group reputation and manual-IP
  protections cannot be assigned to the transient name target;
- a proxy may not support domain targets or may choose an address family the
  caller cannot observe.

### B. Proxy-specific remote RESOLVE command — optional, not the baseline

Tor and some SOCKS implementations expose non-standard resolve commands. They
can return an IP before connection but are not part of portable SOCKS5, may
return only one answer, and add proxy-specific protocol branches. Supporting
one later is reasonable only behind explicit capability/configuration and
never as silent fallback to the host resolver.

### C. DNS-over-HTTPS/TLS through the proxy — rejected initially

This could recover a full address set but adds HTTP/TLS parsing, resolver trust,
certificate/time dependencies, response-size rules, and a new privacy policy.
It is disproportionate when one proxied seed connection can bootstrap normal
Bitcoin address discovery.

### D. Continue explicit/persisted peers only

This is the existing safest behavior and remains a supported choice. It is the
fallback when no deployment requires automatic proxied bootstrap.

## Implementation status

At `b39f75d` nothing of this design existed. As of 2026-08-11 the name rule
alone is implemented, in `src/seed_name.rs`: `SeedName::parse` is the only
constructor, so no unvalidated string can reach an encoder, and it refuses NUL,
path, whitespace, CR/LF, port suffixes, `@`, non-ASCII, empty labels,
over-long labels, hyphens at label edges, unqualified single labels, and IP
literals, while normalising case and a trailing root dot so two spellings of
one authority cannot reach the proxy as different names. Six unit tests cover
those, including that every accepted name fits the single-byte SOCKS5 length
prefix.

The `--name-proxy` flag and its `NodeResourceConfig::name_proxy` field follow,
with their refusal rules: a concrete IP and nonzero port, at most one
occurrence, and refusal under any anonymity-only `--onlynet` because clearnet
seed discovery cannot produce a permitted target there. Supplying it is the
explicit authorisation that lets `--proxy` run alongside DNS seeds; without
it that combination is still refused, and the message now names the flag that
would allow it. Three unit tests cover those rules.

The transient target and its wire encoding follow. `ProxyTarget::SeedName`
carries a validated `SeedName` and a port; `open_socks5_stream` sends it as a
SOCKS5 `ATYP=DOMAIN` request, so the proxy resolves it and this node performs
no lookup. Without a proxy the target is refused outright rather than falling
back to the host resolver, which is the leak the whole path exists to avoid.
`proxy_version_address` advertises an unspecified receiver for it, because the
proxy chose the peer and does not report which address it picked; inventing
one would put an unverified address into a field peers may relay onward. Two
unit tests cover the wire form — including that the normalised name is what
reaches the proxy, so one authority is one name — and the advertisement.

`seed_name_wave` selects which authorities a wave may contact. It preserves
configured order, collapses an authority repeated under two spellings so it is
contacted once, drops individually malformed entries while reporting each with
its reason rather than failing the whole list, and caps the wave at the same
16-authority ceiling configuration already imposes — authorising proxied
discovery must not widen how many authorities this node contacts. No failure
state is persisted: point 8 allows a durable schema only if justified, and
nothing yet justifies one.

`NodePeerTarget::SeedName` makes a validated authority dialable by the
existing connection path, so the ordinary handshake, service, protocol, and
timeout rules apply to it unchanged. What it does not inherit is address
identity: it records no attempt, earns no session-success promotion, enters
no discouragement table, exposes no socket, and is the one target that never
parses back from its textual form, because accepting it there would let
`--connect` hand an arbitrary hostname to a proxy as though it were a peer.
Dialing one uses `name_proxy`, never `proxy`, and a missing name proxy is a
local fault rather than a degraded route. Two unit tests cover the refusal
and the empty peer books.

The call site follows, and it is the first change on this path that alters
network behaviour. With `--name-proxy` configured, a bootstrap that has
exhausted its explicit and persisted peers submits the wave to that proxy.
`seed_bootstrap` states the source choice separately so it can be verified on
its own: the two are exclusive, not ordered, because consulting the resolver
as well — or falling back to it when the proxy fails — would send the lookup
the authorisation exists to prevent. The wave runs once and contacts each
authority once; a failed name is not retried within the run, since repeated
spellings were already collapsed and dialing one again would repeat that
disclosure to buy a retry the per-attempt deadline has already spent. Retry
policy stays where it can be keyed by address: on the `addr` entries a
successful name yields. Ports come from the configured entry that first names
each authority, matching the wave's choice of the first spelling. Two unit
tests cover the source choice and the port pairing.

The `name_proxy` config-file key follows the flag, in its own override group
so that changing where names go says nothing about where peer traffic goes.

Two tests now exercise the path end to end against a mock SOCKS5 server. One
dials a name target through it and completes the Bitcoin handshake, asserting
the request carried `ATYP=DOMAIN` with the normalised authority and the
network port, and that the advertised receiver stayed unspecified. The other
gives the two authorisations separate endpoints, each asserting the address
type it must see, so a name target reaching the ordinary peer proxy fails
rather than passing because a real deployment usually points both fields at
one service.

Still unimplemented: the rest of the acceptance matrix. The local list is
partly covered — the domain request, the proxied handshake, the anonymity-only
refusals, malformed-name rejection, and the absence of any fabricated IP peer
all have tests — but no test yet drives `getaddr` and learned-IP filtering
over a proxied name session, and no run has captured host DNS traffic to
demonstrate the absence of a query rather than reason about it. The
real-environment items are untouched.

## Recommended configuration model

Add an explicit `--name-proxy IP:PORT` (and corresponding typed/config-file
field) rather than silently changing `--proxy` semantics.

- `--proxy` continues to route outbound peer sockets and, by itself, continues
  to conflict with DNS seeds.
- `--name-proxy` explicitly authorizes seed-name submission to that proxy and
  prevents every local lookup for those seeds.
- Supplying both normally points them at the same service for full proxy
  isolation, but keeping the fields distinct expresses the two trust choices.
- No authentication is added; both endpoints retain the current concrete-IP,
  nonzero-port validation.
- `--name-proxy` is rejected for onion-only or I2P-only operation because
  clearnet seed discovery cannot produce permitted anonymity-network targets.
- The first implementation is allowed only when both IPv4 and IPv6 are
  permitted. A domain `CONNECT` does not reliably reveal which family the
  proxy chose, so claiming strict single-family `onlynet` enforcement would be
  false. Family-specific support requires a proxy capability that proves the
  result without local resolution.

## Candidate and persistence model

Introduce a bounded transient target such as `SeedName { host, port, index }`.
It is not a routable `SocketAddr`, onion service, I2P Destination, or peer-store
key.

1. Preserve explicit and persisted IP/onion/I2P candidate order.
2. Only after those waves fail, take at most the existing 16 seed authorities.
3. Start at most the existing global candidate limit, with the existing
   per-attempt and handshake deadlines.
4. Encode each strict hostname as one SOCKS5 domain target and connect through
   `name_proxy`; never call `lookup_host` in this branch.
5. Advertise an unspecified receiver address in the Bitcoin `version` message,
   as the real peer socket is not known authoritatively.
6. Apply ordinary nonce, service, protocol, header, block, and completion
   validation. A successful name target is not promoted directly to the IP
   tried table.
7. Request `addr`/`addrv2`; validate and persist eligible IP entries through
   the existing source/global bounds for use on the next wave or restart.
8. Store bounded failure state by seed authority only if a durable schema is
   justified. Never invent an IP address for discouragement or group scoring.

Hostnames must be strict ASCII DNS names, at most 255 bytes for SOCKS5 and at
most the repository's existing seed count. Reject NUL, path, whitespace,
non-ASCII, empty labels, label overflow, and IP-literal ambiguity before opening
the proxy connection. Errors may name the configured seed authority but must
not include proxy credentials or unbounded replies.

## Privacy and failure invariants

1. With `--name-proxy`, no seed hostname reaches `lookup_host`, libc resolver,
   configured DNS server, or direct UDP/TCP port 53.
2. Proxy refusal, malformed replies, unsupported domain targets, timeout, or
   disconnect fails that candidate; none falls back to local DNS.
3. `--onlynet onion` and `--onlynet i2p` issue no clearnet seed query through
   either the host or name proxy.
4. The proxy sees the seed hostname and connection timing. The selected DNS
   seed and proxy can correlate the bootstrap; documentation must state this.
5. The connected Bitcoin peer can return malicious addresses, but existing
   routability, family, count, source-group, duplicate, and peer-store bounds
   remain authoritative.
6. A name target contributes no IP network-time sample, tried collision,
   per-IP discouragement, or IP diversity claim until a real eligible IP is
   independently learned.

## Acceptance matrix

### Local deterministic tests

- a mock SOCKS5 server observes exactly one `ATYP=DOMAIN` request with the
  configured seed and P2P port;
- the proxied Bitcoin handshake, service checks, `getaddr`, bounded response,
  and learned-IP filtering complete without any resolver hook being called;
- IPv4-only, IPv6-only, onion-only, and I2P-only configurations fail closed as
  specified before network I/O;
- malformed hostnames and oversized proxy replies are rejected within current
  bounds;
- proxy timeout/refusal/malformed response never calls the local resolver;
- seed count, connection-wave count, duplicate, retry, and shutdown bounds are
  unchanged;
- successful transient name targets are not written as fabricated IP peers.

### Real-environment acceptance

- run through a real Tor SOCKS endpoint or the deployment's selected SOCKS5
  implementation and complete a Bitcoin/Testnet4 handshake from a seed name;
- capture DNS traffic on the host and assert no query is emitted;
- confirm proxy logs show domain-target handling and that disabling the proxy
  produces a bounded failure rather than a direct fallback;
- restart using only learned persisted IP peers and confirm DNS/name-proxy
  discovery is not required while viable candidates remain.

## Required product decisions

Before implementation, an owner must accept or change these recommendations:

1. use explicit `--name-proxy` rather than making `--proxy` imply remote name
   discovery;
2. use portable domain `CONNECT` as a one-peer bootstrap, not a non-standard
   remote-resolve command or DoH client;
3. support only dual-stack routable mode initially, because the proxy-selected
   address family is otherwise unverifiable;
4. keep explicit/persisted peers before name discovery and persist only normal
   peer-advertised IP entries;
5. retain no-authentication proxy scope.

Until those decisions are accepted and the acceptance matrix passes, the
current explicit/persisted-peer behavior remains the supported privacy boundary.
