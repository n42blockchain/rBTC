# Shared disk header seeds for standby peers

Standby startup no longer loads a complete `HeaderDag` or clones it per peer.
It streams the raw store through the same validated disk replay as the main
node, then shares one immutable `Arc<DiskHeaderView>` across the connection
wave. Any pending candidate promotion is recovered before that seed is built.
An empty connection wave does not build a seed.

A standby that actually reaches its first keepalive creates a private scratch
index. Queries first read this index and then fall back to the shared seed.
Only new headers enter the private tables; the seed's history and cache are not
copied. Nested overlays are rejected, keeping lookup depth bounded at two
indexes. Skip ancestry crosses the shared boundary and reorgs remain private
to each peer. Overlay validation uses the shared work ledger with extra charges
for the second read layer. A batch is validated and committed atomically.

Each unactivated peer can retain at most 50,000 new headers above the seed's
record count. Exhaustion is a local resource outcome, not invalid peer data.
The scratch index is created lazily and cleaned when its last reader exits.
The seed remains alive until all dependent overlays and readers have exited.
This bound is provisional per-peer admission: it is not a total node bytes,
physical disk, or RSS limit, and does not close the resource gate.

Mac validation includes the existing standby activation, continuous header
validation and invalid-PoW rejection tests (12 passing), plus an independent
fork test that checks shared seed identity, one initial local genesis record,
old read versions, cross-layer locators, private branch visibility, prohibited
nested overlays/eviction, and last-reader cleanup. Full-library and strict
Clippy logs are saved with the checkpoint evidence. The full suite passed
967 tests, with 0 failures and 11 ignored, in 60.56 seconds; strict
all-feature/all-target Clippy passed.

Remaining work includes aggregate node reservations for seeds, active indexes,
per-peer caches and transactions, raw replay buffers, and pinned old read
versions; crash-leftover cleanup; multi-candidate scheduling; offline modes
that still load a full DAG; and sustained whole-node RSS/disk acceptance.
The main index and standby seed can currently coexist as two derived disk
indexes. This change removes per-peer historical RAM copies, not that remaining
shared disk duplication. No new large-scale RSS measurement or public soak is
claimed, and other production gates remain open.
