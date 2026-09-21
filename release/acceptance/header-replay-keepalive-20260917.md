# Header replay keepalive during admission (2026-09-17)

Overall Header scheduling, Windows acceptance and production gates: **OPEN**.

Canonical Header replay, disk-candidate recovery and winner promotion now use
one keepalive helper while awaiting shared work admission. The same admission
future stays pinned across periodic Ping/Pong exchanges, retaining its FIFO
position. Admission continues being polled during ping I/O: a head waiter can
receive its lease and release the admission mutex instead of stalling later
candidates while a peer delays Pong. A granted lease waits for the current
ping exchange to finish before returning. Peer failure drops both the pending
future and any granted lease through normal ownership cleanup.

The 20-second keepalive interval, peer timeout, work capacity/refill rate and
20-second Header recovery test deadline are unchanged. Offline replay without
a peer simply awaits work. Disk operations inside each bounded frame remain
synchronous; this change does not prove a maximum wall time for a frame or
service every other Header/admission wait path.

Prior Windows CI 35195831874/40b629f failed one recovery timeout and a mock-peer
assertion expecting GetHeaders as its first message. The mock did not accept
a legitimate replay Ping, and some final-response mocks closed immediately
while the node could still be publishing a winner. Those two fixtures now
reply to Ping before locators and remain connected for keepalive until local
processing finishes. The intentional interrupted-prefix disconnect remains.
Unexpected messages still fail with their actual variant; timeout diagnostics
include the case and observed request/ping counts. These changes do not raise
or remove the recovery deadline.

Deterministic transport tests start the keepalive clock already due, avoiding
a 20-second sleep. They verify Ping before GetHeaders and after final Headers;
withhold Pong until admission has advanced to prove concurrent polling and a
completed exchange; and disconnect during ping to check transient failure and
pending-admission resource release. Existing durable cursor/promotion, winner,
atomicity and restart assertions remain intact.

The historical Windows log did not record the unexpected message variant, so
its exact cause is not proven by this patch. The repeated Windows 20-second
recovery timeout remains unresolved pending final-source platform evidence.
No new RSS, maintenance, source-freeze or long-node/public acceptance is claimed.

Validation: final all-feature library suite 1,028 passed, 0 failed, 12 ignored
(67.49 s). Header-resync focused suite: 14 passed, 0 failed, 1 ignored (26.53 s).
Strict all-target/all-feature Clippy passed (9.80 s); format/diff checks passed.
During this work, prior-head CI 35197158015/7ecf27f Windows job completed: all
1,009 applicable library tests passed, including the historical Header failures,
but embedded_node_api::host_configured_zmq_endpoint_accepts_a_subscriber failed
its existing two-second server-greeting deadline. That run does not contain
this patch; the independent ZMQ failure and historical Header logs remain open
evidence. The overall run's Linux coverage job was still running at observation.
