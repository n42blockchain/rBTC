# Startup listener progress during synchronous initialization (2026-09-17)

Total startup memory, Windows/whole-node acceptance and production gates: **OPEN**.

The multi-thread node now enters Tokio's blocking region while preparing API
state, constructing the admission pool, and loading AS-map/peer database state.
These synchronous calls keep their existing ownership and return values in the
node task; the runtime can replace the occupied worker and run already-bound
network services. Initialization is not detached into an unjoined background
job, and no new service or test deadline is increased.

A controlled two-worker regression binds the actual ZMQ publisher inside a
runtime task, then immediately blocks that task on initialization completion.
An independent TCP client connects to the bound port and expects the service's
64-byte greeting within two seconds. It releases initialization whether the
read succeeds or fails, so the test leaves no permanently blocked worker.
With the previous inline execution order, the greeting timed out (2.02 s,
WouldBlock). With the blocking region, the same test passed (0.01 s) and
validated the ZMTP version/mechanism bytes. This demonstrates a real scheduling
starvation mechanism consistent with the observed CI symptom.

Prior CI 35197158015/7ecf27f is terminal: Linux including 90% coverage and the
supply-chain job passed; Windows library tests passed, but the embedded ZMQ
subscriber test failed its original two-second greeting deadline. Those logs
remain retained. Only a subsequent Windows run can provide platform evidence
for this source; the local deterministic test does not prove every cause of
that historical failure.

Current-thread embedded runtimes retain their synchronous execution path.
Other synchronous startup/replay/maintenance sections still require review;
this change does not make all initialization preemptible or bound its wall time.
Runtime replacement threads and their stacks also remain part of outstanding
whole-node memory accounting. No RSS/physical-disk/long-node acceptance or final
source freeze is claimed.

Validation: `cargo test --locked --all-features` completed successfully,
including 1,029 library tests (0 failed, 12 ignored; 71.06 s), all seven
embedded-node API tests (3.04 s), other enabled integration tests and doc tests.
The deliberately ignored scale/resource acceptance cases remain unrun.
Strict all-target/all-feature Clippy passed (51.14 s including build-lock wait);
format and diff checks passed. The failing inline-order baseline and passing
fixed regression logs are retained with the full command logs.
