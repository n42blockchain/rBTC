# Embedded ZMQ startup and delivery deadlines

CI35322681853 at15c99f8 passed Linux (including coverage) and supply-chain checks,
but Windows failed host_zmq_endpoint_publishes_an_executed_block at its generic
five-second frame wait. Its library suite passed. The CI log does not prove
which startup operation or frame caused the delay; the failure remains recorded.

On Mac, delaying the fixture peer's accept by six seconds reproduced the same
frame timeout at5.67s. The test had begun its delivery clock before peer startup
and execution completed. Moving its existing execution-status wait before the
notification reads passed with the same delay at7.29s. Startup uses the existing
20-second hang guard; greeting/frame/shutdown limits and exact notification
content/sequence checks remain unchanged. Status, lifecycle and peer termination
are included if execution fails to reach the expected height.

The delay is now an explicit regression case alongside the undelayed case.
All eight embedded tests passed together in7.28s; strict locked all-target,
all-feature Clippy and formatting/diff checks passed. Only the integration test
and this report changed; no production behavior or resource threshold changed.
A new Windows CI run is still needed. This does not close staged recovery,
whole-node resource, final-source or public-network acceptance gates.
