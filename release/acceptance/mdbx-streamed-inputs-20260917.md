# MDBX streamed-input measurement (2026-09-17)

RSS acceptance: **FAILED**. Actual maintenance-load, full-scale and whole-node
production gates remain **OPEN**.

The synthetic driver now has a separately identified STREAM_INPUTS=1 mode. It
generates the same transitions one at a time through commit_connect_batch_stream,
with the same final tip and one atomic transaction for each 64/256 block batch.
Borrowed and owned-vector modes remain unchanged. STREAM_INPUTS and CONSUME_INPUTS
are mutually exclusive, persisted in manifests/reports and checked on resume;
legacy reports cannot be relabeled as streamed. This measures the storage
streaming interface already used by the execution spool, not the full node's
preparation, script workers or shared-memory admission.

Validation before measurement: the three modes produce the same UTXO/undo content
after independently reopening real stores; 11 runner evidence tests passed;
strict all-feature driver Clippy, formatting and diff checks passed.

Frozen source: b7991cb3248a01ada36a97759f1ef116defe5523. The runner froze executable
and source hashes and ran on Mac without concurrent local compilation during RSS
measurement. Workload matches the historical reduced comparison: 2M live UTXOs,
4,096 transitions, 5,000 updates each, seed batches 100,000, 1 GiB geometry,
288 undo retention, compact 55%, minimum reclaim 10%, repeat growth 50%, serial
64/256 lanes, reports every 256 transitions. The RSS limit remains 1.5.

| Batch | Peak RSS bytes | Churn checkpoint seconds | Automatic compactions |
| --- | ---: | ---: | ---: |
| 64 | 469,778,432 | 47.0346 | 0 |
| 256 | 718,831,616 | 51.7367 | 0 |

Ratio: **1.5301503156 > 1.5**. The runner exited 1, review_required. This improves
on the historical owned-vector ratio 2.1607460994 but does not pass. No rerun is
used to select a more favorable sample. Both independent-process reopens passed;
content matches both historical lanes exactly:
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`.
The compaction crash matrix passed separately; neither churn lane triggered an
automatic copy, so it does not replace the earlier actual-maintenance failure.

Final high-water allocation was 349,126,656 / 526,761,984 bytes. Remaining engine
page/allocator/mapping contributions need investigation; these numbers alone do
not identify their RSS shares. Complete preparation ownership and sustained
maintenance acceptance still need work. All old failed measurements are retained.
Evidence and databases remain in
`/Users/jieliu/Documents/n42/rBTC-storage-streamed-inputs-20260917`; only small
reports/logs/hashes were copied to session-state/2026-09-17/mdbx-streamed-inputs.
No database copy or public soak was started.

CI 35201897125/0aa971f ended with the same coverage embedded ZMQ shutdown failure;
Windows and supply-chain passed. Its failed log is retained. The AS-map repair
96ebcd1 and driver b7991cb have been pushed; their CI 35203118084 is in progress.
This measurement is not evidence that that CI or all production gates passed.
