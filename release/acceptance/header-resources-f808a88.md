# Default-redb header resource acceptance

- Gate: `header-resources`
- Commit: `f808a880e6d0536958f12b0f06b0fea153a62249`
- Source SHA-256: `aa12f0141ee8ec579f70fee9085c31bf895d949341415ef3e8e536cb2ece51f7`
- Acceptance checker SHA-256: `3cdb48ce7428823715db19f27bb562cc1aed6445a61e2fd8a4a6793fc6fa8b3e`
- Functional test status: `PASS` (17 passed, 1 ignored)
- Semantic comparison status: `PASS`
- Sustained probe status: `PASS` (3,609.35 seconds; 301,000,000 siblings)
- Restart/fault status: `PASS` (12 header-store retention tests passed, 1 ignored)
- Peak sampled RSS: `421,343,232` bytes of `536,870,912`
- Peak sampled database disk: `67,907,584` bytes of `268,435,456`
- Final-two-window median ratios (RSS, database length, allocated disk): `1.03962`, `1.00000`, `1.00000`
- Acceptance status: `PASS`

## Plateau review

The one-hour run's RSS rose once by about 37 MiB late in the workload as the
database cache warmed, then remained at that level. The prior checker compared
the full middle and final thirds and returned a 1.10330 RSS ratio, just above
the 1.10 rule. The raw run was not changed or shortened. The reviewed checker
compares the last two consecutive ten-minute windows at the same 50,000-side-
header retention phase; that yields 1.03962. The full-run 512 MiB RSS ceiling,
256 MiB database ceiling, one-hour duration, million-sibling floor, sample
coverage, restart checks and tip identity all remain enforced.

The [resource evidence manifest](resource-evidence-f808a88.sha256) binds the
301,004 raw samples, check output, test logs and Core 31 oracle outputs. The raw
directory is retained on the acceptance host at
`/data/bench/rbtc-resource-acceptance-f808c/`.
