# Streamed snapshot index publication (2026-09-17)

Total build memory, RSS, physical disk and production acceptance: **OPEN**.

Index construction previously retained its location records, MPHF, packed
slot table, fingerprint table and a further Vec containing the entire encoded
index. It also encoded the complete fingerprint sidecar into another Vec.
The main index and both fingerprint publication paths now stream through a
fixed 64 KiB buffer. Buffering precedes SHA-256 updates and file writes, avoiding
one hash operation per small encoded integer. The trailing digest and reported
byte length cover exactly the successfully written body bytes.

The existing Vec MPHF encoder remains available; the new fallible streaming
encoder emits identical bytes and propagates short-write failures. Index
headers remain a small fixed-size buffer. Location records and the occupied
bitmap are dropped once slot assignment finishes; the packed table is dropped
after index publication, before sidecar publication.

`snapshot::atomic_write_with` reuses the existing same-directory unique-file,
file-sync, rename and directory-sync publication sequence for streamed output.
The original byte-slice API delegates to it. An encoder error before rename
leaves an existing destination unchanged and removes its temporary file.
Publication remains atomic per file; the optional sidecar is a separate file.

Tests compare streaming MPHF and sidecar bytes with their existing Vec encoders,
exercise short writes and an injected encoder failure after more than one
buffer has reached disk, verify the old destination and temporary cleanup, and
write/verify an 8 MiB body with bounded chunks and the exact digest trailer.
Existing index lookup, corruption, identity and real overlay rebase tests pass.

The scan still retains whole coin groups for canonical vout sorting and the
full group-location list. MPHF construction and packed/fingerprint tables still
scale with input and lack complete node budget admission. The fixed output
buffer is not yet connected to a complete builder memory/disk owner. No claim
is made that this alone closes startup/build or maintenance RSS constraints;
large-data bounded construction and final-source performance acceptance remain.

Validation: all-feature library suite 1,033 passed, 0 failed, 12 ignored
(69.79 s); strict all-target/all-feature Clippy passed (18.84 s). Prior-source
CI 35198913385/396b045 completed successfully on Windows, Linux (including 90%
coverage), and supply-chain checks. It does not validate this new source.

After the final equivalent header-field simplification, all 14 focused index
tests passed (0.45 s); formatting and diff checks also passed. No new RSS or
long-node/public acceptance run was started.
