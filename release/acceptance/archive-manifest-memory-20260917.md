# Bounded shared archive manifest admission

File manifest readers now admit both temporary parsing work and the immutable
returned result before reading/allocating JSON. The source length remains capped
at 16 MiB. Temporary allowance is conservatively eight times input bytes plus
64 KiB for source, parser scratch, error formatting and allocation-growth
overlap. The pinned serde_json 1.0.151 slice parser borrows unescaped strings and
uses a reusable scratch Vec for escapes and ignored nested values; error strings
can also allocate. This is an allocation allowance, not an RSS measurement.

Custom digest deserialization rejects lengths above 64 bytes before copying the
output String. Piece deserialization allocates a fixed 261-slot array and rejects
an additional entry before growing that array. Hex/content validation remains
separate, preserving non-ASCII rejection and legacy records_bytes defaults.
Unknown fields retain their prior ignored-field behavior. Parsed output admission
covers all 261 digest slots, bounded strings, shared storage and its counters.

ArchiveManifest is now an immutable Arc-backed identity. Clones share the fields
and reservation. Its public read-only fields remain available through Deref;
ArchiveManifestFields supplies the wire representation. File-read result leases
survive function return and aliases, while JSON/parser leases end after parsing.
Metadata bytes, field order and equality semantics remain unchanged. Construction
from caller-owned fields and generic serde deserialization remain unbound
compatibility paths; generated writer metadata still requires separate admission.

Validation: all-feature library tests passed (1,062 passed, zero failed,
12 ignored, 72.65 s). Final strict locked all-target/all-feature Clippy passed
(14.11 s); fuzz locked all-target Clippy passed (19.37 s). Formatting and diff
checks passed. Regressions cover one-byte admission shortfall,
wire-identical serialization, shared digest/array pointers, final-alias refund,
128 KiB malicious digest, 262-piece rejection, escaped digests, and budget denial
before reading a declared 16 MiB missing payload. Existing decoder/scratch tests
now include simultaneous manifest ownership in their exact pressure limits.

This does not close generated-write metadata, multithreaded compression,
new network serialization, decoded block/node metadata, legacy serving copies,
resumable memory-pressure scheduling, startup/RSS, physical disk, or final-source
and sustained whole-node acceptance gates.
