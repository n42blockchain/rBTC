# Archive writer metadata admission

Both file writers now obtain result metadata, fixed JSON-output scratch and piece
hashing scratch admission before creating compression scratch or starting native
compression. The normal writer reserves before records_sha256 is constructed;
the prefix writer does so after its first verification pass and before encoding.
Budget failure leaves the existing destination untouched and refunds temporary
disk admission without creating a compression file.

The writer uses a fixed 261-slot digest array. It hashes at most that many pieces
and checks for excess bytes without allocating an extra digest. The immutable
returned manifest retains its result reservation through aliases, just like
file-read manifests. Prefix rewriting uses the same publication helper.

JSON serialization writes into an admitted 32 KiB Vec whose Write implementation
rejects overflow before growth. Generated schema values fit below
512 + 261 * 67 bytes (bounded digest strings, scalar values and fixed keys),
checked using maximum scalar widths and all digest slots. Thus no growing
serde_json::to_vec buffer is needed on file publication paths. Destination
creation follows complete compression, source validation, JSON serialization and
container size checks. Existing ledger fsync/rename ordering is unchanged.

Validation: all-feature library tests passed (1,064 passed, zero failed,
12 ignored, 72.00 s). Final focused writer tests passed (two tests, 0.01 s);
final strict locked all-target/all-feature Clippy passed (12.84 s); fuzz locked
all-target Clippy passed (18.06 s). Formatting and diff checks passed.
Tests cover one-byte total writer-pressure
shortfall, unchanged destination/no scratch residue/refund, result alias lifetime,
byte-identical full and prefix output, maximum generated schema size and rejected
JSON-buffer growth. Existing archive/ledger durability regressions remain enabled.

Native multithreaded encoder allocations and thread stacks still need admission;
this metadata change does not bound those. In-memory compatibility encoders and
caller-owned manifest fields are still unbound. Node decoded/metadata objects,
new network serialization, legacy serving copies, pressure resumption, startup
RSS, physical disk and final-source/sustained acceptance gates remain open.
