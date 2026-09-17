# Shared archive handle-array admission

ArchiveBlocks now owns immutable shared storage for the returned handle array.
Cloning an archive or LedgerBlockBatch result shares both the array and payloads.
A fixed-capacity builder admits array bytes plus shared-storage metadata before
allocation and refuses pushes beyond its declared capacity. Publication moves
the admitted Vec into shared storage without reallocating the array.

Archive selection bounds capacity by requested count, available manifest records,
and the minimum four-byte framing cost under the caller's byte limit. Visitor
scans create no result-array allocation. Ledger cross-segment reads reserve their
destination array before reading source segments; source and destination arrays
retain separate admission throughout their overlap. The owned iterator retains
array storage until drop and yields shared block handles. Individual escaped
blocks keep their own payload reservation after the array is released.

Node replay prevalidation consumes this shared collection directly. Its output
PrevalidatedBlock Vec is still separate, unadmitted node metadata. This change
covers archive and ledger result arrays, including their clones and iterators;
it does not cover every node vector or newly decoded object.

Validation: final all-feature library tests passed: 1,060 passed, zero failed,
12 ignored (76.46 s). Final strict locked all-target/all-feature Clippy passed
(47.77 s); fuzz locked all-target Clippy passed (15.24 s). Formatting and diff
checks passed. An initial new test incorrectly expected coalesced retained
ranges; it was corrected to the existing per-segment contract before the final
full pass. Regression verifies one-byte array admission
shortfall, capacity overflow/exhaustion, shared array pointers, partially consumed
iterator lifetime, zero-allocation empty collections and escaped block behavior.
Existing payload and real node replay tests now verify array release separately
from final payload release. Cross-segment ledger tests exercise admitted merging.

Remaining gates include manifest parse/returned-metadata ownership, multithreaded
native compression, newly serialized network payloads, decoded Bitcoin objects,
legacy Vec serving, node batch metadata and pressure resumption, total startup
RSS and physical disk, and required final-source/long-running node acceptance.
The 32 GiB default remains a shared reservation limit, not a proven RSS ceiling.
