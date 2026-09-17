# Pinned zstd allocation-failure fixes

Source: crates.io `zstd-sys 2.1.0+zstd.1.5.7`, package SHA-256
`0ef0a8027ec3ee71300ab3bcbcd0393f434aa72b91ca6d635a39941deae8eea0`.
Upstream packaging commit: `648acb476da66b4bb856043c9b8ea1fdbfe69093`,
path `zstd-safe/zstd-sys`. Existing licenses are retained. Registry cache markers
and the package's independent lockfile are omitted; the root and fuzz lockfiles
pin this local source. No codec version upgrade is included. Trailing whitespace and extra EOF blank
lines were normalized in wasm-shim/stdio.h, zdict.h, and the CMake files
GetZstdLibraryVersion.cmake, lib/CMakeLists.txt and lib/cmake_uninstall.cmake.in.

rBTC's bounded native encoder intentionally returns NULL when shared memory
admission fails. Failure injection exposed a null memset and cleanup paths
that bypassed custom allocation callbacks. Local changes:

- `common/allocations.h`: custom calloc zeroes only a successful allocation.
- `common/pool.c`: retain the custom allocator before any fallible construction.
- `compress/zstdmt_compress.c`: buffer/context pools retain their allocator
  before allocating child arrays; failed construction now uses custom free.
- The job table reports its rounded size even when allocation fails, and
  partial context destruction skips a missing job table.
- `build.rs`: reject system-library overrides, which could bypass these fixes,
  and track vendored C/header changes for rebuilds.

Regression coverage lives in `src/archive.rs`: single-thread and 2/3/4-worker
encoding, exact output comparison, allocation-by-allocation denial, early
context drop, writer failure and file destination preservation. The wrapper
checks that native destruction returns every tracked allocation in debug/test
builds. Its fixed tracking table and output buffer are admitted before allocation.

This patch does not account for OS thread stacks, synchronization internals,
allocator overhead or resident pages. Native heap admission is not an RSS cap.
