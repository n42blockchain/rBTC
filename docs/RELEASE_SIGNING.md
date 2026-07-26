# Cross-platform release signing

Status date: 2026-07-26.

## Local signing inventory

`../n42appv2` uses an Android keystore alias named `N42` and Apple team
`CFRXH38L48`. Passwords and private material remain outside this repository.
The local keychain currently exposes Apple Development identities and an
`iPhone Distribution` identity for that team. It does **not** expose the
`Developer ID Application` identity required to sign and notarize a standalone
macOS command-line executable.

The Android JKS key, Apple Developer ID key, and Windows Authenticode
certificate are ecosystem-specific identities. Reusing one private key across
all three formats is neither supported nor desirable. “Same developer” is
implemented as the same organization/release authorization plus one common
signed manifest, while each OS retains its native signature:

| Artifact | Native trust | Common release identity |
| --- | --- | --- |
| Linux x86_64/arm64 | SHA-256 + provenance/SBOM | signed release manifest |
| macOS x86_64/arm64 | Developer ID Application, secure timestamp, notarization | signed release manifest |
| Windows x86_64 | Authenticode SHA-256 + timestamp | signed release manifest |
| Android host embedding | existing `N42` JKS signs the host APK/AAB | rBTC manifest/SBOM included in host evidence |

Apple requires Developer ID signing and notarization for directly distributed
macOS software; `notarytool` is the supported upload path. Windows uses
SignTool/Authenticode with explicit file and timestamp digest algorithms.
Cross-platform manifest signing may use an operator-controlled KMS/PKCS#11 key
or Sigstore identity bundles; GitHub artifact attestations remain additional
provenance, not a substitute for native OS signatures.

## Release gates

1. Build Linux x86_64/arm64, macOS x86_64/arm64, and Windows x86_64 from the
   same clean tag and locked dependency graph.
2. Run unit/integration tests on each native OS and verify deterministic
   rebuilds where the object format/toolchain permits.
3. Produce one canonical manifest containing tag, commit, Rust/toolchain,
   target, artifact SHA-256, SBOM SHA-256, and data/schema compatibility.
4. Sign the canonical manifest once with the release identity.
5. Apply and independently verify native macOS/Windows signatures; notarize
   macOS and retain the notary log.
6. Generate provenance attestations and publish the CycloneDX SBOM.
7. Install and cold-start every artifact on a clean supported platform.
8. Verify upgrade, rollback, snapshot compatibility, backup, and recovery
   instructions before publishing.

The signed-release P0 gate remains open until the missing Developer ID
Application and Windows signing identities are provisioned and a real tagged
matrix run publishes verifiable artifacts.
