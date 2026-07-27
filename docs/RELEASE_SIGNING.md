# Cross-platform release signing

Status date: 2026-07-27.

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
The common release identity is the repository/organization-bound GitHub OIDC
identity recorded in a Sigstore bundle for the canonical manifest. This is a
short-lived workflow identity rather than another exportable private key. It
binds every platform artifact to the same repository, workflow, commit, and
tag, while the native Apple and Windows identities still bind the executable
to the platform trust store. GitHub artifact attestations are additional
provenance, not a substitute for native OS signatures.

## Implemented workflow

`.github/workflows/release.yml` now fails closed before building unless all
native signing credentials are present in the protected `release-signing`
environment. A semantic `v*` tag must point at the workflow commit. Manual runs
exercise the identical signed matrix but do not publish.

The matrix:

- reproducibly builds Linux x86_64 twice and builds native Linux arm64;
- uses the same locked default production feature set on every release target;
  the experimental MDBX feature remains covered by ordinary all-feature CI but
  is not silently enabled in supported release binaries;
- imports the base64 PKCS#12 Developer ID key into an ephemeral macOS keychain,
  requires its certificate OU/team to be `CFRXH38L48`, applies hardened-runtime
  signing and a secure timestamp, submits each signed ZIP with `notarytool`,
  retains the full notary log, and runs `codesign` plus Gatekeeper verification;
- imports the base64 Windows PFX only in memory/temporary runner storage,
  verifies its approved subject and private-key presence, signs with SHA-256,
  obtains an RFC3161 SHA-256 timestamp, and verifies with SignTool;
- generates a CycloneDX 1.5 SBOM and per-platform signed provenance bundles;
- generates a deterministic, strictly ordered `RELEASE-MANIFEST.tsv` containing
  the tag, commit, exact Rust version, source epoch, root data schema v3, target,
  native trust type, byte length, and SHA-256 of all ten required release
  assets;
- verifies the manifest before and after producing its offline Sigstore bundle,
  uploads a complete draft release, and publishes only after every gate passes.

The manifest generator accepts no symlinks, requires the exact supported
platform set, and uses fixed record ordering. `scripts/test-release-manifest.sh`
is run in ordinary CI and proves both a valid assembly and rejection after
asset tampering.

## Protected environment inputs

The `release-signing` GitHub environment must restrict tag deployment to
release operators and provide:

| Secret | Required form |
| --- | --- |
| `MACOS_CERTIFICATE_P12` | base64 Developer ID Application PKCS#12 |
| `MACOS_CERTIFICATE_PASSWORD` | PKCS#12 password |
| `MACOS_SIGNING_IDENTITY` | exact `Developer ID Application: ...` name |
| `APPLE_API_KEY_P8` | raw App Store Connect API private-key PEM |
| `APPLE_API_KEY_ID` / `APPLE_API_ISSUER_ID` | notary API identifiers |
| `APPLE_TEAM_ID` | exactly `CFRXH38L48` |
| `WINDOWS_CERTIFICATE_PFX` | base64 Authenticode PFX |
| `WINDOWS_CERTIFICATE_PASSWORD` | PFX password |
| `WINDOWS_CERTIFICATE_SUBJECT` | approved N42 certificate-subject fragment |
| `WINDOWS_TIMESTAMP_URL` | operator-approved HTTPS RFC3161 endpoint |

Private material is removed in `finally`/trap cleanup and is never written to
the repository or uploaded as evidence. Repository release immutability must
also be enabled in GitHub settings; the workflow follows the required
draft-upload-publish sequence so all assets are attached before that release
becomes immutable.

Consumers verify local bytes first:

```bash
scripts/verify-release-manifest.sh RELEASE-MANIFEST.tsv .
sha256sum -c SHA256SUMS
```

They then verify the repository-bound signature/provenance with the GitHub CLI:

```bash
gh attestation verify RELEASE-MANIFEST.tsv -R OWNER/rBTC
gh attestation verify rbtcd-x86_64-unknown-linux-gnu -R OWNER/rBTC
```

Native verification remains mandatory: `codesign --verify --deep --strict`
plus Gatekeeper on macOS, and `signtool verify /pa /all` on Windows.

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

The automation and local preflight are complete. The signed-release P0 gate
remains open only until the missing Developer ID Application and Windows
signing identities are provisioned, release immutability is enabled, and one
real tagged matrix run publishes and clean-host verifies the artifacts.
