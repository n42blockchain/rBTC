# Security policy

rBTC is still a pre-release validating node. It must not be trusted with
mainnet private keys or used as the sole control protecting funds. The wallet
surface is deliberately watch-only and accepts external signatures.

## Supported code

Until the first signed release, only the current `main` branch is supported for
security fixes. Historical commits, locally modified builds, unsigned
artifacts, and the experimental MDBX feature are not supported production
configurations. After signed releases begin, this section must be updated in
the same pull request that publishes or retires a release line.

## Reporting a vulnerability

Use the repository Security tab's private **Report a vulnerability** form.
Do not open a public issue for a suspected consensus, remote denial-of-service,
authentication, snapshot, wallet, signing, or supply-chain vulnerability. If
the private form is unavailable, contact the `n42blockchain` organization owner
through GitHub without including exploit details and request a private channel.

Include the affected commit or version, network and configuration, expected and
observed behavior, reproduction steps, and whether malformed peer or API input
is required. Remove authentication tokens, descriptors, IP addresses, private
keys, database contents, and other operator secrets from the report.

Maintainers will reproduce the issue, classify consensus and data-integrity
impact before convenience impact, prepare regression coverage, and coordinate
disclosure after a fix is available. A report is not considered resolved merely
because malformed input was rejected; restart safety and durable-state residue
must also be checked where applicable.

## Public operational incidents

For an already public chain incident or active exploitation, stop affected
nodes cleanly when possible, preserve the complete data directory and logs, and
follow [docs/DISASTER_RECOVERY.md](docs/DISASTER_RECOVERY.md). Do not delete or
rewrite evidence before the failure boundary is understood.
