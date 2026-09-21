# Release gate audit — 2026-09-18

Audited commit: 79407495fbfefca61dd8f4970c7e7b0b07fd242b, before range-writer edits in development
worktree /Users/jieliu/Documents/n42/rBTC-mac-acceptance-20260912.
Exact branch CI 35388353685 completed successfully. This is not release readiness.

Actual preflight rejects admission-resources, header-resources and public-soak.
The manifest marks optimizer-budget and storage-replay accepted only for
3cf44ae68003ceb78e9f0c77042667b475091716. Its source digest is
41531cb896eba13a13fabde1170e519ab3b90c4b0940232c45125120bfdc0431;
current source digest is
44327c63bed8369b99de089a417ac335a5600dee9dad9ec5672c655bc41f1c4a.
They differ. Preflight exits on open gates before checking source identity;
the independent digest comparison reveals this additional blocker.

Remaining engineering/evidence:

1. Complete shared admission ownership, prevout/escaping allocations and finite
   resumable scheduling across peers and chain changes. Memory/spool error kinds
   are preserved, and finite unstaged memory-pressure downshifts plus complete unexecuted stage
   reuse now exist. Stages larger than the current batch still cannot progress;
   checkpoint-aware suffix preservation and post-commit recovery remain open.
2. Complete Headers/candidate resource resumption and aggregate work/byte/disk
   acceptance. Disk candidates, shared owners and atomic paths already exist;
   their whole-node concurrent, restart, cancellation and fault acceptance remains.
3. Complete startup and steady-state total memory ownership and RSS verification.
   Default 32 GiB limits registered reservations, not total RSS. Native stacks,
   node decoded/metadata/network objects, legacy copies and engine working sets
   remain outside the proven bound. Logical spool allowance is not physical quota.
4. Freeze final source and rerun/rebind affected optimizer and historical storage
   evidence. The selected 935001–963350 window is not genesis-to-tip validation.
5. Both Bitcoin and Testnet4 must catch up before the frozen 604800-second soak.
   Require sample coverage, restart/fault exercises, peer diversity, consistent
   tips, freezer rotation and RSS/disk records; no accepted current-source report.
6. Finish signed artifact rehearsal and release prerequisites below.

Storage scope: default supported release uses redb. Experimental MDBX replacement
has a separate gate. The 4M maintenance run at 9907c8e passed RSS ratio1.296615<=1.5
and recovery/content checks. Final-source160M/900000 and sustained whole-node
acceptance are not established by that run. Do not report the old reduced RSS
failure as current, or make experimental replacement a new default-release feature.
The user's broader all-gates objective still includes completing this work.

Live GitHub/local checks (metadata only, no secret values read):
- release-signing environment now EXISTS, requires a reviewer, permits main and v*.
- Environment secrets:0; repository secrets:0; available organization secrets:0.
- Workflow requires11 Apple Developer ID/notary and Windows signing secret entries.
- Local code-sign identities contain no Developer ID Application identity.
- Release workflow run list is empty; signed matrix rehearsal is not evidenced.
- Repository immutable releases enabled:true, enforced_by_owner:false.
- Correction to the earlier local audit: main run35202180108 is the FAILED
  scheduled Fuzz regression, not ordinary CI. Scheduled Dynamic analysis
  run35070441497 also failed. Actual latest main push CI34890148170 at
  b74c4e04b83fb6d5583d59fc270f57e65ffdb4a3 PASSED. These older scheduled
  failures still require triage, but must not be represented as push CI failure.
  The final release commit must itself have successful main push CI; successful
  work-branch CI does not satisfy release.yml's requirement.

Release sequence: finish code/resource recovery -> freeze source -> final-source
acceptance and catch-up -> full seven-day soak -> commit reviewed evidence-only
reports and matching readiness identities -> final exact main CI -> signed matrix
rehearsal/native artifact verification -> version/tag checks and authorized release.
Provision real organization signing credentials in parallel. No tag or publication
was performed. Old release-facing docs contain historical statuses; this audit
preserves measurements and corrects current facts without changing gate outcomes.

This audit changes no gate status and does not bind old accepted reports to new
source. The range-writer work following the audited HEAD also requires final
source identity and acceptance to be recomputed after engineering is complete.
