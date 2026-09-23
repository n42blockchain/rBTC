#!/usr/bin/env python3
"""Check the sustained retained-header component of the finite release gate."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def check(rows, *, seconds, max_rss, max_disk, growth=1.10):
    errors = []
    samples = [row for row in rows if row.get("phase") in {
        "active-chain", "before-retention", "siblings", "reopen-same-process",
        "reopen-before-load", "reopen-fresh-process"}]
    for row in samples:
        for key in ("rss_kib", "database_bytes", "database_allocated_bytes", "pid"):
            if type(row.get(key)) is not int or row[key] <= 0:
                errors.append(f"missing/invalid {key}")
    if errors or not samples:
        return {"passed": False, "errors": errors or ["no samples"]}
    churn = [r for r in samples if r["phase"] in {"before-retention", "siblings"}]
    completed = [r for r in rows if r.get("phase") == "completed-churn"]
    fresh = [r for r in samples if r["phase"] == "reopen-fresh-process"]
    initial = [r for r in samples if r["phase"] == "active-chain"]
    same = [r for r in samples if r["phase"] == "reopen-same-process"]
    before_load = [r for r in samples if r["phase"] == "reopen-before-load"]
    if not churn or any(len(group) != 1 for group in (completed, fresh, initial, same, before_load)):
        return {"passed": False, "errors": ["incomplete workload/reopen evidence"]}
    duration = completed[0]["elapsed_micros"] / 1_000_000
    if duration < seconds or churn[-1]["elapsed_micros"] / 1_000_000 < seconds:
        errors.append("insufficient measured duration")
    if completed[0]["generated_siblings"] < 1_000_000:
        errors.append("fewer than one million generated siblings")
    if any(r["mode"] != "retained" for r in churn):
        errors.append("requires retained workload")
    times = [r["elapsed_micros"] / 1_000_000 for r in churn]
    gaps = [b - a for a, b in zip([0] + times, times)]
    if any(gap < 0 or gap > 10 for gap in gaps) or duration - times[-1] > 10:
        errors.append("nonmonotonic samples or sampling gap above ten seconds")
    for row in churn:
        ceiling = 52_000 if row["phase"] == "before-retention" else 50_000
        if not 0 <= row["side_entries"] <= ceiling:
            errors.append("side-header retention ceiling exceeded")
        if row["retained_entries"] != row["active_entries"] + row["side_entries"]:
            errors.append("inconsistent retained count")
        if row["pid"] != initial[0]["pid"] or row["tip"] != initial[0]["tip"]:
            errors.append("churn changed process or active tip")
    if (same[0]["pid"] != initial[0]["pid"]
            or before_load[0]["pid"] != fresh[0]["pid"]
            or same[0]["retained_entries"] != churn[-1]["retained_entries"]
            or same[0]["tip"] != initial[0]["tip"]):
        errors.append("same-process reopen or pre-load identity mismatch")
    if (fresh[0]["pid"] == initial[0]["pid"] or fresh[0]["tip"] != initial[0]["tip"]
            or fresh[0]["retained_entries"] != churn[-1]["retained_entries"]):
        errors.append("fresh-process reopen identity/count mismatch")
    peak_rss = max(r["rss_kib"] * 1024 for r in samples)
    peak_disk = max(max(r["database_bytes"], r["database_allocated_bytes"]) for r in samples)
    if peak_rss > max_rss:
        errors.append("sampled RSS exceeds allowance")
    if peak_disk > max_disk:
        errors.append("sampled disk exceeds allowance")
    # Check the final two ten-minute windows at the same retention phase.
    # Comparing the full middle/final thirds can mistake late redb page-cache
    # warm-up for continuing growth when the memory curve settles afterwards.
    window_seconds = max(1, seconds // 6)
    plateau_end = duration
    plateau_split = plateau_end - window_seconds
    plateau_start = plateau_split - window_seconds
    steady = [r for r in churn if r["phase"] == "siblings" and r["side_entries"] == 50_000]
    middle = [r for r in steady if plateau_start <= r["elapsed_micros"] / 1e6 < plateau_split]
    final = [r for r in steady if plateau_split <= r["elapsed_micros"] / 1e6 <= plateau_end]
    ratios = {}
    if min(len(middle), len(final)) < 10:
        errors.append("insufficient plateau samples")
    else:
        for key in ("rss_kib", "database_bytes", "database_allocated_bytes"):
            ratios[key] = statistics.median(r[key] for r in final) / statistics.median(r[key] for r in middle)
            if ratios[key] > growth:
                errors.append(f"{key} plateau growth exceeds allowance")
    return {"passed": not errors, "errors": sorted(set(errors)), "duration_seconds": duration,
            "generated_siblings": completed[0]["generated_siblings"], "samples": len(samples),
            "peak_sampled_rss_bytes": peak_rss, "peak_sampled_disk_bytes": peak_disk,
            "plateau_window_seconds": window_seconds,
            "plateau_median_ratios": ratios, "fresh_reopen_rss_bytes": fresh[0]["rss_kib"] * 1024}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    parser.add_argument("--minimum-seconds", type=int, default=3600)
    parser.add_argument("--max-rss-mib", type=int, required=True)
    parser.add_argument("--max-disk-mib", type=int, required=True)
    args = parser.parse_args()
    if min(args.minimum_seconds, args.max_rss_mib, args.max_disk_mib) <= 0:
        parser.error("all allowances must be positive")
    raw = args.samples.read_bytes()
    result = check([json.loads(line) for line in raw.splitlines()], seconds=args.minimum_seconds,
                   max_rss=args.max_rss_mib * 1024**2, max_disk=args.max_disk_mib * 1024**2)
    result.update(scope="canonical-header-retention-component", component_acceptance_passed=result["passed"],
                  samples_sha256=hashlib.sha256(raw).hexdigest(), minimum_seconds=args.minimum_seconds,
                  max_rss_bytes=args.max_rss_mib * 1024**2, max_disk_bytes=args.max_disk_mib * 1024**2,
                  limitation="combine with the required semantic, network-path and fault tests")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
