#!/usr/bin/env python3
"""Check the finite Linux admission resource probe used by the release gate."""
import argparse
import hashlib
import json
from pathlib import Path


def check(rows, *, max_rss, max_clone_delta, max_seed_micros=30_000_000,
          max_query_micros=2_000_000, max_clone_micros=1_000_000,
          max_admission_micros=2_000_000, max_reconcile_micros=30_000_000):
    errors = []
    if len(rows) != 3:
        errors.append("requires exactly three fresh-process samples")
    required = {
        "entries", "simultaneous_clones", "cluster_entries", "cluster_queries",
        "rss_before_clones_kib", "rss_with_clones_kib", "rss_before_reconcile_kib",
        "rss_after_reconcile_kib", "seed_micros", "query_micros", "clone_micros",
        "admission_micros", "reconcile_micros", "reconciled_removed",
        "retained_bytes_after_admission", "allocator", "network",
    }
    retained = set()
    peak_rss = 0
    peak_clone_delta = 0
    for row in rows:
        if not isinstance(row, dict) or not required.issubset(row):
            errors.append("sample is missing required fields")
            continue
        if (row["entries"], row["simultaneous_clones"], row["cluster_entries"],
                row["cluster_queries"], row["allocator"], row["network"]) != (
                    256, 8, 16, 50, "mimalloc", "regtest"):
            errors.append("sample does not use the canonical 256x8 mimalloc workload")
        for key in ("rss_before_clones_kib", "rss_with_clones_kib",
                    "rss_before_reconcile_kib", "rss_after_reconcile_kib"):
            if type(row[key]) is not int or row[key] <= 0:
                errors.append(f"missing/invalid {key}")
            else:
                peak_rss = max(peak_rss, row[key] * 1024)
        if type(row["rss_before_clones_kib"]) is int and type(row["rss_with_clones_kib"]) is int:
            peak_clone_delta = max(
                peak_clone_delta,
                max(0, row["rss_with_clones_kib"] - row["rss_before_clones_kib"]) * 1024)
        for key, ceiling in (
            ("seed_micros", max_seed_micros), ("query_micros", max_query_micros),
            ("clone_micros", max_clone_micros), ("admission_micros", max_admission_micros),
            ("reconcile_micros", max_reconcile_micros),
        ):
            if type(row[key]) is not int or row[key] < 0 or row[key] > ceiling:
                errors.append(f"{key} exceeds the finite workload allowance")
        if row["reconciled_removed"] != 0:
            errors.append("reconciliation changed the canonical retained workload")
        if type(row["retained_bytes_after_admission"]) is not int or row["retained_bytes_after_admission"] <= 0:
            errors.append("invalid retained byte accounting")
        else:
            retained.add(row["retained_bytes_after_admission"])
    if len(retained) > 1:
        errors.append("retained byte accounting differs across fresh processes")
    if peak_rss > max_rss:
        errors.append("sampled RSS exceeds allowance")
    if peak_clone_delta > max_clone_delta:
        errors.append("clone RSS delta exceeds allowance")
    return {
        "passed": not errors,
        "errors": sorted(set(errors)),
        "samples": len(rows),
        "peak_sampled_rss_bytes": peak_rss,
        "peak_clone_rss_delta_bytes": peak_clone_delta,
        "retained_bytes": next(iter(retained)) if len(retained) == 1 else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    parser.add_argument("--max-rss-mib", type=int, default=512)
    parser.add_argument("--max-clone-delta-mib", type=int, default=64)
    args = parser.parse_args()
    if min(args.max_rss_mib, args.max_clone_delta_mib) <= 0:
        parser.error("allowances must be positive")
    raw = args.samples.read_bytes()
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    result = check(rows, max_rss=args.max_rss_mib * 1024**2,
                   max_clone_delta=args.max_clone_delta_mib * 1024**2)
    result.update(
        scope="canonical-admission-256x8-linux",
        samples_sha256=hashlib.sha256(raw).hexdigest(),
        max_rss_bytes=args.max_rss_mib * 1024**2,
        max_clone_delta_bytes=args.max_clone_delta_mib * 1024**2,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
