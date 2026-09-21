#!/usr/bin/env python3
"""Evidence rejection tests for the scoped header resource checker."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("checker", Path(__file__).with_name("check-header-resource-probe.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def evidence():
    base = dict(mode="retained", pid=10, tip="hash", rss_kib=100, database_bytes=4096,
                database_allocated_bytes=4096, active_entries=2501, retained_entries=52501,
                side_entries=50000, elapsed_micros=0, phase="active-chain")
    rows = [base]
    for i in range(1, 601):
        rows.append(dict(base, phase="before-retention", elapsed_micros=i * 1_000_000,
                         side_entries=52000, retained_entries=54501))
        rows.append(dict(base, phase="siblings", elapsed_micros=i * 1_000_000))
    rows.append(dict(phase="completed-churn", elapsed_micros=600_000_000, generated_siblings=1_200_000))
    rows.append(dict(base, phase="reopen-same-process"))
    rows.append(dict(base, phase="reopen-before-load", pid=11))
    rows.append(dict(base, phase="reopen-fresh-process", pid=11))
    return rows


class CheckTest(unittest.TestCase):
    def check(self, rows, seconds=600):
        return checker.check(rows, seconds=seconds, max_rss=200_000, max_disk=8192)

    def test_complete_scoped_evidence(self):
        self.assertTrue(self.check(evidence())["passed"])

    def test_short_or_missing_rss_never_passes(self):
        rows = evidence()
        self.assertFalse(self.check(rows, seconds=601)["passed"])
        rows[10]["rss_kib"] = None
        self.assertFalse(self.check(rows)["passed"])

    def test_plateau_growth_is_rejected_below_absolute_ceiling(self):
        rows = evidence()
        for row in rows:
            if row.get("phase") == "siblings" and row["elapsed_micros"] >= 400_000_000:
                row["rss_kib"] = 150
        result = self.check(rows)
        self.assertFalse(result["passed"])
        self.assertIn("rss_kib plateau growth exceeds allowance", result["errors"])

    def test_false_reopen_and_missing_samples_are_rejected(self):
        rows = evidence()
        rows[-1]["pid"] = 10
        self.assertFalse(self.check(rows)["passed"])
        rows = evidence()
        del rows[1:40]
        self.assertFalse(self.check(rows)["passed"])

    def test_both_disk_measures_and_retention_are_enforced(self):
        for key, value in [("database_bytes", 16384), ("database_allocated_bytes", 16384),
                           ("side_entries", 52001)]:
            rows = copy.deepcopy(evidence())
            rows[1][key] = value
            self.assertFalse(self.check(rows)["passed"])


if __name__ == "__main__":
    unittest.main()
