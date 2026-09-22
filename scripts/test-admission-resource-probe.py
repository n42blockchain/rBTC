#!/usr/bin/env python3
"""Rejection tests for the canonical admission resource checker."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "checker", Path(__file__).with_name("check-admission-resource-probe.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def sample():
    return {
        "entries": 256, "simultaneous_clones": 8, "cluster_entries": 16,
        "cluster_queries": 50, "allocator": "mimalloc", "network": "regtest",
        "rss_before_clones_kib": 30_000, "rss_with_clones_kib": 31_000,
        "rss_before_reconcile_kib": 31_000, "rss_after_reconcile_kib": 31_000,
        "seed_micros": 3_000_000, "query_micros": 1_000, "clone_micros": 100,
        "admission_micros": 20_000, "reconcile_micros": 3_000_000,
        "reconciled_removed": 0, "retained_bytes_after_admission": 14_429_043,
    }


class AdmissionCheckerTests(unittest.TestCase):
    def check(self, rows):
        return checker.check(rows, max_rss=512 * 1024**2,
                             max_clone_delta=64 * 1024**2)

    def test_three_canonical_samples_pass(self):
        self.assertTrue(self.check([sample(), sample(), sample()])["passed"])

    def test_wrong_shape_missing_rss_and_nondeterministic_accounting_fail(self):
        rows = [sample(), sample(), sample()]
        rows[0]["entries"] = 128
        rows[1]["rss_with_clones_kib"] = None
        rows[2]["retained_bytes_after_admission"] += 1
        self.assertFalse(self.check(rows)["passed"])

    def test_timing_and_memory_ceilings_fail_closed(self):
        for key, value in (("seed_micros", 30_000_001),
                           ("rss_with_clones_kib", 600_000)):
            rows = [sample(), sample(), sample()]
            rows[0][key] = value
            self.assertFalse(self.check(copy.deepcopy(rows))["passed"])

    def test_wrong_sample_count_or_reconciliation_change_fails(self):
        self.assertFalse(self.check([sample(), sample()])["passed"])
        rows = [sample(), sample(), sample()]
        rows[0]["reconciled_removed"] = 1
        self.assertFalse(self.check(rows)["passed"])


if __name__ == "__main__":
    unittest.main()
