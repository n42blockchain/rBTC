"""Evidence validation tests; no large databases or network required."""

import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("gate", Path(__file__).with_name("mdbx_gate_runner.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def fixture():
    values = dict(gate.DEFAULTS)
    return {
        "finished_epoch": 100, "start_height": 0,
        "workload": {
            "live_utxos": 160000000, "target_blocks": 900000, "updates_per_block": 5000,
            "seed_batch": 100000, "commit_batch": 64, "undo_retention": 288,
            "capacity_bytes": 128 * gate.GIB, "compact_enabled": True,
            "compact_trigger_percent": 55, "compact_min_reclaim_percent": 10,
            "recompact_growth_percent": 50,
        },
        "checkpoints": [], "compactions": [],
        "final_audit": {
            "tip_height": 900000, "hot_entries": 160000000, "cold_entries": 0,
            "undo_entries": 288, "content_sha256": "a" * 64, "high_water_bytes": 60 * gate.GIB,
        },
    }, values


class GateTests(unittest.TestCase):
    def test_resuming_completed_lanes_preserves_the_original_rss_failure(self):
        report, values = fixture()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for batch, peak in ((64, 100), (256, 200)):
                attempt = root / f'batch-{batch}/attempt-001'
                attempt.mkdir(parents=True)
                old = copy.deepcopy(report)
                old['workload']['commit_batch'] = batch
                (attempt / 'report.json').write_text(json.dumps(old))
                (attempt / 'attempt.json').write_text('{}')
                (attempt / 'time.txt').write_text(f'Maximum resident set size (kbytes): {peak}\n')

            def audited_run(_binary, selector, attempt, environment, _lock_fd, _ignored=False):
                attempt.mkdir()
                if selector == gate.CRASH_SELECTOR:
                    return {}
                batch = int(environment[gate.PREFIX + 'COMMIT_BATCH'])
                audit = copy.deepcopy(report)
                audit['workload']['commit_batch'] = batch
                audit['start_height'] = values['BLOCKS']
                peak = 1000 if batch == 64 else 250
                (attempt / 'report.json').write_text(json.dumps(audit))
                (attempt / 'attempt.json').write_text('{}')
                (attempt / 'time.txt').write_text(f'Maximum resident set size (kbytes): {peak}\n')
                return {'peak_rss_bytes': peak * 1024}

            with patch.object(gate, 'timed_run', side_effect=audited_run), \
                    patch.object(gate.platform, 'system', return_value='Linux'), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code = gate.run(root, values, {'binaries': {}}, 0)
            self.assertEqual(code, 1)
            matrix = json.loads((root / 'matrix.json').read_text())
            self.assertEqual(matrix['rss_256_over_64'], 2)

    def test_audit_only_resume_cannot_dilute_a_failed_churn_rss_ratio(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for batch, churn_peak, audit_peak in ((64, 100, 1000), (256, 200, 250)):
                lane = root / f"batch-{batch}"
                for index, height, peak in ((1, 0, churn_peak), (2, 900000, audit_peak)):
                    attempt = lane / f"attempt-{index:03}"
                    attempt.mkdir(parents=True)
                    (attempt / "attempt.json").write_text('{}')
                    (attempt / "report.json").write_text(json.dumps({"start_height": height}))
                    (attempt / "time.txt").write_text(f'Maximum resident set size (kbytes): {peak}\n')
            with patch.object(gate.platform, "system", return_value="Linux"):
                small, _ = gate.attempt_measurements(root / 'batch-64', root, 900000)
                large, attempts = gate.attempt_measurements(root / 'batch-256', root, 900000)
            self.assertEqual(max(large) / max(small), 2)
            self.assertFalse(attempts[1]['contributes_to_churn_peak'])
            self.assertEqual(len(attempts), 2)

    def test_audit_only_without_original_measurements_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            attempt = root / 'batch-64/attempt-001'
            attempt.mkdir(parents=True)
            (attempt / 'attempt.json').write_text('{}')
            (attempt / 'report.json').write_text('{"start_height": 900000}')
            (attempt / 'time.txt').write_text('Maximum resident set size (kbytes): 100\n')
            with self.assertRaisesRegex(ValueError, 'no measured seeding/churn'):
                gate.attempt_measurements(root / 'batch-64', root, 900000)

    def test_rss_units_and_missing_measurements(self):
        self.assertEqual(gate.peak_rss("\tMaximum resident set size (kbytes): 123\n", "Linux"), 125952)
        self.assertEqual(gate.peak_rss("  123 maximum resident set size\n", "Darwin"), 123)
        for raw in ("", "0 maximum resident set size\n", "123 page reclaims\n"):
            with self.assertRaises(ValueError):
                gate.peak_rss(raw, "Darwin")

    def test_resume_inherits_every_parameter_and_rejects_changes(self):
        saved = dict(gate.DEFAULTS, UTXOS=20000, UPDATES=100)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(gate.workload(saved), saved)
        for name, value in (("UTXOS", "20001"), ("UPDATES", "99"), ("COMMIT_BATCH", "64")):
            with patch.dict(os.environ, {gate.PREFIX + name: value}, clear=True):
                with self.assertRaises(ValueError):
                    gate.workload(saved)

    def test_incomplete_or_wrong_state_never_passes(self):
        report, values = fixture()
        self.assertEqual(gate.validate_report(report, values, 64), report["final_audit"])
        for section, key, value in (
            ("final_audit", "tip_height", 899999),
            ("final_audit", "hot_entries", 159999999),
            ("final_audit", "undo_entries", 289),
            ("final_audit", "content_sha256", ""),
            ("final_audit", "high_water_bytes", 129 * gate.GIB),
            ("workload", "commit_batch", 256),
            ("workload", "updates_per_block", 4999),
        ):
            altered = copy.deepcopy(report)
            altered[section][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                gate.validate_report(altered, values, 64)
        report["finished_epoch"] = None
        with self.assertRaises(ValueError):
            gate.validate_report(report, values, 64)

    def test_preflight_is_read_only_and_counts_copy_space(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "absent"
            with contextlib.redirect_stdout(io.StringIO()):
                result = gate.preflight(output, gate.DEFAULTS)
            self.assertFalse(output.exists())
            self.assertEqual(result["planning_required_free_bytes"], 443240624948)

    def test_status_excludes_seeding_from_rate(self):
        report, _ = fixture()
        report.update(finished_epoch=None, final_audit=None)
        report["checkpoints"] = [
            {"height": 0, "elapsed_seconds": 1000},
            {"height": 10000, "elapsed_seconds": 2000},
            {"height": 20000, "elapsed_seconds": 2500},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            attempt = output / "batch-64" / "attempt-001"
            attempt.mkdir(parents=True)
            (attempt / "report.json").write_text(json.dumps(report))
            status = gate.progress(output)
        lane = status["lanes"][0]
        self.assertEqual(lane["recent_transitions_per_second"], 20)
        self.assertEqual(lane["remaining_churn_seconds_at_recent_rate"], 44000)
        self.assertEqual(status["lanes"][1]["phase"], "not_started")

    def test_zero_selected_tests_is_not_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = root / "empty-test"
            fake.write_text("#!/bin/sh\necho 'test result: ok. 0 passed; 0 failed;'\n")
            fake.chmod(0o755)
            with (root / "lock").open("w") as lock:
                with self.assertRaisesRegex(ValueError, "selected test did not run"):
                    gate.timed_run(fake, "missing", root / "attempt", os.environ.copy(), lock.fileno())


if __name__ == "__main__":
    unittest.main()
