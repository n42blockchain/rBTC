#!/usr/bin/env python3
"""Synthetic fixtures only; these reports are never release evidence."""

from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("readiness", Path(__file__).with_name("verify-release-readiness.py"))
readiness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(readiness)


class ReleaseReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rbtc-readiness-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Release Test")
        self.git("config", "user.email", "test@example.invalid")
        (self.root / "source.rs").write_text("// frozen synthetic fixture\n")
        scripts = self.root / "scripts"
        scripts.mkdir(exist_ok=True)
        (scripts / "public-network-soak-monitor.sh").write_text("# sampling monitor v1\n")
        for name in ("check-admission-resource-probe.py", "check-header-resource-probe.py"):
            (scripts / name).write_text("# synthetic checker\n")
        self.commit("test: freeze synthetic source")
        self.tested = self.git("rev-parse", "HEAD").decode().strip()
        self.directory = self.root / readiness.EVIDENCE_DIR
        self.directory.mkdir(parents=True)
        self.manifest = self.directory / "readiness.json"
        self.report = self.directory / "soak.md"
        end = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=1)
        start = end - timedelta(seconds=604800)
        stamp = lambda value: value.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.report.write_text(
            "# Synthetic validator fixture, never release evidence\n"
            "- Required minimum seconds: `604800`\n"
            "- Duration status: `PASS`\n- Sample coverage status: `PASS`\n"
            f"- Window: `{stamp(start)}` through `{stamp(end)}` (604800 seconds)\n"
            f"- Commit: `{self.tested}`\n- Binary SHA-256: `{'0' * 64}`\n"
            "| bitcoin | synthetic |\n| testnet4 | synthetic |\n"
            "- Bitcoin controlled restart completions: `1`\n"
            "- Testnet4 controlled restart completions: `1`\n"
            "- Fault scenarios completed: `1`\n- Acceptance status: `PASS`\n")
        other = self.directory / "resource.md"
        other.write_text("Synthetic reviewed resource fixture, never release evidence.\n")
        self.resource_reports = {}
        source_sha256 = readiness.source_digest(self.root, "HEAD")
        for gate, fields in {
            "admission-resources": (
                "Functional test status", "Optimizer differential status",
                "Resource probe status", "Recovery/fault status"),
            "header-resources": (
                "Functional test status", "Semantic comparison status",
                "Sustained probe status", "Restart/fault status"),
        }.items():
            path = self.directory / (gate + ".md")
            path.write_text(
                "# Synthetic resource fixture, never release evidence\n"
                f"- Gate: `{gate}`\n- Commit: `{self.tested}`\n" +
                f"- Source SHA-256: `{source_sha256}`\n" +
                f"- Acceptance checker SHA-256: `{hashlib.sha256((scripts / ('check-' + gate.replace('-resources', '-resource-probe') + '.py')).read_bytes()).hexdigest()}`\n" +
                "".join(f"- {field}: `PASS`\n" for field in fields) +
                "- Acceptance status: `PASS`\n")
            self.resource_reports[gate] = path
        self.data = {"format": 4, "tested_commit": self.tested,
                     "source_sha256": source_sha256,
                     "source_impact": None,
                     "gates": {name: {"status": "accepted", "reason": "Synthetic test only",
                                      "evidence": [self.record(
                                          self.report if name == "public-soak" else
                                          self.resource_reports.get(name, other))]}
                               for name in readiness.GATES}}
        self.save()

    def git(self, *args):
        return readiness.git(self.root, *args)

    def commit(self, subject):
        assert subject.isascii(), "commit titles must be English"
        self.git("add", ".")
        self.git("-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                 "commit", "-q", "--allow-empty", "-m", subject)

    def record(self, path):
        return {"path": path.relative_to(self.root).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def save(self):
        self.manifest.write_text(json.dumps(self.data))
        self.commit("test: record synthetic evidence")

    def reject(self, message):
        with self.assertRaisesRegex((ValueError, subprocess.CalledProcessError), message):
            readiness.verify(self.root)

    def test_evidence_only_commits_preserve_frozen_identity(self):
        self.assertEqual(readiness.verify(self.root), self.tested)
        (self.directory / "notes.md").write_text("Additional synthetic review.\n")
        self.commit("test: append synthetic review")
        self.assertEqual(readiness.verify(self.root), self.tested)

    def test_changed_source_or_workflow_invalidates_evidence(self):
        (self.root / "release.yml").write_text("changed release behavior\n")
        self.commit("test: change fixture workflow")
        self.reject("source impact review")

    def test_changed_runtime_source_invalidates_evidence(self):
        (self.root / "source.rs").write_text("// changed runtime fixture\n")
        self.commit("test: change runtime fixture")
        self.reject("source impact review")

    def test_sampling_tool_delta_requires_explicit_impact_review(self):
        monitor = self.root / "scripts" / "public-network-soak-monitor.sh"
        monitor.write_text("# sampling monitor v2\n")
        self.commit("fix: tighten sampling monitor parsing")
        self.reject("impact review")

    def test_reviewed_sampling_tool_delta_preserves_original_report_identity(self):
        monitor = self.root / "scripts" / "public-network-soak-monitor.sh"
        base_digest = self.data["source_sha256"]
        base_blob = self.git("rev-parse", self.tested + ":scripts/public-network-soak-monitor.sh").decode().strip()
        monitor.write_text("# sampling monitor v2\n")
        self.commit("fix: tighten sampling monitor parsing")
        target = self.git("rev-parse", "HEAD").decode().strip()
        target_digest = readiness.source_digest(self.root, "HEAD")
        review = {"format": 1, "base_commit": self.tested, "base_source_sha256": base_digest,
                  "target_commit": target, "target_source_sha256": target_digest,
                  "changed_source_paths": ["scripts/public-network-soak-monitor.sh"],
                  "base_blob": base_blob,
                  "target_blob": self.git("rev-parse", "HEAD:scripts/public-network-soak-monitor.sh").decode().strip(),
                  "impact": "sampling-tool-only",
                  "validation": "monitor-parser-tests-pass", "validation_commit": target,
                  "validation_run_id": "123456789"}
        impact_path = self.directory / "sampling-tool-impact.json"
        impact_path.write_text(json.dumps(review, sort_keys=True) + "\n")
        self.data["source_impact"] = self.record(impact_path)
        self.save()
        self.assertEqual(readiness.verify(self.root), self.tested)

    def test_sampling_tool_review_cannot_cover_another_runtime_change(self):
        monitor = self.root / "scripts" / "public-network-soak-monitor.sh"
        monitor.write_text("# sampling monitor v2\n")
        (self.root / "source.rs").write_text("// runtime changed too\n")
        self.commit("fix: change monitor and runtime")
        self.reject("impact review")

    def test_documentation_only_commit_preserves_frozen_identity(self):
        documentation = self.root / "docs" / "policy.md"
        documentation.parent.mkdir()
        documentation.write_text("Synthetic policy clarification.\n")
        self.commit("docs: clarify synthetic policy")
        self.assertEqual(readiness.verify(self.root), self.tested)

    def test_acceptance_harness_changes_do_not_change_runtime_source_identity(self):
        scripts = self.root / "scripts"
        scripts.mkdir(exist_ok=True)
        checker = scripts / "check-header-resource-probe.py"
        validator = scripts / "verify-release-readiness.py"
        checker.write_text("# original synthetic checker\n")
        validator.write_text("# original synthetic validator\n")
        self.commit("test: add synthetic acceptance harness")
        frozen = readiness.source_digest(self.root, "HEAD")
        checker.write_text("# revised synthetic checker\n")
        validator.write_text("# revised synthetic validator\n")
        self.commit("test: revise synthetic acceptance harness")
        self.assertEqual(readiness.source_digest(self.root, "HEAD"), frozen)

    def test_dirty_source_is_rejected(self):
        (self.root / "source.rs").write_text("// uncommitted change\n")
        self.reject("differs from HEAD")

    def test_untracked_source_is_rejected(self):
        (self.root / "untracked.rs").write_text("// untracked build input\n")
        self.reject("differs from HEAD")

    def test_dirty_documentation_does_not_change_the_frozen_identity(self):
        documentation = self.root / "docs" / "operator-note.md"
        documentation.parent.mkdir()
        documentation.write_text("Uncommitted synthetic clarification.\n")
        self.assertEqual(readiness.verify(self.root), self.tested)

    def test_dirty_evidence_is_rejected(self):
        self.report.write_text(self.report.read_text() + "uncommitted\n")
        self.reject("SHA-256 mismatch")

    def test_open_missing_and_empty_gates(self):
        gate = self.data["gates"]["header-resources"]
        gate["status"] = "open"
        self.save()
        self.reject("open release gates")
        gate["status"] = "accepted"
        gate["evidence"] = []
        self.save()
        self.reject("no evidence")
        del self.data["gates"]["header-resources"]
        self.save()
        self.reject("every required production gate")

    def test_old_manifest_format_is_rejected(self):
        self.data["format"] = 3
        self.save()
        self.reject("unsupported readiness manifest")

    def test_duplicate_json_key(self):
        self.manifest.write_text(self.manifest.read_text().replace('"format": 4', '"format": 4, "format": 4'))
        self.commit("test: duplicate fixture field")
        self.reject("duplicate JSON key")

    def test_wrong_source_hash(self):
        self.data["source_sha256"] = "0" * 64
        self.save()
        self.reject("source differs")

    def test_report_identity_and_paths(self):
        record = self.data["gates"]["public-soak"]["evidence"][0]
        original = record.copy()
        for path in ("/tmp/report", "release/acceptance/../report", "release/acceptance//soak.md", "source.rs"):
            with self.subTest(path=path):
                record["path"] = path
                self.save()
                self.reject("evidence path")
        record.update(original)
        record["sha256"] = "0" * 64
        self.save()
        self.reject("SHA-256 mismatch")

    def test_symlink_report_and_parent(self):
        self.report.unlink()
        self.report.symlink_to("resource.md")
        self.commit("test: link fixture report")
        self.reject("symlinks")
        linked = self.directory / "linked"
        linked.symlink_to(self.directory, target_is_directory=True)
        self.data["gates"]["public-soak"]["evidence"][0]["path"] = "release/acceptance/linked/resource.md"
        self.save()
        self.reject("symlinks")

    def test_invalid_soak_cannot_be_accepted_by_a_checkbox(self):
        original = self.report.read_text()
        mutations = [
            original.replace("Acceptance status: `PASS`", "Acceptance status: `INCOMPLETE`"),
            original.replace("Sample coverage status: `PASS`", "Sample coverage status: `INCOMPLETE`"),
            original.replace("(604800 seconds)", "(1 seconds)"),
            original.replace("Required minimum seconds: `604800`", "Required minimum seconds: `1`"),
            original.replace("- Required minimum seconds: `604800`\n", ""),
            original.replace(self.tested, "0" * 40),
            original.replace("| testnet4 | synthetic |\n", ""),
            original.replace("Fault scenarios completed: `1`", "Fault scenarios completed: `0`"),
            original + "- Acceptance status: `PASS`\n",
        ]
        for index, content in enumerate(mutations):
            with self.subTest(index=index):
                self.report.write_text(content)
                self.data["gates"]["public-soak"]["evidence"] = [self.record(self.report)]
                self.save()
                self.reject("soak")

    def test_invalid_resource_report_cannot_be_accepted_by_a_checkbox(self):
        path = self.resource_reports["admission-resources"]
        original = path.read_text()
        for content in (
            original.replace("Resource probe status: `PASS`", "Resource probe status: `FAIL`"),
            original.replace(f"Commit: `{self.tested}`", f"Commit: `{'0' * 40}`"),
            original.replace(f"Source SHA-256: `{self.data['source_sha256']}`",
                             f"Source SHA-256: `{'0' * 64}`"),
            original.replace("Gate: `admission-resources`", "Gate: `header-resources`"),
            original.replace("- Recovery/fault status: `PASS`\n", ""),
        ):
            path.write_text(content)
            self.data["gates"]["admission-resources"]["evidence"] = [self.record(path)]
            self.save()
            self.reject("admission-resources")

    def test_resource_report_must_bind_the_acceptance_checker(self):
        checker = self.root / "scripts" / "check-header-resource-probe.py"
        checker.write_text("# changed checker\n")
        self.commit("test: change synthetic acceptance checker")
        self.reject("checker SHA-256")


if __name__ == "__main__":
    unittest.main()
