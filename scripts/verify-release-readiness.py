#!/usr/bin/env python3
"""Require reviewed acceptance evidence for the exact release source tree.

Evidence and documentation-only commits are allowed after freezing/testing the
release-relevant source. This checks identity and completeness of reviewed
reports; it does not replace their tests or independently attest that an
operator's measurements occurred.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys


EVIDENCE_DIR = "release/acceptance"
GATES = ("admission-resources", "header-resources", "storage-replay", "public-soak")
MAX_REPORT_BYTES = 2 * 1024 * 1024
NON_RUNTIME_PREFIXES = (EVIDENCE_DIR + "/", "docs/")
NON_RUNTIME_FILES = {"README.md"}


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.PIPE)


def source_digest(root, revision):
    records = git(root, "ls-tree", "-rz", "-r", "--full-tree", revision).split(b"\0")
    # Bind every release-relevant file recursively, including file modes and
    # symlinks. Human documentation and reviewed evidence cannot change the
    # executable, dependency graph, build, workflow or acceptance tooling, so
    # those files may follow a frozen candidate without invalidating its tests.
    def relevant(row):
        if not row:
            return False
        path = row.split(b"\t", 1)[1].decode("utf-8", "surrogateescape")
        return path not in NON_RUNTIME_FILES and not path.startswith(NON_RUNTIME_PREFIXES)

    source = [row for row in records if relevant(row)]
    return hashlib.sha256(b"\0".join(source) + b"\0").hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_report(root, record):
    if not isinstance(record, dict) or set(record) != {"path", "sha256"}:
        raise ValueError("evidence must specify exactly path and sha256")
    path, digest = record["path"], record["sha256"]
    if not isinstance(path, str) or not isinstance(digest, str):
        raise ValueError("evidence identity must be text")
    relative = PurePosixPath(path)
    if (relative.is_absolute() or ".." in relative.parts or "\\" in path
            or str(relative) != path or not path.startswith(EVIDENCE_DIR + "/")):
        raise ValueError("evidence path must be canonical and inside release/acceptance")
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError("evidence path must not traverse symlinks")
    if not current.is_file() or not 0 < current.stat().st_size <= MAX_REPORT_BYTES:
        raise ValueError("report is missing, empty or larger than 2 MiB")
    payload = current.read_bytes()
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("report SHA-256 mismatch")
    # Never accept an uncommitted local report as release evidence.
    if git(root, "show", "HEAD:" + path) != payload:
        raise ValueError("report differs from the committed evidence")
    return payload.decode("utf-8")


def verify_soak(report, tested_commit):
    def field(pattern):
        matches = re.findall(pattern, report, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError("soak report has a missing or ambiguous acceptance field")
        return matches[0]

    for name in ("Duration status", "Sample coverage status", "Acceptance status"):
        if field(r"^- " + name + r": `([^`]+)`$") != "PASS":
            raise ValueError(f"soak {name} is not PASS")
    minimum = int(field(r"^- Required minimum seconds: `([0-9]+)`$"))
    if minimum < 604800:
        raise ValueError("soak finalizer must enforce a minimum of at least 604800 seconds")
    if field(r"^- Commit: `([0-9a-f]{40})`$") != tested_commit:
        raise ValueError("soak report tested a different commit")
    field(r"^- Binary SHA-256: `([0-9a-f]{64})`$")
    start, end, seconds = field(r"^- Window: `([^`]+)` through `([^`]+)` \(([0-9]+) seconds\)$")
    start, end = [datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                  for value in (start, end)]
    if int(seconds) < minimum or (end - start).total_seconds() != int(seconds):
        raise ValueError("soak must cover at least 604800 real seconds")
    if end > datetime.now(timezone.utc):
        raise ValueError("soak window ends in the future")
    for network in ("bitcoin", "testnet4"):
        field(r"^(\| " + network + r" \| .*)$")
    for name in ("Bitcoin controlled restart completions", "Testnet4 controlled restart completions",
                 "Fault scenarios completed"):
        if int(field(r"^- " + name + r": `([0-9]+)`$")) < 1:
            raise ValueError(f"soak is missing {name}")


def verify_resource_report(report, gate, tested_commit, source_sha256):
    def field(name):
        matches = re.findall(r"^- " + re.escape(name) + r": `([^`]+)`$", report, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError(f"{gate} report has a missing or ambiguous {name}")
        return matches[0]

    if field("Gate") != gate:
        raise ValueError(f"{gate} report names a different gate")
    if field("Commit") != tested_commit:
        raise ValueError(f"{gate} report tested a different commit")
    if field("Source SHA-256") != source_sha256:
        raise ValueError(f"{gate} report tested a different source digest")
    if field("Acceptance status") != "PASS":
        raise ValueError(f"{gate} acceptance is not PASS")
    required = {
        "admission-resources": (
            "Functional test status", "Optimizer differential status",
            "Resource probe status", "Recovery/fault status"),
        "header-resources": (
            "Functional test status", "Semantic comparison status",
            "Sustained probe status", "Restart/fault status"),
    }[gate]
    for name in required:
        if field(name) != "PASS":
            raise ValueError(f"{gate} {name} is not PASS")


def verify(root):
    path = root / EVIDENCE_DIR / "readiness.json"
    if path.is_symlink() or path.stat().st_size > MAX_REPORT_BYTES:
        raise ValueError("unsafe or oversized readiness manifest")
    payload = path.read_bytes()
    if git(root, "show", "HEAD:" + EVIDENCE_DIR + "/readiness.json") != payload:
        raise ValueError("readiness manifest differs from HEAD")
    data = json.loads(payload, object_pairs_hook=unique_object)
    if set(data) != {"format", "tested_commit", "source_sha256", "gates"} or data["format"] != 2:
        raise ValueError("unsupported readiness manifest")
    gates = data["gates"]
    if not isinstance(gates, dict) or set(gates) != set(GATES):
        raise ValueError("readiness must account for every required production gate")
    failures = []
    for name in GATES:
        gate = gates[name]
        if not isinstance(gate, dict) or set(gate) != {"status", "reason", "evidence"}:
            raise ValueError(f"malformed gate: {name}")
        if gate["status"] != "accepted":
            failures.append(f"{name}: {gate['reason']}")
        elif not isinstance(gate["evidence"], list) or not gate["evidence"]:
            failures.append(f"{name}: accepted gate has no evidence")
    if failures:
        raise ValueError("open release gates:\n  " + "\n  ".join(failures))
    commit = data["tested_commit"]
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("tested_commit must identify the frozen source commit")
    git(root, "merge-base", "--is-ancestor", commit, "HEAD")
    expected = source_digest(root, "HEAD")
    if source_digest(root, commit) != expected or data["source_sha256"] != expected:
        raise ValueError("acceptance source differs from the release source; repeat affected acceptance")
    # This also catches local release-relevant edits and untracked build inputs
    # when an operator runs the preflight by hand. Documentation and evidence
    # are intentionally excluded.
    status = git(root, "status", "--porcelain=v1", "--untracked-files=all", "--", ".",
                 ":(exclude)" + EVIDENCE_DIR + "/**", ":(exclude)docs/**",
                 ":(exclude)README.md")
    if status:
        raise ValueError("release-relevant working tree differs from HEAD")
    for name in GATES:
        reports = [read_report(root, record) for record in gates[name]["evidence"]]
        if name == "public-soak":
            if len(reports) != 1:
                raise ValueError("public-soak requires one canonical final report")
            verify_soak(reports[0], commit)
        elif name in ("admission-resources", "header-resources"):
            if len(reports) != 1:
                raise ValueError(f"{name} requires one canonical final report")
            verify_resource_report(reports[0], name, commit, expected)
    return commit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--print-source-digest", action="store_true")
    args = parser.parse_args()
    try:
        if args.print_source_digest:
            print(source_digest(args.repo, "HEAD"))
        else:
            print("release acceptance verified for " + verify(args.repo))
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"release readiness rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
