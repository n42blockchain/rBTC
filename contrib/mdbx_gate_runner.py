#!/usr/bin/env python3
"""Run serial storage lanes with frozen executables and auditable attempts."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
PREFIX = "RBTC_MDBX_GATE_"
GIB = 1024**3
DEFAULTS = {
    "UTXOS": 160_000_000, "BLOCKS": 900_000, "UPDATES": 5_000,
    "SEED_BATCH": 100_000, "UNDO_RETENTION": 288,
    "REPORT_INTERVAL": 10_000, "CAPACITY_BYTES": 128 * GIB,
    "COMPACT": 1, "COMPACT_PERCENT": 55, "MIN_RECLAIM_PERCENT": 10,
    "RECOMPACT_GROWTH_PERCENT": 50,
}
SELECTOR = "mdbx_mainnet_scale_churn_and_compaction_gate"
CRASH_SELECTOR = "abrupt_exit_at_every_compaction_boundary_recovers_exact_four_table_state"


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def workload(saved=None):
    values = dict(DEFAULTS if saved is None else saved)
    for key in DEFAULTS:
        raw = os.environ.get(PREFIX + key)
        if raw is not None:
            value = int(raw)
            if saved is not None and value != saved[key]:
                raise ValueError(f"resume workload mismatch: {PREFIX + key}")
            values[key] = value
    if any(value <= 0 for key, value in values.items() if key != "COMPACT"):
        raise ValueError("workload values must be positive")
    if values["COMPACT"] not in (0, 1):
        raise ValueError("COMPACT must be 0 or 1")
    if values["UPDATES"] > values["UTXOS"]:
        raise ValueError("UPDATES must not exceed UTXOS")
    for key in ("COMPACT_PERCENT", "MIN_RECLAIM_PERCENT", "RECOMPACT_GROWTH_PERCENT"):
        if values[key] > 100:
            raise ValueError(f"{key} must not exceed 100")
    for key in ("BLOCKS", "UPDATES", "UNDO_RETENTION", "REPORT_INTERVAL"):
        if values[key] > 2**32 - 1:
            raise ValueError(f"{key} exceeds u32")
    for key in ("DIR", "REPORT", "COMMIT_BATCH"):
        if PREFIX + key in os.environ:
            raise ValueError(f"unset {PREFIX + key}; the matrix manages both lanes")
    return values


def preflight(output, values, resume=False):
    ancestor = output
    while not ancestor.exists():
        ancestor = ancestor.parent
    free = shutil.disk_usage(ancestor).free
    capacity = values["CAPACITY_BYTES"]
    # Two retained ceilings, one simultaneous copy + 10%, and the store's
    # 16 GiB reserve. Planning headroom, not an immediate allocation claim.
    retained = 0
    if resume:
        for batch in (64, 256):
            directory = output / f"batch-{batch}" / "chainstate.mdbx"
            retained += sum(path.stat().st_blocks * 512 for path in directory.rglob("*")
                            if path.is_file() and not path.is_symlink())
    required = max(16 * GIB, 2 * capacity + (capacity * 11 + 9) // 10 + 16 * GIB - retained)
    result = {
        "platform": platform.platform(), "output": str(output),
        "free_bytes": free, "planning_required_free_bytes": required,
        "retained_database_allocated_bytes": retained,
        "capacity_preflight_passed": free >= required,
        "workload": values, "serial_batches": [64, 256],
        "boundary": "synthetic churn, not mainnet replay or complete MDBX acceptance",
    }
    print(json.dumps(result, indent=2), flush=True)
    return result


def source_identity(output):
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    ).split(b"\0")
    files = {}
    for raw in names:
        if not raw:
            continue
        name = os.fsdecode(raw)
        path = ROOT / name
        if path == output or output in path.parents:
            continue
        if path.is_symlink():
            files[name] = {"symlink": os.readlink(path)}
        elif path.is_file():
            files[name] = digest(path)
        else:
            files[name] = None
    return files


def build(output):
    before = source_identity(output)
    artifacts = {}
    with (output / "build.jsonl").open("w") as messages, (output / "build.log").open("w") as log:
        subprocess.run([
            "cargo", "test", "--locked", "--release", "--all-features",
            "--test", "mdbx_mainnet_scale_gate", "--test", "mdbx_compaction_crash",
            "--no-run", "--message-format=json",
        ], cwd=ROOT, stdout=messages, stderr=log, check=True)
    if before != source_identity(output):
        raise ValueError("source changed during build; use a stable checkout and a new output")
    for line in (output / "build.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row.get("reason") == "compiler-artifact" and row.get("executable"):
            artifacts[row["target"]["name"]] = Path(row["executable"])
    binaries = {}
    for name in ("mdbx_mainnet_scale_gate", "mdbx_compaction_crash"):
        frozen = output / name
        shutil.copy2(artifacts[name], frozen)
        frozen.chmod(0o555)
        binaries[name] = digest(frozen)
    write_json(output / "source-sha256.json", before)
    write_json(output / "build-environment.json", {
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True, cwd=ROOT),
        "cargo": subprocess.check_output(["cargo", "-V"], text=True, cwd=ROOT),
        "host": platform.node(), "cpu_count": os.cpu_count(),
        "build_variables": {key: os.environ.get(key) for key in (
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR")},
    })
    (output / "filesystem-before.txt").write_bytes(subprocess.check_output(["df", "-k", str(output)]))
    (output / "revision.txt").write_bytes(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT))
    (output / "worktree.txt").write_bytes(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT))
    return binaries


def peak_rss(text, system):
    pattern = (r"^\s*(\d+)\s+maximum resident set size\s*$" if system == "Darwin"
               else r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$")
    match = re.search(pattern, text, re.MULTILINE)
    if match is None or int(match[1]) <= 0:
        raise ValueError("missing or zero process peak RSS; refusing incomplete measurement")
    return int(match[1]) * (1 if system == "Darwin" else 1024)


def timed_run(binary, selector, attempt, environment, lock_fd, ignored=False):
    attempt.mkdir()
    command = ["/usr/bin/time", "-l" if platform.system() == "Darwin" else "-v",
               str(binary), "--exact", selector, "--nocapture", "--test-threads=1"]
    if ignored:
        command.append("--ignored")
    state = {"started_epoch": time.time(), "command": command, "exit_code": None}
    write_json(attempt / "attempt.json", state)
    with (attempt / "test.log").open("w") as log, (attempt / "time.txt").open("w") as timing:
        process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log,
                                   stderr=timing, start_new_session=True, pass_fds=(lock_fd,))
        try:
            code = process.wait()
        except BaseException:
            # An interrupted runner must not leave a lane working behind it.
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=30)
            except ProcessLookupError:
                pass
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    state.update(exit_code=code, finished_epoch=time.time())
    write_json(attempt / "attempt.json", state)
    if code:
        raise ValueError(f"test exited {code}; evidence: {attempt}; resume with --resume")
    # Rust's harness succeeds with zero selected tests; that is not acceptance.
    if "test result: ok. 1 passed; 0 failed;" not in (attempt / "test.log").read_text():
        raise ValueError(f"selected test did not run: {attempt}")
    state["peak_rss_bytes"] = peak_rss((attempt / "time.txt").read_text(), platform.system())
    write_json(attempt / "attempt.json", state)
    return state


def next_attempt(directory, prefix):
    index = 1
    while (directory / f"{prefix}-{index:03}").exists():
        index += 1
    return directory / f"{prefix}-{index:03}"


def validate_report(report, values, batch):
    expected = {
        "live_utxos": values["UTXOS"], "target_blocks": values["BLOCKS"],
        "updates_per_block": values["UPDATES"], "seed_batch": values["SEED_BATCH"],
        "commit_batch": batch, "undo_retention": values["UNDO_RETENTION"],
        "capacity_bytes": values["CAPACITY_BYTES"], "compact_enabled": bool(values["COMPACT"]),
        "compact_trigger_percent": values["COMPACT_PERCENT"],
        "compact_min_reclaim_percent": values["MIN_RECLAIM_PERCENT"],
        "recompact_growth_percent": values["RECOMPACT_GROWTH_PERCENT"],
    }
    if report.get("workload") != expected or not report.get("finished_epoch"):
        raise ValueError("report is incomplete or workload identity differs")
    audit = report.get("final_audit") or {}
    if (audit.get("tip_height") != values["BLOCKS"]
            or audit.get("hot_entries", 0) + audit.get("cold_entries", 0) != values["UTXOS"]
            or not 0 <= audit.get("undo_entries", -1) <= values["UNDO_RETENTION"]
            or not re.fullmatch(r"[0-9a-f]{64}", audit.get("content_sha256", ""))):
        raise ValueError("final height, live set, undo bound or content identity failed")
    if audit["high_water_bytes"] > values["CAPACITY_BYTES"]:
        raise ValueError("high-water exceeds configured capacity")
    for row in report["compactions"]:
        if row["after_free_page_bytes"] != 0 or not row["content_sha256"]:
            raise ValueError("compact-copy evidence failed")
    return audit


def attempt_measurements(lane, output, target_blocks):
    """Keep audit-only resumes out of the churn peak comparison."""
    peaks, attempts = [], []
    for path in sorted(lane.glob("attempt-*/attempt.json")):
        state = json.loads(path.read_text())
        state["evidence_directory"] = str(path.parent.relative_to(output))
        report_path = path.parent / "report.json"
        mutation = True  # Interrupted seeding may not have produced a report.
        if report_path.exists():
            state["report"] = json.loads(report_path.read_text())
            mutation = state["report"]["start_height"] < target_blocks
        state["contributes_to_churn_peak"] = mutation
        try:
            peak = peak_rss((path.parent / "time.txt").read_text(), platform.system())
            if mutation:
                peaks.append(peak)
        except (OSError, ValueError):
            state["rss_unavailable"] = True
        attempts.append(state)
    if not peaks:
        raise ValueError("no measured seeding/churn attempt; audit-only runs cannot establish peak RSS")
    return peaks, attempts


def run(output, values, manifest, lock_fd):
    for name, expected in manifest["binaries"].items():
        if digest(output / name) != expected:
            raise ValueError(f"frozen binary hash mismatch: {name}")
    environment = dict(os.environ)
    environment.update({PREFIX + key: str(value) for key, value in values.items()})
    environment["LC_ALL"] = "C"
    scratch = output / "scratch"
    scratch.mkdir(exist_ok=True)
    environment["TMPDIR"] = str(scratch)
    crash = next_attempt(output, "crash")
    timed_run(output / "mdbx_compaction_crash", CRASH_SELECTOR, crash, environment, lock_fd)
    lanes = []
    for batch in (64, 256):
        lane = output / f"batch-{batch}"
        lane.mkdir(exist_ok=True)
        attempt = next_attempt(lane, "attempt")
        environment.update({PREFIX + "DIR": str(lane / "chainstate.mdbx"),
                            PREFIX + "REPORT": str(attempt / "report.json"),
                            PREFIX + "COMMIT_BATCH": str(batch)})
        print(f"Running batch {batch}: {attempt}", flush=True)
        timed_run(output / "mdbx_mainnet_scale_gate", SELECTOR, attempt, environment, lock_fd, True)
        report = json.loads((attempt / "report.json").read_text())
        audit = validate_report(report, values, batch)
        reopen = next_attempt(lane, "reopen")
        environment[PREFIX + "REPORT"] = str(reopen / "report.json")
        timed_run(output / "mdbx_mainnet_scale_gate", SELECTOR, reopen, environment, lock_fd, True)
        reopened = json.loads((reopen / "report.json").read_text())
        if validate_report(reopened, values, batch) != audit:
            raise ValueError("fresh-process reopen changed final audit")
        peaks, attempts = attempt_measurements(lane, output, values["BLOCKS"])
        report.update(observed_peak_rss_bytes=max(peaks), attempts=attempts,
                      fresh_process_reopen_verified=True)
        write_json(lane / "report-with-rss.json", report)
        lanes.append(report)
    if lanes[0]["final_audit"]["content_sha256"] != lanes[1]["final_audit"]["content_sha256"]:
        raise ValueError("64/256 lane content digests differ")
    ratio = lanes[1]["observed_peak_rss_bytes"] / lanes[0]["observed_peak_rss_bytes"]
    full = all(values[key] == DEFAULTS[key] for key in DEFAULTS if key != "REPORT_INTERVAL")
    matrix = {"schema": 3, "lanes": lanes, "full_default_workload": full,
              "content_equal": True, "rss_256_over_64": ratio,
              "rss_budget_review_required": ratio > 1.5,
              "all_attempts_have_rss": all(not attempt.get("rss_unavailable")
                                           for lane in lanes for attempt in lane["attempts"]),
              "complete_mdbx_replacement_gate_accepted": False,
              "boundary": "scale measurements only; real replay and backend migration remain separate",
              "free_bytes_after": shutil.disk_usage(output).free}
    write_json(output / "matrix.json", matrix)
    (output / "filesystem-after.txt").write_bytes(subprocess.check_output(["df", "-k", str(output)]))
    (output / "SHA256SUMS").write_text(digest(output / "matrix.json") + "  matrix.json\n")
    print(f"Results: {output / 'matrix.json'}", flush=True)
    if ratio > 1.5:
        print("RSS ratio exceeds 1.5: batch reduction or explicit memory-budget review required.", file=sys.stderr)
        return 1
    if not matrix["all_attempts_have_rss"]:
        print("A prior attempt lacks RSS evidence; the full measurement is incomplete.", file=sys.stderr)
        return 1
    return 0


def progress(output):
    """Estimate only the remaining churn of this same live-set workload."""
    rows = []
    for batch in (64, 256):
        paths = sorted((output / f"batch-{batch}").glob("attempt-*/report.json"))
        if not paths:
            rows.append({"batch": batch, "phase": "not_started"})
            continue
        report = json.loads(paths[-1].read_text())
        checkpoints = report["checkpoints"]
        height = (report.get("final_audit") or {}).get(
            "tip_height", checkpoints[-1]["height"] if checkpoints else report["start_height"])
        phase = "complete" if report.get("finished_epoch") else ("seeding" if height == 0 else "churn")
        row = {"batch": batch, "phase": phase, "height": height,
               "live_utxos": report["workload"]["live_utxos"],
               "report": str(paths[-1]), "remaining_churn_seconds_at_recent_rate": None}
        # Use checkpoint differences: total average includes initial seeding.
        advancing = [(left, right) for left, right in zip(checkpoints, checkpoints[1:])
                     if right["height"] > left["height"] > 0
                     and right["elapsed_seconds"] > left["elapsed_seconds"]]
        if advancing and phase != "complete":
            left, right = advancing[-1]
            rate = (right["height"] - left["height"]) / (right["elapsed_seconds"] - left["elapsed_seconds"])
            row["recent_transitions_per_second"] = rate
            row["remaining_churn_seconds_at_recent_rate"] = (report["workload"]["target_blocks"] - height) / rate
        rows.append(row)
    state_path = output / "execution.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else None
    return {"lanes": rows, "execution": state, "retired": (output / "retired.json").exists(), "estimate_boundary":
            "same live-set checkpoint rate only; future compactions, final audits and unstarted lanes are not predicted"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--preflight", action="store_true", help="capacity check only; no build or writes")
    parser.add_argument("--resume", action="store_true", help="reuse frozen binaries and workload; retain attempts")
    parser.add_argument("--status", action="store_true", help="read checkpoints and same-scale remaining-time estimate")
    args = parser.parse_args()
    if args.status and (args.resume or args.preflight):
        raise ValueError("--status cannot be combined with --resume or --preflight")
    if platform.system() not in ("Darwin", "Linux"):
        raise ValueError("this runner supports Linux and macOS")
    output = args.output.resolve()
    if args.status:
        if not (output / "run.json").is_file():
            raise ValueError("no initialized run at this output")
        print(json.dumps(progress(output), indent=2))
        return 0
    if (output / "retired.json").exists() and not args.preflight:
        raise ValueError("this run was retired after database cleanup; start a new output directory")
    manifest_path = output / "run.json"
    manifest = json.loads(manifest_path.read_text()) if args.resume else None
    values = workload(manifest["workload"] if manifest else None)
    check = preflight(output, values, args.resume)
    if args.preflight:
        return 0 if check["capacity_preflight_passed"] else 1
    if not check["capacity_preflight_passed"]:
        raise ValueError("insufficient planning headroom; use a larger volume")
    if not args.resume:
        output.mkdir(parents=True, exist_ok=False)
    with (output / "runner.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("another runner already owns this output directory") from error
        if manifest is None:
            manifest = {"schema": 1, "workload": values, "host": platform.platform(),
                        "hostname": platform.node(),
                        "preflight": check, "binaries": build(output)}
            write_json(manifest_path, manifest)
        elif (manifest["host"] != platform.platform()
              or manifest.get("hostname", platform.node()) != platform.node()):
            raise ValueError("resume host/OS changed; keep this matrix on the original host")
        execution = next_attempt(output, "execution")
        execution.mkdir()
        for name in ("matrix.json", "SHA256SUMS"):
            if (output / name).exists():
                (output / name).replace(execution / ("previous-" + name))
        state = {"state": "running", "started_epoch": time.time(), "pid": os.getpid(),
                 "evidence_directory": str(execution.relative_to(output))}
        write_json(output / "execution.json", state)
        try:
            code = run(output, values, manifest, lock.fileno())
        except BaseException as error:
            state.update(state="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                         finished_epoch=time.time(), error=str(error))
            write_json(output / "execution.json", state)
            raise
        state.update(state="measured" if code == 0 else "review_required", finished_epoch=time.time())
        write_json(output / "execution.json", state)
        return code


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.CalledProcessError, KeyError) as error:
        print(f"storage gate: {error}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("storage gate interrupted; binaries and attempts retained for --resume", file=sys.stderr)
        sys.exit(130)
