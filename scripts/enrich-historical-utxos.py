#!/usr/bin/env python3
"""Rebuild exact origin metadata for the compact historical UTXO fixtures."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "tests" / "data" / "bitcoin-core-26"
SUFFIX = ".utxos.json.zst"
DEFAULT_CACHE = ROOT / "target" / "historical-utxo-metadata"
DEFAULT_API = "https://blockstream.info/api"
USER_AGENT = "rBTC-historical-fixture-builder/1"
FIXTURE_FIELDS = {
    "txid",
    "vout",
    "value_sats",
    "script_pubkey",
    "height",
    "creation_mtp",
    "is_coinbase",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=DEFAULT_API, help="Esplora API base URL")
    parser.add_argument(
        "--fallback-api",
        action="append",
        default=[],
        help="additional Esplora API base URL (repeatable)",
    )
    parser.add_argument("--cache", type=pathlib.Path, default=DEFAULT_CACHE)
    parser.add_argument("--jobs", type=int, default=8)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--verify-only",
        action="store_true",
        help="validate exact fixture metadata without rewriting files",
    )
    mode.add_argument(
        "--self-test",
        action="store_true",
        help="run offline generator and committed-fixture checks",
    )
    return parser.parse_args()


def fixture_paths() -> list[pathlib.Path]:
    return sorted(DATA.glob(f"*{SUFFIX}"))


def decode_fixture(path: pathlib.Path) -> list[dict[str, object]]:
    decoded = subprocess.run(
        ["zstd", "-q", "-d", "-c", str(path)],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    value = json.loads(decoded)
    if not isinstance(value, list):
        raise ValueError(f"{path} does not contain a JSON array")
    return value


def atomic_write(path: pathlib.Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(contents)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def cached_json(path: pathlib.Path) -> object | None:
    try:
        return json.loads(path.read_bytes())
    except FileNotFoundError:
        return None


def fetch_json(urls: list[str]) -> object:
    delay = 1.0
    for attempt in range(8):
        last_error: Exception | None = None
        for url in urls:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504):
                    raise
                retry_after = error.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
                last_error = error
            except (ConnectionError, TimeoutError, urllib.error.URLError) as error:
                last_error = error
        if attempt == 7:
            assert last_error is not None
            raise last_error
        time.sleep(delay)
        delay = min(delay * 2.0, 30.0)
    raise AssertionError("retry loop must return or raise")


def load_or_fetch(cache_path: pathlib.Path, urls: list[str]) -> object:
    cached = cached_json(cache_path)
    if cached is not None:
        return cached
    value = fetch_json(urls)
    atomic_write(
        cache_path,
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode(),
    )
    return value


def rotated_apis(apis: list[str], key: int) -> list[str]:
    offset = key % len(apis)
    return apis[offset:] + apis[:offset]


def fetch_proof(
    apis: list[str], cache: pathlib.Path, txid: str
) -> tuple[str, dict[str, object]]:
    path = cache / "proofs" / f"{txid}.json"
    urls = [
        f"{api}/tx/{txid}/merkle-proof"
        for api in rotated_apis(apis, int(txid[:8], 16))
    ]
    value = load_or_fetch(path, urls)
    if not isinstance(value, dict):
        raise ValueError(f"invalid Merkle proof for {txid}")
    return txid, value


def fetch_proofs(
    apis: list[str], cache: pathlib.Path, txids: set[str], jobs: int
) -> dict[str, dict[str, object]]:
    proofs: dict[str, dict[str, object]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        pending = {
            executor.submit(fetch_proof, apis, cache, txid): txid
            for txid in sorted(txids)
        }
        for completed, future in enumerate(
            concurrent.futures.as_completed(pending), start=1
        ):
            txid, proof = future.result()
            proofs[txid] = proof
            if completed % 1000 == 0 or completed == len(pending):
                print(f"Merkle proofs: {completed}/{len(pending)}", flush=True)
    return proofs


def fetch_block_batch(
    apis: list[str], cache: pathlib.Path, start_height: int
) -> list[dict[str, object]]:
    value = fetch_json(
        [
            f"{api}/blocks/{start_height}"
            for api in rotated_apis(apis, start_height)
        ]
    )
    if not isinstance(value, list) or not value:
        raise ValueError(f"invalid block batch at height {start_height}")
    blocks: list[dict[str, object]] = []
    for block in value:
        if not isinstance(block, dict) or type(block.get("height")) is not int:
            raise ValueError(f"invalid block metadata at height {start_height}")
        validate_block_header(block)
        blocks.append(block)
        atomic_write(
            cache / "blocks" / f"{block['height']}.json",
            json.dumps(block, separators=(",", ":"), sort_keys=True).encode(),
        )
    return blocks


def fetch_blocks(
    apis: list[str], cache: pathlib.Path, heights: set[int], jobs: int
) -> dict[int, dict[str, object]]:
    blocks: dict[int, dict[str, object]] = {}
    missing: set[int] = set()
    for height in heights:
        value = cached_json(cache / "blocks" / f"{height}.json")
        if isinstance(value, dict):
            blocks[height] = value
        else:
            missing.add(height)

    planned = set(missing)
    starts: list[int] = []
    while planned:
        start = max(planned)
        starts.append(start)
        planned.difference_update(range(max(0, start - 9), start + 1))

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        pending = [
            executor.submit(fetch_block_batch, apis, cache, start) for start in starts
        ]
        for completed, future in enumerate(
            concurrent.futures.as_completed(pending), start=1
        ):
            for block in future.result():
                height = int(block["height"])
                if height in heights:
                    blocks[height] = block
                    missing.discard(height)
            if completed % 250 == 0 or completed == len(pending):
                print(
                    f"Block metadata batches: {completed}/{len(pending)}; "
                    f"remaining heights: {len(missing)}",
                    flush=True,
                )
    if missing:
        raise ValueError(f"missing {len(missing)} required block metadata records")
    for height, block in blocks.items():
        if block.get("height") != height:
            raise ValueError(f"block cache height mismatch at {height}")
        validate_block_header(block)
    return blocks


def double_sha256(value: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(value).digest()).digest()


def validate_block_header(block: dict[str, object]) -> None:
    if not isinstance(block.get("id"), str) or not isinstance(
        block.get("previousblockhash"), str
    ) or not isinstance(block.get("merkle_root"), str):
        raise ValueError(f"invalid block hashes at height {block.get('height')}")
    if not all(
        type(block.get(field)) is int
        for field in ("version", "timestamp", "bits", "nonce")
    ):
        raise ValueError(f"invalid block fields at height {block.get('height')}")
    try:
        version = int(block["version"])
        timestamp = int(block["timestamp"])
        bits = int(block["bits"])
        nonce = int(block["nonce"])
        header = (
            (version & 0xFFFF_FFFF).to_bytes(4, "little")
            + bytes.fromhex(str(block["previousblockhash"]))[::-1]
            + bytes.fromhex(str(block["merkle_root"]))[::-1]
            + timestamp.to_bytes(4, "little")
            + bits.to_bytes(4, "little")
            + nonce.to_bytes(4, "little")
        )
    except (KeyError, OverflowError, ValueError) as error:
        raise ValueError(f"invalid block header at height {block.get('height')}") from error
    block_hash = double_sha256(header)[::-1].hex()
    if block_hash != block["id"]:
        raise ValueError(f"block header hash mismatch at height {block.get('height')}")
    exponent = bits >> 24
    mantissa = bits & 0x007F_FFFF
    if mantissa == 0 or bits & 0x0080_0000:
        raise ValueError(f"invalid proof-of-work target at height {block.get('height')}")
    target = (
        mantissa >> (8 * (3 - exponent))
        if exponent <= 3
        else mantissa << (8 * (exponent - 3))
    )
    if int(block_hash, 16) > target:
        raise ValueError(f"insufficient proof of work at height {block.get('height')}")


def merkle_root(txid: str, position: int, branch: list[object]) -> str:
    current = bytes.fromhex(txid)[::-1]
    index = position
    for encoded in branch:
        if not isinstance(encoded, str):
            raise ValueError(f"non-string Merkle sibling for {txid}")
        sibling = bytes.fromhex(encoded)[::-1]
        current = (
            double_sha256(sibling + current)
            if index & 1
            else double_sha256(current + sibling)
        )
        index >>= 1
    return current[::-1].hex()


def exact_metadata(
    txid: str,
    proof: dict[str, object],
    blocks: dict[int, dict[str, object]],
) -> tuple[int, int, bool]:
    height = proof.get("block_height")
    position = proof.get("pos")
    branch = proof.get("merkle")
    if type(height) is not int or height <= 0:
        raise ValueError(f"invalid creation height for {txid}")
    if type(position) is not int or position < 0 or not isinstance(branch, list):
        raise ValueError(f"invalid Merkle position for {txid}")
    block = blocks[height]
    if merkle_root(txid, position, branch) != block.get("merkle_root"):
        raise ValueError(f"Merkle proof mismatch for {txid}")
    lineage = [
        blocks[height - offset] for offset in range(min(11, height) + 1)
    ]
    for child, parent in zip(lineage, lineage[1:]):
        if child.get("previousblockhash") != parent.get("id"):
            raise ValueError(f"origin chain discontinuity for {txid}")
    timestamps = sorted(int(ancestor["timestamp"]) for ancestor in lineage[1:])
    creation_mtp = timestamps[len(timestamps) // 2]
    if lineage[1].get("mediantime") != creation_mtp:
        raise ValueError(f"origin MTP mismatch for {txid}")
    return height, creation_mtp, position == 0


def encode_fixture(path: pathlib.Path, records: list[dict[str, object]]) -> None:
    encoded = json.dumps(records, separators=(",", ":")).encode()
    source_descriptor, source_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".json", dir=path.parent
    )
    source = pathlib.Path(source_name)
    output: pathlib.Path | None = None
    try:
        with os.fdopen(source_descriptor, "wb") as uncompressed:
            uncompressed.write(encoded)
            uncompressed.flush()
            os.fsync(uncompressed.fileno())
        output_descriptor, output_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".zst", dir=path.parent
        )
        output = pathlib.Path(output_name)
        os.close(output_descriptor)
        subprocess.run(
            ["zstd", "-q", "-19", "-f", str(source), "-o", str(output)],
            check=True,
        )
        os.replace(output, path)
    finally:
        for leftover in (source, output):
            if leftover is not None and leftover.exists():
                leftover.unlink()


def run_self_test() -> None:
    genesis = {
        "id": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
        "height": 0,
        "version": 1,
        "previousblockhash": "00" * 32,
        "merkle_root": (
            "4a5e1e4baab89f3a32518a88c31bc87f"
            "618f76673e2cc77ab2127b7afdeda33b"
        ),
        "timestamp": 1_231_006_505,
        "mediantime": 1_231_006_505,
        "bits": 0x1D00FFFF,
        "nonce": 2_083_236_893,
    }
    validate_block_header(genesis)
    tampered = dict(genesis)
    tampered["nonce"] = int(tampered["nonce"]) + 1
    try:
        validate_block_header(tampered)
    except ValueError:
        pass
    else:
        raise AssertionError("tampered genesis header unexpectedly passed")

    paths = fixture_paths()
    if len(paths) != 5:
        raise AssertionError(f"expected 5 historical UTXO fixtures, found {len(paths)}")
    record_count = 0
    coinbase_count = 0
    for path in paths:
        for record in decode_fixture(path):
            if set(record) != FIXTURE_FIELDS:
                raise AssertionError(f"{path.name} has a non-canonical record schema")
            if type(record["height"]) is not int or int(record["height"]) <= 0:
                raise AssertionError(f"{path.name} has a normalized origin height")
            if (
                type(record["creation_mtp"]) is not int
                or int(record["creation_mtp"]) <= 0
            ):
                raise AssertionError(f"{path.name} has a normalized origin MTP")
            if type(record["is_coinbase"]) is not bool:
                raise AssertionError(f"{path.name} has a non-boolean coinbase flag")
            if type(record["vout"]) is not int or int(record["vout"]) < 0:
                raise AssertionError(f"{path.name} has an invalid output index")
            if type(record["value_sats"]) is not int or int(record["value_sats"]) < 0:
                raise AssertionError(f"{path.name} has an invalid output value")
            try:
                txid = bytes.fromhex(str(record["txid"]))
                bytes.fromhex(str(record["script_pubkey"]))
            except ValueError as error:
                raise AssertionError(f"{path.name} has invalid hex data") from error
            if len(txid) != 32:
                raise AssertionError(f"{path.name} has an invalid transaction ID")
            record_count += 1
            coinbase_count += int(bool(record["is_coinbase"]))
    if (record_count, coinbase_count) != (23_331, 8):
        raise AssertionError(
            "historical UTXO fixture totals changed: "
            f"{record_count} records, {coinbase_count} coinbase origins"
        )
    print(
        f"Self-test passed: {record_count} records, "
        f"{coinbase_count} coinbase origins",
        flush=True,
    )


def main() -> None:
    args = parse_args()
    if args.jobs < 1 or args.jobs > 32:
        raise ValueError("--jobs must be between 1 and 32")
    if args.self_test:
        run_self_test()
        return
    apis = list(dict.fromkeys(api.rstrip("/") for api in [args.api, *args.fallback_api]))
    if not all(api.startswith(("http://", "https://")) for api in apis):
        raise ValueError("every Esplora API must be an HTTP(S) base URL")
    paths = fixture_paths()
    if len(paths) != 5:
        raise ValueError(f"expected 5 historical UTXO fixtures, found {len(paths)}")
    fixtures = {path: decode_fixture(path) for path in paths}
    txids = {
        str(record["txid"]) for records in fixtures.values() for record in records
    }
    print(f"Unique origin transactions: {len(txids)}", flush=True)
    proofs = fetch_proofs(apis, args.cache, txids, args.jobs)
    origin_heights = {int(proof["block_height"]) for proof in proofs.values()}
    required_heights = {
        height - offset
        for height in origin_heights
        for offset in range(min(11, height) + 1)
    }
    blocks = fetch_blocks(apis, args.cache, required_heights, args.jobs)

    for path, records in fixtures.items():
        changed = 0
        for record in records:
            txid = str(record["txid"])
            height, creation_mtp, is_coinbase = exact_metadata(
                txid, proofs[txid], blocks
            )
            previous_height = int(record["height"])
            if previous_height != 0 and previous_height != height:
                raise ValueError(
                    f"{path.name}: retained BIP68 height mismatch for {txid}: "
                    f"{previous_height} != {height}"
                )
            expected = (height, creation_mtp, is_coinbase)
            actual = (
                previous_height,
                int(record["creation_mtp"]),
                bool(record["is_coinbase"]),
            )
            if actual != expected:
                changed += 1
            record["height"] = height
            record["creation_mtp"] = creation_mtp
            record["is_coinbase"] = is_coinbase
        if args.verify_only:
            if changed:
                raise ValueError(f"{path.name} has {changed} normalized metadata records")
        else:
            encode_fixture(path, records)
            print(f"Rewrote {path.name}: {len(records)} records", flush=True)


if __name__ == "__main__":
    main()
