"""Streaming, atomic day collection and normalization without remote dependencies."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import tempfile
import time
import uuid
from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from atlaspump import __version__
from atlaspump.contracts import (
    ID_STRATEGY_VERSION,
    NORMALIZATION_VERSION,
    SCHEMA_VERSION,
    canonical_observation_id,
    logical_event_id,
    provider_position,
    source_event_id,
)
from atlaspump.decompressor import iter_jsonl_zst
from atlaspump.downloader import sha256_file
from atlaspump.event_classifier import classify_event
from atlaspump.manifests import read_manifest, write_manifest_atomic

COLLECTION = "COLLECTION"
NORMALIZATION = "NORMALIZATION"
STATUSES = {"COMPLETE", "PARTIAL", "MISSING"}
CONTRACT_STATUSES = {"SATISFIED", "NOT_SATISFIED", "NOT_EVALUABLE"}
USABILITY_STATUSES = {"VALID", "LIMITED", "INVALID"}

OBSERVATION_SCHEMA = pa.schema(
    [
        ("source_event_id", pa.string()),
        ("canonical_observation_id", pa.string()),
        ("logical_event_id", pa.string()),
        ("event_type", pa.string()),
        ("protocol_scope", pa.string()),
        ("signature", pa.string()),
        ("instruction_index", pa.int64()),
        ("event_index", pa.int64()),
        ("slot", pa.int64()),
        ("block_height", pa.int64()),
        ("provider_block", pa.string()),
        ("source_partition", pa.string()),
        ("source_cursor", pa.string()),
        ("raw_reference", pa.string()),
        ("token_mint", pa.string()),
        ("wallet", pa.string()),
        ("sol_amount", pa.string()),
        ("token_amount", pa.string()),
        ("schema_version", pa.string()),
        ("normalization_version", pa.string()),
        ("event_id_legacy", pa.string()),
    ]
)


def _relative(root: Path, path: Path) -> str:
    return str(path.relative_to(root))


def _env() -> str:
    return f"{platform.system()}-{platform.release()}-python-{platform.python_version()}"


def _git_commit() -> str:
    return os.environ.get("GIT_COMMIT", "unknown")


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
        temporary = Path(stream.name)
        with source.open("rb") as input_stream:
            shutil.copyfileobj(input_stream, stream, length=1024 * 1024)
    temporary.replace(destination)


def _archive(input_root: Path, day: date, hour: int) -> Path:
    return input_root / day.strftime("%Y/%m/%d") / f"{hour:02d}.jsonl.zst"


def _manifest_path(root: Path, kind: str, day: date) -> Path:
    return root / "manifests" / kind.lower() / f"date={day.isoformat()}" / f"{kind.lower()}_manifest.json"


def _published(path: Path) -> bool:
    manifest = read_manifest(path)
    return bool(manifest and manifest.get("publication_status") == "LOCAL_COMPLETE")


def collect_day(root: Path, input_root: Path, day: date, hours: Iterable[int], resume: bool = False) -> Path:
    """Copy local fixture/replay archives into immutable raw storage and publish a manifest."""
    hours = list(hours)
    manifest_path = _manifest_path(root, COLLECTION, day)
    if _published(manifest_path):
        if resume:
            return manifest_path
        raise FileExistsError(f"Refusing to overwrite published collection: {manifest_path}")
    started = time.monotonic()
    run_id = f"collection-{uuid.uuid4()}"
    raw_dir = root / "raw" / "pumpapi" / "replay" / f"date={day.isoformat()}"
    checkpoint = root / "tmp" / f"collection-{day.isoformat()}.checkpoint.json"
    partitions: list[dict[str, Any]] = []
    missing: list[str] = []
    counts: Counter[str] = Counter()
    for hour in hours:
        partition = f"{hour:02d}"
        source = _archive(input_root, day, hour)
        if not source.is_file() or source.stat().st_size == 0:
            missing.append(partition)
            continue
        target = raw_dir / "archives" / source.name
        if target.exists():
            if sha256_file(target) != sha256_file(source):
                raise FileExistsError(f"Raw archive exists with a different hash: {target}")
        else:
            _atomic_copy(source, target)
        received = invalid = duplicates = 0
        seen: set[str] = set()
        for line in iter_jsonl_zst(target):
            if line.payload is None:
                invalid += 1
                continue
            source_id = source_event_id("pumpapi", partition, str(line.line_number), line.raw)
            duplicates += source_id in seen
            seen.add(source_id)
            received += 1
        counts.update(received=received, invalid=invalid, duplicates=duplicates, bytes=target.stat().st_size)
        item = {"partition": partition, "path": _relative(root, target), "sha256": sha256_file(target), "bytes": target.stat().st_size, "received": received, "invalid": invalid, "duplicates": duplicates}
        partitions.append(item)
        write_manifest_atomic(checkpoint, {"capture_run_id": run_id, "last_checkpoint": partition, "partitions": partitions})
    coverage = "COMPLETE" if not missing and len(partitions) == len(hours) else ("PARTIAL" if partitions else "MISSING")
    manifest = {
        "manifest_id": f"collection-{day.isoformat()}-{run_id}", "manifest_type": COLLECTION,
        "publication_status": "LOCAL_COMPLETE", "capture_run_id": run_id, "provider": "pumpapi", "mode": "REPLAY",
        "requested_window": {"start": f"{day.isoformat()}T00:00:00Z", "end": f"{day.fromordinal(day.toordinal()+1).isoformat()}T00:00:00Z"},
        "covered_window": {"partitions": [item["partition"] for item in partitions]}, "source_partitions": partitions,
        "missing_partitions": missing, "adapter_version": __version__, "id_strategy_version": ID_STRATEGY_VERSION,
        "input_request_hash": hashlib.sha256(f"pumpapi:{day}:{hours}".encode()).hexdigest(), "checkpoints": _relative(root, checkpoint),
        "counters": dict(counts), "coverage_status": coverage,
        "contract_status": "SATISFIED" if coverage == "COMPLETE" else "NOT_SATISFIED",
        "usability_status": "VALID" if coverage == "COMPLETE" else ("LIMITED" if partitions else "INVALID"),
        "code_commit": _git_commit(), "environment_fingerprint": _env(), "created_at": datetime.now(timezone.utc).isoformat(), "elapsed_seconds": time.monotonic() - started,
    }
    write_manifest_atomic(manifest_path, manifest)
    return manifest_path


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _decimal(value: Any) -> str | None:
    return None if value is None else str(value)


def _row(payload: dict[str, Any], raw: str, partition: str, line_number: int, config: dict[str, Any]) -> dict[str, Any]:
    source_id = source_event_id("pumpapi", partition, str(line_number), raw)
    event_type, _ = classify_event(payload, config)
    mint = None if payload.get("mint") is None else str(payload["mint"])
    slot, height, provider_block = provider_position(payload)
    instruction = _integer(payload.get("instructionIndex", payload.get("instruction_index")))
    event = _integer(payload.get("innerInstructionIndex", payload.get("eventIndex", payload.get("event_index"))))
    pool = None if payload.get("pool") is None else str(payload["pool"])
    scopes = {"pump": "PUMPFUN", "pump-amm": "PUMPSWAP"}
    scope = scopes.get(pool, "OTHER") if pool is not None else "OTHER"
    return {"source_event_id": source_id, "canonical_observation_id": canonical_observation_id(source_id), "logical_event_id": logical_event_id(payload, event_type, mint), "event_type": event_type, "protocol_scope": scope, "signature": None if payload.get("signature") is None else str(payload["signature"]), "instruction_index": instruction, "event_index": event, "slot": slot, "block_height": height, "provider_block": provider_block, "source_partition": partition, "source_cursor": str(line_number), "raw_reference": f"{partition}:{line_number}", "token_mint": mint, "wallet": None if payload.get("txSigner") is None else str(payload["txSigner"]), "sol_amount": _decimal(payload.get("solAmount")), "token_amount": _decimal(payload.get("tokenAmount")), "schema_version": SCHEMA_VERSION, "normalization_version": NORMALIZATION_VERSION, "event_id_legacy": None}


def normalize_day(root: Path, day: date, collection_manifest: Path, config: dict[str, Any], resume: bool = False) -> Path:
    """Normalize only the immutable raw archives referenced by a collection manifest."""
    manifest_path = _manifest_path(root, NORMALIZATION, day)
    if _published(manifest_path):
        if resume:
            return manifest_path
        raise FileExistsError(f"Refusing to overwrite published normalization: {manifest_path}")
    collection = read_manifest(collection_manifest)
    if not collection or collection.get("manifest_type") != COLLECTION:
        raise ValueError("A valid collection manifest is required")
    if collection["coverage_status"] == "MISSING":
        raise ValueError("Cannot normalize missing raw coverage")
    started = time.monotonic(); run_id = f"normalization-{uuid.uuid4()}"
    output = root / "normalized" / f"date={day.isoformat()}" / "canonical_observations.parquet"
    rejects = root / "normalized" / f"date={day.isoformat()}" / "rejects.jsonl"
    if output.exists() or rejects.exists():
        raise FileExistsError(f"Refusing to overwrite normalized publication for {day}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.partial")
    counters: Counter[str] = Counter(); observations: set[str] = set(); source_ids: set[str] = set(); logical: set[str] = set()
    batches: list[dict[str, Any]] = []
    with pq.ParquetWriter(temporary, OBSERVATION_SCHEMA, compression="zstd") as writer, rejects.open("w", encoding="utf-8") as rejected:
        for item in collection["source_partitions"]:
            archive = root / item["path"]
            if sha256_file(archive) != item["sha256"]:
                raise ValueError(f"Raw hash mismatch: {archive}")
            for line in iter_jsonl_zst(archive):
                counters["source_events"] += 1
                if line.payload is None:
                    counters["invalid"] += 1; rejected.write(json.dumps({"partition": item["partition"], "line": line.line_number, "error": line.error, "raw": line.raw}) + "\n"); continue
                row = _row(line.payload, line.raw, item["partition"], line.line_number, config)
                if row["source_event_id"] in source_ids:
                    counters["capture_duplicates"] += 1
                source_ids.add(row["source_event_id"])
                if row["canonical_observation_id"] in observations:
                    counters["collisions"] += 1; continue
                observations.add(row["canonical_observation_id"])
                if row["logical_event_id"] is None: counters["logical_unresolved"] += 1
                else: logical.add(row["logical_event_id"])
                if row["event_type"] == "UNKNOWN": counters["unknown"] += 1
                batches.append(row); counters["canonical_observations"] += 1
                if len(batches) >= 10_000:
                    writer.write_table(pa.Table.from_pylist(batches, schema=OBSERVATION_SCHEMA)); batches.clear()
        if batches: writer.write_table(pa.Table.from_pylist(batches, schema=OBSERVATION_SCHEMA))
    temporary.replace(output)
    counters["logical_resolved"] = len(logical)
    if rejects.stat().st_size == 0: rejects.unlink()
    files = [{"path": _relative(root, output), "sha256": sha256_file(output), "bytes": output.stat().st_size, "rows": counters["canonical_observations"]}]
    reject_files = [] if not rejects.exists() else [{"path": _relative(root, rejects), "sha256": sha256_file(rejects), "bytes": rejects.stat().st_size, "rows": counters["invalid"]}]
    manifest = {"manifest_id": f"normalization-{day.isoformat()}-{run_id}", "manifest_type": NORMALIZATION, "publication_status": "LOCAL_COMPLETE", "raw_manifest": _relative(root, collection_manifest), "normalization_run_id": run_id, "schema_version": SCHEMA_VERSION, "normalization_version": NORMALIZATION_VERSION, "id_strategy_version": ID_STRATEGY_VERSION, "requested_window": collection["requested_window"], "covered_window": collection["covered_window"], "input_files": collection["source_partitions"], "normalized_files": files, "reject_files": reject_files, "counters": dict(counters), "coverage_status": collection["coverage_status"], "contract_status": "SATISFIED", "usability_status": "VALID", "code_commit": _git_commit(), "environment_fingerprint": _env(), "created_at": datetime.now(timezone.utc).isoformat(), "elapsed_seconds": time.monotonic() - started}
    write_manifest_atomic(manifest_path, manifest)
    return manifest_path
