"""Versioned, non-destructive compaction of retention-selected token events."""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa


def _sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def compact_quarantine(root: Path, retention_manifest: Path, days: list[str]) -> Path:
    """Copy only selected mint events to a new immutable quarantine run."""
    policy = json.loads(retention_manifest.read_text())
    if policy.get("policy_version") != "retention-policy-v1" or not policy.get("dry_run"):
        raise ValueError("retention manifest must be retention-policy-v1 dry-run output")
    holds = {r["logical_date"]: r for r in policy["records"] if r["category"] == "token_events" and r["state"] == "HOLD"}
    run_id = f"quarantine-v1-{uuid.uuid4()}"
    run_root = root / "quarantine" / "retention-policy-v1" / f"run={run_id}"
    if run_root.exists():
        raise FileExistsError(run_root)
    outputs: list[dict[str, Any]] = []
    for day in days:
        record = holds.get(day)
        selected = policy["summary"]["selected_mints"].get(day)
        if record is None or selected is None:
            raise ValueError(f"retention manifest has no HOLD token_events selection for {day}")
        source = root / record["path"]
        if _sha256(source) != record["checksum_before_action"]:
            raise ValueError(f"source checksum changed: {source}")
        target = run_root / f"date={day}" / "token_events_retained.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_suffix(".parquet.partial")
        con = duckdb.connect()
        con.register("selected_mints", pa.table({"mint": selected}))
        columns = {x[0] for x in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(source)]).fetchall()}
        event_id = "canonical_observation_id" if "canonical_observation_id" in columns else "event_id"
        position = "slot NULLS LAST, " if "slot" in columns else "block NULLS LAST, "
        order = f"blockchain_timestamp NULLS LAST, {position}signature NULLS LAST, {event_id}"
        source_sql = str(source).replace("'", "''")
        target_sql = str(partial).replace("'", "''")
        con.execute(f"COPY (SELECT e.* FROM read_parquet('{source_sql}') e JOIN selected_mints s USING(mint) ORDER BY {order}) TO '{target_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        check = duckdb.connect()
        counted = check.execute("SELECT count(*) FROM read_parquet(?)", [str(partial)]).fetchone()
        assert counted is not None
        retained = counted[0]
        check.close()
        expected = policy["summary"]["events"][day]["kept"]
        if retained != expected:
            raise ValueError(f"retained event mismatch for {day}: {retained} != {expected}")
        os.replace(partial, target)
        outputs.append({"logical_date": day, "source_path": record["path"], "source_bytes": source.stat().st_size, "source_rows": policy["summary"]["events"][day]["all"], "retained_path": str(target.relative_to(root)), "retained_bytes": target.stat().st_size, "retained_rows": retained, "retained_tokens": len(selected), "sha256": _sha256(target)})
    manifest = {"manifest_type": "RETENTION_QUARANTINE_COMPACTION", "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(), "policy_version": policy["policy_version"], "retention_manifest": str(retention_manifest.relative_to(root)), "outputs": outputs, "state": "HOLD"}
    path = run_root / "quarantine_manifest.json"; temp = path.with_suffix(".json.partial"); temp.write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n"); os.replace(temp, path)
    return path
