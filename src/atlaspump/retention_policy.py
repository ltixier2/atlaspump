"""Non-destructive retention-policy planning for published AtlasPump days."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

POLICY_VERSION = "retention-policy-v1"
STATES = {"DISCOVERED", "HOLD", "DELETE_CANDIDATE", "DELETE_APPROVED", "DELETED", "FAILED"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _selected(mint: str) -> bool:
    return int(hashlib.sha256(mint.encode()).hexdigest()[:16], 16) % 10_000 < 300


def plan_retention(root: Path, days: list[date], dry_run: bool = True) -> Path:
    """Write a manifest only; no state transition can delete a physical file."""
    if not dry_run:
        raise ValueError("Retention Policy v1 is planning-only; --dry-run is mandatory")
    con = duckdb.connect()
    records: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"tokens": {}, "events": {}, "selected_mints": {}, "bytes": {"hold": 0, "candidate": 0}}
    today = datetime.now(timezone.utc).date()
    for day in days:
        iso = day.isoformat()
        lifecycle = root / "daily" / iso / "lifecycles"
        event = lifecycle / "token_events.parquet"
        if not event.exists():
            continue
        rows = con.execute(f"SELECT mint,migration_explicit,event_count FROM read_parquet('{lifecycle}/token_lifecycles.parquet')").fetchall()
        migrated = [mint for mint, flag, _ in rows if flag]
        sampled = [mint for mint, flag, _ in rows if not flag and _selected(mint)]
        kept = set(migrated) | set(sampled)
        kept_events = sum(count for mint, _, count in rows if mint in kept)
        summary["tokens"][iso] = {"all": len(rows), "migrated": len(migrated), "sampled_ordinary": len(sampled), "kept": len(kept)}
        summary["events"][iso] = {"all": sum(count for _, _, count in rows), "kept": kept_events}
        summary["selected_mints"][iso] = sorted(kept)
        for path, category, reason in (
            (lifecycle / "token_lifecycles.parquet", "lifecycle", "required_full_retention"),
            (lifecycle / "token_outcomes_preliminary.parquet", "outcome", "required_full_retention"),
            (lifecycle / "lifecycle_anomalies.parquet", "anomaly", "required_full_retention"),
            (event, "token_events", "mixed_file_requires_future_compaction"),
        ):
            if path.exists():
                records.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "logical_date": iso, "category": category, "reason": reason, "state": "HOLD", "checksum_before_action": _sha256(path)})
                summary["bytes"]["hold"] += path.stat().st_size
        for path, category in ((root / "raw" / "pumpapi" / "replay" / f"date={iso}", "raw"), (root / "normalized" / f"date={iso}", "normalized")):
            if path.exists():
                for file in path.rglob("*"):
                    if file.is_file():
                        state = "HOLD" if (today - day).days <= 7 else "DELETE_CANDIDATE"
                        records.append({"path": str(file.relative_to(root)), "bytes": file.stat().st_size, "logical_date": iso, "category": category, "reason": "rolling_7_day_window" if state == "HOLD" else "outside_7_day_window", "state": state, "checksum_before_action": _sha256(file)})
                        summary["bytes"]["hold" if state == "HOLD" else "candidate"] += file.stat().st_size
    manifest = {"policy_version": POLICY_VERSION, "dry_run": True, "created_at": datetime.now(timezone.utc).isoformat(), "states": sorted(STATES), "transition_rule": "DISCOVERED cannot transition directly to DELETED", "records": records, "summary": summary}
    target = root / "reports" / "retention" / f"retention_manifest_{POLICY_VERSION}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, target)
    return target
