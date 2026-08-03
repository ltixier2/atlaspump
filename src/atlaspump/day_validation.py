"""Day-level manifest and quality reports for validated lifecycle artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import statistics
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from atlaspump.file_discovery import discover_parquet


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        temporary = Path(stream.name)
    temporary.replace(path)


def _table_rows(path: Path) -> int:
    return int(pq.ParquetFile(path).metadata.num_rows)


def validate_day(day: date, lifecycle_dir: Path, output_dir: Path) -> tuple[Path, Path]:
    files = list(discover_parquet(lifecycle_dir))
    expected = {
        "token_events.parquet",
        "token_lifecycles.parquet",
        "token_outcomes_preliminary.parquet",
        "lifecycle_anomalies.parquet",
    }
    names = {path.name for path in files}
    if names != expected:
        raise ValueError(f"Expected valid lifecycle Parquet files {expected}, found {names}")
    details = []
    for path in files:
        parquet = pq.ParquetFile(path)
        details.append(
            {
                "path": str(path.relative_to(lifecycle_dir.parent)),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
                "rows": parquet.metadata.num_rows,
                "schema": str(parquet.schema_arrow),
            }
        )
    lifecycle_file = pq.ParquetFile(lifecycle_dir / "token_lifecycles.parquet")
    mints: list[str | None] = []
    event_count: list[int] = []
    bool_names = (
        "creation_event_received",
        "pumpfun_activity_observed",
        "migration_explicit",
        "migration_inferred",
        "pumpswap_activity_observed",
        "left_censored",
        "right_censored",
        "lifecycle_complete",
    )
    bools = {name: 0 for name in bool_names}
    for batch in lifecycle_file.iter_batches(
        batch_size=10_000, columns=["mint", "event_count", *bool_names]
    ):
        values = batch.to_pydict()
        mints.extend(values["mint"])
        event_count.extend(int(value) for value in values["event_count"] if value is not None)
        for name in bool_names:
            bools[name] += sum(bool(value) for value in values[name] if value is not None)
    outcome_mints: list[str | None] = []
    for batch in pq.ParquetFile(lifecycle_dir / "token_outcomes_preliminary.parquet").iter_batches(
        batch_size=10_000, columns=["mint"]
    ):
        outcome_mints.extend(batch.column(0).to_pylist())
    event_file = lifecycle_dir / "token_events.parquet"
    event_rows = _table_rows(event_file)
    unique_mints = len(set(mints))
    checks = {
        "lifecycle_mints_unique": unique_mints == len(mints),
        "outcome_mints_unique": len(set(outcome_mints)) == len(outcome_mints),
        "populations_match": set(mints) == set(outcome_mints),
        "no_null_lifecycle_mint": None not in mints,
        "event_rows_positive": event_rows > 0,
    }
    status = "COMPLETE" if all(checks.values()) else "FAILED"
    excluded = [path.name for path in lifecycle_dir.glob("._*.parquet")]
    manifest = {
        "manifest_id": f"atlaspump-day-{day.isoformat()}-v1",
        "manifest_type": "day_lifecycle",
        "status": status,
        "date": day.isoformat(),
        "observation_start": None,
        "observation_end": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "code_commit": os.environ.get("GIT_COMMIT", "unknown"),
        "environment_fingerprint": (
            f"{platform.system()}-{platform.release()}-python-{platform.python_version()}"
        ),
        "builder_version": "0.1.0",
        "schema_version": "0.1",
        "normalization_version": "0.1.0",
        "lifecycle_version": "0.1.0",
        "outcome_version": "0.1.0",
        "files": details,
        "unique_mints": unique_mints,
        "source_partition_count": 24,
        "exclusions": excluded,
        "appledouble_warning": "AppleDouble files (._*) were ignored after lightweight "
        "Parquet validation.",
        "checks": checks,
    }
    manifest_path = output_dir / f"day_manifest_{day.isoformat()}.json"
    _atomic_json(manifest_path, manifest)
    percentiles = {p: _percentile(event_count, p) for p in (50, 75, 90, 95, 99, 99.9)}
    report = [
        f"# AtlasPump Day 1 Quality Report - {day.isoformat()}",
        "",
        "## Volumetrie",
        f"- Evenements: {event_rows:,}",
        f"- Tokens/lifecycles/outcomes: {len(mints):,}/{len(mints):,}/{len(outcome_mints):,}",
        f"- Anomalies: {_table_rows(lifecycle_dir / 'lifecycle_anomalies.parquet'):,}",
        f"- Evenements/token: moyenne {statistics.mean(event_count):.2f}, "
        f"mediane {statistics.median(event_count):.0f}, min {min(event_count)}, "
        f"max {max(event_count)}",
    ]
    report += [f"- P{p}: {value:.2f}" for p, value in percentiles.items()]
    report += [
        "",
        "## Identites",
        *[f"- {name}: {'PASS' if value else 'FAIL'}" for name, value in checks.items()],
        "",
        "## Etats",
        *[f"- {name}: {value:,}" for name, value in bools.items()],
        "",
        "## Limites",
        "- Les fichiers AppleDouble `._*.parquet` ont ete exclus.",
        "- Les controles de montants non applicables restent non conclusifs lorsque les colonnes "
        "sont absentes ou nulles.",
        "- Zero anomalie ne prouve pas une qualite parfaite; les regles ont ete executees sur "
        "les donnees du builder.",
    ]
    report_path = output_dir / f"lifecycle_report_{day.isoformat()}.md"
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    return manifest_path, report_path


def _percentile(values: list[int], percentile: float) -> float:
    index = (len(values) - 1) * percentile / 100
    low, high = int(index), min(int(index) + 1, len(values) - 1)
    ordered = sorted(values)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)
