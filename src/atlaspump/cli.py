"""Command line entry point for auditing one archive."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import tempfile
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from atlaspump import __version__
from atlaspump.audit_report import build_report, write_report
from atlaspump.config import Settings, load_settings
from atlaspump.day_pipeline import collect_day, normalize_day
from atlaspump.decompressor import iter_jsonl_zst
from atlaspump.downloader import build_archive_url, download_archive, sha256_file
from atlaspump.event_classifier import normalize_event
from atlaspump.manifests import is_complete, read_manifest, write_manifest
from atlaspump.parquet_writer import ParquetWriter, parquet_size
from atlaspump.schema_inspector import SchemaInspector

LOGGER = logging.getLogger(__name__)


class DistinctTracker:
    """Exact distinct counts stored on disk instead of retained in process memory."""

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        for table, type_name in (
            ("event_ids", "TEXT"),
            ("signatures", "TEXT"),
            ("tokens", "TEXT"),
            ("wallets", "TEXT"),
            ("blocks", "INTEGER"),
        ):
            self.connection.execute(f"CREATE TABLE {table} (value {type_name} PRIMARY KEY)")

    def add(self, table: str, value: str | int) -> bool:
        cursor = self.connection.execute(f"INSERT OR IGNORE INTO {table} VALUES (?)", (value,))
        return cursor.rowcount == 1

    def count(self, table: str) -> int:
        return int(self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def block_range(self) -> tuple[int | None, int | None]:
        low, high = self.connection.execute("SELECT MIN(value), MAX(value) FROM blocks").fetchone()
        return low, high

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()


def audit(
    path: Path,
    settings: Settings,
    source: str,
    archive_date: date | None = None,
    hour: int | None = None,
    force: bool = False,
) -> Path:
    label = (
        f"{archive_date.isoformat()}_{hour:02d}"
        if archive_date and hour is not None
        else path.stem.replace(".jsonl", "")
    )
    manifest_path = settings.manifests_dir / f"{label}.json"
    digest = sha256_file(path)
    if not force and is_complete(manifest_path, digest, __version__):
        LOGGER.info("hour already processed with matching source hash: %s", manifest_path)
        return manifest_path
    started = time.monotonic()
    config = settings.values
    inspector = SchemaInspector(config["processing"]["example_limit"])
    writer = ParquetWriter(settings.parquet_dir, config["processing"]["batch_size"])
    stats: dict[str, Any] = {
        "total": 0,
        "valid": 0,
        "invalid": 0,
        "by_type": Counter(),
        "duplicates": 0,
        "missing_signature": 0,
        "missing_block": 0,
        "sha256": digest,
    }
    rejects = settings.reports_dir / f"rejects_{label}.jsonl"
    rejects.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="atlaspump-") as temporary:
        tracker = DistinctTracker(Path(temporary) / "distinct.sqlite")
        with rejects.open("w", encoding="utf-8") as rejected:
            for record in iter_jsonl_zst(path):
                stats["total"] += 1
                if record.payload is None:
                    stats["invalid"] += 1
                    rejected.write(
                        json.dumps(
                            {"line": record.line_number, "error": record.error, "raw": record.raw}
                        )
                        + "\n"
                    )
                    continue
                stats["valid"] += 1
                inspector.observe(record.payload, config["classification"]["field_candidates"])
                row = normalize_event(record.payload, config)
                if not tracker.add("event_ids", row["event_id"]):
                    stats["duplicates"] += 1
                stats["by_type"][row["event_type"]] += 1
                for key, field in (
                    ("signatures", "signature"),
                    ("tokens", "token_mint"),
                    ("wallets", "wallet"),
                    ("blocks", "block"),
                ):
                    if row[field] is not None:
                        tracker.add(key, row[field])
                stats["missing_signature"] += row["signature"] is None
                stats["missing_block"] += row["block"] is None
                writer.add(row)
        stats["signatures"] = tracker.count("signatures")
        stats["tokens"] = tracker.count("tokens")
        stats["wallets"] = tracker.count("wallets")
        stats["blocks"] = tracker.count("blocks")
        stats["block_min"], stats["block_max"] = tracker.block_range()
        tracker.close()
    if rejects.stat().st_size == 0:
        rejects.unlink()
    parquet_paths = writer.close()
    stats["elapsed"] = time.monotonic() - started
    p_size = parquet_size(parquet_paths)
    report = build_report(
        stats, inspector.summary(), source, path.stat().st_size, p_size, archive_date, hour
    )
    report_path = settings.reports_dir / f"audit_{label}.md"
    write_report(report_path, report)
    manifest = {
        "source": "pumpapi",
        "source_url": source,
        "date": archive_date.isoformat() if archive_date else None,
        "hour": hour,
        "download_status": "success",
        "sha256": digest,
        "compressed_size_bytes": path.stat().st_size,
        "lines_total": stats["total"],
        "lines_valid": stats["valid"],
        "lines_invalid": stats["invalid"],
        "events_by_type": dict(stats["by_type"]),
        "parquet_files": [str(item.relative_to(settings.root)) for item in parquet_paths],
        "pipeline_version": __version__,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "report": str(report_path.relative_to(settings.root)),
    }
    write_manifest(manifest_path, manifest)
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit one PumpApi JSONL/Zstandard archive")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)
    remote = sub.add_parser("audit-hour")
    remote.add_argument("--date", required=True, type=date.fromisoformat)
    remote.add_argument("--hour", required=True, type=int)
    remote.add_argument("--force", action="store_true")
    local = sub.add_parser("audit-file")
    local.add_argument("--input", required=True, type=Path)
    local.add_argument("--force", action="store_true")
    lifecycle = sub.add_parser("build-lifecycles")
    lifecycle.add_argument("--input", required=True, type=Path)
    lifecycle.add_argument("--output-dir", type=Path)
    lifecycle.add_argument("--mint")
    lifecycle.add_argument("--from-timestamp", type=int)
    lifecycle.add_argument("--to-timestamp", type=int)
    lifecycle.add_argument("--force", action="store_true")
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--date", required=True, type=date.fromisoformat)
    backfill.add_argument("--hours", required=True)
    backfill.add_argument("--force", action="store_true")
    collect = sub.add_parser("collect-day", help="Publish immutable raw archives from a local replay root")
    collect.add_argument("--date", required=True, type=date.fromisoformat)
    collect.add_argument("--input-root", required=True, type=Path)
    collect.add_argument("--output-root", required=True, type=Path)
    collect.add_argument("--hours", default="0-23")
    collect.add_argument("--resume", action="store_true")
    collect.add_argument("--download-missing", action="store_true")
    collect.add_argument("--validate-only", action="store_true")
    collect.add_argument("--force", action="store_true", help="Reserved; published output is never overwritten")
    normalize = sub.add_parser("normalize-day", help="Normalize one published raw collection")
    normalize.add_argument("--date", required=True, type=date.fromisoformat)
    normalize.add_argument("--raw-manifest", required=True, type=Path)
    normalize.add_argument("--output-root", required=True, type=Path)
    normalize.add_argument("--resume", action="store_true")
    normalize.add_argument("--validate-only", action="store_true")
    normalize.add_argument("--force", action="store_true", help="Reserved; published output is never overwritten")
    day = sub.add_parser("backfill-day", help="Collect then normalize a local replay day")
    day.add_argument("--date", required=True, type=date.fromisoformat)
    day.add_argument("--input-root", required=True, type=Path)
    day.add_argument("--output-root", required=True, type=Path)
    day.add_argument("--hours", default="0-23")
    day.add_argument("--resume", action="store_true")
    day.add_argument("--download-missing", action="store_true")
    day.add_argument("--validate-only", action="store_true")
    day.add_argument("--force", action="store_true", help="Reserved; published output is never overwritten")
    retention = sub.add_parser("plan-retention", help="Create a non-destructive retention manifest")
    retention.add_argument("--date", required=True, action="append", type=date.fromisoformat)
    retention.add_argument("--output-root", required=True, type=Path)
    retention.add_argument("--dry-run", action="store_true", default=True)
    quarantine = sub.add_parser("compact-retention-quarantine", help="Publish selected token-event subsets")
    quarantine.add_argument("--date", required=True, action="append")
    quarantine.add_argument("--output-root", required=True, type=Path)
    quarantine.add_argument("--retention-manifest", required=True, type=Path)
    args = parser.parse_args()
    from atlaspump.logging_config import configure_logging

    configure_logging(args.verbose)
    settings = load_settings(args.config)
    if args.command in {"collect-day", "backfill-day"}:
        from atlaspump.backfill import parse_hours

        if args.validate_only:
            print(json.dumps({"date": args.date.isoformat(), "hours": parse_hours(args.hours), "valid": True}))
            return
        collection = collect_day(
            args.output_root,
            args.input_root,
            args.date,
            parse_hours(args.hours),
            args.resume,
            args.download_missing,
            settings.values["source"]["archive_url_template"],
            settings.values["source"]["timeout_seconds"],
        )
        if args.command == "collect-day":
            print(collection)
            return
        normalization = normalize_day(args.output_root, args.date, collection, settings.values, args.resume)
        print(json.dumps({"collection_manifest": str(collection), "normalization_manifest": str(normalization)}, indent=2))
        return
    if args.command == "normalize-day":
        if args.validate_only:
            manifest = read_manifest(args.raw_manifest)
            if not manifest or manifest.get("manifest_type") != "COLLECTION":
                parser.error("--raw-manifest must be a collection manifest")
            print(json.dumps({"raw_manifest": str(args.raw_manifest), "valid": True}))
            return
        print(normalize_day(args.output_root, args.date, args.raw_manifest, settings.values, args.resume))
        return
    if args.command == "build-lifecycles":
        from atlaspump.lifecycle_builder import build_lifecycles

        output = args.output_dir or settings.data_dir / "lifecycles"
        if not args.input.exists():
            parser.error(f"input does not exist: {args.input}")
        print(json.dumps(build_lifecycles(args.input, output, args.force, args.mint), indent=2))
        return
    if args.command == "plan-retention":
        from atlaspump.retention_policy import plan_retention

        print(plan_retention(args.output_root, args.date, args.dry_run))
        return
    if args.command == "compact-retention-quarantine":
        from atlaspump.retention_quarantine import compact_quarantine

        print(compact_quarantine(args.output_root, args.retention_manifest, args.date))
        return
    if args.command == "backfill":
        from atlaspump.backfill import backfill_day, parse_hours

        print(backfill_day(settings, args.date, parse_hours(args.hours), args.force))
        return
    if args.command == "audit-hour":
        template = settings.values["source"]["archive_url_template"]
        url = build_archive_url(template, args.date, args.hour)
        target = settings.raw_dir / args.date.strftime("%Y/%m/%d") / f"{args.hour:02d}.jsonl.zst"
        if target.exists():
            path = target
        else:
            result = download_archive(url, target, settings.values["source"]["timeout_seconds"])
            path = result.path
        result_path = audit(path, settings, url, args.date, args.hour, args.force)
    else:
        if not args.input.exists():
            parser.error(f"input does not exist: {args.input}")
        result_path = audit(args.input, settings, str(args.input), force=args.force)
    print(result_path)


if __name__ == "__main__":
    main()
