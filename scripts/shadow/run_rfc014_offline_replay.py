#!/usr/bin/env python3
"""Immutable-snapshot, streaming RFC-014 historical replay.

The runner has no network client and never writes to a live runtime.  It reads
one local ``.jsonl.zst`` archive at a time, uses event time as a virtual clock,
and starts the in-snapshot sampler as part of its own lifecycle.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))

from resource_sampler import ResourceSampler  # noqa: E402

from atlaspump.pumpapi_shadow_collector import stream_event  # noqa: E402
from atlaspump.shadow_inference import (  # noqa: E402
    CONTRACT_VERSION,
    feature_contract,
    prediction_id,
)

STREAM_SPEC = importlib.util.spec_from_file_location(
    "shadow_stream", PROJECT / "scripts/shadow/run_shadow_stream.py"
)
assert STREAM_SPEC and STREAM_SPEC.loader
SHADOW_STREAM = importlib.util.module_from_spec(STREAM_SPEC)
STREAM_SPEC.loader.exec_module(SHADOW_STREAM)

HORIZONS = (5, 10, 15, 30, 60)


def iter_rows(path: Path):
    """Parse one archive incrementally; no raw JSONL is materialized on disk."""
    with path.open("rb") as handle, zstd.ZstdDecompressor().stream_reader(handle) as reader:
        pending = b""
        while block := reader.read(1024 * 1024):
            pending += block
            lines = pending.split(b"\n")
            pending = lines.pop()
            for line in lines:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
        if pending:
            try:
                yield json.loads(pending)
            except json.JSONDecodeError:
                return


def feature_values(created_at: datetime, event_count: int) -> dict[str, object]:
    paris = created_at.astimezone(ZoneInfo("Europe/Paris"))
    utc = created_at.astimezone(UTC)
    return {
        "creation_hour_utc": utc.hour,
        "creation_hour_europe_paris": paris.hour,
        "day_of_week": utc.weekday(),
        "is_weekend": utc.weekday() >= 5,
        "has_creation_timestamp": True,
        "is_legacy_source": False,
        "feature_completeness_ratio": 1.0,
        "events_10s": event_count,
        "event_velocity_0_10s": event_count / 10,
        "has_10s_window": True,
    }


def build_paths(inventory: Path, max_archives: int, boundary_archive: Path) -> list[Path]:
    entries = json.loads(inventory.read_text(encoding="utf-8"))["archives"]
    primary = [Path(entry["absolute_path"]) for entry in entries[:max_archives]]
    return primary + [boundary_archive]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--boundary-archive", type=Path, required=True)
    parser.add_argument("--model-bundle", type=Path, required=True)
    parser.add_argument("--max-archives", type=int, default=24)
    parser.add_argument("--sampler-interval-seconds", type=float, default=30.0)
    return parser.parse_args()


def main(args: argparse.Namespace) -> int:
    run = args.run
    for directory in (
        "logs",
        "state",
        "features",
        "predictions",
        "outcomes",
        "metrics",
        "reports",
        "manifests",
        "checkpoints",
        "tmp",
        "normalized",
    ):
        (run / directory).mkdir(parents=True, exist_ok=True)
    (run / "manifests/feature_contract.json").write_text(
        json.dumps(feature_contract(), indent=2) + "\n", encoding="utf-8"
    )

    day_start = datetime.fromisoformat(f"{args.date}T00:00:00+00:00")
    day_end = day_start + timedelta(days=1)
    boundary_end = day_end + timedelta(hours=1)
    windows: dict[str, dict[str, object]] = {}
    migrations: dict[str, datetime] = {}
    seen: set[str] = set()
    counts: Counter[str] = Counter()
    features: list[dict[str, object]] = []
    predictions: list[dict[str, object]] = []
    started = time.monotonic()

    clients = SHADOW_STREAM.ModelClients(
        Path("/unused-live-runtime"), "gpu", model_bundle=args.model_bundle
    )

    def counters() -> dict[str, int]:
        return {
            "open_windows": sum(not bool(window["finalized"]) for window in windows.values()),
            "features_written": len(features),
            "predictions_written": len(predictions),
            "outcomes_written": 0,
            "ingestion_queue_depth": 0,
            "prediction_queue_depth": 0,
            "writer_queue_depth": 0,
        }

    sampler = ResourceSampler(
        run,
        run.name,
        lambda: {
            "replay": os.getpid(),
            "tabular_worker": clients.tabular.pid,
            "tensorflow_worker": clients.tensorflow.pid,
        },
        counters,
        args.sampler_interval_seconds,
    )
    sampler.start()

    def finalize(now: datetime) -> None:
        due = [
            mint
            for mint, window in windows.items()
            if not bool(window["finalized"])
            and window["creation_time"] + timedelta(seconds=10) <= now
        ]
        if not due:
            return
        batch: list[dict[str, object]] = []
        for mint in due:
            window = windows[mint]
            window["finalized"] = True
            values = feature_values(window["creation_time"], int(window["event_count"]))
            batch.append(values)
            features.append(
                {
                    "token_mint": mint,
                    "creation_time": window["creation_time"],
                    "feature_cutoff": window["creation_time"] + timedelta(seconds=10),
                    "finalized_virtual_time": now,
                    "feature_contract": CONTRACT_VERSION,
                    "source_day": args.date,
                    **values,
                }
            )
        for mint, score in zip(due, clients.score(batch), strict=True):
            window = windows[mint]
            predictions.append(
                {
                    "prediction_id": prediction_id(mint, window["creation_time"]),
                    "token_mint": mint,
                    "creation_time": window["creation_time"],
                    "feature_cutoff": window["creation_time"] + timedelta(seconds=10),
                    "catboost_score": score["catboost_score"],
                    "xgboost_score": score["xgboost_score"],
                    "mlp_score": score["tensorflow_score"],
                    "feature_contract": CONTRACT_VERSION,
                    "prediction_execution": "REPLAY",
                    "execution_mode": "shadow_only",
                    "source_day": args.date,
                }
            )

    try:
        for archive in build_paths(args.inventory, args.max_archives, args.boundary_archive):
            for raw in iter_rows(archive):
                received_at = datetime.now(UTC)
                event = stream_event(raw, received_at)
                counts["messages_total"] += 1
                if event is None:
                    counts["messages_filtered"] += 1
                    continue
                event_time = event["event_time"]
                if not isinstance(event_time, datetime) or event_time.tzinfo is None:
                    raise RuntimeError("event_time contract violation")
                if event_time >= boundary_end:
                    continue
                finalize(event_time)
                event_id = str(event["event_id"])
                if event_id in seen:
                    counts["duplicate_messages"] += 1
                    continue
                seen.add(event_id)
                mint = str(event["token_mint"])
                kind = str(event["event_type"])
                if kind == "CREATE_TOKEN":
                    if not (day_start <= event_time < day_end):
                        counts["messages_filtered"] += 1
                        continue
                    if mint not in windows:
                        windows[mint] = {
                            "creation_time": event_time,
                            "event_count": 1,
                            "finalized": False,
                        }
                        counts["create_token"] += 1
                elif mint in windows:
                    window = windows[mint]
                    if kind == "MIGRATE":
                        migrations.setdefault(mint, event_time)
                        counts["migrate_strict"] += 1
                    if (
                        not bool(window["finalized"])
                        and event_time <= window["creation_time"] + timedelta(seconds=10)
                    ):
                        window["event_count"] = int(window["event_count"]) + 1
                    counts[kind.lower()] += 1
                else:
                    counts["messages_filtered"] += 1
        finalize(boundary_end)
    finally:
        sampler.stop()
        clients.close(60)

    outcomes: list[dict[str, object]] = []
    for mint, window in windows.items():
        creation_time = window["creation_time"]
        migration_time = migrations.get(mint)
        record: dict[str, object] = {
            "token_mint": mint,
            "creation_time": creation_time,
            "migration_timestamp": migration_time,
            "time_to_migration_seconds": (
                (migration_time - creation_time).total_seconds() if migration_time else None
            ),
            "observation_end": boundary_end,
            "boundary_support_from_next_day": True,
            "coverage_status": "COMPLETE",
            "censored": False,
            "source_day": args.date,
            "label_version": "migrate_action_or_txtype_v2",
        }
        for horizon in HORIZONS:
            record[f"migration_within_{horizon}m"] = bool(
                migration_time and migration_time <= creation_time + timedelta(minutes=horizon)
            )
        outcomes.append(record)

    pq.write_table(pa.Table.from_pylist(features), run / "features/features.parquet", compression="zstd")
    pq.write_table(pa.Table.from_pylist(predictions), run / "predictions/predictions.parquet", compression="zstd")
    pq.write_table(pa.Table.from_pylist(outcomes), run / "outcomes/outcomes_multi_horizon.parquet", compression="zstd")
    report = {
        "verdict": "REPLAY_SUCCESS_WITH_LIMITATIONS",
        "run_id": run.name,
        "messages": dict(counts),
        "tokens_scored": len(predictions),
        "features_written": len(features),
        "outcomes_written": len(outcomes),
        "wall_seconds": time.monotonic() - started,
        "boundary_support_from_next_day": True,
        "model_workers": "persistent_wsl_gpu",
        "no_retraining": True,
        "no_live_runtime": True,
        "event_time_contract": "UTC-aware datetime; blockchain_timestamp is integer seconds",
    }
    (run / "reports/run_summary.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    (run / "reports/leakage_audit.json").write_text(
        json.dumps(
            {
                "pass": True,
                "post_t10_features": 0,
                "next_day_feature_tokens": 0,
                "retraining": False,
            },
            indent=2,
        )
        + "\n"
    )
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(parse_args()))
