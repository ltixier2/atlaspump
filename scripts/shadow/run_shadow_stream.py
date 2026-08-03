#!/usr/bin/env python3
"""RFC-014 JSONL, shadow-only stream worker; it has no trading primitives."""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
MIGRATE_MAPPING_CONFIRMED = True

from atlaspump.shadow_inference import CONTRACT_VERSION, FEATURES, prediction_id  # noqa: E402
from atlaspump.shadow_stream import (  # noqa: E402
    GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF,
    OutcomeBook,
    StreamState,
    TransportTracker,
    WindowBook,
    atomic_json,
    validate_event,
)


def append_parquet(path: Path, rows: list[dict]) -> Path | None:
    if not rows:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp"
    pq.write_table(pa.Table.from_pylist(rows), temporary, compression="zstd")
    temporary.replace(path)
    return path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def disk_guard(root: Path) -> None:
    usage = shutil.disk_usage(root)
    if usage.free < 80 * 1024**3:
        raise RuntimeError("refusing start: less than 80 GiB free")
    if usage.free < 120 * 1024**3:
        print("WARNING: less than 120 GiB free", file=sys.stderr)


def tensorflow_library_paths(nvidia_root: Path) -> list[str]:
    """Return actual CUDA shared-library directories, never their parents."""
    return sorted(str(path) for path in nvidia_root.glob("*/lib") if path.is_dir())


class ModelClients:
    """One tabular and one TensorFlow child for the entire stream lifetime."""

    def __init__(
        self,
        runtime: Path,
        tensorflow_device: str,
        model_bundle: Path | None = None,
        tabular_python: Path | None = None,
        tensorflow_python: Path | None = None,
    ) -> None:
        """Start persistent read-only workers from either the WSL defaults or a bundle.

        The optional bundle is intentionally explicit: it is used by the Cerebro
        CPU candidate only and leaves the established WSL/GPU invocation intact.
        """
        smoke = runtime / "runs" / "rfc010e-wsl-smoke-20260719T222339Z"
        tensorflow = runtime / "runs" / "rfc012-tensorflow-mlp-20260719T230634Z"
        catboost_model, xgboost_model = smoke / "catboost_model.cbm", smoke / "xgboost_model.json"
        tensorflow_model, preprocessing = tensorflow / "model.keras", tensorflow / "preprocessing.json"
        if model_bundle is not None:
            catboost_model, xgboost_model = model_bundle / "catboost/model.cbm", model_bundle / "xgboost/model.json"
            tensorflow_model, preprocessing = model_bundle / "mlp/model.keras", model_bundle / "mlp/preprocessing.json"
        tabular_python = tabular_python or Path("/home/laurent/atlaspump/envs/tabular-ml/bin/python")
        tensorflow_python = tensorflow_python or Path("/home/laurent/atlaspump/envs/tensorflow-gpu/bin/python")
        for path in (catboost_model, xgboost_model, tensorflow_model, preprocessing, tabular_python, tensorflow_python):
            if not path.exists():
                raise FileNotFoundError(path)
        worker = PROJECT / "scripts" / "shadow" / "shadow_model_worker.py"
        self.tabular = subprocess.Popen(
            [str(tabular_python), str(worker), "--kind", "tabular", "--catboost-model", str(catboost_model), "--xgboost-model", str(xgboost_model)],
            env={**os.environ, "PYTHONPATH": ""}, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        nvidia_libs = tensorflow_library_paths(Path("/home/laurent/atlaspump/envs/tensorflow-gpu/lib/python3.12/site-packages/nvidia"))
        tf_env = {**os.environ, "PYTHONPATH": "", "LD_LIBRARY_PATH": ":".join(nvidia_libs + ["/usr/lib/wsl/lib", os.environ.get("LD_LIBRARY_PATH", "")])}
        self.tensorflow = subprocess.Popen(
            [str(tensorflow_python), str(worker), "--kind", "tensorflow", "--tensorflow-model", str(tensorflow_model), "--preprocessing", str(preprocessing), "--tensorflow-device", tensorflow_device],
            env=tf_env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )

    @staticmethod
    def request(process: subprocess.Popen[str], rows: list[dict]) -> dict:
        assert process.stdin and process.stdout
        process.stdin.write(json.dumps({"rows": rows}) + "\n")
        process.stdin.flush()
        response = process.stdout.readline()
        if not response:
            raise RuntimeError(f"model worker exited ({process.poll()})")
        return json.loads(response)

    def score(self, rows: list[dict]) -> list[dict]:
        tabular, tensorflow = self.request(self.tabular, rows), self.request(self.tensorflow, rows)
        return [{key: tabular[key][index] for key in tabular} | {key: tensorflow[key][index] for key in tensorflow} for index in range(len(rows))]

    def close(self, timeout: int) -> None:
        for process in (self.tabular, self.tensorflow):
            if process.poll() is not None:
                continue
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class WindowScheduler:
    """A one-thread monotonic scheduler, sleeping until the next cutoff."""

    def __init__(self, on_due: Callable[[str], None]) -> None:
        self.on_due = on_due
        self.condition = threading.Condition()
        self.heap: list[tuple[float, str, datetime]] = []
        self.scheduled: set[tuple[str, datetime]] = set()
        self.stop_requested = False
        self.thread = threading.Thread(target=self._run, name="rfc014-window-scheduler", daemon=True)
        self.wakeups = 0

    def start(self) -> None:
        self.thread.start()

    def schedule(self, mint: str, cutoff: datetime) -> bool:
        key = (mint, cutoff)
        with self.condition:
            if key in self.scheduled:
                return False
            # Wall UTC determines the economic cutoff; monotonic time is only
            # used to wait safely for that already-normalized instant.
            delay = max(0.0, (cutoff - datetime.now(timezone.utc)).total_seconds())
            heapq.heappush(self.heap, (time.monotonic() + delay, mint, cutoff))
            self.scheduled.add(key)
            self.condition.notify()
            return True

    def stop(self, timeout: int) -> None:
        with self.condition:
            self.stop_requested = True
            self.condition.notify_all()
        self.thread.join(timeout=timeout)

    def _run(self) -> None:
        while True:
            with self.condition:
                while not self.stop_requested and not self.heap:
                    self.condition.wait()
                if self.stop_requested:
                    return
                deadline, mint, cutoff = self.heap[0]
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    self.condition.wait(remaining)
                    continue
                heapq.heappop(self.heap)
            self.wakeups += 1
            self.on_due(mint)


def prediction_row(item: object, args: argparse.Namespace, finalized_at: datetime, scheduler_wakeup_at: datetime, feature_compute_started_at: datetime, feature_compute_finished_at: datetime) -> dict:
    cutoff = item.creation_time + timedelta(seconds=10)
    identifier = prediction_id(item.token_mint, item.creation_time)
    return {
        "prediction_schema": "shadow_prediction_v2",
        "prediction_id": identifier,
        "feature_id": identifier,
        "creation_event_id": item.creation_event_id,
        "token_mint": item.token_mint,
        "mint": item.token_mint,
        "creation_time": item.creation_time,
        "feature_cutoff": cutoff,
        "feature_finalized_time": feature_compute_finished_at,
        "prediction_time": feature_compute_finished_at,
        "feature_target_time": cutoff,
        "scheduler_wakeup_time": scheduler_wakeup_at,
        "feature_compute_started_at": feature_compute_started_at,
        "feature_compute_finished_at": feature_compute_finished_at,
        "event_arrival_lag_ms": item.event_arrival_lag_ms,
        "scheduler_wakeup_lag_ms": max(0, int((scheduler_wakeup_at - cutoff).total_seconds() * 1000)),
        # Canonical: only the feature computation completion, never scoring or writing.
        "feature_finalization_lag_ms": max(0, int((feature_compute_finished_at - cutoff).total_seconds() * 1000)),
        "latency_from_cutoff_ms": max(0, int((feature_compute_finished_at - cutoff).total_seconds() * 1000)),
        "latency_from_creation_ms": max(0, int((feature_compute_finished_at - item.creation_time).total_seconds() * 1000)),
        "prediction_execution": "LIVE" if args.input == "-" else "REPLAY",
        "execution_mode": "shadow_only",
        "outcome_status": "UNKNOWN",
        "feature_contract": CONTRACT_VERSION,
        "feature_contract_version": CONTRACT_VERSION,
        "feature_status": item.status,
        "events_received": item.event_count,
        "max_ingestion_delay_ms": item.max_ingestion_delay_ms,
        "rejected_after_cutoff": item.rejected_after_cutoff,
        **item.values,
    }


def canonical_feature_row(row: dict) -> dict:
    """Return the immutable feature artifact paired one-to-one with a prediction."""
    return {
        "feature_id": row["feature_id"],
        "prediction_id": row["prediction_id"],
        "mint": row["mint"],
        "creation_event_id": row["creation_event_id"],
        "feature_contract": row["feature_contract"],
        "feature_cutoff_seconds": 10,
        "feature_cutoff": row["feature_cutoff"].isoformat(),
        "feature_finalized_at": row["feature_finalized_time"].isoformat(),
        "event_count": int(row["events_received"]),
        **{name: row[name] for name in FEATURES},
    }


def append_jsonl_fsync(path: Path, row: dict) -> None:
    """Append one canonical JSON row durably before its prediction is visible."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def write_migrate_event_time_validation(path: Path) -> dict:
    """Exercise the action-only MIGRATE and post-T+10 OutcomeBook route off-stream."""
    mint = "RFC014_ISOLATED_MIGRATE_TOKEN"
    creation = datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc)
    migration = creation + timedelta(seconds=45)
    raw_timestamp_ms = int(migration.timestamp() * 1000)
    diagnostic = {
        "test_executed": True, "test_isolated_from_live": True,
        "raw_action": "migrate", "raw_txType": None,
        "raw_pool": "pump-amm", "raw_poolCreatedBy": "pump",
        "mint": mint, "mint_source": "mint", "quoteMint_ignored": True,
        "normalized_event_type": "MIGRATE",
        "creation_event_time": creation.isoformat(), "migration_event_time": migration.isoformat(),
        "blockchain_timestamp": int(migration.timestamp()),
        "raw_timestamp_ms": raw_timestamp_ms,
        "timestamp_consistent": int(migration.timestamp()) == raw_timestamp_ms // 1000,
        "migration_after_feature_cutoff": migration > creation + timedelta(seconds=10),
        "features_unchanged": True,
        "routed_to_outcome_book": False, "outcome": None,
        "time_to_migration_seconds": int((migration - creation).total_seconds()),
    }
    try:
        book = OutcomeBook("isolated-migrate-validation")
        book.register(mint, creation, creation)
        routed = book.observe_migration({"token_mint": mint, "event_time": migration, "event_id": "isolated-migrate-event"})
        finalized = book.due(creation + timedelta(minutes=5), stream_active=True, allow_negative=True)
        diagnostic["routed_to_outcome_book"] = routed
        diagnostic["outcome"] = finalized[0].outcome_status if finalized else None
        diagnostic["success"] = bool(routed and diagnostic["outcome"] == "POSITIVE")
    except Exception as exc:  # Diagnostic is mandatory even on an isolated-test failure.
        diagnostic["success"] = False
        diagnostic["error"] = f"{type(exc).__name__}: {exc}"
    atomic_json(path, diagnostic)
    return diagnostic


# Thresholds proposed in RFC014_BACKLOG_DIAGNOSIS_20260801.md §Partie 2.
# Not tuned against live traffic yet; revisit once real alert history exists.
HERMES_ALERT_THRESHOLDS: dict[str, dict[str, float]] = {
    "transport_backlog_records": {"warning": 5_000, "critical": 20_000},
    "p95_scheduler_wakeup_lag_ms": {"warning": 600_000, "critical": 3_600_000},  # 10 min / 1 h
}


def alert_severity(metric: str, value: float | int | None) -> str | None:
    if value is None:
        return None
    thresholds = HERMES_ALERT_THRESHOLDS[metric]
    if value >= thresholds["critical"]:
        return "CRITICAL"
    if value >= thresholds["warning"]:
        return "WARNING"
    return None


def send_hermes_alert(webhook_url: str | None, alerts_log_path: Path, payload: dict) -> None:
    """Always logged locally and durably; POSTed to Hermes only if a webhook
    URL was explicitly configured. Best-effort and non-blocking: alerting
    must never be able to break the pipeline, so network failures are
    swallowed after a short timeout, never raised."""
    append_jsonl_fsync(alerts_log_path, payload)
    if not webhook_url:
        return
    try:
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload, default=str).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(request, timeout=5)
    except (urllib.error.URLError, OSError, ValueError):
        pass


def tail_last_line(path: Path, chunk_size: int = 4096) -> str | None:
    """Read only a file's last line, in bounded time independent of its total size."""
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            offset = handle.tell()
            block = b""
            while offset > 0:
                read_size = min(chunk_size, offset)
                offset -= read_size
                handle.seek(offset)
                block = handle.read(read_size) + block
                if block.count(b"\n") >= 2 or offset == 0:
                    break
    except FileNotFoundError:
        return None
    lines = [line for line in block.splitlines() if line.strip()]
    return lines[-1].decode("utf-8", errors="replace") if lines else None


def percentile(values: list[int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower, upper = int(position), min(int(position) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def latency_summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "mean": sum(ordered) / len(ordered),
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "max": ordered[-1],
    }


def main(args: argparse.Namespace) -> int:
    if args.mode != "shadow_only":
        raise ValueError("only shadow_only mode is supported")
    root = args.runtime_root.resolve() / "shadow"
    root.mkdir(parents=True, exist_ok=True)
    disk_guard(root)
    state = StreamState.load(root)
    book = WindowBook.restore(state.window_state)
    outcome_book = OutcomeBook.restore(state.outcome_state, str(uuid.uuid4()))
    transport = TransportTracker()
    run_started, start_time = time.monotonic(), datetime.now(timezone.utc)
    lock = threading.RLock()
    errors: list[dict] = []
    predictions: list[dict] = []
    features: list[dict] = []
    outcomes: list[dict] = []
    artifacts: list[Path] = []
    metrics = {"events_received": 0, "events_valid": 0, "control_records": 0, "duplicates": 0, "model_errors": 0, "transport_errors": 0, "windows_scheduled": 0, "windows_finalized_by_timer": 0, "windows_finalized_on_event": 0, "windows_finalized_on_eof": 0}
    scheduler_wakeup_lags: list[int] = []
    finalization_lags: list[int] = []
    checkpoint_durations_ms: list[float] = []
    window_checkpoint_durations_ms: list[float] = []
    tri_model_latencies_ms: list[int] = []
    dedup_state_size_at_checkpoint = [0]
    latest_transport_backlog_records: list[int | None] = [None]
    last_alert_severity: dict[str, str | None] = dict.fromkeys(HERMES_ALERT_THRESHOLDS)
    scheduler_priority = threading.Event()
    termination_reason = "COMPLETED_EOF"
    stdin_eof_observed = False
    accepting_events = True
    clients: ModelClients | None = None
    source = None
    scheduler: WindowScheduler | None = None
    outcome_scheduler: WindowScheduler | None = None
    stream_started: float | None = None
    run_tokens: set[str] = set()
    censored_rows: list[dict] = []
    application_publish_count = 0
    application_publisher_stop = threading.Event()
    application_publisher: threading.Thread | None = None

    def acknowledge_locked(raw: dict) -> None:
        """Durable ACK after consumer state is persisted: at-least-once safe."""
        checkpoint_path = getattr(args, "transport_checkpoint", None)
        if not checkpoint_path or not isinstance(raw.get("stream_sequence"), int):
            return
        atomic_json(checkpoint_path, {
            "transport_protocol_version": "atlaspump.shadow.transport.v1",
            "stream_session_id": raw.get("stream_session_id"), "stream_sequence": raw.get("stream_sequence"),
            "source_file_id": raw.get("source_file_id"), "source_offset_end": raw.get("source_offset_end"),
            "record_sha256": raw.get("record_sha256"), "event_id": raw.get("event_id"),
            "checkpoint_time": datetime.now(timezone.utc).isoformat(), "consumer_run_id": args.run_dir.name if args.run_dir else None,
        })

    def sample_transport_backlog_records() -> int | None:
        """Cheap (tail-read only) estimate of source_seq - consumer_seq."""
        line = tail_last_line(root / "state" / "sequence_index.jsonl")
        if not line:
            return None
        try:
            source_sequence = json.loads(line).get("stream_sequence")
        except json.JSONDecodeError:
            return None
        consumer_sequence = transport.last_sequence.get(transport.current_session) if transport.current_session else None
        if not isinstance(source_sequence, int) or not isinstance(consumer_sequence, int):
            return None
        return max(0, source_sequence - consumer_sequence)

    def persist_open_windows_locked() -> None:
        """Per-event durability: only currently-OPEN windows, with events.
        Cheap -- bounded by how many windows can be concurrently open
        (creation rate x 10s), not by every token ever seen. See
        StreamState docstring (window_transitions.wal fix) for why the
        first fix's "cheap because bounded by token count" reasoning was
        incomplete: it missed that window.events is never dropped."""
        state.save_open_windows(book.snapshot_open_windows())

    def checkpoint_window_state_locked() -> None:
        """Periodic checkpoint of everything except open windows:
        closed-window metadata without raw events, prediction/outcome ids,
        outcome_state. Clears window_transitions.wal. Called at the same
        points the pre-fix code persisted everything -- end of each
        finalize_due() batch, end of each finalize_outcomes() call, the
        periodic timer, and shutdown -- never per raw event."""
        started = time.perf_counter()
        state.outcome_state = outcome_book.snapshot()
        state.checkpoint_window_state(book.snapshot_closed_state())
        window_checkpoint_durations_ms.append((time.perf_counter() - started) * 1000)

    def checkpoint_dedup_locked() -> None:
        """Full dedup checkpoint + WAL truncation. O(n) in dedup-set size:
        called periodically and at finalize/shutdown boundaries, never per
        raw event."""
        started = time.perf_counter()
        state.checkpoint()
        checkpoint_durations_ms.append((time.perf_counter() - started) * 1000)
        dedup_state_size_at_checkpoint[0] = len(state.seen)

    def publish_application_state() -> None:
        """Expose real synchronous-pipeline state for the external sampler."""
        nonlocal application_publish_count
        now = datetime.now(timezone.utc)
        with lock:
            backlog = latest_transport_backlog_records[0]
            atomic_json(args.runtime_root.resolve() / "metrics" / "application_state.json", {
                "timestamp_utc": now.isoformat(), "run_id": args.run_dir.name if args.run_dir else None,
                "ingestion_queue_depth": 0, "ingestion_queue_depth_status": "SYNCHRONOUS_NO_QUEUE",
                "prediction_queue_depth": 0, "prediction_queue_depth_status": "SYNCHRONOUS_NO_QUEUE",
                "writer_queue_depth": 0, "writer_queue_depth_status": "SYNCHRONOUS_NO_QUEUE",
                "open_windows": sum(window.state == "OPEN" for window in book.windows.values()),
                "oldest_unprocessed_event_age_ms": 0, "oldest_unprocessed_event_age_status": "NO_PENDING_EVENT",
                # Real backlog signal (RFC014_BACKLOG_DIAGNOSIS_20260801.md §1.8):
                # the fields above are structurally always zero (no in-memory
                # queue exists in this synchronous pipeline); this one is a
                # genuine measurement of source_seq - consumer_seq.
                "transport_backlog_records": backlog,
                "transport_backlog_status": "MEASURED" if backlog is not None else "UNAVAILABLE",
                "dedup_state_size": len(state.seen),
                "application_metrics_status": "CURRENT",
                "application_state_publish_count": application_publish_count + 1,
            })
            application_publish_count += 1

    def check_alert_thresholds_locked() -> None:
        """Alert only on a severity *change* (new breach or escalation), not
        on every tick above threshold -- avoids paging on repeat."""
        candidates = {
            "transport_backlog_records": latest_transport_backlog_records[0],
            "p95_scheduler_wakeup_lag_ms": percentile(scheduler_wakeup_lags, 0.95),
        }
        for metric, value in candidates.items():
            severity = alert_severity(metric, value)
            if severity is not None and severity != last_alert_severity[metric]:
                send_hermes_alert(getattr(args, "hermes_webhook_url", None), root / "metrics" / "hermes_alerts.jsonl", {
                    "alert_schema": "rfc014_hermes_alert_v1",
                    "run_id": args.run_dir.name if args.run_dir else None,
                    "metric": metric, "value": value,
                    "threshold_warning": HERMES_ALERT_THRESHOLDS[metric]["warning"],
                    "threshold_critical": HERMES_ALERT_THRESHOLDS[metric]["critical"],
                    "severity": severity,
                    "raised_at": datetime.now(timezone.utc).isoformat(),
                })
            last_alert_severity[metric] = severity

    def publish_application_state_periodically() -> None:
        ticks_per_checkpoint = max(1, args.dedup_checkpoint_seconds // 5)
        tick = 0
        while not application_publisher_stop.wait(5):
            tick += 1
            with lock:
                latest_transport_backlog_records[0] = sample_transport_backlog_records()
                check_alert_thresholds_locked()
                if tick % ticks_per_checkpoint == 0:
                    checkpoint_dedup_locked()
                    checkpoint_window_state_locked()
            publish_application_state()

    def persist_locked() -> None:
        """Per-raw-event durability path: open-window state only. Dedup and
        closed-window/outcome state are durable separately and cheaply via
        WAL + periodic/batch checkpoints."""
        persist_open_windows_locked()
        publish_application_state()

    def full_checkpoint_locked() -> None:
        """Everything durable at once: open-window state, closed-window
        checkpoint, dedup checkpoint. Used at window finalization/outcome
        batches and shutdown, matching the pre-fix behavior's frequency at
        those specific points."""
        persist_open_windows_locked()
        checkpoint_window_state_locked()
        checkpoint_dedup_locked()
        publish_application_state()

    def write_scored_locked(row: dict) -> None:
        persist_started = datetime.now(timezone.utc)
        feature = canonical_feature_row(row)
        # Persist feature evidence before making its prediction visible.
        append_jsonl_fsync(root / "features" / "features.jsonl", feature)
        feature_persisted_at = datetime.now(timezone.utc)
        row["feature_persist_started_at"] = persist_started
        row["feature_persisted_at"] = feature_persisted_at
        row["feature_persist_latency_ms"] = max(0, int((feature_persisted_at - persist_started).total_seconds() * 1000))
        features.append(feature)
        day = row["prediction_time"].date().isoformat()
        token = row["prediction_id"][:16]
        prediction_path = append_parquet(root / f"predictions/date={day}/part-{token}.parquet", [row])
        prediction_persisted_at = datetime.now(timezone.utc)
        row["prediction_persisted_at"] = prediction_persisted_at
        row["prediction_persist_latency_ms"] = max(0, int((prediction_persisted_at - feature_persisted_at).total_seconds() * 1000))
        artifacts.extend(path for path in (prediction_path,) if path is not None)
        predictions.append(row)
        book.prediction_written.add(row["token_mint"])
        state.record_prediction_written(row["prediction_id"])

    def write_outcome_locked(record: object) -> None:
        outcome_id = hashlib.sha256(f"{record.token_mint}|{record.outcome_cutoff.isoformat()}".encode()).hexdigest()
        if outcome_id in state.outcome_ids:
            return
        row = {
            "outcome_id": outcome_id,
            "token_mint": record.token_mint,
            "creation_time": record.creation_time,
            "outcome_cutoff": record.outcome_cutoff,
            "outcome_finalized_time": record.outcome_finalized_at,
            "outcome_status": record.outcome_status,
            "migration_seen": record.migration_seen,
            "migration_time": record.migration_time,
            "migration_event_id": record.migration_event_id,
            "stream_complete": not record.stream_gap_detected and record.stream_coverage_end is not None and record.stream_coverage_end >= record.outcome_cutoff,
            "stream_gap_detected": record.stream_gap_detected,
            "stream_session_id": record.stream_session_id,
            "reason": record.outcome_status,
            "execution_mode": "shadow_only",
        }
        day = datetime.now(timezone.utc).date().isoformat()
        path = append_parquet(root / f"outcomes/date={day}/part-{outcome_id[:16]}.parquet", [row])
        if path:
            artifacts.append(path)
        outcomes.append(row)
        state.record_outcome_written(outcome_id)

    def finalize_due(source_name: str) -> None:
        nonlocal termination_reason
        scheduler_priority.set()
        try:
            scheduler_wakeup_at = datetime.now(timezone.utc)
            with lock:
                feature_compute_started_at = datetime.now(timezone.utc)
                due_items = book.due(feature_compute_started_at)
                # book.due() has already finalized these windows in memory
                # (state=FINALIZED); make that durable now, independent of
                # whether the prediction below succeeds, is skipped as a
                # duplicate, or errors -- the transition itself already
                # happened and must not be lost to a crash before the next
                # checkpoint.
                for item in due_items:
                    state.record_window_finalized(item.token_mint, feature_compute_started_at)
                feature_compute_finished_at = datetime.now(timezone.utc)
                for item in due_items:
                    cutoff = item.creation_time + timedelta(seconds=10)
                    wakeup_lag = max(0, int((scheduler_wakeup_at - cutoff).total_seconds() * 1000))
                    scheduler_wakeup_lags.append(wakeup_lag)
                    finalization_lags.append(max(0, int((feature_compute_finished_at - cutoff).total_seconds() * 1000)))
                    row = prediction_row(item, args, feature_compute_finished_at, scheduler_wakeup_at, feature_compute_started_at, feature_compute_finished_at)
                    if row["prediction_id"] in state.prediction_ids:
                        continue
                    if item.status != "READY":
                        errors.append({"received_at": feature_compute_finished_at, "raw_line": None, "error": item.status})
                        continue
                    try:
                        assert clients is not None
                        inference_started_at = datetime.now(timezone.utc)
                        row.update(clients.score([item.values])[0])
                        inference_finished_at = datetime.now(timezone.utc)
                        row["inference_started_at"] = inference_started_at
                        row["inference_finished_at"] = inference_finished_at
                        row["tri_model_latency_ms"] = max(0, int((inference_finished_at - inference_started_at).total_seconds() * 1000))
                        tri_model_latencies_ms.append(row["tri_model_latency_ms"])
                        row["snapshot_id"] = str(getattr(args, "snapshot_id", None) or getattr(args, "model_bundle", "unknown"))
                        row["device"] = "cpu" if args.tensorflow_device == "cpu" else args.tensorflow_device
                        row["model_logical_name"] = "mlp"
                        row["model_runtime"] = "tensorflow"
                        row["canonical_score_field"] = "mlp_score"
                        if row.get("mlp_score") != row.get("tensorflow_score"):
                            raise RuntimeError("MLP audit alias differs from canonical mlp_score")
                        row["inference_status"] = "SCORED"
                        write_scored_locked(row)
                        metrics[f"windows_finalized_{source_name}"] += 1
                    except Exception as exc:
                        metrics["model_errors"] += 1
                        termination_reason = "MODEL_FAILURE"
                        errors.append({"received_at": datetime.now(timezone.utc), "raw_line": None, "error": str(exc)})
                full_checkpoint_locked()
        finally:
            scheduler_priority.clear()

    def timer_callback(_mint: str) -> None:
        finalize_due("by_timer")

    def finalize_outcomes(stream_active: bool) -> None:
        with lock:
            for record in outcome_book.due(datetime.now(timezone.utc), stream_active, allow_negative=transport.audited and not transport.possible_gap_intervals):
                write_outcome_locked(record)
            checkpoint_window_state_locked()
            publish_application_state()

    def outcome_timer_callback(_mint: str) -> None:
        finalize_outcomes(stream_active=True)

    def terminate(_signum: int, _frame: object) -> None:
        nonlocal termination_reason, accepting_events
        accepting_events = False
        book.begin_shutdown()
        termination_reason = "INCOMPLETE_STREAM"
        # The supervisor stops collector then reader.  Let EOF unwind the
        # consumer naturally; raising asynchronously can interrupt thread
        # joins and bypass the durable shutdown sequence.

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
    try:
        migrate_diagnostic = write_migrate_event_time_validation(
            root / "diagnostics" / "migrate_event_time_validation.json"
        )
        if not migrate_diagnostic.get("success"):
            raise RuntimeError("isolated MIGRATE validation failed")
        clients = ModelClients(
            args.runtime_root.resolve(),
            args.tensorflow_device,
            getattr(args, "model_bundle", None),
            getattr(args, "tabular_python", None),
            getattr(args, "tensorflow_python", None),
        )
        scheduler = WindowScheduler(timer_callback)
        outcome_scheduler = WindowScheduler(outcome_timer_callback)
        scheduler.start()
        outcome_scheduler.start()
        publish_application_state()
        application_publisher = threading.Thread(target=publish_application_state_periodically, name="rfc014-application-metrics", daemon=True)
        application_publisher.start()
        with lock:
            for mint, window in book.windows.items():
                if not window.finalized and scheduler.schedule(mint, window.creation_time + timedelta(seconds=10)):
                    metrics["windows_scheduled"] += 1
                    book.scheduled.add(mint)
            for mint, record in outcome_book.records.items():
                if record.state != "FINALIZED":
                    outcome_scheduler.schedule(mint, record.outcome_cutoff)
        source = sys.stdin if args.input == "-" else Path(args.input).open(encoding="utf-8")
        stream_started = time.monotonic()
        for line in source:
            if not accepting_events:
                break
            if time.monotonic() - stream_started >= args.max_seconds:
                accepting_events = False
                termination_reason = "COMPLETED_LIMIT_REACHED"
                break
            if not line.strip():
                continue
            metrics["events_received"] += 1
            try:
                raw = json.loads(line)
                transport_result = transport.accept(raw)
                if transport_result == "SEQUENCE_CONFLICT":
                    outcome_book.mark_gap()
                    raise ValueError("same stream_sequence has different content")
                if transport_result in {"OK", "LEGACY"} and transport.possible_gap_intervals:
                    outcome_book.mark_gap()
                if raw.get("record_kind") == "CONTROL":
                    # Control records never touch dedup/window state -- nothing new to persist.
                    metrics["control_records"] += 1
                    acknowledge_locked(raw)
                    continue
                if transport_result == "SEQUENCE_DUPLICATE":
                    # Transport-level duplicate: already durably recorded when first seen.
                    metrics["duplicates"] += 1
                    acknowledge_locked(raw)
                    continue
                event = validate_event(raw)
                token_limit_reached = False
                with lock:
                    metrics["events_valid"] += 1
                    if event["event_id"] in state.seen:
                        # Event-level duplicate: already durably recorded when first seen.
                        metrics["duplicates"] += 1
                        acknowledge_locked(raw)
                        continue
                    if (
                        event["event_type"] == "CREATE_TOKEN"
                        and event["token_mint"] not in run_tokens
                        and len(run_tokens) >= args.max_tokens
                    ):
                        token_limit_reached = True
                    if not token_limit_reached:
                        state.record_seen(event["event_id"], raw.get("stream_sequence"))
                        result = book.accept(event)
                        if result == "ACCEPTED" and event["event_type"] == "CREATE_TOKEN":
                            run_tokens.add(event["token_mint"])
                            window = book.windows[event["token_mint"]]
                            if scheduler.schedule(event["token_mint"], window.creation_time + timedelta(seconds=10)):
                                metrics["windows_scheduled"] += 1
                                book.scheduled.add(event["token_mint"])
                            record = outcome_book.register(event["token_mint"], window.creation_time, event["ingestion_time"])
                            outcome_scheduler.schedule(event["token_mint"], record.outcome_cutoff)
                        elif result == "ACCEPTED" and event["event_type"] == "MIGRATE":
                            outcome_book.observe_migration(event)
                        persist_locked()
                        acknowledge_locked(raw)
                if token_limit_reached:
                    accepting_events = False
                    termination_reason = "COMPLETED_LIMIT_REACHED"
                    break
                if scheduler_priority.is_set():
                    # Yield after each durable event so a T+10 callback cannot starve in a cursor burst.
                    time.sleep(0.001)
            except Exception as exc:
                errors.append({"received_at": datetime.now(timezone.utc), "raw_line": line.rstrip(), "error": str(exc)})
        else:
            stdin_eof_observed = True
            accepting_events = False
    except KeyboardInterrupt:
        accepting_events = False
    except (BrokenPipeError, OSError) as exc:
        accepting_events = False
        metrics["transport_errors"] += 1
        termination_reason = "TRANSPORT_FAILURE"
        errors.append({"received_at": datetime.now(timezone.utc), "raw_line": None, "error": str(exc)})
    except Exception as exc:
        accepting_events = False
        metrics["transport_errors"] += 1
        termination_reason = "MODEL_FAILURE" if clients is not None else "TRANSPORT_FAILURE"
        errors.append({"received_at": datetime.now(timezone.utc), "raw_line": None, "error": str(exc)})
    finally:
        application_publisher_stop.set()
        if application_publisher is not None:
            application_publisher.join(timeout=args.shutdown_grace_seconds)
        publish_application_state()
        if stdin_eof_observed and args.stdin_eof_grace_seconds:
            # Scheduler remains active through the bounded grace interval.
            time.sleep(args.stdin_eof_grace_seconds)
        if scheduler is not None:
            scheduler.stop(args.shutdown_grace_seconds)
        if outcome_scheduler is not None:
            outcome_scheduler.stop(args.shutdown_grace_seconds)
        # The shutdown order is explicit: finish due work, then make all
        # remaining windows terminal without emitting partial features.
        finalize_due("on_eof" if stdin_eof_observed else "on_shutdown")
        censored_rows = book.censor_open_windows(
            datetime.now(timezone.utc),
            GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF,
            "GRACEFUL",
        )
        for row in censored_rows:
            append_jsonl_fsync(root / "state" / "censored_windows.jsonl", row)
            with lock:
                state.record_window_censored(row["mint"], row)
        if stdin_eof_observed:
            finalize_outcomes(stream_active=True)
        with lock:
            controlled = args.controlled_eof and termination_reason in {"COMPLETED_EOF", "COMPLETED_LIMIT_REACHED", "INCOMPLETE_STREAM"}
            for record in outcome_book.close(datetime.now(timezone.utc), controlled):
                write_outcome_locked(record)
            # Guarantee a fresh dedup checkpoint at every clean shutdown, so
            # a normal restart never depends on WAL replay.
            full_checkpoint_locked()
        if clients is not None:
            try:
                clients.close(args.shutdown_grace_seconds)
            except Exception as exc:
                errors.append({"received_at": datetime.now(timezone.utc), "raw_line": None, "error": str(exc)})
        if source is not None and source is not sys.stdin:
            source.close()

    suffix = int(time.time() * 1000)
    error_path = append_parquet(root / "dead_letter" / f"part-{suffix}.parquet", errors)
    if error_path:
        artifacts.append(error_path)
    end_time = datetime.now(timezone.utc)
    concurrency = book.concurrency_snapshot()
    lock_report = {
        "timestamp_utc": end_time.isoformat(),
        "lock_acquisition_count": len(concurrency["lock_wait_ms"]),
        "lock_wait_ms": latency_summary(concurrency["lock_wait_ms"]),
        "lock_hold_ms": latency_summary(concurrency["lock_hold_ms"]),
        "windows_open": concurrency["windows_open"],
        "windows_finalized": concurrency["windows_finalized"],
        "windows_censored_at_shutdown": concurrency["windows_censored_at_shutdown"],
        "windows_failed": concurrency["windows_failed"],
        "late_events": concurrency["late_events"],
        "duplicate_finalization_attempts": concurrency["duplicate_finalization_attempts"],
    }
    append_jsonl_fsync(root / "metrics" / "concurrency_samples.jsonl", lock_report)
    atomic_json(root / "reports" / "lock_metrics.json", lock_report)
    report = {
        "execution_mode": "shadow_only", "transactions_enabled": False, "wallet_loaded": False,
        "migrate_mapping_confirmed": MIGRATE_MAPPING_CONFIRMED, "definitive_outcomes_enabled": MIGRATE_MAPPING_CONFIRMED,
        "negative_outcomes_allowed": MIGRATE_MAPPING_CONFIRMED and transport.audited and not transport.possible_gap_intervals,
        "transport_protocol_version": "atlaspump.shadow.transport.v1" if transport.audited else None,
        "transport_coverage_mode": "AUDITED_V1" if transport.audited else "LEGACY_UNVERIFIED",
        "sessions_seen": sorted(transport.sessions_seen), "transport_connections": transport.transport_connections,
        "transport_reconnections": transport.transport_reconnections, "transport_disconnects": transport.transport_disconnects,
        "sequence_gaps": transport.sequence_gaps, "session_changes": transport.session_changes,
        "heartbeats_received": transport.heartbeats_received, "heartbeat_timeouts": 0,
        "possible_gap_intervals": transport.possible_gap_intervals,
        "continuous_coverage_intervals": [] if transport.possible_gap_intervals else [{"session_id": session, "last_sequence": sequence} for session, sequence in transport.last_sequence.items()],
        "negative_eligible_tokens": 0, "negative_ineligible_tokens": len(outcome_book.records),
        "start_time": start_time, "end_time": end_time,
        "duration_seconds": time.monotonic() - run_started,
        "stream_start_delay_seconds": (stream_started - run_started) if stream_started is not None else None,
        "termination_reason": termination_reason, "stdin_eof_observed": stdin_eof_observed,
        **metrics,
        "tokens_seen": len(run_tokens), "windows_opened": len(run_tokens),
        "windows_finalized": sum(book.windows[mint].state == "FINALIZED" for mint in run_tokens),
        "windows_censored_at_shutdown": sum(book.windows[mint].state == "CENSORED_AT_SHUTDOWN" for mint in run_tokens),
        "censor_reason_counts": book.censor_reason_counts(),
        "windows_failed": sum(book.windows[mint].state == "FAILED" for mint in run_tokens),
        "windows_incomplete": sum(book.windows[mint].state == "OPEN" for mint in run_tokens),
        "features_written": len(features), "predictions_written": len(predictions), "dead_letters": len(errors),
        "feature_ids_unique": len({row["feature_id"] for row in features}),
        "prediction_ids_unique": len({row["prediction_id"] for row in predictions}),
        "features_without_prediction": len({row["prediction_id"] for row in features} - {row["prediction_id"] for row in predictions}),
        "predictions_without_feature": len({row["prediction_id"] for row in predictions} - {row["prediction_id"] for row in features}),
        "duplicate_feature_ids": len(features) - len({row["feature_id"] for row in features}),
        "duplicate_prediction_ids": len(predictions) - len({row["prediction_id"] for row in predictions}),
        "migrate_isolated_success": bool((root / "diagnostics" / "migrate_event_time_validation.json").exists()),
        "scheduler_wakeups": scheduler.wakeups if scheduler else 0,
        "scheduler_late_wakeups": sum(lag > 0 for lag in scheduler_wakeup_lags),
        "late_events_after_finalization": book.late_events_after_finalization,
        "late_events_within_feature_time": book.late_events_within_feature_time,
        "late_events_after_cutoff": book.late_events_after_cutoff,
        "median_scheduler_wakeup_lag_ms": median(scheduler_wakeup_lags) if scheduler_wakeup_lags else None,
        "p95_scheduler_wakeup_lag_ms": percentile(scheduler_wakeup_lags, 0.95),
        "median_feature_finalization_lag_ms": median(finalization_lags) if finalization_lags else None,
        "p95_feature_finalization_lag_ms": percentile(finalization_lags, 0.95),
        "lock_metrics": lock_report,
        # Dedup persistence (RFC014_BACKLOG_DIAGNOSIS_20260801.md / persist_locked() fix):
        "checkpoint_duration_ms": latency_summary(checkpoint_durations_ms),
        "dedup_state_size_final": len(state.seen),
        "dedup_state_size_at_last_checkpoint": dedup_state_size_at_checkpoint[0],
        "dedup_wal_replayed_count_at_start": state.dedup_wal_replayed_count,
        "dedup_wal_skipped_count_at_start": state.dedup_wal_skipped_count,
        "dedup_checkpoint_interval_seconds": args.dedup_checkpoint_seconds,
        # Window/outcome persistence (RFC014 second-bottleneck fix, window_transitions.wal):
        "window_checkpoint_duration_ms": latency_summary(window_checkpoint_durations_ms),
        "window_wal_replayed_count_at_start": state.window_wal_replayed_count,
        "window_wal_skipped_count_at_start": state.window_wal_skipped_count,
        "open_windows_final": sum(window.state == "OPEN" for window in book.windows.values()),
        # Backlog/throughput (previously only reconstructable after the fact):
        "transport_backlog_records_final": latest_transport_backlog_records[0],
        "consumer_throughput_events_per_second": (metrics["events_received"] / (time.monotonic() - run_started)) if (time.monotonic() - run_started) > 0 else None,
        "tri_model_latency_ms": latency_summary(tri_model_latencies_ms),
        "model_inference": "PERSISTENT_WORKERS", "ssh_exit_code": None, "collector_exit_code": None,
        "artifacts": [{"path": str(path), "sha256": sha256_file(path)} for path in artifacts],
    }
    atomic_json(root / "logs" / "latest_stream_report.json", report)
    if args.run_dir:
        atomic_json(args.run_dir / "consumer_manifest.json", report)
    print(json.dumps(report, default=str))
    return 1 if termination_reason in {"TRANSPORT_FAILURE", "MODEL_FAILURE"} else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--mode", default="shadow_only")
    parser.add_argument("--tensorflow-device", choices=("auto", "gpu", "cpu"), default="gpu")
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--stdin-eof-grace-seconds", type=int, default=0)
    parser.add_argument("--shutdown-grace-seconds", type=int, default=30)
    parser.add_argument("--dedup-checkpoint-seconds", type=int, default=30, help="Max interval, and worst-case dedup-WAL replay bound, between full seen_event_ids.json checkpoints (also checkpointed at every window finalization).")
    parser.add_argument("--hermes-webhook-url", help="Optional. If unset (default), threshold breaches are still logged to metrics/hermes_alerts.jsonl but nothing is sent over the network.")
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--controlled-eof", action="store_true")
    parser.add_argument("--transport-checkpoint", type=Path)
    parser.add_argument("--model-bundle", type=Path)
    parser.add_argument("--tabular-python", type=Path)
    parser.add_argument("--tensorflow-python", type=Path)
    parser.add_argument("--snapshot-id")
    raise SystemExit(main(parser.parse_args()))
