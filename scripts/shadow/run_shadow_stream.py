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

from atlaspump.shadow_inference import CONTRACT_VERSION, prediction_id  # noqa: E402
from atlaspump.shadow_stream import (  # noqa: E402
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


def prediction_row(item: object, args: argparse.Namespace, finalized_at: datetime, scheduler_wakeup_lag_ms: int) -> dict:
    cutoff = item.creation_time + timedelta(seconds=10)
    return {
        "prediction_id": prediction_id(item.token_mint, item.creation_time),
        "token_mint": item.token_mint,
        "creation_time": item.creation_time,
        "feature_cutoff": cutoff,
        "feature_finalized_time": finalized_at,
        "prediction_time": finalized_at,
        "event_arrival_lag_ms": item.event_arrival_lag_ms,
        "scheduler_wakeup_lag_ms": scheduler_wakeup_lag_ms,
        "feature_finalization_lag_ms": int((finalized_at - cutoff).total_seconds() * 1000),
        "latency_from_cutoff_ms": int((finalized_at - cutoff).total_seconds() * 1000),
        "latency_from_creation_ms": int((finalized_at - item.creation_time).total_seconds() * 1000),
        "prediction_execution": "LIVE" if args.input == "-" else "REPLAY",
        "execution_mode": "shadow_only",
        "outcome_status": "UNKNOWN",
        "feature_contract_version": CONTRACT_VERSION,
        "feature_status": item.status,
        "events_received": item.event_count,
        "max_ingestion_delay_ms": item.max_ingestion_delay_ms,
        "rejected_after_cutoff": item.rejected_after_cutoff,
        **item.values,
    }


def percentile(values: list[int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower, upper = int(position), min(int(position) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


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
    outcomes: list[dict] = []
    artifacts: list[Path] = []
    metrics = {"events_received": 0, "events_valid": 0, "control_records": 0, "duplicates": 0, "model_errors": 0, "transport_errors": 0, "windows_scheduled": 0, "windows_finalized_by_timer": 0, "windows_finalized_on_event": 0, "windows_finalized_on_eof": 0}
    scheduler_wakeup_lags: list[int] = []
    finalization_lags: list[int] = []
    termination_reason = "COMPLETED_EOF"
    stdin_eof_observed = False
    accepting_events = True
    clients: ModelClients | None = None
    source = None
    scheduler: WindowScheduler | None = None
    outcome_scheduler: WindowScheduler | None = None
    stream_started: float | None = None
    run_tokens: set[str] = set()

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

    def persist_locked() -> None:
        state.window_state = book.snapshot()
        state.outcome_state = outcome_book.snapshot()
        state.save()

    def write_scored_locked(row: dict) -> None:
        day = row["prediction_time"].date().isoformat()
        token = row["prediction_id"][:16]
        prediction_path = append_parquet(root / f"predictions/date={day}/part-{token}.parquet", [row])
        artifacts.extend(path for path in (prediction_path,) if path is not None)
        predictions.append(row)
        book.prediction_written.add(row["token_mint"])
        state.prediction_ids.add(row["prediction_id"])

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
        state.outcome_ids.add(outcome_id)

    def finalize_due(source_name: str) -> None:
        nonlocal termination_reason
        finalized_at = datetime.now(timezone.utc)
        with lock:
            for item in book.due(finalized_at):
                cutoff = item.creation_time + timedelta(seconds=10)
                wakeup_lag = max(0, int((finalized_at - cutoff).total_seconds() * 1000))
                scheduler_wakeup_lags.append(wakeup_lag)
                finalization_lags.append(wakeup_lag)
                row = prediction_row(item, args, finalized_at, wakeup_lag)
                if row["prediction_id"] in state.prediction_ids:
                    continue
                if item.status != "READY":
                    errors.append({"received_at": finalized_at, "raw_line": None, "error": item.status})
                    continue
                try:
                    assert clients is not None
                    row.update(clients.score([item.values])[0])
                    row["inference_status"] = "SCORED"
                    write_scored_locked(row)
                    metrics[f"windows_finalized_{source_name}"] += 1
                except Exception as exc:
                    metrics["model_errors"] += 1
                    termination_reason = "MODEL_FAILURE"
                    errors.append({"received_at": datetime.now(timezone.utc), "raw_line": None, "error": str(exc)})
            persist_locked()

    def timer_callback(_mint: str) -> None:
        finalize_due("by_timer")

    def finalize_outcomes(stream_active: bool) -> None:
        with lock:
            for record in outcome_book.due(datetime.now(timezone.utc), stream_active, allow_negative=transport.audited and not transport.possible_gap_intervals):
                write_outcome_locked(record)
            persist_locked()

    def outcome_timer_callback(_mint: str) -> None:
        finalize_outcomes(stream_active=True)

    def terminate(_signum: int, _frame: object) -> None:
        nonlocal termination_reason, accepting_events
        accepting_events = False
        termination_reason = "INCOMPLETE_STREAM"
        raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
    try:
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
                    metrics["control_records"] += 1
                    persist_locked()
                    acknowledge_locked(raw)
                    continue
                if transport_result == "SEQUENCE_DUPLICATE":
                    metrics["duplicates"] += 1
                    with lock:
                        persist_locked()
                        acknowledge_locked(raw)
                    continue
                event = validate_event(raw)
                token_limit_reached = False
                with lock:
                    metrics["events_valid"] += 1
                    if event["event_id"] in state.seen:
                        metrics["duplicates"] += 1
                        persist_locked()
                        acknowledge_locked(raw)
                        continue
                    if (
                        event["event_type"] == "CREATE_TOKEN"
                        and event["token_mint"] not in run_tokens
                        and len(run_tokens) >= args.max_tokens
                    ):
                        token_limit_reached = True
                    if not token_limit_reached:
                        state.seen.add(event["event_id"])
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
        if stdin_eof_observed and args.stdin_eof_grace_seconds:
            # Scheduler remains active through the bounded grace interval.
            time.sleep(args.stdin_eof_grace_seconds)
        if scheduler is not None:
            scheduler.stop(args.shutdown_grace_seconds)
        if outcome_scheduler is not None:
            outcome_scheduler.stop(args.shutdown_grace_seconds)
        if stdin_eof_observed:
            finalize_due("on_eof")
            finalize_outcomes(stream_active=True)
        with lock:
            controlled = args.controlled_eof and termination_reason in {"COMPLETED_EOF", "COMPLETED_LIMIT_REACHED", "INCOMPLETE_STREAM"}
            for record in outcome_book.close(datetime.now(timezone.utc), controlled):
                write_outcome_locked(record)
            persist_locked()
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
        "windows_finalized": sum(book.windows[mint].finalized for mint in run_tokens),
        "windows_incomplete": sum(not book.windows[mint].finalized for mint in run_tokens),
        "predictions_written": len(predictions), "dead_letters": len(errors),
        "scheduler_wakeups": scheduler.wakeups if scheduler else 0,
        "scheduler_late_wakeups": sum(lag > 0 for lag in scheduler_wakeup_lags),
        "late_events_after_finalization": book.late_events_after_finalization,
        "late_events_within_feature_time": book.late_events_within_feature_time,
        "late_events_after_cutoff": book.late_events_after_cutoff,
        "median_scheduler_wakeup_lag_ms": median(scheduler_wakeup_lags) if scheduler_wakeup_lags else None,
        "p95_scheduler_wakeup_lag_ms": percentile(scheduler_wakeup_lags, 0.95),
        "median_feature_finalization_lag_ms": median(finalization_lags) if finalization_lags else None,
        "p95_feature_finalization_lag_ms": percentile(finalization_lags, 0.95),
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
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--controlled-eof", action="store_true")
    parser.add_argument("--transport-checkpoint", type=Path)
    parser.add_argument("--model-bundle", type=Path)
    parser.add_argument("--tabular-python", type=Path)
    parser.add_argument("--tensorflow-python", type=Path)
    raise SystemExit(main(parser.parse_args()))
