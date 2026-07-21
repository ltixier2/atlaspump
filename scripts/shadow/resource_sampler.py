#!/usr/bin/env python3
"""Run-local, fsync-flushed resource sampling for offline RFC-014 replays.

This module is deliberately independent of the live transport and has no
network, wallet, or trading dependency.  It is copied into the immutable WSL
replay snapshot and is started by the replay process itself.
"""
from __future__ import annotations

import json
import os
import threading
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

Counters = Callable[[], Mapping[str, int | None]]
Targets = Callable[[], Mapping[str, int]]


class ResourceSampler:
    """Sample each registered PID immediately and at a bounded interval."""

    def __init__(
        self,
        runtime: Path,
        run_id: str,
        targets: Targets,
        counters: Counters,
        interval_seconds: float = 30.0,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.path = runtime / "metrics" / "resource_samples.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.targets = targets
        self.counters = counters
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="rfc014-resource-sampler")

    def start(self) -> None:
        """Write the required first sample synchronously, then start periodic work."""
        self.sample_once()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=self.interval_seconds + 5.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.sample_once()

    def sample_once(self) -> list[dict[str, Any]]:
        counters = dict(self.counters())
        rows = [self._sample(name, pid, counters) for name, pid in self.targets().items()]
        with self.path.open("a", encoding="utf-8", buffering=1) as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        return rows

    def _sample(self, process_name: str, pid: int, counters: Mapping[str, int | None]) -> dict[str, Any]:
        row: dict[str, Any] = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "process_name": process_name,
            "pid": pid,
            "system_load_1m": os.getloadavg()[0],
            "system_memory_available_bytes": psutil.virtual_memory().available,
            "disk_free_bytes": psutil.disk_usage(self.path.parent).free,
            "open_windows": counters.get("open_windows", 0),
            "features_written": counters.get("features_written", 0),
            "predictions_written": counters.get("predictions_written", 0),
            "outcomes_written": counters.get("outcomes_written", 0),
            "ingestion_queue_depth": counters.get("ingestion_queue_depth", 0),
            "prediction_queue_depth": counters.get("prediction_queue_depth", 0),
            "writer_queue_depth": counters.get("writer_queue_depth", 0),
        }
        try:
            process = psutil.Process(pid)
            raw_status = process.status().upper()
            memory = process.memory_info()
            active = raw_status != psutil.STATUS_ZOMBIE.upper()
            row.update(
                {
                    # A sampled Python worker often sleeps while waiting for an
                    # input batch; that is still a living supervised process.
                    "process_status": "RUNNING" if active else "ZOMBIE",
                    "raw_process_status": raw_status,
                    "sample_valid": active,
                    "cpu_percent": process.cpu_percent(None),
                    "rss_bytes": memory.rss,
                    "vms_bytes": memory.vms,
                    "thread_count": process.num_threads(),
                    "error": None,
                }
            )
            if not row["sample_valid"]:
                row["error"] = f"process_status={raw_status}"
        except psutil.NoSuchProcess:
            row.update(
                {
                    "process_status": "EXITED",
                    "sample_valid": False,
                    "cpu_percent": None,
                    "rss_bytes": None,
                    "vms_bytes": None,
                    "thread_count": None,
                    "error": "PID_EXITED",
                }
            )
        except psutil.AccessDenied:
            row.update(
                {
                    "process_status": "ACCESS_DENIED",
                    "sample_valid": False,
                    "cpu_percent": None,
                    "rss_bytes": None,
                    "vms_bytes": None,
                    "thread_count": None,
                    "error": "ACCESS_DENIED",
                }
            )
        return row
