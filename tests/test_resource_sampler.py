import importlib.util
import json
import os
import time
from pathlib import Path

SAMPLER_PATH = Path(__file__).parents[1] / "scripts/shadow/resource_sampler.py"
SPEC = importlib.util.spec_from_file_location("resource_sampler", SAMPLER_PATH)
assert SPEC and SPEC.loader
RESOURCE_SAMPLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RESOURCE_SAMPLER)
ResourceSampler = RESOURCE_SAMPLER.ResourceSampler


def test_sampler_writes_immediate_and_periodic_valid_rows(tmp_path: Path):
    sampler = ResourceSampler(
        tmp_path,
        "run",
        lambda: {"replay": os.getpid()},
        lambda: {"open_windows": 1, "features_written": 2, "predictions_written": 2, "outcomes_written": 0},
        interval_seconds=0.02,
    )
    sampler.start()
    time.sleep(0.06)
    sampler.stop()
    rows = [json.loads(line) for line in (tmp_path / "metrics/resource_samples.jsonl").read_text().splitlines()]
    assert len(rows) >= 3
    assert all(row["sample_valid"] for row in rows)
    assert all(row["process_status"] == "RUNNING" for row in rows)
    assert all(row["rss_bytes"] > 0 and row["vms_bytes"] > 0 and row["thread_count"] > 0 for row in rows)
    assert [row["timestamp_utc"] for row in rows] == sorted(row["timestamp_utc"] for row in rows)


def test_sampler_marks_disappeared_pid_invalid(tmp_path: Path):
    sampler = ResourceSampler(tmp_path, "run", lambda: {"gone": 999_999_999}, lambda: {}, interval_seconds=1)
    row = sampler.sample_once()[0]
    assert row["sample_valid"] is False
    assert row["process_status"] == "EXITED"
    assert row["error"] == "PID_EXITED"
