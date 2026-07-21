import argparse
import importlib.util
import io
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from atlaspump.shadow_stream import (
    STREAM_SCHEMA_VERSION,
    OutcomeBook,
    TransportTracker,
    WindowBook,
    validate_event,
)

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "shadow" / "run_shadow_stream.py"
SPEC = importlib.util.spec_from_file_location("run_shadow_stream", SCRIPT)
run_shadow_stream = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(run_shadow_stream)


def event(kind="CREATE_TOKEN", second=0):
    t=datetime(2026,7,20,tzinfo=timezone.utc)+timedelta(seconds=second)
    return {"schema_version":STREAM_SCHEMA_VERSION,"event_id":f"id-{kind}-{second}","token_mint":"mint","event_type":kind,"event_time":t.isoformat(),"ingestion_time":t.isoformat(),"signature":"sig","wallet":"wallet","protocol_scope":"pump_fun","source":"collector","sequence_index":second,"quality_flags":[]}

def test_schema_and_window_finalize_without_future_events():
    book=WindowBook(); created=validate_event(event()); assert book.accept(created)=="ACCEPTED"
    assert book.accept(validate_event(event("BUY", 5)))=="ACCEPTED"
    due=book.due(created["event_time"]+timedelta(seconds=10))
    assert due[0].status=="READY" and due[0].values["events_10s"]==2

def test_invalid_stream_event_goes_to_dead_letter_boundary():
    bad=event(); del bad["event_id"]
    with pytest.raises(ValueError): validate_event(bad)


def test_unconfirmed_migrate_cannot_create_a_definitive_outcome():
    book = WindowBook()
    created = validate_event(event())
    book.accept(created)
    book.accept(validate_event(event("MIGRATE", 20)))
    outcome = book.outcome("mint", created["event_time"] + timedelta(minutes=5))
    assert outcome["outcome_status"] == "UNKNOWN"


def test_tensorflow_library_paths_are_package_lib_directories(tmp_path):
    (tmp_path / "cuda_runtime" / "lib").mkdir(parents=True)
    (tmp_path / "cudnn" / "lib").mkdir(parents=True)
    (tmp_path / "not_a_library").mkdir()
    paths = run_shadow_stream.tensorflow_library_paths(tmp_path)
    assert paths == sorted([str(tmp_path / "cuda_runtime" / "lib"), str(tmp_path / "cudnn" / "lib")])


def test_scheduler_wakes_without_a_new_stream_event():
    called = threading.Event()
    scheduler = run_shadow_stream.WindowScheduler(lambda _mint: called.set())
    scheduler.start()
    assert scheduler.schedule("mint", datetime.now(timezone.utc)) is True
    assert called.wait(0.5)
    scheduler.stop(1)
    assert not scheduler.thread.is_alive()


class FakeClients:
    closed = False

    def __init__(self, *_args):
        pass

    def score(self, _rows):
        return [{"catboost_score": 0.1, "xgboost_score": 0.2, "tensorflow_score": 0.3}]

    def close(self, _timeout):
        type(self).closed = True


def cli_args(tmp_path):
    return argparse.Namespace(
        runtime_root=tmp_path, input="-", mode="shadow_only", tensorflow_device="gpu",
        max_seconds=60, max_tokens=10, stdin_eof_grace_seconds=0,
        shutdown_grace_seconds=1, run_dir=tmp_path / "run", controlled_eof=False,
    )


def test_eof_finalizes_due_window_writes_manifest_and_prediction_timestamps(tmp_path, monkeypatch):
    created = datetime.now(timezone.utc) - timedelta(seconds=11)
    row = event(); row.update(event_time=created.isoformat(), ingestion_time=created.isoformat())
    monkeypatch.setattr(run_shadow_stream, "ModelClients", FakeClients)
    monkeypatch.setattr(run_shadow_stream, "disk_guard", lambda _root: None)
    monkeypatch.setattr(run_shadow_stream.sys, "stdin", io.StringIO(json.dumps(row) + "\n"))
    assert run_shadow_stream.main(cli_args(tmp_path)) == 0
    manifest = json.loads((tmp_path / "run" / "consumer_manifest.json").read_text())
    assert manifest["termination_reason"] == "COMPLETED_EOF"
    assert manifest["stdin_eof_observed"] is True
    assert manifest["predictions_written"] == 1
    assert FakeClients.closed is True
    import pyarrow.parquet as pq
    prediction = pq.read_table(next((tmp_path / "shadow" / "predictions").rglob("*.parquet"))).to_pylist()[0]
    assert prediction["prediction_time"] is not None
    assert prediction["latency_from_cutoff_ms"] >= 0
    assert prediction["outcome_status"] == "UNKNOWN"


def test_timer_finalizes_without_later_event_or_eof_flush(tmp_path, monkeypatch):
    created = datetime.now(timezone.utc) - timedelta(seconds=11)
    row = event(); row.update(event_time=created.isoformat(), ingestion_time=created.isoformat())
    args = cli_args(tmp_path); args.stdin_eof_grace_seconds = 0.05
    monkeypatch.setattr(run_shadow_stream, "ModelClients", FakeClients)
    monkeypatch.setattr(run_shadow_stream, "disk_guard", lambda _root: None)
    monkeypatch.setattr(run_shadow_stream.sys, "stdin", io.StringIO(json.dumps(row) + "\n"))
    assert run_shadow_stream.main(args) == 0
    manifest = json.loads((tmp_path / "run" / "consumer_manifest.json").read_text())
    assert manifest["windows_finalized_by_timer"] == 1
    assert manifest["windows_finalized_on_eof"] == 0
    assert manifest["predictions_written"] == 1


def test_timer_scores_while_stdin_is_still_open(tmp_path, monkeypatch):
    created = datetime.now(timezone.utc) - timedelta(seconds=11)
    row = event(); row.update(event_time=created.isoformat(), ingestion_time=created.isoformat())
    read_fd, write_fd = os.pipe()
    reader = os.fdopen(read_fd, "r", encoding="utf-8")
    writer = os.fdopen(write_fd, "w", encoding="utf-8")
    args = cli_args(tmp_path)
    monkeypatch.setattr(run_shadow_stream, "ModelClients", FakeClients)
    monkeypatch.setattr(run_shadow_stream, "disk_guard", lambda _root: None)
    monkeypatch.setattr(run_shadow_stream.sys, "stdin", reader)
    worker = threading.Thread(target=run_shadow_stream.main, args=(args,))
    worker.start()
    writer.write(json.dumps(row) + "\n"); writer.flush()
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline and not list((tmp_path / "shadow" / "predictions").rglob("*.parquet")):
        time.sleep(0.01)
    assert list((tmp_path / "shadow" / "predictions").rglob("*.parquet"))
    assert worker.is_alive()  # No EOF was needed to score the window.
    writer.close(); worker.join(1)
    assert not worker.is_alive()


def test_late_event_after_finalization_cannot_change_features():
    book = WindowBook()
    created = validate_event(event())
    book.accept(created)
    book.due(created["event_time"] + timedelta(seconds=10))
    late = validate_event(event("BUY", 5) | {"event_id": "late"})
    assert book.accept(late) == "LATE_AFTER_FINALIZATION"
    assert book.late_events_after_finalization == 1
    assert book.late_events_within_feature_time == 1


def test_window_state_restores_open_and_finalized_windows():
    book = WindowBook()
    created = validate_event(event())
    book.accept(created)
    restored = WindowBook.restore(book.snapshot())
    assert not restored.windows["mint"].finalized
    restored.due(created["event_time"] + timedelta(seconds=10))
    again = WindowBook.restore(restored.snapshot())
    assert again.windows["mint"].finalized
    assert again.snapshot()["windows"]["mint"]["state"] == "FINALIZED"


def test_outcomes_positive_negative_and_incomplete_coverage():
    created = datetime(2026, 7, 20, tzinfo=timezone.utc)
    positive = OutcomeBook("session"); positive.register("mint", created, created)
    positive.observe_migration(validate_event(event("MIGRATE", 60) | {"event_id": "migration"}))
    assert positive.due(created + timedelta(minutes=5), stream_active=True)[0].outcome_status == "POSITIVE"
    negative = OutcomeBook("session"); negative.register("mint", created, created)
    assert negative.due(created + timedelta(minutes=5), stream_active=True)[0].outcome_status == "NEGATIVE"
    assert not negative.due(created + timedelta(minutes=6), stream_active=True)
    incomplete = OutcomeBook("session"); incomplete.register("mint", created, created)
    assert incomplete.close(created + timedelta(seconds=20), controlled=False)[0].outcome_status == "INCOMPLETE_STREAM"
    censored = OutcomeBook("session"); censored.register("mint", created, created)
    assert censored.close(created + timedelta(seconds=20), controlled=True)[0].outcome_status == "CENSORED"


def test_migration_wrong_mint_or_outside_window_is_not_positive():
    created = datetime(2026, 7, 20, tzinfo=timezone.utc)
    outcomes = OutcomeBook("session"); outcomes.register("mint", created, created)
    wrong = validate_event(event("MIGRATE", 60) | {"token_mint": "other", "event_id": "wrong"})
    late = validate_event(event("MIGRATE", 301) | {"event_id": "late-migration"})
    assert not outcomes.observe_migration(wrong)
    assert not outcomes.observe_migration(late)
    assert outcomes.due(created + timedelta(minutes=5), stream_active=True)[0].outcome_status == "NEGATIVE"


def test_outcome_restart_marks_unmeasurable_gap_and_forbids_negative():
    created = datetime(2026, 7, 20, tzinfo=timezone.utc)
    original = OutcomeBook("session-a"); original.register("mint", created, created)
    restored = OutcomeBook.restore(original.snapshot(), "session-b")
    result = restored.due(created + timedelta(minutes=5), stream_active=True)[0]
    assert result.stream_gap_detected is True
    assert result.outcome_status == "UNKNOWN"


def test_eof_with_partial_json_is_dead_lettered_and_young_window_is_incomplete(tmp_path, monkeypatch):
    row = event()
    now = datetime.now(timezone.utc)
    row.update(event_time=now.isoformat(), ingestion_time=now.isoformat())
    monkeypatch.setattr(run_shadow_stream, "ModelClients", FakeClients)
    monkeypatch.setattr(run_shadow_stream, "disk_guard", lambda _root: None)
    monkeypatch.setattr(run_shadow_stream.sys, "stdin", io.StringIO(json.dumps(row) + "\n{partial"))
    assert run_shadow_stream.main(cli_args(tmp_path)) == 0
    manifest = json.loads((tmp_path / "run" / "consumer_manifest.json").read_text())
    assert manifest["dead_letters"] == 1
    assert manifest["windows_incomplete"] == 1
    import pyarrow.parquet as pq
    outcome = pq.read_table(next((tmp_path / "shadow" / "outcomes").rglob("*.parquet"))).to_pylist()[0]
    assert outcome["outcome_status"] == "INCOMPLETE_STREAM"


def test_transport_sequences_heartbeats_and_duplicates_are_audited():
    tracker = TransportTracker()
    record = {"schema_version": STREAM_SCHEMA_VERSION, "record_kind": "EVENT", "stream_session_id": "s", "stream_sequence": 1, "event_id": "one"}
    assert tracker.accept(record) == "OK"
    assert tracker.accept(record) == "SEQUENCE_DUPLICATE"
    heartbeat = {"schema_version": "atlaspump.shadow.transport.v1", "record_kind": "CONTROL", "control_type": "HEARTBEAT", "stream_session_id": "s", "stream_sequence": 2}
    assert tracker.accept(heartbeat) == "OK"
    assert tracker.heartbeats_received == 1
    assert tracker.audited


def test_transport_sequence_gap_and_conflict_forbid_clean_coverage():
    tracker = TransportTracker()
    base = {"schema_version": STREAM_SCHEMA_VERSION, "record_kind": "EVENT", "stream_session_id": "s"}
    assert tracker.accept(base | {"stream_sequence": 1, "event_id": "one"}) == "OK"
    assert tracker.accept(base | {"stream_sequence": 3, "event_id": "three"}) == "OK"
    assert tracker.sequence_gaps == 1
    assert tracker.possible_gap_intervals
    assert tracker.accept(base | {"stream_sequence": 3, "event_id": "different"}) == "SEQUENCE_CONFLICT"
