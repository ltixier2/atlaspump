"""Tests for the persist_window_state_locked() second-bottleneck fix.

Context: the first fix (persist_locked(), dedup) revealed a second one at
real scale: window_outcome_state.json was rewritten in full on every raw
event, and TokenWindow never drops its raw `events` list after
finalization -- at 2351 tokens that file reached 5.1 MB, ~27 min of
cumulative cost over a 2h replay (RFC014_CANDIDATE_SNAPSHOT_VALIDATION_
20260802.md). `events` is provably dead weight once a window leaves OPEN
(WindowBook._due_locked()/censor_open_windows() only ever touch OPEN
windows). The fix splits durability three ways: open_windows_state.json
(cheap, per-event, open windows only, with events), window_transitions.wal
(append-only, O(1) per finalize/censor/prediction/outcome), and
window_outcome_state.json (periodic checkpoint of everything else, without
events).

These tests cover: window snapshot correctness (events kept for OPEN,
dropped for closed), WAL recovery (before checkpoint, after checkpoint,
replayed twice, torn write), window finalization, shutdown censorship,
absence of orphaned features/predictions, sequence integrity, and a
before/after benchmark at multi-MB scale.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Force real pyarrow into sys.modules before any test_shadow_artifact_contract
# style stub can claim it -- see test_shadow_dedup_persistence.py for why.
import pyarrow  # noqa: F401
import pyarrow.parquet  # noqa: F401

from atlaspump.shadow_stream import GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF, StreamState, WindowBook

ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str):
    path = ROOT / "scripts" / "shadow" / name
    spec = importlib.util.spec_from_file_location(name.replace(".py", "") + "_window_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def raw_event(mint: str, event_type: str, when: datetime, event_id: str | None = None) -> dict:
    return {
        "token_mint": mint, "event_type": event_type, "event_time": when, "ingestion_time": when,
        "event_id": event_id or f"{mint}-{event_type}-{when.timestamp()}",
    }


# ---------------------------------------------------------------------------
# WindowBook: split snapshots keep/drop events correctly
# ---------------------------------------------------------------------------

def test_open_window_snapshot_keeps_events_closed_snapshot_drops_them() -> None:
    book = WindowBook()
    still_open_created = datetime.now(UTC)  # within its 10s window: not due yet
    book.accept(raw_event("open-mint", "CREATE_TOKEN", still_open_created))
    book.accept(raw_event("open-mint", "BUY", still_open_created + timedelta(seconds=1)))
    finalized_created = datetime.now(UTC) - timedelta(seconds=20)  # past its 10s window: due
    book.accept(raw_event("closed-mint", "CREATE_TOKEN", finalized_created))
    assert len(book.due(datetime.now(UTC))) == 1  # finalizes "closed-mint" only

    open_snapshot = book.snapshot_open_windows()
    assert set(open_snapshot["windows"]) == {"open-mint"}
    assert len(open_snapshot["windows"]["open-mint"]["events"]) == 2

    closed_snapshot = book.snapshot_closed_state()
    assert set(closed_snapshot["windows"]) == {"closed-mint"}
    assert "events" not in closed_snapshot["windows"]["closed-mint"]
    assert closed_snapshot["windows"]["closed-mint"]["state"] == "FINALIZED"


def test_closed_snapshot_size_excludes_bulk_of_a_busy_window() -> None:
    """The whole point: a window with many events shrinks drastically once closed."""
    book = WindowBook()
    created = datetime.now(UTC) - timedelta(seconds=20)
    book.accept(raw_event("busy-mint", "CREATE_TOKEN", created))
    for index in range(200):
        book.accept(raw_event("busy-mint", "BUY", created + timedelta(milliseconds=index), event_id=f"busy-{index}"))
    open_size = len(json.dumps(book.snapshot_open_windows()))
    assert len(book.due(datetime.now(UTC))) == 1
    closed_size = len(json.dumps(book.snapshot_closed_state()))
    assert closed_size * 20 < open_size, f"expected closed snapshot to be far smaller: open={open_size} closed={closed_size}"


# ---------------------------------------------------------------------------
# StreamState: window WAL recovery
# ---------------------------------------------------------------------------

def test_recovery_before_checkpoint_recovers_open_window_events(tmp_path: Path) -> None:
    """Interruption before any periodic checkpoint: open_windows_state.json
    alone must be enough to recover a window's accumulated events."""
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    created = datetime.now(UTC) - timedelta(seconds=3)
    state.save_open_windows({
        "windows": {"mint-a": {"creation_time": created.isoformat(), "state": "OPEN", "events": [{"event_id": "e1"}, {"event_id": "e2"}]}},
        "delays": {}, "rejected": {}, "creation_arrival_lags": {},
    })
    # No checkpoint_window_state() call -- simulates a crash right here.
    recovered = StreamState.load(root)
    assert recovered.window_state["windows"]["mint-a"]["state"] == "OPEN"
    assert len(recovered.window_state["windows"]["mint-a"]["events"]) == 2


def test_recovery_after_checkpoint_drops_events_but_keeps_finalized_state(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    created = datetime.now(UTC) - timedelta(seconds=20)
    state.save_open_windows({
        "windows": {"mint-a": {"creation_time": created.isoformat(), "state": "OPEN", "events": [{"event_id": "e1"}]}},
        "delays": {}, "rejected": {}, "creation_arrival_lags": {},
    })
    state.record_window_finalized("mint-a", datetime.now(UTC))
    state.checkpoint_window_state({
        "windows": {"mint-a": {"creation_time": created.isoformat(), "state": "FINALIZED", "finalized": True}},
        "delays": {}, "rejected": {}, "creation_arrival_lags": {},
        "late_events_after_finalization": 0, "late_events_within_feature_time": 0, "late_events_after_cutoff": 0,
        "shutdown_started": False, "censored_at_shutdown": {}, "failed": {},
    })
    assert state._window_wal_path().read_text() == ""  # truncated after the checkpoint it supersedes

    recovered = StreamState.load(root)
    assert recovered.window_state["windows"]["mint-a"]["state"] == "FINALIZED"
    assert "events" not in recovered.window_state["windows"]["mint-a"]


def test_window_wal_replayed_twice_is_idempotent(tmp_path: Path) -> None:
    """Replaying window_transitions.wal on top of an already-consistent
    state (e.g. loading twice without any new mutation) must not error or
    change the outcome -- same idempotency guarantee as the dedup WAL."""
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    created = datetime.now(UTC) - timedelta(seconds=20)
    state.save_open_windows({"windows": {"mint-a": {"creation_time": created.isoformat(), "state": "OPEN", "events": []}}, "delays": {}, "rejected": {}, "creation_arrival_lags": {}})
    state.record_window_finalized("mint-a", datetime.now(UTC))
    state.record_prediction_written("pred-1")

    first = StreamState.load(root)
    assert first.window_state["windows"]["mint-a"]["state"] == "FINALIZED"
    assert first.prediction_ids == {"pred-1"}
    assert first.window_wal_replayed_count == 2  # WINDOW_FINALIZED + PREDICTION_WRITTEN

    second = StreamState.load(root)  # replay the same WAL a second time, from scratch
    assert second.window_state["windows"]["mint-a"]["state"] == "FINALIZED"
    assert second.prediction_ids == {"pred-1"}
    assert second.window_wal_replayed_count == 2


def test_window_wal_interruption_mid_append_is_skipped_not_fatal(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    created = datetime.now(UTC) - timedelta(seconds=20)
    state.save_open_windows({"windows": {"mint-a": {"creation_time": created.isoformat(), "state": "OPEN", "events": []}}, "delays": {}, "rejected": {}, "creation_arrival_lags": {}})
    state.record_window_finalized("mint-a", datetime.now(UTC))
    with state._window_wal_path().open("a", encoding="utf-8") as handle:
        handle.write('{"type": "PREDICTION_WRITTEN", "prediction_i')  # torn write

    recovered = StreamState.load(root)
    assert recovered.window_state["windows"]["mint-a"]["state"] == "FINALIZED"
    assert recovered.window_wal_skipped_count == 1
    assert recovered.prediction_ids == set()


def test_window_censored_transition_survives_restart(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    created = datetime.now(UTC) - timedelta(seconds=3)
    state.save_open_windows({"windows": {"mint-a": {"creation_time": created.isoformat(), "state": "OPEN", "events": []}}, "delays": {}, "rejected": {}, "creation_arrival_lags": {}})
    censor_row = {
        "mint": "mint-a", "state": "CENSORED_AT_SHUTDOWN", "censor_reason": GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF,
        "shutdown_type": "GRACEFUL", "state_transition_count": 1, "prediction_created": False,
    }
    state.record_window_censored("mint-a", censor_row)

    recovered = StreamState.load(root)
    assert recovered.window_state["windows"]["mint-a"]["state"] == "CENSORED_AT_SHUTDOWN"
    assert recovered.window_state["censored_at_shutdown"]["mint-a"]["censor_reason"] == GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF
    restored_book = WindowBook.restore(recovered.window_state)
    assert restored_book.windows["mint-a"].state == "CENSORED_AT_SHUTDOWN"
    assert restored_book.censored_at_shutdown["mint-a"]["censor_reason"] == GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF


# ---------------------------------------------------------------------------
# Benchmark: multi-MB state, before (bundled+events) vs after (split)
# ---------------------------------------------------------------------------

def test_split_checkpoint_is_much_cheaper_than_bundled_at_multi_mb_scale() -> None:
    """Reproduces, at the scale that revealed the bug (RFC014_CANDIDATE_
    SNAPSHOT_VALIDATION_20260802.md: 2351 tokens, 5.1 MB), the cost of the
    old bundled-with-events snapshot vs the new closed-state-without-events
    checkpoint."""
    book = WindowBook()
    created = datetime.now(UTC) - timedelta(seconds=20)
    # ~46 events/token mirrors the real 2h replay's ratio (107,542 valid
    # events / 2351 tokens) that produced the 5.1 MB file.
    for token_index in range(2351):
        mint = f"mint-{token_index}"
        book.accept(raw_event(mint, "CREATE_TOKEN", created, event_id=f"create-{token_index}"))
        for event_index in range(45):
            book.accept(raw_event(mint, "BUY", created + timedelta(milliseconds=event_index), event_id=f"buy-{token_index}-{event_index}"))
    finalized = book.due(datetime.now(UTC))
    assert len(finalized) == 2351

    old_bundled_size = len(json.dumps(book.snapshot()))  # pre-fix equivalent: every window, events included
    new_closed_size = len(json.dumps(book.snapshot_closed_state()))  # this fix: no events for closed windows
    assert new_closed_size * 5 < old_bundled_size, f"old={old_bundled_size} new={new_closed_size}"

    started = time.perf_counter()
    json.dumps(book.snapshot())
    old_ms = (time.perf_counter() - started) * 1000
    started = time.perf_counter()
    json.dumps(book.snapshot_closed_state())
    new_ms = (time.perf_counter() - started) * 1000
    assert new_ms * 3 < old_ms, f"old={old_ms:.2f}ms new={new_ms:.2f}ms"


# ---------------------------------------------------------------------------
# End-to-end: real ingestion loop, fake ML subprocess layer only
# ---------------------------------------------------------------------------

class _FakeModelClients:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def score(self, rows: list[dict]) -> list[dict]:
        return [{"catboost_score": 0.5, "xgboost_score": 0.5, "mlp_score": 0.5, "tensorflow_score": 0.5} for _ in rows]

    def close(self, _timeout: int) -> None:
        pass


def _control(kind: str, sequence: int, session: str) -> dict:
    return {"record_kind": "CONTROL", "control_type": kind, "schema_version": "atlaspump.shadow.transport.v1", "stream_session_id": session, "stream_sequence": sequence}


def _event(sequence: int, session: str, mint: str, event_type: str = "CREATE_TOKEN") -> dict:
    return {
        "schema_version": "atlaspump.shadow.stream.v1", "event_id": f"evt-{mint}-{event_type}-{sequence}",
        "token_mint": mint, "event_type": event_type,
        "event_time": "2026-01-01T00:00:00+00:00", "ingestion_time": "2026-01-01T00:00:00+00:00",
        "signature": f"sig-{sequence}", "protocol_scope": "pump", "source": "test",
        "sequence_index": 0, "quality_flags": [], "record_kind": "EVENT", "stream_session_id": session, "stream_sequence": sequence,
    }


def _run(stream, tmp_path: Path, input_path: Path, run_dir: Path | None = None) -> dict:
    runtime_root = tmp_path / "runtime"
    args = argparse.Namespace(
        runtime_root=runtime_root, input=str(input_path), mode="shadow_only",
        tensorflow_device="cpu", max_seconds=30, max_tokens=500,
        stdin_eof_grace_seconds=0, shutdown_grace_seconds=5,
        run_dir=run_dir, controlled_eof=True, transport_checkpoint=None,
        model_bundle=None, tabular_python=None, tensorflow_python=None,
        snapshot_id="test-snapshot", dedup_checkpoint_seconds=30,
    )
    assert stream.main(args) == 0
    return json.loads((runtime_root / "shadow" / "logs" / "latest_stream_report.json").read_text())


def test_window_finalization_end_to_end(tmp_path: Path, monkeypatch) -> None:
    stream = load_script("run_shadow_stream.py")
    monkeypatch.setattr(stream, "ModelClients", _FakeModelClients)
    session = "sess-1"
    records = [_control("SESSION_START", 1, session), _event(2, session, "MINT1"), _control("SESSION_END", 3, session)]
    input_path = tmp_path / "input.jsonl"
    input_path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    report = _run(stream, tmp_path, input_path)
    assert report["windows_finalized"] == 1
    assert report["features_written"] == 1
    assert report["predictions_written"] == 1
    assert report["open_windows_final"] == 0


def test_no_orphan_features_or_predictions_across_restart(tmp_path: Path, monkeypatch) -> None:
    """Run once, then again over fresh input against the same runtime root
    (simulating a restart mid-stream): features/predictions must never end
    up mismatched, and sequence integrity must hold both times."""
    stream = load_script("run_shadow_stream.py")
    monkeypatch.setattr(stream, "ModelClients", _FakeModelClients)
    input_path = tmp_path / "input.jsonl"

    session1 = "sess-1"
    input_path.write_text("\n".join(json.dumps(r) for r in [
        _control("SESSION_START", 1, session1), _event(2, session1, "MINT1"), _control("SESSION_END", 3, session1),
    ]) + "\n")
    first = _run(stream, tmp_path, input_path)
    assert first["sequence_gaps"] == 0
    assert first["features_without_prediction"] == 0
    assert first["predictions_without_feature"] == 0
    assert first["duplicate_feature_ids"] == 0
    assert first["duplicate_prediction_ids"] == 0

    # Simulate redelivery of the same data after a restart (at-least-once
    # transport): a fresh session wrapping the *same* MINT1 CREATE_TOKEN
    # event_id (sequence/mint/type are the only inputs to _event()'s id).
    session2 = "sess-2"
    input_path.write_text("\n".join(json.dumps(r) for r in [
        _control("SESSION_START", 1, session2), _event(2, session2, "MINT1"), _control("SESSION_END", 3, session2),
    ]) + "\n")
    second = _run(stream, tmp_path, input_path)
    assert second["duplicates"] == 1
    assert second["sequence_gaps"] == 0
    assert second["features_without_prediction"] == 0
    assert second["predictions_without_feature"] == 0
    assert second["duplicate_feature_ids"] == 0
    assert second["duplicate_prediction_ids"] == 0
    # The redelivered MINT1 event must not be reprocessed into a second
    # window/feature/prediction.
    assert second["windows_finalized"] == 0
    assert second["tokens_seen"] == 0


def test_shutdown_censorship_end_to_end(tmp_path: Path, monkeypatch) -> None:
    """A window created too close to shutdown to reach T+10s must be
    censored, not finalized, and that must survive being read back."""
    stream = load_script("run_shadow_stream.py")
    monkeypatch.setattr(stream, "ModelClients", _FakeModelClients)
    session = "sess-1"
    now = datetime.now(UTC)
    records = [
        _control("SESSION_START", 1, session),
        {**_event(2, session, "LATE_MINT"), "event_time": now.isoformat(), "ingestion_time": now.isoformat()},
    ]
    input_path = tmp_path / "input.jsonl"
    input_path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    report = _run(stream, tmp_path, input_path)
    # Created just before EOF: cannot reach T+10s within the run, so it must
    # be censored at graceful shutdown, never finalized/predicted.
    assert report["windows_censored_at_shutdown"] == 1
    assert report["windows_finalized"] == 0
    assert report["predictions_written"] == 0
    assert report["censor_reason_counts"] == {GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF: 1}

    runtime_root = tmp_path / "runtime"
    window_file = json.loads((runtime_root / "shadow" / "state" / "window_outcome_state.json").read_text())
    assert window_file["window_state"]["windows"]["LATE_MINT"]["state"] == "CENSORED_AT_SHUTDOWN"
    assert "events" not in window_file["window_state"]["windows"]["LATE_MINT"]
