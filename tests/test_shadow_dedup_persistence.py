"""Tests for the persist_locked() dedup-persistence fix.

Context: RFC014_BACKLOG_DIAGNOSIS_20260801.md identified persist_locked()
rewriting the full, sorted dedup set on every raw event as the root cause of
the 24h run's transport backlog (O(n) per call, O(n^2) over a run). The fix
splits dedup durability into an O(1) append-only WAL (state.record_seen)
plus periodic full checkpoints (state.checkpoint), while window/outcome
state -- bounded by token count, not event count -- keeps its original
per-event durability via a dedicated, smaller file.

These tests cover: dedup correctness, crash recovery (including a torn
write), atomic checkpointing, backward compatibility with the legacy
bundled state file, a before/after cost benchmark, and an end-to-end check
(through the real run_shadow_stream.main() ingestion loop, with only the ML
subprocess layer faked) that COLLECTOR_SHUTDOWN/SESSION_END are always
consumed and never treated as dedup-able events.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import time
from pathlib import Path

# Force the real pyarrow into sys.modules at collection time, before any
# other test module's load_script() gets a chance to stub it out with a bare
# ModuleType via sys.modules.setdefault(...) (see test_shadow_artifact_contract.py).
# setdefault() is a no-op once a real entry exists, so importing for real here
# -- ahead of any test *running* -- makes both files' loaded scripts see the
# genuine pyarrow.parquet, regardless of test collection/execution order.
import pyarrow  # noqa: F401
import pyarrow.parquet  # noqa: F401

from atlaspump.shadow_stream import StreamState

ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str):
    # The end-to-end tests below exercise the real
    # write_scored_locked()/write_outcome_locked() paths, which need real
    # Parquet writing to succeed -- see the pyarrow imports above.
    path = ROOT / "scripts" / "shadow" / name
    spec = importlib.util.spec_from_file_location(name.replace(".py", "") + "_dedup_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# StreamState: dedup WAL + checkpoint correctness
# ---------------------------------------------------------------------------

def test_record_seen_is_durable_without_a_checkpoint(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    state.record_seen("event-1", 10)
    state.record_seen("event-2", 11)
    assert not (root / "state" / "seen_event_ids.json").exists()
    assert (root / "state" / "seen_event_ids.wal").exists()

    recovered = StreamState.load(root)
    assert recovered.seen == {"event-1", "event-2"}
    assert recovered.dedup_wal_replayed_count == 2


def test_checkpoint_clears_the_wal_and_stays_recoverable(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    for index in range(5):
        state.record_seen(f"event-{index}")
    state.checkpoint()
    assert (root / "state" / "seen_event_ids.wal").read_text() == ""
    checkpoint = json.loads((root / "state" / "seen_event_ids.json").read_text())
    assert sorted(checkpoint["seen_event_ids"]) == [f"event-{index}" for index in range(5)]

    state.record_seen("event-5")
    recovered = StreamState.load(root)
    assert recovered.seen == {f"event-{index}" for index in range(6)}


def test_crash_between_save_and_wal_truncate_loses_nothing(tmp_path: Path) -> None:
    """checkpoint() truncates the WAL only after save() durably returns; a
    crash in between must be idempotent-safe on replay, not lossy or
    duplicated."""
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    state.record_seen("a")
    state.record_seen("b")
    state.save()  # simulate: checkpoint() interrupted right before its truncate
    recovered = StreamState.load(root)
    assert recovered.seen == {"a", "b"}
    assert recovered.dedup_wal_replayed_count == 2  # WAL replayed on top of the checkpoint -- harmless


def test_interruption_mid_append_is_skipped_not_fatal(tmp_path: Path) -> None:
    """A crash mid-write can leave a torn last line in the WAL; recovery must
    tolerate it (skip it) rather than fail, and must not resurrect a
    half-written event id."""
    root = tmp_path / "shadow"
    state = StreamState.load(root)
    state.record_seen("a")
    state.record_seen("b")
    wal_path = root / "state" / "seen_event_ids.wal"
    with wal_path.open("a", encoding="utf-8") as handle:
        handle.write('{"event_id": "c", "stream_seq')  # torn write, never completed/newlined

    recovered = StreamState.load(root)
    assert recovered.seen == {"a", "b"}
    assert recovered.dedup_wal_skipped_count == 1
    assert "c" not in recovered.seen


def test_legacy_bundled_state_file_still_loads(tmp_path: Path) -> None:
    """Runtimes created before either fix wrote everything into one file."""
    root = tmp_path / "shadow"
    (root / "state").mkdir(parents=True)
    (root / "state" / "seen_event_ids.json").write_text(json.dumps({
        "seen_event_ids": ["legacy-1", "legacy-2"],
        "window_state": {"windows": {"mint-a": {"state": "OPEN"}}},
        "prediction_ids": ["p1"],
        "outcome_ids": ["o1"],
        "outcome_state": {},
    }))
    state = StreamState.load(root)
    assert state.seen == {"legacy-1", "legacy-2"}
    assert state.window_state["windows"] == {"mint-a": {"state": "OPEN"}}
    assert state.prediction_ids == {"p1"}
    assert state.outcome_ids == {"o1"}


def test_legacy_v1_list_only_file_still_loads(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    (root / "state").mkdir(parents=True)
    (root / "state" / "seen_event_ids.json").write_text(json.dumps(["only-a", "only-b"]))
    state = StreamState.load(root)
    assert state.seen == {"only-a", "only-b"}


def test_new_dedicated_window_file_takes_precedence_over_legacy_bundle(tmp_path: Path) -> None:
    """A run resumed under the new code must prefer freshly-checkpointed
    window state over whatever stale copy is still sitting in the legacy
    bundle."""
    root = tmp_path / "shadow"
    (root / "state").mkdir(parents=True)
    (root / "state" / "seen_event_ids.json").write_text(json.dumps({
        "seen_event_ids": ["a"], "window_state": {"windows": {"stale-mint": {"state": "FINALIZED"}}}, "prediction_ids": [], "outcome_ids": [], "outcome_state": {},
    }))
    (root / "state" / "window_outcome_state.json").write_text(json.dumps({
        "window_state": {"windows": {"fresh-mint": {"state": "FINALIZED"}}}, "prediction_ids": ["p2"], "outcome_ids": [], "outcome_state": {},
    }))
    state = StreamState.load(root)
    assert state.window_state["windows"] == {"fresh-mint": {"state": "FINALIZED"}}
    assert state.prediction_ids == {"p2"}
    assert state.seen == {"a"}


# ---------------------------------------------------------------------------
# Benchmark: before (full rewrite) vs after (incremental) cost at scale
# ---------------------------------------------------------------------------

def test_incremental_record_seen_is_much_cheaper_than_full_rewrite_at_scale(tmp_path: Path) -> None:
    """Reproduces, at realistic scale, the exact operation the pre-fix
    persist_locked() ran on every raw event (RFC014_BACKLOG_DIAGNOSIS_20260801.md
    §1.9 measured ~292ms at n=213,760 for sorted()+json.dumps() alone).
    A single fsync has fixed overhead that dominates at tiny n, so this
    uses production-scale n and realistic id lengths (~130 chars, matching
    the real "<signature>:<index>:<mint>" event_id format) to make the O(n)
    vs O(1) difference the dominant, measured effect rather than noise.
    """
    state = StreamState.load(tmp_path / "shadow")
    for index in range(200_000):
        state.seen.add(f"{'a' * 88}:0:{'b' * 40}{index}")

    started = time.perf_counter()
    state.save()  # the old per-event behavior: sorted() + full rewrite of the whole set
    full_rewrite_ms = (time.perf_counter() - started) * 1000

    samples_ms = []
    for index in range(10):
        started = time.perf_counter()
        state.record_seen(f"new-event-{index}")  # the new per-event behavior
        samples_ms.append((time.perf_counter() - started) * 1000)
    incremental_ms = sum(samples_ms) / len(samples_ms)

    assert incremental_ms * 5 < full_rewrite_ms, (
        "expected record_seen (O(1), fsync-dominated) to stay well under "
        f"full save() (O(n)) at n=200,000: incremental={incremental_ms:.3f}ms "
        f"(avg of 10) full={full_rewrite_ms:.3f}ms"
    )


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
    return {
        "record_kind": "CONTROL", "control_type": kind,
        "schema_version": "atlaspump.shadow.transport.v1",
        "stream_session_id": session, "stream_sequence": sequence,
    }


def _event(sequence: int, session: str) -> dict:
    return {
        "schema_version": "atlaspump.shadow.stream.v1",
        "event_id": "evt-create-1", "token_mint": "MINT1", "event_type": "CREATE_TOKEN",
        "event_time": "2026-01-01T00:00:00+00:00", "ingestion_time": "2026-01-01T00:00:00+00:00",
        "signature": "sig-1", "protocol_scope": "pump", "source": "test",
        "sequence_index": 0, "quality_flags": [],
        "record_kind": "EVENT", "stream_session_id": session, "stream_sequence": sequence,
    }


def test_collector_shutdown_and_session_end_always_consumed_never_deduped(tmp_path: Path, monkeypatch) -> None:
    stream = load_script("run_shadow_stream.py")
    monkeypatch.setattr(stream, "ModelClients", _FakeModelClients)

    session = "test-session"
    records = [
        _control("SESSION_START", 1, session),
        _event(2, session),
        _control("COLLECTOR_SHUTDOWN", 3, session),
        _control("SESSION_END", 4, session),
    ]
    input_path = tmp_path / "input.jsonl"
    input_path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    runtime_root = tmp_path / "runtime"
    checkpoint_path = tmp_path / "transport_checkpoint.json"
    args = argparse.Namespace(
        runtime_root=runtime_root, input=str(input_path), mode="shadow_only",
        tensorflow_device="cpu", max_seconds=30, max_tokens=500,
        stdin_eof_grace_seconds=0, shutdown_grace_seconds=5,
        run_dir=tmp_path / "rundir", controlled_eof=True,
        transport_checkpoint=checkpoint_path, model_bundle=None,
        tabular_python=None, tensorflow_python=None, snapshot_id="test-snapshot",
        dedup_checkpoint_seconds=30,
    )
    (tmp_path / "rundir").mkdir()

    exit_code = stream.main(args)
    assert exit_code == 0

    report = json.loads((runtime_root / "shadow" / "logs" / "latest_stream_report.json").read_text())
    assert report["control_records"] == 3  # SESSION_START, COLLECTOR_SHUTDOWN, SESSION_END
    assert report["events_valid"] == 1
    assert report["dead_letters"] == 0
    assert report["sequence_gaps"] == 0
    assert report["duplicates"] == 0
    assert report["features_written"] == 1
    assert report["predictions_written"] == 1

    # Cursor progresses over every record, control included -- the last ack
    # must be SESSION_END's own sequence, not the CREATE_TOKEN event's.
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["stream_sequence"] == 4

    # Control records never enter dedup state -- only the one real event does.
    # (The WAL itself is checkpointed-and-cleared multiple times over the
    # course of a run, including at finalize and at shutdown, so its final
    # line count is a timing artifact, not the right thing to assert on;
    # the checkpointed dedup set is the durable source of truth.)
    final_checkpoint = json.loads((runtime_root / "shadow" / "state" / "seen_event_ids.json").read_text())
    assert final_checkpoint["seen_event_ids"] == ["evt-create-1"]


def test_recovery_after_simulated_crash_does_not_reprocess_seen_event(tmp_path: Path, monkeypatch) -> None:
    """Run once, then run again over the same input as if redelivered after
    a crash: the CREATE_TOKEN event must be recognized as a duplicate the
    second time (dedup survives across a restart), while the transport
    cursor still advances to the end either way."""
    stream = load_script("run_shadow_stream.py")
    monkeypatch.setattr(stream, "ModelClients", _FakeModelClients)

    session = "test-session"
    records = [_control("SESSION_START", 1, session), _event(2, session), _control("SESSION_END", 3, session)]
    input_path = tmp_path / "input.jsonl"
    input_path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    runtime_root = tmp_path / "runtime"
    args = argparse.Namespace(
        runtime_root=runtime_root, input=str(input_path), mode="shadow_only",
        tensorflow_device="cpu", max_seconds=30, max_tokens=500,
        stdin_eof_grace_seconds=0, shutdown_grace_seconds=5,
        run_dir=None, controlled_eof=True, transport_checkpoint=None,
        model_bundle=None, tabular_python=None, tensorflow_python=None,
        snapshot_id="test-snapshot", dedup_checkpoint_seconds=30,
    )
    assert stream.main(args) == 0
    first_report = json.loads((runtime_root / "shadow" / "logs" / "latest_stream_report.json").read_text())
    assert first_report["events_valid"] == 1
    assert first_report["duplicates"] == 0
    assert first_report["dedup_state_size_final"] == 1

    # Simulate redelivery after a restart: same input replayed from scratch
    # against the *same* runtime root (a fresh session id avoids the
    # transport-level SEQUENCE_DUPLICATE path so this exercises event-level
    # dedup via state.seen specifically).
    session2 = "test-session-2"
    records2 = [_control("SESSION_START", 1, session2), _event(2, session2), _control("SESSION_END", 3, session2)]
    input_path.write_text("\n".join(json.dumps(record) for record in records2) + "\n")
    assert stream.main(args) == 0
    second_report = json.loads((runtime_root / "shadow" / "logs" / "latest_stream_report.json").read_text())
    assert second_report["duplicates"] == 1
    assert second_report["dedup_state_size_final"] == 1


# ---------------------------------------------------------------------------
# Hermes alerting: thresholds, local logging, best-effort network behavior
# ---------------------------------------------------------------------------

def test_alert_severity_thresholds() -> None:
    stream = load_script("run_shadow_stream.py")
    metric = "transport_backlog_records"
    assert stream.alert_severity(metric, None) is None
    assert stream.alert_severity(metric, 4_999) is None
    assert stream.alert_severity(metric, 5_000) == "WARNING"
    assert stream.alert_severity(metric, 19_999) == "WARNING"
    assert stream.alert_severity(metric, 20_000) == "CRITICAL"


def test_alert_is_always_logged_locally_even_without_a_webhook(tmp_path: Path) -> None:
    stream = load_script("run_shadow_stream.py")
    alerts_log = tmp_path / "hermes_alerts.jsonl"
    payload = {
        "alert_schema": "rfc014_hermes_alert_v1", "run_id": "test-run",
        "metric": "transport_backlog_records", "value": 25_000,
        "threshold_warning": 5_000, "threshold_critical": 20_000,
        "severity": "CRITICAL", "raised_at": "2026-08-01T00:00:00+00:00",
    }
    stream.send_hermes_alert(None, alerts_log, payload)  # no webhook configured
    lines = [line for line in alerts_log.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    logged = json.loads(lines[0])
    assert logged == payload  # payload carries metric, value, both thresholds, severity, run id


def test_alert_never_raises_when_webhook_is_unreachable(tmp_path: Path) -> None:
    """Alerting is best-effort: a broken/unreachable webhook must never be
    able to affect the pipeline it is monitoring."""
    stream = load_script("run_shadow_stream.py")
    alerts_log = tmp_path / "hermes_alerts.jsonl"
    stream.send_hermes_alert("http://127.0.0.1:1/unreachable", alerts_log, {"severity": "WARNING"})
    # No exception raised, and the local log still happened regardless of
    # network outcome.
    assert alerts_log.exists()
