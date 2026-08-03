"""Safe, provider-neutral RFC-014 shadow stream primitives.

This module intentionally contains no wallet, signing, swap, or Solana send
operation.  The transport contract is JSONL so a collector can write to stdout
over SSH or append locally; the shadow worker only consumes records.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from atlaspump.shadow_inference import TokenWindow, validate_features

STREAM_SCHEMA_VERSION = "atlaspump.shadow.stream.v1"
TRANSPORT_SCHEMA_VERSION = "atlaspump.shadow.transport.v1"
EVENT_TYPES = {"CREATE_TOKEN", "BUY", "SELL", "TRANSFER", "CREATE_POOL", "MIGRATE"}
REQUIRED = {"schema_version", "event_id", "token_mint", "event_type", "event_time", "ingestion_time", "signature", "protocol_scope", "source", "sequence_index", "quality_flags"}
GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF = "GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF"


def parse_time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value.astimezone(timezone.utc)
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(timezone.utc)


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED - set(event)
    if missing:
        raise ValueError(f"missing required fields: {sorted(missing)}")
    if event["schema_version"] != STREAM_SCHEMA_VERSION:
        raise ValueError("unsupported schema_version")
    if event["event_type"] not in EVENT_TYPES:
        raise ValueError("unsupported event_type")
    if not all(isinstance(event[k], str) and event[k] for k in ("event_id", "token_mint", "signature", "source", "protocol_scope")):
        raise ValueError("empty deterministic identity field")
    if not isinstance(event["sequence_index"], int) or event["sequence_index"] < 0:
        raise ValueError("sequence_index must be a nonnegative int")
    if not isinstance(event["quality_flags"], list):
        raise ValueError("quality_flags must be a list")
    return event | {"event_time": parse_time(event["event_time"]), "ingestion_time": parse_time(event["ingestion_time"])}


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Scheduler and stream reader may persist concurrently; a unique sibling
    # prevents one atomic replace from consuming another writer's temp file.
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def atomic_append_line(path: Path, line: str) -> None:
    """O(1) durable append, independent of the file's current size."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def atomic_truncate(path: Path) -> None:
    """Atomically clear a file via rename-over -- never a partially-truncated file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text("", encoding="utf-8")
    os.replace(temporary, path)


def event_digest(event: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()


@dataclass
class TransportTracker:
    """Audit producer sequences without ever turning uncertainty into coverage."""
    sessions_seen: set[str] = field(default_factory=set)
    last_sequence: dict[str, int] = field(default_factory=dict)
    sequence_digests: dict[tuple[str, int], str] = field(default_factory=dict)
    controls: dict[str, int] = field(default_factory=dict)
    sequence_gaps: int = 0
    session_changes: int = 0
    transport_connections: int = 0
    transport_reconnections: int = 0
    transport_disconnects: int = 0
    heartbeats_received: int = 0
    possible_gap_intervals: list[dict[str, Any]] = field(default_factory=list)
    current_session: str | None = None
    last_heartbeat: datetime | None = None
    legacy_seen: bool = False

    def accept(self, record: dict[str, Any], observed_at: datetime | None = None) -> str:
        observed_at = observed_at or datetime.now(timezone.utc)
        if record.get("record_kind") == "CONTROL":
            kind = str(record.get("control_type", "UNKNOWN"))
            self.controls[kind] = self.controls.get(kind, 0) + 1
            if kind == "TRANSPORT_CONNECTED": self.transport_connections += 1
            elif kind == "TRANSPORT_RECONNECTED": self.transport_reconnections += 1
            elif kind == "TRANSPORT_DISCONNECTED": self.transport_disconnects += 1
            elif kind == "HEARTBEAT": self.last_heartbeat = observed_at
            if kind == "POSSIBLE_GAP":
                self._gap("transport_control", observed_at, record.get("details", {}))
            # Wrapper controls do not share the producer sequence namespace.
            if record.get("schema_version") != TRANSPORT_SCHEMA_VERSION or "stream_sequence" not in record:
                return "CONTROL"
        session = record.get("stream_session_id")
        sequence = record.get("stream_sequence")
        if not isinstance(session, str) or not isinstance(sequence, int):
            self.legacy_seen = True
            return "LEGACY"
        if self.current_session is not None and session != self.current_session:
            self.session_changes += 1
            self._gap("session_change", observed_at, {"previous_session": self.current_session, "next_session": session})
        self.current_session = session
        self.sessions_seen.add(session)
        key, digest = (session, sequence), event_digest(record)
        previous_digest = self.sequence_digests.get(key)
        if previous_digest is not None:
            if previous_digest == digest:
                return "SEQUENCE_DUPLICATE"
            return "SEQUENCE_CONFLICT"
        previous = self.last_sequence.get(session)
        if previous is not None and sequence != previous + 1:
            self.sequence_gaps += 1
            self._gap("sequence_gap", observed_at, {"session": session, "expected": previous + 1, "received": sequence})
        self.sequence_digests[key] = digest
        self.last_sequence[session] = max(sequence, previous or 0)
        if record.get("record_kind") == "CONTROL" and record.get("control_type") == "HEARTBEAT":
            self.heartbeats_received += 1
            self.last_heartbeat = observed_at
        return "OK"

    def _gap(self, reason: str, at: datetime, details: dict[str, Any]) -> None:
        self.possible_gap_intervals.append({"start": at.isoformat(), "end": None, "reason": reason, "details": details})

    @property
    def audited(self) -> bool:
        return bool(self.sessions_seen) and not self.legacy_seen


@dataclass
class StreamState:
    """Dedup/window/outcome durability.

    Two containers scale very differently: ``seen`` grows with every raw
    *event* (hundreds of thousands over 24h); ``window_state``/
    ``prediction_ids``/``outcome_ids``/``outcome_state`` grow with the much
    smaller number of *tokens* (thousands). The pre-fix code bundled all of
    them into one JSON file rewritten -- fully re-sorted and re-serialized --
    on every single raw event, an O(n) cost per call applied n times, i.e.
    O(n^2) over a run. Measured at n=213,760 (the 2026-07-31 24h run's final
    dedup-set size): ~292ms per call, saturating CPU well before 24h of
    sustained ingestion (see RFC014_BACKLOG_DIAGNOSIS_20260801.md).

    The fix splits durability by growth rate. First cut (dedup, still true):
      - ``seen_event_ids.json`` -- the dedup set, checkpointed only
        periodically via ``checkpoint()``.
      - ``seen_event_ids.wal`` -- between checkpoints, each new dedup entry
        is made durable individually via ``record_seen()``, an O(1) fsynced
        append. A crash between checkpoints loses nothing: ``load()``
        replays the WAL on top of the last checkpoint, and replay is
        idempotent (set membership), so re-applying an entry already in the
        checkpoint is harmless. ``checkpoint()`` only ever clears the WAL
        *after* the fresh checkpoint is durably written, so recovery never
        depends on data existing in exactly one of the two files at once.

    Second cut, this fix (window/outcome state): the *first* fix bundled
    window/prediction/outcome state into one file rewritten in full on every
    raw event, reasoning it was "cheap: bounded by token count". That
    reasoning missed that every ``TokenWindow`` also carries its raw
    ``events`` list, which is *not* dropped after finalization -- at real
    scale (2351 tokens, one 2h replay) that file reached 5.1 MB, rewritten
    ~107k times, ~27 min of cumulative cost (RFC014_CANDIDATE_SNAPSHOT_
    VALIDATION_20260802.md). ``window.events`` is provably dead weight once
    a window leaves OPEN: ``WindowBook._due_locked()``/``censor_open_windows()``
    only ever touch OPEN windows, so nothing reads a finalized/censored
    window's events again. This durability is split three ways:
      - ``open_windows_state.json`` -- only currently-OPEN windows, with
        their events. Rewritten in full on every event, but cheap: bounded
        by how many windows can be open concurrently (creation rate x 10s),
        not by every token ever seen.
      - ``window_outcome_state.json`` -- periodic checkpoint of everything
        else: closed-window metadata *without* events, prediction_ids,
        outcome_ids, outcome_state. Checkpointed on the same cadence as the
        dedup set (periodic timer, end of each finalize batch, shutdown).
      - ``window_transitions.wal`` -- between those checkpoints, each
        window finalization/censorship and each prediction/outcome write is
        made durable individually (O(1) append): ``record_window_finalized``,
        ``record_window_censored``, ``record_prediction_written``,
        ``record_outcome_written``. Idempotent replay, same discipline as
        the dedup WAL: truncated only after the checkpoint that supersedes
        it is durably written.

    Legacy runtimes (created before either fix) wrote everything bundled
    into ``seen_event_ids.json``; runtimes created under the first fix wrote
    ``window_outcome_state.json`` with *all* windows (events included).
    ``load()`` still reads both in full whenever the newer, more specific
    files are absent.
    """

    root: Path
    seen: set[str] = field(default_factory=set)
    window_state: dict[str, Any] = field(default_factory=dict)
    prediction_ids: set[str] = field(default_factory=set)
    outcome_ids: set[str] = field(default_factory=set)
    outcome_state: dict[str, Any] = field(default_factory=dict)
    dedup_wal_replayed_count: int = 0
    dedup_wal_skipped_count: int = 0
    window_wal_replayed_count: int = 0
    window_wal_skipped_count: int = 0

    @classmethod
    def load(cls, root: Path) -> StreamState:
        state = cls(root)
        dedup_file = root / "state" / "seen_event_ids.json"
        if dedup_file.exists():
            raw = json.loads(dedup_file.read_text(encoding="utf-8"))
            # Compatibility with the v1 state file, which was simply a list.
            if isinstance(raw, list):
                state.seen = set(raw)
            else:
                state.seen = set(raw.get("seen_event_ids", []))
                # Oldest bundled format: window/outcome state lived in this
                # same file. Superseded below if a dedicated file exists.
                state.window_state = dict(raw.get("window_state", {}))
                state.prediction_ids = set(raw.get("prediction_ids", []))
                state.outcome_ids = set(raw.get("outcome_ids", []))
                state.outcome_state = dict(raw.get("outcome_state", {}))
        window_file = root / "state" / "window_outcome_state.json"
        base_windows: dict[str, Any] = dict(state.window_state)
        if window_file.exists():
            raw = json.loads(window_file.read_text(encoding="utf-8"))
            base_windows = dict(raw.get("window_state", {}))
            state.prediction_ids = set(raw.get("prediction_ids", []))
            state.outcome_ids = set(raw.get("outcome_ids", []))
            state.outcome_state = dict(raw.get("outcome_state", {}))
        # The periodic checkpoint (base_windows, from window_outcome_state.json
        # / snapshot_closed_state()) only ever records windows that are no
        # longer OPEN -- so if it already knows a mint, that is authoritative
        # and must win over a stale open_windows_state.json entry for the
        # same mint (e.g. a crash between record_window_finalized() and the
        # next persist_open_windows_locked() refresh would otherwise leave a
        # finalized window looking OPEN again). open_windows_state.json is
        # only consulted for mints the checkpoint does *not* already know.
        merged_windows = dict(base_windows.get("windows", {}))
        merged_delays = dict(base_windows.get("delays", {}))
        merged_rejected = dict(base_windows.get("rejected", {}))
        merged_lags = dict(base_windows.get("creation_arrival_lags", {}))
        open_file = root / "state" / "open_windows_state.json"
        if open_file.exists():
            raw = json.loads(open_file.read_text(encoding="utf-8"))
            for mint, entry in raw.get("windows", {}).items():
                if mint not in merged_windows:
                    merged_windows[mint] = entry
            for mint, value in raw.get("delays", {}).items():
                merged_delays.setdefault(mint, value)
            for mint, value in raw.get("rejected", {}).items():
                merged_rejected.setdefault(mint, value)
            for mint, value in raw.get("creation_arrival_lags", {}).items():
                merged_lags.setdefault(mint, value)
        state.window_state = {
            **base_windows,
            "windows": merged_windows,
            "delays": merged_delays,
            "rejected": merged_rejected,
            "creation_arrival_lags": merged_lags,
        }
        state._replay_dedup_wal()
        state._replay_window_wal()
        return state

    def _dedup_wal_path(self) -> Path:
        return self.root / "state" / "seen_event_ids.wal"

    def _replay_dedup_wal(self) -> None:
        """Bounded recovery: apply only the mutations since the last checkpoint.

        Idempotent by construction (adding an id already in ``seen`` is a
        no-op), so this is safe even if some WAL lines predate the
        checkpoint just loaded above.
        """
        path = self._dedup_wal_path()
        if not path.exists():
            return
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                # A crash mid-append can leave a truncated last line; skip
                # it rather than fail recovery. Worst case, that single
                # event is later reprocessed as new -- the same at-least-once
                # tolerance the transport checkpoint already documents.
                self.dedup_wal_skipped_count += 1
                continue
            event_id = entry.get("event_id")
            if isinstance(event_id, str):
                self.seen.add(event_id)
                self.dedup_wal_replayed_count += 1

    def record_seen(self, event_id: str, stream_sequence: int | None = None) -> None:
        """Durably record one new dedup entry in O(1), without rewriting the full state."""
        self.seen.add(event_id)
        atomic_append_line(
            self._dedup_wal_path(),
            json.dumps(
                {
                    "event_id": event_id,
                    "stream_sequence": stream_sequence,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                },
                sort_keys=True,
            ),
        )

    def _window_wal_path(self) -> Path:
        return self.root / "state" / "window_transitions.wal"

    def _replay_window_wal(self) -> None:
        """Bounded recovery for window/outcome transitions: apply only the
        mutations since the last periodic checkpoint. Idempotent by
        construction (re-marking an already-FINALIZED window, or re-adding
        an id already in a set, is a no-op), so this is safe even if some
        WAL lines predate the checkpoint just loaded above.
        """
        path = self._window_wal_path()
        if not path.exists():
            return
        windows = self.window_state.setdefault("windows", {})
        censored = self.window_state.setdefault("censored_at_shutdown", {})
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                # Same tolerance as the dedup WAL: a torn trailing line from
                # a crash mid-append is skipped, never fatal to recovery.
                self.window_wal_skipped_count += 1
                continue
            kind = entry.get("type")
            mint = entry.get("mint")
            if kind == "WINDOW_FINALIZED" and mint in windows:
                windows[mint] = {**windows[mint], "state": "FINALIZED", "finalized": True, "finalized_at": entry.get("finalized_at")}
            elif kind == "WINDOW_CENSORED" and mint in windows:
                row = dict(entry.get("row", {}))
                windows[mint] = {**windows[mint], "state": "CENSORED_AT_SHUTDOWN", **row}
                censored[mint] = row
            elif kind == "PREDICTION_WRITTEN" and isinstance(entry.get("prediction_id"), str):
                self.prediction_ids.add(entry["prediction_id"])
            elif kind == "OUTCOME_WRITTEN" and isinstance(entry.get("outcome_id"), str):
                self.outcome_ids.add(entry["outcome_id"])
            else:
                self.window_wal_skipped_count += 1
                continue
            self.window_wal_replayed_count += 1

    def record_window_finalized(self, mint: str, finalized_at: datetime) -> None:
        """Durably record one window's OPEN->FINALIZED transition in O(1)."""
        atomic_append_line(
            self._window_wal_path(),
            json.dumps({"type": "WINDOW_FINALIZED", "mint": mint, "finalized_at": finalized_at.isoformat()}, sort_keys=True, default=str),
        )

    def record_window_censored(self, mint: str, row: dict[str, Any]) -> None:
        """Durably record one window's OPEN->CENSORED_AT_SHUTDOWN transition in O(1)."""
        atomic_append_line(
            self._window_wal_path(),
            json.dumps({"type": "WINDOW_CENSORED", "mint": mint, "row": row}, sort_keys=True, default=str),
        )

    def record_prediction_written(self, prediction_id: str) -> None:
        """Durably record one prediction id in O(1), without a full rewrite."""
        self.prediction_ids.add(prediction_id)
        atomic_append_line(
            self._window_wal_path(),
            json.dumps({"type": "PREDICTION_WRITTEN", "prediction_id": prediction_id}, sort_keys=True),
        )

    def record_outcome_written(self, outcome_id: str) -> None:
        """Durably record one outcome id in O(1), without a full rewrite."""
        self.outcome_ids.add(outcome_id)
        atomic_append_line(
            self._window_wal_path(),
            json.dumps({"type": "OUTCOME_WRITTEN", "outcome_id": outcome_id}, sort_keys=True),
        )

    def save_open_windows(self, open_window_state: dict[str, Any]) -> None:
        """Cheap, per-event durability: bounded by concurrently-open-window
        count, not by every token ever seen. See class docstring."""
        atomic_json(self.root / "state" / "open_windows_state.json", open_window_state)

    def checkpoint_window_state(self, closed_window_state: dict[str, Any]) -> None:
        """Full periodic checkpoint of everything except open windows
        (already covered by save_open_windows): closed-window metadata
        without raw events, prediction/outcome ids, outcome_state. Clears
        window_transitions.wal only after this write is durable -- same
        crash-safety discipline as the dedup checkpoint."""
        atomic_json(
            self.root / "state" / "window_outcome_state.json",
            {
                "window_state": closed_window_state,
                "prediction_ids": sorted(self.prediction_ids),
                "outcome_ids": sorted(self.outcome_ids),
                "outcome_state": self.outcome_state,
            },
        )
        atomic_truncate(self._window_wal_path())

    def save(self) -> None:
        """Full dedup checkpoint. O(n) in the size of ``seen`` -- call
        periodically, never per raw event. See ``checkpoint()``."""
        atomic_json(self.root / "state" / "seen_event_ids.json", {"seen_event_ids": sorted(self.seen)})

    def checkpoint(self) -> None:
        """Full atomic snapshot, then clear the WAL it supersedes.

        The clear only happens after ``save()`` returns, so a crash between
        the two leaves the WAL slightly redundant with the checkpoint --
        never behind it. Nothing is ever deleted before it is durable
        elsewhere.
        """
        self.save()
        atomic_truncate(self._dedup_wal_path())


@dataclass(frozen=True)
class FeatureResult:
    token_mint: str
    creation_time: datetime
    creation_event_id: str
    values: dict[str, Any]
    event_count: int
    max_ingestion_delay_ms: int
    rejected_after_cutoff: int
    event_arrival_lag_ms: int
    status: str


class WindowBook:
    """Idempotent token windows; it never invents outcomes or amount semantics."""
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._lock_wait_ms: list[float] = []
        self._lock_hold_ms: list[float] = []
        self.windows: dict[str, TokenWindow] = {}
        self.delays: dict[str, list[int]] = {}
        self.rejected: dict[str, int] = {}
        self.migrations: dict[str, dict[str, Any]] = {}
        self.creation_arrival_lags: dict[str, int] = {}
        self.scheduled: set[str] = set()
        self.finalized_at: dict[str, datetime] = {}
        self.prediction_written: set[str] = set()
        self.shutdown_started = False
        self.censored_at_shutdown: dict[str, dict[str, Any]] = {}
        self.failed: dict[str, str] = {}
        self.late_events_after_finalization = 0
        self.late_events_within_feature_time = 0
        self.late_events_after_cutoff = 0

    def accept(self, event: dict[str, Any]) -> str:
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns()
            result = "ORPHAN_EVENT"
            mint = event["token_mint"]
            if self.shutdown_started:
                result = "SHUTDOWN_REJECTED"
            elif event["event_type"] == "CREATE_TOKEN":
                self.windows.setdefault(mint, TokenWindow(mint, event["event_time"]))
                self.creation_arrival_lags.setdefault(mint, max(0, int((event["ingestion_time"] - event["event_time"]).total_seconds() * 1000)))
            if self.shutdown_started:
                pass
            elif event["event_type"] == "MIGRATE":
                self.migrations[mint] = event
            window = self.windows.get(mint)
            if not self.shutdown_started and window is None:
                result = "ORPHAN_EVENT"
            elif not self.shutdown_started and window.state != "OPEN":
                self.late_events_after_finalization += 1
                if event["event_time"] <= window.creation_time + timedelta(seconds=10): self.late_events_within_feature_time += 1
                else: self.late_events_after_cutoff += 1
                result = "LATE_AFTER_FINALIZATION"
            elif not self.shutdown_started:
                delay = max(0, int((event["ingestion_time"] - event["event_time"]).total_seconds() * 1000))
                if not window.add(event, event["ingestion_time"]):
                    self.rejected[mint] = self.rejected.get(mint, 0) + 1
                    result = "OUTSIDE_WINDOW"
                else:
                    self.delays.setdefault(mint, []).append(delay)
                    result = "ACCEPTED"
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return result

    def begin_shutdown(self) -> None:
        """Atomically prevent any later event from opening or changing a window."""
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns()
            self.shutdown_started = True
        self._record_lock_timing(started, acquired, time.perf_counter_ns())

    def censor_open_windows(
        self,
        censored_at: datetime,
        censor_reason: str,
        shutdown_type: str,
    ) -> list[dict[str, Any]]:
        """Atomically censor open windows without producing a feature.

        ``CENSORED_AT_SHUTDOWN`` is the terminal state.  The reason remains a
        separate, stable explanation and is deliberately only valid for a
        graceful shutdown before the feature cutoff.
        """
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns()
            self.shutdown_started = True
            rows: list[dict[str, Any]] = []
            for mint, window in list(self.windows.items()):
                if window.state != "OPEN":
                    continue
                cutoff = window.creation_time + timedelta(seconds=10)
                if shutdown_type != "GRACEFUL" or censor_reason != GRACEFUL_SHUTDOWN_BEFORE_FEATURE_CUTOFF:
                    raise ValueError("unsupported shutdown censure semantics")
                if censored_at >= cutoff or mint in self.prediction_written:
                    raise RuntimeError("graceful shutdown censure invariant violated")
                creation_event = next((event for event in window.events if event["event_type"] == "CREATE_TOKEN"), {})
                window.state = "CENSORED_AT_SHUTDOWN"
                row = {
                    "mint": mint,
                    "token_mint": mint,
                    "creation_event_id": creation_event.get("event_id", ""),
                    "creation_time": window.creation_time.isoformat(),
                    "creation_event_time": window.creation_time.isoformat(),
                    "feature_cutoff": cutoff.isoformat(),
                    "shutdown_time": censored_at.isoformat(),
                    "window_state": "CENSORED_AT_SHUTDOWN",
                    "state": "CENSORED_AT_SHUTDOWN",
                    "state_before_shutdown": "OPEN",
                    "state_after_shutdown": "CENSORED_AT_SHUTDOWN",
                    "censored_at_utc": censored_at.isoformat(),
                    "censor_reason": censor_reason,
                    "shutdown_type": shutdown_type,
                    "state_transition_count": 1,
                    "events_observed": len(window.events),
                    "feature_cutoff_reached": False,
                    "prediction_created": False,
                }
                self.censored_at_shutdown[mint] = dict(row)
                rows.append(row)
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return rows

    def due(self, now: datetime) -> list[FeatureResult]:
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns(); result = self._due_locked(now)
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return result

    def _due_locked(self, now: datetime) -> list[FeatureResult]:
        result = []
        for mint, window in list(self.windows.items()):
            if window.state != "OPEN" or now < window.creation_time + timedelta(seconds=10):
                continue
            values = window.finalize(now)
            self.finalized_at[mint] = now
            try:
                validate_features(values)
                status = "READY"
            except ValueError:
                status = "FEATURE_CONTRACT_ERROR"
            creation_event_id = next(
                (event["event_id"] for event in window.events if event["event_type"] == "CREATE_TOKEN"),
                "",
            )
            result.append(FeatureResult(mint, window.creation_time, creation_event_id, dict(values), len(window.events), max(self.delays.get(mint, [0])), self.rejected.get(mint, 0), self.creation_arrival_lags.get(mint, 0), status))
        return result

    def snapshot(self) -> dict[str, Any]:
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns(); result = self._snapshot_locked()
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return result

    def _record_lock_timing(self, started: int, acquired: int, released: int) -> None:
        # Bounded in-memory samples: no per-operation synchronised telemetry.
        self._lock_wait_ms.append((acquired - started) / 1_000_000)
        self._lock_hold_ms.append((released - acquired) / 1_000_000)
        if len(self._lock_wait_ms) > 4096:
            del self._lock_wait_ms[:2048]; del self._lock_hold_ms[:2048]

    def lock_metrics(self) -> dict[str, list[float]]:
        with self.lock:
            return {"lock_wait_ms": list(self._lock_wait_ms), "lock_hold_ms": list(self._lock_hold_ms)}

    def concurrency_snapshot(self) -> dict[str, Any]:
        """Copy bounded aggregates under lock; callers serialize outside it."""
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns()
            snapshot = {
                "lock_wait_ms": list(self._lock_wait_ms),
                "lock_hold_ms": list(self._lock_hold_ms),
                "windows_open": sum(window.state == "OPEN" for window in self.windows.values()),
                "windows_finalized": sum(window.state == "FINALIZED" for window in self.windows.values()),
                "windows_censored_at_shutdown": len(self.censored_at_shutdown),
                "censor_reason_counts": self.censor_reason_counts(),
                "windows_failed": len(self.failed),
                "late_events": self.late_events_after_finalization,
                "duplicate_finalization_attempts": 0,
            }
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return snapshot

    def snapshot_open_windows(self) -> dict[str, Any]:
        """Cheap, per-event durability: only currently-OPEN windows, with
        their accumulating raw events (needed to correctly resume feature
        counting for a window mid-flight at a crash). Bounded by how many
        windows can be concurrently open (creation rate x 10s), not by the
        total number of tokens ever seen -- see StreamState docstring."""
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns(); result = self._snapshot_subset_locked(open_only=True)
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return result

    def snapshot_closed_state(self) -> dict[str, Any]:
        """Periodic-checkpoint durability: every window that is no longer
        OPEN (FINALIZED or CENSORED_AT_SHUTDOWN), *without* its raw events --
        once a window leaves OPEN, nothing ever reads window.events again
        (finalize()/censor_open_windows() only ever touch OPEN windows), so
        carrying it forward is pure dead weight. This is what made the
        pre-fix window_outcome_state.json grow to 5+ MB at real scale."""
        started = time.perf_counter_ns()
        with self.lock:
            acquired = time.perf_counter_ns(); result = self._snapshot_subset_locked(open_only=False)
        self._record_lock_timing(started, acquired, time.perf_counter_ns())
        return result

    def _snapshot_subset_locked(self, open_only: bool) -> dict[str, Any]:
        def encode(event: dict[str, Any]) -> dict[str, Any]:
            return {key: value.isoformat() if isinstance(value, datetime) else value for key, value in event.items()}

        windows: dict[str, Any] = {}
        delays: dict[str, Any] = {}
        rejected: dict[str, Any] = {}
        creation_arrival_lags: dict[str, Any] = {}
        for mint, window in self.windows.items():
            if (window.state == "OPEN") != open_only:
                continue
            entry = {
                "creation_time": window.creation_time.isoformat(),
                "feature_cutoff": (window.creation_time + timedelta(seconds=10)).isoformat(),
                "state": window.state,
                "scheduled": mint in self.scheduled,
                "finalized_at": self.finalized_at.get(mint).isoformat() if mint in self.finalized_at else None,
                "prediction_written": mint in self.prediction_written,
                "finalized": window.finalized,
                **self.censored_at_shutdown.get(mint, {}),
            }
            if open_only:
                entry["events"] = [encode(event) for event in window.events]
            windows[mint] = entry
            if mint in self.delays: delays[mint] = list(self.delays[mint])
            if mint in self.rejected: rejected[mint] = self.rejected[mint]
            if mint in self.creation_arrival_lags: creation_arrival_lags[mint] = self.creation_arrival_lags[mint]
        result: dict[str, Any] = {"windows": windows, "delays": delays, "rejected": rejected, "creation_arrival_lags": creation_arrival_lags}
        if not open_only:
            result |= {
                "late_events_after_finalization": self.late_events_after_finalization,
                "late_events_within_feature_time": self.late_events_within_feature_time,
                "late_events_after_cutoff": self.late_events_after_cutoff,
                "shutdown_started": self.shutdown_started,
                "censored_at_shutdown": {mint: dict(row) for mint, row in self.censored_at_shutdown.items()},
                "failed": dict(self.failed),
            }
        return result

    def _snapshot_locked(self) -> dict[str, Any]:
        def encode(event: dict[str, Any]) -> dict[str, Any]:
            return {key: value.isoformat() if isinstance(value, datetime) else value for key, value in event.items()}

        return {
            "windows": {
                mint: {
                    "creation_time": window.creation_time.isoformat(),
                    "feature_cutoff": (window.creation_time + timedelta(seconds=10)).isoformat(),
                    "state": window.state,
                    "scheduled": mint in self.scheduled,
                    "finalized_at": self.finalized_at.get(mint).isoformat() if mint in self.finalized_at else None,
                    "prediction_written": mint in self.prediction_written,
                    "events": [encode(event) for event in window.events],
                    "finalized": window.finalized,
                    **self.censored_at_shutdown.get(mint, {}),
                }
                for mint, window in self.windows.items()
            },
            "delays": {mint: list(values) for mint, values in self.delays.items()},
            "rejected": dict(self.rejected),
            "creation_arrival_lags": dict(self.creation_arrival_lags),
            "late_events_after_finalization": self.late_events_after_finalization,
            "late_events_within_feature_time": self.late_events_within_feature_time,
            "late_events_after_cutoff": self.late_events_after_cutoff,
            "shutdown_started": self.shutdown_started,
            "censored_at_shutdown": {mint: dict(row) for mint, row in self.censored_at_shutdown.items()},
            "failed": dict(self.failed),
        }

    def censor_reason_counts(self) -> dict[str, int]:
        """Count recorded censure explanations; legacy records may omit one."""
        counts: dict[str, int] = {}
        for row in self.censored_at_shutdown.values():
            reason = row.get("censor_reason")
            if reason is not None:
                counts[str(reason)] = counts.get(str(reason), 0) + 1
        return counts

    @classmethod
    def restore(cls, raw: dict[str, Any]) -> WindowBook:
        book = cls()
        for mint, value in raw.get("windows", {}).items():
            window = TokenWindow(mint, parse_time(value["creation_time"]))
            window.events = [
                event | {
                    key: parse_time(event[key])
                    for key in ("event_time", "ingestion_time")
                    if key in event
                }
                for event in value.get("events", [])
            ]
            window.finalized = bool(value.get("finalized", False))
            window.state = str(value.get("state", "FINALIZED" if window.finalized else "OPEN"))
            book.windows[mint] = window
            if value.get("scheduled", False):
                book.scheduled.add(mint)
            if value.get("finalized_at"):
                book.finalized_at[mint] = parse_time(value["finalized_at"])
            if value.get("prediction_written", False):
                book.prediction_written.add(mint)
        book.delays = {key: list(value) for key, value in raw.get("delays", {}).items()}
        book.rejected = {key: int(value) for key, value in raw.get("rejected", {}).items()}
        book.creation_arrival_lags = {key: int(value) for key, value in raw.get("creation_arrival_lags", {}).items()}
        book.late_events_after_finalization = int(raw.get("late_events_after_finalization", 0))
        book.late_events_within_feature_time = int(raw.get("late_events_within_feature_time", 0))
        book.late_events_after_cutoff = int(raw.get("late_events_after_cutoff", 0))
        book.shutdown_started = bool(raw.get("shutdown_started", False))
        book.censored_at_shutdown = {key: dict(value) for key, value in raw.get("censored_at_shutdown", {}).items()}
        book.failed = {key: str(value) for key, value in raw.get("failed", {}).items()}
        return book

    def outcome(self, mint: str, now: datetime) -> dict[str, Any] | None:
        window = self.windows[mint]
        if now < window.creation_time + timedelta(minutes=5):
            return None
        # RFC-014 deliberately has no confirmed PumpAPI MIGRATE contract yet.
        # A received message must not silently become a positive label, and
        # the absence of one must never become a negative label.
        return {
            "outcome_status": "UNKNOWN",
            "migration_time": None,
            "migration_delay_seconds": None,
        }


@dataclass
class OutcomeRecord:
    token_mint: str
    creation_time: datetime
    outcome_cutoff: datetime
    stream_session_id: str
    stream_coverage_start: datetime
    stream_coverage_end: datetime | None = None
    stream_gap_detected: bool = False
    migration_seen: bool = False
    migration_time: datetime | None = None
    migration_event_id: str | None = None
    state: str = "PENDING"
    outcome_status: str | None = None
    outcome_finalized_at: datetime | None = None


class OutcomeBook:
    """T+5 outcome state; it cannot infer a negative through a coverage gap."""

    def __init__(self, stream_session_id: str) -> None:
        self.stream_session_id = stream_session_id
        self.records: dict[str, OutcomeRecord] = {}

    def register(self, mint: str, creation_time: datetime, coverage_start: datetime) -> OutcomeRecord:
        return self.records.setdefault(
            mint,
            OutcomeRecord(mint, creation_time, creation_time + timedelta(minutes=5), self.stream_session_id, coverage_start),
        )

    def observe_migration(self, event: dict[str, Any]) -> bool:
        record = self.records.get(event["token_mint"])
        if record is None or record.state == "FINALIZED":
            return False
        at = event["event_time"]
        if not record.creation_time <= at <= record.outcome_cutoff:
            return False
        record.migration_seen = True
        record.migration_time = at
        record.migration_event_id = event["event_id"]
        return True

    def mark_gap(self) -> None:
        for record in self.records.values():
            if record.state != "FINALIZED":
                record.stream_gap_detected = True

    def due(self, now: datetime, stream_active: bool, allow_negative: bool = True) -> list[OutcomeRecord]:
        due: list[OutcomeRecord] = []
        for record in self.records.values():
            if record.state == "FINALIZED" or now < record.outcome_cutoff:
                continue
            record.state = "FINALIZING"
            record.stream_coverage_end = now
            if record.migration_seen:
                record.outcome_status = "POSITIVE"
            elif allow_negative and stream_active and not record.stream_gap_detected and record.stream_coverage_start <= record.creation_time:
                record.outcome_status = "NEGATIVE"
            else:
                record.outcome_status = "UNKNOWN"
            record.outcome_finalized_at = now
            record.state = "FINALIZED"
            due.append(record)
        return due

    def close(self, now: datetime, controlled: bool) -> list[OutcomeRecord]:
        result: list[OutcomeRecord] = []
        for record in self.records.values():
            if record.state == "FINALIZED":
                continue
            record.stream_coverage_end = now
            record.outcome_status = "CENSORED" if controlled else "INCOMPLETE_STREAM"
            record.outcome_finalized_at = now
            record.state = "FINALIZED"
            result.append(record)
        return result

    def snapshot(self) -> dict[str, Any]:
        return {
            "stream_session_id": self.stream_session_id,
            "records": {
                mint: {
                    "token_mint": record.token_mint,
                    "creation_time": record.creation_time.isoformat(),
                    "outcome_cutoff": record.outcome_cutoff.isoformat(),
                    "stream_session_id": record.stream_session_id,
                    "stream_coverage_start": record.stream_coverage_start.isoformat(),
                    "stream_coverage_end": record.stream_coverage_end.isoformat() if record.stream_coverage_end else None,
                    "stream_gap_detected": record.stream_gap_detected,
                    "migration_seen": record.migration_seen,
                    "migration_time": record.migration_time.isoformat() if record.migration_time else None,
                    "migration_event_id": record.migration_event_id,
                    "state": record.state,
                    "outcome_status": record.outcome_status,
                    "outcome_finalized_at": record.outcome_finalized_at.isoformat() if record.outcome_finalized_at else None,
                }
                for mint, record in self.records.items()
            },
        }

    @classmethod
    def restore(cls, raw: dict[str, Any], stream_session_id: str) -> OutcomeBook:
        book = cls(stream_session_id)
        for mint, value in raw.get("records", {}).items():
            record = OutcomeRecord(
                token_mint=mint,
                creation_time=parse_time(value["creation_time"]),
                outcome_cutoff=parse_time(value["outcome_cutoff"]),
                stream_session_id=value.get("stream_session_id", stream_session_id),
                stream_coverage_start=parse_time(value["stream_coverage_start"]),
                stream_coverage_end=parse_time(value["stream_coverage_end"]) if value.get("stream_coverage_end") else None,
                # A restart leaves an unmeasurable transport gap by default.
                stream_gap_detected=bool(value.get("stream_gap_detected", False)) or value.get("state") != "FINALIZED",
                migration_seen=bool(value.get("migration_seen", False)),
                migration_time=parse_time(value["migration_time"]) if value.get("migration_time") else None,
                migration_event_id=value.get("migration_event_id"),
                state=value.get("state", "PENDING"),
                outcome_status=value.get("outcome_status"),
                outcome_finalized_at=parse_time(value["outcome_finalized_at"]) if value.get("outcome_finalized_at") else None,
            )
            book.records[mint] = record
        return book
