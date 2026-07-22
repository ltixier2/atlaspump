"""Safe, provider-neutral RFC-014 shadow stream primitives.

This module intentionally contains no wallet, signing, swap, or Solana send
operation.  The transport contract is JSONL so a collector can write to stdout
over SSH or append locally; the shadow worker only consumes records.
"""
from __future__ import annotations

import hashlib
import json
import os
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
    root: Path
    seen: set[str] = field(default_factory=set)
    window_state: dict[str, Any] = field(default_factory=dict)
    prediction_ids: set[str] = field(default_factory=set)
    outcome_ids: set[str] = field(default_factory=set)
    outcome_state: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, root: Path) -> StreamState:
        state = cls(root)
        file = root / "state" / "seen_event_ids.json"
        if file.exists():
            raw = json.loads(file.read_text(encoding="utf-8"))
            # Compatibility with the v1 state file, which was simply a list.
            if isinstance(raw, list):
                state.seen = set(raw)
            else:
                state.seen = set(raw.get("seen_event_ids", []))
                state.window_state = dict(raw.get("window_state", {}))
                state.prediction_ids = set(raw.get("prediction_ids", []))
                state.outcome_ids = set(raw.get("outcome_ids", []))
                state.outcome_state = dict(raw.get("outcome_state", {}))
        return state

    def save(self) -> None:
        atomic_json(
            self.root / "state" / "seen_event_ids.json",
            {
                "seen_event_ids": sorted(self.seen),
                "window_state": self.window_state,
                "prediction_ids": sorted(self.prediction_ids),
                "outcome_ids": sorted(self.outcome_ids),
                "outcome_state": self.outcome_state,
            },
        )


@dataclass
class FeatureResult:
    token_mint: str
    creation_time: datetime
    values: dict[str, Any]
    event_count: int
    max_ingestion_delay_ms: int
    rejected_after_cutoff: int
    event_arrival_lag_ms: int
    status: str


class WindowBook:
    """Idempotent token windows; it never invents outcomes or amount semantics."""
    def __init__(self) -> None:
        self.windows: dict[str, TokenWindow] = {}
        self.delays: dict[str, list[int]] = {}
        self.rejected: dict[str, int] = {}
        self.migrations: dict[str, dict[str, Any]] = {}
        self.creation_arrival_lags: dict[str, int] = {}
        self.scheduled: set[str] = set()
        self.finalized_at: dict[str, datetime] = {}
        self.prediction_written: set[str] = set()
        self.late_events_after_finalization = 0
        self.late_events_within_feature_time = 0
        self.late_events_after_cutoff = 0

    def accept(self, event: dict[str, Any]) -> str:
        mint = event["token_mint"]
        if event["event_type"] == "CREATE_TOKEN":
            self.windows.setdefault(mint, TokenWindow(mint, event["event_time"]))
            self.creation_arrival_lags.setdefault(
                mint,
                max(0, int((event["ingestion_time"] - event["event_time"]).total_seconds() * 1000)),
            )
        if event["event_type"] == "MIGRATE":
            self.migrations[mint] = event
        window = self.windows.get(mint)
        if window is None:
            return "ORPHAN_EVENT"
        if window.finalized:
            self.late_events_after_finalization += 1
            if event["event_time"] <= window.creation_time + timedelta(seconds=10):
                self.late_events_within_feature_time += 1
            else:
                self.late_events_after_cutoff += 1
            return "LATE_AFTER_FINALIZATION"
        delay = max(0, int((event["ingestion_time"] - event["event_time"]).total_seconds() * 1000))
        if not window.add(event, event["ingestion_time"]):
            self.rejected[mint] = self.rejected.get(mint, 0) + 1
            return "OUTSIDE_WINDOW"
        self.delays.setdefault(mint, []).append(delay)
        return "ACCEPTED"

    def due(self, now: datetime) -> list[FeatureResult]:
        result = []
        for mint, window in self.windows.items():
            if window.finalized or now < window.creation_time + timedelta(seconds=10):
                continue
            values = window.finalize(now)
            self.finalized_at[mint] = now
            try:
                validate_features(values)
                status = "READY"
            except ValueError:
                status = "FEATURE_CONTRACT_ERROR"
            result.append(FeatureResult(mint, window.creation_time, values, len(window.events), max(self.delays.get(mint, [0])), self.rejected.get(mint, 0), self.creation_arrival_lags.get(mint, 0), status))
        return result

    def snapshot(self) -> dict[str, Any]:
        def encode(event: dict[str, Any]) -> dict[str, Any]:
            return {key: value.isoformat() if isinstance(value, datetime) else value for key, value in event.items()}

        finalized_at = self.finalized_at
        return {
            "windows": {
                mint: {
                    "creation_time": window.creation_time.isoformat(),
                    "feature_cutoff": (window.creation_time + timedelta(seconds=10)).isoformat(),
                    "state": "FINALIZED" if window.finalized else "OPEN",
                    "scheduled": mint in self.scheduled,
                    "finalized_at": finalized_at[mint].isoformat() if mint in finalized_at else None,
                    "prediction_written": mint in self.prediction_written,
                    "events": [encode(event) for event in window.events],
                    "finalized": window.finalized,
                }
                for mint, window in self.windows.items()
            },
            "delays": self.delays,
            "rejected": self.rejected,
            "creation_arrival_lags": self.creation_arrival_lags,
            "late_events_after_finalization": self.late_events_after_finalization,
            "late_events_within_feature_time": self.late_events_within_feature_time,
            "late_events_after_cutoff": self.late_events_after_cutoff,
        }

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
