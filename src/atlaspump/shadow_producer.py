"""Append-only Cerebro producer for the provider-neutral RFC-014 stream."""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlaspump.shadow_stream import EVENT_TYPES, STREAM_SCHEMA_VERSION, validate_event

TRANSPORT_SCHEMA_VERSION = "atlaspump.shadow.transport.v1"

NORMALIZED_TYPES = {
    "CREATE": "CREATE_TOKEN",
    "BUY": "BUY",
    "SELL": "SELL",
    "TRANSFER": "TRANSFER",
    "MIGRATION": "MIGRATE",
}


def canonical_record(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def record_hash(value: dict[str, Any]) -> str:
    raw = dict(value); raw.pop("record_sha256", None)
    return hashlib.sha256(canonical_record(raw)).hexdigest()


def repair_active_file(root: Path) -> dict[str, Any]:
    """Repair only an incomplete final active record; archives are immutable."""
    active = root / "active" / "events.jsonl"
    report = {"file_id": None, "original_size": 0, "recovered_size": 0, "bytes_truncated": 0, "last_valid_sequence": None, "recovery_reason": None, "recovery_time": datetime.now(UTC).isoformat()}
    if not active.exists(): return report
    data = active.read_bytes(); report["original_size"] = len(data)
    valid_end = 0
    for line in data.splitlines(keepends=True):
        if not line.endswith(b"\n"): break
        try:
            value = json.loads(line)
            if value.get("record_sha256") != record_hash(value): break
        except (UnicodeDecodeError, json.JSONDecodeError): break
        valid_end += len(line); report["file_id"] = value.get("source_file_id"); report["last_valid_sequence"] = value.get("stream_sequence")
    if valid_end != len(data):
        with active.open("r+b") as handle:
            handle.truncate(valid_end); handle.flush(); os.fsync(handle.fileno())
        report["bytes_truncated"] = len(data) - valid_end; report["recovery_reason"] = "ACTIVE_SUFFIX_PARTIAL_OR_INVALID"
    report["recovered_size"] = valid_end
    path = root / "state" / "recovery_report.json"; path.write_text(json.dumps(report, sort_keys=True) + "\n")
    return report


def rebuild_sequence_index(root: Path, dry_run: bool = False) -> dict[str, Any]:
    """JSONL is authoritative; append only missing index entries, reject conflicts."""
    (root / "state").mkdir(parents=True, exist_ok=True)
    repair = repair_active_file(root)
    index_path = root / "state" / "sequence_index.jsonl"; existing: dict[tuple[str, int], dict[str, Any]] = {}
    if index_path.exists():
        for line in index_path.read_text().splitlines():
            try:
                item = json.loads(line); key = (str(item.get("stream_session_id")), int(item.get("stream_sequence")))
                existing[key] = item
            except (ValueError, json.JSONDecodeError): continue
    files = sorted((root / "archive").glob("*.jsonl")) + [root / "active" / "events.jsonl"]
    missing: list[dict[str, Any]] = []; conflicts: list[dict[str, Any]] = []; verified = 0
    for path in files:
        if not path.exists(): continue
        offset = 0
        for raw in path.open("rb"):
            end = offset + len(raw)
            try: value = json.loads(raw)
            except json.JSONDecodeError: break
            key = (str(value.get("stream_session_id")), int(value.get("stream_sequence")))
            expected = {k: value.get(k) for k in ("stream_session_id", "stream_sequence", "source_file_id", "source_offset_start", "source_offset_end", "record_sha256", "record_kind", "control_type", "event_id")}
            if value.get("record_sha256") != record_hash(value) or value.get("source_offset_start") != offset or value.get("source_offset_end") != end:
                conflicts.append({"status": "INDEX_CONFLICT", "key": key, "reason": "record_hash_or_offset"})
            elif key in existing and existing[key] != expected:
                conflicts.append({"status": "INDEX_CONFLICT", "key": key, "reason": "index_hash_or_offset"})
            elif key not in existing: missing.append(expected)
            verified += 1; offset = end
    report = {"repair": repair, "records_verified": verified, "entries_missing": len(missing), "conflicts": conflicts, "dry_run": dry_run, "status": "INDEX_CONFLICT" if conflicts else "OK"}
    if conflicts: return report
    if missing and not dry_run:
        with index_path.open("a", encoding="utf-8") as handle:
            for item in missing: handle.write(canonical_record(item).decode() + "\n")
            handle.flush(); os.fsync(handle.fileno())
    (root / "state" / "index_validation_report.json").write_text(json.dumps(report, sort_keys=True, default=str) + "\n")
    return report


def _iso_epoch(value: Any, fallback: datetime) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z")
    return fallback.isoformat().replace("+00:00", "Z")


def deterministic_event_id(event: dict[str, Any]) -> str:
    if event.get("event_id"):
        return str(event["event_id"])
    signature, mint = event.get("signature"), event.get("token_mint")
    index = event.get("instruction_index", event.get("sequence_index", 0))
    if signature and mint:
        return f"{signature}:{index}:{mint}"
    return hashlib.sha256(json.dumps(event, sort_keys=True, default=str).encode()).hexdigest()


def to_stream_event(normalized: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    mapped = NORMALIZED_TYPES.get(str(normalized.get("event_type")), str(normalized.get("event_type")))
    if mapped not in EVENT_TYPES:
        raise ValueError(f"event type not exportable: {mapped}")
    event = {
        "schema_version": STREAM_SCHEMA_VERSION,
        "event_id": deterministic_event_id(normalized),
        "token_mint": normalized.get("token_mint"),
        "event_type": mapped,
        "event_time": _iso_epoch(normalized.get("blockchain_timestamp"), now),
        "ingestion_time": _iso_epoch(normalized.get("archive_timestamp"), now),
        "signature": normalized.get("signature") or "MISSING_SIGNATURE",
        "wallet": normalized.get("wallet") or "",
        "protocol_scope": normalized.get("protocol_scope") or "OTHER",
        "source": normalized.get("source") or "cerebro_normalized_event",
        "sequence_index": int(normalized.get("instruction_index", 0)),
        "quality_flags": ["MISSING_SIGNATURE"] if not normalized.get("signature") else [],
    }
    return validate_event(event)


class JsonlProducer:
    def __init__(self, root: Path, rotate_bytes: int = 128 * 1024 * 1024) -> None:
        self.root, self.rotate_bytes = root, rotate_bytes
        self.active = root / "active" / "events.jsonl"
        self.state = root / "state" / "seen_event_ids.json"
        for path in (root / "active", root / "archive", root / "state", root / "dead_letter", root / "logs", root / "manifests"):
            path.mkdir(parents=True, exist_ok=True)
        raw_state = json.loads(self.state.read_text()) if self.state.exists() else []
        # v1 persisted only a JSON list.  Keep it readable while preserving
        # transport sequence state for every subsequent write.
        self.seen = set(raw_state if isinstance(raw_state, list) else raw_state.get("seen_event_ids", []))
        self.stream_session_id: str | None = None if isinstance(raw_state, list) else raw_state.get("stream_session_id")
        self.stream_sequence = 0 if isinstance(raw_state, list) else int(raw_state.get("stream_sequence", 0))
        self.source_file_id = ""
        self.rotation_index = 0
        self.file_first_sequence: int | None = None
        self.file_records = 0
        self.previous_source_file_id: str | None = None
        self._ensure_active_file()

    @property
    def index_path(self) -> Path:
        return self.root / "state" / "sequence_index.jsonl"

    def _ensure_active_file(self) -> None:
        if not self.source_file_id:
            self.source_file_id = str(uuid.uuid4())
        self.active.touch(exist_ok=True)
        self._write_manifest(closed=False)
        self._write_active_pointer()

    def _write_active_pointer(self) -> None:
        path = self.root / "manifests" / "active.json"; path.parent.mkdir(parents=True, exist_ok=True)
        value = {"stream_session_id": self.stream_session_id, "source_file_id": self.source_file_id, "path": str(self.active), "previous_source_file_id": self.previous_source_file_id, "first_sequence": self.file_first_sequence, "updated_at": datetime.now(UTC).isoformat()}
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(value, sort_keys=True) + "\n"); handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)

    def _manifest_path(self, file_id: str | None = None) -> Path:
        return self.root / "manifests" / "files" / f"{file_id or self.source_file_id}.json"

    def _write_manifest(self, closed: bool, rotation_reason: str | None = None, next_source_file_id: str | None = None) -> None:
        path = self._manifest_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        value = {
            "source_file_id": self.source_file_id,
            "stream_session_id": self.stream_session_id,
            "path": str(self.active if not closed else self.root / "archive" / f"{self.source_file_id}.jsonl"),
            "created_at": datetime.now(UTC).isoformat(),
            "closed_at": datetime.now(UTC).isoformat() if closed else None,
            "first_sequence": self.file_first_sequence,
            "last_sequence": self.stream_sequence if self.file_records else None,
            "size_bytes": self.active.stat().st_size if self.active.exists() else 0,
            "records_written": self.file_records,
            "sha256": hashlib.sha256(self.active.read_bytes()).hexdigest() if closed and self.active.exists() else None,
            "rotation_reason": rotation_reason,
            "immutable": closed,
            "previous_source_file_id": self.previous_source_file_id,
            "next_source_file_id": next_source_file_id,
            "next_path": str(self.root / "active" / "events.jsonl") if next_source_file_id else None,
            "rotation_control_sequence": self.stream_sequence if next_source_file_id else None,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
        os.replace(temporary, path)

    def start_session(self, stream_session_id: str) -> None:
        self.stream_session_id = stream_session_id
        self.stream_sequence = 0
        self.source_file_id = str(uuid.uuid4())
        self.previous_source_file_id = None
        self.file_first_sequence = None
        self.file_records = 0
        self._ensure_active_file()
        self._save()

    def _next_sequence(self) -> int:
        self.stream_sequence += 1
        return self.stream_sequence

    def control(self, control_type: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.stream_session_id:
            raise RuntimeError("transport session has not started")
        record = {
            "schema_version": TRANSPORT_SCHEMA_VERSION,
            "record_kind": "CONTROL",
            "control_type": control_type,
            "stream_session_id": self.stream_session_id,
            "stream_sequence": self._next_sequence(),
            "control_time": datetime.now(UTC).isoformat(),
            "producer_host": "cerebro-one",
            "details": details or {},
        }
        self._write_record(record)
        return record

    def _save(self) -> None:
        temp = self.state.with_suffix(".tmp")
        temp.write_text(json.dumps({
            "seen_event_ids": sorted(self.seen),
            "stream_session_id": self.stream_session_id,
            "stream_sequence": self.stream_sequence,
        }) + "\n")
        os.replace(temp, self.state)

    @staticmethod
    def _canonical(value: dict[str, Any]) -> bytes:
        return canonical_record(value)

    def _write_record(self, record: dict[str, Any]) -> dict[str, Any]:
        start = self.active.stat().st_size if self.active.exists() else 0
        record = record | {"source_file_id": self.source_file_id, "source_offset_start": start}
        end = start
        for _ in range(8):
            without_hash = record | {"source_offset_end": end}
            without_hash.pop("record_sha256", None)
            digest = record_hash(without_hash)
            candidate = without_hash | {"record_sha256": digest}
            line = self._canonical(candidate) + b"\n"
            new_end = start + len(line)
            if new_end == end:
                record = candidate
                break
            end = new_end
            record = candidate
        else:
            raise RuntimeError("offset serialization did not converge")
        with self.active.open("ab") as handle:
            handle.write(line)
            handle.flush(); os.fsync(handle.fileno())
        index = {
            key: record.get(key) for key in ("stream_session_id", "stream_sequence", "source_file_id", "source_offset_start", "source_offset_end", "record_sha256", "record_kind", "control_type", "event_id")
        }
        with self.index_path.open("a", encoding="utf-8") as handle:
            handle.write(self._canonical(index).decode() + "\n")
            handle.flush(); os.fsync(handle.fileno())
        self.file_first_sequence = self.file_first_sequence or record.get("stream_sequence")
        self.file_records += 1
        self._write_manifest(closed=False)
        self._write_active_pointer()
        return record

    def _rotate(self, at: datetime) -> Path | None:
        if not self.active.exists() or self.active.stat().st_size < self.rotate_bytes:
            return None
        old_file_id, new_file_id = self.source_file_id, str(uuid.uuid4())
        # This record is durable inside A before A is closed, giving a reader
        # at A's EOF a self-contained successor proof.
        if self.stream_session_id:
            self.control("FILE_ROTATION", {"old_file_id": old_file_id, "new_file_id": new_file_id, "last_old_sequence": self.stream_sequence + 1, "first_new_sequence": self.stream_sequence + 2})
        self._write_manifest(closed=True, rotation_reason="size", next_source_file_id=new_file_id)
        destination = self.root / "archive" / f"{self.source_file_id}.jsonl"
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self.active, destination)
        self.source_file_id = new_file_id
        self.previous_source_file_id = old_file_id
        self.rotation_index += 1
        self.file_first_sequence = None
        self.file_records = 0
        self._ensure_active_file()
        return destination

    def append(self, event: dict[str, Any]) -> bool:
        parsed = validate_event(event)
        if parsed["event_id"] in self.seen:
            return False
        self._rotate(parsed["ingestion_time"])
        if self.stream_session_id:
            event = event | {
                "record_kind": "EVENT",
                "transport_protocol_version": TRANSPORT_SCHEMA_VERSION,
                "stream_session_id": self.stream_session_id,
                "stream_sequence": self._next_sequence(),
            }
        self._write_record(event)
        self.seen.add(parsed["event_id"])
        self._save()
        return True

    def dead_letter(self, raw: Any, error: Exception) -> None:
        path = self.root / "dead_letter" / "events.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"raw": raw, "error": str(error), "at": datetime.now(UTC).isoformat()}) + "\n")


class ShadowEventSink:
    """Fail-open adapter for a collector immediately after ``normalize_event``.

    A collector owns this object and calls :meth:`emit` after its historical
    output path.  Every failure is contained here, so the collector cannot be
    delayed or terminated by shadow observability.
    """

    def __init__(self, producer: JsonlProducer, enabled: bool = False) -> None:
        self.producer = producer
        self.enabled = enabled
        self.emitted = 0
        self.skipped = 0
        self.errors = 0

    def emit(self, normalized_event: dict[str, Any]) -> None:
        if not self.enabled:
            self.skipped += 1
            return
        try:
            self.emitted += int(self.producer.append(to_stream_event(normalized_event)))
        except Exception as exc:  # fail-open is the collector safety contract
            self.errors += 1
            try:
                self.producer.dead_letter(normalized_event, exc)
            except Exception:
                pass
