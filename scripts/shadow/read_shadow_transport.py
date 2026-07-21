#!/usr/bin/env python3
"""Cursor-aware, read-only Cerebro transport reader for RFC-014."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
from atlaspump.shadow_producer import rebuild_sequence_index  # noqa: E402


def emit_control(kind: str, details: dict) -> None:
    print(json.dumps({"schema_version": "atlaspump.shadow.transport.v1", "record_kind": "CONTROL", "control_type": kind, "details": details}, sort_keys=True), flush=True)


def entries(root: Path) -> list[dict]:
    path = root / "state" / "sequence_index.jsonl"
    result = []
    if not path.exists():
        return result
    for line in path.read_text(encoding="utf-8").splitlines():
        try: result.append(json.loads(line))
        except json.JSONDecodeError: continue
    return result


def file_for(root: Path, file_id: str) -> Path | None:
    manifest = root / "manifests" / "files" / f"{file_id}.json"
    if manifest.exists():
        try:
            path = Path(json.loads(manifest.read_text())["path"])
            if path.exists(): return path
        except (KeyError, json.JSONDecodeError): pass
    active = root / "active" / "events.jsonl"
    if active.exists():
        for line in active.read_text(encoding="utf-8", errors="replace").splitlines()[:1]:
            try:
                if json.loads(line).get("source_file_id") == file_id: return active
            except json.JSONDecodeError: pass
    archived = root / "archive" / f"{file_id}.jsonl"
    return archived if archived.exists() else None


def manifest_chain_valid(root: Path, previous: dict, current: dict) -> bool:
    if previous.get("source_file_id") == current.get("source_file_id"): return True
    first, second = root / "manifests" / "files" / f"{previous.get('source_file_id')}.json", root / "manifests" / "files" / f"{current.get('source_file_id')}.json"
    try:
        a, b = json.loads(first.read_text()), json.loads(second.read_text())
        return a.get("next_source_file_id") == b.get("source_file_id") and b.get("previous_source_file_id") == a.get("source_file_id") and b.get("first_sequence") == a.get("last_sequence", 0) + 1
    except (OSError, json.JSONDecodeError): return False


def verify_cursor(root: Path, args: argparse.Namespace, index: list[dict]) -> int | None:
    if args.after_sequence is None:
        # A new reader starts at the newest session boundary, never by
        # replaying all historical sessions.
        for pos in range(len(index) - 1, -1, -1):
            if index[pos].get("record_kind") == "CONTROL" and index[pos].get("control_type") == "SESSION_START":
                return pos - 1
        return len(index) - 1
    for pos, item in enumerate(index):
        if item.get("stream_session_id") == args.session_id and item.get("stream_sequence") == args.after_sequence:
            if args.after_file_id and item.get("source_file_id") != args.after_file_id:
                emit_control("CURSOR_INVALID", {"reason": "file_id_mismatch"}); return None
            if args.after_offset is not None and item.get("source_offset_end") != args.after_offset:
                emit_control("CURSOR_INVALID", {"reason": "offset_mismatch"}); return None
            if args.after_hash and item.get("record_sha256") != args.after_hash:
                emit_control("CURSOR_HASH_MISMATCH", {}); return None
            if not file_for(root, str(item["source_file_id"])):
                emit_control("RETENTION_GAP", {"source_file_id": item["source_file_id"]}); return None
            emit_control("CURSOR_ACCEPTED", {"after_sequence": args.after_sequence})
            return pos
    emit_control("CURSOR_NOT_FOUND", {"after_sequence": args.after_sequence})
    return None


def emit_from(root: Path, index: list[dict], start: int) -> int:
    sent = 0
    previous: dict | None = None
    for item in index[start + 1:]:
        if previous is not None and not manifest_chain_valid(root, previous, item):
            emit_control("ROTATION_CHAIN_INCOMPLETE", {"from": previous.get("source_file_id"), "to": item.get("source_file_id")}); return sent
        path = file_for(root, str(item.get("source_file_id")))
        if path is None: emit_control("RETENTION_GAP", {"source_file_id": item.get("source_file_id")}); return sent
        with path.open("rb") as handle:
            handle.seek(int(item["source_offset_start"]))
            raw = handle.read(int(item["source_offset_end"]) - int(item["source_offset_start"]))
        try: record = json.loads(raw)
        except json.JSONDecodeError: emit_control("CURSOR_INVALID", {"reason": "partial_record"}); return sent
        if record.get("record_sha256") != item.get("record_sha256"):
            emit_control("CURSOR_HASH_MISMATCH", {"stream_sequence": item.get("stream_sequence")}); return sent
        print(json.dumps(record, sort_keys=True), flush=True); sent += 1
        previous = item
    return sent


def main(args: argparse.Namespace) -> int:
    root = args.stream_root
    if args.repair_index:
        report = rebuild_sequence_index(root, dry_run=args.dry_run)
        print(json.dumps(report, sort_keys=True, default=str), flush=True)
        return 2 if report["status"] == "INDEX_CONFLICT" else 0
    index = entries(root)
    cursor = verify_cursor(root, args, index)
    if cursor is None: return 2
    emit_control("CURSOR_REPLAY_STARTED", {"after_sequence": args.after_sequence})
    while True:
        index = entries(root)
        sent = emit_from(root, index, cursor)
        cursor += sent
        if sent:
            emit_control("NO_GAP_CONFIRMED", {"last_index": cursor, "proof": "cursor_hash_offset_and_contiguous_sequence"})
            emit_control("CURSOR_CAUGHT_UP", {"last_index": cursor})
        if not args.follow: return 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-root", type=Path, required=True)
    parser.add_argument("--session-id")
    parser.add_argument("--after-sequence", type=int)
    parser.add_argument("--after-file-id")
    parser.add_argument("--after-offset", type=int)
    parser.add_argument("--after-hash")
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=0.25)
    parser.add_argument("--repair-index", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    raise SystemExit(main(parser.parse_args()))
