#!/usr/bin/env python3
"""Cursor-v1 SSH relay.  The consumer owns the durable acknowledgement."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = "atlaspump.shadow.transport.v1"


def emit(kind: str, details: dict) -> None:
    print(json.dumps({"schema_version": SCHEMA, "record_kind": "CONTROL", "control_type": kind, "control_time": datetime.now(UTC).isoformat(), "producer_host": "lolopc-wsl", "details": details}, sort_keys=True), flush=True)


def checkpoint(path: Path) -> dict | None:
    try: return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError): return None


def remote_command(args: argparse.Namespace, cursor: dict | None) -> str:
    command = [".venv/bin/python", "scripts/shadow/read_shadow_transport.py", "--stream-root", args.stream_root, "--follow"]
    if cursor:
        command += ["--session-id", str(cursor.get("stream_session_id", "")), "--after-sequence", str(cursor["stream_sequence"]), "--after-file-id", str(cursor.get("source_file_id", "")), "--after-offset", str(cursor.get("source_offset_end", "")), "--after-hash", str(cursor.get("record_sha256", ""))]
    return "cd /mnt/atlaspump/code/atlaspump && " + " ".join(command)


def main(args: argparse.Namespace) -> int:
    reconnects = business_events = 0; simulated = False
    while args.max_reconnects < 0 or reconnects <= args.max_reconnects:
        cursor = checkpoint(args.checkpoint)
        process = subprocess.Popen(["ssh", "-i", args.identity_file, args.host, remote_command(args, cursor)], stdout=subprocess.PIPE, stderr=sys.stderr, text=True, bufsize=1)
        emit("TRANSPORT_RECONNECTED" if reconnects else "TRANSPORT_CONNECTED", {"reconnect": reconnects, "checkpoint": cursor})
        assert process.stdout
        forced = False
        for line in process.stdout:
            print(line.rstrip("\n"), flush=True)
            try:
                raw = json.loads(line)
                if raw.get("record_kind") == "EVENT": business_events += 1
            except json.JSONDecodeError:
                emit("POSSIBLE_GAP", {"reason": "invalid_reader_line"})
            if args.simulate_disconnect_after_events and business_events >= args.simulate_disconnect_after_events and not simulated:
                simulated = forced = True; process.terminate(); break
        code = process.wait()
        emit("TRANSPORT_DISCONNECTED", {"ssh_exit_code": code, "forced": forced, "checkpoint": checkpoint(args.checkpoint)})
        reconnects += 1
        if args.max_reconnects >= 0 and reconnects > args.max_reconnects: return code
        time.sleep(min(args.reconnect_max_seconds, args.reconnect_initial_seconds * 2 ** max(0, reconnects - 1)))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport-mode", choices=("cursor-v1", "legacy-tail"), default="cursor-v1")
    parser.add_argument("--host", required=True); parser.add_argument("--identity-file", required=True)
    parser.add_argument("--stream-root", default="/mnt/atlaspump/shadow-stream")
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--reconnect-initial-seconds", type=float, default=1); parser.add_argument("--reconnect-max-seconds", type=float, default=30)
    parser.add_argument("--max-reconnects", type=int, default=-1); parser.add_argument("--simulate-disconnect-after-events", type=int, default=0)
    raise SystemExit(main(parser.parse_args()))
