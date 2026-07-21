#!/usr/bin/env python3
"""Single-connection, passive PumpAPI Data Stream collector."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
from atlaspump.pumpapi_shadow_collector import InspectionBook, PumpApiShadowFilter  # noqa: E402
from atlaspump.shadow_producer import JsonlProducer, ShadowEventSink  # noqa: E402


async def run(args: argparse.Namespace) -> None:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("install the websocket client dependency in Cerebro .venv before network validation") from exc
    producer = JsonlProducer(args.output_root, rotate_bytes=args.rotate_bytes)
    stream_session_id = str(uuid.uuid4())
    producer.start_session(stream_session_id)
    session_started = datetime.now(UTC)
    producer.control("SESSION_START", {"connection_state": "STARTING"})
    sink = ShadowEventSink(producer, enabled=not args.inspect_only)
    collector = PumpApiShadowFilter(sink, args.max_tracked_tokens, args.retention_grace_seconds)
    inspection = InspectionBook(args.sample_per_action_pool)
    started, backoff, connections, bytes_received = time.monotonic(), args.reconnect_initial_seconds, 0, 0
    stop_reason = "limit"
    controls_emitted = 1
    next_heartbeat = time.monotonic() + args.heartbeat_seconds
    while time.monotonic() - started < args.max_seconds and collector.metrics.messages_received < args.max_messages and collector.metrics.new_tokens_seen < args.max_tokens:
        try:
            async with websockets.connect(args.websocket_url, ping_interval=20, ping_timeout=20) as socket:
                connections += 1; backoff = args.reconnect_initial_seconds
                while time.monotonic() - started < args.max_seconds and collector.metrics.messages_received < args.max_messages and collector.metrics.new_tokens_seen < args.max_tokens:
                    timeout = min(args.read_timeout_seconds, max(0.05, next_heartbeat - time.monotonic()))
                    try:
                        raw = await asyncio.wait_for(socket.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        producer.control("HEARTBEAT", {
                            "producer_time": datetime.now(UTC).isoformat(),
                            "last_event_sequence": producer.stream_sequence,
                            "events_emitted": collector.metrics.events_emitted,
                            "connection_state": "CONNECTED",
                        })
                        controls_emitted += 1
                        next_heartbeat = time.monotonic() + args.heartbeat_seconds
                        continue
                    bytes_received += len(raw)
                    try:
                        payload = json.loads(raw)
                        received = datetime.now(UTC)
                        if args.inspect_only:
                            inspection.add(payload, received)
                            collector.metrics.messages_received += 1
                            if args.stop_when_targets_observed and inspection.target_creates() >= args.min_create_events:
                                stop_reason = "minimum_compatible_create_events_observed"
                                break
                        else:
                            collector.accept(payload, received)
                    except Exception:
                        collector.metrics.invalid_messages += 1
                    if not args.inspect_only and time.monotonic() >= next_heartbeat:
                        producer.control("HEARTBEAT", {
                            "producer_time": datetime.now(UTC).isoformat(),
                            "last_event_sequence": producer.stream_sequence,
                            "events_emitted": collector.metrics.events_emitted,
                            "connection_state": "CONNECTED",
                        })
                        controls_emitted += 1
                        next_heartbeat = time.monotonic() + args.heartbeat_seconds
        except Exception:
            await asyncio.sleep(backoff); backoff = min(args.reconnect_max_seconds, backoff * 2)
    session_ended = datetime.now(UTC)
    if not args.inspect_only:
        producer.control("COLLECTOR_LIMIT_REACHED" if stop_reason == "limit" else "COLLECTOR_SHUTDOWN", {"termination_reason": stop_reason})
        producer.control("SESSION_END", {"termination_reason": stop_reason, "events_emitted": collector.metrics.events_emitted})
        controls_emitted += 2
    report = collector.inspection() | {"connections": connections, "bytes_received": bytes_received, "elapsed_seconds": time.monotonic() - started, "inspect_only": args.inspect_only, "stop_reason": stop_reason, "stream_session_id": stream_session_id, "session_start_time": session_started, "session_end_time": session_ended, "first_sequence": 1, "last_sequence": producer.stream_sequence, "events_emitted": collector.metrics.events_emitted, "control_records_emitted": controls_emitted if not args.inspect_only else 0, "termination_reason": stop_reason, "inspection": {"actions": dict(inspection.actions), "pools": dict(inspection.pools), "action_pools": dict(inspection.pairs), "samples": dict(inspection.samples), "first_ingestion": inspection.first_ingestion, "last_ingestion": inspection.last_ingestion}}
    target = args.output_root / ("schema_inspection" if args.inspect_only else "logs") / f"pumpapi-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    target.parent.mkdir(parents=True, exist_ok=True); target.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--websocket-url", default="wss://stream.pumpapi.io/")
    parser.add_argument("--inspect-only", action="store_true")
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-tokens", type=int, default=100)
    parser.add_argument("--max-messages", type=int, default=1_000_000)
    parser.add_argument("--max-tracked-tokens", type=int, default=5000)
    parser.add_argument("--retention-grace-seconds", type=int, default=15)
    parser.add_argument("--reconnect-initial-seconds", type=int, default=1)
    parser.add_argument("--reconnect-max-seconds", type=int, default=60)
    parser.add_argument("--read-timeout-seconds", type=int, default=30)
    parser.add_argument("--heartbeat-seconds", type=float, default=5.0)
    parser.add_argument("--rotate-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--target-actions", default="create,migrate,createPool,add,remove")
    parser.add_argument("--min-create-events", type=int, default=3)
    parser.add_argument("--min-migrate-events", type=int, default=1)
    parser.add_argument("--min-create-pool-events", type=int, default=1)
    parser.add_argument("--stop-when-targets-observed", action="store_true")
    parser.add_argument("--sample-per-action-pool", type=int, default=3)
    asyncio.run(run(parser.parse_args()))
