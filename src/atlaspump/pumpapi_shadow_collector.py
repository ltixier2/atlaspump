"""Passive PumpAPI Data Stream filtering for RFC-014.

There is no API key, wallet, RPC or transaction capability in this module.
It accepts global stream messages, retains only new Pump.fun/PumpSwap tokens,
and emits the bounded shadow subset through the fail-open sink.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from atlaspump.shadow_producer import ShadowEventSink

# Confirmed by live inspection (buy/sell/transfer), bounded PumpAPI replay
# evidence (create on pool=pump), and three historical Pump.fun→PumpSwap
# migrations (txType=migrate, pool=pump-amm, poolCreatedBy=pump).
CANDIDATE_ACTIONS = {"create": "CREATE_TOKEN", "buy": "BUY", "sell": "SELL", "transfer": "TRANSFER", "migrate": "MIGRATE"}
SUPPORTED_POOLS = {"pump": "PUMPFUN_BONDING_CURVE", "pump-amm": "PUMPSWAP"}
# These observed actions are deliberately out of the first feature contract.
# They are normal stream traffic, not malformed input and not dead letters.
UNSUPPORTED_UNTIL_OBSERVED = {"migration", "createpool", "create_pool", "add", "remove", "claimcreatorfees"}


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def stream_event(payload: dict[str, Any], received_at: datetime) -> dict[str, Any] | None:
    action = str(payload.get("action") or payload.get("txType") or "").lower()
    event_type = CANDIDATE_ACTIONS.get(action)
    mint = payload.get("mint")
    if not event_type or not mint:
        return None
    pool = str(payload.get("pool", "")).lower()
    # Historical PumpAPI evidence has two equivalent type encodings:
    # ``action=migrate`` (July 2026 replay) and ``txType=migrate`` (earlier
    # captured samples).  The protocol constraints remain strict; a generic
    # migrate action from Meteora/Raydium is deliberately not accepted.
    if event_type == "MIGRATE" and not (
        action == "migrate"
        and pool == "pump-amm"
        and str(payload.get("poolCreatedBy", "")).lower() == "pump"
    ):
        return None
    timestamp = payload.get("timestamp")
    if event_type == "MIGRATE" and not (
        isinstance(payload.get("signature"), str)
        and payload["signature"]
        and isinstance(timestamp, (int, float))
        and timestamp > 0
    ):
        return None
    # PumpAPI inspection confirms epoch milliseconds (for example 1784531213034).
    seconds = timestamp / 1000 if isinstance(timestamp, (int, float)) and timestamp > 100_000_000_000 else timestamp
    event_time = datetime.fromtimestamp(seconds, UTC) if isinstance(seconds, (int, float)) else received_at
    signature = payload.get("signature")
    index = payload.get("index", payload.get("instruction_index", 0))
    identity = str(payload.get("id") or (f"{signature}:{index}:{mint}" if signature else payload_hash(payload)))
    return {
        "event_id": identity,
        "event_type": event_type,
        "token_mint": str(mint),
        "signature": str(signature) if signature else None,
        "wallet": payload.get("trader") or payload.get("wallet") or payload.get("txSigner"),
        "protocol_scope": SUPPORTED_POOLS.get(pool, f"UNSUPPORTED_{pool.upper() or 'UNKNOWN'}"),
        "event_time": event_time,
        "blockchain_timestamp": int(event_time.timestamp()),
        "archive_timestamp": int(received_at.timestamp()),
        "instruction_index": int(index) if isinstance(index, int) else 0,
        "source": "pumpapi_data_stream",
        "raw_payload_hash": payload_hash(payload),
        "amount_semantics_status": "UNPROVEN",
    }


@dataclass
class PumpApiMetrics:
    messages_received: int = 0
    messages_by_action: Counter[str] = field(default_factory=Counter)
    messages_by_pool: Counter[str] = field(default_factory=Counter)
    new_tokens_seen: int = 0
    events_in_feature_window: int = 0
    events_after_feature_cutoff: int = 0
    migrations_seen: int = 0
    events_emitted: int = 0
    events_filtered_out: int = 0
    invalid_messages: int = 0
    duplicates: int = 0
    peak_tracked_tokens: int = 0
    global_transfers_seen: int = 0
    global_transfers_filtered: int = 0
    tracked_transfers_emitted: int = 0
    invalid_transfers: int = 0


class PumpApiShadowFilter:
    def __init__(self, sink: ShadowEventSink, max_tracked_tokens: int = 5000, grace_seconds: int = 15) -> None:
        self.sink, self.max_tracked_tokens, self.grace = sink, max_tracked_tokens, timedelta(seconds=grace_seconds)
        self.tracked: dict[str, datetime] = {}
        self.seen: set[str] = set()
        self.metrics = PumpApiMetrics()

    def accept(self, payload: dict[str, Any], received_at: datetime | None = None) -> None:
        now = received_at or datetime.now(UTC)
        self.metrics.messages_received += 1
        self.metrics.messages_by_action[str(payload.get("action", payload.get("txType", "UNKNOWN"))).lower()] += 1
        self.metrics.messages_by_pool[str(payload.get("pool", "UNKNOWN")).lower()] += 1
        action = str(payload.get("action") or payload.get("txType") or "UNKNOWN").lower()
        normalized = stream_event(payload, now)
        if normalized is None:
            if action == "transfer":
                self.metrics.global_transfers_seen += 1
                self.metrics.global_transfers_filtered += 1
                self.metrics.events_filtered_out += 1
                return
            if action in UNSUPPORTED_UNTIL_OBSERVED:
                self.metrics.events_filtered_out += 1
                return
            if action == "migrate" and not (
                str(payload.get("pool", "")).lower() == "pump-amm"
                and str(payload.get("poolCreatedBy", "")).lower() == "pump"
            ):
                self.metrics.events_filtered_out += 1
                return
            self.metrics.invalid_messages += 1
            self.sink.emit({"event_type": "UNKNOWN", "raw": payload})
            return
        event_id, mint, kind = normalized["event_id"], normalized["token_mint"], normalized["event_type"]
        if event_id in self.seen:
            self.metrics.duplicates += 1
            return
        self.seen.add(event_id)
        if kind == "CREATE_TOKEN":
            if normalized["protocol_scope"].startswith("UNSUPPORTED") or len(self.tracked) >= self.max_tracked_tokens:
                self.metrics.events_filtered_out += 1
                return
            self.tracked[mint] = now
            self.metrics.new_tokens_seen += 1
            self.metrics.peak_tracked_tokens = max(self.metrics.peak_tracked_tokens, len(self.tracked))
        elif mint not in self.tracked:
            if kind == "TRANSFER":
                self.metrics.global_transfers_seen += 1
                self.metrics.global_transfers_filtered += 1
            self.metrics.events_filtered_out += 1
            return
        created = self.tracked[mint]
        if now <= created + timedelta(seconds=10):
            self.metrics.events_in_feature_window += 1
        else:
            self.metrics.events_after_feature_cutoff += 1
        if kind == "MIGRATE":
            self.metrics.migrations_seen += 1
        self.sink.emit(normalized)
        self.metrics.events_emitted += 1
        if kind == "TRANSFER":
            self.metrics.tracked_transfers_emitted += 1
        self.expire(now)

    def expire(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=300) - self.grace
        for mint, created in list(self.tracked.items()):
            if created < cutoff:
                del self.tracked[mint]

    def inspection(self) -> dict[str, Any]:
        return {"actions": dict(self.metrics.messages_by_action), "pools": dict(self.metrics.messages_by_pool), "tracked_tokens": len(self.tracked), "metrics": self.metrics.__dict__ | {"messages_by_action": dict(self.metrics.messages_by_action), "messages_by_pool": dict(self.metrics.messages_by_pool)}}


class InspectionBook:
    """Bounded schema evidence; it never emits a shadow event."""

    def __init__(self, sample_per_action_pool: int = 3) -> None:
        self.limit = sample_per_action_pool
        self.actions: Counter[str] = Counter()
        self.pools: Counter[str] = Counter()
        self.pairs: Counter[str] = Counter()
        self.samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.first_ingestion: str | None = None
        self.last_ingestion: str | None = None

    def add(self, payload: dict[str, Any], received_at: datetime) -> None:
        action, pool = str(payload.get("action", "UNKNOWN")), str(payload.get("pool", "unknown"))
        pair = f"{action}/{pool}"
        self.actions[action] += 1
        self.pools[pool] += 1
        self.pairs[pair] += 1
        stamp = received_at.isoformat()
        self.first_ingestion = self.first_ingestion or stamp
        self.last_ingestion = stamp
        if len(self.samples[pair]) >= self.limit:
            return
        redacted = {key: "REDACTED" if any(word in key.lower() for word in ("secret", "private", "key")) else value for key, value in payload.items()}
        self.samples[pair].append({"payload_sha256": payload_hash(payload), "message_bytes": len(json.dumps(payload, default=str).encode()), "ingestion_time": stamp, "keys": {key: type(value).__name__ for key, value in payload.items()}, "payload": redacted})

    def target_creates(self) -> int:
        return sum(count for pair, count in self.pairs.items() if pair.startswith("create/") and pair.split("/", 1)[1].lower() in SUPPORTED_POOLS)
