from datetime import UTC, datetime

from atlaspump.shadow_producer import (
    JsonlProducer,
    ShadowEventSink,
    deterministic_event_id,
    rebuild_sequence_index,
    repair_active_file,
    to_stream_event,
)


def normalized():
    return {"event_id": "canonical", "event_type": "CREATE", "token_mint": "mint", "signature": "sig", "wallet": "wallet", "protocol_scope": "PUMPFUN_BONDING_CURVE", "blockchain_timestamp": 1, "archive_timestamp": 2}


def test_normalized_event_has_stable_stream_identity():
    first, second = to_stream_event(normalized(), datetime(2026, 1, 1, tzinfo=UTC)), to_stream_event(normalized(), datetime(2026, 1, 1, tzinfo=UTC))
    assert first["event_id"] == second["event_id"] == "canonical"
    assert first["event_type"] == "CREATE_TOKEN"
    assert deterministic_event_id({"signature": "sig", "token_mint": "mint", "sequence_index": 3}) == "sig:3:mint"


def test_append_is_idempotent_and_rotates(tmp_path):
    producer = JsonlProducer(tmp_path, rotate_bytes=1)
    event = to_stream_event(normalized())
    assert producer.append(event)
    assert not producer.append(event)
    second = {**event, "event_id": "other"}
    assert producer.append(second)
    assert list((tmp_path / "archive").rglob("*.jsonl"))


def test_sink_is_fail_open_for_unsupported_event(tmp_path):
    sink = ShadowEventSink(JsonlProducer(tmp_path), enabled=True)
    sink.emit({"event_type": "UNKNOWN"})
    assert sink.errors == 1
    assert list((tmp_path / "dead_letter").glob("*.jsonl"))


def test_rebuild_repairs_missing_index_and_partial_active_suffix(tmp_path):
    producer = JsonlProducer(tmp_path); producer.start_session("session")
    producer.control("SESSION_START"); producer.append(to_stream_event(normalized()))
    index = producer.index_path; index.write_text("")  # simulated crash after JSONL fsync
    report = rebuild_sequence_index(tmp_path)
    assert report["entries_missing"] == 2 and report["status"] == "OK"
    with producer.active.open("ab") as handle: handle.write(b'{"partial"')
    repaired = repair_active_file(tmp_path)
    assert repaired["bytes_truncated"] > 0


def test_rotation_manifest_and_index_cross_archive(tmp_path):
    producer = JsonlProducer(tmp_path, rotate_bytes=1); producer.start_session("session")
    producer.control("SESSION_START"); producer.append(to_stream_event(normalized()))
    producer.append({**to_stream_event(normalized()), "event_id": "second"})
    archived = list((tmp_path / "archive").glob("*.jsonl"))
    assert archived
    manifest = __import__("json").loads((tmp_path / "manifests" / "files" / f"{archived[0].stem}.json").read_text())
    assert manifest["immutable"] and manifest["sha256"]
