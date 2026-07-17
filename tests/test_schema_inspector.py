from atlaspump.schema_inspector import SchemaInspector


def test_schema_tracks_nested_fields_and_types() -> None:
    inspector = SchemaInspector()
    inspector.observe({"type": "buy", "meta": {"slot": 1}}, ["type"])
    inspector.observe({"type": "sell", "meta": {"slot": "2"}}, ["type"])
    summary = inspector.summary()
    assert summary["fields"]["meta.slot"]["types"] == {"int": 1, "str": 1}
    assert summary["event_type_candidates"]["type"] == {"buy": 1, "sell": 1}
