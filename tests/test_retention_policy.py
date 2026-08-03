import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from atlaspump.retention_policy import _selected, plan_retention


def test_selection_is_stable() -> None:
    assert _selected("mint-a") == _selected("mint-a")


def test_plan_is_non_destructive(tmp_path: Path) -> None:
    life = tmp_path / "daily/2026-04-19/lifecycles"
    life.mkdir(parents=True)
    pq.write_table(pa.table({"mint": ["a"], "migration_explicit": [True], "event_count": [2]}), life / "token_lifecycles.parquet")
    pq.write_table(pa.table({"mint": ["a"]}), life / "token_outcomes_preliminary.parquet")
    pq.write_table(pa.table({"mint": ["a"]}), life / "token_events.parquet")
    pq.write_table(pa.table({"mint": []}, schema=pa.schema([("mint", pa.string())])), life / "lifecycle_anomalies.parquet")
    target = plan_retention(tmp_path, [date(2026, 4, 19)])
    value = json.loads(target.read_text())
    assert value["dry_run"] is True
    assert all(item["state"] != "DELETED" for item in value["records"])
    assert (life / "token_events.parquet").exists()
