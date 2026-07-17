"""Sequential, resumable hourly backfill and a DuckDB daily view."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb

from atlaspump.cli import audit
from atlaspump.config import Settings
from atlaspump.downloader import build_archive_url, download_archive


def parse_hours(value: str) -> list[int]:
    start, end = (int(part) for part in value.split("-", maxsplit=1))
    if not 0 <= start <= end <= 23:
        raise ValueError("hours must be an inclusive range within 0-23")
    return list(range(start, end + 1))


def backfill_day(settings: Settings, archive_date: date, hours: list[int], force: bool) -> Path:
    data_dir = settings.data_dir
    day_root = data_dir / "daily" / archive_date.isoformat()
    raw_root = data_dir / "raw" / archive_date.strftime("%Y/%m/%d")
    template = settings.values["source"]["archive_url_template"]
    for hour in hours:
        archive = raw_root / f"{hour:02d}.jsonl.zst"
        url = build_archive_url(template, archive_date, hour)
        if not archive.exists():
            download_archive(url, archive, settings.values["source"]["timeout_seconds"])
        hourly_dir = day_root / "hours" / f"{hour:02d}"
        previous = os.environ.get("ATLAS_DATA_DIR")
        os.environ["ATLAS_DATA_DIR"] = str(hourly_dir)
        try:
            audit(archive, settings, url, archive_date, hour, force)
        finally:
            if previous is None:
                os.environ.pop("ATLAS_DATA_DIR", None)
            else:
                os.environ["ATLAS_DATA_DIR"] = previous
    view_path = day_root / "events_daily.parquet"
    source_pattern = str(day_root / "hours" / "*" / "parquet" / "events_all.parquet")
    connection = duckdb.connect()
    quoted_source = source_pattern.replace("'", "''")
    quoted_output = str(view_path).replace("'", "''")
    connection.execute(
        "COPY (SELECT * EXCLUDE (row_number) FROM (SELECT *, row_number() OVER "
        "(PARTITION BY event_id ORDER BY archive_timestamp, block) AS row_number "
        f"FROM read_parquet('{quoted_source}')) WHERE row_number = 1) "
        f"TO '{quoted_output}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.close()
    return view_path
