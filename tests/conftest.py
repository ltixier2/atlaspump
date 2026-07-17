from __future__ import annotations

import json
from pathlib import Path

import pytest
import zstandard as zstd


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    content = (
        "\n".join(
            [
                json.dumps(
                    {
                        "event_type": "buy",
                        "signature": "sig-a",
                        "slot": 9,
                        "mint": "mint-a",
                        "wallet": "w1",
                        "sol_amount": "1.2",
                    }
                ),
                "{bad json",
                json.dumps({"type": "create", "signature": "sig-b", "slot": 10, "creator": "w2"}),
            ]
        )
        + "\n"
    )
    path = tmp_path / "sample.jsonl.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(content.encode()))
    return path
