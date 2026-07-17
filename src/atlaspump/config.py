"""Configuration loading and repository-relative paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    root: Path
    values: dict[str, Any]

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def data_dir(self) -> Path:
        return Path(os.environ.get("ATLAS_DATA_DIR", self.root / "data"))

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @property
    def manifests_dir(self) -> Path:
        return self.data_dir / "manifests"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_settings(config_path: Path | None = None) -> Settings:
    root = project_root()
    path = config_path or root / "config" / "default.yaml"
    with path.open("r", encoding="utf-8") as stream:
        values = yaml.safe_load(stream)
    if not isinstance(values, dict):
        raise ValueError(f"Invalid configuration in {path}")
    return Settings(root=root, values=values)
