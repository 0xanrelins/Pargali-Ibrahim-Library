#!/usr/bin/env python3
"""DuckDB connection helpers for the metadata catalog."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

_CONFIG_DB_PATH = re.compile(r"^\s*path:\s*(\S+)\s*$", re.MULTILINE)
_CONFIG_SCHEMA_VERSION = re.compile(r"^schema_version:\s*(\d+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class CatalogConfig:
    project_root: Path
    db_path: Path
    data_root: Path
    schema_version: int


def load_catalog_config(project_root: Path | None = None) -> CatalogConfig:
    root = project_root or Path(__file__).resolve().parent.parent.parent
    config_path = root / "catalog" / "config.yaml"

    db_rel = "catalog/catalog.duckdb"
    schema_version = 0
    if config_path.exists():
        text = config_path.read_text(encoding="utf-8")
        db_match = _CONFIG_DB_PATH.search(text)
        version_match = _CONFIG_SCHEMA_VERSION.search(text)
        if db_match:
            db_rel = db_match.group(1)
        if version_match:
            schema_version = int(version_match.group(1))

    return CatalogConfig(
        project_root=root,
        db_path=root / db_rel,
        data_root=root / "data",
        schema_version=schema_version,
    )


def connect_catalog(config: CatalogConfig | None = None) -> duckdb.DuckDBPyConnection:
    cfg = config or load_catalog_config()
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(cfg.db_path))
