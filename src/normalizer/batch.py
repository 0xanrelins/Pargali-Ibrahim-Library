"""Normalize one raw JSONL batch into one immutable parquet batch."""

from __future__ import annotations

from pathlib import Path

from parse_polymarket import OutcomeTokenRegistry

from normalizer.parser import parse_jsonl_file
from normalizer.paths import batch_parquet_path_from_jsonl, parse_raw_jsonl_path
from normalizer.writer import write_parquet_batch


def normalize_jsonl_batch(
    jsonl_path: Path,
    *,
    project_root: Path,
    data_root: Path,
    symbol: str | None = None,
    token_registry: OutcomeTokenRegistry | None = None,
) -> tuple[Path, int]:
    """1 JSONL → parse → DuckDB write → 1 parquet. Does not update processed state."""
    parsed = parse_raw_jsonl_path(jsonl_path, project_root=project_root)
    rows = parse_jsonl_file(
        jsonl_path,
        exchange=parsed.exchange,
        category=parsed.category,
        token_registry=token_registry,
    )
    out_path = batch_parquet_path_from_jsonl(
        jsonl_path,
        project_root=project_root,
        data_root=data_root,
        symbol=symbol,
    )
    written = write_parquet_batch(out_path, rows, parsed.category)
    return out_path, written
