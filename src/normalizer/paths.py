"""Map raw JSONL batch paths to immutable normalized parquet batch paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datasets import (
    BINANCE_FUTURES,
    BINANCE_FUTURES_SYMBOL,
    POLYMARKET,
    POLYMARKET_SYMBOL,
    parquet_batch_path,
)

RAW_PREFIX = "raw"


@dataclass(frozen=True)
class RawJsonlPath:
    """Parsed layout: raw/{exchange}/{category}/{date}/{batch_stamp}.jsonl."""

    exchange: str
    category: str
    date: str
    batch_stamp: str

    @property
    def batch_filename(self) -> str:
        return f"{self.batch_stamp}.parquet"


def project_paths() -> tuple[Path, Path, Path, Path]:
    """Return (project_root, data_root, processed_state_path, polymarket_tokens_path)."""
    project_root = Path(__file__).resolve().parent.parent.parent
    return (
        project_root,
        project_root / "data",
        project_root / "state" / "processed.json",
        project_root / "state" / "polymarket_tokens.json",
    )


def relative_to_project(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def default_symbol_for_exchange(exchange: str) -> str:
    if exchange == BINANCE_FUTURES:
        return BINANCE_FUTURES_SYMBOL
    if exchange == POLYMARKET:
        return POLYMARKET_SYMBOL
    raise ValueError(f"No default symbol for exchange {exchange!r}")


def parse_raw_jsonl_path(jsonl_path: Path, *, project_root: Path) -> RawJsonlPath:
    rel = relative_to_project(jsonl_path, project_root)
    parts = rel.split("/")
    if len(parts) != 5:
        raise ValueError(f"Expected raw/{{exchange}}/{{category}}/{{date}}/{{batch}}.jsonl, got {rel!r}")
    prefix, exchange, category, date, filename = parts
    if prefix != RAW_PREFIX:
        raise ValueError(f"Raw JSONL path must start with {RAW_PREFIX}/, got {rel!r}")
    if not filename.endswith(".jsonl"):
        raise ValueError(f"Expected .jsonl file, got {filename!r}")

    batch_stamp = filename[: -len(".jsonl")]
    if not batch_stamp:
        raise ValueError(f"Missing batch stamp in {rel!r}")

    return RawJsonlPath(
        exchange=exchange,
        category=category,
        date=date,
        batch_stamp=batch_stamp,
    )


def batch_parquet_path_from_jsonl(
    jsonl_path: Path,
    *,
    project_root: Path,
    data_root: Path,
    symbol: str | None = None,
) -> Path:
    """1:1 mapping from a raw JSONL batch file to its normalized parquet batch path."""
    parsed = parse_raw_jsonl_path(jsonl_path, project_root=project_root)
    resolved_symbol = symbol or default_symbol_for_exchange(parsed.exchange)
    return parquet_batch_path(
        data_root,
        parsed.exchange,
        parsed.category,
        resolved_symbol,
        parsed.date,
        parsed.batch_stamp,
    )
