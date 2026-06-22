"""DuckDB normalizer: raw JSONL → immutable parquet batches."""

from normalizer.batch import normalize_jsonl_batch
from normalizer.parser import parse_jsonl_file
from normalizer.paths import (
    batch_parquet_path_from_jsonl,
    parse_raw_jsonl_path,
    project_paths,
    relative_to_project,
)
from normalizer.runner import collect_backlog_metrics, normalize_once
from normalizer.schemas import (
    category_columns,
    create_empty_table_sql,
    insert_typed_select_sql,
    normalized_categories,
)
from normalizer.state import (
    load_polymarket_tokens,
    load_processed_files,
    save_polymarket_tokens,
    save_processed_files,
)
from normalizer.writer import write_parquet_batch

__all__ = [
    "batch_parquet_path_from_jsonl",
    "category_columns",
    "create_empty_table_sql",
    "insert_typed_select_sql",
    "collect_backlog_metrics",
    "load_polymarket_tokens",
    "load_processed_files",
    "normalize_jsonl_batch",
    "normalize_once",
    "normalized_categories",
    "parse_jsonl_file",
    "parse_raw_jsonl_path",
    "project_paths",
    "relative_to_project",
    "save_polymarket_tokens",
    "save_processed_files",
    "write_parquet_batch",
]
