"""DuckDB normalizer: raw JSONL → immutable parquet batches."""

from normalizer.paths import (
    batch_parquet_path_from_jsonl,
    parse_raw_jsonl_path,
    project_paths,
    relative_to_project,
)
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

__all__ = [
    "batch_parquet_path_from_jsonl",
    "category_columns",
    "create_empty_table_sql",
    "insert_typed_select_sql",
    "load_polymarket_tokens",
    "load_processed_files",
    "normalized_categories",
    "parse_raw_jsonl_path",
    "project_paths",
    "relative_to_project",
    "save_polymarket_tokens",
    "save_processed_files",
]
