"""DuckDB in-memory writer for immutable normalized parquet batches."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pyarrow as pa

from normalizer.schemas import (
    category_column_specs,
    category_columns,
    create_empty_table_sql,
    insert_typed_select_sql,
)

logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "ZSTD"
ROWS_TABLE = "rows"
NORMALIZED_TABLE = "normalized"

# DuckDB contract type (``normalizer.schemas``) → Arrow type for the bulk conversion.
_ARROW_TYPES: dict[str, pa.DataType] = {
    "BIGINT": pa.int64(),
    "DOUBLE": pa.float64(),
    "VARCHAR": pa.string(),
}
_ARROW_SCHEMAS: dict[str, pa.Schema] = {}


def arrow_schema(category: str) -> pa.Schema:
    """Arrow schema mirroring the typed DuckDB table for ``category``."""
    cached = _ARROW_SCHEMAS.get(category)
    if cached is not None:
        return cached

    fields: list[pa.Field] = []
    for name, column_type in category_column_specs(category):
        try:
            arrow_type = _ARROW_TYPES[column_type]
        except KeyError as exc:
            raise ValueError(
                f"No Arrow type for DuckDB type {column_type!r} "
                f"(column {name!r}, category {category!r})"
            ) from exc
        fields.append(pa.field(name, arrow_type))
    schema = pa.schema(fields)
    _ARROW_SCHEMAS[category] = schema
    return schema


def rows_to_arrow(rows: list[dict[str, object]], category: str) -> pa.Table:
    """Convert parsed row dicts into an Arrow table shaped like the contract.

    ``pa.Table.from_pylist`` walks the rows once in C++ against a fixed schema,
    instead of building one Python list per column and letting Arrow infer every
    column type. Rows that do not fit the fixed schema (numeric strings, bools,
    ...) fall back to that inference-based conversion, so DuckDB's ``TRY_CAST``
    receives exactly the same input as before and the written batches are
    unchanged.
    """
    try:
        return pa.Table.from_pylist(rows, schema=arrow_schema(category))
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        logger.debug(
            "Arrow schema fast path rejected %s rows for %s; using per-column conversion",
            len(rows),
            category,
            exc_info=True,
        )
        return _rows_to_arrow_per_column(rows, category_columns(category))


def _rows_to_arrow_per_column(
    rows: list[dict[str, object]], columns: tuple[str, ...]
) -> pa.Table:
    """Previous conversion path: one inferred Arrow array per column."""
    data = {column: [row.get(column) for row in rows] for column in columns}
    return pa.table(data)


class ParquetBatchWriter:
    """Reusable DuckDB session that writes immutable parquet batches.

    A normalizer batch produces one parquet file per raw JSONL file and opening
    a fresh in-memory DuckDB costs ~0.1 s (measured) per file, so the session is
    created once and reused for the whole run. Each batch still rebuilds its
    category table,
    re-inserts through the typed ``INSERT ... SELECT`` contract and ``COPY``s
    into ``<path>.tmp`` before ``os.replace`` publishes it: batches stay
    atomic, never appended, and the in-memory table stays bounded. Recreating
    beats ``DELETE`` here because DuckDB keeps deleted rows resident: 30 batches
    of 35k rows left 51 MB allocated after ``DELETE`` vs 11 MB after ``DROP``.
    """

    def __init__(self) -> None:
        self._con: duckdb.DuckDBPyConnection | None = duckdb.connect(":memory:")

    def write_batch(
        self,
        out_path: Path,
        rows: list[dict[str, object]],
        category: str,
    ) -> int:
        """Write one immutable parquet batch. Creates parent dirs; tmp + atomic rename."""
        con = self._require_connection()
        table_name = _normalized_table_name(category)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(f"{out_path.name}.tmp")
        if tmp_path.exists():
            tmp_path.unlink()

        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(create_empty_table_sql(category, table_name=table_name))
        if rows:
            con.register(ROWS_TABLE, rows_to_arrow(rows, category))
            try:
                con.execute(
                    insert_typed_select_sql(
                        category,
                        source_table=ROWS_TABLE,
                        target_table=table_name,
                    )
                )
            finally:
                self._unregister_rows()

        con.execute(
            f"COPY {table_name} TO ? (FORMAT PARQUET, COMPRESSION {PARQUET_COMPRESSION})",
            [str(tmp_path)],
        )
        os.replace(tmp_path, out_path)
        logger.debug("Wrote parquet batch: %s (%s rows)", out_path, len(rows))
        return len(rows)

    def close(self) -> None:
        con, self._con = self._con, None
        if con is not None:
            con.close()

    def __enter__(self) -> "ParquetBatchWriter":
        self._require_connection()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def _unregister_rows(self) -> None:
        con = self._con
        if con is None:
            return
        try:
            con.unregister(ROWS_TABLE)
        except Exception:  # pragma: no cover - registration never partially fails
            logger.debug("Could not unregister %r", ROWS_TABLE, exc_info=True)

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("ParquetBatchWriter is closed")
        return self._con


def _normalized_table_name(category: str) -> str:
    return f"{NORMALIZED_TABLE}_{category}"


_default_writer: ParquetBatchWriter | None = None


def get_batch_writer() -> ParquetBatchWriter:
    """Process-wide writer, so a normalizer run pays DuckDB startup only once."""
    global _default_writer
    if _default_writer is None:
        _default_writer = ParquetBatchWriter()
    return _default_writer


def close_batch_writer() -> None:
    """Close the process-wide writer (shutdown, tests)."""
    global _default_writer
    writer, _default_writer = _default_writer, None
    if writer is not None:
        writer.close()


def write_parquet_batch(out_path: Path, rows: list[dict[str, object]], category: str) -> int:
    """Write one immutable parquet batch through the shared session."""
    writer = get_batch_writer()
    try:
        return writer.write_batch(out_path, rows, category)
    except Exception:
        # One failed statement must not poison every later batch in this run.
        _discard_default_writer(writer)
        raise


def _discard_default_writer(writer: ParquetBatchWriter) -> None:
    global _default_writer
    if _default_writer is writer:
        _default_writer = None
    try:
        writer.close()
    except Exception:  # pragma: no cover - close on a broken connection is best effort
        logger.debug("Failed to close unusable batch writer", exc_info=True)
