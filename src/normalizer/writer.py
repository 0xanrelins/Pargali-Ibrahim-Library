"""DuckDB in-memory writer for immutable normalized parquet batches."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pyarrow as pa

from normalizer.schemas import (
    category_columns,
    create_empty_table_sql,
    insert_typed_select_sql,
)

logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "ZSTD"


def write_parquet_batch(out_path: Path, rows: list[dict[str, object]], category: str) -> int:
    """Write one immutable parquet batch. Creates parent dirs; uses tmp + atomic rename."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")

    if tmp_path.exists():
        tmp_path.unlink()

    columns = category_columns(category)
    con = duckdb.connect(":memory:")
    try:
        con.execute(create_empty_table_sql(category))
        if rows:
            table = _rows_to_arrow(rows, columns)
            con.register("rows", table)
            con.execute(insert_typed_select_sql(category))

        copy_sql = (
            "COPY normalized TO ? (FORMAT PARQUET, COMPRESSION "
            f"{PARQUET_COMPRESSION})"
        )
        con.execute(copy_sql, [str(tmp_path)])
    finally:
        con.close()

    os.replace(tmp_path, out_path)
    logger.debug("Wrote parquet batch: %s (%s rows)", out_path, len(rows))
    return len(rows)


def _rows_to_arrow(rows: list[dict[str, object]], columns: tuple[str, ...]) -> pa.Table:
    data = {column: [row.get(column) for row in rows] for column in columns}
    return pa.table(data)
