"""Arrow parquet writer for immutable normalized batches."""

from __future__ import annotations

import json
import logging
import math
import operator
import os
import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from normalizer.schemas import category_column_specs

logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 1

# Merged batches are large, so their row groups are bounded explicitly: DuckDB
# uses row-group statistics for predicate pushdown on ``timestamp``, and
# 100k rows keeps a group in the tens of MB rather than one huge group.
MERGED_ROW_GROUP_SIZE = 100_000

# Footer keys written into merged batches. ``raw_sources`` is the crash-recovery
# contract: the next turn adopts a published file whose sources are not in
# state, instead of re-normalizing those raw files into a second file.
MERGED_SOURCES_METADATA_KEY = b"normalizer.raw_sources"
MERGED_MODE_METADATA_KEY = b"normalizer.batch_mode"

# The one-off archive merge tool writes archive-relative paths under its own
# key, so the normalizer's crash recovery never mistakes an archive output for
# a project batch (``normalizer.merged.recover_orphaned_merged_batches``).
ARCHIVE_MERGE_SOURCES_METADATA_KEY = b"archive.merge_sources"

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_INT64_MAX_PLUS_ONE = Decimal(_INT64_MAX) + 1
_INT64_MIN_MINUS_ONE = Decimal(_INT64_MIN) - 1

# ASCII decimal / exponent grammar only: no hex, no binary, no Unicode digits
# and no PEP 515 underscores. DuckDB's VARCHAR cast accepts some of those, but
# the parsers only ever produce plain decimal text, and accepting less is the
# safe direction (see ``_int_from_text``).
_NUMBER_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_SPECIAL_FLOAT_TEXT = frozenset(
    {"inf", "+inf", "-inf", "infinity", "+infinity", "-infinity", "nan", "+nan", "-nan"}
)

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
    """Convert parsed row dicts into an Arrow table with the contract schema.

    Each column is built with a single ``pa.array`` call against the contract
    type: the common case (values already carry the parsed Python type) is one
    C++ pass with no per-row Python branching. Values the contract type rejects
    (numeric strings, floats in integer columns, ...) take the per-value
    fallback in :func:`_coerce_value`, which reproduces the old writer's
    ``TRY_CAST`` results, including its float→BIGINT truncation and its
    ``OverflowError`` for Python ints outside int64.

    String coercion is deliberately stricter than DuckDB's ``VARCHAR`` cast:
    only ASCII signed decimal/exponent text is accepted, so hex/binary text,
    Unicode digits and underscore separators become NULL. The normalizer's
    parsers do not emit those forms.
    """
    schema = arrow_schema(category)
    arrays = [
        _column_array(_column_values(rows, field.name), field) for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def _column_values(rows: list[dict[str, object]], name: str) -> list[object]:
    """Extract one column; ``itemgetter`` walks complete rows in C."""
    try:
        return list(map(operator.itemgetter(name), rows))
    except KeyError:
        # Sparse rows (a contract column missing) use ``dict.get`` so the
        # missing value becomes NULL, exactly as before.
        return [row.get(name) for row in rows]


def _column_array(values: list[object], field: pa.Field) -> pa.Array:
    # ``pa.array`` truncates floats into an integer type, which matches the old
    # Arrow fast path, but it does not reject every value the old writer
    # rejected. Integer columns containing floats therefore go through the
    # explicit per-value conversion, and a rejected bulk conversion falls back
    # to it as well (big Python ints raise ``OverflowError`` there).
    if field.type == pa.int64() and any(isinstance(value, float) for value in values):
        return _coerced_array(values, field)
    try:
        return pa.array(values, type=field.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
        logger.debug(
            "Arrow bulk conversion rejected column %s for %s rows; coercing per value",
            field.name,
            len(values),
            exc_info=True,
        )
    return _coerced_array(values, field)


def _coerced_array(values: list[object], field: pa.Field) -> pa.Array:
    return pa.array([_coerce_value(value, field.type) for value in values], type=field.type)


def _coerce_value(value: object, arrow_type: pa.DataType) -> object:
    """Old-writer casting semantics: uncastable values become ``None``."""
    if value is None:
        return None
    if arrow_type == pa.int64():
        return _to_int64(value)
    if arrow_type == pa.float64():
        return _to_float64(value)
    if arrow_type == pa.string():
        return _to_string(value)
    return None


def _checked_int(value: int) -> int:
    """Arrow's Python-int conversion only accepts int64; the old writer failed otherwise."""
    if _INT64_MIN <= value <= _INT64_MAX:
        return value
    raise OverflowError("Python int too large to convert to C long")


def _to_int64(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return _checked_int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        # The old Arrow ``from_pylist`` fast path truncated floats toward zero
        # before DuckDB ever saw them (2.5 → 2, -2.5 → -2). Values outside
        # int64 fall through to NULL, as DuckDB's ``TRY_CAST`` did.
        truncated = math.trunc(value)
        return truncated if _INT64_MIN <= truncated <= _INT64_MAX else None
    if isinstance(value, str):
        return _int_from_text(value)
    return None


def _int_from_text(text: str) -> int | None:
    text = text.strip()
    if _NUMBER_TEXT.fullmatch(text) is None:
        return None
    try:
        if "." not in text and "e" not in text and "E" not in text:
            parsed = int(text, 10)
        else:
            # DuckDB parses decimal/exponent strings (``"12.5"`` → 13) and
            # rounds halves away from zero. Decimal keeps that exact for
            # values near the int64 boundary.
            number = Decimal(text)
            if number >= _INT64_MAX_PLUS_ONE or number <= _INT64_MIN_MINUS_ONE:
                return None
            parsed = int(number.to_integral_value(rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return None
    return parsed if _INT64_MIN <= parsed <= _INT64_MAX else None


def _to_float64(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int):
        return float(_checked_int(value))
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        text = value.strip()
        if _NUMBER_TEXT.fullmatch(text) is not None:
            try:
                return float(text)
            except ValueError:
                return None
        if text.lower() in _SPECIAL_FLOAT_TEXT:
            return float(text)
        return None
    return None


def _to_string(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(_checked_int(value))
    if isinstance(value, float):
        # DuckDB renders floats with the shortest round-trip representation,
        # which is what ``repr``/``str`` produce for Python floats.
        return repr(value)
    return None


class ParquetBatchWriter:
    """Write immutable parquet batches atomically.

    One parquet file per raw JSONL file: the batch is serialized to ``<path>.tmp``
    and published with ``os.replace``, so readers never observe a half-written
    file and a failed write leaves the previously published batch untouched.
    The writer holds no per-batch state; the class is kept so callers and the
    process-wide :func:`get_batch_writer` API stay unchanged.

    Schema and values match the old DuckDB writer for every value the parsers
    produce. A Python int outside int64 raises ``OverflowError`` (the old
    Arrow fast path failed the same way), so the batch is not published and
    the runner does not mark the raw file processed.
    """

    def __init__(self) -> None:
        self._closed = False

    def write_batch(
        self,
        out_path: Path,
        rows: list[dict[str, object]],
        category: str,
    ) -> int:
        """Write one immutable parquet batch. Creates parent dirs; tmp + atomic rename."""
        self._require_open()

        table = rows_to_arrow(rows, category)
        _write_table_atomic(table, out_path)

        logger.debug("Wrote parquet batch: %s (%s rows)", out_path, len(rows))
        return len(rows)

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> "ParquetBatchWriter":
        self._require_open()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("ParquetBatchWriter is closed")


def write_merged_parquet_batch(
    out_path: Path,
    tables: list[pa.Table],
    category: str,
    *,
    source_paths: list[str],
    row_group_size: int = MERGED_ROW_GROUP_SIZE,
) -> int:
    """Publish one merged parquet batch atomically.

    ``tables`` are per-raw-file tables produced by :func:`rows_to_arrow`, in raw
    order; concatenation keeps that order and the contract schema. The footer
    records the raw source paths (crash recovery) and the batch mode. An empty
    batch still publishes a valid, schema-only parquet file, matching the
    per-file writer's behavior for empty raw files.
    """
    if not source_paths:
        raise ValueError("A merged batch needs at least one source path")

    table = pa.concat_tables(tables) if tables else arrow_schema(category).empty_table()
    metadata = dict(table.schema.metadata or {})
    metadata[MERGED_SOURCES_METADATA_KEY] = json.dumps(sorted(source_paths)).encode("utf-8")
    metadata[MERGED_MODE_METADATA_KEY] = b"merged"
    table = table.replace_schema_metadata(metadata)

    # The parquet file and its footer are written before the state file, so a
    # crash in between leaves a complete published batch behind (see
    # ``normalizer.merged.recover_orphaned_merged_batches``).
    _write_table_atomic(table, out_path, row_group_size=row_group_size)
    logger.debug(
        "Wrote merged parquet batch: %s (%s rows from %s raw files)",
        out_path,
        table.num_rows,
        len(source_paths),
    )
    return table.num_rows


def _write_table_atomic(
    table: pa.Table,
    out_path: Path,
    *,
    row_group_size: int | None = None,
) -> None:
    """Write ``table`` to a sibling tmp file, then ``os.replace`` onto ``out_path``."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    write_kwargs: dict[str, object] = {}
    if row_group_size is not None:
        write_kwargs["row_group_size"] = row_group_size

    try:
        pq.write_table(
            table,
            tmp_path,
            compression=PARQUET_COMPRESSION,
            compression_level=PARQUET_COMPRESSION_LEVEL,
            **write_kwargs,
        )
        os.replace(tmp_path, out_path)
    except BaseException:
        _discard_path(tmp_path)
        raise


def _discard_path(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - best effort cleanup
        logger.debug("Could not remove temporary file %s", path, exc_info=True)


_default_writer: ParquetBatchWriter | None = None


def get_batch_writer() -> ParquetBatchWriter:
    """Process-wide writer shared by every batch in a normalizer run."""
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
    """Write one immutable parquet batch through the shared writer."""
    writer = get_batch_writer()
    try:
        return writer.write_batch(out_path, rows, category)
    except Exception:
        # One failed write must not poison every later batch in this run.
        _discard_default_writer(writer)
        raise


def _discard_default_writer(writer: ParquetBatchWriter) -> None:
    global _default_writer
    if _default_writer is writer:
        _default_writer = None
    try:
        writer.close()
    except Exception:  # pragma: no cover - close is best effort
        logger.debug("Failed to close unusable batch writer", exc_info=True)
