"""Arrow parquet writer for immutable normalized batches."""

from __future__ import annotations

import logging
import math
import operator
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from normalizer.schemas import category_column_specs

logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 1

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

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
    ``TRY_CAST``-equivalent fallback in :func:`_coerce_value`, so uncastable
    values become nulls instead of aborting the batch. The resulting table is
    identical to the one the old DuckDB ``TRY_CAST`` + ``COPY`` writer produced.
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
    # ``pa.array`` truncates floats into an integer type instead of raising, so
    # those columns must go through the explicit TRY_CAST-equivalent rounding.
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
    """DuckDB ``TRY_CAST(value AS <target>)`` semantics: uncastable → ``None``."""
    if value is None:
        return None
    if arrow_type == pa.int64():
        return _to_int64(value)
    if arrow_type == pa.float64():
        return _to_float64(value)
    if arrow_type == pa.string():
        return _to_string(value)
    return None


def _to_int64(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value if _INT64_MIN <= value <= _INT64_MAX else None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        # DuckDB casts DOUBLE → BIGINT with round-half-to-even.
        rounded = round(value)
        return rounded if _INT64_MIN <= rounded <= _INT64_MAX else None
    if isinstance(value, str):
        return _int_from_text(value)
    return None


def _int_from_text(text: str) -> int | None:
    text = text.strip()
    if not text:
        return None
    try:
        parsed = int(text, 10)
    except ValueError:
        try:
            lowered = text.lower()
            if lowered.lstrip("+-").startswith(("0x", "0b")):
                parsed = int(text, 0)
            else:
                # DuckDB parses decimal/exponent strings for integer targets and
                # rounds halves away from zero.
                number = float(text)
                if math.isnan(number) or math.isinf(number):
                    return None
                parsed = math.floor(number + 0.5) if number >= 0 else math.ceil(number - 0.5)
        except (ValueError, OverflowError):
            return None
    return parsed if _INT64_MIN <= parsed <= _INT64_MAX else None


def _to_float64(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except OverflowError:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _to_string(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        # DuckDB renders floats with the shortest round-trip representation,
        # which is what ``repr``/``str`` produce for Python floats.
        return repr(value) if isinstance(value, float) else str(value)
    return None


class ParquetBatchWriter:
    """Write immutable parquet batches atomically.

    One parquet file per raw JSONL file: the batch is serialized to ``<path>.tmp``
    and published with ``os.replace``, so readers never observe a half-written
    file and a failed write leaves the previously published batch untouched.
    The writer holds no per-batch state; the class is kept so callers and the
    process-wide :func:`get_batch_writer` API stay unchanged.
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

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(f"{out_path.name}.tmp")
        if tmp_path.exists():
            tmp_path.unlink()

        try:
            table = rows_to_arrow(rows, category)
            pq.write_table(
                table,
                tmp_path,
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LEVEL,
            )
            os.replace(tmp_path, out_path)
        except BaseException:
            _discard_path(tmp_path)
            raise

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
