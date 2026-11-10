"""Tests for the reusable DuckDB parquet batch writer.

The reference for every parity test is the pre-change writer, reimplemented
verbatim in ``_legacy_write_parquet_batch`` below: fresh in-memory DuckDB per
file, ``row.get(column)`` column building, typed ``INSERT ... SELECT`` and
``COPY ... COMPRESSION ZSTD`` into a ``.tmp`` file plus ``os.replace``.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pyarrow as pa

from normalizer import writer as writer_module
from normalizer.schemas import (
    CATEGORY_SPECS,
    category_column_specs,
    create_empty_table_sql,
    insert_typed_select_sql,
)
from normalizer.writer import (
    PARQUET_COMPRESSION,
    ParquetBatchWriter,
    arrow_schema,
    close_batch_writer,
    get_batch_writer,
    rows_to_arrow,
    write_parquet_batch,
)

PREDICTION_PRICE = "prediction_price"
TRADES = "trades"


def _legacy_write_parquet_batch(
    out_path: Path, rows: list[dict[str, object]], category: str
) -> int:
    """The writer as it was before the speed-up (parity reference)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    columns = tuple(name for name, _type in category_column_specs(category))
    con = duckdb.connect(":memory:")
    try:
        con.execute(create_empty_table_sql(category))
        if rows:
            con.register("rows", pa.table({c: [row.get(c) for row in rows] for c in columns}))
            con.execute(insert_typed_select_sql(category))
        con.execute(
            f"COPY normalized TO ? (FORMAT PARQUET, COMPRESSION {PARQUET_COMPRESSION})",
            [str(tmp_path)],
        )
    finally:
        con.close()

    os.replace(tmp_path, out_path)
    return len(rows)


def _price_row(index: int) -> dict[str, object]:
    return {
        "timestamp": 1_762_000_000_000 + index,
        "symbol": "BTC_15M",
        "event_type": "price_change",
        "asset_id": f"0x{index:04x}",
        "outcome": "Up" if index % 2 else "Down",
        "best_bid": 0.42 + index / 10_000,
        "best_ask": 0.45 + index / 10_000,
        "spread": 0.03,
        "price": None if index % 3 == 0 else 0.43 + index / 10_000,
        "side": "buy" if index % 2 else "sell",
        "exchange": "polymarket",
    }


def _dense_rows(count: int = 250) -> list[dict[str, object]]:
    return [_price_row(index) for index in range(count)]


def _sparse_rows() -> list[dict[str, object]]:
    """Missing columns, extra keys, ints where the contract says DOUBLE."""
    rows = _dense_rows(4)
    del rows[0]["best_ask"]
    del rows[0]["spread"]
    rows[1].pop("price")
    rows[2]["unknown_extra_column"] = "ignored"
    rows[3]["best_bid"] = 1  # int into a DOUBLE column
    rows.append(
        {
            "timestamp": 1_762_000_000_500.0,  # float into a BIGINT column
            "symbol": "BTC_15M",
            "event_type": "best_bid_ask",
            "asset_id": None,
            "outcome": None,
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "price": None,
            "side": None,
            "exchange": "polymarket",
        }
    )
    return rows


def _stringy_rows() -> list[dict[str, object]]:
    """Numeric values as strings: exercises the per-column fallback conversion."""
    rows = _dense_rows(5)
    for row in rows:
        for column, column_type in category_column_specs(PREDICTION_PRICE):
            value = row.get(column)
            if column_type != "VARCHAR" and value is not None:
                row[column] = str(value)
    return rows


def _trade_rows(count: int = 5) -> list[dict[str, object]]:
    return [
        {
            "timestamp": 1_762_000_000_000 + index,
            "symbol": "BTCUSDT",
            "agg_trade_id": index,
            "price": 68_000.5 + index,
            "amount": 0.01 * (index + 1),
            "side": "BUY",
            "exchange": "binance_futures",
        }
        for index in range(count)
    ]


class WriterTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        close_batch_writer()
        self.addCleanup(close_batch_writer)
        self._con = duckdb.connect(":memory:")
        self.addCleanup(self._con.close)

    def describe(self, path: Path) -> list[tuple[str, str]]:
        rows = self._con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        return [(name, column_type) for name, column_type, *_rest in rows]

    def count(self, path: Path) -> int:
        return self._con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]

    def except_all(self, left: Path, right: Path, column: str | None = None) -> int:
        projection = f'"{column}"' if column is not None else "*"
        return self._con.execute(
            f"SELECT count(*) FROM (SELECT {projection} FROM read_parquet(?) "
            f"EXCEPT ALL SELECT {projection} FROM read_parquet(?))",
            [str(left), str(right)],
        ).fetchone()[0]

    def assert_rows_identical(self, left: Path, right: Path) -> None:
        self.assertEqual(self.except_all(left, right), 0, f"{left.name} has rows {right.name} lacks")
        self.assertEqual(self.except_all(right, left), 0, f"{right.name} has rows {left.name} lacks")


class WriterParityTest(WriterTestBase):
    """The new writer must produce byte-for-byte equivalent batches."""

    def _assert_parity(self, rows: list[dict[str, object]], category: str) -> None:
        legacy_path = self.root / "legacy.parquet"
        new_path = self.root / "new.parquet"

        self.assertEqual(_legacy_write_parquet_batch(legacy_path, rows, category), len(rows))
        self.assertEqual(write_parquet_batch(new_path, rows, category), len(rows))

        self.assertEqual(
            self.describe(new_path),
            list(category_column_specs(category)),
            "written schema must match the category contract",
        )
        self.assertEqual(self.describe(legacy_path), self.describe(new_path))
        self.assertEqual(self.count(legacy_path), self.count(new_path))
        self.assertEqual(self.count(new_path), len(rows))
        self.assert_rows_identical(legacy_path, new_path)
        for column, _type in category_column_specs(category):
            self.assertEqual(
                self.except_all(legacy_path, new_path, column),
                0,
                f"column {column} differs from the legacy writer",
            )

    def test_parity_dense_rows(self) -> None:
        self._assert_parity(_dense_rows(), PREDICTION_PRICE)

    def test_parity_sparse_and_extra_keys(self) -> None:
        self._assert_parity(_sparse_rows(), PREDICTION_PRICE)

    def test_parity_numeric_strings_use_fallback_conversion(self) -> None:
        rows = _stringy_rows()
        self._assert_parity(rows, PREDICTION_PRICE)

    def test_parity_other_category(self) -> None:
        self._assert_parity(_trade_rows(), TRADES)

    def test_empty_batch_matches_legacy(self) -> None:
        for category in (TRADES, PREDICTION_PRICE):
            with self.subTest(category=category):
                legacy_path = self.root / f"empty_legacy_{category}.parquet"
                new_path = self.root / f"empty_new_{category}.parquet"
                self.assertEqual(
                    _legacy_write_parquet_batch(legacy_path, [], category), 0
                )
                self.assertEqual(write_parquet_batch(new_path, [], category), 0)
                self.assertTrue(new_path.exists())
                self.assertEqual(self.describe(legacy_path), self.describe(new_path))
                self.assertEqual(self.describe(new_path), list(category_column_specs(category)))
                self.assertEqual(self.count(new_path), 0)

    def test_all_categories_write_empty_batches(self) -> None:
        for category in CATEGORY_SPECS:
            with self.subTest(category=category):
                path = self.root / f"contract_{category}.parquet"
                self.assertEqual(write_parquet_batch(path, [], category), 0)
                self.assertEqual(self.describe(path), list(category_column_specs(category)))


class WriterFormatTest(WriterTestBase):
    def test_batch_is_zstd_compressed(self) -> None:
        path = self.root / "compressed.parquet"
        write_parquet_batch(path, _dense_rows(50), PREDICTION_PRICE)
        compressions = {
            row[0]
            for row in self._con.execute(
                "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]
            ).fetchall()
        }
        self.assertEqual(compressions, {PARQUET_COMPRESSION})

    def test_parent_directories_are_created(self) -> None:
        path = self.root / "data" / "prediction_price" / "polymarket" / "BTC_15M" / "01-02-03.parquet"
        write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE)
        self.assertTrue(path.exists())
        self.assertEqual(self.count(path), 3)

    def test_rewriting_same_path_replaces_contents(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(10), PREDICTION_PRICE)
        write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)
        self.assertEqual(self.count(path), 4)
        self.assertFalse(path.with_name(f"{path.name}.tmp").exists())

    def test_rows_to_arrow_uses_contract_schema(self) -> None:
        table = rows_to_arrow(_dense_rows(10), PREDICTION_PRICE)
        self.assertEqual(table.schema, arrow_schema(PREDICTION_PRICE))
        self.assertEqual(table.num_rows, 10)


    def test_numeric_strings_take_fallback_path(self) -> None:
        """Rows the fixed schema rejects must fall back to the legacy conversion."""
        rows = _stringy_rows()
        with self.assertRaises((pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError)):
            pa.Table.from_pylist(rows, schema=arrow_schema(PREDICTION_PRICE))

        table = rows_to_arrow(rows, PREDICTION_PRICE)
        columns = tuple(name for name, _type in category_column_specs(PREDICTION_PRICE))
        legacy = pa.table({column: [row.get(column) for row in rows] for column in columns})
        self.assertEqual(table.schema, legacy.schema)
        self.assertEqual(table.to_pydict(), legacy.to_pydict())


class WriterSessionTest(WriterTestBase):
    """One DuckDB session must serve many batches without leaking rows between them."""

    def test_single_session_writes_many_batches(self) -> None:
        paths = [self.root / f"batch_{index}.parquet" for index in range(3)]
        writer = ParquetBatchWriter()
        with writer:
            for index, path in enumerate(paths):
                self.assertEqual(
                    writer.write_batch(path, _dense_rows(index + 1), PREDICTION_PRICE),
                    index + 1,
                )

        for index, path in enumerate(paths):
            self.assertEqual(self.count(path), index + 1)
            self.assertEqual(
                self._con.execute(
                    "SELECT min(timestamp), max(timestamp) FROM read_parquet(?)", [str(path)]
                ).fetchone(),
                (1_762_000_000_000, 1_762_000_000_000 + index),
            )

    def test_single_session_serves_multiple_categories(self) -> None:
        first = self.root / "price_1.parquet"
        trades = self.root / "trades_1.parquet"
        second = self.root / "price_2.parquet"

        writer = ParquetBatchWriter()
        with writer:
            writer.write_batch(first, _dense_rows(2), PREDICTION_PRICE)
            writer.write_batch(trades, _trade_rows(7), TRADES)
            writer.write_batch(second, _dense_rows(3), PREDICTION_PRICE)

        self.assertEqual(self.describe(first), self.describe(second))
        self.assertEqual(self.describe(trades), list(category_column_specs(TRADES)))
        self.assertEqual((self.count(first), self.count(trades), self.count(second)), (2, 7, 3))
        self.assertEqual(
            self._con.execute(
                "SELECT count(DISTINCT symbol) FROM read_parquet(?)", [str(trades)]
            ).fetchone()[0],
            1,
        )

    def test_empty_batch_between_batches_does_not_leak_rows(self) -> None:
        writer = ParquetBatchWriter()
        with writer:
            writer.write_batch(self.root / "filled.parquet", _dense_rows(5), PREDICTION_PRICE)
            writer.write_batch(self.root / "empty.parquet", [], PREDICTION_PRICE)
            writer.write_batch(self.root / "refilled.parquet", _dense_rows(2), PREDICTION_PRICE)

        self.assertEqual(self.count(self.root / "filled.parquet"), 5)
        self.assertEqual(self.count(self.root / "empty.parquet"), 0)
        self.assertEqual(self.count(self.root / "refilled.parquet"), 2)

    def test_default_writer_is_shared_and_closable(self) -> None:
        self.assertIs(get_batch_writer(), get_batch_writer())
        writer = get_batch_writer()
        close_batch_writer()
        with self.assertRaises(RuntimeError):
            writer.write_batch(self.root / "closed.parquet", _dense_rows(1), PREDICTION_PRICE)

    def test_default_writer_is_recreated_after_close(self) -> None:
        first = get_batch_writer()
        close_batch_writer()
        self.assertIsNot(first, get_batch_writer())


class WriterAtomicityTest(WriterTestBase):
    """A failed write must leave the previously published batch untouched."""

    def test_failed_write_keeps_previous_batch(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(6), PREDICTION_PRICE)
        original = path.read_bytes()

        with mock.patch.object(writer_module.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(2), PREDICTION_PRICE)

        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(self.count(path), 6)

    def test_write_after_failure_uses_fresh_session(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(9), PREDICTION_PRICE)

        with mock.patch.object(writer_module.os, "replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                write_parquet_batch(path, _dense_rows(2), PREDICTION_PRICE)

        self.assertIsNone(writer_module._default_writer)  # discarded, not poisoned
        self.assertEqual(write_parquet_batch(path, _dense_rows(3), PREDICTION_PRICE), 3)
        self.assertEqual(self.count(path), 3)

    def test_failed_insert_does_not_publish_partial_batch(self) -> None:
        path = self.root / "batch.parquet"
        write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)
        original = path.read_bytes()

        with mock.patch.object(
            writer_module, "rows_to_arrow", side_effect=duckdb.Error("insert failed")
        ):
            with self.assertRaises(duckdb.Error):
                write_parquet_batch(path, _dense_rows(4), PREDICTION_PRICE)

        self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
