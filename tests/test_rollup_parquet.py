"""Tests for the derived rollup layer (hourly + daily) over immutable batches.

Every test builds a throw-away ``data/`` tree with the production batch writer,
runs a rollup pass against a fixed ``now`` and asserts on the produced
``data_rollup/`` tree. Nothing here touches a real data directory.

The rollup buckets rows by their parquet ``timestamp`` column, never by the
batch file name, so every fixture derives its timestamps from the UTC day/hour
it actually means to test (``ts(DAY, 9, ...)``).
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from normalizer.schemas import category_column_specs
from normalizer.writer import close_batch_writer, write_parquet_batch
from rollup_parquet import (
    DAILY_LAYER,
    DiscoveryCounters,
    DiscoveryPolicy,
    HOURLY_LAYER,
    TEMP_DIR_NAME,
    build_plan,
    daily_rollup_path,
    day_is_closed,
    discover_day_groups,
    hour_is_closed,
    hourly_rollup_path,
    hours_covered,
    main,
    prune_hourly,
    run_rollup,
    scan_day_batches,
    source_select_sql,
)

TRADES = "trades"
PREDICTION_PRICE = "prediction_price"
BINANCE = "binance_futures"
POLYMARKET = "polymarket"
BTCUSDT = "BTCUSDT"
BTC_15M = "BTC_15M"

# Fixed "now" shared by every test: 2026-11-10 12:10 UTC.
NOW = datetime(2026, 11, 10, 12, 10, tzinfo=timezone.utc)
TODAY = date(2026, 11, 10)
DAY = date(2026, 11, 8)
OLD_DAY = date(2026, 11, 1)


def ts(
    day: date,
    hour: int,
    minute: int = 0,
    second: int = 0,
    millisecond: int = 0,
) -> int:
    """Epoch milliseconds for a UTC moment on ``day``."""
    moment = datetime(
        day.year,
        day.month,
        day.day,
        hour,
        minute,
        second,
        millisecond * 1000,
        tzinfo=timezone.utc,
    )
    return int(moment.timestamp() * 1000)


def _trade_row(timestamp: int, agg_trade_id: int) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "symbol": BTCUSDT,
        "agg_trade_id": agg_trade_id,
        "price": 68_000.5 + agg_trade_id,
        "amount": 0.01 * (agg_trade_id + 1),
        "side": "BUY",
        "exchange": BINANCE,
    }


def _trade_rows(timestamp: int, count: int, *, first_id: int = 0) -> list[dict[str, object]]:
    return [_trade_row(timestamp + index, first_id + index) for index in range(count)]


def _price_row(timestamp: int, index: int) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "symbol": BTC_15M,
        "event_type": "price_change",
        "asset_id": f"0x{index:04x}",
        "outcome": "Up" if index % 2 else "Down",
        "best_bid": 0.42,
        "best_ask": 0.45,
        "spread": 0.03,
        "price": 0.43,
        "side": "buy",
        "exchange": POLYMARKET,
    }


class RollupTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = Path(self._tmpdir.name)
        self.data_root = self.root / "data"
        self.rollup_root = self.root / "data_rollup"
        close_batch_writer()
        self.addCleanup(close_batch_writer)
        self._con = duckdb.connect(":memory:")
        self.addCleanup(self._con.close)

    # -- fixtures ---------------------------------------------------------- #

    def day_dir(self, day: date = DAY, *, category: str = TRADES) -> Path:
        exchange, symbol = (
            (POLYMARKET, BTC_15M) if category.startswith("prediction_") else (BINANCE, BTCUSDT)
        )
        return self.data_root / category / exchange / symbol / day.isoformat()

    def write_batch(
        self,
        day: date,
        stamp: str,
        rows: list[dict[str, object]],
        *,
        category: str = TRADES,
    ) -> Path:
        path = self.day_dir(day, category=category) / f"{stamp}.parquet"
        write_parquet_batch(path, rows, category)
        return path

    def write_named_batch(
        self,
        day: date,
        name: str,
        rows: list[dict[str, object]],
        *,
        category: str = TRADES,
    ) -> Path:
        """Write a valid batch under an arbitrary (e.g. merged) file name."""
        path = self.day_dir(day, category=category) / name
        write_parquet_batch(path, rows, category)
        return path

    def write_empty_batch(self, day: date, stamp: str, *, category: str = TRADES) -> Path:
        """A valid parquet file with the contract schema and zero rows."""
        path = self.day_dir(day, category=category) / f"{stamp}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_batch(path, [], category)
        return path

    def reference_rows(self, *glob: str) -> list[tuple[object, ...]]:
        """Independent DuckDB grouping of the source tree by UTC hour."""
        pattern = str(self.data_root.joinpath(*glob))
        return self._con.execute(
            "SELECT hour(epoch_ms(timestamp)) AS h, count(*), sum(price) "
            "FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
            [pattern],
        ).fetchall()

    def run_pass(self, **overrides: object):
        params: dict[str, object] = {
            "data_root": self.data_root,
            "rollup_root": self.rollup_root,
            "now": NOW,
        }
        params.update(overrides)
        return run_rollup(**params)

    def hourly(self, day: date = DAY, hour: int = 9, *, category: str = TRADES) -> Path:
        exchange, symbol = (
            (POLYMARKET, BTC_15M) if category.startswith("prediction_") else (BINANCE, BTCUSDT)
        )
        return hourly_rollup_path(self.rollup_root, category, exchange, symbol, day, hour)

    def daily(self, day: date = DAY, *, category: str = TRADES) -> Path:
        exchange, symbol = (
            (POLYMARKET, BTC_15M) if category.startswith("prediction_") else (BINANCE, BTCUSDT)
        )
        return daily_rollup_path(self.rollup_root, category, exchange, symbol, day)

    def plan(self, **overrides: object):
        params: dict[str, object] = {
            "rollup_root": self.rollup_root,
            "now": NOW,
        }
        params.update(overrides)
        return build_plan(discover_day_groups(self.data_root), **params)

    # -- assertions -------------------------------------------------------- #

    def count(self, path: Path) -> int:
        return self._con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]

    def describe(self, path: Path) -> list[tuple[str, str]]:
        rows = self._con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        return [(name, column_type) for name, column_type, *_rest in rows]

    def except_all(self, left: Path, right: Path) -> int:
        return self._con.execute(
            "SELECT count(*) FROM (SELECT * FROM read_parquet(?) "
            "EXCEPT ALL SELECT * FROM read_parquet(?))",
            [str(left), str(right)],
        ).fetchone()[0]

    def assert_same_rows(self, left: Path, right: Path) -> None:
        self.assertEqual(self.except_all(left, right), 0, f"{left} has rows {right} lacks")
        self.assertEqual(self.except_all(right, left), 0, f"{right} has rows {left} lacks")

    def assert_no_temp_files(self, directory: Path) -> None:
        leftovers = [p.name for p in directory.iterdir()] if directory.is_dir() else []
        self.assertEqual([n for n in leftovers if n.startswith(".")], [], "tmp file left behind")

    def mark_stale(self, targets: Sequence[Path], sources: Sequence[Path]) -> None:
        """Age every target by an hour, then stamp the sources just after them.

        Everything stays in the past, so whatever the next pass writes (mtime
        ~now) is newer than both.
        """
        for target in targets:
            mtime = target.stat().st_mtime
            os.utime(target, (mtime - 3600, mtime - 3600))
        stamp = max(target.stat().st_mtime for target in targets) + 60
        for source in sources:
            os.utime(source, (stamp, stamp))

    def snapshot(self, root: Path) -> dict[str, tuple[int, int]]:
        return {
            path.relative_to(root).as_posix(): (path.stat().st_size, path.stat().st_mtime_ns)
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }


class HourlyRollupTest(RollupTestBase):
    def test_merges_only_the_rows_of_its_own_hour(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_batch(DAY, "09-01-10", _trade_rows(ts(DAY, 9, 1, 10), 1, first_id=2))
        self.write_batch(DAY, "10-00-05", _trade_rows(ts(DAY, 10, 0, 5), 4, first_id=3))

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(stats.hourly_failed, 0)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 3)
        self.assertEqual(self.count(self.hourly(DAY, 10)), 4)
        self.assertEqual(self.count(self.daily(DAY)), 7)
        self.assertFalse((self.rollup_root / TEMP_DIR_NAME).exists())

    def test_hour_comes_from_the_timestamp_not_the_file_name(self) -> None:
        # A batch named for 09:00 that actually carries 10:15 events belongs to
        # hour 10; the name no longer decides anything.
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 10, 15), 2))

        stats = self.run_pass()

        self.assertFalse(self.hourly(DAY, 9).exists())
        self.assertTrue(self.hourly(DAY, 10).exists())
        self.assertEqual(self.count(self.hourly(DAY, 10)), 2)
        self.assertEqual(stats.hourly_written, 1)

    def test_merged_file_spanning_two_hours_splits_rows(self) -> None:
        merged = self.write_named_batch(
            DAY,
            "09-00-00--10-59-59.parquet",
            _trade_rows(ts(DAY, 9, 15), 2)
            + _trade_rows(ts(DAY, 10, 45), 3, first_id=2),
        )

        batches = scan_day_batches(self.day_dir(), TRADES)

        self.assertEqual([batch.path for batch in batches], [merged])
        self.assertEqual(sorted(batches[0].hours()), [9, 10])
        self.assertEqual(
            (batches[0].start_ms, batches[0].end_ms),
            (ts(DAY, 9, 15), ts(DAY, 10, 45, 0, 2)),
        )

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 2)
        self.assertEqual(self.count(self.hourly(DAY, 10)), 3)
        self.assertEqual(self.count(self.daily(DAY)), 5)
        self.assertEqual(
            self.count(self.hourly(DAY, 9)) + self.count(self.hourly(DAY, 10)),
            5,
            "rows must not be duplicated across the hour split",
        )

    def test_mixed_day_of_per_file_and_merged_batches(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_named_batch(
            DAY,
            "10-00-00--11-59-59.parquet",
            _trade_rows(ts(DAY, 10, 20), 3) + _trade_rows(ts(DAY, 11, 40), 1, first_id=3),
        )

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 3)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 2)
        self.assertEqual(self.count(self.hourly(DAY, 10)), 3)
        self.assertEqual(self.count(self.hourly(DAY, 11)), 1)
        self.assertEqual(self.count(self.daily(DAY)), 6)

    def test_hourly_matches_an_independent_group_by_hour(self) -> None:
        # Late events mean file names and event hours disagree on purpose.
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 8, 59, 30), 2))
        self.write_batch(DAY, "09-59-10", _trade_rows(ts(DAY, 9, 0, 5), 3, first_id=2))
        self.write_named_batch(
            DAY,
            "10-00-00--10-59-59.parquet",
            _trade_rows(ts(DAY, 9, 30), 4, first_id=5)
            + _trade_rows(ts(DAY, 10, 30), 2, first_id=9)
            + _trade_rows(ts(DAY, 11, 0, 1), 1, first_id=11),
        )

        self.run_pass()

        expected = self.reference_rows(f"{TRADES}/{BINANCE}/{BTCUSDT}/{DAY.isoformat()}/*.parquet")
        actual = []
        for path in sorted(self.hourly(DAY, 0).parent.iterdir()):
            hour = int(path.stem)
            rows, total = self._con.execute(
                "SELECT count(*), sum(price) FROM read_parquet(?)", [str(path)]
            ).fetchone()
            actual.append((hour, rows, total))
        self.assertEqual(actual, expected)

    def test_rows_follow_the_source_batch_order(self) -> None:
        # No extra sort pass: batches are per-minute and already in event order,
        # so a rollup is near time-ordered for free.
        first = self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        second = self.write_batch(
            DAY, "09-01-10", _trade_rows(ts(DAY, 9, 1, 0), 3, first_id=2)
        )

        self.run_pass()

        expected = self._con.execute(
            "SELECT agg_trade_id FROM read_parquet(?) ORDER BY agg_trade_id",
            [str(first)],
        ).fetchall() + self._con.execute(
            "SELECT agg_trade_id FROM read_parquet(?) ORDER BY agg_trade_id",
            [str(second)],
        ).fetchall()
        written = self._con.execute(
            "SELECT agg_trade_id FROM read_parquet(?)", [str(self.hourly(DAY, 9))]
        ).fetchall()
        self.assertEqual(sorted(written), sorted(expected))
        self.assertEqual(written, expected)

    def test_parquet_is_zstd_compressed(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 3))

        self.run_pass()

        metadata = pq.ParquetFile(self.hourly(DAY, 9)).metadata
        compressions = {
            metadata.row_group(group).column(column).compression
            for group in range(metadata.num_row_groups)
            for column in range(metadata.num_columns)
        }
        self.assertEqual(compressions, {"ZSTD"})

    def test_empty_hours_are_not_written(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        self.day_dir().mkdir(parents=True, exist_ok=True)
        (self.day_dir() / "notes.txt").write_text("not a batch", encoding="utf-8")

        self.run_pass()

        self.assertTrue(self.hourly(DAY, 9).exists())
        self.assertEqual(
            sorted(p.name for p in self.hourly(DAY, 9).parent.iterdir()), ["09.parquet"]
        )

    def test_any_parquet_name_is_read_as_a_source(self) -> None:
        # Filenames are no longer decisive: an unexpected but valid parquet is
        # still rolled up by its timestamps.
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        odd = self.write_named_batch(
            DAY, "2026-11-08.parquet", _trade_rows(ts(DAY, 9, 30), 1, first_id=1)
        )

        batches = scan_day_batches(self.day_dir(), TRADES)

        self.assertIn(odd, [batch.path for batch in batches])
        self.assertEqual(len(batches), 2)

        self.run_pass()

        self.assertEqual(self.count(self.hourly(DAY, 9)), 2)

    def test_open_hour_is_skipped(self) -> None:
        # Hour 11 of today ends at 12:00 + 5 min margin == 12:05 <= now(12:10).
        self.write_batch(TODAY, "11-58-00", _trade_rows(ts(TODAY, 11, 58), 1))
        self.write_batch(TODAY, "12-00-30", _trade_rows(ts(TODAY, 12, 0, 30), 2, first_id=1))

        stats = self.run_pass()

        self.assertTrue(self.hourly(TODAY, 11).exists())
        self.assertFalse(self.hourly(TODAY, 12).exists())
        self.assertEqual(stats.hours_skipped_open, 1)
        self.assertFalse(self.daily(TODAY).exists(), "an open day must not get a daily rollup")

    def test_closed_margin_boundary(self) -> None:
        h = 11
        self.assertTrue(hour_is_closed(DAY, h, now=datetime(2026, 11, 8, 12, 5, tzinfo=timezone.utc)))
        self.assertFalse(
            hour_is_closed(DAY, h, now=datetime(2026, 11, 8, 12, 4, 59, tzinfo=timezone.utc))
        )
        self.assertTrue(day_is_closed(DAY, now=datetime(2026, 11, 9, 0, 5, tzinfo=timezone.utc)))
        self.assertFalse(
            day_is_closed(DAY, now=datetime(2026, 11, 9, 0, 4, 59, tzinfo=timezone.utc))
        )

    def test_cross_midnight_rows_stay_in_their_day_directory(self) -> None:
        # The normalizer files a flush under its own date; the first file of a
        # day can carry the previous day's last seconds. Those rows are bucketed
        # by hour-of-day inside the same day so no row is dropped or duplicated.
        self.write_batch(
            DAY,
            "00-00-30",
            [
                _trade_row(ts(DAY - timedelta(days=1), 23, 59, 30), 0),
                _trade_row(ts(DAY, 0, 0, 10), 1),
            ],
        )

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(self.count(self.hourly(DAY, 23)), 1)
        self.assertEqual(self.count(self.hourly(DAY, 0)), 1)
        self.assertEqual(self.count(self.daily(DAY)), 2)


class IdempotencyTest(RollupTestBase):
    def test_second_pass_writes_nothing(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_batch(DAY, "10-00-10", _trade_rows(ts(DAY, 10, 0, 10), 2, first_id=2))

        first = self.run_pass()
        self.assertGreater(first.hourly_written, 0)
        self.assertGreater(first.daily_written, 0)
        before = self.snapshot(self.rollup_root)

        second = self.run_pass()

        self.assertEqual(second.hourly_written, 0)
        self.assertEqual(second.daily_written, 0)
        self.assertEqual(second.hourly_rows, 0)
        self.assertEqual(second.daily_rows, 0)
        self.assertEqual(second.hours_skipped_fresh, 2)
        self.assertEqual(second.days_skipped_fresh, 1)
        self.assertEqual(self.snapshot(self.rollup_root), before, "a clean pass must not touch files")

    def test_fresh_hour_is_skipped_but_stale_hour_is_rebuilt(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.run_pass()
        self.assertEqual(self.count(self.hourly(DAY, 9)), 2)

        late = self.write_batch(DAY, "09-30-00", _trade_rows(ts(DAY, 9, 30), 3, first_id=2))
        self.mark_stale([self.hourly(DAY, 9), self.daily(DAY)], [late])

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 1)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 5)
        self.assertEqual(self.count(self.daily(DAY)), 5)

    def test_a_late_batch_rebuilds_the_hour_it_belongs_to(self) -> None:
        # The new file is named for 10:00 but only carries late 09:50 events.
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.run_pass()

        late = self.write_batch(DAY, "10-00-00", _trade_rows(ts(DAY, 9, 50), 3, first_id=2))
        self.mark_stale([self.hourly(DAY, 9), self.daily(DAY)], [late])

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 1)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 5)
        self.assertEqual(self.count(self.daily(DAY)), 5)
        self.assertFalse(self.hourly(DAY, 10).exists())

    def test_a_new_hourly_forces_the_daily_to_be_rebuilt(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.run_pass()

        # Hour 9's hourly file is missing while the daily is still fresh: the
        # daily must follow the hourly it is derived from.
        self.hourly(DAY, 9).unlink()
        stale = self.write_batch(DAY, "09-40-00", _trade_rows(ts(DAY, 9, 40), 1, first_id=2))
        self.mark_stale([self.daily(DAY)], [stale])

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 1)
        self.assertEqual(stats.daily_written, 1)
        self.assertEqual(self.count(self.daily(DAY)), 3)

    def test_dry_run_writes_nothing(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))

        stats = self.run_pass(dry_run=True)

        self.assertFalse(self.rollup_root.exists())
        self.assertEqual((stats.hourly_written, stats.daily_written), (0, 0))

        plan = self.plan()
        self.assertEqual(len(plan.hourly_tasks), 1)
        self.assertEqual(len(plan.daily_tasks), 1)

        self.run_pass()
        self.assertTrue(self.hourly(DAY, 9).exists())


class DailyRollupTest(RollupTestBase):
    def test_daily_matches_source_and_hourly_union(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 3))
        self.write_batch(DAY, "09-40-00", _trade_rows(ts(DAY, 9, 40), 2, first_id=3))
        self.write_batch(DAY, "13-20-00", _trade_rows(ts(DAY, 13, 20), 4, first_id=5))

        self.run_pass()

        self.assertEqual(self.count(self.daily(DAY)), 9)
        self.assertEqual(
            self.count(self.hourly(DAY, 9)) + self.count(self.hourly(DAY, 13)),
            self.count(self.daily(DAY)),
        )
        self.assertFalse(self.daily(TODAY).exists())

    def test_schema_parity_between_source_and_rollups(self) -> None:
        source = self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))

        self.run_pass()

        expected = list(category_column_specs(TRADES))
        self.assertEqual(self.describe(source), expected)
        self.assertEqual(self.describe(self.hourly(DAY, 9)), expected)
        self.assertEqual(self.describe(self.daily(DAY)), expected)

    def test_schema_parity_for_a_polymarket_category(self) -> None:
        rows = [_price_row(ts(DAY, 9, 0, 10) + index, index) for index in range(3)]
        source = self.write_batch(DAY, "09-00-10", rows, category=PREDICTION_PRICE)

        self.run_pass()

        expected = list(category_column_specs(PREDICTION_PRICE))
        self.assertEqual(self.describe(source), expected)
        self.assertEqual(self.describe(self.hourly(DAY, 9, category=PREDICTION_PRICE)), expected)
        self.assertEqual(self.describe(self.daily(DAY, category=PREDICTION_PRICE)), expected)
        self.assertEqual(self.count(self.daily(DAY, category=PREDICTION_PRICE)), 3)

    def test_datasets_are_kept_separate(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_batch(
            DAY,
            "09-00-10",
            [_price_row(ts(DAY, 9, 0, 10), 0)],
            category=PREDICTION_PRICE,
        )

        self.run_pass()

        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assertEqual(self.count(self.daily(DAY, category=PREDICTION_PRICE)), 1)

    def test_daily_contains_the_whole_source_day(self) -> None:
        stamps = [("00-00-30", 0), ("07-15-30", 7), ("23-59-30", 23)]
        for stamp, hour in stamps:
            self.write_batch(
                DAY,
                stamp,
                _trade_rows(ts(DAY, hour, 30), 2, first_id=hour),
            )

        self.run_pass()

        expected = self._con.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(self.day_dir() / "*.parquet")],
        ).fetchone()[0]
        self.assertEqual(self.count(self.daily(DAY)), expected)
        self.assertEqual(expected, 6)


class LayoutEquivalenceTest(RollupTestBase):
    def _seed(self, name: str, files: Sequence[tuple[str, list[dict[str, object]]]]) -> Path:
        data_root = self.root / name / "data"
        rollup_root = self.root / name / "rollup"
        day_dir = data_root / TRADES / BINANCE / BTCUSDT / DAY.isoformat()
        for file_name, rows in files:
            write_parquet_batch(day_dir / file_name, rows, TRADES)
        run_rollup(data_root=data_root, rollup_root=rollup_root, now=NOW)
        return rollup_root

    def test_per_file_and_merged_layouts_produce_the_same_rollup(self) -> None:
        early = _trade_rows(ts(DAY, 9, 0, 10), 2)
        middle = _trade_rows(ts(DAY, 9, 30), 3, first_id=2)
        late = _trade_rows(ts(DAY, 10, 15), 2, first_id=5)

        per_file = self._seed(
            "per_file",
            [
                ("09-00-10.parquet", early),
                ("09-30-00.parquet", middle),
                ("10-15-00.parquet", late),
            ],
        )
        merged = self._seed(
            "merged",
            [
                ("09-00-10--09-30-00.parquet", early + middle),
                ("10-15-00--10-15-00.parquet", late),
            ],
        )

        for hour in (9, 10):
            left = hourly_rollup_path(per_file, TRADES, BINANCE, BTCUSDT, DAY, hour)
            right = hourly_rollup_path(merged, TRADES, BINANCE, BTCUSDT, DAY, hour)
            self.assert_same_rows(left, right)
        self.assert_same_rows(
            daily_rollup_path(per_file, TRADES, BINANCE, BTCUSDT, DAY),
            daily_rollup_path(merged, TRADES, BINANCE, BTCUSDT, DAY),
        )

    def test_merged_hour_split_has_no_duplicate_rows(self) -> None:
        rows = _trade_rows(ts(DAY, 9, 50), 2) + _trade_rows(ts(DAY, 10, 10), 3, first_id=2)
        rollup_root = self._seed(
            "one_merged", [("09-50-00--10-10-00.parquet", rows)]
        )

        first = hourly_rollup_path(rollup_root, TRADES, BINANCE, BTCUSDT, DAY, 9)
        second = hourly_rollup_path(rollup_root, TRADES, BINANCE, BTCUSDT, DAY, 10)
        daily = daily_rollup_path(rollup_root, TRADES, BINANCE, BTCUSDT, DAY)

        self.assertEqual(self.count(first), 2)
        self.assertEqual(self.count(second), 3)
        self.assertEqual(self.count(daily), 5)


class BrokenSourceTest(RollupTestBase):
    def test_batch_without_a_timestamp_column_blocks_the_day(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        broken = self.day_dir() / "09-30-00.parquet"
        pq.write_table(pa.table({"price": [1.0], "symbol": ["BTCUSDT"]}), broken)

        with self.assertLogs("rollup_parquet", level="WARNING") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertEqual(stats.days_blocked, 1)
        self.assertTrue(
            any("without a 'timestamp' column" in message for message in logs.output),
            logs.output,
        )
        self.assertFalse(
            self.hourly(DAY, 9).exists(),
            "a day with a rejected source must not publish a partial hourly file",
        )
        self.assertFalse(self.daily(DAY).exists())

    def test_unreadable_batch_fails_the_pass_and_previous_targets_survive(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.run_pass()
        target = self.hourly(DAY, 9)
        before = self.count(target)

        corrupt = self.write_batch(DAY, "09-30-00", _trade_rows(ts(DAY, 9, 30), 1, first_id=9))
        corrupt.write_bytes(b"this is not a parquet file")
        self.mark_stale([target, self.daily(DAY)], [corrupt])

        with self.assertLogs("rollup_parquet", level="WARNING") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertEqual(stats.days_blocked, 1)
        self.assertEqual(stats.hourly_failed, 0)
        self.assertEqual(stats.hourly_written, 0)
        self.assertTrue(any("unreadable parquet" in message for message in logs.output))
        self.assertEqual(self.count(target), before, "the old hourly file must survive")
        self.assert_no_temp_files(target.parent)
        self.assert_no_temp_files(self.daily(DAY).parent)

    def test_day_with_only_unreadable_batches_creates_nothing(self) -> None:
        corrupt = self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        corrupt.write_bytes(b"this is not a parquet file")

        with self.assertLogs("rollup_parquet", level="WARNING"):
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertEqual(
            stats.days_blocked,
            1,
            "a day with no usable source but a rejected batch is still blocked",
        )
        self.assertEqual(stats.hourly_failed, 0)
        self.assertEqual(stats.daily_skipped_empty, 0)
        self.assertFalse(self.daily(DAY).exists(), "no source must not create an empty daily")
        self.assertFalse((self.rollup_root / TEMP_DIR_NAME).exists())

    def test_implausible_timestamps_are_skipped(self) -> None:
        # Seconds instead of milliseconds: year 1970, not silently bucketed.
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000, 2))

        with self.assertLogs("rollup_parquet", level="WARNING") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertTrue(any("implausible" in message for message in logs.output))
        self.assertFalse(self.hourly(DAY, 9).exists())

    def test_span_over_two_days_is_rejected_and_fails_the_pass(self) -> None:
        rows = [
            _trade_row(ts(DAY, 0, 0), 0),
            _trade_row(ts(DAY + timedelta(days=3), 0, 0), 1),
        ]
        self.write_batch(DAY, "00-00-00", rows)

        with self.assertLogs("rollup_parquet", level="WARNING") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertTrue(
            any("exceeds 2 days" in message for message in logs.output), logs.output
        )
        self.assertFalse(self.rollup_root.exists())

    def test_main_exits_non_zero_when_a_source_is_rejected(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        corrupt = self.write_batch(DAY, "09-30-00", _trade_rows(ts(DAY, 9, 30), 1, first_id=9))
        corrupt.write_bytes(b"this is not a parquet file")

        with self.assertLogs("rollup_parquet", level="WARNING"):
            code = main(
                [
                    "--run",
                    "--data-root",
                    str(self.data_root),
                    "--rollup-root",
                    str(self.rollup_root),
                ]
            )

        self.assertEqual(code, 1)
        self.assertFalse(self.daily(DAY).exists())

        corrupt.unlink()
        with self.assertLogs("rollup_parquet", level="INFO"):
            clean = main(
                [
                    "--run",
                    "--data-root",
                    str(self.data_root),
                    "--rollup-root",
                    str(self.rollup_root),
                ]
            )
        self.assertEqual(clean, 0)
        self.assertTrue(self.daily(DAY).exists())


class RejectedBatchKeepsTheDayTest(RollupTestBase):
    def test_rejected_source_keeps_the_existing_daily_untouched(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.run_pass()
        daily_before = self.snapshot(self.rollup_root)
        hourly_mtime = self.hourly(DAY, 9).stat().st_mtime_ns

        broken = self.write_batch(DAY, "09-30-00", _trade_rows(ts(DAY, 9, 30), 1, first_id=9))
        broken.write_bytes(b"this is not a parquet file")

        with self.assertLogs("rollup_parquet", level="WARNING"):
            stats = self.run_pass()

        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.failed, 1)
        self.assertEqual(stats.days_blocked, 1)
        self.assertEqual(stats.daily_written, 0)
        self.assertEqual(stats.hourly_written, 0)
        self.assertEqual(
            self.snapshot(self.rollup_root), daily_before, "a blocked day must not be rewritten"
        )
        self.assertEqual(self.hourly(DAY, 9).stat().st_mtime_ns, hourly_mtime)

    def test_good_and_rejected_sources_in_one_day_write_no_partial_daily(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        broken = self.day_dir() / "09-30-00.parquet"
        pq.write_table(pa.table({"price": [1.0], "symbol": ["BTCUSDT"]}), broken)

        with self.assertLogs("rollup_parquet", level="WARNING"):
            stats = self.run_pass()

        self.assertEqual(stats.days_blocked, 1)
        self.assertFalse(self.daily(DAY).exists())
        self.assertFalse(self.hourly(DAY, 9).exists())


class EmptySourceTest(RollupTestBase):
    def test_empty_batch_is_counted_but_not_skipped(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_empty_batch(DAY, "09-30-00")

        with self.assertLogs("rollup_parquet", level="DEBUG") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_empty, 1)
        self.assertEqual(stats.sources_skipped, 0)
        self.assertEqual(stats.failed, 0)
        self.assertEqual(stats.days_blocked, 0)
        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assertFalse(
            [message for message in logs.output if message.startswith("WARNING")],
            logs.output,
        )

    def test_day_with_only_an_empty_batch_writes_nothing(self) -> None:
        self.write_empty_batch(DAY, "09-00-10")

        stats = self.run_pass()

        self.assertEqual(stats.sources_empty, 1)
        self.assertEqual(stats.sources_skipped, 0)
        self.assertEqual(stats.failed, 0)
        self.assertFalse(self.rollup_root.exists())


class EmptyHourTest(RollupTestBase):
    def _sparse_merged_day(self) -> Path:
        return self.write_named_batch(
            DAY,
            "09-10-00--12-10-00.parquet",
            _trade_rows(ts(DAY, 9, 10), 1) + _trade_rows(ts(DAY, 12, 10), 1, first_id=1),
        )

    def test_sparse_span_writes_no_empty_hourly_files(self) -> None:
        self._sparse_merged_day()

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(stats.hourly_empty_skipped, 2)
        self.assertEqual(
            sorted(path.name for path in self.hourly(DAY, 9).parent.iterdir()),
            ["09.parquet", "12.parquet"],
        )
        self.assertFalse(self.hourly(DAY, 10).exists())
        self.assertFalse(self.hourly(DAY, 11).exists())
        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assert_no_temp_files(self.hourly(DAY, 9).parent)

    def test_second_pass_does_not_rewrite_the_daily_for_empty_hours(self) -> None:
        self._sparse_merged_day()
        self.run_pass()
        before = self.snapshot(self.rollup_root)

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 0)
        self.assertEqual(stats.hourly_empty_skipped, 2)
        self.assertEqual(stats.daily_written, 0)
        self.assertEqual(stats.daily_skipped_unchanged, 1)
        self.assertEqual(self.snapshot(self.rollup_root), before)


class EmptyAndRejectedDayTest(RollupTestBase):
    def test_empty_plus_rejected_day_still_counts_as_blocked(self) -> None:
        self.write_empty_batch(DAY, "09-00-00")
        corrupt = self.write_batch(DAY, "10-00-00", _trade_rows(ts(DAY, 10, 0), 1))
        corrupt.write_bytes(b"not a parquet file")

        with self.assertLogs("rollup_parquet", level="ERROR") as logs:
            stats = self.run_pass()

        self.assertEqual(stats.sources_empty, 1)
        self.assertEqual(stats.sources_skipped, 1)
        self.assertEqual(stats.days_blocked, 1)
        self.assertEqual(stats.failed, 1)
        self.assertTrue(
            any("1 day(s) blocked" in line for line in logs.output), logs.output
        )
        self.assertFalse(self.daily(DAY).exists())


class StaleHourlyTest(RollupTestBase):
    """A re-normalized batch must not leave an hour behind for the daily."""

    def test_batch_moved_to_another_hour_removes_the_old_hour(self) -> None:
        self.write_batch(DAY, "09-00-00", _trade_rows(ts(DAY, 9, 10), 3))
        self.run_pass()
        self.assertEqual(self.count(self.hourly(DAY, 9)), 3)
        self.assertEqual(self.count(self.daily(DAY)), 3)

        # Same batch path, re-normalized: its three rows now live in hour 10.
        self.write_batch(DAY, "09-00-00", _trade_rows(ts(DAY, 10, 10), 3))

        stats = self.run_pass()

        self.assertFalse(self.hourly(DAY, 9).exists())
        self.assertEqual(self.count(self.hourly(DAY, 10)), 3)
        self.assertEqual(self.count(self.daily(DAY)), 3)
        self.assertEqual(stats.hourly_stale_deleted, 1)
        self.assertEqual(stats.daily_written, 1)
        self.assert_no_temp_files(self.hourly(DAY, 10).parent)

        again = self.run_pass()
        self.assertEqual(again.hourly_stale_deleted, 0)
        self.assertEqual(again.hourly_written, 0)
        self.assertEqual(again.daily_written, 0)

    def test_backfill_removes_a_stale_hour_and_repairs_the_daily(self) -> None:
        self.write_batch(DAY, "09-00-00", _trade_rows(ts(DAY, 9, 10), 3))
        self.write_batch(DAY, "10-00-00", _trade_rows(ts(DAY, 10, 10), 2, first_id=3))
        self.run_pass()
        self.assertEqual(self.count(self.daily(DAY)), 5)

        # Simulate the regression: the hour-9 source disappears while its
        # hourly file and the 5-row daily stay behind.
        (self.day_dir() / "09-00-00.parquet").unlink()

        stats = self.run_pass(backfill=True)

        self.assertFalse(self.hourly(DAY, 9).exists())
        self.assertTrue(self.hourly(DAY, 10).exists())
        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assertEqual(stats.hourly_stale_deleted, 1)
        self.assertEqual(stats.daily_written, 1)

    def test_stale_cleanup_only_touches_hh_parquet_names(self) -> None:
        self.write_batch(DAY, "09-00-00", _trade_rows(ts(DAY, 9, 10), 1))
        self.write_batch(DAY, "10-00-00", _trade_rows(ts(DAY, 10, 10), 2, first_id=1))
        self.run_pass()
        day_dir = self.hourly(DAY, 10).parent
        hourly_bytes = self.hourly(DAY, 10).read_bytes()
        foreign = {
            "notes.parquet": hourly_bytes,
            "24.parquet": hourly_bytes,
            "0.parquet": hourly_bytes,
            "09.parquet.bak": b"backup",
            "readme.txt": b"hello",
        }
        for name, payload in foreign.items():
            (day_dir / name).write_bytes(payload)
        (self.day_dir() / "09-00-00.parquet").unlink()

        stats = self.run_pass()

        self.assertFalse(self.hourly(DAY, 9).exists())
        for name in foreign:
            self.assertTrue((day_dir / name).exists(), f"{name} must be preserved")
        self.assertEqual(
            self.count(self.daily(DAY)),
            2,
            "a foreign .parquet in the hourly dir must not leak into the daily",
        )
        self.assertEqual(stats.hourly_stale_deleted, 1)

    def test_zero_row_planned_write_removes_its_old_target(self) -> None:
        merged = "09-00-00--12-00-00.parquet"
        self.write_named_batch(
            DAY,
            merged,
            _trade_rows(ts(DAY, 9, 10), 1)
            + _trade_rows(ts(DAY, 11, 10), 2, first_id=1)
            + _trade_rows(ts(DAY, 12, 10), 1, first_id=3),
        )
        self.run_pass()
        self.assertEqual(self.count(self.hourly(DAY, 11)), 2)

        # Same span (9..12) but no row in hour 11 anymore. Hour 11 stays in
        # ``group.hours``, so this is a planned write yielding zero rows, not
        # plan-time stale detection.
        time.sleep(0.01)
        self.write_named_batch(
            DAY,
            merged,
            _trade_rows(ts(DAY, 9, 10), 1)
            + _trade_rows(ts(DAY, 12, 10), 1, first_id=1),
        )

        stats = self.run_pass()

        self.assertFalse(self.hourly(DAY, 11).exists())
        self.assertEqual(stats.hourly_stale_deleted, 0)
        self.assertEqual(stats.hourly_empty_targets_deleted, 1)
        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assert_no_temp_files(self.hourly(DAY, 9).parent)


class DiscoveryGateTest(RollupTestBase):
    def _policy(self, window_start: date = DAY, *, backfill: bool = False) -> DiscoveryPolicy:
        return DiscoveryPolicy(
            rollup_root=self.rollup_root,
            window_start=window_start,
            backfill=backfill,
        )

    def test_only_window_days_and_missing_dailies_are_scanned(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 1))
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        self.run_pass(hourly_days=1)
        self.assertTrue(self.daily(OLD_DAY).exists())
        self.assertTrue(self.daily(DAY).exists())

        counters = DiscoveryCounters()
        groups = discover_day_groups(
            self.data_root, policy=self._policy(), counters=counters
        )

        self.assertEqual([group.day for group in groups], [DAY])
        self.assertEqual(counters.days_scanned, 1)
        self.assertEqual(counters.days_gated_out, 1)
        self.assertEqual(counters.sources_scanned, 1)

    def test_missing_daily_outside_the_window_forces_a_scan(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 1))

        counters = DiscoveryCounters()
        groups = discover_day_groups(
            self.data_root, policy=self._policy(), counters=counters
        )

        self.assertEqual([group.day for group in groups], [OLD_DAY])
        self.assertEqual(counters.days_scanned, 1)
        self.assertEqual(counters.days_gated_out, 0)

    def test_late_batch_outside_the_window_invalidates_the_daily(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 2))
        self.run_pass(hourly_days=1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 2)

        late = self.write_batch(OLD_DAY, "10-30-00", _trade_rows(ts(OLD_DAY, 10, 30), 2, first_id=2))
        self.mark_stale([self.daily(OLD_DAY)], [late])

        counters = DiscoveryCounters()
        groups = discover_day_groups(
            self.data_root, policy=self._policy(), counters=counters
        )
        self.assertEqual([group.day for group in groups], [OLD_DAY])
        self.assertEqual(counters.days_scanned, 1)

        stats = self.run_pass(hourly_days=1)

        self.assertEqual(stats.daily_written, 1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 4)

    def test_late_batch_without_utime_invalidates_the_daily(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 2))
        self.run_pass(hourly_days=1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 2)

        # Only a real file write: no utime, no mark_stale. The new file bumps
        # the day directory mtime and is newer than the daily by itself.
        time.sleep(0.01)
        self.write_batch(
            OLD_DAY, "10-30-00", _trade_rows(ts(OLD_DAY, 10, 30), 2, first_id=2)
        )

        counters = DiscoveryCounters()
        groups = discover_day_groups(
            self.data_root, policy=self._policy(), counters=counters
        )
        self.assertEqual([group.day for group in groups], [OLD_DAY])
        self.assertEqual(counters.days_scanned, 1)

        stats = self.run_pass(hourly_days=1)

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(stats.daily_written, 1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 4)

    def test_backfill_scans_days_outside_the_window(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 1))
        self.run_pass(hourly_days=1)

        counters = DiscoveryCounters()
        groups = discover_day_groups(
            self.data_root, policy=self._policy(backfill=True), counters=counters
        )

        self.assertEqual([group.day for group in groups], [OLD_DAY])
        self.assertEqual(counters.days_gated_out, 0)

    def test_unchanged_tree_is_gated_out_on_the_second_pass(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 1))
        self.run_pass(hourly_days=1)

        stats = self.run_pass(hourly_days=1)

        self.assertEqual(stats.days_scanned, 0)
        self.assertEqual(stats.days_gated_out, 1)
        self.assertEqual(stats.hourly_written, 0)
        self.assertEqual(stats.daily_written, 0)
        self.assertEqual(stats.failed, 0)


class HourlyDaysPolicyTest(RollupTestBase):
    def _seed_three_days(self) -> None:
        for day in (OLD_DAY, DAY, date(2026, 11, 9)):
            self.write_batch(day, "09-00-10", _trade_rows(ts(day, 9, 0, 10), 1))

    def test_old_hourly_is_pruned_but_daily_is_kept(self) -> None:
        self._seed_three_days()

        # Window covers 2026-11-09 .. 2026-11-10 only.
        stats = self.run_pass(hourly_days=2)

        self.assertTrue(self.daily(OLD_DAY).exists())
        self.assertTrue(self.daily(DAY).exists())
        self.assertTrue(self.daily(date(2026, 11, 9)).exists())
        self.assertFalse((self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / OLD_DAY.isoformat()).exists())
        self.assertFalse((self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / DAY.isoformat()).exists())
        self.assertTrue(self.hourly(date(2026, 11, 9), 9).exists())
        self.assertEqual(stats.hourly_dirs_pruned, 2)

    def test_keep_old_hourly_flag(self) -> None:
        self._seed_three_days()

        self.run_pass(hourly_days=2, keep_hourly=True)

        self.assertTrue(self.hourly(DAY, 9).exists())
        self.assertTrue(self.hourly(OLD_DAY, 9).exists())

    def test_late_batch_after_prune_rebuilds_the_daily(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 2))
        self.write_batch(OLD_DAY, "10-00-10", _trade_rows(ts(OLD_DAY, 10, 0, 10), 1, first_id=2))
        self.run_pass(hourly_days=1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 3)
        self.assertFalse((self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / OLD_DAY.isoformat()).exists())

        late = self.write_batch(OLD_DAY, "10-30-00", _trade_rows(ts(OLD_DAY, 10, 30), 2, first_id=3))
        self.mark_stale([self.daily(OLD_DAY)], [late])

        stats = self.run_pass(hourly_days=1)

        self.assertEqual(stats.hourly_written, 2, "both hours of the day are needed again")
        self.assertEqual(stats.daily_written, 1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 5, "the rebuilt daily keeps the earlier rows")
        self.assertFalse((self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / OLD_DAY.isoformat()).exists())

    def test_hourly_days_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            self.run_pass(hourly_days=0)


class TempFileCleanupTest(RollupTestBase):
    def test_stale_temp_files_are_removed_but_live_ones_are_kept(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        day_dir = self.hourly(DAY, 9).parent
        day_dir.mkdir(parents=True, exist_ok=True)
        stale = day_dir / ".09.999999.tmp"
        stale.write_bytes(b"half written")
        old = time.time() - 7200
        os.utime(stale, (old, old))
        live = day_dir / ".09.123456.tmp"
        live.write_bytes(b"in flight")

        stats = self.run_pass()

        self.assertFalse(stale.exists(), "a leftover temp file must be cleaned up")
        self.assertTrue(live.exists(), "a recent temp file may belong to a live pass")
        self.assertEqual(stats.temp_files_cleaned, 1)
        self.assertTrue(self.hourly(DAY, 9).exists())


class PruneSafetyTest(RollupTestBase):
    def test_hourly_without_a_daily_is_kept(self) -> None:
        day_dir = self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / OLD_DAY.isoformat()
        day_dir.mkdir(parents=True)
        (day_dir / "09.parquet").write_bytes(b"placeholder")

        self.assertEqual(prune_hourly(self.rollup_root, before=DAY), [])
        self.assertTrue(day_dir.exists())

        daily = self.daily(OLD_DAY)
        daily.parent.mkdir(parents=True, exist_ok=True)
        daily.write_bytes(b"placeholder")
        self.assertEqual(prune_hourly(self.rollup_root, before=DAY, dry_run=True), [day_dir])
        self.assertTrue(day_dir.exists(), "dry-run must not delete")
        self.assertEqual(prune_hourly(self.rollup_root, before=DAY), [day_dir])
        self.assertFalse(day_dir.exists())


class BackfillTest(RollupTestBase):
    def test_backfill_rebuilds_fresh_targets(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))

        self.run_pass()
        first_mtime = self.hourly(DAY, 9).stat().st_mtime_ns
        first_daily_mtime = self.daily(DAY).stat().st_mtime_ns

        stats = self.run_pass(backfill=True)

        self.assertEqual(stats.hourly_written, 1)
        self.assertEqual(stats.daily_written, 1)
        self.assertNotEqual(self.hourly(DAY, 9).stat().st_mtime_ns, first_mtime)
        self.assertNotEqual(self.daily(DAY).stat().st_mtime_ns, first_daily_mtime)
        self.assertEqual(self.count(self.daily(DAY)), 2)

    def test_backfill_after_deleting_the_rollup_tree(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 2))
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 3))
        self.run_pass()
        shutil.rmtree(self.rollup_root)

        stats = self.run_pass(backfill=True)

        self.assertEqual(stats.daily_written, 2)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 2)
        self.assertEqual(self.count(self.daily(DAY)), 3)
        self.assertTrue((self.rollup_root / DAILY_LAYER).exists())
        self.assertTrue(self.hourly(DAY, 9).exists())


class SourceImmutabilityTest(RollupTestBase):
    def test_data_tree_is_never_modified(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 2))
        self.write_batch(DAY, "10-00-10", _trade_rows(ts(DAY, 10, 0, 10), 2, first_id=2))
        self.write_batch(TODAY, "03-00-10", _trade_rows(ts(TODAY, 3, 0, 10), 1))
        before = self.snapshot(self.data_root)

        self.run_pass()

        self.assertEqual(self.snapshot(self.data_root), before)
        self.assertTrue(self.rollup_root.exists())


class HelperTest(RollupTestBase):
    def test_source_select_requires_a_source(self) -> None:
        with self.assertRaises(ValueError):
            source_select_sql(TRADES, [])

    def test_source_select_rejects_an_out_of_range_hour(self) -> None:
        with self.assertRaises(ValueError):
            source_select_sql(TRADES, [Path("x.parquet")], hour=24)

    def test_plan_reports_open_and_fresh_days(self) -> None:
        self.write_batch(TODAY, "09-00-10", _trade_rows(ts(TODAY, 9, 0, 10), 1))
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))

        plan = self.plan()

        self.assertEqual(plan.days_skipped_open, 1)
        self.assertEqual(len(plan.daily_tasks), 1)
        self.assertEqual(len(plan.hourly_tasks), 2)

    def test_hourly_window_excludes_older_days_when_daily_is_fresh(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(ts(OLD_DAY, 9, 0, 10), 1))
        self.run_pass(hourly_days=1)

        plan = self.plan(hourly_days=1)

        self.assertEqual(plan.hourly_tasks, [])
        self.assertEqual(plan.daily_tasks, [])
        self.assertEqual(plan.days_skipped_fresh, 1)

    def test_discover_groups_ignores_unknown_categories_and_non_date_dirs(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 9, 0, 10), 1))
        stray = self.data_root / "unknown_category" / BINANCE / BTCUSDT / DAY.isoformat()
        stray.mkdir(parents=True)
        (stray / "09-00-10.parquet").write_bytes(b"stray")
        not_a_day = self.data_root / TRADES / BINANCE / BTCUSDT / "not-a-day"
        not_a_day.mkdir(parents=True)
        (not_a_day / "09-00-10.parquet").write_bytes(b"stray")

        groups = discover_day_groups(self.data_root)

        self.assertEqual([(g.category, g.day) for g in groups], [(TRADES, DAY)])
        self.assertEqual(sorted(groups[0].hours), [9])

    def test_hour_is_derived_from_the_timestamp(self) -> None:
        path = self.write_batch(DAY, "09-00-10", _trade_rows(ts(DAY, 23, 59, 59), 1))
        groups = discover_day_groups(self.data_root)
        self.assertEqual(sorted(groups[0].hours), [23])
        self.assertEqual(groups[0].hours[23][0].path, path)
        self.assertEqual(groups[0].hours[23][0].start_ms, ts(DAY, 23, 59, 59))
        self.assertEqual(groups[0].sources, groups[0].hours[23])
        self.assertEqual(groups[0].newest_mtime(), path.stat().st_mtime)

    def test_hours_covered_handles_midnight(self) -> None:
        self.assertEqual(
            hours_covered(ts(DAY, 23, 59, 59), ts(DAY + timedelta(days=1), 0, 0, 30)),
            (0, 23),
        )
        self.assertEqual(hours_covered(ts(DAY, 9, 0), ts(DAY, 9, 59, 59)), (9,))

    def test_today_boundary_uses_utc_dates(self) -> None:
        # 2026-11-09 23:30 UTC is 2026-11-10 in a +1 timezone; UTC keeps the day open.
        self.write_batch(date(2026, 11, 9), "23-30-00", _trade_rows(ts(date(2026, 11, 9), 23, 30), 1))
        early = datetime(2026, 11, 10, 0, 5, tzinfo=timezone.utc)
        late = datetime(2026, 11, 10, 0, 6, tzinfo=timezone.utc)

        self.assertFalse(day_is_closed(date(2026, 11, 9), now=early - timedelta(minutes=1)))
        self.assertTrue(day_is_closed(date(2026, 11, 9), now=late))
        self.assertFalse(self.hourly(date(2026, 11, 9), 23).exists())

        self.run_pass(now=late)

        self.assertTrue(self.daily(date(2026, 11, 9)).exists())


if __name__ == "__main__":
    unittest.main()
