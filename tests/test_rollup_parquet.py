"""Tests for the derived rollup layer (hourly + daily) over immutable batches.

Every test builds a throw-away ``data/`` tree with the production batch writer,
runs a rollup pass against a fixed ``now`` and asserts on the produced
``data_rollup/`` tree. Nothing here touches a real data directory.
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
from pyarrow import parquet as pq

from normalizer.schemas import category_column_specs
from normalizer.writer import close_batch_writer, write_parquet_batch
from rollup_parquet import (
    DAILY_LAYER,
    HOURLY_LAYER,
    TEMP_DIR_NAME,
    build_plan,
    daily_rollup_path,
    day_is_closed,
    discover_day_groups,
    hour_is_closed,
    hourly_rollup_path,
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

    def write_batch(
        self,
        day: date,
        stamp: str,
        rows: list[dict[str, object]],
        *,
        category: str = TRADES,
    ) -> Path:
        exchange, symbol = (
            (POLYMARKET, BTC_15M) if category.startswith("prediction_") else (BINANCE, BTCUSDT)
        )
        path = (
            self.data_root
            / category
            / exchange
            / symbol
            / day.isoformat()
            / f"{stamp}.parquet"
        )
        write_parquet_batch(path, rows, category)
        return path

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
    def test_merges_only_the_batches_of_its_own_hour(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(DAY, "09-01-10", _trade_rows(1_762_000_100_000, 1, first_id=2))
        self.write_batch(DAY, "10-00-05", _trade_rows(1_762_003_600_000, 4, first_id=3))

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 2)
        self.assertEqual(stats.hourly_failed, 0)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 3)
        self.assertEqual(self.count(self.hourly(DAY, 10)), 4)
        self.assertEqual(self.count(self.daily(DAY)), 7)
        self.assertFalse((self.rollup_root / TEMP_DIR_NAME).exists())

    def test_rows_follow_the_source_batch_order(self) -> None:
        # No extra sort pass: batches are per-minute and already in event order,
        # so a rollup is near time-ordered for free.
        first = self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        second = self.write_batch(DAY, "09-01-10", _trade_rows(1_762_000_060_000, 3, first_id=2))

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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 3))

        self.run_pass()

        metadata = pq.ParquetFile(self.hourly(DAY, 9)).metadata
        compressions = {
            metadata.row_group(group).column(column).compression
            for group in range(metadata.num_row_groups)
            for column in range(metadata.num_columns)
        }
        self.assertEqual(compressions, {"ZSTD"})

    def test_empty_hours_are_not_written(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
        (self.data_root / TRADES / BINANCE / BTCUSDT / DAY.isoformat()).mkdir(
            parents=True, exist_ok=True
        )
        # An hour directory exists for hour 11 only as a non-batch file.
        (self.data_root / TRADES / BINANCE / BTCUSDT / DAY.isoformat() / "notes.txt").write_text(
            "not a batch", encoding="utf-8"
        )

        self.run_pass()

        self.assertTrue(self.hourly(DAY, 9).exists())
        self.assertEqual(
            sorted(p.name for p in self.hourly(DAY, 9).parent.iterdir()), ["09.parquet"]
        )

    def test_non_batch_parquet_names_are_ignored(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
        day_dir = self.data_root / TRADES / BINANCE / BTCUSDT / DAY.isoformat()
        (day_dir / "2026-11-08.parquet").write_bytes(b"legacy daily rollup, ignored")

        hours = scan_day_batches(day_dir)

        self.assertEqual(sorted(hours), [9])
        self.assertEqual(len(hours[9]), 1)

    def test_open_hour_is_skipped(self) -> None:
        # Hour 11 of today ends at 12:00 + 5 min margin == 12:05 <= now(12:10).
        self.write_batch(TODAY, "11-58-00", _trade_rows(1_762_774_680_000, 1))
        self.write_batch(TODAY, "12-00-30", _trade_rows(1_762_776_000_000, 2, first_id=1))

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


class IdempotencyTest(RollupTestBase):
    def test_second_pass_writes_nothing(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(DAY, "10-00-10", _trade_rows(1_762_003_600_000, 2, first_id=2))

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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.run_pass()
        self.assertEqual(self.count(self.hourly(DAY, 9)), 2)

        late = self.write_batch(DAY, "09-30-00", _trade_rows(1_762_001_800_000, 3, first_id=2))
        self.mark_stale([self.hourly(DAY, 9), self.daily(DAY)], [late])

        stats = self.run_pass()

        self.assertEqual(stats.hourly_written, 1)
        self.assertEqual(self.count(self.hourly(DAY, 9)), 5)
        self.assertEqual(self.count(self.daily(DAY)), 5)

    def test_dry_run_writes_nothing(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))

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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 3))
        self.write_batch(DAY, "09-40-00", _trade_rows(1_762_002_400_000, 2, first_id=3))
        self.write_batch(DAY, "13-20-00", _trade_rows(1_762_015_200_000, 4, first_id=5))

        self.run_pass()

        self.assertEqual(self.count(self.daily(DAY)), 9)
        self.assertEqual(
            self.count(self.hourly(DAY, 9)) + self.count(self.hourly(DAY, 13)),
            self.count(self.daily(DAY)),
        )
        self.assertFalse(self.daily(TODAY).exists())

    def test_schema_parity_between_source_and_rollups(self) -> None:
        source = self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))

        self.run_pass()

        expected = list(category_column_specs(TRADES))
        self.assertEqual(self.describe(source), expected)
        self.assertEqual(self.describe(self.hourly(DAY, 9)), expected)
        self.assertEqual(self.describe(self.daily(DAY)), expected)

    def test_schema_parity_for_a_polymarket_category(self) -> None:
        rows = [_price_row(1_762_000_000_000 + index, index) for index in range(3)]
        source = self.write_batch(DAY, "09-00-10", rows, category=PREDICTION_PRICE)

        self.run_pass()

        expected = list(category_column_specs(PREDICTION_PRICE))
        self.assertEqual(self.describe(source), expected)
        self.assertEqual(self.describe(self.hourly(DAY, 9, category=PREDICTION_PRICE)), expected)
        self.assertEqual(self.describe(self.daily(DAY, category=PREDICTION_PRICE)), expected)
        self.assertEqual(self.count(self.daily(DAY, category=PREDICTION_PRICE)), 3)

    def test_datasets_are_kept_separate(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(
            DAY,
            "09-00-10",
            [_price_row(1_762_000_000_000, 0)],
            category=PREDICTION_PRICE,
        )

        self.run_pass()

        self.assertEqual(self.count(self.daily(DAY)), 2)
        self.assertEqual(self.count(self.daily(DAY, category=PREDICTION_PRICE)), 1)

    def test_daily_contains_the_whole_source_day(self) -> None:
        stamps = [("00-00-30", 0), ("07-15-30", 2), ("23-59-30", 5)]
        for stamp, count in stamps:
            hour = int(stamp[:2])
            self.write_batch(
                DAY,
                stamp,
                _trade_rows(1_762_000_000_000 + hour * 3_600_000, count, first_id=hour),
            )

        self.run_pass()

        expected = self._con.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(self.data_root / TRADES / BINANCE / BTCUSDT / DAY.isoformat() / "*.parquet")],
        ).fetchone()[0]
        self.assertEqual(self.count(self.daily(DAY)), expected)


class AtomicityTest(RollupTestBase):
    def test_failed_rebuild_keeps_the_previous_target(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.run_pass()
        target = self.hourly(DAY, 9)
        before = self.count(target)

        corrupt = self.write_batch(DAY, "09-30-00", _trade_rows(1_762_001_800_000, 1, first_id=9))
        corrupt.write_bytes(b"this is not a parquet file")
        self.mark_stale([target, self.daily(DAY)], [corrupt])

        stats = self.run_pass()

        self.assertEqual(stats.hourly_failed, 1)
        self.assertEqual(stats.hourly_written, 0)
        self.assertEqual(self.count(target), before, "the old hourly file must survive a failed write")
        self.assert_no_temp_files(target.parent)
        self.assert_no_temp_files(self.daily(DAY).parent)

    def test_daily_without_hourly_sources_creates_no_file(self) -> None:
        corrupt = self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
        corrupt.write_bytes(b"this is not a parquet file")

        stats = self.run_pass()

        self.assertEqual(stats.hourly_failed, 1)
        self.assertEqual(stats.daily_skipped_empty, 1)
        self.assertFalse(self.daily(DAY).exists(), "no hourly input must not create an empty daily")
        self.assertFalse((self.rollup_root / TEMP_DIR_NAME).exists())


class HourlyDaysPolicyTest(RollupTestBase):
    def _seed_three_days(self) -> None:
        for day, day_offset in ((OLD_DAY, 0), (DAY, 6), (date(2026, 11, 9), 7)):
            self.write_batch(day, "09-00-10", _trade_rows(1_762_000_000_000 + day_offset * 86_400_000, 1))

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
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(OLD_DAY, "10-00-10", _trade_rows(1_762_003_600_000, 1, first_id=2))
        self.run_pass(hourly_days=1)
        self.assertEqual(self.count(self.daily(OLD_DAY)), 3)
        self.assertFalse((self.rollup_root / HOURLY_LAYER / TRADES / BINANCE / BTCUSDT / OLD_DAY.isoformat()).exists())

        late = self.write_batch(OLD_DAY, "10-30-00", _trade_rows(1_762_005_400_000, 2, first_id=3))
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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))

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
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_600_000_000, 3))
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
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 2))
        self.write_batch(DAY, "10-00-10", _trade_rows(1_762_003_600_000, 2, first_id=2))
        self.write_batch(TODAY, "03-00-10", _trade_rows(1_762_750_800_000, 1))
        before = self.snapshot(self.data_root)

        self.run_pass()

        self.assertEqual(self.snapshot(self.data_root), before)
        self.assertTrue(self.rollup_root.exists())


class HelperTest(RollupTestBase):
    def test_source_select_requires_a_source(self) -> None:
        with self.assertRaises(ValueError):
            source_select_sql(TRADES, [])

    def test_plan_reports_open_and_fresh_days(self) -> None:
        self.write_batch(TODAY, "09-00-10", _trade_rows(1_762_750_800_000, 1))
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))

        plan = self.plan()

        self.assertEqual(plan.days_skipped_open, 1)
        self.assertEqual(len(plan.daily_tasks), 1)
        self.assertEqual(len(plan.hourly_tasks), 2)

    def test_hourly_window_excludes_older_days_when_daily_is_fresh(self) -> None:
        self.write_batch(OLD_DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
        self.run_pass(hourly_days=1)

        plan = self.plan(hourly_days=1)

        self.assertEqual(plan.hourly_tasks, [])
        self.assertEqual(plan.daily_tasks, [])
        self.assertEqual(plan.days_skipped_fresh, 1)

    def test_discover_groups_ignores_unknown_categories_and_non_date_dirs(self) -> None:
        self.write_batch(DAY, "09-00-10", _trade_rows(1_762_000_000_000, 1))
        stray = self.data_root / "unknown_category" / BINANCE / BTCUSDT / DAY.isoformat()
        stray.mkdir(parents=True)
        (stray / "09-00-10.parquet").write_bytes(b"stray")
        not_a_day = self.data_root / TRADES / BINANCE / BTCUSDT / "not-a-day"
        not_a_day.mkdir(parents=True)
        (not_a_day / "09-00-10.parquet").write_bytes(b"stray")

        groups = discover_day_groups(self.data_root)

        self.assertEqual([(g.category, g.day) for g in groups], [(TRADES, DAY)])
        self.assertEqual(sorted(groups[0].hours), [9])

    def test_hour_is_derived_from_the_batch_stamp(self) -> None:
        self.write_batch(DAY, "23-59-59", _trade_rows(1_762_000_000_000, 1))
        groups = discover_day_groups(self.data_root)
        self.assertEqual(sorted(groups[0].hours), [23])
        self.assertEqual(groups[0].hours[23][0].hour, 23)
        self.assertEqual(len(groups[0].hours[23]), 1)
        self.assertEqual(
            groups[0].newest_mtime(), groups[0].hours[23][0].mtime
        )

    def test_today_boundary_uses_utc_dates(self) -> None:
        # 2026-11-09 23:30 UTC is 2026-11-10 in a +1 timezone; UTC keeps the day open.
        self.write_batch(date(2026, 11, 9), "23-30-00", _trade_rows(1_762_740_600_000, 1))
        early = datetime(2026, 11, 10, 0, 5, tzinfo=timezone.utc)
        late = datetime(2026, 11, 10, 0, 6, tzinfo=timezone.utc)

        self.assertFalse(day_is_closed(date(2026, 11, 9), now=early - timedelta(minutes=1)))
        self.assertTrue(day_is_closed(date(2026, 11, 9), now=late))
        self.assertFalse(self.hourly(date(2026, 11, 9), 23).exists())

        self.run_pass(now=late)

        self.assertTrue(self.daily(date(2026, 11, 9)).exists())


if __name__ == "__main__":
    unittest.main()
