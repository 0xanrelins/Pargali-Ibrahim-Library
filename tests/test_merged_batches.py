"""End-to-end tests for per-turn merged parquet batches.

Every test builds a throwaway project root (raw/, data/, state/) under
``tempfile``; no repository data directory is touched.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from datasets import BINANCE_FUTURES
from normalizer import runner
from normalizer.merged import (
    MERGED_MAX_FILES_PER_BATCH,
    chunk_pending,
    merged_batch_path,
    merged_batch_stamps,
    read_merged_sources,
)
from normalizer.runner import BATCH_MODE_MERGED, BATCH_MODE_PER_FILE, resolve_batch_mode
from normalizer.state import ProcessedState, load_processed_state, save_processed_state
from normalizer.tokens import invalidate_polymarket_outcome_batches
from normalizer.writer import MERGED_ROW_GROUP_SIZE, write_merged_parquet_batch

DATE = "2026-08-14"
SYMBOL = "BTCUSDT"


def _trade_line(index: int) -> str:
    return json.dumps(
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1_794_873_567_937 + index,
                "a": 3_528_592_811 + index,
                "s": "BTCUSDT",
                "p": f"{64871 + index / 100:.2f}",
                "q": "0.014",
                "nq": "0.014",
                "f": 1,
                "l": 1,
                "T": 1_794_873_567_787 + index,
                "m": True,
                "st": 1,
            },
        }
    )


def _liquidation_line(index: int) -> str:
    return json.dumps(
        {
            "e": "forceOrder",
            "E": 1_794_873_571_160 + index,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "o": "LIMIT",
                "f": "IOC",
                "q": "0.5",
                "p": "64800.00",
                "ap": "64801.00",
                "X": "FILLED",
                "l": "0.5",
                "z": "0.5",
                "T": 1_794_873_570_153 + index,
                "ps": "BTCUSDT",
                "st": 1,
            },
        }
    )


def _orderbook_line(index: int) -> str:
    return json.dumps(
        {
            "stream": "btcusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1_794_873_582_276 + index,
                "T": 1_794_873_582_275 + index,
                "s": "BTCUSDT",
                "ps": "BTCUSDT",
                "U": 12_137_691_406_379 + index * 10,
                "u": 12_137_691_414_184 + index * 10,
                "pu": 12_137_691_406_331 + index * 10,
                "b": [["64871.00", "0.5"], ["64870.00", "0.0"]],
                "a": [["64872.00", "1.0"]],
            },
        }
    )


LINE_BUILDERS = {
    "trades": _trade_line,
    "liquidations": _liquidation_line,
    "orderbook_diff": _orderbook_line,
}


def write_raw(
    root: Path,
    category: str,
    stamp: str,
    *,
    date: str = DATE,
    lines_per_file: int = 3,
    start_index: int = 0,
) -> Path:
    path = root / "raw" / BINANCE_FUTURES / category / date / f"{stamp}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    builder = LINE_BUILDERS[category]
    path.write_text(
        "\n".join(builder(start_index + offset) for offset in range(lines_per_file)) + "\n",
        encoding="utf-8",
    )
    return path


def make_project() -> tuple[tempfile.TemporaryDirectory, Path]:
    tmpdir = tempfile.TemporaryDirectory()
    return tmpdir, Path(tmpdir.name)


class NormalizerProjectTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir, self.root = make_project()
        self.addCleanup(self._tmpdir.cleanup)
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.tokens_path = self.root / "state" / "polymarket_tokens.json"

        patcher = mock.patch.object(
            runner,
            "project_paths",
            return_value=(self.root, self.data_root, self.state_path, self.tokens_path),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_normalize(self, mode: str | None = None) -> dict[str, int]:
        env = {} if mode is None else {"NORMALIZE_BATCH_MODE": mode}
        with mock.patch.dict(os.environ, env, clear=False):
            if mode is None:
                os.environ.pop("NORMALIZE_BATCH_MODE", None)
            return runner.normalize_once()

    def parquet_paths(self, category: str = "trades") -> list[Path]:
        category_root = self.data_root / category / BINANCE_FUTURES / SYMBOL / DATE
        if not category_root.exists():
            return []
        return sorted(category_root.glob("*.parquet"))

    def read_table(self, path: Path) -> pa.Table:
        return pq.read_table(path)


class BatchModeTests(unittest.TestCase):
    def test_default_is_per_file(self) -> None:
        self.assertEqual(resolve_batch_mode({}), BATCH_MODE_PER_FILE)
        self.assertEqual(resolve_batch_mode({"NORMALIZE_BATCH_MODE": "  "}), BATCH_MODE_PER_FILE)
        self.assertEqual(runner.DEFAULT_BATCH_MODE, BATCH_MODE_PER_FILE)

    def test_explicit_modes_and_case(self) -> None:
        self.assertEqual(
            resolve_batch_mode({"NORMALIZE_BATCH_MODE": "merged"}),
            BATCH_MODE_MERGED,
        )
        self.assertEqual(
            resolve_batch_mode({"NORMALIZE_BATCH_MODE": " PER_FILE "}),
            BATCH_MODE_PER_FILE,
        )

    def test_unknown_mode_fails_loudly(self) -> None:
        with self.assertRaises(ValueError):
            resolve_batch_mode({"NORMALIZE_BATCH_MODE": "auto"})

    def test_normalize_interval_is_five_minutes(self) -> None:
        import normalize_parquet as cli

        self.assertEqual(runner.NORMALIZE_INTERVAL_SEC, 300)
        self.assertIs(cli.NORMALIZE_INTERVAL_SEC, runner.NORMALIZE_INTERVAL_SEC)

    def test_loop_sleeps_for_the_configured_interval(self) -> None:
        import normalize_parquet as cli

        from normalizer.raw_retention import RawRetentionConfig

        sleeps: list[float] = []

        def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            cli.stop_requested = True

        poller = mock.Mock()
        with (
            mock.patch.object(cli, "normalize_once", return_value={}),
            mock.patch.object(cli, "log_stats"),
            mock.patch.object(cli, "start_exporter"),
            mock.patch.object(cli, "start_storage_poller", return_value=poller),
            mock.patch.object(cli, "sleep_until_stop", side_effect=fake_sleep),
            mock.patch.object(cli.signal, "signal"),
        ):
            cli.stop_requested = False
            try:
                cli.run_loop(retention_config=RawRetentionConfig())
            finally:
                cli.stop_requested = False

        poller.stop.assert_called_once()
        self.assertEqual(sleeps, [runner.NORMALIZE_INTERVAL_SEC])


class MergedBatchEndToEndTests(NormalizerProjectTestCase):
    def test_default_mode_still_writes_one_file_per_raw_file(self) -> None:
        for index, stamp in enumerate(("00-00-00", "00-01-00", "00-02-00")):
            write_raw(self.root, "trades", stamp, start_index=index * 10)

        stats = self.run_normalize()

        self.assertEqual(stats["parquet_batches_written"], 3)
        self.assertEqual(
            [path.name for path in self.parquet_paths()],
            ["00-00-00.parquet", "00-01-00.parquet", "00-02-00.parquet"],
        )
        self.assertFalse(any("--" in path.name for path in self.parquet_paths()))

    def test_merged_mode_writes_one_file_per_category(self) -> None:
        raws: dict[str, list[Path]] = {}
        for category in ("trades", "liquidations", "orderbook_diff"):
            raws[category] = [
                write_raw(
                    self.root,
                    category,
                    stamp,
                    start_index=index * 10,
                )
                for index, stamp in enumerate(("00-00-00", "00-01-00", "00-02-00"))
            ]

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["raw_files_processed"], 9)
        self.assertEqual(stats["parquet_batches_written"], 3)
        self.assertEqual(stats["raw_files_failed"], 0)
        for category in raws:
            paths = self.parquet_paths(category)
            self.assertEqual(len(paths), 1, paths)
            self.assertEqual(paths[0].name, "00-00-00--00-02-00.parquet")

        state = load_processed_state(self.state_path)
        self.assertEqual(len(state.processed_files), 9)
        self.assertEqual(len(state.batches), 9)
        for category, paths in raws.items():
            merged_rel = self.parquet_paths(category)[0].relative_to(self.root).as_posix()
            for raw in paths:
                self.assertEqual(state.batches[raw.relative_to(self.root).as_posix()], merged_rel)

    def test_merged_rows_equal_concat_of_per_file_batches(self) -> None:
        for category in ("trades", "liquidations", "orderbook_diff"):
            for index, stamp in enumerate(("00-00-00", "00-01-00", "00-02-00", "00-03-00")):
                write_raw(self.root, category, stamp, start_index=index * 10)

        per_file_stats = self.run_normalize(BATCH_MODE_PER_FILE)
        per_file = {
            category: [self.read_table(path) for path in self.parquet_paths(category)]
            for category in ("trades", "liquidations", "orderbook_diff")
        }
        per_file_files = sum(len(tables) for tables in per_file.values())
        self.assertEqual(per_file_stats["parquet_batches_written"], per_file_files)

        # Fresh project root so state and data do not overlap.
        self._tmpdir.cleanup()
        self._tmpdir, self.root = make_project()
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.tokens_path = self.root / "state" / "polymarket_tokens.json"
        with mock.patch.object(
            runner,
            "project_paths",
            return_value=(self.root, self.data_root, self.state_path, self.tokens_path),
        ):
            for category in ("trades", "liquidations", "orderbook_diff"):
                for index, stamp in enumerate(("00-00-00", "00-01-00", "00-02-00", "00-03-00")):
                    write_raw(self.root, category, stamp, start_index=index * 10)
            merged_stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(merged_stats["parquet_batches_written"], 3)
        for category, tables in per_file.items():
            merged_table = self.read_table(self.parquet_paths(category)[0])
            expected = pa.concat_tables(tables).replace_schema_metadata(None)
            actual = merged_table.replace_schema_metadata(None)
            self.assertTrue(actual.equals(expected), category)
            self.assertEqual(actual.num_rows, expected.num_rows)

    def test_merged_footer_records_sources_in_order(self) -> None:
        raw_paths = [
            write_raw(self.root, "trades", stamp, start_index=index * 10)
            for index, stamp in enumerate(("00-00-00", "00-01-00"))
        ]

        self.run_normalize(BATCH_MODE_MERGED)

        merged = self.parquet_paths()[0]
        sources = read_merged_sources(merged)
        self.assertEqual(
            sources,
            sorted(path.relative_to(self.root).as_posix() for path in raw_paths),
        )

    def test_empty_raw_file_is_still_covered_by_the_merged_batch(self) -> None:
        write_raw(self.root, "trades", "00-00-00", lines_per_file=0)
        write_raw(self.root, "trades", "00-01-00", lines_per_file=2, start_index=5)

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["raw_files_processed"], 2)
        self.assertEqual(stats["rows_written"], 2)
        table = self.read_table(self.parquet_paths()[0])
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(len(read_merged_sources(self.parquet_paths()[0])), 2)

    def test_merged_batches_do_not_cross_date_directories(self) -> None:
        write_raw(self.root, "trades", "12-00-00", date="2026-08-14", start_index=0)
        write_raw(self.root, "trades", "00-00-00", date="2026-08-15", start_index=10)

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["parquet_batches_written"], 2)
        for day in ("2026-08-14", "2026-08-15"):
            paths = sorted(
                (self.data_root / "trades" / BINANCE_FUTURES / SYMBOL / day).glob("*.parquet")
            )
            self.assertEqual(len(paths), 1, paths)
            self.assertEqual(self.read_table(paths[0]).num_rows, 3)
        state = load_processed_state(self.state_path)
        self.assertEqual(
            {raw.split("/")[3] for raw in state.batches},
            {"2026-08-14", "2026-08-15"},
        )

    def test_catch_up_chunks_are_deterministic_and_ordered(self) -> None:
        stamps = [f"00-{minute:02d}-00" for minute in range(MERGED_MAX_FILES_PER_BATCH + 2)]
        for index, stamp in enumerate(stamps):
            write_raw(self.root, "trades", stamp, lines_per_file=1, start_index=index)

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["parquet_batches_written"], 2)
        paths = self.parquet_paths()
        self.assertEqual(
            [path.name for path in paths],
            [
                f"{stamps[0]}--{stamps[MERGED_MAX_FILES_PER_BATCH - 1]}.parquet",
                f"{stamps[MERGED_MAX_FILES_PER_BATCH]}--{stamps[-1]}.parquet",
            ],
        )
        rows = sum(self.read_table(path).num_rows for path in paths)
        self.assertEqual(rows, len(stamps))

    def test_chunk_pending_keeps_order_and_expected_sizes(self) -> None:
        paths = [Path(f"00-{minute:02d}-00.jsonl") for minute in range(5)]
        self.assertEqual(chunk_pending(paths, max_files=2), [paths[:2], paths[2:4], paths[4:]])
        self.assertEqual(merged_batch_stamps(paths[:2]), ("00-00-00", "00-01-00"))
        self.assertEqual(
            merged_batch_path(Path("/tmp/day"), "00-00-00", "00-01-00").name,
            "00-00-00--00-01-00.parquet",
        )


class MergedBatchCrashRecoveryTests(NormalizerProjectTestCase):
    def test_published_batch_without_state_is_adopted_not_rewritten(self) -> None:
        for index, stamp in enumerate(("00-00-00", "00-01-00", "00-02-00")):
            write_raw(self.root, "trades", stamp, start_index=index * 10)

        with mock.patch.object(runner, "save_processed_state", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.run_normalize(BATCH_MODE_MERGED)

        merged = self.parquet_paths()[0]
        before = merged.read_bytes()
        self.assertFalse(self.state_path.exists())

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["raw_files_processed"], 0)
        self.assertEqual(stats["raw_files_skipped"], 3)
        self.assertEqual(stats["parquet_batches_written"], 0)
        self.assertEqual(merged.read_bytes(), before)
        state = load_processed_state(self.state_path)
        self.assertEqual(len(state.processed_files), 3)
        self.assertEqual(set(state.batches.values()), {merged.relative_to(self.root).as_posix()})

    def test_new_raw_files_after_a_crash_do_not_duplicate_rows(self) -> None:
        for index, stamp in enumerate(("00-00-00", "00-01-00")):
            write_raw(self.root, "trades", stamp, start_index=index * 10)

        with mock.patch.object(runner, "save_processed_state", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.run_normalize(BATCH_MODE_MERGED)
        first_batch = self.parquet_paths()[0]
        first_rows = self.read_table(first_batch).num_rows

        write_raw(self.root, "trades", "00-02-00", start_index=20)
        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["raw_files_skipped"], 2)
        self.assertEqual(stats["raw_files_processed"], 1)
        paths = self.parquet_paths()
        self.assertEqual(len(paths), 2)
        self.assertEqual(self.read_table(first_batch).num_rows, first_rows)
        self.assertEqual(
            sum(self.read_table(path).num_rows for path in paths),
            first_rows + 3,
        )
        state = load_processed_state(self.state_path)
        self.assertEqual(len(state.processed_files), 3)

    def test_existing_batch_with_different_sources_is_never_overwritten(self) -> None:
        for index, stamp in enumerate(("00-00-00", "00-01-00")):
            write_raw(self.root, "trades", stamp, start_index=index * 10)

        target = merged_batch_path(
            self.data_root / "trades" / BINANCE_FUTURES / SYMBOL / DATE,
            "00-00-00",
            "00-01-00",
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        foreign = pa.table({"timestamp": [1]})
        pq.write_table(foreign, target)
        before = target.read_bytes()

        stats = self.run_normalize(BATCH_MODE_MERGED)

        self.assertEqual(stats["raw_files_failed"], 2)
        self.assertEqual(stats["parquet_batches_written"], 0)
        self.assertEqual(target.read_bytes(), before)
        state = load_processed_state(self.state_path)
        self.assertEqual(state.processed_files, set())
        self.assertEqual(state.batches, {})


class MergedWriterTests(unittest.TestCase):
    def test_row_group_size_is_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "00-00-00--00-01-00.parquet"
            tables = [
                pa.table({"a": list(range(5))}),
                pa.table({"a": list(range(5, 10))}),
            ]
            write_merged_parquet_batch(
                out,
                tables,
                "trades",
                source_paths=["raw/a.jsonl", "raw/b.jsonl"],
                row_group_size=4,
            )
            metadata = pq.read_metadata(out)
            self.assertEqual(metadata.num_row_groups, 3)
            self.assertEqual(pq.read_table(out).column("a").to_pylist(), list(range(10)))

    def test_default_row_group_size_constant(self) -> None:
        self.assertEqual(MERGED_ROW_GROUP_SIZE, 100_000)

    def test_empty_table_list_still_writes_contract_schema(self) -> None:
        from normalizer.schemas import category_column_specs

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "empty.parquet"
            rows = write_merged_parquet_batch(
                out,
                [],
                "trades",
                source_paths=["raw/empty.jsonl"],
            )
            self.assertEqual(rows, 0)
            schema = pq.read_schema(out)
            self.assertEqual(
                schema.names,
                [name for name, _type in category_column_specs("trades")],
            )


class InvalidatePolymarketOutcomeBatchesTests(unittest.TestCase):
    """Token-registry backfill must not delete a merged batch it cannot rebuild."""

    def setUp(self) -> None:
        self._tmpdir, self.root = make_project()
        self.data_root = self.root / "data"
        self.state_path = self.root / "state" / "processed.json"
        self.category = "prediction_price"
        self.date = "2026-08-14"
        self.raw_dir = self.root / "raw" / "polymarket" / self.category / self.date
        self.raw_dir.mkdir(parents=True)
        self.raw_a = self.raw_dir / "12-00-00.jsonl"
        self.raw_b = self.raw_dir / "12-01-00.jsonl"
        for path in (self.raw_a, self.raw_b):
            path.write_text("{}\n", encoding="utf-8")
        self.rel_a = self.raw_a.relative_to(self.root).as_posix()
        self.rel_b = self.raw_b.relative_to(self.root).as_posix()
        self.batch_rel = (
            f"data/{self.category}/polymarket/BTC_15M/{self.date}/"
            "12-00-00--12-01-00.parquet"
        )
        self.batch_path = self.root / self.batch_rel

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _write_batch(self, sources: list[str]) -> None:
        write_merged_parquet_batch(
            self.batch_path,
            [],
            self.category,
            source_paths=sources,
        )

    def _state(self, sources: list[str]) -> ProcessedState:
        state = ProcessedState(
            processed_files=set(sources),
            batches={source: self.batch_rel for source in sources},
        )
        save_processed_state(self.state_path, state.processed_files, state.batches)
        return state

    def test_batch_is_deleted_only_when_every_source_still_exists(self) -> None:
        self._write_batch([self.rel_a, self.rel_b])
        state = self._state([self.rel_a, self.rel_b])

        invalidated = invalidate_polymarket_outcome_batches(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            state=state,
        )

        self.assertEqual(invalidated, 1)
        self.assertFalse(self.batch_path.exists())
        reloaded = load_processed_state(self.state_path)
        self.assertEqual(reloaded.processed_files, set())
        self.assertEqual(reloaded.batches, {})

    def test_batch_with_a_missing_source_is_kept(self) -> None:
        self._write_batch([self.rel_a, "raw/polymarket/prediction_price/2026-08-14/11-00-00.jsonl"])
        state = self._state([self.rel_a])

        invalidated = invalidate_polymarket_outcome_batches(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            state=state,
        )

        self.assertEqual(invalidated, 0)
        self.assertTrue(self.batch_path.exists())
        reloaded = load_processed_state(self.state_path)
        self.assertEqual(reloaded.batches, {self.rel_a: self.batch_rel})

    def test_batch_with_unreadable_sources_is_kept(self) -> None:
        self.batch_path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_path.write_bytes(b"PAR1PAR1")
        state = self._state([self.rel_a])

        invalidated = invalidate_polymarket_outcome_batches(
            project_root=self.root,
            data_root=self.data_root,
            state_path=self.state_path,
            state=state,
        )

        self.assertEqual(invalidated, 0)
        self.assertTrue(self.batch_path.exists())
        reloaded = load_processed_state(self.state_path)
        self.assertEqual(reloaded.batches, {self.rel_a: self.batch_rel})


class MergedStateMapTests(unittest.TestCase):
    def test_save_and_load_round_trip_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state" / "processed.json"
            save_processed_state(
                state_path,
                {"raw/a.jsonl", "raw/b.jsonl"},
                {"raw/a.jsonl": "data/x.parquet"},
            )
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["version"], 2)
            self.assertEqual(payload["processed_files"], ["raw/a.jsonl", "raw/b.jsonl"])
            self.assertEqual(payload["processed_batches"], {"raw/a.jsonl": "data/x.parquet"})

            state = load_processed_state(state_path)
            self.assertEqual(state.processed_files, {"raw/a.jsonl", "raw/b.jsonl"})
            self.assertEqual(state.batches, {"raw/a.jsonl": "data/x.parquet"})


if __name__ == "__main__":
    unittest.main()
