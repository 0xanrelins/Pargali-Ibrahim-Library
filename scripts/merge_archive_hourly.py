#!/usr/bin/env python3
"""One-off migration: consolidate a per-minute parquet archive into hourly files.

Source layout (read only)::

    <source>/<category>/<exchange>/<symbol>/<YYYY-MM-DD>/<HH-MM-SS>.parquet

Target layout (``--out``, never the source root)::

    <out>/<category>/<exchange>/<symbol>/<YYYY-MM-DD>/<first>--<last>.parquet

``first`` and ``last`` are the stamps of the first and last source file of the
hour, using the same separator and pattern as ``normalizer/merged.py``. All
source files whose ``HH`` is one hour become one output file, in stamp order.
Rows and columns are copied as they are; no sort is applied.

The default is a dry run. ``--apply`` writes output files atomically. Source
files are only ever read. An existing output is reused when its row count,
schema and recorded source list match the sources; otherwise the run stops and
leaves it untouched.

Every output carries ``normalizer.raw_sources`` footer metadata (source paths
relative to ``--source``) and a JSONL report is kept under the output root.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from normalizer.merged import (  # noqa: E402
    MERGED_BATCH_PATTERN,
    MERGED_NAME_SEPARATOR,
    MERGED_ROW_GROUP_SIZE,
)
from normalizer.writer import MERGED_SOURCES_METADATA_KEY  # noqa: E402

logger = logging.getLogger("merge_archive_hourly")

REPORT_NAME = "_merge_report.jsonl"
TEMP_SUFFIX = ".tmp"
DAY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SOURCE_STAMP_PATTERN = re.compile(r"^(?P<stamp>\d{2}-\d{2}-\d{2})\.parquet$")
TIME_COLUMN_CANDIDATES: tuple[str, ...] = ("timestamp", "event_time")


class MergeError(RuntimeError):
    """A source, output or verification problem that stops the migration."""


@dataclass(frozen=True)
class SourceFile:
    path: Path
    relative: str
    stamp: str
    size: int


@dataclass(frozen=True)
class SourceMeta:
    source: SourceFile
    rows: int
    schema: pa.Schema
    timestamp_column: str | None
    timestamp_min: object | None
    timestamp_max: object | None


@dataclass(frozen=True)
class DayBatch:
    category: str
    exchange: str
    symbol: str
    day: str
    sources: tuple[SourceFile, ...]


@dataclass(frozen=True)
class HourGroup:
    category: str
    exchange: str
    symbol: str
    day: str
    hour: str
    sources: tuple[SourceFile, ...]

    @property
    def target_name(self) -> str:
        first = self.sources[0].stamp
        last = self.sources[-1].stamp
        return f"{first}{MERGED_NAME_SEPARATOR}{last}.parquet"

    @property
    def relative_dir(self) -> Path:
        return Path(self.category) / self.exchange / self.symbol / self.day

    @property
    def target_relative(self) -> str:
        return (self.relative_dir / self.target_name).as_posix()

    @property
    def label(self) -> str:
        return f"{self.category}/{self.exchange}/{self.symbol} {self.day} {self.hour}:00"

    def source_bytes(self) -> int:
        return sum(source.size for source in self.sources)


@dataclass
class Stats:
    days: int = 0
    hours: int = 0
    written: int = 0
    reused: int = 0
    planned: int = 0
    files_in: int = 0
    files_out: int = 0
    files_written: int = 0
    files_reused: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    bytes_written: int = 0
    ignored: int = 0
    failures: int = 0


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _resolve_source(path: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise MergeError(
            f"source root does not exist or is not accessible: {path}"
        ) from exc
    if not resolved.is_dir():
        raise MergeError(f"source root is not a directory: {path}")
    return resolved


def _resolve_output(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.exists() and not resolved.is_dir():
        raise MergeError(f"output root exists but is not a directory: {path}")
    return resolved


def _check_roots_do_not_overlap(source_root: Path, out_root: Path) -> None:
    if _is_within(out_root, source_root) or _is_within(source_root, out_root):
        raise MergeError(
            "source and output roots overlap; "
            f"source={source_root} out={out_root} (choose a separate --out root)"
        )


def _subdirs(path: Path, *, boundary: Path) -> list[Path]:
    if not path.is_dir():
        return []
    directories: list[Path] = []
    for entry in sorted(path.iterdir(), key=lambda item: item.name):
        if entry.name.startswith("."):
            continue
        if not entry.is_dir():
            continue
        if entry.is_symlink():
            resolved = entry.resolve()
            if not _is_within(resolved, boundary):
                raise MergeError(
                    f"directory symlink escapes the source root: {entry} -> {resolved}"
                )
        directories.append(entry)
    return directories


def _validate_day_name(name: str, path: Path) -> None:
    if DAY_PATTERN.fullmatch(name) is None:
        raise MergeError(
            f"unexpected directory under a symbol: {path} (expected YYYY-MM-DD)"
        )
    try:
        date.fromisoformat(name)
    except ValueError as exc:
        raise MergeError(f"invalid date directory: {path}") from exc


def _validate_stamp(stamp: str, path: Path) -> None:
    try:
        time.fromisoformat(stamp.replace("-", ":"))
    except ValueError as exc:
        raise MergeError(f"invalid HH-MM-SS stamp in {path.name}: {stamp!r}") from exc


def _scan_day(day_dir: Path, *, source_root: Path, stats: Stats) -> list[SourceFile]:
    sources: list[SourceFile] = []
    for entry in sorted(day_dir.iterdir(), key=lambda item: item.name):
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            raise MergeError(f"unexpected directory inside a day directory: {entry}")
        if not entry.is_file():
            stats.ignored += 1
            continue
        if entry.is_symlink():
            resolved = entry.resolve()
            if not _is_within(resolved, source_root):
                raise MergeError(
                    f"file symlink escapes the source root: {entry} -> {resolved}"
                )
        if entry.suffix != ".parquet":
            stats.ignored += 1
            logger.warning("Ignoring non-parquet file in the source archive: %s", entry)
            continue
        match = SOURCE_STAMP_PATTERN.fullmatch(entry.name)
        if match is None:
            raise MergeError(
                f"unexpected parquet file name {entry}; "
                "expected a per-minute HH-MM-SS.parquet source"
            )
        stamp = match.group("stamp")
        _validate_stamp(stamp, entry)
        sources.append(
            SourceFile(
                path=entry,
                relative=entry.relative_to(source_root).as_posix(),
                stamp=stamp,
                size=entry.stat().st_size,
            )
        )
    return sources


def scan_source_tree(source_root: Path, stats: Stats) -> Iterator[DayBatch]:
    for category_dir in _subdirs(source_root, boundary=source_root):
        category = category_dir.name
        for exchange_dir in _subdirs(category_dir, boundary=source_root):
            exchange = exchange_dir.name
            for symbol_dir in _subdirs(exchange_dir, boundary=source_root):
                symbol = symbol_dir.name
                for day_dir in _subdirs(symbol_dir, boundary=source_root):
                    day = day_dir.name
                    _validate_day_name(day, day_dir)
                    sources = _scan_day(day_dir, source_root=source_root, stats=stats)
                    if sources:
                        yield DayBatch(
                            category=category,
                            exchange=exchange,
                            symbol=symbol,
                            day=day,
                            sources=tuple(sources),
                        )


def group_day(day_batch: DayBatch) -> list[HourGroup]:
    buckets: dict[str, list[SourceFile]] = {}
    for source in sorted(day_batch.sources, key=lambda item: item.stamp):
        buckets.setdefault(source.stamp[:2], []).append(source)

    groups: list[HourGroup] = []
    for hour in sorted(buckets):
        sources = tuple(buckets[hour])
        if len({source.stamp for source in sources}) != len(sources):
            raise MergeError(
                "duplicate source stamp in "
                f"{day_batch.category}/{day_batch.exchange}/{day_batch.symbol} "
                f"{day_batch.day} {hour}:00"
            )
        group = HourGroup(
            category=day_batch.category,
            exchange=day_batch.exchange,
            symbol=day_batch.symbol,
            day=day_batch.day,
            hour=hour,
            sources=sources,
        )
        if MERGED_BATCH_PATTERN.fullmatch(group.target_name) is None:
            raise MergeError(
                "constructed target name does not match the merged pattern: "
                f"{group.target_name}"
            )
        groups.append(group)
    return groups


def existing_outputs_by_hour(day_out_dir: Path) -> dict[str, list[Path]]:
    """Merged output files already present in one output day directory.

    The key is the hour of the first/last stamps. A file spanning two hours is
    not a target of this tool, so it stops the run instead of being ignored.
    """
    if not day_out_dir.is_dir():
        return {}
    by_hour: dict[str, list[Path]] = {}
    for entry in sorted(day_out_dir.iterdir(), key=lambda item: item.name):
        if entry.name.startswith(".") or not entry.is_file():
            continue
        match = MERGED_BATCH_PATTERN.fullmatch(entry.name)
        if match is None:
            continue
        first = match.group("first")
        last = match.group("last")
        if first[:2] != last[:2]:
            raise MergeError(
                f"merged parquet output spans more than one hour: {entry}"
            )
        by_hour.setdefault(first[:2], []).append(entry)
    return by_hour


def read_source_meta(source: SourceFile) -> SourceMeta:
    try:
        parquet_file = pq.ParquetFile(source.path)
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
    except Exception as exc:
        raise MergeError(
            f"cannot read parquet footer for {source.relative}: {exc}"
        ) from exc
    if metadata is None:
        raise MergeError(f"parquet footer is missing for {source.relative}")

    timestamp_column = next(
        (name for name in TIME_COLUMN_CANDIDATES if name in schema.names), None
    )
    timestamp_min: object | None = None
    timestamp_max: object | None = None
    if timestamp_column is not None:
        column_index = schema.names.index(timestamp_column)
        minimums: list[object] = []
        maximums: list[object] = []
        complete = True
        for row_group in range(metadata.num_row_groups):
            statistics = metadata.row_group(row_group).column(column_index).statistics
            if statistics is None or not statistics.has_min_max:
                complete = False
                break
            minimums.append(statistics.min)
            maximums.append(statistics.max)
        if complete and minimums:
            try:
                timestamp_min = min(minimums)
                timestamp_max = max(maximums)
            except TypeError:
                timestamp_min = None
                timestamp_max = None

    return SourceMeta(
        source=source,
        rows=metadata.num_rows,
        schema=schema,
        timestamp_column=timestamp_column,
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
    )


def _validate_source_schemas(group: HourGroup, metas: Sequence[SourceMeta]) -> None:
    reference = metas[0]
    for meta in metas[1:]:
        if not reference.schema.equals(meta.schema, check_metadata=False):
            raise MergeError(
                "source schema mismatch: "
                f"{reference.source.relative} vs {meta.source.relative}"
            )


def _timestamp_bounds(metas: Sequence[SourceMeta]) -> tuple[object | None, object | None]:
    # An empty source has no timestamp rows; it must not hide the bounds of the
    # non-empty sources in the same hour.
    populated = [meta for meta in metas if meta.rows > 0]
    if not populated:
        return None, None
    if any(meta.timestamp_column is None for meta in populated):
        return None, None
    if any(
        meta.timestamp_min is None or meta.timestamp_max is None
        for meta in populated
    ):
        return None, None
    try:
        return (
            min(meta.timestamp_min for meta in populated if meta.timestamp_min is not None),
            max(meta.timestamp_max for meta in populated if meta.timestamp_max is not None),
        )
    except TypeError:
        return None, None


def _jsonable(value: object | None) -> object | None:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return str(value)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path_list(paths: Sequence[Path]) -> str:
    return "[" + ", ".join(_sql_literal(str(path)) for path in paths) + "]"


def _duckdb_schema(
    connection: duckdb.DuckDBPyConnection, paths: Sequence[Path]
) -> tuple[tuple[str, str], ...]:
    rows = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet("
        + _sql_path_list(paths)
        + ", union_by_name=false)"
    ).fetchall()
    return tuple((str(row[0]), str(row[1])) for row in rows)


def _expected_sources(metas: Sequence[SourceMeta]) -> list[str]:
    return sorted(meta.source.relative for meta in metas)


def _read_recorded_sources(path: Path) -> list[str] | None:
    metadata = pq.read_metadata(path).metadata or {}
    raw = metadata.get(MERGED_SOURCES_METADATA_KEY)
    if raw is None:
        return None
    try:
        recorded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MergeError(f"invalid source metadata in {path}: {exc}") from exc
    if not isinstance(recorded, list) or not all(isinstance(item, str) for item in recorded):
        raise MergeError(f"unexpected source metadata shape in {path}")
    return recorded


def _verify_output(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    metas: Sequence[SourceMeta],
    expected_schema: Sequence[tuple[str, str]],
) -> None:
    try:
        metadata = pq.read_metadata(path)
    except Exception as exc:
        raise MergeError(f"output is not readable: {path}: {exc}") from exc
    expected_rows = sum(meta.rows for meta in metas)
    if metadata.num_rows != expected_rows:
        raise MergeError(
            f"row count mismatch after write: output={metadata.num_rows} "
            f"sources={expected_rows}"
        )
    actual_schema = _duckdb_schema(connection, [path])
    if actual_schema != tuple(expected_schema):
        raise MergeError(
            f"schema mismatch after write: expected {tuple(expected_schema)}, "
            f"got {actual_schema}"
        )
    recorded = _read_recorded_sources(path)
    if recorded != _expected_sources(metas):
        raise MergeError("output source metadata does not match the sources written")


def _validate_existing(
    connection: duckdb.DuckDBPyConnection,
    target: Path,
    target_relative: str,
    metas: Sequence[SourceMeta],
    report_entries: dict[str, dict[str, object]],
) -> int:
    try:
        metadata = pq.read_metadata(target)
    except Exception as exc:
        raise MergeError(f"existing output is not readable: {target}: {exc}") from exc
    expected_rows = sum(meta.rows for meta in metas)
    if metadata.num_rows != expected_rows:
        raise MergeError(
            f"existing output row count is {metadata.num_rows}, sources sum to "
            f"{expected_rows}; refusing to overwrite"
        )
    expected_schema = _duckdb_schema(connection, [meta.source.path for meta in metas])
    actual_schema = _duckdb_schema(connection, [target])
    if actual_schema != expected_schema:
        raise MergeError(
            f"existing output schema differs from its sources: "
            f"expected {expected_schema}, got {actual_schema}; refusing to overwrite"
        )
    expected_sources = _expected_sources(metas)
    recorded = _read_recorded_sources(target)
    entry = report_entries.get(target_relative)
    if recorded is None and entry is None:
        raise MergeError(
            "existing output has no recorded source set; refusing to reuse it"
        )
    if recorded is not None and recorded != expected_sources:
        raise MergeError(
            "existing output records a different source set; refusing to overwrite"
        )
    if entry is not None:
        if entry.get("sources") != expected_sources:
            raise MergeError(
                "existing report entry records a different source set; "
                "refusing to overwrite"
            )
        if entry.get("rows") != expected_rows:
            raise MergeError(
                "existing report entry records a different row count; "
                "refusing to overwrite"
            )
    return target.stat().st_size


def write_hour(
    connection: duckdb.DuckDBPyConnection,
    group: HourGroup,
    metas: Sequence[SourceMeta],
    out_root: Path,
) -> Path:
    target = out_root / group.relative_dir / group.target_name
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(target.name + TEMP_SUFFIX)
    temp.unlink(missing_ok=True)

    source_paths = [meta.source.path for meta in metas]
    expected_schema = _duckdb_schema(connection, source_paths)
    metadata_json = json.dumps(
        _expected_sources(metas), separators=(",", ":"), ensure_ascii=False
    )
    sql = (
        "COPY (SELECT * FROM read_parquet("
        + _sql_path_list(source_paths)
        + ", union_by_name=false)) TO "
        + _sql_literal(str(temp))
        + " (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE "
        + str(MERGED_ROW_GROUP_SIZE)
        + ", KV_METADATA {\"normalizer.raw_sources\": "
        + _sql_literal(metadata_json)
        + "})"
    )
    try:
        connection.execute(sql)
        _verify_output(connection, temp, metas, expected_schema)
        os.replace(temp, target)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    return target


def _report_entry(
    group: HourGroup,
    metas: Sequence[SourceMeta],
    rows: int,
    output_bytes: int,
    status: str,
    *,
    target_relative: str | None = None,
) -> dict[str, object]:
    timestamp_min, timestamp_max = _timestamp_bounds(metas)
    return {
        "target": target_relative or group.target_relative,
        "category": group.category,
        "exchange": group.exchange,
        "symbol": group.symbol,
        "date": group.day,
        "hour": group.hour,
        "first": group.sources[0].stamp,
        "last": group.sources[-1].stamp,
        "sources": _expected_sources(metas),
        "source_count": len(metas),
        "rows": rows,
        "source_bytes": group.source_bytes(),
        "output_bytes": output_bytes,
        "timestamp_column": metas[0].timestamp_column,
        "timestamp_min": _jsonable(timestamp_min),
        "timestamp_max": _jsonable(timestamp_max),
        "status": status,
    }


def load_report(path: Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    entries: dict[str, dict[str, object]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(
                    "Ignoring malformed report line %s in %s", line_number, path
                )
                continue
            if not isinstance(entry, dict):
                logger.warning(
                    "Ignoring non-object report line %s in %s", line_number, path
                )
                continue
            target = entry.get("target")
            if isinstance(target, str) and target:
                entries[target] = entry
    return entries


def write_report(path: Path, entries: dict[str, dict[str, object]]) -> None:
    if not entries:
        return
    text = "".join(
        json.dumps(entries[target], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
        for target in sorted(entries)
    )
    if path.is_file():
        try:
            if path.read_text(encoding="utf-8") == text:
                return
        except OSError:
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + TEMP_SUFFIX)
    try:
        temp.write_text(text, encoding="utf-8")
        os.replace(temp, path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def _progress(group: HourGroup, metas: Sequence[SourceMeta], status: str) -> None:
    rows = sum(meta.rows for meta in metas)
    print(
        f"{group.day} {group.hour} {group.category}/{group.exchange}/{group.symbol} "
        f"files={len(metas)} rows={rows} -> {group.target_name} ({status})",
        flush=True,
    )


def _print_summary(stats: Stats, *, apply: bool) -> None:
    print(
        "SUMMARY "
        f"mode={'apply' if apply else 'dry-run'} "
        f"days={stats.days} hours={stats.hours} "
        f"files_in={stats.files_in} files_out={stats.files_out} "
        f"written={stats.written} reused={stats.reused} planned={stats.planned} "
        f"bytes_in={stats.bytes_in} bytes_out={stats.bytes_out} "
        f"bytes_written={stats.bytes_written} ignored_files={stats.ignored} "
        f"failures={stats.failures}",
        flush=True,
    )


def run(args: argparse.Namespace) -> Stats:
    source_root = _resolve_source(args.source)
    out_root = _resolve_output(args.out)
    _check_roots_do_not_overlap(source_root, out_root)

    apply = bool(args.apply)
    report_path = out_root / REPORT_NAME
    report_entries = load_report(report_path)
    stats = Stats()
    connection = duckdb.connect()
    connection.execute("SET preserve_insertion_order = true")

    def finish() -> None:
        if apply:
            write_report(report_path, report_entries)
        _print_summary(stats, apply=apply)

    try:
        for day_batch in scan_source_tree(source_root, stats):
            stats.days += 1
            day_out_dir = (
                out_root
                / day_batch.category
                / day_batch.exchange
                / day_batch.symbol
                / day_batch.day
            )
            existing_by_hour = existing_outputs_by_hour(day_out_dir)
            for group in group_day(day_batch):
                try:
                    metas = [read_source_meta(source) for source in group.sources]
                    _validate_source_schemas(group, metas)
                    existing = existing_by_hour.get(group.hour, [])
                    if len(existing) > 1:
                        raise MergeError(
                            "more than one existing output for this hour: "
                            + ", ".join(path.name for path in existing)
                        )
                    if existing:
                        target = existing[0]
                        target_relative = target.relative_to(out_root).as_posix()
                        output_bytes = _validate_existing(
                            connection,
                            target,
                            target_relative,
                            metas,
                            report_entries,
                        )
                        if target.name != group.target_name:
                            raise MergeError(
                                f"existing output {target.name} does not match the "
                                f"expected name {group.target_name}; refusing to "
                                "create a second file for the hour"
                            )
                        status = "reused"
                        stats.reused += 1
                        stats.files_reused += 1
                        if target_relative not in report_entries:
                            report_entries[target_relative] = _report_entry(
                                group,
                                metas,
                                sum(meta.rows for meta in metas),
                                output_bytes,
                                status,
                                target_relative=target_relative,
                            )
                    elif apply:
                        target = write_hour(connection, group, metas, out_root)
                        output_bytes = target.stat().st_size
                        status = "written"
                        stats.written += 1
                        stats.files_written += 1
                        stats.bytes_written += output_bytes
                        report_entries[group.target_relative] = _report_entry(
                            group,
                            metas,
                            sum(meta.rows for meta in metas),
                            output_bytes,
                            status,
                        )
                    else:
                        output_bytes = 0
                        status = "would write"
                        stats.planned += 1
                except MergeError as exc:
                    stats.failures += 1
                    raise MergeError(f"{group.label}: {exc}") from exc
                except Exception as exc:
                    stats.failures += 1
                    raise MergeError(f"{group.label}: {exc}") from exc

                stats.hours += 1
                stats.files_in += len(metas)
                stats.bytes_in += group.source_bytes()
                if status in {"written", "reused"}:
                    stats.files_out += 1
                    stats.bytes_out += output_bytes
                _progress(group, metas, status)
    except Exception:
        stats.failures = max(stats.failures, 1)
        try:
            finish()
        except Exception as report_exc:
            print(f"ERROR: could not write report: {report_exc}", file=sys.stderr)
        raise
    finally:
        connection.close()

    finish()
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consolidate per-minute parquet files into hourly merged files "
            "(dry run by default)."
        )
    )
    parser.add_argument(
        "source",
        type=Path,
        help="source archive root: <category>/<exchange>/<symbol>/<date>/<HH-MM-SS>.parquet",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="output root; must not overlap the source root",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--apply",
        action="store_true",
        help="write output files (default is a dry run)",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="explicitly request the default dry run",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="log warnings and informational messages",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(message)s",
    )
    try:
        run(args)
    except MergeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("ERROR: interrupted", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
