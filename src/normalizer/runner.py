"""Orchestrate raw JSONL → immutable normalized parquet batches."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Mapping
from pathlib import Path

import pyarrow as pa

from datasets import POLYMARKET, REGISTERED_SOURCES, all_categories, iter_jsonl_paths, raw_root
from metrics.pipeline_registry import (
    parquet_failed_conversions_total,
    parquet_files_processed_total,
    parquet_last_batch_duration_seconds,
    parquet_last_batch_files_processed,
    parquet_last_batch_timestamp,
    parquet_raw_backlog_files,
)
from normalizer.batch import normalize_jsonl_batch
from normalizer.merged import (
    chunk_pending,
    merged_batch_path,
    merged_batch_stamps,
    read_merged_sources,
    recover_orphaned_merged_batches,
)
from normalizer.parser import parse_jsonl_file
from normalizer.paths import (
    batch_parquet_path_from_jsonl,
    per_file_batch_rel_path,
    project_paths,
    relative_to_project,
)
from normalizer.state import (
    ProcessedState,
    load_processed_state,
    migrate_legacy_batch_mappings,
    save_polymarket_tokens,
    save_processed_state,
)
from normalizer.tokens import (
    invalidate_polymarket_outcome_batches,
    mark_polymarket_outcome_backfill_done,
    prepare_polymarket_token_registry,
)
from normalizer.writer import rows_to_arrow, write_merged_parquet_batch
from parse_polymarket import OutcomeTokenRegistry

logger = logging.getLogger(__name__)

# One turn costs a full raw/ scan, a state read and a state write regardless of
# how many files arrived. At 60 s that fixed cost ran 60 times an hour for data
# that lands in ~1-minute files; 300 s (the storage-metrics cadence) keeps
# backlog latency at a few minutes while cutting the fixed cost 5x, and gives
# each turn enough files to make merged batches worthwhile.
NORMALIZE_INTERVAL_SEC = 300

BATCH_MODE_ENV = "NORMALIZE_BATCH_MODE"
BATCH_MODE_PER_FILE = "per_file"
BATCH_MODE_MERGED = "merged"
DEFAULT_BATCH_MODE = BATCH_MODE_PER_FILE
BATCH_MODES = (BATCH_MODE_PER_FILE, BATCH_MODE_MERGED)


def resolve_batch_mode(environ: Mapping[str, str] | None = None) -> str:
    """Read ``NORMALIZE_BATCH_MODE``; missing means the historical per-file behavior."""
    raw_value = (environ if environ is not None else os.environ).get(BATCH_MODE_ENV, "")
    value = raw_value.strip().lower()
    if not value:
        return DEFAULT_BATCH_MODE
    if value not in BATCH_MODES:
        raise ValueError(
            f"{BATCH_MODE_ENV} must be one of {BATCH_MODES}, got {raw_value!r}"
        )
    return value


def collect_backlog_metrics(
    *,
    project_root: Path,
    state_path: Path,
) -> dict[str, int]:
    processed_files = load_processed_state(state_path).processed_files
    backlog = {category: 0 for category in all_categories()}

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue
        exchange_raw_root = raw_root(project_root, source.exchange)
        for category in source.categories:
            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                rel_path = relative_to_project(jsonl_path, project_root)
                if rel_path not in processed_files:
                    backlog[category] += 1
            parquet_raw_backlog_files.labels(category=category).set(backlog[category])

    return backlog


def _load_processed_state(
    *,
    project_root: Path,
    data_root: Path,
    state_path: Path,
) -> ProcessedState:
    """Load state and upgrade legacy entries to the raw → batch mapping (in memory)."""
    state = load_processed_state(state_path)
    migrated = migrate_legacy_batch_mappings(
        state,
        resolve_batch_rel=lambda rel_path: per_file_batch_rel_path(
            rel_path,
            project_root=project_root,
            data_root=data_root,
        ),
    )
    if migrated:
        logger.info(
            "Migrated %d legacy processed entries to per-file batch mappings",
            migrated,
        )
    return state


def _persist_processed_state(
    *,
    state_path: Path,
    state: ProcessedState,
    scanned_paths: set[str],
    interrupted: bool,
) -> None:
    """Write processed state once per batch, bounded to raw files still on disk.

    ``scanned_paths`` is every raw JSONL path seen during this batch. Entries that
    were not seen belong to raw files deleted by retention (or to sources that
    are no longer registered), so they are pruned and the state file stops growing.
    The batch mapping is pruned with the set, so it can never point at a raw file
    that is no longer listed as processed.

    When the batch stopped early the scan is partial: paths not visited yet are
    not proof of deletion, so the state is written as-is and pruned on the next
    full batch instead.
    """
    if interrupted:
        save_processed_state(state_path, state.processed_files, state.batches)
        logger.info(
            "Stop requested mid-batch; saved %d processed entries without pruning",
            len(state.processed_files),
        )
        return

    retained = state.processed_files & scanned_paths
    pruned = len(state.processed_files) - len(retained)
    retained_batches = {
        raw_path: batch_path
        for raw_path, batch_path in state.batches.items()
        if raw_path in retained
    }
    save_processed_state(state_path, retained, retained_batches)
    state.processed_files = retained
    state.batches = retained_batches
    logger.info(
        "Saved processed state: %d entries, %d batch mappings (%d pruned, raw file no longer on disk)",
        len(retained),
        len(retained_batches),
        pruned,
    )


def _token_snapshot(
    token_registry: OutcomeTokenRegistry | None,
    *,
    exchange: str,
    category: str,
) -> dict[str, str] | None:
    """Registry mapping before a parse, only for files that can change it.

    Only ``prediction_metadata`` rows add asset → outcome entries, so every
    other category skips the copy and the post-parse comparison.
    """
    if token_registry is None or exchange != POLYMARKET:
        return None
    if category != "prediction_metadata":
        return None
    return token_registry.to_mapping()


def _save_tokens_if_changed(
    *,
    token_registry: OutcomeTokenRegistry | None,
    tokens_before: dict[str, str] | None,
    tokens_path: Path,
) -> None:
    """Persist the Polymarket registry when a metadata batch changed it.

    The registry is state, not parquet output, so both batch modes call this:
    the historical ``per_file`` path used to skip the write and only the merged
    path persisted it, which delayed a new asset → outcome mapping by one turn.
    """
    if token_registry is None or tokens_before is None:
        return
    mapping_after = token_registry.to_mapping()
    if mapping_after != tokens_before:
        save_polymarket_tokens(tokens_path, mapping_after)


def _parse_rows_with_token_sync(
    jsonl_path: Path,
    *,
    exchange: str,
    category: str,
    tokens_path: Path,
    token_registry: OutcomeTokenRegistry | None,
):
    """Parse one raw file, persisting the Polymarket token registry when it changes."""
    registry = token_registry if exchange == POLYMARKET else None
    tokens_before = _token_snapshot(registry, exchange=exchange, category=category)
    rows = parse_jsonl_file(
        jsonl_path,
        exchange=exchange,
        category=category,
        token_registry=registry,
    )
    _save_tokens_if_changed(
        token_registry=registry,
        tokens_before=tokens_before,
        tokens_path=tokens_path,
    )
    return rows


def _process_per_file_pending(
    pending: list[tuple[Path, str]],
    *,
    should_stop: Callable[[], bool] | None,
    project_root: Path,
    data_root: Path,
    tokens_path: Path,
    token_registry: OutcomeTokenRegistry | None,
    state: ProcessedState,
    stats: dict[str, int],
    written_paths: set[Path],
) -> bool:
    """Normalize pending files one-by-one (historical behavior). Returns interrupted.

    Parquet output stays byte-identical to the historical writer. The
    Polymarket token registry is persisted after a metadata file adds a
    mapping, exactly as in merged mode: it is state, so it must not depend on
    which writer layout is active.
    """
    for jsonl_path, rel_path in pending:
        if should_stop is not None and should_stop():
            return True

        exchange = _exchange_for(jsonl_path)
        category = jsonl_path.parts[-3]
        registry = token_registry if exchange == POLYMARKET else None
        tokens_before = _token_snapshot(registry, exchange=exchange, category=category)
        try:
            written_path, row_count = normalize_jsonl_batch(
                jsonl_path,
                project_root=project_root,
                data_root=data_root,
                token_registry=registry,
            )
            _save_tokens_if_changed(
                token_registry=registry,
                tokens_before=tokens_before,
                tokens_path=tokens_path,
            )
        except Exception:
            stats["raw_files_failed"] += 1
            parquet_failed_conversions_total.labels(reason="normalize").inc()
            logger.exception("Failed to normalize raw file: %s", jsonl_path)
            continue

        state.processed_files.add(rel_path)
        state.batches[rel_path] = relative_to_project(written_path, project_root)

        stats["raw_files_processed"] += 1
        stats["rows_written"] += row_count
        written_paths.add(written_path)
        parquet_files_processed_total.inc()

    return False


def _exchange_for(jsonl_path: Path) -> str:
    # raw/{exchange}/{category}/{date}/{stamp}.jsonl
    return jsonl_path.parts[-4]


def _process_merged_pending(
    pending: list[tuple[Path, str]],
    *,
    should_stop: Callable[[], bool] | None,
    project_root: Path,
    data_root: Path,
    tokens_path: Path,
    token_registry: OutcomeTokenRegistry | None,
    state: ProcessedState,
    stats: dict[str, int],
    written_paths: set[Path],
) -> bool:
    """Merge pending files into one parquet per chunk. Returns interrupted.

    A file that fails to parse/convert is left out of the merged batch and stays
    unprocessed, so the next turn retries it; the other files still progress.
    The whole chunk is abandoned (nothing published) when a stop is requested,
    so a published batch always contains every source it lists in its footer.
    """
    rel_by_path = dict(pending)

    for chunk in chunk_pending([jsonl_path for jsonl_path, _rel in pending]):
        if should_stop is not None and should_stop():
            return True

        tables: list[pa.Table] = []
        included: list[Path] = []
        category = chunk[0].parts[-3]
        exchange = _exchange_for(chunk[0])
        day_data_dir = batch_parquet_path_from_jsonl(
            chunk[0],
            project_root=project_root,
            data_root=data_root,
        ).parent

        for jsonl_path in chunk:
            if should_stop is not None and should_stop():
                return True
            try:
                rows = _parse_rows_with_token_sync(
                    jsonl_path,
                    exchange=exchange,
                    category=category,
                    tokens_path=tokens_path,
                    token_registry=token_registry,
                )
                tables.append(rows_to_arrow(rows, category))
            except Exception:
                stats["raw_files_failed"] += 1
                parquet_failed_conversions_total.labels(reason="normalize").inc()
                logger.exception("Failed to normalize raw file into merged batch: %s", jsonl_path)
                continue
            included.append(jsonl_path)

        if not included:
            continue

        first_stamp, last_stamp = merged_batch_stamps(included)
        out_path = merged_batch_path(day_data_dir, first_stamp, last_stamp)
        out_rel = relative_to_project(out_path, project_root)
        sources = [rel_by_path[path] for path in included]

        if out_path.exists():
            # A file at this exact name normally means a previous turn published
            # it and crashed before saving state. Adopt it only when its footer
            # lists exactly this source set; never overwrite a different batch.
            existing_sources = read_merged_sources(out_path)
            if existing_sources == sorted(sources):
                for path in included:
                    rel_path = rel_by_path[path]
                    state.processed_files.add(rel_path)
                    state.batches[rel_path] = out_rel
                stats["raw_files_skipped"] += len(included)
                logger.info(
                    "Recovered processed state for existing merged batch: %s (%d raw files)",
                    out_rel,
                    len(included),
                )
                continue

            stats["raw_files_failed"] += len(included)
            parquet_failed_conversions_total.labels(reason="merged_collision").inc()
            logger.error(
                "Refusing to overwrite merged batch with a different source set: %s "
                "(included sources: %s)",
                out_path,
                sources,
            )
            continue

        try:
            row_count = write_merged_parquet_batch(
                out_path,
                tables,
                category,
                source_paths=sources,
            )
        except Exception:
            stats["raw_files_failed"] += len(included)
            parquet_failed_conversions_total.labels(reason="normalize").inc()
            logger.exception("Failed to write merged batch: %s", out_path)
            continue

        for path in included:
            rel_path = rel_by_path[path]
            state.processed_files.add(rel_path)
            state.batches[rel_path] = out_rel

        stats["raw_files_processed"] += len(included)
        stats["rows_written"] += row_count
        written_paths.add(out_path)
        # Counts raw source files, not output files: a merged batch covers
        # every raw file in the chunk while writing one parquet. The metric
        # name and help text are explicit about that (INFO-2 in review).
        parquet_files_processed_total.inc(len(included))

    return False


def normalize_once(*, should_stop: Callable[[], bool] | None = None) -> dict[str, int]:
    batch_started = time.monotonic()
    project_root, data_root, state_path, tokens_path = project_paths()
    mode = resolve_batch_mode()
    state = _load_processed_state(
        project_root=project_root,
        data_root=data_root,
        state_path=state_path,
    )

    token_registry, backfill_outcomes = prepare_polymarket_token_registry(
        project_root=project_root,
        tokens_path=tokens_path,
    )
    if backfill_outcomes:
        invalidate_polymarket_outcome_batches(
            project_root=project_root,
            data_root=data_root,
            state_path=state_path,
            state=state,
        )
        mark_polymarket_outcome_backfill_done(tokens_path)

    stats = {
        "raw_files_seen": 0,
        "raw_files_processed": 0,
        "raw_files_skipped": 0,
        "raw_files_failed": 0,
        "rows_written": 0,
        "parquet_batches_written": 0,
    }
    written_paths: set[Path] = set()
    scanned_paths: set[str] = set()
    interrupted = False

    logger.info("Normalize batch mode: %s", mode)
    collect_backlog_metrics(project_root=project_root, state_path=state_path)

    for source in REGISTERED_SOURCES.values():
        if not source.has_parquet_pipeline:
            continue

        exchange_raw_root = raw_root(project_root, source.exchange)
        for category in source.categories:
            pending: list[tuple[Path, str]] = []
            pending_day: Path | None = None

            for jsonl_path in iter_jsonl_paths(exchange_raw_root, category):
                if should_stop is not None and should_stop():
                    interrupted = True
                    break

                day_dir = jsonl_path.parent
                if pending and day_dir != pending_day:
                    interrupted = _process_pending(
                        pending,
                        mode=mode,
                        should_stop=should_stop,
                        project_root=project_root,
                        data_root=data_root,
                        tokens_path=tokens_path,
                        token_registry=token_registry,
                        state=state,
                        stats=stats,
                        written_paths=written_paths,
                    )
                    pending = []
                    if interrupted:
                        break

                if mode == BATCH_MODE_MERGED and day_dir != pending_day:
                    adopted = recover_orphaned_merged_batches(
                        data_day_dir=batch_parquet_path_from_jsonl(
                            jsonl_path,
                            project_root=project_root,
                            data_root=data_root,
                        ).parent,
                        raw_day_dir=day_dir,
                        project_root=project_root,
                        processed_files=state.processed_files,
                        batches=state.batches,
                    )
                    if adopted:
                        logger.info(
                            "Adopted %d raw files from previously published merged batches in %s",
                            len(adopted),
                            day_dir,
                        )
                pending_day = day_dir

                stats["raw_files_seen"] += 1
                rel_path = relative_to_project(jsonl_path, project_root)
                scanned_paths.add(rel_path)

                if rel_path in state.processed_files:
                    stats["raw_files_skipped"] += 1
                    continue

                out_path = batch_parquet_path_from_jsonl(
                    jsonl_path,
                    project_root=project_root,
                    data_root=data_root,
                )
                if out_path.exists():
                    state.processed_files.add(rel_path)
                    state.batches[rel_path] = relative_to_project(out_path, project_root)
                    stats["raw_files_skipped"] += 1
                    logger.info(
                        "Recovered processed state for existing batch: %s",
                        rel_path,
                    )
                    continue

                pending.append((jsonl_path, rel_path))

            if not interrupted and pending:
                interrupted = _process_pending(
                    pending,
                    mode=mode,
                    should_stop=should_stop,
                    project_root=project_root,
                    data_root=data_root,
                    tokens_path=tokens_path,
                    token_registry=token_registry,
                    state=state,
                    stats=stats,
                    written_paths=written_paths,
                )

            if interrupted:
                break
        if interrupted:
            break

    stats["parquet_batches_written"] = len(written_paths)

    _persist_processed_state(
        state_path=state_path,
        state=state,
        scanned_paths=scanned_paths,
        interrupted=interrupted,
    )

    collect_backlog_metrics(project_root=project_root, state_path=state_path)

    parquet_last_batch_timestamp.set(time.time())
    parquet_last_batch_duration_seconds.set(time.monotonic() - batch_started)
    parquet_last_batch_files_processed.set(stats["raw_files_processed"])

    return stats


def _process_pending(
    pending: list[tuple[Path, str]],
    *,
    mode: str,
    should_stop: Callable[[], bool] | None,
    project_root: Path,
    data_root: Path,
    tokens_path: Path,
    token_registry: object | None,
    state: ProcessedState,
    stats: dict[str, int],
    written_paths: set[Path],
) -> bool:
    if mode == BATCH_MODE_MERGED:
        return _process_merged_pending(
            pending,
            should_stop=should_stop,
            project_root=project_root,
            data_root=data_root,
            tokens_path=tokens_path,
            token_registry=token_registry,
            state=state,
            stats=stats,
            written_paths=written_paths,
        )
    return _process_per_file_pending(
        pending,
        should_stop=should_stop,
        project_root=project_root,
        data_root=data_root,
        tokens_path=tokens_path,
        token_registry=token_registry,
        state=state,
        stats=stats,
        written_paths=written_paths,
    )
