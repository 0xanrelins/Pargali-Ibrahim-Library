"""Merged batch naming, chunking and crash recovery for ``NORMALIZE_BATCH_MODE=merged``.

One turn normally publishes one parquet file per ``(category, exchange, symbol,
date)``: the rows of every raw JSONL batch processed in that turn, in raw-file
order and row order. Because batches live under a date directory, a catch-up
turn that spans several days still writes one file per day; and to bound peak
memory on a large catch-up the pending list is split into chunks of at most
:data:`MERGED_MAX_FILES_PER_BATCH` raw files, each chunk becoming one file.

Naming
------
``data/{category}/{exchange}/{symbol}/{YYYY-MM-DD}/{first}--{last}.parquet``

``first``/``last`` are the ``HH-MM-SS`` batch stamps of the first and last raw
file included in the file (``--`` separates them so a name is unambiguous and
cannot be confused with a single per-file batch stamp).

Crash recovery
--------------
The writer stores the project-relative raw source paths in the parquet footer
(``normalizer.raw_sources``). A turn writes the parquet, then the state file.
A crash between those two steps leaves a published file that state does not
know about. The next turn scans the affected day directories, reads the footer
of merged files state does not reference, and re-adopts their sources instead
of re-normalizing them into a second file (which would duplicate rows). Files
without that metadata are ignored, never adopted.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Sequence
from pathlib import Path

import pyarrow.parquet as pq

from normalizer.paths import relative_to_project
from normalizer.writer import MERGED_ROW_GROUP_SIZE, MERGED_SOURCES_METADATA_KEY

logger = logging.getLogger(__name__)

MERGED_NAME_SEPARATOR = "--"
MERGED_BATCH_PATTERN = re.compile(
    r"^(?P<first>\d{2}-\d{2}-\d{2})--(?P<last>\d{2}-\d{2}-\d{2})\.parquet$"
)
# Upper bound on raw files per merged file. Five-minute turns carry a handful
# of files; this only caps memory during a large catch-up, where the pending
# list is deterministic and sorted, so chunk boundaries stay stable across runs.
MERGED_MAX_FILES_PER_BATCH = 64


def merged_batch_stamps(paths: Sequence[Path]) -> tuple[str, str]:
    """First and last ``HH-MM-SS`` batch stamps of ``paths`` (already in order)."""
    if not paths:
        raise ValueError("A merged batch needs at least one raw file")
    return paths[0].stem, paths[-1].stem


def merged_batch_path(day_dir: Path, first_stamp: str, last_stamp: str) -> Path:
    return day_dir / f"{first_stamp}{MERGED_NAME_SEPARATOR}{last_stamp}.parquet"


def is_merged_batch_name(name: str) -> bool:
    return MERGED_BATCH_PATTERN.match(name) is not None


def chunk_pending(
    paths: Sequence[Path],
    *,
    max_files: int = MERGED_MAX_FILES_PER_BATCH,
) -> list[list[Path]]:
    """Split an ordered pending list into deterministic merged-batch chunks."""
    if max_files < 1:
        raise ValueError(f"max_files must be >= 1, got {max_files}")
    return [list(paths[index : index + max_files]) for index in range(0, len(paths), max_files)]


def read_merged_sources(path: Path) -> list[str] | None:
    """Raw source paths recorded in a merged parquet footer, or ``None`` if absent."""
    try:
        metadata = pq.read_metadata(path).metadata
    except Exception:
        logger.warning("Could not read parquet metadata from %s", path, exc_info=True)
        return None

    raw_sources = (metadata or {}).get(MERGED_SOURCES_METADATA_KEY)
    if raw_sources is None:
        return None
    try:
        sources = json.loads(raw_sources.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Invalid raw-source metadata in %s", path)
        return None
    if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
        logger.warning("Unexpected raw-source metadata shape in %s", path)
        return None
    return sources


def recover_orphaned_merged_batches(
    *,
    data_day_dir: Path,
    raw_day_dir: Path,
    project_root: Path,
    processed_files: set[str],
    batches: dict[str, str],
) -> list[str]:
    """Adopt merged batches that were published but never recorded in state.

    Scans the data day directory, because that is where merged batches live.
    Returns the raw paths that were recovered. Only files whose footer lists
    source paths inside ``raw_day_dir`` are adopted, and sources already mapped
    to a different batch are left alone, so a hand-copied or foreign parquet
    cannot steal another file's state.
    """
    if not data_day_dir.is_dir():
        return []

    day_rel = relative_to_project(raw_day_dir, project_root)
    day_prefix = f"{day_rel}/"
    known_batches = set(batches.values())
    adopted: list[str] = []

    for path in sorted(data_day_dir.iterdir()):
        if not path.is_file() or not is_merged_batch_name(path.name):
            continue
        batch_rel = relative_to_project(path, project_root)
        if batch_rel in known_batches:
            continue

        sources = read_merged_sources(path)
        if not sources:
            logger.warning(
                "Merged parquet without raw-source metadata was not adopted: %s",
                path,
            )
            continue

        if not sources or not all(source.startswith(day_prefix) for source in sources):
            logger.warning(
                "Merged parquet lists sources outside %s and was not adopted: %s",
                day_rel,
                path,
            )
            continue

        adopted_before = len(adopted)
        for raw_path in sources:
            if raw_path in processed_files:
                if batches.get(raw_path) != batch_rel:
                    logger.warning(
                        "Not adopting %s for %s: raw file already mapped to %s",
                        batch_rel,
                        raw_path,
                        batches.get(raw_path),
                    )
                continue
            processed_files.add(raw_path)
            batches[raw_path] = batch_rel
            adopted.append(raw_path)

        recovered = len(adopted) - adopted_before
        if recovered:
            logger.info(
                "Recovered %d processed raw files from published merged batch: %s",
                recovered,
                batch_rel,
            )

    return adopted


__all__ = [
    "MERGED_BATCH_PATTERN",
    "MERGED_MAX_FILES_PER_BATCH",
    "MERGED_NAME_SEPARATOR",
    "MERGED_ROW_GROUP_SIZE",
    "chunk_pending",
    "is_merged_batch_name",
    "merged_batch_path",
    "merged_batch_stamps",
    "read_merged_sources",
    "recover_orphaned_merged_batches",
]
