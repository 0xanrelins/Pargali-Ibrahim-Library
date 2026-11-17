"""Normalizer idempotency and optional Polymarket token registry persistence.

Processed state is a versioned JSON document:

``processed_files``
    Every raw JSONL path that has been normalized (project-relative).
``processed_batches``
    ``raw path -> published parquet path`` for entries whose batch is known.
    The mapping is what retention trusts: a raw file is deleted only when its
    recorded batch still exists on disk.

State written by older normalizers has only ``processed_files`` (a flat list)
and no mapping. :func:`migrate_legacy_batch_mappings` upgrades such entries in
memory by re-deriving the historical 1:1 batch path and accepting it only when
that parquet exists, which is exactly the rule the old retention pass used.
The migration is one-way and idempotent: once saved, every entry that has a
batch carries a mapping, and running it again changes nothing.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypedDict

STATE_VERSION = 2


class ProcessedStatePayload(TypedDict):
    version: int
    processed_files: list[str]
    processed_batches: dict[str, str]


class PolymarketTokenState(TypedDict):
    asset_to_outcome: dict[str, str]


@dataclass
class ProcessedState:
    """Processed raw paths plus the parquet batch each one was published into."""

    processed_files: set[str] = field(default_factory=set)
    batches: dict[str, str] = field(default_factory=dict)


def load_processed_state(state_path: Path) -> ProcessedState:
    """Read processed state; legacy flat lists are accepted and map to an empty batch map."""
    if not state_path.exists():
        return ProcessedState()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    files = payload.get("processed_files", [])
    if not isinstance(files, list):
        raise ValueError(f"Invalid processed state file: {state_path}")

    batches_payload = payload.get("processed_batches", {})
    if not isinstance(batches_payload, dict):
        raise ValueError(f"Invalid processed batch map in state file: {state_path}")
    batches = {str(raw): str(batch) for raw, batch in batches_payload.items()}

    # A mapping entry proves the raw file was processed even if a legacy
    # ``processed_files`` list was written by an older reader in between.
    processed_files = {str(item) for item in files} | set(batches)
    return ProcessedState(processed_files=processed_files, batches=batches)


def load_processed_files(state_path: Path) -> set[str]:
    """Backward-compatible view: every processed raw path, mapped or not."""
    return load_processed_state(state_path).processed_files


def migrate_legacy_batch_mappings(
    state: ProcessedState,
    *,
    resolve_batch_rel: Callable[[str], str | None],
) -> int:
    """Derive a batch mapping for legacy processed entries that predate the map.

    ``resolve_batch_rel`` returns the historical 1:1 batch path for a raw path,
    or ``None`` when the path cannot be mapped. The mapping is only added when
    the caller's resolver proves the batch exists; entries without a batch stay
    unmapped so retention keeps refusing to delete them.
    """
    migrated = 0
    for raw_path in state.processed_files:
        if raw_path in state.batches:
            continue
        batch_rel = resolve_batch_rel(raw_path)
        if batch_rel is None:
            continue
        state.batches[raw_path] = batch_rel
        migrated += 1
    return migrated


def _fsync_directory(directory: Path) -> None:
    """Best-effort directory fsync so a rename survives power loss (POSIX only)."""
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


def _write_json_atomic(path: Path, payload: Any) -> None:
    """Serialize to a sibling tmp file, fsync it, then os.replace onto ``path``.

    A crash, kill or full disk leaves the previous file untouched and parseable
    instead of truncating it mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = (path.stat().st_mode & 0o777) if path.exists() else 0o644
    serialized = json.dumps(payload, indent=2) + "\n"

    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    _fsync_directory(path.parent)


def save_processed_state(
    state_path: Path,
    processed_files: set[str],
    batches: Mapping[str, str],
) -> None:
    """Persist processed paths and their batch mapping (mapping pruned to live entries)."""
    retained_batches = {
        raw_path: batch_path
        for raw_path, batch_path in batches.items()
        if raw_path in processed_files
    }
    payload: ProcessedStatePayload = {
        "version": STATE_VERSION,
        "processed_files": sorted(processed_files),
        "processed_batches": dict(sorted(retained_batches.items())),
    }
    _write_json_atomic(state_path, payload)


def save_processed_files(state_path: Path, processed_files: set[str]) -> None:
    """Persist only the processed set (legacy callers; drops any batch mapping)."""
    save_processed_state(state_path, processed_files, {})


def load_polymarket_tokens(state_path: Path) -> dict[str, str]:
    """Load asset_id → outcome map. Empty dict when file is missing (PM registry deferred)."""
    if not state_path.exists():
        return {}

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    mapping = payload.get("asset_to_outcome", {})
    if not isinstance(mapping, dict):
        raise ValueError(f"Invalid polymarket token state file: {state_path}")
    return {str(asset_id): str(outcome) for asset_id, outcome in mapping.items()}


def save_polymarket_tokens(state_path: Path, asset_to_outcome: dict[str, str]) -> None:
    payload: PolymarketTokenState = {
        "asset_to_outcome": dict(sorted(asset_to_outcome.items())),
    }
    _write_json_atomic(state_path, payload)
