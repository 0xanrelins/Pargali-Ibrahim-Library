"""Normalizer idempotency and optional Polymarket token registry persistence."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, TypedDict


class ProcessedState(TypedDict):
    processed_files: list[str]


class PolymarketTokenState(TypedDict):
    asset_to_outcome: dict[str, str]


def load_processed_files(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    files = payload.get("processed_files", [])
    if not isinstance(files, list):
        raise ValueError(f"Invalid processed state file: {state_path}")
    return {str(item) for item in files}


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


def save_processed_files(state_path: Path, processed_files: set[str]) -> None:
    payload: ProcessedState = {
        "processed_files": sorted(processed_files),
    }
    _write_json_atomic(state_path, payload)


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
