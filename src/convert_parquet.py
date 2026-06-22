"""Deprecated entrypoint — use normalize_parquet.py instead.

The legacy daily-rollup converter (append_parquet, derive, catalog pollers) was
replaced by the DuckDB normalizer: 1 raw JSONL batch → 1 immutable parquet batch.
"""

from __future__ import annotations

import logging
import warnings

from normalizer.runner import collect_backlog_metrics, normalize_once

logger = logging.getLogger(__name__)

_DEPRECATION = (
    "convert_parquet.py is deprecated; use normalize_parquet.py "
    "(src/normalize_parquet.py --once | --loop)."
)


def convert_once() -> dict[str, int]:
    warnings.warn(_DEPRECATION, DeprecationWarning, stacklevel=2)
    return normalize_once()


def collect_parquet_backlog_metrics(
    *,
    project_root,
    state_path,
) -> dict[str, int]:
    warnings.warn(_DEPRECATION, DeprecationWarning, stacklevel=2)
    return collect_backlog_metrics(project_root=project_root, state_path=state_path)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.warning(_DEPRECATION)
    from normalize_parquet import main as normalize_main

    return normalize_main()


if __name__ == "__main__":
    raise SystemExit(main())
