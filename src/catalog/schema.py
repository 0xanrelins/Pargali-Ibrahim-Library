#!/usr/bin/env python3
"""DuckDB schema for the metadata catalog (Phase 3, Section 6)."""

from __future__ import annotations

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS datasets (
        dataset_id VARCHAR PRIMARY KEY,
        dataset_type VARCHAR NOT NULL,
        exchange VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        path VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_stats (
        dataset_id VARCHAR NOT NULL,
        file_path VARCHAR NOT NULL,
        rows BIGINT NOT NULL,
        file_size BIGINT NOT NULL,
        start_timestamp BIGINT NOT NULL,
        end_timestamp BIGINT NOT NULL,
        PRIMARY KEY (dataset_id, file_path),
        FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS schema_registry (
        dataset_id VARCHAR NOT NULL,
        schema_version INTEGER NOT NULL,
        column_name VARCHAR NOT NULL,
        data_type VARCHAR NOT NULL,
        PRIMARY KEY (dataset_id, schema_version, column_name),
        FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_metadata (
        dataset_id VARCHAR PRIMARY KEY,
        summary VARCHAR NOT NULL DEFAULT '',
        tags VARCHAR NOT NULL DEFAULT '',
        use_cases VARCHAR NOT NULL DEFAULT '',
        indexing_status VARCHAR NOT NULL DEFAULT 'pending',
        FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
    )
    """,
)


def initialize_schema(connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
