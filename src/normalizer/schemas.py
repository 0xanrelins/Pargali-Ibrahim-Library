"""Normalized parquet column contracts and DuckDB DDL per category."""

from __future__ import annotations

from typing import Final

ColumnSpec = tuple[str, str]

TRADES: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("agg_trade_id", "BIGINT"),
    ("price", "DOUBLE"),
    ("amount", "DOUBLE"),
    ("side", "VARCHAR"),
    ("exchange", "VARCHAR"),
)

LIQUIDATIONS: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("side", "VARCHAR"),
    ("price", "DOUBLE"),
    ("avg_price", "DOUBLE"),
    ("quantity", "DOUBLE"),
    ("filled_qty", "DOUBLE"),
    ("exchange", "VARCHAR"),
)

ORDERBOOK_DIFF: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("event_time", "BIGINT"),
    ("transaction_time", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("first_update_id", "BIGINT"),
    ("final_update_id", "BIGINT"),
    ("previous_final_update_id", "BIGINT"),
    ("side", "VARCHAR"),
    ("price", "DOUBLE"),
    ("quantity", "DOUBLE"),
    ("exchange", "VARCHAR"),
)

ORDERBOOK_SNAPSHOTS: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("last_update_id", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("side", "VARCHAR"),
    ("level_index", "BIGINT"),
    ("price", "DOUBLE"),
    ("quantity", "DOUBLE"),
    ("exchange", "VARCHAR"),
)

PREDICTION_METADATA: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("event_type", "VARCHAR"),
    ("slug", "VARCHAR"),
    ("condition_id", "VARCHAR"),
    ("question", "VARCHAR"),
    ("end_date", "VARCHAR"),
    ("yes_token_id", "VARCHAR"),
    ("no_token_id", "VARCHAR"),
    ("winning_outcome", "VARCHAR"),
    ("winning_asset_id", "VARCHAR"),
    ("resolved_source", "VARCHAR"),
    ("exchange", "VARCHAR"),
)

PREDICTION_PRICE: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("event_type", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("outcome", "VARCHAR"),
    ("best_bid", "DOUBLE"),
    ("best_ask", "DOUBLE"),
    ("spread", "DOUBLE"),
    ("price", "DOUBLE"),
    ("side", "VARCHAR"),
    ("exchange", "VARCHAR"),
)

PREDICTION_TRADES: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("outcome", "VARCHAR"),
    ("price", "DOUBLE"),
    ("side", "VARCHAR"),
    ("size", "DOUBLE"),
    ("exchange", "VARCHAR"),
)

PREDICTION_ORDERBOOK: Final[tuple[ColumnSpec, ...]] = (
    ("timestamp", "BIGINT"),
    ("symbol", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("outcome", "VARCHAR"),
    ("side", "VARCHAR"),
    ("level_index", "BIGINT"),
    ("price", "DOUBLE"),
    ("quantity", "DOUBLE"),
    ("exchange", "VARCHAR"),
)

CATEGORY_SPECS: dict[str, tuple[ColumnSpec, ...]] = {
    "trades": TRADES,
    "liquidations": LIQUIDATIONS,
    "orderbook_diff": ORDERBOOK_DIFF,
    "orderbook_snapshots": ORDERBOOK_SNAPSHOTS,
    "prediction_metadata": PREDICTION_METADATA,
    "prediction_price": PREDICTION_PRICE,
    "prediction_trades": PREDICTION_TRADES,
    "prediction_orderbook": PREDICTION_ORDERBOOK,
}


def normalized_categories() -> tuple[str, ...]:
    return tuple(CATEGORY_SPECS.keys())


def category_columns(category: str) -> tuple[str, ...]:
    specs = _require_category(category)
    return tuple(name for name, _type in specs)


def category_column_specs(category: str) -> tuple[ColumnSpec, ...]:
    return _require_category(category)


def create_empty_table_sql(category: str, *, table_name: str = "normalized") -> str:
    specs = _require_category(category)
    columns_sql = ", ".join(f"{name} {col_type}" for name, col_type in specs)
    return f"CREATE TABLE {table_name} ({columns_sql})"


def insert_typed_select_sql(
    category: str,
    *,
    source_table: str = "rows",
    target_table: str = "normalized",
) -> str:
    specs = _require_category(category)
    select_exprs = ", ".join(
        f"TRY_CAST({name} AS {col_type}) AS {name}" for name, col_type in specs
    )
    column_list = ", ".join(name for name, _type in specs)
    return (
        f"INSERT INTO {target_table} ({column_list}) "
        f"SELECT {select_exprs} FROM {source_table}"
    )


def _require_category(category: str) -> tuple[ColumnSpec, ...]:
    try:
        return CATEGORY_SPECS[category]
    except KeyError as exc:
        known = ", ".join(sorted(CATEGORY_SPECS))
        raise ValueError(f"Unknown normalized category {category!r}; known: {known}") from exc
