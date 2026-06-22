"""Central registry for data sources, exchanges, and dataset layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

BINANCE_FUTURES = "binance_futures"
BINANCE_FUTURES_SYMBOL = "BTCUSDT"
POLYMARKET = "polymarket"
POLYMARKET_SYMBOL = "BTC_15M"
KNOWLEDGE = "knowledge"
KNOWLEDGE_SYMBOL = "BTC_SUITE"

BinanceCategory = Literal[
    "trades",
    "liquidations",
    "orderbook_diff",
    "orderbook_snapshots",
]

BINANCE_FUTURES_CATEGORIES: tuple[BinanceCategory, ...] = (
    "trades",
    "liquidations",
    "orderbook_diff",
    "orderbook_snapshots",
)

ORDERBOOK_CATEGORIES: frozenset[str] = frozenset(
    {"orderbook_diff", "orderbook_snapshots"}
)

PolymarketCategory = Literal[
    "prediction_metadata",
    "prediction_orderbook",
    "prediction_trades",
    "prediction_price",
]

POLYMARKET_CATEGORIES: tuple[PolymarketCategory, ...] = (
    "prediction_metadata",
    "prediction_orderbook",
    "prediction_trades",
    "prediction_price",
)

KnowledgeCategory = Literal["btc_signals"]

KNOWLEDGE_CATEGORIES: tuple[KnowledgeCategory, ...] = ("btc_signals",)

PREDICTION_ORDERBOOK_CATEGORIES: frozenset[str] = frozenset({"prediction_orderbook"})


@dataclass(frozen=True)
class SourceConfig:
    """Registered data source with filesystem layout and pipeline flags."""

    exchange: str
    categories: tuple[str, ...]
    has_parquet_pipeline: bool = True
    derived_categories: tuple[str, ...] = ()


REGISTERED_SOURCES: dict[str, SourceConfig] = {
    BINANCE_FUTURES: SourceConfig(
        exchange=BINANCE_FUTURES,
        categories=BINANCE_FUTURES_CATEGORIES,
        has_parquet_pipeline=True,
    ),
    POLYMARKET: SourceConfig(
        exchange=POLYMARKET,
        categories=POLYMARKET_CATEGORIES,
        has_parquet_pipeline=True,
        derived_categories=("prediction_derived",),
    ),
    KNOWLEDGE: SourceConfig(
        exchange=KNOWLEDGE,
        categories=(),
        has_parquet_pipeline=False,
        derived_categories=KNOWLEDGE_CATEGORIES,
    ),
}


def all_categories() -> tuple[str, ...]:
    """Flat list of every category across registered sources."""
    categories: list[str] = []
    for source in REGISTERED_SOURCES.values():
        categories.extend(source.categories)
    return tuple(categories)


def categories_for_exchange(exchange: str) -> tuple[str, ...]:
    source = REGISTERED_SOURCES.get(exchange)
    if source is None:
        return ()
    return source.categories


def raw_root(project_root: Path, exchange: str) -> Path:
    return project_root / "raw" / exchange


def category_raw_root(project_root: Path, exchange: str, category: str) -> Path:
    return raw_root(project_root, exchange) / category


def exchange_category_root(data_root: Path, exchange: str, category: str) -> Path:
    return data_root / category / exchange


def symbol_parquet_root(
    data_root: Path,
    exchange: str,
    category: str,
    symbol: str,
) -> Path:
    return exchange_category_root(data_root, exchange, category) / symbol


def parquet_path(
    data_root: Path,
    exchange: str,
    category: str,
    symbol: str,
    day: str,
) -> Path:
    """Legacy daily rollup layout: data/{category}/{exchange}/{symbol}/{day}.parquet."""
    return symbol_parquet_root(data_root, exchange, category, symbol) / f"{day}.parquet"


def parquet_batch_path(
    data_root: Path,
    exchange: str,
    category: str,
    symbol: str,
    date: str,
    batch_stamp: str,
) -> Path:
    """Immutable batch layout: data/{category}/{exchange}/{symbol}/{date}/{batch_stamp}.parquet."""
    return (
        symbol_parquet_root(data_root, exchange, category, symbol)
        / date
        / f"{batch_stamp}.parquet"
    )


def dataset_id(exchange: str, category: str, symbol: str) -> str:
    return f"{category}:{exchange}:{symbol}"


def dataset_relative_path(exchange: str, category: str, symbol: str) -> str:
    return f"data/{category}/{exchange}/{symbol}"


def iter_jsonl_paths(raw_root_path: Path, category: str) -> list[Path]:
    category_dir = raw_root_path / category
    if not category_dir.exists():
        return []
    return sorted(category_dir.glob("*/*.jsonl"))


def discover_symbol_dirs(data_root: Path) -> list[tuple[str, str, str]]:
    """Return (exchange, category, symbol) tuples for all parquet datasets."""
    datasets: list[tuple[str, str, str]] = []
    for source in REGISTERED_SOURCES.values():
        categories = list(source.categories)
        categories.extend(source.derived_categories)
        for category in categories:
            category_root = exchange_category_root(data_root, source.exchange, category)
            if not category_root.exists():
                continue
            for symbol_dir in sorted(category_root.iterdir()):
                if symbol_dir.is_dir():
                    datasets.append((source.exchange, category, symbol_dir.name))
    return datasets


def discover_category_symbol_pairs(data_root: Path) -> list[tuple[str, str]]:
    """Return (category, symbol) for metrics that do not label by exchange."""
    return [
        (category, symbol)
        for _exchange, category, symbol in discover_symbol_dirs(data_root)
    ]


def discover_metric_datasets(data_root: Path) -> list[tuple[str, str, str]]:
    """Return (exchange, category, symbol) tuples for coverage and quality scans."""
    return discover_symbol_dirs(data_root)
