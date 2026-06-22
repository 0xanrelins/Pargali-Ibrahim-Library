# Pargalı Ibrahim Library — BTC Intelligence Platform

## Project Goal

Collect raw market data and convert it into immutable normalized parquet batches.

Nothing else.

---

A self-hosted, **BTC-only** collector pipeline: live market data → raw JSONL → immutable normalized parquet batches — with Prometheus metrics and Grafana OSS monitoring.

---

## Pipeline

```
Collector (:9100–9103)
    ↓
raw/          JSONL batches (kept permanently)
    ↓
Normalizer (:9101)   DuckDB writer
    ↓
data/         immutable parquet batches (1 JSONL → 1 parquet)
```

```
Binance WS ──┐
             ├──► raw/ ──► data/
Polymarket ──┘              ↓
                    Prometheus :9100–9103, :9101  →  Grafana :3001
```

Optional future tiers (derived, catalog, agent) live under `archive/tier2/` — not part of the running pipeline.

---

## What it does

| Component | Role |
|-----------|------|
| **Binance collector** (`:9100`) | BTCUSDT agg trades + liquidations |
| **Orderbook collector** (`:9102`) | BTCUSDT depth@100ms + 15 min snapshots |
| **Polymarket collector** (`:9103`) | BTC 15m Up/Down — direct CLOB WebSocket |
| **Normalizer** (`:9101`) | raw JSONL → immutable parquet batches (DuckDB writer) |
| **Monitoring** | 6 Grafana dashboards, Prometheus alerts |

**Symbols:** `BTCUSDT` (Binance), `BTC_15M` (Polymarket)

---

## Requirements

- **Python** 3.11+ (tested on 3.14)
- **Docker** + Docker Compose (monitoring stack only)
- Outbound internet (Binance + Polymarket WebSocket, Docker pulls)
- ~10 GB free disk for sustained operation

No API keys required for public market streams.

---

## Quick start

### 1. Install

```bash
git clone git@github.com:0xanrelins/Pargali-Ibrahim-Library.git
cd Pargali-Ibrahim-Library

python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

Verify:

```bash
./venv/bin/python -c "from unicorn_binance_websocket_api import BinanceWebSocketApiManager; print('UBWA OK')"
./venv/bin/python -c "import pandas, pyarrow, duckdb; print('deps OK')"
```

### 2. Start collectors + normalizer

From project root (four terminals, or background with `&`):

```bash
./venv/bin/python -u src/run_collector.py                  # :9100
./venv/bin/python -u src/run_orderbook_collector.py        # :9102
./venv/bin/python -u src/run_polymarket_collector.py       # :9103
./venv/bin/python -u src/normalize_parquet.py --loop       # :9101
```

Stop any process with `Ctrl+C`.

### 3. Start monitoring

```bash
cd monitoring
cp .env.example .env   # optional — defaults admin/admin
docker compose up -d
```

| Service | URL |
|---------|-----|
| Grafana | http://localhost:3001 |
| Prometheus | http://localhost:9090 |

Open folder **PargaliIbrahim Canvas**. Key dashboards: **Overview**, **Normalizer Health**, **Collector Health**.

---

## Command reference

Project root:

```bash
cd "/path/to/Pargali-Ibrahim-Library"
```

| Task | Command |
|------|---------|
| Parse dry-run (no writes) | `./venv/bin/python src/parse_raw.py` |
| One-shot normalize | `./venv/bin/python src/normalize_parquet.py --once` |
| Normalizer loop | `./venv/bin/python -u src/normalize_parquet.py --loop` |
| Metrics | `curl -s localhost:9100/metrics \| head` (also `:9101`, `:9102`, `:9103`) |
| Recreate Grafana | `cd monitoring && docker compose up -d grafana --force-recreate` |
| Stop monitoring | `cd monitoring && docker compose down` |

**Normalizer catch-up / cutover** — see [AGENTS.md](AGENTS.md) (*Normalizer cutover and catch-up*). Never run two processes on `:9101`.

---

## Project layout

```
├── src/
│   ├── collect_raw.py / run_collector.py          # Binance trades + liq (:9100)
│   ├── collect_orderbook.py / run_orderbook_...   # Orderbook (:9102)
│   ├── collect_polymarket.py / run_polymarket_...# Polymarket (:9103)
│   ├── normalize_parquet.py                       # Normalizer CLI (:9101)
│   ├── normalizer/                                # DuckDB batch writer
│   ├── convert_parquet.py                         # Deprecated → normalize_parquet
│   ├── collectors/                                # Adapter layer
│   ├── metrics/                                   # Prometheus registries
│   ├── archive/tier2/                             # derived, catalog, agent (not run)
├── raw/                     # JSONL (gitignored)
├── data/                    # Parquet (gitignored)
├── monitoring/              # Docker Compose stack
├── infra/monitoring/grafana/dashboards/  # Dashboard JSON source of truth
├── README.md
└── AGENTS.md                # AI agent guidance
```

---

## Data model

**Layout (current):** `data/{category}/{exchange}/{symbol}/{date}/{time}.parquet` — one immutable batch per raw JSONL file.

**Legacy (deprecated):** `data/{category}/{exchange}/{symbol}/{day}.parquet` — daily rollup from old converter.

**Binance (`binance_futures`, symbol `BTCUSDT`):**

| Category | Content |
|----------|---------|
| `trades` | aggTrade |
| `liquidations` | forceOrder |
| `orderbook_diff` | depth@100ms deltas |
| `orderbook_snapshots` | 15 min full book |

**Polymarket (`polymarket`, symbol `BTC_15M`):**

| Category | Content |
|----------|---------|
| `prediction_metadata` | Market slug, token ids, expiry |
| `prediction_price` | best_bid_ask, price_change |
| `prediction_orderbook` | book snapshots |
| `prediction_trades` | last_trade_price |

**Dataset ID:** `{category}:{exchange}:{symbol}` — e.g. `trades:binance_futures:BTCUSDT`

---

## DuckDB queries

```bash
# Trade counts (batch glob)
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute('''
  SELECT symbol, COUNT(*) AS n
  FROM read_parquet('data/trades/binance_futures/BTCUSDT/*/*.parquet')
  GROUP BY 1
''').df())
"
```

Healthy pipeline: trade counts grow over time; normalizer keeps backlog near zero (see **Normalizer Health** dashboard).

---

## Monitoring & alerting

**Dashboards** (folder **PargaliIbrahim Canvas**):

Overview · Normalizer Health · Storage Health · Trades/Liq Collector Health · Orderbook Collector Health · Polymarket Collector Health

Archived (not provisioned): `infra/monitoring/grafana/archive/` — catalog, coverage, quality, AI, market intelligence, legacy converter lifecycle.

Alert rules are provisioned from `monitoring/grafana/provisioning/alerting/`. Threshold reference: `monitoring/alerts/config.yaml`.

Contact points are stubs — configure SMTP or webhook before expecting notifications.

---

## Configuration

| File | Purpose |
|------|---------|
| `monitoring/.env` | Grafana credentials |
| `monitoring/alerts/config.yaml` | Alert thresholds (reference) |
| `src/datasets.py` | Exchange/category registry |

BTC symbol lists: `TARGET_SYMBOLS` / `TRADE_MARKETS` in `collect_raw.py`.

---

## Operational notes

- **No historical backfill** — recording starts when collectors connect.
- **Raw retention** — raw JSONL is kept; the normalizer does not delete raw files.
- **Parquet backlog** — metric `mdp_pipeline_parquet_raw_backlog_files` on `:9101`.
- **Liquidations are sparse** — empty liquidation batches are normal.
- **Prometheus** scrapes host ports via `host.docker.internal` — keep collectors + normalizer running.

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Empty Grafana panels | All four processes up? `curl localhost:9100/metrics` … `:9103` |
| `--once` port in use | Normalizer loop on `:9101` — stop duplicate process |
| Parquet backlog high | AGENTS.md — cutover, reset `processed.json`, batch catch-up |
| PM or DuckDB panels stale | Panels may target legacy paths — see AGENTS.md |

---

## Disclaimer

For research and operational visibility only — not financial advice. You are responsible for exchange terms, rate limits, and infrastructure costs.

---

## License

Private repository — all rights reserved unless otherwise specified by the owner.
