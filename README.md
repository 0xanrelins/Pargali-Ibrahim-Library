# Pargalı Ibrahim Library — BTC Intelligence Platform

## Project Goal

Collect raw market data and convert it into immutable normalized parquet batches.

Nothing else.

---

A self-hosted, **BTC-only** research system: live market data ingestion (Binance perp + Polymarket BTC 15m), Parquet normalization, derived cross-venue metrics, knowledge scores, and a local analyst agent — with Prometheus metrics, DuckDB catalog, and Grafana OSS monitoring.

Built for operators who want a reproducible single-node pipeline without proprietary SaaS lock-in.

---

## Five layers

```
Layer 1  RAW           → append-only JSONL under raw/
Layer 2  NORMALIZED   → immutable parquet batches under data/ (1 JSONL → 1 parquet)
Layer 3  DERIVED      → implied prob, PM–perp divergence, liquidity
Layer 4  KNOWLEDGE    → momentum, positioning, fear, conviction scores
Layer 5  AGENTS       → on-demand CLI briefing (catalog + derived + knowledge)
```

```
Binance WS ──┐
             ├──► raw/ ──► data/ (immutable parquet batches)
Polymarket ──┘              ↓
                    Prometheus :9100–9103, :9101  →  Grafana :3001
```

Derived, knowledge, catalog, and agent layers exist in the repo but are **out of scope** for the normalizer process.

---

## What it does

| Component | Role |
|-----------|------|
| **Binance collector** (`:9100`) | BTCUSDT agg trades + liquidations |
| **Orderbook collector** (`:9102`) | BTCUSDT depth@100ms + 15 min snapshots |
| **Polymarket collector** (`:9103`) | BTC 15m Up/Down — direct CLOB WebSocket |
| **Normalizer** (`:9101`) | raw JSONL → immutable parquet batches (DuckDB writer) |
| **Monitoring** | 11 Grafana dashboards, Prometheus alerts |
| **Agent** | `run_agent.py brief` / `ask` — local, no LLM |

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

Open folder **PargaliIbrahim Canvas**. Key dashboards: **Overview**, **BTC Market Intelligence**, **Polymarket Collector Health**.

### 4. Ask the agent

```bash
PYTHONPATH=src ./venv/bin/python src/run_agent.py brief
PYTHONPATH=src ./venv/bin/python src/run_agent.py ask "pm ne diyor"
PYTHONPATH=src ./venv/bin/python src/run_agent.py ask "divergence"
```

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
│   ├── derive_polymarket.py / derive_cross_market.py
│   ├── knowledge/btc_suite.py
│   ├── agents/btc_analyst.py / run_agent.py
│   ├── datasets.py                                # Source registry
│   ├── collectors/                                # Adapter layer
│   ├── metrics/ / catalog/
├── raw/                     # JSONL (gitignored)
├── data/                    # Parquet (gitignored)
├── catalog/catalog.duckdb   # Runtime catalog (gitignored)
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
| `prediction_derived` | implied prob, divergence, liquidity (computed) |

**Knowledge (`knowledge`, symbol `BTC_SUITE`):**

| Category | Content |
|----------|---------|
| `btc_signals` | momentum, positioning, fear, conviction |

**Dataset ID (catalog):** `{category}:{exchange}:{symbol}` — e.g. `trades:binance_futures:BTCUSDT`

---

## DuckDB queries

```bash
# Trade counts
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute('''
  SELECT symbol, COUNT(*) AS n
  FROM read_parquet('data/trades/binance_futures/BTCUSDT/*/*.parquet')
  GROUP BY 1
''').df())
"

# Latest PM derived (Up)
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute('''
  SELECT timestamp, implied_prob, pm_perp_divergence, btc_return_bps
  FROM read_parquet('data/prediction_derived/polymarket/BTC_15M/*.parquet')
  WHERE outcome = 'Up'
  ORDER BY timestamp DESC LIMIT 5
''').df())
"

# Knowledge scores
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute('''
  SELECT * FROM read_parquet('data/btc_signals/knowledge/BTC_SUITE/*.parquet')
  ORDER BY timestamp DESC LIMIT 3
''').df())
"
```

Healthy pipeline: trade counts grow over time; normalizer keeps backlog near zero (see **Pipeline Health** dashboard).

---

## Monitoring & alerting

**Dashboards** (folder **PargaliIbrahim Canvas**):

Overview · Collector Health · Orderbook Collector Health · Polymarket Collector Health · Pipeline Health · Storage Health · Data Coverage · Data Quality · Metadata Catalog · AI Layer · **BTC Market Intelligence**

Alert rules are provisioned from `monitoring/grafana/provisioning/alerting/`. Threshold reference: `monitoring/alerts/config.yaml`.

Contact points are stubs — configure SMTP or webhook before expecting notifications.

---

## Configuration

| File | Purpose |
|------|---------|
| `catalog/config.yaml` | DuckDB path, data roots |
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
