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
Normalizer (:9101)   Arrow/parquet writer
    ↓
data/         immutable parquet batches (1 JSONL → 1 file by default)
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
| **Normalizer** (`:9101`) | raw JSONL → immutable parquet batches (Arrow writer) |
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
| Normalizer loop, custom interval | `NORMALIZE_INTERVAL_SEC=3600 ./venv/bin/python -u src/normalize_parquet.py --loop` (default 300s) |
| Normalizer loop, merged batches | `NORMALIZE_BATCH_MODE=merged ./venv/bin/python -u src/normalize_parquet.py --loop` |
| Rollup pass (timer) | `./venv/bin/python src/rollup_parquet.py --run` |
| Rollup rebuild (all history) | `./venv/bin/python src/rollup_parquet.py --backfill` |
| Rollup preview (no writes) | `./venv/bin/python src/rollup_parquet.py --run --dry-run` |
| Rollup hourly retention | add `--hourly-days 7` (default), `D` for the window |
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
│   ├── normalizer/                                # Arrow/parquet batch writer
│   ├── rollup_parquet.py                          # Derived hourly + daily rollups
│   ├── convert_parquet.py                         # Deprecated → normalize_parquet
│   ├── collectors/                                # Adapter layer
│   ├── metrics/                                   # Prometheus registries
│   ├── archive/tier2/                             # derived, catalog, agent (not run)
├── raw/                     # JSONL (gitignored)
├── data/                    # Parquet (gitignored, source of truth)
├── data_rollup/             # Derived rollups (gitignored, regenerable)
├── monitoring/              # Docker Compose stack
├── infra/monitoring/grafana/dashboards/  # Dashboard JSON source of truth
├── README.md
└── AGENTS.md                # AI agent guidance
```

---

## Data model

**Layout (current):** `data/{category}/{exchange}/{symbol}/{date}/{time}.parquet` — one immutable batch per raw JSONL file (default, `NORMALIZE_BATCH_MODE=per_file`).

**Merged layout (opt-in):** `NORMALIZE_BATCH_MODE=merged` writes one batch per turn and category (and per date when a catch-up spans days):

```
data/{category}/{exchange}/{symbol}/{date}/{first}--{last}.parquet
```

`first`/`last` are the `HH-MM-SS` stamps of the first and last raw file in the file, so a name is readable and cannot collide with a per-file batch name (`--` separator). A turn larger than 64 raw files is split into deterministic chunks; a crash between writing the parquet and saving state is recovered on the next turn from the file's footer (`normalizer.raw_sources`) instead of rewriting or duplicating rows. Merged files use `row_group_size=100000` so DuckDB predicate pushdown on `timestamp` keeps working. The normalizer never overwrites an existing batch: a name collision with a different source set is logged as an error and the raw files stay unprocessed.

> **Rollup caveat:** `src/rollup_parquet.py` only recognizes `HH-MM-SS.parquet` source names, so it ignores merged batches until the rollup scanner learns the merged name. That is why merged mode is opt-in and off by default; `data/` itself stays complete.

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

## Rollup layer (`data_rollup/`, derived)

`data/` stays the source of truth: one immutable batch per raw JSONL file means
~1.4k parquet files per dataset per day, so a plain `count(*)` over one day
opens ~1.4k footers. Once the normalizer merged-batch change (PR #10) is merged,
`NORMALIZE_BATCH_MODE=merged` lets a turn publish `{first}--{last}.parquet`
instead, so one file can span several hours; this rollup already reads both
layouts. `src/rollup_parquet.py` writes a read-optimised copy on top:

```
data_rollup/hourly/{category}/{exchange}/{symbol}/{YYYY-MM-DD}/{HH}.parquet
data_rollup/daily/{category}/{exchange}/{symbol}/{YYYY-MM-DD}.parquet
```

| Layer | Content | Retention |
|-------|---------|-----------|
| `hourly` | every row whose UTC **timestamp** falls in one **closed** hour | last `--hourly-days` days (default 7) |
| `daily` | the same day's hourly rollups, merged | all history, written once the day is closed |

Rules:

- **Closed only** — an hour is rolled up 5 minutes after it ends, a day after it
  ends. The newest data is therefore at most ~10 min (timer) + 5 min behind `data/`.
- **Timestamp buckets** — the hour comes from each row's timestamp column
  (`timestamp` in `src/normalizer/schemas.py`, `event_time` as a future fallback),
  never from the file name. A merged batch that spans several hours contributes
  only its own rows to each `HH.parquet`; a per-file batch whose late events
  belong to another hour is split the same way. The source date directory still
  decides the day partition, so a day's rollup never drops or duplicates the rows
  the normalizer filed under that date.
- **Incremental** — an hourly target is written only when it is missing or older
  than its newest contributing source; a daily is also rebuilt whenever one of its
  hourly files is written or removed. A second pass writes nothing. `--backfill`
  ignores freshness and rebuilds the whole layer.
- **Stale hours are removed** — if a re-normalized batch moves every row out of
  an hour, the old `HH.parquet` is deleted (only names matching the rollup
  contract are touched) and the daily is rebuilt from the hours this pass
  actually produces. A planned write that yields zero rows removes its old
  target too, so `--backfill` repairs a day the incremental pass could not.
- **Atomic** — `tmp` file in the target directory + `os.replace`; a killed run
  leaves the previous file intact. Output is `ZSTD` parquet.
- **Same schema as `data/`** — columns are cast to the normalizer contract
  (`src/normalizer/schemas.py`) in contract order. Rows keep the source file
  order (batches are near event-ordered), so a rollup stays near time-ordered and
  parquet statistics still prune time ranges — without paying for a sort pass.
- **Bounded discovery** — a pass reads parquet footers only for day directories
  inside the `--hourly-days` window and for days whose daily file is missing or
  older than its day directory (the normalizer publishes batches with an atomic
  rename, so a new or replaced batch bumps the directory mtime). The rest of
  `data/` is not opened, so discovery does not grow with the full history.
- **Restored in place** — restoring a batch over its old path while preserving
  its mtime does not bump the day directory, so the gate can skip that day. Run
  `--backfill` after an in-place restore.
- **Empty day dirs** — a day with no usable source never gets a daily, so the
  gate cannot close it (~1,028 such directories in production). Each costs one
  `stat` per pass; the effect on a pass is negligible.
- **Empty vs rejected** — a zero-row batch is normal (a quiet flush interval): it
  is counted as `sources_empty` and ignored. A **non-empty** batch that cannot be
  bucketed (unreadable parquet, missing/non-integer timestamp column, implausible
  epoch, or a span over **two days**) counts as `sources_skipped`; the pass logs
  one summary line with the reasons and exits non-zero. A single batch may span
  at most two days, so normalizer merged mode must never produce a longer one.
- **No partial day** — if any source of a day is rejected, that day's daily is
  neither written nor rewritten (an existing daily stays exactly as it is). That
  keeps `daily` equal to every row of the day instead of silently publishing an
  incomplete file. The day's hourly files follow normal retention: outside the
  `--hourly-days` window the hourly directory may still be pruned once a daily
  exists, but the daily itself is kept. Fix or remove the batch; the next pass
  fills the day in.
- **Derived, not history** — `data/`, `raw/` and `state/` are never written. Delete
  `data_rollup/` and rebuild it at any time:

```bash
rm -rf data_rollup && ./venv/bin/python src/rollup_parquet.py --backfill
```

**Timer (operator):** `infra/systemd/pargali-rollup.service` + `.timer` run
`--run` every 10 minutes. Install after merge, from the project root:

```bash
cp infra/systemd/pargali-rollup.service infra/systemd/pargali-rollup.timer /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now pargali-rollup.timer
journalctl -u pargali-rollup.service -n 20
```

Set the `ExecStart`/`WorkingDirectory` paths if the checkout is not at
`/root/Pargali-Ibrahim-Library`. Point new dashboards/queries at `data_rollup/`:

```bash
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute('''
  SELECT count(*) FROM read_parquet('data_rollup/daily/prediction_price/polymarket/BTC_15M/*.parquet')
''').df())
"
```

**Disk cost:** the daily layer is a recompressed copy of `data/` (ZSTD); the
hourly layer adds the last `--hourly-days` days. Measured on the
production dataset after the first backfill: `data/` 7.2 GB →

| Path | Files | Size |
|------|-------|------|
| `data_rollup/daily/` | _pending measurement_ | _pending_ |
| `data_rollup/hourly/` | _pending measurement_ | _pending_ |

Treat it as cache: it costs disk, saves query time.

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
- **Raw retention (optional)** — by default the normalizer does **not** delete raw JSONL. When enabled, retention runs after each normalize batch in `--loop` (evaluation throttled to once per hour after a full run). Deletes raw (Binance and Polymarket) only when the batch recorded in `state/processed.json` still exists and raw is older than 1h; backlog is reported as a metric but never blocks deletion. Skip reasons on `:9101` are distinct: `no_mapping` = no batch in state and no legacy per-file batch on disk, `no_parquet` = state records a batch that is missing. See AGENTS.md for rollout.
- **Parquet backlog** — metric `mdp_pipeline_parquet_raw_backlog_files` on `:9101`.
- **`mdp_pipeline_parquet_files_processed_total` counts raw JSONL source files**, not parquet files: in merged mode one batch covers every raw file of the turn, so compare it with `parquet_batches_written` (logs) rather than with the number of files under `data/`.
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
