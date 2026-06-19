# AGENTS.md — Pargalı Ibrahim Library

Guidance for AI coding agents working in this repository.

---

## Project summary

**BTC Intelligence Platform (BTC Suite)** — self-hosted, single-node, BTC-only research system.

Five layers: **Raw → Normalized → Derived → Knowledge → Agents**

Data sources today:
- **Binance USDT-M perp** — `BTCUSDT` (trades, liquidations, orderbook)
- **Polymarket** — `BTC_15M` 15-minute Up/Down (direct CLOB WebSocket)

Phases 1–3 plus Polymarket, derived metrics, knowledge, and agent v1 are implemented. Do not rewrite collection or conversion semantics unless explicitly requested.

---

## Repository map

| Path | Role |
|------|------|
| `src/datasets.py` | Central exchange/category registry (`REGISTERED_SOURCES`) |
| `src/collectors/` | Adapter layer (`base.py`, `binance.py`, `polymarket.py`) |
| `src/collect_raw.py` | Binance UBWA → `raw/` |
| `src/run_collector.py` | Wrapper + `:9100` metrics |
| `src/collect_orderbook.py` | BTC orderbook → `raw/` |
| `src/run_orderbook_collector.py` | Wrapper + `:9102` metrics |
| `src/collect_polymarket.py` | Polymarket CLOB WS → `raw/` |
| `src/run_polymarket_collector.py` | Wrapper + `:9103` metrics |
| `src/parse_raw.py` / `src/parse_polymarket.py` | JSONL → row dicts |
| `src/convert_parquet.py` | Multi-exchange parquet + pollers on `:9101` |
| `src/derive_polymarket.py` | PM derived metrics (Tier 1–2) |
| `src/derive_cross_market.py` | PM–perp divergence (Tier 3) |
| `src/knowledge/btc_suite.py` | Knowledge scores (Layer 4) |
| `src/agents/btc_analyst.py` | Rule-based analyst (Layer 5) |
| `src/run_agent.py` | Agent CLI (`brief`, `ask`) |
| `src/polymarket/discovery.py` | Gamma API — active BTC 15m market |
| `src/metrics/` | Prometheus registries (`mdp_*`) |
| `src/catalog/` | DuckDB schema, sync, AI metadata hooks |
| `monitoring/` | Docker Compose + provisioning |
| `infra/monitoring/grafana/dashboards/` | Dashboard JSON source of truth |
| `catalog/config.yaml` | Catalog DB path and data roots |
| `monitoring/alerts/config.yaml` | Alert threshold reference (not loaded at runtime) |

---

## Runtime processes

Four Python processes + Docker for full observability:

```bash
./venv/bin/python -u src/run_collector.py               # :9100
./venv/bin/python -u src/run_orderbook_collector.py     # :9102
./venv/bin/python -u src/run_polymarket_collector.py      # :9103
./venv/bin/python -u src/convert_parquet.py --loop        # :9101
cd monitoring && docker compose up -d                     # Grafana :3001
```

Agent (on-demand, no port):

```bash
PYTHONPATH=src ./venv/bin/python src/run_agent.py brief
```

Run Python with `PYTHONPATH=src` or from project root using `./venv/bin/python src/...` (imports use `from metrics...`, `from catalog...`).

---

## Data layout

```
raw/{exchange}/{category}/{date}/{time}.jsonl
data/{category}/{exchange}/{symbol}/{day}.parquet
```

Registry: `src/datasets.py` — add new sources/categories here first.

---

## Adding a new data source (checklist)

Every new category must flow through **all** layers:

```text
collect → raw → parse → parquet → catalog → coverage/quality → metrics → Grafana → docs
```

1. **Registry** — `src/datasets.py` (`REGISTERED_SOURCES`, categories)
2. **Collect** — adapter + `collect_*.py` + `run_*_collector.py` + dedicated metrics port
3. **Raw** — JSONL under `raw/{exchange}/{category}/`, 60 s flush pattern
4. **Parse** — `parse_*.py` row schema
5. **Convert** — `convert_parquet.py` column map + `REGISTERED_SOURCES` routing
6. **Catalog** — auto via `sync.py` if parquet exists under `data/`
7. **Scanners** — `coverage_scanner.py` / `quality_validator.py` use `discover_metric_datasets()` (exchange-aware)
8. **Prometheus** — scrape target in `monitoring/prometheus/prometheus.yml`
9. **Grafana** — dashboard JSON in `infra/monitoring/grafana/dashboards/`, recreate Grafana
10. **Alerts** — `monitoring/grafana/provisioning/alerting/rules.yaml` + `monitoring/alerts/config.yaml`
11. **Docs** — update `README.md` and this file only (no extra `.md` files)

For Polymarket-style non-UBWA sources: implement health in adapter (reconnect, resubscribe, stall detection) — see `src/collectors/polymarket.py`.

---

## Conventions

- **Imports:** top of file only; no inline imports unless circular dependency is documented.
- **TypeScript-style switches:** use `never` in default case for exhaustive unions (where applicable).
- **Metrics naming:** prefix `mdp_`; labels use snake_case.
- **Grafana stat panels:** Prometheus gauges need `instant: true, range: false`.
- **Multi-query tables:** use `label_join(..., "dataset", "/", "category", "symbol")` + `joinByField` on `dataset`.
- **Pollers:** 300 s interval inside `convert_parquet.py`; do not add duplicate background threads without reason.
- **Parquet catch-up:** never run `convert_once()` concurrently with `convert_parquet.py --loop` (append race). See README.
- **Commits:** only when the user asks. Never force-push to main.

---

## Do not change without explicit request

- Phase 1/2 core collection and parquet write semantics (`collect_raw.py`, core conversion in `convert_parquet.py`)
- Binance / Polymarket symbol scope unless user asks (BTC-only is intentional)
- Running terminals the user did not ask to restart

Safe to change when asked: monitoring dashboards, alert thresholds, catalog sync, derived/knowledge metrics, agent templates, documentation.

---

## Grafana specifics

- **Local OSS only** at `http://localhost:3001` — not Grafana Cloud.
- Datasource UID: `prometheus`
- Dashboard folder: **Market Data Platform**
- Do **not** add `GF_INSTALL_PLUGINS=frser-duckdb-datasource` — plugin 404 crashes Grafana.
- After provisioning changes: `docker compose up -d grafana --force-recreate` from `monitoring/`.

Key dashboards: `overview.json`, `market-intelligence.json`, `polymarket-collector-health.json`

---

## Key metrics (alerting)

| Alert | PromQL sketch |
|-------|----------------|
| Stream unrepairable | `increase(mdp_ubwa_stream_signal_total{signal="STREAM_UNREPAIRABLE"}[5m])` |
| Collector down | `mdp_up{service="market-data-platform-collector"} < 1` |
| Orderbook collector down | `mdp_up{service="market-data-platform-orderbook"} < 1` |
| Polymarket collector down | `mdp_up{service="market-data-platform-polymarket"} < 1` |
| Converter down | `mdp_up{service="market-data-platform-converter"} < 1` |
| Buffer capacity | `buffer_length / buffer_maxlen > 0.85` |
| Parquet failed | `increase(mdp_pipeline_parquet_failed_conversions_total[15m])` |
| Catalog stale | `time() - mdp_pipeline_catalog_last_sync_timestamp_seconds > 900` |
| Disk high | `mdp_storage_used_percent > 85` |
| Coverage low | `min(mdp_coverage_percent{category="trades"}) < 95` |
| Schema change | `increase(mdp_catalog_schema_changes_total[15m])` |

Derived / cross / knowledge gauges export from **converter** (`:9101`).

---

## Gitignored (never commit)

`venv/`, `raw/*`, `data/*`, `state/*`, `catalog/*.duckdb`, `monitoring/.env`, `.agents/`, `skills-lock.json`

---

## Testing changes

```bash
PYTHONPATH=src ./venv/bin/python -c "from catalog.sync import sync_catalog"
PYTHONPATH=src ./venv/bin/python -c "from derive_polymarket import derive_polymarket_once"
PYTHONPATH=src ./venv/bin/python src/run_agent.py brief
curl -s localhost:9100/metrics | head
curl -s localhost:9101/metrics | rg mdp_cross
docker logs mdp-grafana 2>&1 | rg -i provision
```

---

## Language

**README.md is English** for external users. **AGENTS.md is English** for coding agents. Match the language of the file you edit. Do not add new standalone `.md` files — consolidate into README or AGENTS.
