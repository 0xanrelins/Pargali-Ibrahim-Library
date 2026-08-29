# AGENTS.md — Pargalı Ibrahim Library

Guidance for AI coding agents working in this repository.

---

## Project summary

**BTC Collector Pipeline** — self-hosted, single-node, BTC-only.

```
Collector → raw JSONL → DuckDB Normalizer → immutable parquet batches
```

Data sources:
- **Binance USDT-M perp** — `BTCUSDT` (trades, liquidations, orderbook)
- **Polymarket** — `BTC_15M` 15-minute Up/Down (direct CLOB WebSocket)

Do not rewrite collection or normalizer semantics unless explicitly requested.

Optional future tiers (derived, knowledge, agent) are under `archive/tier2/` — not loaded at runtime.

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
| `src/normalize_parquet.py` | Normalizer CLI (`--once` / `--loop`) on `:9101` |
| `src/normalizer/` | DuckDB writer, schemas, paths, runner (1 JSONL → 1 parquet) |
| `src/convert_parquet.py` | **Deprecated** — delegates to `normalize_parquet.py` |
| `archive/tier2/` | Derived, knowledge, agent, catalog, coverage, quality (not runtime) |
| `src/polymarket/discovery.py` | Gamma API — active BTC 15m market |
| `src/metrics/` | Prometheus registries (`mdp_*`) |
| `monitoring/` | Docker Compose + provisioning |
| `infra/monitoring/grafana/dashboards/` | Dashboard JSON source of truth |
| `monitoring/alerts/config.yaml` | Alert threshold reference (not loaded at runtime) |

---

## Runtime processes

Four Python processes + Docker for observability:

```bash
./venv/bin/python -u src/run_collector.py               # :9100
./venv/bin/python -u src/run_orderbook_collector.py     # :9102
./venv/bin/python -u src/run_polymarket_collector.py      # :9103
./venv/bin/python -u src/normalize_parquet.py --loop   # :9101
cd monitoring && docker compose up -d                     # Grafana :3001
```

Run Python with `PYTHONPATH=src` or from project root using `./venv/bin/python src/...`.

---

## Data layout

```
raw/{exchange}/{category}/{date}/{time}.jsonl
data/{category}/{exchange}/{symbol}/{date}/{time}.parquet   # immutable batches (current)
data/{category}/{exchange}/{symbol}/{day}.parquet             # legacy daily rollup (deprecated)
```

Registry: `src/datasets.py` — add new sources/categories here first.

---

## Normalizer cutover and catch-up

**Project goal:** collect raw JSONL and write immutable normalized parquet batches — nothing else.

Normalizer state lives in `state/processed.json` (which raw JSONL files were normalized). Parquet batches live under `data/`. By default raw JSONL is **not deleted**. Optional retention (feature-flagged) deletes eligible raw batches on an hourly schedule; see **Raw retention** below.

**Never run two processes on `:9101`** (old converter loop + normalizer loop).

### Raw retention (optional, v1)

Module: `src/normalizer/raw_retention.py`, triggered from `normalize_parquet.py --loop` **after each `normalize_once()` batch**.

**Defaults:** disabled; when enabled, dry-run is true; evaluation interval 3600s (throttle applies only after a full evaluation); grace 1h.

**Scheduling:**

- No independent retention thread.
- After each normalize batch, retention is attempted.
- If backlog > 0, skip and **retry on the next normalize cycle** (~60s) — backlog skips do not start the interval throttle.
- After a full candidate evaluation (dry-run or live delete), wait `interval_sec` before evaluating again.

**Delete only when all gates pass:**

1. Matching batch parquet exists (`batch_parquet_path_from_jsonl`).
2. Raw mtime older than grace period.
3. Global backlog zero (`collect_backlog_metrics`, not Prometheus reads).
4. Source/category has `has_parquet_pipeline=True` (Binance and Polymarket pipeline categories).

`processed.json` is backlog signal only, not deletion proof. Retention applies to Polymarket the same as Binance once the matching batch parquet exists.

**Metrics on `:9101`:** `mdp_raw_retention_would_delete_*` (dry-run), `mdp_raw_retention_files_deleted_total` / `mdp_raw_retention_bytes_freed_total` (live), skip gauges labeled by `reason/exchange/category`, backlog block gauges by `category`.

**Operator rollout (manual restart required):**

1. Enable dry-run:  
   `./venv/bin/python -u src/normalize_parquet.py --loop --raw-retention-enabled`
2. Review logs/metrics for one hour (check skip breakdown and backlog block detail).
3. Live delete: add `--no-raw-retention-dry-run` (operator restarts normalizer manually).

Polymarket raw is included under the same gates after parquet exists. Unprocessed PM raw (including metadata used for token registry seed) remains until normalized.

### Cutover from legacy converter

1. **Stop** `convert_parquet.py --loop` if still running.
2. **Optional:** archive legacy daily rollups: `mv data data_legacy` (creates empty `data/` for batch layout).
3. **Reset** `state/processed.json` if raw JSONL must be re-normalized into batch layout (old converter marked files processed but wrote `{day}.parquet`, not batch paths).
4. Start `./venv/bin/python -u src/normalize_parquet.py --loop`.
5. Verify 1:1 audit: each `raw/.../{time}.jsonl` eventually has `data/.../{date}/{time}.parquet`.

### Manual catch-up (large backlog)

```bash
PYTHONPATH=src ./venv/bin/python -c "
from normalizer.runner import normalize_once, collect_backlog_metrics
from normalizer.paths import project_paths
root, _, state, _ = project_paths()
print('before', collect_backlog_metrics(project_root=root, state_path=state))
print('batch', normalize_once())
print('after', collect_backlog_metrics(project_root=root, state_path=state))
"
```

Or: `./venv/bin/python src/normalize_parquet.py --once`

Check backlog on `:9101`: `mdp_pipeline_parquet_raw_backlog_files`.

### Re-normalize batches (schema fix)

1. Stop `normalize_parquet.py --loop`.
2. Delete target batch parquet files under `data/`.
3. Remove matching paths from `state/processed.json`.
4. Run `--once` or restart `--loop`.

Immutable batches are write-once; there is no append race. Re-run overwrites the same batch path atomically.

### Symptoms

| Symptom | Likely cause |
|---------|----------------|
| Backlog not draining | Normalizer down; check `:9101` / `mdp_up` |
| jsonl processed but no batch parquet | Old `processed.json` from legacy converter — reset state |
| Grafana DuckDB panels stale | Panels may still point at legacy `{day}.parquet` paths — separate sprint |

---

## Adding a new data source (checklist)

Every new category must flow through **all** layers:

```text
collect → raw → parse → normalize → metrics → Grafana → docs
```

1. **Registry** — `src/datasets.py` (`REGISTERED_SOURCES`, categories)
2. **Collect** — adapter + `collect_*.py` + `run_*_collector.py` + dedicated metrics port
3. **Raw** — JSONL under `raw/{exchange}/{category}/`, 60 s flush pattern
4. **Parse** — `parse_*.py` row schema
5. **Normalize** — `src/normalizer/schemas.py` + `normalizer/runner.py`
6. **Prometheus** — scrape target in `monitoring/prometheus/prometheus.yml`
7. **Grafana** — dashboard JSON in `infra/monitoring/grafana/dashboards/`
8. **Alerts** — `monitoring/grafana/provisioning/alerting/rules.yaml`
9. **Docs** — update `README.md` and this file only

For Polymarket-style non-UBWA sources: implement health in adapter (reconnect, resubscribe, stall detection) — see `src/collectors/polymarket.py`. BTC 15m lifecycle: subscribe **15 min pre-window**, wait for **WS `market_resolved`** up to **5 min after window end**, then stop tracking (no Gamma poll in collector) — see `src/polymarket/lifecycle.py`.

---

## Conventions

- **Imports:** top of file only; no inline imports unless circular dependency is documented.
- **TypeScript-style switches:** use `never` in default case for exhaustive unions (where applicable).
- **Metrics naming:** prefix `mdp_`; labels use snake_case.
- **Grafana stat panels:** Prometheus gauges need `instant: true, range: false`.
- **Multi-query tables:** use `label_join(..., "dataset", "/", "category", "symbol")` + `joinByField` on `dataset`.
- **Pollers:** storage metrics scan (300 s) runs inside `normalize_parquet.py --loop` on `:9101`.
- **Parquet catch-up / reset:** see **Normalizer cutover and catch-up** above.
- **Commits:** only when the user asks. Never force-push to main.

---

## Do not change without explicit request

- Phase 1/2 core collection semantics (`collect_raw.py`, collector flush pattern)
- Binance / Polymarket symbol scope unless user asks (BTC-only is intentional)
- Running terminals the user did not ask to restart

Safe to change when asked: monitoring dashboards, alert thresholds, documentation.

Do not re-enable `archive/tier2/` in the normalizer without an explicit request.

---

## Grafana specifics

- **Local OSS only** at `http://localhost:3001` — not Grafana Cloud.
- Datasource UID: `prometheus`
- Dashboard folder: **PargaliIbrahim Canvas**
- Do **not** add `GF_INSTALL_PLUGINS=frser-duckdb-datasource` — plugin 404 crashes Grafana.
- **DuckDB analytics datasource:** use `monitoring/scripts/install_duckdb_plugin.sh` (MotherDuck plugin, unsigned). Grafana image must be `grafana/grafana:*-ubuntu` (not Alpine). Parquet paths in panel SQL: `/data/...` (mounted read-only).
- After provisioning changes: `docker compose up -d grafana --force-recreate` from `monitoring/`.
- **Provisioned dashboards** (`infra/monitoring/grafana/dashboards/*.json`): `allowUiUpdates: false` — edit JSON in repo, not Grafana UI (UI changes are overwritten every 30s).
- **Archived dashboards** (`infra/monitoring/grafana/archive/`): not provisioned; restore to `dashboards/` only if a future tier is re-enabled.
- **Personal dashboards** (e.g. Polymarket Monitor): create/save in Grafana UI only — **never** add a JSON file under `infra/monitoring/grafana/dashboards/`. `disableDeletion: true` keeps them if a file is removed.

Key dashboards: `overview.json`, `pipeline-health.json` (Normalizer Health), `storage-health.json`, collector health dashboards. Archived tier dashboards live under `infra/monitoring/grafana/archive/`.

---

## Key metrics (alerting)

| Alert | PromQL sketch |
|-------|----------------|
| Stream unrepairable | `increase(mdp_ubwa_stream_signal_total{signal="STREAM_UNREPAIRABLE"}[5m])` |
| Collector down | `mdp_up{service="market-data-platform-collector"} < 1` |
| Orderbook collector down | `mdp_up{service="market-data-platform-orderbook"} < 1` |
| Polymarket collector down | `mdp_up{service="market-data-platform-polymarket"} < 1` |
| Normalizer down | `mdp_up{service="market-data-platform-normalizer"} < 1` |
| Buffer capacity | `buffer_length / buffer_maxlen > 0.85` |
| Parquet failed | `increase(mdp_pipeline_parquet_failed_conversions_total[15m])` |
| Disk high | `mdp_storage_used_percent > 85` |

Derived / cross / knowledge are archived under `archive/tier2/` — not exported by the normalizer.

---

## Gitignored (never commit)

`venv/`, `raw/*`, `data/*`, `state/*`, `catalog/*.duckdb`, `monitoring/.env`, `.agents/`, `skills-lock.json`

---

## Testing changes

```bash
PYTHONPATH=src ./venv/bin/python -c "from normalizer.runner import normalize_once; print(normalize_once())"
curl -s localhost:9100/metrics | head
curl -s localhost:9101/metrics | rg mdp_pipeline_parquet
docker logs mdp-grafana 2>&1 | rg -i provision
```

---

## Language

**README.md is English** for external users. **AGENTS.md is English** for coding agents. Match the language of the file you edit. Do not add new standalone `.md` files — consolidate into README or AGENTS.
