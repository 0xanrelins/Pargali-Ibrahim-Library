# AGENTS.md — Pargalı Ibrahim Library

Guidance for AI coding agents working in this repository.

---

## Project summary

Self-hosted Binance USDT-M Futures pipeline: **collect raw JSONL → convert to Parquet → expose Prometheus metrics + DuckDB catalog → visualize in local Grafana OSS**.

Phases 1–3 are implemented. Do not rewrite collection or conversion logic unless explicitly requested.

---

## Repository map

| Path | Role |
|------|------|
| `src/collect_raw.py` | Phase 1 core — UBWA WebSocket → `raw/` |
| `src/run_collector.py` | Phase 1 wrapper + `:9100` metrics |
| `src/convert_parquet.py` | Phase 2 conversion + pollers (storage, coverage, quality, catalog) on `:9101` |
| `src/metrics/` | Prometheus registries (`exporter`, `ubwa_*`, `pipeline_*`, `storage_*`, `coverage_*`, `quality_*`, `catalog_*`, `ai_*`) |
| `src/catalog/` | DuckDB schema, sync, AI metadata hooks |
| `monitoring/` | Docker Compose stack + Grafana/Prometheus provisioning |
| `infra/monitoring/grafana/dashboards/` | Dashboard JSON source of truth |
| `catalog/config.yaml` | Catalog DB path and data roots |
| `monitoring/alerts/config.yaml` | Alert threshold reference (not loaded at runtime) |

---

## Runtime processes

Three processes for full observability:

```bash
./venv/bin/python -u src/run_collector.py          # :9100
./venv/bin/python -u src/convert_parquet.py --loop  # :9101
cd monitoring && docker compose up -d               # Grafana :3001
```

Run Python with `PYTHONPATH=src` or from project root using `./venv/bin/python src/...` (imports use `from metrics...`, `from catalog...`).

---

## Conventions

- **Imports:** top of file only; no inline imports unless circular dependency is documented.
- **TypeScript-style switches:** use `never` in default case for exhaustive unions (where applicable).
- **Metrics naming:** prefix `mdp_`; labels use snake_case.
- **Grafana stat panels:** Prometheus gauges need `instant: true, range: false`.
- **Multi-query tables:** use `label_join(..., "dataset", "/", "category", "symbol")` + `joinByField` on `dataset`.
- **Pollers:** 300 s interval inside `convert_parquet.py`; do not add duplicate background threads without reason.
- **Commits:** only when the user asks. Never force-push to main.

---

## Do not change without explicit request

- Phase 1/2 data collection and parquet write semantics (`collect_raw.py`, core conversion in `convert_parquet.py`)
- Binance stream markets list unless user asks to add symbols
- Running terminals the user did not ask to restart

Safe to change when asked: monitoring dashboards, alert thresholds, catalog sync, new metrics, documentation.

---

## Grafana specifics

- **Local OSS only** at `http://localhost:3001` — not Grafana Cloud.
- Datasource UID: `prometheus` (see `monitoring/grafana/provisioning/datasources/datasources.yml`).
- Dashboard folder: **Market Data Platform**.
- Do **not** add `GF_INSTALL_PLUGINS=frser-duckdb-datasource` — plugin 404 crashes Grafana.
- After provisioning changes: `docker compose up -d grafana --force-recreate` from `monitoring/`.

---

## Key metrics (alerting)

| Alert | PromQL sketch |
|-------|----------------|
| Stream unrepairable | `increase(mdp_ubwa_stream_signal_total{signal="STREAM_UNREPAIRABLE"}[5m])` |
| Collector down | `mdp_up{service="market-data-platform-collector"} < 1` |
| Converter down | `mdp_up{service="market-data-platform-converter"} < 1` |
| Buffer capacity | `buffer_length / buffer_maxlen > 0.85` |
| Parquet failed | `increase(mdp_pipeline_parquet_failed_conversions_total[15m])` |
| Catalog stale | `time() - mdp_pipeline_catalog_last_sync_timestamp_seconds > 900` |
| Disk high | `mdp_storage_used_percent > 85` |
| Coverage low | `min(mdp_coverage_percent{category="trades"}) < 95` |
| Schema change | `increase(mdp_catalog_schema_changes_total[15m])` |

---

## Gitignored (never commit)

`venv/`, `raw/*`, `data/*`, `state/*`, `catalog/*.duckdb`, `monitoring/.env`, `.agents/`, `skills-lock.json`

---

## Testing changes

- Python import check: `PYTHONPATH=src ./venv/bin/python -c "from catalog.sync import sync_catalog"`
- Metrics: `curl -s localhost:9100/metrics | head` and `:9101`
- Grafana provisioning logs: `docker logs mdp-grafana 2>&1 | rg -i provision`

---

## Language

User-facing docs may be Turkish (`komutlar.md`, plan files). **README.md is English** for external users. Match the language of the file you edit.
