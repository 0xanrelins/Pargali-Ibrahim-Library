# Terminal Komutları

Bu dosya projede kullanılan terminal komutlarını toplar. Yeni komutlar eklendikçe buraya eklenir.

Proje kök dizini:

```bash
cd "/Users/0xanrelins/Documents/PargalıIbrahim Library"
```

---

## Kurulum

Sanal ortam oluştur ve bağımlılıkları kur:

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

Bağımlılık kontrolü:

```bash
./venv/bin/python -c "from unicorn_binance_websocket_api import BinanceWebSocketApiManager; print('UBWA OK')"
./venv/bin/python -c "import pandas, pyarrow, duckdb; print('pandas', pandas.__version__, 'pyarrow', pyarrow.__version__, 'duckdb', duckdb.__version__)"
```

---

## Plan 1 — Ham veri toplama (UBWA)

Collector'ı başlat (canlı WebSocket → `raw/`):

```bash
./venv/bin/python -u src/collect_raw.py
```

Collector + Prometheus metrikleri (Phase 3 — önerilen):

```bash
./venv/bin/python -u src/run_collector.py
```

Metrik endpoint: http://127.0.0.1:9100/metrics

Durdurmak için: `Ctrl+C`

---

## Plan 2 — Parquet dönüşümü

Parse dry-run (ham JSONL okuma testi, dosya yazmaz):

```bash
./venv/bin/python src/parse_raw.py
```

Tek seferlik dönüşüm (`raw/` → `data/`):

```bash
./venv/bin/python src/convert_parquet.py --once
```

Saatlik döngü (her 3600 sn bir batch):

```bash
./venv/bin/python -u src/convert_parquet.py --loop
```

---

## DuckDB — Parquet sorgulama

Adım adım rehber: [duckdb-parquet-kontrol.md](duckdb-parquet-kontrol.md)

Trades sembol sayıları:

```bash
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute(\"\"\"
  SELECT symbol, COUNT(*) AS n
  FROM read_parquet('data/trades/binance_futures/*/*.parquet')
  GROUP BY 1 ORDER BY 1
\"\"\").df())
"
```

Liquidations sembol sayıları:

```bash
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute(\"\"\"
  SELECT symbol, COUNT(*) AS n
  FROM read_parquet('data/liquidations/binance_futures/*/*.parquet')
  GROUP BY 1 ORDER BY 1
\"\"\").df())
"
```

Örnek trade satırı:

```bash
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute(\"\"\"
  SELECT * FROM read_parquet('data/trades/binance_futures/BTCUSDT/*.parquet') LIMIT 5
\"\"\").df())
"
```

---

## Plan 3 — Monitoring stack (local Grafana OSS)

Stack başlat (Grafana `:3001`, Prometheus `:9090`, Loki `:3100`):

```bash
cd monitoring
docker compose up -d
```

Durdur:

```bash
cd monitoring
docker compose down
```

Durum kontrol:

```bash
docker compose -f monitoring/docker-compose.yml ps
```

Grafana UI (local, ücretsiz — **grafana.com değil**):

```
http://localhost:3001
```

Varsayılan giriş: `admin` / `admin` (veya `monitoring/.env`)

Dashboard JSON (version controlled):

```
infra/monitoring/grafana/dashboards/
```

Metadata catalog config:

```
catalog/config.yaml
catalog/catalog.duckdb   # gitignore — henüz oluşturulmadı
```

Grafana MCP token: `monitoring/grafana-mcp.env.example`
