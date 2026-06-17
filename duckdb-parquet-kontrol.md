# Parquet Kontrolü — DuckDB

`data/` altındaki Parquet dosyalarını DuckDB ile hızlıca kontrol etmek için rehber.

Detaylı terminal komutları: [komutlar.md](komutlar.md)

---

## Hazırlık

Proje klasörüne git:

```bash
cd "/Users/0xanrelins/Documents/PargalıIbrahim Library"
```

DuckDB, `venv` içinde kurulu — ayrı kurulum gerekmez.

---

## 1. Trades — sembol başına satır sayısı

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

**Beklenti:**
- Sadece `BTCUSDT`, `SOLUSDT`, `XRPUSDT`, `DOGEUSDT`
- `n` değerleri zamanla artmalı (collector + convert loop çalışıyorsa)

---

## 2. Liquidations — sembol başına satır sayısı

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

**Beklenti:**
- Yine sadece 4 hedef sembol
- Likidasyon az olabilir — bu normal

---

## 3. Örnek satırlar — veri doğru mu?

Son 3 BTC trade:

```bash
./venv/bin/python -c "
import duckdb
print(duckdb.connect().execute(\"\"\"
  SELECT * FROM read_parquet('data/trades/binance_futures/BTCUSDT/*.parquet')
  ORDER BY timestamp DESC LIMIT 3
\"\"\").df())
"
```

**Beklenti:**
- `timestamp`, `price`, `amount`, `side` (buy/sell) dolu
- `exchange` = `binance_futures`

---

## 4. Toplam satır sayısı (hızlı özet)

```bash
./venv/bin/python -c "
import duckdb
con = duckdb.connect()
trades = con.execute(\"SELECT COUNT(*) FROM read_parquet('data/trades/binance_futures/*/*.parquet')\").fetchone()[0]
liqs = con.execute(\"SELECT COUNT(*) FROM read_parquet('data/liquidations/binance_futures/*/*.parquet')\").fetchone()[0]
print(f'trades: {trades}, liquidations: {liqs}')
"
```

---

## Pipeline sağlıklı mı?

| Kontrol | Sağlıklı işaret |
|---------|-----------------|
| Trades sayıları | Artıyor |
| Semboller | Sadece 4 hedef coin |
| Örnek satırlar | Fiyat/miktar/timestamp dolu |
| Convert loop | Saatte bir yeni raw → parquet (veya `--once` ile anında) |

Sayılar artmıyorsa:
1. Terminal 1: `collect_raw.py` çalışıyor mu?
2. Terminal 2: `convert_parquet.py --loop` çalışıyor mu?
3. Bekleyen raw var mı? → `./venv/bin/python src/convert_parquet.py --once`
