# Aşama 2 — Parquet Dönüşümü ve Dosya Düzeni

← [Plan özeti](plan.md) · Önceki: [Aşama 1 — Veri çekme](plan-1-binance-veri-cekme.md) · [Aşama 3 — Dashboard](plan-3-ubwa-izleme-dashboard.md)

> Not: Kod yazmıyoruz — sadece öğrenme ve planlama.

---

## 2.1 Girdi

[Aşama 1](plan-1-binance-veri-cekme.md)'den gelen ham JSON Lines dosyaları (`raw/`)

## 2.2 Dönüşüm Kuralları

- Ham JSON string parse edilir
- Multiplex sarmalayıcı varsa `data` alanı çıkarılır
- Event tipine göre ayrılır: `aggTrade`, `forceOrder`
- Liquidations: sadece hedef semboller filtrelenir (`BTCUSDT`, `SOLUSDT`, `XRPUSDT`, `DOGEUSDT`)
- Normalize için UnicornFy kullanılabilir: `UnicornFy.binance_com_futures_websocket(json_string)`

### Trades (aggTrade) → Parquet


| Kolon          | Tip     | Binance alanı                      |
| -------------- | ------- | ---------------------------------- |
| `timestamp`    | int64   | `T` (trade time)                   |
| `symbol`       | string  | `s`                                |
| `agg_trade_id` | int64   | `a`                                |
| `price`        | float64 | `p`                                |
| `amount`       | float64 | `q`                                |
| `side`         | string  | `m` → `m=true` sell, `m=false` buy |
| `exchange`     | string  | `binance_futures`                  |


### Liquidations → Parquet


| Kolon        | Tip     | Binance alanı     |
| ------------ | ------- | ----------------- |
| `timestamp`  | int64   | `E` (event time)  |
| `symbol`     | string  | `o.s`             |
| `side`       | string  | `o.S`             |
| `price`      | float64 | `o.p`             |
| `avg_price`  | float64 | `o.ap`            |
| `quantity`   | float64 | `o.q`             |
| `filled_qty` | float64 | `o.z`             |
| `exchange`   | string  | `binance_futures` |


## 2.3 Dosya Organizasyonu

```
data/
├── trades/
│   └── binance_futures/
│       ├── BTCUSDT/
│       ├── SOLUSDT/
│       ├── XRPUSDT/
│       └── DOGEUSDT/
│           └── 2026-06-16.parquet
├── liquidations/
│   └── binance_futures/
│       └── ... (aynı 4 sembol)
│           └── 2026-06-16.parquet
```

- Günlük dosya bölme
- Sembol + veri tipi bazlı klasör

## 2.4 Akış

```
[raw/ — ham JSON Lines]
    ↓
[Parse — multiplex sarmalayıcıyı aç]
    ↓
[Event tipine göre ayır — aggTrade / forceOrder]
    ↓
[Liquidations — hedef sembolleri filtrele]
    ↓
[Parquet şemasına dönüştür]
    ↓
[data/ — günlük Parquet dosyaları]
```

## 2.5 Araçlar


| Araç                     | Rol                                             |
| ------------------------ | ----------------------------------------------- |
| `unicorn-fy` (UnicornFy) | `binance_com_futures_websocket()` ile normalize |
| `pandas` + `pyarrow`     | JSON → Parquet dönüşümü                         |
| `duckdb`                 | Parquet sorgulama ve analiz                     |


## 2.6 Başarı Kriterleri

- [ ] Ham JSON → Parquet dönüşümü çalışıyor
- [ ] 2 veri tipi (trades, liquidations) doğru şemada
- [ ] Liquidations doğru sembollere filtreleniyor
- [ ] Günlük dosya yapısı çalışıyor
- [ ] Parquet dosyaları okunabiliyor

## Kararlar (bu aşama)

- [x] Dönüşüm → **batch, saatte bir** (ham JSON toplu okunur → Parquet yazılır)
- [x] Araçlar → **pandas + pyarrow** (dönüşüm), **DuckDB** (analiz / sorgulama)