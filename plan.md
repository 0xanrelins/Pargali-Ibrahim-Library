# Binance WS → Parquet Veri Pipeline Planı

> Amaç: Binance USDT-M Perp'ten canlı WebSocket verisi almaktan, Parquet arşivine ve UBWA izleme dashboard'una kadar olan süreci planlamak.  
> Not: Bu aşamada kod yazmıyoruz — sadece öğrenme ve planlama.  
> Uygulama referansı: `unicorn-binance-suite` skill (UBWA llms.txt)

---

## Aşamalar

| # | Dosya | Özet |
|---|-------|------|
| 1 | [plan-1-binance-veri-cekme.md](plan-1-binance-veri-cekme.md) | Binance WS → UBWA → `raw/` (ham JSON) |
| 2 | [plan-2-parquet-donusum.md](plan-2-parquet-donusum.md) | `raw/` → parse + filtre + şema → `data/` (Parquet) |
| 3 | [plan-3-ubwa-izleme-dashboard.md](plan-3-ubwa-izleme-dashboard.md) | UBWA monitoring API'leri → İzleme Dashboard |

```
Aşama 1:  Binance WS (/market)  →  UBWA  →  raw/ (ham JSON)
Aşama 2:  raw/  →  parse + filtre + şema  →  data/ (Parquet)
Aşama 3:  UBWA monitoring API'leri  →  İzleme Dashboard
```

---

## Kapsam (v1)

- **Sadece WebSocket** — geçmiş backfill yok
- **Sadece Binance USDT-M Perp** → `exchange="binance.com-futures"`
- Araç: **UBWA** (`unicorn-binance-websocket-api`)
- Kurulum: `pip install unicorn-binance-suite` veya `pip install unicorn-binance-websocket-api`
- Kayıt bağlandığın andan başlar — kopuk süredeki veri geri alınamaz

### Hedef semboller

| Coin | UBWA market (create_stream) | JSON / klasör adı |
|------|---------------------------|-------------------|
| BTC | `btcusdt` | `BTCUSDT` |
| SOL | `solusdt` | `SOLUSDT` |
| XRP | `xrpusdt` | `XRPUSDT` |
| DOGE | `dogeusdt` | `DOGEUSDT` |

### Stream'ler

| Veri | UBWA channel | Abonelik |
|------|--------------|----------|
| Trades | `aggTrade` | 4 |
| Liquidations | `!forceOrder` + `arr` | 1 |
| **Toplam** | | **5** |

---

## Açık Kararlar

- [x] Semboller → **BTC, SOL, XRP, DOGE**
- [x] Stream'ler → **aggTrade, liquidations**
- [x] [Aşama 1](plan-1-binance-veri-cekme.md): raw flush → **her 60 saniyede bir**
- [x] [Aşama 2](plan-2-parquet-donusum.md): dönüşüm → **batch, saatte bir**
- [x] [Aşama 2](plan-2-parquet-donusum.md): araçlar → **pandas + pyarrow** (dönüşüm), **DuckDB** (analiz)
- [ ] [Aşama 3](plan-3-ubwa-izleme-dashboard.md): dashboard + poll → **Aşama 1+2 tamamlandıktan sonra**
- [x] Altyapı → **tek makine, MacBook (lokal)**
