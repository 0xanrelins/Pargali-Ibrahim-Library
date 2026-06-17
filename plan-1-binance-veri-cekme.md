# Aşama 1 — Binance'ten Veri Çekme

← [Plan özeti](plan.md) · Sonraki: [Aşama 2 — Parquet](plan-2-parquet-donusum.md) · [Aşama 3 — Dashboard](plan-3-ubwa-izleme-dashboard.md)

> Not: Kod yazmıyoruz — sadece öğrenme ve planlama.  
> Uygulama referansı: `unicorn-binance-suite` skill (UBWA llms.txt)

---

## Kapsam (bu aşama)

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

- `create_stream` market'leri: **küçük harf**, slash yok
- JSON içindeki sembol: **büyük harf** (`BTCUSDT`)

---

## 1.1 Bağlantı

```python
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

with BinanceWebSocketApiManager(exchange="binance.com-futures") as ubwa:
    # stream oluştur, buffer'dan oku
    ...
```

- API key gerekmez (public market data)
- `with` context manager kullan — çıkışta `stop_manager()` otomatik çağrılır
- Veri varsayılan olarak **ham JSON string** gelir (`output_default="raw_data"`)
- Buffer'dan okuma: `ubwa.pop_stream_data_from_stream_buffer()`
- Auto-reconnect built-in — manuel retry gerekmez
- İzleme ve durum takibi → bkz. [Aşama 3](plan-3-ubwa-izleme-dashboard.md)

## 1.2 Dinlenecek Stream'ler

| Veri | UBWA channel | Abonelik sayısı |
|------|--------------|-----------------|
| Trades | `aggTrade` | 4 (sembol başına) |
| Liquidations | `!forceOrder` + market `arr` | 1 (tüm market) |

**Toplam: 5 abonelik** (UBWA: markets × channels = abonelik)

> Binance USDT-M futures'ta trade stream'i `aggTrade`'dir — `trade` değil.

### Liquidations — tek kanal

Binance tüm coinlerin likidasyonlarını tek stream'de gönderir: `!forceOrder@arr`  
UBWA: `markets="arr"` kullan — `arr`'ı channel listesine ekleme.  
[Aşama 2](plan-2-parquet-donusum.md)'de hedef semboller filtrelenir (BTC, SOL, XRP, DOGE).

### Abonelik

```python
# Trades (sembol başına) — 4 abonelik
ubwa.create_stream(
    channels='aggTrade',
    markets=['btcusdt', 'solusdt', 'xrpusdt', 'dogeusdt']
)

# Likidasyonlar (tüm market) — 1 abonelik
ubwa.create_stream(channels='!forceOrder', markets='arr')
```

## 1.3 Çıktı — Ham Veri

Buffer'dan gelen her mesaj ham JSON string olarak kaydedilir.

```
raw/
└── binance_futures/
    ├── trades/
    └── liquidations/
        └── 2026-06-16/
            └── *.jsonl
```

- Format: JSON Lines (her satır bir event)
- Multiplex stream'lerde payload `{"stream":"...", "data":{...}}` sarmalayıcısı içinde gelir — parse sırasında `data` alınır
- Event tipi (`data.e`): `aggTrade`, `forceOrder`
- UBWA `/market` endpoint routing'i otomatik halleder (aggTrade, forceOrder → `/market`)

## 1.4 Bilinen Kısıtlar

### Bağlantı (UBWA halleder)

- Auto-reconnect (sınırsız)
- Maintenance window handling
- Ping/pong otomatik (varsayılan 20 sn)

### İzlenmesi gerekenler

| Risk | Ne olur | Ne yap |
|------|---------|--------|
| Buffer dolması | Veri kaybı | [Aşama 3](plan-3-ubwa-izleme-dashboard.md) dashboard'da izle; alert kur |
| Kopuk süre | O aralıktaki veri yok | Aşama 3'te logla, alert kur |
| `STREAM_UNREPAIRABLE` | Stream kurtarılamaz | Aşama 3'te görünür kıl; yeniden oluştur |
| Script crash | Kayıt durur | systemd/cron ile auto-restart |

### Veri davranışları

| Veri | Davranış |
|------|----------|
| Liquidations | Tek kanal — tüm coinler gelir, filtre Aşama 2'de |
| Liquidations | Likidasyon yoksa mesaj gelmez |
| Liquidations | Binance 1000ms'de sembol başına sadece en büyük likidasyonu gönderir |
| aggTrade | 100ms'de aynı fiyat/yön birleştirilmiş trade |

## 1.5 Akış

```
[Binance Futures WebSocket — /market]
    ↓
[UBWA — auto-reconnect + stream_buffer]
    ↓
[pop_stream_data_from_stream_buffer() — ham JSON string]
    ↓
[Ham JSON → raw/ klasörüne yaz]
```

## 1.6 Başarı Kriterleri

- [ ] `binance.com-futures` WS'e bağlanılabiliyor
- [ ] 5 abonelik aktif (4 aggTrade + 1 likidasyon)
- [ ] Ham JSON dosyaları `raw/` altına yazılıyor
- [ ] Bağlantı kopunca auto-reconnect çalışıyor
- [ ] Buffer overflow olmuyor

## Kararlar (bu aşama)

- [x] Raw flush → **her 60 saniyede bir** (`raw/` dosyasına zaman bazlı yazım)
