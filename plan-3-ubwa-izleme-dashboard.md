# Aşama 3 — UBWA İzleme, Feedback ve Durum Takibi (Dashboard)

← [Plan özeti](plan.md) · Önceki: [Aşama 1 — Veri çekme](plan-1-binance-veri-cekme.md) · [Aşama 2 — Parquet](plan-2-parquet-donusum.md)

> Not: Kod yazmıyoruz — sadece öğrenme ve planlama.  
> Uygulama referansı: `unicorn-binance-suite` skill (UBWA llms.txt)

---

## 3.1 Amaç

[Aşama 1](plan-1-binance-veri-cekme.md)'deki `BinanceWebSocketApiManager` örneğinden UBWA'nın sağladığı izleme API'lerini periyodik olarak okuyup **UBWA İzleme, Feedback ve Durum Takibi** dashboard'unda görselleştirmek.

- Aşama 1 ile **aynı process** içinde çalışır (paylaşılan `ubwa` instance)
- Ham market verisini kaydetmez — sadece pipeline sağlığını gösterir
- Alert eşikleri (buffer dolması, reconnect, `STREAM_UNREPAIRABLE`) burada tanımlanır

## 3.2 UBWA API'leri — 12 Kategori

UBWA'nın sağladığı tüm bilgi edinme ve izleme araçları — önem sırasına göre. Dashboard panelleri bu kategorilere göre tasarlanır.

### ① Stream yaşam döngüsü sinyalleri — KRİTİK

Bağlantı olaylarını anlık yakalamak için:

```python
def on_signal(signal_type=None, stream_id=None, data_record=None, error_msg=None):
  ...

ubwa = BinanceWebSocketApiManager(
    exchange="binance.com-futures",
    process_stream_signals=on_signal,
    enable_stream_signal_buffer=True,  # sinyalleri buffer'da tut
)
```

| Sinyal | Ne anlama gelir |
|--------|-----------------|
| `CONNECT` | Stream Binance'e bağlandı |
| `FIRST_RECEIVED_DATA` | İlk veri geldi — stream "canlı" |
| `DISCONNECT` | Geçici kopma — UBWA yeniden bağlanır |
| `STOP` | Stream bilinçli olarak durduruldu |
| `STREAM_UNREPAIRABLE` | Kurtarılamaz — stream'i yeniden oluştur |

| Metod | Ne için |
|-------|---------|
| `process_stream_signals` | Sinyal callback'i (önerilen) |
| `enable_stream_signal_buffer=True` | Sinyalleri buffer'da sakla |
| `pop_stream_signal_from_stream_buffer()` | Buffer'dan sinyal oku (FIFO) |
| `send_stream_signal(...)` | Manuel sinyal gönder |

**Dashboard:** canlı sinyal akışı / olay zaman çizelgesi; `STREAM_UNREPAIRABLE` ve `DISCONNECT` için uyarı

### ② Buffer durumu — KRİTİK

Veri tüketim hızını ve kayıp riskini izlemek için:

| Metod | Ne için |
|-------|---------|
| `get_stream_buffer_length()` | Buffer'daki bekleyen mesaj sayısı |
| `get_stream_buffer_byte_size()` | Buffer'ın tahmini byte boyutu |
| `get_stream_buffer_maxlen()` | Buffer max limit (aşılırsa eski veri düşer) |
| `pop_stream_data_from_stream_buffer(mode='FIFO')` | Veri oku — FIFO (default) veya LIFO |
| `clear_stream_buffer()` | Buffer'ı temizle |

**Dashboard:** buffer doluluk göstergesi (mesaj + MB); maxlen'e yaklaşınca uyarı

### ③ Genel sistem özeti — YÜKSEK

Tek bakışta tüm pipeline sağlığı:

| Metod | Ne döner / ne yapar |
|-------|---------------------|
| `get_monitoring_status_plain()` | Dict: `active_streams`, `crashed_streams`, `restarting_streams`, `stopped_streams`, `reconnects`, `uptime`, `stream_buffer_items`, `stream_buffer_mb`, `average_receives_per_second`, `average_speed_per_second`, `total_received_mb` |
| `print_summary()` | Konsola tam durum tablosu yazdır |
| `print_summary_to_png(path)` | Özeti PNG olarak export et |
| `print_stream_info(stream_id)` | Tek stream debug çıktısı |

**Dashboard:** üst özet kartları (`active_streams`, `uptime`, `reconnects`, `total_received_mb`)

### ④ Throughput / veri hızı — YÜKSEK

Veri akışının sağlıklı olup olmadığını anlamak için:

| Metod | Ne için |
|-------|---------|
| `get_current_receiving_speed()` | Son saniyedeki alım hızı (byte/sn) |
| `get_current_receiving_speed_all_streams()` | Tüm stream'ler toplam hız |
| `get_highest_receiving_speed()` | Peak hız kaydı |
| `get_receives_per_second(stream_id)` | Stream başına mesaj/sn |

**Dashboard:** anlık ve ortalama hız grafikleri; stream başına mesaj/sn

### ⑤ Hatalar ve endpoint yanıtları — YÜKSEK

| Metod | Ne için |
|-------|---------|
| `get_errors_from_endpoints()` | Ringbuffer'daki hata mesajları listesi |
| `get_results_from_endpoints()` | Ringbuffer'daki endpoint yanıtları |
| `set_ringbuffer_error_max_size(n)` | Hata ringbuffer boyutu |
| `set_ringbuffer_result_max_size(n)` | Sonuç ringbuffer boyutu |

**Dashboard:** son hatalar listesi; endpoint yanıt logu

### ⑥ Stream detayları — ORTA

Hangi stream'lerin aktif olduğunu görmek için:

| Metod | Ne için |
|-------|---------|
| `get_stream_list()` | Aktif stream ID listesi |
| `get_stream_info(stream_id)` | Tek stream'in tüm bilgisi |
| `get_stream_statistic(stream_id)` | Stream istatistikleri |
| `get_stream_label(stream_id)` | İnsan okunur etiket |
| `get_stream_id_by_label(label)` | Etiket → stream ID |
| `get_stream_subscriptions(stream_id)` | Binance'ten aktif abonelik listesi sorgula |
| `get_result_by_request_id(request_id)` | Abonelik sorgu sonucu |
| `is_stop_request(stream_id)` | Stream durdurulma isteği var mı |
| `is_manager_stopping()` | Manager kapanıyor mu |

**Dashboard:** stream tablosu (ID, etiket, durum, abonelik sayısı)

### ⑦ Reconnect ve uptime — ORTA

| Metod | Ne için |
|-------|---------|
| `get_total_reconnects()` | Toplam reconnect sayısı |
| `get_start_time()` | Manager başlangıç zamanı |

**Dashboard:** uptime sayacı; reconnect sayacı ve geçmişi

### ⑧ Abonelik limitleri — ORTA

5 aboneliğimizin limite yaklaşıp yaklaşmadığını kontrol:

| Metod | Ne için |
|-------|---------|
| `get_limit_of_subscriptions_per_stream()` | Stream başına max abonelik (Binance: 1024) |
| `get_number_of_subscriptions(stream_id)` | Bu stream'deki abonelik sayısı |
| `get_number_of_free_subscription_slots(stream_id)` | Kalan boş slot |
| `get_total_number_of_subscriptions()` | Toplam abonelik |
| `get_number_of_streams()` | Aktif stream sayısı |

**Dashboard:** toplam abonelik (5/1024); stream başına slot kullanımı

### ⑨ UnicornFy — parse doğrulama — ORTA (Aşama 2 ile paylaşımlı)

| Metod | Ne için |
|-------|---------|
| `UnicornFy.binance_com_futures_websocket(json)` | Ham JSON → normalize dict |
| `UnicornFy.is_json(data)` | String geçerli JSON mu |
| `output_default="UnicornFy"` | UBWA'da otomatik normalize (Aşama 1 alternatifi) |

**Dashboard:** opsiyonel — örnek mesaj parse doğrulama paneli (debug)

### ⑩ Veri alma modları — DÜŞÜK (Aşama 1 mimari kararı)

Buffer polling yerine kullanılabilecek alternatifler — dashboard'da bilgi amaçlı:

| Yöntem | Ne zaman |
|--------|----------|
| `pop_stream_data_from_stream_buffer()` | Polling — **bizim plan (Aşama 1)** |
| `process_stream_data` | Sync callback — her mesajda fonksiyon çağrılır |
| `process_stream_data_async` | Async callback |
| `process_asyncio_queue` | En hızlı — asyncio queue |
| `output_default="dict"` | Ham string yerine Python dict |

### ⑪ Process kaynakları — DÜŞÜK

| Metod | Ne için |
|-------|---------|
| `get_process_usage_cpu()` | CPU kullanımı |
| `get_process_usage_memory()` | Bellek kullanımı |
| `get_process_usage_threads()` | Thread sayısı |

**Dashboard:** CPU / bellek / thread mini grafikleri

### ⑫ Versiyon ve güncelleme — DÜŞÜK

| Metod / parametre | Ne için |
|-------------------|---------|
| `is_update_available()` | Yeni UBWA sürümü var mı |
| `get_latest_version()` | Son sürüm numarası |
| `get_latest_release_info()` | Release detayları |
| `warn_on_update=True` | Başlangıçta güncelleme uyarısı (default) |

**Dashboard:** mevcut vs. son sürüm; güncelleme uyarısı

### v1 öncelik sırası — 12 kategori

| Öncelik | # | Kategori | Dashboard paneli | Ana metod |
|---------|---|----------|------------------|-----------|
| KRİTİK | ① | Stream sinyalleri | Olay zaman çizelgesi | `process_stream_signals` |
| KRİTİK | ② | Buffer durumu | Doluluk göstergesi | `get_stream_buffer_length()` |
| YÜKSEK | ③ | Genel sistem özeti | Özet kartları | `get_monitoring_status_plain()` |
| YÜKSEK | ④ | Throughput | Hız grafikleri | `get_current_receiving_speed()` |
| YÜKSEK | ⑤ | Hatalar | Hata listesi | `get_errors_from_endpoints()` |
| ORTA | ⑥ | Stream detayları | Stream tablosu | `get_stream_info()` |
| ORTA | ⑦ | Reconnect / uptime | Uptime + reconnect | `get_total_reconnects()` |
| ORTA | ⑧ | Abonelik limitleri | Abonelik özeti | `get_number_of_subscriptions()` |
| ORTA | ⑨ | UnicornFy parse | Debug (opsiyonel) | `UnicornFy.binance_com_futures_websocket()` |
| DÜŞÜK | ⑩ | Veri alma modları | Bilgi notu | — |
| DÜŞÜK | ⑪ | Process kaynakları | Kaynak grafikleri | `get_process_usage_memory()` |
| DÜŞÜK | ⑫ | Versiyon / güncelleme | Sürüm kartı | `is_update_available()` |

## 3.3 Dashboard Yapısı (taslak)

```
┌─────────────────────────────────────────────────────────┐
│  Özet: uptime · active streams · reconnects · MB alındı │
├──────────────────────┬──────────────────────────────────┤
│  Buffer doluluk      │  Throughput (byte/sn, msg/sn)    │
├──────────────────────┼──────────────────────────────────┤
│  Stream sinyalleri   │  Hatalar / endpoint yanıtları    │
├──────────────────────┴──────────────────────────────────┤
│  Stream tablosu (ID, etiket, abonelik, durum)           │
├─────────────────────────────────────────────────────────┤
│  CPU · bellek · thread  │  UBWA sürümü                  │
└─────────────────────────────────────────────────────────┘
```

- Yenileme: periyodik poll (ör. 1–5 sn) — `get_monitoring_status_plain()` tek çağrıyla çoğu özet alanı
- Sinyaller: `process_stream_signals` callback veya `pop_stream_signal_from_stream_buffer()` ile push

## 3.4 Akış

```
[Aşama 1 — paylaşılan ubwa instance]
    ↓
[Periyodik poll: monitoring metodları]
    ↓
[Callback: stream sinyalleri]
    ↓
[UBWA İzleme Dashboard — canlı görünüm + alertler]
```

## 3.5 Başarı Kriterleri

- [ ] Dashboard Aşama 1 ile aynı process'te çalışıyor
- [ ] 12 kategorinin KRİTİK ve YÜKSEK önceliklileri görünür
- [ ] Buffer doluluk ve reconnect uyarıları tetikleniyor
- [ ] `STREAM_UNREPAIRABLE` olayı dashboard'da belirgin şekilde gösteriliyor
- [ ] 5 abonelik durumu doğrulanabiliyor

## Kararlar (bu aşama)

> Aşama 3'e **Aşama 1 ve 2 tamamlandıktan sonra** girilecek. Şimdilik §3.3 taslak ve UBWA API listesi yeterli.

- [ ] Dashboard teknolojisi — sonra
- [ ] Poll aralığı — sonra
