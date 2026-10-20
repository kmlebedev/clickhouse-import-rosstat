# Рыночные ряды MOEX: Brent, RVI и медь

**Дата:** 2026-10-20
**Статус:** утверждено планом `docs/superpowers/plans/2026-10-20-moex-market.md`
**Пункты роадмапа:** `key-rate/phase-2-brent` (issue [#113](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/113)) → `key-rate/phase-2-moex-market` ([#254](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/254)); `key-rate/phase-2-urals` ([#114](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/114)) — остаётся `Blocked`
**Владелец:** [docs/ROADMAP_KEY_RATE.md](../ROADMAP_KEY_RATE.md) §4.2 №13, §6 (фаза 2)
**Решение владельца:** 2026-10-20 — только MOEX; Urals/VIX/CRB не подменяем другими источниками, разрыв фиксируем в отчёте.

---

## 1. Контекст

Для повторения CB_COMP (issue [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)) нужны рыночные контроли: Brent, Urals, VIX, RVI, медь, CRB. В архиве авторов они лежат в `raw_data_store/market_data/`, но в экспортированную панель `data_panel_bi_sa.fst` не входят: из рыночных индексов там только `usd_rub` (756 строк = 189 месяцев × 4 вида). Модель статьи и режим `full` из #221 используют кредит/ставки/USD/тренд CPI, а не эти шесть рядов. Поэтому:

- для опубликованной модели CB_COMP шесть рядов **не обязательны**;
- для полного сырого паритета MOEX закрывает только три из шести (Brent, RVI, медь);
- Urals, VIX и CRB на MOEX отсутствуют — это фиксируется как разрыв, а не подменяется FRED/архивом.

## 2. Источник: проверено живыми запросами 2026-10-20

| Ряд | Инструмент MOEX ISS | Глубина | Единица |
|---|---|---|---|
| `brent_front` | фьючерс `BR`, `ASSETCODE=BR` | с 2009-10-01 (первый фронт-месяц; `BR-10.09` — с 2009-10-05), ~260 торговых дней на контракт | USD за баррель (`UNIT` контракта) |
| `rvi` | индекс `RVI`, `/iss/engines/stock/markets/index/securities/RVI/candles.json` | с 2013-11-18, 3 252 торговых дня | пункты индекса |
| `copper_front` | фьючерс `COPPER`, `ASSETCODE=COPPER` | с 2024-10-08 (`CEZ4`), ~50 торговых дней у первого контракта | USD за 1 000 кг (USD/т) |

Проверенные эндпоинты:

- текущие контракты: `/iss/engines/futures/markets/forts/securities.json?iss.meta=off&iss.only=securities` — поля `SECID`, `SHORTNAME`, `ASSETCODE`, `LASTTRADEDATE`;
- исторические контракты на дату: `/iss/history/engines/futures/markets/forts/securities.json?iss.meta=off&date=YYYY-MM-DD&assetcode=BR` — фильтр `assetcode` работает; календарные спреды и служебные инструменты приходят с пустым или `null` `ASSETCODE` и отбрасываются;
- история контракта: `/iss/history/engines/futures/markets/forts/securities/<secid>.json?iss.meta=off&from=YYYY-MM-DD&start=<n>` — блоки `history` и `history.cursor`, страницы по 100;
- RVI: `/iss/engines/stock/markets/index/securities/RVI/candles.json?iss.meta=off&interval=24&limit=100&from=YYYY-MM-DD&start=<n>`.

Поля истории: `TRADEDATE`, `SECID`, `CLOSE`, `SETTLEPRICE`, `VOLUME`, `OPENPOSITION`, `SHORTNAME`, `ASSETCODE`. В 2009–2010 у дальних контрактов `CLOSE` может быть `null`, а `SETTLEPRICE` заполнен — поэтому расчётная цена дня приоритетнее закрытия.

### 2.1. Чего на MOEX нет

- **Urals**: `RURIURL` («Индекс Урала», `LATNAME = Moscow Exchange Urals Index`) — это `stock_index` с `ISSUEDATE 2008-01-09`, без `boards`, свечей и истории. Это не цена нефти Urals.
- **VIX**: индекса CBOE VIX нет. `VIX6`/`VIZ6` — фьючерсы на RVI, а не VIX.
- **CRB**: индекса CRB нет ни в `stock/markets/index`, ни в поиске бумаг.
- **Медь до 2024-10**: фьючерс `COPPER` запущен только в 2024; истории 2009–2023 нет.

## 3. Решение

### 3.1. Схема

Новая таблица (канон — `ARCHITECTURE.md` §6.2):

```sql
CREATE TABLE IF NOT EXISTS moex_market (
      series LowCardinality(String)
    , date Date
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (series, date);
```

- `series` — `brent_front` | `rvi` | `copper_front`;
- `date` — торговый день;
- `value` — `SETTLEPRICE` для фьючерсов (USD/барр. или USD/т), `close` для RVI (пункты).

Витрина `v_moex_market`:

```sql
SELECT series, date, value FROM moex_market FINAL
```

Комментарии: это **фронт-месяц фьючерса**, не спот и не back-adjusted continuous; на роллах возможен разрыв. `NULL` в витрине не бывает — отсутствие точки означает, что в этот день не было валидной расчётной цены/закрытия.

### 3.2. Импортёр `moex_market`

- Пакет `moex/`, файл `moex/market.go`; `Name()` = `moex_market` = имя таблицы.
- Регистрация — в новом `moex/importers.go`; персональные `init()` из `ofz_curve.go` и `keyrate.go` убираются (стандарт §8.1).
- `Import()` свой, не `util.ClickHouseImport`: источник — не файл, а набор ISS-запросов; точки собираются в память, затем один `PrepareBatch` → `Append` → `Send`; на ошибке — `Abort`.
- `AfterImport`: `util.UpsertSeriesCatalog` + `util.CreateView(v_moex_market)`.
- Точка контроля публикации не объявляется: источник дневной, URL без версии, no-op по HEAD недостижим (стандарт §9, прецеденты — `cbr_key_rate`, `cbr_zcyc`).

### 3.3. Алгоритм фронт-месяца

Для Brent и меди — общий `futuresSpec{asset, series, start}`:

1. **Окно.** Первый прогон — от `spec.start` (Brent `2009-10-01`, медь `2024-10-01`); повтор — от `max(date) − 400 дней` (перехват роллов и правок).
2. **Обнаружение контрактов.** По одному запросу на месяц: `date=15-е` (если пусто — 14, 13 … 10-е), `assetcode=<asset>`. Плюс текущий список контрактов из `securities.json`. Отбор: `ASSETCODE == asset`, `SHORTNAME` разбирается как `<asset>-<M>.<YY>`; спреды и опционы с пустым/`null` `ASSETCODE` или чужим `SHORTNAME` отбрасываются.
3. **История контракта.** `from=<окно>`, пагинация `start` по 100 до пустой страницы или `TOTAL` из `history.cursor`.
4. **Значение.** `SETTLEPRICE > 0`, иначе `CLOSE > 0`, иначе точка пропускается.
5. **Фронт.** На каждую дату — контракт с минимальным `(year, month)` среди контрактов с валидным значением. Это ближайший по месяцу контракт; точная дата экспирации не нужна.
6. **Запись.** Точки всех рядов — один батч. Повтор перезаписывает те же даты (`ReplacingMergeTree`), `count() FINAL` не меняется.

Для RVI:

1. Первый прогон — с `2013-11-18`; повтор — от `max(date)+1`.
2. `fetchCandles` → `close`.
3. Запись в тот же батч.

### 3.4. Каталог, витрина, MCP

- `moex/series_meta.go`: три записи `source='moex'` — `brent_front`, `rvi`, `copper_front`; `title`, `unit`, `frequency="D"`, `origin`, `description` (ролл, не спот, дата старта).
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_moex_market TO kimi_reader;`.
- `scripts/mcp_check.py`: витрина читается; `series` содержит три ряда; `count() > 0`; крайние даты непустые.
- `dagu/moex.yaml`: шаг `moex_market` в хвосте цепочки, после `keyrate_futures`, retry 3 × 300 с.

## 4. Тесты

- `market_parse_test.go`:
  - `TestParseContractMonth` — `BR-10.09`, `COPPER-12.24`, мусор;
  - `TestParseFuturesHistory` — реальная фикстура: спреды отброшены, `null CLOSE` + `SETTLEPRICE`, объём/позиция;
  - `TestFrontMonthPoints` — два контракта, ближайший месяц, fallback `settle → close`, пропуск нулей, ролл;
  - `TestParseFuturesSecids` — фильтр `ASSETCODE`/`SHORTNAME` (опционы и спреды), `nil` ASSETCODE, дедуп.
- `market_test.go` (`httptest`, подмена `moexBaseUrl`):
  - пагинация истории (`from`, `start`, `limit`);
  - обнаружение по `date` и текущий список;
  - RVI-свечи (`from`, `interval=24`, пагинация).
- `moex/importers_test.go` — литеральный список четырёх импортёров.
- `moex/moex_test.go` — `TestSeriesMetaCoversAllSeries` на три новых ряда.
- Фикстуры — реальные ответы ISS, усечённые до 3–5 строк.

## 5. Приёмка

```bash
make import STAT=moex_market
make import STAT=moex_market   # повтор
make mcp-check
make all
dagu validate dagu/moex.yaml
make docs-check
```

Ожидаемо:
- Brent: 3 916 точек, `2009-10-01 … 2026-10-19` (живая приёмка 2026-10-20);
- RVI: 3 252 точки, `2013-11-18 … последний торговый день` (второй PR);
- медь: ~500 точек, `2024-10-08 … последний торговый день` (второй PR);
- повтор — `count() FINAL` не изменился;
- MCP: `SELECT series, count(), min(date), max(date) FROM v_moex_market GROUP BY series ORDER BY series` — три ряда.

## 6. Ограничения

- Brent/медь — расчётные фьючерсы, не спот; фронт-месяц не является back-adjusted рядом. Для CB_COMP это прокси, а не паритет с вендорским рядом авторов.
- Медь не покрывает train CB_COMP 2010–2018; RVI не покрывает 2010–2013.
- Urals/VIX/CRB на MOEX отсутствуют; #114 остаётся `Blocked`, VIX/CRB — разрыв в [аудите](../research/2026-10-20-moex-market-series-audit.md).
- `date=`-обнаружение может пропустить контракт, который не торговался в выбранный день; окно 400 дней и текущий список компенсируют, пропуск виден в логе.
- Первый бэкфилл Brent — ~200 контрактов, 2–3 минуты; ежедневный прогон — 400-дневное окно (~20 контрактов).

## 7. Вне объёма

- FRED `DCOILBRENTEU`/`VIXCLS`/`PCOPPUSDM`, архив авторов, любые другие источники.
- VIX/CRB как live-ряды.
- Изменение #114 (только комментарий и статус `Blocked`).
