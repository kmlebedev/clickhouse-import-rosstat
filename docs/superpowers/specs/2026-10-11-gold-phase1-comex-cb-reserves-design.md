# Дизайн: Фаза 1 контура золота — цена COMEX (`comex_front`) и резервы ЦБ (IMF RA_GOLD)

**Дата:** 2026-10-11
**Статус:** на ревью
**Пункт роадмапа:** [GOLD_THESIS_DATA_ROADMAP.md](../../GOLD_THESIS_DATA_ROADMAP.md) §2, Фаза 1 «datasource → контур»
**Связанные документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6, [спека gold-nav](2026-10-10-gold-nav-plugin-design.md), [статья «Золото Q3-2026»](../../Золото_Q3-2026_итоги_и_прогноз_Q4-2026.md)

---

## 1. Замысел и критерии успеха

**Что закрывает.** «Пол» $4 000 сейчас тестируется только по MOEX-прокси (`gold_prices.venue='moex_fix_usd'` — производная цена с лагом выходных), золотые резервы ЦБ (тезисы «Польша +98 т, Китай +80 т YTD») в контуре отсутствуют. Фаза 1 добавляет два ряда, добываемых сессионно через kimi-datasource:

- **цена золота COMEX front-month (`GC=F`)** → `gold_prices.venue = 'comex_front'` (venue в DDL предусмотрен заранее);
- **золотые резервы 7 центробанков** (IMF IRFCL, индикатор `RA_GOLD`; POL, CHN, RUS, UZB, KAZ, CZE, TUR) → `macro_series.source = 'imf'`.

**Критерии приёмки (из роадмапа):**
1. «Пол» $4 000 тестируется по `comex_front`, не только по MOEX-прокси.
2. Дельты резервов ЦБ читаются из MCP одним запросом.
3. Регламент сессионного обновления зафиксирован в роадмапе (runbook Фазы 1).
4. `make all` зелёный; агент видит новый ряд через витрину MCP.

**Не цель (YAGNI):** конверсия резервов в тонны и витрина дельт `v_cb_gold_dynamics` — пункт §3 роадмапа; переключение входов DCF `v_model_inputs` на `comex_front` для приёмки не требуется.

## 2. Что проверено (фактическая база, 2026-10-11)

| Факт | Проверка |
|---|---|
| `GC=F` (COMEX, $/oz, дневные OHLCV) доступен через datasource; история — до 2 лет | живая проба: close 2026-10-09 = $4 216,3 |
| `v_gold_prices` venue-agnostic — отдаёт все venue как `(source='gold', series=venue, date, value)`; новая площадка появится в витрине без правок select | `gold/series_meta.go`, ARCHITECTURE §6.3 |
| `gold_prices` DDL создаётся только импортёром `gold`; ingest его не заводит | `gold/gold.go`, `ingest/schema.go` |
| RA_GOLD — «monetary gold» IRFCL в **млн USD (рыночная стоимость)**, месячная частота, лаг ~2 мес. | документация IRFCL (World Bank Data360: unit USD, Millions); живая проба отложена — datasource просит `/login` |
| ingest: два endpoint'а, `BatchWriter`; `manualSeriesSource='manual'` жёстко; добавление поля `source` обратно совместимо (DisallowUnknownFields не ломается от нового поля) | `ingest/server.go`, `ingest/model_run.go`, `ingest/clickhouse.go` |
| Шаблон витрины источника: `SELECT date, series, value FROM macro_series FINAL WHERE source='…'` + комментарии таблицы/колонок | `fred/fred.go`, `bls/bls.go` |

## 3. Архитектура решения

### 3.1 Запись — ingest-контур (единственный путь записи, ARCHITECTURE §6.5)

1. **Новый endpoint `POST /v1/gold_prices`.** Тело — JSON-массив точек `{"venue","date","usd"}`. Валидация до записи: `venue` — whitelist (`comex_front`; словарь расширяемый), `date` — строго `YYYY-MM-DD`, `usd > 0`; пустой массив → `{"inserted":0}`. Запись — батч `PrepareBatch → Append (err checked) → Send` в `gold_prices`. Ответ — `{"inserted":N}`.
2. **`POST /v1/manual_series`: опциональное поле `source`.** Пустое → `manual` (поведение не меняется); иначе whitelist {`manual`, `webbridge`, `imf`}, вне списка — 400. Ряд резервов пишется как `{"source":"imf","series":"ra_gold_pol","date":"YYYY-MM-01","value":N}`.
3. **DDL `gold_prices` — копия в ingest** (`EnsureTables`), по прецеденту `company_financials` (копии обязаны совпадать, комментарий в коде): ingest самодостаточен, POST не зависит от того, отработал ли уже импортёр `gold`.
4. В `ingest/schema.go` обновляется комментарий `macro_series.source` (+`'imf'`).

Идемпотентность: ReplacingMergeTree по `(venue,date)` / `(source,series,date)`; повторная запись точки перекрывает прежнее значение, витрины читают `FINAL`.

### 3.2 Чтение — витрины и каталог

- **`v_imf_macro`** — новый, `views/imf_macro.go`: `SELECT date, series, value FROM macro_series FINAL WHERE source='imf'`; комментарии таблицы и колонок по шаблону `v_fred_macro`; включается в `goldViewsList` (импортёр `gold_views`; гейт по `macro_series`); `GRANT SELECT` в `sql/mcp_kimi_reader.sql`.
- **`v_gold_prices`** — обновляется комментарий: две площадки (`moex_fix_usd` — производная, не LBMA; `comex_front` — фронт-месяц COMEX из сессии, не спот).
- **Каталог** (`v_series_catalog`, правило 11 AGENTS.md):
  - `(gold, comex_front)` — в `gold/series_meta.go` (код пакета — владельца source `gold`; upsert при прогоне `gold_prices`): единица «USD за унцию (фьючерс, не спот)», частота `D`, origin «Yahoo Finance GC=F, сессионный ввод через POST /v1/gold_prices»;
  - `(imf, ra_gold_<iso3>)` × 7 — сид `sql/imf_ra_gold_catalog.sql` (у source `imf` нет импортёра, который вызвал бы `UpsertSeriesCatalog`): единица «млн USD (рыночная стоимость)», частота `M`.

### 3.3 Данные (разовое заполнение)

- `GC=F`: 2 года дневных закрытий (~500 точек) одним батчем (~30 КБ при лимите 1 MiB).
- `RA_GOLD`: 7 стран × ~33 месяца (с 2024-01) через `imf_irfcl_data`; дата точки — первый день месяца.
- Регламент (в runbook роадмапа): GC=F — еженедельно (и в дни событий); RA_GOLD — ежемесячно после публикации IRFCL.

### 3.4 Ошибки и валидация

| Вход | Ответ |
|---|---|
| `venue` вне whitelist / `date` не `YYYY-MM-DD` / `usd ≤ 0` | 400, текст перечисляет допустимое |
| `source` вне whitelist | 400 |
| пустой массив | 200 `{"inserted":0}` |
| тело > 1 MiB | 413 (существующий механизм) |

### 3.5 Тестирование

- `ingest/server_test.go` — `/v1/gold_prices`: happy path, невалидные venue/date/usd, пустой массив; `/v1/manual_series`: source по умолчанию, `imf`, невалидный.
- `ingest/clickhouse_test.go` (фейк-стор) — `InsertGoldPrices`: Append всех полей, Abort при ошибке; `InsertManualSeries` пробрасывает source.
- `gold/series_meta_test.go` — каталог описывает оба venue.
- `views` — тест списка витрин `gold_views` включает `v_imf_macro` (расширение существующего теста регистрации; если такого нет — новый).
- `make all` (gofmt/lint/vet/`go test -race`/сборка).

## 4. Риски

1. **Живая проба IMF заблокирована** (datasource требует `/login`) — шаг данных и финальная приёмка ждут повторного входа; код и тесты от неё не зависят.
2. **Юнит RA_GOLD** — ожидание «млн USD» подтверждено документацией, но не живой пробой; если проба отдаст другой юнит — каталог заполняется фактическим (решение зафиксировано здесь).
3. **`comex_front` — фьючерс, не спот**: базис к споту не устраняется, это фиксируется в каталоге, чтобы агент не принимал ряд за спот.
4. Повторная запись исправленного значения: `ReplacingMergeTree()` без версионной колонки перекрывает прежнюю строку при merge — практика «перезапись `(venue,date)`», а не правка строк.

## 5. Что не входит

- `v_cb_gold_dynamics` (дельты/YTD/тонны) — §3 роадмапа; здесь только сырые ряды и общая витрина.
- Обновление репозитория `../gold-nav` (сессионный регламент плагина) — follow-up вне этого PR.
- Деплой нового ingest на staging — владелец (`deploy/staging/README.md`).
- Ежедневный Go-импортёр COMEX — невозможен (Yahoo 429 с этой сети); datasource остаётся сессионным каналом.

## 6. Приёмка (runbook)

1. `make all` — зелёный.
2. `make run-ingest`; батч GC=F → `POST /v1/gold_prices`; RA_GOLD ×7 → `POST /v1/manual_series`.
3. Через MCP: `SELECT count(), min(date), max(date) FROM v_gold_prices WHERE series='comex_front'` (~500 строк); `SELECT series, max(date) FROM v_imf_macro GROUP BY series` (7 рядов).
4. Дельты резервов одним запросом (пример в PR: YTD по странам через `argMin/argMax` или `lagInFrame`).
5. `make mcp-user` (грант новой витрины) + `make mcp-check` — контур виден агенту; запрос к `gold_prices` под `kimi_reader` → `ACCESS_DENIED`.
6. Роадмап: Фаза 1 → ✅, runbook, факты в §6.
