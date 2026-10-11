# Фаза 3 роадмапа золота: спрос, консенсус и обратная связь — design

Дата: 2026-10-11. Пункт: `docs/GOLD_THESIS_DATA_ROADMAP.md` §2, «Фаза 3 — спрос, консенсус и обратная связь (~3–5 дней, код)».
Приёмка пункта: `v_forecast_accuracy` показывает ошибки прогнозов Q3/Q4 (`is_resolved`, `error_pct`); консенсус банков читается запросом к одной таблице.

## Пробел

1. **ETF-потоки WGC** живут в тексте статьи: в БД есть только ручной `etf_flows_month` (млрд USD, читает `v_gold_dashboard`), структурированных рядов WGC (поток в USD и тоннах, запасы, AUM, по регионам) нет.
2. **Банковские цели** (Goldman/JPM/HSBC/UBS/Citi) — только в статье; таблица `gold_forecasts` описана в ARCHITECTURE.md §6.2, но в коде не реализована.
3. **Ошибки прошлых прогнозов не считаются**: `forecast_log.actual` никто не заполняет, `v_forecast_accuracy` вечно отдаёт `is_resolved = 0`. Прогнозов Q3 в журнале нет вовсе — контур прогноза начался 10.10.2026.

Проверено на входе (живая БД, 2026-10-11): в `forecast_log` 2 строки прогона 10.10.2026 (`nav` 12 500, `xau_q_avg` 4 285, target 2026-12-31), обе нерезолвленные; `gold_prices`: `comex_front` 502 точки (2024-10-11…2026-10-09), `moex_fix_usd` 554 точки (2024-08-05…2026-10-08); средние Q3-2026 (01.07–30.09): comex 4 300,80, moex 4 240,32.

## Цель

Три результата фазы:

1. Структурированные ряды WGC ETF Flows в `macro_series` (`source = 'webbridge'`), описанные в каталоге и читаемые витриной `v_gold_etf_flows`.
2. Таблица `gold_forecasts` в коде + валидирующий endpoint `POST /v1/gold_forecasts` + сид консенсуса + витрина `v_gold_forecasts`.
3. Импортёр `forecast_resolve` (пакет `forecast/`): автозаполнение `forecast_log.actual`/`error_pct` после `target_date` сверкой с `gold_prices`, расписание dagu.

## 1. WGC ETF Flows — структурированные ряды (20 рядов)

**Состав (выбран в брейншторме):** 4 метрики × 5 регионов = 20 рядов, имена `etf_<метрика>_<регион>`:

| Метрика (суффикс) | Что | Единица | Семантика |
|---|---|---|---|
| `flow_usd_mln` | чистый поток за месяц | млн USD | знак — направление (плюс приток) |
| `flow_tonnes` | чистый поток за месяц | тонны | знак — направление |
| `holdings_tonnes` | запасы (holdings) на конец месяца | тонны | сток |
| `aum_usd_bn` | AUM на конец месяца | млрд USD | сток |

Регионы: `world`, `north_america`, `europe`, `asia`, `other` (разбивка WGC; `world` = итог). Дата точки — **последний день отчётного месяца** (как у `etf_flows_month`: «дата наблюдения — конец отчётного месяца»).

**Источник и статус:** WGC Gold ETF Flows (monthly, gold.org/goldhub; страница подтверждает разрезы «flows by region» / «holdings by region» в тоннах и USD). Полуавтомат: сбор — сессионно (WebBridge или вручную) после ежемесячного отчёта WGC (~10-е число), запись — существующим `POST /v1/manual_series` c `source = 'webbridge'`; эталон — `etf_flows_month`. Код ingest не меняется.

**Каталог** (правило 11a): у сессионных рядов нет импортёра, который вызвал бы `util.UpsertSeriesCatalog`, поэтому описания — сид `sql/wgc_etf_flows_catalog.sql` (20 записей `(source='webbridge', series)`: title, unit, frequency `M`, origin — WGC Gold ETF Flows, description — что измеряет, как читать, связь с `etf_flows_month`, периодичность обновления).

**Витрина `v_gold_etf_flows`** (`views/gold_etf_flows.go`, импортёр `gold_views`) — pivot: одна строка на `(date, region)` с колонками `flow_usd_mln`, `flow_tonnes`, `holdings_tonnes`, `aum_usd_bn`. Значения — по приоритету источников `webbridge > manual` (та же конвенция, что в `v_gold_dashboard`: агрегат по `(series, source, date)` внутри, `coalesce(maxIfOrNull(...))` снаружи; `maxIf` запрещён — возвращает 0). Комментарии таблицы и каждой колонки; `GRANT SELECT` в `sql/mcp_kimi_reader.sql`.

Список 20 рядов и разметка `(region, metric)` — данные в Go (`views/gold_etf_flows.go`), из них собирается и `IN (...)`-фильтр, и mapping-массив `ARRAY JOIN`; тест сверяет список с каталог-сидом (все 20 имён присутствуют в `sql/wgc_etf_flows_catalog.sql`).

**Не трогаем:** `etf_flows_month` и `v_gold_dashboard` (контракт фазы 0). Дубль мирового потока (`etf_flows_month` в млрд USD и `etf_flow_usd_mln_world` в млн USD) существует раздельно; связь описана в каталоге.

## 2. `gold_forecasts` — консенсус банков

**DDL** — канонический из ARCHITECTURE.md §6.2, в код копией: `ingest/schema.go` (создаётся `EnsureTables`) и сид (самодостаточность, прецедент `sql/events_calendar_q4_2026.sql`). Единственное изменение — комментарий `scenario`: добавляется `'range'` (банковский диапазон; DDL-комментарий синхронно правится в ARCHITECTURE.md §6.2):

```sql
CREATE TABLE IF NOT EXISTS gold_forecasts (
    author LowCardinality(String),   -- 'goldman','jpm','hsbc','ubs','citi','own',...
    published Date,
    horizon LowCardinality(String),  -- 'Q4-2026','YE-2026','2027','12M','short'
    scenario LowCardinality(String), -- 'point','range','bull','base','bear'
    low Float64, high Float64, point Float64,
    probability Float32
) ENGINE = ReplacingMergeTree ORDER BY (author, published, horizon, scenario);
```

**Endpoint `POST /v1/gold_forecasts`** (единственный путь записи со стороны контура — ingest):

```json
[{"author":"goldman","published":"2026-10-07","horizon":"YE-2026","scenario":"point",
  "low":0,"high":0,"point":4900,"probability":0}]
```

Валидация (ошибка любой строки — 400, запись не выполняется):

| Поле | Правило |
|---|---|
| `author` | непустой после `TrimSpace` |
| `published` | строго `YYYY-MM-DD`, диапазон Date32 [1900-01-01, 2299-12-31] (`parsePointDate`) |
| `horizon` | непустой после `TrimSpace` |
| `scenario` | ∈ {`point`,`range`,`bull`,`base`,`bear`} |
| `low`,`high`,`point` | ≥ 0; хотя бы одно > 0; если оба (`low`,`high`) > 0 — `high ≥ low` |
| `probability` | 0..100 (0 — «не задана») |

Запись — одним батчем `PrepareBatch → Append (err checked) → Send` (правило 2); ответ `{"inserted": N}`; пустой массив — `N = 0` (конвенция `/v1/manual_series`).

**Сид `sql/gold_forecasts_q4_2026_seed.sql`** — консенсус из статьи 07.10.2026 (§ «Сравнение с консенсусом аналитиков»), `published = 2026-10-07` для всех строк:

| author | horizon | scenario | low | high | point | probability |
|---|---|---|---|---|---|---|
| goldman | YE-2026 | point | — | — | 4900 | — |
| goldman | 2027 | range | 5400 | 5600 | — | — |
| hsbc | YE-2026 | point | — | — | 4750 | — |
| hsbc | 2027 | point | — | — | 5025 | — |
| jpm | Q4-2026 | point | — | — | 4500 | — |
| jpm | 2027 | point | — | — | 5400 | — |
| ubs | 12M | point | — | — | 5200 | — |
| citi | short | point | — | — | 4000 | — |
| own | Q4-2026 | bull | 4600 | 5000 | 4800 | 25 |
| own | Q4-2026 | base | 4000 | 4600 | 4300 | 40 |
| own | Q4-2026 | bear | 3750 | 4050 | 3900 | 35 |

own-точки — середины диапазонов статьи; взвешенная 0.25·4800 + 0.4·4300 + 0.35·3900 = 4 285 совпадает с `xau_q_avg` прогона 10.10.2026. «—» в таблице = `0` в столбцах.

**Витрина `v_gold_forecasts`** (`views/gold_forecasts.go`, импортёр `gold_views`): `author`, `published`, `horizon`, `scenario`, `low`, `high`, `point` (нули → NULL через `nullIf`), `midpoint` (= `point`, иначе середина `low`/`high`), `probability` (0 → NULL); `ORDER BY published DESC, author, horizon, scenario`. Комментарии таблицы и колонок; грант `kimi_reader`.

## 3. `forecast_resolve` — автозаполнение `forecast_log.actual`

**Пакет `forecast/`**, импортёр `Name()` = `forecast_resolve`:

- читает `forecast_log FINAL` (`metric = 'xau_q_avg' AND target_date <= today()`), порядок `forecast_date`;
- окно сверки — **квартал, содержащий `target_date`**: `[начало квартала, target_date]` (соответствует `verify.md` плагина gold-nav: «факт `xau_q_avg` — среднее `GC=F` за квартал `target_date`»);
- `actual` = `avg(usd)` по `gold_prices FINAL` в окне, приоритет `venue = 'comex_front' > 'moex_fix_usd'`; выбор — по гейту `countIf > 0` (пустой `avg` даёт NaN, поэтому гейт по количеству точек, а не по `coalesce`); окно без точек — пропуск с `log.Warn`, строка остаётся нерезолвленной;
- `error_pct = (predicted − actual) / actual × 100` (при `actual <= 0` — пропуск с `log.Warn`);
- запись — **только если** вычисленный `actual` отличается от сохранённого (нет — молчание); замена — одним батчем; ключ `(metric, forecast_date)` → повторная запись идемпотентна под ReplacingMergeTree, частично записанное значение (сверка в день `target_date`) самокорректируется следующим прогоном;
- **`nav` не резолвится** — факта нет (прямо записано в `verify.md` плагина: «`nav` — факт не определён, оставь `actual` пустым»); прочие метрики не трогаются.

**Тесты офлайн** (без БД/сети): окно квартала (границы года, середина квартала), `error_pct`, решение «пропустить/записать», выбор venue; оркестрация `Import` — через узкий интерфейс-стор (паттерн `chStore` из `ingest/clickhouse.go`) с фейком.

**Расписание `dagu/forecast.yaml`:** `type: chain`, `schedule: "23 20 * * *"` (после `gold.yaml` 18:47 и `moex.yaml` 19:13), `overlap_policy: skip`, retry 3 × 300 с, `CLICKHOUSE_IMPORT_STAT=forecast_resolve`.

**Бэкфилл `sql/forecast_log_backfill.sql`:** прогноз от 31.07.2026 (статья §1: «базовый сценарий Q3 $4 300–4 600, средняя ~$4 450») — `('2026-07-31','2026-09-30','xau_q_avg',4450,NULL,NULL)`. Без него приёмку «ошибки Q3» показать нечем: журнал начался 10.10.2026. Применяется вручную (`make ch-sql`).

**Комментарии `v_forecast_accuracy`** (`views/forecast_accuracy.go`): назвать механизм заполнения (импортёр `forecast_resolve`: окно-квартал, приоритет venue comex → moex), зафиксировать «`nav` — факт не определён».

## Границы (не делаем)

- Не трогаем `v_gold_dashboard` и `etf_flows_month`; пороговая/stale-механика для WGC-рядов не заводится (в статье §2 поток читается по месяцам).
- Не делаем ETL-импортёр WGC (источника в API нет; канал — сессионный, «полуавтомат»).
- Не меняем DDL `forecast_log` (расширение окна/горизонтов не требуется: окно выводится из `target_date`).
- Не резолвим `nav` (см. §3). Не пишем в `forecast_log` ничего, кроме резолва `xau_q_avg`.
- staging-применение (сиды, `gold_views`, перезапуск ingest) — вручную, как в фазе 2; фиксируется в роадмапе.

## Риски и ограничения

- **Q4-прогноз резолвится только после 2026-12-31** (target 2026-12-31): на приёмке Q4 виден как `is_resolved = 0`; корректность окна Q4 покрыта юнит-тестами, Q3 — живым прогоном.
- **`unitsMarker`-подобной свободы нет:** имена рядов и разметка регионов фиксированы в Go; расширение набора регионов/метрик — правка кода и каталога согласованно.
- **Расхождение источников цены** (comex vs moex-прокси) — осознанное: сверка по comex (как в `verify.md`), фолбэк moex — только при отсутствии comex в окне; выбранный venue логируется.

## Приёмка

1. `make all` зелёный (gofmt, golangci-lint, vet, `go test -race`, сборка).
2. Свежий `mcp_check.py`: новые проверки витрин, каталога WGC и запрета сырой `gold_forecasts`; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `make mcp-user` выдаёт гранты.
3. Сиды применены (`make ch-sql` × 3), `make import STAT=forecast_resolve` резолвит Q3-строку: `actual` ≈ **4 300,80** (comex, 64 точки), `error_pct` ≈ **+3,47 %**; Q4-строки (target 2026-12-31) остаются `is_resolved = 0`.
4. MCP: `SELECT * FROM v_forecast_accuracy` — Q3-строка с `is_resolved = 1` и ошибкой; `SELECT * FROM v_gold_forecasts` — консенсус банков и own-сетка одним запросом; `v_series_catalog WHERE source = 'webbridge'` — 20 описаний.
5. Живой ввод WGC: `POST /v1/manual_series` (`source = 'webbridge'`) с сентябрьскими мировыми значениями из статьи → `SELECT * FROM v_gold_etf_flows` показывает строку за 2026-09-30.
6. Повторный `make import STAT=forecast_resolve` — 0 записей (значения совпадают, идемпотентность).
