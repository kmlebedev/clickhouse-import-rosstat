# Спека: витрина `v_key_rate_dashboard` (пункт `key-rate/phase-4-dashboard`, issue #128)

**Дата:** 19 октября 2026 г.
**Пункт роадмапа:** `key-rate/phase-4-dashboard`, фаза 4 [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) («❌ витрина `v_key_rate_dashboard` — сводка двух контуров, пороги, ближайшее заседание, последний прогноз и его ошибка»).
**Зависимость:** `key-rate/phase-4-forecast-log` (#127) — ✅; `forecast_log.error_bp`, `probabilities`, `model`, `data_version`, `prompt_version` уже в живой схеме.
**Сопутствующие документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.3 (витрины), [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §4, §6, §7, [KEY_RATE_MODEL.md](../../KEY_RATE_MODEL.md) §5, [KEY_RATE_CHANNELS.md](../../KEY_RATE_CHANNELS.md) §6–§7, [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md) §10.

---

## 1. Проблема

Входы контура уже собираются, но лежат в разных витринах: ставка и базовая инфляция — `v_cbr_macro`, ИПЦ — `v_rosstat_macro`, мониторинг предприятий — `v_cbr_survey`, вменённая ставка — `v_keyrate_expectations`, кривая — `v_ofz_curve`, заседания — `v_events_calendar`, прогнозы — `forecast_log` (агенту не выдан). Агент и публикационный прогон вынуждены склеивать ответы вручную, а последний прогноз и его ошибка в базисных пунктах вообще недоступны агенту: `v_forecast_accuracy` не отдаёт `error_bp`, `probabilities` и метаданные модели.

## 2. Цель

Одна витрина `v_key_rate_dashboard`, которая в одном `SELECT` отвечает на пять вопросов:

1. что сейчас говорит **макро-контур** (ставка, ИПЦ, базовая SAAR-инфляция, инфляционные и ценовые ожидания, деловой климат);
2. что говорит **рыночный контур** (вменённая ставка KEYRATE на ближайшее заседание, её ликвидность, кривая ОФЗ, RUONIA и спред к ставке);
3. где **пороги** — какие условия пробьены (`flag = 1`), а какие нет;
4. когда **ближайшее заседание** и сколько до него дней;
5. что записано в **журнал прогнозов** по каждому треку и какова **ошибка в б.п.** после заседания.

## 3. Решение: long-форма «одна строка на сигнал»

Выбрана форма `v_gold_dashboard` — одна строка на индикатор. Это позволяет:

- добавлять новый трек прогноза строкой (`metric LIKE 'cbr_rate:%'`), а не новой колонкой;
- держать у каждого сигнала свои `unit`, `threshold`, `flag`, `stale`;
- читать витрину одним `SELECT *` и строить по ней таблицу Grafana без pivot.

Отвергнуто: wide-строка в духе `v_model_inputs` (не выдерживает несколько треков и разные пороги), отдельные витрины на каждый контур (агент снова склеивал бы ответы).

### 3.1. Колонки

| Колонка | Тип | Смысл |
|---|---|---|
| `contour` | `LowCardinality(String)` | `spot` — текущая ставка; `fundamental` — макро-контур; `market` — рыночный; `divergence` — расхождение контуров; `forecast` — журнал прогнозов; `calendar` — ближайшее заседание |
| `indicator` | `LowCardinality(String)` | код сигнала; у `forecast` — `metric` из `forecast_log` (`cbr_rate:taylor`, `cbr_rate:ordered_logit`, `cbr_rate:llm`, будущий `cbr_rate:ml`) |
| `value` | `Nullable(Float64)` | значение в `unit`; у `next_meeting` — дней до заседания; NULL — данных нет |
| `unit` | `LowCardinality(String)` | `% годовых`, `% г/г`, `% г/г (SAAR)`, `%`, `пункты`, `б.п.`, `контрактов`, `дней` |
| `as_of` | `Nullable(Date)` | дата значения (последнее наблюдение/сделка/дата прогноза; у `next_meeting` — `today()`) |
| `target_date` | `Nullable(Date)` | дата заседания для `market`/`forecast`/`divergence`/`calendar`; NULL у остальных |
| `threshold` | `String` | текст порога/условия; `—` — числового порога нет |
| `flag` | `UInt8` | 1 — условие `threshold` выполнено; 0 — нет или порога нет |
| `stale` | `UInt8` | 1 — значения нет или оно старше естественного цикла (daily 7 дней, monthly 45 дней; прогноз не свёрено после `target_date` или старше 45 дней; календарь без будущего заседания) |
| `error_bp` | `Nullable(Float64)` | ошибка прогноза в б.п. из `forecast_log.error_bp` (`(predicted − actual) × 100`); NULL — не свёрено или не forecast-строка |
| `probabilities` | `String` | JSON `cut/hold/hike` из `forecast_log`; `''` — не forecast-строка |
| `model` | `LowCardinality(String)` | модель/трек из `forecast_log`; `''` — не forecast-строка |
| `data_version` | `String` | версия набора данных из `forecast_log`; `''` — не forecast-строка |
| `prompt_version` | `UInt16` | редакция промпта LLM-трека из `forecast_log`; 0 — не forecast-строка |

## 4. Состав строк

| contour | indicator | значение | unit | threshold / flag |
|---|---|---|---|---|
| `spot` | `key_rate` | `argMax(rate, date)` из `cbr_key_rate` | % годовых | `—` / 0 |
| `fundamental` | `ipc_yoy` | `(exp(sum(log(percent/100))) − 1) × 100` за 12 последних месяцев `rosstat_ipc_mes` | % г/г | таргет ЦБ 4 %: выше — проинфляционно / `value > 4` |
| `fundamental` | `base_ipc_saar` | `((1 + v/100)^12 − 1) × 100` по последнему и предыдущему `Базовый ИПЦ` (`cbr_indicators_cpd`, м/м SA) | % г/г (SAAR) | `> 4 %` два месяца подряд — прокси условия «две недели подряд» (недельного базового ИПЦ в БД нет) / оба месяца > 4 |
| `fundamental` | `infl_exp_expected` | последняя волна `ожидаемая инфляция` (`cbr_infl_exp`) | % | рост к прошлой волне — проинфляционный разворот / `last > prev` |
| `fundamental` | `price_expectations_sa` | `macro_series` (`source='cbr_survey'`, `series='price_expectations_sa'`) | пункты | `—` / 0 |
| `fundamental` | `business_climate_sa` | `macro_series` (`cbr_survey`, `business_climate_sa`) | пункты | `—` / 0 |
| `market` | `keyrate_implied_rate` | `settle` ближайшей будущей серии (`keyrate_futures` ⋈ `keyrate_contract`, `last_trade = min(last_trade) ≥ today()`) | % годовых | `—` / 0 |
| `market` | `keyrate_implied_change_bp` | `round((settle − key_rate) × 100, 1)`, где `key_rate` — последняя ставка (`nullIf(argMax(rate, date), 0)`: пустая `cbr_key_rate` даёт NULL, а не 0) | б.п. | `—` / 0 |
| `market` | `keyrate_volume` | `volume` последнего торгового дня серии | контрактов | 0 — сделок не было: `settle` не рыночное ожидание / `volume = 0` |
| `market` | `keyrate_openposition` | `openposition` | контрактов | `—` / 0 |
| `market` | `ofz_1y`, `ofz_3y`, `ofz_5y`, `ofz_10y` | `yield` соответствующего тенора | % годовых | `—` / 0 |
| `market` | `ofz_slope_10y_1y_bp` | `(ofz_10y − ofz_1y) × 100` | б.п. | `< 0` — инверсия кривой / `< 0` |
| `market` | `ruonia` | `argMax(ruo, date)` из `cbr_ruania` | % годовых | `—` / 0 |
| `market` | `ruonia_spread_bp` | `(ruonia − key_rate) × 100` | б.п. | `—` / 0 |
| `calendar` | `next_meeting` | `dateDiff('day', today(), min(event_date))` по будущим `category='cbr'` | дней | ≤ 3 календарных дня — предзаседание; T-0 — день решения / `≤ 3` |
| `divergence` | `taylor_vs_market_bp` | `(predicted_taylor − settle) × 100` | б.п. | `|расхождение| ≥ 25 б.п.` — контуры расходятся на шаг / `abs ≥ 25` |
| `divergence` | `ordered_logit_vs_market_bp` | `(predicted_ordered_logit − settle) × 100` | б.п. | то же / `abs ≥ 25` |
| `forecast` | `cbr_rate:<track>` — динамически, `metric LIKE 'cbr_rate:%'` | последний `predicted` по `forecast_date` | % годовых | ошибка в б.п. — в `error_bp` после заседания / 0 |

**Почему расхождение считается так.** `predicted` — ожидаемая ставка после заседания, `settle` — вменённая рынком ставка на то же заседание. Их разность не зависит от текущей ставки, поэтому `divergence = (predicted − settle) × 100` б.п. Отрицательное значение — макро-трек ждёт более низкой ставки, чем рынок.

**Почему `next_meeting` — число, а не дата.** Дата заседания лежит в `target_date`; `value` даёт числовой сигнал «сколько дней осталось», пригодный для `flag` и сортировки. `as_of` = `today()`, потому что значение пересчитывается в момент запроса.

## 5. Источники и SQL

Витрина читает **сырые** таблицы (как `v_model_inputs` и `v_keyrate_expectations`), а не другие витрины: определение не зависит от порядка создания чужих витрин, а гейт `util.CreateView` проверяет ровно те таблицы, без которых запрос неисполним.

Гейт (11 таблиц): `cbr_key_rate`, `cbr_indicators_cpd`, `cbr_infl_exp`, `rosstat_ipc_mes`, `ofz_curve`, `cbr_ruania`, `macro_series`, `keyrate_futures`, `keyrate_contract`, `events_calendar`, `forecast_log`.

Структура `Select`:

- `WITH` из one-row CTE: `kr` (ставка), `cpd` (последний и предыдущий `Базовый ИПЦ`), `ie` (последняя и предыдущая `ожидаемая инфляция`), `ipc` (12 последних месяцев ИПЦ и их YTD-произведение), `ofz` (четыре тенора и дата), `ru` (RUONIA), `kf` (ближайшая будущая серия KEYRATE), `ct` (ближайшее заседание), `pe` и `bc` (мониторинг предприятий).
- `UNION ALL` веток по числу строк; внешний `SELECT * FROM (...) ORDER BY contour, indicator`.
- Все числовые значения — `Float64`, даты — `Nullable(Date)`, флаги — `UInt8`, строки — `String`, чтобы `UNION ALL` не разъехался по типам.

Особенности, проверенные на ClickHouse 26.10:

1. **Алиас до `FINAL`**: `FROM keyrate_futures AS f FINAL`; форма `keyrate_futures FINAL f` — `SYNTAX_ERROR` code 62.
2. **`ILLEGAL_AGGREGATION` 184**: фильтр по агрегатному алиасу `decision_date` в `WHERE` того же `SELECT` запрещён; фильтр по `last_trade` стоит во внутреннем подзапросе, а `argMax` — снаружи.
3. **Пустая `cbr_key_rate`**: `argMax(rate, date)` по пустой таблице возвращает `0`, а не NULL; поэтому `nullIf(..., 0)` — иначе вменённое изменение выродилось бы в `settle × 100`.
4. **`forecast_log`**: `FINAL` схлопывает версии; последний прогноз трека — `row_number() OVER (PARTITION BY metric ORDER BY forecast_date DESC) = 1`; `metric LIKE 'cbr_rate:%'` (треки различаются суффиксом, голый `cbr_rate` не трек).
5. **`ofz_curve`** — снимок: история теноров накапливается с первого прогона, поэтому `stale = 1` у ОФЗ может означать «импортёр не ходил 7 дней», а не «кривой нет».

## 6. Пороги, `flag` и `stale`

Пороги взяты из [KEY_RATE_CHANNELS.md](../../KEY_RATE_CHANNELS.md) §6 и [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §4 п. 6:

- **`base_ipc_saar` > 4 % два месяца подряд** — главный триггер (в источнике — «две недели подряд» по недельной базовой инфляции; недельного базового ИПЦ в БД нет, поэтому используется месячный `Базовый ИПЦ` как явно названный прокси). `flag = 1`, только если оба последних годовых значения > 4 %.
- **`infl_exp_expected`**: рост к прошлой волне — проинфляционный разворот; снижение — пространство для снижения ставки. `flag = 1` при росте.
- **`ipc_yoy` > 4 %** — выше таргета ЦБ.
- **`keyrate_volume = 0`** — сделок не было; `settle` не рыночное ожидание (инструмент низколиквиден).
- **`ofz_slope_10y_1y_bp < 0`** — инверсия кривой.
- **`divergence` |value| ≥ 25 б.п.** — контуры расходятся на шаг и больше.
- **`next_meeting` ≤ 3 календарных дня** — окно предзаседания (T-0 — день решения).

`flag` — не «хорошо/плохо», а «условие выполнено»; читать вместе с `threshold`. `stale` — «данные отсутствуют или устарели по естественному циклу», а не «значение плохое»: daily-ряды (ставка, KEYRATE, ОФЗ, RUONIA) — 7 дней; monthly-ряды (ИПЦ, базовая инфляция, ожидания, мониторинг) — 45 дней; `forecast` — `target_date < today()` и `actual IS NULL`, либо `forecast_date` старше 45 дней; `calendar` — нет будущего заседания; `divergence` — нет прогноза/рынка или `target_date` прогноза не совпадает с ближайшим заседанием.

## 7. Публикация витрины

- Новый импортёр **`key_rate_views`** в пакете `views/` (файл `views/key_rate_views.go`), по образцу `gold_views`: `keyRateViewsList`, `Name() = "key_rate_views"`, `Import()` → `util.CreateView`, `init()` в `chimport.Stats`.
- В dagu **не ставится**: витрина динамическая и создаётся один раз после `make run-ingest` (создаёт `forecast_log`) и импортёров контура. Запуск — `make import STAT=key_rate_views`; повтор идемпотентен.
- Комментарии таблицы и всех 14 колонок — через `util.View` (канонический текст живёт в коде; в ARCHITECTURE.md он не дублируется).
- Грант `kimi_reader` — в `sql/mcp_kimi_reader.sql`; сырой `forecast_log` агенту **не выдаётся** (витрина читает его как `SQL SECURITY DEFINER`).
- Проверки `scripts/mcp_check.py`: витрина читается и непуста; пять фиксированных контуров на месте (`spot`, `fundamental`, `market`, `divergence`, `calendar`), `forecast` — информационная строка (0 треков — не FAIL); `next_meeting.target_date` = `min(event_date)` из `v_events_calendar`; `base_ipc_saar` и `keyrate_implied_change_bp` присутствуют; `error_bp` читается; `probabilities`/`model` у `cbr_rate:ordered_logit` непусты (если строка есть); `SELECT count() FROM forecast_log` → `ACCESS_DENIED` (497).

## 8. Тесты

В тестовой среде нет ClickHouse, поэтому тесты контрактные, текстовые (по образцу `views/bank_forward_test.go`):

- имя `v_key_rate_dashboard`, ровно 11 таблиц гейта в объявленном порядке;
- 14 колонок с непустыми комментариями; список алиасов задан явно (хелпер `selectedColumns` не умеет `UNION ALL`);
- в `Select` есть маркеры: `'spot'`, `'fundamental'`, `'market'`, `'divergence'`, `'forecast'`, `'calendar'`, `'base_ipc_saar'`, `'keyrate_implied_change_bp'`, `'next_meeting'`, `error_bp`, `LIKE 'cbr_rate:%'`, `pow(1 +`, `FINAL`, `minOrNull`;
- витрина зарегистрирована в `keyRateViewsList`;
- импортёр `key_rate_views` зарегистрирован в `chimport.Stats` (init) — проверяется тестом регистрации, как у `gold_views`/`company_views`.

Живой прогон (приёмка) проверяет числа на dev-ClickHouse; тесты офлайн.

## 9. Границы

**В объёме:** витрина, импортёр `key_rate_views`, тесты, грант, `mcp_check`, ARCHITECTURE/README/IMPORTERS/DEVELOPMENT, роадмап и публикационный контракт.

**Вне объёма:**

- панель/график Grafana `key-rate.json` и панели по новой витрине — отдельный пункт фазы 4;
- консенсус-бенчмарк №4 (медиана макропроса) — отдельный пункт;
- PIT-слой, ML-трек, автоверификация, публикационный прогон — отдельные пункты;
- изменение DDL `forecast_log`, writer'а и резолвера — не требуется;
- новые dagu-шаги — витрина создаётся вручную, как `gold_views`.

## 10. Приёмка

1. `make all` — зелёный (gofmt, golangci-lint, `go vet`, `go test -race ./...`, сборка).
2. `make import STAT=key_rate_views` — `View v_key_rate_dashboard created: true`; повтор — те же строки.
3. MCP `kimi_reader`: `SELECT count() FROM v_key_rate_dashboard` = 23 на снимке 2026-10-18; `count(DISTINCT contour)` = 6; значения и флаги — по §4; `SELECT count() FROM forecast_log` → `ACCESS_DENIED`.
4. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА, новые проверки зелёные.
5. `make docs-check` — зелёный.
6. Протокол приёмки — комментарием в PR и в issue #128; статус пункта в роадмапе — ✅ тем же PR.

## 11. Отвергнутые альтернативы

1. **Wide-строка** в духе `v_model_inputs`: не выдерживает несколько треков прогноза и разные пороги; каждая новый сигнал — колонка.
2. **Отдельные витрины `v_key_rate_fundamental` / `v_key_rate_market`**: агент снова склеивал бы ответы, а публикационному прогону нужна одна точка чтения.
3. **Читать другие витрины вместо сырых таблиц**: определение зависело бы от порядка создания чужих витрин; гейт `util.CreateView` проверяет наличие таблиц, а не витрин, и молча пропустил бы витрину при отсутствии одной из них. Прецедент чтения сырья — `v_model_inputs` и `v_keyrate_expectations`.
