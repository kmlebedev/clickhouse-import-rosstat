# Дашборд «Ключевая ставка — публикации» (Grafana): контур ставки как код — design

**Дата:** 2026-10-16. **Пункт:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §6 фаза 4 — «панель Grafana `key-rate.json`» (она же — отложенный подпункт «графики (Grafana, картинки)» спеки [2026-10-15-key-rate-model-v0-design.md](2026-10-15-key-rate-model-v0-design.md) §161). **Ветка:** `feat/grafana-key-rate`.
**Приёмка (кратко):** дашборд `key-rate-publications` собран кодом (grafana-foundation-sdk) в артефакт `dashboard/key-rate-publications.json`; 19 панелей покрывают все данные [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §4/§6/§8; каждый запрос читает только витрины `v_*` (проверено живьём через MCP); SQL-идиомы и визуальные конвенции выведены из UI-экспорта `grafana_dashboard_rosstat` (разведка 2026-10-16); CI собирает, диффит и линтит оба артефакта; локальный деплой + smoke; статья получает раздел «Иллюстрации»; README/ARCHITECTURE/ROADMAP синхронны. Детали — §8.

## 1. Пробел и место в роадмапе

`KEY_RATE_PUBLICATIONS.md` — «устав» публикационного контура (форматы, шаблон публикации, контракт данных); читателю нужны **иллюстрации-графики** «что произошло с данными». Построить их должно «как код»: дашборд Grafana в модуле `grafana/` (конвейер dashboards-as-code обкатан на `gold-thesis`, PR #41), а не ручной экспорт из UI. Роадмап фазы 4 заканчивается строкой «панель Grafana `key-rate.json`»; решением владельца (2026-10-16) дашборд называется `key-rate-publications` (uid/`metadata.name`/файл артефакта), роадмапное имя заменяется.

**Решения владельца (2026-10-16):**
1. Иллюстрации в статью — **разделом со ссылкой на дашборд** (таблица «раздел статьи → панель», где смотреть); PNG-снимки в репозиторий не коммитятся — рендер отдельным пунктом позже (как в gold-thesis, follow-up).
2. Имя/uid: `key-rate-publications`, артефакт `dashboard/key-rate-publications.json`.

## 2. Живые факты (проверено 2026-10-16)

**Конвейер (существует, менять не надо):** `grafana/` — отдельный Go-модуль (SDK не тянется в ETL-бинарь): `clickhouse.go` (вариант запроса плагина `grafana-clickhouse-datasource` 4.3.2, `DatasourceUID = ch-rosstat`), `validate.go` (uid; ненулевые уникальные id панелей/рядов; датасорс-константа; `FROM v_` в каждом `rawSql`), `main.go` (`-out`, регистрация плагинов), `gold_thesis.go` (9 панелей/5 рядов, шаблоны SQL), `gold_thesis_test.go`. `make grafana-build` → артефакт; CI `.github/workflows/dashboards.yml` (vet/тесты, сборка, `git diff --exit-code`, `gcx dev lint run`, upload); локальный стенд `make grafana-up` + `make grafana-deploy-test` + smoke `scripts/grafana_check.py --uid <uid>`; staging — датасорс-юзер `kimi_reader`, owner-assisted.

**UI-экспорт `grafana_dashboard_rosstat` (референс владельца) читает СЫРЫЕ таблицы** — 104 запроса, 15 таблиц, `WHERE name = '…'`, ноль `v_*`. На staging под `kimi_reader` такие запросы отказали бы, а `Validate` требует `FROM v_`. Значит из экспорта переносятся **SQL-идиомы и визуальные настройки**, а имена таблиц заменяются на витрину+`series`. Таблица соответствия:

| UI-источник (сырая таблица.колонка) | Витрина | `source`/`series` |
|---|---|---|
| `cbr_key_rate.rate` | `v_cbr_macro` | `series = 'cbr_key_rate'` |
| `cbr_ruania.ruo` | `v_cbr_macro` | `series = 'cbr_ruania_ruo'` (+`_min_rate`, `_max_rate`, `_p25`, `_p75`, `_trans`, `_vol`) |
| `cbr_m2.balance` | `v_cbr_macro` | `series = 'М2 SA'` (млрд руб.), `'Широкая денежная масса М2Х'` (млн руб.) |
| `cbr_credit_m2x.value` | `v_cbr_macro` | `series = 'Кредит экономике'` (млн руб.), `'Кредиты кредитных организаций - всего'` |
| `cbr_indicators_cpd.value` | `v_cbr_macro` | `series = 'Базовый ИПЦ'` (% к предыдущему месяцу — м/м прирост) |
| `cbr_infl_exp.value` | `v_cbr_macro` | `series = 'наблюдаемая инфляция'`, `'ожидаемая инфляция'` (% годовых) |
| `cbr_int_rate_loans.rate` | `v_cbr_macro` | `series = 'Всего: …'`/`'СМП: …'` (8+8 строк по срокам, % годовых) |
| `ipc_mes.percent` | `v_rosstat_macro` | `series = 'Индексы потребительских цен на товары и услуги по Российской Федерации'` (индекс, 100 = без изменения) |
| `ipc_weeks.percent` | `v_rosstat_macro` | 131 товарный ряд (индекс, % к предыдущей дате); недельные — `v_series_catalog.frequency = 'W'` |
| `salaries_mes.salary` | `v_rosstat_macro` | `series = 'Среднемесячная номинальная начисленная заработная плата работников по полному кругу организаций'` (руб./мес) |
| — (нет в БД UI-дашборда) | `v_rosstat_macro` | `series = 'Уровень безработицы'`, `'Рабочая сила'`, `'Безработные'` |
| `minfin_fed_bud_mes` / `_mesyats` | `v_minfin_budget` | `source = 'minfin'` / `'minfin_mesyats'`; `series = 'Доходы, всего'`, `'Расходы, всего'`, `'Дефицит (-)/Профицит (+)'` (млрд руб.; дефицит — только `minfin_mesyats`) |
| `sber_izmenenie_trat` | `v_bank_sber_spending` | `source = 'sber_izmenenie_trat'`, `series = 'Все категории'` (% г/г, недельно) |
| нет в UI-дашборде | `v_keyrate_expectations` (`date, secid, series_name, decision_date, settle, close, volume, openposition, expected_change_bp`), `v_ofz_curve` (`tenor`: `RGBI` — 4 252 точки с 2010; `1y/3y/5y/10y` — по 1 точке), `v_cbr_mo_br` (`key_rate_avg_<год>_<стат>`, `cpi_avg_<год>_<стат>`), `v_cbr_survey` (`price_expectations_sa` — пункты), `v_cbr_mp_summary` / `v_cbr_mp_summary_channels` (`kind ∈ {prose, note, risk_pro, risk_dez}` × `method ∈ {regex, llm}`), `v_events_calendar` (`category='cbr'`), `v_news_events` | — |

`FINAL` уже внутри витрин — в панельных запросах не пишется. Все панельные витрины уже выданы `kimi_reader` (`sql/mcp_kimi_reader.sql` — проверено, 14 из 14).

## 3. Архитектура

```
grafana/ (отдельный Go-модуль, grafana-foundation-sdk)
   └─ go run . [ -dashboard gold-thesis|key-rate-publications|all ] [ -out-dir ../dashboard ]
         ├─ dashboard/gold-thesis.json            (коммитится; генерируется, руками не правится)
         └─ dashboard/key-rate-publications.json  (коммитится; генерируется, руками не правится)
   CI dashboards.yml: тесты модуля + сборка обоих + git diff обоих + gcx dev lint обоих + upload
   make grafana-deploy-test / -staging: push обоих + smoke по обоим uid
```

Селектор дашбордов — **упорядоченный слайс** (не map: итерация map в Go недетерминирована, а на детерминизме стоит CI-diff).

## 4. Каркас дашборда

- uid / `metadata.name`: `key-rate-publications`; title «Ключевая ставка — публикации»; tags `["key-rate", "publications"]`;
- `Refresh("30m")` (данные обновляются не чаще суточного dagu-прогона; 5m как у gold — избыточно), `Time("now-3y", "now")` (полный цикл ДКП 2024–2026 + контекст), `Timezone(browser)`;
- ряды: id 21–24 (панели 1–19; нулевой id Grafana заменит своим — id задаются явно);
- ширина: панели 5 и 10 — `Span(24)`, остальные `Span(12)`; у каждой панели — `Description` (источник, как читать, ловушки единиц).

## 5. Панели (19; SQL — дословный контракт реализации)

Единицы: `pct-u` = `percentunit` (доля: SQL отдаёт `pow(...)-1`, Grafana печатает «3.2%»); `pct` = `percent` (п.% в SQL — п.п.); `RUB` = `currencyRUB`. Смешение единиц на панели — только пер-серийными unit-оверрайдами. Разрежённые серии — пунктир/точки (`custom.lineWidth: 0` + `custom.showPoints: "always"`). Прошлые годы — пунктир `{"dash":[10,10],"fill":"dash"}`.

### Ряд 21 «Ставка и инфляция» (панели 1–5)

**1. «Ключевая ставка и решения ЦБ»** (timeseries, pct) — A: линия ставки; B: точки решений (override B: only points + fixed color).

```sql
-- A
SELECT date AS time, value AS "Ключевая ставка, % годовых"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND $__timeFilter(date)
ORDER BY date
-- B
SELECT meeting_date AS time, decision_rate AS "Решение ЦБ, % годовых"
FROM v_cbr_mp_summary
WHERE $__timeFilter(meeting_date)
ORDER BY meeting_date
```

**2. «Недельные цены: годовое выражение»** (timeseries, pct-u; порог 4% `line`; календарное наложение лет — B пунктиром) — средний недельный индекс возводится в степень 52 (52 недели в году); «SAAR считает модель» — в Description.

```sql
-- A (текущий год)
SELECT date AS time, pow(avg(value) / 100, 52) - 1 AS "Текущий год, годовое выражение"
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND toYear(date) = toYear(now())
GROUP BY date
ORDER BY date
-- B (прошлый год, наложение: время сдвинуто на +1 год)
SELECT date_add(YEAR, 1, date) AS time, pow(avg(value) / 100, 52) - 1 AS "Прошлый год, годовое выражение"
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND toYear(date) = toYear(now()) - 1
GROUP BY date
ORDER BY time
```

**3. «Недельные цены: доля подорожавших позиций»** (timeseries, pct) — доля товаров с индексом > 100.

```sql
SELECT date AS time, countIf(value > 100) / count() * 100 AS "Доля подорожавших, %"
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND $__timeFilter(date)
GROUP BY date
ORDER BY date
```

**4. «Инфляция: ИПЦ и базовая»** (timeseries, pct-u; порог 4% `line`) — формулы разные и это в Description: ИПЦ — индекс (100=без изменения) → `pow(value/100,12)-1`; базовая — м/м прирост в % → `pow(1+value/100,12)-1`.

```sql
-- A
SELECT date AS time, pow(value / 100, 12) - 1 AS "ИПЦ, годовое выражение"
FROM v_rosstat_macro
WHERE series = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
  AND $__timeFilter(date)
ORDER BY date
-- B
SELECT date AS time, pow(1 + value / 100, 12) - 1 AS "Базовая инфляция, годовое выражение"
FROM v_cbr_macro
WHERE series = 'Базовый ИПЦ' AND $__timeFilter(date)
ORDER BY date
```

**5. «Инфляционные и ценовые ожидания»** (timeseries; A/B — pct; C — override unit `short` + fixed color) — население и бизнес.

```sql
-- A/B: series = 'наблюдаемая инфляция' / 'ожидаемая инфляция' из v_cbr_macro, алиасы «Наблюдаемая/Ожидаемая инфляция, % годовых»
SELECT date AS time, value AS "…"
FROM v_cbr_macro
WHERE series = '…' AND $__timeFilter(date)
ORDER BY date
-- C
SELECT date AS time, value AS "Ценовые ожидания бизнеса (SA), пункты"
FROM v_cbr_survey
WHERE series = 'price_expectations_sa' AND $__timeFilter(date)
ORDER BY date
```

### Ряд 22 «Деньги, кредит, бюджет, труд» (панели 6–10)

**6. «Денежная масса и кредит, г/г»** (timeseries, pct-u; порог нуля `[{null,red},{0,green}]` `line`) — YoY окном по ПОЛНОМУ ряду, фильтр окна снаружи (как `yoyTemplate` gold; иначе первые 12 точек считались бы по обрезанному ряду).

```sql
SELECT time, yoy AS "…, г/г"
FROM (
    SELECT date AS time, value / nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0) - 1 AS yoy
    FROM v_cbr_macro
    WHERE series = 'М2 SA'   -- B: 'Кредит экономике'
)
WHERE $__timeFilter(time)
ORDER BY time
```

**7. «Кредитные ставки и ключевая»** (timeseries, pct) — трансмиссия: ключевая и ставки по кредитам юрлицам (месячные, 30-е числа).

```sql
-- A: 'cbr_key_rate' из v_cbr_macro (см. панель 1)
-- B/C:
SELECT date AS time, value AS "…"
FROM v_cbr_macro
WHERE series = 'Всего: до 1 года, включая «до востребования»'   -- C: 'СМП: до 1 года, включая «до востребования»'
  AND $__timeFilter(date)
ORDER BY date
```

**8. «Консолидированный бюджет: доходы, расходы, дефицит»** (timeseries, RUB; заголовок называет источник — это консолидированный бюджет Минфина) — A/B/C из `source='minfin_mesyats'` (единственный источник с дефицитом; проверить живьём, что все три ряда там есть).

```sql
SELECT date AS time, value AS "Расходы, всего, млрд руб."
FROM v_minfin_budget
WHERE source = 'minfin_mesyats' AND series = 'Расходы, всего'
  AND $__timeFilter(date)
ORDER BY date
-- B: 'Доходы, всего'; C: 'Дефицит (-)/Профицит (+)'
```

**9. «Рынок труда: безработица и зарплата»** (timeseries; A — pct; B — pct-u + override правой оси) — зарплата г/г окном по полному ряду.

```sql
-- A
SELECT date AS time, value AS "Уровень безработицы, %"
FROM v_rosstat_macro
WHERE series = 'Уровень безработицы' AND $__timeFilter(date)
ORDER BY date
-- B
SELECT time, yoy AS "Зарплата, г/г"
FROM (
    SELECT date AS time, value / nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0) - 1 AS yoy
    FROM v_rosstat_macro
    WHERE series = 'Среднемесячная номинальная начисленная заработная плата работников по полному кругу организаций'
)
WHERE $__timeFilter(time)
ORDER BY time
```

**10. «Расходы Сбера (nowcast), % г/г»** (timeseries, pct, span 24) — Description: атрибуция СберИндекс, лицензия не подтверждена (KEY_RATE_MODEL §4).

```sql
SELECT date AS time, value AS "Расходы Сбера, % г/г"
FROM v_bank_sber_spending
WHERE source = 'sber_izmenenie_trat' AND series = 'Все категории'
  AND $__timeFilter(date)
ORDER BY date
```

### Ряд 23 «Рыночный контур» (панели 11–14)

**11. «KEYRATE: вменённая траектория»** (timeseries, pct; KKG7 — точки, дальняя серия без сделок) — три известные серии; при появлении новых — точечное обновление (переменные — вне v1).

```sql
SELECT date AS time, settle AS "KEYRATE-10.26, % годовых"
FROM v_keyrate_expectations
WHERE secid = 'KKV6'   -- B: 'KKZ6'; C: 'KKG7'
  AND $__timeFilter(date)
ORDER BY date
```

**12. «KEYRATE: последний срез и ликвидность»** (table) — значение рядом с ликвидностью (чек-лист §9 статьи); `LIMIT` в таблице допустим (анти-паттерн — `LIMIT` на timeseries).

```sql
SELECT series_name AS "Серия", decision_date AS "Заседание", settle AS "Вменённая ставка, %",
       expected_change_bp AS "Ожидаемый шаг, б.п.", volume AS "Объём", openposition AS "Открытый интерес"
FROM v_keyrate_expectations
WHERE date = (SELECT max(date) FROM v_keyrate_expectations)
ORDER BY decision_date
```

**13. «Кривая ОФЗ и RGBI»** (timeseries; RGBI — unit `short`; теноры — pct + точки) — теноры по 1 точке (снимок); точки, не линии.

```sql
-- A
SELECT date AS time, yield AS "RGBI"
FROM v_ofz_curve
WHERE tenor = 'RGBI' AND $__timeFilter(date)
ORDER BY date
-- B..E: tenor IN ('1y','3y','5y','10y'), алиасы «1y, %» и т.д., override точек
SELECT date AS time, yield AS "1y, %"
FROM v_ofz_curve
WHERE tenor = '1y' AND $__timeFilter(date)
ORDER BY date
```

**14. «RUONIA и спред к ключевой»** (timeseries, pct) — спред считается в SQL самоджойном витрины.

```sql
-- A: 'cbr_ruania_ruo' из v_cbr_macro (см. панель 1); B: 'cbr_key_rate'
-- C
SELECT r.date AS time, r.value - k.value AS "Спред RUONIA к ключевой, п.п."
FROM v_cbr_macro AS r
INNER JOIN v_cbr_macro AS k ON k.date = r.date AND k.series = 'cbr_key_rate'
WHERE r.series = 'cbr_ruania_ruo' AND $__timeFilter(r.date)
ORDER BY r.date
```

### Ряд 24 «Сигнал, консенсус, календарь» (панели 15–19)

**15. «Консенсус макроопроса: ставка и инфляция»** (timeseries, pct) — медианы на 2026/2027 + интервалы 2027 (fixed colors).

```sql
SELECT date AS time, value AS "Ключевая, 2026 (медиана)"
FROM v_cbr_mo_br
WHERE series = 'key_rate_avg_2026_median' AND $__timeFilter(date)
ORDER BY date
-- B: 'key_rate_avg_2027_median'; C: 'cpi_avg_2026_median'; D: 'cpi_avg_2027_median';
-- E/F: 'key_rate_avg_2027_p10'/'_p90' («Интервал 2027, p10/p90»)
```

**16. «Последние резюме ЦБ: решение и сигнал»** (table).

```sql
SELECT meeting_date AS "Заседание", published AS "Публикация", decision_action AS "Действие",
       decision_rate AS "Ставка, %", decision_delta_bp AS "Шаг, б.п.", signal AS "Сигнал"
FROM v_cbr_mp_summary
ORDER BY meeting_date DESC
LIMIT 3
```

**17. «Каналы последнего резюме (риски)»** (table) — детерминированный слой регексов, риски обоих знаков.

```sql
SELECT channel AS "Канал", kind AS "Вид", text AS "Фрагмент"
FROM v_cbr_mp_summary_channels
WHERE method = 'regex' AND kind IN ('risk_pro', 'risk_dez')
  AND meeting_date = (SELECT max(meeting_date) FROM v_cbr_mp_summary_channels)
ORDER BY channel, kind
```

**18. «Календарь заседаний ЦБ»** (table).

```sql
SELECT event_date AS "Дата", event_time AS "Время", title AS "Событие", threshold AS "Порог"
FROM v_events_calendar
WHERE category = 'cbr' AND event_date >= today()
ORDER BY event_date
LIMIT 10
```

**19. «Событийный фон (лента)»** (table).

```sql
SELECT date_msk AS "Дата", source AS "Источник", title AS "Событие"
FROM v_news_events
ORDER BY date_msk DESC
LIMIT 15
```

## 6. Конвенции из UI-экспорта: что берём и что осознанно нет (разведка 2026-10-16)

**SQL-идиомы (переносим):** окно — `$__timeFilter(date)` (в UI — явные `$__fromTime/$__toTime` по DateTime-колонке `time`); бакетирование `$__timeInterval(date) as "time"` + `GROUP BY` там, где агрегируют; наложение лет — `date_add(YEAR, 1, date)` (в UI — со сдвинутым окном `date_sub(YEAR,…)`; у нас — календарные `toYear(date) = toYear(now()) - 1`, проще и не зависит от краёв окна); неделя→годовое `pow(avg(percent)/100,52)-1` (магические хак-даты первой недели UI — НЕ переносим); месяц→годовое `pow(x/100,12)-1` / `pow(1+x/100,12)-1`; изменение уровня — `lagInFrame(x, N, x) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` с запасом окна; алиасы `AS "…"`, осмысленные refId, `ORDER BY date/time ASC`.

**Визуал (переносим):** `palette-classic` + фикс-цвета override'ом; `lineWidth 1`, `fillOpacity 0`, `gradientMode none`, `spanNulls false`; осмысленные пороги (4% на инфляции, нуль на темпах) с обязательным `ThresholdsStyle`; единицы `percent`/`percentunit`/`currencyRUB`; легенда снизу с `calcs ["last","mean","max"]`; tooltip `single`; `description` — где значение легко прочитать неверно.

**Только в UI (переносим впервые):** пунктир прошлых лет + фикс-цвета, пороги-ступени с `super-light-*`/hex, пер-серийные `unit`-override для разнобоя размерностей, `hide:true` вспомогательных таргетов (в v1 не понадобился).

**Анти-паттерны UI-экспорта (НЕ переносим):** `LIMIT` на timeseries; врезка точек `UNION ALL SELECT toDateTime('…')`; хардкод годов/дат; забытый фильтр окна; разъезд имён (`'М2_SA'` vs `'М2 SA'`); дублирующая колонка `date` рядом с `"time"`; опечатки в алиасах/матчерах; `%%` и слишком широкий `LIKE`; смесь единиц одной серии; агрегаты поверх несмёрженных `final`; `meta.builderOptions`/`pluginVersion` панели — шум конструктора.

**Чего нет ни в UI, ни в коде (проектируем впервые):** панели OFZ/KEYRATE/консенсуса/каналов резюме, таблицы-срезы, `$__timeFilter` на сдвинутых выражениях. **Не делаем в v1:** аннотации-запросы из ClickHouse (заседания и так видны точками панели 1; форма — в отчёте разведки, follow-up), переменные темплейтинга, `transformations` (считаем в SQL), `stat`/`gauge`-плитки.

## 7. Реализация

**Go-структура** (все новые идентификаторы — с префиксом `kr`/`KeyRate`, чтобы не конфликтовать с gold в одном пакете):

- `grafana/key_rate_publications.go`: `const KeyRatePublicationsUID = "key-rate-publications"`; `NewKeyRatePublications() *dashboard.DashboardBuilder` (ряды id 21–24, панели 1–19, `Span(12)`/`Span(24)`); SQL-константы `kr…SQL` (или шаблоны с `%s` там, где отличаются только ряд/алиас — как `yoyTemplate` в gold); билдеры панелей `kr…Panel()`; переиспользование из gold: `datasourceRef()`, `timeseriesQuery()`, `tableQuery()`, `pointsOnlyProps()`, `axisPlacementRightProp()`, `ptr()`.
- `grafana/clickhouse.go`: `DashboardUID` → `GoldThesisUID` (переименование; второй uid — смысл имени «DashboardUID» пропадает).
- `grafana/validate.go`: `Validate(d dashboard.Dashboard, wantUID string) error` (сейчас uid зашит в константу).
- `grafana/gold_thesis.go`: `Manifest(d)` берёт `metadata.name` из `*d.Uid` (инвариант — непустой uid проверяет `Validate`).
- `grafana/main.go`: флаги `-dashboard` (`gold-thesis|key-rate-publications|all`, дефолт `all`) и `-out-dir` (дефолт `../dashboard`); запись `<out-dir>/<uid>.json`; список дашбордов — упорядоченный слайс (детерминизм).
- `grafana/gold_thesis_test.go`: обновить вызовы `Validate`/`Manifest`/константы (не более).

**Тесты `grafana/key_rate_publications_test.go`** (образец — gold-тесты; идут в том же PR, пишутся до кода):

1. состав: 19 панелей, 4 ряда, заголовки рядов, порядок id;
2. каждый таргет: `CHQuery` + `FROM v_`;
3. yoy-панели (6, 9): `lagInFrame` есть, `$__timeFilter(time)` — снаружи подзапроса;
4. панель 2: у B есть `date_add(YEAR, 1, date)` и `toYear(now()) - 1`;
5. единицы: панель 4 — `percentunit`, панель 9-B — override `unit=percentunit` + правая ось, панель 5-C — override `unit=short`;
6. разрежённые серии: панели 11 (KKG7) и 13 (теноры) — overrides `lineWidth 0` + `showPoints always`;
7. пороги: панели 2/4 (0.04) и 6 (0) — `ThresholdsStyle` ≠ off;
8. uuid/манифест/детерминизм: сборка дважды — одинаковый JSON; `Validate` для обоих дашбордов с их uid.

**Интеграция:**
- Makefile: `DASHBOARDS := gold-thesis key-rate-publications`; `grafana-build` собирает все; `grafana-deploy-test`/`-deploy-staging` пушят все артефакты; `grafana-check` прогоняет smoke по всем uid;
- `.github/workflows/dashboards.yml`: `git diff --exit-code -- dashboard/` (оба артефакта), `gcx dev lint run` по каждому файлу, upload обоих;
- `scripts/grafana_check.py` — без изменений (уже параметризован `--uid`).

**Документация (тем же PR):**
- `docs/KEY_RATE_PUBLICATIONS.md` — раздел «Иллюстрации: дашборд `key-rate-publications`»: таблица «раздел статьи → панели», где смотреть (локальный стенд/`make grafana-deploy-test`, staging), правило «дашборд — код, `make grafana-build`»;
- `README.md`, `ARCHITECTURE.md` — раздел Grafana: два дашборда, новый синтаксис CLI (`-dashboard`/`-out-dir`), артефакты, цели (скил `sync-readme-architecture`);
- `docs/ROADMAP_KEY_RATE.md` §6 фаза 4 — статус пункта ✅ (дата, объём v1); имя `key-rate.json` → `key-rate-publications.json`.

## 8. Приёмка пункта (протокол — комментарием в PR)

1. `make all` зелёный; `cd grafana && go vet ./... && go test ./...` зелёные.
2. `make grafana-build` детерминирован: повторный прогон не меняет ни один артефакт.
3. Живая SQL-сверка: каждый таргет артефакта прогнан через MCP (`kimi_reader`) с подстановкой окна вместо `$__timeFilter` — данные непустые; разрежённость (теноры ОФЗ, KKG7) названа явно.
4. Локальный стенд: `make grafana-deploy-test` (оба дашборда) + `make grafana-check` (оба uid) — ПРОВЕРКА ПРОЙДЕНА; визуальный просмотр — owner-assisted.
5. CI `dashboards` на PR зелёный.
6. Staging — owner-assisted (`make grafana-deploy-staging`), протокол в PR.
7. Документы синхронны (KEY_RATE_PUBLICATIONS / README / ARCHITECTURE / ROADMAP).

## 9. Границы (вне объёма v1)

- Панели по `v_key_rate_dashboard` (расхождение контуров, пороги, журнал прогнозов, точность) — когда появится витрина (фаза 4 дальше).
- Данные статьи, которых ещё нет в БД (❌ §6): диапазоны среднесрочного прогноза ЦБ, лента smart-lab.
- Аннотации-запросы, переменные темплейтинга, PNG-рендер панелей в репозиторий, SAAR-расчёт в SQL (SAAR — модель, не БД; панель 2 честно названа «годовое выражение»).
- Возможный конфликт при мерже: ветка `key-rate-model-v0` правит тот же §6 `ROADMAP_KEY_RATE.md`.
