---
name: dashboard-migration
description: Reproducible process for migrating a legacy Grafana UI export to dashboards-as-code, adding a dashboard for a new track, and revising an existing dashboard to the standard in clickhouse-import-rosstat
type: prompt
whenToUse: When a Grafana dashboard is migrated from a legacy UI export to grafana/ code, added for a new track (banks, metals, …), or revised to the standard
---

Дашборды Grafana в этом репозитории — код, а не экспорт из UI: источник истины — Go-модуль `grafana/` (grafana-foundation-sdk), артефакт `dashboard/<uid>.json` коммитится и собирается детерминированно целью `make grafana-build`; нормативное устройство механизма — [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.7. Скил описывает **процесс** для трёх режимов:

- **A — миграция легаси-экспорта**: UI-дашборд старой формы (`dashboard/*.json`) переводится в код панель за панелью — строго на существующих витринах `v_*`.
- **B — новый дашборд трека** (банки, металлургия, …): перечень графиков прогноза и прогнозных точек даёт владелец.
- **C — ревизия существующего дашборда** (в том числе уже собранного кодом) до стандарта — uid не меняется.

**Что прочитать перед стартом.** Спека и план текущего дашборда, если пункт уже оформлен (`docs/superpowers/{specs,plans}`); [sql/mcp_kimi_reader.sql](../../../sql/mcp_kimi_reader.sql) — гранты витрин агенту; `views/*.go` — определения и комментарии витрин, которые читают панели. Образцы кода — `grafana/gold_thesis.go`, `grafana/key_rate_publications.go` и их тесты; точки интеграции — упорядоченный слайс в `grafana/main.go`, `DASHBOARDS` в `Makefile`, `.github/workflows/dashboards.yml`. Синк доков — скил [sync-readme-architecture](../sync-readme-architecture/SKILL.md).

**Чек-лист (9 шагов).**

1. **Пункт роадмапа.** Назвать или завести пункт (инфраструктурный — `ROADMAP_INFRA`, трековый — роадмап трека) и объявить его с критерием приёмки до старта; без пункта работа не начинается.
2. **Инвентаризация источника.** Легаси: панели, таргеты, источники данных, юниты, анти-паттерны — по экспорту. Трек: перечень графиков прогноза и прогнозных точек от владельца.
3. **Гэп-анализ «источник → витрина».** Каждая панель → существующая `v_*` + `series`/ключи. Недостающие слои не выдумывать: либо минимальный набор v1 на готовом, либо отдельный пункт-следствие.
4. **Гранты.** Сверить `sql/mcp_kimi_reader.sql`: панель на невыданной витрине валит staging-smoke; при отсутствии — добавить `GRANT SELECT` в `sql/mcp_kimi_reader.sql` и выполнить `make mcp-user` (AGENTS.md, правило 11в).
5. **Маппинг «раздел статьи/трек → панели».** Таблица в спеке; для статьи со временем — её §«Иллюстрации» (образец — [docs/KEY_RATE_PUBLICATIONS.md](../../../docs/KEY_RATE_PUBLICATIONS.md) §11).
6. **Спека + план** (`docs/superpowers/{specs,plans}`, первыми коммитами ветки): SQL — дословный контракт, серии сверены живыми запросами до фиксации.
7. **Реализация.** Builder в `grafana/`, тесты (состав панелей и рядов, `FROM v_` в каждом таргете, `$__dateFilter`/`$__timeFilter` в каждом таргете, юниты, точки и пороги, детерминизм), конвенции — по ссылкам ниже и памятке «Time-series SQL: практики ClickHouse».
8. **Регистрация.** Упорядоченный слайс в `grafana/main.go`, `DASHBOARDS` в `Makefile`, списки lint/upload в CI; артефакт `dashboard/<uid>.json` коммитится (при миграции заменяет легаси-экспорт, история — в git).
9. **Приёмка и доки.** `make all`; живой SQL всех таргетов через MCP под `kimi_reader`; `make grafana-deploy-test` + `make grafana-check` (staging — owner-assisted); CI зелёный; протокол живой приёмки — комментарием в PR ([docs/IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md) §16.1 п. 8); синк доков скилом `sync-readme-architecture`; регистрация следствий и обновление очереди.

**Конвенции** (нормативные формулировки не дублируются — только ссылки):

- [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.7 — дашборд как код: модуль `grafana/`, детерминизм, `Validate`, провижининг датасорса, CI и деплой.
- [Спека gold-thesis](../../../docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md) §3 (архитектура) и §6 (runbook); [спека key-rate-publications](../../../docs/superpowers/specs/2026-10-16-grafana-key-rate-design.md) §6 (конвенции из UI-экспорта: что берём и что осознанно нет).
- Time-series SQL — памятка «Time-series SQL: практики ClickHouse» ниже (10 правил; проверяются тестами `grafana/`).

Ссылки покрывают: запросы только к `v_*`; `$__timeFilter` в каждом графике — по вычисленному `time`, если время сдвинуто (сдвиг года, скользящее окно), норма проверяется тестами `grafana/` и валит `make grafana-validate`; YoY окном по полному ряду; юниты `percent`/`percentunit`/`currencyRUB`/`currencyUSD`/`short`; точки для разрежённых серий; пороги со `ThresholdsStyle`; русские алиасы; детерминизм; описания панелей.

**Общие правила чтения витрин** (поиск ряда, `argMax`, фильтр по сырой колонке, бакеты, пропуск ≠ ноль, `EXPLAIN ESTIMATE`, ловушки данных) — скил `rosstat-sql` плагина [`kmlebedev/rosstat-mcp`](https://github.com/kmlebedev/rosstat-mcp/blob/main/skills/rosstat-sql/SKILL.md), разделы *Workflow*, *Query rules* и *Data traps*; готовые запросы — `references/templates.md`. Памятка ниже — их Grafana-проекция: макросы `$__timeFilter`/`$__timeInterval`, панели и то, что проверяют тесты `grafana/`.

**Time-series SQL: практики ClickHouse (нормативная памятка).** Источник — [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse); актуальные возможности — [`toStartOfInterval`](https://clickhouse.com/docs/sql-reference/functions/date-time-functions#toStartOfInterval), [`ORDER BY … WITH FILL`](https://clickhouse.com/docs/sql-reference/statements/select/order-by#fill), [`timeSeries*ToGrid`](https://clickhouse.com/docs/reference/functions/aggregate-functions/timeSeriesRateToGrid). Правила применяются к каждому графику; тесты `grafana/` проверяют фильтры, отсутствие `LIMIT` на графиках и сырых таблиц.

1. **Тип времени и зона.** Время — `DateTime`/`DateTime64` с явной зоной (`Europe/Moscow` для биржи). Панель не конвертирует зону в SQL: отображение задаёт `Timezone(browser)`; `$__timeFilter` сравнивает инстанты и от зоны не зависит.
2. **Фильтр — по сырой колонке.** `$__timeFilter(time)` для `DateTime`, `$__dateFilter(date)` для `Date`; в `WHERE` до агрегации, без функций над колонкой — иначе теряется индекс. Фильтр по вычисленной оси — только осознанное исключение (наложение года, скользящее окно) и под тестом.
3. **Бакеты.** Адаптивный шаг — `$__timeInterval(col)` (разворачивается в `toStartOfInterval`); фиксированный — `toStartOfInterval(col, INTERVAL n unit)`. **Ловушка, проверенная на 26.10.1 (сервер `Asia/Yekaterinburg`):** интервал в секундах выравнивается по UTC, unit-форма — по календарю зоны колонки; `INTERVAL 86400 SECOND` даёт дневной бакет `03:00 МСК`, `INTERVAL 1 DAY` — `00:00 МСК`. Календарные день/неделя/месяц — только unit-форма; 3-й аргумент `toStartOfInterval` — зона, не origin; origin-вариант (`date_bin`/`time_bucket` из docs) проверять на версии стенда.
4. **Пропуск ≠ ноль.** `WITH FILL … STEP` + `INTERPOLATE` — только если отсутствие строки означает ноль (счётчик) или последнее значение (gauge). Для разрывов ленты/неизвестных данных нули не подставлять: `spanNulls=false`, точки, оговорка в описании панели.
5. **Счётчик vs gauge.** Потоки и объёмы — `sum()` по бакету; gauge — `any()`/`argMax` + `INTERPOLATE`; сырые Prometheus-счётчики — `timeSeriesRateToGrid`/`DeltaToGrid`/`ResampleToGridWithStaleness` (25.6+) вместо ручного `lagInFrame`.
6. **Агрегация — в ClickHouse.** Никаких `LIMIT` на timeseries (только top-N категориальных панелей), `ORDER BY time`, суммы/разности/знаки — в SQL; `transformations` не нужны.
7. **Зерно.** Диапазонные агрегаты — по уже агрегированной витрине нужного зерна (`*_daily`), минутные графики — по минутной; сырые события в панели не сканируются.
8. **Разреженность.** Линию по редким точкам не рисовать без оговорки; top-N — `LIMIT`, распределение — таблица/бар; юнит и decimals фиксируются (`currencyRUB`, `percent`, …).
9. **Схема — справочно.** Для панельных фильтров `ORDER BY` начинается с измерений фильтра, затем время; `LowCardinality(String)` для измерений, `Delta`+`ZSTD` для времени; изменение схемы — только спекой и DDL §6.2.
10. **Проверка.** Перед тяжёлым SELECT — `EXPLAIN ESTIMATE`; живой прогон SQL под `kimi_reader`; smoke на непустых данных; тесты `grafana/` проверяют `$__dateFilter`/`$__timeFilter`, отсутствие `LIMIT` на графиках и сырых таблиц.

**Очередь миграции.** Живой список — в пункте `ROADMAP_INFRA` (Фаза 2, «Шаг 0: механизм миграции и добавления дашбордов»); состав на 2026-10-31:

1. `finance-polyus` — дашборд статьи PLZL, мигрирован 2026-10-16;
2. `transaq-trades` — дашборд ленты сделок из sibling-проекта `transaq-clickhouse-exporter` (пункт `ofz-market/1.6`), мигрирован 2026-10-31;
3. `chmf.json` — шаг 6 [docs/ROADMAP_DCF_METALS.md](../../../docs/ROADMAP_DCF_METALS.md);
4. `rosstat.json` и корневой `grafana_dashboard_rosstat`;
5. дашборды новых треков (банки, металлургия и др.) — по этому скилу.

Каждый следующий дашборд обновляет очередь тем же PR, что и сам дашборд (шаг 9).
