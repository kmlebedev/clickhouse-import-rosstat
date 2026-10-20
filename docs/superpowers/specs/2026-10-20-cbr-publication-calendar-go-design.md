# Спека: календарь публикаций ЦБ и `published_at` для рядов контура ставки

**Дата:** 2026-10-20
**Пункт:** `key-rate/phase-4-cbr-publication-calendar`, Task 1 (issue [#299](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/299))
**Владелец:** [docs/ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §6, фаза 4
**Research:** [2026-10-20-cbr-publication-calendar.md](../../research/2026-10-20-cbr-publication-calendar.md)
**План:** [2026-10-20-cbr-publication-calendar-go.md](../plans/2026-10-20-cbr-publication-calendar-go.md)

## 1. Цель

Дать контуру ключевой ставки два слоя публикационной информации:

1. `cbr_publication_calendar` — график публикаций ЦБ (`https://www.cbr.ru/calendar/`): статистическое окно вперёд и 22 аналитических месячных блока назад.
2. `series_publication` — точные исторические `published_at` для точек `cbr_infl_exp`, `cbr_indicators_cpd`, `cbr_survey` (`price_expectations`, `price_expectations_sa`), с явным качеством `exact_time` / `date` / `fallback`.

Python-часть (as-of по заседанию, #300) и ML-rerun (#301) в этот PR не входят; `forecast_log` не меняется.

## 2. Источники и что из них берём

| Компонент | URL | Что берём | Глубина |
|---|---|---|---|
| Календарь, статистика | `https://www.cbr.ru/calendar/`, вкладка `#tab_content_t1`, `table.data` с заголовком «Дата публикации» | `publication_date`, `publication_time`, `title`, `period` (текст после `<br>`), `source_url` | окно вперёд; снимок 2026-10-20 — 173 строки, 20.10.2026 … 30.12.2026 |
| Календарь, аналитика | та же страница, `#tab_content_t2 .dropdown` | 22 блока «Календарь публикации на <месяц> <год> года»; строки `(title, date, source_url)`, `period = YYYY-MM` | январь 2025 … октябрь 2026, 445 строк |
| `cbr_infl_exp` | `https://www.cbr.ru/analytics/dkp/inflationary_expectations/` | `a.versions_item` с `data-tooltip-content="Опубликовано <дата>"`; период из имени `Infl_exp_YY-MM.(xlsx\|pdf)`/`inFOM_YY-MM.pdf` | 2013-07+ |
| `cbr_indicators_cpd` | `https://www.cbr.ru/analytics/dkp/dinamic/` | PDF `INF_YYYY-MM.pdf` с tooltip-датой (2016–2022); HTML `CPD_YYYY-M` — дата со страницы версии (2021+), селектор `.news-info-line_date` | 2016+ |
| `cbr_survey` | `https://www.cbr.ru/analytics/dkp/monitoring/` | ссылки `monitoring/<MMYY>` и `<MM_YY>`; дата со страницы версии, `.news-info-line_date` | 2022-05+ |

Дата в календаре — **план**, а не журнал фактов: для прошедших месяцев research сверил 12 дат с релизными страницами, совпадение полное. Для исторического `published_at` источник — только релизные архивы, не календарь.

## 3. Схемы

### 3.1. `cbr_publication_calendar`

```sql
CREATE TABLE IF NOT EXISTS %s (
    publication_date Date,
    publication_time Nullable(String),      -- «16:00»; NULL — время не указано
    title String,
    topic LowCardinality(String),           -- statistics | analytics
    period String,                          -- период из строки ЦБ либо YYYY-MM блока
    source_url String,                      -- cbr.ru + href показателя/публикации
    planned UInt8,                          -- 1 — дата была в будущем на момент снимка
    loaded_at DateTime('Europe/Moscow')     -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (publication_date, title, period, source_url);
```

- `topic`: `statistics` для вкладки `t1`, `analytics` для блоков `t2`.
- `period`: у статистики — исходный текст («II квартал 2026 г.», «01.10.2026»), у аналитики — `YYYY-MM` блока.
- `planned = 1`, если `publication_date >= дата снимка` (в MSK), иначе `0`. Значение — снимок на момент импорта; прошлые строки не переписываются.
- `loaded_at` — момент записи версии; в витрину не выходит.

### 3.2. `series_publication` (generic)

DDL — `util/series_publication.go`, по образцу `util.PublicationsDdl`.

```sql
CREATE TABLE IF NOT EXISTS series_publication (
    source LowCardinality(String),          -- как в v_series_catalog: cbr | cbr_survey
    series LowCardinality(String),
    period_date Date,                       -- точная дата точки источника
    published_at DateTime('Europe/Moscow'), -- effective: 23:59:59 MSK для date-only
    quality LowCardinality(String),         -- exact_time | date | fallback
    source_url String,
    loaded_at DateTime('Europe/Moscow') DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (source, series, period_date);
```

- `source` — каталожный, чтобы join к `v_series_catalog`/`v_cbr_macro` шёл по `(source, series)`: `cbr` для `cbr_infl_exp` и `cbr_indicators_cpd`, `cbr_survey` для мониторинга предприятий.
- `period_date` — дата точки в таблице источника: 14-е у инфляционных ожиданий, последний день месяца у ИПЦ, 1-е у мониторинга. Месяц для поиска в архиве — `toStartOfMonth(period_date)`.
- `qualities`:
  - `exact_time` — известно время публикации. Схема поддерживает; текущие три релиза времени не публикуют, поэтому в живой таблице таких строк 0 (тест на чистую функцию покрывает);
  - `date` — известна дата релиза; `published_at = <дата> 23:59:59 MSK`;
  - `fallback` — точной даты нет; `published_at = последний день M+1 23:59:59 MSK`, где M — отчётный месяц точки.
- `loaded_at` — версия ReplacingMergeTree: нужна для детерминированного апгрейда `fallback → date` и `date → exact_time`. В витрину не выходит. Понижение качества запрещено.
- Строка создаётся только для существующей точки источника; периоды без точек не заводятся.

## 4. Импортёр `cbr_publication_calendar`

Пакет `cbr/`, регистрация — только `cbr/importers.go`. Один импортёр пишет две таблицы (прецедент `cbr_mp_summary` с тремя таблицами): обе — один публикационный контур, и приёмка issue — одна команда `make import STAT=cbr_publication_calendar`.

### 4.1. Шаблон

`util.ClickHouseImport` с `CrawFunc`:

- `TableName = "cbr_publication_calendar"`, `CreateTable` — DDL §3.1 в `%s`-форме;
- `DataUrl = "https://www.cbr.ru/calendar/"`;
- `CrawFunc` — полный цикл: DDL `series_publication` → разбор календаря → дельта-запись календаря → сбор точек трёх источников → архивные даты → дельта-запись `series_publication`;
- `AfterImport` — витрины `v_cbr_publication_calendar` и `v_series_publication`;
- `PublicationCountPoints` не объявляется.

### 4.2. Почему без HEAD-префлайта

Календарь — ежедневный источник: окно статистики сдвигается каждый день, поэтому отпечаток `HEAD` (у ЦБ есть только `Content-Length`) меняется ежедневно и устойчивого no-op не даёт. Идемпотентность держит дельта по содержимому: существующие ключи читаются `FINAL`, новые/изменившиеся строки пишутся одним батчем. Прецедент отказа — `cbr_zcyc` (дневной HTML-источник). `source_publications` не пишется. Повтор в тот же день — `Imported 0 rows`.

### 4.3. Дельта-запись календаря

Ключ строки — `(publication_date, title, period, source_url)`. Сравниваются `publication_time`, `topic`, `planned`; при изменении пишется новая версия с `loaded_at = now()`. Исчезнувшие из источника строки не удаляются — это история.

Парсер fail-fast: нет таблицы с заголовком «Дата публикации», нет ни одной статистической строки или ни одного аналитического блока — ошибка с именем таблицы. Строки без даты (ежедневные таблицы «ежедневно по рабочим дням») пропускаются.

### 4.4. Backfill `series_publication`

1. Читаем точки источников: `SELECT name, date FROM cbr_infl_exp FINAL`, `SELECT name, date FROM cbr_indicators_cpd FINAL`, `SELECT series, date FROM macro_series FINAL WHERE source='cbr_survey' AND series IN ('price_expectations','price_expectations_sa')`.
2. Читаем существующие `series_publication FINAL` в map `(source, series, period_date)`.
3. Для периодов, где нужна точная дата (`нет строки` или `quality='fallback'`), качаем индексы архивов:
   - `infl_exp`: `a.versions_item[data-tooltip-content]`, имя файла `(Infl_exp|inFOM)_YY-MM.(xlsx|pdf)`; XLSX приоритетнее PDF при дубле периода;
   - `cpd`: `INF_YYYY-MM.pdf` tooltip (2016–2022) + `CPD_YYYY-M/` HTML-страницы (2021+), дата — `.news-info-line_date`; дубль периода — HTML приоритетнее PDF;
   - `survey`: ссылки `monitoring/(MMYY|MM_YY)`; для каждого дельта-периода — страница версии, дата — `.news-info-line_date`.
4. Если точной даты нет (404, кривая ссылка, периода нет в архиве) — `fallback` на конец M+1. Ошибка сети/индекса — `warn`, fallback, прогон не падает.
5. Пишем только отсутствующие строки и апгрейды качества/даты. Понижение `exact_time → date → fallback` запрещено.

Ожидаемый объём на снимке 2026-10-20: `series_publication` ≈ 1 274 строки — 90 `infl_exp` + 590 `cpd` + 594 `survey`; из них ≈ 452 `date` и ≈ 822 `fallback`.

### 4.5. Транспорт и ошибки

- Только `util.GetPage`/`util.HttpClient`; сетевых вызовов в `init()` нет.
- `batch.Append` и `time.Parse` — с проверкой; на ошибке `Abort`.
- Один `PrepareBatch` на таблицу за прогон; построчного `conn.Exec` в цикле нет.
- Логи: статистических строк, аналитических блоков, точек источников, `date`/`fallback`, новых/изменённых; `Import` возвращает сумму новых строк.

## 5. Витрины и гранты

- `v_cbr_publication_calendar` — `SELECT publication_date, publication_time, title, topic, period, source_url, planned FROM cbr_publication_calendar FINAL`; комментарии таблицы и всех колонок; предупреждение, что это план, а не журнал фактов.
- `v_series_publication` — `SELECT source, series, period_date, published_at, quality, source_url FROM series_publication FINAL`; комментарии: quality, effective 23:59:59 для `date`, fallback M+1, строгая граница as-of `published_at < cutoff`, source-конвенция.
- `GRANT SELECT` на обе витрины в `sql/mcp_kimi_reader.sql`; сырые таблицы не выдаются.
- `series_catalog` не расширяется: новых числовых рядов нет, это метаданные к уже описанным рядам. Отступление от правила 11(а) AGENTS.md названо здесь; п. 11(б)–(г) выполнены (витрины с комментариями, гранты, MCP-проверки).

## 6. Расписание и реестр

- `dagu/cbr.yaml` — шаг `cbr_publication_calendar` в хвосте цепочки, тот же retry, что у остальных.
- `cbr/importers.go` — запись с литералом таблицы; `cbr/importers_test.go` — шаг в `keyScheduled` (обратная проверка «шаг не пропал»); набор preflight-импортёров не меняется.

## 7. Тесты

Фикстуры — реальные срезы 2026-10-20 в `cbr/testdata/`:

- `publication_calendar_2026-10-20.html` — 173 статистических строки, 22 блока, 445 аналитических строк; проверяются даты, NULL-время, `planned`, `period`, `source_url`;
- `infl_exp_archive_2026-10-20.html` — 2026-09 → 2026-09-29, 2025-12 → 2025-12-24, 2013-09 → 2013-10-16;
- `cpd_archive_2026-10-20.html` — `INF_2022-06.pdf` tooltip, битые `CPD_2025-4/5`, `CPD_2022-6`; отсутствие tooltip у HTML-ссылок не ломает разбор;
- `cpd_2026-09.html` — `14 октября 2026 года` из `.news-info-line_date`;
- `mp_archive_2026-10-20.html` — 54 ссылки, `0926` → 2026-09, `05_22` → 2022-05;
- `mp_0926.html` — `15 сентября 2026 года`.

Чистые функции: `dateOnlyPublishedAt` (23:59:59 MSK), `fallbackPublishedAt` (конец M+1), `calendarRowChanged`, `seriesPublicationChanged` (апгрейд разрешён, понижение — нет), `exact_time` — тест без живых данных. Парсеры не ходят в сеть; 404/кривая ссылка — пропуск, не ошибка.

## 8. Приёмка

1. `make import STAT=cbr_publication_calendar` — 173+ статистических строки, 22 блока; логи точных/fallback; строки в обеих таблицах.
2. Повтор — `Imported 0 rows`, `count()` обеих таблиц не изменился.
3. MCP: `v_cbr_publication_calendar` — `topic` statistics+analytics; `v_series_publication` — `date`+`fallback`; точные даты research: `cbr`/`ожидаемая инфляция`/2026-09-14 → 2026-09-29, `cbr`/`Все товары и услуги`/2026-08-31 → 2026-09-16, `cbr_survey`/`price_expectations`/2026-09-01 → 2026-09-15.
4. `make all`, `go test -race -count=1 ./cbr/`, `make docs-check`, `dagu validate dagu/cbr.yaml`, `make mcp-user && make mcp-check` — зелёные.
5. Сырые `cbr_publication_calendar` и `series_publication` закрыты для `kimi_reader`; Python/ML и `forecast_log` не изменены.

## 9. Риски

- **Research PR #302 ещё не merged.** Ветка Go-части базируется на `research/240-cbr-calendar`; PR в `main` — после мержа #302, иначе в диффе окажутся research-документы.
- **Архив ЦБ недоступен.** Остаётся fallback; на следующем прогоне точные даты обновятся. Это деградация, не падение.
- **Дата-зависимые фикстуры.** Снимок 2026-10-20; живые числа после сдвига окна отличаются, тесты фиксируют снимок.
- **`planned` — снимок, не текущий статус.** Прошлые строки не переписываются, чтобы не плодить версии.
- **MSK.** Используется `time.FixedZone("MSK", 3*3600)` по конвенции репозитория; для fallback 2002–2013 час исторического DST несущественен на фоне месячного буфера.
