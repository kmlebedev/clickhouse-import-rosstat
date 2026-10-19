# Импорт 16 отраслевых листов `mp_survey_data.xlsx` (Task 6)

> **Объект:** Task 6 пункта `key-rate/phase-4-ml-components` (issue [#239](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/239), родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
> **Задача:** завести в ClickHouse публичные 16 отраслевых листов `mp_survey_data.xlsx` (12 raw + 11 SA показателей каждого листа), не используя 87 внутренних кодов ОКВЭД2 ЦБ; опубликовать каталог рядов и `v_cbr_survey`; проверить MCP.
> **Зависимость:** #238 (Task 5, вердикт v3) открыт. Эта работа — только Go-импорт публичных данных; вердикт v3/паритет с CB_COMP не заявляется, #238 не закрывается. Владелец явно запросил Task 6.

**Статус:** реализация.

---

## 1. Цель и границы

### 1.1. Цель

1. Расширить существующий импортёр `cbr_mp_survey` так, чтобы он читал 16 отраслевых листов публичной книги `mp_survey_data.xlsx` и писал ряды в общую `macro_series` с `source = 'cbr_survey'`.
2. Описать каждый новый ряд в `series_catalog` через `util.SeriesMeta` (`title`, `unit`, `frequency`, `origin`, `description`).
3. Обновить витрину `v_cbr_survey` и её комментарии; грант `kimi_reader` уже существует и покрывает новые строки.
4. Обновить `scripts/mcp_check.py` (каталог 381 ряд, отраслевые 368) и пройти живую приёмку.

### 1.2. Не входит

- 87 кодов ОКВЭД2 и любые внутренние выгрузки ЦБ — в БД, DDL, витрины, гранты и dagu не попадают.
- Изменение существующих 13 агрегатных рядов `cbr_survey`, их имён и значений.
- Новая таблица/DDL: все точки идут в существующую `macro_series`.
- Новый импортёр, новый шаг `dagu/cbr.yaml`, новый `Name()`.
- v3-модель, `make`/dagu для v3, сравнение с CB_COMP, вердикт #238.
- Изменение `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_cbr_survey TO kimi_reader` уже выдан (строка 41).
- Point-in-time/винтажность отраслевых рядов: источник публикует одну версию книги, префлайт уже есть у импортёра.

## 2. Источник и данные

- URL: `https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx` (стабильный id 135603; имя файла не меняется).
- Книга: 31 лист, 2 193 461 байт, sha256 `c5ad8996bda9f21715a05de3bbe98d1511fb6c160b064f1e9c1090eb501c945b` (проверено 2026-10-19).
- Из 31 листа читаются 20: 4 прежних агрегатных/квартальных + 16 отраслевых. Остальные 11 — методические/спецификации/«Участники опроса».
- Все 16 отраслевых листов месячные, 297 месяцев `2002-01 … 2026-09`; данные — с 5-й строки, маркеры блоков в строке 3, заголовки в строке 4.
- В каждом листе:
  - raw-блок `C…N` — 12 показателей;
  - SA-блок `P…Z` — 11 показателей (у `credit_conditions` SA-пары нет);
  - `price_growth_expected` (N/Z) — вопрос анкеты с 2020 г., 81 точка `2020-01 … 2026-09`;
  - остальные 21 ряд — 297 точек `2002-01 … 2026-09`.

### 2.1. Листы и slug’и

| Лист | Slug |
|---|---|
| `Промышленность` | `industry_total_*` |
| `Добыча` | `industry_mining_*` |
| `Обработка` | `industry_manufacturing_*` |
| `Обработка_потребительские` | `industry_manufacturing_consumer_*` |
| `Обработка_инвестиционные` | `industry_manufacturing_investment_*` |
| `Обработка_промежуточные` | `industry_manufacturing_intermediate_*` |
| `ЭЭГП` | `industry_electricity_*` |
| `Водоснабжение` | `industry_water_*` |
| `Сельское хозяйство` | `industry_agriculture_*` |
| `Строительство` | `industry_construction_*` |
| `Торговля (45+46+47)` | `industry_trade_*` |
| `Торговля авто (45)` | `industry_trade_auto_*` |
| `Торговля оптовая (46)` | `industry_trade_wholesale_*` |
| `Торговля розничная (47)` | `industry_trade_retail_*` |
| `Транспортировка и хранение` | `industry_transport_*` |
| `Услуги` | `industry_services_*` |

### 2.2. Показатели

| Колонка raw | Slug | SA-колонка | Единица | Заголовок (подстрока) |
|---|---|---|---|---|
| C | `ibk` | P | пункты | `Индикатор бизнес-климата Банка России` (исключить `(факт)`, `(ожид)`) |
| D | `ibk_fact` | Q | пункты | `Индикатор бизнес-климата Банка России (факт)` |
| E | `ibk_expected` | R | пункты | `Индикатор бизнес-климата Банка России (ожид)` |
| F | `output_change` | S | пункты | `Как изменился объем производства` |
| G | `output_expected` | T | пункты | `Как изменится в ближайшие три месяца объем производства` |
| H | `demand_change` | U | пункты | `Как изменился спрос` |
| I | `demand_expected` | V | пункты | `Как изменится в ближайшие три месяца спрос` |
| J | `price_expectations` | W | пункты | `Ценовые ожидания предприятий на следующие 3 месяца` |
| K | `price_change` | X | пункты | `Как изменились цены на готовую продукцию` |
| L | `cost_change` | Y | пункты | `Как изменились издержки производства` |
| M | `credit_conditions` | — | пункты | `Как изменились условия кредитования` |
| N | `price_growth_expected` | Z | % годовых | `Средний ожидаемый` |

Имя ряда: `industry_<sheet_slug>_<indicator_slug>`; SA — суффикс `_sa`. Примеры:
- `industry_total_ibk`, `industry_total_ibk_sa`;
- `industry_mining_price_expectations`, `industry_mining_price_expectations_sa`;
- `industry_services_cost_change` (без `_sa`);
- `industry_trade_retail_price_growth_expected_sa`.

### 2.3. Итоговые числа

- 16 листов × 23 ряда = **368 отраслевых рядов**.
- На лист: 21 × 297 + 2 × 81 = **6 399 точек**; всего **102 384**.
- Плюс 13 агрегатных / 2 097 точек (снимок 2026-10-19; на 2026-10-13 было 2 084) → **381 ряд, 104 481 точка** `source='cbr_survey'`.
- `max(date)` после импорта — `2026-10-01` (квартальные ряды волны 3к26); отраслевые листы — до `2026-09-01`, месячные агрегатные — до `2026-09-01`.

## 3. Реализация

### 3.1. Определения и каталог — `cbr/mp_survey_industry_meta.go`

- `industrySheet{Sheet, Slug, Title}` — 16 записей.
- `industryIndicator{Slug, Header, Exclude, SA, Unit, Title, Description}` — 12 записей.
- `buildIndustrySeries(sheets, indicators) ([]seriesDef, []util.SeriesMeta)`:
  - порядок детерминирован: лист → показатель → raw, затем SA;
  - `Frequency: "M"`, `Source: mpSurveySource`;
  - `Origin`: `ЦБ, мониторинг предприятий (mp_survey_data.xlsx, id 135603), лист «<sheet>», колонка «<header>…» (<блок>)`;
  - `Title`: `<показатель> — <лист>[, с.к.], <единица>`;
  - `Description`: смысл показателя, отраслевой разрез, конвенция даты (1-е число отчётного месяца; волна публикуется в первую декаду следующего месяца ~10–15-го), для SA — «сезонно сглаженный ряд», для `credit_conditions` — «SA-варианта в источнике нет», для `price_growth_expected` — «вопрос с 2020 г., 81 точка».
- `var mpSurveyIndustrySeries, mpSurveyIndustrySeriesMeta = buildIndustrySeries(...)`.
- `var mpSurveyAllSeries = slices.Concat(mpSurveySeries, mpSurveyIndustrySeries)`.
- `var mpSurveyAllMeta = slices.Concat(mpSurveySeriesMeta, mpSurveyIndustrySeriesMeta)`.
- `func isMonthlySurveySheet(sheet string) bool` — true для `monthlySheet` и всех 16 отраслевых листов.

### 3.2. Парсер — `cbr/mp_survey_parse.go`

- В `readSheet`: `if isMonthlySurveySheet(sheet)` вместо `if sheet == monthlySheet`.
- Комментарий: месячные листы — «Экономика всего» + 16 отраслевых; квартальные — три прежних.
- `findColumn`/`extractPoints`/пороги не меняются. Отраслевые листы проходят `minDatedRows = 200` (297 строк).
- Raw-чтение (`RawCellValue`) сохраняется: значения и даты не теряют точность.

### 3.3. Импортёр — `cbr/mp_survey.go`

- `ImportFunc`: `parseSurvey(xlsx, mpSurveyAllSeries)`.
- Лог: `Собрано %d точек мониторинга предприятий (13 агрегатных + 16 отраслевых листов)`.
- `AfterImport`: `util.UpsertSeriesCatalog(ctx, conn, mpSurveyAllMeta)`, затем `util.CreateView` (без изменений).
- `mpSurveyCountPoints` не меняется: `SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'`.
- `Name()`/`TableName`/`PublicationSource` не меняются.

### 3.4. Витрина — `cbr/views.go`

- `cbrSurveyView.Select` не меняется: `macro_series FINAL WHERE source = 'cbr_survey'`.
- `Comment`: 13 агрегатных + 368 отраслевых рядов, 16 листов, `industry_<sheet>_<indicator>`, SA-пары, дата/публикация, ссылка на `v_series_catalog`.
- `Columns["series"]`: 13 агрегатных + 368 отраслевых (`industry_*`).

### 3.5. MCP — `scripts/mcp_check.py`

- Каталог `cbr_survey`: 381 (было 13).
- Дополнительно: `startsWith(series,'industry_')` в `v_series_catalog` = 368; в `v_cbr_survey` отраслевые строки непусты.
- Точное число точек витрины не проверять: оно растёт с волнами источника.

### 3.6. Грант

`sql/mcp_kimi_reader.sql:41` уже содержит `GRANT SELECT ON default.v_cbr_survey TO kimi_reader;`. Новых грантов не требуется; приёмка — `make mcp-user` / `make mcp-check`.

## 4. Критическая миграция префлайта

`cbr_mp_survey` уже имеет строку в `source_publications` (`points = 2084`) для текущей версии файла. После расширения набора рядов префлайт увидит тот же HEAD-отпечаток и `count() = 2084` и уйдёт в no-op, не импортировав отраслевые ряды. Перед первым прогоном:

```sql
SELECT source, name, fingerprint, points
FROM source_publications FINAL
WHERE source = 'cbr_mp_survey';

SET mutations_sync = 1;
ALTER TABLE source_publications DELETE WHERE source = 'cbr_mp_survey';
```

- Выполнить admin-пользователем (`make ch-sql FILE=build/mp-survey-migration.sql`; файл не коммитить) на dev/staging/prod.
- Первый `make import STAT=cbr_mp_survey` после удаления — полный импорт; строка версии запишется с `points = 104481`; далее no-op.
- Прерывание импорта после удаления безопасно: следующий прогон повторит полный импорт (идемпотентно).
- Правило зафиксировать в `docs/IMPORTER_PACKAGE_STANDARD.md` §9.7 и `ARCHITECTURE.md` §8 п. 13.

## 5. Тесты и фикстура

### 5.1. Фикстура

- `cbr/testdata/mp_survey_industry.xlsx` — полная реальная книга (2 193 461 байт, sha256 из §2), не синтетика.
- Тест читает только 16 отраслевых листов; остальные 15 листов игнорируются.
- Существующая `cbr/testdata/mp_survey_data.xlsx` (4 листа) и агрегатные тесты не меняются.

### 5.2. Парсер

`cbr/mp_survey_industry_test.go`:
- 102 384 точки, 368 уникальных рядов, каждое имя с префиксом `industry_`;
- 297 точек у 21 ряда, 81 у `price_growth_expected`/`_sa`; крайние даты;
- 16 `credit_conditions` без `_sa`;
- spot-значения (tolerance `1e-6`):
  - `industry_total_ibk` 2026-09-01 = `1.55627937374226`; 2002-01-01 = `-0.0321376505404771`;
  - `industry_mining_price_expectations` 2026-09-01 = `6.89655172413794`; `_sa` = `8.868355744567`;
  - `industry_services_cost_change` 2026-09-01 = `37.3328877005348`;
  - `industry_total_price_growth_expected` 2026-09-01 = `6.184698248960752`; `_sa` = `7.743586534122584`;
- fail-fast: отсутствующий лист, отсутствующая колонка, неоднозначная колонка.

### 5.3. Каталог

`cbr/mp_survey_series_meta_test.go`:
- 13 агрегатных + 368 отраслевых = 381 определение и 381 запись каталога;
- двусторонняя сверка, уникальность, непустые поля, `source = 'cbr_survey'`, `frequency` M/Q;
- `SA`-флаг ↔ суффикс `_sa`;
- `credit_conditions`: 1 агрегатный + 16 отраслевых, без `_sa`;
- каждое `industry_*`-имя есть и в defs, и в meta.

## 6. Документация

- `docs/IMPORTERS.md` — 20 из 31 листа, 368 отраслевых рядов, 102 384 точки, 104 481 всего, 381 запись каталога, диапазоны дат, миграция.
- `ARCHITECTURE.md` — шапка, §2, §6.1, §6.2, §6.3, §6.5, §7, §8 п. 13.
- `docs/CONFIGURATION.md` — `cbr_survey (381 ряд: 13 агрегатных + 368 отраслевых)`.
- `docs/LIMITATIONS.md` — то же.
- `README.md` — строка импортёра `cbr`: «16 отраслевых листов МП».
- `docs/IMPORTER_PACKAGE_STANDARD.md` §9.7 — миграция при расширении набора рядов.
- `docs/ROADMAP_KEY_RATE.md` — Task 6 ✅ 2026-10-19, числа, #239, ссылки на спеку/план; #238 открыт, вердикт v3 не заявлен.
- `scripts/mcp_check.py` — 381/368.

## 7. Приёмка

1. `make ch-status`.
2. Миграция §4 (если строка версии есть).
3. `make import STAT=cbr_mp_survey` → `Собрано 104481 точек`, `Imported 104481 rows of cbr_mp_survey`.
4. MCP/`kimi_reader`:
   - `v_cbr_survey`: 104 481 строка, 381 ряд, `2002-01-01 … 2026-10-01`;
   - `startsWith(series,'industry_')`: 102 384;
   - `v_series_catalog` `source='cbr_survey'`: 381, из них `industry_*` — 368.
   - spot-сверка `industry_total_price_expectations_sa` 2026-09 = `21.5845138071129`, `industry_mining_ibk` 2026-09 = `-3.34877342731565`.
5. Повтор `make import STAT=cbr_mp_survey` → `no-op, версия … уже импортирована, 104481 точек на месте`.
6. `make mcp-user` → `make mcp-check` → ПРОВЕРКА ПРОЙДЕНА; сырая `macro_series` — `ACCESS_DENIED` 497.
7. `dagu validate dagu/cbr.yaml` (если dagu установлен).
8. `make all`, `make docs-check`.
9. Архив — при настроенном `ARCHIVE_BACKEND`; иначе «не проверено».
10. Протокол — комментарием в PR; `Closes #239` вне backticks.

## 8. Риски

| Риск | Решение |
|---|---|
| Префлайт no-op из-за старого `points=2084` | Миграция §4 до первого прогона; правило в §9.7/§8 п. 13 |
| Размер фикстуры 2,2 МБ | Реальная книга допустима (§11.1–11.2); при требовании ревью — zip-тримминг до 16 листов |
| Короткие ряды N/Z (81 точка) | Свойство источника; в каталоге и тестах зафиксировано |
| Новые листы/колонки в источнике | `findColumn` fail-fast с именем ряда; новый ряд — отдельный пункт |
| Смена структуры книги | Пороги `minDatedRows`/`minQuarterLabels`, ошибки с именем листа |

## 9. Файлы

| Файл | Действие |
|---|---|
| `cbr/mp_survey_industry_meta.go` | новый |
| `cbr/mp_survey_parse.go` | правка `isMonthlySurveySheet` + комментарий |
| `cbr/mp_survey.go` | `mpSurveyAllSeries`/`mpSurveyAllMeta`, лог, комментарии |
| `cbr/views.go` | комментарии `cbrSurveyView` |
| `cbr/mp_survey_industry_test.go` | новый |
| `cbr/mp_survey_series_meta_test.go` | 381/368 |
| `cbr/testdata/mp_survey_industry.xlsx` | новая реальная фикстура |
| `scripts/mcp_check.py` | 381/368 |
| `docs/superpowers/specs/2026-10-19-cbr-mp-industry-design.md` | этот файл |
| `docs/superpowers/plans/2026-10-19-cbr-mp-industry.md` | план |
| `docs/IMPORTERS.md`, `ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/LIMITATIONS.md`, `README.md`, `docs/IMPORTER_PACKAGE_STANDARD.md`, `docs/ROADMAP_KEY_RATE.md` | документация |
