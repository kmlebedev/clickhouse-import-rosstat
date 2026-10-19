# План: импорт 16 отраслевых листов `mp_survey_data.xlsx` (Task 6, #239)

**Цель:** `cbr_mp_survey` читает 16 отраслевых листов публичной книги и пишет 368 рядов `industry_*` / 102 384 точки в `macro_series` (`source='cbr_survey'`); каталог — 381 ряд; витрина `v_cbr_survey`; MCP-проверки обновлены; повтор — no-op.

**Спека:** [2026-10-19-cbr-mp-industry-design.md](../specs/2026-10-19-cbr-mp-industry-design.md).

**Инварианты:**
- 87 кодов ОКВЭД2 не импортируются.
- 13 агрегатных рядов не меняются.
- Нет новых таблиц/DDL/импортёров/шагов dagu/грантов.
- `Name() = cbr_mp_survey`, `TableName = macro_series`, `PublicationSource = cbr_mp_survey`.

---

## Task 0. Ветка, issue, спека/план

- [x] worktree `.worktrees/cbr-mp-industry`, ветка `feat/cbr-mp-industry` от `main`.
- [x] комментарий в #239, Project V2: `Status=In Progress`, `Branch`, `Spec / Plan`, `Depends on=#238`.
- [x] спека и план — первый коммит ветки.

**Проверка:** `git log --oneline -1` — docs-коммит; `git status` чистый.

---

## Task 1. Определения и каталог

**Файл:** `cbr/mp_survey_industry_meta.go` (новый).

1. `industrySheet{Sheet, Slug, Title}` — 16 листов (см. спеку §2.1).
2. `industryIndicator{Slug, Header, Exclude, SA, Unit, Title, Description}` — 12 показателей (спека §2.2).
3. `buildIndustrySeries(sheets, indicators) ([]seriesDef, []util.SeriesMeta)`:
   - цикл «лист → показатель»;
   - raw-определение; если `SA` — SA-определение с суффиксом `_sa`;
   - `Name = "industry_" + sheet.Slug + "_" + ind.Slug`;
   - `Sheet` — точное имя листа, `Header` — подстрока, `Exclude` — из показателя;
   - `SeriesMeta`: `Source=mpSurveySource`, `Frequency="M"`, `Unit`, `Title`, `Origin`, `Description`.
4. `var mpSurveyIndustrySeries, mpSurveyIndustrySeriesMeta = buildIndustrySeries(...)`.
5. `var mpSurveyAllSeries = slices.Concat(mpSurveySeries, mpSurveyIndustrySeries)`.
6. `var mpSurveyAllMeta = slices.Concat(mpSurveySeriesMeta, mpSurveyIndustrySeriesMeta)`.
7. `func isMonthlySurveySheet(sheet string) bool`.

**Проверка:** `go test ./cbr -run TestSeriesMetaCompleteness` после Task 4; `len(mpSurveyIndustrySeries) == 368`.

---

## Task 2. Парсер и импортёр

**Файл:** `cbr/mp_survey_parse.go`.

- В `readSheet` заменить `if sheet == monthlySheet` на `if isMonthlySurveySheet(sheet)`.
- Обновить комментарий про месячные/квартальные листы.
- `findColumn`, `extractPoints`, пороги не менять.

**Файл:** `cbr/mp_survey.go`.

- `ImportFunc`: `parseSurvey(xlsx, mpSurveyAllSeries)`.
- Лог: `Собрано %d точек мониторинга предприятий (13 агрегатных + 16 отраслевых листов)`.
- `AfterImport`: `util.UpsertSeriesCatalog(ctx, conn, mpSurveyAllMeta)`.
- Комментарии: 20 листов из 31, 381 ряд.
- `mpSurveyCountPoints` не менять.

**Проверка:** `go test ./cbr -run 'TestParseSurvey|TestSurveyView|TestImporterName'`.

---

## Task 3. Витрина

**Файл:** `cbr/views.go`.

- `cbrSurveyView.Select` не менять.
- `Comment`: 13 агрегатных + 368 отраслевых, 16 листов, `industry_*`, SA, дата/публикация, `v_series_catalog`.
- `Columns["series"]`: 13 + 368 (`industry_*`).
- При желании — тест, что комментарий упоминает `industry_`.

**Проверка:** `go test ./cbr -run TestSurveyViewDefinition`.

---

## Task 4. Тесты и фикстура

1. Добавить `cbr/testdata/mp_survey_industry.xlsx` — полная реальная книга (2 193 461 байт, sha256 `c5ad8996…945b`), не синтетика.
2. Новый `cbr/mp_survey_industry_test.go`:
   - `TestParseSurveyIndustryFixture`: 102 384 точки, 368 рядов, даты/количества, 16 `credit_conditions` без `_sa`, spot-значения (спека §5.2).
   - `TestParseSurveyIndustryFailFast`: нет листа, нет колонки, неоднозначная колонка.
   - использовать `bySeries`, `firstLast`, `valueAt`, `utcDate` из `mp_survey_parse_test.go`; локальный `openIndustryFixture`.
3. `cbr/mp_survey_series_meta_test.go`: 13 + 368 = 381; двусторонняя сверка, уникальность, поля, SA-флаг, `credit_conditions`, префикс `industry_`.
4. Существующие агрегатные тесты и фикстуру не трогать.

**Проверка:** `go test -race -count=1 ./cbr/`.

---

## Task 5. MCP и документация

**`scripts/mcp_check.py`:**
- каталог `cbr_survey` = 381 (было 13);
- `v_series_catalog` `startsWith(series,'industry_')` = 368;
- `v_cbr_survey` отраслевые строки непусты;
- точное число точек витрины не проверять.

**Документация (тем же PR):**
- `docs/IMPORTERS.md` — 20/31 листов, 368 рядов, 102 384 точки, 104 481 всего, 381 каталог, миграция.
- `ARCHITECTURE.md` — шапка, §2, §6.1, §6.2, §6.3, §6.5, §7, §8 п. 13.
- `docs/CONFIGURATION.md`, `docs/LIMITATIONS.md` — 381/368.
- `README.md` — строка `cbr`: «16 отраслевых листов МП».
- `docs/IMPORTER_PACKAGE_STANDARD.md` §9.7 — миграция при расширении набора рядов.
- `docs/ROADMAP_KEY_RATE.md` — Task 6 ✅ 2026-10-19, числа, #239, спека/план; #238 открыт, вердикт v3 не заявлен.

**Проверка:** `make docs-check`.

---

## Task 6. Живая приёмка — протокол 2026-10-19

1. `make ch-status` — ClickHouse отвечает на 8123.
2. `source_publications` для `cbr_mp_survey` до работы: `fingerprint=a3ac58d0…`, `points=2084` (2026-10-14). Выполнена миграция:
   ```sql
   SET mutations_sync = 1;
   ALTER TABLE source_publications DELETE WHERE source = 'cbr_mp_survey';
   ```
3. `make import STAT=cbr_mp_survey` — `Get xlsx file`, `Собрано 104481 точек`, `Imported 104481 rows of cbr_mp_survey`; новый `source_publications`: `fingerprint=be103476…`, `points=104481`, `2026-10-19 16:43:28`.
4. MCP/`kimi_reader`:
   - `v_cbr_survey`: 104 481 строка, 381 ряд, `2002-01-01 … 2026-10-01`;
   - `industry_*`: 102 384 точки; агрегатные — 2 097 (+13 точек волны 2026-09/3к26 к снимку 2026-10-13);
   - `v_series_catalog`: 381, из них 368 `industry_*`;
   - spot: `industry_total_price_expectations_sa` 2026-09 = 21.5845138071129; `industry_mining_ibk` 2026-09 = -3.34877342731565; `industry_services_cost_change` 2026-09 = 37.3328877005348.
5. Повтор `make import STAT=cbr_mp_survey` — `no-op, версия be103476… уже импортирована, 104481 точек на месте`, `Imported 104481 rows`.
6. `make mcp-user` → `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; в т.ч. `витрина v_cbr_survey … 104481`, `каталог … 381`, `368 отраслевых`, `витрина … отраслевые ряды: 102384`.
7. `dagu validate dagu/cbr.yaml` — exit 0.
8. `make all` — exit 0 (`0 issues`, `go vet`, `go test -race ./...`, обе сборки); `make docs-check` — 36 страниц, 33 теста OK.
9. Архив — не проверено: в логе импорта архивации не было (`ARCHIVE_BACKEND` в сессии не задан).
10. Протокол — в этот план и комментарий PR; `Closes #239` вне backticks.

---

## Task 7. PR и статус

- Один PR: `[key-rate/phase-4-ml-components] Task 6: industry MP survey import`.
- В описании: пункт, источник, схема, запуск, пример проверки, `Closes #239`.
- Project V2: `Status=In Review`, `PR`.
- После мержа: `Done`, синхронизация роадмапа по `github-tracker` §10, удалить worktree.

---

## Порядок коммитов

1. `docs: spec and plan for cbr mp industry import`
2. `cbr: parse 16 industry sheets of mp_survey_data`
3. `cbr: catalog and view for industry survey series`
4. `cbr: tests and fixture for industry survey`
5. `docs: sync architecture, importers and roadmap for mp industry`
6. `docs: record cbr mp industry live acceptance` (после приёмки)

## Проверки перед PR

- [x] `go test -race -count=1 ./cbr/`
- [x] `make all`
- [x] `make docs-check`
- [x] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА
- [x] `dagu validate dagu/cbr.yaml` — exit 0
- [x] живая приёмка: 104 481 / 381 / 2026-10-01; повтор no-op
- [x] нет 87 кодов ОКВЭД2; все новые имена `industry_*`
- [x] `source_publications` миграция выполнена и записана в протокол
- [ ] `Closes #239` вне backticks (при открытии PR)
