# План: `key-rate/phase-4-cbr-publication-calendar` Task 1 — календарь и `published_at`

**Дата:** 2026-10-20
**Пункт:** `key-rate/phase-4-cbr-publication-calendar` (issue [#299](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/299))
**Спека:** [2026-10-20-cbr-publication-calendar-go-design.md](../specs/2026-10-20-cbr-publication-calendar-go-design.md)
**Research:** [2026-10-20-cbr-publication-calendar.md](../../research/2026-10-20-cbr-publication-calendar.md)
**Ветка:** `feat/cbr-publication-calendar` от `origin/research/240-cbr-calendar`
**Worktree:** `.worktrees/cbr-publication-calendar`

## 1. Задачи

### T1. Спека, фикстуры, каркас

- [x] Спека и этот план — первыми коммитами ветки.
- [x] Скачать реальные срезы 2026-10-20 в `cbr/testdata/`:
  - `publication_calendar_2026-10-20.html`;
  - `infl_exp_archive_2026-10-20.html`;
  - `cpd_archive_2026-10-20.html`;
  - `cpd_2026-09.html`;
  - `mp_archive_2026-10-20.html`;
  - `mp_0926.html`.
- [x] Проверить размеры и что фикстуры не содержат секретов/персональных данных.

### T2. Чистые парсеры и тесты

- [x] `cbr/publication_calendar_parse.go`:
  - статистика: таблица `table.data` с заголовком «Дата публикации»; дата, время (`—` → NULL), title из `<a>`, period после `<br>`, `source_url`;
  - аналитика: `#tab_content_t2 .dropdown` → 22 блока, `period=YYYY-MM`, строки `(title, date, source_url)`;
  - `planned` от даты снимка; fail-fast на пустой обязательный разбор.
- [x] `cbr/publication_dates_parse.go`:
  - `infl_exp`: `a.versions_item[data-tooltip-content]` + regex `(Infl_exp|inFOM)_YY-MM.(xlsx|pdf)`;
  - `cpd`: `INF_YYYY-MM.pdf` tooltip + `CPD_YYYY-M/` HTML-страницы, `.news-info-line_date`;
  - `survey`: `monitoring/(MMYY|MM_YY)`, дельта-страницы, `.news-info-line_date`;
  - чистые `dateOnlyPublishedAt`, `fallbackPublishedAt`, `calendarRowChanged`, `seriesPublicationChanged`.
- [x] `cbr/publication_calendar_parse_test.go`, `cbr/publication_dates_parse_test.go`:
  - 173 статистических, 22 блока, 445 аналитических строк;
  - 2026-09 infl_exp → 2026-09-29; 2025-12 → 2025-12-24;
  - CPD: tooltip PDF, битые `CPD_2025-4/5` и `CPD_2022-6` не ломают;
  - survey 0926 → 2026-09-15, 05_22 → 2022-05;
  - fallback = конец M+1 23:59:59 MSK; date-only = 23:59:59 MSK; `exact_time` не понижается;
  - регрессия Date в зоне сессии +05:00: fallback не уезжает на предыдущий месяц.

### T3. Схема и витрины

- [x] `util/series_publication.go`: `SeriesPublicationDdl`, `EnsureSeriesPublication`, `SeriesPublication`.
- [x] `cbr/publication_calendar.go`: импортёр на `util.ClickHouseImport` с `CrawFunc`, `DataUrl`, `CreateTable`, `AfterImport`.
- [x] `CrawFunc`: DDL `series_publication`; дельта-запись календаря; сбор точек трёх источников; архивные даты; дельта-запись `series_publication`; один батч на таблицу.
- [x] `cbr/views.go`: `v_cbr_publication_calendar`, `v_series_publication` с комментариями таблицы и колонок.
- [x] `cbr/importers.go`: запись `cbr_publication_calendar`.
- [x] `dagu/cbr.yaml`: шаг в хвосте.
- [x] `cbr/importers_test.go`: `cbr_publication_calendar` в `keyScheduled`.

### T4. Гранты, MCP, документация

- [x] `sql/mcp_kimi_reader.sql`: `GRANT SELECT` на обе витрины.
- [x] `scripts/mcp_check.py`: обе витрины видны/читаются; `topic` statistics+analytics; `quality` date+fallback; точные даты research; сырые таблицы закрыты.
- [x] `docs/IMPORTERS.md`: источник, таблицы, витрины, запуск, приёмка.
- [x] `ARCHITECTURE.md`: §6.1 (импортёр), §6.2 (DDL), §6.3 (витрины).
- [x] `README.md`: список импортёров.
- [x] `docs/ROADMAP_KEY_RATE.md`: статус `🔶`, протокол приёмки.

### T5. Живая приёмка

- [x] `make import STAT=cbr_publication_calendar` — числа, 173 статистических, 22 блока, `date`/`fallback`.
- [x] Повтор — `Imported 0 rows`, `count()` не изменился.
- [x] MCP: `v_cbr_publication_calendar` (`topic`, 173+), `v_series_publication` (quality, точные даты).
- [x] `make all`.
- [x] `go test -race -count=1 ./cbr/`.
- [x] `make docs-check`.
- [x] `dagu validate dagu/cbr.yaml`.
- [x] `make mcp-user && make mcp-check`.

### T6. PR

- [ ] Ребейз на `origin/main` после мержа PR #302.
- [ ] PR в `main`: `Closes #299`, `Refs #240`, источник, схема, запуск, пример проверки.
- [ ] Протокол живой приёмки — комментарием в PR.
- [ ] Project V2: `In Review`; `Done` — после мержа и протокола владельца.

## 2. Протокол живой приёмки

Заполнен после T5; переносится в комментарий PR.

| Проверка | Результат |
|---|---|
| `make import STAT=cbr_publication_calendar` | `Imported 1892 rows` (618 календарь + 1 274 publication dates) |
| статистических строк / аналитических блоков | 173 / 22 (445 аналитических строк) |
| `series_publication`: всего / `date` / `fallback` | 1 274 / 446 / 828 |
| повтор (`Imported 0 rows`) | `Imported 0 rows`, `count()` 618 / 1 274 не изменился |
| `v_cbr_publication_calendar` count | 618 (statistics 173, analytics 445) |
| `v_series_publication` count / quality | 1 274 (date 446, fallback 828) |
| точные даты research | инфляционные ожидания 2026-09-14 → 2026-09-29; ИПЦ 2026-08-31 → 2026-09-16; мониторинг 2026-09-01 → 2026-09-15 |
| `make all` / `make docs-check` / `dagu validate` / `make mcp-check` | зелёные; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА |

## 3. Риски

- Ветка базируется на research-ветке, пока PR #302 не merged; PR в `main` — после его мержа.
- Сеть ЦБ может отдать 403/503 на серии страниц; при недоступном архиве остаётся fallback, прогон не падает.
- Живые числа календаря зависят от даты снимка; тесты — на фикстуре 2026-10-20.
