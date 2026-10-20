# План: `key-rate/phase-4-cbr-publication-calendar` Task 1 — календарь и `published_at`

**Дата:** 2026-10-20
**Пункт:** `key-rate/phase-4-cbr-publication-calendar` (issue [#299](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/299))
**Спека:** [2026-10-20-cbr-publication-calendar-go-design.md](../specs/2026-10-20-cbr-publication-calendar-go-design.md)
**Research:** [2026-10-20-cbr-publication-calendar.md](../../research/2026-10-20-cbr-publication-calendar.md)
**Ветка:** `feat/cbr-publication-calendar` от `origin/research/240-cbr-calendar`
**Worktree:** `.worktrees/cbr-publication-calendar`

## 1. Задачи

### T1. Спека, фикстуры, каркас

- [ ] Спека и этот план — первыми коммитами ветки.
- [ ] Скачать реальные срезы 2026-10-20 в `cbr/testdata/`:
  - `publication_calendar_2026-10-20.html`;
  - `infl_exp_archive_2026-10-20.html`;
  - `cpd_archive_2026-10-20.html`;
  - `cpd_2026-09.html`;
  - `mp_archive_2026-10-20.html`;
  - `mp_0926.html`.
- [ ] Проверить размеры и что фикстуры не содержат секретов/персональных данных.

### T2. Чистые парсеры и тесты

- [ ] `cbr/publication_calendar_parse.go`:
  - статистика: таблица `table.data` с заголовком «Дата публикации»; дата, время (`—` → NULL), title из `<a>`, period после `<br>`, `source_url`;
  - аналитика: `#tab_content_t2 .dropdown` → 22 блока, `period=YYYY-MM`, строки `(title, date, source_url)`;
  - `planned` от даты снимка; fail-fast на пустой обязательный разбор.
- [ ] `cbr/publication_dates_parse.go`:
  - `infl_exp`: `a.versions_item[data-tooltip-content]` + regex `(Infl_exp|inFOM)_YY-MM.(xlsx|pdf)`;
  - `cpd`: `INF_YYYY-MM.pdf` tooltip + `CPD_YYYY-M/` HTML-страницы, `.news-info-line_date`;
  - `survey`: `monitoring/(MMYY|MM_YY)`, дельта-страницы, `.news-info-line_date`;
  - чистые `dateOnlyPublishedAt`, `fallbackPublishedAt`, `calendarRowChanged`, `seriesPublicationChanged`.
- [ ] `cbr/publication_calendar_parse_test.go`, `cbr/publication_dates_parse_test.go`:
  - 173 статистических, 22 блока, 445 аналитических строк;
  - 2026-09 infl_exp → 2026-09-29; 2025-12 → 2025-12-24;
  - CPD: tooltip PDF, битые `CPD_2025-4/5` и `CPD_2022-6` не ломают;
  - survey 0926 → 2026-09-15, 05_22 → 2022-05;
  - fallback = конец M+1 23:59:59 MSK; date-only = 23:59:59 MSK; `exact_time` не понижается.

### T3. Схема и витрины

- [ ] `util/series_publication.go`: `SeriesPublicationDdl`, тип `SeriesPublication` (при необходимости).
- [ ] `cbr/publication_calendar.go`: импортёр на `util.ClickHouseImport` с `CrawFunc`, `DataUrl`, `CreateTable`, `AfterImport`.
- [ ] `CrawFunc`: DDL `series_publication`; дельта-запись календаря; сбор точек трёх источников; архивные даты; дельта-запись `series_publication`; один батч на таблицу.
- [ ] `cbr/views.go`: `v_cbr_publication_calendar`, `v_series_publication` с комментариями таблицы и колонок.
- [ ] `cbr/importers.go`: запись `cbr_publication_calendar`.
- [ ] `dagu/cbr.yaml`: шаг в хвосте.
- [ ] `cbr/importers_test.go`: `cbr_publication_calendar` в `keyScheduled`.

### T4. Гранты, MCP, документация

- [ ] `sql/mcp_kimi_reader.sql`: `GRANT SELECT` на обе витрины.
- [ ] `scripts/mcp_check.py`: обе витрины видны/читаются; `topic` statistics+analytics; `quality` date+fallback; точные даты research; сырые таблицы закрыты.
- [ ] `docs/IMPORTERS.md`: источник, таблицы, витрины, запуск, приёмка.
- [ ] `ARCHITECTURE.md`: §6.1 (импортёр), §6.2 (DDL), §6.3 (витрины).
- [ ] `README.md`: список импортёров.
- [ ] `docs/ROADMAP_KEY_RATE.md`: статус `🔶`, протокол приёмки.

### T5. Живая приёмка

- [ ] `make import STAT=cbr_publication_calendar` — числа, 173 статистических, 22 блока, `date`/`fallback`.
- [ ] Повтор — `Imported 0 rows`, `count()` не изменился.
- [ ] MCP: `v_cbr_publication_calendar` (`topic`, 173+), `v_series_publication` (quality, точные даты).
- [ ] `make all`.
- [ ] `go test -race -count=1 ./cbr/`.
- [ ] `make docs-check`.
- [ ] `dagu validate dagu/cbr.yaml`.
- [ ] `make mcp-user && make mcp-check`.

### T6. PR

- [ ] Ребейз на `origin/main` после мержа PR #302.
- [ ] PR в `main`: `Closes #299`, `Refs #240`, источник, схема, запуск, пример проверки.
- [ ] Протокол живой приёмки — комментарием в PR.
- [ ] Project V2: `In Review`; `Done` — после мержа и протокола владельца.

## 2. Протокол живой приёмки

Заполняется после T5; переносится в комментарий PR.

| Проверка | Результат |
|---|---|
| `make import STAT=cbr_publication_calendar` | _TBD_ |
| статистических строк / аналитических блоков | _TBD_ / 22 |
| `series_publication`: всего / `date` / `fallback` | _TBD_ |
| повтор (`Imported 0 rows`) | _TBD_ |
| `v_cbr_publication_calendar` count | _TBD_ |
| `v_series_publication` count / quality | _TBD_ |
| точные даты research | _TBD_ |
| `make all` / `make docs-check` / `dagu validate` / `make mcp-check` | _TBD_ |

## 3. Риски

- Ветка базируется на research-ветке, пока PR #302 не merged; PR в `main` — после его мержа.
- Сеть ЦБ может отдать 403/503 на серии страниц; при недоступном архиве остаётся fallback, прогон не падает.
- Живые числа календаря зависят от даты снимка; тесты — на фикстуре 2026-10-20.
