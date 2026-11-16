# План: дата оперативной справки инФОМ в `series_publication` (#624)

Спека — [2026-11-16-series-publication-infl-exp-spravka-design.md](../specs/2026-11-16-series-publication-infl-exp-spravka-design.md).
Ветка `feat/key-rate-series-publication-spravka`, worktree `.worktrees/series-publication-spravka`.

## Шаг 1. Источник дат

`cbr/publication_calendar.go`:

- `inflExpSpravkaTitlePrefix` — префикс названия строки календаря.
- `spravkaRow{Period string; Date time.Time; SourceURL string}` и чистая функция
  `spravkaDates(rows, today) map[publicationPeriod]archivePublication`: разбор `YYYY-MM`,
  отсев даты в будущем (MSK), выбор максимальной даты месяца.
- `loadInflExpSpravkaDates(ctx, conn, today)` — `SELECT period, publication_date, source_url
  FROM cbr_publication_calendar FINAL WHERE topic='analytics' AND startsWith(title, ?)`,
  затем `spravkaDates`.
- `writeSeriesPublications`: справка читается до `loadPublicationArchives`; месяцы справки
  исключаются из `needed`; в лог — число дат из справки.
- `buildSeriesPublications(points, archives, spravka)`: для `Archive == cbrInflExpTable`
  справка (если `Date >= point.Date`) → архив → fallback.

## Шаг 2. Тесты

`cbr/publication_dates_parse_test.go`:

- `TestSpravkaDates` — фильтр будущего, максимум из дублей, не-`YYYY-MM` и пустой URL.
- `TestBuildSeriesPublicationsPrefersSpravka` — справка бьёт tooltip; дата раньше волны
  игнорируется; без справки — tooltip; без обоих — fallback; `cbr_indicators_cpd`/`cbr_survey`
  не задеты.

## Шаг 3. Витрина и документы

- `cbr/views.go`: комментарии `v_series_publication` и `v_cbr_publication_calendar`.
- `docs/IMPORTERS.md` (раздел `cbr_publication_calendar`), `ARCHITECTURE.md` §6.1 (строка
  импортёра) и §6.3 (`v_series_publication`) — правило приоритета, покрытие 2025+, остаток.
- `scripts/mcp_check.py`: `cbr_infl_exp 2026-09-14 → 2026-09-22`, новая проверка
  `2026-07-14 → 2026-07-21`.
- `docs/ROADMAP_KEY_RATE.md` §4.2 — строка пункта; статус меняется тем же PR.

## Шаг 4. Проверка

1. `go test ./cbr/... -run 'Spravka|Publication'`.
2. `make all`, `make docs-check`.
3. `make import STAT=cbr_publication_calendar` — 42 строки; повтор — 0.
4. SQL-сверка на dev (три контрольные точки, все месяцы 2025+, неизменность 2023–2024,
   распределение `quality`).
5. `make mcp-check`.
6. Протокол — комментарием в PR, `Closes #624`.
