# План: даты публикации ИПЦ Росстата для `v_ofz_signal` (#626)

Спека — [2026-11-16-rosstat-ipc-publication-dates-design.md](../specs/2026-11-16-rosstat-ipc-publication-dates-design.md).
Ветка `feat/key-rate-rosstat-ipc-publication-dates`, worktree `.worktrees/rosstat-ipc-publication-dates`.

## Шаг 1. Импортёр `rosstat_ipc_pubdates`

`rosstat/ipc_pubdates.go`:

- DDL `rosstat_ipc_pubdates` (ключ `period_date`, версия `loaded_at`).
- `ipcPubdatesFeedURL(page)` — `central-news?per_page=20&search=Об индексе потребительских
  цен в&page=N`.
- `parseIpcPubdates(page)` — карточки `.news-card`: титул, `.news-card__data`, ссылка;
  строгий регекс титула (месяц/год), дата карточки = дата в имени файла `<id>_DD-MM-YYYY`;
  расхождение — ошибка.
- `crawlIpcPubdates` — страницы до первой пустой (cap 20), дельта по `period_date`.
- `afterIpcPubdates` — `util.EnsureSeriesPublication` + дельта-запись `series_publication`
  (`source='rosstat'`, ряд ИПЦ, `quality='date'`, `published_at = 23:59:59 MSK`).
- Регистрация в `rosstat/importers.go` (15-я запись, без префлайта); шаг в хвосте
  `dagu/rosstat.yaml`.

## Шаг 2. Тесты

`rosstat/ipc_pubdates_test.go`: фикстура ленты (2–3 карточки, включая недельную — она
отсекается), разбор даты/титула, month-end, сверка с именем файла (ok/mismatch), пустая
страница, дельта (повтор 0). `cbr/zcyc_test.go` (или новый файл): контракт SQL витрины —
`Tables` содержит `series_publication`, `Select` содержит `available_at` и оба источника.

## Шаг 3. Витрина

`cbr/views.go` — `cbrZcycSignalView`:

- `Tables` += `util.SeriesPublicationTable`.
- ASOF-ключ ИПЦ — `available_at` (coalesce: Росстат → ЦБ → конец M+1); окно 12 месяцев
  считается по отчётным месяцам до выбора даты.
- Комментарии таблицы/колонки `inflation_yoy`.
- `cbrPublicationCalendarImporter.AfterImport` дополнительно создаёт `cbrZcycSignalView`
  (самоисцеление порядка шагов).

## Шаг 4. Документы и проверки

- `scripts/mcp_check.py`: `inflation_yoy` на 2025-06-04 = 10,23 и на 2025-06-11 = 9,89.
- `docs/IMPORTERS.md` (новый импортёр), `ARCHITECTURE.md` §6.1/§6.2 (DDL) и §6.3
  (`v_ofz_signal`, `v_series_publication`), `docs/ROADMAP_KEY_RATE.md` §4.2.

## Шаг 5. Проверка

1. `go test ./rosstat/ -run 'Pubdates'`, `go test ./cbr/ -run 'Signal|Publication'`.
2. `make all`, `make docs-check`.
3. `make import STAT=rosstat_ipc_pubdates` — 49 строк таблицы и 49 строк
   `series_publication`; повтор — 0.
4. `make import STAT=cbr_publication_calendar` — витрина пересоздана, 0 строк данных.
5. SQL: 2025-06-04 → 10,23; 2025-06-11 → 9,89; 2025-05-19 → март; 2026-10-14 → сентябрь;
   `key_rate`/`ofz_*` не изменились.
6. `make mcp-check`.
7. Протокол — комментарием в PR, `Closes #626`.
