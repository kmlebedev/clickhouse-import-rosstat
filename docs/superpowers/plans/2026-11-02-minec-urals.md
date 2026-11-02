# План: Urals Минэка — месячный ряд (issue #509)

**Спека:** [2026-11-02-minec-urals-design.md](../specs/2026-11-02-minec-urals-design.md)
**Пункт:** `key-rate/phase-2-urals` ([#114](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/114), источник найден) → реализация [#509](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/509)
**Ветка:** `feat/minec-urals`, worktree `.worktrees/minec-urals`

## Задачи

### T1. Скелет пакета и реестр

- `minec/urals.go` — `minecUrals{util.ClickHouseImport}`: `TableName = "macro_series"`, копия канонического DDL `macro_series`, `DataUrl` = листинг, `CrawFunc`, `AfterImport`; `func (s *minecUrals) Name() string { return "minec_urals_price" }`.
- `minec/importers.go` — `minecImporter` + единственный `init()`; литерал таблицы `"macro_series"`, `countPoints: ""`.
- `main.go` — blank-import `minec`.

Проверка: `go build ./...`; `make import STAT=__none__` показывает список без `minec_urals_price` → после регистрации импортёр находится.

### T2. Разбор (`minec/urals_parse.go`)

- `minecLink{href, month, title}`; `minecParseListing` — маркер `<title>` «Конъюнктура мировых товарных рынков», анкоры `a[href*="…konyunktura…/"][title]`, «Юралс» по `title`, месяц из `за <месяц> <год>`, карта месяцев, `ё`→`е`.
- `minecParseArticle` — `header.e-title.e-title_main`, `section.e-material__content`, regex цены, ровно одно совпадение, `price > 0`, месяц из заголовка.
- Ошибки с URL/заголовком; без `panic`.

### T3. Фикстуры

Скачать живьём и положить в `minec/testdata/`:
- `materials_p1.html` — листинг `?page=1`, урезать до 4 карточек (2 Urals + 2 прочих), разметку не менять;
- `yurals_2018_01.html` — 68,60 («Средняя цена на нефть…» + «Уровень цены»);
- `yurals_2024_01.html` — 64,14 («Уровень цены»);
- `yurals_2026_10.html` — 101,46 («Средняя цена»);
- `onm_2026_06.html` — «новое морское месторождение» (нет «за баррель»).

### T4. Обход и запись (`minec/urals.go`)

- `minecStartMonth(ctx, conn)`: `max(date)` по `source='minec'`; env `MINEC_URALS_FROM` (YYYY-MM) переопределяет; пустая таблица → нулевая дата.
- `minecCollect(ctx, listingURL, start)`: страницы `?page=N` (cap 60, пауза 300 мс перед каждым запросом, `util.GetFileBytesChecked`), стоп по пустой странице (`page > 1`) и по минимальному месяцу < `start`; пропуск старых ссылок; дубль месяца — предупреждение, берётся первая ссылка; GET статьи → `minecParseArticle` → сверка месяца с листингом.
- `minecInsert(ctx, conn, points)`: один `PrepareBatch`, `Append` с проверкой, `Abort` на ошибке, `Send`.
- `minecUralsCraw` собирает шаги, логирует `Imported %d rows of %s`; пустая дельта — `log.Infof` и `0`, без ошибки.

### T5. Производные

- `minec/series_meta.go` — одна запись каталога (source `minec`, series `urals_avg_price`, unit, frequency M, origin, description с пропуском 2018-06/07).
- `minec/views.go` — `v_minec_macro` с комментариями; `afterMinecMacro` = `UpsertSeriesCatalog` + `CreateView`.
- `sql/mcp_kimi_reader.sql` — грант.
- `scripts/mcp_check.py` — блок проверок после ФАО: витрина `count()=104` и `max(date) >= 2026-10-01`, spot `2026-10 = 101.46`, каталог `source='minec'` = 1.

### T6. Тесты

- `urals_parse_test.go`: листинг (4 карточки, 2 Urals, месяцы), статья вместо листинга — ошибка, три эры статьи (spot 68.60 / 64.14 / 101.46), негатив offshore, контракт каталога.
- `urals_test.go`: `httptest` для `minecCollect` (пагинация, стоп, гейт, счётчик запросов, дубль месяца — предупреждение, берётся первая ссылка), `appendRecorder` для `minecInsert`, `minecStartMonth` на фейковом conn (env/месяц+1).
- `importers_test.go`: энфорсер §8.3 (префикс, литерал таблицы `macro_series`, `Name() != table`, уникальность, регистрация, dagu-регулярка в обе стороны, отсутствие publication-объявления).

### T7. Расписание и документация

- `dagu/minec.yaml` — ежедневно 20:43, один шаг, retry 3 × 300.
- `ARCHITECTURE.md` §2/§6.1/§6.3/§7; `README.md` (таблица); `docs/IMPORTERS.md` (строка + подробная спецификация); `docs/ROADMAP_KEY_RATE.md` §4.2 №13 и абзац фазы 2 (источник найден, issue #509); `docs/LIMITATIONS.md` (пропуск 2018-06/07, ревизии, PIT).

### T8. Приёмка

- `make all`, `go test -race -count=1 ./minec/`, `dagu validate dagu/minec.yaml`, `make docs-check`.
- Живой прогон, повтор no-op, ремонт через `MINEC_URALS_FROM`, `make mcp-check` — протокол ниже.
- Коммит статуса `✅` в `docs/ROADMAP_KEY_RATE.md`, PR (`Closes #509`, `Closes #114`), приёмочный комментарий в PR.

## Протокол живой приёмки (2026-11-02)

- `make all` — зелёный: gofmt, golangci-lint «0 issues», `go vet ./...`, `go test -race ./...` (`minec` 2,7 с), обе сборки.
- `dagu validate dagu/minec.yaml` — exit 0; `make docs-check` — 87 страниц, 33 теста OK.
- Первый прогон: `make import STAT=minec_urals_price` → **`Imported 104 rows`** (75 с: 22 страницы листинга + 104 статьи).
  `SELECT count(), min(date), max(date) FROM v_minec_macro` → **104 / 2018-01-01 / 2026-10-01**, рядов 1.
  Spot-значения совпали с публикацией: 2018-01 = 68,60; 2018-05 = 75,14; 2020-04 = 16,26; 2026-09 = 92,08; 2026-10 = 101,46.
- Повтор: `Imported 0 rows` за 4,2 с (включая сборку) — один GET листинга, статьи не запрашивались, `count()` не изменился.
- Ремонт: `MINEC_URALS_FROM=2026-10 make import STAT=minec_urals_price` → `Imported 1 rows`, `count()` остался 104 (`ReplacingMergeTree`).
- `make mcp-check`: три новые проверки зелёные — витрина `v_minec_macro` 104 точки 2018-01 … 2026-10, spot 2026-10 = 101,46, каталог `source='minec'` = 1 ряд. Четыре падения пре-существующие и вне пункта (те же на `main` без этих изменений): `distillate_stocks` stale, bootstrap-разметка news-каналов, каталог fedstat 1734 vs 1727, `v_fedstat_ipc` 204101/1112 vs 204038/1105.

### Итог

Пункт `key-rate/phase-2-urals` закрыт: ряд `urals_avg_price` (`macro_series`, `source='minec'`) — 104 месяца 2018-01 … 2026-10, витрина `v_minec_macro`, инкремент по `max(date)` (повтор — один GET листинга и 0 строк), ремонт `MINEC_URALS_FROM`. Разрыв 2018-06/07 — в источнике, зафиксирован в `LIMITATIONS.md`.
