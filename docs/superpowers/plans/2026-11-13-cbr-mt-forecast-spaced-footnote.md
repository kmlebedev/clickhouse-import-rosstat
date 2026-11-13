# План: `cbr_mt_forecast` — сноска через пробел в значении «Табл 1»

**Дата:** 13 ноября 2026 г.
**Спека:** [2026-11-13-cbr-mt-forecast-spaced-footnote-design.md](../specs/2026-11-13-cbr-mt-forecast-spaced-footnote-design.md).
**Пункт:** `key-rate/phase-2-cbr-mt-forecast-spaced-footnote` (issue #567).
**Ветка:** `fix/cbr-mt-forecast-spaced-footnote`, worktree `.worktrees/cbr-mt-forecast-footnote`.

---

## Task 1. Спека и план — первым коммитом

- [x] Спека §1–§7.
- [x] План Task 1–6.
- Коммит: `docs: spec and plan for cbr_mt_forecast spaced footnote`.

## Task 2. Фикстура и падающий тест (до кода)

- [x] Скачать `commet_graph_05112026.xlsx` (sha256 `5a197a12…`), урезать:
      `python3 scripts/xlsx_trim.py trim … cbr/testdata/mt_forecast_05112026.xlsx --keep "Табл 1"`,
      `verify` — PASS (инвариант §11.2).
- [x] `mtForecastFixture05112026`; запись в `mtForecastVintages` (64 точки, годы 2026–2029,
      споты 2026 из живого файла); запись в `mtForecastFixtureReleases`
      (2026-10-23 / 2026-11-05 / `…/comment_05112026/`); комментарии «два выпуска» → «три».
- [x] Прогнать `go test ./cbr/ -run 'MtForecast'`: падение ровно на
      `Табл 1!C10 … значение "14,6 1"` — подтверждение, что фикстура воспроизводит staging-сбой.

## Task 3. Разбор: форма сноски через пробел

Файл `cbr/mt_forecast_parse.go`.

- [x] `mtForecastSpacedFootnoteRe` рядом с `mtForecastFootnoteRe`.
- [x] `mtForecastBounds` — обёртка: нормализация → снятие формы (только если остаток разбирается)
      → `mtForecastValueBounds`; помощник несёт прежнюю логику точка/диапазон и текст ошибки.
- [x] Комментарии: шапка файла, `mtForecastFootnoteRe`, `mtForecastBounds` — вторая форма сноски,
      причина (rich-text run `vertAlign=superscript`, склейка run'ов `GetRows`).
- [x] `TestMtForecastValueForms`: `"14,6 1"`, `"12,5-13,5 1"`, `"14,6\u00a01"`.
- [x] `TestMtForecastValueErrors`: `"5,0 4,0"` остаётся ошибкой.

## Task 4. Документы и трекер

- [x] Спека [2026-10-17-cbr-mt-forecast-design.md](../specs/2026-10-17-cbr-mt-forecast-design.md):
      §3.3 — вторая форма сноски; §3.9 — третья фикстура; §4 — файл фикстуры.
- [x] [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 — пункт со статусом и ссылками.
- [ ] Issue #567 — статус, `Closes #567` в PR (вне backticks), протокол приёмки — комментарием в PR.

## Task 5. Проверка

- [x] `make all` — gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки (exit 0).
- [x] `make docs-check` — 48 страниц, 33 теста OK.
- [x] Живой прогон (dev-ClickHouse): `make import STAT=cbr_mt_forecast` — выпуск 2026-11-05,
      `импортировано 64 точек`, `Imported 64 rows`; повтор — no-op («новых выпусков нет
      (комментариев 12: страниц 3, PDF 9; прогнозов 12; в таблице 24 выпусков)», `Imported 0 rows`);
      spot в `v_cbr_mt_forecast`: `key_rate_avg` 2026 `release_date = 2026-11-05` → 14.6/14.6,
      точек у выпуска 64, показателей 16.

## Task 6. PR

- [ ] Один PR: пункт, `Closes #567`, источник, улика, запуск, протокол приёмки комментарием.
- [ ] Staging после мержа и деплоя — действие владельца.
