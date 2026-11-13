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
- [ ] Прогнать `go test ./cbr/ -run 'MtForecast'`: падение ровно на
      `Табл 1!C10 … значение "14,6 1"` — подтверждение, что фикстура воспроизводит staging-сбой.

## Task 3. Разбор: форма сноски через пробел

Файл `cbr/mt_forecast_parse.go`.

- [ ] `mtForecastSpacedFootnoteRe` рядом с `mtForecastFootnoteRe`.
- [ ] `mtForecastBounds` — обёртка: нормализация → снятие формы (только если остаток разбирается)
      → `mtForecastValueBounds`; помощник несёт прежнюю логику точка/диапазон и текст ошибки.
- [ ] Комментарии: шапка файла, `mtForecastFootnoteRe`, `mtForecastBounds` — вторая форма сноски,
      причина (rich-text run `vertAlign=superscript`, склейка run'ов `GetRows`).
- [ ] `TestMtForecastValueForms`: `"14,6 1"`, `"12,5-13,5 1"`, `"14,6\u00a01"`.
- [ ] `TestMtForecastValueErrors`: `"5,0 4,0"` остаётся ошибкой.

## Task 4. Документы и трекер

- [ ] Спека [2026-10-17-cbr-mt-forecast-design.md](../specs/2026-10-17-cbr-mt-forecast-design.md):
      §3.3 — вторая форма сноски; §3.9 — третья фикстура; §4 — файл фикстуры.
- [ ] [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 — пункт со статусом и ссылками.
- [ ] Issue #567 — статус, `Closes #567` в PR (вне backticks), протокол приёмки — комментарием в PR.

## Task 5. Проверка

- [ ] `make all` — gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки.
- [ ] `make docs-check`.
- [ ] Живой прогон `make import STAT=cbr_mt_forecast` (dev-ClickHouse поднимает владелец):
      `Imported 64 rows` за выпуск 2026-11-05, повтор — no-op, spot `key_rate_avg` 2026 = 14.6/14.6.
      Если ClickHouse не поднят — «не проверено», прогон оставить владельцу.

## Task 6. PR

- [ ] Один PR: пункт, `Closes #567`, источник, улика, запуск, протокол приёмки комментарием.
- [ ] Staging после мержа и деплоя — действие владельца.
