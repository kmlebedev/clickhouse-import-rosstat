# План: завершение #575 — живое приложение, гейт, синхронизация доков

**Дата:** 14 ноября 2026 г.
**Спека:** [2026-11-14-infl-exp-history-completion-design.md](../specs/2026-11-14-infl-exp-history-completion-design.md).
**Пункт:** `key-rate/phase-2-infl-exp-history` (issue [#575](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/575)).
**PR:** [#587](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/587).
**Ветка:** `feat/infl-exp-history-completion`, worktree `.worktrees/infl-exp-history`, база `9601046`.

---

## Task 1. Спека и план — первым коммитом

- [x] Спека §1–§6.
- [x] План Task 1–6.
- Коммит: `docs: spec and plan for infl_exp history completion`.

## Task 2. Живая фикстура и тест разбора (до правок доков)

- [x] Скачать `stat_Infl_exp_24-09.xlsx` браузерным UA байт-в-байт в
      `cbr/testdata/infl_exp_hist_annex_24-09.xlsx`; `sha256`
      `9ad7e75aabc10ae6e41d48cf58dce1a54d02d9a46bd6c774a061497d11d29d2c`, 1 364 728 Б.
- [x] `TestCbrInflExpHistAnnexReal2409`: 89 точек на ряд в `2017-01-14 … 2024-09-14`,
      одинаковые даты, пропуски ровно 2020-04…07, контрольные значения 2017-01-14,
      2020-03-14 и 2020-08-14 через `inflExpHistAnnexCheck`.
- Коммит: `test cbr_infl_exp_hist on real production annex 24-09`.

## Task 3. Регрессия гейта

- [x] `TestCbrInflExpKnownDatesPairAndTimezone`: фейковые `driver.Conn`/`driver.Rows`
      (модель `dipFakeConn`/`dipFakeRows`), дата — полночь в `time.FixedZone("UTC+5", 5*3600)`,
      ключ — тот же календарный день; месяц одного ряда даёт ключ только этого ряда.
- Коммит: `cover cbr_infl_exp_hist known-dates gate: pair key and timezone`.

## Task 4. Прозаическая фикстура 2026

- [x] `TestCbrInflExpHistRowsProse2026`: `inflExpHistRows` — 0 строк и без ошибки.
- Коммит: `pin prose-era boundary in cbr_infl_exp_hist rows test`.

## Task 5. Документы

- [x] `dagu/cbr.yaml` — только комментарий над шагом `cbr_infl_exp_hist`.
- [x] `docs/IMPORTERS.md` — клауза «история инфляционных ожиданий» в строке `cbr`.
- [x] `docs/ROADMAP_KEY_RATE.md:81` — покрытие `2017-01-14 … 2026-10-14 (228)`.
- [x] `docs/ROADMAP_KEY_RATE.md:72` — 21 месяц перекрытия приложения с книгой + 23 PDF;
      кумулятивность 114 точек.
- [x] `docs/research/2026-11-14-infl-exp-history-source.md` — §8, история не переписана.
- [x] `docs/LIMITATIONS.md:95` — дословная сноска ЦБ, более поздние бюллетени и оси графиков.
- Коммит: `sync infl_exp history docs with the accepted 2020-04..07 source break`.

## Task 6. Проверка

- [ ] `go test -race -count=1 ./cbr/ -run 'CbrInflExp'` — зелёный.
- [ ] `make all` — gofmt, golangci-lint, `go vet`, `go test -race`, сборка.
- [ ] `make docs-check`.
- [ ] `dagu validate dagu/cbr.yaml`.
- [ ] `make import STAT=cbr_infl_exp_hist` **не запускается**: живую приёмку делает
      координатор после ревью (на живой прогон уже опирается приёмка T3).
