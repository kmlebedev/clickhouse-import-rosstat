# План: закрытие Deferred minors ревью Фазы 1 контура золота

**Спека:** [2026-10-11-deferred-minors-phase1-review-design.md](../specs/2026-10-11-deferred-minors-phase1-review-design.md)
**Ветка:** `fix/deferred-minors-phase1-review` от `e193aa7`. Один PR в конце.

## Шаг 1 — Спека, план, готовая правка AGENTS.md (п.3)

- [x] Спека и план — первые коммиты ветки.
- [ ] `AGENTS.md` (правка п.5 уже в рабочем дереве: строка 29 называет оба роадмапа) — в коммит.

## Шаг 2 — Код (п.4, п.5) и тесты (п.5, п.7)

- [ ] `ingest/clickhouse.go:24` — комментарий `chStore`: «нужен InsertModelRun» → формулировка про методы записи writer'а (`InsertModelRun`, `InsertManualSeries`, `InsertGoldPrices`).
- [ ] `ingest/model_run.go`:
  - константы `minDate32 = "1900-01-01"`, `maxDate32 = "2299-12-31"`;
  - в `ValidateManualSeriesPoint` и `ValidateGoldPricePoint` после разбора даты: проверка `date` вне `[minDate32, maxDate32]` → `date %q out of Date32 range [1900-01-01, 2299-12-31]`;
  - обновить doc-комментарии валидаторов.
- [ ] `ingest/model_run_test.go` — табличные случаи «1899-12-31» и «2300-01-01» → ошибка для обеих валидаторов (рядом с `TestValidateManualSeriesPointErrors`).
- [ ] `ingest/server_test.go` — `TestManualSeriesExplicitEmptySourceDefaults`: тело с явным `"source": ""` → `200`, фейк-writer получил точку с `source == "manual"` (по образцу `TestManualSeriesSourceDefault`).
- Проверка: `go test ./ingest/... -race`.

## Шаг 3 — Документация (п.1, п.2, п.6)

- [ ] `ingest/server.go:25` — doc-комментарий `Server`: добавить `gold_prices`.
- [ ] `ARCHITECTURE.md:595` — «Единственный путь записи в `model_runs`, `forecast_log`, `macro_series` и `gold_prices`…».
- [ ] `ARCHITECTURE.md:273` — комментарий DDL `macro_series`: `'fred','bls','bea','manual','webbridge','imf'`.
- [ ] `ARCHITECTURE.md:269` — примечание: актуальные значения source — в комментарии DDL; `eia`/`wgc`/`cme` — планируемые источники, в DDL не входят.
- [ ] `docs/GOLD_THESIS_DATA_ROADMAP.md` runbook Фазы 1: шаг 4 — добавить `make import STAT=gold_prices` (витрина создаётся импортёром, `EnsureTables` её не делает); шаг 5 — пояснение про чистую БД.
- [ ] Skill `sync-readme-architecture`: сверить README (список source DDL README не дублирует — ожидается правка только ARCHITECTURE.md).

## Шаг 4 — Проверка

- [ ] `make all` целиком.
- [ ] `git status`/`git diff` — только заявленные файлы.

## Шаг 5 — PR

- [ ] Один PR: описание — семь пунктов, что проверено (`make all`, `go test ./ingest/... -race`).
