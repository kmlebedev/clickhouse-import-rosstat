# План: Минфин — федеральный дефицит и нефтегазовые доходы

**Пункт:** `key-rate/phase-2-minfin-scrape` ([ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №15, §4.3 №15).
**Issue:** [#116](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/116).
**Спека:** [2026-10-19-minfin-deficit-oil-gas-design.md](../specs/2026-10-19-minfin-deficit-oil-gas-design.md).
**Ветка:** `key-rate-minfin-scrape`.

## Задача 0. Старт трекера

- Project V2 (project 11, item 274754236): `Status = In Progress`, `Roadmap = key-rate`, `Phase = 2`, `Item = phase-2-minfin-scrape`, `Branch = key-rate-minfin-scrape`.
- Issue #116: `Acceptance`, `Spec`, `Plan` в шапке тела.
- Ветка от `main`.

## Задача 1. Спека и план

- [x] Спека и план закоммичены первыми коммитами ветки.
- Коммит: `docs: spec and plan for minfin deficit and oil-gas importer`.

## Задача 2. Поведение трекера

- [ ] `AGENTS.md`, «Разработка строго по дорожной карте», п. 5 — при взятии пункта сразу `Status = In Progress` в Project V2.
- [ ] `AGENTS.md`, правило 12 — та же норма (`In Progress` → `In Review` при PR → `Done` после мержа и протокола).
- [ ] `.kimi-code/skills/github-tracker/SKILL.md` §3 — операционная строка.
- Коммит: `docs: set roadmap item In Progress when work starts`.

## Задача 3. Федеральный дефицит

- [ ] `minfin/fedbud_mes.go`: `"Дефицит (-)/Профицит (+)"` в `fedbudMesFields`; конфиг вынесен в `var fedbudMes`, персональный `init()` удалён.
- [ ] `minfin/series_meta.go`: описание ряда дефицита (source `minfin`, млрд руб., M, месячные разности YTD).
- [ ] `minfin/series_meta_test.go` зелёный без изменений (контракт каталога автоматически покрывает новый ряд).

## Задача 4. Импортёр `minfin_oil_gas_mes`

- [ ] `minfin/oil_gas_mes.go`:
  - константы страницы, селектора, таблицы, листа, якоря, имён рядов, запроса чека;
  - `oilGasImport` — raw-значения, классификация колонок (сериал/год), карта подписей, fail-fast;
  - `Import()` — `util.GetPage` → `util.FindLink` → `minfinUrl + href` → шаблон;
  - `AfterImport: publishFedbud(oilGasSeriesMeta)`, `PublicationCountPoints`.
- [ ] `minfin/fedbud_mesyats.go`: конфиг вынесен в `var fedbudMesyats`, персональный `init()` удалён.
- [ ] `minfin/importers.go`: реестр трёх импортёров, единственный `init()`, литеральные таблицы и запросы чека.
- [ ] `minfin/series_meta.go`: `minfinOilGasSource`, `oilGasSeriesMeta`, `minfinBudgetView` на три таблицы.
- [ ] `dagu/minfin.yaml`: шаг `minfin_oil_gas_mes`.

## Задача 5. Тесты и фикстуры

- [ ] `minfin/testdata/Svedeniya_20261005.xlsx` — реальный файл.
- [ ] `minfin/testdata/fedbud_page.html` — реальная страница.
- [ ] `minfin/oil_gas_mes_test.go`: ссылка, 210 точек, крайние даты, spot-значения, отсутствие годовых точек, fail-fast, двусторонний каталог.
- [ ] `minfin/importers_test.go`: энфорсер реестра и расписания.

## Задача 6. Документация и MCP

- [ ] `ARCHITECTURE.md` §2, §6.1, §6.2, §6.3, §7, §8 п. 13.
- [ ] `README.md`, `docs/IMPORTERS.md`.
- [ ] `docs/IMPORTER_PACKAGE_STANDARD.md` §18.
- [ ] `sql/key_rate_coverage.sql`.
- [ ] `scripts/mcp_check.py` — блок Минфина.
- [ ] `sql/mcp_kimi_reader.sql` — проверить, что сырые таблицы не выданы (грант витрины уже есть).

## Задача 7. Проверки

- [ ] `make all`.
- [ ] `go test -race -count=1 ./minfin/`.
- [ ] `make ch-status`.
- [ ] `make import STAT=minfin_fed_bud_mes`; SQL по ряду дефицита.
- [ ] `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev make import STAT=minfin_oil_gas_mes`.
- [ ] Повтор того же прогона — no-op.
- [ ] SQL: `v_minfin_budget`, `v_series_catalog`, закрытость сырых таблиц.
- [ ] `make mcp-check`.
- [ ] `dagu validate dagu/minfin.yaml`.
- [ ] Архив: объект и строка `registry.jsonl`.
- [ ] `make docs-check`.

## Задача 8. Приёмка и PR

- [ ] Статус пункта ✅ в `docs/ROADMAP_KEY_RATE.md` §4.2 №15, §4.3 №15 и в цепочке фазы 2 — с командой и результатом.
- [ ] PR с `Closes #116`, пунктом `key-rate/phase-2-minfin-scrape`, источником, схемой, запуском и примером проверки.
- [ ] Project V2: `Status = In Review`, `PR`, `Spec / Plan`, `Acceptance`.
- [ ] Протокол живой приёмки — комментарием в PR.
- [ ] После мержа: `Status = Done`; синхронизация роадмапа с issue (скил `github-tracker` §10).

## Протокол живой приёмки

Заполняется после прогонов: команды, число строк, крайние даты, spot-значения, no-op повтора, результат `make mcp-check`, объект архива и строка `registry.jsonl`.
