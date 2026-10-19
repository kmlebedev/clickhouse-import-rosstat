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
- [x] `minfin/series_meta_test.go`: общий контракт каталога не менялся, добавлен адресный `TestFedbudMesDeficitSeries`.

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

## Протокол живой приёмки (2026-10-19)

- `make import STAT=minfin_fed_bud_mes` — `Imported 1076 rows`. Ряд `Дефицит (-)/Профицит (+)`: **189 точек, 2011-01-01 … 2026-09-01**; spot 2026-09 = **614.3** млрд руб. — разность накопленного сальдо из источника `−5 185.6 − (−5 799.9) = 614.3` (legacy-парсер `fedBudImport` читает форматированные значения `#,##0.0`, поэтому точность ряда — 0,1 млрд руб.; сырые значения дают 614.2690, но в таблицу не попадают). Остальные ряды: 189 точек (кроме «Национальная оборона» 132 и «привлечение» 188).
- `make import STAT=minfin_oil_gas_mes` (активный архивный бэкенд из `~/.config/rosstat/env`; `ARCHIVE_YANDEX_TOKEN` задан) — `Imported 210 rows`: `Нефтегазовые доходы, всего` — 105 точек, `НДПИ` — 105 точек, обе **2018-01-01 … 2026-09-01**. Spot: 2018-01 — 661.3 / 442.9; 2026-09 — 452.4 / 807.0.
- `source_publications` — строка `minfin_oil_gas_mes` / `Svedeniya_20261005.xlsx`, `points = 210`, `first_seen = 2026-10-19 05:36:55`.
- Архив — `make archive-list` показывает версию `minfin_oil_gas_mes · Svedeniya_20261005.xlsx · 9bfefd6d · 56799 · 2026-10-19T05:36:52+05:00 · https://minfin.gov.ru/common/upload/library/2026/10/main/Svedeniya_20261005.xlsx`.
- Повторный прогон — **no-op**: `версия 9bfefd6de4b343e79930fb339b115c1a уже импортирована, 210 точек на месте` (без скачивания файла).
- `v_minfin_budget` — **2162 строки, 3 источника**: `minfin` 1076 строк / 6 рядов, `minfin_mesyats` 876 / 5, `minfin_oil_gas` 210 / 2. `v_series_catalog`: `minfin_oil_gas` — 2 ряда; `minfin` — 6 рядов, включая дефицит. Сырые `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats`, `minfin_oil_gas_mes` — `ACCESS_DENIED` (497).
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА**. Для зелёного общего прогона поднят уже отревьюенный фикс `b1c909a` (`list_tables` с `page_size=1000`) из ветки `point-in-time`: без него единственный FAIL был предсуществующим — `v_rosstat_macro` уходила на вторую страницу `list_tables`, к Минфину отношения не имеет.
- `dagu validate dagu/minfin.yaml` — exit 0 (штатный WARN про `auth.mode`); `make docs-check` — 33 страницы / 33 теста OK; `make all` — зелёный (golangci-lint 0 issues, `go test -race ./...`, обе сборки).
