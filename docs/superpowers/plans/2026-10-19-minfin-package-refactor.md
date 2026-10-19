# План: полный рефакторинг пакета `minfin/`

> **Для исполнителя:** пункт `key-rate/phase-2-minfin-refactor` (issue #233); отдельная ветка `refactor/minfin-package`, один PR. Спека — [2026-10-19-minfin-package-refactor-design.md](../specs/2026-10-19-minfin-package-refactor-design.md). WIP=1: не начинать `phase-2-ofz-debt-stress` (#170), пока этот пункт не смержен.

**Цель:** привести `minfin/` к общему шаблону: `BeforeImport`/`AfterImport`/`ImportFunc`, общий разбор листа «месяц», `Float32`→`Float64` с идемпотентной миграцией, реальная федеральная фикстура; убрать `colly` и ручные `Import()`.

**Архитектура:** `minfinFileImporter` (конструктор) + `fedbud_parse.go` (общий разбор) + три тонких конфига импортёров; DDL `Float64` + `ALTER ... MODIFY COLUMN value Float64 SETTINGS mutations_sync = 2` для двух бюджетных таблиц.

**Технологии:** Go, `util.ClickHouseImport`, `excelize/v2`, ClickHouse `ReplacingMergeTree`, Dagu, MCP `kimi_reader`.

---

## Task 1. Спека, план, схема, роадмап

**Files:**
- Create: `docs/superpowers/specs/2026-10-19-minfin-package-refactor-design.md`
- Create: `docs/superpowers/plans/2026-10-19-minfin-package-refactor.md`
- Modify: `ARCHITECTURE.md` (§6.2 — DDL `Float64` и миграция; правило 8 — до кода)
- Modify: `docs/ROADMAP_KEY_RATE.md` (пункт `phase-2-minfin-refactor`, порядок перед #170)

- [ ] Спека и план созданы; DDL зафиксирован; роадмап обновлён.
- [ ] Коммит: `minfin refactor: add spec, plan and schema`.

---

## Task 2. Общий шаблон и DDL

**Files:**
- Create: `minfin/importer.go`
- Modify: `minfin/fedbud_mes.go`, `minfin/fedbud_mesyats.go`, `minfin/oil_gas_mes.go`, `minfin/importers.go`

**Step 1.** `minfinURL`, `minfinFileImporter`, `newMinfinFileImporter`, `resolveDataURL`, `minfinLink`.
**Step 2.** `minfinSeriesCreate(migrate bool)`: `CREATE TABLE ... value Float64`, для бюджетов — `ALTER TABLE %s MODIFY COLUMN value Float64 SETTINGS mutations_sync = 2`.
**Step 3.** Перевести три импортёра на конструктор; удалить `*Stat` и ручные `Import()`.
**Step 4.** Реестр: `fedbudMes`, `fedbudMesyats`, `oilGasMes`; форма `minfinImporter` не меняется.
**Step 5.** `go test ./minfin/`.

---

## Task 3. Общий разбор

**Files:**
- Create: `minfin/fedbud_parse.go`
- Modify: `minfin/fedbud_mes.go`, `minfin/fedbud_mesyats.go`

**Step 1.** `fedbudLayout`, `importFedbud`, `normalizeFedbudLabel`, `parseFedbudDate`, `parseFedbudNumber`, `dateReplacer` (все 12 месяцев).
**Step 2.** `RawCellValue: true`; шапка по «Показатель»; даты из шапки; месячные разности; fail-fast.
**Step 3.** `fedBudImport`/`fedbudMesyatsImport` — обёртки; удалить дублирующий разбор и `fedbudMesyatsDate`.
**Step 4.** Комментарии: общий шаблон, без legacy/colly; `go test ./minfin/`.

---

## Task 4. Фикстура и тесты

**Files:**
- Create: `minfin/testdata/Prilozhenie_3_dannye_109-111_mes_202610.xlsx`
- Create: `minfin/fedbud_mes_test.go`
- Modify: `minfin/fedbud_mesyats_test.go`, `minfin/series_meta_test.go`, `minfin/oil_gas_mes_test.go`, `minfin/importers_test.go`

**Step 1.** Скачать реальный федеральный XLSX 2026/10 по ссылке из `testdata/fedbud_page.html`; сохранить книгу.
**Step 2.** Тест ссылки: `fedbudMesLinkSelector` на `fedbud_page.html` → 2026/10, `_mes.xlsx`.
**Step 3.** Тест разбора: 1076 точек; 189/189/189/132/188/189; нет `*`; spot-значения; max-даты 2026-09 / 2021-12 / 2026-08.
**Step 4.** Fail-fast: нет листа, шапки, ожидаемой строки, пустая ячейка внутри ряда, ноль точек.
**Step 5.** `fedbud_mesyats_test.go`: 884 точки, spot-значения, нормализация `Акцизы ****`.
**Step 6.** `series_meta_test.go`: `TestFedbudMesDeficitSeries` строит полную книгу.
**Step 7.** `importers_test.go`: `TestMinfinDDLValueFloat64`, `TestMinfinTemplateWiring`, комментарий о префлайте; заменить oil-gas-specific wiring-тест.
**Step 8.** `go test -race -count=1 ./minfin/`.

---

## Task 5. Документация

**Files:**
- Modify: `ARCHITECTURE.md` (§2, §6.1, §6.3, §7, шапка)
- Modify: `README.md`
- Modify: `docs/IMPORTERS.md`
- Modify: `docs/IMPORTER_PACKAGE_STANDARD.md` §18
- Modify: `docs/ROADMAP_KEY_RATE.md` (статус после приёмки)

**Step 1.** §2 — общий шаблон, Float64, без colly.
**Step 2.** §6.1 — строки двух бюджетных импортёров; §6.3 — `v_minfin_budget` Float64; §7 — расписание/префлайт.
**Step 3.** README/IMPORTERS — строка `minfin`; §18 — `minfin` ✅, colly 0, fmt.Print* 0, 12 `.go`-файлов, 5 тестовых.
**Step 4.** `make docs-check`.

---

## Task 6. Живая приёмка

```bash
make all
go test -race -count=1 ./minfin/
make import STAT=minfin_fed_bud_mes
make import STAT=minfin_fed_bud_mesyats
make import STAT=minfin_oil_gas_mes
make mcp-check
dagu validate dagu/minfin.yaml
make docs-check
```

**Ожидаемо:**
- `Imported 1076 rows`; `Imported 884 rows`; oil gas — no-op или `Imported 210 rows` после снятия его строки `source_publications` в dev.
- `OPTIMIZE ... FINAL`; типы `Float64` у всех трёх; `v_minfin_budget` — 2170 строк, 6/5/2 ряда.
- spot: `minfin/Доходы` 2011-01 = 804.8582643055; `minfin_mesyats/Доходы` 2011-01 = 1115.85497612064; `minfin/Акцизы` 2026-09 = 153.07289659012986; `minfin_mesyats/Акцизы` 2026-08 = 286.6877654519035.
- Повтор бюджетов идемпотентен; oil gas — no-op; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

### Протокол живой приёмки (2026-10-19)

1. `make all` — exit 0: golangci-lint `0 issues`, `go vet ./...`, `go test -race ./...` (`minfin` 4.148s), обе сборки.
2. `make import STAT=minfin_fed_bud_mes` — `Imported 1076 rows` (ссылка `.../2026/10/..._mes.xlsx`).
3. `make import STAT=minfin_fed_bud_mesyats` — `Imported 884 rows`.
4. `make import STAT=minfin_oil_gas_mes` — no-op: версия `9bfefd6de4b343e79930fb339b115c1a`, 210 точек на месте.
5. `OPTIMIZE ... FINAL` → `value Float64` у всех трёх таблиц; `v_minfin_budget` — 2170 строк (1076 + 884 + 210), 6/5/2 ряда, крайние даты 2011-01-01 … 2026-09-01 / … 2026-08-01 / 2018-01-01 … 2026-09-01.
6. Spot-значения после миграции: `minfin/Доходы, всего` 2011-01 = 804.8582643055 (было 804.9000244140625); `minfin_mesyats/Доходы, всего` 2011-01 = 1115.85497612064 (было 1115.85498046875); `minfin/Акцизы` 2026-09 = 153.07289659012986 (было 153.10000610351562); `minfin_mesyats/Акцизы` 2026-08 = 286.6877654519035 (было 286.6877746582031); `minfin/привлечение` 2026-08 = 0.
7. Повтор бюджетов — те же 1076/884, значения стабильны; oil gas — no-op. Это названное исправление точности: федеральный бюджет раньше читался без `RawCellValue`, оба бюджета хранились в `Float32`; набор точек не изменился.
8. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (минфин-блок: витрина 2170, oil gas 210/2, дефицит 189, сырые таблицы — 497, freshness 1 мес. / 0 мес.).
9. `dagu validate dagu/minfin.yaml` — exit 0 (информационный warning `No auth.mode configured`).
10. `make docs-check` — `checked 36 pages`, `docs_build_test.py` — 33 OK.

**Протокол живой приёмки** — комментарием в PR: команды, числа, крайние даты, повтор, mcp-check, dagu, docs-check.

---

## Task 7. PR

1. Один PR `[key-rate/phase-2-minfin-refactor] Полный рефакторинг пакета minfin/`; `Closes #233` вне backticks.
2. Project: `Status = In Review`, `PR = <номер/URL>`.
3. После мержа (решает владелец): `Status = Done`, статус роадмапа `✅`, удалить worktree.

---

## Out of scope

- Переименование таблиц/рядов/импортёров.
- Новые импортёры ОФЗ (#170), PIT/архив бюджетных файлов, префлайт бюджетным импортёрам (§9.3).
- Другие пакеты, ML/Python.
