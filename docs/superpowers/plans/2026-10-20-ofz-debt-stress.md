# План: `key-rate/phase-2-ofz-debt-stress` — аукционы, госдолг, обслуживание

**Дата:** 2026-10-20
**Спека:** [2026-10-20-ofz-debt-stress-design.md](../specs/2026-10-20-ofz-debt-stress-design.md)
**Source report:** [2026-10-20-minfin-ofz-debt-sources.md](../../research/2026-10-20-minfin-ofz-debt-sources.md)
**Ветка:** `feat/ofz-debt-stress`, worktree `.worktrees/ofz-debt-stress`
**Пункт:** `key-rate/phase-2-ofz-debt-stress` (issue [#170](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/170))

## 1. Порядок

### Шаг 0. Ветка и захват

- `git worktree add .worktrees/ofz-debt-stress -b feat/ofz-debt-stress origin/main` — сделано.
- Claim в issue #170, Project V2: `In Progress`, `Branch`, `Priority=P1`, `Depends on=#233 (closed, PR #242 merged)`.
- Первый коммит — source report + эта спека + этот план: `docs: add ofz debt stress source report, spec and plan`.

### Шаг 1. Реестр, DDL, каталог и витрина

1. `minfin/ofz_meta.go`:
   - `minfinOfzAuctionSeriesMeta`, `minfinDebtStockSeriesMeta`, `minfinDebtServiceSeriesMeta`;
   - `ofzStressView` (`util.View`): таблицы `minfin_ofz_auction`, `minfin_debt_stock`, `minfin_debt_service`, `minfin_fed_bud_mes`, `cbr_zcyc`; Select с ближайшим тенором через `row_number()`, месячными join'ами, `ofz_stock_change`; комментарии таблицы и колонок.
   - `publishOfzStress(ctx, conn, meta)`: `UpsertSeriesCatalog` + `CreateView`.
2. `minfin/importers.go` — три записи реестра: `minfin_ofz_auction`, `minfin_debt_stock`, `minfin_debt_service`.
3. `minfin/importers_test.go`:
   - ожидаемый набор импортёров и DDL (`Float64`, `Nullable`, `ReplacingMergeTree`, ключи);
   - тест шаблона: `ImportFunc`/`BodyFunc`/свой `Import()` по типу;
   - тест расписания читает `dagu/minfin.yaml` и `dagu/ofz.yaml` в обе стороны.

### Шаг 2. `minfin_ofz_auction`

1. `minfin/ofz_auction.go`:
   - `ofzAuction` с `util.ClickHouseImport` (TableName, CreateTable, AfterImport);
   - свой `Import()`: текущий год — резолв тега `1349` → documento → XLSX; 2023/2024/2025 — константы; на каждый файл — `util.ClickHouseImport.Import`;
   - исторические годы — гейт `SELECT count() FROM minfin_ofz_auction FINAL WHERE toYear(date) = ?`;
   - текущий год — `PublicationCountPoints` (префлайт/архив).
2. `minfin/ofz_auction_parse.go`:
   - нормализация подписей шапки, обе версии 2023/2024+;
   - `parseOfzDate` (Excel-сериал, `DD.MM.YYYY`, `YYYY-MM-DD`), `parseOfzNumber` (`-`/`****` → NULL);
   - `parseOfzAuction(xlsx, batch)` с fail-fast.
3. `minfin/ofz_auction_test.go` — обе фикстуры, spot 2026-01-14, `-` → NULL, fail-fast.
4. `sql/mcp_kimi_reader.sql` — пока не грант (после витрины).

### Шаг 3. `minfin_debt_stock`

1. `minfin/debt_stock.go`:
   - свой `Import()`: внутренний `.xls` (`BodyFunc`, `xlsReader`), внешний `.xlsx` (`ImportFunc`);
   - ОФЗ-номинал: годовой `.xls` 2022 (`BodyFunc`) + месячные `debtDynamicsMonth_*.xlsx` (`ImportFunc`);
   - бэкфилл доступных месяцев 2023-05+ по шаблону URL, 404 — пропуск; повтор — последний документ с тега `1192`;
   - AfterImport — каталог + `v_ofz_stress`.
2. `minfin/debt_stock_parse.go`:
   - `parseInternalDebtXls(body, batch)` — листы-годы, строки «всего»/гарантии, даты-сериалы, пустые будущие ячейки;
   - `parseExternalDebtXlsx(xlsx, batch)` — `По состоянию на`, «Всего», гарантии;
   - `parseOfzStructure2022Xls(body, batch)` — лист `на 1 января 2023`, типы ОФЗ, сумма;
   - `parseOfzStructureMonthXlsx(xlsx, batch)` — шапка `По состоянию на`, строки ОФЗ, сумма без ГСО.
3. `minfin/debt_stock_test.go` — spot 2009/2011/2022/2026, пропуски, fail-fast.

### Шаг 4. `minfin_debt_service`

1. `minfin/debt_service.go`:
   - свой `Import()`: бюджетный `*_mes.xlsx` (`ImportFunc` с `fedbudLayout` из строки 2.13) + `indicatorsBudget_*.xlsx` (`ImportFunc`);
   - AfterImport — каталог + `v_ofz_stress`.
2. `minfin/debt_service_parse.go`:
   - бюджетная строка — переиспользовать `importFedbud`;
   - `parseIndicatorsBudget(xlsx, batch)` — месяцы, колонки `Привлечение`, `Погашение осн. долга`, `Чистое привлечение`, `Расходы на обслуживание`; млн → млрд; пропуск `в том числе:` и видов обязательств.
3. `minfin/debt_service_test.go` — январь 2026 (141.112 / 0 / 141.112 / 223.704 млрд), fail-fast.

### Шаг 5. Расписание

- `dagu/minfin.yaml` — в хвост `minfin_debt_stock`, `minfin_debt_service`.
- `dagu/ofz.yaml` — `schedule: "23 11 * * 4"`, один шаг `minfin_ofz_auction`.
- `dagu validate` (если установлен).

### Шаг 6. Грант и MCP

- `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_ofz_stress TO kimi_reader;`.
- `scripts/mcp_check.py` — проверки витрины, каталога, закрытости сырых таблиц, spot-точки.
- `make mcp-user` (владелец) и `make mcp-check` в приёмке.

### Шаг 7. Grafana

- `grafana/key_rate_publications.go` — две панели на `v_ofz_stress` (покрытие/спред; обслуживание/Δзапас).
- `grafana/key_rate_publications_test.go` — панели, `FROM v_ofz_stress`, `$__timeFilter`, юниты.
- `make grafana-build` → `dashboard/key-rate-publications.json`.

### Шаг 8. Документация

- `docs/ROADMAP_KEY_RATE.md` §4.2 №25 и §6 — `⏳ In Progress`, ссылки на issue/spec/plan/source report, состав и история.
- `ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7/§8.
- `docs/IMPORTERS.md` — строка `minfin`; `README.md` — список импортёров/таблиц.
- `docs/CONFIGURATION.md`/`docs/OPERATIONS.md` — только если появится env/команда.

## 2. Тесты и фикстуры

- Фикстуры уже скачаны в `minfin/testdata/` (см. source report §8). Никаких сетевых тестов: `httptest` + подмена base URL, HTML-фикстуры.
- Парсеры: обе шапки аукционов, legacy `.xls`, внешний XLSX, 2022 структура, месячный `debtDynamicsMonth`, indicators.
- Витрина: SQL содержит `FROM cbr_zcyc`, `row_number()`, `toStartOfMonth`, `ofz_stock_change`; комментарии заданы.
- Регистрация/расписание: оба dagu-файла, DDL, `Publication()`.

## 3. Проверка

1. `go test ./minfin/...` — по ходу.
2. `make all` — gofmt, golangci-lint, `go vet`, `go test -race`, сборка.
3. `make docs-check`.
4. `dagu validate dagu/minfin.yaml`, `dagu validate dagu/ofz.yaml`.
5. Живая приёмка:
   - `make import STAT=minfin_ofz_auction` → строки, `2023-01-11 … <последняя дата>`;
   - `make import STAT=minfin_debt_stock` → строки, крайние даты;
   - `make import STAT=minfin_debt_service` → строки, крайние даты;
   - повтор — `count()` не меняется;
   - `make mcp-check` — «ПРОВЕРКА ПРОЙДЕНА».
6. `make grafana-build`; SQL панелей через MCP; `make grafana-check` (staging — owner-assisted).
7. PR: `Closes #170` вне backticks, протокол приёмки комментарием; Project `In Review`.
8. После мержа — `Done`, синхронизация роадмапа, удалить worktree.

## 4. Стоп-правила

- Внутренний `.xls` или 2022 структура не разбираются — Q&A в #170, не подменять источник.
- Месячный `debtDynamicsMonth` отсутствует — NULL, не ноль; пропуски фиксируются в source report.
- ClickHouse недоступен — живые проверки «не проверено», не подменять.
- Объём вырос (например, план заимствований) — отдельный пункт роадмапа.
