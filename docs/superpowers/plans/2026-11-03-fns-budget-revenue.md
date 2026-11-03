# План реализации `rosstat/1.11` — ФНС 1-НМ: `fns_budget_revenue`

**Спека:** `docs/superpowers/specs/2026-11-03-fns-budget-revenue-design.md`
**Issue:** [#498](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/498)
**Ветка:** `feat/rosstat-1.11-fns-budget-revenue` · **worktree:** `.worktrees/rosstat-1.11-fns-budget-revenue`

Порядок задач — сверху вниз; каждая задача коммитится отдельно, PR один и в конце (правило 3 разработки по роадмапу).

## Task 1: каноническая схема и реестр (scaffolding)

- `ARCHITECTURE.md` §6.2 — DDL `fns_budget_revenue` (правило 8: схема сначала в каноне), §2 — строка пакета `fns/`.
- `fns/importers.go` — тип записи реестра, `fnsImporters = {fnsBudgetRevenue, "fns_budget_revenue", ""}`, единственный `init()`.
- `main.go` — blank-import `fns`.
- `dagu/fns.yaml` — `type: chain`, `schedule: "37 9 * * *"`, шаг `fns_budget_revenue`, retry 3 × 300 с.
- Коммит: `add fns package skeleton for 1-NM budget revenue`.

## Task 2: разбор листа `11О` (TDD на фикстурах)

- Фикстуры: `fns/testdata/1nm010926reg.zip`, `1nm010220reg.zip` (только `11О`, листы `1000`, `1010-1020`, `1130-1151`, `1610-1626`, все 94 строки); `forms_page.html`, `year_page_2026.html`, `year_page_2021.html`.
- `fns/budget_revenue_parse.go` — по спеке §3.5: выбор внутреннего файла, строка-якорь `А`, `(код, суффикс)` → уровень, канонизация территорий + алиасы, `RawCellValue`, пропуск `X`/`-`/пустых, fail-fast, один `PrepareBatch`/`Abort`.
- Карта рядов `fnsSeriesByCode` (13 кодов → стаб, 47 пар) и `fnsTerritories` (94) — в коде, не из заголовков (стандарт §6.8).
- Тесты: число точек, spot-значения обоих лет, счёт территорий, отрицательные значения, все ветки fail-fast, двусторонний контракт каталога.
- Коммит: `parse 1-NM territory sheet into fns_budget_revenue points`.

## Task 3: импортёр и инкремент

- `fns/budget_revenue.go` — собственный `Import()`: ссылки годов → месячные артефакты (дрейф имён, обратный слэш, `ut`-ревизии) → гейт по `SELECT DISTINCT date` + `FNS_1NM_FROM` → внутренний `util.ClickHouseImport` с `BodyFunc` на каждый новый месяц → `AfterImport` один раз.
- Тесты на `httptest`: известные месяцы не качаются, счётчик запросов, `FNS_1NM_FROM` смещает старт, пустая база → бэкфилл с 2020-01, ошибки обхода называют домен и ожидаемое имя.
- Коммит: `increment by month with FNS_1NM_FROM repair knob`.

## Task 4: каталог, витрины, гранты, MCP

- `fns/series_meta.go` — 47 записей (unit, frequency, origin, description: накопленное ↔ помесячное, `local ⊂ regional`, отрицательные значения, пропуски месяцев, сверка с Минфином и её разложение).
- `fns/views.go` — `v_fns_budget_revenue` и `v_fns_budget_revenue_monthly` (разность накопленных, `NULL` на пропуске), комментарии таблицы и колонок; `afterFnsMacro`.
- `sql/mcp_kimi_reader.sql` — два `GRANT SELECT`; `scripts/mcp_check.py` — витрины непусты, 47 рядов каталога, spot-значение, сырая таблица закрыта.
- Тест `fns/views_test.go` (или в `series_meta_test.go`) — контракт каталога и полнота колонок витрины.
- Коммит: `publish fns_budget_revenue catalog, monthly view and grants`.

## Task 5: документация и статус пункта

- `ARCHITECTURE.md` — реестр §4 (строка `fns_budget_revenue`), §6.2 (число копий DDL), §6.3 (обе витрины), §7/§9.3 (расписание и обоснование отсутствия точки контроля).
- `README.md` — строка пакета в таблице; `docs/IMPORTERS.md` — строка таблицы + абзац про источник, территории и накопленное/помесячное.
- `docs/ROADMAP_ROSSTAT.md` — `1.11` ✅ с командой и результатом (в теле коммита — протокол, по стандарту §16.1 п. 8) и таблица §2 источника (строка «Налоговая нагрузка… / поступления») — правка после живой приёмки.
- Коммит: `document fns 1-NM importer and close roadmap 1.11`.

## Task 6: проверки и приёмка

```bash
make all                                   # gofmt, golangci-lint, go vet, go test -race, обе сборки
go test -race -count=1 ./fns/
dagu validate dagu/fns.yaml
make ch-status                             # ClickHouse поднимает владелец
make import STAT=fns_budget_revenue        # бэкфилл 2020-01 …
make import STAT=fns_budget_revenue        # повтор: Imported 0 rows, count() не изменился
make mcp-user && make mcp-check
make docs-check
```

Сверка §4 спеки (по годам 2020–2026, тыс. руб. → трлн против млрд Минфина) и внутренний контроль (сумма субъектов ≈ РФ) — числами в протоколе.

## Task 7: ревью и PR

- Ревью по `subagent-dispatch`: task-reviewer на модели, отличной от авторской (`haiku` для задач), fix-loop; финальное ревью ветки — на сильнейшей доступной модели.
- PR один: пункт `rosstat/1.11`, `Closes #498` вне backticks, описание — источник, схема, запуск, пример проверки; протокол живой приёмки — комментарием в PR.
- Project V2: `Status = In Review` при открытии, `PR` заполнен. После мержа — `Done`, синхронизация трекера (§10 `github-tracker`), удаление worktree.

## Протокол приёмки (шаблон, заполняется по мере выполнения)

| Шаг | Результат |
|---|---|
| `make all` | |
| `go test -race -count=1 ./fns/` | |
| `dagu validate dagu/fns.yaml` | |
| первый `make import STAT=fns_budget_revenue` | Imported … rows |
| повтор | Imported 0 rows; `count()` … |
| `SELECT count(), min(date), max(date), uniqExact(region), uniqExact(series)` | |
| сверка с `v_minfin_budget` по годам | |
| `make mcp-check` | |
| `make docs-check` | |
