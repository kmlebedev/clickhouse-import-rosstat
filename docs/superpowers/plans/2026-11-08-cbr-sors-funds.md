# План: импортёр `cbr_sors_funds` — средства клиентов банков (пункт `key-rate/phase-2-cbr-sors-funds`, issue #544)

**Дата:** 8 ноября 2026 г.
**Спека:** [2026-11-08-cbr-sors-funds-design.md](../specs/2026-11-08-cbr-sors-funds-design.md)
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №26, фаза 2.
**Ветка:** `feat/cbr-sors-funds`. **Worktree:** `.worktrees/cbr-sors-funds`.
**Исполнение:** контроллер (эта сессия); ревью — `subagent-dispatch` (task-reviewer на другой модели).

---

## Критерий приёмки пункта

Зелёные `make all`, `make docs-check`, `dagu validate dagu/cbr.yaml`; живой прогон
`make import STAT=cbr_sors_funds` с записью фактов (команда, число строк, крайние даты);
повтор — no-op по версии файла; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (включая новые проверки
каталога, витрины и deny 497 на сырой таблице); `ARCHITECTURE.md` / `README.md` /
`docs/IMPORTERS.md` / статус роадмапа обновлены тем же PR. `Closes #544` — вне backticks.

## Задачи

### Task 1: спека и план (первые коммиты ветки)

- `docs/superpowers/specs/2026-11-08-cbr-sors-funds-design.md` — спека §1–§6.
- `docs/superpowers/plans/2026-11-08-cbr-sors-funds.md` — этот план.
- Коммит до кода.

### Task 2: импортёр и разбор

- `cbr/sors_funds.go`:
  - константы: `cbrSorsFundsUrl`, `cbrSorsFundsTable = "cbr_sors_funds"`,
    `cbrSorsFundsCountPoints`, `cbrSorsFundsDateLayout = "02.01.2006"`, лимит дат шапки;
  - `cbrSorsFundsSheets` — три листа и суффикс валюты (`""`, `_rub`, `_fx`);
  - `cbrSorsFundsRows` — карта «нормализованная подпись → код» (7 строк);
  - `sorsFundsImport(xlsx, batch)`: три листа, поиск строки дат по признаку, карта подписей,
    сырые значения, `""`/`-` — пропуск, ноль — точка, non-numeric — ошибка, 0 точек — ошибка;
  - `util.ClickHouseImport`: DDL спеки §3.1, `DataUrl`, `ImportFunc`,
    `PublicationCountPoints`, `AfterImport: afterCbrMacro(cbrSorsFundsSeriesMeta)`;
  - регистрация значения `cbrSorsFundsImporter` (указатель на обёртку со встроенным шаблоном —
    как `householdsBMesImporter`).
- `cbr/testdata/02_01_Funds_all.xlsx` — живой файл целиком (42 КБ).

### Task 3: каталог, витрина, реестр, расписание

- `cbr/series_meta.go`: `cbrSorsFundsSeriesMeta` — 21 запись; описания по спеке §2/§3.3.
- `cbr/views.go`: `cbrMacroView.Tables` += `cbr_sors_funds`; UNION после `cbr_households_b_mes`;
  комментарии таблицы и колонки `series`.
- `cbr/importers.go`: запись в хвост реестра.
- `cbr/importers_test.go`: литерал `"SELECT count() FROM cbr_sors_funds FINAL"` в
  `expectedPublicationCountPoints`.
- `dagu/cbr.yaml`: шаг в хвосте, `retry_policy` 3 × 300 с, комментарий почему в хвосте.
- `sql/key_rate_coverage.sql`: UNION ALL `GROUP BY name`.

### Task 4: тесты

- `cbr/sors_funds_test.go`:
  - `TestSorsFundsSeriesMetaCoversParsedRows` — двусторонний контракт (`checkCbrSeriesMeta`);
  - `TestSorsFundsFixtureRows` — 3 177 точек, 21 ряд, 2012-02-01 … 2026-10-01, 1 059 на лист;
  - `TestSorsFundsSpotValues` — `clients_total` 2012-02-01 = 25 865 164 и 2026-10-01 = 154 200 014;
    `corp_deposits_fx` 2012-02-01 = 1 723 058; `org_accounts_rub` 2026-10-01 = 21 990 559;
    `households_deposits` 2019-02-01 = 27 855 014; `escrow_accounts_fx` 2026-10-01 = 0;
  - `TestSorsFundsEmptySheetFails` — нет листа, нет строки дат, пропала подпись, 0 точек,
    нечисловая ячейка (синтетические книги только для веток ошибок).

### Task 5: MCP-контур

- `scripts/mcp_check.py`: каталог 21, `uniqExact(series)` по списку 21 кода в `v_cbr_macro` = 21,
  сырая `cbr_sors_funds` → 497.

### Task 6: документация

- `ARCHITECTURE.md`: §2 (состав `cbr/`), §6.1 (строка импортёра), §6.2 (строка + DDL-блок),
  §6.3 (`v_cbr_macro`), §7 (33 → 34 шага, шаг 34-й), §8 п.13 (37 → 38, 22 → 23), шапка.
- `docs/IMPORTERS.md` и `README.md`: строка `cbr` — источник, таблица, счётчики.
- `docs/ROADMAP_KEY_RATE.md`: §4.2 №26 (пробел и предмет), параграф фазы 2; после живой
  приёмки — строка §4.1 и ✅ с числами.

### Task 7: проверки и приёмка

1. `make all`; `go test -race -count=1 ./cbr/`; `make docs-check`; `dagu validate dagu/cbr.yaml`.
2. `make import STAT=cbr_sors_funds` — ожидаемо `Imported 3177 rows`, 21 ряд,
   2012-02-01 … 2026-10-01. Фактические числа — в протокол (файл мог обновиться).
3. Повтор — no-op «3 177 точек на месте»; строка версии в `source_publications`.
4. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
5. Протокол живой приёмки — комментарием в PR (стандарт §16.1 п. 8).

## Риски и наблюдения

- **Число точек зависит от винтажа файла:** 3 177 — снимок 2026-11-08. Если ЦБ выпустит ноябрь
  до прогона, число вырастет на 3 (по одному на лист); тесты фикстуры пинят снимок, живой
  прогон — факт.
- **Строгая проверка 7 подписей:** переименование строки источником уронит шаг (это ошибка
  §6.4, а не «0 строк»); шаг в хвосте цепочки ограничивает радиус.
- **`clients_total` не аддитивен:** в каталоге зафиксировано, чтобы агент не собирал «всего»
  суммой компонент.
