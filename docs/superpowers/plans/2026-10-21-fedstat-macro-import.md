# План: импорт макро-блоков ЕМИСС в `fedstat/` (2026-10-21)

> **Пункт:** `key-rate/phase-4-emiss-macro`.
> **Спека:** [2026-10-21-fedstat-macro-import-design.md](../specs/2026-10-21-fedstat-macro-import-design.md).
> **Research и данные:** [2026-10-21-fedstat-macro-indicators.md](../../research/2026-10-21-fedstat-macro-indicators.md).
> **Правило:** один импортёр/таблица — один PR (стандарт §3.2); этот план — эпик, задачи 1–7 идут отдельными PR в указанном порядке.

> **Поправка 2026-10-21 (реализация):** ключи `fedstat_retail` и `fedstat_investment` дополняются `period` (подпись периода источника), у `fedstat_retail` — ещё `indicator_type`. Без этого накопительные периоды (`январь-март`, `январь-июнь`, …) имеют одну дату начала и схлопываются `ReplacingMergeTree`; у `31066` два вида показателя, которые спека не различала. Спека §4.2/§4.4/§5.3/§5.5 исправлена; канонические DDL — в `ARCHITECTURE.md` §6.2.

---

## Task 0. Ветка, спека, план, research (этот PR)

- Ветка: `feat/key-rate-emiss-macro` (worktree `.worktrees/key-rate-emiss-macro`).
- Файлы: research + CSV + фикстура; спека; план; строка роадмапа; `LIMITATIONS.md`; `site-pages.json`.
- Проверка: `make docs-check` — зелёный; кода, DDL и записи в ClickHouse нет.
- PR — draft: реализация в следующих PR, issue не закрывается.

---

## Task 1. Общий транспорт и Excel-парсер

**Файлы:** `fedstat/client.go`, `fedstat/excel.go`, `fedstat/client_test.go`, `fedstat/excel_test.go`, `fedstat/testdata/*`.

1. Вынести из `fedstat_weekly_prices.go`:
   - `fedstatGetPage(ctx, client, indicatorID)`;
   - `fedstatToken(page)`;
   - `fedstatPostForm(ctx, client, indicatorID, body)` (SDMX, retry/backoff);
   - `fedstatFormBody` — оставить в weekly или обобщить.
2. Добавить `fedstatPostExcel(ctx, client, indicatorID, body)`:
   - POST `downloadData.do?format=excel`;
   - проверка `Content-Type` (`application/vnd.ms-excel`), BIFF-магии `D0 CF 11 E0 A1 B1 1A E1`, размера;
   - HTML «Нет данных» → отдельная ошибка `errNoData`, не retry.
3. Добавить `excel.go`:
   - `readXLS(data []byte) (*xls.Workbook, error)` через `github.com/shakinm/xlsReader/xls` (уже в `go.mod`);
   - `sheetRows(sheet) [][]string` / `cellFloat`;
   - `findPeriodHeader(rows)` — матрица (периоды-колонки), long (периоды-строки), activity-columns (59001), year-columns (34129).
4. Тесты:
   - токен на HTML-фикстуре;
   - BIFF-детект;
   - HTML «Нет данных» → `errNoData`;
   - разбор каждой раскладки на реальном урезанном XLS;
   - поведение 37426 не изменилось (`fedstat/transport_test.go` зелёный).
5. Приёмка: `make all`; `go test -race -count=1 ./fedstat/`.

**Оценка:** 1–2 дня.

---

## Task 2. `fedstat_ipp` (43046 + 57806)

**Файлы:** `fedstat/ipp.go`, `fedstat/ipp_test.go`, `fedstat/testdata/ipp_*.xls`, `fedstat/series_meta.go` (часть), `fedstat/views.go` (часть), `fedstat/importers.go`.

1. DDL `fedstat_ipp` (см. спеку §4.1); ключ `(okved_version, okved, indicator_type, region, date)`.
2. Импортёр:
   - `43046`, 2000–2016, long-раскладка, 3 типа;
   - `57806`, 2015–2026, matrix, все ОКВЭД2, 3 типа;
   - `regionsFor(year)` — оба кода РФ;
   - пауза 10 с, retry/backoff.
3. Парсер: имена ОКВЭД2/типов из Excel; дата — первое число месяца; `okved_version`.
4. No-op: `max(date)` по каждому источнику; при отсутствии новых дат — `Imported 0 rows`.
5. Каталог: `ipp/<okved_version>/<okved>/<type>`; `origin` — ID + фильтры.
6. Витрина `v_fedstat_ipp`; грант; MCP-check.
7. Тесты: 36 строк/год для 43046; 55 356 строк для 57806; spot 2024-01 = 96,3 (Добыча); overlap 2015–2016 — mean abs diff 1,05 п.п. (не гейт, а отчёт).
8. Приёмка: `make import STAT=fedstat_ipp` — **55 968** строк; повтор — no-op.

**Оценка:** 2–3 дня.

---

## Task 3. `fedstat_retail` (31066 + 31260)

**Файлы:** `fedstat/retail.go`, `retail_test.go`, `testdata/retail_*.xls`, `series_meta.go`/`views.go`/`importers.go`.

1. DDL `fedstat_retail` (спека §4.2).
2. `31066`: 2000–2026, 2 типа × 3 субъекта, только месячные периоды.
3. `31260`: 2000–2026, 3 субъекта, месячные + накопительные; `period_type`.
4. No-op по `max(date)`.
5. Каталог `retail/<series>/<subject>`; витрина `v_fedstat_retail`; грант; MCP-check.
6. Тесты: 105 строк/год для 31066; 69 строк/год для 31260; spot 2000-01 = 83,3 (индекс) / 170 269,4 млн руб. (оборот).
7. Приёмка: **4 638** строк; повтор — no-op.

**Оценка:** 1–2 дня.

---

## Task 4. `fedstat_agri` (34128 + 59001 + 31062 + 43337)

**Файлы:** `fedstat/agri.go`, `agri_test.go`, `testdata/agri_*.xls`, `series_meta.go`/`views.go`/`importers.go`.

1. DDL `fedstat_agri` (спека §4.3).
2. `34128`: 2018–2026, Q, 1–2 категории; `59001`: 2017–2025, Y, activity-columns; `31062`: 1991–2016, Y; `43337`: 1990–2011, Y, стоимость.
3. No-op по `max(date)`; `series` = `index_q`/`index_y`/`value_y`.
4. Каталог `agri/<series>/<activity>/<category>`; витрина `v_fedstat_agri`; грант; MCP-check.
5. Тесты: 60 + 108 + 312 + 85 = 565 строк; spot 2024 Q1 = 101,5; 2025 = 105,2; 2010 = 2 462 187 млн руб.
6. Приёмка: **565** строк; повтор — no-op.

**Оценка:** 2 дня.

---

## Task 5. `fedstat_investment` (58089 + 58090 + 34129 + 40562 + 34475)

**Файлы:** `fedstat/investment.go`, `investment_test.go`, `testdata/investment_*.xls`, `series_meta.go`/`views.go`/`importers.go`.

1. DDL `fedstat_investment` (спека §4.4).
2. `58089`: 2017–2026, Q/CUM, total, full circle; `58090`: 2017–2026, Q/CUM, total, ОКФС/ОКОФ «ВСЕГО»; `34129`: 2000–2024, Y; `40562`: 2005–2025, Y; `34475`: 1998–2016, Y.
3. `regionsFor(year)` — `58089` 2023 под `1688487`; fallback.
4. Нули `58090` 2024 Jan–Jun/Q2 = 0.0 — сохранять, пометить в описании.
5. No-op по `max(date)`; `indicator_id` различает источники.
6. Каталог `investment/<series>/<indicator_id>`; витрина `v_fedstat_investment`; грант; MCP-check.
7. Тесты: 41 + 47 + 25 + 21 + 19 = 153 строки; spot 2026 Jan–Jun = 90,1; 2025 = 34 501 717 162 млн руб. (40562); 2024 = 108,4 (34129).
8. Приёмка: **153** строки; повтор — no-op.

**Оценка:** 2–3 дня.

---

## Task 6. `fedstat_ipc` (31074)

**Файлы:** `fedstat/ipc.go`, `ipc_test.go`, `testdata/ipc_*.xls`, `series_meta.go`/`views.go`/`importers.go`.

1. DDL `fedstat_ipc` (спека §4.5).
2. 2002–2026, один POST на год, оба кода РФ, все 1 105 товаров/услуг, тип «К предыдущему месяцу», 12 месяцев.
3. Matrix-парсер: `region`, `product` + месяцы; имена из Excel.
4. No-op по `(max(date), count())`; 2026 — 9 месяцев.
5. Каталог `ipc31074/<product>`; витрина `v_fedstat_ipc`; грант; MCP-check.
6. Тесты: 204 038 строк, 1 105 продуктов; spot 2002-01 = 103,09; 2022-01 = 100,99; 2026-09 = 100,35; сверка агрегата с `rosstat_ipc_mes` — 0 расхождений на 2002/2022/2025.
7. Приёмка: **204 038** строк; повтор — no-op.

**Оценка:** 3–4 дня (основная стоимость — 25 POST × 30 с и фикстуры).

---

## Task 7. Сквозные проверки и синхронизация

Каждый PR задачи 2–6 уже включает **свою** таблицу, каталог, витрину, грант и шаг dagu (правило 11 AGENTS.md, стандарт §10–§13). Этот task — финальная сквозная проверка и синхронизация:

1. `series_meta.go`: проверить полноту каталога (31074 — 1 105, 57806 — ~400, остальные — десятки).
2. `views.go`: пять long-витрин + комментарии.
3. `sql/mcp_kimi_reader.sql`: `GRANT SELECT` на пять витрин.
4. `scripts/mcp_check.py`: непустота, число рядов, отказ `497`.
5. `dagu/fedstat.yaml`: пять шагов в хвосте, пятница 14:07–14:47, retry.
6. `ARCHITECTURE.md` §6.1/§6.2/§6.3/§7/§8, `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`.
7. Приёмка: `make mcp-check`, `dagu validate`, `make docs-check`.

**Оценка:** 1–2 дня.

---

## Task 8. Живая приёмка и протокол

1. `make all`; `go test -race -count=1 ./fedstat/`.
2. Пять прогонов `make import STAT=...` — числа из §9 спеки; повтор — no-op.
3. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
4. `dagu validate dagu/fedstat.yaml` — exit 0.
5. Протокол — комментарием в PR; статус роадмапа — ✅ после мержа.

---

## Порядок PR

| PR | Задачи | Таблица | Зависит от |
|---|---|---|---|
| 1 (этот) | Task 0 | — | — |
| 2 | Task 1 | — | PR 1 |
| 3 | Task 2 | `fedstat_ipp` + каталог/витрина/грант/шаг dagu | PR 2 |
| 4 | Task 3 | `fedstat_retail` + каталог/витрина/грант/шаг dagu | PR 2 |
| 5 | Task 4 | `fedstat_agri` + каталог/витрина/грант/шаг dagu | PR 2 |
| 6 | Task 5 | `fedstat_investment` + каталог/витрина/грант/шаг dagu | PR 2 |
| 7 | Task 6 | `fedstat_ipc` + каталог/витрина/грант/шаг dagu | PR 2 |
| 8 | Task 7 | сквозные MCP-check, dagu, docs | PR 3–7 |

**Правило:** каждый PR — один импортёр/таблица; PR 2 (транспорт) не меняет поведение 37426. Если владелец разрешит один PR на все пять таблиц, план всё равно выполняется по задачам, но коммиты идут в одну ветку.

---

## Риски и откат

- **503/HTML** — явная ошибка, не «0 строк»; пауза 30 с для 31074.
- **Регион 2023+** — fallback `1688487`; в приёмке проверить `58089` 2023.
- **`58090` нули** — сохранить, пометить; Q&A в issue.
- **31074** — 204 038 строк, первый прогон долгий; resume по годам.
- **Откат** — каждый PR независим: удалить таблицу/витрину/грант и шаг dagu; 37426 не затрагивается.

---

## Приёмка 2026-10-21 (ветка `feat/emiss-macro-import`)

- `make import STAT=fedstat_ipp` — **55 968** строк, 2000-01-01…2026-08-01; повтор — `Imported 0 rows`.
- `make import STAT=fedstat_retail` — **4 638**, 2000-01-01…2026-08-01; повтор — `Imported 0 rows`.
- `make import STAT=fedstat_agri` — **565**, 1990-01-01…2026-04-01; повтор — `Imported 0 rows`.
- `make import STAT=fedstat_investment` — **153**, 1998-01-01…2026-01-01; повтор — `Imported 0 rows`.
- `make import STAT=fedstat_ipc` — **204 038**, 2002-01-01…2026-09-01; повтор — `Imported 0 rows`. 31074 импортируется **по годам с resume**: первый прогон был прерван на 2021 (rate limit), записанные годы сохранены; продолжение 2004–2026 добавило 190 142 строки, 2002–2003 — 13 896, итог 204 038.
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА**: `v_fedstat_ipp` 55 968/432/2, `v_fedstat_retail` 4 638/9/2, `v_fedstat_agri` 565/18/2, `v_fedstat_investment` 153/5/2, `v_fedstat_ipc` 204 038/1 105/2; каталог `source='fedstat'` — 1 727; сырые таблицы — 497.
- `make all`, `make docs-check`, `dagu validate dagu/fedstat.yaml` — зелёные.
