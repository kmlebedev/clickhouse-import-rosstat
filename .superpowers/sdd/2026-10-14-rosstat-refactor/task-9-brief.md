### Task 9: Живая приёмка и миграция данных

**Files:**
- Modify (по результату): `docs/ROADMAP_KEY_RATE.md` (§4.1, §6, §8) — статусы и приёмочный блок

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: запись живой приёмки, на которую ссылается роадмап.

- [ ] **Step 1: Полная проверка сборки и тестов**

Run: `make all`
Expected: `0 issues.` (gofmt + golangci-lint), `go vet` без замечаний, `go test -race ./...` — все пакеты `ok`, обе сборки. Отдельно свежим прогоном: `go test -race -count=1 ./rosstat/ ./cbr/ ./util/`.

- [ ] **Step 2: Grep-чистота пакета**

Run: `grep -rn "gocolly\|fmt\.Print" --include=*.go rosstat/`
Expected: пусто.

- [ ] **Step 3: Прогнать четыре импортёра по одному**

Run: `make import STAT=rosstat_ipc_mes`, затем `rosstat_ipc_weeks`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal`.
Expected: rc=0 у всех; новые таблицы создаются **рядом** со старыми, `v_model_inputs` продолжает читать старые.

- [ ] **Step 4: Сверить состав и данные**

Run (через MCP по витрине, сырые закрыты) и записать факты: число рядов и строк по каждому из четырёх источников, крайние даты.
Ожидание: `rosstat_ipc_mes` — 4 ряда / 1716 строк (имена рядов новые, значения совпадают с базовой линией); `rosstat_ipc_weeks` — 131 ряд / 26 611 строк (прежние 128 / 25 691 объясняются: −28 нулей, записанных старым разбором из ячеек «…», +948 числовых ячеек 19 строк товаров, которые обрыв листа отрезал, и +3 товара, которых в таблице не было вовсе); `rosstat_salaries_mes` — 1 ряд, 421 точка, 1991-08-01…2026-08-01 (нулей января–июня 1991 нет; дата точки — 1-е число месяца, следующего за отчётным, поэтому июль 1991 записан на 1991-08-01, а июль 2026 — на 2026-08-01); `rosstat_vvp_kvartal` — 5 рядов, 302 точки, с 2011-04-01 по 2026-07-01 (2 квартал 2026).

- [ ] **Step 5: Spot-сверка с публикацией Росстата**

Сверить и записать: средняя номинальная зарплата за июнь 2026 (из `salaries_mes`), ВВП в текущих ценах за 1кв и 2кв 2026 (лист «2»), ИФО ВВП за те же кварталы (лист «12»). Источник сверки — открытая публикация Росстата (страница рынка труда / национальных счётов).

- [ ] **Step 6: Повтор — no-op**

Run: `make import STAT=rosstat_ipc_mes`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal`
Expected: `no-op, версия … точек на месте` без скачивания; `rosstat_ipc_weeks` — идемпотентный повтор, счётчики в витрине не меняются.

- [ ] **Step 7: Переключить витрины контура золота**

Run: `make import STAT=gold_views`
Expected: `v_model_inputs` пересоздана на новых таблицах; запрос `SELECT ipc_mes_last, ipc_mes_last_date, ipc_week_ytd, ipc_week_ytd_date FROM v_model_inputs` — значения не NULL и совпадают с базовой линией.

- [ ] **Step 8: Удалить старые таблицы и осиротевшие версии**

Проверить, что старых имён нет ни в коде, ни в SQL, ни в дашбордах (`grep -rn "ipc_mes\|ipc_weeks\|salaries_mes\|vvp_kvartal" --include=*.go --include=*.sql . | grep -v rosstat_`). Затем admin-командой `make ch-sql FILE=<файл>` удалить `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal` и осиротевшие строки `source_publications` по четырём старым ключам. Временный SQL-файл не коммитить.
Expected: витрина `v_rosstat_macro` по-прежнему читается агентом, `v_model_inputs` не деградировала.

- [ ] **Step 9: MCP и расписание**

Run: `make mcp-user` (если требуются новые гранты — их нет) и `make mcp-check`; `dagu validate dagu/rosstat.yaml`.
Expected: `ПРОВЕРКА ПРОЙДЕНА`; exit 0 (единственное сообщение — общее предупреждение `auth.mode`).

- [ ] **Step 10: Обновить статусы роадмапа**

`docs/ROADMAP_KEY_RATE.md`: §4.1 — строки `salaries_mes`/`vvp_kvartal` больше не «пусто», с числом строк и диапазоном; §6 фаза 2 — первый шаг закрыт ссылкой на блок приёмки; §8 — строка риска про технический долг `rosstat/` обновлена. Приёмочный блок — по образцу блока фазы 3 (`cbr/`): команда → результат, плюс названные расхождения (raw-точность, изменение имён рядов `ipc_mes`, рост каталога 132 → 141).

- [ ] **Step 11: Коммит**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "roadmap: phase 2 first step done — rosstat empty series fixed, package refactored"
```

---

