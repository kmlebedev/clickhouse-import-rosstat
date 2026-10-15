### Task 5: Документация и живая приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§2, §6.1, §6.3, §7, §8 п. 13)
- Modify: `README.md` (таблица импортёров, эксплуатационный раздел)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§4.1, §4.2 п. 4 → ✅, §6 блок живой приёмки)

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: закрытый пункт роадмапа с числами приёмки.

- [ ] **Step 1: Обновить документацию**

- `ARCHITECTURE.md` §2 — файлы пакета `rosstat/`; §6.1 — строка импортёра `rosstat_labour_force_mes` (источник, лист, шесть рядов, точка контроля публикации, расписание); §6.3 — состав витрины `v_rosstat_macro` (плюс рынок труда); §7 — расписание `dagu/rosstat.yaml` (пятый шаг); §8 п. 13 — список импортёров с точкой контроля публикации: **14 → 15**.
- `README.md` — таблица импортёров и раздел эксплуатации (`make import STAT=rosstat_labour_force_mes`).
- `docs/ROADMAP_KEY_RATE.md` — §4.1: новая строка ряда; §4.2 п. 4: ✅ со ссылкой на блок приёмки; §6: блок «Живая приёмка 2026-10-14 — фаза 2, шаг 2».
- Порядок и полнота — skill `sync-readme-architecture`.

- [ ] **Step 2: `make all`**

Run: `make all`
Expected: `0 issues.` (gofmt + golangci-lint), `go vet ./...` без замечаний, `go test -race ./...` — все пакеты `ok`, обе сборки прошли.

- [ ] **Step 3: Живой прогон**

Run: `make import STAT=rosstat_labour_force_mes`
Expected: `Imported 690 rows of rosstat_labour_force_mes` (число берётся из факта; при обновлении файла Росстатом оно больше). Записать число строк и крайние даты.

Повтор:
Run: `make import STAT=rosstat_labour_force_mes`
Expected: `no-op, версия <fingerprint> уже импортирована, 690 точек на месте` — без скачивания файла (в логе нет `Get xlsx file`).

- [ ] **Step 4: Spot-сверка с живым источником**

Скачать живой `trud-1_15-s.xlsx`, взять ячейки последнего отчётного месяца и сверить с витриной (SQL — только через MCP `clickhouse`, пользователь `kimi_reader`; сырая таблица агенту закрыта, поэтому сверка идёт по витрине):

```
SELECT series, date, value FROM v_rosstat_macro WHERE series IN ('Рабочая сила','Уровень безработицы','Уровень занятости') ORDER BY date DESC LIMIT 3
```

Expected: значения совпадают с ячейками источника (в приёмке записать пары «ячейка → значение»).

- [ ] **Step 5: MCP-проверка**

Run: `make mcp-check`
Expected: **ПРОВЕРКА ПРОЙДЕНА** (exit 0), в том числе `витрина v_rosstat_macro непуста: строк: … , рядов: …` (было 29 050 / 141, станет больше), `каталог rosstat полон: рядов rosstat: 147`, `сырая rosstat_labour_force_mes закрыта (рабочая сила и безработица)`.

- [ ] **Step 6: Записать приёмку в роадмап и закоммитить**

В блок §6 роадмапа — числа из шагов 2–5 (команда, число строк, крайние даты, no-op, споты, результат `make mcp-check`).

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_KEY_RATE.md
git commit -m "docs: rosstat labour force importer in architecture, readme and roadmap"
```

---

## Приёмка пункта (после Task 5)

1. `make all` — зелёный.
2. `go test -race -count=1 ./rosstat/` — `ok`.
3. Живой прогон + повторный прогон (no-op) — числа записаны.
4. Spot-сверка с живым файлом Росстата.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
6. `dagu validate dagu/rosstat.yaml` — exit 0.
7. Документация и статус роадмапа — тем же PR.

Предпосылка: dev-ClickHouse поднят владельцем (`make ch-up` ассистент не запускает). Дальше — `superpowers:requesting-code-review` (финальное ревью ветки на `deepseek/deepseek-flash`, effort `max`) и `superpowers:verification-before-completion`, затем один PR.
