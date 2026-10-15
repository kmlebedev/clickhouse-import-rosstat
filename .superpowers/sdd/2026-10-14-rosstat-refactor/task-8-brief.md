### Task 8: Каталог, витрина и проверки MCP

**Files:**
- Modify: `scripts/mcp_check.py`
- Modify: `rosstat/series_meta.go` (комментарий витрины, если состав изменился)

**Interfaces:**
- Consumes: результат T4–T7 (все четыре ряда описаны), `v_rosstat_macro`, `v_series_catalog`.
- Produces: строки проверки контура Росстата в `scripts/mcp_check.py`.

- [ ] **Step 1: Добавить проверки в `scripts/mcp_check.py`**

По образцу блоков `v_cbr_macro` и `v_bea_pce` (там же — хелперы `expect`/`text`):

```python
# Контур Росстата: витрина видна и НЕПУСТА (пустая витрина от неотработавшего
# импортёра успехом не считается), каталог описывает все 138 рядов источника,
# четыре сырые таблицы агенту закрыты.
failed += not expect("витрина v_rosstat_macro видна", "v_rosstat_macro" in tables, "list_tables default")
rosstat = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, uniqExact(series) AS series FROM v_rosstat_macro"}))
# ожидание: строк > 0 (число меняется с каждым прогоном источника), рядов 138
# (4 ИПЦ + 128 недельных + 5 ВВП + 1 зарплата)
rosstat_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat'"}))
# ожидание: n == 138
for raw in ("rosstat_ipc_mes", "rosstat_ipc_weeks", "rosstat_salaries_mes", "rosstat_vvp_kvartal"):
    denied = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {raw}"}))
    failed += not expect(f"сырая {raw} закрыта", denied_clickhouse(denied), f"отказ ClickHouse на {raw} (ожидаемо)")
```

Использовать существующие в файле хелперы проверки отказа (`497`/`ACCESS_DENIED`) — как в строке про `cbr_households_b_mes`; не изобретать новую логику.

- [ ] **Step 2: Прогнать проверку**

Run: `make mcp-check`
Expected: `ПРОВЕРКА ПРОЙДЕНА` (exit 0). До выполнения T9 число рядов может быть 132 — это ожидаемое состояние до первого живого прогона; в этом случае зафиксировать и вернуться к шагу после T9.

- [ ] **Step 3: Коммит**

```bash
git add scripts/mcp_check.py rosstat/series_meta.go
git commit -m "mcp_check: verify rosstat view, catalog and closed raw tables"
```

---

