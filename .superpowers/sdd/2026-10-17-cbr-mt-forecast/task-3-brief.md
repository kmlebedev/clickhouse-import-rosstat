### Task 3: MCP-контур — проверки, грант, снимок покрытия

**Files:**
- Modify: `scripts/mcp_check.py` (блок `cbr_mt_forecast` по образцу блока `cbr_dip_bulletin` строки ~832–929)
- Modify: `sql/mcp_kimi_reader.sql` (+1 строка)
- Modify: `sql/key_rate_coverage.sql` (+1 ветка `UNION ALL`)

**Interfaces:** Produces: проверки `make mcp-check` для витрины/каталога/винтажности/закрытости сырой таблицы; грант `kimi_reader`; ветка снимка покрытия для §4.1 роадмапа.

- [ ] **Step 1: `sql/mcp_kimi_reader.sql`** — `GRANT SELECT ON default.v_cbr_mt_forecast TO kimi_reader;` (в конец списка).
- [ ] **Step 2: `sql/key_rate_coverage.sql`** — ветка `cbr_mt_forecast`: группировка по `indicator`, `count()`, `min(release_date)`, `max(release_date)` + комментарий: даты — публикации выпусков (винтаж), само число точек умножается на число выпусков, число выпусков — отдельным запросом `uniqExact(release_date)` (стиль комментария дипа).
- [ ] **Step 3: `scripts/mcp_check.py`** — блок по образцу дипа (строки ~832–929): витрина `v_cbr_mt_forecast` существует, читается и непуста; каталог — ровно 16 (`count() AS n … WHERE source = 'cbr_mt_forecast'`); винтажность — `uniqExact(release_date) > 1`, пустых `release_date`/`meeting_date` нет; сырая `cbr_mt_forecast` закрыта (`ACCESS_DENIED`/497, `UNKNOWN_TABLE` — провал). Комментарии — с причинно-следственной связью (как у дипа).
- [ ] **Step 4: Проверка синтаксиса** — `python3 -c "import ast; ast.parse(open('scripts/mcp_check.py').read())"`; `python3 scripts/mcp_setup_user_test.py` (если есть офлайн-тест — зелёный).
- [ ] **Step 5: Commit** — `cbr: mt_forecast MCP checks, grants, coverage snapshot`.

