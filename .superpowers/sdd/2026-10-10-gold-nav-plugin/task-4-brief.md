### Task 4: Витрины v_model_inputs и v_gold_dashboard

**Files:**
- Create: `views/model_inputs.go`, `views/gold_dashboard.go`
- Modify: `main.go` (blank-import `views`)
- Modify: `sql/mcp_kimi_reader.sql`

**Interfaces:**
- Consumes: `util.CreateView` (`util/views.go`); таблицы `gold_prices`, `cbr_currency_usd`, `cbr_key_rate`, `ofz_curve`, `ipc_mes`, `ipc_weeks`, `macro_series`, `stock_prices`, `model_runs` (Task 3 создаёт `model_runs`; CreateView пропускает витрину до появления всех таблиц — поэтому `Name()` импортёра витрин `gold_views` запускается после ingest-первого-прогона; зафиксировать в комментарии пакета).
- Produces: витрины со составом колонок из спеки §5.1/§5.2; импортёр `Name() = "gold_views"`.

- [ ] **Step 1: `views/model_inputs.go`**

`util.View{Name: "v_model_inputs", Tables: [9 таблиц выше], Select: ...}` — один ряд: по каждому ряду `argMax(value, date)` + `argMax(date, date)`; золото `venue='moex_fix_usd'`; FRED-ряды по `(source='fred', series=...)`; `last_run_*` — `argMax` по `model_runs FINAL` с `toNullable` (Review Focus #1: при пустой таблице — NULL, строка всё равно есть). Комментарии всех колонок (что измеряет, источник, дата актуальности).

- [ ] **Step 2: `views/gold_dashboard.go`**

`SELECT` из заранее зафиксированного списка индикаторов (`crack_ulsd_proxy`, `distillate_stocks`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy`) левым соединением к последним значениям `macro_series FINAL` по `(source IN ('manual','fred'), series)`; колонки: `indicator, value Nullable(Float64), threshold String, flag UInt8, as_of Nullable(Date32), stale UInt8` (`stale=1` если `as_of` NULL или старше 14 дней — Review Focus #5). Комментарий таблицы: пороги из статьи §6 (crack >50 bear / FedWatch >65% >80% / DXY >102).

- [ ] **Step 3: Гранты + blank-import + прогон**

`GRANT SELECT ON default.v_model_inputs TO kimi_reader;` и `... v_gold_dashboard ...` в `sql/mcp_kimi_reader.sql`; blank-import в `main.go`; `make import STAT=gold_views`.
Expected: `SELECT * FROM v_model_inputs` — ровно 1 строка; `SELECT * FROM v_gold_dashboard` — 5 строк, у невведённых `stale=1`.

- [ ] **Step 4: Commit**

```bash
git add views/ main.go sql/mcp_kimi_reader.sql
git commit -m "add v_model_inputs and v_gold_dashboard views"
```

