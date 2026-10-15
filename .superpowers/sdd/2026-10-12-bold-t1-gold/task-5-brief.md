### Task 5: документы и живая приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§5, §6.1, §6.2, §6.3, §7, шапка), `README.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: статус пункта в роадмапе + PR.

- [ ] **Step 1: Живая приёмка — полный и инкрементный прогон**

```bash
make import STAT=bold_etf_flows    # ожидание: 21 054 точки ETF + 73 строки фондов, in log «полный режим»/история
make import STAT=bold_gold_price   # ожидание: 3 138 точек, площадка bold
make import STAT=bold_etf_flows    # повтор: no-op или инкремент, те же ключи (идемпотентно)
```
Expected: числа из шапки спеки; ошибок нет; в логе инкремента ETF-импортёра — 4 запроса (у ценового — 1).

- [ ] **Step 2: Проверка через MCP (`kimi_reader`)**

```sql
SELECT toString(date) AS date, flow_tonnes, flow_usd_mln, flow_7d_tonnes, flow_30d_tonnes, flow_90d_tonnes, holdings_tonnes, aum_usd_bn
FROM v_gold_etf_flows_daily ORDER BY date DESC LIMIT 3;
SELECT count() AS funds, toString(max(date)) AS d FROM v_gold_etf_funds_daily;
SELECT series, toString(max(date)) AS d, count() AS n FROM v_gold_prices WHERE series = 'bold' GROUP BY series;
```
Expected: 2026-10-09 — `4.233 / 573.75 / 1.360 / 30.825 / 167.214 / 3520.318 / 476.678`; фонды 73; площадка `bold` на 2026-10-09, 3138 точек.

- [ ] **Step 3: Синхронизировать документы**

По скилу `sync-readme-architecture`: `ARCHITECTURE.md` — §5 (новых env нет), §6.1 (два импортёра: источник, режимы, охват, T+1, ограничения), §6.2 (`source='bold'`, `bold_fund_snapshot`, площадка `bold`), §6.3 (две витрины), §7 (расписание 08:07 МСК), шапка; `README.md`; `docs/GOLD_THESIS_DATA_ROADMAP.md` — новый follow-up со ссылкой на приёмку и пометка, что план по эмитентам снят в пользу агрегатора.

- [ ] **Step 4: Финальная проверка**

Run: `make all && make mcp-check`
Expected: `make all` зелёный; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (две новые витрины, каталог `bold` = 7, сырая `bold_fund_snapshot` закрыта).

- [ ] **Step 5: Commit и PR**

```bash
git add ARCHITECTURE.md README.md docs/GOLD_THESIS_DATA_ROADMAP.md
git commit -m "docs: bold t+1 gold contour"
```
Затем — `superpowers:finishing-a-development-branch`: один PR в конце (источник, схема таблиц, `CLICKHOUSE_IMPORT_STAT=bold_etf_flows|bold_gold_price`, пример проверки), без промежуточных PR.
