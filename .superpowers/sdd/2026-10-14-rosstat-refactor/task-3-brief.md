### Task 3: Фикстуры `rosstat/testdata/` и базовая линия

**Files:**
- Create: `rosstat/testdata/{ipc_mes.xlsx,ipc_weeks.xlsx,salaries_mes.xlsx,vvp_kvartal.xlsx,price_page.html,salaries_page.html,accounts_page.html}`
- Delete: `rosstat/data/nedel_ipc_2023.xlsx`
- Create (git-ignored): `.superpowers/sdd/2026-10-14-rosstat-refactor/rosstat-baseline.md`

**Interfaces:**
- Produces: имена фикстур, на которые ссылаются тесты T4–T7.

- [ ] **Step 1: Скачать живые артефакты в временный каталог**

Источники (проверены 2026-10-14): страница `https://rosstat.gov.ru/statistics/price` (877 КБ; ссылки `ipc_mes_ММ-ГГГГ.xlsx` и `Nedel_ipc.xlsx`), `https://rosstat.gov.ru/labor_market_employment_salaries` (ссылка `tab1-zpl_ММ-ГГГГ.xlsx`), `https://rosstat.gov.ru/statistics/accounts` (ссылка `VVP_kvartal_s-1995-ГГГГ.xlsx`). Адреса самих XLSX брать со страниц (`grep -o 'href="[^"]*<маска>[^"]*"'`).

- [ ] **Step 2: Положить XLSX в `rosstat/testdata/`**

`ipc_mes.xlsx` (~38 КБ), `ipc_weeks.xlsx` (~173 КБ, листы `Содержание, 2022…2026`), `salaries_mes.xlsx` (~21 КБ), `vvp_kvartal.xlsx` (~115 КБ) — как есть, без урезания (форматы и листы не менять, стандарт §11.2). `Nedel_ipc.xlsx` кладётся как `ipc_weeks.xlsx`.

- [ ] **Step 3: Положить HTML-страницы**

`price_page.html` (одна страница обслуживает и `ipc_mes`, и `ipc_weeks` — разные селекторы), `salaries_page.html`, `accounts_page.html`.

- [ ] **Step 4: Удалить `rosstat/data/` и обновить ссылку в тесте**

`git rm -r rosstat/data`; в `rosstat/series_meta_test.go` путь `data/nedel_ipc_2023.xlsx` → `testdata/ipc_weeks.xlsx`.

- [ ] **Step 5: Снять базовую линию (read-only)**

Через MCP `kimi_reader` (сырые таблицы агенту закрыты — снимать по витрине) записать в `.superpowers/sdd/2026-10-14-rosstat-refactor/rosstat-baseline.md`:

```sql
SELECT series, count() AS points, toString(min(date)) AS d0, toString(max(date)) AS d1
FROM v_rosstat_macro GROUP BY series ORDER BY series;
SELECT count() AS catalog_rows FROM v_series_catalog WHERE source = 'rosstat';
```

Известное на 2026-10-14 (сверка, что снимок не разошёлся): витрина 27 407 строк / 132 ряда, `d0 = 1991-01-31`, `d1 = 2026-10-05`; каталог `source='rosstat'` — **132 строки** (4 ИПЦ + 128 товаров: `salaries_mes` и `vvp_kvartal` до `AfterImport` не доходят и своих описаний не пишут); `salaries_mes` и `vvp_kvartal` в витрину не входят вовсе.

- [ ] **Step 6: Коммит**

```bash
git add -A rosstat/testdata rosstat/series_meta_test.go && git rm -r --cached rosstat/data 2>/dev/null; git add -A rosstat/
git commit -m "rosstat: real source fixtures in testdata/, drop legacy data dir"
```

---

