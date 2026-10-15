### Task 3: Витрины `v_gost_*` — стейтменты, гранты, mcp-check

**Files:**
- Create: `craw/gost_views.go`, `craw/gost_views_test.go`
- Modify: `craw/gost.go` (вызов `gostPublishViews` в пишущих режимах до обхода дней), `sql/mcp_kimi_reader.sql` (2 GRANT), `scripts/mcp_check.py` (блок проверок)

**Interfaces:**
- Consumes: таблица `gost_vehicle_safety_certificate` (Task 2); образец стейтментов — `news/schema.go`.
- Produces:
  - `var gostViewStatements []string` — 14 стейтментов: 2 × `CREATE OR REPLACE VIEW … DEFINER = default SQL SECURITY DEFINER`, комментарий таблицы + 9 комментариев колонок у `v_gost_vehicle_safety_certificate`, комментарий таблицы + 3 комментария колонок у `v_gost_vehicle_safety_daily`.
  - `func gostPublishViews(ctx context.Context, conn driver.Conn) error` — исполняет стейтменты по порядку (образец — `news/news.go`).

- [ ] **Step 1: Написать падающие тесты** — `craw/gost_views_test.go`:

```go
func TestGostViewStatementsShape(t *testing.T)  // len == 14; [0] содержит "CREATE OR REPLACE VIEW v_gost_vehicle_safety_certificate",
                                                // "DEFINER = default SQL SECURITY DEFINER", "FROM gost_vehicle_safety_certificate FINAL",
                                                // без "uid" и без "ORDER BY"; дневная: "uniqExact(certificate_number)", "GROUP BY date, mark",
                                                // "ORDER BY date DESC, mark"; один ALTER MODIFY COMMENT и по COMMENT COLUMN на каждую колонку обеих витрин
func TestGostViewsDocumentSemantics(t *testing.T) // в текстах комментариев есть: «дата выдачи СБКТС (МСК)», «дата публикации среза»,
                                                  // «реестр СБКТС Росстандарта», «REST API», «изменения сертификата дают вторую строку»,
                                                  // «год выпуска 0 — не указан/до бэкфилла»
func TestGostImportWritesPublishViews(t *testing.T) // Import(GOST_MODE=since, пустая таблица) → в calls есть 14 стейтментов витрин ДО первого PrepareBatch
```

- [ ] **Step 2: Прогнать — падает** (`undefined: gostViewStatements`)

- [ ] **Step 3: Реализовать `craw/gost_views.go`** + дописать гранты и проверки

- Витрина строк: `SELECT date, publication_date, mark, type, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer FROM gost_vehicle_safety_certificate FINAL` — **без ORDER BY** (3.4 млн строк: сортировка на каждом запросе не нужна, комментарий велит фильтровать по `date`; `uid` в витрину не выводится).
- Дневная: `SELECT date, mark, uniqExact(certificate_number) AS certificates FROM … FINAL GROUP BY date, mark ORDER BY date DESC, mark`.
- Комментарии: таблицы и колонок, из спеки §«Витрины» (включая оговорки: amendments дают вторую строку; `certificates` — `uniqExact`, а не число строк; легаси-строки).
- `sql/mcp_kimi_reader.sql`: в конец блока `GRANT SELECT ON default.v_gost_vehicle_safety_certificate TO kimi_reader;` и `… ON default.v_gost_vehicle_safety_daily …` (одна строка на грант, как соседние).
- `scripts/mcp_check.py`: блок по образцу bold-блока — существование и читаемость обеих витрин подтверждаются запросом `SELECT count() FROM v_gost_*` (не поиском имени в `list_tables`), `SELECT count() FROM gost_vehicle_safety_certificate` → ожидание `ACCESS_DENIED` (497).

- [ ] **Step 4: Прогнать — проходит + саботаж** (убрать `uniqExact` из дневной → падает `TestGostViewStatementsShape`)

- [ ] **Step 5: Commit** — `git add craw/gost_views.go craw/gost_views_test.go craw/gost.go sql/mcp_kimi_reader.sql scripts/mcp_check.py && git commit -m "craw: publish gost views and mcp checks"`

---

