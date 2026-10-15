### Task 4: Окружение — расписание, грант, канонический DDL

**Files:**
- Modify: `dagu/moex.yaml`, `sql/mcp_kimi_reader.sql`, `ARCHITECTURE.md`

- [ ] **Step 1: Шаг в `dagu/moex.yaml`**

Добавить третьим шагом (после `ofz_curve`) по образцу соседей:

```yaml
  - id: keyrate_futures
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=keyrate_futures
    retry_policy:
      limit: 3
      interval_sec: 300
```

- [ ] **Step 2: Грант** — в `sql/mcp_kimi_reader.sql` добавить `GRANT SELECT ON default.v_keyrate_expectations TO kimi_reader;` (сырые `keyrate_futures`/`keyrate_contract` агенту не выдаются).

- [ ] **Step 3: `ARCHITECTURE.md`** — канонический DDL двух таблиц в §6.2 (дословно как в `moex/keyrate.go`), строка импортёра в §6.1 (источник, метод, расписание, где каталог), строка витрины в §6.3 с комментарием; календарь §7 — ближайшие заседания СД ЦБ уже перечислены, добавить пометку, что даты экспирации KEYRATE берутся из `cbr.ru/dkp/cal_mp/`.

- [ ] **Step 4: Проверки**

Run: `dagu validate dagu/moex.yaml` → OK
Run: `make all` → зелёный

---

