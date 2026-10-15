### Task 5: Живая приёмка и статус пункта

- [ ] **Step 1:** `make import STAT=keyrate_futures` → в логе строки по трём сериям; повторный прогон идемпотентен (`0` новых).

- [ ] **Step 2:** витрина:

```sql
SELECT secid, date, settle, expected_change_bp FROM v_keyrate_expectations ORDER BY secid, date DESC LIMIT 10;
```

Сверить с фактом: серия ближайшего заседания (`KKV6`, экспирация 23.10.2026) при ставке 14,00% должна давать ≈ −7 б.п. (наблюдённое `settle` 13,93). Результат записать в роадмап и в приёмку пункта.

- [ ] **Step 3:** `make mcp-check` → **ПРОВЕРКА ПРОЙДЕНА** (витрина читается, `keyrate_futures` — `ACCESS_DENIED`).

- [ ] **Step 4:** `dagu validate dagu/moex.yaml` → OK; каталог: `SELECT count() FROM v_series_catalog WHERE source='moex'` вырос на число серий.

- [ ] **Step 5:** `docs/ROADMAP_KEY_RATE.md` — статус 1.1 ✅ с командой и числом строк; «Приёмка» пункта заполнена фактом, не обещанием.
