### Task 6: Живая приёмка и статус 1.5

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Interfaces:**
- Consumes: всё выше; числа Task 1 (фактические).

- [ ] **Step 1: `make all`** — зелёный (`0 issues.`, vet, тесты, сборки).
- [ ] **Step 2: Первый прогон** — `make import STAT=cbr_mo_br` → полный импорт (`Imported N rows`, N зафиксировать). Админ-проверка (`make ch-sql`, временный SELECT): `SELECT source, name, fingerprint, first_seen, points, url FROM source_publications FINAL WHERE source = 'cbr_mo_br'` → одна строка, `points = N`.
- [ ] **Step 3: Повтор** — `make import STAT=cbr_mo_br` → в логе no-op-строка («версия … уже импортирована, N точек на месте»); новых строк в `source_publications` нет; `SELECT count() FROM v_cbr_mo_br` не изменился.
- [ ] **Step 4: MCP** — через `mcp__clickhouse__run_query` (`kimi_reader`): `SELECT count(), uniqExact(series), min(date), max(date) FROM v_cbr_mo_br` → ряды ≥ фактического из Task 1, глубина ≥ 2021-05-01…2026-09-01; каталог: `SELECT count() FROM v_series_catalog WHERE source='cbr_mo_br'` = числу рядов.
- [ ] **Step 5: `make mcp-user` → `make mcp-check`** — ПРОВЕРКА ПРОЙДЕНА (включая новые строки `v_cbr_mo_br`); `dagu validate dagu/cbr.yaml` → exit 0.
- [ ] **Step 6: Статус 1.5 в роадмапе** — `docs/ROADMAP_KEY_RATE.md`: статус 1.5 → `✅ **сделан этим PR** — приёмка ниже` + блок `#### Живая приёмка 2026-10-13 — шаг 1.5 (макроопрос ЦБ) — ✅ выполнен` со ссылкой на спеку/план и нумерованными фактами Step 1–5 с числами (no-op повтора — отдельным пунктом).
- [ ] **Step 7: Commit**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "roadmap key rate: mark 1.5 done (cbr_mo_br consensus acceptance)"
```

---

## После задач (вне нумерации)

Финальное ревью всей ветки (сильная модель) + `verification-before-completion`; затем один PR `feat/cbr-mo-br` → `main` (как в фазе 3). Спека и план — первыми коммитами ветки (уже на месте).
