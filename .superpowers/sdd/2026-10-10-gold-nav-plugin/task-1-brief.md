### Task 1: Пересмотр ROADMAP_DCF_POLYUS.md

**Files:**
- Modify: `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: —
- Produces: актуальный роадмап, на который ссылаются Task 6 и AGENTS.md-правило 10.

- [ ] **Step 1: Внести правки по спеке §4**

Точечные изменения (не переписывать документ):
- §2.2 «карта пробелов»: строкам eia/lbma/wgc/fedwatch/plzl добавить колонку-пометку или переписать столбец «Источник»: eia → «datasource `HO=F` (crack) + ручной ввод запасов», LBMA → «datasource `GC=F`; Nasdaq Data Link снят с критического пути», PLZL → «`moex_iss` ✅», WGC/FedWatch → «ручной ввод / WebBridge (фаза 5)».
- §7 фаза 1: удалить `eia`, нативный `lbma_gold`, `wgc`, `fedwatch` из плана импортёров; изменить статус `fred`/`bls`/`bea` на «исторический базлайн + Grafana + верификация; первичный источник для сессии — datasource».
- §7 фаза 2: добавить `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`, плагин `gold-nav`, mcp-clickhouse в HTTP-режиме с токеном, ingest-endpoint.
- Quick wins: заменить список на (1) `events_calendar` Q4-2026, (2) `v_model_inputs` + `v_gold_dashboard`, (3) плагин `gold-nav`.
- §11 (Kimi Work): дописать результаты спайка 2026-10-10 — плагины/персональный маркет/remote MCP в Kimi Work подтверждены, datasource-эквиваленты (Global Finance Data, IMF) доступны; добавить модель шаринга «хостинг у автора» и ссылку на спеку.

- [ ] **Step 2: Самопроверка консистентности**

Прогнать `Grep` по файлу: не осталось ли упоминаний `eia`-импортёра как планируемого Go-кода вне фазы 5. Ожидание: упоминания только в контексте datasource/ручного ввода/фазы 5.

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP_DCF_POLYUS.md
git commit -m "revise roadmap for datasource-first contour"
```

