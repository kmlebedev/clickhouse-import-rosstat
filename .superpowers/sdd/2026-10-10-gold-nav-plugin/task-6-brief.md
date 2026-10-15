### Task 6: Синхронизация ARCHITECTURE.md и README.md

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`

**Interfaces:**
- Consumes: результаты Task 1–5; skill `sync-readme-architecture`.
- Produces: §6.1 (статусы импортёров), §6.2 (без изменений DDL), §6.3 (три новые витрины), §6.5 (HTTP-режим MCP + ingest-endpoint + пакет `ingest/`), новый подраздел про `gold-nav`; README — блок подключения плагина.

- [ ] **Step 1: Внести правки по skill `sync-readme-architecture`**

Вызвать `Skill(sync-readme-architecture)` и следовать его чек-листу: структура репозитория (пакеты `calendar/`, `views/`, `ingest/`, `cmd/ingest/`), таблица §6.1 (статусы + datasource-пометки), §6.3 (v_model_inputs, v_gold_dashboard, v_events_calendar), §6.5 (ingest-контракты и токены без значений), ссылка на спеку и плагин.

- [ ] **Step 2: Commit**

```bash
git add ARCHITECTURE.md README.md
git commit -m "sync architecture and readme with gold-nav contour"
```

