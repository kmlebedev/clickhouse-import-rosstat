### Task 5: Репозиторий плагина gold-nav

**Files:**
- Create: `../gold-nav/kimi.plugin.json`
- Create: `../gold-nav/README.md`
- Create: `../gold-nav/skills/gold-forecast-session/SKILL.md`
- Create: `../gold-nav/skills/dcf-methodology/SKILL.md`
- Create: `../gold-nav/commands/session.md`, `../gold-nav/commands/verify.md`

(Каталог-sibling `/Users/whitefox/GolandProjects/gold-nav`; публикацию на GitHub делает пользователь.)

**Interfaces:**
- Consumes: витрины Task 4 (`v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`); ingest-контракты Task 3 (`POST /v1/model_run`, `/v1/manual_series`, Bearer-токен); формат manifest'а из официальной документации плагинов Kimi.
- Produces: устанавливаемый плагин; команды `/gold-nav:session`, `/gold-nav:verify`.

- [ ] **Step 1: `kimi.plugin.json`**

```json
{
  "name": "gold-nav",
  "version": "0.1.0",
  "description": "Scenario gold forecast to DCF NAV of Polyus (PLZL); reads RF macro via hosted ClickHouse MCP",
  "skills": "./skills/",
  "commands": "./commands/",
  "mcpServers": {
    "clickhouse": {
      "url": "https://PLACEHOLDER_HOST/mcp",
      "headers": { "Authorization": "Bearer ${GOLD_NAV_MCP_TOKEN}" }
    }
  },
  "interface": { "displayName": "Gold NAV Forecast", "shortDescription": "Gold scenario forecast to PLZL NAV" }
}
```

- [ ] **Step 2: `skills/gold-forecast-session/SKILL.md`**

6 шагов спеки §6: (1) `v_model_inputs` + `v_gold_dashboard` через MCP; (2) мировые входы — две ветки (Kimi Code: kimi-datasource `GC=F`/`DX-Y.NYB`/`HO=F`, при верификации `fred_query` с `realtime_end`; Kimi Work: плагины Global Finance Data/IMF, тикеры те же); (3) `v_events_calendar` ближайшие события и пороги; (4) пересчёт bull/base/bear, нормировка к 100%; (5) NAV (до фазы 3 — упрощённый AISC×production, зафиксировать в comment run'а); (6) POST JSON на ingest (клиентский `run_id` = UUID сессии), markdown-выжимка локально. Формат записи прогноза — фиксированный JSON-шаблон (поля `ModelRun` Task 3).

- [ ] **Step 3: `skills/dcf-methodology/SKILL.md` + `commands/*.md`**

Методология rev.2 из спеки/ARCHITECTURE §6.4 (sum-of-parts, НДПИ `10%×max(gold−1900,0)`, двухконтурная ставка 5% real USD + ОФЗ, price decks, пороги триггеров из статьи). `session.md` — frontmatter description + вызов сценария; `verify.md` — чтение `forecast_log`, сверка predicted vs actual, отчёт об ошибках.

- [ ] **Step 4: README.md**

Установка: Kimi Code — `/plugins install <github-url>`; Kimi Work — Plugin Builder → регистрация в personal market (`kimi-daimon kimi-plugin register-personal`); настройка `GOLD_NAV_MCP_TOKEN` (выдаёт автор); явно описать обход, если env-подстановка в `mcpServers.headers` не сработает (Review Focus #4): отредактировать установленную копию манифеста.

- [ ] **Step 5: Локальная регистрация и проверка в Kimi Work**

`kimi-daimon kimi-plugin register-personal ../gold-nav` (путь из SKILL plugin-builder); в Kimi Work «Плагины → Персональный» плагин виден и включается. Зафиксировать результат (в т.ч. работоспособность `${GOLD_NAV_MCP_TOKEN}`) в README.

- [ ] **Step 6: Commit**

```bash
cd ../gold-nav && git init && git add -A && git commit -m "initial gold-nav plugin"
```

