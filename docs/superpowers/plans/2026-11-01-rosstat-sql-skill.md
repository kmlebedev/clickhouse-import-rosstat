# Скил `rosstat-sql` в плагине `rosstat-mcp` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Выпустить `rosstat-mcp` 0.4.0: MCP `rosstat-staging` плюс один компактный английский скил `rosstat-sql` о том, как находить ряды и писать оптимальные запросы ClickHouse к витринам; ролевые скилы этого репо ссылаются на него.

**Architecture:** Два репозитория, две ветки, два PR. В `kmlebedev/rosstat-mcp` (ветка `feat/rosstat-sql-skill`): `skills/rosstat-sql/SKILL.md`, версия 0.4.0 в манифестах, явное поле `skills` только у Codex и Kimi, контрактные тесты. В `clickhouse-import-rosstat` (ветка `feat/infra-5.11-rosstat-sql-skill`, worktree `.worktrees/rosstat-sql-skill`): ссылки из ролевых скилов, docs, роадмап. Факты скила (схема витрин, SQL-примеры) проверяются живьём через MCP до коммита.

**Tech Stack:** JSON-манифесты, Markdown, Python 3 `unittest` (stdlib), bash + curl к MCP (JSON-RPC), `make`.

**Spec:** `docs/superpowers/specs/2026-11-01-rosstat-sql-skill-design.md`

## Global Constraints

- Имя плагина и маркетплейсов — `rosstat-mcp`; версия — `0.4.0` во всех пяти манифестах и трёх маркетплейсах.
- LONG = «Read-only MCP connection to Rosstat/CBR data marts (v_* views, point-in-time layer) on the clickhouse-import-rosstat staging server, plus the rosstat-sql skill for writing efficient ClickHouse queries against them.»
- SHORT = «Read-only MCP to Rosstat/CBR data marts + SQL skill»
- Скил: `skills/rosstat-sql/SKILL.md`, `name: rosstat-sql`, `description` ≤ 300 символов, файл ≤ 6144 байт, текст по-английски, первое правило — отвечать пользователю по-русски (слово `Russian` в теле).
- `"skills": "./skills/"` — только в `.codex-plugin/plugin.json` и `kimi.plugin.json`; Claude, Cursor, Qwen — без поля (подхват по умолчанию).
- `agents`, `commands`, `hooks` — нигде.
- Репозиторий `rosstat-mcp` ≤ 65536 байт суммарно, allowlist + `skills/rosstat-sql/SKILL.md`.
- Токен `ROSSTAT_MCP_TOKEN` — только из env через `make`; значение не печатается и не попадает в файлы.
- Коммиты — по-английски, кратко, с трейлером `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Агент отвечает по-английски** при английском скиле — правило про русский должно стоять первым и в повелительной форме; проверяется в живой приёмке (Task 4).
2. **Пример SQL в скиле не выполняется** (опечатка колонки, витрина без гранта) — каждый SQL-блок прогоняется `sqlcheck.sh` (Task 2), тест запрещает таблицы не из `v_*`/`system.*`.
3. **Двойная загрузка скила в Claude Code** при явном поле `skills` — поле у Claude не объявляется; тест это фиксирует (Task 1).
4. **Скил распух** при правках — тест на 6144 байт и 300 символов description (Task 2).
5. **Старая ссылка в ролевых скилах** на правила, которых нет в `rosstat-sql` — Task 3 сверяет каждую ссылку с разделом скила.

---

### Task 1: Манифесты 0.4.0 и правила компонентов

**Files (репо `/Users/whitefox/GolandProjects/rosstat-mcp`, ветка `feat/rosstat-sql-skill` от `main`):**
- Modify: `tests/test_manifests.py`
- Modify: все пять манифестов, три маркетплейса

**Interfaces:**
- Produces: константы теста `VERSION = "0.4.0"`, новые `LONG`/`SHORT`; `SKILLS_DECLARED = {".codex-plugin/plugin.json", "kimi.plugin.json"}`, `SKILLS_PATH = "./skills/"`.

- [ ] **Step 1: Тест.** В `tests/test_manifests.py`: `VERSION`, `LONG`, `SHORT` — значения из Global Constraints; `COMPONENT_KEYS = ("agents", "commands", "hooks")`; `test_no_component_fields` → `test_components`: для каждого манифеста нет ключей `COMPONENT_KEYS`; `data.get("skills") == SKILLS_PATH` если `rel in SKILLS_DECLARED`, иначе `"skills" not in data`.
- [ ] **Step 2: RED.** `python3 -m unittest discover -s tests -v` → FAIL в `test_names_versions_descriptions` и `test_components`.
- [ ] **Step 3: Манифесты.** Версия, описания (длинное и короткое — где они есть сейчас), `"skills": "./skills/"` в Codex и Kimi (рядом с `mcpServers`).
- [ ] **Step 4: Проверка.** Тесты → 7/7 OK (существование `skills/` проверяет `test_skill` из Task 2); `claude plugin validate .` → без ошибок.
- [ ] **Step 5: Commit** `bump to 0.4.0, declare skills for codex and kimi`.

### Task 2: Скил `rosstat-sql`

**Files:**
- Create: `skills/rosstat-sql/SKILL.md`
- Modify: `tests/test_manifests.py`
- Scratch (не коммитится): `<scratchpad>/sqlcheck.sh`, `<scratchpad>/sqlcheck.mk`

**Interfaces:**
- Consumes: `TRACKED_FILES`, `MAX_BYTES` из теста.
- Produces: `SKILL_REL = "skills/rosstat-sql/SKILL.md"`, функция `skill_tables(sql: str) -> set[str]` (имена после `FROM`/`JOIN`, без табличной функции-аргументов).

- [ ] **Step 1: Разведка схемы через MCP.** Скрипт `sqlcheck.sh` — на основе `<scratchpad>/accept.sh` (initialize → tools/call `run_query`), но принимает файл: вытаскивает блоки ```` ```sql ```` и выполняет каждый, печатает `OK`/`ERROR` и первые 200 символов ответа, exit 1 при любом `ERROR`. Запуск: `make -f Makefile -f <scratchpad>/sqlcheck.mk sqlcheck FILE=<path>` (из основного worktree, где Makefile подключает env). Сначала прогнать probe-файл: `DESCRIBE v_series_catalog`; `DESCRIBE v_point_in_time`; `SELECT name, comment FROM system.tables WHERE database='default' AND name LIKE 'v\\_%' LIMIT 5`; `SELECT count() FROM system.columns WHERE database='default' AND table='v_series_catalog'`; пример дублей: `SELECT series, date, count() c FROM v_rosstat_macro GROUP BY series, date HAVING c > 1 LIMIT 3`. Expected: все OK; зафиксировать реальные имена колонок.
- [ ] **Step 2: Тест.** `test_skill`: `SKILL_REL` существует; в `skills/` ровно один подкаталог `rosstat-sql`; frontmatter (между первыми `---`) содержит `name: rosstat-sql` и `description:` длиной 1..300; размер файла ≤ 6144; в теле есть `Russian`; для каждого ```` ```sql ```` блока `skill_tables` ⊆ имена с префиксом `v_` или `system.`; `secret_problems` по строкам скила — нет литеральных `Bearer <...>`/`password=`. В `TRACKED_FILES` — `SKILL_REL`.
- [ ] **Step 3: RED.** Тесты → FAIL `test_skill` (файла нет).
- [ ] **Step 4: Текст скила** (английский, императивные пункты, разделы по спеке §2.1): 1 Answer in Russian; 2 Access; 3 Find data; 4 Query rules; 5 Data traps; 6 Point-in-time; 7 Templates (четыре запроса: каталог, последнее значение + свежесть, месячная динамика с г/г оконной функцией, срез `as_of` с ревизией `unemployment_sa`). Только колонки, подтверждённые Step 1.
- [ ] **Step 5: Живая проверка SQL.** `sqlcheck FILE=skills/rosstat-sql/SKILL.md` → все блоки OK; срез ПИТ возвращает 2.885274 / 2.916127.
- [ ] **Step 6: GREEN.** Тесты → 8/8 OK; `wc -c` скила ≤ 6144.
- [ ] **Step 7: Commit** `add rosstat-sql skill`.

### Task 3: Документация обоих репо

**Files:**
- `rosstat-mcp`: Modify `README.md` (что даёт плагин: MCP + скил; как вызвать скил в каждом клиенте; раздел «Разработка» — скил ≤ 6 КБ, английский).
- Этот репо: Modify `.kimi-code/skills/dashboard-migration/SKILL.md` (перед памяткой — строка-ссылка на скил `rosstat-sql` как источник общих правил чтения витрин), `.kimi-code/skills/editor-in-chief/SKILL.md` (шаги 3–4 — ссылка), `ARCHITECTURE.md` §6.5 и §2 (абзац о плагине), `README.md` (раздел плагина), `AGENTS.md` (абзац «Плагин:»), `docs/ROADMAP_INFRA.md` (5.10 → ✅ Done 2026-11-01; строка 5.11 🟡 In Progress; «Границы фазы»).

- [ ] **Step 1:** Правки README `rosstat-mcp`; тесты → OK (размер в бюджете); commit `document rosstat-sql skill`.
- [ ] **Step 2:** Правки этого репо. Каждая ссылка из ролевых скилов называет раздел `rosstat-sql`, который реально есть в Task 2.
- [ ] **Step 3: Проверка.** `grep -rn "без скилов\|no skills" README.md AGENTS.md ARCHITECTURE.md` → только исторические строки 5.10; `make all > <scratchpad>/make-all-511.log 2>&1; echo $?` → 0; `make docs-check` → OK.
- [ ] **Step 4: Commit** `link roles to rosstat-sql skill, roadmap 5.11`.

### Task 4: Живая приёмка и PR

- [ ] **Step 1:** Push `feat/rosstat-sql-skill`, PR в `kmlebedev/rosstat-mcp`; CI → зелёный.
- [ ] **Step 2: Claude Code**, изолированный `CLAUDE_CONFIG_DIR=<scratchpad>/accept511`: `claude plugin marketplace add kmlebedev/rosstat-mcp@feat/rosstat-sql-skill` (или локальный путь ветки), `claude plugin install rosstat-mcp@rosstat-mcp`; `du -sh` кэша и маркетплейса → десятки КБ; `claude plugin validate .` без ошибок; скил `rosstat-sql` присутствует в кэше; `claude mcp list` через `make` → `✔ Connected`.
- [ ] **Step 3: Ответ по-русски.** Если `claude -p` залогинен: `claude -p "Какое последнее значение безработицы SA в point-in-time слое?"` в изолированном конфиге с токеном → ответ по-русски, запрос через `rosstat-staging`. Если не залогинен — «не проверено» с причиной.
- [ ] **Step 4:** Codex/Kimi/Qwen/Cursor — если установлены; иначе «не проверено».
- [ ] **Step 5:** Push ветки этого репо, PR с `Closes #503` (вне backticks), источник, порядок мержа (сначала `rosstat-mcp`), протокол приёмки комментарием в PR; Project → In Review, поля `PR`.
