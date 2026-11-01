# MCP-only плагин в отдельном репозитории (`infra/5.10`) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** публичный репозиторий `kmlebedev/rosstat-mcp` с плагином `rosstat-mcp` 0.3.0, который подключает только MCP `rosstat-staging` и ставится десятками КБ во всех пяти клиентах; из `clickhouse-import-rosstat` манифесты плагина и их тулинг удаляются.

**Architecture:** весь новый репо — это плагин: манифесты пяти клиентов в корне, маркетплейсы ссылаются на корень. Контракт держит `tests/test_manifests.py` (stdlib unittest) в CI нового репо. В основном репо остаются ролевые скилы для работы в нём и ссылка на новый репо в документации.

**Tech Stack:** JSON-манифесты, Python 3.9+ stdlib `unittest`, GitHub Actions, `gh`, Makefile.

**Spec:** [docs/superpowers/specs/2026-11-01-mcp-only-plugin-design.md](../specs/2026-11-01-mcp-only-plugin-design.md)

## Global Constraints

- Новый репо: `kmlebedev/rosstat-mcp`, публичный, MIT, ветка по умолчанию `main`; локальный клон — `/Users/whitefox/GolandProjects/rosstat-mcp`.
- Имя плагина и всех маркетплейсов — `rosstat-mcp`; версия — `0.3.0`.
- Длинное описание: `Read-only MCP connection to Rosstat/CBR data marts (v_* views, point-in-time layer) on the clickhouse-import-rosstat staging server.`; короткое: `Read-only MCP to Rosstat/CBR data marts`.
- MCP: сервер `rosstat-staging`, URL `https://mcp.selfinvesting.ru:8443/mcp`, токен только ссылкой на `ROSSTAT_MCP_TOKEN` (Claude/Qwen/Cursor — `Authorization: Bearer ${ROSSTAT_MCP_TOKEN}`, Kimi — `bearerTokenEnvVar`, Codex — `bearer_token_env_var`; Cursor — плюс `variables.ROSSTAT_MCP_TOKEN` в `required`).
- В манифестах нет ключей `skills`, `agents`, `commands`, `hooks`; сервера `clickhouse` нет.
- Источники маркетплейсов: Claude `"./"`, Cursor `"."`, Codex `{"source": "url", "url": "https://github.com/kmlebedev/rosstat-mcp.git", "ref": "main"}`.
- Состав нового репо — ровно 16 файлов спеки §3.1, суммарно ≤ 65536 байт.
- Секреты не печатаются; токен для приёмки — только через `make` основного репо (env-файл); GitHub-вызовы — `gh`; коммиты — по-английски, кратко, с trailer `Co-Authored-By`.

## Review Focus

1. **Относительный `mcpServers` Codex/Cursor** (`./.codex-plugin/mcp.json`) должен указывать на существующий файл внутри репо — Task 1, `test_mcp_server_contract`.
2. **Литеральный токен** в любом JSON (`"Authorization": "Bearer abc…"`, ключ `token`) — Task 1, `test_no_secrets`.
3. **Расхождение версии** между девятью манифестами после ручной правки (синхронизатора нет) — Task 1, `test_names_versions_descriptions`.
4. **Посторонний файл** в репо (`.DS_Store`, скопированный скил) уезжает к каждому пользователю — Task 1, `test_tracked_files_allowlist` + `.gitignore`.
5. **Старый маркетплейс у пользователей** перестанет обновляться после мержа в основном репо — Task 1 README (миграция), Task 2 README основного репо.

---

### Task 1: Репозиторий `kmlebedev/rosstat-mcp`

**Files (в новом репо):**
- Create: `.claude-plugin/plugin.json`, `.claude-plugin/marketplace.json`, `.codex-plugin/plugin.json`, `.codex-plugin/mcp.json`, `.agents/plugins/marketplace.json`, `.cursor-plugin/plugin.json`, `.cursor-plugin/marketplace.json`, `.cursor-plugin/mcp.json`, `kimi.plugin.json`, `qwen-extension.json`, `README.md`, `LICENSE`, `.gitignore`, `tests/test_manifests.py`, `.github/workflows/check.yml`

**Interfaces:**
- Produces: репо `kmlebedev/rosstat-mcp` с `main` = версия 0.3.0; Task 2 ссылается на него URL `https://github.com/kmlebedev/rosstat-mcp`; Task 3 ставит из него.

- [ ] **Step 1: Создать репо и клон**

```bash
gh repo create kmlebedev/rosstat-mcp --public --license mit \
  --description "Read-only MCP to Rosstat/CBR data marts — plugin for Claude Code, Codex, Cursor, Kimi Code and Qwen Code" \
  --clone=false
git clone https://github.com/kmlebedev/rosstat-mcp.git /Users/whitefox/GolandProjects/rosstat-mcp
git -C /Users/whitefox/GolandProjects/rosstat-mcp switch -c feat/initial-plugin
```

Expected: в `main` один коммит с `LICENSE`.

- [ ] **Step 2: Написать `tests/test_manifests.py`**

Константы из Global Constraints; корень репо — `Path(__file__).resolve().parent.parent`. Тесты (`unittest.TestCase`):

- `test_names_versions_descriptions` — `name == "rosstat-mcp"` у пяти манифестов плагина, трёх маркетплейсов и их `plugins[0].name`; `version == "0.3.0"` у манифестов плагина и `.claude-plugin/marketplace.json`; длинное описание — у манифестов плагина, `plugins[0].description` маркетплейсов Claude/Cursor и `interface.longDescription` Codex/Kimi; короткое — `description` маркетплейса Claude, `metadata.description` Cursor, `interface.shortDescription` Codex/Kimi.
- `test_no_component_fields` — нет ключей `skills`, `agents`, `commands`, `hooks` в манифестах плагина.
- `test_mcp_server_contract` — для каждого манифеста плагина: строковый `mcpServers` разрешается от корня репо, файл существует и не выходит за корень; сервер `rosstat-staging` есть, `clickhouse` нет; `url`/`httpUrl` == URL; токен — `headers.Authorization == "Bearer ${ROSSTAT_MCP_TOKEN}"` или `bearerTokenEnvVar`/`bearer_token_env_var == "ROSSTAT_MCP_TOKEN"`; Cursor `variables.properties` и `variables.required` содержат `ROSSTAT_MCP_TOKEN`.
- `test_no_secrets` — обход всех JSON: ключи по `(?i)(api[_-]?key|token|password|secret)` запрещены, кроме `bearerTokenEnvVar`/`bearer_token_env_var`/`ROSSTAT_MCP_TOKEN` со значением `ROSSTAT_MCP_TOKEN`; строки `Bearer …` только вида `Bearer ${ROSSTAT_MCP_TOKEN}`.
- `test_marketplace_sources` — источники из Global Constraints.
- `test_tracked_files_allowlist` — `git ls-files` == 16 файлов из Files; сумма размеров ≤ 65536.

- [ ] **Step 3: Убедиться, что тесты падают**

Run: `python3 -m unittest discover -s tests -v`
Expected: FAIL / ERROR — файлов манифестов нет.

- [ ] **Step 4: Создать манифесты, README, `.gitignore`, CI**

Манифесты — из `git show 27df17f:<path>` основного репо (версия 0.2.0) с правками по Global Constraints: `name`/`version`/описания, удалить `skills`/`agents`, `displayName` — `Rosstat MCP`, `keywords`/`tags` — `rosstat`, `cbr`, `clickhouse`, `mcp`, `macro`, `homepage`/`repository` — `https://github.com/kmlebedev/rosstat-mcp`. README — что даёт плагин (только MCP, витрины `v_*`, read-only, staging), токен `ROSSTAT_MCP_TOKEN` (выдаёт владелец), таблица установки спеки §3.2, миграция §3.4, проверка `claude mcp list` → `✔ Connected`, ссылка на основной репо. `check.yml` — `on: [push, pull_request]`, `actions/checkout@v4`, `actions/setup-python@v5` (3.12), `python3 -m unittest discover -s tests -v`.

- [ ] **Step 5: Прогнать тесты и валидатор**

Run: `python3 -m unittest discover -s tests -v` → OK (6 тестов) — после `git add -A`, иначе allowlist по `git ls-files` не увидит файлы.
Run: `claude plugin validate .` → `✔ Validation passed`, без warnings.

- [ ] **Step 6: Commit, push, PR**

```bash
git add -A
git commit -m "add rosstat-mcp plugin manifests 0.3.0"
git push -u origin feat/initial-plugin
gh pr create --repo kmlebedev/rosstat-mcp --title "rosstat-mcp 0.3.0: MCP-only plugin" --body "<состав, установка, ссылка на kmlebedev/clickhouse-import-rosstat#474>"
```

Expected: CI `check` зелёный на PR.

---

### Task 2: Чистка основного репозитория и документация

**Files (в worktree `.worktrees/mcp-only-plugin`):**
- Delete (`git rm -r`): `kimi.plugin.json`, `qwen-extension.json`, `.claude-plugin/`, `.codex-plugin/`, `.cursor-plugin/`, `.agents/plugins/marketplace.json`, `plugin_manifest_test.go`, `scripts/plugin_manifests.py`, `scripts/plugin_manifests_test.py`, `.github/workflows/sync-manifests.yml`
- Modify: `Makefile` (удалить цели `plugin-manifests-check`/`plugin-manifests-sync`, их комментарий и записи в `.PHONY`), `README.md`, `AGENTS.md`, `ARCHITECTURE.md` (§2 раскладка, §6.5 «Плагин репозитория → MCP», конвенция 17), `docs/ROADMAP_INFRA.md` (строка `5.10`)

**Interfaces:**
- Consumes: URL `https://github.com/kmlebedev/rosstat-mcp` (Task 1).

- [ ] **Step 1: Удалить файлы и цели Makefile**

После удаления: `.agents/` пуст — удалить каталог целиком, если в нём больше ничего нет.

- [ ] **Step 2: Проверить отсутствие ссылок**

Run: `grep -rn -e plugin_manifests -e plugin-manifests -e sync-manifests -e "kimi.plugin.json" -e "qwen-extension.json" -e ".claude-plugin" -e ".codex-plugin" -e ".cursor-plugin" --include=*.md --include=*.yml --include=Makefile --include=*.go --include=*.py . | grep -v -e docs/superpowers -e "docs/ROADMAP_INFRA.md" -e "^./.worktrees"`
Expected: только строки README/AGENTS/ARCHITECTURE, которые Step 3 переписывает; после Step 3 — пусто.

- [ ] **Step 3: Документация**

- README, раздел «Подключение как плагин LLM-клиента»: плагин — отдельный репо `kmlebedev/rosstat-mcp`, даёт только MCP `rosstat-staging`; таблица установки спеки §3.2; токен `ROSSTAT_MCP_TOKEN`; скилы `.kimi-code/skills/` — для сессий в этом репо, плагином не публикуются; локальный dev-MCP — `.kimi-code/mcp.json`. Удалить фразы про ~86 МБ, namespace скилов, агента `coder` и синхронизацию манифестов.
- `AGENTS.md`, абзац «Плагин:»: плагин публикует только MCP и живёт в `kmlebedev/rosstat-mcp`; скилы — источник `.kimi-code/skills/` для работы в репо.
- `ARCHITECTURE.md`: §2 — убрать строки манифестов из раскладки; §6.5 — манифесты и их инварианты живут в `kmlebedev/rosstat-mcp` (`tests/test_manifests.py`), MCP-контракт прежний; конвенция 17 — переписать: скилы в `.kimi-code/skills/` для сессий в репо, LLM-плагин MCP-only в отдельном репо.
- `docs/ROADMAP_INFRA.md`: строка `5.10` — содержание по спеке, статус `🔶 In Progress`, критерий приёмки из #474.

- [ ] **Step 4: Проверки**

Run: `make all` → exit 0. Run: `make docs-check` → exit 0. Run: grep из Step 2 → пусто.

- [ ] **Step 5: Commit**

```bash
git add -A Makefile README.md AGENTS.md ARCHITECTURE.md docs/ROADMAP_INFRA.md
git commit -m "infra 5.10: drop plugin manifests, point to kmlebedev/rosstat-mcp"
```

---

### Task 3: Живая приёмка и PR

**Files:**
- Modify: `docs/ROADMAP_INFRA.md` (статус и протокол `5.10`)

- [ ] **Step 1: Claude Code из ветки нового репо** — изолированный `CLAUDE_CONFIG_DIR` в scratchpad, `CLAUDE_CODE_PLUGIN_PREFER_HTTPS=1`: `claude plugin marketplace add kmlebedev/rosstat-mcp#feat/initial-plugin`, `claude plugin install rosstat-mcp@rosstat-mcp`; `du -sh plugins/marketplaces plugins/cache`; `find plugins/cache -type f` — только файлы Task 1; через `make` основного репо (токен из env-файла, не печатается) `claude mcp list` → `plugin:rosstat-mcp:rosstat-staging … ✔ Connected`.
- [ ] **Step 2: Прочие клиенты** — Kimi (`~/.kimi-code/bin/kimi`): установка в изолированный домашний каталог, если CLI это позволяет неинтерактивно; Codex, Qwen, Cursor — если установлены; иначе «не проверено».
- [ ] **Step 3: Роадмап** — строка `5.10`: `🟢 In Review` с размерами и результатами шагов 1–2; commit `infra 5.10: record live acceptance`.
- [ ] **Step 4: PR основного репо** — `git push -u origin feat/infra-5.10-mcp-only-plugin`; `gh pr create` с `Closes #474` вне backticks и ссылкой на PR нового репо; протокол приёмки — комментарием в PR; Project V2 → `In Review`, поле `PR`.

Мерж обоих PR — решение владельца; порядок: сначала `kmlebedev/rosstat-mcp`, затем основной репо.
