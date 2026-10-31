# MCP-only плагин (`infra/5.10`) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** плагин `rosstat-mcp` 0.3.0 в `plugins/rosstat-mcp/` подключает только MCP `rosstat-staging` и ставится килобайтами вместо 214 МБ.

**Architecture:** все манифесты клиентов переезжают в подкаталог; Claude Code берёт маркетплейс по URL и плагин через `git-subdir`, Codex — sparse-клон, Kimi/Qwen — zip из GitHub Release, Cursor — маркетплейс в корне. Контракт держат `plugin_manifest_test.go` (Go) и `scripts/plugin_manifests.py` (синхронизация метаданных + сборка zip).

**Tech Stack:** JSON-манифесты, Go `testing`, Python 3.9+ stdlib (`zipfile`, `unittest`), GitHub Actions, Makefile.

**Spec:** [docs/superpowers/specs/2026-11-01-mcp-only-plugin-design.md](../specs/2026-11-01-mcp-only-plugin-design.md)

## Global Constraints

- Каталог плагина: `plugins/rosstat-mcp/`; имя плагина и маркетплейсов — `rosstat-mcp`; версия — `0.3.0`.
- MCP: сервер `rosstat-staging`, URL `https://mcp.selfinvesting.ru:8443/mcp`, токен только ссылкой на `ROSSTAT_MCP_TOKEN` (Claude/Qwen/Cursor — `Authorization: Bearer ${ROSSTAT_MCP_TOKEN}`, Kimi — `bearerTokenEnvVar`, Codex — `bearer_token_env_var`; Cursor дополнительно `variables.ROSSTAT_MCP_TOKEN`, required).
- В манифестах нет ключей `skills`, `agents`, `commands`, `hooks`.
- Каталог плагина — только файлы спеки §4.1, суммарно ≤ 64 КБ (65536 байт).
- Длинное описание: `Read-only MCP connection to Rosstat/CBR data marts (v_* views, point-in-time layer) on the clickhouse-import-rosstat staging server.`; короткое: `Read-only MCP to Rosstat/CBR data marts`.
- Источник плагина в маркетплейсе Claude: `{"source": "git-subdir", "url": "https://github.com/kmlebedev/clickhouse-import-rosstat.git", "path": "plugins/rosstat-mcp", "ref": "main"}`; `version` в записи маркетплейса не дублируется (её держит `plugin.json`).
- Тег релиза zip — `rosstat-mcp-v<version>`, ассет — `rosstat-mcp.zip`, релиз `--latest=false`; тег ставит владелец, ассистент тег не пушит.
- Секреты не печатаются; GitHub-вызовы — `gh`; коммиты — по-английски, кратко, с trailer `Co-Authored-By`.

## Review Focus

1. **Относительные пути MCP-конфигов Codex/Cursor** (`"mcpServers": "./.codex-plugin/mcp.json"`) разрешаются от корня плагина, а не репо: тест обязан искать файл в `plugins/rosstat-mcp/` — Task 1, `TestLLMPluginRemoteMCP`.
2. **Посторонний файл в каталоге плагина** (`.DS_Store`, скопированный скил) молча попадёт в zip и в кэш клиента — Task 1, `TestLLMPluginDirAllowlist`; Task 2, `test_zip_contains_exactly_plugin_files`.
3. **Недетерминированный zip** (mtime, порядок) меняет SHA-256 при пересборке того же коммита — Task 2, `test_zip_is_reproducible`.
4. **Тег не совпал с версией** (`rosstat-mcp-v0.3.1` при `version: 0.3.0`) выпустит релиз со старыми манифестами — Task 3, шаг проверки версии в workflow.
5. **Забытый корневой манифест** (`kimi.plugin.json` остался) — клиент по-прежнему ставит весь репо со скилами — Task 1, `TestLLMPluginRootHasNoPluginManifests`.

---

### Task 1: Каталог плагина, чистка корня и Go-контракт

**Files:**
- Create: `plugins/rosstat-mcp/.claude-plugin/plugin.json`, `plugins/rosstat-mcp/.claude-plugin/marketplace.json`, `plugins/rosstat-mcp/.codex-plugin/plugin.json`, `plugins/rosstat-mcp/.codex-plugin/mcp.json`, `plugins/rosstat-mcp/.cursor-plugin/plugin.json`, `plugins/rosstat-mcp/.cursor-plugin/mcp.json`, `plugins/rosstat-mcp/kimi.plugin.json`, `plugins/rosstat-mcp/qwen-extension.json`, `plugins/rosstat-mcp/README.md` (заглушка в одну строку; содержание — Task 4)
- Delete (`git rm`): `kimi.plugin.json`, `qwen-extension.json`, `.claude-plugin/`, `.codex-plugin/`, `.cursor-plugin/plugin.json`, `.cursor-plugin/mcp.json`
- Modify: `.agents/plugins/marketplace.json`, `.cursor-plugin/marketplace.json`
- Test: `plugin_manifest_test.go` (переписать)

**Interfaces:**
- Produces: файлы каталога плагина и константа `pluginDir = "plugins/rosstat-mcp"` в тесте; Task 2 читает канон из `plugins/rosstat-mcp/.claude-plugin/{plugin,marketplace}.json`.

- [ ] **Step 1: Переписать `plugin_manifest_test.go` под новый контракт**

Константы: `pluginDir = "plugins/rosstat-mcp"`, `pluginName = "rosstat-mcp"`, `pluginVersion = "0.3.0"`, `pluginMaxBytes = 65536`, MCP-константы без изменений. Пути манифестов — через `filepath.Join(pluginDir, …)`. Удалить `assertSkillDir`, проверку `skills`/`agents` и `license`.

Тесты и утверждения:

- `TestLLMPluginManifests` — для 5 манифестов `name == "rosstat-mcp"`, `version == "0.3.0"`; в сыром JSON нет ключей `skills`, `agents`, `commands`, `hooks`.
- `TestLLMPluginDirAllowlist` — множество файлов под `pluginDir` (относительные пути, `filepath.WalkDir`) **равно** 9 файлам из Files выше; сумма размеров ≤ `pluginMaxBytes`.
- `TestLLMPluginRootHasNoPluginManifests` — `os.Stat` возвращает `fs.ErrNotExist` для `kimi.plugin.json`, `qwen-extension.json`, `.claude-plugin`, `.codex-plugin`, `.cursor-plugin/plugin.json`, `.cursor-plugin/mcp.json`.
- `TestLLMPluginMarketplaces`:
  - `plugins/rosstat-mcp/.claude-plugin/marketplace.json`: `name == "rosstat-mcp"`, одна запись `rosstat-mcp`, `source` == объект из Global Constraints, у записи нет `version`;
  - `.agents/plugins/marketplace.json`: `name == "rosstat-mcp"`, `source == {"source":"local","path":"./plugins/rosstat-mcp"}`;
  - `.cursor-plugin/marketplace.json`: `name == "rosstat-mcp"`, `source == "plugins/rosstat-mcp"`.
- `TestLLMPluginManifestsNoSecrets` — по всем JSON под `pluginDir` и двум корневым маркетплейсам; `walkJSON` без изменений.
- `TestLLMPluginRemoteMCP` — как раньше, но строковый `mcpServers` разрешается от `pluginDir`: `filepath.Join(pluginDir, ref)`; проверка, что путь не выходит за `pluginDir`. Подтест Cursor читает `plugins/rosstat-mcp/.cursor-plugin/plugin.json`.

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test -run 'TestLLMPlugin' -v .`
Expected: FAIL — `read plugins/rosstat-mcp/...: no such file or directory` и `kimi.plugin.json exists`.

- [ ] **Step 3: Создать каталог плагина и удалить корневые манифесты**

Содержимое — из текущих корневых манифестов (`git show HEAD:<path>`) с правками: `name`/`version`/описания — по Global Constraints; поля `skills`/`agents` удалить; `keywords`/`tags` — `rosstat`, `cbr`, `clickhouse`, `mcp`, `macro`; `displayName` — `Rosstat MCP`; Codex `interface.capabilities` — `["Read"]`, `category` — `Data`. Claude `plugin.json` — `mcpServers` inline (как в 0.2.0). Claude `marketplace.json` — `$schema`, `name`, `version`, `description` (короткое), `owner`, одна запись с `description` (длинное) и `source` из Global Constraints. MCP-файлы Codex/Cursor копируются без изменений. `git rm -r` корневых путей из Files; корневые маркетплейсы Codex/Cursor правятся по Step 1.

- [ ] **Step 4: Прогнать тесты и валидатор**

Run: `go test -run 'TestLLMPlugin' -v .` → PASS.
Run: `claude plugin validate plugins/rosstat-mcp` → `✔ Validation passed` без warnings о версии.

- [ ] **Step 5: Commit**

```bash
git add -A plugins/rosstat-mcp plugin_manifest_test.go .agents/plugins/marketplace.json .cursor-plugin/marketplace.json
git commit -m "infra 5.10: move plugin to plugins/rosstat-mcp as MCP-only 0.3.0"
```

(`git rm` из Step 3 уже в индексе.)

---

### Task 2: Синхронизация метаданных и сборка zip

**Files:**
- Modify: `scripts/plugin_manifests.py`, `scripts/plugin_manifests_test.py`, `Makefile` (цель `plugin-zip`, `.PHONY`), `.github/workflows/sync-manifests.yml`

**Interfaces:**
- Consumes: каталог `plugins/rosstat-mcp/` из Task 1.
- Produces: `PLUGIN_DIR = "plugins/rosstat-mcp"`; `build_zip(root: Path, dest: Path) -> list[str]` (возвращает имена в архиве); CLI `--zip PATH` (взаимоисключающе с `--check`/`--write`); `make plugin-zip` → `build/rosstat-mcp.zip`. Task 3 вызывает `python3 scripts/plugin_manifests.py --zip build/rosstat-mcp.zip`.

- [ ] **Step 1: Обновить тесты**

`plugin_manifests_test.py`: фикстуры `base_manifests()` — на новых путях констант (`pm.CLAUDE_PLUGIN` и т. д.), `NAME = "rosstat-mcp"`; из ожиданий синхронизации убрать `plugins.0.version` Claude-маркетплейса. Новые тесты:

- `test_zip_contains_exactly_plugin_files` — во временном корне создать `plugins/rosstat-mcp/{a.json,.claude-plugin/plugin.json}` и файл вне каталога; `build_zip` → `namelist() == [".claude-plugin/plugin.json", "a.json"]` (отсортировано, без префикса `plugins/rosstat-mcp/`, без файла вне каталога и без записей-каталогов).
- `test_zip_is_reproducible` — две сборки в разные пути, `os.utime` между ними → байты равны.
- `test_zip_missing_plugin_dir_fails` — нет каталога → `ManifestError`.

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `python3 scripts/plugin_manifests_test.py -v`
Expected: FAIL — `AttributeError: module 'plugin_manifests' has no attribute 'build_zip'` и ошибки путей.

- [ ] **Step 3: Реализовать**

- Константы путей: `PLUGIN_DIR`; `CLAUDE_PLUGIN`, `CLAUDE_MARKETPLACE`, `CURSOR_PLUGIN`, `CODEX_PLUGIN`, `QWEN`, `KIMI` — под `PLUGIN_DIR`; `CURSOR_MARKETPLACE = ".cursor-plugin/marketplace.json"`, `CODEX_MARKETPLACE = ".agents/plugins/marketplace.json"` — в корне. Docstring модуля — о новом каноне.
- `updates()`: из Claude-маркетплейса убрать `plugins.0.version`; остальное без изменений.
- `build_zip`: обход `root / PLUGIN_DIR` (`rglob`, только файлы), имена — POSIX-пути относительно каталога, отсортированы; `zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))`, `external_attr = 0o644 << 16`, `ZIP_DEFLATED`; `dest.parent.mkdir(parents=True, exist_ok=True)`; нет каталога → `ManifestError`.
- `main`: `--zip` печатает `OK: <dest> (<n> files, <bytes> bytes)`.
- `Makefile`: `plugin-zip:` → `python3 scripts/plugin_manifests.py --zip build/rosstat-mcp.zip`; добавить в `.PHONY`; комментарий над целями плагина — о новом каноне.
- `sync-manifests.yml`: `paths` и `git add` — новые пути (`plugins/rosstat-mcp/**` целиком и два корневых маркетплейса).

- [ ] **Step 4: Прогнать проверки**

Run: `python3 scripts/plugin_manifests_test.py -v` → все OK.
Run: `make plugin-manifests-check` → `OK: plugin manifests in sync`.
Run: `make plugin-zip && unzip -l build/rosstat-mcp.zip` → 9 файлов, корень архива — `.claude-plugin/`, `kimi.plugin.json`…

- [ ] **Step 5: Commit**

```bash
git add scripts/plugin_manifests.py scripts/plugin_manifests_test.py Makefile .github/workflows/sync-manifests.yml
git commit -m "infra 5.10: sync manifests from plugin dir and build plugin zip"
```

---

### Task 3: Workflow релиза zip

**Files:**
- Create: `.github/workflows/rosstat-mcp-release.yml`

**Interfaces:**
- Consumes: `python3 scripts/plugin_manifests.py --check` и `--zip build/rosstat-mcp.zip` (Task 2).

- [ ] **Step 1: Написать workflow**

- `on: push: tags: ['rosstat-mcp-v*']` — единственный триггер.
- `permissions: contents: write`; `concurrency: rosstat-mcp-release`.
- Шаги: `actions/checkout@v4`; `actions/setup-python@v5` (3.12); проверка версии — `python3 -c` читает `version` из `plugins/rosstat-mcp/.claude-plugin/plugin.json` и падает, если `rosstat-mcp-v<version> != $GITHUB_REF_NAME`; `--check`; `python3 scripts/plugin_manifests_test.py`; `--zip build/rosstat-mcp.zip`; `sha256sum`; `gh release create "$GITHUB_REF_NAME" build/rosstat-mcp.zip --title "rosstat-mcp <version>" --notes "<SHA-256 и команды установки Kimi/Qwen>" --latest=false` с `GH_TOKEN: ${{ github.token }}`.

- [ ] **Step 2: Проверить синтаксис**

Run: `actionlint .github/workflows/rosstat-mcp-release.yml` (если установлен), иначе `python3 -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" .github/workflows/rosstat-mcp-release.yml`.
Expected: без ошибок. Шаг проверки версии прогнать локально с `GITHUB_REF_NAME=rosstat-mcp-v0.3.0` (exit 0) и `rosstat-mcp-v9.9.9` (exit ≠ 0).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/rosstat-mcp-release.yml
git commit -m "infra 5.10: release workflow for rosstat-mcp plugin zip"
```

---

### Task 4: Документация

**Files:**
- Modify: `plugins/rosstat-mcp/README.md`, `README.md` (раздел «Подключение как плагин LLM-клиента»), `AGENTS.md` (абзац «Плагин:»), `ARCHITECTURE.md` (§2 строки про манифесты, §6.5 абзац «Плагин репозитория → MCP», конвенция 17), `docs/ROADMAP_INFRA.md` (строка `5.10`, «Границы фазы»)

- [ ] **Step 1: README плагина** — что даёт плагин (только MCP `rosstat-staging`, витрины `v_*`, read-only), токен `ROSSTAT_MCP_TOKEN` (выдаёт владелец), таблица установки — команды спеки §4.3, миграция с `clickhouse-import-rosstat` (§4.6: `claude plugin marketplace remove clickhouse-import-rosstat`, `codex plugin marketplace remove clickhouse-import-rosstat`, Kimi/Qwen — удаление плагина/расширения `clickhouse-import-rosstat`), проверка — `claude mcp list` → `✔ Connected`.
- [ ] **Step 2: Корневой README** — раздел сокращается до: плагин = только MCP, скилы — в репо для сессий в нём, ссылка на `plugins/rosstat-mcp/README.md`, таблица установки (та же), метаданные синхронизирует `scripts/plugin_manifests.py` из `plugins/rosstat-mcp/.claude-plugin/`. Удалить фразы про ~86 МБ, namespace скилов и агента `coder`.
- [ ] **Step 3: `AGENTS.md` и `ARCHITECTURE.md`** — плагин MCP-only в `plugins/rosstat-mcp/`; скилы плагином не публикуются; корень держит только маркетплейсы Codex/Cursor; инварианты — `plugin_manifest_test.go`, zip — `make plugin-zip` и `rosstat-mcp-release.yml`. `AGENTS.md`: из строки манифестов убрать список корневых файлов.
- [ ] **Step 4: Роадмап** — строка `5.10`: содержание по спеке, статус `🔶 In Progress` (финальный — после приёмки, Task 5), критерий приёмки из issue #474.
- [ ] **Step 5: Проверка** — `make docs-check` → exit 0; `grep -rn "kimi.plugin.json\|qwen-extension.json" --include=*.md . | grep -v -e docs/superpowers -e docs/ROADMAP_INFRA.md -e plugins/rosstat-mcp` → пусто (исторические спеки и строки 5.7–5.9 не правятся).
- [ ] **Step 6: Commit**

```bash
git add plugins/rosstat-mcp/README.md README.md AGENTS.md ARCHITECTURE.md docs/ROADMAP_INFRA.md
git commit -m "infra 5.10: document MCP-only plugin install"
```

---

### Task 5: Полная проверка и живая приёмка

**Files:**
- Modify: `docs/ROADMAP_INFRA.md` (статус и протокол `5.10`)

- [ ] **Step 1: CI-набор** — `make all` → exit 0; `make plugin-manifests-check`; `python3 scripts/plugin_manifests_test.py`; `make docs-check`.
- [ ] **Step 2: Push ветки** — `git push -u origin feat/infra-5.10-mcp-only-plugin` (нужно, чтобы `git-subdir` мог взять каталог до мержа).
- [ ] **Step 3: Claude Code** — в изолированном `CLAUDE_CONFIG_DIR` (scratchpad): копия `plugins/rosstat-mcp/.claude-plugin/marketplace.json` с `ref` = имя ветки, добавленная как локальный файл; `claude plugin install rosstat-mcp@rosstat-mcp`; `du -sh plugins/marketplaces plugins/cache`; `find plugins/cache -type f` — только файлы §4.1; через `make` (токен из env-файла) `claude mcp list` → `plugin:rosstat-mcp:rosstat-staging … ✔ Connected`. Отдельно: `curl -sI` raw-URL маркетплейса на ветке → 200.
- [ ] **Step 4: Codex** — если `codex` установлен: `codex plugin marketplace add kmlebedev/clickhouse-import-rosstat@feat/infra-5.10-mcp-only-plugin --sparse .agents --sparse plugins/rosstat-mcp` в изолированном `CODEX_HOME`, замер размера; иначе — «не проверено».
- [ ] **Step 5: Kimi / Qwen** — если клиент установлен: установка из `build/rosstat-mcp.zip` (локальный путь) в изолированный домашний каталог клиента, MCP-сервер виден; иначе — «не проверено». Релиз по тегу — после мержа владельцем.
- [ ] **Step 6: Роадмап** — строка `5.10`: `🟢 In Review` с результатами шагов 1–5 (размеры, «не проверено» явно).
- [ ] **Step 7: Commit**

```bash
git add docs/ROADMAP_INFRA.md
git commit -m "infra 5.10: record live acceptance"
```

PR: один, в конце, `Closes #474` вне backticks, протокол приёмки — комментарием в PR; Project V2 → `In Review`, поле `PR`.
