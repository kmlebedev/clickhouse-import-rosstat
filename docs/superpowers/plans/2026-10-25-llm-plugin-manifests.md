# План: LLM-плагин репозитория `clickhouse-import-rosstat` (`infra/5.7`)

**Дата:** 2026-10-25
**Пункт:** `infra/5.7`, issue [#382](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/382)
**Спека:** [2026-10-25-llm-plugin-manifests-design.md](../specs/2026-10-25-llm-plugin-manifests-design.md)
**Ветка:** `feat/infra-5.7-llm-plugin-manifests`
**Worktree:** `.worktrees/llm-plugin-manifests`

---

## Task 0. Подготовка (сделано)

- [x] Worktree от `origin/main`, ветка `feat/infra-5.7-llm-plugin-manifests`.
- [x] Строка `5.7` в `docs/ROADMAP_INFRA.md`, диапазон `5.1`–`5.7` в `docs/ROADMAP_INDEX.md`.
- [x] Issue #382, labels `roadmap:infra` + `role:shared`, Project V2: `In Progress`, `Branch`, `Spec / Plan`, `Acceptance`.
- [x] Спека.

## Task 1. Контрактный тест (red)

Файл: `plugin_manifest_test.go` (корневой `package main`).

1. Таблица манифестов:
   - `kimi.plugin.json` — name, version, `skills` string, `agents` string;
   - `.claude-plugin/plugin.json` — name, version, `skills` []string;
   - `.codex-plugin/plugin.json` — name, version, `skills` string;
   - `.cursor-plugin/plugin.json` — name, version, `skills` string;
   - `qwen-extension.json` — name, version, `skills` string;
   - `.claude-plugin/marketplace.json` — marketplace name + plugin entry + `source: "./"`;
   - `.agents/plugins/marketplace.json` — marketplace name + git source + `ref: main`;
   - `.cursor-plugin/marketplace.json` — marketplace name + `source: "."`.
2. Хелперы: `readManifest`, `skillPaths(any)`, `walkJSON` (поиск запрещённых ключей), `assertRepoPath` (путь не абсолютный, не выходит за repo-root).
3. Проверки: `name == "clickhouse-import-rosstat"`, `version == "0.1.0"`; каждая `skills`-директория существует и содержит `SKILL.md` с `name:`; `agents` существует; `LICENSE` содержит `MIT License`; нет ключей `api_key/token/password/secret`.
4. Запуск: `go test -run TestLLMPluginManifests -v .` — **должен упасть** (файлов ещё нет).
5. Коммит: `add plugin manifest contract test`.

## Task 2. Манифесты и LICENSE (green)

Создать:

1. `kimi.plugin.json`:
```json
{
  "name": "clickhouse-import-rosstat",
  "version": "0.1.0",
  "description": "Role skills and agent for the clickhouse-import-rosstat data platform: Rosstat, CBR, MOEX, macro and financial ETL, ML nowcasts and publications.",
  "author": {"name": "kmlebedev", "url": "https://github.com/kmlebedev"},
  "homepage": "https://github.com/kmlebedev/clickhouse-import-rosstat",
  "repository": "https://github.com/kmlebedev/clickhouse-import-rosstat",
  "license": "MIT",
  "keywords": ["rosstat", "cbr", "clickhouse", "macro", "finance", "etl", "go", "ml", "agent-skills"],
  "skills": "./.kimi-code/skills/",
  "agents": "./.kimi-code/agents/coder.md",
  "interface": {
    "displayName": "ClickHouse Import Rosstat",
    "shortDescription": "Repo role skills for the Rosstat/CBR data platform",
    "longDescription": "Ten repo-specific skills for Go, Python, ML, research, publications, dashboards, news onboarding and the GitHub tracker, plus the coder agent. Install to work with the clickhouse-import-rosstat repository in Kimi Code.",
    "developerName": "kmlebedev",
    "websiteURL": "https://github.com/kmlebedev/clickhouse-import-rosstat"
  }
}
```

2. `.claude-plugin/plugin.json` — те же метаданные, `"skills": ["./.kimi-code/skills/"]`; без `agents`.
3. `.claude-plugin/marketplace.json` — `$schema`, `name: clickhouse-import-rosstat`, `owner`, `plugins[0]` с `name`, `source: "./"`, `description`, `version`.
4. `.codex-plugin/plugin.json` — `skills: "./.kimi-code/skills/"`, `interface` (displayName, shortDescription, longDescription, developerName, category, capabilities `["Read"]`, websiteURL).
5. `.agents/plugins/marketplace.json` — `name: clickhouse-import-rosstat`, `interface.displayName`, `plugins[0].source = {source:"url", url:"https://github.com/kmlebedev/clickhouse-import-rosstat.git", ref:"main"}`, `policy`.
6. `.cursor-plugin/plugin.json` — `skills: "./.kimi-code/skills/"`, `category`, `tags`.
7. `.cursor-plugin/marketplace.json` — `name: clickhouse-import-rosstat`, `owner`, `metadata`, `plugins[0].source: "."`.
8. `qwen-extension.json` — `name`, `version`, `description`, `skills: "./.kimi-code/skills/"`.
9. `LICENSE` — MIT, `Copyright (c) 2026 kmlebedev`.

Проверки:

```bash
jq -e . kimi.plugin.json .claude-plugin/plugin.json .claude-plugin/marketplace.json \
  .codex-plugin/plugin.json .agents/plugins/marketplace.json \
  .cursor-plugin/plugin.json .cursor-plugin/marketplace.json qwen-extension.json
go test -run TestLLMPluginManifests -v .
claude plugin validate . --strict
```

Если `claude plugin validate` ругается на скрытый путь `.kimi-code/skills`, применить fallback из спеки §7 (корневой `skills/` symlink), не дублировать скилы. Если ругается на поле marketplace — удалить недокументированное поле, а не отключать `--strict`.

Коммит: `add llm plugin manifests and mit license`.

## Task 3. Документация

1. `README.md` — после «Роли и скилы» добавить «Подключение как плагин LLM-клиента»:
   - таблица: Kimi `/plugins install https://github.com/kmlebedev/clickhouse-import-rosstat/tree/main`; Claude `claude plugin marketplace add kmlebedev/clickhouse-import-rosstat` + `claude plugin install clickhouse-import-rosstat@clickhouse-import-rosstat`; Codex `codex plugin marketplace add ...` + `codex plugin add clickhouse-import-rosstat@clickhouse-import-rosstat`; Cursor `/add-plugin https://github.com/kmlebedev/clickhouse-import-rosstat`; Qwen `qwen extensions install kmlebedev/clickhouse-import-rosstat`;
   - namespace `/skill:clickhouse-import-rosstat:go-engineer`, `/reload`;
   - предупреждение о снапшоте ~86 МБ и о Kimi latest-release;
   - явно: MCP не входит в плагин, в репозитории — `.kimi-code/mcp.json`.
2. `ARCHITECTURE.md`:
   - шапка «Последнее обновление» с пунктом `infra/5.7` и кратким составом;
   - блок структуры каталогов: манифесты и `LICENSE` рядом с `.kimi-code/skills/`;
   - конвенция №17 про единый источник скилов и запрет дублирования MCP.
3. `AGENTS.md` — одна строка после таблицы ролей про plugin-манифесты и единый источник.
4. `docs/ROADMAP_INFRA.md` — статус `5.7` → `🔶 In Review` при открытии PR; ссылка на issue #382.
5. `docs/ROADMAP_INDEX.md` — уже обновлён.

Коммит: `document llm plugin install`.

## Task 4. Верификация

```bash
go test -run TestLLMPluginManifests -v .
python3 scripts/plugin_manifests_test.py -v
make plugin-manifests-check
go test -race ./...
claude plugin validate . --strict
make docs-check
make all
```

Живые проверки:

1. Kimi (владелец, TUI): `/plugins install <worktree>` → `/plugins info clickhouse-import-rosstat` без diagnostics; список скилов; `/skill:clickhouse-import-rosstat:go-engineer`.
2. Claude (если возможно, изолированный `CLAUDE_CONFIG_DIR`): `claude plugin marketplace add <worktree>` → `claude plugin install clickhouse-import-rosstat@clickhouse-import-rosstat --scope user` → `claude plugin list`.
3. Codex/Cursor/Qwen — CLI отсутствуют; записать «не проверено» и причину.

## Task 5. Синхронизация метаданных (`infra/5.8`, follow-up к 5.7)

1. `scripts/plugin_manifests.py` — `--check`/`--write`; канон: `.claude-plugin/plugin.json` (`name`, `version`, длинное `description`) и `.claude-plugin/marketplace.json` (`name` marketplace, короткое `description`).
2. `scripts/plugin_manifests_test.py` — 6 офлайн-тестов: synced, drift→fix, all clients, missing canonical/target field, idempotency.
3. `.github/workflows/sync-manifests.yml` — на PR `--check` + тесты; на push в `main` `--write` + коммит `chore: sync plugin manifests [skip ci]`.
4. Makefile — `plugin-manifests-check` и `plugin-manifests-sync` в `.PHONY`.
5. README — команда Codex `...@clickhouse-import-rosstat` и абзац про sync; спека/план/роадмап/ARCHITECTURE обновлены.
6. Имена marketplace у Claude/Codex/Cursor выровнены на `clickhouse-import-rosstat`; Go-тест и README синхронны.
7. Проверки: `make plugin-manifests-check`, `python3 scripts/plugin_manifests_test.py -v`, `make all`, `make docs-check`; workflow на PR зелёный.

## Task 6. PR и приёмка

1. Push ветки, PR: заголовок `[infra/5.7] LLM plugin manifests for Kimi, Claude, Codex, Cursor and Qwen`.
2. В теле: пункт `infra/5.7`, `Closes #382` вне backticks, источник, схема, запуск, проверки.
3. Project: `Status = In Review`, `PR = #N`.
4. Протокол живой приёмки — комментарием в PR: команды и результаты, включая честные «не проверено» для отсутствующих CLI.
5. `Done` и `✅` в роадмапе — только после мержа и протокола.

## Rollback

- Удалить 8 JSON, `LICENSE`, `plugin_manifest_test.go`, `scripts/plugin_manifests*.py`, `.github/workflows/sync-manifests.yml`, вернуть README/ARCHITECTURE/AGENTS/роадмап/Makefile; ветка не мержится. Скилы и `.kimi-code/mcp.json` не затрагиваются.
