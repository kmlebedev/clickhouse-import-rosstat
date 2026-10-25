# Дизайн: LLM-плагин репозитория `clickhouse-import-rosstat`

**Дата:** 2026-10-25
**Пункт роадмапа:** `infra/5.7` (issue [#382](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/382))
**Роль:** `shared`
**Связанные документы:** [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md), [ROADMAP_INDEX.md](../../ROADMAP_INDEX.md), [ARCHITECTURE.md](../../../ARCHITECTURE.md), [README.md](../../../README.md), [AGENTS.md](../../../AGENTS.md)

---

## 1. Замысел и критерии успеха

Репозиторий уже содержит операционные ролевые скилы (`.kimi-code/skills/`) и Kimi-агента (`.kimi-code/agents/coder.md`), но воспользоваться ими можно только внутри рабочего дерева и только в Kimi Code. Нужно упаковать тот же набор как LLM-плагин, устанавливаемый одной командой из GitHub в пяти клиентах, не заводя второй источник скилов.

**Критерии успеха:**

1. В корне репозитория лежат манифесты Kimi Code, Claude Code, Codex, Cursor и Qwen Code; все они ссылаются на `.kimi-code/skills/`.
2. `claude plugin validate . --strict` проходит без ошибок и предупреждений; контрактный Go-тест проверяет имена, версии, пути и отсутствие секретов.
3. README содержит команды установки для пяти клиентов; `ARCHITECTURE.md` фиксирует конвенцию «один источник скилов».
4. Живая установка в Kimi Code (`/plugins install` → `/plugins info`) не даёт diagnostics.
5. Go-код импортёров, схема ClickHouse и гранты не меняются.

## 2. Что проверено (фактическая база)

### 2.1 Kimi Code

Официальная документация: [Plugins](https://www.kimi.com/code/docs/en/kimi-code-cli/customization/plugins.html#plugin-manifest).

- Манифест — `<plugin_root>/kimi.plugin.json` или `<plugin_root>/.kimi-plugin/plugin.json`; первый имеет приоритет.
- Поддержаны `name` (id по regex `[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*(@[A-Za-z0-9_-]+)?`), `version`, `description`, `author`, `homepage`, `license`, `interface`, `skills` (пути внутри plugin root), `agents` (пути к файлам агентов), `mcpServers`, `commands`, `sessionStart`, `systemPrompt`.
- Скилы плагина видны как `<plugin-id>:<skill-name>`; `/skill:<plugin-id>:<skill-name>`.
- Установка из GitHub: `https://github.com/<owner>/<repo>` ставит **последний релиз** и только при его отсутствии падает на default branch; `.../tree/<ref>` ставит конкретную ветку.
- Плагины ставятся per-user; после установки нужен `/reload` или новая сессия.
- MCP-серверы из манифеста включаются по умолчанию и могут быть выключены через `/plugins mcp disable`.

### 2.2 Claude Code

Официальная документация: [Plugin manifest reference](https://code.claude.com/docs/en/plugins-reference), [Create a marketplace](https://code.claude.com/docs/en/plugin-marketplaces).

- Манифест — `.claude-plugin/plugin.json`; `name` обязателен, kebab-case.
- `skills` принимает путь или массив путей к директориям; `agents` — только `.md`-файлы.
- Marketplace — `.claude-plugin/marketplace.json`; относительный `source: "./"` указывает на корень marketplace.
- `claude plugin validate <path> --strict` — авторитетная проверка манифеста, marketplace и скилов.

### 2.3 Codex, Cursor, Qwen Code

- Codex: `.codex-plugin/plugin.json` с `skills` и `interface`; реестр marketplace — `.agents/plugins/marketplace.json` (раскладка и поля проверены на `FinamWeb/finam-skill`).
- Cursor: `.cursor-plugin/plugin.json` с `skills`; реестр — `.cursor-plugin/marketplace.json`; Cursor также поддерживает open standard Agent Plugins с корневым `plugin.json`, но тот требует фиксированный `skills/`.
- Qwen Code: `qwen-extension.json` с `skills`; примеры в `QwenLM/qwen-code` и `QwenLM/qwen-code-examples` подтверждают строковый путь.

### 2.4 `FinamWeb/finam-skill`

Репозиторий **архивирован**, README перенаправляет в `FinamWeb/finam-trade-api`; внутри — один чужой trading-скил и собственные marketplace-имена. Форк не даёт нам ни маркетплейса, ни аудитории и тянет чужую историю/бренд. Используем только раскладку манифестов как образец; отдельный публичный skill-репозиторий, если понадобится, заводится с нуля после появления публичного MCP-эндпоинта.

### 2.5 Размер репозитория

`git archive HEAD` — около 86 МБ tracked-файлов (1705 файлов). Установка плагина копирует снапшот репозитория; это осознанная цена за то, что относительные ссылки скилов (`../../../ARCHITECTURE.md` и т.п.) продолжают разрешаться внутри plugin root.

## 3. Архитектура упаковки

```
clickhouse-import-rosstat/                  # plugin root = repo root
├── kimi.plugin.json                        # Kimi Code
├── .claude-plugin/
│   ├── plugin.json                         # Claude Code
│   └── marketplace.json
├── .codex-plugin/plugin.json               # Codex
├── .agents/plugins/marketplace.json        # Codex marketplace
├── .cursor-plugin/
│   ├── plugin.json                         # Cursor
│   └── marketplace.json
├── qwen-extension.json                     # Qwen Code
├── LICENSE                                 # MIT
├── .kimi-code/skills/                      # единственный источник скилов (10 шт.)
├── .kimi-code/agents/coder.md              # только Kimi
└── plugin_manifest_test.go                 # контрактный тест
```

**Принцип:** манифесты — только указатели. Ни один скил не копируется и не переписывается; добавление/удаление скила в `.kimi-code/skills/` автоматически меняет состав плагина. Меняется только путь — тогда правятся манифесты и тест.

### 3.1 Матрица манифестов

| Клиент | Файл | `skills` | Дополнительно |
|---|---|---|---|
| Kimi Code | `kimi.plugin.json` | `"./.kimi-code/skills/"` | `agents: "./.kimi-code/agents/coder.md"`, `interface` |
| Claude Code | `.claude-plugin/plugin.json` | `["./.kimi-code/skills/"]` | без агента: `coder.md` использует Kimi-имена инструментов |
| Codex | `.codex-plugin/plugin.json` | `"./.kimi-code/skills/"` | `interface` |
| Cursor | `.cursor-plugin/plugin.json` | `"./.kimi-code/skills/"` | `tags` |
| Qwen Code | `qwen-extension.json` | `"./.kimi-code/skills/"` | — |

Общие поля: `name: clickhouse-import-rosstat`, `version: 0.1.0`, `description`, `author: {name: kmlebedev, url: https://github.com/kmlebedev}`, `homepage`, `repository`, `license: MIT`, `keywords`.

Marketplace-файлы:

| Клиент | Файл | Имя marketplace | `source` |
|---|---|---|---|
| Claude Code | `.claude-plugin/marketplace.json` | `clickhouse-import-rosstat` | `"./"` |
| Codex | `.agents/plugins/marketplace.json` | `clickhouse-import-rosstat-marketplace` | git URL + `ref: main` |
| Cursor | `.cursor-plugin/marketplace.json` | `clickhouse-import-rosstat-marketplace` | `"."` |

### 3.2 Версия

`0.1.0` — версия упаковки, а не Go-релиза. Меняется синхронно во всех plugin-манифестах; контрактный тест ловит рассинхрон. Claude pin-ит установленную версию, поэтому при изменении состава скилов версию нужно поднимать.

### 3.3 Секреты и MCP

- В манифестах нет токенов, ключей и URL с учётными данными.
- `mcpServers` в манифесты **не добавляется**: в репозитории уже есть `.kimi-code/mcp.json` с сервером `clickhouse`, который читает `~/.config/rosstat/env`. Второй сервер с тем же именем в plugin-манифесте создал бы дубль и неоднозначность.
- Внешний data-скил с remote MCP — отдельный репозиторий и отдельный пункт; там MCP будет обязательным.

### 3.4 Что не задаём

- `sessionStart` — плагин per-user; автозагрузка ролевого скила в каждую сессию недопустима.
- `systemPrompt`/`systemPromptPath` — не инжектим глобальные инструкции.
- `commands` — готовых команд в репозитории нет.
- Корневой `plugin.json` Agent Plugins — стандарт требует фиксированный `skills/`; переносить каталог ради него не будем. Cursor покрыт `.cursor-plugin/plugin.json`.

## 4. Контрактный тест

`plugin_manifest_test.go` в корневом `package main` (попадает в `go test -race ./...`):

1. все 8 JSON-файлов существуют и парсятся;
2. `name` = `clickhouse-import-rosstat`, `version` = `0.1.0` в пяти plugin-манифестах;
3. `skills` (string или `[]string`) — относительный путь внутри repo-root; директория существует; в каждой директории есть `SKILL.md` с `name:`;
4. `agents` указывает на `.kimi-code/agents/coder.md`;
5. marketplace-манифесты: имя, `plugins[0].name`, корректный `source`;
6. `LICENSE` существует и содержит `MIT License`;
7. рекурсивный обход JSON не находит ключей `api_key`, `token`, `password`, `secret`.

Тест офлайн, без сети и без внешних зависимостей.

## 5. Документация

- `README.md` — раздел «Подключение как плагин LLM-клиента»: команды для пяти клиентов, namespace, `/reload`, предупреждение о снапшоте ~86 МБ и о Kimi latest-release.
- `ARCHITECTURE.md` — шапка «Последнее обновление», манифесты в структуре каталогов, конвенция №17.
- `AGENTS.md` — одна строка после таблицы ролей.
- `docs/ROADMAP_INFRA.md` — пункт 5.7 и статус.
- `docs/ROADMAP_INDEX.md` — диапазон item-id `5.1`–`5.7`.

## 6. Приёмка

1. `go test -run TestLLMPluginManifests -v .` — PASS.
2. `claude plugin validate . --strict` — exit 0 без warnings.
3. `make docs-check` — exit 0, ссылки разрешаются.
4. `make all` — gofmt, lint, vet, `go test -race`, обе сборки.
5. Kimi: `/plugins install <worktree>` → `/plugins info clickhouse-import-rosstat` без diagnostics; скилы видны. Выполняет владелец в TUI, протокол — в PR.
6. Claude: живая установка из локального marketplace в изолированный `CLAUDE_CONFIG_DIR` — если CLI позволяет; иначе `validate` достаточно, живая установка помечается «не проверено».
7. Codex/Cursor/Qwen: CLI локально отсутствуют; живые установки не заявляются, манифесты проверены структурно и по документации.

## 7. Риски

| Риск | Митигация |
|---|---|
| Снапшот ~86 МБ, ссылки в скилах ведут в снапшот, а не в рабочее дерево | Предупреждение в README; переустановка после изменения docs/скилов |
| Kimi короткий URL ставит latest release без манифеста | В README — `/tree/main`; после мержа — тег `v*` |
| Клиент не принимает скрытую `.kimi-code/skills` | Fallback: корневой `skills/` как symlink на `.kimi-code/skills`, правка только проблемного манифеста; без дублирования |
| Рассинхрон версий/имён | Контрактный тест |
| Codex/Cursor/Qwen не проверены живьём | Явно фиксируется в PR; структурная проверка + официальная документация |

## 8. Отклонённые варианты

- **Форк `FinamWeb/finam-skill`** — архивирован, переехал в `finam-trade-api`, содержит чужой trading-скил и свои marketplace-имена. Не даёт аудитории и создаёт путаницу.
- **Отдельный thin skill-репозиторий сразу** — нет публичного MCP-эндпоинта; внешний data-скил нечем проверить. Отдельный пункт после `gold-nav`/публичного MCP.
- **Корневой `plugin.json` Agent Plugins** — требует фиксированный `skills/`; перенос каталога ломает ссылки и привычную структуру.
- **MCP в манифестах** — дубль `.kimi-code/mcp.json`, конфликт имени `clickhouse`.
- **Копирование скилов в `skills/`** — два источника истины, рассинхрон.
- **`sessionStart` с ролевым скилом** — глобальная инъекция в каждую сессию.
