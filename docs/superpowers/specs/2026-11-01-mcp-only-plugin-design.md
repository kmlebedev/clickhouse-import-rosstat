# Спека: `infra/5.10` — MCP-only плагин без скилов и малой установкой

**Дата:** 2026-11-01
**Пункт:** `infra/5.10` (issue [#474](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/474)); зависит от `infra/5.9` (#456)
**Роль:** Go-инженер (манифесты, контрактный тест, Makefile/CI, docs)

## 1. Цель

Приёмка `infra/5.9` ([комментарий в #456](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/456#issuecomment-6547537802)) показала: плагин в Claude Code ставится клоном всего репозитория — **123 МБ маркетплейс + 91 МБ плагин** — и несёт 11 ролевых скилов. Решение владельца:

1. Плагин распространяет **только подключение к MCP** `https://mcp.selfinvesting.ru:8443/mcp` (сервер `rosstat-staging`), без скилов и агентов.
2. При установке не скачивается ничего, что не нужно для MCP.
3. Размер плагина и маркетплейса при установке снижается кардинально.

Ролевые скилы `.kimi-code/skills/` и агент `.kimi-code/agents/coder.md` остаются в репозитории для сессий, работающих в нём самом (роутер `AGENTS.md`); из плагина они уходят.

## 2. Почему сейчас много

Каждый клиент получает весь репозиторий: Claude Code и Codex клонируют маркетплейс-репо по `owner/repo`, Kimi качает zip всего репо, Qwen делает shallow-клон, а плагин объявлен корнем репо (`"source": "./"`). Убрать `skills` из манифестов недостаточно — нужен маленький **источник** плагина.

## 3. Разведка (2026-11-01)

Замеры Claude Code 2.1.287 в изолированном `CLAUDE_CONFIG_DIR`:

| Вариант | Маркетплейс | Плагин |
|---|---|---|
| Сейчас: `marketplace add kmlebedev/clickhouse-import-rosstat` | 123 МБ | 91 МБ |
| `marketplace add … --sparse .claude-plugin` | 2,9 МБ (cone-режим тянет все файлы корня) | — |
| Источник плагина `git-subdir` (частичный клон, один подкаталог) | — | **12 КБ**, 5,7 с |
| Маркетплейс по прямому URL на `marketplace.json` | один файл | — |

Остальные клиенты (документация и исходники; клиенты не ставились):

| Клиент | Подкаталог репо | Что скачивает |
|---|---|---|
| Codex (`openai/codex`, `marketplace_add/install.rs`, `loader.rs`) | да: `marketplace add --sparse <paths>` и источник `git-subdir` | `--sparse` → `git clone --filter=blob:none` + sparse-checkout; `git-subdir` → то же в no-cone режиме |
| Kimi Code (`MoonshotAI/kimi-code`, `source.ts`, `github-resolver.ts`) | нет: путь после `/tree/` читается как имя ref | zip всего репо с `codeload.github.com`; **zip по прямому URL поддержан** |
| Qwen Code (`QwenLM/qwen-code`, `extension/github.ts`) | нет | `git clone --depth 1` всего репо; **zip/tar.gz по URL поддержан** |
| Cursor | marketplace в корне репо с относительным `source` на подкаталог | импорт репо на стороне Cursor; размер на клиенте не документирован |

## 4. Решение

### 4.1 Каталог плагина

Плагин целиком живёт в `plugins/rosstat-mcp/`; в нём только манифесты, MCP-конфиги и README:

```text
plugins/rosstat-mcp/
  README.md
  .claude-plugin/plugin.json        # Claude Code: mcpServers inline
  .claude-plugin/marketplace.json   # маркетплейс Claude Code (добавляется по URL)
  .codex-plugin/plugin.json         # Codex: mcpServers → ./.codex-plugin/mcp.json
  .codex-plugin/mcp.json
  .cursor-plugin/plugin.json        # Cursor: mcpServers → ./.cursor-plugin/mcp.json, variables.ROSSTAT_MCP_TOKEN
  .cursor-plugin/mcp.json
  kimi.plugin.json                  # Kimi Code: mcpServers inline
  qwen-extension.json               # Qwen Code: mcpServers inline
```

- Поля `skills`, `agents`, `commands`, `hooks` не объявляются ни в одном манифесте.
- MCP-контракт `infra/5.9` не меняется: сервер `rosstat-staging`, URL `https://mcp.selfinvesting.ru:8443/mcp`, токен только ссылкой на `ROSSTAT_MCP_TOKEN` в синтаксисе клиента.
- **Имя плагина и маркетплейса — `rosstat-mcp`**, версия **0.3.0**. Состав другой, а маркетплейс Claude всё равно новый (другой адрес), поэтому имя `clickhouse-import-rosstat` не сохраняется; описание — «Read-only MCP to Rosstat/CBR data marts (v_* views) on staging».

### 4.2 Корень репозитория

Удаляются: `kimi.plugin.json`, `qwen-extension.json`, `.claude-plugin/`, `.codex-plugin/`, `.cursor-plugin/plugin.json`, `.cursor-plugin/mcp.json`.

Остаются в корне только маркетплейсы, которые клиент обязан искать в корне репо:

- `.agents/plugins/marketplace.json` (Codex) — запись `rosstat-mcp` с локальным источником `./plugins/rosstat-mcp`;
- `.cursor-plugin/marketplace.json` (Cursor) — запись `rosstat-mcp` с `source: "plugins/rosstat-mcp"`.

### 4.3 Доставка по клиентам

| Клиент | Установка | Ожидаемый размер |
|---|---|---|
| Claude Code | `claude plugin marketplace add https://raw.githubusercontent.com/kmlebedev/clickhouse-import-rosstat/main/plugins/rosstat-mcp/.claude-plugin/marketplace.json`<br>`claude plugin install rosstat-mcp@rosstat-mcp` | маркетплейс — один файл; плагин ≈ 12 КБ |
| Codex | `codex plugin marketplace add kmlebedev/clickhouse-import-rosstat --sparse .agents --sparse plugins/rosstat-mcp`<br>`codex plugin add rosstat-mcp@rosstat-mcp` | blobless sparse-клон: каталоги `.agents`, `plugins/rosstat-mcp` и файлы корня (≈ 3 МБ, замер — при приёмке) |
| Kimi Code | `/plugins install https://github.com/kmlebedev/clickhouse-import-rosstat/releases/download/rosstat-mcp-v0.3.0/rosstat-mcp.zip` | несколько КБ |
| Qwen Code | `qwen extensions install https://github.com/kmlebedev/clickhouse-import-rosstat/releases/download/rosstat-mcp-v0.3.0/rosstat-mcp.zip` | несколько КБ |
| Cursor | импорт репозитория как team marketplace / публикация; плагин `rosstat-mcp` из `.cursor-plugin/marketplace.json` | на клиенте не контролируется |

Источник плагина в маркетплейсе Claude — `{"source": "git-subdir", "url": "https://github.com/kmlebedev/clickhouse-import-rosstat.git", "path": "plugins/rosstat-mcp", "ref": "main"}`. Маркетплейс добавляется по URL, поэтому относительный источник (`./…`) там невозможен — Claude Code скачивает только `marketplace.json`.

### 4.4 Zip для Kimi и Qwen

- `python3 scripts/plugin_manifests.py --zip build/rosstat-mcp.zip` собирает детерминированный zip (фиксированные mtime и порядок, корень архива = корень плагина) только из разрешённых файлов §4.1; цель Makefile — `make plugin-zip`.
- Workflow `.github/workflows/rosstat-mcp-release.yml`: на push тега `rosstat-mcp-v*` проверяет, что версия тега равна `version` в `plugins/rosstat-mcp/.claude-plugin/plugin.json`, гоняет `make plugin-manifests-check`, собирает zip и создаёт GitHub Release с ассетом `rosstat-mcp.zip` и SHA-256 в описании; релиз помечается `--latest=false`. Тег `rosstat-mcp-v*` не совпадает с `v*` у `release.yml` (goreleaser), так что бинарный релиз не запускается.
- Тег ставит владелец после мержа; ассистент тег не пушит.

### 4.5 Синхронизация метаданных

`scripts/plugin_manifests.py`: канон переезжает в `plugins/rosstat-mcp/.claude-plugin/plugin.json` (`name`, `version`, длинное описание) и `plugins/rosstat-mcp/.claude-plugin/marketplace.json` (`name` маркетплейса, короткое описание). Цели синхронизации — манифесты §4.1 и два корневых маркетплейса §4.2. `--check`/`--write` и `sync-manifests.yml` — на новых путях.

### 4.6 Миграция установленных копий

Старый маркетплейс `clickhouse-import-rosstat` после мержа перестаёт находить свой `marketplace.json` в корне. README плагина даёт одну команду удаления на клиент (`claude plugin marketplace remove clickhouse-import-rosstat` и аналоги), затем установку по §4.3. Карта `renames` не используется: адрес и имя маркетплейса новые.

## 5. Проверки

`plugin_manifest_test.go` переписывается под новый контракт:

- `TestLLMPluginManifests` — пять манифестов в `plugins/rosstat-mcp/` разбираются, `name = rosstat-mcp`, версии равны; ни в одном нет `skills`, `agents`, `commands`, `hooks`; в корне нет удалённых манифестов §4.2.
- `TestLLMPluginDirAllowlist` — в `plugins/rosstat-mcp/` только файлы из §4.1, суммарно ≤ 64 КБ.
- `TestLLMPluginMarketplaces` — Claude: источник `git-subdir` на `plugins/rosstat-mcp` этого репозитория; Codex и Cursor: источник указывает на `plugins/rosstat-mcp`.
- `TestLLMPluginManifestsNoSecrets` и `TestLLMPluginRemoteMCP` — без изменения смысла, на новых путях.

`scripts/plugin_manifests_test.py` — тесты синхронизации на новых путях плюс тест `--zip` (состав архива = allowlist, корень архива = корень плагина, две сборки побайтно равны).

Живая приёмка:

1. `make all`, `make plugin-manifests-check`, `make plugin-zip`, `claude plugin validate plugins/rosstat-mcp`.
2. Claude Code в изолированном `CLAUDE_CONFIG_DIR`: маркетплейс по URL (до мержа — URL ветки), установка, размеры `plugins/marketplaces` и `plugins/cache`, состав кэша (нет `.kimi-code/`), `claude mcp list` с токеном → ✔ Connected.
3. Codex: команда §4.3 на ветке и замер клона — если `codex` установлен; иначе «не проверено».
4. Kimi/Qwen: установка из локально собранного zip — если клиент установлен; установка из релиза — после тега владельцем.
5. Cursor — «не проверено» (нет CLI-установки).

## 6. Вне объёма

- Отдельный репозиторий плагина (владелец выбрал подкаталог).
- Публикация в каталоги Cursor/Anthropic.
- Изменения MCP-сервера, токенов, TLS (`infra/5.9`).
- Сокращение файлов корня репозитория ради sparse-клона Codex.

## 7. Риски

- **Codex/Cursor** всё ещё тянут больше, чем плагин: Codex — файлы корня в sparse-клоне, Cursor — неизвестно. Размер Codex фиксируется замером.
- **Qwen**: подстановка `${ROSSTAT_MCP_TOKEN}` в `headers` не подтверждена документацией; при приёмке без Qwen — «не проверено».
- **URL маркетплейса Claude** привязан к ветке `main` и пути файла; переименование каталога ломает установку — путь фиксируется тестом.
- **Zip-ссылка** версионная: каждый релиз меняет URL в README; README обновляется тем же PR, что и версия.
