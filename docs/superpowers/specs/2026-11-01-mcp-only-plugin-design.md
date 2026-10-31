# Спека: `infra/5.10` — MCP-only плагин в отдельном репозитории

**Дата:** 2026-11-01
**Пункт:** `infra/5.10` (issue [#474](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/474)); зависит от `infra/5.9` (#456)
**Роль:** Go-инженер (манифесты, CI, Makefile, docs)

## 1. Цель

Приёмка `infra/5.9` ([комментарий в #456](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/456#issuecomment-6547537802)) показала: плагин в Claude Code ставится клоном всего репозитория — **123 МБ маркетплейс + 91 МБ плагин** — и несёт 11 ролевых скилов. Решение владельца:

1. Плагин распространяет **только подключение к MCP** `https://mcp.selfinvesting.ru:8443/mcp` (сервер `rosstat-staging`), без скилов и агентов.
2. При установке не скачивается ничего, что не нужно для MCP.
3. Размер плагина и маркетплейса при установке снижается кардинально.
4. Путь установки — **единообразный во всех клиентах, через отдельный репозиторий** (2026-11-01, после отклонения варианта с подкаталогом как слишком сложного).

Ролевые скилы `.kimi-code/skills/` и агент `.kimi-code/agents/coder.md` остаются в этом репозитории для сессий, работающих в нём самом (роутер `AGENTS.md`); плагином они больше не публикуются.

## 2. Почему сейчас много и почему отдельный репозиторий

Все пять клиентов получают плагин как git-репозиторий целиком: Claude Code и Codex клонируют маркетплейс-репо, Kimi качает zip репо, Qwen делает shallow-клон, Cursor импортирует репо. Сейчас плагин — это корень `clickhouse-import-rosstat` (`"source": "./"`), поэтому клиент получает весь проект.

Разведка 2026-11-01 (Claude Code 2.1.287 — замер; Codex, Kimi, Qwen, Cursor — по документации и исходникам) показала, что подкаталог в этом репо уменьшает установку только в Claude Code (`git-subdir`, 12 КБ) и Codex (`--sparse`, ≈ 3 МБ), а Kimi, Qwen и Cursor всё равно берут весь репо и потребовали бы zip-релизов. Отдельный крошечный репозиторий решает задачу одинаково для всех: весь репо и есть плагин.

## 3. Решение

### 3.1 Репозиторий `kmlebedev/rosstat-mcp`

Публичный (клиенты клонируют без git-учётки; токена в нём нет), лицензия MIT. Создаёт ассистент через `gh` после одобрения спеки и плана; первая версия приходит в `main` нового репо через PR.

```text
rosstat-mcp/
  README.md                         # что даёт плагин, токен, установка, миграция, проверка
  LICENSE                           # MIT
  .gitignore                        # .DS_Store
  .claude-plugin/plugin.json        # Claude Code: mcpServers inline
  .claude-plugin/marketplace.json   # Claude Code: source "./"
  .codex-plugin/plugin.json         # Codex: mcpServers → ./.codex-plugin/mcp.json
  .codex-plugin/mcp.json
  .agents/plugins/marketplace.json  # Codex: source → этот репозиторий, ref main
  .cursor-plugin/plugin.json        # Cursor: mcpServers → ./.cursor-plugin/mcp.json, variables.ROSSTAT_MCP_TOKEN
  .cursor-plugin/marketplace.json   # Cursor: source "."
  .cursor-plugin/mcp.json
  kimi.plugin.json                  # Kimi Code: mcpServers inline
  qwen-extension.json               # Qwen Code: mcpServers inline
  tests/test_manifests.py           # контракт манифестов, Python stdlib
  .github/workflows/check.yml       # CI: python3 -m unittest на каждый push/PR
```

- **Имя плагина и маркетплейсов — `rosstat-mcp`, версия — 0.3.0** (продолжение линии 0.2.0).
- Описание длинное: «Read-only MCP connection to Rosstat/CBR data marts (v_* views, point-in-time layer) on the clickhouse-import-rosstat staging server.»; короткое: «Read-only MCP to Rosstat/CBR data marts».
- Поля `skills`, `agents`, `commands`, `hooks` не объявляются ни в одном манифесте.
- MCP-контракт `infra/5.9` без изменений: сервер `rosstat-staging`, URL `https://mcp.selfinvesting.ru:8443/mcp`, токен только ссылкой на `ROSSTAT_MCP_TOKEN` в синтаксисе клиента (Claude/Qwen/Cursor — `Authorization: Bearer ${ROSSTAT_MCP_TOKEN}`, Kimi — `bearerTokenEnvVar`, Codex — `bearer_token_env_var`, Cursor — плюс `variables.ROSSTAT_MCP_TOKEN`, required).
- Синхронизатора метаданных нет: манифестов девять и они меняются редко; равенство `name`/`version`/описаний во всех манифестах проверяет тест.

### 3.2 Установка

| Клиент | Команда |
|---|---|
| Claude Code | `claude plugin marketplace add kmlebedev/rosstat-mcp`<br>`claude plugin install rosstat-mcp@rosstat-mcp` |
| Codex | `codex plugin marketplace add kmlebedev/rosstat-mcp`<br>`codex plugin add rosstat-mcp@rosstat-mcp` |
| Kimi Code | `/plugins install https://github.com/kmlebedev/rosstat-mcp` |
| Qwen Code | `qwen extensions install kmlebedev/rosstat-mcp` |
| Cursor | `/add-plugin https://github.com/kmlebedev/rosstat-mcp` |

Ожидаемый размер — десятки КБ на клиент (весь репо плюс его `.git`).

### 3.3 Этот репозиторий

Удаляются: `kimi.plugin.json`, `qwen-extension.json`, `.claude-plugin/`, `.codex-plugin/`, `.cursor-plugin/`, `.agents/plugins/marketplace.json`, `plugin_manifest_test.go`, `scripts/plugin_manifests.py`, `scripts/plugin_manifests_test.py`, `.github/workflows/sync-manifests.yml`, цели Makefile `plugin-manifests-check`/`plugin-manifests-sync`. `LICENSE` остаётся (лицензия проекта).

Документация: раздел README «Подключение как плагин LLM-клиента» — ссылка на `kmlebedev/rosstat-mcp` и таблица §3.2; `AGENTS.md` — абзац «Плагин:» говорит, что плагин живёт отдельно и публикует только MCP; `ARCHITECTURE.md` — §2 (раскладка), §6.5 (абзац «Плагин репозитория → MCP»), конвенция 17; роадмап — строка `5.10`. Исторические спеки и строки `5.7`–`5.9` не правятся.

### 3.4 Миграция установленных копий

Старый маркетплейс `clickhouse-import-rosstat` после мержа не находит манифестов. README нового репо даёт по одной команде удаления на клиент (`claude plugin marketplace remove clickhouse-import-rosstat`, `codex plugin marketplace remove clickhouse-import-rosstat`, удаление плагина/расширения `clickhouse-import-rosstat` в Kimi, Qwen и Cursor) и затем установку по §3.2.

## 4. Проверки

**Новый репо, `tests/test_manifests.py` (unittest, stdlib):**

- все JSON разбираются; у пяти манифестов плагина и трёх маркетплейсов `name == "rosstat-mcp"`, версии равны `.claude-plugin/plugin.json`, описания равны каноническим из `.claude-plugin/plugin.json` и `.claude-plugin/marketplace.json`;
- в манифестах нет ключей `skills`, `agents`, `commands`, `hooks`;
- у каждого манифеста плагина есть сервер `rosstat-staging` с URL выше и токеном только из `ROSSTAT_MCP_TOKEN`; строковый `mcpServers` указывает на существующий файл внутри репо; сервера `clickhouse` нет;
- нет ключей вида `token|secret|password|api_key` со значениями и литеральных `Bearer <значение>` (перенос `TestLLMPluginManifestsNoSecrets`);
- источники маркетплейсов: Claude — `"./"`, Cursor — `"."`, Codex — `{"source": "url", "url": "https://github.com/kmlebedev/rosstat-mcp.git", "ref": "main"}`;
- множество отслеживаемых файлов (`git ls-files`) равно списку §3.1, суммарно ≤ 64 КБ.

**Этот репо:** `make all` зелёный; в корне нет удалённых файлов §3.3; `grep` по README/AGENTS/ARCHITECTURE не находит удалённых путей.

**Живая приёмка (после первого PR в новом репо):**

1. CI нового репо зелёный; `claude plugin validate .` в новом репо — без ошибок и warnings.
2. Claude Code в изолированном `CLAUDE_CONFIG_DIR`: команды §3.2, размеры `plugins/marketplaces` и `plugins/cache`, состав кэша; `claude mcp list` с токеном (через `make`) → `plugin:rosstat-mcp:rosstat-staging … ✔ Connected`.
3. Kimi — установка в изолированный домашний каталог клиента, если CLI это позволяет; Codex, Qwen, Cursor — если клиент установлен; иначе «не проверено».

## 5. Вне объёма

- Синхронизация манифестов между репозиториями (в основном репо манифестов больше нет).
- Публикация в каталоги Cursor/Anthropic.
- Изменения MCP-сервера, токенов, TLS (`infra/5.9`).
- Перенос ролевых скилов в отдельный плагин.

## 6. Риски

- **Два репозитория — два PR.** Изменение MCP-контракта (URL, имя сервера, переменная токена) теперь правится в `kmlebedev/rosstat-mcp`; `ARCHITECTURE.md` §6.5 ссылается на него как на источник истины для манифестов.
- **Qwen**: подстановка `${ROSSTAT_MCP_TOKEN}` в `headers` не подтверждена документацией; без установленного Qwen — «не проверено».
- **Cursor**: CLI-установки нет; приёмка — «не проверено».
