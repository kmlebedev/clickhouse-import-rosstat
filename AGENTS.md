# AGENTS.md — общий контракт и роутер ролей

> Файл читается ассистентом в начале каждой сессии. Он отвечает на вопросы «кто я», «что мне прочитать» и «какие правила общие для всех». Глубина — в ролевых скилах; архитектура — в [ARCHITECTURE.md](ARCHITECTURE.md), реестр роадмапов и трекер — в [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md).

---

## Роли и маршрутизация

| Триггер задачи | Роль | Скил | Границы |
|---|---|---|---|
| Go-пакет, импортёр, DDL, витрина, грант, `cmd/ingest`, Makefile/CI, deploy, dagu | Go-инженер | [`go-engineer`](.kimi-code/skills/go-engineer/SKILL.md) | Не делает ML-эксперименты и ad-hoc Python; схема — только по спеке |
| `scripts/*.py` вне ML, коллектор, harvest, docs-генератор, MCP-check, forecast-log sender, ingest-payload | Python-инженер | [`python-engineer`](.kimi-code/skills/python-engineer/SKILL.md) | Не пишет в ClickHouse напрямую, не делает DDL/GRANT |
| nowcast, CatBoost, бэктест, MLflow, семантика `forecast_log`, DM/CI | ML-инженер | [`ml-engineer`](.kimi-code/skills/ml-engineer/SKILL.md) | Не делает DDL/витрины/гранты и Go-импортёры |
| Разведка источника, аудит покрытия/винтажей, методология, research-документ, спека | Research-инженер | [`research-engineer`](.kimi-code/skills/research-engineer/SKILL.md) | Не пишет прод-код и не делает запись в БД |
| Issue, Project V2, роадмап, PR-протокол | все | [`github-tracker`](.kimi-code/skills/github-tracker/SKILL.md) | Источник истины — файл роадмапа |
| Дашборд как код, миграция легаси-экспорта | Go-инженер | [`dashboard-migration`](.kimi-code/skills/dashboard-migration/SKILL.md) | Артефакт и код — в одном PR |
| Новый RSS/Telegram-источник | Go-инженер | [`news-source-onboarding`](.kimi-code/skills/news-source-onboarding/SKILL.md) | Bootstrap LLM-разметки обязателен |
| README/ARCHITECTURE после изменения архитектуры | все | [`sync-readme-architecture`](.kimi-code/skills/sync-readme-architecture/SKILL.md) | Тем же PR |
| Выбор модели и диспатч субагентов | все | [`subagent-dispatch`](.kimi-code/skills/subagent-dispatch/SKILL.md) | Модель — только явным `model:` |

Скилы не отменяют правила 1–12 и [стандарт пакета импортёров](docs/IMPORTER_PACKAGE_STANDARD.md). Смешанный пункт (например, #184) делится по ролям в спеке/плане; Go-часть ревьюит Go-разработчик, ML-часть — ML-инженер.

## Обязательный порядок в начале сессии

1. Определи роль по таблице выше и прочитай её скил.
2. Прочитай владеющий роадмап и issue пункта: `<slug>/<item-id>`, критерий приёмки, `Depends on`, спека/план.
3. Прочитай только нужные разделы `ARCHITECTURE.md`: §2–§4 (структура, реестр, конвенции), §6.2/§6.3 (DDL/витрины), §6.5 (MCP), §6.8 (ML). Полное чтение файла не требуется.
4. Проверь worktree, ветку и активные локи. «Захвати» issue: комментарий «Taking …», `In Progress`, `Branch`, `Spec / Plan`.
5. Если спеки/плана нет — сначала создай их (`docs/superpowers/{specs,plans}/YYYY-MM-DD-<тема>*`), потом код.

## Разработка по роадмапу

1. **Один пункт — одна ветка — один PR.** WIP=1: не бери второй пункт, пока первый не смержен. Незапланированное — отдельный пункт роадмапа, а не «попутно».
2. **Зависимости.** Не начинай пункт с открытым `Depends on`; если заблокирован — `Blocked` и отчёт владельцу.
3. **Спека и план — первые коммиты ветки**, до кода. Реализация — в той же ветке; PR один и в конце, после зелёного `make all` и приёмки.
4. **Статусы.** `In Progress` — до первого коммита; `In Review` — при открытии PR; `Done` — только после мержа и протокола приёмки. Статус в роадмапе обновляется тем же PR.
5. **Владелец решает merge.** Промежуточные PR не открываются.

## Параллельные сессии

Над репозиторием одновременно работают несколько сессий; общие — GoLand, ClickHouse, Grafana и staging.

1. **Одна сессия — одно рабочее дерево — одна ветка — один пункт.** В корне репозитория вторая сессия не запускается.
2. **Общие ресурсы не гасятся и не деплоятся без явной просьбы владельца:** `make ch-down`, `make grafana-down`, `make deploy-staging*`.
3. **Один писатель на артефакт:** `ARCHITECTURE.md`, `sql/*`, `dashboard/*.json`, `docs/archive/registry.jsonl`, `.superpowers/sdd/<тема>/`.
4. Полный регламент (worktrees, локи, pre-commit) — пункт `infra/5.3`; до его мержа действуют эти минимальные правила.

## Жёсткие правила (нарушение = переделка)

1. **Секреты только через `os.Getenv`**. Никогда — в коде, комментариях, curl-примерах. Не читать и не печатать `~/.config/rosstat/env`.
2. **Вставка в ClickHouse только батчами**: `PrepareBatch` → `Append` → `Send`. Построчный `conn.Exec` в цикле запрещён.
3. **Никаких сетевых вызовов в `init()`** — URL и соединения вычисляются внутри `Import()`.
4. **HTTP — только через `util.HttpClient`/`GetXlsx`/`GetCSV`/`GetJSON`/`GetXML`** (нац. TLS, ротация UA, проверка статуса и дедлайн).
5. **DDL** — `CREATE TABLE IF NOT EXISTS`, `ReplacingMergeTree`, измерения `LowCardinality(String)`, новые денежные/ценовые значения — `Float64`.
6. **Ошибки не проглатывать**: каждый `strconv.Parse*`, `batch.Append`, `time.Parse` — с проверкой.
7. **Новые импортёры — по [стандарту](docs/IMPORTER_PACKAGE_STANDARD.md)** (§14 новый пакет, §15 рефакторинг); предпочтительно `util.ClickHouseImport`. `financial/` — legacy, новый код туда не добавлять.
8. **Новые таблицы — по каноническим DDL `ARCHITECTURE.md` §6.2**; изменение схемы = сначала обновить `ARCHITECTURE.md`.
9. **После кода `make all`** (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) — зелёный; те же проверки в CI.
10. **Изменил архитектуру, схему или конвенцию** — в том же PR обнови `ARCHITECTURE.md` и `README.md` (скил `sync-readme-architecture`); изменил план — владеющий роадмап.
11. **MCP-агент должен понимать каждый новый источник и ряд.** В том же PR: (а) `series_catalog` с `title`, `unit`, `frequency`, `origin`, `description`; (б) витрина `v_<источник>_*` с комментариями таблицы и колонок; (в) `GRANT SELECT` в `sql/mcp_kimi_reader.sql`; (г) проверка через MCP; (д) `ARCHITECTURE.md` §6.2/§6.3 и `README.md`.
12. **Трекер — GitHub.** Каждый открытый пункт имеет issue `<slug>/<item-id>`; заголовок `[<slug>/<item-id>] <короткое имя>`; поля Project V2 — по `docs/ROADMAP_INDEX.md`. Q&A — комментарием в issue (label `needs-answer`), PR содержит `Closes #N` **вне backticks**. После мержа PR, изменившего `ROADMAP_INDEX.md` или владеющий роадмап, — синхронизация по скилу `github-tracker` §10; issue автоматически не закрываются. GitHub-вызовы — MCP `mcp__github__*`, `gh` — фолбэк.

**Граница применения.** Правила 2–8 и 11 относятся к Go-коду; Python/ML не обходят их через ad-hoc `CREATE TABLE`, `INSERT`, `ALTER` или `GRANT`.

## Окружение разработки

- Ключи и пароли — в `~/.config/rosstat/env`; Makefile подключает файл сам (`-include`). Все команды с окружением — через `make`, не `go run`/`python` напрямую.
- Dev-ClickHouse запускает пользователь (`make ch-up`). Ассистент не делает `make ch-up`/`ch-down` без явной просьбы.
- Типовые цели: `make env-check`, `make ch-status`, `make import STAT=<имя>`, `make mcp-user`, `make mcp-check`, `make all`, `make docs-check`.
- Пароль `kimi_reader` не печатать; агент читает только витрины `v_*`.
- SQL к ClickHouse — только через MCP `mcp__clickhouse__run_query`; GoLand MCP — по `go-engineer`.

## Стиль коммитов и PR

- Коммиты: кратко, по-английски, в духе истории (`add fred importer`, `fix batch insert cbr_queries`).
- PR: одна фича = один импортёр/таблица; в описании — пункт `<slug>/<item-id>`, `Closes #N`, источник, схема, запуск, пример проверки.
- Протокол живой приёмки — **комментарием в PR** (состав — `docs/IMPORTER_PACKAGE_STANDARD.md` §16.1 п. 8).
- Логирование — logrus (`log.Infof("Imported %d rows of %s", ...)`), без `fmt.Print` в прод-коде.

## Календарь и приоритеты

Календарь триггеров — `ARCHITECTURE.md` §7. Приоритет — фазы владеющего роадмапа (`docs/ROADMAP_INDEX.md`); очередь открытых issue и разрывы — `docs/research/2026-10-19-engineering-issue-review.md`.
