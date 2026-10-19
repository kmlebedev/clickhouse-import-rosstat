---
name: subagent-dispatch
description: Use when dispatching implementation, review or exploration subagents in clickhouse-import-rosstat and you need the repo-specific model choice, escalation and prompt contract
type: prompt
whenToUse: When using superpowers:subagent-driven-development or executing-plans to dispatch coder/reviewer subagents in clickhouse-import-rosstat
---

# Диспатч субагентов в clickhouse-import-rosstat

**Назначение.** Зафиксировать выбор модели, эскалацию и обязательный контракт промпта, чтобы субагент не наследовал модель сессии молча и не терял правила репозитория. Это операционная политика, а не контракт кода; она не отменяет `AGENTS.md` и ролевые скилы.

## 1. Когда применять

- `superpowers:subagent-driven-development` или `superpowers:executing-plans`.
- Диспатч `coder`-субагента на реализацию, `task-reviewer` на ревью задачи, финальное ревью ветки.
- Любой вызов `Agent`, где важны модель, бюджет вывода или набор MCP-инструментов.

## 2. Модель — только явным `model:`

Пропущенный параметр молча наследует модель сессии. Ключ пиши ровно так, как он назван в пуле `[secondary_model.models]` файла `~/.kimi-code/config.toml`. На 2026-10-15 в пуле: `joingonka/zai-org/GLM-5.3-Flash`, `xiaomimimo/flash`, `deepseek/deepseek-flash`, `anthropic/haiku`.

Если ключа нет в пуле — диспатч падает с `Invalid model "…"`; не подменяй модель молча, скажи владельцу.

| Роль | Модель |
|---|---|
| Имплементер Go-кода — мелко-единичные задачи (критерий ниже) | `joingonka/zai-org/GLM-5.3-Flash` |
| Имплементер Go-кода — крупные задачи, и прочие задачи | `deepseek/deepseek-flash` |
| Имплементер документации — мелкие механические правки `.md` по образцу | `xiaomimimo/flash` |
| `task-reviewer` задачи и scoped re-review в fix-loop | `anthropic/haiku` |
| Финальное ревью всей ветки (`superpowers:requesting-code-review`) | `deepseek/deepseek-flash` |
| Fix-раунды 4–5 (эскалация по §Model Selection) | `deepseek/deepseek-flash` |

Пункт с кодом и доками сразу — по правилу кода. Ревью идёт не на той модели, что писала код; исключение — финальное ревью: оно берёт самую способную конфигурацию пула, `deepseek/deepseek-flash` с `support_efforts = [low, high, max]` и контекстом 1 048 576 при `[thinking].effort = "max"`. Если в сессии effort ниже `max` — скажи владельцу, модель не подменяй.

## 3. GLM — только мелко-единичные задачи

Критерий по статистике (754 субагент-прогона, 651 coder, 14 SDD-прогонов; 2026-10-16). `max_output_size = 8192` — потолок одного сообщения: он режет самый крупный **единичный** артефакт задачи (один `Write`/`Edit` файла или отчёта), а не задачу целиком. Замеренная цена вывода ≈ 380 output-токенов на 1 КБ артефакта, включая thinking (Go 8,8 КБ → 2,7К токенов; Go 24 КБ → 8,9К; тест 30 КБ → 19К).

- **Можно на GLM:** fix-раунды 1–3; правки одного существующего файла (парсер, вынос/ренейм хелперов, дописать тесты); проводка (реестр, шаг dagu, `GRANT SELECT`, MCP-проверки, DDL-фрагмент); каталоговые правки отдельными `Edit`; документы и отчёты — когда каждый единичный `Write`/`Edit` ≤ ≈12 КБ.
- **Нельзя на GLM:** новый импортёр-пакет целиком (тест 26–37 КБ не влезает физически), регенерация крупных файлов (`series_meta.go` 32,8 КБ), ревью задачи и финальное ревью ветки, многофайловые рефакторинги, любой единичный артефакт > ≈16 КБ; на большой дифф (270 КБ, ветка `rosstat`, 2026-10-14) GLM падала по таймауту и бюджету.
- **Дисциплина диспатча:** оцени по брифу самый крупный единичный артефакт — ≤ 12–16 КБ → GLM, иначе `deepseek/deepseek-flash`; при `providerFinishReason=truncated` не повторять на GLM, а эскалировать; большие файлы на GLM не дробить — канал медленный (~56 с/вызов против ~5 с у deepseek), в волне `cbr_dip_bulletin` (2026-10-15) обе GLM-задачи были остановлены и переназначены на deepseek.

## 4. `xiaomimimo/flash`

Только мелкие механические правки документов; Go-код на ней не писать. Данные на 2026-10-16: ни одной сданной задачи — единственная реальная проба (спека+план для `cbr_dip_bulletin`, тоже `.md`) остановлена через 25 минут без единого записанного файла (≈40К output-токенов ушло в рассуждения), один smoke-прогон упал с `error`; ~24 с/вызов против ~5 с у deepseek (большой лимит вывода — 131 072 — нулевую сдачу не компенсирует). Границы: только doc-правки по образцу (статусы роадмапа, счётчики, синхронизация чисел), один-два файла, вне критического пути; не отдавать ей спеки, планы, аналитику и многофайловые наборы документов. Если за ~10 минут нет ни одного `Write`/`Edit` или пошли API-ошибки — забирать на `deepseek/deepseek-flash`, не ждать. Ниша экспериментальная: первый реальный результат доложить владельцу.

## 5. Обязательный блок в промпте coder-субагента

Тип `coder` переопределён файлом `.kimi-code/agents/coder.md`: у него есть `mcp__jetbrains__*` и `mcp__clickhouse__*`. Типы `explore`/`plan` — без MCP, им работать через `Grep`/`Read`/`Edit`. В **каждый** промпт coder-субагента вписывай блок:

> У тебя есть инструменты `mcp__jetbrains__*` (GoLand MCP) и `mcp__clickhouse__*`. Используй `mcp__jetbrains__*` для проверки правок и навигации вместо grep и лишней сборки; SQL к ClickHouse — только через `mcp__clickhouse__run_query`. Если вдруг MCP-инструментов в твоём наборе нет — скажи об этом в отчёте (`DONE_WITH_CONCERNS`), не молчи.

Скил `subagent-driven-development` не пробрасывает `AGENTS.md` субагенту автоматически — инструкцию надо вписать в диспатч-промпт руками, иначе она до субагента не доедет. Также впиши ролевой скил: `go-engineer`, `python-engineer`, `ml-engineer` или `research-engineer`.

## 6. Чего не делать

- Не пропускать `model:` — иначе модель сессии унаследуется молча.
- Не давать GLM крупный единичный артефакт > ≈16 КБ.
- Не давать `xiaomimimo/flash` спеки, планы, аналитику или Go-код.
- Не ревьюить код на модели автора; финальное ревью — на сильнейшей модели.
- Не заводить новые типы субагентов без решения владельца.

## 7. Ссылки

- [AGENTS.md](../../../AGENTS.md), [go-engineer](../go-engineer/SKILL.md), [python-engineer](../python-engineer/SKILL.md), [ml-engineer](../ml-engineer/SKILL.md), [research-engineer](../research-engineer/SKILL.md).
- `superpowers:subagent-driven-development`, `superpowers:requesting-code-review`, `superpowers:verification-before-completion`.
