---
name: go-engineer
description: Use when the task is Go code in clickhouse-import-rosstat — an importer package, DDL, ClickHouse view, grants, cmd/ingest, Makefile/CI, deploy or dagu — and you need the repo-specific implementation and review cycle
type: prompt
whenToUse: When working on Go importers, ClickHouse schema/views/grants, ingest endpoint, Makefile/CI, deploy or DAG schedules in clickhouse-import-rosstat
---

# Go-инженер в clickhouse-import-rosstat

**Назначение.** Маршрут Go-задачи от ready-issue до merge: что прочитать, как выбрать пункт, как реализовать по стандарту, что проверить и что считается готовым результатом. Скил — надстройка над `AGENTS.md`, а не его замена.

**Канонические документы.** Архитектура и DDL — [ARCHITECTURE.md](../../../ARCHITECTURE.md); стандарт пакета — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md); роадмапы — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md); трекер — [github-tracker](../github-tracker/SKILL.md); обзор issue — [2026-10-19-engineering-issue-review.md](../../../docs/research/2026-10-19-engineering-issue-review.md).

## 1. Когда применять

- Новый импортёр или рефакторинг существующего пакета.
- DDL, витрина, комментарии, `GRANT SELECT`, `sql/mcp_kimi_reader.sql`.
- `cmd/ingest`, Makefile, CI-workflow, deploy/dagu.
- Любая задача, где ответ — Go-код или схема ClickHouse.

**Когда не применять:** ML-эксперимент (→ `ml-engineer`), не-ML Python (→ `python-engineer`), разведка источника (→ `research-engineer`), дашборд (→ `dashboard-migration`).

## 2. Что прочитать перед стартом

1. [AGENTS.md](../../../AGENTS.md) — общий контракт, правила 1–12.
2. Владеющий роадмап и issue пункта — [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md); не делай работу вне пункта.
3. [ARCHITECTURE.md](../../../ARCHITECTURE.md) §2–§4 и нужный §6.x: §6.2 DDL, §6.3 витрины, §6.5 MCP, §6.7 дашборды, §6.8 ML.
4. [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md): §14 новый пакет, §15 рефакторинг, §16 приёмка.
5. Спеку и план пункта; если их нет — создай до кода.

## 3. Границы роли

| Go-инженер делает | Не делает |
|---|---|
| Go-пакеты, импортёры, DDL, views, grants, `cmd/ingest` | ML-эксперименты и признаки |
| Makefile/CI, deploy, dagu | ad-hoc Python-скрипты без пункта |
| Каталог рядов, комментарии, MCP-check | Изменение схемы без спеки |
| Винтажный архив, `source_publications` | Прямую запись в ClickHouse из Python/ML |

Смешанный пункт (например, #184) делится по ролям в спеке/плане; Go-часть ревьюит Go-разработчик.

## 4. Цикл 12 шагов

1. **Выбрать ready-пункт:** `Status = Todo`, все `Depends on` закрыты, нет claim, приоритет фазы. Очередь — review-документ §5.
2. **Захватить issue:** комментарий «Taking `<slug>/<item-id>`; branch `<type>/<slug>`; worktree `.worktrees/<slug>`», затем `In Progress`, `Branch`, `Spec / Plan`.
3. **Изолироваться:** worktree + ветка от `main`. До `infra/5.3` — `git worktree add .worktrees/<slug> -b feat/<slug> main`; после — `make wt-new NAME=<slug> TYPE=feat BASE=main`. В корне не работать.
4. **Прочитать минимум** по §2; не читать `ARCHITECTURE.md` целиком.
5. **Спека/план — первыми коммитами**, если их нет.
6. **Реализовать по стандарту:** один импортёр/таблица на PR; батчи; DDL; каталог; витрина; комментарии; `GRANT`.
7. **Проверить живьём:** `make import STAT=...`, число строк, крайние даты, повтор no-op, `make mcp-check`.
8. **Прогнать CI-набор:** `make all`, `make docs-check`; для dagu — `dagu validate`.
9. **Ревью:** task-reviewer на другой модели; fix-loop; финальное ревью на сильнейшей модели (`subagent-dispatch`).
10. **Один PR в конце:** item, `Closes #N`, источник, схема, запуск, пример проверки; протокол — комментарием.
11. **После мержа:** `Done` только после протокола; синхронизация роадмапа (правило 12); удалить worktree.
12. **Стоп-правила:** приёмка не проверена → «не проверено»; объём вырос → новый пункт роадмапа; противоречие issue/роадмапа → Q&A и стоп.

## 5. Чек-лист импортёра

**Новый пакет — стандарт §14:**

- [ ] пакет `<domain>/`, файл импортёра, `Name()` с префиксом домена;
- [ ] регистрация в `chimport.Stats` — в реестре пакета (`<domain>/importers.go`), не персональным `init()` в файле импортёра (стандарт §8.1);
- [ ] blank-import пакета в `main.go` (иначе `init()` не выполнится, а `make import` скажет «no importer named»);
- [ ] транспорт через `util.*`, разбор с проверкой ошибок, батч-вставка;
- [ ] `TableName`, ключ `ReplacingMergeTree`, `CreateTable` через шаблон;
- [ ] `SeriesMeta`/`series_catalog` для каждого ряда;
- [ ] витрина `v_<источник>_*`, комментарии таблицы и колонок;
- [ ] `GRANT SELECT` в `sql/mcp_kimi_reader.sql`;
- [ ] тесты парсера и HTTP-слоя на фикстурах (`httptest`, подмена base URL); для schema-only — drift DDL + энфорсер реестра + регулярка dagu;
- [ ] `dagu/<домен>.yaml`; новый шаг — в хвосте цепочки;
- [ ] `docs/IMPORTERS.md`, `ARCHITECTURE.md` (§6.1/§6.2, список имён), `README.md` при изменении контракта;
- [ ] живая приёмка и протокол в PR.

**Рефакторинг — стандарт §15:** сохранить поведение, сначала тесты/фикстуры, затем перевод на `util.ClickHouseImport`; не менять имена таблиц/рядов без отдельного пункта.

## 5.1. Особые случаи

- **Общая каноническая таблица.** Если импортёр пишет в существующую таблицу (`company_financials`), `Name()` не равен `TableName`; нужен свой `Import()`/обёртка, а не прямое встраивание шаблона. Персональные витрины `v_<эмитент>_*` не заводятся — используются общие `v_company_*` (ARCHITECTURE §6.3). Тест-энфорсер пакета держит литеральный allowlist общей таблицы и допускает `Name() != TableName` (стандарт §8.2/§8.3).
- **Schema-only импортёр.** `util.ClickHouseImport` без `ImportFunc`/`BodyFunc`/`CrawFunc` падает; для пустого шага нужен свой `Import()` (прецедент — `mlregistry`): DDL + `Imported 0 rows`. Минимальные тесты: drift DDL, энфорсер реестра, регулярка dagu; парсер/фикстуры появятся на шаге с данными.
- **Многошаговый пункт и правило 11.** Каталог/витрина/грант обязательны, если текущий шаг поставляет ряды. Если ряды появляются на следующем шаге, это явно фиксируется в спеке и критерии приёмки; «забыли» — не вариант.
- **Копии канонического DDL.** Новый писатель в общую таблицу добавляет drift-тест против канонического DDL и обновляет `ARCHITECTURE.md` §6.2 (число копий и список).
- **Dagu без источника.** Шаг нового пакета допустим, если он безопасен (DDL + 0 строк); иначе — named follow-up, а не «пустой» шаг в проде.
- **Приёмка 0-строчного импортёра.** `make import` дважды → `Imported 0 rows`, `count()` не изменился; `make mcp-check` — регрессия, а не доказательство данных.
- **Зависимости.** Проверь issue state и Project `Status` через MCP; открытая зависимость → `Blocked`, отчёт владельцу, код не начинать. Решение предыдущего шага должно быть записано в **файле роадмапа**, а не только в закрытом issue (например, step-2 MAGN в `ROADMAP_DCF_METALS.md`); step-3 = #160.
- **Окружение.** ClickHouse не отвечает → попроси владельца; `dagu` нет → «не проверено»; после изменения грантов — `make mcp-user` и `make mcp-check`.
- **Имя пакета.** `metals/` vs `steel/` фиксируется в спеке до ветки.
- **Ревью.** Мелкий docs-only пункт можно inline по AGENTS.md; код — через `subagent-dispatch`, task-reviewer на другой модели.

## 6. Go-правила (2–9)

1. **Батчи:** один `PrepareBatch` на прогон, `Append` с проверкой, `Send`; на пути ошибки — `Abort`, иначе соединение останется в пуле.
2. **DDL:** `CREATE TABLE IF NOT EXISTS`, `ReplacingMergeTree`, `LowCardinality(String)`, `Float64` для денег/цен.
3. **HTTP:** только `util.HttpClient`/`GetXlsx`/`GetCSV`/`GetJSON`/`GetXML`; `GetJSON`/`GetXML` проверяют статус и держат дедлайн.
4. **Сеть:** никаких вызовов в `init()`; URL и клиенты — внутри `Import()`.
5. **Ошибки:** `strconv.Parse*`, `time.Parse`, `batch.Append` — всегда с проверкой; не глотать `err`.
6. **Секреты:** только `os.Getenv`; не печатать и не комментировать.
7. **Legacy:** `financial/` — не добавлять новый код.
8. **Идемпотентность:** повторный импорт — no-op; для редко обновляемых источников — `PublicationCountPoints`/`Source`, `source_publications`, архив версии.

## 7. Команды

| Цель | Команда |
|---|---|
| Формат/линт/тесты/сборка | `make all` |
| Один импортёр | `make import STAT=<имя>` |
| Восстановление из архива | `make archive-restore STAT=<имя> ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` |
| MCP end-to-end | `make mcp-check` |
| Docs-портал | `make docs-check` |
| Dagu | `dagu validate dagu/<домен>.yaml` |
| Dev-ClickHouse | `make ch-status`; поднимает пользователь |

## 8. Выбор открытого issue

Очередь на 2026-10-19 (детали — review-документ §5):

1. `infra/1.2` #109 → `1.3` #110 → `1.4` #111.
2. `dcf-metals/step-1..8` #158–#165 (step-3 = #160) — последовательно.
3. `key-rate/phase-2` #113/#114/#115/#118/#119/#120/#121/#142 — независимые импортёры.
4. `gold-thesis` #173, #143–#153.
5. `dcf-polyus` #131–#141 — проверить `Depends on`.
6. `infra/4.3` #184 — Go-часть после ML-спеки.
7. Research-first #77/#81/#85/#51/#58/#48/#76.

Правило: один агент — один пункт; не брать пункт с открытым `Depends on`; не дублировать claim.

## 9. GoLand MCP

- Проверяй свой тул-набор: есть ли `mcp__jetbrains__*`.
- Если есть — `get_file_problems`/`lint_files` для проверки правок, `get_symbol_info` для сигнатур, `search_symbol`/`analyze_calls` для навигации, `rename_refactoring` для переименований.
- В worktree передавай `projectPath` = абсолютный путь своего дерева; результаты отбирай по префиксу `.worktrees/<slug>/…` (выдача IDE — объединение открытых проектов).
- Если инструментов нет: GoLand запущен, но MCP недоступен — сообщи о поломке канала; GoLand не запущен — работай `Grep`/`Read`/`Edit` и скажи об этом одной строкой.
- Терминал IDE, run-конфигурации и `build_project` не использовать: команды — через `make`.

## 10. Definition of Done

- [ ] Пункт и issue названы; спека/план — первые коммиты.
- [ ] Код по стандарту §14/§15; один импортёр/таблица на PR.
- [ ] Пакет зарегистрирован в `chimport.Stats` и импортирован в `main.go`.
- [ ] Каталог, витрина, комментарии, `GRANT`, MCP-check (для schema-only — по §5.1).
- [ ] Живая приёмка: команда, строки, даты, повтор no-op; для schema-only — `Imported 0 rows` и неизменный `count()`.
- [ ] `make all`, `make docs-check`; `dagu validate`, если расписание.
- [ ] README/ARCHITECTURE/IMPORTERS синхронны.
- [ ] Протокол — комментарием в PR; статус роадмапа — тем же PR.
- [ ] Нет секретов, ad-hoc DDL и изменений вне пункта.

## 11. Частые ошибки

| Ошибка | Почему плохо | Как правильно |
|---|---|---|
| Нет no-op/идемпотентности | Повторный прогон перезаписывает/качает заново | `ReplacingMergeTree` + префлайт/гейт; проверить повтор |
| Забыт `series_catalog` | MCP-агент не понимает ряд | `SeriesMeta` для каждого ряда + тест полноты |
| Забыт `GRANT`/комментарии | Агент не видит витрину | `sql/mcp_kimi_reader.sql`, `MODIFY COMMENT`, `COMMENT COLUMN`, `make mcp-check` |
| Нет `source_publications` у редко обновляемого источника | Ежедневный прогон качает файл зря | `PublicationCountPoints`/`Source`, HEAD-префлайт |
| Хрупкий парсер | Смена подписи/опечатки ломает импорт | Фикстуры, приём двух вариантов подписей (`commet`, `standart`) |
| Игнор лимита источника | КБД ЦБ ≈5 лет на запрос | Окна по 4 года, инкремент по `max(date)` |
| Нет браузерного UA | Минфин отдаёт 503 | `util.HttpClient` с UA |
| Пустой `archive_key` | Приёмка e-disclosure не проходит | Включить архив в прогоне приёмки |
| Новый шаг dagu в начале цепочки | Падение нового шага лишает данных проверенные | Новый шаг — в хвосте |
| «Staging проверен» без SSH | Ложная приёмка | Писать «не проверено», оставить владельцу |
| `Closes #N` в backticks | GitHub не связывает PR и issue | Вне code formatting |
| `make all` считают полной проверкой Python/ML | Python-тесты не входят | Для Python/ML — их собственные наборы |

## 12. Ревью-чек-лист

- [ ] Диф не выходит за пункт; нет «попутных» рефакторингов.
- [ ] Батчи, DDL, ошибки, секреты, `init()` — по правилам 1–9.
- [ ] Каталог/витрина/комментарии/грант — по правилу 11.
- [ ] Тесты: парсер, HTTP, идемпотентность, фикстуры.
- [ ] Документация: `IMPORTERS`, `ARCHITECTURE`, `README`, `CONFIGURATION`/`OPERATIONS` при необходимости.
- [ ] Приёмка: команда, результат, no-op, MCP.
- [ ] PR: item, `Closes #N`, протокол комментарием.

## 13. Ссылки

- [AGENTS.md](../../../AGENTS.md), [ARCHITECTURE.md](../../../ARCHITECTURE.md), [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md).
- [ROADMAP_INDEX.md](../../../docs/ROADMAP_INDEX.md), [engineering issue review](../../../docs/research/2026-10-19-engineering-issue-review.md).
- [github-tracker](../github-tracker/SKILL.md), [subagent-dispatch](../subagent-dispatch/SKILL.md), [sync-readme-architecture](../sync-readme-architecture/SKILL.md).
