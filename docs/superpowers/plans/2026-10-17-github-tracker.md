# GitHub-трекер роадмапов — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Зафиксировать в репозитории модель GitHub-трекера для пяти роадмапов: реестр, составной ID, поля/виды Project, статусы, владение, зависимости, синхронизацию, шаблоны и операционный скил.

**Architecture:** `docs/ROADMAP_INDEX.md` — единый реестр роадмапов и правил ID; `AGENTS.md` правило 12 — обязательная норма; `.kimi-code/skills/github-tracker/SKILL.md` — операционные шаги; `.github/ISSUE_TEMPLATE/roadmap-item.yml` и `.github/pull_request_template.md` — формы; `README.md`/`ARCHITECTURE.md` — ссылки на норму. Живой Project V2 и бэкфилл — отдельный пункт `infra/3.2`.

**Tech Stack:** Markdown, GitHub Issue Forms (YAML), GitHub Project V2 через MCP/`gh` (в шаге 3.2).

**Spec:** `docs/superpowers/specs/2026-10-17-github-tracker-design.md`

## Global Constraints

- Источник истины — файлы роадмапов; GitHub — журнал и вид.
- Составной ID: `<slug>/<item-id>`; слаги: `dcf-polyus`, `gold-thesis`, `dcf-metals`, `key-rate`, `infra`.
- Заголовок issue: `[<slug>/<item-id>] <короткое имя>`.
- Пункт живёт ровно в одном роадмапе; в других — ссылка.
- `Done` — только после мержа PR и протокола приёмки; `✅ сделан этим PR` = `In Review`.
- В этом пункте нет изменений Go-кода и схемы ClickHouse; `make all` должен остаться зелёным.
- Секреты, токены и реальные пароли в документы не попадают.
- Один пункт = одна ветка = один PR; PR открывается в конце.

## Review Focus

- Коллизии ID: `1.1` в `infra` и `key-rate` не должны смешиваться; заголовок issue всегда содержит слаг.
- Границы владения: EIA/WGC/bold — `gold-thesis`, ставка — `key-rate`, металлурги — `dcf-metals`, архив/приёмка — `infra`.
- Статусы: `🔶`/«срез» → `In Progress`, `✅ сделан этим PR` → `In Review`, `Done` только после мержа и протокола.
- Ссылки: все относительные ссылки в новых документах ведут на существующие файлы; `AGENTS.md` не пересказывает спеку, а ссылается.
- Шаблон issue form: YAML валиден, обязательные поля не ломают создание issue.

---

### Task 1: Спека и план

**Files:**
- Create: `docs/superpowers/specs/2026-10-17-github-tracker-design.md`
- Create: `docs/superpowers/plans/2026-10-17-github-tracker.md`

**Interfaces:**
- Produces: спека — источник требований для задач 2–4; план — порядок работ и критерии проверки.

- [x] **Step 1: Записать спеку**
- [x] **Step 2: Записать план**
- [x] **Step 3: Проверить ссылки**

Run: `test -f docs/superpowers/specs/2026-10-17-github-tracker-design.md && test -f docs/superpowers/plans/2026-10-17-github-tracker.md`
Expected: exit 0.

- [x] **Step 4: Коммит**

```bash
git add docs/superpowers/specs/2026-10-17-github-tracker-design.md docs/superpowers/plans/2026-10-17-github-tracker.md
git commit -m "docs: add github tracker design and plan"
```

---

### Task 2: Реестр роадмапов и пункт infra/3.1

**Files:**
- Create: `docs/ROADMAP_INDEX.md`
- Modify: `docs/ROADMAP_INFRA.md` (шапка; новая фаза 3 после фазы 2; §5 — приёмка фазы 3)

**Interfaces:**
- Produces: `docs/ROADMAP_INDEX.md` — таблица «слаг → файл → владение → схема ID», правило владения, составной ID, краткая карта статусов, ссылки на спеку и скил.
- Produces: `docs/ROADMAP_INFRA.md` пункт `3.1` со статусом `✅ сделан этим PR (2026-10-17)` и пункт `3.2` «не начато».

- [x] **Step 1: Создать `docs/ROADMAP_INDEX.md`**

Содержание:

1. Назначение: единый реестр пяти роадмапов и правил трекера.
2. Таблица: слаг, файл, роль/граница владения, схема item-id.
3. Правило владения: пункт живёт ровно в одном роадмапе; в других — ссылка.
4. Составной ID `<slug>/<item-id>` и шаблон заголовка issue.
5. Краткая карта статусов (`Todo`/`In Progress`/`In Review`/`Done`/`Blocked`).
6. Ссылки: спека `docs/superpowers/specs/2026-10-17-github-tracker-design.md`, скил `.kimi-code/skills/github-tracker/SKILL.md`, `AGENTS.md` правило 12.
7. Правило добавления нового пункта: сначала правка владеющего роадмапа, затем issue; новый роадмап — только решением владельца.

- [x] **Step 2: Добавить в `docs/ROADMAP_INFRA.md` фазу 3**

После таблицы фазы 2 и до `## 5. Приёмка и риски`:

Шаблон для вставки в `docs/ROADMAP_INFRA.md`; пути в ссылках относительны `docs/` — в плане это code block, а не кликабельные ссылки.

```markdown
## 4. Фаза 3 — Трекер пунктов и связи с GitHub

| Шаг | Содержание | Статус | Критерий приёмки |
|---|---|---|---|
| **3.1** | **Модель трекера зафиксирована в репозитории**: `docs/ROADMAP_INDEX.md` (реестр пяти роадмапов, слаги, владение, составной ID), правило 12 `AGENTS.md`, скил `.kimi-code/skills/github-tracker/SKILL.md`, шаблоны `.github/ISSUE_TEMPLATE/roadmap-item.yml` и `.github/pull_request_template.md`; спека [2026-10-17-github-tracker-design.md](superpowers/specs/2026-10-17-github-tracker-design.md), план [2026-10-17-github-tracker.md](superpowers/plans/2026-10-17-github-tracker.md). Живой Project V2 и бэкфилл — шаг 3.2 | ✅ сделан этим PR (2026-10-17) | Все пять роадмапов описаны в индексе; правило AGENTS.md ссылается на индекс и скил; шаблоны существуют; `make all` зелёный; ссылки в README/ARCHITECTURE синхронны |
| 3.2 | **Живой GitHub Project V2 и бэкфилл**: scope `project`, toolset `projects`, создание проекта, поля/виды, бэкфилл открытого фронта и триаж существующих issue | не начато | Project виден через MCP; поля и виды соответствуют спеке; открытый фронт заведён; существующие issue разложены по `Roadmap`/`Inbox` |
```

Ruling: нумерация разделов — `## 4` (фаза) и `## 5` (приёмка), потому что `## 3` занят фазой 2.

- [x] **Step 3: Обновить шапку `ROADMAP_INFRA.md`**

В строке «Роль документа» добавить «трекер пунктов и процесс приёмки»; в «Сопутствующие документы» добавить ссылку на `ROADMAP_INDEX.md`.

- [x] **Step 4: Проверить ссылки и таблицу**

Run: `test -f docs/ROADMAP_INDEX.md && grep -q '3.1' docs/ROADMAP_INFRA.md && grep -q 'ROADMAP_INDEX.md' docs/ROADMAP_INFRA.md`
Expected: exit 0.

- [x] **Step 5: Коммит**

```bash
git add docs/ROADMAP_INDEX.md docs/ROADMAP_INFRA.md
git commit -m "docs: add roadmap registry and tracker item"
```

---

### Task 3: Норма в AGENTS.md и операционный скил

**Files:**
- Modify: `AGENTS.md` (шапка; порядок начала сессии; правило 1; правило 6; новое правило 12; раздел «Стиль коммитов и PR»)
- Create: `.kimi-code/skills/github-tracker/SKILL.md`

**Interfaces:**
- Consumes: `docs/ROADMAP_INDEX.md` из задачи 2.
- Produces: правило 12 — обязательная норма; скил — пошаговые операции.

- [x] **Step 1: Обновить `AGENTS.md`**

1. В шапке (строка 3) добавить ссылку на `docs/ROADMAP_INDEX.md` рядом с планом.
2. В «Обязательный порядок в начале сессии» добавить пункт: «Реестр роадмапов и правила ID — `docs/ROADMAP_INDEX.md`; трекер — правило 12».
3. В правиле 1 заменить перечисление трёх файлов на «реестр всех роадмапов — `docs/ROADMAP_INDEX.md`».
4. В правиле 6 заменить «в соответствующем `docs/GOLD_THESIS_DATA_ROADMAP.md` или `docs/ROADMAP_DCF_POLYUS.md`» на «в соответствующем роадмапе (список — `docs/ROADMAP_INDEX.md`)».
5. Добавить правило 12:

```markdown
12. **Трекер пунктов роадмапа — GitHub.** Каждый пункт роадмапа имеет issue с составным ID `<slug>/<item-id>`; заголовок — `[<slug>/<item-id>] <короткое имя>`; поля Project V2 (`Roadmap`, `Phase`, `Item`, `Status`, `Branch`, `PR`, `Spec / Plan`, `Acceptance`, `Depends on`) — по `docs/ROADMAP_INDEX.md`. Пункт живёт ровно в одном роадмапе; в других файлах — ссылка. Q&A дублируется в issue комментарием (label `needs-answer`), PR содержит `Closes #N`, статус `Done` — только после мержа и протокола приёмки. Операции — скил `.kimi-code/skills/github-tracker/SKILL.md`, модель — спека `docs/superpowers/specs/2026-10-17-github-tracker-design.md`.
```

6. В «Стиль коммитов и PR» добавить пункт: «В описании PR — роадмап и пункт (`<slug>/<item-id>`) и `Closes #N`; протокол живой приёмки — комментарием в PR (см. правило 12).»

- [x] **Step 2: Создать скил `.kimi-code/skills/github-tracker/SKILL.md`**

Frontmatter:

```yaml
---
name: github-tracker
description: Operate the GitHub issue and Project V2 tracker for roadmap items in clickhouse-import-rosstat
type: prompt
whenToUse: When starting, updating, or closing a roadmap item; creating sub-issues; mirroring Q&A; auditing roadmap issues
---
```

Разделы:

1. Назначение и источник истины (файлы роадмапов; GitHub — журнал).
2. Реестр: ссылка на `docs/ROADMAP_INDEX.md`; таблица слагов.
3. Создание issue: `gh issue create` или MCP `issue_write`; заголовок; шапка тела; labels `roadmap:<slug>`, `needs-answer`.
4. Project V2: `projects_get` для ID полей и опций; `projects_write` методы `add_project_item`, `update_project_item`; значения полей.
5. Sub-issue: `sub_issue_write` method `add`; один уровень; заголовок Task.
6. Q&A: формат `### Q<N>`, label, резолюция; при изменении состава фаз — правка роадмапа.
7. PR: `Closes #N`, `Status = In Review`, поле `PR`; после мержа — `Done` и статус в роадмапе.
8. Аудит: список открытых пунктов каждого файла vs issue; отчёт «нет issue / нет пункта / дубль / статус разошёлся».
9. Чего не делать: дубли, issue на historical `✅`, перенос спек в тело.
10. До шага 3.2: если Project недоступен, вести issue и labels, не блокировать работу.

- [x] **Step 3: Проверить скил**

Run: `test -f .kimi-code/skills/github-tracker/SKILL.md && grep -q '^name: github-tracker' .kimi-code/skills/github-tracker/SKILL.md`
Expected: exit 0.

- [x] **Step 4: Коммит**

```bash
git add AGENTS.md .kimi-code/skills/github-tracker/SKILL.md
git commit -m "docs: add roadmap tracker rule and skill"
```

---

### Task 4: Шаблоны и синхронизация README/ARCHITECTURE

**Files:**
- Create: `.github/ISSUE_TEMPLATE/roadmap-item.yml`
- Create: `.github/pull_request_template.md`
- Modify: `README.md` (вступление; структура репозитория; раздел «Разработка»)
- Modify: `ARCHITECTURE.md` (§2 структура; §8 правила)
- Modify: `docs/IMPORTER_PACKAGE_STANDARD.md` (§16.1 п. 8 — ссылка на трекер)

**Interfaces:**
- Consumes: `docs/ROADMAP_INDEX.md`, `AGENTS.md` правило 12.
- Produces: формы issue/PR и перекрёстные ссылки в обзорных документах.

- [x] **Step 1: Создать `.github/ISSUE_TEMPLATE/roadmap-item.yml`**

Issue Form с полями: `roadmap` (dropdown, пять значений), `item-id` (input, required), `phase` (input), `acceptance` (textarea, required), `spec` (input), `plan` (input), `depends-on` (input), `context` (textarea). Заголовок — `[<slug>/<item-id>] `. Без обязательных labels, чтобы форма работала до шага 3.2.

- [x] **Step 2: Создать `.github/pull_request_template.md`**

Разделы: «Пункт роадмапа» (`<slug>/<item-id>`, `Closes #N`), «Что сделано», «Проверка» (чек-лист `make all`, живая приёмка, `make mcp-check` при схеме/грантах, статус в роадмапе, README/ARCHITECTURE), «Протокол живой приёмки».

- [x] **Step 3: Обновить `README.md`**

1. Во вступлении (строка ~5) добавить: «Реестр всех роадмапов и правила трекера — в `docs/ROADMAP_INDEX.md`».
2. В «Структура репозитория» в строке `docs/` упомянуть `ROADMAP_INDEX.md`, спеки и планы; при необходимости добавить строку `.kimi-code/skills/`.
3. В разделе «Разработка» после таблицы команд добавить абзац: «Пункт роадмапа ведётся через GitHub issue с составным ID; шаблоны — `.github/ISSUE_TEMPLATE/roadmap-item.yml` и `.github/pull_request_template.md`; операции — скил `github-tracker`; правила — `docs/ROADMAP_INDEX.md` и правило 12 `AGENTS.md`.»

- [x] **Step 4: Обновить `ARCHITECTURE.md`**

1. В §2 в строке `docs/` добавить `ROADMAP_INDEX.md` и `superpowers/{specs,plans}`.
2. В §8 добавить пункт 15: «Трекер пунктов роадмапа — `docs/ROADMAP_INDEX.md` + скил `github-tracker`; issue с составным ID, Project V2 — вид; источник истины — файлы роадмапов.»

- [x] **Step 5: Обновить `docs/IMPORTER_PACKAGE_STANDARD.md` §16.1 п. 8**

Добавить в конец пункта: «Связь протокола с пунктом роадмапа — по `docs/ROADMAP_INDEX.md`: issue с ID `<slug>/<item-id>`, PR содержит `Closes #N`; после мержа статус пункта — `Done` в Project V2.»

- [x] **Step 6: Проверить ссылки и YAML**

Run: `test -f .github/ISSUE_TEMPLATE/roadmap-item.yml && test -f .github/pull_request_template.md && grep -q 'ROADMAP_INDEX.md' README.md && grep -q 'ROADMAP_INDEX.md' ARCHITECTURE.md`
Expected: exit 0.

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/ISSUE_TEMPLATE/roadmap-item.yml')); print('yaml ok')"`
Expected: `yaml ok` (если PyYAML нет — `ruby -e "require 'yaml'; YAML.load_file('.github/ISSUE_TEMPLATE/roadmap-item.yml'); puts 'yaml ok'"`).

- [x] **Step 7: Коммит**

```bash
git add .github/ISSUE_TEMPLATE/roadmap-item.yml .github/pull_request_template.md README.md ARCHITECTURE.md docs/IMPORTER_PACKAGE_STANDARD.md
git commit -m "docs: add roadmap issue and PR templates"
```

---

### Task 5: Проверка и финальное ревью

**Files:**
- Modify: `docs/ROADMAP_INFRA.md` (только если проверка вскрыла дефект)
- Все файлы задач 2–4 — при fix-раундах.

**Interfaces:**
- Consumes: все предыдущие задачи.
- Produces: зелёный `make all`, отчёт проверки, PR.

- [ ] **Step 1: Прогнать `make all`**

Run: `make all`
Expected: gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки — зелёные; изменений кода нет, но проверка обязательна.

- [ ] **Step 2: Проверить ссылки и отсутствие секретов**

Run: `git diff --check && ! git diff --name-only | grep -E '(^|/)\.env$'`
Expected: exit 0.

- [ ] **Step 3: Проверить полноту модели**

Вручную сверить:

- все пять слагов встречаются в `ROADMAP_INDEX.md`;
- примеры ID не коллизируют;
- статусы `✅`/`❌`/`⏳`/`🔶`/«не начато»/нет маркера отображены;
- правило владения разрешает EIA/WGC, ставку, металлургов, infra;
- шаблоны issue/PR ссылаются на `ROADMAP_INDEX.md`.

- [ ] **Step 4: Финальное ревью ветки**

Dispatch: `anthropic/haiku` — task review; `deepseek/deepseek-flash` — final whole-branch review (по правилам AGENTS.md).

- [ ] **Step 5: Открыть PR**

Заголовок: `docs: add multi-roadmap GitHub tracker model`

В описании: пункт `infra/3.1`, ссылка на issue, что зафиксировано, проверка `make all`, ссылка на спеку/план, `Closes #<issue>`.

---

## Проверка плана (self-review)

- **Spec coverage:** реестр (задача 2), ID (2–3), поля/виды (3, скил), статусы (2–3), sub-issues/Q&A/зависимости (3), синхронизация (3), бэкфилл (3, шаг 3.2 в роадмапе), шаблоны (4), README/ARCHITECTURE (4), проверка (5).
- **Step scan:** каждая задача заканчивается проверяемым результатом; код не пишется.
- **Type consistency:** слаг `github-tracker` совпадает в спеке, плане, AGENTS.md и скиле; пути совпадают.
- **Review Focus:** коллизии ID, владение, статусы, ссылки, YAML — вынесены в ревью.
- **Proportion:** план короче спеки; реализации кода нет.
