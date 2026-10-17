# Живой GitHub Project V2 — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Создать живой GitHub Project V2 для пяти роадмапов, завести поля/виды, бэкфиллить 57 открытых пунктов и разложить 18 существующих issue.

**Architecture:** Project V2 принадлежит `@me`, привязан к `kmlebedev/clickhouse-import-rosstat`. Поля и виды — по спеке `2026-10-18-github-project-v2-design.md`. Issue создаются по конвенции 3.1 (`[slug/item-id]`), добавляются в проект и получают `Roadmap`/`Item`/`Status`. Существующие issue либо маппятся, либо остаются в `Inbox`; четыре явно принятых закрываются.

**Tech Stack:** `gh project` (scope `project`), MCP `mcp__github__projects_*` для чтения/видов, `gh issue`, Python-скрипты идемпотентного бэкфилла.

**Spec:** `docs/superpowers/specs/2026-10-18-github-project-v2-design.md`

## Global Constraints

- Источник истины — файлы роадмапов; GitHub — журнал и вид.
- Заголовок issue: `[<slug>/<item_id>] <title>`; labels `roadmap:<slug>`.
- 59 строк открытого фронта; 2 уже покрыты (#105, #47); новых issue — 57.
- Исторические `✅` issue не получают.
- Закрываются только #86, #75, #74, #42 с ссылкой на приёмку.
- Проект и поля — ровно по §3 спеки; дефолтный `Status` заменяется.
- Views — по §4 спеки; фильтр проверяется через `gh project item-list --query`.
- Секреты, токены, пароли в команды/доки не попадают.
- Go-код, DDL, Makefile, CI не меняются; `make all` остаётся зелёным.
- Основной worktree занят веткой `infra-github-pages` — работаем в `.worktrees/github-project-v2`.

## Review Focus

- Дубли: перед созданием issue проверять точный заголовок; #105/#47 не дублировать.
- Статусы: `✅ сделан этим PR` → `In Review`; `Done` — только после мержа и протокола.
- Inbox: `Roadmap` пуст, `Status=Todo`; не выдумывать роадмап для идей вне реестра.
- Views: `roadmap:...`, `status:"In Progress","In Review"`, `no:roadmap` — проверить API.
- Закрытия: только четыре issue и только со ссылкой на приёмку.
- Параллельная сессия `infra-github-pages`: не трогать её ветку/файлы; #105 только добавить в проект.

---

### Task 1: Спека и план

**Files:**
- Create: `docs/superpowers/specs/2026-10-18-github-project-v2-design.md`
- Create: `docs/superpowers/plans/2026-10-18-github-project-v2.md`

- [x] **Step 1: Записать спеку**
- [x] **Step 2: Записать план**
- [ ] **Step 3: Проверить ссылки**

Run: `test -f docs/superpowers/specs/2026-10-18-github-project-v2-design.md && test -f docs/superpowers/plans/2026-10-18-github-project-v2.md`
Expected: exit 0.

- [ ] **Step 4: Коммит**

```bash
git add docs/superpowers/specs/2026-10-18-github-project-v2-design.md docs/superpowers/plans/2026-10-18-github-project-v2.md
git commit -m "docs: add live github project design and plan"
```

---

### Task 2: Project V2, поля и labels

**Files:** внешние (GitHub), изменений в репозитории нет.

**Interfaces:**
- Produces: project number/id/url; field IDs/options; labels.

- [ ] **Step 1: Создать проект**

```bash
gh project create --owner @me --title "clickhouse-import-rosstat — Roadmap" --format json
gh project link <number> --owner @me --repo kmlebedev/clickhouse-import-rosstat
```

- [ ] **Step 2: Посмотреть дефолтные поля**

```bash
gh project field-list <number> --owner @me --format json
```

- [ ] **Step 3: Заменить дефолтный `Status`**

Удалить поле `Status` (Todo/In Progress/Done) и создать заново:

```bash
gh project field-delete --id <status-field-id>
gh project field-create <number> --owner @me --name "Status" --data-type SINGLE_SELECT \
  --single-select-options "Todo,In Progress,In Review,Done,Blocked"
```

- [ ] **Step 4: Создать остальные поля**

`Roadmap` (SINGLE_SELECT: `dcf-polyus,gold-thesis,dcf-metals,key-rate,infra`), `Phase` (TEXT), `Item` (TEXT), `Branch` (TEXT), `PR` (TEXT), `Spec / Plan` (TEXT), `Acceptance` (TEXT), `Depends on` (TEXT), `Priority` (SINGLE_SELECT: `P0,P1,P2`), `Target date` (DATE).

- [ ] **Step 5: Создать labels**

`roadmap:dcf-polyus`, `roadmap:gold-thesis`, `roadmap:dcf-metals`, `roadmap:key-rate`, `needs-answer` (цвета — по образцу `roadmap:infra`; `roadmap:infra` уже есть).

- [ ] **Step 6: Проверить**

```bash
gh project field-list <number> --owner @me --format json
gh label list --limit 200 | grep -E 'roadmap:|needs-answer'
```

---

### Task 3: Бэкфилл 57 новых issue

**Files:** внешние (GitHub); манифест — `/tmp/github-tracker-review/open-items.json` (в репозиторий не коммитится).

- [ ] **Step 1: Загрузить манифест**

`python3 -c "import json; d=json.load(open('/tmp/github-tracker-review/open-items.json')); print(len(d))"` → 59.

- [ ] **Step 2: Создать issue идемпотентно**

Скрипт читает манифест, для каждой строки:
- пропускает `existing_issue != null`;
- проверяет `gh issue list --state all --search 'in:title "<title>"'`, не создаёт дубль;
- создаёт issue с заголовком `[<slug>/<item_id>] <title>`, телом по шапке спеки §5, label `roadmap:<slug>`;
- сохраняет `item_id → issue number` в `/tmp/github-tracker-review/created-issues.json`.

- [ ] **Step 3: Проверить количество**

```bash
python3 -c "import json; d=json.load(open('/tmp/github-tracker-review/created-issues.json')); print(len(d))"
```

Ожидается 57 (или 59 минус уже существующие). Повторный прогон — 0 новых.

- [ ] **Step 4: Спот-проверка 5 issue**

Открыть 5 issue разных роадмапов, проверить заголовок, шапку, label и ссылку на источник.

---

### Task 4: Триаж 18 существующих issue

**Files:** внешние (GitHub).

- [ ] **Step 1: Добавить labels и поля для #105/#90/#47/#46**
- [ ] **Step 2: Закрыть #86/#75/#74/#42**

Комментарий закрытия — ссылка на пункт/PR/приёмку (README/ROADMAP). Не закрывать остальные.

- [ ] **Step 3: Оставить #89/#88/#85/#81/#77/#76/#58/#52/#51/#48 в `Inbox`**

`Roadmap` пуст, `Status=Todo`.

- [ ] **Step 4: Проверить**

`gh issue list --state open --limit 200 --json number,title,labels` — 14 открытых (18 минус 4 закрытых).

---

### Task 5: Добавить items в Project и заполнить поля

**Files:** внешние (GitHub).

- [ ] **Step 1: Добавить все issue в проект**

75 items = 57 новых + 18 существующих. Использовать `gh project item-add <number> --owner @me --url <issue-url>`; сохранить `issue number → item id`.

- [ ] **Step 2: Заполнить поля**

Для каждого item — `gh project item-edit --url <issue-url> --field "<name>" --value "<option>"` (single select) или `--text` (text):
- `Roadmap` — slug; `Phase`; `Item`; `Status`;
- `Spec / Plan` — пути; `Acceptance` — критерий; `Depends on` — `#N`/составной ID;
- `Branch`/`PR` — только для существующих активных (#105, #90);
- `Priority`/`Target date` — пусто.

`Inbox`-items получают только `Status=Todo`.

- [ ] **Step 3: Проверить**

`gh project item-list <number> --owner @me --limit 100 --format json` — 75 items; у не-Inbox непустые `Roadmap`/`Item`/`Status`.

---

### Task 6: Views

**Files:** внешние (GitHub).

- [ ] **Step 1: Проверить фильтры**

```bash
gh project item-list <number> --owner @me --query 'roadmap:key-rate' --limit 5
gh project item-list <number> --owner @me --query 'status:"In Progress","In Review"' --limit 5
gh project item-list <number> --owner @me --query 'no:roadmap' --limit 5
gh project item-list <number> --owner @me --query 'roadmap:dcf-polyus,gold-thesis' --limit 5
```

- [ ] **Step 2: Создать views через MCP `projects_write`**

`Все пункты`, пять роадмапов, `Gold`, `В работе`, `Ждут ответа`, `Inbox`, `Roadmap` (roadmap layout). Если фильтр отклонён — не создавать вид, записать отклонение в протокол.

- [ ] **Step 3: Проверить**

`mcp__github__projects_get` (method `get_project`, `get_project_view`) — все views на месте.

---

### Task 7: Живая приёмка, документация, PR

**Files:**
- Modify: `docs/ROADMAP_INFRA.md` (3.2 → `✅`, ссылка на project и числа)
- Modify: `docs/ROADMAP_INDEX.md` (ссылка на project, число items)
- Modify: `docs/superpowers/specs/2026-10-18-github-project-v2-design.md` (фактический URL проекта)

- [ ] **Step 1: Проверить через MCP**

`projects_list` видит проект; `projects_get` отдаёт поля и views; `gh project item-list` — 75 items.

- [ ] **Step 2: Проверить counts**

57 новых issue, 14 открытых существующих, 75 items; 4 закрытых issue.

- [ ] **Step 3: Обновить документацию**

`ROADMAP_INFRA.md` 3.2: `✅` с датой, командами и результатом; `ROADMAP_INDEX.md`: URL проекта и фактическое число items.

- [ ] **Step 4: `make all`**

Run: `make all`
Expected: exit 0.

- [ ] **Step 5: Коммит и PR**

```bash
git add docs/ROADMAP_INFRA.md docs/ROADMAP_INDEX.md docs/superpowers/specs/2026-10-18-github-project-v2-design.md
git commit -m "docs: record live github project acceptance"
git push -u origin feat/github-project-v2
gh pr create --base main --head feat/github-project-v2 --title "docs: bootstrap github project v2" --body-file /tmp/github-tracker-review/pr-3.2.md
```

PR: `Closes #<issue 3.2>`, проект/поля/виды, counts, протокол.

- [ ] **Step 6: Протокол комментарием в PR**

Команды, числа items/issue, поля/виды, MCP-проверки, `make all`.

---

## Проверка плана (self-review)

- **Spec coverage:** проект/поля/виды (Task 2/6), бэкфилл (3), триаж (4), поля items (5), приёмка (7).
- **Step scan:** каждая задача заканчивается проверяемым результатом.
- **Consistency:** `slug/item_id` совпадает с `ROADMAP_INDEX.md`; #105/#47 не дублируются; статусы по спеке 3.1.
- **Proportion:** план короче спеки; внешние операции описаны командами.
