# Role Skills Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сделать `AGENTS.md` тонким роутером ролей, добавить скилы Go/Python/Research и `subagent-dispatch`, зафиксировать review 36 закрытых и 64 открытых issue и проверяемый цикл работы по issue.

**Architecture:** `AGENTS.md` — общий контракт и таблица «триггер → роль → скил»; роль-специфичная глубина — в `.kimi-code/skills/*/SKILL.md`; review-документ хранит доказательства и очередь; шаблоны issue/PR и `github-tracker` закрепляют `Role` и claim protocol.

**Tech Stack:** Markdown, `.github` templates, `docs/site-pages.json`, `make docs-check`, `make all`; без изменений Go/Python/ClickHouse.

**Spec:** `docs/superpowers/specs/2026-10-19-role-skills-design.md`

## Global Constraints

- Рабочая директория — worktree `.worktrees/role-skills`, ветка `feat/role-skills`; главный worktree не трогать.
- Спека и план — первые коммиты ветки; один PR в конце.
- Не менять Go/Python/ML-код, схему ClickHouse, импортёры, витрины и гранты.
- Не заводить новые типы субагентов и шестой роадмап.
- `AGENTS.md` — только роутер и общий контракт, ≤ 120 строк; нумерация правил 1–12 сохраняется.
- Все относительные ссылки новых файлов должны разрешаться; `make docs-check` и `make all` — зелёные.
- Секреты не читать и не печатать; команды с окружением — через `make`.

## Review Focus

1. **Роли не дублируются:** Go-правила — в `go-engineer`, Python — в `python-engineer`, Research — в `research-engineer`, ML — в `ml-engineer`; `AGENTS.md` только маршрутизирует.
2. **Нумерация 1–12 не изменилась:** на неё ссылаются docs, роадмапы и скилы.
3. **Issue-workflow проверяем:** ready-фильтр, claim, worktree, зависимости, стоп-правила.
4. **Нет ложных обещаний:** CI-долг `5.2` и parallel-sessions `5.3` явно вне объёма.
5. **Трекер:** issue #211, Project V2 и `infra/5.1` согласованы; `5.2`/`5.3` — `Todo`.

---

### Task 1: Роадмап, спека, план

**Files:** Modify `docs/ROADMAP_INFRA.md`, `docs/ROADMAP_INDEX.md`; create `docs/superpowers/specs/2026-10-19-role-skills-design.md`, `docs/superpowers/plans/2026-10-19-role-skills.md`.

- [ ] **Step 1: Роадмап.** Добавить фазу 5 `5.1`–`5.3` в `ROADMAP_INFRA.md`; расширить схему `infra` в `ROADMAP_INDEX.md`.
- [ ] **Step 2: Спека и план.** Создать файлы по шаблону `2026-10-19-ml-engineer-skill*`.
- [ ] **Step 3: Issue.** Создать #211 (`5.1`), #212 (`5.2`), #213 (`5.3`); добавить в Project V2; `5.1` — `In Progress`, `Branch`, `Spec / Plan`, `Acceptance`, `Depends on`.
- [ ] **Step 4: Commit** `docs: spec and plan for role skills`.

### Task 2: Review-документ

**Files:** Create `docs/research/2026-10-19-engineering-issue-review.md`; modify `docs/site-pages.json`.

- [ ] **Step 1: Закрытые issue.** Таблица по трём группам (данные/импортёры, ML, инфра/трекер/docs), результаты и уроки.
- [ ] **Step 2: Scorecard.** Практики: roadmap-driven, one item/PR, spec/plan first, acceptance protocol, CI gates, tracker sync, catalog/grants, idempotency, archive, parallel isolation, role skills.
- [ ] **Step 3: Разрывы.** Ролевая маршрутизация, Python CI, gitleaks, branch protection, skills-check, WIP/claim, parallel-sessions, Inbox.
- [ ] **Step 4: Открытые issue.** Портфель по ролям, зависимости, очередь.
- [ ] **Step 5: Рекомендации Go-агентам.** 12-шаговый цикл, стоп-правила, метрики.
- [ ] **Step 6: Commit** `docs: review engineering issues and role workflow`.

### Task 3: `AGENTS.md`

**Files:** Rewrite `AGENTS.md`.

- [ ] **Step 1: Роутер.** Таблица «триггер → роль → скил → границы».
- [ ] **Step 2: Порядок сессии.** 5 шагов, включая минимальное чтение `ARCHITECTURE.md`.
- [ ] **Step 3: Роадмап и issue.** WIP=1, `Depends on`, claim, статусы.
- [ ] **Step 4: Параллельные сессии.** Краткий блок без ссылки на невлитый документ.
- [ ] **Step 5: Правила 1–12.** Сохранить нумерацию, сжать Go-детали.
- [ ] **Step 6: Окружение, трекер, стиль PR, приоритеты, таблица скилов.**
- [ ] **Step 7: Commit** `docs: make AGENTS.md a role router`.

### Task 4: Скилы

**Files:** Create `.kimi-code/skills/{go-engineer,python-engineer,research-engineer,subagent-dispatch}/SKILL.md`; modify `.kimi-code/skills/ml-engineer/SKILL.md`, `.kimi-code/agents/coder.md`.

- [ ] **Step 1: Go.** Цикл 12 шагов, стандарт §14/§15, Go-правила, GoLand MCP, очередь, DoD, частые ошибки.
- [ ] **Step 2: Python.** Границы записи, `uv`-пины, офлайн-тесты, payload, DoD, CI-долг.
- [ ] **Step 3: Research.** Протокол разведки, evidence-правила, research-документ, handoff, DoD.
- [ ] **Step 4: Dispatch.** Перенести модель-политику и промпт coder-субагента из `AGENTS.md`.
- [ ] **Step 5: ML cross-links.** Python/Research-границы без дублирования.
- [ ] **Step 6: coder.md.** 3–4 строки про ролевые скилы.
- [ ] **Step 7: Commit** `docs: add role skills`.

### Task 5: Шаблоны и указатели

**Files:** Modify `.github/ISSUE_TEMPLATE/roadmap-item.yml`, `.github/pull_request_template.md`, `.kimi-code/skills/github-tracker/SKILL.md`, `README.md`, `docs/DEVELOPMENT.md`, `ARCHITECTURE.md`.

- [ ] **Step 1: Issue template.** `Role` dropdown + шапка `Role:`.
- [ ] **Step 2: PR template.** Role-specific DoD.
- [ ] **Step 3: Tracker.** `Role` и claim protocol.
- [ ] **Step 4: README.** Таблица ролей и скилов.
- [ ] **Step 5: DEVELOPMENT.** Раздел «Роли и скилы» + статус Python CI.
- [ ] **Step 6: ARCHITECTURE §8.** Ссылка на ролевые скилы.
- [ ] **Step 7: Commit** `docs: route roles in templates and docs`.

### Task 6: Проверки и PR

**Files:** Modify `docs/ROADMAP_INFRA.md` (статус `5.1`).

- [ ] **Step 1: Ссылки.** Проверить относительные пути новых файлов.
- [ ] **Step 2: Docs.** `make docs-check` — зелёный.
- [ ] **Step 3: CI.** `make all` — зелёный.
- [ ] **Step 4: Тест скилов.** Read-only субагенты: `go-engineer` + #160; `python-engineer` + задача на `scripts/*`; `research-engineer` + #81/#77. Проверить наличие ролей, границ, команд, DoD; при пропусках — дополнить и повторить.
- [ ] **Step 5: Статус.** `infra/5.1` — ✅ с командой и результатом; Project V2 — `In Review`.
- [ ] **Step 6: PR.** Один PR, `Closes #211`; протокол приёмки — комментарием.

## Self-Review

- **Покрытие спеки:** §2–§4 → Task 3/4/5; §5 → Task 2/5; §7 → Task 6.
- **Согласованность:** review-документ и скилы ссылаются на одни канонические документы; `AGENTS.md` не дублирует глубину.
- **Review Focus:** каждый пункт проверяется тестом скилов и `make docs-check`.
- **Пропорция:** docs-only; код и схема не меняются.
