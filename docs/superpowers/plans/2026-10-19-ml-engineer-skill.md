# ML Engineer Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Зафиксировать процесс ML-инженера в отдельном скиле, коротком указателе `AGENTS.md` и review-документе по 14 ML-issue, не раздувая общий промпт и не меняя ML-код.

**Architecture:** Скил `.kimi-code/skills/ml-engineer/SKILL.md` маршрутизирует ML-задачи и ссылается на канонические документы; `AGENTS.md` получает только блок «ML-задачи»; review-документ хранит доказательства и рекомендации; пункт `infra/4.5` связывает всё в роадмапе и GitHub-трекере.

**Tech Stack:** Markdown, `docs/site-pages.json`, `make docs-check`, `make all`; без изменений Go/Python/ClickHouse.

**Spec:** `docs/superpowers/specs/2026-10-19-ml-engineer-skill-design.md`

## Global Constraints

- Рабочая директория — worktree `.worktrees/ml-agent-skill`, ветка `feat/ml-agent-skill`; главный worktree не трогать.
- Спека и план — первые коммиты ветки; один PR в конце.
- Не менять ML-код, схему ClickHouse, импортёры, витрины и гранты.
- Не заводить шестой роадмап и новый тип субагента.
- `AGENTS.md` — только маршрутизация, ≤ 10 строк; методология — в скиле и канонических документах.
- Все относительные ссылки в новых файлах должны разрешаться; `make docs-check` и `make all` — зелёные.
- Секреты не читать и не печатать; все команды с окружением — через `make`.

## Review Focus

1. **Границы:** ML-агент не делает ad-hoc DDL и не пишет в сырые таблицы; Go-разработчик не обязан читать ML-скил.
2. **Нет дублирования:** скил ссылается на `docs/guides/stat-ml-models.md` и `docs/research/2026-10-18-ml-experiment-management.md`, а не переписывает их.
3. **Нет ложных обещаний:** `champion` alias — не deployment; staging MLflow не проверен; MCS/SPA и model card — gap/future.
4. **Очередь:** #184 → #191 → gold C1–C4 → #147; смежные #124/#125/#145 — после, как challengers.
5. **Трекер:** issue #209, Project V2 и `infra/4.5` согласованы; статус меняется тем же PR.

---

### Task 1: Пункт роадмапа, спека и план

**Files:** Modify `docs/ROADMAP_INFRA.md`, `docs/ROADMAP_INDEX.md`; create `docs/superpowers/specs/2026-10-19-ml-engineer-skill-design.md`, `docs/superpowers/plans/2026-10-19-ml-engineer-skill.md`.

- [x] **Step 1: Роадмап.** Добавить `infra/4.5` в §5 фазы 4; расширить схему `infra` в `ROADMAP_INDEX.md` до `4.1`–`4.5`.
- [x] **Step 2: Спека и план.** Создать файлы по шаблону `2026-10-18-ml-quick-runs*`.
- [x] **Step 3: Issue.** Создать `[infra/4.5]` (#209), добавить в Project V2, выставить `In Progress`, `Branch`, `Spec / Plan`, `Acceptance`, `Depends on`.
- [x] **Step 4: Commit** `docs: spec and plan for ml engineer skill`.

### Task 2: Review-документ

**Files:** Create `docs/research/2026-10-19-ml-issue-review.md`.

- [x] **Step 1: Закрытые issue.** Таблица 7 пунктов (#123, #127, #178, #183, #189, #190, #197): результат, артефакты, урок.
- [x] **Step 2: Scorecard.** Сопоставить практики (предрегистрация, walk-forward/purge/embargo, DM-HLN/CI, детерминизм, паспорт run, MLflow, промоушен/live, PIT, MCS/SPA, model card) с состоянием и разрывом.
- [x] **Step 3: Открытые issue.** Очередь #184 → #191 → #154–#157 → #147; смежные #124/#125/#145; зависимости и следующий шаг.
- [x] **Step 4: Формат и рекомендации.** Почему SKILL + указатель в AGENTS.md; пошаговые рекомендации ML-агенту; источники.
- [x] **Step 5: Commit** `docs: review ml issues and recommendations`.

### Task 3: Скил ML-инженера

**Files:** Create `.kimi-code/skills/ml-engineer/SKILL.md`.

- [x] **Step 1: Frontmatter.** `name: ml-engineer`, `description` с «Use when…», `type: prompt`, `whenToUse`.
- [x] **Step 2: Разделы 1–5.** Когда применять; что прочитать; границы ML/Go; цикл 10 шагов; жёсткие правила.
- [x] **Step 3: Разделы 6–10.** Команды; промоушен/live; открытая очередь; Definition of Done; частые ошибки.
- [x] **Step 4: Ссылки.** Проверить каждый относительный путь; объём 150–200 строк.
- [x] **Step 5: Commit** `docs: add ml engineer skill`.

### Task 4: Указатели и docs-портал

**Files:** Modify `AGENTS.md`, `README.md`, `docs/site-pages.json`.

- [x] **Step 1: AGENTS.md.** Блок «ML-задачи» после «Обязательный порядок в начале сессии»: скил, приоритет правил 1–12, Go-стандарт для DDL/импортёров.
- [x] **Step 2: README.md.** Строка рядом со ссылкой на `github-tracker`.
- [x] **Step 3: site-pages.json.** Запись review-документа в секции «Исследования».
- [x] **Step 4: Commit** `docs: route ml tasks to skill`.

### Task 5: Проверки, статус и PR

**Files:** Modify `docs/ROADMAP_INFRA.md` (статус `4.5`).

- [x] **Step 1: Docs.** `make docs-check` — зелёный.
- [x] **Step 2: CI.** `make all` — зелёный.
- [x] **Step 3: Ссылки.** Проверить относительные пути скила и review-документа.
- [x] **Step 4: Тест скила.** Read-only субагент с задачей «составь план для #184»; проверить наличие предрегистрации, `ML_LOG`/`MLFLOW`, DM/CI, `forecast_log`, gate, границ с Go; при пропусках — дописать скил и повторить.
- [x] **Step 5: Статус.** `infra/4.5` — ✅ с командой и результатом; Project V2 — `In Review` при открытии PR.
- [ ] **Step 6: PR.** Один PR, `Closes #209`, описание пункта; протокол приёмки — комментарием в PR.

## Self-Review

- **Покрытие спеки:** §3 → Task 3; §4 → Task 2; §5 → Task 4; §6 → Task 5.
- **Согласованность:** review-документ и скил ссылаются на одни и те же канонические документы; AGENTS.md не дублирует методологию.
- **Review Focus:** каждый пункт проверяется тестом скила и `make docs-check`.
- **Пропорция:** это docs-only пункт; код и схема не меняются.
