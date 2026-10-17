# README entry-point restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сделать `README.md` читаемой точкой входа, вынеся полные спецификации импортёров и эксплуатационные разделы в семь файлов `docs/`, без потери фактов.

**Architecture:** Дословная экстракция блоков README по диапазонам строк в `docs/IMPORTERS.md`, `CONFIGURATION.md`, `OPERATIONS.md`, `DEVELOPMENT.md`, `GOLD_CONTOUR.md`, `LIMITATIONS.md`, `GRAFANA.md`; README переписывается как компактная landing page с картой документации и компактными таблицами. Новые страницы добавляются в манифест docs-портала.

**Tech Stack:** Markdown, существующий генератор docs-портала (`scripts/docs_build.py`, `docs/site-pages.json`), mkdocs-material 9.7.7.

**Spec:** `docs/superpowers/specs/2026-10-18-readme-structure-design.md`

## Global Constraints

- Перенос **дословный**: никаких сокращений, пересказов, изменения чисел/URL/команд.
- README — до ~300 строк; подробности — только в `docs/`.
- Все новые файлы лежат в `docs/`; ссылка на входную точку — `[README.md](../README.md)`.
- `docs/superpowers/**`, `docs/plans/**`, `docs/archive/**` на docs-портале не публикуются.
- `make all` не должен зависеть от docs; `make docs-check` и `make docs-build` — зелёные.
- Секреты не печатать; переносить только имена переменных.

## Review Focus

- Полнота: каждый раздел исходного README либо остался в README, либо перенесён в один новый файл; ни один факт не потерян.
- Ссылки: README → новые docs, новые docs → README/ARCHITECTURE, docs-портал — всё разрешается `--check` и `mkdocs --strict`.
- Дубли: детальная таблица импортёров только в `docs/IMPORTERS.md`; полная таблица команд только в `docs/DEVELOPMENT.md`; полный env — только в `docs/CONFIGURATION.md`.
- Читаемость README: быстрый старт и карта документации видны в начале; нет простыней на 100+ строк.
- Docs-портал: 7 новых страниц в манифесте, в `build/site` нет исключённых каталогов.

---

### Task 1: Дословная экстракция в `docs/`

**Files:**
- Create: `docs/IMPORTERS.md`, `docs/CONFIGURATION.md`, `docs/OPERATIONS.md`, `docs/DEVELOPMENT.md`, `docs/GOLD_CONTOUR.md`, `docs/LIMITATIONS.md`, `docs/GRAFANA.md`
- Source: `README.md` (диапазоны строк из спеки §3)

- [ ] **Step 1: Зафиксировать исходные диапазоны**

Проверить `grep -n '^## \|^### ' README.md` и убедиться, что границы совпадают со спекой §3:
`7–166`, `228–256`, `257–262`, `263–297`, `298–351`, `352–377`, `378–437`, `438–469`, `470–500`, `502–553`, `555–562`, `563–579`, `580–627`, `628–732`.

- [ ] **Step 2: Извлечь блоки**

Одноразовым stdlib-скриптом (не коммитится) скопировать диапазоны в новые файлы с заголовками:

```text
IMPORTERS:        # Импортёры и источники + строки 9–166 (таблица и абзацы)
CONFIGURATION:    # Конфигурация и окружение + 230–256 + 440–469 + 502–553
OPERATIONS:       # Эксплуатация + 259–262 + 354–377 + 472–500
DEVELOPMENT:      # Разработка + 300–351 + 380–437 + 555–562 + 565–579
GOLD_CONTOUR:     # Контур прогноза золота + 265–297
LIMITATIONS:      # Ограничения и известные проблемы + 582–627
GRAFANA:          # Grafana + 630–732
```

В каждом файле после заголовка — строка `> Часть документации проекта; входная точка — [README.md](../README.md).`

- [ ] **Step 3: Проверить полноту**

Сверить число строк: сумма новых файлов + оставшиеся блоки README ≥ исходных 732 строк (плюс заголовки). Выборочно сверить 10 контрольных строк (команды `make import STAT=...`, числа `502/502`, `22 421`, URL `cbr.ru/...`, `403`, `29.09.2026`) в исходном README и новом файле — совпадают дословно.

- [ ] **Step 4: Commit**

```bash
git add docs/IMPORTERS.md docs/CONFIGURATION.md docs/OPERATIONS.md docs/DEVELOPMENT.md docs/GOLD_CONTOUR.md docs/LIMITATIONS.md docs/GRAFANA.md
git commit -m "docs: extract importer specs and runbooks from readme"
```

---

### Task 2: Переписать README как точку входа

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Собрать новый README**

Структура (см. спеку §3):
1. Заголовок и 2–3 абзаца «что это» (сохранить строки 1–5, обновить ссылки на новые docs).
2. `## Документация` — таблица-карта: README, ARCHITECTURE, IMPORTERS, CONFIGURATION, OPERATIONS, DEVELOPMENT, LIMITATIONS, GRAFANA, GOLD_CONTOUR, IMPORTER_PACKAGE_STANDARD, ROADMAP_INDEX, AGENTS + ссылка на docs-портал.
3. `## Быстрый старт` — сохранить строки 205–223.
4. `## Что импортируется` — компактная таблица ~14 строк; ссылка на `docs/IMPORTERS.md`; без длинных абзацев.
5. `## Структура репозитория` — 12–15 верхнеуровневых строк; ссылка на `ARCHITECTURE.md` §2.
6. `## Конфигурация` — обязательные переменные и ссылка на `docs/CONFIGURATION.md`.
7. `## Оркестрация (Dagu)` — 2–3 строки, ссылка на `docs/OPERATIONS.md`.
8. `## Контур прогноза золота` — 3–4 строки, ссылка на `docs/GOLD_CONTOUR.md`.
9. `## Разработка` — 8–10 ключевых целей, ссылка на `docs/DEVELOPMENT.md`.
10. `## Ограничения` — 1–2 строки, ссылка на `docs/LIMITATIONS.md`.
11. `## Grafana` — 1–2 строки, ссылка на `docs/GRAFANA.md`.
12. `## CI и релизы` — 2–3 строки, ссылка на `docs/DEVELOPMENT.md#ci-и-релизы`.

- [ ] **Step 2: Проверить читаемость**

`wc -l README.md` — ≤ 300. Первые 60 строк содержат «что это», карту документации и быстрый старт.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: make readme an entry point"
```

---

### Task 3: Манифест docs-портала, ARCHITECTURE и роадмап

**Files:**
- Modify: `docs/site-pages.json`
- Modify: `ARCHITECTURE.md`
- Modify: `docs/ROADMAP_INFRA.md`
- Modify: `docs/ROADMAP_INDEX.md`

- [ ] **Step 1: Добавить 7 страниц в манифест**

Разделы: «Импортёры» — `importers.md`; «Эксплуатация» — `configuration.md`, `operations.md`, `gold-contour.md`, `grafana.md`; «Разработка» — `development.md`, `limitations.md`. Порядок — как в README.

- [ ] **Step 2: ARCHITECTURE**

В §2 добавить строки новых файлов и абзац «Карта документации»: README — входная точка; `docs/IMPORTERS.md` — спецификации; `docs/CONFIGURATION.md`, `docs/OPERATIONS.md`, `docs/DEVELOPMENT.md`, `docs/LIMITATIONS.md`, `docs/GRAFANA.md`, `docs/GOLD_CONTOUR.md` — соответствующие разделы.

- [ ] **Step 3: Роадмап**

`docs/ROADMAP_INFRA.md` §4 — строка `3.4` с критерием из спеки §5; `docs/ROADMAP_INDEX.md` — `3.1`–`3.4`.

- [ ] **Step 4: Commit**

```bash
git add docs/site-pages.json ARCHITECTURE.md docs/ROADMAP_INFRA.md docs/ROADMAP_INDEX.md
git commit -m "docs: register readme structure and site pages"
```

---

### Task 4: Проверка и PR

- [ ] **Step 1: Локальные проверки**

```bash
python3 scripts/docs_build.py --check
make docs-check
make docs-build
make all
```

Ожидается: `checked N pages`; тесты OK; `mkdocs build --strict` без WARNING; `build/site` содержит новые страницы; `make all` зелёный.

- [ ] **Step 2: Проверить ссылки**

`python3`-скриптом проверить внутренние ссылки `build/site/*.html` (кроме `404.html`): 0 битых. Отдельно — ссылки README на новые docs: все файлы существуют.

- [ ] **Step 3: Issue и PR**

- Issue `[infra/3.4] README как точка входа: спецификации — в docs/` с `Closes` в PR.
- PR в `main`: пункт, спека/план, что перенесено, что осталось в README, проверки, `Closes #N`.
- Протокол — комментарием в PR после зелёных проверок.

---

## Self-review

- **Spec coverage:** §3 (7 файлов) — Task 1; §3 README — Task 2; §3 manifest/ARCHITECTURE/roadmap — Task 3; §5 acceptance — Task 4.
- **Step scan:** каждый шаг имеет проверяемый результат (диапазоны, `wc -l`, `--check`, `mkdocs --strict`, ссылки).
- **Type consistency:** имена файлов и страниц манифеста совпадают со спекой; ссылки README и манифеста — на те же пути.
- **Review Focus:** полнота, ссылки, дубли, читаемость, docs-портал — покрыты проверками Task 1–4.
- **Proportion:** план фиксирует структуру и проверки, а не переписывает 732 строки в текст плана.
