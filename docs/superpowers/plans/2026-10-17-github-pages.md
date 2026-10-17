# GitHub Pages docs portal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать и опубликовать docs-портал `https://kmlebedev.github.io/clickhouse-import-rosstat/` из канонических `README.md`, `ARCHITECTURE.md` и `docs/*.md` без второй копии документов.

**Architecture:** Манифест `docs/site-pages.json` перечисляет публикуемые страницы; stdlib-генератор `scripts/docs_build.py` собирает `build/site-docs/` с баннерами и переписанными ссылками и генерирует `build/mkdocs.yml`; `mkdocs-material` 9.7.7 строит статический сайт; GitHub Actions собирает его на PR и публикует на `main` через `actions/deploy-pages`.

**Tech Stack:** Python 3 stdlib (генератор и тесты), mkdocs-material 9.7.7 / MkDocs 1.6.1, GitHub Actions Pages, Material for MkDocs.

**Spec:** `docs/superpowers/specs/2026-10-17-github-pages-design.md`

## Global Constraints

- Пункт роадмапа: `infra/3.3`; ветка `infra-github-pages`; один PR в конце.
- Публикуются только страницы из манифеста; `docs/superpowers/**`, `docs/plans/**`, `docs/archive/**` не попадают на сайт ни при каких условиях.
- Пины: `mkdocs-material==9.7.7` (тянет `mkdocs>=1.6,<2`); Python в CI — 3.12; локально генератор обязан работать на Python 3.9 (системный `python3`).
- Генератор — только stdlib; никаких сетевых вызовов; вывод детерминирован (повторный прогон — байт-идентичный).
- Секреты не читать и не публиковать; `docs/archive/registry.jsonl` исключён.
- `site_url` — `https://kmlebedev.github.io/clickhouse-import-rosstat/`; `use_directory_urls: false`.
- `make all` не должен зависеть от Python/mkdocs; docs-цели отдельные.
- Коммиты — по-английски, кратко, в духе истории (`docs: add github pages build`).

## Review Focus

- Относительные ссылки на спеки/планы и якоря: после переписывания они должны вести на GitHub или на страницу сайта, а `--check` — падать с `файл:строка` на неразрешимой.
- Исключённые каталоги: ни `superpowers/`, ни `plans/`, ни `archive/` не должны появиться в `build/site`, даже если манифест ошибочно их упомянет.
- Детерминированность: повторный `--out` не меняет ни один байт; порядок навигации — порядок манифеста.
- `mkdocs build --strict`: предупреждения (битые ссылки, неизвестные якоря) валят сборку, а не проходят молча.
- Python 3.9: не использовать `X | Y`, `match`, `tomllib`; `from __future__ import annotations` обязателен.

---

### Task 1: Манифест, валидация и разрешение ссылок

**Files:**
- Create: `docs/site-pages.json`
- Create: `scripts/docs_build.py`
- Test: `scripts/docs_build_test.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Page: source: str; page: str; title: str; section: str`
  - `load_manifest(path: Path) -> list[Page]`
  - `validate_manifest(pages: list[Page], repo_root: Path) -> list[str]`
  - `build_page_map(pages: list[Page]) -> dict[str, str]`
  - `resolve_target(source: str, target: str, page_map: dict[str, str], repo_root: Path, repo_url: str) -> tuple[Optional[str], Optional[str]]`
  - `rewrite_links(text: str, source: str, page_map: dict[str, str], repo_root: Path, repo_url: str) -> tuple[str, list[str]]`
  - `REPO_URL = "https://github.com/kmlebedev/clickhouse-import-rosstat"`
  - `EXCLUDED_PREFIXES = ("docs/superpowers/", "docs/plans/", "docs/archive/")`
  - `PAGE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*\.md$")`
- Consumes: nothing.

- [ ] **Step 1: Создать манифест**

`docs/site-pages.json` — массив в порядке навигации. Разделы: `Обзор`, `Архитектура`, `Контур ключевой ставки`, `DCF и золото`, `Справочники`, `Роадмапы`, `Исследования`, `Статьи`. Записи — по таблице §5 спеки. Для семи файлов `docs/research/` слаги взять из имён файлов без даты-префикса, например `research-2026-10-16-cpi-weekly-pre2022-probe.md`. Для двух статей — `article-gold-q3-2026.md` и `article-gold-diesel-2026-10-12.md`.

- [ ] **Step 2: Написать падающие тесты**

`scripts/docs_build_test.py` на `unittest`; вспомогательная `make_repo(tmp: Path) -> Path` создаёт:

```text
README.md
ARCHITECTURE.md
docs/ROADMAP_INDEX.md
docs/superpowers/specs/x-design.md
docs/archive/registry.jsonl
docs/guides/stat-ml-models.md
docs/research/2026-10-16-a.md
docs/site-pages.json
```

Тесты (имена и утверждения):

```python
def test_load_manifest_returns_pages_in_order()
def test_validate_reports_missing_source()
def test_validate_reports_duplicate_page()
def test_validate_rejects_bad_slug()          # "Bad Page.md"
def test_validate_rejects_excluded_source()   # docs/superpowers/specs/x-design.md
def test_resolve_mirrored_page()              # docs/ROADMAP_INDEX.md -> roadmap-index.md
def test_resolve_mirrored_page_keeps_anchor() # ...#3 -> roadmap-index.md#3
def test_resolve_spec_becomes_github_blob()   # docs/superpowers/specs/x-design.md -> REPO_URL/blob/main/...
def test_resolve_directory_becomes_github_tree()
def test_resolve_external_and_anchor_unchanged()
def test_resolve_missing_target_returns_error()
def test_rewrite_inline_reference_and_html_links()
```

- [ ] **Step 3: Запустить тесты и убедиться, что падают**

Run: `python3 scripts/docs_build_test.py -v`
Expected: FAIL — модуль/функции не определены.

- [ ] **Step 4: Реализовать загрузку и валидацию**

`load_manifest`: `json.loads`, проверка типа списка и полей, возврат `Page`. `validate_manifest`: источник существует и файл; `PAGE_RE`; `page` уникален; `source` не начинается с `EXCLUDED_PREFIXES`; `source` не абсолютный и не содержит `..`.

- [ ] **Step 5: Реализовать разрешение ссылок**

`build_page_map`: `{source: page}`. `resolve_target`:

1. `http://`, `https://`, `mailto:`, `tel:`, `#...` → `(target, None)`.
2. Разделить `path` и `#anchor`; пустой `path` → `(target, None)`.
3. `resolved = posixpath.normpath(target if target.startswith("/") else posixpath.join(posixpath.dirname(source), path)).lstrip("/")`.
4. `resolved in page_map` → `(page_map[resolved] + anchor, None)`.
5. `(repo_root / resolved).is_file()` → `(f"{repo_url}/blob/main/{resolved}{anchor}", None)`.
6. `(repo_root / resolved).is_dir()` → `(f"{repo_url}/tree/main/{resolved}{anchor}", None)`.
7. Иначе `(None, f"{source}: link target not found: {target}")`.

`rewrite_links`: три прохода — reference-определения (`^\[[^\]]+\]:\s*(\S+)`), HTML `href`/`src`, inline `[..](..)` (не трогать `![..](..)` — изображения обрабатывает Task 2). Каждый найденный target прогнать через `resolve_target`; ошибки вернуть списком с номером строки (можно `source` без номера, но лучше `source:line`).

- [ ] **Step 6: Прогнать тесты**

Run: `python3 scripts/docs_build_test.py -v`
Expected: PASS (все тесты Task 1).

- [ ] **Step 7: Commit**

```bash
git add docs/site-pages.json scripts/docs_build.py scripts/docs_build_test.py
git commit -m "docs: add site manifest and link resolver"
```

---

### Task 2: Генерация страниц, баннер, изображения и mkdocs-конфиг

**Files:**
- Modify: `scripts/docs_build.py`
- Modify: `scripts/docs_build_test.py`

**Interfaces:**
- Consumes: `Page`, `load_manifest`, `validate_manifest`, `build_page_map`, `resolve_target`, `rewrite_links`, `REPO_URL`.
- Produces:
  - `insert_banner(text: str, source: str, repo_url: str) -> str`
  - `copy_images(text: str, source: str, out_dir: Path, repo_root: Path) -> tuple[str, list[str]]`
  - `render_page(page: Page, repo_root: Path, out_dir: Path, repo_url: str) -> tuple[str, list[str]]`
  - `write_mkdocs_config(pages: list[Page], out_path: Path) -> None`
  - `write_site(pages: list[Page], out_dir: Path, repo_root: Path, repo_url: str) -> list[str]`
  - `main(argv: Optional[list[str]] = None) -> int` (CLI: `--check`, `--out`, `--repo-root`)

- [ ] **Step 1: Написать падающие тесты**

```python
def test_banner_inserted_after_first_h1()
def test_banner_prepended_without_h1()
def test_image_copied_and_rewritten()
def test_write_site_creates_pages_and_excludes_nothing()
def test_write_site_is_deterministic()
def test_write_mkdocs_config_has_inherit_dirs_and_nav()
def test_main_check_ok_returns_zero()
def test_main_check_broken_link_returns_one()
def test_main_out_writes_pages_and_config()
```

`test_write_site_is_deterministic`: два прогона в разные temp-каталоги, сравнить `read_bytes()` всех файлов.

- [ ] **Step 2: Запустить и убедиться, что падают**

Run: `python3 scripts/docs_build_test.py -v`
Expected: FAIL — новых функций нет.

- [ ] **Step 3: Реализовать баннер и изображения**

`insert_banner`:

```text
> **Источник:** [<source>](<repo_url>/blob/main/<source>). Страница собрана автоматически; правки — только в репозитории.
```

Вставить после первой строки, начинающейся с `# `; если её нет — в начало. Не добавлять баннер повторно, если он уже есть (идемпотентность).

`copy_images`: regex `!\[([^\]]*)\]\(([^)\s]+)(\s+"[^"]*")?\)`; для локального `src` (не `http(s)`, не `data:`) скопировать файл в `out_dir/assets/<basename>` (при коллизии — `<stem>-<sha1[:8]><suffix>`), заменить `src` на `assets/<name>`; отсутствующий файл — ошибка.

- [ ] **Step 4: Реализовать сборку страниц и конфига**

`render_page`: прочитать `repo_root/page.source`; `insert_banner`; `copy_images`; `rewrite_links`; вернуть текст и ошибки.

`write_site`: `shutil.rmtree(out_dir, ignore_errors=True)`; `mkdir(parents=True)`; для каждой `Page` записать `out_dir/page.page`; собрать ошибки; если ошибки есть — вернуть их (файлы можно оставить для отладки).

`write_mkdocs_config`: сгруппировать страницы по `section` в порядке первого появления; записать:

```yaml
INHERIT: ../mkdocs.base.yml
docs_dir: site-docs
site_dir: site
nav:
  - "Обзор":
      - "Главная": index.md
```

Все заголовки/ключи — через `json.dumps(..., ensure_ascii=False)` (валидный YAML).

`main`: argparse; `--repo-root` по умолчанию `Path(__file__).resolve().parent.parent`; `--check` — загрузить, валидировать, прогнать `render_page` в temp-каталог и вернуть 1 при ошибках; `--out` — `write_site` + `write_mkdocs_config(pages, out_dir.parent / "mkdocs.yml")`; печатать число страниц. `--check` и `--out` взаимоисключающие.

- [ ] **Step 5: Прогнать тесты**

Run: `python3 scripts/docs_build_test.py -v`
Expected: PASS.

- [ ] **Step 6: Проверить на реальном репозитории**

Run: `python3 scripts/docs_build.py --check`
Expected: exit 0, `checked N pages`. Если ссылка не разрешается — добавить её в манифест (если это документ) или исправить ссылку в источнике; не ослаблять проверку.

- [ ] **Step 7: Commit**

```bash
git add scripts/docs_build.py scripts/docs_build_test.py
git commit -m "docs: generate site pages and mkdocs config"
```

---

### Task 3: MkDocs, Makefile и workflow Pages

**Files:**
- Create: `mkdocs.base.yml`
- Modify: `Makefile` (переменные, `.PHONY`, цели)
- Create: `.github/workflows/pages.yml`

**Interfaces:**
- Consumes: `python3 scripts/docs_build.py --check|--out build/site-docs`; `build/mkdocs.yml`.
- Produces: `make docs-check`, `make docs-build`, `make docs-serve`; workflow `pages`.

- [ ] **Step 1: Создать `mkdocs.base.yml`**

```yaml
site_name: clickhouse-import-rosstat
site_description: Импорт макростатистики и финансовых данных в ClickHouse
site_url: https://kmlebedev.github.io/clickhouse-import-rosstat/
use_directory_urls: false

theme:
  name: material
  language: ru
  features:
    - navigation.sections
    - navigation.top
    - search.suggest
    - search.highlight
    - content.code.copy
    - toc.follow

plugins:
  - search

markdown_extensions:
  - admonition
  - attr_list
  - def_list
  - footnotes
  - md_in_html
  - tables
  - toc:
      permalink: true
  - pymdownx.highlight
  - pymdownx.superfences
  - pymdownx.tabbed:
      alternate_style: true
  - pymdownx.details
```

- [ ] **Step 2: Добавить цели Makefile**

В переменные: `MKDOCS_VERSION ?= 9.7.7`; `MKDOCS := uv run --with mkdocs-material==$(MKDOCS_VERSION) --python 3.12 mkdocs`.

```make
docs-check:
	python3 scripts/docs_build.py --check
	python3 scripts/docs_build_test.py

docs-build:
	python3 scripts/docs_build.py --out build/site-docs
	$(MKDOCS) build -f build/mkdocs.yml --strict

docs-serve:
	python3 scripts/docs_build.py --out build/site-docs
	$(MKDOCS) serve -f build/mkdocs.yml
```

Добавить `docs-check docs-build docs-serve` в `.PHONY`. Не менять `all`.

- [ ] **Step 3: Создать workflow**

`.github/workflows/pages.yml`:

```yaml
name: Pages
on:
  push:
    branches: [main]
    paths: [README.md, ARCHITECTURE.md, mkdocs.base.yml, docs/**, scripts/docs_build.py, scripts/docs_build_test.py, .github/workflows/pages.yml]
  pull_request:
    paths: [README.md, ARCHITECTURE.md, mkdocs.base.yml, docs/**, scripts/docs_build.py, scripts/docs_build_test.py, .github/workflows/pages.yml]
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: pages
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install mkdocs-material==9.7.7
      - run: python3 scripts/docs_build.py --check
      - run: python3 scripts/docs_build_test.py
      - run: python3 scripts/docs_build.py --out build/site-docs
      - run: mkdocs build -f build/mkdocs.yml --strict
      - uses: actions/upload-pages-artifact@v3
        with:
          path: build/site

  deploy:
    if: github.event_name != 'pull_request'
    needs: build
    runs-on: ubuntu-latest
    permissions:
      pages: write
      id-token: write
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```

Шага `actions/configure-pages` в build-job нет: PR-сборка должна оставаться зелёной до включения Pages.

- [ ] **Step 4: Локальная проверка**

Run: `make docs-check`
Expected: `checked N pages`, тесты OK.

Run: `make docs-build`
Expected: `build/site/index.html` существует; stderr без `WARNING`; exit 0.

Run: `find build/site -path '*superpowers*' -o -path '*plans*' -o -path '*archive*'`
Expected: пусто.

- [ ] **Step 5: Commit**

```bash
git add mkdocs.base.yml Makefile .github/workflows/pages.yml
git commit -m "docs: add mkdocs and pages workflow"
```

---

### Task 4: Синхронизация README, ARCHITECTURE и роадмапа

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`
- Modify: `docs/ROADMAP_INFRA.md`
- Modify: `docs/ROADMAP_INDEX.md`

- [ ] **Step 1: README**

- В «Структуре репозитория» добавить `mkdocs.base.yml`, `docs/site-pages.json`, `scripts/docs_build.py`, `.github/workflows/pages.yml` с одной строкой описания каждый.
- В таблицу «Разработка» добавить `make docs-check`, `make docs-build`, `make docs-serve`.
- В «CI и релизы» добавить абзац: workflow `pages` собирает сайт на PR и публикует на `main`; URL; Pages включается один раз (Source: GitHub Actions).
- Добавить короткий раздел «Документация» после «Быстрого старта»: ссылка на портал и правило «источник истины — репозиторий; спеки/планы на сайт не публикуются».

- [ ] **Step 2: ARCHITECTURE**

- В §2 (структура) добавить строки новых файлов и абзац «Публикация документации»: `docs/site-pages.json` → `scripts/docs_build.py` → `build/site-docs` → `mkdocs` → Pages; спеки/планы/архив исключены; сайт производный.
- В §8 (правила) не добавлять новых правил: публикация не меняет конвенции кода.

- [ ] **Step 3: ROADMAP_INFRA**

В §4 добавить строку `3.3`:

`| **3.3** | **Публичный docs-портал на GitHub Pages**: манифест `docs/site-pages.json`, генератор `scripts/docs_build.py`, `mkdocs-material` 9.7.7, workflow `pages.yml`; спеки/планы/архив не публикуются | ✅ сделан этим PR (2026-10-17) | `make docs-check` и `make docs-build` зелёные; PR-сборка сайта зелёная; после мержа deploy на `main` публикует URL, страницы и меню открываются; `make all` зелёный |`

- [ ] **Step 4: ROADMAP_INDEX**

В строке `infra` расширить схему item-id: `3.1`–`3.2` → `3.1`–`3.3`.

- [ ] **Step 5: Commit**

```bash
git add README.md ARCHITECTURE.md docs/ROADMAP_INFRA.md docs/ROADMAP_INDEX.md
git commit -m "docs: register github pages portal"
```

---

### Task 5: Issue, PR и живая приёмка

**Files:** нет изменений кода.

- [ ] **Step 1: Создать label и issue**

```bash
gh label create roadmap:infra --description "Roadmap: infra" --color 5319e7 2>/dev/null || true
```

Через MCP `issue_write` (method `create`) или `gh issue create`:

- Title: `[infra/3.3] Публичный docs-портал на GitHub Pages`
- Body: шапка `Roadmap: docs/ROADMAP_INFRA.md`, `Item: 3.3`, `Phase: 3`, `Contour: infra`, `Acceptance: ...`, `Spec: docs/superpowers/specs/2026-10-17-github-pages-design.md`, `Plan: docs/superpowers/plans/2026-10-17-github-pages.md`, `Depends on: infra/3.1`; далее цель, состав страниц, что не публикуется, одноразовое включение Pages.
- Labels: `roadmap:infra`.

- [ ] **Step 2: Финальные проверки**

```bash
make all
make docs-check
make docs-build
python3 scripts/docs_build.py --check
```

Expected: всё зелёное; `build/site/index.html` есть; `git diff --exit-code -- build/` не требуется (`build/` в `.gitignore`).

- [ ] **Step 3: Push и PR**

```bash
git push -u origin infra-github-pages
```

PR (MCP `create_pull_request` или `gh pr create`):

- base: `feat/github-tracker-model` (стек; после мержа трекера ретаргетить на `main`);
- title: `docs: add GitHub Pages docs portal (infra/3.3)`;
- body: пункт, спека/план, `Closes #<issue>`, что публикуется, что нет, как проверять, остаточный шаг владельца (Pages → GitHub Actions).

- [ ] **Step 4: Включить Pages (владелец/ассистент по разрешению)**

```bash
gh api -X POST repos/kmlebedev/clickhouse-import-rosstat/pages -f build_type=workflow
```

Ожидается `201`/`200`. Если API требует иных прав — Settings → Pages → Source: GitHub Actions.

- [ ] **Step 5: Живая приёмка**

После мержа в `main` (или `workflow_dispatch` на ветке, если владелец разрешит):

```bash
gh run list --workflow pages.yml --limit 3
curl -sS -o /dev/null -w '%{http_code}\n' https://kmlebedev.github.io/clickhouse-import-rosstat/
```

Ожидается: workflow `build` и `deploy` success; HTTP `200`; на сайте открываются `index.html`, `architecture.html`, `roadmap-infra.html`; меню и поиск работают; спеки/планы/архив отсутствуют. Протокол — комментарием в PR (команды и результаты).

---

## Self-review

- **Spec coverage:** §1–§8 спеки покрыты Task 1–5: манифест/валидация/ссылки — Task 1; генерация/баннер/изображения/nav — Task 2; mkdocs/Makefile/workflow — Task 3; README/ARCHITECTURE/роадмап — Task 4; issue/PR/Pages/приёмка — Task 5. §9–§10 — риски и границы, не требуют кода.
- **Step scan:** каждый шаг — одно действие с проверяемым результатом; тесты заданы именами и утверждениями; сигнатуры фиксированы.
- **Type consistency:** `Page` и функции Task 1 используются в Task 2 без переименований; `build/mkdocs.yml` — общий контракт Task 2 и Task 3.
- **Review Focus:** каждая строка имеет тест или проверку в Task 1–3 (ссылки, исключения, детерминированность, `--strict`, Python 3.9).
- **Proportion:** план описывает решения и тесты, а не тела функций; код генератора — за исполнителем.
