# Публичный docs-портал на GitHub Pages — дизайн

**Дата:** 17 октября 2026 г.
**Статус:** спека на исполнение (пункт `infra/3.3`).
**Основание:** владелец выбрал GitHub Pages вместо Wiki. В GitHub MCP нет wiki-инструментов, а вики — отдельный git-репозиторий, не покрытый REST/GraphQL; Pages собирается из канонического `docs/` и не требует второй копии документов.
**Связанные документы:** [ROADMAP_INFRA.md](../ROADMAP_INFRA.md) §4, [ROADMAP_INDEX.md](../ROADMAP_INDEX.md), [README.md](../../README.md), [ARCHITECTURE.md](../../ARCHITECTURE.md), скил `sync-readme-architecture`, спека трекера [2026-10-17-github-tracker-design.md](2026-10-17-github-tracker-design.md).

---

## 1. Цель

Опубликовать читаемый docs-портал по адресу `https://kmlebedev.github.io/clickhouse-import-rosstat/`: навигация по разделам, поиск, подсветка кода, страницы из канонических файлов репозитория. Источник истины остаётся в `docs/`, `README.md` и `ARCHITECTURE.md`; сайт — производный артефакт, который пересобирается на каждый push в `main`.

## 2. Не цели

- Wiki и её зеркало (отдельный git-репозиторий; MCP-инструментов для неё нет).
- Публикация `docs/superpowers/specs`, `docs/superpowers/plans`, `docs/plans`, `docs/archive/registry.jsonl`.
- Версионирование документации, кастомный домен, аналитика, комментарии.
- Автоматическое обнаружение новых файлов: состав страниц задаётся манифестом и ревьюится как код.

## 3. Почему Pages, а не Wiki

| Критерий | Wiki | Pages |
|---|---|---|
| Интерфейс записи | отдельный git-репозиторий `<repo>.wiki.git` | git-репозиторий проекта |
| Поддержка MCP | нет ни одного инструмента | нет отдельного инструмента, но публикация — обычный CI |
| Навигация | плоская, `_Sidebar.md` | вложенное меню, поиск, темы |
| Вторая копия документов | нужна (или генератор) | не нужна: сайт собирается из `docs/` |
| Ревью изменений | отдельная история вики | обычный PR в репозиторий |

## 4. Архитектура

```text
README.md, ARCHITECTURE.md, docs/*.md      канонические источники (PR-ревью)
                │
                ├── docs/site-pages.json    манифест: source → slug, раздел, заголовок
                │
                ▼
scripts/docs_build.py  ──►  build/site-docs/   страницы + баннер + переписанные ссылки
                │              build/mkdocs.yml  (INHERIT + docs_dir + site_dir + nav)
                ▼
mkdocs.base.yml + mkdocs-material 9.7.7  ──►  build/site/   статический сайт
                │
                ▼
.github/workflows/pages.yml  (PR — сборка; main/workflow_dispatch — deploy)
                │
                ▼
GitHub Pages: https://kmlebedev.github.io/clickhouse-import-rosstat/
```

Компоненты:

- `docs/site-pages.json` — единственное место, где перечислены публикуемые страницы. Формат: массив объектов `{ "source": "<путь от корня>", "page": "<slug>.md", "title": "<заголовок>", "section": "<раздел>" }`. Порядок массива задаёт порядок навигации.
- `scripts/docs_build.py` — генератор на stdlib Python: `--check` (валидация манифеста и ссылок) и `--out <dir>` (запись страниц и `build/mkdocs.yml`). Детерминирован: без дат, сортировка только по манифесту.
- `mkdocs.base.yml` — общая конфигурация сайта (тема, язык, расширения Markdown, `site_url`, `use_directory_urls: false`).
- `scripts/docs_build_test.py` — офлайн-тесты генератора на временных фикстурах.
- `Makefile` — цели `docs-check`, `docs-build`, `docs-serve`.
- `.github/workflows/pages.yml` — сборка на PR, деплой на `main` и `workflow_dispatch`.
- GitHub Pages включается один раз: Settings → Pages → Source: **GitHub Actions**.

## 5. Состав страниц v1

| Раздел | Страница | Источник |
|---|---|---|
| Обзор | `index.md` | `README.md` |
| Архитектура | `architecture.md` | `ARCHITECTURE.md` |
| Архитектура | `importer-standard.md` | `docs/IMPORTER_PACKAGE_STANDARD.md` |
| Контур ключевой ставки | `key-rate-model.md` | `docs/KEY_RATE_MODEL.md` |
| Контур ключевой ставки | `key-rate-channels.md` | `docs/KEY_RATE_CHANNELS.md` |
| Контур ключевой ставки | `key-rate-publications.md` | `docs/KEY_RATE_PUBLICATIONS.md` |
| DCF и золото | `dcf-data-coverage.md` | `docs/DCF_DATA_COVERAGE.md` |
| Справочники | `guides.md` | `docs/guides/README.md` |
| Справочники | `stat-ml-models.md` | `docs/guides/stat-ml-models.md` |
| Роадмапы | `roadmap-index.md` | `docs/ROADMAP_INDEX.md` |
| Роадмапы | `roadmap-dcf-polyus.md` | `docs/ROADMAP_DCF_POLYUS.md` |
| Роадмапы | `roadmap-gold-thesis.md` | `docs/GOLD_THESIS_DATA_ROADMAP.md` |
| Роадмапы | `roadmap-key-rate.md` | `docs/ROADMAP_KEY_RATE.md` |
| Роадмапы | `roadmap-dcf-metals.md` | `docs/ROADMAP_DCF_METALS.md` |
| Роадмапы | `roadmap-infra.md` | `docs/ROADMAP_INFRA.md` |
| Исследования | `research-<дата>-<тема>.md` (7 файлов) | `docs/research/*.md` |
| Статьи | `article-gold-q3-2026.md` | `docs/Золото_Q3-2026_итоги_и_прогноз_Q4-2026.md` |
| Статьи | `article-gold-diesel-2026-10-12.md` | `docs/Золото_сделка_по_дизелю_2026-10-12.md` |

Не публикуются: `docs/superpowers/specs/**`, `docs/superpowers/plans/**`, `docs/plans/**`, `docs/archive/**`, `build/**`, `wgc/data/**`.

## 6. Правила генерации

1. **Баннер источника.** На каждой странице после первого заголовка `# …` вставляется строка:
   `> **Источник:** [<путь>](https://github.com/kmlebedev/clickhouse-import-rosstat/blob/main/<путь>). Страница собрана автоматически; правки — только в репозитории.`
   Если заголовка нет — баннер ставится в начало.
2. **Ссылки.** Относительная ссылка разрешается от каталога исходного файла:
   - цель входит в манифест → `slug.md` (якорь сохраняется);
   - цель — существующий файл репозитория → `https://github.com/kmlebedev/clickhouse-import-rosstat/blob/main/<путь>`;
   - цель — существующий каталог → `https://github.com/kmlebedev/clickhouse-import-rosstat/tree/main/<путь>`;
   - внешние URL, `mailto:`, `#anchor` не меняются;
   - неразрешимая относительная ссылка — ошибка с `файл:строка`; сборка не выпускается.
3. **Ссылки в Markdown и HTML.** Обрабатываются inline-ссылки `[..](..)`, reference-определения `[id]: path`, `<a href="…">` и `<img src="…">`.
4. **Изображения.** Локальное изображение копируется в `build/site-docs/assets/<имя>`; при совпадении имён — префикс из пути источника. Внешние изображения не трогаются.
5. **Якоря.** `#fragment` сохраняется; проверяется только путь.
6. **Навигация.** `build/mkdocs.yml` генерируется из манифеста:
   ```yaml
   INHERIT: ../mkdocs.base.yml
   docs_dir: site-docs
   site_dir: site
   nav:
     - "Обзор":
         - "Главная": index.md
   ```
   Заголовки и ключи экранируются как JSON-строки (валидный YAML).
7. **Детерминированность.** Повторный `--out` даёт байт-идентичный результат; каталог `build/site-docs` очищается перед записью.
8. **Проверки `--check`.** Существование источников; уникальность `page`; формат `^[a-z0-9][a-z0-9-]*\.md$`; отсутствие источников из исключённых каталогов; разрешимость всех относительных ссылок.

## 7. Сборка и CI

`mkdocs.base.yml`:

- `site_name: clickhouse-import-rosstat`, `site_url: https://kmlebedev.github.io/clickhouse-import-rosstat/`;
- `use_directory_urls: false` — плоские URL `*.html`;
- тема `material`, `language: ru`, `navigation.sections`, `navigation.top`, `search.suggest`, `search.highlight`, `content.code.copy`, `toc.follow`;
- расширения: `admonition`, `attr_list`, `def_list`, `footnotes`, `md_in_html`, `tables`, `toc.permalink`, `pymdownx.highlight`, `pymdownx.superfences`, `pymdownx.tabbed`, `pymdownx.details`;
- плагин `search`.

Makefile:

| Цель | Действие |
|---|---|
| `make docs-check` | `python3 scripts/docs_build.py --check` + `python3 scripts/docs_build_test.py` |
| `make docs-build` | `python3 scripts/docs_build.py --out build/site-docs` + `uv run --with mkdocs-material==9.7.7 --python 3.12 mkdocs build -f build/mkdocs.yml --strict` |
| `make docs-serve` | генерация + `mkdocs serve -f build/mkdocs.yml` |

Workflow `.github/workflows/pages.yml`:

- триггеры: `pull_request`, `push` в `main` (пути: `README.md`, `ARCHITECTURE.md`, `mkdocs.base.yml`, `docs/**`, `scripts/docs_build.py`, `scripts/docs_build_test.py`, `.github/workflows/pages.yml`), `workflow_dispatch`;
- `permissions: contents: read`; у job `deploy` — `pages: write`, `id-token: write`;
- job `build`: checkout → setup-python 3.12 → `pip install mkdocs-material==9.7.7` → `docs_build.py --check` → `docs_build_test.py` → `docs_build.py --out build/site-docs` → `mkdocs build -f build/mkdocs.yml --strict` → `actions/configure-pages@v5` → `actions/upload-pages-artifact@v3` (`build/site`);
- job `deploy`: только не на PR, `environment: github-pages`, `actions/deploy-pages@v4`;
- `concurrency: group: pages`, `cancel-in-progress: false`.

Одноразовое действие владельца: включить Pages (Source: GitHub Actions). До этого deploy-джоб падает с явной ошибкой, сборка на PR работает.

## 8. Критерий приёмки

1. `python3 scripts/docs_build.py --check` — 0, без ошибок.
2. `python3 scripts/docs_build_test.py` — все тесты OK.
3. `make docs-check` — зелёный.
4. `make docs-build` — `build/site/index.html` и страницы; `mkdocs build --strict` без предупреждений; в `build/site` нет `superpowers/`, `plans/`, `archive/`.
5. `make docs-serve` — локальный сайт открывается, меню и поиск работают, ссылки на спеки/планы ведут в GitHub.
6. `make all` — зелёный (Go-контур не затронут).
7. Workflow `pages` на PR — build зелёный; после мержа в `main` и включения Pages — deploy зелёный, URL отдаёт 200, страницы и навигация на месте; повторный `workflow_dispatch` идемпотентен.
8. README/ARCHITECTURE/ROADMAP_INDEX/ROADMAP_INFRA обновлены; issue `[infra/3.3]` закрыт PR; протокол приёмки — комментарием в PR.

## 9. Риски

| Риск | Мера |
|---|---|
| Дрейф навигации | `nav` генерируется из манифеста, а не пишется руками |
| Битая ссылка после переписывания | `--check` падает с `файл:строка`; `mkdocs --strict` |
| Попадание спек/планов/архива на сайт | Манифест их не содержит; тест проверяет отсутствие в `build/site` |
| Не включён Pages | Явный шаг владельца; до него PR-сборка всё равно проверяет сайт |
| Зависимость mkdocs-material | Пин `9.7.7` в Makefile и workflow; локально через `uv` |
| Python 3.9 на macOS | Генератор совместим с 3.9 (`from __future__ import annotations`, без `X | Y`) |
| Публичность данных | Публикуются только уже публичные файлы; `docs/archive/registry.jsonl` исключён |
| Стек на незмерженную ветку трекера | PR базируется на `feat/github-tracker-model`; после мержа трекера ретаргетится на `main` |

## 10. Вне объёма

- Wiki-зеркало и любые wiki-MCP-инструменты.
- Автоматическое добавление новых файлов `docs/research/*` — только через манифест и PR.
- Публикация `docs/superpowers/**` и `docs/plans/**`.
- Кастомный домен, HTTPS-сертификат, аналитика, комментарии, версии документации.
