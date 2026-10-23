# План: починка `domrf_mortgage` — ссылка из JSON-API раздела

**Пункт:** `key-rate/phase-2-source-fixes` (фаза 2).
**Issue:** [#336](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/336).
**Спека:** [2026-10-23-domrf-mortgage-source-fix-design.md](../specs/2026-10-23-domrf-mortgage-source-fix-design.md).
**Ветка:** `fix/domrf-mortgage-source`, worktree `.worktrees/domrf-mortgage-source`.

---

## 1. Задачи

### Task 1 — источник ссылки: JSON-список раздела

Файл: `bank/domrf_mortgage.go`.

1. Переписать блок комментария констант: HTML-страница аналитики закрыта антиботом ServicePipe (HTTP 200 + ~1,8 КБ JS-оболочки вместо HTML, окнами), поэтому ссылка берётся из JSON-API того же раздела; сам файл и HEAD доступны общим клиентом.
2. Заменить `domrfAnalyticsUrl` на `domrfListUrl = "…/api/v2/content/analytics/list/?section=838&include=Y"`; `domrfDataUrlPrefix` и `domrfPageBlockAnchor` остаются.
3. Добавить типы ответа `domrfListResponse`, `domrfListItem`, `domrfListProps`, `domrfFile` (только нужные поля).
4. Добавить чистый разбор `domrfXlsSrcFromList(list domrfListResponse) (string, error)`:
   - `success=false` — ошибка;
   - первый элемент с `props.title == "Топ ипотечных банков"` (список от новых к старым);
   - пустой `props.file2.src` — ошибка;
   - `extension != "xlsx"` — ошибка;
   - блока нет — ошибка.
5. `Import()` — `util.GetJSON(domrfListUrl, &list)` → `domrfXlsSrcFromList` → локальная копия шаблона с `DataUrl`; остальной путь (префлайт, архив, батч) не трогать.
6. Удалить HTML-разбор: `domrfPageBlock`, `domrfXlsSrcFromPage`, импорты `bytes`/`goquery`; обновить doc-комментарии `DomrfMortgageStat` и `Import`.

### Task 2 — фикстура

- `bank/testdata/domrf_mortgage_list.json` — живой ответ API `section=838` (2026-10-23), 10 элементов; книга — прежняя `domrf_mortgage.xlsx`.
- Сверить `domrfFixtureSrc`/`domrfFixtureUrl` с `props.file2.src` текущего блока; если источник выпустил новый отчёт — обновить константы и spot-значения книги.
- Удалить `bank/testdata/domrf_mortgage_page.html` (HTML-путь больше не используется).

### Task 3 — тесты

Файл: `bank/domrf_mortgage_test.go`.

1. `TestDomrfXlsSrcFromList` — разбор живой фикстуры: `src` текущего блока и абсолютный `DataUrl`.
2. `TestDomrfXlsSrcFromListFailFast` — `success=false`, пустой список, нет блока, нет `file2`, `file2` не xlsx, пустой `src`: ошибка с причиной, без имени таблицы.
3. Существующие тесты книги (`TestParseDomrfSheet`, fail-fast, `Append`, DDL) — без изменений.
4. Удалить тесты HTML-страницы и антибот-повтора.

### Task 4 — документация

1. `ARCHITECTURE.md:4` — строка «Последнее обновление»: пункт `key-rate/phase-2-source-fixes` (#336), ссылка из JSON-API.
2. `ARCHITECTURE.md:299` — «Ограничение» шаблона А: DOM.RF за ServicePipe, поэтому `domrf_mortgage` читает ссылку из JSON-API раздела, а не HTML.
3. `ARCHITECTURE.md:450` — строка `domrf_mortgage`: та же поправка вместо «разведкой не подтвердилась».
4. `ARCHITECTURE.md:2089` — строка расписания банков: ссылка из JSON-API.
5. `docs/IMPORTERS.md:23` — описание пакета `bank/`.
6. `docs/ROADMAP_KEY_RATE.md` — абзац `phase-2-source-fixes` в фазе 2 (после блока рыночных контролей MOEX): issue #336, причина, решение, статус `✅ сделан этим PR`.

### Task 5 — приёмка

Из worktree, dev-ClickHouse поднят:

```bash
make all
make import STAT=domrf_mortgage
make ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=$(mktemp -d) import STAT=domrf_mortgage
make mcp-check
dagu validate dagu/bank.yaml
make docs-check
```

Второй запуск — полный путь (версия известна, но в свежем архиве её нет) → `Imported 15 rows`; повтор с тем же каталогом — no-op. Переменные архива — **make-переменными** (не env перед make): `-include ~/.config/rosstat/env` перекрывает окружение.

MCP: `v_bank_macro` по `source='domrf_mortgage'` — 15 точек / 15 рядов / `max(date)=2026-08-31`; `v_series_catalog` — 15 строк.

### Task 6 — ревью и PR

1. `task-reviewer` на `anthropic/haiku`, fix-loop.
2. Финальное ревью ветки — `deepseek/deepseek-flash`.
3. Один PR: `[key-rate/phase-2-source-fixes] …`, `Closes #336` вне backticks; протокол приёмки — комментарием в PR.
4. Project V2: `Status = In Review`, `PR`, `Spec / Plan`; `Done` — только после мержа владельцем.

## 2. Протокол живой приёмки (шаблон для PR)

```text
Пункт: key-rate/phase-2-source-fixes (#336)
Ветка: fix/domrf-mortgage-source

- make all — <результат>
- make import STAT=domrf_mortgage — <no-op по версии / Imported N rows>
- полный путь со свежим архивом — <Imported N rows, дата, число банков>
- повтор с тем же архивом — <no-op>
- v_bank_macro / v_series_catalog — <числа>
- make mcp-check — <результат>
- dagu validate dagu/bank.yaml — <exit>
- make docs-check — <результат>
- не проверено: <если есть>
```
