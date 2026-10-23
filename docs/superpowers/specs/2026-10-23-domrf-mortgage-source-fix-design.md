# Спека: `domrf_mortgage` — ссылка на книгу из JSON-API раздела, а не из HTML-страницы

**Пункт:** `key-rate/phase-2-source-fixes` (фаза 2).
**Issue:** [#336](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/336).
**Роль:** `role:go`.
**Дата:** 23 октября 2026 г.
**Ветка:** `fix/domrf-mortgage-source`, worktree `.worktrees/domrf-mortgage-source`.
**Сопутствующие документы:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) фаза 2, [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md) §4.4/§5.4/§6/§11, [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.1.

---

## 1. Дефект

`make import STAT=domrf_mortgage` (и группа `key_rate_sources`) падает на разборе страницы аналитики ДОМ.РФ:

```text
domrf_mortgage: https://дом.рф/analytics/mortgage/: страница …: не найден блок «Топ ипотечных банков» (JSON блока в атрибутах страницы)
```

Импортёр брал ссылку на книгу из JSON-атрибута `mortgage-top-banks` элемента `main#app` HTML-страницы; имя файла несёт отчётный месяц, поэтому статичного URL в коде нет. Дефект найден живым прогоном `make import STAT=key_rate_sources` 2026-10-21 (PR #333).

## 2. Разведка (2026-10-23, живые запросы)

| Проверка | Результат |
|---|---|
| Блок «Топ ипотечных банков» на странице | **На месте**: атрибут `mortgage-top-banks` элемента `main#app`, `props.title` = `Топ ипотечных банков`, `props.file2.src` = `/api/v2/content/download/629470/135272296/` (выпуск «Август 2026 г.») |
| Разметка блока | Не менялась: живой HTML совпадает с прежней фикстурой в этой части; прежний разбор на фикстуре зелёный |
| Причина падения | HTML-страница закрыта антиботом ServicePipe: на часть запросов приходит `HTTP 200` с ~1,8 КБ JS-оболочки (`js-challenge-loader`, `servicepipe.tech`) вместо HTML. Заглушка **не вероятностная по одному запросу**, а окнами: живой прогон импортёра получил её 8 раз подряд, после паузы тот же клиент получил полную страницу; серия запросов в окне блокировки сама поддерживает блокировку. Повторы внутри `Import()` проблему не решают |
| Тот же блок в JSON-API | `GET https://xn--d1aqf.xn--p1ai/api/v2/content/analytics/list/?section=838&include=Y` → `200`, JSON; среди элементов — `id=629470`, `props.title="Топ ипотечных банков"`, `props.period="Август 2026"`, `props.file2.src="/api/v2/content/download/629470/135272296/"`, `extension="xlsx"`. `section=838` — id раздела «Топ ипотечных банков» (найден в бандле страницы рядом с ссылкой «Архив» блока) |
| Транспорт JSON | `util.GetJSON` общим `util.HttpClient` — `200`, `success=true`, 10 элементов; антибота нет |
| HEAD-префлайт | `util.HeadFingerprint` по `domrfDataUrlPrefix + src` — `984b5398448d06092a62b481671f02a6`; ровно версия, уже записанная в `source_publications` (импорт 2026-10-17) |
| Скачивание книги | `util.GetFileBytes` — `200`, 259 366 байт, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`; cookie не нужны (прежние 403 наблюдались у curl и были следствием блокировки клиента, а не обязательным cookie-гейтом) |
| Вывод | Контракт «ссылка вычисляется в `Import()`» сохраняется, но источник — **JSON-API раздела**, а не HTML. Это ровно вариант issue «новая страница/эндпоинт» |

Прежнее утверждение документации — «прежняя запись про ServicePipe разведкой не подтвердилась» — этим пунктом исправляется: антибот на HTML-странице реален, поэтому импортёр перестаёт от неё зависеть.

## 3. Решение

`Import()` импортёра получает ссылку из JSON-списка раздела:

```go
var list domrfListResponse
if err = util.GetJSON(domrfListUrl, &list); err != nil { … }
src, err := domrfXlsSrcFromList(list)
```

- `domrfListUrl = "…/api/v2/content/analytics/list/?section=838&include=Y"` — константа пакета.
- `domrfListResponse`/`domrfListItem`/`domrfListProps`/`domrfFile` — типы ответа (только нужные поля: `success`, `data[].id/name/props.title/period/file2.{src,extension,name,date}`).
- `domrfXlsSrcFromList(list domrfListResponse) (string, error)` — чистый разбор (§6.1), офлайн-тестируемый:
  1. `success=false` — ошибка;
  2. идём по `data` от новых к старым; берём **первый** элемент с `props.title == "Топ ипотечных банков"` (имя несёт месяц и меняется, признак — заголовок);
  3. пустой `props.file2.src` — ошибка (без неё импортёр пошёл бы в `GetXlsx("")`);
  4. `extension != "xlsx"` — ошибка (PDF лежит в `file`);
  5. ни одного блока — ошибка с именем раздела/блока.
- Ошибки называют список и блок, но не таблицу: префикс `domrf_mortgage:` добавляет `Import()`.
- HTML-разбор (`domrfXlsSrcFromPage`, `domrfPageBlock`, goquery/bytes) и повтор с детекцией заглушки удаляются: источник ссылки больше не HTML.

## 4. Что не меняется

- `DataUrl` вычисляется в `Import()`; статичного URL в коде нет.
- Точка контроля публикации (`PublicationCountPoints`), HEAD-префлайт, `source_publications`, винтажный архив — без изменений.
- Схема `domrf_mortgage (name, date, rate)`, имена и единицы рядов, витрина `v_bank_macro` — без изменений. Состав каталога `series_catalog` (15 рядов) тоже не меняется, но поле `origin` обновляется: оно называет JSON-API раздела, а не удалённый HTML-путь, — иначе MCP-агент видит старую точку входа. Текст origin обновляется полным импортом (no-op-путь `AfterImport` не зовёт).
- Разбор книги (`parseDomrfSheet`), запись батчем — без изменений.
- `dagu/bank.yaml`, `Makefile`, `main.go`, реестр `bank/importers.go` — без изменений.

## 5. Тесты и фикстуры

- `bank/testdata/domrf_mortgage_list.json` — живой ответ API `section=838` (2026-10-23), 10 элементов; книга блока — прежняя `domrf_mortgage.xlsx`.
- `TestDomrfXlsSrcFromList` — разбор живой фикстуры даёт `src` текущего блока и абсолютный `DataUrl`.
- `TestDomrfXlsSrcFromListFailFast` — `success=false`, пустой список, нет блока, нет `file2`, `file2` не xlsx, пустой `src`: ошибка с причиной, без имени таблицы.
- Существующие тесты книги (`TestParseDomrfSheet`, fail-fast, `Append`, DDL) — без изменений.
- HTML-фикстура страницы и заглушки удаляются: HTML-путь больше не используется.

## 6. Документация

- `ARCHITECTURE.md` — строка «Последнее обновление», §6.1 (шаблон А), строка `domrf_mortgage` в §6.1-таблице, §7 (расписание банков): ссылка — из JSON-API раздела, HTML-страница за ServicePipe не используется.
- `docs/IMPORTERS.md` — описание пакета `bank/`.
- `docs/ROADMAP_KEY_RATE.md` — абзац пункта `phase-2-source-fixes` в фазе 2.
- `README.md` — без изменений (состав импортёров, таблицы и расписание те же).

## 7. Вне объёма

- Общий антибот-обход/ретраи в `util` и другие импортёры.
- Проверка HTTP-статуса в `util.GetFile` (общее поведение, не дефект пункта).
- `ARCHIVE_ACTION=restore` с вычисляемой ссылкой (существующее поведение).
- `tbank_group_ifrs` и прочие источники.

## 8. Риски

| Риск | Мера |
|---|---|
| Источник выпустит сентябрьский отчёт до реализации | Фикстура и `domrfFixtureSrc`/spot-константы обновляются по фактическому содержимому; тест поймает расхождение |
| Сменится id раздела `838` или формат ответа | Ошибка «нет блока»/`success=false` — явная, не «0 строк»; правка константы и фикстуры |
| Новый месяц опубликован без `file2` | Fail-fast с именем блока: старый месяц молча не подставляется |
| Список раздела ограничен 10 последними элементами | `section=838` возвращает только блоки этого раздела; текущий месяц — первый элемент списка |

## 9. Критерий приёмки

- `make import STAT=domrf_mortgage` проходит; полный путь (свежий архив) даёт 15 точек (15 банков «Новостройки», отчётный месяц 2026-08-31), повтор — no-op по версии без скачивания.
- `make all` — зелёный; `make mcp-check` — все проверки пункта зелёные (248 OK на прогоне 2026-10-23; единственный провал — news LLM bootstrap, вне пункта); `dagu validate dagu/bank.yaml` — exit 0; `make docs-check` — зелёный.
- `v_bank_macro` по `source = 'domrf_mortgage'` — 15 точек / 15 рядов, без изменений против baseline; `v_series_catalog` — 15 рядов, `origin` называет JSON-API раздела.
- Протокол живой приёмки — комментарием в PR; строка роадмапа — тем же PR.
