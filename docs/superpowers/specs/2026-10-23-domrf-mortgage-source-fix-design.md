# Спека: `domrf_mortgage` — повтор страницы при антибот-заглушке ServicePipe

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

Импортёр берёт ссылку на книгу из JSON-блока «Топ ипотечных банков» страницы `https://дом.рф/analytics/mortgage/`; имя файла несёт отчётный месяц, поэтому статичного URL в коде нет. Дефект найден живым прогоном `make import STAT=key_rate_sources` 2026-10-21 (PR #333).

## 2. Разведка (2026-10-23, живые запросы)

| Проверка | Результат |
|---|---|
| Блок «Топ ипотечных банков» | **На месте**: атрибут `mortgage-top-banks` элемента `main#app`, `props.title` = `Топ ипотечных банков`, `props.file2.src` = `/api/v2/content/download/629470/135272296/` (выпуск «Август 2026 г.») |
| Разметка | Не менялась: атрибут живого HTML байт-в-байт совпадает с фикстурой `bank/testdata/domrf_mortgage_page.html`; `go test ./bank -run TestDomrf` на фикстуре зелёный |
| Причина | Страница вероятностно (≈50 % запросов) отдаёт **антибот-заглушку ServicePipe**: `HTTP 200`, ~1,8 КБ, `<js-challenge-loader>`, `servicepipe.tech`, редирект `/exhkqyad`. Блока в такой странице нет — парсер возвращает ровно сообщение из issue |
| Повтор | С тем же cookie jar (глобальный `util.HttpClient`) страница отдаётся полностью за 1–4 попытки: замер 10 прогонов × до 8 попыток — успех 10/10, максимум 4 попытки. Браузерные `Accept`/`Accept-Language`/`Sec-Fetch-*` и HTTP/2 против HTTP/1.1 картину не меняют |
| Скачивание книги | Без cookie успешной загрузки страницы — `HTTP 403` (WAF-страница «Доступ заблокирован»); с cookie из того же jar — `HTTP 200`, 259 366 байт, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` |
| HEAD-префлайт | `HEAD` с cookie: `307` на тот же URL (`set-cookie: spsc`), затем `200`; Go-клиент следует редиректу и получает `Content-Length` для отпечатка версии |
| Вывод | Блок и контракт ссылки не менялись. Нужен повтор **страницы** при заглушке; jar глобального клиента после успешной страницы авторизует и HEAD, и скачивание |

Прежнее утверждение документации — «страница отдаёт обычный серверный HTML, прежняя запись про ServicePipe разведкой не подтвердилась» — этим пунктом исправляется: заглушка реальна, но она **вероятностная**, а не постоянная.

## 3. Решение

Повтор живёт в пакете `bank/` рядом с импортёром: антибот — свойство источника, а не общая конвенция. Прецедент локальной транспортной особенности — `util.NewDefaultTLSClient` для ЕМИСС (`fedstat_weekly_prices`).

### 3.1. Признак заглушки

```go
func domrfChallengePage(page []byte) bool {
    return bytes.Contains(page, []byte("js-challenge-loader")) ||
        bytes.Contains(page, []byte("servicepipe.tech"))
}
```

Маркеры — из живой заглушки; фикстура `domrf_mortgage_challenge.html` пинит их тестом.

### 3.2. Цикл получения ссылки

```go
func domrfPageSrc(fetch func(string) ([]byte, error), attempts int, delay time.Duration) (string, error)
```

1. `fetch(domrfAnalyticsUrl)`; ошибка сети — вернуть сразу (причина названа транспортом).
2. `domrfXlsSrcFromPage(page)`; успех — вернуть `src`.
3. Ошибка разбора и страница **не** похожа на заглушку — вернуть ошибку сразу: реальная смена разметки повтором не лечится.
4. Заглушка — `log.Warnf` с номером попытки, пауза `delay`, следующая попытка.
5. Исчерпание — ошибка «антибот-заглушка ServicePipe вместо HTML после N попыток»; имя таблицы добавляет `Import()`, как и для остальных ошибок страницы.

Параметры: `domrfPageAttempts = 8`, `domrfPageRetryDelay = 300 * time.Millisecond`. При p≈0,5 восемь попыток дают отказ ≈0,4 %; dagu-ретраи (`limit: 3`, `interval_sec: 300`) остаются вторым эшелоном. `fetch` и `delay` — параметры, а не константы внутри функции: тесты не ходят в сеть и не спят.

### 3.3. Транспорт

`Import()` заменяет прямой `util.GetPage` на `domrfPageSrc(util.GetPage, domrfPageAttempts, domrfPageRetryDelay)`. Клиент — по-прежнему глобальный `util.HttpClient`: успешная страница оставляет в его jar cookie (`spid`, `PHPSESSID`, `session-cookie`), без которых WAF отдаёт книгу как `403`. Локальный клиент с отдельным jar сломал бы скачивание.

## 4. Что не меняется

- `DataUrl` вычисляется в `Import()` из страницы; статичного URL в коде нет.
- Точка контроля публикации (`PublicationCountPoints`), HEAD-префлайт, `source_publications`, винтажный архив — без изменений.
- Схема `domrf_mortgage (name, date, rate)`, имена и единицы рядов, каталог `series_catalog`, витрина `v_bank_macro` — без изменений.
- Разбор книги (`parseDomrfSheet`), запись батчем, точка контроля — без изменений.
- `dagu/bank.yaml`, `Makefile`, `main.go`, реестр `bank/importers.go` — без изменений.

## 5. Тесты и фикстуры

- `bank/testdata/domrf_mortgage_page.html` — обновить живой страницей (2026-10-23).
- `bank/testdata/domrf_mortgage_challenge.html` — новая фикстура живой заглушки (~1,8 КБ).
- `TestDomrfPageSrcRetriesChallenge` — заглушка ×2, затем страница: src получен ровно за 3 вызова.
- `TestDomrfPageSrcChallengeExhausted` — всегда заглушка: ошибка называет ServicePipe и число попыток, имени таблицы в ней нет.
- `TestDomrfPageSrcRealPageNoBlock` — реальная страница без блока: ровно один вызов, ошибка называет блок.
- `TestDomrfPageSrcFetchError` — ошибка `fetch` пробрасывается, один вызов.
- Существующие тесты страницы, книги, fail-fast и DDL остаются без изменений.

## 6. Документация

- `ARCHITECTURE.md` — строка «Последнее обновление», §6.1 (шаблон А) и строка `domrf_mortgage` в §6.1-таблице, §7 (расписание банков): исправить утверждение о ServicePipe и описать повтор.
- `docs/IMPORTERS.md` — описание пакета `bank/`.
- `docs/ROADMAP_KEY_RATE.md` — абзац пункта `phase-2-source-fixes` в фазе 2.
- `README.md` — без изменений (состав импортёров, таблицы и расписание те же).

## 7. Вне объёма

- Обобщение антибот-повтора в `util` и другие импортёры.
- Проверка HTTP-статуса в `util.GetFile` (общее поведение, не дефект пункта).
- `ARCHIVE_ACTION=restore` с вычисляемой ссылкой (существующее поведение).
- `tbank_group_ifrs` и прочие источники.

## 8. Риски

| Риск | Мера |
|---|---|
| Источник выпустит сентябрьский отчёт до реализации | Фикстура и `domrfFixtureSrc`/spot-константы обновляются по фактическому содержимому |
| Заглушка без известных маркеров | Ошибка разбора без повтора — явная, не «0 строк» |
| Повтор исчерпан (≈0,4 %) | Dag-ретраи 3 × 300 с; ошибка называет антибот и число попыток |
| Скачивание без cookie | Повтор идёт тем же глобальным клиентом; jar после успешной страницы авторизует HEAD и GET |

## 9. Критерий приёмки

- `make import STAT=domrf_mortgage` проходит; полный путь даёт 15 точек (15 банков «Новостройки», отчётный месяц 2026-08-31), повтор — no-op по версии без скачивания.
- `make all` — зелёный; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/bank.yaml` — exit 0; `make docs-check` — зелёный.
- `v_bank_macro` и `v_series_catalog` по `source = 'domrf_mortgage'` — 15 точек / 15 рядов, без изменений против baseline.
- Протокол живой приёмки — комментарием в PR; строка роадмапа — тем же PR.
