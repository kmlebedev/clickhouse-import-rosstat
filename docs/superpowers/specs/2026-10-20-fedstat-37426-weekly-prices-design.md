# Спека: импортёр `fedstat_weekly_prices` — недельные средние потребительские цены ЕМИСС 37426

**Дата:** 20 октября 2026 г.
**Пункт роадмапа:** `key-rate/phase-4-emiss-weekly-prices`, issue [#263](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/263), Task 1 [#264](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/264), Task 2 [#265](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/265).
**План:** [2026-10-20-fedstat-37426-weekly-prices.md](../plans/2026-10-20-fedstat-37426-weekly-prices.md).
**Research:** [2026-10-20-fedstat-37426-weekly-prices.md](../../research/2026-10-20-fedstat-37426-weekly-prices.md), [CSV товаров](../../research/2026-10-20-fedstat-37426-products.csv).
**Статус:** спека дизайна; нормативные правила не пересказываются — ссылки «стандарт §N» ([IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md)), «ARCHITECTURE §N».

---

## 1. Зачем

Недельные ряды ИПЦ (`rosstat_ipc_weeks`) начинаются только с 2022-01-10, а недельная оценка ИПЦ до 2022 собирается из бюллетеней (`rosstat_ipc_weeks_bulletins`) за 2009–2021. Для pre-2019 признаков nowcast-а (`share_rising_wk`, `median_wk`, `gasoline_4w`) и кросс-чека товарных рядов нужна **ценовая панель в рублях**, а не индекс. ЕМИСС 37426 — единственный публичный недельный источник цен с 2008 года.

Это **не ИПЦ**: значение — средняя цена в рублях за единицу товара/услуги, а не индекс. Ряд `rosstat_ipc_weeks` не заменяется и не дублируется.

## 2. Источник (проверен live 2026-10-20)

- Страница `https://www.fedstat.ru/indicator/37426`, HTTP 200, UTF-8, 541 578 байт; фильтры встроены в JS `FGrid`.
- Фильтры: `0` показатель `37426`; `3` год (`2008–2022`, `2026`); `58273` товары (158); `57831` ОКАТО (392); `33560` период (769); `30611` единица (`950351` рубль).
- Оси: `left_columns [57831, 58273]`, `top_columns [3, 33560]`, `filterObjectIds [30611]`.
- Токен: `#downloadTokenHolder` (`struts.token.name=token`, `token=<32>`).
- Выгрузка: `GET` страницы → `POST /indicator/downloadData.do?format=sdmx` с cookie, `Referer`/`Origin`, полями `id`, `lineObjectIds`, `columnObjectIds`, `filterObjectIds`, `selectedFilterIds`, `struts.token.name`, `token`.
- Ответ: `text/xml`, SDMX `GenericData`: `SeriesKey` (`s_OKATO`, `s_grtov`), `Attributes` (`EI`, `PERIOD`), `Obs` (`Time`, `ObsValue`) с десятичной запятой.
- Регионы v1: `s_OKATO=643` «Российская Федерация» (фильтр `1688487`) — 2008–2022; `s_OKATO=643004.АГ` «Российская Федерация без учета новых субъектов (с 01.01.2023)» (фильтр `1849012`) — 2026 недели 39–40.
- **Замер полного POST** (`2008–2022` + `2026`, 158 товаров, оба региона): 27 630 517 байт, 41,4 с, HTTP 200, **56 201 серия** (56 133 уникальные точки после дедупликации 68 точных дублей). Рабочий режим — **годовые батчи**: полный запрос под нагрузкой рвётся `EOF`/пустым конвертом, а годовой (~1,8 МБ, 3 536 серий за 2016, 2,5 с) стабилен.
- **2023–2025 отсутствуют**: POST за эти годы возвращает HTTP 200 + `text/html` (страница показателя), не XML. Это ограничение источника, а не ошибка импортёра.
- **Rate limit:** серия POST без паузы получает 503; рабочий режим — один POST на год с паузой 30 с (`fedstatBatchPause`) и backoff 30/60/120 с на 503/5xx/EOF.
- Terms/robots: `robots.txt` запрещает только `/login.do` и `/changeLanguage.do`; явных условий использования в `/terms.do`/`/userAgreement.do` не найдено (см. research §7).

## 3. Таблица и регион

```sql
CREATE TABLE IF NOT EXISTS fedstat_weekly_prices (
    product LowCardinality(String),
    region  LowCardinality(String),
    date    Date,
    price   Float64
) ENGINE = ReplacingMergeTree
ORDER BY (product, region, date);
```

- `product` — стабильное имя товара/услуги из кода (стандарт §6.8), совпадает с `series` в каталоге.
- `region` — две строки: `Российская Федерация` (2008–2022) и `Российская Федерация без учета новых субъектов (с 01.01.2023)` (2026). Смешивать их нельзя: это разные определения РФ, поэтому регион — часть ключа.
- `date` — дата регистрации цен (понедельник недели); `price` — рубли, `Float64` (правило 5 AGENTS.md).
- Ключ `(product, region, date)`: повторный прогон перезаписывает свои точки, дубли источника (68 точных повторов) схлопываются.

## 4. Товары и каталог

- В коде — литеральная карта **158 товаров**: `filterID` (для POST), `s_grtov` (для разбора), имя (для ряда/каталога). Источник карты — фильтр `58273` страницы и `CodeList s_grtov` реального ответа; имена совпали 158/158.
- Покрытие: 143 товара в 2008–2022, 114 в 2026, 15 только в 2026, 44 только в истории; объединение — 158. Флаги/точки — в CSV research.
- `series_catalog`: **158 записей**, `source='fedstat'`, `title`=имя, `unit='рубль'`, `frequency='W'`, `origin='fedstat.ru/indicator/37426, s_grtov=<код>; SDMX downloadData.do'`, `description` — что измеряет, дата = дата регистрации цен, 2023–2025 отсутствуют, 2026 — другой регион. Для 15 товаров, которых нет в 2008–2022, в описании явно сказано «в 2008–2022 значений нет, только 2026».
- Незнакомый `s_grtov` в ответе — ошибка с кодом (источник изменил таксономию); отсутствие товара в ответе — не ошибка (у товара может не быть точек в диапазоне).
- Дубли `(product, region, date)` с одинаковым значением отбрасываются; с разным — ошибка.
- Структура XML: годовой ответ — `GenericData > DataSet > generic:Series`, комбинированный/фикстуры — `Series` прямо в `GenericData`; парсер читает оба варианта (`fedstatSDMX.Series` + `DataSets`).

## 5. Дата и значение

Метки `PERIOD` (проверено на всех 769):

1. `на D <месяц> [YYYY года]` — день/месяц из метки, год из метки, иначе из `Time`. Пробел между днём и месяцем может отсутствовать (`17сентября`), лишние пробелы нормализуются. **Год метки сильнее `Time`**: 68 серий начала января имеют `Time` предыдущего года.
2. `N неделя [YYYY]` — год из метки или `Time`; неделя 1 = первый понедельник 8 января или позже; дата = неделя 1 + (N−1)×7. Проверено на 2021/2022/2026 против `rosstat_ipc_weeks` (совпадение всех дат).
3. Иное — ошибка с меткой.

Значение: trim, замена `,` на `.`, `strconv.ParseFloat`; пусто/`…`/`-` — не точка; NaN/Inf — ошибка. Ноль — значение (в проверенной выборке нулей нет).

## 6. Цикл `Import()`

Собственный `Import()` (прецеденты `rosstat_ipc_weeks_bulletins`, `cbr_dip_bulletin`); шаблон `util.ClickHouseImport` не подходит: он умеет только GET `DataUrl`, а источник требует CSRF POST.

1. DDL — `conn.Exec` по `CreateTable`.
2. **Гейт**: `SELECT uniqExact(product), uniqExact(region), uniqExact(toYear(date)), count() FROM fedstat_weekly_prices FINAL`. Если `(158, 2, 16, 56133)` — no-op: лог, `AfterImport` (каталог/витрина чинятся и на no-op-пути), `return 0` без сети.
3. **Цикл по годам** (16 лет: 2008–2022 и 2026): `GET` страницы (60 с) → токен; `POST` (180 с) → проверка статуса, `Content-Type`, корня `GenericData`, непустого тела. На 503/5xx/EOF — 3 попытки с backoff 30/60/120 с; между годами — пауза 30 с (`fedstatBatchPause`), иначе серия POST получает 503. HTML вместо XML — ошибка с сниппетом тела. 2026 идёт под регионом `1849012`, 2008–2022 — под `1688487`.
4. Разбор каждого года в память, слияние по `(s_grtov, регион, дата)` с проверкой конфликтов значений; проверка покрытия: 16 лет, 158 товаров, оба региона, 56 133 точки; иначе ошибка (частичный ответ не пишется).
5. Один `PrepareBatch("INSERT INTO fedstat_weekly_prices")` → `Append` с проверкой → `Send`; на ошибке `Abort`; `count = batch.Rows()`.
6. `AfterImport`: `util.UpsertSeriesCatalog` + `util.CreateView`.
7. Лог `Imported %d rows of fedstat_weekly_prices`.

Сетевых вызовов в `init()` нет; URL/токен вычисляются внутри `Import()`. Секретов нет.

## 7. Витрина, грант, MCP

`v_fedstat_weekly_prices` — long-форма с комментариями таблицы/колонок:

```sql
SELECT 'fedstat' AS source, product AS series, region, date, price AS value
FROM fedstat_weekly_prices FINAL
```

- `source` — всегда `fedstat`; `series` — товар (совпадает с `v_series_catalog`); `region` — два значения; `date` — дата регистрации цен; `value` — рубли, единица измерения в названии товара.
- Комментарий таблицы: 2008–2022 + 2026, 158 товаров, 56 133 точки, 2023–2025 в источнике отсутствуют, 2026 — другое определение РФ.
- `GRANT SELECT ON default.v_fedstat_weekly_prices TO kimi_reader` в `sql/mcp_kimi_reader.sql`; сырая таблица не выдаётся.
- `scripts/mcp_check.py`: витрина видна и непуста (`count() = 56 133`), `uniqExact(series) = 158`, `uniqExact(region) = 2`, каталог `source='fedstat'` = 158, сырая `fedstat_weekly_prices` → отказ `497`.

## 8. Тесты и фикстуры

- `fedstatDate` — таблица всех вариантов: 2008 (`на 9 января 2008 года`), 2016 (`23 неделя (на 13 июня 2016 года)`), 2018 (`37 неделя (на 17сентября 2018 года)`), 2021 (`1 неделя 2021`), 2022 (`51 неделя 2022`), 2026 (`39 неделя 2026`); год метки против `Time`; невалидная дата.
- Парсер SDMX на реальных урезанных фикстурах (2008, 2016, 2018, 2021, 2026): число точек, spot-значения, десятичная запятая, пустое значение, дубли, неизвестный `s_grtov`, чужой `s_OKATO`.
- Транспорт на `httptest`: страница с токеном; проверка годовой POST-формы (158 `58273_*` в порядке фильтра, один `3_2016`, один `57831_1688487`, `30611_950351`, оси, токен, `Referer`/`Origin`); 503 → retry; HTML → ошибка; нет токена → ошибка. `fedstatBaseURL`, `fedstatRetryDelays`, `fedstatBatchPause` — переменные пакета, подменяются в тесте.
- Реестр: `TestFedstatImportersPrefixedAndRegistered` — префикс `fedstat_`, уникальность, литерал таблицы, пустой `countPoints`, `Publication()` нулевая, `dagu/fedstat.yaml` содержит `CLICKHOUSE_IMPORT_STAT=fedstat_weekly_prices`.
- Каталог: 158 записей, уникальные имена, `source='fedstat'`, `frequency='W'`, `unit='рубль'`; 143 исторических товара в каталоге; 15 2026-only помечены в описании.
- DDL: `Float64`, `ReplacingMergeTree`, `LowCardinality`, ключ `(product, region, date)`.
- Ни один тест не ходит в сеть (стандарт §11.3).

## 9. Расписание

`dagu/fedstat.yaml`: `type: chain`, weekly (пятница 13:43 — после недельной публикации; точный день источником не подтверждён), один шаг `id: fedstat_weekly_prices`, `env: CLICKHOUSE_IMPORT_STAT=fedstat_weekly_prices`, `retry_policy: limit 3 / interval_sec 300`. Новый шаг — в хвосте своей цепочки; `dagu/rosstat.yaml` не меняется.

## 10. Отступления от стандарта (названы явно, §1.5)

1. **Транспорт POST+CSRF вместо `util.ClickHouseImport`.** Шаблон умеет GET `DataUrl`; источник требует GET страницы → токен → POST формы. Используется `util.NewDefaultTLSClient()` (клиент из `util`, правило 4 AGENTS.md соблюдено), отдельный cookie jar на прогон, `Referer`/`Origin`; сетевой код — в пакете, парсер отделён от сети.
2. **Нет точки контроля публикации, винтажного архива и `source_publications`.** Артефакт — не файл, а годовые POST-ответы (~1,8 МБ каждый, пауза 30 с); темп источника (недельный) равен шагу расписания (стандарт §9.3). No-op держит гейт по своей таблице, как у `rosstat_ipc_weeks_bulletins`.
3. **Два региона в одной таблице.** 2026 публикуется под новым определением РФ; `region` — часть ключа, чтобы не смешивать 2008–2022 и 2026.
4. **Расширение диапазона против issue.** Issue просил 2008–2018 и 158 товаров/≥88 000 точек; фактически за 2008–2018 только 77 товаров/36 335 точек. Владелец выбрал 2008–2022 + 2026 (158 товаров, 56 133 точки); 2023–2025 — разрыв источника.
5. **Транспорт — `util.NewDefaultTLSClient()` и фиксированный порядок полей.** Кастомный TLS-конфиг `util.HttpClient` (системный пул + `CERT_FILES`) источник отвергает: POST отдаёт `HTTP 200` + `GenericData` без `Series` или частичный ответ, тогда как `http.DefaultTransport` — все 56 201 серию (побайтово та же форма, cookie и заголовки). Поэтому импортёр создаёт на прогон `util.NewDefaultTLSClient()` (стандартный TLS, собственный cookie jar); остальные источники — прежний `util.HttpClient`. `selectedFilterIds` разбираются позиционно (indicator, year, region, unit, products), товары — в порядке фильтра `58273`, а не по алфавиту; тело собирается вручную (`fedstatFormBody`), не через `url.Values.Encode()`. Годовой SDMX оборачивает `Series` в `DataSet`, комбинированный — кладёт прямо в `GenericData`: парсер читает оба варианта.

## 11. Приёмка и follow-up

Критерий Task 2 (уточнён по факту источника):

- `make import STAT=fedstat_weekly_prices` — **158 товаров, 56 133 точки**, 2008–2022 + 2026, два региона; крайние даты 2008-01-09 … 2022-12-26 и 2026-10-05 … 2026-10-12.
- Повтор — no-op (0 строк, без сети).
- Spot-сверка: Говядина, Бензин АИ-92, Яйца — значение/дата против XML.
- `make mcp-check`, `make all`, `make docs-check`, `dagu validate dagu/fedstat.yaml` — зелёные.
- `docs/IMPORTERS.md`, `ARCHITECTURE.md` §6.1–§6.3/§7, `docs/LIMITATIONS.md`, `README.md` — обновлены.

Follow-up (не в этом PR): 2023–2025, если источник их откроет; продолжение 2026+; региональный разрез 392 ОКАТО (~50 млн точек); PIT/винтажи.
