# No-op-префлайт версии артефакта для импортёров с редким обновлением файла — дизайн

> **Дата:** 2026-10-14 · **Ветка:** `feat/publication-preflight` · **Пункт:** follow-up §8 п. 13 и §6.2 [ARCHITECTURE.md](../../../ARCHITECTURE.md) — роллаут общего механизма точек контроля публикаций (`source_publications` + HEAD-префлайт) на импортёры, чей файл обновляется реже прогонов.
> **Критерий приёмки:** повторный `make import STAT=<имя>` переведённого импортёра даёт в логе `no-op, версия <fingerprint> уже импортирована, N точек на месте` без скачивания файла и без записи в БД; в `source_publications` на каждого — одна строка версии с `points` = фактическому числу точек ряда; счётчики витрин не растут; `make all` и `make mcp-check` зелёные.

## 1. Цель и предыстория

Пункт 1.5 контура ключевой ставки (импортёр `cbr_mo_br`) завёл общий механизм точек контроля публикаций — служебную таблицу `source_publications` и лёгкий HEAD-префлайт (`util.HeadFingerprint`, `util.PublicationKnown`, `util.SavePublication`), чтобы ежедневный прогон не скачивал файл, который обновляется 8 раз в год. Тогда же было сказано прямо: **первый потребитель — `cbr_mo_br`, миграция остальных импортёров — вне объёма** (спека `2026-10-13-cbr-mo-br-design.md` §6; `ARCHITECTURE.md` §8 п. 13).

Сейчас механизм есть, но им пользуется один импортёр, а логика префлайта **написана инлайн** внутри `moBrStat.Import` (~45 строк) и не переиспользуется. Между тем в пакете `cbr/` восемь импортёров качают XLSX, обновляемый ~раз в месяц, — ежедневно, впустую:

| Импортёр | Файл | Прогон по расписанию |
|---|---|---|
| `cbr_mp_survey` | `mp_survey_data.xlsx` (2,1 МБ) | ежедневно 09:17 |
| `cbr_households_b_mes` | `households_bm.xlsx` | ежедневно 09:17 |
| `cbr_m2` | `monetary_agg_SA.xlsx` (0,6 МБ) | ежедневно 09:17 |
| `cbr_loans_to_corporations` | `01_01_A_New_loans_corp_by_activity.xlsx` | ежедневно 09:17 |
| `cbr_loans_to_individuals` | `02_04_New_loans_ind.xlsx` | ежедневно 09:17 |
| `cbr_bank_int_rate` | `regulatory_rates.xlsx` | ежедневно 09:17 |
| `cbr_credit_m2x` | `credit_m2x.xlsx` (ссылка со страницы ДКС) | ежедневно 09:17 |
| `cbr_indicators_cpd` | `indicators_cpd.xlsx` (ссылка со страницы aipd) | ежедневно 09:17 |
| `cbr_infl_exp` | `Infl_exp_ГГ-ММ.xlsx` (ссылка со страницы) | ежедневно 09:17 |

Задача — сделать точку контроля публикации **свойством общего шаблона импортёра** (`util.ClickHouseImport`), включаемым декларативно, перевести на неё эти девять вместе с `cbr_mo_br` (сняв инлайн-копию) и, по решению владельца, распространить механизм на те импортёры остальных пакетов, которые проходят три критерия применимости (§3.6).

## 2. Источник: проверено живым HEAD (2026-10-14)

| URL | HTTP | `ETag` | `Last-Modified` | `Content-Length` |
|---|---|---|---|---|
| `/Content/Document/File/135603/mp_survey_data.xlsx` | 200 | `"000269B7"` | `Wed, 09 Sep 2026 21:00:00 GMT` | 2183234 |
| `/vfs/statistics/households/households_bm.xlsx` | 200 | — | `Tue, 06 Oct 2026 16:14:45 GMT` | 284565 |
| `/vfs/statistics/credit_statistics/monetary_agg_SA.xlsx` | 200 | — | `Fri, 18 Sep 2026 14:34:34 GMT` | 583746 |
| `/vfs/statistics/pdko/int_rat/regulatory_rates.xlsx` | 200 | — | `Thu, 08 Oct 2026 06:18:40 GMT` | 20311 |
| `/vfs/statistics/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx` | 200 | — | `Wed, 07 Oct 2026 06:17:22 GMT` | 62042 |
| `/vfs/statistics/BankSector/Mortgage/02_04_New_loans_ind.xlsx` | 200 | — | `Wed, 30 Sep 2026 08:28:50 GMT` | 163420 |

Выводы, на которых стоит дизайн:

1. **Все шесть статических файлов отвечают 200 и отдают версионный сигнал.** Под `/vfs/statistics/…` `ETag` отсутствует, но композит `имя|ETag|Last-Modified|Content-Length` от этого не ломается — это ровно тот случай, который `HeadFingerprint` описан как штатный («пропажа заголовка не ломает»). Смену файла ловит `Last-Modified`.
2. Файлы перезаписываются **по тому же URL** (имя не меняется) — значит без версионного чека прогон каждый день скачивает и импортирует одно и то же.
3. Периодичность обновления (по `Last-Modified` приёмки) — месяц и реже; прогон — ежедневный.

**Не подходят под механизм** (зафиксировано, чтобы это не выглядело пропуском): `cbr_key_rate`, `cbr_ruania`, `cbr_currency_usd`, `cbr_gold`. У них данные дневные, а URL собирается с сегодняшней датой окончания диапазона (`UniDbQuery.To=%s`, `date_req2=%s`, `ToDate=%s`) — отпечаток меняется каждый прогон, no-op недостижим по построению, а HEAD был бы чистыми накладными расходами.

## 3. Решения

### 3.1 Механизм — в общий шаблон `util.ClickHouseImport`

Точка контроля публикации становится частью шаблона, которым уже пользуются импортёры всего репозитория (`AGENTS.md` правило 7: «новые импортёры — по шаблонам из ARCHITECTURE.md §3, предпочтительно `util.ClickHouseImport`»). Включение — декларативное, тремя опциональными полями:

```go
// PublicationCountPoints — SQL, возвращающий число точек ряда этого импортёра. Непустое
// значение включает точку контроля публикации (§8 п. 13, ARCHITECTURE §6.2): HEAD-префлайт
// по DataUrl, no-op известной версии, строка версии в source_publications последней.
PublicationCountPoints string
// PublicationSource — ключ версии в source_publications и префикс лога префлайта.
// Пусто -> TableName. Нужен импортёрам, чей Name() не равен TableName
// (cbr_mp_survey и cbr_mo_br пишут в общую macro_series).
PublicationSource string
// AfterImport вызывается после успешного Send точек и до записи строки версии: здесь
// импортёр публикует производные (series_catalog, витрину). Ошибка отменяет запись
// версии — порядок «точки -> производные -> версия» (ARCHITECTURE §8 п. 13).
AfterImport func(ctx context.Context, conn driver.Conn) error
```

Поля опциональны: у импортёров, которые их не задают (все остальные пакеты репозитория), поведение `Import` не меняется ни на шаг. Транзитное состояние через поля структуры не передаётся — только декларация.

**Порядок в `ClickHouseImport.Import`:**

1. DDL `CreateTable` (как сейчас). Первым — потому что количественный чек делает `SELECT count()` по своей таблице, которой на первом прогоне ещё нет.
2. `BeforeImport` (как сейчас) — вычисляет `DataUrl`, если он не константа.
3. **Префлайт** (только если `PublicationCountPoints != ""`): `util.PublicationPreflight(ctx, conn, PublicationKey(), DataUrl, PublicationCountPoints)`. Версия известна и число точек сошлось → вернуть проверенное число точек, **не скачивая файл и не делая записей в БД**.
4. Полный путь: `CrawFunc` / `ImportXls` — как сейчас.
5. `AfterImport` — производные (каталог, витрина), если задан.
6. **Строка версии** (`util.SavePublication`) — последней и только при непустом отпечатке. Недоступный HEAD (отпечаток пуст) — не сбой, а деградация: полный импорт без записи версии, иначе следующая волна считалась бы уже импортированной.

Ключевое: версия пишется **после** производных, прямо в шаблоне, — а не вызывающим кодом. Это единственный способ сохранить контракт §8 п. 13 для всех девяти импортёров сразу: обёртка, которая публикует каталог и витрину **после** возврата из шаблона, физически не может гарантировать «версия последней» (её сбой оставил бы строку версии уже записанной, и следующий прогон ушёл бы в no-op с устаревшим каталогом). Поэтому публикация производных переезжает в шаблон полем `AfterImport`, а не выполняется снаружи.

### 3.2 `points` версии = результат запроса импортёра

`source_publications.points` — это **результат `PublicationCountPoints`**, а не `batch.Rows()`. Причина конкретна: `cbr_households_b_mes` пишет 9030 ячеек листа, а уникальных пар `(name, date)` в таблице — 7350 (ключ `ORDER BY (name, date)`, дубли схлопывает `ReplacingMergeTree`). Если записать в версию 9030, чек `dbPoints == points` не сойдётся никогда, и no-op не наступит ни разу — механизм будет молча простаивать.

Отсюда правило: **и записываемое число, и предмет чека — один и тот же запрос импортёра**, берётся он после успешного `Send` и до записи версии. Для `cbr_mo_br` оба числа равны 9348, регресса при переходе на общий путь нет.

### 3.3 Обёртка `publishedStat` упраздняется

Сейчас каталог и витрину публикует обёртка `publishedStat` (`cbr/views.go`), которая держит `meta` и оборачивает импортёр. С появлением `AfterImport` она становится лишним слоем: производные объявляются там же, где импортёр, и публикуются шаблоном в правильном порядке. Поэтому:

- `publishedStat` и его `Import` удаляются; одиннадцать записей вида `{cbrM2Importer, "cbr_m2"}` в `cbr/importers.go` начинают ссылаться прямо на импортёр;
- производные задаются одной строкой в литерале импортёра: `AfterImport: afterCbrMacro(cbrM2SeriesMeta)` — хелпер в `cbr/views.go` возвращает замыкание над `publishSeries(ctx, conn, meta)` (каталог + витрина `v_cbr_macro`); у `cbr_mp_survey` — свой (каталог + `v_cbr_survey`), у `cbr_mo_br` — свой (каталог из разбора + `v_cbr_moBr`-витрина);
- `mpSurveyStat.Import` удаляется вовсе (всё делает шаблон), `moBrStat.Import` остаётся — он достаёт ссылку на файл волны и собирает меты разбора — но из него уходит инлайн-префлайт: он выставляет `PublicationSource`/`PublicationCountPoints`/`AfterImport` и вызывает шаблон;
- три импортёра со ссылкой со страницы (`cbr_infl_exp`, `cbr_credit_m2x`, `cbr_indicators_cpd`) **своего кода не меняют**: они уже выставляют `s.DataUrl` до делегирования в шаблон, и префлайт в шаблоне видит готовый URL. Страница на no-op-прогоне всё равно скачивается — без ссылки отпечатка не построить (так же устроен `cbr_mo_br`).

### 3.4 Хелперы `util`

- `func PublicationPreflight(ctx context.Context, conn driver.Conn, source, url, countPoints string) (fingerprint string, noop bool, points uint64, err error)` — HEAD → известна ли версия → количественный чек. Логи и деградации переносятся из `mo_br` дословно: недоступный HEAD → `warn` «полный импорт без записи версии» и пустой отпечаток; сбой SELECT версии → `warn` и полный импорт; известная версия с несовпавшим числом → `warn` и полный импорт (self-heal); совпало → `log.Infof("%s: no-op, версия %s уже импортирована, %d точек на месте", …)`.
- `func CountPoints(ctx context.Context, conn driver.Conn, query string) (uint64, error)` — выполняет запрос импортёра; используется и чеком, и записью версии.
- `func (s *ClickHouseImport) PublicationKey() string` — `PublicationSource`, а при пустом — `TableName`.

### 3.5 Запросы количественного чека (литералы)

| Импортёр | `PublicationCountPoints` |
|---|---|
| `cbr_mp_survey` | `SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'` |
| `cbr_households_b_mes` | `SELECT count() FROM cbr_households_b_mes FINAL` |
| `cbr_m2` | `SELECT count() FROM cbr_m2 FINAL` |
| `cbr_credit_m2x` | `SELECT count() FROM cbr_credit_m2x FINAL` |
| `cbr_loans_to_corporations` | `SELECT count() FROM cbr_loans_to_corporations FINAL` |
| `cbr_loans_to_individuals` | `SELECT count() FROM cbr_loans_to_individuals FINAL` |
| `cbr_bank_int_rate` | `SELECT count() FROM cbr_bank_int_rate FINAL` |
| `cbr_indicators_cpd` | `SELECT count() FROM cbr_indicators_cpd FINAL` |
| `cbr_infl_exp` | `SELECT count() FROM cbr_infl_exp FINAL` |
| `cbr_mo_br` | существующая `moBrCountPoints` (`SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'`) |

`FINAL` обязателен: без него `count()` считает несмёрженные части и растёт после каждого повторного импорта, то есть чек сравнивал бы величину, меняющуюся от прогона к прогону. `WHERE source = '<…>'` у владельцев общей `macro_series` обязателен по той же причине, что в `moBrCountPoints`: иначе чек считал бы чужие ряды.

### 3.6 Реестр и тест-энфорсер

`cbrImporter` получает третью колонку `countPoints string` (литерал, как колонка `table`), а `TestCbrImportersPrefixedAndRegistered` — новые проверки:

- множество импортёров с непустым объявлением равно ожидаемому (десять имён литералами) — забытое объявление молча выключило бы префлайт, и ни один тест этого бы не заметил;
- литерал колонки совпадает с объявлением самого импортёра (достаётся через `util.ClickHouseImport.Publication()`, которую импортёры пакета получают по встраиванию), — не тавтология: расхождение и есть та правка, от которой проверка должна падать (прецедент — колонка `table`);
- у импортёров вне ожидаемого множества объявления нет (дневные `cbr_key_rate`/`cbr_ruania`/`cbr_currency_usd`/`cbr_gold` не обзавелись префлайтом «на всякий случай»).

### 3.7 Роллаут за пределы `cbr/` — инвентарь (факт, 2026-10-14)

Механизм общий, поэтому после перевода `cbr/` пройден **инвентарь** остальных нетестовых импортёров репозитория: 51 запись `chimport.Stats`, из них 14 в `cbr/` (переведены по §3.5) и 37 вне него. Переводятся лишь те, кто проходит все три критерия:

1. артефакт — **один URL**, применимый к `HeadFingerprint` (не параметризованный датой/периодом, не POST-API без версии);
2. содержимое меняется **существенно реже расписания** шага (`dagu/*.yaml`);
3. у артефакта есть версионный сигнал в заголовках — проверяется живым HEAD.

Проверка — не `curl`-прикидка: по фактическому URL артефакта прогонялся `util.HeadFingerprint` (тот же код, что в префлайте), расписания взяты из `dagu/*.yaml`, вне расписания — из шапок файлов `dagu/`.

**Прошли все три критерия и переведены (4):**

| Импортёр | Пакет | Артефакт | Шаг расписания | `PublicationCountPoints` |
|---|---|---|---|---|
| `ipc_mes` | `rosstat/` | `rosstat.gov.ru/storage/mediabank/ipc_mes_ММ-ГГГГ.xlsx` (ссылка со страницы `/statistics/price`) | чт 12:17, недельный | `SELECT count() FROM ipc_mes FINAL` |
| `salaries_mes` | `rosstat/` | `…/tab1-zpl_ММ-ГГГГ.xlsx` (со страницы рынка труда) | чт 12:17, недельный | `SELECT count() FROM salaries_mes FINAL` |
| `vvp_kvartal` | `rosstat/` | `…/VVP_kvartal_s-1995-ГГГГ.xlsx` (со страницы нацсчетов) | чт 12:17, недельный | `SELECT count() FROM vvp_kvartal FINAL` |
| `sber_finansovie_rezultaty` | `bank/` | `sberbank.com/…/sber_finansovie_rezultaty_ras_2022-2024.xlsx` (ссылка статична, обновляется вручную) | пн 09:23, недельный | `SELECT count() FROM sber_finansovie_rezultaty FINAL` |

Живой HEAD 2026-10-14: все четыре артефакта отвечают 200 и отдают `ETag` + `Last-Modified` (у трёх rosstat-файлов ещё и `Content-Length`). Имя файла несёт месяц или квартал (у Сбера — диапазон лет), поэтому новая волна источника меняет отпечаток сама и прогон остаётся полным, а между волнами чек даёт no-op: источник обновляется месяц/квартал, шаг — недельный. Производные этих импортёров (`series_catalog` + `v_rosstat_macro`) переехали в `AfterImport` (`rosstat.afterRosstatMacro`) — по §3.1 «версия последней»; у `sber_finansovie_rezultaty` производных нет, объявлены только `DataUrl`/`CrawFunc`/`PublicationCountPoints` (полный путь не изменился: DDL → скачивание → построчные `INSERT`).

**Отказы (26).** Пустой результат инвентаря — законный результат, поэтому отказ фиксируется причиной, а не пропуском.

*Критерий 1 — не один URL / API без версии (17):*

| Импортёр | Артефакт | Причина |
|---|---|---|
| `bea` | `apps.bea.gov/api/data` | JSON-API по ключу, параметры запроса |
| `bls` | `api.bls.gov/publicAPI/v2/timeseries/data/` | POST-API по ключу, HEAD теряет смысл |
| `bold_etf_flows`, `bold_gold_price` | `bold.report/api/v1/…` | JSON-API: 4–5 эндпоинтов на прогон, данные дневные T+1 |
| `eia` | `api.eia.gov/v2/…` | JSON-API по ключу с параметрами; ряд недельный |
| `fred` | `fred.stlouisfed.org/graph/fredgraph.csv?id=%s` | по URL на каждый из шести рядов, ряд дневной |
| `gold_prices`, `stock_prices`, `ofz_curve`, `keyrate_futures` | ISS MOEX | постраничный API (`start=%d`) и параметры `from`/`till`; ряды дневные |
| `wgc_etf_flows` | `fscdn.gold.org/api/v11/…` | JSON-API, два эндпоинта + break-cache дата в запросе |
| `gost_vehicle_safety_certificate` | `gost.ru/…/rest-api/libraries/search` | POST REST-API с токеном, шаг ежечасный |
| `news_watch` | 5 RSS-лент | пять URL, содержимое обновляется непрерывно (шаг 30 мин) |
| `rus_vtb_group_ifrs` | страница результатов ВТБ | на странице список xlsx по отчётным периодам (не один URL); сама страница сейчас 404 |
| `polyus_financial_metrics`, `databook_polyus` | `polyus.com/…/*.pdf` | по PDF на отчётный период (список в `polyus/pages.go`) |
| `customs_vneshn_torg` | листалка `customs.gov.ru` | файлы по месяцам (`структура…xlsx`), а сам артефакт на HEAD — 405 |

*Критерий 2 — обновляется в темпе шага или чаще (4):*

| Импортёр | Артефакт | Причина |
|---|---|---|
| `ipc_weeks` | `…/Nedel_ipc.xlsx` | файл перезаписывается по тому же URL, но в недельном темпе (шаг — недельный); HEAD и `ETag` есть — не хватает ровно критерия 2 |
| `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats` | `minfin.gov.ru/common/upload/library/ГГГГ/ММ/main/Prilozhenie_*_mes(…mesyats).xlsx` | каждый месяц новый URL в новой папке при месячном расписании (10-го числа) — no-op-прогонов не бывает; производные (`publishFedbud`) публикуются вне импортёра, поэтому порядок «точки → производные → версия» шаблоном не обеспечивается |
| `fao_food_price` | `fao.org/…/food_price_indices_data.csv` | CSV обновляется помесячно, шаг — 5-го числа каждого месяца |

*Критерий 3 — версионного сигнала нет (3):*

| Импортёр | Артефакт | Причина |
|---|---|---|
| `sber_consumper_spending_index`, `sber_izmenenie_trat` | `sberindex.ru/proxy/…` | HEAD → 500 (GET отвечает 200): `HeadFingerprint` отпечатка не построит, префлайт деградировал бы в полный импорт всегда |
| `domrf_mortgage` | `xn--d1aqf.xn--p1ai/upload/iblock/…xlsx` | URL отдаёт 404 (файл убран с портала) — ссылку надо менять вручную |

*Артефакта-URL нет вовсе (2):* `wgc_cb_reserves` (выгрузки ЦБ кладутся в папку данных вручную), `databook_ugk` (локальный `financial/data/ЮГК_databook_2024.xlsx`).

**Формально проходит, но не переведён (1): `tbank_group_ifrs`.** Один статичный URL, `ETag` и `Last-Modified` есть, ссылка меняется вручную реже недельного шага — но полный путь у импортёра сейчас всегда падает: ссылка ведёт на PDF, а разбор ожидает xlsx (`README.md`, раздел про ручные ссылки), и своей таблицы у импортёра нет — он пишет в `rus_vtb_group_ifrs` (`vtbIfrsTableDdl`/`vtbIfrsDataInsert`). Строка версии не записалась бы никогда, то есть точка контроля публикации была бы мёртвым кодом, а количественный чек объявить не на чем. Это отдельная задача (починка ссылки и таблицы), а не работа инвентаря; в перевод не берётся, чтобы не закреплять чужой дефект объявлением.

**Вне объёма инвентаря (6):** `dcf_engine`, `dcf_mine_plans`, `forecast_resolve`, `events_calendar`, `company_views`, `gold_views` — импортёры без артефакта: читают и создают таблицы и витрины, файл не качают (`events_calendar` — сид лежит в `sql/events_calendar_q4_2026.sql`).

### 3.8 Схема БД, агент и документация

Схема **не меняется**: `source_publications` уже канонична (`ARCHITECTURE.md` §6.2). Правится только комментарий колонки `source` (сейчас `-- источник-потребитель: 'cbr_mo_br'`) — **в двух копиях, которые обязаны совпадать**: `util/publications.go` (`PublicationsDdl`) и `ARCHITECTURE.md` §6.2.

Агенту `kimi_reader` таблица по-прежнему **не выдана**: `sql/mcp_kimi_reader.sql` и `scripts/mcp_check.py` не трогаются. Новых витрин нет, новых грантов нет.

## 4. Приёмка

1. `make all` — зелёный (gofmt, golangci-lint, `go vet`, `go test -race ./...`, обе сборки).
2. На каждого переведённого импортёра: первый `make import STAT=<имя>` — как раньше (`Imported N rows`), плюс строка версии в `source_publications` с `points` = фактическому числу точек ряда; повторный — `no-op, версия <fingerprint> уже импортирована, N точек на месте`, **без** `Get xlsx file` и без `собрано … точек` в логе; новых строк версии нет; счётчики витрин не изменились.
3. `cbr_mo_br` — регресс: повторный прогон даёт no-op с отпечатком `4fef4f25716d388e9048149fa296039c` и **9348** точками, как в приёмке 2026-10-13.
4. `cbr_households_b_mes` — `points` версии равен **7350** (уникальные точки), а не 9030 (ячейки листа); это и есть проверка решения §3.2.
5. Числа рядов совпадают с baseline фазы 3 (`docs/ROADMAP_KEY_RATE.md`, блок приёмки фазы 3) — расхождений нет.
6. `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА**; `dagu validate dagu/cbr.yaml` → exit 0.
7. Временные admin-SQL — через `make ch-sql`, файлы не коммитятся (прецедент — приёмка `cbr_mo_br`).

## 5. Документация того же PR

`ARCHITECTURE.md`: §3 — поля `PublicationSource`/`PublicationCountPoints`/`AfterImport` шаблона и хелперы `util.PublicationPreflight`/`CountPoints`; §6.1 — строка каждого переведённого импортёра; §6.2 — комментарий DDL `source_publications` и перечень потребителей; §8 п. 13 — конвенция без оговорки «миграция остальных — вне объёма», с фактическим списком. `README.md` — таблица импортёров `cbr` и расписание. `docs/ROADMAP_KEY_RATE.md` — новый пункт со статусом и блоком живой приёмки. Синхронизацию проверить скилом `sync-readme-architecture`.

## 6. Вне объёма

- Дневные импортёры (`cbr_key_rate`, `cbr_ruania`, `cbr_currency_usd`, `cbr_gold`) — префлайт неприменим (§2).
- Выдача `source_publications` агенту — решение прежнее: не выдавать.
- Новые витрины, новый каталог, изменения `v_cbr_macro`, `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`.
- Point-in-time слой фазы 4, кроме наблюдаемых версий файла.
- Правка `ingest/schema.go`: DDL `source_publications` там не дублируется (таблица создаётся `util.EnsurePublications`).

## 7. Риски

| Риск | Митигация |
|---|---|
| `points` = `batch.Rows()` не сходится с `count() FINAL` (дубли ячеек: `cbr_households_b_mes` 9030 против 7350) | Пишем результат запроса числа точек (§3.2); для `mo_br` числа совпадают — регресса нет |
| Сбой публикации каталога/витрины оставил бы версию записанной и заблокировал self-heal | `AfterImport` вызывается **до** записи версии; ошибка отменяет её запись — контракт §8 п. 13 сохранён (§3.1) |
| У `/vfs/statistics/…` нет `ETag` | Композит из `Last-Modified` + `Content-Length`, проверено живым HEAD (§2); описано в `HeadFingerprint` как штатное |
| Файл перезаписан в ту же секунду с тем же размером | Практически исключено для месячных выгрузок ЦБ; грозит лишним полным импортом, а не потерей данных |
| Забытое объявление у импортёра — префлайт молча выключен | Тест-энфорсер реестра (§3.6): ожидаемое множество имён литералами |
| Ошибка в `AfterImport` после успешного `Send` | Возвращается как есть: импортёр падает, версия не пишется, следующий прогон повторяет полный путь (идемпотентный upsert) |
| Правка общего `util.ClickHouseImport` задевает чужие пакеты | Поля опциональные, `AfterImport` по умолчанию nil; `make all` и `go test -race ./...` гоняют весь репозиторий |
