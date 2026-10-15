# Task 5 report — документы и живая приёмка

**Статус: DONE_WITH_CONCERNS.** Документы синхронизированы и закоммичены; **живая приёмка контура bold не пройдена** — источник отвечает `403` на боевые пути. Код не менялся (кроме синхронизации комментария канона DDL `macro_series` в `ingest/schema.go` — требование брифа, п. 3/§6.2; правка только в тексте SQL-комментария внутри строковой константы, поведение не меняется).

**Ветка:** `feat/bold-t1-gold` (рабочее дерево чистое).

**Коммит:** `528ef81d0c534f585c7b93cf9987a17a4da70d87` — `docs: bold t+1 gold contour` (4 файла, +82/−11: `ARCHITECTURE.md`, `README.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`, `ingest/schema.go`).

**Итог одной строкой:** `make all` зелёный, документы (ARCHITECTURE §2/§6.1/§6.2/§6.3/§6.5/§7 + шапка, README, роадмап) описывают контур bold корректно, но оба импортёра пишут 0 строк и падают с `http status 403`, потому что в константах путей `bold/parse.go` нет обязательного суффикса `.json`; ожидаемые приёмкой числа воспроизводятся из живого источника (проверено вручную по `*.json`) — то есть ждала именно ошибка URL, а не данных. PR и `git push` не делались (по контракту).

---

## 1. Приёмка: что запускалось и что вышло

Порядок — как в брифе.

### 1.1 `make import STAT=bold_gold_price` (площадка пуста — ожидался полный путь: 3138 точек)

```
go build -o build/clickhouse-import-rosstat-native .
CLICKHOUSE_IMPORT_STAT=bold_gold_price ./build/clickhouse-import-rosstat-native
level=info  msg="Connected to clickhouse"
level=error msg="bold_gold_price: bold: gold/price: http status 403: <?xml version=\"1.0\" encoding=\"UTF-8\"?> <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
level=info  msg="Imported 0 rows of bold_gold_price"
make: *** [import] Error 1
```

Факт: **0 строк, код выхода 1**, ошибка транспорта. Ожидания брифа (3138 точек, в логе `2026-10-09` / `4216.3`) недостижимы.

### 1.2 `make import STAT=bold_etf_flows` (ряды пусты — ожидался полный путь: 21054 точки + 73 строки среза)

```
level=info    msg="Connected to clickhouse"
level=warning msg="bold_etf_flows: сохранённый максимум дат не прочитан (code: 60, message: Unknown table expression identifier 'bold_fund_snapshot'. In scope SELECT maxOrNull(date) FROM bold_fund_snapshot FINAL) — считаю ряды пустыми и беру полную историю"
level=error   msg="bold_etf_flows: bold: gold/funds/units-latest: http status 403: <Error><Code>AccessDenied</Code>..."
level=info    msg="Imported 0 rows of bold_etf_flows"
make: *** [import] Error 1
```

Warn про `bold_fund_snapshot` — **штатный путь** (таблицы ещё нет, Ruling 7: ноль в любой половине уводит на полный путь), а не дефект. До остальных запросов полного пути дело не доходит: падает первый же запрос инкрементного набора, который на полном пути выполняется первым (спека §5).

### 1.3 Повторы (проверка идемпотентности)

```
=== repeat 1: bold_gold_price → level=error ... bold: gold/price: http status 403 ... Imported 0 rows ... Error 1
=== repeat 2: bold_etf_flows  → level=error ... bold: gold/funds/units-latest: http status 403 ... Imported 0 rows ... Error 1
```

Идемпотентность **проверить нечем**: ни один прогон не пишет ни строки, ключей в БД не появилось (см. §4 — `v_gold_prices` с `series='bold'` пуста, `bold_fund_snapshot` не существует). Ошибка детерминирована: 10 запросов подряд (2 прогона + 8 повторов `curl`) дают один и тот же `403`.

### 1.4 `make import STAT=gold_views` → `make mcp-user` → `make mcp-check`

```
View v_gold_etf_flows_daily created: true
View v_gold_etf_funds_daily created: false
Imported 0 rows of gold_views
```

Ровно ожидаемое поведение гейта `util.CreateView`: `v_gold_etf_flows_daily` гейтится по `macro_series` (есть) — создана; `v_gold_etf_funds_daily` гейтится по `bold_fund_snapshot` (её создаёт импортёр `bold_etf_flows`, который не отработал) — пропущена.

`make mcp-user` — **все 26 утверждений OK** (создание пользователя + 25 грантов), включая обе новые витрины:

```
OK   GRANT SELECT ON default.v_cb_gold_dynamics TO kimi_reader
OK   GRANT SELECT ON default.v_gold_etf_flows_daily TO kimi_reader
OK   GRANT SELECT ON default.v_gold_etf_funds_daily TO kimi_reader
make-exit=0
```

Наблюдение: ClickHouse 26.10.1 **принимает `GRANT` на ещё не существующую витрину** (`v_gold_etf_funds_daily` в БД нет — проверено `system.tables`; при этом `GRANT` отработал). Комментарий Makefile «GRANT на несуществующую витрину падает» для этого сервера не подтвердился — на прогон это не влияет (грант применится, когда витрина появится), но знать полезно.

`make mcp-check` — **ПРОВАЛЕНО: 3** (34 OK / 3 FAIL), все три — следствия `403`:

```
OK   витрина v_gold_etf_flows_daily видна: list_tables default
OK   витрина v_gold_etf_funds_daily видна: list_tables default        # ложное OK, см. ниже
OK   дневной агрегат bold читается: {"columns": ["count()"], "rows": [[0]]}
FAIL пер-фондовый срез bold читается: Query execution failed: ... Unknown table expression identifier 'v_gold_etf_funds_daily' (UNKNOWN_TABLE)
FAIL каталог bold полон: рядов bold: 0, want 7 — прогнать make import STAT=bold_etf_flows (каталог пишется после успешной записи)
FAIL сырой пер-фондовый срез закрыт: Query execution failed: ... Unknown table expression identifier 'bold_fund_snapshot' (UNKNOWN_TABLE)
ПРОВАЛЕНО: 3
make: *** [mcp-check] Error 1
```

Хедж из п. 4 брифа (`count()` приходит строкой) **не потребовался**: MCP отдаёт `count()` числом (`{"columns": ["n"], "rows": [[0]]}`), `scripts/mcp_check.py` не менялся — приёмка споткнулась не об это.

Отдельная находка (не правил, вне разрешённого хеджа): проверка «витрина `v_gold_etf_funds_daily` видна» даёт **ложное OK**, когда витрины нет: имя встречается в `COMMENT` витрины дневного агрегата, а `list_tables` возвращает `create_table_query` вместе с комментариями, и проверка ищет подстроку в тексте ответа. Реальный признак отсутствия — упавшее чтение (оно и упало). Зафиксировано в ARCHITECTURE §6.5.

### 1.5 MCP-сверка (пользователь `kimi_reader`, `mcp__clickhouse__run_query`)

```sql
SELECT toString(date) AS d, flow_tonnes, flow_usd_mln, flow_7d_tonnes, flow_30d_tonnes, flow_90d_tonnes, holdings_tonnes, aum_usd_bn
FROM v_gold_etf_flows_daily ORDER BY date DESC LIMIT 3;
-- {"columns": [], "rows": []}                       ← 0 строк (ожидалось 3: 2026-10-09, -08, -07)

SELECT count() AS funds, toString(max(date)) AS d FROM v_gold_etf_funds_daily;
-- ERROR code 60 UNKNOWN_TABLE 'v_gold_etf_funds_daily'. Maybe you meant default.v_gold_etf_flows_daily?

SELECT count() AS n, toString(max(date)) AS d, min(value) AS lo, max(value) AS hi
FROM v_gold_prices WHERE series='bold';
-- {"columns": ["n","d","lo","hi"], "rows": [[0, "1970-01-01", 0.0, 0.0]]}   ← 0 точек (max по пустому множеству)
```

Для контроля — состояние `v_gold_prices` по всем площадкам: `comex_front` 502 точки (max `2026-10-09`), `moex_fix_usd` 554 точки (max `2026-10-08`), `bold` — 0.

**Наблюдаемый пункт брифа** (максимум даты `v_gold_etf_flows_daily` обязан совпасть с максимумом `v_gold_etf_funds_daily`) проверить нельзя: первая пуста, вторая не существует. Числа зафиксированы выше; код не правил.

### 1.6 `make all`

```
make all exit=0        # lint (gofmt + golangci-lint) → go vet → go test -race ./... → обе сборки
```

Прогнал трижды (до правок, после правки `ingest/schema.go`, после коммита) — все три раза exit 0.

---

## 2. Причина: у эндпоинтов источника обязателен суффикс `.json`

`bold/parse.go:20-28`:

```go
boldBaseURL = "https://bold.report/api/v1/"
boldPathPrice       = "gold/price"
boldPathPriceLatest = "gold/price-latest"
boldPathUnits       = "gold/funds/units"
...
```

Живой источник отдаёт на эти ключи `403` (проверено `curl`, HTTP/2, несколько раз, с браузерным UA и `Referer`):

```
GET https://bold.report/api/v1/gold/price-latest      → 403  server: AmazonS3, x-cache: Error from cloudfront
                                                            body: <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
GET https://bold.report/api/v1/gold/price-latest.json → 200  {"version":"1","updated":"2026-10-11T04:32:04.826Z","date":"2026-10-09","value":4216.3}
```

`403 AccessDenied` (а не `404 NoSuchKey`) — это семантика CloudFront + S3 OAC без `s3:ListBucket`: объекта по такому ключу просто нет. Подтверждения, что дело именно в расширении:

- **страница самого источника** `https://bold.report/data-api/` перечисляет эндпоинты **с** `.json`/`.csv` (`/api/v1/gold/price.json`, `/api/v1/gold/funds/units-latest.json`, `/api/v1/gold/summary.json`, …), а HTML-атрибуты сайта (`data-endpoint="gold/price"` + `data-apipath="/api/v1"`) собирают имя файла добавлением `.json`;
- **веб-архив**: в CDX для `bold.report/api/v1/*` есть только `.json`/`.csv`-варианты — расширенных путей без суффикса не зафиксировано ни разу;
- суффикса нет и в свежих `HEAD`-запросах, то есть это не флаки-поведение: 10 запросов подряд → `403`.

Вывод: боевые константы путей в `bold/parse.go` неверны с самого начала (коммит `5b2341a`), а офлайн-тесты (`httptest` + фикстуры `bold/testdata/`) этого поймать не могли — httptest отвечает на любой путь. **Правку кода не делал** (прямой запрет брифа); зафиксировал в роадмапе и в README («Ограничения»).

---

## 3. Что удалось проверить по живому источнику (ожидания брифа воспроизводятся)

Запросил документированные `*.json`-эндпоинты вручную и посчитал строки/значения:

| файл источника | строк | строк со значениями | последняя дата |
|---|---|---|---|
| `gold/price.json` | **3138** | 3138 (`value`) | `2026-10-09` = **4216,3** |
| `gold/funds/units.json` | 3072 | **3008** (`units`) | `2026-10-09` `units=113180843`, `price=4216.3` |
| `gold/funds/aum.json` | 3072 | **3008** (`aum`) | `2026-10-09` `aum=476677811738` |
| `gold/flows/all.json` | 3138 | **3008** (все три `culm*`) | `2026-10-09` `culm7/30/90 = 43740 / 991060 / 5376055` |
| `gold/summary.json` | — | **73** фонда | (даты у среза нет — по спецификации) |

Невязка ожидаемых чисел:

- **21054** = 3008 запасов + 3008 AUM + 3×3008 окон (7/30/90) + 2×3007 дневного потока — сходится точно;
- `aum` 476 677 811 738 ÷ 1e9 = **476,678** млрд $ (ожидание брифа — `476.678`);
- `units` 113 180 843 ÷ 32150,7465686 = **3520,318** т (ожидание — `3520.318`);
- `culm7/30/90` ÷ 32150,7465686 = **1,360 / 30,825 / 167,214** т (ожидание — `1.360 / 30.825 / 167.214`);
- цена — **4216,3** за `2026-10-09` (ожидание — `4216.3`);
- 73 фонда, в том числе три `GLD` с разными именами (ключ `(date, ticker, name)` обязателен — подтверждается живым срезом).

То есть числа приёмки реальны и воспроизводимы; не работает ровно передача URL. После добавления `.json` к константам путь должен пройти (проверить в рамках приёмки нельзя — правка кода вне объёма задачи).

---

## 4. Документы: что изменено

Все правки — коммит `528ef81`.

### `ARCHITECTURE.md` (+52/−11)

- **Шапка («Последнее обновление»)** — новая первая запись о контуре bold: пакет `bold/`, два импортёра, `bold_fund_snapshot`, площадка `bold`, §6.1–§6.3, §7 (08:07 МСК) и ⚠️ о непройденной приёмке со ссылкой на README и роадмап; прежняя запись про WGC стала «ранее 2026-10-12».
- **§2 (структура)** — добавлен блок `bold/` (источник, два импортёра, таблица среза, витрины, расписание, офлайн-тесты).
- **§3** — правок нет: раздел описывает паттерн реестра и хелперы `util/`; bold новых хелперов не добавляет (`util.GetJSON` уже описан записью WGC), пакет в §3 не упоминается.
- **§5 (env)** — правок нет: **новых переменных окружения контур не вводит** (`os.Getenv` в `bold/` нет вовсе — ключей у источника нет, адрес захардкожен константой).
- **§6.1** — две строки таблицы импортёров: `bold_etf_flows` и `bold_gold_price`. Указаны: источник (JSON API без ключа + обязательный суффикс `.json` и связь с провалом приёмки), режимы (4 запроса инкремента / 8 полного пути у ETF, 1 запрос у цены), три триггера полного пути (пустая таблица; расхождение с сохранённым максимумом > 4 календарных дней / >96 ч; расхождение половин «ряды ≠ срез» с взятием минимума), охват ≈83 % мировой величины WGC, T+1, fail-fast-набор, ключ среза `(date, ticker, name)`, каталог 7 рядов, тесты, расписание и витрины. Плюс дополнен абзац со списком `Name()` (добавлены оба имени и абзац о двух импортёрах пакета и порядке шагов в `dagu/bold.yaml`).
- **§6.2** — (а) канонический DDL `macro_series`: комментарий `source` дополнен `'bold'`, **и копия синхронизирована в `ingest/schema.go`** (как требует бриф); (б) в перечень реализованных таблиц добавлен `bold_fund_snapshot`; (в) в перечень значений `source` добавлено `bold`; (г) в блок канонических DDL добавлен полный DDL `bold_fund_snapshot` (ключ `(date, ticker, name)`, объяснение, почему тикер не уникален, откуда берётся дата, что история копится с первого прогона) — побайтово совпадает с `boldFundDdl` из `bold/bold.go`; (д) комментарий `venue` в DDL `gold_prices` дополнен `'bold'`.
- **§6.3** — две новые витрины (их в разделе не было, хотя ссылки из описаний контура уже стояли): `v_gold_etf_flows_daily` (8 колонок, пивот семи рядов `source='bold'`, `maxIfOrNull`, NULL = «ряда за день нет», оговорки про ≈83 % и календарные окна) и `v_gold_etf_funds_daily` (10 колонок из `bold_fund_snapshot`, ключ с именем фонда, гейт по таблице, порядок строк). В описание `v_gold_prices` добавлена третья площадка `bold`.
- **§6.5 (MCP-контур, правило 11)** — абзац про контур bold: каталог ведёт сам импортёр, витрины с комментариями, гранты в `sql/mcp_kimi_reader.sql`, сырая `bold_fund_snapshot` агенту не выдана, проверки в `scripts/mcp_check.py`, состояние на приёмке и оговорка про ложное «витрина видна».
- **§7 (календарь триггеров)** — строка «ежедневно»: `dagu/bold.yaml`, 08:07 МСК (источник готов к 07:32 МСК), цена дня и ETF-контур; что читается витринами; оговорка «73 фонда ≈83 %, не складывать с мировыми рядами WGC».

### `README.md` (+20/−3)

- Таблица «Что импортируется» — строка домена `bold` (источник, таблицы/каталог/витрины, расписание 08:07 и ручные команды).
- Прозаический абзац про контур bold после абзаца WGC: T+1, отсутствие ключей и обязательный `.json`, агрегат 73 фондов ≈83 %, непересекаемость с мировыми рядами WGC, календарные окна, пер-фондовая история с первого прогона, размеры прогонов (4+1 запроса / 8 на полном пути), расписание цепочкой, гейт `gold_views` + блок команд `make import`.
- «Структура репозитория» — строка `bold/`; список витрин в строке `views/` дополнен двумя новыми.
- Таблица `views` в «Что импортируется» и перечень в «Контуре прогноза золота» — добавлены `v_gold_etf_flows_daily`, `v_gold_etf_funds_daily`.
- «Порядок проверки окружения» — три команды (`bold_gold_price`, `bold_etf_flows`, `gold_views`).
- «Ограничения и известные проблемы» — **новый первый пункт**: провал живого прогона 2026-10-12 (`403`, причина, что именно пусто в БД, что подтверждено `curl`, что чинит), со ссылкой на роадмап.

### `docs/GOLD_THESIS_DATA_ROADMAP.md` (+19)

- Новый пункт **«Фаза 3 — follow-up 4: T+1 золото из bold.report (ETF-контур и цена)»** после follow-up 3, перед «Фазой 4»: пробел, что даёт, метод (пакет, импортёры, витрины, режимы, триггеры, расписание) и **отдельная пометка, что ранее запланированный парсинг эмитентов (топ-4 американских ETF, ≈42,5 %) снят в пользу агрегатора** (один источник вместо трёх-четырёх парсеров, охват ≈83 % против 42,5 %, пер-фондовая детализация бонусом).
- Статус пункта — **⚠️ «реализован, живая приёмка не пройдена»**, а не «✅ выполнен» (см. §5 сомнений); в разделе приёмки: вывод обоих прогонов `make import` и причины отказа, повторные прогоны, разбор `403`/`.json`, таблица сверки живого источника с ожидаемыми числами, состояние MCP и `make mcp-check` (ПРОВАЛЕНО: 3), `make all` зелёный, а также «Что осталось» (правка суффикса + чек-лист повторной приёмки) и ограничения данных.

### `ingest/schema.go` (+1/−1)

- Комментарий канона DDL `macro_series` в копии: `-- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold'` (требование брифа п. 3/§6.2 — «синхронизировать копию»). Это текст SQL-комментария внутри строковой константы `macroSeriesDdl`: поведение (`EnsureTables`) не меняется, `make all` зелёный, тесты не пинят этот текст (проверено поиском по репозиторию).

---

## 5. Сомнения и расхождения с брифом (для вызывающего)

1. **Главное: приёмка не пройдена, и починить её в рамках контракта нельзя.** Причина — отсутствие суффикса `.json` в константах `bold/parse.go` (коммит `5b2341a`), а не данные и не БД. Правка кода прямо запрещена брифом («код не менять», «не подгоняй числа и не правь код: зафиксируй факт и верни DONE_WITH_CONCERNS»). Одна строка на путь (9 констант) закрывает всё: после неё ожидаются 3138 точек цены, 21054 + 73 у ETF и зелёные витрины/`mcp-check`.
2. **Формулировка роадмапа отклонена от буквы задания.** Бриф просил пометку «✅ выполнен» с результатами приёмки; я поставил ⚠️ «реализован, живая приёмка не пройдена» и привёл фактические результаты. «✅ выполнен» без пройденной приёмки был бы ложью в документе, по которому дальше работают люди и агенты. Если для вызывающего важнее буква — статус меняется одной строкой после починки URL.
3. **README/ARCHITECTURE описывают источник с `.json`, а код — без.** Писал документы так, чтобы противоречие было видно и объяснено (в §6.1 и в «Ограничениях» прямо сказано: у источника суффикс обязателен, в константах его нет). Это осознанное «документируй реальность, а не баг» — если предпочтительнее формулировка «в коде то же, что в доке», нужно править код.
4. **`ingest/schema.go` — формально код.** Правка сделана, потому что бриф явно требует «синхронизировать копию в `ingest/schema.go`»; изменён только комментарий внутри DDL-строки, поведение и тесты не затронуты (`make all` зелёный). Если «код не менять» трактуется буквально и для этого файла — правку надо откатить (тогда комментарии канона и копии разойдутся, а это прецедент, за которым в репозитории следят).
5. **`scripts/mcp_check.py` не менялся:** хедж про `count()`-строкой не понадобился (MCP отдаёт число). Зато нашлась другая слабость проверки — ложное «витрина `v_gold_etf_funds_daily` видна» из-за имени в `COMMENT` соседней витрины (проверка ищет подстроку в тексте `list_tables`); чинить не стал (вне разрешённого хеджа), зафиксировал в ARCHITECTURE §6.5.
6. **Состояние локальной БД после приёмки:** создана пустая `v_gold_etf_flows_daily`, гранты `kimi_reader` включая несуществующую `v_gold_etf_funds_daily` (ClickHouse принял GRANT на отсутствующую витрину), `make mcp-check` красный (3 проверки bold). Это ожидаемое следствие `403` и лечится повторным прогоном шагов 1–4 после правки URL: `make import STAT=bold_gold_price` → `make import STAT=bold_etf_flows` → `make import STAT=gold_views` → `make mcp-user` → `make mcp-check` (и повтор шагов 1–2 для проверки идемпотентности).
7. **Не делал** (по контракту): PR, `git push`, правки `Makefile`/CI, правки кода контура. Секреты не читались и не печатались (все команды — через `make`, пароль нигде не выводился).

---

# Fix round 1 — Ruling 14 (суффикс `.json`), Ruling 15 (ложное OK) и зелёная приёмка

**Статус: DONE — приёмка пройдена полностью.** Найденный на первой приёмке дефект был настоящим; после фикса все шаги runbook проходят, все ожидаемые числа сошлись.

**Коммиты:**
- `1a50a60b8c9b875f426fbb6242bcab61401e001a` — `bold: require .json suffix on api paths` (`bold/parse.go`, `bold/parse_test.go`, `bold/price_test.go`, `scripts/mcp_check.py`);
- `d5b4d6ea8b1d068f40d12407fe2da17a5d757ba8` — `docs: bold t+1 gold acceptance` (`ARCHITECTURE.md`, `README.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`).

Некоммитнутые правки контролёра в `docs/superpowers/plans/2026-10-12-bold-t1-gold.md` и `docs/superpowers/specs/2026-10-12-bold-t1-gold-design.md` **не трогал и в коммиты не включал** — остались в дереве как были (`git status` показывает их как ` M` и после обоих коммитов).

## 1. Ruling 14 — суффикс `.json` у всех девяти путей

`bold/parse.go`: `gold/price.json`, `gold/price-latest.json`, `gold/funds/units.json`, `gold/funds/units-latest.json`, `gold/funds/aum.json`, `gold/funds/aum-latest.json`, `gold/flows/all.json`, `gold/flows/all-latest.json`, `gold/summary.json`. В комментарий к блоку констант добавлено объяснение: источник — статика S3 за CloudFront, ключа без расширения нет, CDN маскирует это `403 AccessDenied` (не `404`); офлайн-`httptest` отвечает на любой путь и потому дефект не ловит.

Пин в `bold/parse_test.go` (`TestBoldEndpointsPinPaths`): в таблице ожиданий все девять путей теперь с `.json`, плюс отдельная проверка в цикле — `strings.HasSuffix(p.got, ".json")`, иначе `t.Fatalf` с объяснением про `403`/`httptest`. Doc-комментарий теста дополнен.

**Одна правка сверх буквы Ruling 14 (сообщаю отдельно):** `bold/price_test.go` сверял `strings.Contains(m.Origin, boldPathPrice+".json")` — после фикса константы это стало `gold/price.json.json` и тест падал. Заменил на `strings.Contains(m.Origin, boldPathPrice)` (origin каталога в `gold/series_meta.go` суффикс уже несёт). Правка в тесте, поведение кода не меняет.

Проверка «нигде не осталось пути без суффикса» — `grep -rn "bold\.report\|boldPath" bold/`: пути без `.json` в **коде** не осталось, все запросы собираются из констант. Осталось одно место, которое я **не трогал** по вашему запрету на тексты каталога:

- `bold/series_meta.go:18` — `boldETFOrigin = "bold.report/api/v1/: gold/funds/units, gold/funds/aum, gold/flows/all …"`. Это текст записи `series_catalog.origin` (и он попадает агенту), пути в нём — без `.json`, то есть по такому тексту руками получить данные нельзя. Поведение импортёра не затронуто; если хотите — поправлю отдельным коммитом (`gold/funds/units.json` и т. д.), это правка строки каталога.

## 2. Ruling 15 — существование витрины проверяется запросом, а не подстрокой

`scripts/mcp_check.py`, блок bold: вместо `view in tables` (поиск имени в тексте ответа `list_tables`, где лежат `create_table_query` с комментариями) — по одной проверке на витрину, которая **сама** доказывает существование и читаемость:

```python
for view, label in (("v_gold_etf_flows_daily", "дневной агрегат bold"),
                    ("v_gold_etf_funds_daily", "пер-фондовый срез bold")):
    bold_view = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {view}"}))
    bold_view_rows = result_rows(bold_view)
    ...
    failed += not expect(f"витрина {view} существует и читается ({label})", bool(bold_view_rows), bold_detail)
```

`result_rows` возвращает `None` на ошибке (`UNKNOWN_TABLE`, `ACCESS_DENIED`), поэтому «витрины нет» и «нет прав» дают FAIL, а не ложное OK; пустая витрина остаётся успехом (значение `count` не проверяется — до первого прогона строк нет вовсе). Комментарий в файле объясняет, почему подстрока `list_tables` непригодна, с историей дефекта.

Результат (`make mcp-check` после приёмки, блок bold):

```
OK   витрина v_gold_etf_flows_daily существует и читается (дневной агрегат bold): строк: 3008
OK   витрина v_gold_etf_funds_daily существует и читается (пер-фондовый срез bold): строк: 73
OK   каталог bold полон: рядов bold: 7
OK   сырой пер-фондовый срез закрыт: отказ ClickHouse 497 на bold_fund_snapshot (ожидаемо)
ПРОВЕРКА ПРОЙДЕНА
```

Старые проверки не трогал. **Тот же изъян у них есть** — проверка существования через `view in tables`; ложное OK случится, если имя пропавшей витрины процитировано в комментарии *другой* витрины. Вычислил точную карту ссылок по `system.tables` (MCP, `kimi_reader`):

```sql
SELECT a.name AS view_with_comment, b.name AS mentioned_in_comment
FROM system.tables AS a CROSS JOIN system.tables AS b
WHERE a.database = 'default' AND b.database = 'default'
  AND a.name != b.name AND a.comment LIKE concat('%', b.name, '%');
```

Из проверяемых `in tables` витрин имя реально цитируется у соседей у: `v_company_financials` (назван в комментариях `v_company_metric_sources` и `v_company_operating`), `v_company_metric_sources` (назван в `v_company_financials`), `v_gold_etf_flows` (назван в `v_gold_etf_funds_daily`) и `v_cb_gold_dynamics` (назван в `v_imf_macro`). У `v_bea_pce`, `v_eia_petroleum`, `v_gold_forecasts`, `v_dcf_assumptions`, `v_company_operating` ссылок нет — их проверки от этого изъяна не страдают. Сейчас ложных OK не видно только потому, что витрины существуют; если любая из четырёх пропадёт, `in tables`-проверка этого не заметит (у bold-витрин это уже исправлено, и второй проверкой «читается» пропажа всё равно ловилась бы). Не правил — по вашей инструкции.

## 3. Полный runbook приёмки (после фикса)

### 3.1 `make import STAT=bold_gold_price` — полный путь

```
level=info msg="bold_gold_price: полный режим: сохранённый максимум (таблицы пусты), точек истории 3138"
level=info msg="Imported 3138 rows of bold_gold_price (цена gold/price.json: площадка bold в gold_prices)"
level=info msg="Imported 3138 rows of bold_gold_price"
exit=0
```

Ожидание брифа «в логе дата `2026-10-09`, значение `4216.3`»: **число точек (3138) в логе есть, а дата/значение полным путём не логируются** — `boldGoldPrice.Import` печатает `точек истории 3138` и в финальной строке называет эндпоинт/площадку/таблицу. Дату и значение подтвердил запросом через MCP:

```sql
SELECT toString(date) AS d, value FROM v_gold_prices WHERE series = 'bold' ORDER BY date DESC LIMIT 3;
-- 2026-10-09 = 4216.3 | 2026-10-08 = 4199.4 | 2026-10-07 = 4160.8
SELECT count() AS n, toString(min(date)) AS lo, toString(max(date)) AS hi, min(value) AS min_value, max(value) AS max_value
FROM v_gold_prices WHERE series = 'bold';
-- 3138 | 2014-10-01 | 2026-10-09 | 1050.6 | 5557.9
```

Код логов не менял (это не дефект: инкрементная ветка дату печатает, полная — объём).

### 3.2 `make import STAT=bold_etf_flows` — полный путь

```
level=warning msg="bold_etf_flows: сохранённый максимум дат не прочитан (code: 60 … 'bold_fund_snapshot' …) — считаю ряды пустыми и беру полную историю"
level=info    msg="bold_etf_flows: полный режим: сохранённый максимум (таблицы пусты), дата источника 2026-10-09, дней истории 3008"
level=info    msg="Imported 21054 rows of bold_etf_flows (bold ETF: 21054 точек, 73 фондов)"
exit=0
```

Warn про `bold_fund_snapshot` — штатный путь первого прогона (таблицы ещё нет), не ошибка. **21054** точки + **73** строки среза — как ожидалось; в логе дата источника `2026-10-09` и 3008 торговых дней.

### 3.3 Повторы — идемпотентный no-op

```
bold_gold_price: дата источника 2026-10-09 не новее сохранённого максимума 2026-10-09 — нечего обновлять (0 строк)
bold_etf_flows:  дата источника 2026-10-09 не новее сохранённого максимума 2026-10-09 — нечего обновлять (0 строк)
```

Оба — `Imported 0 rows`, код выхода 0: инкрементный путь (4 запроса у ETF + 1 у цены), ни одного DDL/батча/каталога.

### 3.4 `gold_views` → `mcp-user` → `mcp-check` → `make all`

```
View v_gold_etf_flows_daily created: true
View v_gold_etf_funds_daily created: true
OK   GRANT SELECT ON default.v_gold_etf_flows_daily TO kimi_reader
OK   GRANT SELECT ON default.v_gold_etf_funds_daily TO kimi_reader        (26/26 OK)
ПРОВЕРКА ПРОЙДЕНА                                                          (make mcp-check, exit 0)
make all exit=0                                                            (gofmt, golangci-lint, go vet, go test -race, обе сборки)
```

### 3.5 MCP-сверка — числа сошлись

`v_gold_etf_flows_daily` (`ORDER BY date DESC LIMIT 3`):

| дата | flow_tonnes | flow_usd_mln | flow_7d | flow_30d | flow_90d | holdings_tonnes | aum_usd_bn |
|---|---|---|---|---|---|---|---|
| 2026-10-09 | 4.232561122944 | 573.754104 | 1.360466075232 | 30.825411717408 | 167.214001968024 | 3520.3177244549426 | 476.677811738 |
| 2026-10-08 | −12.319465090944 | −1663.298352 | −0.7816614754608 | 27.6500266674192 | 166.06553719066082 | 3516.0851633319985 | 469.507787742 |
| 2026-10-07 | 5.5416131510256 | 741.3172536 | 13.1113907137488 | 42.047950492046404 | 175.3008748325472 | 3528.4046284229426 | 470.077716723 |

Ожидание брифа `4.233 / 573.75 / 1.360 / 30.825 / 167.214 / 3520.318 / 476.678` — **совпало** при округлении до указанных знаков (все семь значений).

Состав витрины и наблюдаемый пункт:

```sql
SELECT count() AS days, countIf(flow_tonnes IS NOT NULL) AS flow_days, countIf(flow_usd_mln IS NOT NULL) AS usd_days,
       countIf(holdings_tonnes IS NOT NULL) AS holdings_days, countIf(aum_usd_bn IS NOT NULL) AS aum_days,
       toString(min(date)) AS first_day, toString(max(date)) AS last_day FROM v_gold_etf_flows_daily;
-- 3008 | 3007 | 3007 | 3008 | 3008 | 2015-04-01 | 2026-10-09

SELECT count() AS funds, toString(max(date)) AS d FROM v_gold_etf_funds_daily;
-- 73 | 2026-10-09

SELECT ticker, name, units_oz, aum_usd FROM v_gold_etf_funds_daily WHERE ticker = 'GLD' ORDER BY name;
-- GLD | KTAM Gold ETF Tracker Class GLD (Bangkok) | 9903 | 41483159
-- GLD | NewGold Issuer Ltd                     | 5221 | 21848470
-- GLD | SPDR Gold Shares                       | 33552628 | 141684193497

SELECT (SELECT toString(max(date)) FROM v_gold_etf_flows_daily) AS flows_max,
       (SELECT toString(max(date)) FROM v_gold_etf_funds_daily) AS funds_max,
       (SELECT max(date) FROM v_gold_etf_flows_daily) = (SELECT max(date) FROM v_gold_etf_funds_daily) AS same_day;
-- 2026-10-09 | 2026-10-09 | 1     ← наблюдаемый пункт брифа: максимумы совпадают
```

Дополнительно (сходимость разложения 21054): поток дня заполнен в 3007 днях из 3008 — у первой точки ряда потока нет, как и задокументировано; запасы и AUM — во всех 3008 днях; пер-фондовая витрина: 73 строки при **71** уникальном тикере (три `GLD`), обе даты — `2026-10-09` (пер-фондовая история начинается с первого прогона).

## 4. Документы после приёмки

- **`docs/GOLD_THESIS_DATA_ROADMAP.md`**: статус пункта переведён в **«✅ выполнен 2026-10-12»**; раздел приёмки переписан с фактическими результатами (команды + числа: 3138 / 21054 + 73 / no-op / ПРОВЕРКА ПРОЙДЕНА / 3008 и 73 строки / 7 рядов каталога / совпадение максимумов), плюс отдельный абзац «Дефект, найденный первой приёмкой, и его фикс» (причина `403`, офлайн-`httptest` слеп к суффиксу, фикс `bold: require .json suffix on api paths`). Ограничения данных — прежние.
- **`ARCHITECTURE.md`**: шапка — приёмка пройдена, суффикс закреплён тестом; §6.1 — обе строки импортёров с фактическими числами приёмки, формулировка «в константах суффикса нет — причина провала» заменена на «суффикс стоит у всех девяти констант и пришпилен тестом»; §6.3 — обе витрины с числами (3008 дней / 3007 потоков / 73 фонда, 71 тикер, три `GLD`); §6.5 — проверки описаны в новой редакции (существование через запрос) + подтверждение «ПРОВЕРКА ПРОЙДЕНА» и оговорка про ненадёжность `list_tables`-подстроки.
- **`README.md`**: пункт «Ограничения» переписан — теперь это описание свойства источника (суффикс обязателен) с историей дефекта, датой обнаружения и ссылкой на фикс; сам пункт остался (предупреждение для тех, кто будет править пути).
- Пара «скобки» в шапке `ARCHITECTURE.md`: при правке восстановил баланс до исходного (`-1`, как было до контура bold) — добавил открывающую скобку к записи WGC, чья закрывающая скобка осталась в строке.

## 5. Сомнения фикс-раунда

1. **`bold/series_meta.go` (`boldETFOrigin`) по-прежнему описывает пути без `.json`** — не менял по вашему запрету на тексты каталога. Это единственное место, где осталось «путь без суффикса»; агент читает его как происхождение рядов. Готов поправить отдельным коммитом.
2. **Проверки старых витрин в `mcp_check.py`** остались на подстроке `list_tables`; из них реально уязвимы `v_company_financials`, `v_company_metric_sources`, `v_gold_etf_flows`, `v_cb_gold_dynamics` (карта ссылок — §2). Не правил, как договорились; это отдельная задача.
3. **Правка `bold/price_test.go`** (снял `+".json"` в ожидании origin) — формально за рамками «код + тест + скрипт» из Ruling 14, но без неё `make all` красный после фикса констант. Сообщаю, чтобы не выглядело скрытым изменением.
4. **Логи цены на полном пути не печатают дату/значение** — ожидание брифа «в логе дата и значение» выполняется только инкрементной веткой; дату и значение подтвердил MCP-запросом (3138 точек, `2026-10-09` = 4216.3). Код логов не менял (не просили, и это не дефект данных).
5. **Пер-фондовая история начинается с прогона** (`min(date) = 2026-10-09`, 73 строки): у источника истории по фондам нет — это свойство источника, зафиксировано в каталоге и в ARCHITECTURE §6.3.

---

# Fix round 2 — Ruling 16 (agent-facing origin и публикация каталога на no-op)

**Статус: DONE.** Коммит `34c63f295ba8780ab5f9a5d58a2d5c9fa2231513` — `bold: fix catalog origins and publish catalog on no-op` (3 файла, +63/−19: `bold/series_meta.go`, `bold/bold.go`, `bold/bold_test.go`). Правки контролёра в `docs/superpowers/…` не тронуты и в коммит не вошли; тексты `ARCHITECTURE`/`README`/роадмапа не менялись (роадмап остался со статусом ✅ — он и без этой правки верен).

## 1. `boldETFOrigin` собирается из констант (все пути — с `.json`)

Текст больше не дублирует литералы: `const boldETFOrigin` — конкатенация констант `boldPath*` (константное выражение, остался `const`), поэтому расхождение с боевыми путями теперь невозможно по построению:

```go
const boldETFOrigin = "bold.report/api/v1/: " + boldPathUnits + ", " + boldPathAum + ", " + boldPathFlows + " " +
	"(ответы *-latest на инкременте, файлы истории — в полном режиме); " +
	"пер-фондовый срез — " + boldPathSummary + " (таблица bold_fund_snapshot), " +
	"цена дня — " + boldPathPrice + " (ряд gold/bold в gold_prices)"
```

Проверил остальные тексты: в `bold/series_meta.go` других путей нет (`boldETFScope`, `Title`, `Description` путей API не называют); `gold/series_meta.go` (`BoldPriceSeriesMeta`, ряд `source='gold'`, `series='bold'`) уже нёс `bold.report/api/v1/gold/price.json (инкремент — gold/price-latest.json)` — **не менял**.

Пин в тесте: `TestBoldEtfCatalogDescribesEverySeries` теперь дополнительно требует, чтобы `Origin` каждой из семи записей совпадал с общим `boldETFOrigin` и называл все пять боевых эндпоинтов (`boldPathUnits`, `boldPathAum`, `boldPathFlows`, `boldPathSummary`, `boldPathPrice`) — а они, по `TestBoldEndpointsPinPaths`, обязаны оканчиваться на `.json`. Комментарии и теста, и константы ссылаются на Ruling 16.

## 2. Каталог публикуется и в ветке no-op

`boldEtfFlows.Import`: в ветке «дата источника не новее сохранённого максимума» теперь после лога вызывается `util.UpsertSeriesCatalog(ctx, conn, boldETFSeriesMeta())` и ошибка публикации возвращается как ошибка импорта:

```go
if !stored.IsZero() && !latestDate.After(stored) {
    ...
    log.Infof("%s: дата источника %s не новее сохранённого максимума %s — нечего обновлять (0 строк, каталог перепубликован)", ...)
    if cerr := util.UpsertSeriesCatalog(ctx, conn, boldETFSeriesMeta()); cerr != nil {
        return 0, fmt.Errorf("%s: каталог рядов: %w", boldEtfName, cerr)
    }
    return 0, nil
}
```

Обоснование в комментарии: предусловие ветки — непустой сохранённый максимум, то есть ряды в таблице уже есть, и их описания обязаны быть актуальны без новых данных (иначе правка каталога доезжает до агента только вместе с новыми точками, а их в выходные и праздники нет). Doc-комментарий `Import` (шаг 3) обновлён: «ни батча, ни DDL, ни чтения стока, но каталог рядов публикуется».

Тест `TestBoldEtfImportNoopWhenUpToDate` переписан: проверяет, что **данные не тронуты** (нет `exec: CREATE TABLE … bold_fund_snapshot`, нет `exec: CREATE TABLE … macro_series`, нет `preparebatch: INSERT INTO macro_series …` и `… bold_fund_snapshot`, нет запроса за предыдущим стоком `etf_bold_holdings_tonnes`), что оба максимума прочитаны, что **появился батч каталога** (`conn.batchFor(t, "INSERT INTO series_catalog")`, `sent == true`, 7 строк) и что запросов к источнику по-прежнему 4.

## 3. Живая проверка

```
$ make import STAT=bold_etf_flows
level=info msg="bold_etf_flows: дата источника 2026-10-09 не новее сохранённого максимума 2026-10-09 — нечего обновлять (0 строк, каталог перепубликован)"
level=info msg="Imported 0 rows of bold_etf_flows"          exit=0

$ make import STAT=bold_gold_price
level=info msg="bold_gold_price: дата источника 2026-10-09 не новее сохранённого максимума 2026-10-09 — нечего обновлять (0 строк)"
level=info msg="Imported 0 rows of bold_gold_price"         exit=0
```

MCP (`kimi_reader`) — каталог `source='bold'` в БД:

```sql
SELECT series, origin FROM v_series_catalog WHERE source='bold' ORDER BY series LIMIT 1;
-- etf_bold_aum_usd_bn | bold.report/api/v1/: gold/funds/units.json, gold/funds/aum.json,
--   gold/flows/all.json (ответы *-latest на инкременте, файлы истории — в полном режиме);
--   пер-фондовый срез — gold/summary.json (таблица bold_fund_snapshot),
--   цена дня — gold/price.json (ряд gold/bold в gold_prices)

-- ни одного эндпоинта без суффикса: 0 строк из 7 «плохих», origin у всех семи один и тот же
SELECT countIf(position(origin,'gold/funds/units.json')=0 OR position(origin,'gold/funds/aum.json')=0
   OR position(origin,'gold/flows/all.json')=0 OR position(origin,'gold/summary.json')=0
   OR position(origin,'gold/price.json')=0) AS rows_without_json, count() AS rows_total,
   uniqExact(origin) AS distinct_origins FROM v_series_catalog WHERE source='bold';
-- 0 | 7 | 1

-- все токены вида gold/… в origin: единственный без .json — имя РЯДА (не путь API)
SELECT series, p AS token, endsWith(p,'.json') AS json_suffix FROM v_series_catalog
ARRAY JOIN extractAll(origin, 'gold/[a-z/.-]+') AS p
WHERE source='bold' AND endsWith(p,'.json') = 0 ORDER BY series, token;
-- 7 строк «gold/bold» (фрагмент «ряд gold/bold в gold_prices») — серия, а не эндпоинт

-- в описаниях путей API нет вовсе (единственный токен — то же имя ряда gold/bold)
SELECT series, p FROM v_series_catalog ARRAY JOIN extractAll(description,'gold/[a-z/.-]+') AS p
WHERE source='bold';
-- etf_bold_aum_usd_bn | gold/bold

-- ряд цены (source='gold', series='bold') — тоже с .json, не менялся
SELECT source, series, origin FROM v_series_catalog WHERE series='bold';
-- gold | bold | bold.report/api/v1/gold/price.json (инкремент — gold/price-latest.json)
```

Данные после каталожных прогонов не изменились (как и должно быть):

```sql
SELECT toString(date) AS d, round(flow_tonnes,3) AS flow_tonnes, round(flow_usd_mln,2) AS flow_usd_mln,
       round(flow_7d_tonnes,3) AS f7, round(flow_30d_tonnes,3) AS f30, round(flow_90d_tonnes,3) AS f90,
       round(holdings_tonnes,3) AS holdings, round(aum_usd_bn,3) AS aum
FROM v_gold_etf_flows_daily ORDER BY date DESC LIMIT 1;
-- 2026-10-09 | 4.233 | 573.75 | 1.36 | 30.825 | 167.214 | 3520.318 | 476.678

SELECT (SELECT count() FROM v_gold_etf_flows_daily) AS flows_rows,
       (SELECT count() FROM v_gold_etf_funds_daily) AS funds_rows,
       (SELECT max(date) FROM v_gold_etf_flows_daily) = (SELECT max(date) FROM v_gold_etf_funds_daily) AS same_day,
       (SELECT count() FROM v_gold_prices WHERE series='bold') AS price_rows;
-- 3008 | 73 | 1 | 3138
```

`make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА** (35 OK / 0 FAIL; bold-блок: обе витрины читаются — 3008 и 73, каталог `bold` = 7, сырая `bold_fund_snapshot` — `ACCESS_DENIED 497`). `make all` — зелёный (gofmt, `golangci-lint`, `go vet`, `go test -race`, обе сборки).

## 4. Сомнения фикс-раунда 2

1. **Цена на no-op каталог не перепубликовывает.** Симметричная ветка есть только у `bold_etf_flows` — как и просил Ruling 16. Текст ряда цены (`gold/series_meta.go`) уже корректен, поэтому в БД он верный; но если этот текст когда-нибудь поправят, до базы он доедет только вместе с новой ценой (а цена обновляется каждый торговый день, так что окно расхождения — часы). Готов добавить публикацию и в `boldGoldPrice.Import` отдельным коммитом, если решите, что симметрия важнее объёма.
2. **No-op теперь не «ничего не делает»:** `util.UpsertSeriesCatalog` выполняет `CREATE TABLE IF NOT EXISTS series_catalog`, девять `ALTER/CREATE OR REPLACE` по витрине `v_series_catalog` и один батч `Send` — то есть 10 идемпотентных утверждений за прогон. Это осознанная цена Ruling 16 (иначе исправленный origin не доехал бы до БД); данные при этом не пишутся, DDL обеих таблиц контура и чтение стока не выполняются — это и проверяет тест.
3. **Наивная проверка «все `gold/…`-токены с `.json`» даёт ложное срабатывание** на имени ряда `gold/bold` (это `series` в `gold_prices`, а не эндпоинт). В отчёте привожу её с оговоркой; в тестах использую точные проверки по константам, а не regex по тексту.

---

# Fix round 3 — Ruling 18 (доки о no-op-публикации), Ruling 19 (симметрия цены), Ruling 20 (снимковые числа AUM)

**Статус: DONE.** Все три группы правок сделаны, живая проверка и `make all` / `make mcp-check` зелёные, данные не изменились.

**Коммиты (от HEAD = `063aea0`):**
- `0a849a3c6e160ed633e832c3ae68fa0cc28b9395` — `bold: republish price catalog on no-op` (`bold/bold.go`, `bold/price_test.go`, `bold/series_meta.go`, `scripts/mcp_check.py`);
- `7a14d082696ffd696b4bb1e1cf259fb9955d0e1d` — `docs: document catalog republish on no-op` (`docs/superpowers/specs/2026-10-12-bold-t1-gold-design.md`, `docs/superpowers/plans/2026-10-12-bold-t1-gold.md`, `ARCHITECTURE.md`).

Рабочее дерево чистое; файлы вне списка не трогались (каталог правок — ровно указанные места).

## 1. Ruling 18 — тексты приведены к поведению no-op-публикации

Все 12 пар OLD→NEW применены буквально, каждая — ровно в одном месте; проверено двумя проверками: старые формулировки в репозитории больше не встречаются, каждая новая — ровно один раз (13 строк, потому что у пункта 8 два места):

| # | Файл | Что стало |
|---|---|---|
| 1 | спека §5, стр. 132 | `каталог — после успешного `Send`, а в ветке no-op — перепубликовывается (Ruling 16: …)` |
| 2 | спека §5, стр. 142 | `идемпотентный no-op; каталог рядов при этом перепубликовывается` |
| 3 | план, «Architecture» | `один батч на таблицу, каталог после `Send` (и перепубликация в ветке no-op)` |
| 4 | план, Task 2 шаг 2 | ``return 0, nil` (батч не открывается вовсе; каталог перепубликовывается)` |
| 5 | план, Task 2 шаг 7 | `… — после успешных `Send`, а также в ветке no-op (перепубликация)` |
| 6 | план, Task 3 шаг 2 | `… → `0, nil` без DDL и батчей таблицы цены (каталог перепубликовывается)` |
| 7 | план, Task 3 (каталог) | `… после `Send`, а в ветке no-op — перепубликация` |
| 8a | ARCHITECTURE §6.1 (`bold_etf_flows`) | `no-op без записи точек (каталог перепубликовывается)` |
| 8b | ARCHITECTURE §6.1 (`bold_etf_flows`) | `каталог 7 рядов — после успешного `Send` (и перепубликация в ветке no-op)` |
| 9 | ARCHITECTURE §6.1 (`bold_gold_price`) | `… (`source='gold'`, `series='bold'`); в ветке no-op каталог перепубликовывается.` |
| 10 | ARCHITECTURE §6.5 | `через `util.UpsertSeriesCatalog` (после успешного `Send`, а в ветке no-op — перепубликацией; `source = 'bold'`), поэтому` |
| 11 | `scripts/mcp_check.py`, комментарий | `импортёр перепубликовывает каталог на каждом прогоне (после успешного Send или в ветке no-op), поэтому «меньше 7» значит ни разу не отработавший импорт…` |
| 12 | `scripts/mcp_check.py`, detail | `make import STAT=bold_etf_flows (каталог перепубликовывается прогоном импортёра)` |

Код и тесты Ruling 18 не трогал.

## 2. Ruling 19 — цена перепубликовывает каталог на no-op

`bold/bold.go`, ветка no-op `boldGoldPrice.Import` (по образцу ETF):

```go
		// Каталог ряда перепубликовывается и здесь: предусловие ветки — непустой сохранённый
		// максимум, то есть точка уже в таблице, и её описание обязано быть актуально без новых
		// данных (иначе правка текста каталога доезжала бы до агента только вместе с новой точкой).
		// Логика та же, что у ETF-импортёра (Ruling 16/19).
		if !latestDate.After(stored) {
			log.Infof("%s: дата источника %s не новее сохранённого максимума %s — нечего обновлять (0 строк, каталог перепубликован)", …)
			if cerr := util.UpsertSeriesCatalog(ctx, conn, []util.SeriesMeta{gold.BoldPriceSeriesMeta()}); cerr != nil {
				return 0, fmt.Errorf("%s: каталог рядов: %w", boldGoldPriceName, cerr)
			}
			return 0, nil
		}
```

`bold/price_test.go`, `TestBoldPriceImportNoopWhenUpToDate` обновлён: запрет теперь только на данные таблицы цены (`exec: CREATE TABLE IF NOT EXISTS ` + `boldPriceTable` и `preparebatch: ` + `boldGoldPricesInsert`), добавлено требование прочтения максимума площадки и проверка каталога — батч `INSERT INTO series_catalog` открыт, `sent == true`, **1 строка** (ряд `gold/bold`); запрос к источнику по-прежнему один (`price-latest`). Комментарий теста переписан («0 строк, данных не касаемся; каталог перепубликовывается»); саботаж-проверка прежняя — снять `latestDate.After(stored)` и тест падает.

**Правка сверх списка (сообщаю):** doc-комментарий `boldGoldPrice.Import` утверждал «(4) … соединение дальше чтения максимума не трогается (ни DDL, ни батча, ни каталога)» и «(7) каталог — только после успешного `Send`» — после Ruling 19 это ложь в коде, поэтому шаги (4) и (7) привёл к новому поведению. Правка текстовая, в том же файле, который Ruling 19 и меняет.

## 3. Ruling 20 — снимковая пара чисел убрана из описания AUM

`bold/series_meta.go`: описание `etf_bold_aum_usd_bn` теперь `aum / 1e9 — совокупная стоимость активов 73 фондов. Публикуется источником в USD; AUM считается источником по своей цене фондов, поэтому с нашим рядом цены gold/bold может расходиться. ` + `boldETFScope`. Пин теста на «83 %» не пострадал (он в хвосте `boldETFScope`); остальные agent-facing тексты не трогал.

## 4. Проверки

**Живые прогоны (ClickHouse поднят):**

```
$ make import STAT=bold_gold_price
… — нечего обновлять (0 строк, каталог перепубликован)   → Imported 0 rows, exit=0
$ make import STAT=bold_etf_flows
… — нечего обновлять (0 строк, каталог перепубликован)   → Imported 0 rows, exit=0
```

**MCP (`kimi_reader`, read-only):**

```sql
SELECT series, position(description,'476,678') AS has_476, length(description) AS len
FROM v_series_catalog WHERE source='bold' AND series='etf_bold_aum_usd_bn';
-- etf_bold_aum_usd_bn | 0 | 931      ← снимковой пары чисел в описании больше нет

SELECT source, series, origin FROM v_series_catalog
WHERE series='bold' OR (source='bold' AND series='etf_bold_flow_tonnes');
-- bold | etf_bold_flow_tonnes | bold.report/api/v1/: gold/funds/units.json, gold/funds/aum.json,
--        gold/flows/all.json …; пер-фондовый срез — gold/summary.json …; цена дня — gold/price.json …
-- gold | bold | bold.report/api/v1/gold/price.json (инкремент — gold/price-latest.json)   ← .json на месте

SELECT (SELECT count() FROM v_gold_etf_flows_daily) AS flows_rows,
       (SELECT count() FROM v_gold_etf_funds_daily) AS funds_rows,
       (SELECT count() FROM v_gold_prices WHERE series='bold') AS price_rows,
       (SELECT max(date) FROM v_gold_etf_flows_daily) = (SELECT max(date) FROM v_gold_etf_funds_daily) AS same_day,
       (SELECT round(flow_tonnes,3) FROM v_gold_etf_flows_daily ORDER BY date DESC LIMIT 1) AS flow_tonnes,
       (SELECT round(aum_usd_bn,3)  FROM v_gold_etf_flows_daily ORDER BY date DESC LIMIT 1) AS aum;
-- 3008 | 73 | 3138 | 1 | 4.233 | 476.678
```

То есть каталогные прогоны **данных не изменили**: дневной агрегат 3008 дней, пер-фондовый срез 73 строки, цена 3138 точек, максимумы дат витрин совпадают, значения `flow_tonnes`/`aum` за `2026-10-09` — те же.

**Ворота:** `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА** (35 OK / 0 FAIL; bold-блок: обе витрины читаются — 3008 и 73, каталог `bold` = 7, сырая `bold_fund_snapshot` — `ACCESS_DENIED 497`). `make all` — **exit 0** (gofmt, `golangci-lint`, `go vet`, `go test -race`, обе сборки).

**GoLand MCP:** инструменты доступны и использованы — `search_symbol` (нашёл объявление `boldGoldPricesInsert` для точного фрагмента теста), `lint_files` по трём правленым Go-файлам (только предсуществующие WARNING «SQL dialect is not configured» на строках SQL-констант, ни одного замечания к правкам), `get_file_problems` вернул пустой список ошибок, но с `timedOut: true` (IDE ещё индексировал проект), поэтому авторитетной проверкой считаю `make all`. ClickHouse — только через `mcp__clickhouse__run_query` (read-only).

## 5. Сомнения фикс-раунда 3

1. **Правка doc-комментария `boldGoldPrice.Import`** (шаги 4 и 7) не входила в список Ruling 18/19; сделал потому, что после Ruling 19 комментарий прямо противоречил бы коду («ни каталога»). Если предпочитаете минимальный дифф — откатывается одной правкой.
2. **Стоимость no-op у цены** теперь такая же, как у ETF: `util.UpsertSeriesCatalog` = 10 идемпотентных утверждений (DDL `series_catalog`, девять по витрине `v_series_catalog`) + один батч `Send`. Данные при этом не пишутся — это проверяет тест.
3. **Проверки старых витрин в `mcp_check.py`** по-прежнему ищут имя в тексте `list_tables` (изъян из Ruling 15 остался у них); не трогал, как и было сказано.

---

# Fix wave 4 — Ruling 22 (гейт соседнего торгового дня), Ruling 23 (правдивые тексты), Ruling 24 (лог после публикации)

**Статус: DONE.** Три пункта закрыты ровно в объёме; данные в ClickHouse не мутировались (оба живых прогона — no-op).

**Коммиты (от HEAD `82161ac`):**
- `d348a0f1e6d8325f1e19ff06a6fca1c30b6a582e` — `bold: require previous trading day for incremental flow` (`bold/bold.go`, `bold/bold_test.go`; +145/−16);
- `a0d4b65a51e3210c504302aeb9d213375145ce8b` — `docs: previous-trading-day trigger and step order` (`ARCHITECTURE.md`, `dagu/bold.yaml`, спека, план; +16/−12).

## 1. Ruling 22 — инкремент ETF только от непосредственно предыдущего торгового дня

**Код (`bold/bold.go`).** Добавлен чистый гейт рядом с `boldNeedsFullFetch`:

```go
func boldIsPreviousTradingDay(latest, stored time.Time) bool {
	if stored.IsZero() {
		return false
	}
	prev := latest.AddDate(0, 0, -1)
	for prev.Weekday() == time.Saturday || prev.Weekday() == time.Sunday {
		prev = prev.AddDate(0, 0, -1)
	}
	return prev.Format(boldDateLayout) == stored.Format(boldDateLayout)
}
```

Сравнение — по **календарной дате** (`Format(boldDateLayout)`, тот же способ, каким даты уже сериализуются в `boldStoredUnits`), а не по моментам времени: ClickHouse отдаёт `Date32`, и `time.Time.Equal` сравнивал бы инстанты. Гейт инкремента заменён: `if !boldIsPreviousTradingDay(latestDate, stored) || diverged {`. `boldNeedsFullFetch` **не удалён и не изменён** — он остался гейтом цены (`boldNeedsFullFetch(s.clock().UTC(), stored)`, Ruling 10); `boldSourceLooksStale` и `boldFullFetchGapHours` не тронуты.

Doc-комментарий нового гейта объясняет строгость (инкремент считает Δ к стоку максимума, поэтому пропущенный прогон сделал бы Δ многодневным и записал бы её как «дневной поток», а потерянный торговый день не попал бы ни в один из семи рядов — молча, потому что warn застоя не срабатывает и расхождение половин такую дыру не видит), перепрыгивание выходных и то, что праздники источника дают лишний полный прогон — безопасное направление (идемпотентно).

**Комментарии приведены в порядок:** `boldFullFetchGapHours` (теперь обслуживает цену и warn застоя; пояснено, почему ETF отказался от порога), заголовок `Import` (шаг 4 — новый триггер), комментарий у ветки полного пути, doc `boldNeedsFullFetch` (помечен как гейт цены), doc `boldStoredUnits` (база Δ — гарантированно соседний торговый день, вызов только после гейта).

**Тесты.**

- `TestBoldIsPreviousTradingDay` — таблица: 10-08→10-09 `true`; пятница 10-09 → понедельник 10-12 `true`; 10-12→10-13 `true`; 10-07→10-09 `false`; 10-06→10-09 `false`; нулевой максимум `false`; совпадение дат `false` (это разбирает no-op выше по потоку).
- `TestBoldEtfImportFullFetchWhenPreviousDayMissing` — интеграционный: максимумы обеих половин 10-07, источник = фикстуры 10-09; ожидание полного пути (`count = 12`, в батче оба дня — 10-08 промежуточный и 10-09), 8 запросов (4 «последнего дня» + 3 файла истории + `summary` дважды), сток за 10-07 не читается. В фейковое соединение добавлен третий ответ (сток 10-07): в норме он не читается, но с ним саботаж «вернуть пороговый гейт» падает на **арифметике** (7 строк вместо 12), а не на ошибке «нет запасов».
- Существующие `TestBoldEtfImportNoopWhenUpToDate` (stored == latest) и `TestBoldEtfImportIncrementalWritesOneDay` (10-08 → 10-09) остались зелёными без правок семантики; `TestBoldEtfImportFullWritesAllSeries`, `...WhenSeriesAndFundsDiverge`, `...MissingUnitsAtMaxFails`, `TestBoldNeedsFullFetch` — тоже.

**Саботаж-проверки (проведены и откачены, дерево чистое):**

| Саботаж | Результат |
|---|---|
| (а) убран скип выходных в `boldIsPreviousTradingDay` | `FAIL TestBoldIsPreviousTradingDay/пятница_→_понедельник_(выходные_перепрыгнуты)`: `boldIsPreviousTradingDay(2026-10-12, 2026-10-09) = false, ожидалось true` — валится ровно один кейс, остальные шесть зелёные |
| (б) в гейт ETF возвращён `boldNeedsFullFetch` | `FAIL TestBoldEtfImportFullFetchWhenPreviousDayMissing`: `Import вернул 7 строк, ожидалось 12`; `промежуточного дня 2026-10-08 в батче нет (дни: map[2026-10-09:7])`; читался сток за 10-07; запросы — инкрементные (`gold/...-latest.json`), а не файлы истории. То есть тест ловит именно подмену многодневной Δ дневной |

**Доки:** спека §5 (триггер полного пути), план (Review Focus 3; Interfaces — добавлен `boldIsPreviousTradingDay` и помечено, кто какой функцией гейтится; Task 2 шаг 3 — условие с `!boldIsPreviousTradingDay`; шаг 3 саботаж-список — добавлены (а2) и (а3)), `ARCHITECTURE.md:204` (триггеры полного пути + «Δ запасов к стоку непосредственно предыдущего торгового дня»), `dagu/bold.yaml` (комментарий о полном режиме). Каталожные и витринные тексты не менялись.

## 2. Ruling 23 — правдивые тексты

1. `dagu/bold.yaml`: ложное обоснование заменено — «Шаги независимы: USD-поток контура считается по цене из ответа источника, а цена дня живёт отдельным рядом в gold_prices; цепочка задаёт лишь порядок прогонов — сначала цена, затем ETF-контур.»
2. `ARCHITECTURE.md:205`: «шаг 1 (порядок шагов — только порядок прогонов: зависимости нет, USD-поток считается по цене из ответа источника, а не по площадке `bold`)».
3. `ARCHITECTURE.md:205` (режим цены): «пустая (или отсутствующая) площадка либо максимум старше 96 ч (`now − stored > 96 ч`) → полный файл `price.json`; иначе `price-latest`, и если его дата не новее сохранённого максимума → no-op» — сверено со спекой §5:155 (там формулировка та же по смыслу: пустая площадка или `now − максимум > 96 ч` → полный файл, иначе `price-latest`).
4. `grep 'множителем' \| 'без неё USD'` по репозиторию (кроме `.worktrees`): в правимых доках — пусто. Осталось два совпадения в коде — `bold/bold.go:435` и `bold/bold_test.go:453` — там речь о том, что цена входит множителем в `etf_bold_flow_usd_mln` **из ответа источника** (проверка «дата из одного файла, цена из другого»), это верно и к порядку шагов DAG отношения не имеет; ещё совпадения в `docs/ROADMAP_DCF_POLYUS.md` и `dcf/model.go` — другой смысл. Не менял.

## 3. Ruling 24 — лог после публикации

В обеих ветках no-op (`boldEtfFlows.Import` и `boldGoldPrice.Import`) порядок теперь: `util.UpsertSeriesCatalog` (ошибка → `return 0, err`) → `log.Infof(… «0 строк, каталог перепубликован»)`. Больше текст логов не менялся.

## 4. Проверки

- `go test ./bold/... -count=1` — **ok** (включая новые тесты; проверил и целевым прогоном `-run 'TestBoldIsPreviousTradingDay|TestBoldEtfImport'`: 7 подтестов + 7 интеграционных — все PASS).
- `make all` — **exit 0** (gofmt, `golangci-lint`, `go vet`, `go test -race`, обе сборки), прогонял до и после коммитов.
- GoLand MCP: `lint_files` по `bold/bold.go` и `bold/bold_test.go` — только предсуществующие WARNING «SQL dialect is not configured» на SQL-строках, замечаний к правкам нет; `search_symbol` (объявление `boldGoldPricesInsert`), `search_regex` для навигации. SQL — только `mcp__clickhouse__run_query`.
- Живые прогоны (данные НЕ мутировались):
  - `make import STAT=bold_etf_flows` → `Imported 0 rows`, exit 0, лог `… нечего обновлять (0 строк, каталог перепубликован)`;
  - `make import STAT=bold_gold_price` → `Imported 0 rows`, exit 0, тот же лог.
  - Сегодня `stored = latest = 2026-10-09`, поэтому новый гейт живьём не воспроизводится (для него нужен пропущенный торговый день) — это ожидаемо и покрыто офлайн-тестами.
- MCP после прогонов: `flows_rows = 3008`, `funds_rows = 73`, `price_rows = 3138`, максимумы дат всех трёх = `2026-10-09`, каталог `source='bold'` = 7 — **данные не изменились**.
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА** (35 OK / 0 FAIL, exit 0).

## 5. Сомнения

1. **Живой регресс дефекта Ruling 22 не воспроизводим сегодня** (stored == latest): доказательство — офлайн-тесты и обе саботаж-проверки выше. Полный путь для такого сценария живьём можно получить, только подделав данные в ClickHouse, а этого делать не разрешено.
2. **Третий ответ фейкового соединения в новом интеграционном тесте** — не читается в норме, но нужен, чтобы саботаж падал на арифметике; это осознанное дублирование «ветки дефекта» в тесте (альтернатива — падение на ошибке «нет запасов», менее информативное).
3. `ARCHITECTURE.md:205` в графе «Метод» цены теперь перечисляет оба режима подробнее, чем раньше (порог 96 ч назван явно) — это ответ на Ruling 23.3; если формулировка кажется длинной, сокращается до одной фразы без потери смысла.
4. В `dagu/bold.yaml` комментарий о полном режиме пришлось сделать двухчастным (у ETF — соседний торговый день, у цены — пустая площадка или разрыв > 96 ч): исходная однострочная формулировка «пропуск дней» после Ruling 22 описывала бы только половину контура.
