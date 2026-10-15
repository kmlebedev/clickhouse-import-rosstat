# Task 4 report — витрины, гранты, mcp-check и расписание

**Статус:** выполнено, один коммит `217bcb7` — `add bold daily views, grants, mcp checks and schedule`
(ветка `feat/bold-t1-gold`, рабочее дерево чистое).

**Итог одной строкой:** `gofmt -l views/ gold/` пусто, `go test ./views/... ./gold/... -v -count=1` — 54 PASS /
0 FAIL, `make all` (lint + `golangci-lint run ./...` = 0 issues + `go vet` + `go test -race` + обе сборки) — exit 0,
SQL обеих витрин проверен живым ClickHouse через MCP (`EXPLAIN SYNTAX` и пивот на синтетическом источнике) —
выдача ровно 8 и 10 колонок в ожидаемом порядке.

---

## 1. Что сделано

### 1.1 `views/bold_etf_daily.go` (новый) — две витрины контура bold

**Один срез `boldETFSeries` на весь пивот.** `[]boldETFSeriesPair{Series, Metric}` из семи пар — единственный
источник и `IN (...)`-фильтра, и mapping-массива пивота; метрика-колонка собирается из пары
`(code ряда, имя колонки)`, а не пишется второй раз рядом. Из него собраны `boldETFSeriesNamesSQL()` (7 кодов для
`IN`) и `boldETFSeriesMapSQL()` (7 пар `(series, metric)` для `arrayJoin`).

`v_goldEtfFlowsDailyView = util.View{Name: "v_gold_etf_flows_daily", Tables: []string{"macro_series"}, …}` —
по образцу `v_gold_etf_flows`: mapping-массив → `INNER JOIN` → `maxIfOrNull(v.value, m.metric = '…') AS …`,
`GROUP BY date`, `ORDER BY date DESC`. Отличия от месячной WGC-витрины (объяснены в doc-комментарии):
источник один (`source = 'bold'`), поэтому приоритет `coalesce` по источникам не нужен, а версии
ReplacingMergeTree схлопывает `FINAL`; `maxIfOrNull`, а не `maxIf` (иначе «ряда за день нет» стало бы нулём).
Вложенный подзапрос `SELECT date, series, value FROM macro_series FINAL WHERE source = 'bold' AND series IN (…)`
отдаёт по одной строке на `(date, series)`.

`boldFundsDailyView = util.View{Name: "v_gold_etf_funds_daily", Tables: []string{"bold_fund_snapshot"}, …}` —
`SELECT date, ticker, name, units_oz, aum_usd, price_usd, units_change_oz, aum_change_usd, shares, shares_change
FROM bold_fund_snapshot FINAL ORDER BY date DESC, ticker, name` (10 из 11 колонок среза: `price_change_usd`
пишется, но в выдачу не выведен — это сказано и в комментарии таблицы, и в doc-комментарии).

**Комментарии (правило 11).** Комментарий таблицы и комментарий каждой из 8 и 10 колонок — тексты из спеки §4.4:
агрегат 73 фондов ≈83 % мировой величины WGC; «не складывать и не сравнивать напрямую с
`etf_holdings_tonnes_world`/`etf_flow_tonnes_world` (там все фонды мира и месячная частота)»; окна 7/30/90 —
календарные, сумма 5 торговых дней ≠ `culm7day`, окна пересекаются; `flow_tonnes` = Δ совокупных унций между
соседними торговыми днями, у первой точки ряда потока нет; источник — bold.report (ByteTree), ключей нет, режим
щадящий; метод цены источником не раскрыт («Futures Price», не спот и не `comex_front`); у пер-фондовой витрины —
«история копится с первого прогона (у источника пер-фондовой истории нет)» и «тикер не уникален: GLD трижды,
строку определяет пара (ticker, name)».

### 1.2 `views/gold_views.go` — регистрация

Обе переменные добавлены в `goldViewsList` (`goldEtfFlowsDailyView`, `boldFundsDailyView`). Литерал среза
переформатирован в многострочный (по 4 переменные в строке) — состав списка не изменился, порядок прежний,
новые витрины в конце. Добавлен комментарий: `v_gold_etf_funds_daily` гейтится по `bold_fund_snapshot`, которую
создаёт импортёр `bold_etf_flows`, поэтому `gold_views` запускают после его первого прогона. В package-док
добавлены две строки про новые витрины.

### 1.3 `views/bold_etf_daily_test.go` (новый) — 6 тестов

| тест | что пинит |
|---|---|
| `TestGoldEtfFlowsDailyViewShape` | имя, `Tables == [macro_series]`, `macro_series FINAL`, `source = 'bold'`, все 7 кодов рядов в SELECT, `maxIfOrNull(` есть / `maxIf(` нет, `GROUP BY date` |
| `TestBoldFundsDailyViewShape` | имя, `Tables == [bold_fund_snapshot]`, `bold_fund_snapshot FINAL`, `ORDER BY date DESC, ticker, name` |
| `TestBoldViewsRegistered` | обе витрины в `goldViewsList`; `Tables` совпадает с ожидаемым списком И таблица названа в самом `SELECT` (гейт `CreateView` + читаемость) |
| `TestBoldViewsHaveComments` | непустой `Comment`, покрытие `Columns` всеми выходными колонками, отсутствие «лишних» комментариев, **порядок и число колонок (8 и 10)** через `selectedColumns` |
| `TestBoldEtfFlowsDailySeriesSetMatchesViewAndImporter` | 7 пар без дублей; каждая пара стоит в mapping-массиве и разложена в колонку (`) AS metric`); **имена сверяются с файлом импортёра** `../bold/parse.go` |
| `TestBoldViewsOrderIsDeterministic` | `ORDER BY date DESC` и `ORDER BY date DESC, ticker, name` |

Сверка набора рядов идёт по **самому файлу импортёра**: `regexp boldSeries\w+\s*=\s*"(etf_bold_[a-z0-9_]+)"` по
`../bold/parse.go` (прецедент — тесты, читающие `sql/*.sql`-сиды: «сверка идёт по САМОМУ файлу, а не по копии
списка в тесте»). Если правило разбора разойдётся с файлом, тест падает с явным сообщением, а не «сверяет» с
пустым списком. Это единственное отклонение от буквы брифа по составу тестов (см. §5.1).

### 1.4 `sql/mcp_kimi_reader.sql`

Две строки в конце: `GRANT SELECT ON default.v_gold_etf_flows_daily TO kimi_reader;` и
`GRANT SELECT ON default.v_gold_etf_funds_daily TO kimi_reader;`.

### 1.5 `scripts/mcp_check.py` — блок контура bold (в конце, после проверки `macro_series`)

- видимость обеих витрин в `tables` (тот же `list_tables`, что и у остальных);
- читаемость (`SELECT count()`) каждой — по образцу `v_dcf_assumptions`/`v_gold_etf_flows`: проверяется
  РАЗРЕШЕНИЕ, а не число строк (до первого прогона и в выходной день строк нет);
- `SELECT count() AS n FROM v_series_catalog WHERE source = 'bold'` → ожидание **7**, detail подсказывает
  `make import STAT=bold_etf_flows` (каталог пишется только после успешного `Send`);
- недоступность сырой `bold_fund_snapshot` — по образцу `macro_series` (`ACCESS_DENIED` / «Not enough privileges»).

### 1.6 `dagu/bold.yaml` (новый)

`type: chain`, `schedule: "7 8 * * *"`, `overlap_policy: skip`, шаги `bold_gold_price` → `bold_etf_flows`,
`retry_policy: {limit: 3, interval_sec: 300}` — по образцу `dagu/wgc.yaml`. Шапка-комментарий: данные источника
готовы к 07:32 МСК и слот свободен (cbr/gold 18:47, wgc 20:17); инкремент — 4 запроса у `bold_etf_flows`
(`units-latest`, `aum-latest`, `flows/all-latest`, `summary`) + 1 у `bold_gold_price` (`price-latest`), то есть
до 5 запросов в сутки, режим щадящий; полный режим (первый прогон, пропуск > 4 календарных дней, расхождение
максимумов ряда и среза) — до 8 запросов, но редкий и идемпотентный; fail-fast и T+1 названы. Порядок шагов
обоснован: цена дня входит множителем в `etf_bold_flow_usd_mln`.

### 1.7 `gold/series_meta.go` — хвост из Task 3 (одна строка)

Строка 60–61 описания каталога `bold`: было «Глубину ряда и текущий максимум смотреть в v_gold_prices
(ARCHITECTURE.md §6.2), а не здесь…» → стало «Глубину ряда и текущий максимум смотреть в v_gold_prices, а не
здесь: … (описание витрины — ARCHITECTURE.md §6.3)». Обещание глубины в ARCHITECTURE убрано (глубина живёт в
`v_gold_prices`), ссылка ведёт в §6.3 — раздел, где описана сама витрина (в §6.2 описаны таблицы, глубины ряда
там нет). Заодно поправлен внутренний doc-комментарий функции, повторявший ту же неточность: было «Глубина ряда и
текущий максимум живут в v_gold_prices и ARCHITECTURE» → «…живут в v_gold_prices (ARCHITECTURE.md §6.3 — там
описана сама витрина), а не в каталоге». Текст описания ряда (`Description`) тесты `bold`/`gold` не пинят —
проверено (`grep` по фрагментам в `bold/price_test.go`, `gold/series_meta_test.go`).

**Не тронуто:** `Select` существующих витрин, `sql/wgc_etf_flows_catalog.sql`, `ARCHITECTURE.md`/`README.md`/
роадмап (Task 5), код импортёров `bold/`, `Makefile`, CI.

---

## 2. Проверки SQL живым ClickHouse (MCP, `kimi_reader`)

- **Витрин ещё нет в БД** (импортёры не прогонялись): `SELECT name FROM system.tables WHERE database =
  currentDatabase() AND name IN ('v_gold_etf_flows_daily','v_gold_etf_funds_daily','bold_fund_snapshot')` → 0 строк.
  `currentUser()` = `kimi_reader`, в `default` 23 таблицы.
- **`EXPLAIN SYNTAX`** точного текста SELECT дневной витрины (подставил только синтетический источник вместо
  `macro_series FINAL`) → разобралось, `maxIfOrNull` → `maxIf`-OrNull, `FINAL` на месте, фильтр
  `equals(source,'bold') AND in(series, …)`.
- **Пивот на синтетических данных** тех же типов (`LowCardinality(String)`, `Date32`, `Float64`): выдача —
  ровно `["date","flow_tonnes","flow_usd_mln","flow_7d_tonnes","flow_30d_tonnes","flow_90d_tonnes",
  "holdings_tonnes","aum_usd_bn"]`, две строки `2026-10-09` и `2026-10-08`, у отсутствующих метрик — `null`
  (проверено, что отсутствие ряда не превращается в 0). Аналогично пер-фондовый SELECT: ровно 10 колонок в
  ожидаемом порядке.
- **Сырые таблицы закрыты агенту:** `SELECT count() FROM macro_series` → `Code: 497 … ACCESS_DENIED`;
  `SELECT count() FROM bold_fund_snapshot` → сейчас `Code: 60 … UNKNOWN_TABLE` — см. §5.2.

---

## 3. Команды и результаты

| команда | результат |
|---|---|
| `gofmt -l views/ gold/` | пусто |
| `go test ./views/... ./gold/... -v -count=1` | 54 `--- PASS`, 0 `--- FAIL` (в т.ч. 6 новых тестов задач) |
| `go test -race ./...` (внутри `make all`) | все пакеты `ok`, включая `views` и `bold` |
| `golangci-lint run ./...` (`make lint`) | `0 issues.` |
| `make all` | exit 0 (`gofmt` + lint + `go vet` + `go test -race ./...` + сборка сервера и `build/ingest`) |
| `python3 -m py_compile scripts/mcp_check.py` | OK |
| `dagu validate dagu/bold.yaml` | exit 0 (только warning dagu про `auth.mode`, как и на других прогонах) |
| YAML-разбор `dagu/bold.yaml` | `type=chain`, `schedule="7 8 * * *"`, `overlap_policy=skip`, шаги `[bold_gold_price, bold_etf_flows]`, `retry_policy={limit:3, interval_sec:300}` |
| IDE `get_file_problems` (JetBrains MCP) по 5 файлам | ошибок нет; только предсуществующие warning «SQL dialect is not configured» (точно такие же на `views/gold_etf_flows.go` и `gold/series_meta.go` до правок) |

MCP доступны оба (JetBrains и ClickHouse) — «MCP нет» не применимо.

---

## 4. Что получают Task 5 и приёмка

1. Гранты обеих витрин лежат в `sql/mcp_kimi_reader.sql` → `make mcp-user` / `deploy-staging-mcp` подхватят их
   без правок Makefile.
2. `dagu/*.yaml` копируется глобом (`scp dagu/*.yaml` в `deploy-staging`), отдельной регистрации `bold.yaml` не
   требуется.
3. Порядок запуска для приёмки: `make import STAT=bold_etf_flows` и `STAT=bold_gold_price` (они создают
   `bold_fund_snapshot`) → `make import STAT=gold_views` (создаёт обе витрины) → `make mcp-user` → `make mcp-check`.
4. `ARCHITECTURE.md` §6.3 (описание двух витрин), §6.2 (таблица `bold_fund_snapshot` и `source='bold'` в
   `macro_series`), §7 (расписание 08:07) — за Task 5; комментарии таблиц/колонок уже несут весь требуемый текст,
   дублировать его в доке не обязательно, но §6.3 стоит сослаться на готовые `Comment` витрин.
5. Watch-пункт из ре-ревью (прогноз.md:70): максимум `v_gold_etf_flows_daily` и `v_gold_etf_funds_daily` на
   живых данных должен совпасть (отставание истории источника) — витрины для этого и разведены: дата в обеих —
   из одного ответа «последнего дня».

---

## 5. Сомнения и отклонения от буквы брифа

### 5.1 Тест сверяет имена рядов с файлом импортёра (сверх списка тестов брифа)

Бриф перечисляет 4 теста; я добавил два (набор рядов и порядок выдачи) и в тесте набора читаю
`../bold/parse.go`. Польза: переименование ряда в `bold/` уронило бы тест витрины (иначе колонка молча стала бы
вечным NULL — именно этого класс ошибок брифа и опасается). Цена: тест завязан на форму объявлений констант в
файле (`boldSeriesX = "etf_bold_…"`); если объявления перепишут иначе (например, в `const`-блоке без знака `=`
в строке), тест упадёт с явным сообщением «правило разбора разошлось с файлом» — править тест, а не молча
терять сверку. Альтернатива (копия семи имён списком в тесте) отброшена как «тест, проверяющий сам себя».

### 5.2 Проверка «сырой `bold_fund_snapshot` закрыт» строгая: сейчас в этой БД она даёт UNKNOWN_TABLE

Бриф требует «недоступность сырой `bold_fund_snapshot` по образцу проверки `macro_series`» — сделано строго
(`ACCESS_DENIED` / «Not enough privileges»). Проверено MCP: у `kimi_reader` `macro_series` даёт код 497
`ACCESS_DENIED`, а `bold_fund_snapshot` — код 60 `UNKNOWN_TABLE`, потому что импортёр на этой машине ещё не
прогонялся. Значит, при приёмке `make mcp-check` даст по этой строке FAIL до первого прогона `bold_etf_flows`
(вместе с видимостью витрин и каталогом — весь блок bold до прогона красный, и это осознанно: контура в БД ещё
нет). Альтернатива — принимать `UNKNOWN_TABLE` как успех — отброшена: она маскировала бы «таблицы нет / импортёр
не прогонялся» и расходилась бы с образцом `macro_series`. Если контроллер хочет мягкий вариант, правка — одна
строка (`or "UNKNOWN_TABLE" in raw`), но тогда проверка перестанет что-либо доказывать о самой таблице.

### 5.3 Ссылка §6.3 в каталоге — компромисс двух указаний

В брифе одновременно «замени ссылку на §6.3» и «убери обещание „глубину ряда смотреть в ARCHITECTURE“».
Сделано так: глубина и максимум — только `v_gold_prices`; ссылка `ARCHITECTURE.md §6.3` осталась, но теперь
относится к описанию самой витрины, а не к глубине ряда. Если контроллер читал это как «убрать упоминание
ARCHITECTURE вовсе», правка — удалить скобку `(описание витрины — ARCHITECTURE.md §6.3)`.

### 5.4 Мелочи

- `goldViewsList` переформатирован в многострочный литерал — состав и порядок те же, но это правка строки,
  которую бриф не просил.
- В комментарии пер-фондовой витрины названо, что `price_change_usd` в выдачу не выведен: спека §4.4 требует
  ровно 10 колонок, а молча «спрятанная» колонка среза выглядела бы как потеря данных.
- В комментарии дневной витрины глубина названа как «строк до 2015-04-01 у источника нет, первые 64 дня несут
  только цену» — формулировка повторяет `bold/series_meta.go`, а не числа из ручного среза; снимковых чисел
  (3520,318 т / 476,678 млрд) в комментариях витрин нет намеренно — они устареют при первом же инкременте.
- `dagu validate` доступен на этой машине (`/opt/homebrew/bin/dagu`), им и проверен `bold.yaml`; в CI этого шага
  нет, так что проверка ручная.
