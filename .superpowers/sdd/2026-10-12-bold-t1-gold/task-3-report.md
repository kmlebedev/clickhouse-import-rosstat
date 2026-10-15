# Task 3 report — импортёр `bold_gold_price` и каталог цены

STATUS: **DONE_WITH_CONCERNS** → fix round 1 закрыт (Ruling 12/13 + выравнивание комментария
саботажа) коммитом **`1d78814`**; существенная интерпретация §6.1 ратифицирована контроллером
(см. §6.1 и §7).

BASE: `0bb8c1b` (docs-коммит после Task 2; BASE плана — `89894ae`).
Commits:
- **`a730e2e`** — `add bold_gold_price importer and price catalog entry`
  (`bold/bold.go`, `bold/price_test.go`, `gold/series_meta.go`, `gold/series_meta_test.go`; 4 файла, +568/−18);
- **`1d78814`** — `bold price: keep the catalog description factual`
  (`bold/bold.go`, `bold/price_test.go`, `gold/series_meta.go`; 3 файла, +81/−10).

---

## 1. Что сделано

### `bold/bold.go` — второй импортёр пакета

- Шапка файла переписана: теперь описывает оба импортёра (`bold_etf_flows` и `bold_gold_price`),
  package-комментарий остался один и по-прежнему в `parse.go`.
- Импорт `.../gold` (без цикла: `gold` не импортирует `bold`) — нужен ровно ради
  `gold.BoldPriceSeriesMeta()`.
- Новая секция «Контур цены (спека §4.2)»:
  - константы `boldGoldPriceName = "bold_gold_price"`, `boldPriceTable = "gold_prices"`,
    `boldPriceVenue = "bold"`;
  - `boldGoldPricesDdl` — **копия канонического DDL** `gold_prices` из `gold/gold.go` (побайтово
    совпадает: проверено скриптом, §4.4) с комментарием «копии обязаны совпадать» и ссылкой на
    вторую копию (`ingest/schema.go`) и ARCHITECTURE §6.2; `boldGoldPricesInsert = "INSERT INTO
    gold_prices (venue, date, usd)"` — явный список колонок, как у ingest;
  - `boldPriceStoredMaxQuery = "SELECT maxOrNull(date) FROM gold_prices FINAL WHERE venue = 'bold'"`
    (площадка в условии: таблицу делят три писателя).
- `type boldGoldPrice struct { apiBase string; now func() time.Time }`, `Name()`, `base()`,
  `clock()` — поля и швы как в Task 2.
- `Import(ctx, conn)`: сохранённый максимум → **решение о режиме до запроса** → один запрос
  (`price.json` или `price-latest`) → на инкременте no-op при дате не новее максимума
  (`log.Infof`, `0, nil`, без DDL/батча/каталога) → `Exec(boldGoldPricesDdl)` → один батч →
  `util.UpsertSeriesCatalog(ctx, conn, []util.SeriesMeta{gold.BoldPriceSeriesMeta()})` после `Send`.
- `boldPriceStoredMaxDate(ctx, conn) (time.Time, error)` — обёртка над `boldScanMaxDate` Task 2
  (NULL пустой таблицы и отсутствие таблицы → нулевое время); ошибка чтения → `log.Warnf` и
  нулевое время (полный путь), как в Task 2.
- `boldPricePoints(rows) ([]point, error)` — строки цены → точки; `nil` в `value` — ошибка (не
  тихий пропуск дня), пустой результат — ошибка (fail-fast).
- `boldWritePrices(ctx, conn, points)` — один батч `PrepareBatch → Append(err) → Send`, Abort после
  ошибки append, сортировка **копии** по дате, venue берётся из `point.Series` (= площадка).
- `init()`: `chimport.Stats = append(chimport.Stats, &boldEtfFlows{}, &boldGoldPrice{})` —
  `make import STAT=bold_gold_price` находит импортёр.

### `gold/series_meta.go`

- `const boldVenue = "bold"`.
- **`func BoldPriceSeriesMeta() util.SeriesMeta`** рядом с `goldSeriesMeta`: `source='gold'`,
  `series='bold'`, `unit = "USD за тройскую унцию (фьючерс источника, метод детально не раскрыт)"`,
  `frequency="D"`, `origin = "bold.report/api/v1/gold/price.json (инкремент — gold/price-latest.json)"`,
  title и description («Futures Price», метод источником не раскрыт, не спот и не LBMA, не GC=F,
  с `comex_front` совпадает лишь частично — 3 из 13 контрольных дат, расхождения до $42,
  2026-10-08: 4157,0 против 4199,4 → отдельная серия, а не подмена; 3 138 точек с 2014-10-01).
- Комментарий витрины `v_gold_prices`: «Две площадки» → **«Три площадки»**, `bold` назван
  фьючерсной ценой источника с нераскрытым методом (не спот и не GC=F, с `comex_front` совпадает
  частично); комментарий колонки `series` расширен до трёх площадок.

### `gold/series_meta_test.go`

- `TestSeriesMetaCoversAllVenues` расширен, ничего не ослаблено: в `want` добавлен `boldVenue`, а
  полнота полей проверяется по объединению `BoldPriceSeriesMeta()` и `goldSeriesMeta` (с тем же
  требованием `source == goldSource`, непустых полей и отсутствия дублей). Без этого запись,
  переехавшая в отдельную функцию, тихо вышла бы из-под теста.

## 2. Тесты (`bold/price_test.go`, 8 функций)

Семь написаны до реализации (падали на `undefined: boldGoldPrice`), восьмая
(`TestBoldPriceImportModeIsZoneAgnostic`) добавлена в fix round 1 (Ruling 13, §7).

| тест | что пинит |
|---|---|
| `TestBoldGoldPriceName` | `Name() == "bold_gold_price"` |
| `TestBoldGoldPriceIsRegistered` | `*boldGoldPrice` в `chimport.Stats` (тот же контракт, что у Task 2) |
| `TestBoldPriceImportFullWritesVenue` | пустая площадка → 1 запрос `gold/price`, 2 точки (08.10 4199.4, 09.10 **4216.3**), `venue='bold'`, DDL до батча, каталог после `Send`, текст гвардейского запроса |
| `TestBoldPriceImportIncrementalWritesOneDay` | максимум 08.10 → 1 запрос `gold/price-latest`, ровно одна точка 09.10 = 4216.3 |
| `TestBoldPriceImportFullWhenStoredIsStale` | максимум 06.10 против дня прогона 10.10 → полный путь, 1 запрос `price.json`, пишется весь файл (оба дня) |
| `TestBoldPriceImportNoopWhenUpToDate` | максимум 09.10 → `0, nil`, ноль `Exec`, ни одного `PrepareBatch`, один запрос `price-latest`; **здесь же** живёт проверка «точка новее максимума» (саботаж валит именно этот тест, §3) |
| `TestBoldPriceImportModeIsZoneAgnostic` | Ruling 13: порог 96 ч — по моментам UTC: ровно 96 ч → инкремент, +1 мин → полный путь, та же граница в поясе −07:00 (локальные сутки 09.10) → та же ветка инкремента |
| `TestBoldPriceCatalogEntry` | `gold.BoldPriceSeriesMeta()`: `source "gold"`, `series "bold"`, `D`, unit с «USD», в описании «метод … не раскрыт», «не спот», «comex_front», «bold.report», origin с `gold/price.json` |

Пины фикстур: 2026-10-09 → **4216.3**, 2026-10-08 → 4199.4. Полный путь в фикстуре — **2 точки**:
общая фикстура `bold_price_history.json` урезана до трёх дат ради тестов ETF-контура (2015-01-01
там — день без `units`/`aum`), а для цены та строка тоже точка, поэтому тело батча собирается
локальным хелпером `boldPriceHistoryBody` (из фикстуры убирается `2015-01-01`); на живых данных
полный путь пишет весь ряд — 3 138 точек с 2014-10-01 (комментарий в хелпере это фиксирует).

## 3. Саботаж-проверки (3; файл правился и восстанавливался)

| саботаж | упавший тест | вывод |
|---|---|---|
| `if false && !latestDate.After(stored)` (снята проверка «точка новее максимума») | `TestBoldPriceImportNoopWhenUpToDate` (и только он) | проверка держится |
| `point{Series: "comex_front", …}` (чужая площадка) | `TestBoldPriceImportFullWritesVenue`, `TestBoldPriceImportIncrementalWritesOneDay` | venue пинится |
| `boldNeedsFullFetch(time.Time{}, stored)` (решение о режиме игнорирует часы) | `TestBoldPriceImportFullWhenStoredIsStale` | старение площадки реально уводит на полный путь |

Восстановление проверено: `git status` чистый, `gofmt -l` пуст, тесты зелёные, дальше — коммит
`a730e2e` только с четырьмя файлами.

Замечание (выровнено в fix round 1, Ruling «fix 3»): проверка «дата новее сохранённого максимума»
живёт в ветке no-op, поэтому саботаж «снять условие `latestDate.After(stored)`» валит
`TestBoldPriceImportNoopWhenUpToDate`, а не `TestBoldPriceImportIncrementalWritesOneDay`: в
инкременте максимум 08.10, дата источника 09.10, и точка дня пишется при любом варианте условия.
Формулировка в Step 3 брифа говорила о другом тесте; перепроверено 12.10.2026 после fix round 1 —
падает ровно один тест, `TestBoldPriceImportNoopWhenUpToDate`. Так же сказано и в комментарии
самого теста.

## 4. Команды и результаты

1. `gofmt -l bold/ gold/` → пусто.
2. `go vet ./bold/... ./gold/...` → без замечаний.
3. `go test ./bold/... ./gold/... -v -count=1` → **PASS: bold 49, gold 3, FAIL 0** (52 top-level;
   было 51 до добавления `TestBoldPriceImportModeIsZoneAgnostic`, §7).
4. `make all` → `0 issues` (golangci-lint), `go vet`, `go test -race ./...` — все пакеты ok
   (в т.ч. `bold 1.545s`, `gold 1.737s`), обе сборки (`build/clickhouse-import-rosstat`,
   `build/ingest`) собраны.
5. Копия DDL сверена скриптом (python3, офлайн): тело `boldGoldPricesDdl` из `bold/bold.go`
   **побайтово равно** телу `goldDdl` из `gold/gold.go`
   (`' (\n\t\t\t  venue LowCardinality(String)\n\t\t\t, date Date32\n\t\t\t, usd Float64\n\t\t) ENGINE = ReplacingMergeTree ORDER BY (venue, date);\n\t'`).
6. IDE MCP: `get_file_problems` по `bold/bold.go`, `bold/price_test.go`, `gold/series_meta.go` —
   ошибок нет (только предсуществующие warning-и «SQL dialect is not configured» на строковых
   константах с SQL); `get_symbol_info` по `boldVenue`, `goldTable` и `boldGoldPricesDdl` —
   символы разрешаются, resolved value копии DDL = `CREATE TABLE IF NOT EXISTS gold_prices ( … )
   ENGINE = ReplacingMergeTree ORDER BY (venue, date);`.
7. ClickHouse MCP (read-only `kimi_reader`): `SELECT series, count(), min(date), max(date) FROM
   v_gold_prices GROUP BY series` → сейчас `comex_front` 502 точки, `moex_fix_usd` 554 — **площадки
   `bold` ещё нет**, значит первый живой прогон в Task 5 пойдёт полным путём (3 138 точек);
   `SELECT … FROM v_series_catalog WHERE source='gold'` → запись `bold` появится после прогона.
   Прямой SELECT к `gold_prices` у `kimi_reader` отбит `ACCESS_DENIED` — это подтверждает
   комментарий к гвардейским запросам (проверить их иначе нечем, пин — текстовый).

## 5. Что получают Task 4–5

- Имя импортёра `bold_gold_price` (dagu и `make import STAT=bold_gold_price`) — Task 4/5.
- Площадка `venue='bold'` в `v_gold_prices` (та же витрина, что у `gold/`), запись каталога
  `source='gold'`, `series='bold'` появляется тем же прогоном.
- Публичные имена для навигации: `boldPriceTable`, `boldPriceVenue`, `boldPriceStoredMaxQuery`,
  `boldWritePrices`, `boldPricePoints`, `boldPriceStoredMaxDate`, `gold.BoldPriceSeriesMeta()`.
- Task 5 (живая приёмка): `make import STAT=bold_gold_price` на текущей базе → полный режим,
  ожидание **3 138 точек**, `v_gold_prices.series='bold'`, максимум 2026-10-09 = 4216.3; повторный
  прогон в те же сутки → `нечего обновлять (0 строк)` и один запрос.

## 6. Сомнения и отклонения от буквы брифа

### 6.1 Существенное: чем кормится `boldNeedsFullFetch` при выборе режима

Бриф требует «обычный инкремент — **1 запрос** (`price-latest`), полный путь — **1 запрос**
(`price.json`) при пустой площадке **или при `boldNeedsFullFetch`**». Дата ответа при этом
неизвестна до запроса, а «пробный» `price-latest` перед файлом истории сделал бы полный путь
двухзапросным. Поэтому решение принимается **до запроса** и `boldNeedsFullFetch` получает часы
прогона (`s.clock()`) вместо даты `*-latest`: сегодняшний день — верхняя граница возможной даты
источника, и «максимум старше 4 календарных дней» значит, что источник мог опубликовать дни,
которых у нас нет. Пустая площадка уводит на полный путь тем же вызовом (нулевой максимум).
Потребитель у шва `now` — именно это решение (в Task 2 он был нужен только warn-у).

Цена решения: если прогонов не было 5+ дней, каждый такой прогон читает `price.json` целиком
(117 КБ, перезапись идемпотентна) — иначе пропущенные дни не закрыть. Обратная сторона: разрыв
1–3 дня (длинные выходные) закрывается инкрементом по одной точке, как и у ETF-импортёра.

**Ратифицировано контроллером (12.10.2026):** в рабочем дереве лежат его правки доков —
спека §5 («Цена (`bold_gold_price`) — свой ритм: один запрос в любом режиме, поэтому режим
решается **до** запроса — пустая площадка или `now − сохранённый максимум > 96 ч` → полный файл,
иначе `price-latest`») и план, Step 2 Task 3 (то же решение плюс «пишутся **все** точки файла»).
Это ровно реализованное поведение, поэтому вопрос §6.1 закрыт; правки доков в мой коммит не
включены — доки коммитит контроллер отдельными `docs:`-коммитами (прецедент `0bb8c1b`), и они
остаются в рабочем дереве (см. §7, п. 4).

### 6.2 Полный путь пишет **все** точки файла, без фильтра «новее максимума»

Так сказано в брифе («Полный путь пишет все точки файла (идемпотентно) — так же, как в Task 2»),
так и сделано; текст плана («пишутся точки новее сохранённого максимума — на пустой таблице весь
ряд») этому не противоречит только на пустой таблице. При старении площадки перезаписывается и
старая часть ряда — это и есть лечение прошлых дыр, ReplacingMergeTree по (venue, date) делает
это идемпотентным.

### 6.3 Тесты сверх пяти из брифа

`TestBoldGoldPriceIsRegistered` (без записи в `chimport.Stats` `make import STAT=bold_gold_price` не
найдёт импортёр, а тесты останутся зелёными — прецедент Task 2),
`TestBoldPriceImportFullWhenStoredIsStale` (без него решение о режиме по часам не пинилось бы
вовсе: пустая площадка уводит на полный путь и без часов) и добавленный в fix round 1
`TestBoldPriceImportModeIsZoneAgnostic` (Ruling 13, §7). Все — про поведение, требуемое брифом и
рулингами.

### 6.4 Прочее

- `boldPricePoints` — новая функция, которой нет в списке интерфейсов брифа: она нужна, чтобы
  `nil` в `value` истории не превратился в тихий пропуск дня (класс ошибок Review Focus 1 у
  `boldFundPoints` в Task 1). `errors` и `slices` были в файле и раньше.
- `boldGoldPricesInsert` пишет с явным списком колонок `(venue, date, usd)`, в отличие от
  `goldInsert` в `gold/` (там `INSERT INTO gold_prices` без списка). Порядок колонок в DDL
  одинаков, поэтому вставки совместимы; явный список страхует от будущего переупорядочивания.
- В `bold/` появился импорт пакета `gold` — это следствие решения контроллера («запись каталога
  объявляется в `gold/series_meta.go` экспортированной функцией, а публикует её твой импортёр»);
  цикла нет, `gold` пакет `bold` не импортирует.
- Тесты офлайн (httptest + фикстуры Task 1), новых зависимостей нет, все `err` проверены,
  запись — только батчами, логирование — `logrus`, секреты не читались.

---

## 7. Fix round 1 — коммит `1d78814` (`bold price: keep the catalog description factual`)

Ревью: Approved, Critical/Important нет; закрыты три мелочи (две — того же класса «тихая ложь в
agent-facing тексте», что ловили в Task 1–2). Файлы коммита: `gold/series_meta.go` (+13/−8),
`bold/bold.go` (+6/−1), `bold/price_test.go` (+62/−1); итого +81/−10, ровно три файла.

### 7.1 Ruling 12 — описание каталога стало качественным

Из `gold/series_meta.go` убраны все снимковые числа: «3 из 13 контрольных дат», «расхождения до
$42», «2026-10-08: 4157,0 против 4199,4», «ряд начинается 2014-10-01», «на 2026-10-09 — 3 138
точек». Площадка в БД пуста (проверить числа нечем), а число точек устаревает при первом же
инкременте — каталог читает MCP-агент, и устаревшее число в описании ряда хуже его отсутствия.
Осталось качественное: источник называет это **Futures Price**; конкретный метод (контракт, время
снимка) детально не раскрыт; совпадение с `comex_front` (GC=F) лишь частичное → **отдельная серия**,
не спот и не GC=F; глубина ряда и текущий максимум — в `v_gold_prices`/`ARCHITECTURE.md §6.2`.
Причина запрета чисел записана и в док-комментарии функции, чтобы следующая правка их не вернула.
Проверка: `grep -nE "3 из 13|\\$42|3 138|3138|2014-10-01|2026-10-08|2026-10-09" gold/series_meta.go`
→ пусто. `TestBoldPriceCatalogEntry` не менялся и проходит (он держит качественные фрагменты:
«метод», «не раскрыт», «не спот», «comex_front», «bold.report», unit с «USD», origin с
`gold/price.json`).

### 7.2 Ruling 13 — порог считается по UTC

`boldGoldPrice.Import` теперь зовёт `boldNeedsFullFetch(s.clock().UTC(), stored)`: сохранённый
максимум — дата UTC-календаря (`boldScanMaxDate` собирает её как 00:00 UTC), и порог обязан
считаться по одной шкале с ней (Ruling 10 — «по часам прогона», оставлен). Комментарий у условия
объясняет и зачем, и почему локальная зона дала бы другое число часов.

Новый тест `TestBoldPriceImportModeIsZoneAgnostic` (3 подтеста, все PASS): максимум 06.10, тогда
«ровно 96 ч» → инкремент (1 запрос `price-latest`), «+1 минута» → полный путь (1 запрос
`price.json`), «та же граница, выраженная в поясе −07:00» (локальные сутки 09.10) → та же ветка
инкремента. Режим читается по единственному запрошенному пути — сервер отдаёт оба набора фикстур.

**Честная оговорка по этому тесту:** он пинит границу и независимость решения от пояса, но **не
дискриминирует именно `.UTC()`** — `time.Time.Sub` сравнивает моменты, а не календарные поля,
поэтому тест зелёный и с `s.clock()`, и с `s.clock().UTC()` (проверено: временно убрал `.UTC()` —
3/3 PASS). Ruling 13 это допускает («достаточно, что вход в UTC по построению»): приведение к UTC —
документарная страховка на случай, если кто-то однажды начнёт считать режим по календарным полям
локальной зоны (тот тест такую правку поймает, если она изменит выбор ветки на границе).

### 7.3 Fix 3 — комментарий саботажа выровнен с реальностью

В `bold/price_test.go` у `TestBoldPriceImportNoopWhenUpToDate` прямо сказано: саботаж «снять
условие `latestDate.After(stored)`» валит **этот** тест (прогон переписал бы точку 09.10 поверх
сохранённой), а не инкрементный — там максимум 08.10, и точка дня пишется при любом варианте
условия. §3 отчёта переписан соответственно. Саботаж перепроверен после fix round 1:
`if false && !latestDate.After(stored)` → падает ровно `TestBoldPriceImportNoopWhenUpToDate`
(`--- FAIL: TestBoldPriceImportNoopWhenUpToDate`), остальные тесты пакета зелёные; правка
откачена.

### 7.4 Проверки перед коммитом (вывод)

```
### gofmt -l bold/ gold/
(пусто)
### go test ./bold/... ./gold/... -v -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/bold	0.278s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/gold	0.414s
### счёт PASS/FAIL
bold: 49
gold: 3
### make all
0 issues.                                   # golangci-lint
go vet ./...
go test -race ./...
ok  	github.com/kmlebedev/clickhouse-import-rosstat/bold	(cached)
ok  	github.com/kmlebedev/clickhouse-import-rosstat/gold	(cached)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o build/clickhouse-import-rosstat .
go build -o build/ingest ./cmd/ingest
```

Ни один из трёх пунктов «не трогать» не задет: полный путь по-прежнему пишет все точки файла,
режим по-прежнему стоит одного запроса, DDL-копия, порядок «батч → каталог», пин `4216.3` и
фикстуры не менялись (`git diff 1d78814 --numstat`: только 3 файла).

Открытый хвост для контроллера: в рабочем дереве остались **незакоммиченные правки доков**
(`docs/superpowers/plans/2026-10-12-bold-t1-gold.md` — Step 2 Task 3; `docs/superpowers/specs/2026-10-12-bold-t1-gold-design.md` — §5), сделанные не мной и фиксирующие
реализованное поведение (§6.1). В коммит `1d78814` они не включены: доки-правки контроллер
коммитит отдельными `docs:`-коммитами (прецедент `0bb8c1b`), а сообщение моего коммита — про
описание каталога.
