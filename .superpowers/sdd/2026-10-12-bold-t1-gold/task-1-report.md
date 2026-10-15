# Task 1 report — `bold/` разбор JSON, конверсии и построение точек (включая fix round 1)

- **Статус:** DONE_WITH_CONCERNS — все 6 замечаний раунда 1 закрыты; открыто только унаследованное сомнение про ключ `(date, ticker)` (§7.1, задача Task 2/4, не Task 1).
- **Ветка:** `feat/bold-t1-gold`
- **Коммиты:**
  - `5b2341aafdc77ea54dc6f4c7c9342bb595077d60` — `add bold.report json parsing and conversions` (11 файлов, +870) — волна 1;
  - `ba6bcf409292e93bd47cdcece0246e39b8891ca5` — `bold parse: fail fast on partial fund rows and windows` (2 файла, +206/−50) — fix round 1.
- **Тесты:** `go test ./bold/... -v -count=1` → **PASS 17/17** (`ok github.com/kmlebedev/clickhouse-import-rosstat/bold 0.213s`); `gofmt -l bold/` пусто; `golangci-lint run ./bold/...` → `0 issues.`; `make all` **зелёный** (`0 issues.` → `go vet ./...` → `go test -race ./...` → обе сборки).
- **Сеть в тестах:** нет (только чтение `bold/testdata/*.json`); `os.Getenv`, HTTP, БД в пакете отсутствуют.

## 1. Что сделано (волна 1)

Созданы `bold/parse.go` и `bold/parse_test.go`, каталог `bold/testdata/` (**9 фикстур**; в первой редакции отчёта было ошибочно написано «10» — исправлено).

Произведено ровно то, что обещано в Interfaces брифа (контракт для Task 2/3):

- константы: `boldBaseURL`, `boldPathPrice/PriceLatest/Units/UnitsLatest/Aum/AumLatest/Flows/FlowsLatest/Summary`;
- типы: `point`, `boldRow` + `boldEnvelope`, `boldFundRow` + `boldFundSnapshot`, `boldFundPoint`, `boldMetric` (+ `boldMetricPrice/Units/Aum/Flows`);
- методы: `boldMetric.name()`, `boldMetric.missing()`, `boldMetric.require(r boldRow) error`, `boldMetric.present(r boldRow) bool`;
- функции: `parseBoldDate`, `parseLatest(body, metric)`, `parseHistory(body, metric)`, `parseFundSnapshot(body)`, `ozToTonnes`, `boldETFSets(rows, prevUnits, prevDate) ([]point, error)`, `boldFundPoints(snapshot, date) ([]boldFundPoint, error)`;
- имена 7 рядов — локальными константами: `etf_bold_holdings_tonnes`, `etf_bold_aum_usd_bn`, `etf_bold_flow_tonnes`, `etf_bold_flow_usd_mln`, `etf_bold_flow_7d_tonnes`, `etf_bold_flow_30d_tonnes`, `etf_bold_flow_90d_tonnes`.

Семантика (в комментариях к коду):

- `nil` в поле истории — **отсутствие данных, а не ноль**: строка остаётся в срезе с `nil`, точку решает `boldETFSets` (2015-01-01 не даёт ни одного ряда);
- окна берутся **как публикует источник** (`culm7/30/90day ÷ 32150.74656862`), без пересчёта из дневного ряда;
- дневной поток: Δ к предыдущей строке среза, для первой строки — к `prevUnits/prevDate` из ClickHouse; нет предыдущего — потока нет; `etf_bold_flow_usd_mln` только при непустом `price_t`;
- `boldETFSets` сортирует строки по дате (`sort.SliceStable`); пустой результат — ошибка «нет точек»;
- даты — сразу `time.ParseInLocation("2006-01-02", s, time.UTC)`, без нормализации.

## 2. Fix round 1 — что изменено по замечаниям

| № | Замечание | Что сделано |
|---|---|---|
| **1 (Important, Ruling 4)** | `boldFundPoints` молча превращала `nil` в 0 | сигнатура → `func boldFundPoints(snapshot []boldFundRow, date time.Time) ([]boldFundPoint, error)`; внутри — тот же `requireFundRow`, ошибка вида `bold: фонд TEST (Test Gold ETF): нет обязательного поля aum`; хелпер `fundValue` (ноль вместо nil) **удалён**, поля разыменовываются только после проверки. Проверка в `parseFundSnapshot` оставлена, в комментариях обеих функций отмечено, что это **два уровня** (смена формы ответа ловится на разборе с номером строки; функция защищается при прямом вызове) |
| **2 (Important)** | `TestBoldETFSetsErrorsOnDateMismatch` проверял не сценарий из плана | тест переименован в `TestBoldETFSetsRejectsPrevDateNotOlder` («prevDate не старше даты строки → ошибка»), комментарий переписан: у строки `boldETFSets` одна общая `Date` на все метрики, межэндпоинтная сверка дат — в Task 2 (`boldEtfFetchLatest` / `TestBoldEtfFetchLatestMismatchedDatesFails`). Сам guard `!baseDate.Before(date)` сохранён; asserts усилены до подстрок `"предыдущий сток за 2026-10-09"` и `"не старше строки 2026-10-08"` |
| **3 (Minor, Ruling 5)** | молчаливая частичная потеря | добавлен `requireCulmWindows` (правило «**все три `culm*` или ни одного**»), вызывается и в `parseHistory`, и в `boldETFSets`; сообщение называет и заполненные, и отсутствующие поля: `окна заполнены частично: есть culm7day, culm90day; нет culm30day — в источнике либо все три, либо ни одного`. Добавлен запрет **дубля даты** в `parseHistory` (по файлу) и в `boldETFSets` (по срезу) — `дата 2026-10-08 повторяется (строки 2 и 3) — это дало бы дубли точек`. Правила проверены по живым файлам: в `flows/all` (3138 строк) дублей дат нет, частичных `culm*` нет — строки до 2015-04-01 без окон, с 2015-04-01 все три; в `units`/`aum` (3072) и `price` (3138) дублей дат тоже нет |
| **4 (Minor)** | слабые asserts на `units` | вместо `Contains(err, "units")` проверяются фрагменты, невозможные без правильного сообщения: `"метрики units за 2026-10-09 нет обязательного поля units"`, `"нет обязательного поля culm30day"`, `"в истории метрики units нет ни одной строки с её полями"`, `"нет обязательного поля aum"` |
| **5 (Minor)** | в отчёте «10 фикстур» | исправлено на **9** (см. §1) |
| — | фикстуры | **не менялись** (ни одна строка не добавлена): оба новых правила выражаются правкой живой фикстуры в памяти (`testMutateJSON`) |

Новые тесты (5) и один переименованный; всего **17**:

- `TestBoldFundPointsRejectsNilFields` — nil по очереди в каждом из 8 числовых полей → ошибка с тикером и именем поля; полная строка проходит;
- `TestParseHistoryRejectsPartialWindows` — `culm30day` удалён из строки 09.10 → ошибка «окна заполнены частично… нет culm30day»;
- `TestParseHistoryRejectsDuplicateDates` — 09.10 подменена копией 08.10 → ошибка «дата 2026-10-08 повторяется (строки 2 и 3)»;
- `TestBoldETFSetsRejectsPartialWindows` — частичное окно в строке среза → ошибка;
- `TestBoldETFSetsRejectsDuplicateDates` — та же дата дважды в срезе → ошибка;
- `TestBoldETFSetsRejectsPrevDateNotOlder` — переименование из замечания 2.

**Для Task 2 (важно):** вызов из плана (`docs/…/2026-10-12-bold-t1-gold.md:168`) `fundPoints := boldFundPoints(funds, latestDate)` теперь не компилируется — нужно `fundPoints, err := boldFundPoints(funds, latestDate)` с проверкой `err` (план в этой строке ещё не поправлен, Ruling 4 это допускает).

## 3. Фикстуры (не изменились с волны 1)

Сгенерированы из живых ответов в `/tmp` (сеть не понадобилась): `latest_*` — как есть, история — отфильтрована по `date in {2015-01-01, 2026-10-08, 2026-10-09}` (по 3 строки).

| файл | байт | содержимое |
|---|---|---|
| `bold_price_latest.json` | 87 | `{version,updated,date:2026-10-09,value:4216.3}` |
| `bold_units_latest.json` | 105 | `… date, units:113180843, price:4216.3` |
| `bold_aum_latest.json` | 106 | `… date, aum:476677811738, price:4216.3` |
| `bold_flows_latest.json` | 143 | `… date, culm7day:43740, culm30day:991060, culm90day:5376055, price:4216.3` |
| `bold_summary.json` | 14 921 | 73 фонда, 10 полей |
| `bold_units_history.json` | 209 | 3 строки (2015-01-01 — только `price`) |
| `bold_aum_history.json` | 211 | 3 строки (2015-01-01 — только `price`) |
| `bold_flows_history.json` | 286 | 3 строки (2015-01-01 — только `price`) |
| `bold_price_history.json` | 173 | 3 строки |
| **итого (9 файлов)** | **16 241** | единицы КБ, как и ожидал бриф |

## 4. Арифметика пинов (проверена на фикстурах, не подгонялась; в fix round 1 не менялась)

`OZ = 1e6/31.1034768 = 32150.746568627983`:

| факт (2026-10-09) | пину брифом | из фикстур | вердикт |
|---|---|---|---|
| `units` | 113 180 843 oz | 113 180 843 | ✓ |
| `units → т` | 3520.318 | 3520.317724455 | ✓ (±1e-3) |
| `aum` | 476 677 811 738 | 476 677 811 738 | ✓ |
| `aum → млрд $` | 476.678 | 476.677811738 | ✓ |
| `culm7day → т` | 1.360 | 1.360466075 | ✓ |
| `culm30day → т` | 30.825 | 30.825411717 | ✓ |
| `culm90day → т` | 167.214 | 167.214001968 | ✓ |
| `price` | 4216.3 | 4216.3 | ✓ |
| Δ к 08.10 | 136 080 oz | 113 180 843 − 113 044 763 = 136 080 | ✓ |
| Δ → т | +4.233 | +4.232561123 | ✓ |
| Δ × price → млн $ | 573.75 | 573.754104 | ✓ (пин до 2 знаков, тест — допуск 1e-2) |
| фондов в срезе | 73 | 73 | ✓ |
| GLD (SPDR Gold Shares) `units` | 33 552 628 | 33 552 628 | ✓ |

## 5. Прогоны и саботаж-проверки

### 5.1 Проверки перед коммитом fix round 1

```
$ gofmt -l bold/                         # пусто
$ go test ./bold/... -v -count=1         # 17/17 PASS, ok … 0.213s
$ golangci-lint run ./bold/...           # 0 issues.
$ make all                               # зелёный: 0 issues. → go vet ./... → go test -race ./... → сборки
   … ok github.com/kmlebedev/clickhouse-import-rosstat/bold
   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath … -o build/clickhouse-import-rosstat .
   go build -o build/ingest ./cmd/ingest
$ IDE get_file_problems bold/parse.go, bold/parse_test.go   # errors: []
```

### 5.2 Саботаж-проверки fix round 1 (файл правился, затем восстановлен из бэкапа; `md5` до и после — `33f0ea2e3217b86bcdb6876b3b52eb32`)

```
A) requireCulmWindows: правило «все три culm*» снято (сообщение собирается, но не возвращается)
--- FAIL: TestBoldETFSetsRejectsPartialWindows
    parse_test.go:393: строка с частично заполненными окнами обязана быть ошибкой
--- FAIL: TestParseHistoryRejectsPartialWindows
    parse_test.go:526: строка с частью окон culm* обязана быть ошибкой
FAIL

B) boldFundPoints: ошибку на nil заменили молчаливым нулём (вернули fundValue)
--- FAIL: TestBoldFundPointsRejectsNilFields
    parse_test.go:500: nil в поле units обязан быть ошибкой, а не нулём
FAIL
```

(При снятии правила `requireCulmWindows` первая попытка саботажа вообще не собралась: `strings` остался неиспользуемым — это косвенно подтверждает, что проверка была единственным потребителем `strings.Join`.)

### 5.3 Саботаж-проверки волны 1 (перепроверены после правок)

| саботаж | упавшие тесты |
|---|---|
| убрать `/32150.74656862` | `TestOzToTonnes`, `TestBoldETFSetsPinsFacts`, `TestBoldETFSetsUsesPrevUnitsFromDB` |
| считать `null` нулём (`parseHistory` заполняет `units` нулём) | `TestParseHistorySkipsNullUnits` (units ≠ nil), `TestBoldETFSetsPinsFacts` (15 точек вместо 12: нулевой сток за 2015-01-01 + фиктивный поток 3516.085 т за 08.10) |
| окна суммой 5 торговых дней | `TestBoldETFSetsPinsFacts` (7d = 4.233 вместо 1.360), `TestBoldETFSetsSkipsFirstFlow` (4 вместо 5), `TestBoldETFSetsUsesPrevUnitsFromDB` (6 вместо 7) |

Полный список 17 тестов: `TestParseLatestPinsFacts`, `TestParseLatestRequiresMetricFields`, `TestParseLatestRejectsBadDate`, `TestParseHistorySkipsNullUnits`, `TestBoldETFSetsPinsFacts`, `TestBoldETFSetsSkipsFirstFlow`, `TestBoldETFSetsUsesPrevUnitsFromDB`, `TestBoldETFSetsRejectsPrevDateNotOlder`, `TestBoldETFSetsRejectsDuplicateDates`, `TestBoldETFSetsRejectsPartialWindows`, `TestParseFundSnapshotPinsFacts`, `TestParseFundSnapshotRejectsMissingFields`, `TestBoldFundPointsRejectsNilFields`, `TestOzToTonnes`, `TestParseHistoryRejectsPartialWindows`, `TestParseHistoryRejectsDuplicateDates`, `TestBoldEndpointsPinPaths`.

## 6. Границы задачи

`go.mod` не тронут, новых зависимостей нет (stdlib: `encoding/json`, `errors`, `fmt`, `sort`, `strings`, `time`; в тестах также `math`, `net/url`, `os`, `path/filepath`, `strconv`, `strings`, `testing`); сеть/БД/витрины не добавлены; фикстуры не менялись; пины и конверсии не тронуты. MCP `jetbrains` использован (`get_file_problems` — чисто); `clickhouse` MCP в этой задаче не нужен (SQL не выполнялся).

## 7. Сомнения

### 7.1 Тикер фонда НЕ уникален (унаследованное, не Task 1)

В живом `gold/summary` **73 строки, но 71 уникальный тикер: `GLD` встречается трижды** (`KTAM Gold ETF Tracker Class GLD (Bangkok)`, `NewGold Issuer Ltd`, `SPDR Gold Shares`). Спека §4.3 объявляет `bold_fund_snapshot` как `… ReplacingMergeTree ORDER BY (date, ticker)`, а приёмка (Task 5) ждёт **73 строки** и витрину по 73 фондам — при таком ключе получится **71**. Task 1 это починить не может (его контракт — `boldFundPoint` без venue/ключа), решение нужно в Task 2/4 (например `ORDER BY (date, ticker, name)` — у трёх `GLD` имена разные). Факт закреплён тестом `TestParseFundSnapshotPinsFacts` (73 строки, `GLD` ×3, точки по всем 73).

### 7.2 Прочее (осознанные решения)

- `parseHistory` падает, если в файле нет ни одной строки с полями метрики (fail-fast §5 спеки; единственное осмысленное применение `boldMetric.present`).
- `boldETFSets` падает, если `prevDate` не старше первой строки среза — иначе дельта считалась бы с будущим стоком.
- Новые правила («все три или ни одного», запрет дублей) делают импорт красным, если источник начнёт штатно отдавать неполные дни/дубли — это заявленная цена (заметно в логе), на живых данных (3138/3072 строк) нарушений нет.
- `ouncesPerTonne = 1e6/31.1034768` (= 32150.746568627983) вместо литерала `32150.74656862` из брифа — как записано в глобальных ограничениях плана; расхождение 1.3e-13, на пины не влияет.
- Комментарий пакета начинается с «Package bold …» (Go-конвенция: иначе IDE даёт weak warning).
- Пины `3520.318`, `476.678`, `1.360/30.825/167.214`, `573.75` — округлённые; точные значения фикстур `3520.317724455`, `476.677811738`, `1.360466075/30.825411717/167.214001968`, `573.754104`.
