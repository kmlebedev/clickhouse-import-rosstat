# Task 5 report — `vvp_kvartal`: починка разбора (заголовок A2, обход с нулевой колонки, перенос года)

Status: **DONE** (единственная претензия — опечатка метки года на листе «14» — подтверждена контроллером и закрыта в Fix-round 1: повтор метки не принимается на веру, год выводится по позиции; см. секцию «Fix-round 1» внизу).

> Fix-round 1 (после ревью контроллера) меняет описанный ниже контракт разбора в одном месте:
> повтор метки года больше не принимается как есть. Актуальное описание — в секции
> «Fix-round 1» в конце файла; текст «What I implemented» оставлен как есть, чтобы diff
> отчёта читался от первой итерации.

## What I implemented

`rosstat/vvp_kvartal.go` — разбор переписан целиком по четырём заявленным расхождениям:

1. **Заголовок A2 больше не читается вовсе.** Имя ряда объявлено кодом (§6.8): `vvpKvartalSeriesNames` — карта «лист → имя», 1:1 с `vvpKvartalSeriesMeta[].Series` (включая опечатки источника «внутрений» на листах «2»/«9» и «внутренний» на «10»). Паника `rows[1][1]` ушла вместе с чтением заголовка.
2. **Обход колонок строго с нуля** (`for i := 0; i < len(data); i++`, граница — длина среза данных строки, `excelize` обрезает хвостовые пустые ячейки). Пустая ячейка замыкает таблицу листа.
3. **Перенос года.** Строки находятся по содержимому: `labelRow` — первая строка, чья первая ячейка начинается с `I `/`II `/`III `/`IV ` и содержит «квартал»; `yearRow = labelRow-1`, `dataRow = labelRow+1`, выход за границы — ошибка. Непустая `yearRow[i]` задаёт текущий год (первые 4 символа, `strconv.Atoi`; мусор/короче 4 знаков — ошибка), пустые колонки группы его переносят. Год 0 на колонке данных — ошибка с именем листа и номером колонки.
4. **`getQuarterDate(year int, quarter string) (time.Time, error)`** — `panic("Invalid quarter")` заменён на ошибку; семантика даты сохранена дословно (I → 1 апреля, II → 1 июля, III → 1 октября, IV → 1 января следующего года, `time.Date(year, startMonth+3, 1, …, UTC)`).

Дополнительно:

- `xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})` (§6.2) — сырые значения, полная точность float.
- Ошибки с именем таблицы и листа на всех ветках: нет листа (обёрнутый `GetRows`), нет строки подписей, нет строки годов/данных, нет подписи квартала, подпись пуста, год 0, год не число, значение не число, **0 точек на листе**. Разбор идёт целиком до первой записи в батч: ошибка любого листа оставляет батч пустым.
- Коды транспортного уровня: `colly`-коллектор заменён на `util.GetPage` + `util.FindLink` с константой `vvpKvartalLinkSelector = "a[href*=\"VVP_kvartal\"]"`; домен навешивается через `rosstatUrl`, ошибки не проглатываются (§4.1/§4.4).
- Порядок обхода — слайс `vvpKvartalSheetNames = []string{"2","9","10","12","14"}` (не ключи карты: обход карты в Go случаен).
- Оба `fmt.Printf` удалены без замены (лог — только logrus; импорты colly/logrus из файла убраны).
- `Name()`/`TableName` = `rosstat_vvp_kvartal`, переменная `vvpKvartalImporter`, DDL и `PublicationCountPoints` — без изменений; `init()` не добавлялся.
- Мелочь сверх буквы брифа (осознанно, по образцу Task 4 в `salaries_mes.go`): NaN/Inf в ячейке значения отвергаются ошибкой «не конечное число» — иначе `ParseFloat` пропустил бы их точкой ряда в витрину. Покрыто тест-кейсом.

`rosstat/vvp_kvartal_test.go` — новый файл (11 тестов), `rosstat/series_meta_test.go` — удалён устаревший синтетический `TestVvpKvartalSeriesMetaCoversParsedSheets` (он строил книгу с заголовком в колонке **B**, то есть ровно с тем допущением, которое прятало дефект).

## TDD Evidence

### RED-1 (паника на нынешнем коде; соответствует Step 3 брифа)

Тест-файл на этом шаге содержал только те проверки, которые компилируются против текущего API (`vvpKvartalImport` + фикстура + `appendRecorder`).

```
$ go test ./rosstat/ -run 'TestVvpKvartalReadsFixture|TestVvpKvartalKeepsFirstGroup' -count=1
exit=1
--- FAIL: TestVvpKvartalReadsFixture (0.01s)
panic: runtime error: index out of range [1] with length 1 [recovered, repanicked]
goroutine 49 [running]:
testing.tRunner.func1.2({0x106140ba0, 0x7905c6222ee8})
...
github.com/kmlebedev/clickhouse-import-rosstat/rosstat.parseVvpKvartal(0x7905c6514608)
	/Users/whitefox/GolandProjects/clickhouse-import-rosstat/rosstat/vvp_kvartal.go:87 +0x5b0
github.com/kmlebedev/clickhouse-import-rosstat/rosstat.vvpKvartalImport(...)
	/Users/whitefox/GolandProjects/clickhouse-import-rosstat/rosstat/vvp_kvartal.go:121 +0x24
...
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.379s
```

Почему это ожидаемо: строка 87 — `strings.Split(rows[1][1], ")")`. На живом листе A2 — заголовок, объединённый A2:D2, поэтому строка заголовка одноячейковая и `[1]` вне среза. Это ровно тот дефект, из-за которого ряд в проде даёт 0 точек (импортёр падает). Заодно `TestVvpKvartalSeriesMetaCoversParsedSheets` в старом тесте печатал `table name …` и `I kvartal …` — следы `fmt.Printf`, которые бриф требует убрать.

### RED-2 (новый контракт ещё не реализован)

После дописывания остальных тестов файл перестаёт собираться против старого кода (это и есть «фиксируем новый интерфейс до реализации»):

```
$ go test ./rosstat/ -run TestVvpKvartal -count=1
# github.com/kmlebedev/clickhouse-import-rosstat/rosstat [github.com/kmlebedev/clickhouse-import-rosstat/rosstat.test]
rosstat/vvp_kvartal_test.go:167:22: cannot range over table (variable of type *[]vvpKvartal)
rosstat/vvp_kvartal_test.go:254:35: undefined: vvpKvartalLinkSelector
rosstat/vvp_kvartal_test.go:276:17: undefined: parseVvpKvartalSheet
rosstat/vvp_kvartal_test.go:360:19: undefined: parseVvpKvartalSheet
rosstat/vvp_kvartal_test.go:390:16: assignment mismatch: 2 variables but getQuarterDate returns 1 value
rosstat/vvp_kvartal_test.go:398:15: assignment mismatch: 2 variables but getQuarterDate returns 1 value
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/rosstat [build failed]
```

Про `TestVvpKvartalSpotValue` (допуск 1e-6): проверка идёт по результату разбора (`parseVvpKvartal` → `float64`), а не по батчу, и это осознанно: колонка таблицы — `Float32`, а шаг представления float32 около 13024 — 2^-10 ≈ 1e-3, так что допуск 1e-6 к значению из батча неприменим в принципе (`float32(13024.799539024814)` = 13024.7998046875, отличие ~2.7e-4). Сырое значение ячейки обязано сохраняться в `float64` вплоть до записи в батч — тогда 1e-6 отличит его и от форматированного «13,024.8», и от усечения. Это записано комментарием в тесте.

### GREEN

```
$ go test ./rosstat/ -run TestVvpKvartal -v -count=1
=== RUN   TestVvpKvartalReadsFixture
--- PASS: TestVvpKvartalReadsFixture (0.01s)
=== RUN   TestVvpKvartalKeepsFirstGroup
--- PASS: TestVvpKvartalKeepsFirstGroup (0.01s)
=== RUN   TestVvpKvartalSpotValue
--- PASS: TestVvpKvartalSpotValue (0.01s)
=== RUN   TestVvpKvartalSeriesMetaCoversParsedRows
--- PASS: TestVvpKvartalSeriesMetaCoversParsedRows (0.01s)
=== RUN   TestVvpKvartalMissingSheetFails
--- PASS: TestVvpKvartalMissingSheetFails (0.00s)
=== RUN   TestVvpKvartalUnknownQuarterFails
--- PASS: TestVvpKvartalUnknownQuarterFails (0.00s)
=== RUN   TestVvpKvartalFindLink
--- PASS: TestVvpKvartalFindLink (0.00s)
=== RUN   TestVvpKvartalCarriesYearAcrossDataGroups
--- PASS: TestVvpKvartalCarriesYearAcrossDataGroups (0.00s)
=== RUN   TestVvpKvartalSheetErrorBranches
    --- PASS: TestVvpKvartalSheetErrorBranches/строка_подписей_кварталов_не_найдена (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/строка_годов_отсутствует (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/строка_данных_отсутствует (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/год_данных_не_задан (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/ячейка_года_короче_четырёх_цифр (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/год_не_число (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/значение_не_число (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/значение_не_конечное_число (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/подписей_меньше,_чем_данных (0.00s)
    --- PASS: TestVvpKvartalSheetErrorBranches/точек_нет (0.00s)
=== RUN   TestGetQuarterDate
--- PASS: TestGetQuarterDate (0.00s)
--- PASS: TestVvpKvartalAppendErrorNotSwallowed (0.01s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.873s
```

## Everything I tested (exact commands and results)

| Command | Result |
|---|---|
| `go test ./rosstat/ -run 'TestVvpKvartalReadsFixture\|TestVvpKvartalKeepsFirstGroup' -count=1` (RED, до правок) | FAIL — `panic: runtime error: index out of range [1] with length 1`, `vvp_kvartal.go:87` |
| `go test ./rosstat/ -run TestVvpKvartal -count=1` (RED-2, до правок) | FAIL (build) — `cannot range over table (… *[]vvpKvartal)`, `undefined: vvpKvartalLinkSelector`, `undefined: parseVvpKvartalSheet`, `assignment mismatch: 2 variables but getQuarterDate returns 1 value` |
| `go test ./rosstat/ -run TestVvpKvartal -v -count=1` (GREEN) | **PASS**, 11 тестов + 10 подтестов |
| `go test ./rosstat/ -count=1` | **ok** github.com/kmlebedev/clickhouse-import-rosstat/rosstat 0.497s |
| `go test ./... -count=1` | **ok** — все 20 пакетов с тестами зелёные (bea, bls, bold, calendar, cbr, craw, dcf, eia, forecast, fred, gold, ingest, minfin, moex, news, polyus, rosstat, util, views, wgc) |
| `go build ./...` | **BUILD_OK** |
| `gofmt -l rosstat` | пусто (чисто) |
| `grep -n "colly\|fmt\.Print\|panic(" rosstat/vvp_kvartal.go` | пусто (exit 1) |
| `go vet ./rosstat/` | чисто |
| `go test ./rosstat/ -coverprofile` + `go tool cover -func` | `vvpKvartalImport` 100%, `getQuarterDate` 100%, `vvpKvartalYear` 100%, `vvpKvartalValue` 100%, `parseVvpKvartalSheet` 97.6%, `parseVvpKvartal` 90.9%, `vvpKvartalIsLabelRow` 88.9%; `getVvpKvartalXlsDataUrl`/`Import` 0% — сетевые, офлайн не вызываются (селектор проверяется через `TestVvpKvartalFindLink` на HTML-фикстуре) |
| GoLand MCP `get_file_problems rosstat/vvp_kvartal.go` | ошибок в моём коде нет; два предупреждения среды: «Vulnerable dependency usage» на импорте `excelize` (предсуществующее, репозиторное) и «SQL dialect is not configured» на строке запроса |

Проверка фактических данных на фикстуре (временный тест, удалён после прогона):

```
2 Валовой внутрений продукт                                  points=62 distinct=62 dupDates=0 2011-04-01..2026-07-01
9 Валовой внутрений продукт                                  points=62 distinct=62 dupDates=0 2011-04-01..2026-07-01
10 Валовой внутренний продукт                                points=62 distinct=62 dupDates=0 2011-04-01..2026-07-01
12 Индексы физического объема валового внутреннего продукта  points=58 distinct=58 dupDates=0 2012-04-01..2026-07-01
14 Индексы - дефляторы валового внутреннего продукта         points=58 distinct=56 dupDates=2 2012-04-01..2026-01-01
```

Итого 302 точки; spot-значение листа «2» на 2011-04-01 = 13024.799539024814 (отличие от эталона 0, допуск 1e-6).

## Which test catches which regression (mutation testing, выполнено)

**RED-мутации прогнаны на настоящем коде и откачены** (файл восстановлен, sha256 `0cd6166e…` совпадает с рабочим до мутаций).

**(a) Обход с нулевой колонки** — мутация `for i := 0` → `for i := 1`:

```
--- FAIL: TestVvpKvartalReadsFixture
--- FAIL: TestVvpKvartalKeepsFirstGroup
--- FAIL: TestVvpKvartalSpotValue
--- FAIL: TestVvpKvartalCarriesYearAcrossDataGroups
… (ещё 4 теста)
```

Прямые ловцы: **`TestVvpKvartalKeepsFirstGroup`** (первая точка ряда «2» перестаёт быть 2011-04-01) и **`TestVvpKvartalReadsFixture`** (302 → меньше). Причина падения остальных: у нулевой колонки пропадает и год, поэтому первая группа вообще остаётся без года.

**(b) Перенос года по колонкам.** Две формы регрессии:

1. Чтение года со сдвигом (`years[i+1]` — то, что делал прежний разбор) → `--- FAIL` у всех тестов на фикстуре с сообщением `rosstat_vvp_kvartal: лист "2", колонка 1: год данных не задан` (нулевая колонка теперь без года). Тесты ловят это как нарушение контракта «год 0 → ошибка».
2. Год читается один раз и больше не обновляется (`if year == 0 && …`) → дат-ловцов два:
   - **`TestVvpKvartalReadsFixture`**: `ряд "2 …": последняя точка 2012-01-01, ожидалась 2026-07-01`, `ряд "12 …": последняя точка 2013-01-01, ожидалась 2026-07-01`;
   - **`TestVvpKvartalCarriesYearAcrossDataGroups`** (синтетический лист, точные даты обеих групп) — падает целиком;
   - дополнительно `TestVvpKvartalSpotValue` показывает, что без переноса года квартальные даты групп схлопываются (`2011-04-01` начинает совпадать с данными 2013–2026 годов).

Итого: (a) ловит `TestVvpKvartalKeepsFirstGroup` (+ `ReadsFixture`), (b) — `TestVvpKvartalCarriesYearAcrossDataGroups` (+ границы рядов в `ReadsFixture`).

## Files changed

| File | Change |
|---|---|
| `rosstat/vvp_kvartal.go` | переписан разбор и транспорт: `getQuarterDate` → `(time.Time, error)`; новые `vvpKvartalSheetNames`, `vvpKvartalSeriesNames`, `vvpKvartalLinkSelector`, `vvpKvartalPoint`, `vvpKvartalIsLabelRow`, `vvpKvartalYear`, `vvpKvartalValue`, `parseVvpKvartalSheet`; `parseVvpKvartal` → `[]vvpKvartal`; `getVvpKvartalXlsDataUrl` → `(string, error)` через `util`; удалены colly, logrus, оба `fmt.Printf` |
| `rosstat/vvp_kvartal_test.go` | **новый**, 11 тестов на фикстуре + синтетических книгах |
| `rosstat/series_meta_test.go` | удалён `TestVvpKvartalSeriesMetaCoversParsedSheets` (30 строк) |

Файлы вне задачи не тронуты: `rosstat/ipc_mes.go`, `ipc_weeks.go`, `salaries_mes.go`, `importers.go`, `dagu/rosstat.yaml`, вьюха в `series_meta.go`, `deploy/staging/dagu.service`, `dashboard/rosstat.json`, `grafana_dashboard_rosstat`. В `git status` фигурирует `M deploy/staging/dagu.service` — это **предсуществующая** правка рабочего дерева, не моя; в коммит я её не включал (стейджил три файла поимённо). Зависимости не добавлялись (`math` — stdlib).

## Self-review findings

- Проверил по списку брифа: A2 не читается; подписи ищутся по содержимому (не по индексу); границы `yearRow`/`dataRow`/`labelRow`/`labels` проверены до индексации; год переносится; год 0 — ошибка с листом и колонкой; подпись — первый токен, незнакомая → ошибка (не panic); значение — `ParseFloat` сырого текста; 0 точек и отсутствие листа — ошибки с именем листа и таблицы; `getQuarterDate` с сохранённой семантикой; имена рядов из карты 1:1 с каталогом (тест двусторонний); порядок обхода — слайс; селектор — константа; `Name()`/`TableName`/`vvpKvartalImporter`/DDL/чек версии/`init()` — по правилам; `fmt.Printf`, colly, panic — вычищены.
- Самопроверка диффа: лишних `fmt`-ов нет (`fmt.Errorf` — нужен), `strconv`/`strings`/`math` используются, `util.HttpClient` больше не нужен (внутри `util.GetPage`), мёртвого кода нет. Ветка «лист без имени в карте» в `parseVvpKvartal` недостижима (обе структуры — константы пакета), оставлена защитной, как nil-check в `util.FindLink`.
- Пограничные неприятности, которые я предусмотрел: `yearRow` короче `dataRow` (61 vs 62 в фикстуре) — чтение года под `i < len(years)`, последняя колонка несёт год предыдущей; `labels` короче `data` — ошибка «подписи квартала нет»; хвостовые пустые ячейки — цикл ограничен `len(data)` и замыкается на первой пустой.
- Проверил, что тест дат 1e-6 не «случайно проходит»: он сравнивает `row.vvp` (`float64` из разбора), а не `float32` из батча; значение `13024.799539024814` отличается от формата ячейки «13,024.8» на 5e-4 — то есть проверка различает именно сырое значение.
- Коммит-сообщение брифа («read title from A2») не соответствует итоговой реализации (заголовок теперь не читается вовсе) — взял точное: `fix vvp_kvartal: stable series names, scan from column A, carry year across group`.

## Issues / concerns

1. **Опечатка в самой выгрузке: лист «14», последняя группа годов помечена «20252)» вместо «20262)».** Разбор переносит год так, как он записан в файле (спека шага 4), и не угадывает, поэтому две последние точки листа «14» получают даты 2025-04-01 и 2025-07-01 — те же, что I и II кварталы 2025 из предыдущей группы. Точек всё равно 58 (ожидание брифа выполняется), но **различающихся дат 56**, и в `ReplacingMergeTree ORDER BY (name, date)` эти две схлопнутся: у ряда дефлятора не будет 2026-I/2026-II, зато в ряду будут дубликаты дат из группы 2025. Это свойство источника, а не разбора; «лечить» его можно либо правилом монотонности лет (тогда фикстура падает с ошибкой — противоречит ожиданию 302 точки), либо выводом года из позиции колонки (догадка). Оставляю на решение контроллера; в тесте поведение задокументировано комментарием, чтобы его не «поправили» молча.
2. `getVvpKvartalXlsDataUrl` не покрыт тестом (сеть): по §11.3 проверяется его селектор на HTML-фикстуре (`util.FindLink`), а сам обход страницы — только в проде. Так же устроен `salaries_mes` после Task 4.
3. Добавил один шаг сверх буквы брифа — отказ на NaN/Inf в ячейке значения (по образцу `salariesMesValue`, Task 4). Если это нежелательно, удаляется в три строки вместе с тест-кейсом.

---

# Fix-round 1 — повтор метки года: год выводится по позиции

**Ruling контроллера принят:** повтор (или откат) метки года запрещён — год выводится из структуры
таблицы, а не принимается на веру. Пункт 1 прежней секции «Concerns» этим закрыт; Status задачи
поднят до **DONE**.

## Что изменено

### 1. `rosstat/vvp_kvartal.go` — вывод года вместо принятия повторной метки

В `parseVvpKvartalSheet` чтение непустой ячейки года разделено на «метка» и «текущий год»:

```go
if i < len(years) && strings.TrimSpace(years[i]) != "" {
	labelYear, err := vvpKvartalYear(years[i])
	if err != nil { /* … ошибка с листом и колонкой … */ }
	if year != 0 && labelYear <= year {
		log.Warnf("vvpKvartal: %s: лист %q, колонка %d: метка года %q не больше текущего %d — год выведен как %d",
			rosstatVVPKvartalTable, sheet, i+1, strings.TrimSpace(years[i]), year, year+1)
		labelYear = year + 1
	}
	year = labelYear
}
```

- Условие ровно по решению: `labelYear <= year` → `year + 1`. Срабатывает в том числе на «откат»
  метки, а не только на точное равенство.
- `log.Warnf` (logrus) с именем таблицы, листом, номером колонки, меткой источника и выведенным
  годом — как требует решение; молчаливого принятия повтора нет.
- Комментарий в коде объясняет, почему это вывод из структуры, а не догадка: группы кварталов
  идут I→IV и продолжаются I следующего года, поэтому колонка под повторившейся меткой — I квартал
  следующего года; лист «14» помечен «20252)» там, где листы «2», «9», «10», «12» того же выпуска
  Росстата несут «20262)»; принять метку на веру нельзя — значения 2026-I/2026-II затёрли бы
  настоящие 2025-I/2025-II в `ReplacingMergeTree ORDER BY (name, date)` (тихая подмена данных);
  когда источник исправит метку, вывод совпадёт с ней и предупреждение исчезнет само.
- Одна деталь сверх буквы решения, зафиксирована в том же комментарии: при `year == 0` (года ещё
  не было) вывода нет — иначе метка «0000» превратилась бы в «1» вместо ошибки. Нулевой год
  остаётся ошибкой «год данных не задан», то есть ветка `if year == 0` остаётся достижимой.
- Вернулся импорт `log "github.com/sirupsen/logrus"` (logging — logrus only; `fmt.Print`, colly и
  panic по-прежнему отсутствуют).

### 2. `rosstat/vvp_kvartal_test.go` — новый тест `TestVvpKvartalRepeatedYearLabelAdvances`

Проверяет ровно то, что предписал контроллер: у ряда листа «14» **58 точек и 58 различных дат**;
последние две даты по порядку записи — `2026-04-01` и `2026-07-01`; точки `2025-04-01`
(значение `106.7618431956141`) и `2026-04-01` (`104.1602983322909`) существуют **каждая по одной**
со своим значением. Даты/порядок берутся из батча (это то, что уезжает в таблицу), значения — из
`parseVvpKvartal` (`float64`, допуск 1e-6): колонка таблицы Float32, и допуск 1e-6 к ней
неприменим.

Обновлён и `TestVvpKvartalReadsFixture`: в таблицу границ добавлены ряды «9», «10» и «14»
(последний теперь `2012-04-01 … 2026-07-01`), а прежний комментарий про «даты кончаются на
2025-II / 56 различных дат» заменён на ссылку на новый тест — устаревшего описания дефекта в
коде не осталось.

## Fix-round 1: RED → GREEN

### RED (новый тест против прежнего кода — HEAD `882300e`, до правки парсера)

```
$ go test ./rosstat/ -run TestVvpKvartalRepeatedYearLabelAdvances -v -count=1
=== RUN   TestVvpKvartalRepeatedYearLabelAdvances
    vvp_kvartal_test.go:313: различающихся дат 56, ожидалось 58: повторная метка года не выводится
    vvp_kvartal_test.go:318: последние две даты [2025-04-01 2025-07-01], ожидались [2026-04-01 2026-07-01]
    vvp_kvartal_test.go:342: 2025-04-01: значение 104.1602983322909, ожидалось 106.7618431956141
    vvp_kvartal_test.go:346: точек на 2025-04-01 — 2, ожидалась 1
    vvp_kvartal_test.go:346: точек на 2026-04-01 — 0, ожидалась 1
--- FAIL: TestVvpKvartalRepeatedYearLabelAdvances (0.02s)
FAIL
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.710s
```

Падение подтверждает дефект ровно в той форме, что описана в решении: слот `2025-04-01`
занят **двумя** точками, и одна из них — значение 2026-I (`104.16…`), то есть настоящий 2025-I
(`106.76…`) был бы затёрт; `2026-04-01` не существует вовсе.

### GREEN (после правки)

```
$ go test ./rosstat/ -run TestVvpKvartalRepeatedYearLabelAdvances -v -count=1
=== RUN   TestVvpKvartalRepeatedYearLabelAdvances
time="2026-10-14T12:00:11+05:00" level=warning msg="vvpKvartal: rosstat_vvp_kvartal: лист \"14\", колонка 57: метка года \"20252)\" не больше текущего 2025 — год выведен как 2026"
--- PASS: TestVvpKvartalRepeatedYearLabelAdvances (0.02s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.650s
```

(В тексте теста предупреждение печатается дважды: тест дважды прогоняет разбор — через батч и
через `parseVvpKvartal`; колонка 57 — единственный повтор в книге, поэтому warning один на прогон.)

## Fix-round 1: проверка чисел по фикстуре (пункт 3 решения)

Временный тест-дамп (запущен и удалён):

```
всего точек: 302
2 Валовой внутрений продукт                                  points=62 distinct=62 2011-04-01..2026-07-01
9 Валовой внутрений продукт                                  points=62 distinct=62 2011-04-01..2026-07-01
10 Валовой внутренний продукт                                points=62 distinct=62 2011-04-01..2026-07-01
12 Индексы физического объема валового внутреннего продукта  points=58 distinct=58 2012-04-01..2026-07-01
14 Индексы - дефляторы валового внутреннего продукта         points=58 distinct=58 2012-04-01..2026-07-01
  2025-04-01 106.7618431956141   (лист 14 — настоящий 2025-I)
  2026-04-01 104.1602983322909   (лист 14 — выведенный 2026-I)
  2026-07-01 112.4888745408079   (лист 14 — выведенный 2026-II, последняя точка ряда)
```

Итого: 302 точки, распределение 62/62/62/58/58, у листа «14» 58 точек и **58 различных дат**
(было 56), границы ряда — `2012-04-01 … 2026-07-01`; дубликатов дат больше нет ни у одного ряда.

## Fix-round 1: прогнанные команды

| Command | Result |
|---|---|
| `go test ./rosstat/ -count=1 -v -run TestVvpKvartal` | **PASS** — 12 тестов + 10 подтестов (`ReadsFixture`, `KeepsFirstGroup`, `SpotValue`, `SeriesMetaCoversParsedRows`, `MissingSheetFails`, `UnknownQuarterFails`, `FindLink`, `CarriesYearAcrossDataGroups`, `RepeatedYearLabelAdvances`, `SheetErrorBranches`, `GetQuarterDate`, `AppendErrorNotSwallowed`) |
| `go test ./rosstat/ -count=1` | **ok** rosstat 0.519s |
| `go test ./... -count=1` | **ok** — все 20 пакетов с тестами зелёные |
| `go build ./...` | **BUILD_OK** |
| `gofmt -l rosstat` | пусто (чисто) |
| `grep -n "colly\|fmt\.Print\|panic(" rosstat/vvp_kvartal.go` | пусто (exit 1) |
| GoLand MCP `get_file_problems rosstat/vvp_kvartal.go` | ошибок нет; те же два предупреждения среды (CVE-советник на импорте `excelize`, «SQL dialect is not configured» на строке запроса) |

## Fix-round 1: файлы

| File | Change |
|---|---|
| `rosstat/vvp_kvartal.go` | +26/−4: вывод года из структуры при повторной/откатной метке, `log.Warnf`, комментарий-обоснование, импорт logrus |
| `rosstat/vvp_kvartal_test.go` | +63/−12: `TestVvpKvartalRepeatedYearLabelAdvances`, границы рядов «9», «10», «14» в `ReadsFixture`, снятие устаревшего комментария о дефекте |

`deploy/staging/dagu.service` не тронут (предсуществующая правка рабочего дерева, в коммит не
попадает).

## Fix-round 1: замечания

- NaN/Inf-отказ оставлен как есть — согласован с Task 4.
- Проверка «тест падает на прежнем поведении» сделана на настоящем прежнем коде (HEAD `882300e`),
  вывод приведён в секции RED; мутации для (a) обхода с нулевой колонки и (b) переноса года из
  первой итерации остаются в силе.
