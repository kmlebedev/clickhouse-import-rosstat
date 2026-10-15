# Task 2 report — `parseLabourForce` и фикстуры

**Статус:** DONE_WITH_CONCERNS (одно вынужденное отклонение от списка файлов брифа — §6 ниже)
**Коммит:** `dea4826` — `add rosstat labour force parser with fixtures` (8 файлов, `deploy/staging/dagu.service` не тронут)
**Дата:** 2026-10-14, ветка `rosstat-labour-force`, база `65ce9e8`
**Бриф:** `.superpowers/sdd/2026-10-14-rosstat-labour-force/task-2-brief.md`
**Спека:** `docs/superpowers/specs/2026-10-14-rosstat-labour-force-design.md` §2–§3
**Стандарт:** `docs/IMPORTER_PACKAGE_STANDARD.md` §4–§6, §11

---

## 1. Что сделано

| Файл | Действие | Размер | sha256 |
|---|---|---|---|
| `rosstat/labour_force.go` | создан (константы, шесть пинов рядов, `labourForceLabel`, `parseLabourForce`); после fix round 1–2 — 320 строк / 18.2 КБ | 18.7 КБ | — |
| `rosstat/labour_force_test.go` | создан (14 тестов + два хелпера фикстур); после fix round 1–2 — 479 строк / 24.8 КБ | 25.4 КБ | — |
| `rosstat/testdata/labour_force.xlsx` | создан: реальная книга, урезанная zip-хирургией (листы «Содержание» и «5») | 27 615 Б | `ab7b1cc861a29a69913704e42344d5a81cdea14f0e626c34df20e5ba03fe26b2` |
| `rosstat/testdata/labour_force_page.html` | создан: фрагмент реальной страницы, карточка «Занятость и безработица» целиком | 15 489 Б | `3b4c34565ab04dee2f6cfc68476245015b2d97b4aed0eff40f4ec4c9451a07a5` |
| `rosstat/series_meta_test.go` | правка: харнесс `appendRecorder` — `value float64` + `valueType string` | — | — |
| `rosstat/ipc_mes_test.go`, `rosstat/ipc_weeks_test.go`, `rosstat/salaries_mes_test.go` | правка: 7 сравнений переведены на `float64(...)` (компиляция, см. §6) | — | — |

Сетевых вызовов в `labour_force.go` нет: на этой задаче файл — только адресация (таблица, лист,
страница, селектор) и разбор (§6.1). Импортёр, DDL, `series_meta`, реестр, `dagu` — Task 3.

## 2. Фикстура книги (Step 1)

Команды:

```bash
curl -sS -o /tmp/trud1.xlsx "https://rosstat.gov.ru/storage/mediabank/trud-1_15-s.xlsx"
# → HTTP 200, 343 705 байт (343 КБ, 8 листов)
```

Карта книги: «Содержание» = `xl/worksheets/sheet1.xml`, «5» = `xl/worksheets/sheet6.xml`
(`xl/workbook.xml` + `xl/_rels/workbook.xml.rels`).

**Хирургия zip** (python `zipfile`, 18 записей в архиве-результате; `xl/worksheets/sheet6.xml`
не изменялся ни на байт — разметка и значения листа «5» те же, что в источнике):

- `[Content_Types].xml` — убраны `<Override>` листов `sheet2…5,7,8` и рисунков `drawing1…4,6,7`;
- `xl/workbook.xml` — убраны `<sheet>` листов «1»…«4», «6», «7»;
- `xl/_rels/workbook.xml.rels` — убраны шесть `<Relationship>` этих листов (осталось 5: два листа,
  sharedStrings, styles, theme);
- в архив не вошли `sheet2…5,7,8.xml`, их `_rels/`, `drawing1…4,6,7` и лишние printerSettings;
- **две правки, о которых надо знать** (иначе книга невалидна, а не «другая»):
  - удалён блок `<definedNames>`: единственное имя `_xlnm._FilterDatabase` ссылалось на
    `localSheetId=4` — удалённый лист «4»;
  - `activeTab="5"` → `activeTab="1"`: индекс указывал на лист «5» в старой нумерации, в книге
    из двух листов он выходит за диапазон;
  - `docProps/app.xml` приведён в соответствие (было 8 листов — стало 2, `Содержание`, `5`).
- сохранены целиком: `xl/sharedStrings.xml`, `xl/styles.xml` (числовые форматы ячеек листа «5»),
  `xl/theme`, `xl/drawings/drawing5.xml` + `_rels` + `xl/media/image1.png` (ссылка «К содержанию»
  на листе «5»), `printerSettings1/4.bin`, `docProps/core.xml`.

Проверки фикстуры:

```bash
python3 -c "import openpyxl; wb=openpyxl.load_workbook('rosstat/testdata/labour_force.xlsx', data_only=True); print(wb.sheetnames); ws=wb['5']; print(ws.max_row, ws.max_column)"
# → ['Содержание', '5'] / 135 7   (совпало с ожиданием брифа)
```
- `excelize.OpenFile` + `GetRows("5", RawCellValue: true)` — книга открывается, 133 непустые
  строки, шапка 5–6 и значения листа совпадают с полным файлом (сверял spot-значения и общее
  число ячеек-значений: 690);
- python `zipfile.testzip()` → `None`; все `Target` в `.rels` разрешаются в существующие записи
  (0 висячих ссылок).

## 3. Фикстура HTML (Step 2)

```bash
curl -sS -o /tmp/labour_force_page.html "https://rosstat.gov.ru/labour_force?print=1"
# → HTTP 200, 95 306 байт
```

Из страницы вырезан фрагмент — карточка `<div class="list__item">` /
`<div class="toggle-card …>`, строки 314–616 исходника (13 786 байт), целиком, вместе с
вложенными списками; классы и структура ссылок не менялись. Обёртка — минимальный
`<!DOCTYPE html><html><body>…` плюс комментарий-провенанс (URL, дата, размер). В фикстуре обе
нужные ссылки, порядок как в источнике:

```
26: <a class="btn btn-icon btn-white btn-br btn-sm" href="/storage/mediabank/trud-1_15-s.xlsx">
39: <a class="btn btn-icon btn-white btn-br btn-sm" href="/storage/mediabank/trud_1_15-72.xlsx">
```

Проверка селектора `a[href$="trud-1_15-s.xlsx"]` на этой фикстуре даёт
`/storage/mediabank/trud-1_15-s.xlsx` (а не соседний файл с подчёркиванием) — закреплено тестом.

Для ветки «страницы без ссылки» взята существующая реальная HTML-фикстура
`testdata/salaries_page.html` (страница рынка труда и зарплат Росстата): ссылки на
`trud-1_15-s.xlsx` там нет, `util.FindLink` возвращает ошибку.

## 4. Тесты (Step 3, Step 7)

Исходные девять тестов задачи (после fix round 1–2 их 14; добавленные — в разделах «Fix round 1» и
«Fix round 2»): `go test -race -count=1 ./rosstat/ -run 'LabourForce' -v` — **9 PASS**:

| Тест | Что проверяет |
|---|---|
| `TestLabourForceReadsFixture` | 690 точек, шесть различных рядов (по 115 точек), крайние даты 2017-01-31 … 2026-07-31 |
| `TestLabourForceSpotValues` | B8 `Рабочая сила` 2017-01-31 = 76489.193; G8 `Уровень безработицы` 2017-01-31 = 5.6; B131 `Рабочая сила` 2026-07-31 = 76389.482; F131 `Уровень занятости` = 61.5; G131 `Уровень безработицы` = 2.3 |
| `TestLabourForceFootnoteLabels` | `январь1)`→`январь`, `октябрь1)2)`→`октябрь`, `Уровень занятости, \nв %`→`Уровень занятости, в %`, `2017` не меняется |
| `TestLabourForceMissingSheetFails` | копия фикстуры с `DeleteSheet("5")` → ошибка, текст несёт `rosstat_labour_force_mes` и `5`; точки `nil` |
| `TestLabourForceMissingHeaderFails` | копия с очищенной B5 → ошибка с именем листа |
| `TestLabourForceNonNumericCellFails` | копия с `B8 = "н/д"` → ошибка «значение ячейки … не число», без пропуска |
| `TestLabourForceEmptyCellIsNotPoint` | копия с очищенными B131:G131 → ровно 690−6 = 684 точки, нулевых точек нет, точки за 2026-07 нет |
| `TestLabourForceIgnoresTrailingFootnoteRows` | строки 132–133 (текст сносок) разбор замыкают: число точек равно «число строк-месяцев книги × 6», точек позже 2026-07-31 нет |
| `TestLabourForceFindLink` | `util.FindLink` по HTML-фикстуре → `/storage/mediabank/trud-1_15-s.xlsx`; на странице без ссылки — ошибка; пин `labourForcePage` |

Книги-мутанты собираются **из копии реальной фикстуры** (решение владельца №1, R2): `os.ReadFile`
→ `t.TempDir()` → `excelize.OpenFile` → `DeleteSheet`/`SetCellValue` → `Save()` → открытие
сохранённой копии → разбор. Синтетическая `newTestWorkbook` не использована нигде.

Step 4 подтверждён до реализации: `go test -race ./rosstat/ -run 'LabourForce' -v` →
build failed, `undefined: labourForcePoint / parseLabourForce / labourForceLabel / labourForceSheet /
rosstatLabourForceTable`.

## 5. Харнесс `appendRecorder` (Step 5)

`appendedRow.value` — `float64`, добавлено `valueType string` (`fmt.Sprintf("%T", v[2])`),
`Append` принимает `float32` и `float64`, иначе — ошибка. `go test -race -count=1 ./rosstat/`
— **ok**, весь пакет зелёный (существующие тесты не поменяли смысла проверок, см. §6).

Оговорка по порядку шагов брифа: «Step 5 → `go test ./rosstat/` без правок» в этом порядке
недостижим — после Step 3 пакет не собирается (тесты ссылаются на ещё не написанный разбор), то
есть Step 5 в исходной последовательности всегда видит красную сборку. Фактически харнесс
проверен полным прогоном пакета после Step 6 (и по отдельности — компиляцией `go vet ./rosstat/`).

## 6. Вынужденное отклонение от списка файлов брифа (concern)

Бриф называет правкой только `rosstat/series_meta_test.go` и требует положить в индекс ровно
перечисленные файлы. Расширение харнесса до `float64` **ломает компиляцию** трёх существующих
тестовых файлов, потому что там ожидания — типизированные `float32` (`invalid operation:
row.value != want (mismatched types float64 and float32)`), то есть заявление плана «расширение
типа обратно совместимо» неверно. Оставить эти файлы нетронутыми и получить зелёный пакет
невозможно.

Правки минимальны и семантику не меняют (значение по-прежнему сравнивается с тем же числом
после float32-округления записи):

| Файл:строка | Было | Стало |
|---|---|---|
| `ipc_mes_test.go:128` | `row.value == float32(2608.84)` | `row.value == float64(float32(2608.84))` |
| `ipc_mes_test.go:152` | `row.value != want` | `row.value != float64(want)` |
| `ipc_weeks_test.go:219` | `row.value != want` | `row.value != float64(want)` |
| `ipc_weeks_test.go:358` | `got != float32(100.126)` | `got != float64(float32(100.126))` |
| `ipc_weeks_test.go:388` | `row.value != wantValue[i]` | `row.value != float64(wantValue[i])` |
| `salaries_mes_test.go:86,118` | поле `value float32` | поле `value float64` (ожидания — целые: 1, 114743, 62470, 63060, 88468) |

Итого в коммит уходят восемь файлов: пять из брифа плюс эти три. Альтернатива (оставить
`appendedRow.value` как `float32`) нарушила бы интерфейс Task 2 → Task 3 (`valueType == "float64"`
пинит схему новой таблицы) и не дала бы отличить `Float64`-запись от legacy `Float32`.

## 7. Решения по разбору (сверх буквы брифа)

- **Строка-«год»** распознаётся как четырёхзначная подпись первой ячейки, остальные ячейки строки
  пусты (`labourForceYear`). Строка, чья подпись не год и не месяц, замыкает таблицу (текст
  сносок) — как в Step 5 брифа; **устарело после fix round 1–2:** изначально это замыкание было
  молчаливым (незнакомая подпись просто останавливала разбор), теперь оно проверяется — строка с
  нераспознанной подписью, несущая значения, и данные ниже неё дают ошибку разбора (см. «Fix
  round 1» и «Fix round 2»). Строки сносок 132–133 (значения только в колонке A) замыкание
  проходят без ошибки.
- **Шапка — две строки**: ищем якоря в найденной строке и строке под ней; нижняя граница шапки —
  самая нижняя строка, где нашёлся якорь (не константа «+1»), данные начинаются под ней.
- **Якорь дважды** (в разных колонках) — ошибка с именем якоря и номерами колонок (§6.4).
- **Месяц до строки-года** и **неубывающий месяц внутри года** — ошибки (спека §3.6).
- **Пустая ячейка** — пропуск (§6.7), выход за границу строки (`GetRows` обрезает хвостовые
  пустые ячейки) обрабатывается как пустая ячейка, а не паника (§6.5).
- **Дата точки** — `time.Date(year, month+1, 0, …, time.UTC)` = последний день отчётного месяца
  (спека §3.4). Ветка «дата в будущем» не проверяется: у последнего месяца выгрузки значения есть.
- **`labourForcePage`** до Task 3 не имеет потребителя в коде, а golangci-lint `unused` валит
  сборку. Разрешение: константа пинится литералом в `TestLabourForceFindLink` (адрес страницы, с
  которой снята HTML-фикстура) — сравнение с литералом, а не с самой собой (§8.5).

## 8. Проверки

| Команда | Результат |
|---|---|
| `go test -race -count=1 ./rosstat/ -run 'LabourForce' -v` | 9 PASS |
| `go test -race -count=1 ./rosstat/` | ok (5.0 s), весь пакет |
| `go test -race -count=1 ./...` | exit 0, падений нет (весь репозиторий, ~55 s) |
| `gofmt -l .` | пусто |
| `go vet ./...` | пусто |
| `golangci-lint run ./rosstat/...` | 0 issues |
| `go build ./...` | ok |
| GoLand MCP `get_file_problems` по 6 файлам (`labour_force.go`, `labour_force_test.go`, `series_meta_test.go`, `ipc_mes_test.go`, `ipc_weeks_test.go`, `salaries_mes_test.go`) | errors: [] у всех |

Сеть в тестах не используется; новых зависимостей в `go.mod` нет; `deploy/staging/dagu.service`
не тронут и в индекс не попал.

## 9. Что осталось (Task 3 и далее)

- `rosstat/labour_force.go`: `labourForceDdl`, `labourForceCountPoints`, `labourForceDataUrl()`
  (потребитель `labourForcePage` и `labourForceLinkSelector`), `labourForceImport` (append
  `float64`), `LabourForceStat.Import`, `labourForceImporter`.
- `rosstat/series_meta.go`: шесть записей `labourForceSeriesMeta` (имена обязаны совпасть с
  `labourForceSeriesPins[].name` дословно) + `rosstatMacroView`.
- `rosstat/labour_force_test.go`: двусторонний контракт каталога, `valueType == "float64"`,
  ошибка `Append`; реестр `importers.go` + `importers_test.go`, шаг `dagu/rosstat.yaml`,
  `scripts/mcp_check.py`, документация.
- Замечание для Task 3: фикстура фиксирует **живой** срез 2026-10-14 (690 точек, последний месяц
  2026-07); при обновлении источника числа в тестах придётся обновлять вместе с фикстурой.

**Concern (один):** список файлов коммита шире брифа на три тестовых файла (§6) — вынужденно, из-за
неверного утверждения плана об обратной совместимости харнесса.

---

## Fix round 1

Коммит-база — `dea4826`; ревью нашло три вещи (F1–F3), контроллер снял Step 6 п. 5 брифа как
неверный. Правки — только `rosstat/labour_force.go` и `rosstat/labour_force_test.go`; чужие
тестовые файлы, фикстуры и остальные тесты не тронуты.

### F1. Молчаливый обрыв разбора → fail-fast

**Было.** Строка с нераспознанной подписью (`break`) и строка с пустой подписью при заполненных
значениях (`continue`) отдавали усечённый ряд без ошибки: опечатка «феврал» в A9 — 6 точек и
`err=nil`, пустая A9 — 684 точки и `err=nil`.

**Стало** (`labourForceFilledCell` + проверка под циклом; `tableEnd` вместо `break` «насовсем»):

- строка с нераспознанной подписью по-прежнему замыкает таблицу, но **замыкание проверяется**:
  если ниже в колонках шести рядов есть непустая ячейка — ошибка с таблицей, листом, номером
  строки с подписью, номером строки, где продолжаются данные, самой подписью и значением;
- строка с **пустой** подписью, у которой в колонках шести рядов есть значения, — ошибка
  («подпись месяца пуста, а ячейка … заполнена»); пустая строка-разделитель без значений
  (`continue`) ошибкой не является;
- строка с пустой подписью проверяется **до** строки-года и месяца, поэтому потеря подписи
  ловится и там, где подпись года/месяца тоже испорчена.

Комментарий в `parseLabourForce` и в теле цикла поправлен: «незнакомая подпись замыкает таблицу»
→ «замыкает с проверкой, что ниже нет данных».

Воспроизведение ревьюера, после фикса (`A9` копии реальной фикстуры):

```
A9="феврал": points=0 err=rosstat_labour_force_mes: лист "5", строка 10: данные продолжаются ниже строки 9, подпись "феврал" не распознана (значение "76395.993000000002")
A9="":       points=0 err=rosstat_labour_force_mes: лист "5", строка 9: подпись месяца пуста, а ячейка "76008.773000000001" (колонка 2) заполнена
```

Живая фикстура не пострадала: строки 132–133 (текст сносок, значения только в колонке A) разбор
по-прежнему замыкают, точек — **690** (`TestLabourForceReadsFixture`).
Принятая плата за строгость: замыкание считает данными любую непустую ячейку в колонках B–G, а не
только числовую (формулировки F1 расходятся: для строки без подписи — «непустые значения», для
остатка листа — «непустое числовое значение»). Проза в колонках значений под таблицей даст
громкую ошибку с цитатой ячейки, а не тихую потерю; правка — одна ветка, если источник начнёт
печатать там текст.

Новые тесты: `TestLabourForceMisspelledMonthFails` (`A9="феврал"`), `TestLabourForceLostMonthLabelFails`
(`A9=""`); оба ждут ошибку с именем таблицы, листа, подписью и «не распознана»/«подпись месяца
пуста».

### F2. Ожидания — литералами

`TestLabourForceIgnoresTrailingFootnoteRows` больше не выводит ожидание из `util.MonthsToNum`:
литералы `wantMonths = 115`, `wantPoints = 690`, а число строк-месяцев книги считается отдельно и
сверяется с `115` — пропавший месяц уронит тест, а не пересчитается вместе с ним.
`TestLabourForceEmptyCellIsNotPoint` вместо `len(parseLabourForceFixture(t))` использует литерал
`const baseline = 690` (ожидание 684 = 690 − 6 очищенных ячеек). Литеральные 690, 115, шесть
рядов и крайние даты `2017-01-31`/`2026-07-31` уже были в `TestLabourForceReadsFixture`.

### F3. Тест на пустой результат

`TestLabourForceEmptyParseFails`: копия реальной фикстуры в `t.TempDir()`, у всех строк с
подписью месяца (115 строк; подписи остаются на месте) очищены ячейки шести колонок значений →
ожидание: ошибка, в тексте `rosstat_labour_force_mes`, имя листа `5` и «разбор дал 0 точек».

### Прогоны после правок

```
$ go test -race -count=1 ./rosstat/ -run LabourForce -v
--- PASS: TestLabourForceReadsFixture
--- PASS: TestLabourForceSpotValues
--- PASS: TestLabourForceFootnoteLabels
--- PASS: TestLabourForceMissingSheetFails
--- PASS: TestLabourForceMissingHeaderFails
--- PASS: TestLabourForceNonNumericCellFails
--- PASS: TestLabourForceEmptyCellIsNotPoint
--- PASS: TestLabourForceMisspelledMonthFails
--- PASS: TestLabourForceLostMonthLabelFails
--- PASS: TestLabourForceEmptyParseFails
--- PASS: TestLabourForceIgnoresTrailingFootnoteRows
--- PASS: TestLabourForceFindLink
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	2.760s

$ go test -race -count=1 ./rosstat/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	5.335s

$ gofmt -l rosstat/ ; go vet ./rosstat/ ; golangci-lint run ./rosstat/...
0 issues.
```

GoLand MCP `get_file_problems` по `rosstat/labour_force.go` и `rosstat/labour_force_test.go` —
`errors: []` у обоих (дополнительно прогнан до правок по остальным правленым файлам, там тоже
пусто). Точек на живой фикстуре — те же **690**.

---

## Fix round 2

Коммит-база — `2ff021c`. Ревьюер воспроизвёл остаток F1: проверка замыкания начиналась с
`tableEnd + 1` и **саму строку-замыкание не смотрела**. `SetCellValue("A131", "xxx")` при
заполненных `B131:G131` давал 684 точки и `err=nil` — июль 2026 исчезал молча (тот же дефект, что
и в F1, но в последней строке таблицы).

### Правка `rosstat/labour_force.go`

- цикл проверки идёт с `tableEnd`, а не с `tableEnd + 1`;
- два случая разведены:
  - `i == tableEnd` — «подпись %q не распознана, а значение %q заполнено» (с таблицей, листом,
    номером строки и колонки): строка-замыкание сама несёт данные;
  - `i > tableEnd` — прежнее «данные продолжаются ниже строки %d, подпись %q не распознана
    (значение %q)»;
- метка строки-замыкания (`closingLabel`) вычисляется до цикла;
- поправлены комментарии — и в теле цикла, и в doc-комментарии `parseLabourForce` (замыкание
  проверяет саму строку и всё, что ниже).

Воспроизведение ревьюера после правки:

```
A131="xxx": points=0 err=rosstat_labour_force_mes: лист "5", строка 131, колонка 2: подпись "xxx" не распознана, а значение "76389.482000000004" заполнено
```

### Тесты

- `TestLabourForceMisspelledMonthFails` — ожидание `"строка 10"` → `"строка 9"`: ошибка теперь
  называет саму строку-замыкание (её значения заполнены), а не строку ниже;
- **новый** `TestLabourForceGarbageLabelOnLastMonthFails` — копия фикстуры, `A131 = "xxx"` при
  заполненных значениях → ошибка с упоминанием строки **131**, таблицы, листа и мусорной подписи;
- **новый** `TestLabourForceUnparsedLabelWithDataBelowFails` — вторая ветка проверки: строка с
  нераспознанной подписью **без** значений (`A9 = "феврал"`, `B9:G9` очищены), данные идут ниже →
  ошибка «данные продолжаются ниже строки 9 … строка 10». Тест добавлен сверх списка правок:
  после смены точки отсчёта прежний тест на «феврал» ушёл в ветку `i == tableEnd`, и ветка
  `i > tableEnd` осталась бы непокрытой (ровно тот класс замечания, что и F3).

### Прогоны после правок

```
$ go test -race -count=1 ./rosstat/ -run LabourForce -v
--- PASS: TestLabourForceReadsFixture
--- PASS: TestLabourForceSpotValues
--- PASS: TestLabourForceFootnoteLabels
--- PASS: TestLabourForceMissingSheetFails
--- PASS: TestLabourForceMissingHeaderFails
--- PASS: TestLabourForceNonNumericCellFails
--- PASS: TestLabourForceEmptyCellIsNotPoint
--- PASS: TestLabourForceMisspelledMonthFails
--- PASS: TestLabourForceGarbageLabelOnLastMonthFails
--- PASS: TestLabourForceUnparsedLabelWithDataBelowFails
--- PASS: TestLabourForceLostMonthLabelFails
--- PASS: TestLabourForceEmptyParseFails
--- PASS: TestLabourForceIgnoresTrailingFootnoteRows
--- PASS: TestLabourForceFindLink
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	3.082s

$ go test -race -count=1 ./rosstat/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	5.493s

$ gofmt -l rosstat/ ; go vet ./rosstat/ ; golangci-lint run ./rosstat/...
0 issues.
```

GoLand MCP `get_file_problems`: `rosstat/labour_force.go` — `errors: []`,
`rosstat/labour_force_test.go` — `errors: []`.

Живая фикстура: **690 точек**, 115 строк-месяцев, строки сносок 132–133 разбор по-прежнему
замыкают без ошибки (`TestLabourForceReadsFixture`, `TestLabourForceIgnoresTrailingFootnoteRows`).
Осознанные решения предыдущего раунда сохранены: критерий «данными считается любая непустая ячейка,
а не только числовая» и «незнакомая подпись без данных ниже ошибкой не является».
