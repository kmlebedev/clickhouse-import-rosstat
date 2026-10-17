# Task 1 — отчёт: разбор «Табл 1» + реестр показателей + фикстуры (`cbr_mt_forecast`)

**Статус:** DONE_WITH_CONCERNS (см. §6: один интерфейсный вопрос к Task 2 и три мелких замечания)
**Ветка/дерево:** `feat/cbr-mt-forecast`, `.worktrees/cbr-mt-forecast`; BASE = `3c7cb67`
**Дата:** 2026-10-17

## 1. Что сделано (по шагам брифа)

| Шаг | Результат |
|---|---|
| 1. Фикстуры | `python3 scripts/xlsx_trim.py trim …--keep 'Табл 1'` для обоих выпусков, затем `verify` — **PASS** обе пары (XML листа побайтово совпал с исходником, архив цел, дампы совпали). `cp page_05082026.html` → фикстура страницы (байт-в-байт копия, `grep -c commet_graph_05082026.xlsx` = 2 ≥ 1) |
| 2. Тесты (RED) | `cbr/mt_forecast_parse_test.go` — 16 тестов `TestMtForecast*`: споты двух выпусков, формы значений, двусторонний контракт каталога, fail-fast и warn-ветки, харнесс `appendRecorder` |
| 3. RED | `go test ./cbr/ -run 'MtForecast' -v` → `[build failed]`, `undefined: mtForecastRelease / mtForecastPoint / mtForecastParseTabl1 / mtForecastIndicators …` (слоя разбора и меты ещё нет) |
| 4. Мета | `cbr/mt_forecast_meta.go`: реестр 16 показателей (`Code/Label/Title/Unit/Core`), `mtForecastIndicatorByLabel`, карта описаний, `var cbrMtForecastSeriesMeta = mtForecastMeta()` (source `cbr_mt_forecast`, frequency `A`, origin по спеке §3.7) |
| 5. Разбор | `cbr/mt_forecast_parse.go`: слоистая нормализация, якоря (лист → первая строка-показатель → шапка годов → конец таблицы `^\d+ `), чтение значений (точка/диапазон, оба тире, скобочные отрицательные, надстрочные цифры, приклеенная сноска), fail-fast и warn по спеке §3.3, `mtForecastAppendPoints(release)` как ImportFunc выпуска |
| 6. GREEN | `go test -race -count=1 ./cbr/ -run 'MtForecast'` → **ok** (16/16); весь пакет `go test -race -count=1 ./cbr/` → **ok 63.820s** |
| 7. Коммит | см. §5 |

## 2. TDD Evidence

**RED** — до реализации (после написания только тестов):

```
$ go test ./cbr/ -run 'MtForecast' -v
# github.com/kmlebedev/clickhouse-import-rosstat/cbr [github.com/kmlebedev/clickhouse-import-rosstat/cbr.test]
cbr/mt_forecast_parse_test.go:137:44: undefined: mtForecastRelease
cbr/mt_forecast_parse_test.go:180:58: undefined: mtForecastPoint
cbr/mt_forecast_parse_test.go:183:17: undefined: mtForecastParseTabl1
cbr/mt_forecast_parse_test.go:195:50: undefined: mtForecastPoint
cbr/mt_forecast_parse_test.go:203:9: undefined: mtForecastPoint
cbr/mt_forecast_parse_test.go:209:49: undefined: mtForecastPoint
cbr/mt_forecast_parse_test.go:267:9: undefined: mtForecastIndicators
cbr/mt_forecast_parse_test.go:272:44: too many errors
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/cbr [build failed]
```

Ожидаемо: это ровно ветка «Step 3» брифа/плана — «компиляция не проходит: функций нет»; ни один тест не мог бы пройти, потому что ни `cbr/mt_forecast_parse.go`, ни `cbr/mt_forecast_meta.go` ещё не существовали.

**Промежуточный RED после первой реализации** (фиксация поведения, а не догадок) — три расхождения теста с источником/спекой были исправлены в пользу кода, подтверждённого данными:

```
--- FAIL: TestMtForecastNormalizeCell/узкий_пробел_и_hair_space: mtForecastNormalizeCell("13,3 –\u200a14,0") = "13,3 – 14,0", want "13,3 - 14,0"
--- FAIL: TestMtForecastNormalizeCell/EN_SPACE: mtForecastNormalizeCell("–\u2002к организациям") = "– к организациям", want "- к организациям"
--- FAIL: TestMtForecastUnknownLabelWarns: предупреждений 2, want 1 (незнакомая подпись строки): [...]
--- FAIL: TestMtForecastSeriesMeta: ряд "cpi_dec": origin "…таблица „Табл 1“, строка „…“" не называет таблицу и строку источника
```

Первый и второй — ошибка ожидания в тесте: тире снимает слой **подписи** (спека §3.3, слой 2), базовая нормализация тире не трогает (иначе базовая форма перестала бы быть общей для подписей и значений) — тест приведён в соответствие спеке. Третий — фактическое поведение верно: переименованная строка порождает и warn о незнакомой подписи, и warn о пропавшем из-за этого `m2`; тест теперь ждёт оба сообщения. Четвёртый — в тесте были кавычки-ёлочки вместо „лапок“ шаблона origin спеки §3.7.

**GREEN** — после реализации:

```
$ go test -race -count=1 ./cbr/ -run 'MtForecast'
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	2.452s
$ go test -race -count=1 ./cbr/            # весь пакет, один раз перед коммитом
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	63.820s
```

Тестов 16, все PASS: `TestMtForecast{IndericatorRegistry, NormalizeCell, NormalizeLabel, FixtureRows, SpotValues, ValueForms, ValueErrors, FailFastOnFixture, UnknownLabelWarns, MissingNonCoreWarns, AppendPoints, AppendPointsParseError, AppendPointsAppendError, SeriesMeta, SeriesMetaCoversParsedRows, CoreIndicatorSetsUnionOfFixtures}` (второе имя — `IndicatorRegistry`).

## 3. Что проверено тестами (состав)

- **Фикстуры.** 05082026: 64 точки (16 × 4 года, 2026–2029), 07052026: 48 (16 × 3, 2026–2028); порядок разбора построчный (строки книги сверху вниз × годы по возрастанию) — сверяется полной последовательностью `(indicator, year)`.
- **Споты (литералами из живого файла).** Спековые: `key_rate_avg` 2026=(14.5,14.6) (ячейка `'14,5-14,61'`), 2027=(10.5,12.5), 2029=(7.5,8.5); `cpi_dec` 2026=(6,7), 2028=(4,4) (точка); `gross_capital_formation` 2026=(-3.5,-1.5); `fixed_capital_investment` 2026=(-1.5,0.5); `m2` 2026=(7,12); `bank_claims_mortgage` 2028=(10,15); 07052026: `key_rate_avg` 2026=(14,14.5), 2027=(8,10), `cpi_dec` 2026=(4.5,5.5), `m2` 2026=(5,10). Плюс все 16 строк первого прогнозного года на обоих выпусках (23 + 20 спотов).
- **Формы значений.** Точка/диапазон, дефис + en/em dash, скобочные отрицательные, целые, пробелы вокруг тире, надстрочные `¹`/U+2075, NBSP и хвостовой пробел; ошибки: пустая, «около 5», `lo > hi` («5,0-4,0» — с проверкой, что сообщение называет порядок границ), недописанный диапазон, факт-ячейка «– 2», «4,0 %», одна надстрочная цифра.
- **Fail-fast (мутации копии живой книги в памяти).** нет листа «Табл 1»; лист без строки-показателя; шапка без годов; колонка шапки ни год, ни факт; дубль года; пустая прогнозная ячейка; неразбираемая ячейка; границы в обратном порядке; нет core-строки. Ошибки называют таблицу и **адрес ячейки** (`Табл 1!C8`, `Табл 1!D6`), при ошибке точек не возвращается.
- **Warn-ветки.** Незнакомая подпись строки → warn(строка 19) + warn о пропавшем `m2`, 60 точек, разбор не падает; снятая подпись не-core (`exports`) → ровно одно предупреждение со списком кодов, 60 точек; core-показатель (`cpi_avg`) → ошибка.
- **Двусторонний контракт каталога (§11.4).** `checkCbrSeriesMeta` на 05082026 через `mtForecastBatch` (разбор ⊆ каталог и каталог ⊆ разбор); `cbrMtForecastSeriesMeta` описывает ровно 16 кодов; объединение двух выпусков даёт ровно реестр; каждая разобранная подпись источника (сырые строки с NBSP/EN SPACE/сноской) находится в карте реестра.
- **Харнесс `appendRecorder` (§11.6).** `mtForecastAppendPoints(release)` кладёт 64 строки, порядок колонок DDL `meeting_date, release_date, url, indicator, year, lo, hi`, тип `uint16` в `year`, `float64` в `lo/hi`, атрибуты выпуска в каждой строке; первая строка — `cpi_dec` 2026 (6,7), последняя — `bank_claims_mortgage` 2029 (10,15); ошибка разбора → ни одной записанной строки; сбой `Append` не проглатывается и называет выпуск и точку.
- **Ветка «книга без листа»** и остальные fail-fast — на копии реальной книги (стандарт §11.1), файлы фикстур на диске не менялись.

## 4. Изменённые файлы

Созданы (существующие файлы пакета не правились):

| Файл | sha256 (для фикстур) |
|---|---|
| `cbr/mt_forecast_parse.go` (454 строки) | — |
| `cbr/mt_forecast_meta.go` (175 строк) | — |
| `cbr/mt_forecast_parse_test.go` (895 строк) | — |
| `cbr/testdata/mt_forecast_05082026.xlsx` (16 872 Б) | `94af8ed6b5f708130852fbfb891b91176359d588043cb7168abce4cb992d522c` |
| `cbr/testdata/mt_forecast_07052026.xlsx` (16 781 Б) | `3882a011c48f231cb8d4738a26de5f69568ded826c100b5c7c8d9725b13d2f36` |
| `cbr/testdata/mt_forecast_page_05082026.html` (69 433 Б) | `5681ffc0848f931b9fb91493f37d03996be3a399e6f6d07a154d3c53e42fae58` |

Проверки: `python3 scripts/xlsx_trim.py verify … --keep 'Табл 1'` → **PASS** для обеих пар; `diff -q build/recon-2026-10-17/page_05082026.html cbr/testdata/mt_forecast_page_05082026.html` → идентичны; `gofmt -l` по трём новым файлам — пусто; `go vet ./cbr/` — чисто; `golangci-lint run ./cbr/...` — `0 issues`.

## 5. Коммит

`cbr: mt_forecast parse — Табл 1, meta registry, fixtures` (short SHA — см. ответ контроллеру).

## 6. Находки self-review (что исправлено до отчёта)

1. **Порядок операций в `mtForecastBounds`.** Проверка пустоты выполнялась до снятия надстрочных цифр: ячейка из одной сноски (`'¹'`) давала «значение "" не точка и не диапазон» вместо «пустая прогнозная ячейка». Пустота теперь проверяется после нормализации и снятия надстрочных; добавлены тест-кейсы «только номер сноски» и «только пробелы-двойники».
2. **Мёртвая защита в тесте.** `release, ok := mtForecastFixtureReleases[<const>]; if !ok` — ветка недостижима (ключ константный); приведено к стилю `dip`-тестов (`release := …`).
3. **Комментарии-обманки.** В комментарии к `mtForecastPointRe` приводилась форма `«(0,5)»` как наблюдённая — в источнике скобки стоят только у отрицательных границ диапазонов; формулировка исправлена. В описании `key_rate_avg` фраза «диапазон… он уже» читалась двояко; переписана. В `mtForecastPeerNote` «сверить (unit)» → «сверить по unit».
4. **Валидация каталога.** Проверка `strings.Contains(desc, mtForecastDescriptions[code])` тавтологична при пустом описании (`Contains(x, "")` всегда true) — добавлена явная проверка непустоты карты описаний по каждому коду.
5. **Проверено глазами по excelize v2.11.0** (`rows.go`): пустые ячейки не попадают в `GetRows`, но промежутки заполняются `""` — поэтому индексы колонок стабильны, а «пустая прогнозная ячейка» действительно приходит как `""`, и правило «пустая ячейка шапки — ошибка» не срабатывает на вертикальных merge'ах `A6:A7`, `B6:B7`.

## 7. Сомнения / вопросы (для контроллера и Task 2)

1. **`mtForecastAppendPoints` и `mtForecastRelease` определены в Task 1.** Бриф Task 1 (Step 2) требует теста «`mtForecastAppendPoints(release)` кладёт 64 строки», а план называет эту функцию продуктом Task 2 (`cbr/mt_forecast.go`). Чтобы задача заканчивалась зелёной, функция и тип выпуска объявлены в `cbr/mt_forecast_parse.go`:
   `type mtForecastRelease struct { MeetingDate, ReleaseDate time.Time; URL string }`,
   `func mtForecastAppendPoints(release mtForecastRelease) func(*excelize.File, driver.Batch) error`.
   **Task 2 должна их использовать, а не объявлять заново** (дубль — ошибка компиляции; при необходимости можно дописать поле, напр. `IsPDF`, но не менять порядок колонок строки). Порядок колонок строки: `meeting_date, release_date, url, indicator, year, lo, hi` (спека §3.1).
2. **`cbrAllSeriesMeta()` в `cbr/series_meta_test.go` не расширен** — файл не входит в список Files задачи, а свою мету я проверяю собственными тестами (полнота полей, отсутствие дублей, source/frequency, origin/description). Если Task 2/финальное ревью хочет видеть каталог нового импортёра в пакетном списке — это одна строка в том же PR.
3. **Спека §3.3 упоминает fail-fast «ни одной точки после разбора»** — ветка реализована, но недостижима на живых книгах (если есть первая строка-показатель, шапка и core-строки — точки будут). Оставлена как защита от «0 строк как успешный прогон» (стандарт §6.4), тестом не покрыта — покрыть нечем без синтетической книги (стандарт §11.1 запрещает).
4. **Warn-сообщения печатаются logrus'ом при `go test -v`** (не в обычном прогоне: Go подавляет вывод прошедших тестов). Это поведение существующего пакета — `dipWarnHook`/`TestDipMissingSheetWarns` шумят так же; хук перехватывает сообщения для проверок, но `logrus` пишет и в stderr. Менять глобальный логгер в тестах не стал, чтобы не отходить от прецедента пакета.
5. **GoLand MCP недоступен для worktree.** `mcp__jetbrains__execute_tool` знает только проект `/Users/whitefox/GolandProjects/clickhouse-import-rosstat` (корневой чекаут), а вызовы на файлах worktree возвращают «Unable to determine the target project»; передать `projectPath`/`filePath` через universal-обёртку не удалось (у `execute_tool` нет собственного параметра `projectPath`). Инструмент в наборе есть, но адресовать дерево задачи им нельзя. Компенсация: `gofmt -l` (пусто), `go vet ./cbr/` (чисто), `golangci-lint run ./cbr/...` (0 issues), `go test -race -count=1 ./cbr/` (зелёно). Корневой репозиторий не трогал.

## 8. Замеченные ограничения разбора (названы в спеке, не дефекты)

- Приклеенная цифра сноски снимается **только** у дроби с ровно двумя знаками после разделителя (спека §3.3): границы в таблице печатаются с ≤1 знаком. Страховка — проверка `lo ≤ hi` и спот-тесты обеих фикстур.
- Факт-колонка не импортируется (у неё дрейфует пометка: `'2025 (факт)'`, `'1,0 '` с хвостовым пробелом, `'– 2'`).
- Прогнозные годы берутся из шапки: горизонт 3–4 года и меняется от выпуска к выпуску, поэтому число точек пинится тестом на каждый выпуск (64/48), а не ожидается константой.

---

## Fix round 1 (по ревью Task 1)

**Находка (Important):** `cbr/mt_forecast_meta.go:44–46` — комментарий реестра утверждал, что «коды первых шести совпадают с базовыми именами рядов cbr_mo_br». Это неверно: первые шесть строк книги — `cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, gdp_q4_yoy, consumption_total`, а соответствие задаёт `mtForecastPeerCodes` = `cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, exports, imports` (exports/imports стоят в реестре десятым и одиннадцатым). Подтверждено чтением `cbr/mt_forecast_meta.go:71–73`.

**Что изменено:**

1. `cbr/mt_forecast_meta.go` — комментарий `mtForecastIndicators` переписан и называет **ровно те шесть кодов, что лежат в `mtForecastPeerCodes`**, со ссылкой на карту как на источник истины и с явной оговоркой, что это не «первые шесть» реестра (иначе формулировка снова сползёт):

   > Коды cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, exports, imports совпадают с базовыми именами рядов макроэкономического опроса ЦБ (cbr_mo_br) — см. mtForecastPeerCodes: агент сопоставляет прогноз ЦБ с консенсусом аналитиков по общим именам, а остальные десять кодов такого соответствия не имеют (это не «первые шесть» реестра: gdp_q4_yoy и consumption_total стоят в нём раньше exports/imports).

2. `cbr/mt_forecast_parse_test.go` — добавлен пин-тест `TestMtForecastPeerCodes`, закрывающий **причину** находки (комментарий разошёлся с кодом, потому что контракт не был проверяем): набор соответствий — ровно шесть объявленных кодов (`cpi_dec, cpi_avg, key_rate_avg, gdp_yoy, exports, imports`, литералами), каждый код карты объявлен реестром, и оговорка `mtForecastPeerNote` стоит в описании ровно этих шести рядов каталога — ни у одного другого.

**Прогоны:**

```
$ gofmt -l cbr/mt_forecast_meta.go cbr/mt_forecast_parse_test.go
(пусто)
$ go test -race -count=1 ./cbr/ -run 'MtForecast' -v | grep -E '^(--- (PASS|FAIL)|ok|FAIL)'
--- PASS: TestMtForecastAppendPoints … TestMtForecastPeerCodes (0.00s) … TestMtForecastValueForms
1 ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	2.513s      # 17/17 PASS
$ go test -race -count=1 ./cbr/          # весь пакет
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	60.095s
```

Полных прогонов `MtForecast` — 17 (было 16: добавлен `TestMtForecastPeerCodes`), все PASS; вывод чистый.

**Коммит:** `cbr: mt_forecast — fix peer-code comment (review r1)`.
