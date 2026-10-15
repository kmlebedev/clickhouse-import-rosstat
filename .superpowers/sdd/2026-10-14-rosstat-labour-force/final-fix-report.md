# Финальная фикс-волна — `rosstat_labour_force_mes`

Ветка: `rosstat-labour-force`. Вердикт ревью: «Ready to merge: with fixes». Волна одна,
все находки закрыты одним коммитом. Инструменты: GoLand MCP (`get_file_problems`) и
ClickHouse MCP (`run_query`, read-only) — доступны и использованы. Субагенты не запускались.

## Что исправлено по находкам

### I1 (Important) — проверка последовательности месяцев и лет

`rosstat/labour_force.go`, `parseLabourForce`:

- **Ветка строки-секции года** (перед `year, month = sectionYear, 0`): год обязан быть строго
  больше предыдущего.
  `if year != 0 && sectionYear <= year { … }` — ошибка «год %d идёт после года %d — годы
  обязаны идти по возрастанию».
- **Ветка строки месяца**: прежняя `sectionMonth <= month` заменена на `sectionMonth != month+1`
  — месяц обязан быть следующим за предыдущим. Строка-секция года по-прежнему сбрасывает
  `month` в 0, поэтому январь (= 1) проходит законно; последний неполный год остаётся законным
  (проверки «12 месяцев в году» нет). Ошибка: «месяц %q не следует за месяцем %q — месяцы
  обязаны идти по возрастанию».

Проверка `year == 0` («месяц до строки-года») осталась **до** проверки последовательности, так
что сообщение о потерянном январе не подменяется сообщением о последовательности.

**Фактические тексты новых ошибок** (сняты прогоном на копиях реальной фикстуры):

- удалена строка месяца (`RemoveRow("5", 9)`, февраль 2017):
  `rosstat_labour_force_mes: лист "5", строка 9: месяц "март" не следует за месяцем "январь" — месяцы обязаны идти по возрастанию`
- подменена подпись года (`SetCellValue("A124", "2025")`):
  `rosstat_labour_force_mes: лист "5", строка 124: год 2025 идёт после года 2025 — годы обязаны идти по возрастанию`

До правки оба случая давали `err=nil` (684 и 690 точек соответственно) — теперь `points == nil`
и громкая ошибка с таблицей, листом, номером строки и подписями.

**Два новых теста** (`rosstat/labour_force_test.go`, оба на копиях реальной фикстуры в
`t.TempDir()` через уже принятый `mutateLabourForceFixture` — не синтетические книги):

- `TestLabourForceRemovedMonthRowFails` — `RemoveRow("5", 9)`; проверяет ошибку, `points == nil`,
  строку 9, фразу `месяц "март" не следует за месяцем "январь"` и отсутствие английских
  `March`/`January`;
- `TestLabourForceMismatchedYearSectionFails` — `SetCellValue("A124", "2025")`; проверяет ошибку,
  `points == nil`, строку 124 и фразу `год 2025 идёт после года 2025`.

Все существующие тесты пакета остались зелёными.

### Minor-1 — документ обещал «десять cbr/ и четыре вне него»

После ветки объявлений **15** (проверено грепом `PublicationCountPoints:`: 10 в `cbr/` и 5 вне
него — `rosstat/ipc_mes`, `rosstat/salaries_mes`, `rosstat/vvp_kvartal`,
`rosstat/labour_force_mes`, `bank/sber_fin_rez`). §8 п. 13 уже говорил «15 импортёров, пять вне
него» — правил только две отставшие канонические копии:

- `ARCHITECTURE.md` §6.2 (строка 393, комментарий DDL `source_publications`):
  «десять импортёров cbr/ и четыре вне него» → «пятнадцать импортёров: десять в cbr/ и пять вне него»;
- `util/publications.go` (комментарий `PublicationsTable`, строка 21–22): та же правка.

`docs/ROADMAP_KEY_RATE.md` не тронут — там это запись о прошлом шаге (историю не переписываем).

### Minor-2 — месяц печатался по-английски

Добавлены `labourForceMonthNames` (12 русских подписей) и `labourForceMonthName(month)`
(`rosstat/labour_force.go`); месяц вне 1–12 (0 — до первого месяца года) печатается числом.
Использованы во **всех** местах разбора, где месяц попадает в сообщение:

- переписанная ошибка последовательности месяцев (бывший `месяц "январь" идёт после месяца "January"`);
- новых ошибках I1.

Текст `месяц %q до строки-года` печатал подпись строки (русскую) и не менялся. В
`TestLabourForceRemovedMonthRowFails` добавлена проверка отсутствия `March`/`January` в тексте.

### Minor-4 — spot-значения трёх рядов из шести

`TestLabourForceSpotValues` пинил 3 ряда из 6. Добавлены значения за `2026-07-31` (ячейки
`C131`/`D131`/`E131`, сверены с ревьюером и с фикстурой):

- `Занятые` → `74660.473`
- `Безработные` → `1729.009`
- `Уровень участия в составе рабочей силы` → `62.9`

### Minor-5 — тест объявления сравнивал константу с самой собой

`TestLabourForcePublicationDeclaration` теперь сравнивает `pub.CountPoints` с **литералом**
`"SELECT count() FROM rosstat_labour_force_mes FINAL"` (а не с `labourForceCountPoints`).
Настоящая защита объявления — литерал закреплён здесь и в колонке `countPoints` реестра
(`rosstat/importers.go`), которую сверяет энфорсер `TestRosstatImportersPrefixedAndRegistered`.

### Minor-7 — README не называл импортёр в перечне «берут ссылку со страницы»

`README.md` (~строка 483): добавлен `rosstat_labour_force_mes` в перечень (рядом с остальными
`rosstat_*`).

## Не тронуто (отложено контроллером)

Тесты на ветки «дубль якоря в шапке», «нет колонки ряда», «месяц до строки-года», NaN/Inf и знак
в годе; скрипт урезания фикстуры; warn о незнакомой колонке шапки. `deploy/staging/dagu.service`
(чужая незакоммиченная правка) не тронут и в индекс не добавлен.

## Проверки

### `make all` — зелёный

```
0 issues.                                  # golangci-lint (gofmt-проверка перед ним — без замечаний)
go vet ./...                               # чисто
go test -race ./...
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	5.736s
ok  	…/cbr …/util …/views …/gold … (остальные пакеты ok / cached, [no test files] у пакетов без тестов)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build … -o build/clickhouse-import-rosstat .
go build -o build/ingest ./cmd/ingest
```

### `go test -race -count=1 ./rosstat/` без кэша — зелёный

```
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	6.010s
```

Отдельно прогнаны целевые тесты (`-v`): `TestLabourForceReadsFixture`, `TestLabourForceSpotValues`,
`TestLabourForceRemovedMonthRowFails`, `TestLabourForceMismatchedYearSectionFails`,
`TestLabourForceIgnoresTrailingFootnoteRows`, `TestLabourForcePublicationDeclaration` — все PASS.

### Инвариант фикстуры: 690 точек / 115 месяцев

`TestLabourForceReadsFixture` (реальная фикстура, 6 рядов × 115 месяцев, крайние даты
`2017-01-31` … `2026-07-31`) — PASS; `TestLabourForceIgnoresTrailingFootnoteRows` (115 строк-месяцев
в книге против 690 точек) — PASS. Правка I1 разбор не урезала.

Дополнительно, read-only через ClickHouse MCP (`v_rosstat_macro`; сырая
`rosstat_labour_force_mes` агенту не выдана): у всех шести рядов `points = 115`,
`months = 115`, `2017-01-31` … `2026-07-31` — месячных дыр нет.

### GoLand `get_file_problems`

Проверены `rosstat/labour_force.go` и `rosstat/labour_force_test.go` — правки новых проблем не
внесли. Остаются только предсуществующие: `WARNING SQL dialect is not configured` (на строке
SQL-константы чека, и на новом литерале в тесте — та же инспекция) и `WEAK WARNING Vulnerable
dependency usage` (CVE в `excelize`, зависимость не трогалась). Ошибок нет. У второго файла
дополнительно предсуществующий `WARNING Condition 'labourForcePage != "…"' is always 'false'` —
это намеренная сверка константы с литералом в `findLink`-тесте, не связана с волной.

## Замечания

- Коммит один: `fix: check month and year sequence in labour force parser` (5 файлов:
  `rosstat/labour_force.go`, `rosstat/labour_force_test.go`, `util/publications.go`,
  `ARCHITECTURE.md`, `README.md`).
- Отчёт лежит в игнорируемом `.superpowers/sdd/…` (`.superpowers/sdd/.gitignore` → `*`) и в коммит
  не входит.
- Проверка последовательности строже прежней: повтор/понижение месяца теперь тоже падает (их
  раньше ловила `sectionMonth <= month`) — поведение сохранено, сообщение унифицировано.

## Fix wave round 2 — F2 (остаток I1)

Scoped-ревью подтвердило Minor-1/2/4/5/7 закрытыми, но I1 закрыт **частично**: появились проверки
последовательности лет и месяцев, однако секция нового года не проверяла, что предыдущий год
**закрыт декабрём**. Потеря декабря 2025 (строка 123) давала 684 точки и `err=nil` — так же тихо,
как удалённый месяц: префлайт записал бы `points=684` и импортёр ушёл бы в вечный no-op.

### Правка

`rosstat/labour_force.go`, ветка строки-секции года — **перед** `year, month = sectionYear, 0`
(после проверки «год идёт по возрастанию»):

```go
if year != 0 && month != 12 {
    return nil, fmt.Errorf("%s: лист %q, строка %d: год %d не закончен: последний месяц %q, ожидался %q",
        rosstatLabourForceTable, labourForceSheet, i+1, year, labourForceMonthName(month), labourForceMonthName(12))
}
```

Фактический текст ошибки (снят прогоном на копии фикстуры; удалена строка 123 — декабрь 2025):

`rosstat_labour_force_mes: лист "5", строка 123: год 2025 не закончен: последний месяц "ноябрь", ожидался "декабрь"`

Инварианты сохранены:

- первый год проверкой не задет (`year == 0`);
- неполный **последний** год законен — секции следующего года за ним нет (январь–июль 2026 без
  декабря проходит);
- пропуск целой секции года разрешён по Ruling R6 — проверки `sectionYear == year+1` нет.

### Тест

`TestLabourForceRemovedDecemberRowFails` (`rosstat/labour_force_test.go`) — на копии реальной
фикстуры в `t.TempDir()` через существующий `mutateLabourForceFixture`: `RemoveRow("5", 123)`
(декабрь 2025). Проверяет ошибку, `points == nil`, таблицу/лист, `строка 123`,
`год 2025 не закончен`, `декабрь` и отсутствие английского `December`.

**Проверка «падает без правки»** — в копии репозитория в `/tmp/lf-verify` (рабочее дерево не
трогалось) блок с проверкой декабря удалён, прогон дал:

```
=== RUN   TestLabourForceRemovedDecemberRowFails
    labour_force_test.go:446: потеря строки декабря разобрана без ошибки: 684 точек вместо ошибки
--- FAIL: TestLabourForceRemovedDecemberRowFails (0.02s)
```

— ровно воспроизведение ревьюера (684 точки, `err=nil`). Копия удалена.

### Проверки round 2

- `make all` — зелёный: golangci-lint `0 issues.`, `go vet ./...` чисто, `go test -race ./...`
  (`ok …/rosstat 6.133s`, остальные ok/cached), обе сборки.
- `go test -race -count=1 ./rosstat/` без кэша → `ok …/rosstat 5.772s`.
- Инвариант фикстуры: `TestLabourForceReadsFixture` (690 точек, 6×115, `2017-01-31` … `2026-07-31`)
  и `TestLabourForceIgnoresTrailingFootnoteRows` — PASS. Дополнительно read-only через ClickHouse
  MCP: `v_rosstat_macro` по шести рядам — `points=690, months=115, series=6, 2017-01-31 … 2026-07-31`.
- GoLand `get_file_problems --errorsOnly true` по `rosstat/labour_force.go` и
  `rosstat/labour_force_test.go` — `errors: []`. С `errorsOnly false` — только предсуществующие
  (SQL-dialect inspection на строке чека, CVE-адвизори `excelize`, намеренная сверка константы с
  литералом в `findLink`-тесте); новых нет.

Коммит round 2: `4a05425` `fix: require december to close each labour force year` — только
`rosstat/labour_force.go` и `rosstat/labour_force_test.go`.
