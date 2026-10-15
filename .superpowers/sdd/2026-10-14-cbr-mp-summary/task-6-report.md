# Task 6 — отчёт: импортёр `cbr_mp_summary` + проводка (реестр, dagu, гранты, MCP, витрины)

**Status: DONE**
**Коммит:** `3d456ae` — `cbr: cbr_mp_summary importer with channel tagging, views and wiring`
(ветка `feat/cbr-mp-summary`, BASE `02755b9`; 7 files changed, 872 insertions(+), 2 deletions(-)).
Коммит один, сообщение — дословно из брифа; после коммита один раз выполнен `--amend --no-edit`
(правка диагностики в тесте, см. §4, SHA `1baf2aa` → `3d456ae`, родитель не менялся).

**Файлы (ровно семь из брифа):** `cbr/mp_summary.go` (596, новый), `cbr/mp_summary_test.go` (212, новый),
`cbr/importers.go` (+8/−2), `cbr/importers_test.go` (+11/−1), `dagu/cbr.yaml` (+8),
`sql/mcp_kimi_reader.sql` (+3), `scripts/mcp_check.py` (+36).
`deploy/staging/dagu.service` не трогался и в индекс не попадал (`git status --short` до и после
`git add`: ` M deploy/staging/dagu.service` остаётся единственной записью вне коммита).

---

## 1. Гейты (все — на закоммиченном состоянии)

```
$ go test ./cbr/ -count=1                 → ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  1.784s
$ go build ./...                          → ok
$ gofmt -l cbr/                           → (пусто)
$ golangci-lint run ./cbr/...             → 0 issues
$ grep -c "  - id:" dagu/cbr.yaml         → 15
$ dagu validate dagu/cbr.yaml             → exit 0
$ python3 -m py_compile scripts/mcp_check.py → ok
$ go test ./... -count=1                  → 19 пакетов ok, строк FAIL — 0
```

MCP (JetBrains): `get_file_problems` по `cbr/mp_summary.go` → только IDE-предупреждения
«SQL dialect is not configured» на SQL-литералах (та же инспекция молчит на них во всех DDL
пакета: `mo_br.go`, `mp_survey.go`), ошибок нет; по `cbr/mp_summary_test.go` → `{"errors":[]}`.
MCP (ClickHouse, read-only): три DDL прогнаны через `EXPLAIN SYNTAX` — все три разбираются
ClickHouse без ошибок, `ORDER BY tuple(meeting_date, block, subsection, kind, seq)` и
`ORDER BY tuple(meeting_date, channel, kind, method, model, prompt_version, seq)`, как в спеке.
Живых вставок нет: импорт в ClickHouse — приёмка T9.

**Копии DDL сверены со спекой машинно:** питон-скрипт извлёк три оператора `CREATE TABLE` из
`docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md` §3.3 и сравнил с Go-литералами
по-байтово → `cbr_mp_summary IDENTICAL`, `cbr_mp_summary_block IDENTICAL`,
`cbr_mp_summary_channel IDENTICAL`.

---

## 2. Что произведено (интерфейс для T9/документации)

```go
const mpSummaryIndexUrl = "https://www.cbr.ru/dkp/mp_dec/decision_key_rate/"
const mpSummaryName         = "cbr_mp_summary"          // Name() == TableName
const mpSummaryTable        = "cbr_mp_summary"
const mpSummaryBlockTable   = "cbr_mp_summary_block"
const mpSummaryChannelTable = "cbr_mp_summary_channel"
const mpSummaryDdl, mpSummaryBlockDdl, mpSummaryChannelDdl string  // дословно спека §3.3

var mpSummaryImporter = &mpSummaryStat{ClickHouseImport: util.ClickHouseImport{TableName: mpSummaryTable}}
var mpSummaryView, mpSummaryBlocksView, mpSummaryChannelsView util.View  // v_cbr_mp_summary{,_blocks,_channels}
```

Один прогон `Import(ctx, conn)` (порядок шагов — часть контракта, в комментарии функции):
DDL × 3 → `getPage(mpSummaryIndexUrl)` → `parseMPDecIndex` → известные адреса из
`SELECT DISTINCT url FROM cbr_mp_summary FINAL` → `mpLLMConfigFromEnv` + `mpLLMPending` →
no-op, если нет ни новых резюме, ни отстающих LLM-документов → прошлые решения таблицы →
скачивание и разбор новых документов (fail-fast) → дельта → батчи шапки/блоков/regex-каналов
и их `Send` → LLM-догон (новые + отстающие документы) → три `util.CreateView` → `Infof`
итогов и `return count` (сумма Append'ов всех четырёх батчей).

Проводка: реестр — 15-я запись `{mpSummaryImporter, "cbr_mp_summary", ""}` (пустая третья
колонка: префлайта нет сознательно, дельта по индексу — тест (д) требует пустые обе стороны),
комментарии «14 записей» → «15 записей» и оговорка про отсутствие префлайта;
`importers_test.go` — явная обратная проверка `scheduled["cbr_mp_summary"]`; `dagu/cbr.yaml` —
шаг `cbr_mp_summary` последним (15-й, копия формы `cbr_mo_br`); `sql/mcp_kimi_reader.sql` —
три гранта на витрины; `scripts/mcp_check.py` — три чтения витрин (>0) + три `ACCESS_DENIED`
на сырые таблицы, в стиле блока `v_cbr_mo_br`.

---

## 3. Решения (сверх зафиксированных в брифе; каждое — с причиной)

1. **Страницы отстающих LLM-документов берутся из колонки `url` таблицы-шапки.**
   `mpLLMPendingQuery` (дословно из брифа) даёт только `meeting_date`, а для разметки нужен
   текст документа; адрес лежит в шапке, поэтому запрос прошлых решений читает три колонки:
   `SELECT meeting_date, decision_rate, url FROM cbr_mp_summary FINAL ORDER BY meeting_date`
   (в брифе он назван как `meeting_date, decision_rate`). Так один запрос обслуживает и дельту,
   и догон, и не нужен второй проход по таблице или `IN (?)` с массивом дат.
2. **`mpDay` — приведение даты к календарному дню в UTC.** clickhouse-go отдаёт `Date` в зоне
   сессии (21:00 предыдущего дня по UTC), а дата разобранного документа — всегда UTC-полночь;
   без приведения ключ дельты и сопоставление «отстающий документ ↔ строка таблицы» не совпали бы
   (шаг считался бы по двум разным рядам). Прецедент —
   `forecast/resolve.go:utcDate` (та же причина, тот же приём: год/месяц/день берутся у значения
   КАК ЕСТЬ, а не через `.UTC()`).
3. **`mpBatch` — ленивый батч (PrepareBatch на первой строке, Abort на ошибке Append).** Прогон
   LLM-догона оставляет первые три батча пустыми, а прогон без новых резюме — батч LLM;
   пустой `PrepareBatch/Send` — лишний круг к базе и запись в лог сервера (прецедент —
   `forecast/resolve.go`: «пустой батч не отправляется»). Abort на ошибке Append обязателен:
   иначе соединение останется с незакрытым батчем и следующий импортёр получит
   «уже есть незакрытый батч» (там же).
4. **Fail-fast на скачивании/разборе нового документа.** Ошибка отдаётся наверх, а не warn'ом:
   `decision_delta_bp` считается по непрерывному ряду `meeting_date`, и частичная запись
   (часть новых резюме без соседних) дала бы шаг «через заседание», который читатель принял бы
   за решение. Ошибка одного документа в **LLM-догоне**, наоборот, warn и продолжение —
   деградация до regex-слоя, догон повторится (спека §3.5); ошибка базы в любом батче — наверх.
5. **Ветка no-op не публикует витрины** (как и требует бриф): они созданы прошлым прогоном,
   а сам прогон ничего не пишет. Число для сообщения читается тем же прогоном
   (`SELECT count() FROM cbr_mp_summary FINAL`).
6. **`CreateTable` оставлен пустым, `PublicationSource`/`PublicationCountPoints`/`AfterImport`
   не объявлены** — у импортёра свой `Import()` (три таблицы, три витрины, дельта по индексу),
   шаблон `util.ClickHouseImport` не участвует; DDL трёх таблиц исполняет `Import()` сам.
   Оговорка про «почему нет префлайта» — рядом с реестром и в комментарии `var mpSummaryImporter`.
7. **Константы методов/версий:** `mpMethodRegex = "regex"`, `mpMethodLLM = "llm"`,
   `mpPromptVersionRegex = 0` — вместо строк-литералов в Appends (литерал `'llm'` в
   `mpLLMPendingQuery` собирается из `mpMethodLLM`, чтобы источник правды был один);
   `mpPromptVersion` для LLM-строк берётся из T5 без изменений.
8. **Комментарии витрин** — по спеке §3.6: у шапки оговорка «решение — факт ЦБ, сигнал —
   дословная цитата», у каналов — обязательная фраза про эвристическую (regex) и модельную (llm)
   разметку, которая не является позицией ЦБ, и про `note` как краткий вывод модели; у каждой
   колонки трёх витрин есть комментарий (это пинится тестом).

---

## 4. Тесты

Обязательные по брифу (все — офлайн, без сети и БД):

- `TestMPSummaryImporterNameAndTable` — `Name()` и `TableName` литералом `"cbr_mp_summary"`.
- `TestMPSummaryDdlPins` — три DDL-литерала: `CREATE TABLE IF NOT EXISTS <таблица> (` с
  литеральным именем, `ENGINE = ReplacingMergeTree`, ключ ORDER BY
  (`meeting_date` / `(meeting_date, block, subsection, kind, seq)` /
  `(meeting_date, channel, kind, method, model, prompt_version, seq)`) и колонки с типами
  (в том числе `decision_delta_bp Nullable(Int16)`, `effective_from Nullable(Date)`,
  `prompt_version UInt16`, `seq UInt16`).
- `TestMPSummaryViewPins` — для каждой витрины Select сверяется **целиком** с собранным
  ожиданием `SELECT <колонки> FROM <таблица> FINAL` (значит, проверены и FINAL, и имена таблиц,
  и отсутствие source-фильтра, и явный список колонок), плюс `Name`, `Tables`, непустой
  Comment и комментарий на каждую колонку.
- Правки `cbr/importers_test.go`: явная обратная проверка `scheduled["cbr_mp_summary"]`
  (по образцу владельцев `macro_series`) + оговорка в описании проверки (г).

Дополнительно (сверх списка брифа, названо здесь сознательно): `TestMPSummaryDecisionDeltas` —
чистая функция `mpDecisionDeltas`: NULL у самого раннего решения (проверка через
`got, ok := map[…]`, чтобы «нет ключа» не выглядело как NULL), `0` у hold, `-50` у снижений,
и «новый документ той же даты сильнее прошлого». Дельта — единственная арифметика импортёра,
её ошибка тихая (число выглядит правдоподобно), поэтому пин дешевле, чем поиск её на приёмке.

**Проверка чувствительности пинов (mutation check).** Реализация писалась до тестов (бриф
требует обратного порядка — TDD-FAIL), поэтому вместо прогона «FAIL до реализации» пины
проверены мутациями (файл после каждой — `git checkout --`):

- `mpSummaryName` → `"cbr_mp_summary_mutated"`: падают `TestMPSummaryImporterNameAndTable`
  и тест реестра (`реестр записал импортёру "cbr_mp_summary_mutated" таблицу "cbr_mp_summary"`,
  `шаг dagu/cbr.yaml CLICKHOUSE_IMPORT_STAT=cbr_mp_summary не найден в реестре`).
- `* 100` → `* 10` в `mpDecisionDeltas`: падает `TestMPSummaryDecisionDeltas` (три строки,
  в том числе случай уточнения ставки).

Мутация №2 заодно вскрыла дефект диагностики (в сообщении печатался адрес указателя `*int16`);
исправлено (`%d` по разыменованию, отдельная ветка «NULL, want N»), правка внесена в тот же
коммит через `--amend --no-edit` — сообщение коммита и состав файлов не изменились.

---

## 5. Наблюдения (не блокеры)

1. **No-op без LLM-ключа молчит про LLM.** Если конфиг не задан, `mpLLMPending` не выполняется
   (нет модели для запроса), поэтому прогон без новых документов уходит в no-op и warn
   «LLM-разметка пропущена» не печатается. Это ровно поведение, зафиксированное брифом
   (шаг 4/5 и шаг 8), и оно безопасно: документ остаётся с regex-слоем, а warn появится на
   первом же прогоне с новым резюме. Если владелец захочет видеть отставание и в тихом
   состоянии, это отдельная правка (проверять пару с пустой моделью «нет строк method='llm'»).
2. **Повторный прогон при 22 известных документах** делает только три чтения (адреса, прошлые
   решения, отстающие LLM) и не качает ни одной страницы — проверка адресов вместо HEAD-префлайта.
3. **Догон отстающих качает их страницы заново** (в таблицах лежит разметка, а не текст) — это
   восстановительный сценарий (LLM падал/ключ добавили позже), в стабильном режиме
   `llmPending` пуст.
4. Живые вставки и `make mcp-check` не выполнялись: MCP-инструменты дают `kimi_reader`
   read-only, импорт и приёмка — T9. Проверено офлайн (тесты/гейты) и чтением: DDL разбираются
   `EXPLAIN SYNTAX`, порядок колонок `INSERT`-литералов сверен с порядком Append вручную
   (шапка 8, блоки 6, каналы 8 значений).

---

# Fix round 1 (ревью Task 6, коммит `3d456ae`)

**Status: DONE — Important-находка закрыта; коммит `a81a165`** (ветка `feat/cbr-mp-summary`,
родитель `3d456ae`; 3 файла: `cbr/mp_summary.go`, `cbr/mp_summary_test.go`,
`docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md`).
`deploy/staging/dagu.service` снова не трогался (`git status --short` до и после `git add`:
` M deploy/staging/dagu.service` — единственная запись вне коммита).

## Находка (Important) и решение контроллера

*Partial-failure:* шапка `cbr_mp_summary` пишется первым батчем, известность документа
проверялась только по `url` (`SELECT DISTINCT url FROM cbr_mp_summary FINAL`). Если
`blockRows.send()` или `regexRows.send()` падал после `header.send()`, следующий прогон считал
документ известным; без LLM-конфига `pending` пуст → no-op → блоки и regex-каналы терялись
навсегда. Противоречит спеке §3.1 п. 2 («сбой батчей слоя 1 = следующий прогон повторит»).

**Решение (вариант (а), фиксируется как принятое):** документ считается известным, **только если
его `url` есть в шапке И по его `meeting_date` есть хотя бы одна строка в
`cbr_mp_summary_block FINAL`**. Недописанный документ снова попадает в `missing`, перекачивается
и перезаписывается целиком (ключи ReplacingMergeTree делают перезапись идемпотентной). Порядок
батчей и ветка no-op не менялись — ровно как предписано рулингом.

## Что изменено

1. **Запросы известности** (`cbr/mp_summary.go`): вместо `mpSummaryKnownUrlsQuery`
   (`SELECT DISTINCT url …`) два запроса — `mpSummaryImportedQuery`
   (`SELECT url, meeting_date FROM cbr_mp_summary FINAL`) и `mpSummaryBlockDatesQuery`
   (`SELECT DISTINCT meeting_date FROM cbr_mp_summary_block FINAL`). Даты в обеих картах
   приводятся к календарному дню UTC тем же `mpDay`, что и раньше (иначе даты блоков и шапок
   не совпали бы: драйвер отдаёт `Date` в зоне сессии).
2. **Чистая функция `mpMissingRefs(refs []mpSummaryRef, imported map[string]time.Time, blockDates map[time.Time]bool) []mpSummaryRef`**
   — решение «известен/не известен» без обращений к БД: карточка остаётся в `missing`, если её
   адреса нет в шапке **или** по дате её заседания нет блоков. Порядок индекса сохраняется.
   Функция вызывается из `Import()` шагом 3 (`mpImportedSummaries` → `mpBlockDates` →
   `mpMissingRefs`); шаг 3 в комментарии `Import` переписан под новое условие.
3. Хелперы чтения — `mpImportedSummaries` (url → дата) и `mpBlockDates` (множество дат) вместо
   `mpKnownUrls`; старых символов в пакете не осталось (`grep mpSummaryKnownUrlsQuery\|mpKnownUrls` → пусто).
4. **Тест `TestMPSummaryMissingRefs`** (`cbr/mp_summary_test.go`) — три карточки индекса:
   полная (шапка + блоки), «шапка есть, блоков нет» и вовсе неизвестная; ожидание
   `missing = [шапка без блоков, неизвестная]` (и порядок сохранён), плюс обратное направление:
   у полного документа `missing` пуст (иначе ежедневный шаг заново разбирал бы все 22 резюме).
5. **Спека** `docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md` §3.1 п. 1: фраза
   «Известность проверяется по `url` (SELECT DISTINCT url из `cbr_mp_summary FINAL`)» заменена
   на формулировку рулинга («Известность документа — `url` в шапке **и** хотя бы одна строка
   блоков по его `meeting_date` (`cbr_mp_summary_block FINAL`): частично записанный документ
   (сбой среднего/последнего батча) перекачивается и дописывается идемпотентно»).

## Red-green (тест пина действительно ловит регресс)

Мутация в `mpMissingRefs` — `if ok && blockDates[date]` → `if ok || blockDates[date]`
(то есть прежняя семантика «известен по одному `url`»):

```
--- FAIL: TestMPSummaryMissingRefs (0.00s)
    mp_summary_test.go:185: missing: 1 карточек [{…summary_key_rate_05082026/}], want 2 […]:
    шапка без блоков обязана перекачиваться
```

С возвращённым условием — `--- PASS: TestMPSummaryMissingRefs (0.00s)`. Дополнительно проверено
второе направление мутации (`&&` → заведомо ложное условие): тест падает и на «полный документ
попал в missing» — то есть пин держит обе стороны контракта.
**Важно для следующих раундов:** откат мутации делался копией файла (`cp /tmp/mp_summary.go.fixed`),
а не `git checkout -- <файл>`: индекс в fix-раунде пуст, и `git checkout` уже один раз снёс
незакоммиченные правки этого раунда (файл был восстановлен повторным применением правок,
копия сверена `diff -q` → `RESTORED_IDENTICAL`).

## Гейты fix-раунда (на рабочем дереве после правок)

```
$ go test ./cbr/ -count=1        → ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  1.965s
$ gofmt -l cbr/                  → (пусто)
$ golangci-lint run ./cbr/...    → 0 issues
$ go build ./... && go vet ./cbr/ → ok
$ go test ./... -count=1         → все пакеты ok (19 ok, строк FAIL — 0)
```

MCP (JetBrains): `get_file_problems` по `cbr/mp_summary.go` (errorsOnly) → `{"errors":[]}`.
Дополнительно повторена машинная сверка трёх DDL-литералов со спекой §3.3 → все три
`IDENTICAL` (правки раунда DDL не касались).

## Остаточное наблюдение (не переоткрытие рулинга)

Условие рулинга покрывает сбой батча блоков, но не сбой **только третьего** батча
(regex-каналы) при удачно записанных шапке и блоках: такой документ по-прежнему считается
известным, и regex-слой сам не догоняется (LLM-слой догоняется — пара `(model, prompt_version)`
в ключе). Это сознательно принятая граница варианта (а); если понадобится — отдельный пункт:
признаком полноты сделать и наличие regex-строк (`method='regex'`) по дате заседания.

**→ Закрыто расширением рулинга (коммит `450f03d`, см. ниже): признак полноты стал тройным,
regex-условие в него включено, остаточного случая больше нет.**

---

# Fix round 1 (продолжение): полнота слоя 1 = шапка + блоки + regex-каналы

**Status: DONE — расширение рулинга реализовано; коммит `450f03d`** (ветка `feat/cbr-mp-summary`,
родитель `a81a165`; 3 файла: `cbr/mp_summary.go`, `cbr/mp_summary_test.go`,
`docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md`). `deploy/staging/dagu.service`
не трогался (`git status --short` до и после `git add`: ` M deploy/staging/dagu.service` —
единственная запись вне коммита).

## Что изменено

1. **Третий уровень известности.** Новый запрос
   `mpSummaryRegexDatesQuery = "SELECT DISTINCT meeting_date FROM cbr_mp_summary_channel FINAL WHERE method = 'regex'"`
   (имя метода подставляется из `mpMethodRegex`, а не литералом `'llm'`/`'regex'` в тексте).
   Отбор именно по `method='regex'`: строки LLM-слоя лежат в той же таблице и догоняются
   отдельной веткой, поэтому по ним потерянный третий батч слоя 1 выглядел бы записанным.
2. **`mpMissingRefs` стал тройным:**
   `mpMissingRefs(refs []mpSummaryRef, imported map[string]time.Time, blockDates, regexDates map[time.Time]bool) []mpSummaryRef`
   — документ остаётся в `missing`, если нет адреса в шапке **или** блоков по его `meeting_date`
   **или** regex-строк по той же дате. Порядок индекса сохранён.
3. **Шаг 3 `Import`** читает три уровня: `mpImportedSummaries` + два вызова нового `mpDateSet`
   (`mpSummaryBlockDatesQuery` → «даты блоков», `mpSummaryRegexDatesQuery` → «даты regex-разметки»).
   Функция `mpBlockDates` заменена параметризованным `mpDateSet` (тот же проход, что и раньше:
   одна колонка `meeting_date`, `mpDay` на каждую дату); комментарий шага 3 в шапке `Import`
   и комментарии запросов/функции обновлены под «три батча слоя 1».
4. **Тест `TestMPSummaryMissingRefs`** расширен: четыре карточки — полная (все три уровня),
   «шапка без блоков», «шапка и блоки без regex-строк» (сбой третьего батча) и вовсе неизвестная;
   ожидание `missing = [шапка без блоков, без regex, неизвестная]` с сохранением порядка.
   Обратное направление (у полного документа `missing` пуст) сохранено. Добавлен отдельный пин
   запроса третьего уровня против литерала
   `"SELECT DISTINCT meeting_date FROM cbr_mp_summary_channel FINAL WHERE method = 'regex'"` —
   чтобы фильтр по `regex` нельзя было «упростить» до всей таблицы, не заметив.
5. **Спека §3.1 п. 1**: условие известности стало тройным —
   «…**и** хотя бы одна строка блоков по его `meeting_date` (`cbr_mp_summary_block FINAL`),
   **и** хотя бы одна regex-строка каналов по `meeting_date` (`cbr_mp_summary_channel FINAL`,
   `method='regex'`; именно `regex` — иначе строки LLM-разметки замаскировали бы отсутствие
   слоя 1): частично записанный документ (сбой любого из трёх батчей слоя 1) перекачивается
   и дописывается идемпотентно».

## Red-green (продолжение)

Мутация в `mpMissingRefs` — снято условие по `regexDates` (`if ok && blockDates[date] && regexDates[date]`
→ `if ok && blockDates[date]`):

```
--- FAIL: TestMPSummaryMissingRefs (0.00s)
    mp_summary_test.go:193: missing: 2 карточек [{…/summary_key_rate_05082026/} {…/summary_key_rate_01042026/}],
    want 3 [… {…/summary_key_rate_01072026/} …]: неполный документ слоя 1 обязан перекачиваться
```

Документ со шапкой и блоками, но без regex-строк, в мутанте считался известным — ровно тот
случай, ради которого расширение и делалось. С возвращённым условием —
`--- PASS: TestMPSummaryMissingRefs (0.00s)`. Откат мутации — копией файла (`cp`, `diff -q` →
`RESTORED_IDENTICAL`), без `git checkout` (урок предыдущего шага).

## Гейты (на закоммиченном состоянии)

```
$ go test ./cbr/ -count=1        → ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  1.771s
$ gofmt -l cbr/                  → (пусто)
$ golangci-lint run ./cbr/...    → 0 issues
$ go build ./... && go vet ./cbr/ → ok
$ go test ./... -count=1         → 19 пакетов ok
```

MCP (JetBrains): `get_file_problems` по `cbr/mp_summary.go` (errorsOnly) → `{"errors":[]}`.

## Сомнения (не блокеры)

1. Тройное условие читает на каждом прогоне даты из двух таблиц (`DISTINCT meeting_date`) —
   это два дешёвых запроса по ключу вместо одного; на 22 документах разница неощутима.
   Оптимизация «одним запросом с `EXISTS`/`JOIN`» сознательно не делалась: отдельные чистые
   множества лучше пинятся и переиспользуются в `mpMissingRefs`.
2. Гарантия полноты остаётся по уровням (шапка/блоки/regex-строки), а не по числу строк:
   документ, у которого ЦБ сократил текст (часть regex-строк не записалась из-за частичного
   сбоя внутри одного `Send`), считается полным. `Send` одного батча в clickhouse-go —
   одна транзакция, поэтому частичной записи внутри батча быть не может; принято как достаточное.

---

# Fix round 2: пин условия по блокам в тесте известности

**Status: DONE — тест пинит все три условия; коммит `0a04135`** (ветка `feat/cbr-mp-summary`,
родитель `450f03d`; один файл — `cbr/mp_summary_test.go`). Код `cbr/mp_summary.go` не менялся:
ревьюер подтвердил его верность, правка чисто тестовая.

## Находка (scoped re-review)

Мутация **M-A** (снять условие `blockDates[date]` из `mpMissingRefs`, оставив `url` и
`regexDates`) проходила `TestMPSummaryMissingRefs`: в наборе кейсов не было документа
«шапка + regex-строки, **без** блоков» — были только `headerOnly` (ни блоков, ни regex)
и `noRegex` (блоки есть, regex нет). Условие по блокам, таким образом, не пинилось ни одним
кейсом.

## Что изменено (только тест)

1. Добавлен кейс **`regexOnly`** — карточка `summary_key_rate_07052026/` (публикация 07.05.2026,
   дата заседания 24.04.2026 из шапки таблицы), у которой есть адрес в `imported` и
   `regexDates[24.04.2026] = true`, но **нет** записи в `blockDates`. В ожидании `missing`
   он стоит третьим (порядок индекса: `full`, `headerOnly`, `noRegex`, `regexOnly`, `unknown`).
2. Сверка списка дополнена отдельной проверкой того же кейса в одиночку
   (`mpMissingRefs([]mpSummaryRef{regexOnly}, …)` → ровно один элемент) с диагностикой
   «без блоков документ неполон» — чтобы падение называло именно это условие.
3. Комментарии теста переписаны: теперь перечислены пять карточек и явно сказано, зачем нужен
   `regexOnly` (без него условие по `blockDates` можно было бы выбросить незамеченным).
   Каждое из трёх условий известности пинится отдельным кейсом.

## Red-green (M-A воспроизведена и закрыта)

Мутация M-A в копии файла (`mp_summary.go`: `if ok && blockDates[date] && regexDates[date]`
→ `if ok && regexDates[date]`):

```
--- FAIL: TestMPSummaryMissingRefs (0.00s)
    mp_summary_test.go:200: missing: 3 карточек [{…05082026/} {…01072026/} {…01042026/}], want 4
    [… {…07052026/} …]: неполный документ слоя 1 (шапка, блоки, regex-строки — все три уровня)
    обязан перекачиваться
```

Документ `regexOnly` («шапка + regex-строки без блоков») в мутанте считался известным — ровно
случай находки. С восстановленным условием — `--- PASS: TestMPSummaryMissingRefs (0.00s)`.
Откат — копией файла (`cp` + `diff -q` → `RESTORED_IDENTICAL`), без `git checkout --`.

## Гейты (на закоммиченном состоянии)

```
$ go test ./cbr/ -count=1 -run TestMPSummaryMissingRefs -v → --- PASS: TestMPSummaryMissingRefs (0.00s)
$ go test ./cbr/ -count=1        → ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  2.101s
$ gofmt -l cbr/                  → (пусто)
$ golangci-lint run ./cbr/...    → 0 issues
$ go build ./... && go vet ./cbr/ → ok
```

`deploy/staging/dagu.service` не трогался (`git status --short` до и после `git add`:
` M deploy/staging/dagu.service` — единственная запись вне коммита).



