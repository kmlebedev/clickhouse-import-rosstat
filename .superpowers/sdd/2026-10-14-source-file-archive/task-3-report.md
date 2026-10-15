# Task 3 — отчёт: реестр `registry.jsonl` (`archive/registry.go`)

- **Статус: DONE** (сомнения — в конце).
- Ветка: `source-file-archive`, база `941921d`. Коммит: `865f258 add archive registry with jsonl upsert` (2 файла: `archive/registry.go`, `archive/registry_test.go`).
- Тул-набор: `mcp__jetbrains__*` доступен и использован (`get_file_problems` на `registry.go` и `registry_test.go` → `"errors":[]`); `mcp__clickhouse__*` не потребовался — SQL в задаче нет, схема ClickHouse не меняется.

## Что реализовано

| Файл | Что |
|---|---|
| `archive/registry.go` (new, 183 строки) | `RegistryKey = "registry.jsonl"`; `Entry{Source, Name, Fingerprint, SHA256 string; Size int64; URL, Key string; FirstSeen time.Time}` с тегами `source,name,fingerprint,sha256,size,url,key,first_seen`; `NewRegistry(store Store) *Registry`; `(*Registry).Ensure(ctx, e) error`; `(*Registry).Entries() []Entry` (копия); внутренние `load`, `parseEntries`, `upsert`, `marshalEntries` |
| | **Ленивое чтение**: `load` при первом `Ensure` — `Exists`, и только если файл есть — `Get`; ни один из этих вызовов не делается в `NewRegistry` (конструктор не ходит в хранилище, правило 3 AGENTS.md) |
| | **Отсутствие файла — не ошибка** (пустой реестр, Review Focus №1); **пустой файл — тоже** (обнулённый реестр не роняет архивацию); **битая непустая строка — ошибка** с номером строки (1-based): реестр объявлен источником истины, молча пропустить строку = потерять версию |
| | **Upsert по `Key`**: совпали `Key` и `SHA256` → изменений нет, `Put` не делается вовсе; тот же `Key` с другим `SHA256` → строка заменяется **на месте** (порядок появления версии сохраняется, число строк не растёт); новый `Key` → дописывается в конец (порядок добавления = порядок `first_seen`) |
| | **Запись**: `json.Marshal` каждой записи + `\n`, весь файл собирается в буфер и уходит **одним `Put`** (объект в хранилище пишется целиком); состояние в памяти принимается только **после** успешного `Put` — неудачная запись не утечёт в файл следующим `Ensure` строкой на несуществующий объект |
| | **Мьютекс на процесс** (`sync.Mutex` вокруг `Ensure`/`Entries`): внутри процесса read-modify-write иначе теряет строки; между процессами писателя не разводит — спека §3.4 принимает это (штатный писатель один, потеря самолечится префлайтом) |
| | `Entries()` отдаёт копию (`append([]Entry(nil), …)`); до первого `Ensure` список пуст (файл читается лениво); ошибки не содержат ни содержимого строк, ни секретов — только имя ключа, номер строки и причину |
| `archive/registry_test.go` (new, 364 строки) | 8 тестов, in-package, офлайн: `LocalStore` в `t.TempDir()`, ни одного сетевого вызова. Проверяются байты **файла на диске** (`os.ReadFile(dir/registry.jsonl)`), число строк, их порядок и разобранное содержимое — не моки; единственный декоратор (`registryPutCounter`) оборачивает настоящий `LocalStore` и только считает `Put` |

Новых зависимостей нет: stdlib (`bytes`, `context`, `encoding/json`, `fmt`, `slices`, `strings`, `sync`, `time`; `slices` — stdlib, в репозитории уже используется, `go.mod` — `go 1.27.2`). Секретов в записях нет (`source/name/url/sha256`), в тестах — фиктивные имена и суммы.

## Тесты и результаты

| Команда | Результат |
|---|---|
| `gofmt -l archive/` | пусто |
| `go vet ./archive/` | чисто |
| `go test -race -count=1 ./archive/` | `ok github.com/kmlebedev/clickhouse-import-rosstat/archive 1.476s` |
| `go test -count=1 -v ./archive/` | 33/33 top-level теста PASS, 0 FAIL/SKIP (было 25; +8 новых `TestRegistry*`) |
| `golangci-lint run ./archive/...` | `0 issues.` |
| `go build ./...` | OK |
| `make all` (весь репозиторий: gofmt, golangci-lint `./...`, vet, `go test -race ./...`, обе сборки) | `0 issues.`, все пакеты `ok`, `build/clickhouse-import-rosstat` и `build/ingest` собраны |
| `mcp__jetbrains__get_file_problems` на обоих файлах | `"errors":[]` |

Покрытие по брифу: `TestRegistryEnsureCreatesMissingFile` (пустое хранилище → файл, одна строка, `\n` в конце, ровно восемь полей + имя ключа литералом), `TestRegistryEnsureIsIdempotent` (тот же `Key`+`SHA256`: байты до/после совпадают **и** `Put` не вызван, в том же и в новом `Registry`), `TestRegistryEnsureReplacesChangedContent` (тот же `Key`, другой `SHA256`: одна строка вместо двух, новая версия стоит на месте прежней), `TestRegistryAppendsNewKeys` (второй ключ — строкой ниже, порядок добавления), `TestRegistryEntriesReturnsCopy` (правка копии не меняет ни `Entries()`, ни файл). Сверх брифа: `TestRegistryEnsureEmptyFile` (Review Focus №1 — «отсутствующий **или пустой**»), `TestRegistryBrokenLineIsError` (ошибка с номером строки, файл не перезаписан), `TestRegistryConcurrentEnsure` (8 горутин: строк 8, ключи уникальны, `-race` чист).

## TDD-доказательства

**RED (шаги 1–2 брифа)** — `go test -run TestRegistry -count=1 ./archive/` после `archive/registry_test.go`, до `archive/registry.go`:

```
# github.com/kmlebedev/clickhouse-import-rosstat/archive [github.com/kmlebedev/clickhouse-import-rosstat/archive.test]
archive/registry_test.go:22:54: undefined: Entry
archive/registry_test.go:23:9: undefined: Entry
archive/registry_test.go:59:46: undefined: RegistryKey
archive/registry_test.go:61:35: undefined: RegistryKey
archive/registry_test.go:82:49: undefined: Entry
archive/registry_test.go:85:16: undefined: Entry
archive/registry_test.go:87:9: undefined: Entry
archive/registry_test.go:98:50: undefined: Entry
archive/registry_test.go:118:12: undefined: NewRegistry
archive/registry_test.go:123:5: undefined: RegistryKey
archive/registry_test.go:123:5: too many errors
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/archive [build failed]
FAIL
```

Ожидаемо: ровно предсказанное брифом `undefined: NewRegistry` (+ `undefined: Entry`/`RegistryKey` — типы названы в хелперах теста); остальное в пакете уже собрано, значит падение вызвано только отсутствием реализации. Прогон повторён на финальном тексте тестов (перенесённый на время `registry.go`) — тот же вывод, `restored OK ac2abc2b…`.

**GREEN (шаг 3)** — сразу после `archive/registry.go`, `go test -run TestRegistry -count=1 -v ./archive/`: все 8 `TestRegistry*` PASS, `ok … 0.568s`.

**RED (self-review: идемпотентность, доказательство невакуумности теста)** — временная замена ветки «изменений нет» на «всегда `Put`»:

```
--- FAIL: TestRegistryEnsureIsIdempotent (0.02s)
    registry_test.go:192: повторный Ensure сделал Put (3 вызовов против 1): содержимое не менялось
```

То есть тест ловит именно то, ради чего написан: байты при перезаписи тем же содержимым совпали бы, а лишний запрос к хранилищу — нет (см. находку 2).

**RED (self-review: битая строка)** — временный `continue` вместо `return … err` в `parseEntries`:

```
--- FAIL: TestRegistryBrokenLineIsError (0.01s)
    registry_test.go:312: Ensure на реестре с битой строкой не дал ошибки
```

**RED (self-review: мьютекс)** — временное снятие `Lock/Unlock` с `Ensure` и `Entries`:

```
WARNING: DATA RACE (×4)
--- FAIL: TestRegistryConcurrentEnsure (0.03s)
    registry_test.go:354: строк в реестре 1, ожидалось 8 (потеря записи при конкурентном Ensure)
```

Все три временные правки откачены, файл сверен по sha256 (`ac2abc2b…` — тот же, что в коммите), финальный `go test -race -count=1 ./archive/` зелёный.

## Изменённые файлы (коммит `865f258`)

```
 archive/registry.go      | 183 ++++++++++++++++++++++++
 archive/registry_test.go | 364 +++++++++++++++++++++++++++++++++++++++++++++++
 2 files changed, 547 insertions(+)
```

`deploy/staging/dagu.service` — незакоммиченная правка владельца, не тронута (осталась в рабочем дереве, `git status --short` показывает только её).

## Находки self-review

1. **`json.Marshal` экранирует `&` в `url` как `\u0026`** (и `<`/`>` там же). Проверено на живом файле: `"url":"https://www.cbr.ru/vfs/macro/list.xlsx?id=1\u0026lang=ru"`. Альтернатива — `json.Encoder` с `SetEscapeHTML(false)` (по-человечески читаемое зеркало) — сознательно **не** взята: бриф называет `json.Marshal` дословно, а на разбор экранирование не влияет (это тот же JSON, round-trip через `json.Unmarshal` проходит — тесты это и делают). Решение зафиксировано комментарием в `marshalEntries` и вынесено в сомнения.
2. **Равенства байтов мало для «не делай `Put`»**: перезапись тем же содержимым даёт те же байты. Поэтому в тесты добавлен `registryPutCounter` — декоратор над настоящим `LocalStore`, считающий `Put`; именно он (а не сравнение байтов) падает при мутации из TDD-блока. Экономика: без этой ветки на Диск уходил бы запрос на каждый прогон каждого импортёра.
3. **Замена «на месте» — выбор, которого бриф не проговаривает**: бриф требует только «число строк не выросло». Строка остаётся там, где версия была записана впервые, и файл читается как порядок появления источников (спека §3.4: «новые строки — в порядке `first_seen`»), а не как порядок последних обновлений; тест это пинит позицией.
4. **Состояние принимается после успешного `Put`.** Если бы `r.entries` обновлялся до `Put`, неудачная запись одной версии утекла бы в файл следующим `Ensure` — строкой на объект, которого в хранилище нет (объект пишется раньше реестра и к этому моменту мог не записаться). В брифе этого нет, но именно здесь read-modify-write и «источник истины» расходятся молча.
5. **Пустая (whitespace-only) строка не ошибка, непустая битая — ошибка.** Пустая строка записи не несёт, и лишний `\n` мог оставить ручной разбор или инструмент зеркала — терять при этом нечего; непустая неразобранная строка теряет версию, поэтому ошибка с номером (1-based), по которому дежурный находит её и в зеркале, и на Диске. Осознанное прочтение «битая строка = ошибка» (в сомнениях).
6. **При ошибке разбора файл не перезаписывается** — иначе `Ensure` «вылечил» бы реестр, усекнув его до разобранного префикса; тест сравнивает байты до/после.
7. **`load` не помечает реестр прочитанным при сбое** `Exists`/`Get`: ошибку видит вызывающий (архивация не состоялась, строка версии не пишется), а повтор в том же процессе снова попробует прочитать, а не затрёт файл пустым состоянием.
8. YAGNI: не добавлено валидации пустого `Key` (в брифе нет; в `Entry` его всегда ставит `util` из `archive.Key`), файловых блокировок/compare-and-swap между процессами (спека §3.4 принимает одного писателя), логирования (Task 4), экспорта реестра для зеркала (Task 5), сортировки, поля версии схемы.

## Сомнения

1. **Экранирование `\u0026` в `url`** (находка 1) — единственное видимое отличие от «красивого» зеркала. Цена правки — `json.Encoder` + `SetEscapeHTML(false)` в `marshalEntries` (тесты не изменятся, они разбирают JSON, а не сравнивают байты строки).
2. **Три теста сверх пяти из брифа** (`TestRegistryEnsureEmptyFile`, `TestRegistryBrokenLineIsError`, `TestRegistryConcurrentEnsure`): первые два закрывают Review Focus №1 и требование Step 3 («битая строка — ошибка»), третий доказывает невакуумность мьютекса. Если контроллер считает лишние тесты шумом — снимаются без потери покрытия брифа.
3. **Пустая строка в реестре трактуется как «не запись»** (находка 5). Если требуется строгий режим («любая непустая-JSON-строка = ошибка»), правка — один `if` в `parseEntries`; тестов на это нет.
4. **`Entries()` до первого `Ensure` пуст** — реестр читается лениво и только на запись, а `Entries()` не принимает `ctx` (сигнатура из брифа), поэтому «прочитать файл по требованию» им нельзя; для зеркала в Task 5 путь другой — `Store.Get` (так и в плане). Если `Entries` понадобится вызывать до первой записи, понадобится либо `ctx`-версия, либо явный метод загрузки.
5. **Мьютекс защищает только процесс.** Два одновременных процесса (например, ручной прогон рядом с dagu) по-прежнему могут потерять строку реестра — принято по спеке §3.4 (самолечение), но проверяемо только в приёмке Task 5/6, а не тестом.

---

# Fix round 1

Коммит: `84ace77 make registry state process-wide and ordered` (только `archive/registry.go` 187 строк, `archive/registry_test.go` 489 строк). База — `865f258`. `deploy/staging/dagu.service` не тронута.

## Что изменено

**Important 1 — мьютекс и состояние на процесс.** `mu`/`loaded` из `Registry` убраны, вместо них пакетная `var registryMu sync.Mutex`; кэш чтения снят — `load` заменён на `read(ctx) ([]Entry, error)`, и **`Ensure` перечитывает файл целиком на каждой записи**: `Exists` → `Get` → `parseEntries` → `upsert` → `Put`. Под `registryMu` идут `Ensure` и `Entries`. Два `NewRegistry(store)` сходятся: трасса `r1.Ensure(A)` → `r2.Ensure(B)` → `r1.Ensure(C)` даёт три строки. Состояние `r.entries` — «копия содержимого файла после последнего завершённого `Ensure` этого экземпляра» (так подписано в комментарии): после успешного `Put` — слитый слайс, а в ветке «изменений не потребовалось» — только что прочитанный файл (не прежний слайс экземпляра), поэтому соседние строки видны и в `Entries`; при сбое `Put` состояние не принимается. Цена — лишний `Get` на `Ensure`, он же вызывается только при смене версии; прогон без архивации в хранилище по-прежнему не ходит.

**Extra (следствие правки 1).** Убрав `mu` с экземпляра, я получил гонку `Entries` (читает поле) против `Ensure` (пишет поле) — её поймал `-race` в `TestRegistryConcurrentEnsure` в первом же прогоне:

```
WARNING: DATA RACE
Write at 0x00c000218490 by goroutine 42: …(*Registry).Ensure() … registry.go:95
Previous read at 0x00c000218490 by goroutine 41: …(*Registry).Entries() … registry.go:104
```

Поэтому `Entries` тоже берёт `registryMu` (один замок на оба обращения к состоянию, вложенности нет — дедлока нет). Без этой правки фикс 1 не собирался бы зелёным под `-race`.

**Important 2 — порядок `first_seen` при замене.** В `upsert` замена больше не «на месте»: строка удаляется из своей позиции и уезжает в конец (`append(slices.Clone(entries[:i]), entries[i+1:]...)`, затем новая запись в хвост) с новыми `SHA256`/`Size`/`URL`/`Fingerprint`/`FirstSeen`. Файл всегда упорядочен по `first_seen`, число строк не растёт.

**Minor 3 — HTML-экранирование.** `marshalEntries` переведён на `json.Encoder` + `SetEscapeHTML(false)`; одна запись — одна строка с `\n` в конце, как было. `&` в `url` лежит в файле дословно.

**Minor 4 — тест сбоя `Put`.** `registryFailOnceStore` (декоратор над `LocalStore` с падающим один раз `Put`) + `TestRegistryPutFailureKeepsState`.

**Попутно (self-review, вне findings).** Проверка имени файла в `TestRegistryEnsureCreatesMissingFile` была сравнением константы с литералом — GoLand (`get_file_problems`) справедливо ругался `Condition 'RegistryKey != "registry.jsonl"' is always 'false'`; заменил на `os.Stat(dir/registry.jsonl)` — пин имени файла на диске остался, предупреждения нет. Также упорядочил комментарии: `registryMu` теперь документирован у `var`, а не «съедал» doc-комментарий типа `Registry`.

## Новые и изменённые тесты

| Тест | Что пинит |
|---|---|
| `TestRegistryInstancesShareState` (new) | два экземпляра на одном store: `r1.Ensure(A)` → `r2.Ensure(B)` → `r1.Ensure(C)` → в файле A, B, C по порядку; `r1.Entries()` показывает 3 записи |
| `TestRegistryEnsureReplacesChangedContent` (изменён) | после замены новая версия — **последняя** строка, `first_seen` новый (время версий в тесте разное), число строк 2; порядок проверяется по полю `first_seen`, а не только по позициям |
| `TestRegistryURLWithoutHTMLEscaping` (new) | `url` с `?id=1&lang=ru` лежит в файле дословно, без `\u0026`, и строка по-прежнему разбирается в исходную `Entry` |
| `TestRegistryPutFailureKeepsState` (new) | сбой `Put`: файл байт-в-байт прежний, `Entries` не показывает непринятую строку, следующий `Ensure` повторяет запись — и версия доезжает |

Остальные тесты не трогались; `TestRegistryEnsureIsIdempotent` продолжает держать `puts == 1` уже без кэша (повтор читает файл, видит тот же `Key`+`SHA256` и `Put` не делает).

## Мутационные проверки (тесты не вакуумные)

Правки вносились временно, файл восстанавливался из копии и сверялся по sha256 (`bbce0b69…` до и после).

```
=== A: кэш состояния на экземпляре (finding 1) ===
--- FAIL: TestRegistryInstancesShareState (0.02s)
    registry_test.go:379: строк в реестре 2, ожидалось 3 (строка второго Registry потеряна):
        [first.xlsx … third.xlsx …]        // ровно трасса ревью: B потеряна
FAIL

=== B: замена на месте вместо переноса в конец (finding 2) ===
--- FAIL: TestRegistryEnsureReplacesChangedContent (0.02s)
    registry_test.go:239: запись реестра = {…Name:monetary_agg_SA.xlsx…}, ожидалось {…Name:other.xlsx…}
FAIL

=== C: HTML-экранирование (&) (finding 3) ===
--- FAIL: TestRegistryURLWithoutHTMLEscaping (0.01s)
    registry_test.go:405: url лежит в файле не дословно: {…"url":"https://www.cbr.ru/vfs/macro/list.xlsx?id=1\u0026lang=ru"…}
FAIL

=== D: состояние принято до успешного Put (finding 4) ===
--- FAIL: TestRegistryPutFailureKeepsState (0.01s)
    registry_test.go:437: после сбоя Put Entries вернул 2 записей, ожидалась 1 (состояние не принято)
FAIL

RESTORED_OK bbce0b69db9375e40acf43d068a03867ae8b77c367c50d912e4211d99e9539c1
```

## Команды и вывод

```
$ gofmt -l archive/                       # пусто
$ go vet ./archive/                       # чисто
$ go test -race -count=1 ./archive/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/archive	1.692s
$ go test -count=1 -v ./archive/ | grep -c '^--- PASS'
36                                        # было 33: +3 новых теста, 0 FAIL/SKIP
$ golangci-lint run ./archive/... → 0 issues.
$ go build ./...                  → OK
$ mcp__jetbrains__get_file_problems registry.go / registry_test.go → "errors": []
```

## Что осталось как было (осознанно)

- `Entries()` до первого `Ensure` пуст — по решению контроллера (сигнатура без `ctx`).
- Пустая строка реестра — «не запись», битая непустая — ошибка с номером; при ошибке разбора файл не перезаписывается.
- Писателей между процессами `registryMu` не разводит (спека §3.4: один штатный писатель, потеря самолечится префлайтом) — проверяемо только приёмкой.
- Новых зависимостей нет; `ClickHouse`-схема не менялась, `mcp__clickhouse__*` не потребовался.
