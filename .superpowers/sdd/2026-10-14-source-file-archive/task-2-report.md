# Task 2 — отчёт: бэкенд `YandexDisk` (`archive/yandex.go`)

- **Статус: DONE** (сомнения — в конце; одно осознанное микро-отступление от буквы брифа помечено ниже).
- Ветка: `source-file-archive`, база `58bdd56`. Коммит: `941921d add yandex disk archive backend` (4 файла: `archive/yandex.go`, `archive/yandex_test.go`, `archive/fromenv.go`, `archive/fromenv_test.go`).
- Модель/Effort: deepseek/deepseek-flash (диспатч контроллера).
- Тул-набор: `mcp__jetbrains__*` доступен и использован (`get_file_problems` на `yandex.go`, `yandex_test.go`, `fromenv.go` — пусто); `mcp__clickhouse__*` не потребовался (схема ClickHouse не меняется, SQL в задаче нет).

## Что реализовано

| Файл | Что |
|---|---|
| `archive/yandex.go` (new, 320 строк) | `defaultYandexAPI = "https://cloud-api.yandex.net/v1/disk"`; переменные `opTimeout = 2 * time.Minute` и `retrySleeps = []time.Duration{time.Second, 2 * time.Second}`; `NewYandex(token, root, baseURL string, hc *http.Client) *YandexDisk` (пустой `baseURL` → прод-адрес); `Name() == "yandex"`; `Put`/`Get`/`Exists`; `var _ Store = (*YandexDisk)(nil)` |
| | Путь на Диске = `root + "/" + key` (`disk:/rosstat-archive/cbr_m2/8f3c…_файл.xlsx`), `path` идёт значением query-параметра (`url.Values.Encode()` — кириллица и `disk:/` кодируются корректно) |
| | `Put`: `PUT resources?path=<каталог>` по всем уровням от корня до папки источника (`201` и `409` — успех) → `GET resources/upload?path=…&overwrite=true` → `PUT` тела на `href` (`201`/`202` — успех). `Get`: `resources/download` → `GET href` целиком. `Exists`: `GET resources?path=…` → `200` = true, `404` = false (не ошибка) |
| | Повторы: до 3 попыток на шаг (`len(retrySleeps)+1`), паузы `retrySleeps`, только на `429`/`503`; запрос строится заново на каждую попытку (тело — reader, первой попыткой израсходован). `401` → `%w` `ErrAuth`, `403`/`413`/`423`/`507` → `%w` `ErrQuota`, прочее → `fmt.Errorf("%s %s: статус %d", <операция>, <путь на Диске>, код)` |
| | Дедлайн: `context.WithTimeout(ctx, opTimeout)` на операцию (`Put`/`Get`/`Exists`), пауза повтора прерывается отменой контекста. Авторизация: `Authorization: OAuth <token>` — только на запросы к `baseURL`; на `href`-запрос заголовка нет |
| | Секреты: токен уходит только в заголовок к API; `href` не логируется и не попадает в текст ошибки — ошибки транспорта разворачиваются из `*url.Error` в причину (`transportError`), потому что `Error()` у `*url.Error` содержит URL запроса, а URL загрузки/скачивания — подписанная ссылка. В текстах ошибок — операция, путь на Диске и код |
| `archive/fromenv.go` | yandex-ветка достроена по ruling'у T1: `token := os.Getenv(EnvYandexToken)`, пусто → прежняя ошибка с именем переменной, иначе `return NewYandex(token, yandexRoot(), "", hc), nil` (заглушка «бэкенд Диска не реализован» удалена). Отдельно отмечено, что конструктор в сеть не ходит (правило 3 AGENTS.md), а `hc` теперь реально используется (минор ревью T1 закрыт) |
| `archive/yandex_test.go` (new, 639 строк) | 10 тестов, in-package, офлайн: `httptest`-эмулятор REST API Диска (`fakeDisk`) + `t.Cleanup`-восстановление подменённых `retrySleeps`/`opTimeout`. Мок стоит на уровне `http.Handler`, поэтому проверяются настоящие запросы: `path` в query, `overwrite=true`, заголовок авторизации, метод, тело, коды |
| `archive/fromenv_test.go` | `TestFromEnvYandexRoot` переписан: корень и токен проверяются у полученного `*YandexDisk` (неэкспортируемые поля, in-package), а не по тексту ошибки. `TestFromEnvYandexRequiresToken` не тронут и зелёный |

Новых зависимостей нет (stdlib: `bytes`, `context`, `encoding/json`, `errors`, `io`, `net/http`, `net/url`, `strings`, `time`). Схема ClickHouse не менялась. Секретов в коде/логах/тестах нет: в тестах фиктивное значение `SECRET_TOKEN_VALUE`, настоящий токен — только `os.Getenv`.

## Тесты и результаты

| Команда | Результат |
|---|---|
| `gofmt -l .` | пусто |
| `go vet ./archive/` | чисто |
| `go test -race -count=1 ./archive/` | `ok github.com/kmlebedev/clickhouse-import-rosstat/archive 1.448s` |
| `go test -count=1 -v ./archive/` | 25/25 top-level тестов PASS (из них 10 новых `TestYandex*`, у `TestYandexQuotaError` 4 подтеста, у `TestYandexHrefNotInError` — 2, у `TestYandexDirsToCreate` — 4) |
| `golangci-lint run ./archive/...` | `0 issues.` |
| `make all` (репо целиком: gofmt, golangci-lint `./...`, vet, `go test -race ./...`, обе сборки) | всё зелёное, `0 issues.`, все пакеты `ok`, сборки `build/clickhouse-import-rosstat` и `build/ingest` прошли |
| `mcp__jetbrains__get_file_problems` на `yandex.go`/`yandex_test.go`/`fromenv.go` | `"errors":[]` |

Покрытие по брифу: happy path (`TestYandexPutGetExists`: Put → каталоги → upload → Get отдаёт исходные байты → Exists true, после «удаления» false и `Get` — ошибка, а не пустые байты), идемпотентность каталогов (`TestYandexFoldersIdempotent`, `409` на повторе не ломает), повтор на `429` (`TestYandexRetriesOn429`, ровно 2 запроса к ручке ссылки), `ErrAuth` без значения токена в тексте (`TestYandexAuthError`), `ErrQuota` (`TestYandexQuotaError`, 403/413/423/507), прочий статус — «статус 500» и путь на Диске без URL API (`TestYandexUnexpectedStatus`), дедлайн операции (`TestYandexOperationDeadline`), прод-адрес API (`TestYandexDefaultBaseURL`), уровни каталогов при вложенном корне (`TestYandexDirsToCreate`), отсутствие `href`/подписи/токена в тексте ошибки (`TestYandexHrefNotInError`). Сетевых вызовов в тестах нет: только `httptest` (в двух тестах — внутри `httptest`, закрытый сервер и зависший хендлер).

## TDD-доказательства

**RED (шаг 1–2 брифа)** — `go test -run TestYandex -count=1 ./archive/` после `archive/yandex_test.go` с happy-path, до `archive/yandex.go`:

```
# github.com/kmlebedev/clickhouse-import-rosstat/archive [github.com/kmlebedev/clickhouse-import-rosstat/archive.test]
archive/yandex_test.go:101:33: undefined: YandexDisk
archive/yandex_test.go:102:9: undefined: NewYandex
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/archive [build failed]
```

Ожидаемо: ровно предсказанное брифом `undefined: NewYandex` (плюс `undefined: YandexDisk` — тип назван в конструкторе-хелпере теста); всё остальное в пакете уже собрано, значит падение вызвано только отсутствием реализации.

**GREEN** — `go test -run TestYandex -count=1 -v ./archive/` сразу после `archive/yandex.go`: `--- PASS: TestYandexPutGetExists (0.00s)`, `ok … 0.474s`. Затем шаг 4 брифа (остальные тесты) — `go test -race -count=1 ./archive/` → PASS, 25/25.

**RED (правка `FromEnv`, ruling T1)** — при возвращённой заглушке T1 (временно, до восстановления `git checkout -- archive/fromenv.go`):

```
--- FAIL: TestFromEnvYandexRoot (0.00s)
    --- FAIL: TestFromEnvYandexRoot/корень_по_умолчанию (0.00s)
        fromenv_test.go:106: FromEnv ARCHIVE_BACKEND=yandex: ARCHIVE_BACKEND=yandex: бэкенд Диска не реализован (корень disk:/rosstat-archive)
    --- FAIL: TestFromEnvYandexRoot/корень_из_переменной (0.00s)
        fromenv_test.go:106: FromEnv ARCHIVE_BACKEND=yandex: ARCHIVE_BACKEND=yandex: бэкенд Диска не реализован (корень disk:/custom-archive)
FAIL
```

Ожидаемо: переписанный тест требует от `yandex`-ветки хранилище, а заглушка отдаёт ошибку. После возврата `NewYandex(...)` — `go test -race -count=1 ./archive/` → `ok … 1.373s`; рабочее дерево сверено (`git status --short` — только чужая `deploy/staging/dagu.service`).

**RED (self-review, утечка `href` в текст ошибки)** — правка `transportError` для ошибки разбора URL (см. «находки self-review» №1) проверена откатом: без неё

```
    --- FAIL: TestYandexHrefNotInError/ссылка_не_парсится (0.00s)
        yandex_test.go:554: в тексте ошибки "загрузка disk:/rosstat-archive/cbr_m2/8f3c1d2e4a5b6c7d_x.xlsx: parse \"://bad/upload-target/1?signature=SIGNED_QUERY_SECRET\": missing protocol scheme" есть "://bad/upload-target/1?signature=SIGNED_QUERY_SECRET"
FAIL
```

то есть тест не вакуумный: он ловит именно ту утечку, ради которой написан.

## Находки self-review

1. **Утечка подписанной ссылки в текст ошибки (важно).** `http.NewRequestWithContext` на неразбираемом URL возвращает `*url.Error`, чей `Error()` содержит URL; для `href`-запроса URL — это подписанная ссылка. Изначально разворачивал в причину (`transportError`) только ошибку `hc.Do`, а ошибку построения запроса оборачивал как есть → подпись утекла бы в лог. Исправлено (`transportError` применён и там), доказано RED/GREEN выше, покрыто подтестом `TestYandexHrefNotInError/ссылка_не_парсится`.
2. **`409` как успех — только потому, что родитель гарантированно создан.** У ручки ресурсов `409` означает и «уже существует», и, у настоящего API, отсутствие родителя — второй случай молча прошёл бы, а сломалось бы позже на загрузке с `404` (текст ошибки уже не про каталог). Поэтому `Put` заводит **все** уровни пути от корня, а не одну папку: корень на dev/приёмке бывает вложенным. Покрыто `TestYandexDirsToCreate` и `TestYandexFoldersIdempotent`.
3. **Пустая ссылка в ответе** — отдельная ошибка («Диск не вернул ссылку»), иначе дежурный получил бы «unsupported protocol scheme» вместо «Диск не выдал ссылку».
4. **Метод `Exists` пинится тестом** (`GET resources`): эмулятор различает `PUT`/`GET` по методу, и подмена метода на `POST` сломала бы тест, а не прод молча.
5. **Сон между попытками прерывается контекстом** (`sleep`), иначе отменённый прогон ждал бы паузу повтора зря.
6. **Токен не уходит на `href`** — пин `assertYandexAuth` в happy path и после `Get`/`Exists` (5 запросов к API с токеном, 2 по ссылке без него).
7. Мелкое: убраны два «мусорных» комментария/вызова из черновика тестов (`f.failNext(fakeUpload)` без кодов, несуществовавший `hrefOverride`), добавлено поле `uploadHref` + метод `overrideUploadHref` — в отчёте оставлено, чтобы было видно, что черновик не ушёл в коммит как есть.

YAGNI: не добавлено ни валидации корня, ни nil-guard для `hc` (прод проходит non-nil `util.HttpClient`), ни S3-абстракций, ни логирования (логи архивации — Task 4, `util`), ни листинга/удаления объектов.

## Изменённые файлы (коммит `941921d`)

```
 archive/fromenv.go      |  14 +-
 archive/fromenv_test.go |  34 ++-
 archive/yandex.go       | 320 ++++++++++++++++++++++++++++++++++++++++
 archive/yandex_test.go  | 639 ++++++++++++++++++++++++++++++++++++++++++++++++
 4 files changed, 990 insertions(+), 17 deletions(-)
```

`deploy/staging/dagu.service` — незакоммиченная правка владельца, не тронута (осталась в рабочем дереве).

## Сомнения

1. **«Весь шаг повторяется» понято как повтор HTTP-шага.** Бриф: «весь шаг повторяется до 3 попыток (`retrySleeps` между ними) на статусах `429`/`503`». Реализован повтор на уровне отдельного HTTP-запроса (`mkdir`/`upload`-ссылка/тело/`download`-ссылка/скачивание/наличие) — это ровно то, что проверяет `TestYandexRetriesOn429` («первый `upload`-ответ `429`, второй `200`»), и это дешевле повтора всей операции: тело файла не перекачивается. Если задумывался повтор всей операции `Put`/`Get`, цена правки — обёртка вокруг метода, поведение тестов не изменится (кроме счётчика запросов к `mkdir`).
2. **Создание корня архива на Диске.** Бриф говорит «создать родительские папки», ничего не говоря о самом `ARCHIVE_YANDEX_DIR`. Я создаю и корень (и все промежуточные уровни вложенного корня): иначе первый прогон на новом корне падал бы, а архив пришлось бы заводить руками. Лишний запрос на `Put` (и `409` на повторах) — принято осознанно; если контроллер считает, что корень заводит владелец на Диске, правка — одна строка в `Put` (не обходить `dirsToCreate` от корня).
3. **`409` у настоящего API не различим по коду.** Эмулятор считает `409` только «уже существует»; у живого API `409` бывает и «родитель не найден» (docs-страницы Яндекса отдаются JS и текст подтверждения из них вытащить не удалось — `create-folder.html` не извлекается). От этого риска защищает порядок создания уровней (см. находку 2), но подтвердить семантику кодов можно только живым прогоном — это шаг 3 приёмки Task 5.
4. **Живой Диск не проверялся** (в задаче этого и не требуется): сеть в тестах не используется вообще, эмулятор отдаёт те же коды, которые описаны в спеке §3.1. Инъекция `hc` (прод — `util.HttpClient` с национальными TLS-сертификатами) в живом режиме не проверена — это Task 5.
5. **`retrySleeps`/`opTimeout` — глобальные переменные** (как `fileTimeout` в `util`), тесты подменяют их без `t.Parallel()`. Если в пакете появятся параллельные тесты, подмену надо будет изолировать (например, полем `YandexDisk`), но бриф требует именно переменную.
