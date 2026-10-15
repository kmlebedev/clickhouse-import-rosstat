# Task 1 report — `util.PostJSON` (POST JSON для LLM-вызовов)

**Status:** DONE
**Commit:** `93da75b` — `util: PostJSON helper for LLM calls (deadline, status check)` (ветка `feat/cbr-mp-summary`, BASE `1e1b310` + этот коммит)
**Files:** `util/json.go` (+58), `util/json_test.go` (+169) — ровно эти два файла в индексе.

## Что сделано

TDD-цикл по брифу (`task-1-brief.md`), шаги 1–5.

### Step 1 — тесты (`util/json_test.go`, 6 новых функций, `-run TestPostJSON` ловит все)

| Тест | Что пинит |
|---|---|
| `TestPostJSONDecodesBody` | 200 + `{"value":42}` → поле заполнено; в хендлере: метод строго POST, `User-Agent` непустой (randomUA), `Content-Type: application/json`, тело запроса — JSON и содержит `model: test-model` (т.е. сериализация `request`, а не query) |
| `TestPostJSONSendsHeaders` | `Authorization: Bearer test` из `headers` доезжает до сервера |
| `TestPostJSONNonOKStatusFails` | 500 с телом `{"error":"llm down"}` → ошибка содержит `500` и начало тела; **не** содержит URL сервера (конвенция GetJSON — не светить query в логе) |
| `TestPostJSONMalformedBodyFails` | 200 + `<html>blocked</html>` → ошибка разбора, не тихий ноль |
| `TestPostJSONEmptyBodyFails` | 200 + пустое тело → ошибка (требование интерфейса брифа, в списке Step 1 не названо явно — добавил, т.к. путь есть в интерфейсе) |
| `TestPostJSONTimeoutFails` | `postJSONTimeout = 50 * time.Millisecond` (присваивание переменной, не env) + спящий хендлер → `errors.Is(err, context.DeadlineExceeded)` |

Добавлен импорт `encoding/json` в тестовый файл; формат остальных импортов не тронут.

### Step 2 — RED (до реализации)

```
$ go test ./util/ -run TestPostJSON -v
# github.com/kmlebedev/clickhouse-import-rosstat/util [build failed]
util/json_test.go:178:12: undefined: PostJSON
util/json_test.go:206:12: undefined: PostJSON
util/json_test.go:228:9:  undefined: PostJSON
util/json_test.go:253:12: undefined: PostJSON
util/json_test.go:267:12: undefined: PostJSON
util/json_test.go:288:9:  undefined: postJSONTimeout
util/json_test.go:289:2:  undefined: postJSONTimeout
util/json_test.go:290:21: undefined: postJSONTimeout
util/json_test.go:295:9:  undefined: PostJSON
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/util [build failed]
```

FAIL ровно по брифу (`undefined: PostJSON` + ещё `postJSONTimeout`, т.к. тест таймаута подставляет переменную).

### Step 3 — реализация (`util/json.go`, рядом с `GetJSON`)

- `var postJSONTimeout = 65 * time.Second` — переменная по образцу `jsonTimeout` (свой комментарий: почему 65 s, а не минута, и зачем дедлайн — деградация в регекс-слой вместо зависания шага dagu).
- `func PostJSON(url string, headers map[string]string, request any, out any) error` — сигнатура дословно из брифа:
  - `json.Marshal(request)` → `http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))`;
  - `req.Header.Set("User-Agent", randomUA)` (конвенция пакета: GetFile/GetXML/GetJSON/HeadFingerprint — тест пиннит это), `Content-Type: application/json`, затем поверх — переданные `headers` (ключ LLM в `Authorization`);
  - `HttpClient.Do(req)`, `defer resp.Body.Close()`, `io.ReadAll`;
  - не-2xx (`< 200 || >= 300`) → `fmt.Errorf("http status %d: %s", code, errorSnippet(body))` — та же структура текста ошибки, что у `GetJSON`, обрезка теми же `maxErrorSnippet`/`emptyBodySnippet`; URL в ошибку не пишется, заголовки в обёртки не подставляются;
  - пустое тело (200) → ошибка `"пустое тело"`; иначе `json.Unmarshal(body, out)`.
- Добавлен импорт `bytes`.
- Никаких сетевых вызовов в `init()`, новых зависимостей нет (`go.mod` не тронут).

### Step 4 — GREEN

```
$ go test ./util/ -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	2.678s
$ gofmt -l util/     # пусто
$ go vet ./util/     # пусто
```

Дополнительно (жёсткое правило 9 AGENTS.md), фоном:

```
$ make all
0 issues.                      # golangci-lint
go vet ./...                   # чисто
go test -race ./...            # все пакеты ok (util 5.234s, cbr 17.346s, wgc 48.299s)
CGO_ENABLED=0 ... go build ... # build и build-ingest собраны
exit code 0
```

IDE-проверка (GoLand MCP): `get_file_problems` на `util/json.go` и `util/json_test.go` — `errors: []` у обоих.

### Step 5 — commit

```
$ git status --short
 M deploy/staging/dagu.service   # чужая незакоммиченная правка — не тронута и НЕ в индексе
 M util/json.go
 M util/json_test.go
$ git add util/json.go util/json_test.go && git commit -m "util: PostJSON helper for LLM calls (deadline, status check)"
[feat/cbr-mp-summary 93da75b] util: PostJSON helper for LLM calls (deadline, status check)
 2 files changed, 227 insertions(+)
```

Сообщение коммита — дословно из брифа. `deploy/staging/dagu.service` остаётся незакоммиченным (`M`, не в индексе) — как и требовалось.

## Решения, выходящие за букву брифа (для ревьюера)

1. **`User-Agent: randomUA`.** Бриф не называл этот заголовок явно, но правило 4 AGENTS.md («ротация User-Agent») и все четыре соседних хелпера (`GetFile`, `GetXML`, `GetJSON`, `HeadFingerprint`) его ставят; тест `TestPostJSONDecodesBody` пиннит непустой UA, как в `TestGetJSONDecodesBody`. Если для LLM-провайдера это нежелательно — снимается одной строкой.
2. **Проверка статуса — диапазоном 2xx**, а не `!= 200` как у `GetJSON`: бриф прямо говорит «не-2xx». Текст ошибки при этом неотличим от `GetJSON` (`http status %d: %s`).
3. **Шестой тест на пустое тело** — поведение объявлено в интерфейсе брифа («пустое тело — ошибка»), отдельного теста в Step 1 для него не было.
4. **Ассерт «URL отсутствует в тексте ошибки»** в тесте не-200 — конвенция `GetJSON`; для LLM URL секрета не несёт (ключ в заголовке), но хелпер общий и конвенцию сохраняем.

## Сомнения

- Секретов нигде нет: ключ передаётся вызывающим в `headers`, хелпер его ни в ошибки, ни в логи не пишет; транспортные `*url.Error` содержат только URL (без заголовков) — это проверено чтением кода net/http, отдельным тестом не пиннится.
- `postJSONTimeout` как package-level переменная: тесты `TestGetJSONTimeoutFails`/`TestPostJSONTimeoutFails` мутируют разные переменные и не параллельны (`t.Parallel()` в пакете нет) — гонки нет, `make all` с `-race` зелёный. Если в пакете когда-нибудь появятся параллельные тесты, обе подмены надо будет изолировать.
- 65 s для LLM — из брифа; в живом вызове (Task 5) реальное время ответа не измерялось.

---

## Fix round 1 (вакуумный ассерт User-Agent) — DONE

**Находка ревьюера (Important, мутационно доказанная):** `util/json_test.go:149–150` проверял
`r.Header.Get("User-Agent") == ""`, но Go-клиент сам подставляет `Go-http-client/1.1`, когда заголовок не
задан, — поэтому мутация «удалить `req.Header.Set("User-Agent", randomUA)` из `util/json.go`» проходила все
`TestPostJSON` зелёными, и регрессия ротации UA (правило 4 AGENTS.md) не ловилась.

**Что изменено (только `util/json_test.go`):**

- `TestPostJSONDecodesBody`: ассерт стал проверкой вхождения в список пакета —
  `slices.Contains(userAgents, ua)` (список из `util/xls.go`, из которого берётся `randomUA`); добавлен
  импорт `slices`. Проверка на непустоту убрана как вакуумная, в тексте `t.Errorf` объяснено почему.
- Способ выбран именно membership, а не сравнение с `randomUA` или с литералом UA: при смене/расширении
  списка `userAgents` тест не станет хрупким — он принимает любой UA из текущего списка и падает только на
  чужом (в т.ч. на дефолтном `Go-http-client/1.1`).
- `util/json.go` в этом раунде не менялся (мутация была временной и возвращена; `git diff util/json.go` пуст).

**Команды и вывод.**

Базовая проверка после фикса (PASS):

```
$ go test ./util/ -run TestPostJSON -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	2.516s
$ gofmt -l util/     # пусто
```

Red-green доказательство на мутации — временно удалена строка `req.Header.Set("User-Agent", randomUA)`
в `util/json.go` (импорт `randomUA` остался, сборка проходит):

```
$ go test ./util/ -run TestPostJSON -count=1
--- FAIL: TestPostJSONDecodesBody (0.00s)
    json_test.go:151: User-Agent = "Go-http-client/1.1" — не из списка userAgents: хелпер обязан слать randomUA, как GetJSON. Проверка на непустоту тут вакуумна: без установки заголовка Go сам шлёт Go-http-client/1.1, и удаление строки req.Header.Set("User-Agent", randomUA) прошло бы зелёным
FAIL
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/util	2.546s
```

Строка возвращена на место, повторный прогон (PASS) + полный пакет + gofmt + vet:

```
$ git diff --stat util/json.go     # пусто — файл вернулся к состоянию коммита 93da75b
$ go test ./util/ -run TestPostJSON -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	2.252s
$ go test ./util/ -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	2.450s
$ gofmt -l util/     # пусто
$ go vet ./util/     # пусто
```

GoLand MCP `get_file_problems` на `util/json_test.go` — `errors: []`.

**Коммит:**

```
$ git status --short
 M deploy/staging/dagu.service   # чужая правка, не тронута и не в индексе
 M util/json_test.go
$ git add util/json_test.go && git commit -m "util: fix PostJSON UA test to catch missing User-Agent"
[feat/cbr-mp-summary 8ebf39c] util: fix PostJSON UA test to catch missing User-Agent
 1 file changed, 5 insertions(+), 2 deletions(-)
```

**Хэш фикс-коммита: `8ebf39c`** (поверх `93da75b`); после коммита `go test ./util/ -count=1` → `ok ... 2.435s`,
`git status --short` — только незакоммиченный `M deploy/staging/dagu.service`.

**Сомнения:** те же вакуумные проверки UA остаются в легаси-тестах `TestGetJSONDecodesBody`
(`util/json_test.go`), `TestGetXMLDecodesBody`, `TestGetFileReturnsBody` — вне объёма этого фикс-раунда
(ревьюер отметил только PostJSON); если решать системно, это отдельная задача на весь пакет `util`.
