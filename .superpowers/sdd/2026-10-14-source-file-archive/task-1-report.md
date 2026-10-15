# Task 1 — отчёт: пакет `archive/` (контракт, ключ, локальный бэкенд, конфигурация)

- **Статус: DONE_WITH_CONCERNS** (см. «Сомнения» — одна неоднозначность брифа, решена явной заглушкой + след для Task 2).
- Ветка: `source-file-archive`, база `cd76576`. Коммит: `47be21a add archive store contract, key scheme and local backend` (только 7 файлов пакета `archive/`).
- Модель/Effort: deepseek/deepseek-flash (диспатч контроллера).

## Что реализовано

| Файл | Что |
|---|---|
| `archive/store.go` | Package-комментарий (зачем архив), интерфейс `Store` (`Name`/`Put`/`Get`/`Exists`) с комментарием «контракт минимален под Яндекс.Диск сегодня и S3 (`PutObject`/`GetObject`/`HeadObject`) — потом; ключи относительные, корень навешивает бэкенд»; sentinels `ErrAuth` («токен не принят (401)») и `ErrQuota` («квота или лимит хранилища (403/413/423/507)») — с пояснением, что обёртываются через `%w` и различаются `errors.Is`, потому что действия дежурного по логу разные |
| `archive/key.go` | `Key(source, basename, fingerprint string, sum [sha256.Size]byte) string`: непустой отпечаток → `source/<fp[:16]>_<имя>`; пустой → `source/sha-<hex(sum)[:16]>_<имя>`. `prefix16` — короткий отпечаток отдаётся как есть (нет паники, нет пустого префикса). `sanitizeName` — `/ \ : * ? " < > \|` и `unicode.IsControl` → `_`, кириллица сохраняется, обрезка до 200 байт **по границе руны** (`utf8.RuneStart`), пустое → `file` (константа `keyNameFallback`). Компонент ключа ≤ 221 байта ≤ 255 |
| `archive/local.go` | `LocalStore{Dir}` + `NewLocal`; `Put` — `os.MkdirAll` родителя (0o755) + `os.WriteFile` (0o644); `Get` — `os.ReadFile`; `Exists` — `os.Stat`, `os.ErrNotExist` → `(false, nil)`. Защита от выхода за каталог: `path(key)` = `filepath.Join(abs(Dir), FromSlash(key))` и проверка префикса `abs(Dir)+separator`. `var _ Store = (*LocalStore)(nil)` |
| `archive/fromenv.go` | Константы `EnvBackend`/`EnvYandexToken`/`EnvYandexDir`/`EnvLocalDir`, `DefaultYandexDir = "disk:/rosstat-archive"`, `FromEnv(hc *http.Client)`: пусто → `(nil, nil)`; `local` → `NewLocal(ARCHIVE_LOCAL_DIR)`, пусто → ошибка с именем переменной; `yandex` → токен обязателен (ошибка с именем переменной, без значения), корень из `ARCHIVE_YANDEX_DIR` или `DefaultYandexDir`; неизвестное значение → ошибка с именем и значением |
| Тесты | `archive/key_test.go`, `archive/local_test.go`, `archive/fromenv_test.go` — in-package, офлайн: только `t.TempDir()` и `t.Setenv`, ни одного сетевого вызова (httptest не понадобился: в T1 нет ни одного HTTP-вызова) |

Новых зависимостей нет (только stdlib). Схема ClickHouse не трогалась. Секреты — только `os.Getenv`; в тексты ошибок и тесты попадает лишь **имя** переменной, значение — никогда.

## TDD-доказательства

**RED (ключ)** — `go test -count=1 ./archive/` до `archive/key.go`:

```
# github.com/kmlebedev/clickhouse-import-rosstat/archive [github.com/.../archive.test]
archive/key_test.go:25:9: undefined: Key
archive/key_test.go:31:5: undefined: Key
archive/key_test.go:42:9: undefined: Key
archive/key_test.go:48:5: undefined: Key
archive/key_test.go:67:11: undefined: Key
archive/key_test.go:80:10: undefined: Key
archive/key_test.go:102:10: undefined: Key
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/archive [build failed]
```

Ожидаемо: это ровно предсказанный брифом `undefined: Key`; все прочие символы на месте (`store.go` уже написан), значит падение вызвано только отсутствием реализации ключа.

**RED (local, шаг 5 сделан тоже «тест первым»)** — `go test -count=1 -run TestLocal ./archive/`:

```
archive/local_test.go:17:11: undefined: NewLocal
archive/local_test.go:48:11: undefined: NewLocal
archive/local_test.go:69:11: undefined: NewLocal
archive/local_test.go:91:11: undefined: NewLocal
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/archive [build failed]
```

**RED (fromenv, шаг 6)** — `go test -count=1 -run TestFromEnv ./archive/`:

```
archive/fromenv_test.go:21:11: undefined: EnvBackend
archive/fromenv_test.go:23:16: undefined: FromEnv
archive/fromenv_test.go:25:37: undefined: EnvBackend
... (too many errors — только undefined: Env*/FromEnv)
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/archive [build failed]
```

**GREEN** — после реализации, на закоммиченном состоянии:

```
$ gofmt -l archive/
(пусто)
$ go vet ./archive/
(пусто, exit 0)
$ golangci-lint run ./archive/
0 issues.
$ go test -race -count=1 ./archive/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/archive	1.256s
```

`-v` показывает **14/14 PASS** (top-level) + **8/8 PASS** (subtests: 6 в `TestKeySanitizesBasename`, 2 в `TestFromEnvYandexRoot`), вывод чистый — ни `log`-шума, ни предупреждений, ни сетевых обращений.

Покрытие требований брифа тестами: `TestKeyFromFingerprint`, `TestKeyWithoutFingerprintUsesSHA`, `TestKeySanitizesBasename` (разделители пути / запрещённые в именах / управляющие / кириллица / пустое имя / обрезка длинного имени по границе руны + ≤255), `TestKeyFingerprintShorterThan16`, `TestLocalRoundTrip`, `TestLocalMissingKey`, `TestLocalRejectsParentKey` (ошибка + файл вне каталога не создан), `TestLocalNestedKey`, `TestFromEnvDisabled`, `TestFromEnvLocal`, `TestFromEnvLocalWithoutDir`, `TestFromEnvYandexRequiresToken`, `TestFromEnvYandexRoot` (корень из переменной и из default + отсутствие значения токена в тексте ошибки), `TestFromEnvUnknownBackend`.

Дополнительно проверено, чего бриф не требовал, но что важно для T2–T4: `Key` с непустым отпечатком **игнорирует** `sum` (иначе `versionExists` в T4 с `[32]byte{}` не находил бы объект, записанный `save`).

## Изменённые файлы

Только новые файлы пакета, 622 строки:

```
archive/store.go       |  38 +
archive/key.go         |  77 +
archive/local.go       |  95 +
archive/fromenv.go     |  63 +
archive/key_test.go    | 107 +
archive/local_test.go  | 112 +
archive/fromenv_test.go| 130 +
```

`git status` после коммита: ` M deploy/staging/dagu.service` — незакоммиченная правка владельца, не трогал и не коммитил.

## Находки self-review (починены до отчёта)

1. **Пустая строка внутри интерфейса** `Store` (комментарий `Get` оторвался от метода после правки) — убрана.
2. **Нечёткие комментарии-константы**: `keyNameMaxBytes` (приписывал константе правило границы руны), `keyPrefixHex` (не объяснял, почему 16), `prefix16` (ссылался на «текст ошибки», которого нет) — переписаны «по почему».
3. **`token := os.Getenv(...)`** в yandex-ветке читалась, но дальше не использовалась — заменено на проверку `os.Getenv(EnvYandexToken) == ""`, комментарий про секрет перенесён к проверке.
4. **Неверная мотивировка `NewLocal`** («импортёр с выключенным архивом не оставляет пустых каталогов» — до `NewLocal` дело не доходит вовсе) — переписана: каталог заводит `Put`, дерево содержит только реально сохранённые версии.
5. **Package-level `testClient` в тестах** — убран: имя слишком «общее» для пакета, куда T2/T3 добавят свои тесты (риск конфликта объявлений). Вместо него inline `&http.Client{}` и одно пояснение в `TestFromEnvDisabled`.
6. **Ложное срабатывание защиты пути при `Dir = "."` / `Dir = ""`**: буквальная формула брифа (`Join(dir, …)` против `Clean(dir)+sep`) отвергла бы валидный ключ, потому что `Join(".", "x") == "x"` теряет `./`. В `path` сравниваются **абсолютные** пути (`filepath.Abs(Dir)` + `Join` + префикс с `separator`) — инвариант брифа («ключ с `..` → ошибка») сохранён и покрыт тестом, но нет ложных отказов при относительном `Dir`. Отмечено как отступление от буквы брифа с обоснованием в комментарии.
7. Проверено глазами: YAGNI (никаких лишних экспортов — ни констант-имён бэкендов, ни «на будущее»); тесты проверяют поведение (байты, ошибки, файлы на диске), а не моки (моков нет вовсе); `Key` применяет санитизацию только к basename — источник (`source`) приходит из кода (`TableName`/`PublicationSource`), и его защищает сам бэкенд (`path` в `LocalStore`).

## Сомнения / проблемы

1. **Главное: yandex-ветка `FromEnv` в T1 физически не может отдать `Store`.** Бриф (шаг 6) требует от `FromEnv` разобрать `ARCHIVE_BACKEND=yandex` (токен обязателен, корень из `ARCHIVE_YANDEX_DIR`/`DefaultYandexDir`), но конструктор `NewYandex` появляется только в Task 2 (`archive/yandex.go`), а RED-шаг T2 прямо ждёт `undefined: NewYandex` в начале T2. Одновременно: шаг 7 T1 требует `go test ./archive/` зелёным. Эти три требования совместны только если в T1 yandex-ветка **не собирает** хранилище.
   Решение (выбрано осознанно, не «угадано молча»): ветка валидирует токен и корень и возвращает ошибку `ARCHIVE_BACKEND=yandex: бэкенд Диска не реализован (корень disk:/rosstat-archive)`. Возврат `(nil, nil)` рассматривался и отвергнут: он выглядел бы для вызывающего (T4 `activeArchive`) как «архивация включена и работает», и пропажу версий заметили бы только при попытке восстановления. Возврат «пустого» store — тем более.
   **След для Task 2 (одна строка + один тест):** в `archive/fromenv.go` заменить `return nil, fmt.Errorf(...)` на `return NewYandex(token, yandexRoot(), "", hc), nil` (сигнатура T2: `NewYandex(token, root, baseURL string, hc *http.Client) *YandexDisk`; `yandexRoot()` уже реализует разбор `ARCHIVE_YANDEX_DIR`/`DefaultYandexDir`) и переписать `TestFromEnvYandexRoot` на проверку корня через полученное хранилище (сейчас он пинит текущее поведение: ошибка незакрытой конфигурации + отсутствие значения токена в тексте). Прошу контроллера подтвердить этот рулинг или заменить его своим — вопрос возник из-за несовместимости шагов 6 и 7 брифа с RED-шагом T2, спросить до начала работы возможности не было (в моём тул-наборе нет инструмента вопроса).
2. Мелочь: `hc` в `FromEnv` пока не используется ни в одной ветке (в `yandex` пойдёт в `NewYandex`); параметр оставлен по сигнатуре брифа и подписан комментарием.
3. Мелочь: в `sanitizeName` управляющие символы заменяются на `_`, а не удаляются, поэтому «пустое после санитизации → `file`» срабатывает только для пустого basename (что и покрыто тестом). Так и было задумано брифом («пустое после санитизации»); отмечаю на случай иного чтения.

## Fix round 1

Ревью вернуло один Important и два минора; починены все три. Коммит: `58bdd56 make local archive writes atomic` (4 файла пакета `archive/`, 107 вставок / 6 удалений; правка владельца `deploy/staging/dagu.service` не тронута).

### 1. Important — `LocalStore.Put` писал неатомарно (`os.WriteFile`)

`archive/local.go`:

- `Put` теперь зовёт новый `writeFileAtomic(p, body)` вместо `os.WriteFile(p, body, 0o644)`; целевой каталог по-прежнему создаёт `MkdirAll` в `Put`, текст ошибки для вызывающего не изменился (`archive local: запись <ключ>: <причина>`).
- `writeFileAtomic`: `os.CreateTemp(dir, "*.tmp")` **в каталоге целевого объекта** (rename атомарен только в пределах одной ФС; в чужом каталоге он выродился бы в копирование) → `Write` → `Chmod(0o644)` → `Sync` → `Close` → `os.Rename`. При любой неудаче временный файл удаляется (`os.Remove`), ошибка оборачивается причиной (`запись:`/`права:`/`синхронизация:`/`переименование:`). `Sync` до `rename` добавлен осознанно: без него объект может пережить переименование лишь в кэше ФС и не дойти до диска при падении машины — ровно та потеря версии, ради которой всё и делается.
- `Chmod(0o644)` вместо «как отдаст `CreateTemp`»: `CreateTemp` создаёт файл `0600`, а бриф предписывал объекту `0644`; без правки молча изменились бы права всех объектов архива. В комментарии это названо явно.

Тест (написан до правки, см. ниже о RED): `TestLocalPutIsAtomic` + хелпер `assertNoTempLeftovers` в `archive/local_test.go` — после `Put` в каталоге объекта нет `*.tmp`; объект читается целиком; права объекта `0644`; повторный `Put` того же ключа с новым содержимым отдаёт **новое** содержимое (без хвоста прежней версии).

О RED честно: для этого класса дефекта падающего теста получить нельзя — «запись оборвалась на середине» внутри процесса не воспроизводится (нет точки, где можно прервать `Write`), и тест проходит и на прежнем `os.WriteFile`, и на новом коде. Поэтому он документирует контракт `Put` (нет мусора, перезапись вместо дозаписи, права), а сам фикс — структурный; его предмет (усечённый объект как «версия есть» и молча пропущенная архивация) проверяется рассуждением и ревью, а не тестом. Прогнал его до правки — PASS, после правки — PASS:

```
$ go test -count=1 -run 'TestLocalPutIsAtomic|TestLocal' -v ./archive/     # ДО правки local.go
--- PASS: TestLocalPutIsAtomic (0.00s)   # (и остальные TestLocal* — PASS)
```

### 2. Minor — ложный комментарий в `archive/key.go`

«Управляющие символы убираются вместе с ними» → «…разделители пути и запрещённые в именах символы заменяются на `_` — вместе с ними на `_` заменяются и управляющие символы: имя с переводом строки ломает и лог, и список объектов». Код не менялся (поведение действительно замена на `_`, а не удаление).

### 3. Minor — неверный размер в комментарии `archive/key_test.go`

Было «„a“ + 100 „я“ (201 байт)», стало «basename здесь — 206 байт (1 на „a“ + 200 на сто „я“ + 5 на „.xlsx“), и обрезка даёт 199 байт». Ожидания теста не менялись — они и были верны.

### Проверки после починки (на закоммиченном состоянии)

```
$ gofmt -l archive/
(пусто)
$ go vet ./archive/
(пусто, exit 0)
$ golangci-lint run ./archive/
0 issues.
$ go test -race -count=1 ./archive/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/archive	1.412s
```

`-v`: **15/15 PASS** (top-level, +1 новый `TestLocalPutIsAtomic`) и **8/8 PASS** (subtests), ни одного FAIL, вывод чистый — без лог-шума и без сетевых вызовов. Итого в пакете: 15 тестов, 23 проверки с подтестами.
