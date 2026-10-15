# Task 5 — отчёт: режим `ARCHIVE_ACTION=registry`, Makefile, staging-env, git-зеркало

**Статус: DONE** (ожидает ревью контроллера)

## Что реализовано

1. **`main.go` — режим одного действия.** В начале `main()`, сразу после `log.ParseLevel` и **до**
   `clickhouse.ParseDSN`: непустой `ARCHIVE_ACTION` уводит процесс в режим и завершает его.
   - `ARCHIVE_ACTION=registry` → `runArchiveRegistry(ctx)`; любое другое непустое значение →
     `log.Fatalf("неизвестное ARCHIVE_ACTION (ожидается registry)")` с кодом 1.
   - Константы `archiveActionRegistry = "registry"`, `envArchiveRegistryOut = "ARCHIVE_REGISTRY_OUT"`,
     `defaultRegistryOut = "docs/archive/registry.jsonl"` — рядом с `ctx`, как и прочие имена
     переменных окружения в репозитории (ср. `archive/fromenv.go`).
   - `runArchiveRegistry`: `archive.FromEnv(util.HttpClient)`; `store == nil` → ошибка
     `fmt.Errorf("%s не задан", archive.EnvBackend)` → «ARCHIVE_BACKEND не задан» (без значения
     переменной, текст собран из константы пакета `archive`, поэтому чинится вместе с ней);
     `store.Exists(ctx, archive.RegistryKey)`; нет файла → `log.Warnf("реестр архива пуст: %s", archive.RegistryKey)`
     и запись пустого файла; есть → `store.Get`; `os.MkdirAll(filepath.Dir(out), 0o755)` +
     `os.WriteFile(out, body, 0o644)`; итог — `log.Infof("реестр архива: %d строк -> %s", bytes.Count(body, []byte("\n")), out)`.
   - Ошибки режима возвращаются из функции и завершают процесс через `log.Fatal(err)` — стиль
     существующего `main()`; код возврата 1.
2. **`Makefile`.** Цель `archive-registry` (сборка `-native` как в `import`, запуск
   `ARCHIVE_ACTION=registry ./$(BUILD_DIR)/$(BINARY)-native`) — вставлена сразу после `import`;
   `archive-registry` добавлена в `.PHONY`; в `env-check` — строка
   `ARCHIVE_YANDEX_TOKEN задан / не задан (нужен при ARCHIVE_BACKEND=yandex)` в стиле соседних
   (`@if [ -n "$$ARCHIVE_YANDEX_TOKEN" ]; then echo "OK   ..."; else echo "НЕТ  ..."; fi`).
3. **`deploy/staging/clickhouse-import-rosstat.env.example`.** Блок архивации в стиле файла:
   три переменные добавлены и в шапку-реестр (`ARCHIVE_BACKEND` / `ARCHIVE_YANDEX_TOKEN` /
   `ARCHIVE_YANDEX_DIR`), и в абзац обязательных пометок (`ARCHIVE_YANDEX_TOKEN ОБЯЗАТЕЛЕН при
   ARCHIVE_BACKEND=yandex: … импортёр пишет в лог «архив версий файлов выключен» и продолжает без
   архива … При пустом ARCHIVE_BACKEND архивация выключена целиком`), значения —
   `ARCHIVE_BACKEND=yandex`, `ARCHIVE_YANDEX_TOKEN=REPLACE_ME`, `ARCHIVE_YANDEX_DIR=disk:/rosstat-archive`.
4. **`docs/archive/README.md`** (новый). Что это git-зеркало, а истина — `registry.jsonl` в
   хранилище; файлы архива в git не хранятся; как обновлять (`make archive-registry`,
   `[ARCHIVE_BACKEND=yandex|local]`, `ARCHIVE_REGISTRY_OUT`); коммит ручной/периодический —
   на staging нет git-дерева, автокоммит невозможен; различие «пустой реестр в настроенном
   хранилище — законный результат» и «пустой ARCHIVE_BACKEND — ошибка режима»; таблица 8 полей
   `archive.Entry` (`source`, `name`, `fingerprint`, `sha256`, `size`, `url`, `key`, `first_seen`)
   и пример строки.
5. `docs/archive/registry.jsonl` **не создавался** — проверено `ls -A docs/archive` → только `README.md`.

## Проверки (все — руками, вывод вставлен)

**(а) `make archive-registry` без `ARCHIVE_BACKEND` → ненулевой код и понятная ошибка:**

```
$ env -u ARCHIVE_BACKEND -u ARCHIVE_YANDEX_TOKEN make archive-registry
go build -o build/clickhouse-import-rosstat-native .
ARCHIVE_ACTION=registry ./build/clickhouse-import-rosstat-native
time="2026-10-14T22:24:46+05:00" level=fatal msg="ARCHIVE_BACKEND не задан"
make: *** [archive-registry] Error 1
exit=2
```

**(б) `make env-check` → строка присутствует, оба варианта:**

```
$ make env-check
файл окружения: /Users/whitefox/.config/rosstat/env найден
OK   BEA_API_KEY задан
OK   BLS_API_KEY задан
OK   EIA_API_KEY задан
OK   CLICKHOUSE_URL задан
OK   CLICKHOUSE_PASSWORD задан
OK   ARCHIVE_YANDEX_TOKEN задан            # значение не печатается
OK   GRAFANA_TEST_TOKEN задан
OK   GRAFANA_STAGING_TOKEN задан

$ make env-check ARCHIVE_YANDEX_TOKEN=      # негативная ветка, env-файл не трогался
НЕТ  ARCHIVE_YANDEX_TOKEN не задан (нужен при ARCHIVE_BACKEND=yandex)
```

**(в) режим `registry` не ходит в ClickHouse.** Без `CLICKHOUSE_URL` процесс доходит именно до
ошибки про бэкенд, а не падает на DSN:

```
$ env -u CLICKHOUSE_URL ARCHIVE_ACTION=registry ./build/clickhouse-import-rosstat-native
time="2026-10-14T22:26:41+05:00" level=fatal msg="ARCHIVE_BACKEND не задан"
exit=1                                   # ← про DSN («invalid CLICKHOUSE_URL») речи нет

$ env -u CLICKHOUSE_URL -u ARCHIVE_ACTION ./build/clickhouse-import-rosstat-native
time="2026-10-14T22:25:35+05:00" level=fatal msg="invalid CLICKHOUSE_URL"   # обычный путь не изменён
exit=1
```

Дополнительно (сверх требуемого, тем же бинарником, `ARCHIVE_BACKEND=local`, ClickHouse не запущен
и не упоминается):

```
# пустое хранилище → пустое зеркало, exit 0
level=warning msg="реестр архива пуст: registry.jsonl"
level=info    msg="реестр архива: 0 строк -> /tmp/…/out/registry.jsonl"   # файл 0 байт
exit=0

# хранилище с 2 строками → зеркало байт-в-байт (cmp), повторный прогон идемпотентен
level=info msg="реестр архива: 2 строк -> /tmp/…/out/registry.jsonl"; exit=0
cmp → OK зеркало байт-в-байт = реестр хранилища

# ARCHIVE_BACKEND=yandex без токена → ошибка конфигурации без значения токена
level=fatal msg="ARCHIVE_YANDEX_TOKEN не задан (нужен при ARCHIVE_BACKEND=yandex)"; exit=1

# неизвестное действие
$ ARCHIVE_ACTION=bogus ./build/clickhouse-import-rosstat-native
level=fatal msg="неизвестное ARCHIVE_ACTION (ожидается registry)"; exit=1

# make-плеймбинг цели с local-бэкендом: реестр скачан в указанный ARCHIVE_REGISTRY_OUT
$ make archive-registry ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=$tmp/store ARCHIVE_REGISTRY_OUT=$tmp/out/registry.jsonl
level=info msg="реестр архива: 1 строк -> /tmp/…/out/registry.jsonl"; make exit=0
cmp → OK
```

## Прогон проверок качества

```
gofmt -l main.go                → пусто (exit 0)
go build ./...                  → BUILD_OK
go vet ./...                    → VET_OK
go test -race -count=1 ./util/ ./archive/
  ok …/util    2.469s
  ok …/archive 1.685s
golangci-lint run ./...         → 0 issues (v2.x, стандартный набор, конфиг .golangci.yml)
mcp__jetbrains__ get_file_problems main.go → errors: [] (до и после правок)
```

## TDD-доказательства

Тестов **не писал**: `runArchiveRegistry` живёт в `package main`, у которого тестов нет (сети и
внешних процессов он не требует, но и тестового шва для `archive.FromEnv` в пакете нет —
`fromenv.go` читает `os.Getenv` напрямую, а добавлять тестовый переключатель в `archive/` значило бы
трогать чужой готовый пакет из Task 1). Покрытие — ручное, через (а)–(в) и дополнительные прогоны
выше: обе ветки (`store == nil`, пустой/непустой реестр), неизвестное действие, обычный путь без
изменений, make-цель end-to-end на `local`-бэкенде (без сети и без секретов).

## Изменённые файлы

- `main.go` — режим + `runArchiveRegistry` (импорты `bytes`, `fmt`, `path/filepath`, `archive`, `util`)
- `Makefile` — цель `archive-registry`, `.PHONY`, строка в `env-check`
- `deploy/staging/clickhouse-import-rosstat.env.example` — блок архивации
- `docs/archive/README.md` — новый

Не тронуты: `deploy/staging/dagu.service` (правка владельца, осталась в `git status` как ` M`,
не коммитил), `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`, схема ClickHouse, `archive/`,
`util/`. SQL-запросов к ClickHouse не требовалось — схема не менялась, `mcp__clickhouse__` не звал.

## Self-review (найденное)

- **Обычный прогон не изменён**: диф `main.go` вставляет только блок режима перед `ParseDSN`;
  ветка импортёров, `parseImportFilter`, коды выхода — посимвольно прежние. Подтверждено (в-4):
  без `CLICKHOUSE_URL` обычный путь по-прежнему даёт `invalid CLICKHOUSE_URL`, exit 1.
- **Секреты**: значения не логируются. Значение `ARCHIVE_ACTION` в текст «неизвестное действие»
  не попадает (в отличие от `archive/fromenv.go`, где печатается `ARCHIVE_BACKEND` — там это
  конфигурационный ключ, тут режим ровно один, и печатать нечего); токен в (а)–(в) не выводился,
  `make env-check` печатает только «задан/не задан».
- **YAGNI**: без флагов, без отдельного пакета, без ретраев и без загрузки реестра в ClickHouse;
  три константы вместо литералов по месту (стиль `archive/fromenv.go`).
- **Идемпотентность**: повторный прогон перезаписывает зеркало теми же байтами (проверено `cmp`);
  в целе нет `@echo`-шума, кроме вывода сборки и лога бинарника.
- **Мелочь (осознанно не правил)**: `log.Infof("реестр архива: %d строк -> %s", 1, …)` даёт
  «1 строк» вместо «1 строка» — формат взят дословно из брифа/решения контроллера; плюрализация
  потребовала бы хелпера ради косметики.
- **Мелочь**: `dotenv`-подобного разбора нет — `ARCHIVE_REGISTRY_OUT` без значения и с пробелами
  не тримится; в штатном прогоне переменная не задаётся вовсе.

## Сомнения

1. `os.WriteFile` пишет неатомарно (сделано дословно по решению контроллера): прерванный прогон
   мог бы оставить усечённое зеркало. Для артефакта в git это некритично — следующий прогон
   перезапишет; `archive.LocalStore` для сравнения пишет через temp+rename.
2. Формулировка ошибки «неизвестное ARCHIVE_ACTION» не показывает полученное значение — выбрано
   сознательно по требованию «ошибки без значений переменных»; при отладке смотреть env руками.
3. ~~`ARCHIVE_BACKEND=yandex` в `.env.example` означает, что копия примера (с реальным токеном)
   сразу включает архивацию. Так предписано брифом; альтернатива — `ARCHIVE_BACKEND=` с
   комментарием — отклонена.~~ **Снято в fix round 1 (finding 2)**: строка закомментирована.

---

# Fix round 1

Ревью: два Important (оба plan-mandated) + один свёрнутый минор. Все три починены.

## Что изменил

1. **[Important] Scope токена.** `deploy/staging/clickhouse-import-rosstat.env.example`: в шапке
   теперь `токену нужны ОБА scope: cloud_api:disk.write (запись версий) и cloud_api:disk.read
   (проверка наличия, чтение реестра и скачивание версий)` — по коду это так: `Exists` →
   `GET /resources`, `Get` → `GET /resources/download` (spec §2, л. 40 и §3.5 — там указан
   `cloud_api:disk.write`/`read`). Заодно исправлен план
   `docs/superpowers/plans/2026-10-14-source-file-archive.md` (Step 3, стр. 285): в описании
   scope — «**и** `cloud_api:disk.read`» со ссылкой на л. 40 и §3.5 спеки.
2. **[Important] Пример больше не включает архив с заглушкой.** Строка
   `ARCHIVE_BACKEND=yandex` закомментирована (`#ARCHIVE_BACKEND=yandex`), над ней — инструкция
   включения: «подставьте рабочий токен в `ARCHIVE_YANDEX_TOKEN` и раскомментируйте строку ниже».
   Комментарий блока переписан: пустой/отсутствующий `ARCHIVE_BACKEND` = архивация выключена
   целиком (штатный режим, пока хранилище не заведено); включение — ПОСЛЕ подстановки рабочего
   токена; `REPLACE_ME` — заглушка, а не токен: с включённым бэкендом и нерабочим токеном каждый
   прогон упирается в 401 на проверке архива (`ErrAuth`, `archive/yandex.go:240`), no-op не
   наступает и импорт идёт полным каждый день — данные не теряются, но версии не сохраняются.
   В плане (Step 3) формулировка приведена в соответствие: `#ARCHIVE_BACKEND=yandex`
   (закомментирован: архивация — опция).
3. **[Minor] Противоречие в README.** `docs/archive/README.md`: было
   «`source`/`name`/`url`/`sha256` и есть весь его состав» (4 поля против таблицы из 8) → стало
   «Секретов в реестре нет: полный состав строки — в таблице полей выше (восемь полей, все публичные)».

## Проверки (вывод вставлен)

```
$ gofmt -l main.go                       → пусто (чисто)
$ go build ./...                         → BUILD_OK
$ go vet ./...                           → VET_OK
$ go test -race -count=1 ./util/ ./archive/
  ok …/util    2.870s
  ok …/archive 2.012s

# пример как env-файл: после копирования архивация выключена (set -a; . пример)
ARCHIVE_BACKEND=[<пусто>] → архивация выключена
ARCHIVE_YANDEX_TOKEN=[REPLACE_ME]        # заглушка осталась, но при пустом бэкенде не читается

# (а) make archive-registry без ARCHIVE_BACKEND — ошибка осталась, код возврата ненулевой
$ env -u ARCHIVE_BACKEND make archive-registry
ARCHIVE_ACTION=registry ./build/clickhouse-import-rosstat-native
time="2026-10-14T22:34:50+05:00" level=fatal msg="ARCHIVE_BACKEND не задан"
make: *** [archive-registry] Error 1
exit=2

# (б) make env-check — строка на месте
$ make env-check
файл окружения: /Users/whitefox/.config/rosstat/env найден
OK   ARCHIVE_YANDEX_TOKEN задан
```

Правки не касались `main.go` и `Makefile` — режим, цель и `env-check` перепроверены на том же
бинарнике без пересборки логики; `docs/archive/registry.jsonl` по-прежнему не создан
(`ls -A docs/archive` → `README.md`).

## Коммит fix round 1

`3c0f060` — `fix staging archive env example scopes and opt-in`, только
`deploy/staging/clickhouse-import-rosstat.env.example`, `docs/archive/README.md`,
`docs/superpowers/plans/2026-10-14-source-file-archive.md` (3 файла, +16/−6). Одна строка-инструкция
в примере перенесена на две, чтобы не выбиваться по ширине из соседних комментариев; SHA выше —
после `--amend` этого же, ещё не отправленного коммита. `deploy/staging/dagu.service` при этом
остался единственной незакоммиченной правкой владельца.
