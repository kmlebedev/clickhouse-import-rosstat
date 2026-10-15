# Финальное ревью `source-file-archive` — fix wave (2 Important + 1 рекомендация)

**Дата:** 2026-10-14 · **Ветка:** `source-file-archive` · **Коммиты:** `bbf4a8b`, `71f3a49`
**Область:** `util/archive.go` (запись объекта), `main.go` (режим `registry`), синхронизация формулировок в спеке/плане/роадмапе/ARCHITECTURE/README/`docs/archive/README.md`, regression-тест.

---

## 1. Important — строка реестра могла описывать содержимое, которого в архиве нет

**Что было.** `save` пропускал `Store.Put`, если объект по ключу уже лежал (`Exists` → true), но `Registry.Ensure` вызывался безусловно с новым `sha256`/`size`. Ключ выводится из отпечатка HEAD, а отпечаток содержимое не покрывает: при неизменных заголовках и изменённом теле (полный импорт уходил в self-heal по числу точек) в реестре оказывался `sha256` содержимого B, а в хранилище оставалось содержимое A.

**Что сделано (ровно решение контроллера).**

- `util/archive.go`, `save`: `Exists`-предпроверка убрана — порядок стал `sha256 → ключ → Put → Registry.Ensure`. Объект пишется **всегда** (оба бэкенда перезаписывающие: `LocalStore` — атомарный rename, `Яндекс.Диск` — `overwrite=true`), поэтому инвариант «строка реестра описывает записанные байты» держится конструктивно. В комментарии названа цена (повторная загрузка идентичных байтов в редких ветках: смена версии, самолечение; при 150–250 МБ/год незаметно), снят лишний вызов `Exists` и добавлено, что пропавшая строка при живом объекте так не восстанавливается (см. п. 2).
- Формулировки старой схемы приведены к «объект записывается всегда, реестр — после него»: спека §3.2/§3.3/§4.1, план (Step 3, таблица файлов, Step T3), `docs/archive/README.md` (описание полей `sha256`/`size`), `ARCHITECTURE.md` §8 п. 13, `README.md` («Винтажный архив (опция)»). `archive/registry.go` — комментарий про запись строки.

**Regression-тест.** `util/archive_test.go`: `TestClickHouseImportArchiveOverwritesStaleObject` (рядом с `TestClickHouseImportArchivesVersion`). Предварительно кладёт в `LocalStore` по ожидаемому ключу объект с ДРУГИМИ байтами — тем же способом, что и прод (`rt.save(ctx, source, url, stale, fingerprint)`: при непустом отпечатке ключ от содержимого не зависит), затем моделирует триггер: версия известна (42 точки), чек в таблице не сошёлся (7) → полный импорт. Проверки: объект содержит новые байты, `sha256`/`size` строки реестра = фактический объект (общий хелпер `archiveTestEntryMatchesContent`, вынесен и переиспользован в `TestClickHouseImportArchivesVersion` вместо дублированного блока).

**Мутационная проверка (тест действительно ловит старый код):**

```
$ git stash push -- util/archive.go && go test -count=1 -run TestClickHouseImportArchiveOverwritesStaleObject ./util/
--- FAIL: TestClickHouseImportArchiveOverwritesStaleObject (0.04s)
    archive_test.go:387: объект cbr_m2/e506be9394de74be_full.xlsx (134 байт) не совпал с отданным файлом (6058 байт): запись объекта пропущена
FAIL
$ git stash pop
```

С новым `save` тест зелёный (см. прогоны ниже).

## 2. Important — неверное утверждение о самолечении потерянной строки реестра

Самолечение срабатывает по потере **объекта** (`Exists` → false → полный импорт → `save`); при живом объекте и пропавшей строке no-op-путь до `save` не доходит, и строка не восстанавливается.

- **Спека §3.4** (было «потерянная строка самолечится — `Exists` вернёт «нет», полный импорт допишет») → теперь сказано, что самолечение работает по потере объекта, а потерянная строка при живом объекте не возвращается; реконсиляция «объект без строки» — шаг 1.2, дорогой путь (чтение реестра на каждом no-op) не делается.
- **Спека §5, риски** — строка про read-modify-write реестра приведена к реальности (та же формулировка про шаг 1.2).
- **Дубли формулировки** исправлены там же, где она встречалась: `archive/registry.go` (комментарий к `registryMu`), план (Step 1 T1, Step 3 T3), `README.md` («Ограничения и известные проблемы»).
- **`docs/ROADMAP_INFRA.md`, шаг 1.2** — добавлен блок «Обязательные подпункты шага 1.2» (плюс отсылка «Обязательные подпункты — ниже таблицы» в самой строке 1.2):
  1. **реконсиляция «объект без строки реестра»** (листинг/восстановление не должны считать реестр полным описанием хранилища);
  2. **осиротевшие `*.tmp` игнорируются** (dev-бэкенд `LocalStore` пишет атомарно через временный файл; после SIGKILL `.tmp` остаётся, объектом не считается);
  3. **возможные дубли содержимого** — один файл под `sha-`-ключом (HEAD был недоступен) и под ключом отпечатка (HEAD починился) — листинг обязан называть дубль дублем, а не двумя версиями.

## 3. Рекомендация — защита git-зеркала от затирания пустым реестром

`main.go`, `runArchiveRegistry`:

- новая константа `envArchiveRegistryAllowEmpty = "ARCHIVE_REGISTRY_ALLOW_EMPTY"`;
- если скачанный реестр пуст (`len(bytes.TrimSpace(body)) == 0`), а существующий `docs/archive/registry.jsonl` непуст → **отказ** с текстом «реестр в хранилище пуст, а … непуст: запись затёрла бы git-зеркало (ARCHIVE_REGISTRY_ALLOW_EMPTY=1 — разрешить)»; с `ARCHIVE_REGISTRY_ALLOW_EMPTY=1` — запись пустого файла + `log.Warnf`. Отдельная ветка: если зеркало не читается по причине, отличной от «нет файла», — ошибка (нельзя писать поверх «наугад»);
- док-комментарий `runArchiveRegistry` обновлён; документация — `docs/archive/README.md` (абзац про пустой реестр), env-таблицы в том же стиле, что `ARCHIVE_ACTION`/`ARCHIVE_REGISTRY_OUT`: `ARCHITECTURE.md` §5 (строка `ARCHIVE_ACTION`, `ARCHIVE_REGISTRY_OUT`, `ARCHIVE_REGISTRY_ALLOW_EMPTY`) и `README.md` «Ключи и окружение» + строка цели `make archive-registry`; спека §3.4/§3.5/§4.2/§5/§6.

---

## Проверки

```
$ gofmt -l .                       # пусто (по затронутым файлам и по всему дереву)
$ go vet ./util/ ./archive/ .      # без замечаний
$ go test -race -count=1 ./util/ ./archive/
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	2.095s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/archive	2.135s
$ go build ./...                   # BUILD_OK
$ make all                         # exit=0: golangci-lint "0 issues."; go vet ./... чисто;
                                   # go test -race ./... — все пакеты ok (util 2.0s, rosstat и прочие cached/ok);
                                   # обе сборки прошли (clickhouse-import-rosstat linux/amd64, build/ingest)
```

`mcp__jetbrains__get_file_problems` по `main.go`, `util/archive.go`, `archive/registry.go` — `errors: []` (в `util/archive_test.go` остались только pre-existing weak-warning про дублирование/SQL-диалект и предупреждение о CVE в `excelize`, к правкам не относящиеся).

### Ручная проверка п. 3 (вывод в отчёте)

Зеркало перед проверкой — непустое (1 строка, `cbr_m2/1024093306d187c8_monetary_agg_SA.xlsx`).

**1. Пустой реестр при непустом зеркале — отказ:**

```
$ EMPTY=$(mktemp -d)
$ make archive-registry ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR="$EMPTY"
go build -o build/clickhouse-import-rosstat-native .
ARCHIVE_ACTION=registry ./build/clickhouse-import-rosstat-native
time=… level=warning msg="реестр архива пуст: registry.jsonl"
time=… level=fatal msg="реестр в хранилище пуст, а docs/archive/registry.jsonl непуст: запись затёрла бы git-зеркало (ARCHIVE_REGISTRY_ALLOW_EMPTY=1 — разрешить)"
make: *** [archive-registry] Error 1
exit=2
$ wc -l docs/archive/registry.jsonl    # 1 — зеркало НЕ затёрто
```

**2. `ARCHIVE_REGISTRY_ALLOW_EMPTY=1` — пустой файл + warn:**

```
$ ARCHIVE_REGISTRY_ALLOW_EMPTY=1 make archive-registry ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR="$EMPTY"
time=… level=warning msg="реестр архива пуст: registry.jsonl"
time=… level=warning msg="реестр в хранилище пуст: docs/archive/registry.jsonl затирается по ARCHIVE_REGISTRY_ALLOW_EMPTY=1"
time=… level=info msg="реестр архива: 0 строк -> docs/archive/registry.jsonl"
exit=0
$ wc -c docs/archive/registry.jsonl    # 0 байт — пустое зеркало записано
```

**3. Пустой реестр при пустом зеркале — штатно, без отказа** (проверено тем же пустым каталогом при уже пустом зеркале): `exit=0`, `warning "реестр архива пуст"`, `info "реестр архива: 0 строк"`.

**4. Восстановление зеркала:**

```
$ git checkout -- docs/archive/registry.jsonl && wc -l docs/archive/registry.jsonl
1
$ git status --short docs/archive/registry.jsonl    # пусто — зеркало как было
```

Временный каталог удалён (`rm -rf "$EMPTY"`).

## Изменённые файлы

**`bbf4a8b` — always store archived object and fix lost-row self-heal claim**

| Файл | Что |
|---|---|
| `util/archive.go` | `save`: удалена `Exists`-предпроверка, `Put` всегда; комментарий переписан (инвариант, цена, потерянная строка) |
| `util/archive_test.go` | `TestClickHouseImportArchiveOverwritesStaleObject`; хелпер `archiveTestEntryMatchesContent`, применённый и в `TestClickHouseImportArchivesVersion` |
| `archive/registry.go` | комментарий к `registryMu`: потерянная строка при живом объекте не самолечится (шаг 1.2) |
| `docs/superpowers/specs/2026-10-14-source-file-archive-design.md` | §3.2/§3.3/§3.4/§3.5/§4.1/§4.2/§5/§6: инвариант «объект пишется всегда», реальность самолечения, новый env, приёмка |
| `docs/superpowers/plans/2026-10-14-source-file-archive.md` | Step 3 (T1), Step 1 (T3), таблица файлов: формулировки порядка `save`, гард пустого зеркала, `ARCHIVE_REGISTRY_ALLOW_EMPTY` |
| `docs/ROADMAP_INFRA.md` | шаг 1.2: явные подпункты (реконсиляция строки, `*.tmp`, дубли `sha-`/fp-ключей) |

**`71f3a49` — refuse empty registry over non-empty mirror and sync archive docs**

| Файл | Что |
|---|---|
| `main.go` | константа `ARCHIVE_REGISTRY_ALLOW_EMPTY`, гард пустого реестра поверх непустого зеркала (+ ошибка нечитаемого зеркала), док-комментарий |
| `ARCHITECTURE.md` | §5: env `ARCHIVE_REGISTRY_ALLOW_EMPTY`; §8 п. 13: «объект записывается всегда» |
| `README.md` | «Ключи и окружение» (env), строка цели `make archive-registry`, раздел «Винтажный архив» (инвариант), «Ограничения» (потерянная строка → шаг 1.2) |
| `docs/archive/README.md` | пустой реестр при непустом зеркале — отказ; поля `sha256`/`size` описывают записанные байты |

Не тронуто: `deploy/staging/dagu.service` (незакоммиченная правка владельца) — остаётся `M` в рабочем дереве; `docs/archive/registry.jsonl` восстановлен из git и идентичен HEAD.

## Сомнения

1. **Дубли `sha-`-ключ vs ключ отпечатка** названы как подпункт шага 1.2, но не решены: сегодня один и тот же файл может лежать двумя объектами, и в реестре будут две строки. Это ожидаемое следствие ключа §3.2, а не дефект этой волны; решать его (слияние/пометка дубля) — в 1.2.
2. **Потерянная строка при живом объекте** остаётся невосстановимой до 1.2 — принято контроллером (дорогой путь «читать реестр на каждом no-op» отклонён); явно названо в спеке, README и роадмапе.
3. **Гард читает зеркало без блокировки** относительно параллельного коммита зеркала — гонка возможна только между ручным `make archive-registry` и ручным коммитом того же файла; для целей защиты от молчаливого затирания этого достаточно.
4. **Ручная проверка п. 3 выполнена на dev-машине** (`ARCHIVE_BACKEND=local`, пустой `mktemp -d`); зеркало — приёмочное (`docs/archive/registry.jsonl`) и восстановлено; после проверок `git status` чист, кроме файла владельца.
5. **Тексты в `.superpowers/sdd/.../task-*-brief.md`, `task-*-report.md`** (исторические артефакты задач) не правились: они фиксируют состояние на момент задач, а не действующую схему; актуальные формулировки — в спеке, плане и нормативных документах.
