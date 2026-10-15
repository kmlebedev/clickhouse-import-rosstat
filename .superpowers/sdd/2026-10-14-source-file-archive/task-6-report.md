# Task 6 — отчёт: нормативные документы (винтажный архив версий файлов источников)

**Ветка:** `source-file-archive` · **BASE:** `3c0f060` · **Дата:** 2026-10-14
**Объём:** документация (код не меняется, кроме дрейфа комментариев, п. 7 брифа).

## Что сделано по шагам

| Шаг | Файл | Суть правки |
|---|---|---|
| 1 | `docs/ROADMAP_INFRA.md` (**новый**, 38 строк) | Шапка (дата, роль документа, ссылки на четыре трековых роадмапа + ARCHITECTURE/AGENTS/стандарт, правило «один пункт за раз»); §1 «Зачем отдельный роадмап» — инфраструктура обслуживает все треки и не принадлежит ни одному, граница «только общее»; §2 «Фаза 1 — Надёжность данных» — таблица `Шаг / Содержание / Статус / Критерий приёмки`, строка **1.1** «Винтажная база: архив версий файлов источников (`archive/` + `registry.jsonl` на Яндекс.Диске, git-зеркало `docs/archive/registry.jsonl`; покрывает 14 импортёров с точкой контроля публикации, включая `rosstat_labour_force_mes` из PR #61)» со статусом **⏳ реализуется в ветке `source-file-archive`** и критерием приёмки, сжатым из спеки §6; следом follow-up-заготовки 1.2 наполнение/восстановление БД из архива, 1.3 S3-бэкенд, 1.4 детектор «урезанного покрытия»; блок «Вне объёма шага 1.1» (краулеры, прочие пакеты, PDF, HTML/XML/JSON, таблица реестра, автообновление зеркала на staging); §3 «Приёмка и риски» — порядок приёмки (команда → результат, `make all`, `make mcp-check`, статус ✅ тем же PR), правило «один пункт = одна ветка = один PR», риски |
| 2 | `docs/IMPORTER_PACKAGE_STANDARD.md` | §2 «Термины» — строка «**Винтажный архив** — сохранённые версии файлов источника в объектном хранилище (пакет `archive/`) + реестр `registry.jsonl`; вход будущего восстановления базы (§9.6)»; §9 — новый пункт **9.6 (ОБЯЗАТЕЛЬНО)** «Винтажная база» (механизм шаблона `ARCHIVE_BACKEND` + точка контроля, кода импортёра не требует; обязанность качать артефакт шаблонным путём `DataUrl`/`BeforeImport` + `ImportFunc`; `CrawFunc` — пример `bank/sber_fin_rez.go` / `sber_finansovie_rezultaty` — в архив не попадает, отступление называется в спеке (п. 1.5); ссылка на `ARCHITECTURE.md` §8 п. 13); §14 — новый пункт 9 «Винтажная база по §9.6…» (остальные пункты перенумерованы 10–14); §16.1 — новый пункт 6 «Файл версии в архиве, строка в `registry.jsonl` — для импортёров с точкой контроля (§9.6)» (пункт про документацию стал 7) |
| 3 | `AGENTS.md` | В правиле «Разработка строго по дорожной карте» п. 1 добавлен третий роадмап: «инфраструктурные пункты, не принадлежащие ни одному треку, — `docs/ROADMAP_INFRA.md`». Больше в файле ничего не тронуто |
| 4 | `ARCHITECTURE.md` | §2 — строка пакета `archive/` (Store + LocalStore/YandexDisk, `registry.jsonl` в корне хранилища + git-зеркало/`make archive-registry`, `FromEnv`, S3 — будущий бэкенд); §3 — в описание полного цикла шаблона добавлен шаг «**архив версии файла**» между точками и `AfterImport` + оговорка про сбой (прогон не роняет, строка версии не пишется) и про ветку `CrawFunc`; §5 — пять строк env (`ARCHIVE_BACKEND`, `ARCHIVE_YANDEX_TOKEN` с формулировкой секрета по образцу `GOST_APP_ID`, `ARCHIVE_YANDEX_DIR`, `ARCHIVE_LOCAL_DIR`, `ARCHIVE_ACTION`+`ARCHIVE_REGISTRY_OUT`) и фраза про ловушку env-файла в абзаце «⚠️ В env-файле из `GOST_*`…»; §8 п. 13 — расширенная семантика префлайта «версия известна ⇒ версия в архиве» (`Store.Exists` тем же ключом, «нет/ошибка → warn и полный импорт»), гейт `ImportFunc` и 14/15, архив между точками и производными, сбой архива отменяет строку версии, реестр читается без ClickHouse, ссылка на стандарт §9.6 |
| 5 | `README.md` | «Конфигурация» — строка про шесть `ARCHIVE_*`-переменных; «Разработка» (таблица команд) — строка `make archive-registry`; «Ключи и окружение» — закомментированная строка `ARCHIVE_YANDEX_TOKEN` в примере env-файла + запрет писать туда `ARCHIVE_BACKEND`, и подраздел «Винтажный архив версий файлов источников (опция)»: пошагово OAuth-приложение Яндекса (оба scope) → implicit-flow токен → куда положить (dev/staging) → `make env-check`, ловушка `ARCHIVE_BACKEND` и реальные команды приёмки из спеки §6 (local и yandex + `make archive-registry`), различение `ErrAuth`/`ErrQuota` и поведение при сбое; «Структура репозитория» — строка пакета `archive/` (требование скила к новому пакету); «Ограничения и известные проблемы» — новый пункт: что архивируется (14 импортёров, XLSX-путь шаблона) и что нет, первый прогон — полный ре-импорт, сбой архива не роняет прогон, восстановление — follow-up `ROADMAP_INFRA.md` шаг 1.2 |
| 6 | `docs/superpowers/specs/2026-10-14-source-file-archive-design.md`, `docs/superpowers/plans/2026-10-14-source-file-archive.md` | Дрейф убран: «`ImportXls` остаётся тонкой обёрткой (его используют тесты)» → «`ImportXls` **удалён**: его место занял приватный `importXlsBody(ctx, body, conn)`; потребителей вне `util` не было, непотребляемый экспорт — мёртвый код (стандарт §3.3)» — в спеке §3.1 и в плане (Step 4) |
| 7 | `cbr/mo_br.go`, `util/clickhouse_import.go`, `archive/registry.go` | Только комментарии: в `mo_br.go` цикл шаблона переписан с устаревшего `GetXlsx` на факт («скачивание шаблоном и разбор из памяти → Send → архив версии файла → каталог и витрина → строка версии») + оговорка «сбой архива отменяет запись версии»; в `clickhouse_import.go` комментарий к `importXlsBody` больше не рассказывает историю про `ImportXls`, а называет контракт («внутренняя рука шаблона, снаружи не зовётся»); в `archive/registry.go` «source/name/url/sha256 и есть весь состав» заменено на восемь публичных полей записи |

## Что сверял с кодом (а не по памяти)

- **14 импортёров**: `grep -rn "PublicationCountPoints:"` по `cbr/ rosstat/ bank/` без тестов — 15 файлов; из них `ImportFunc` у 14 (десять в `cbr/`: `mp_survey`, `mo_br`, `households`, `m2`, `credit_m2x`, `new_loans_corp`, `new_loans_ind`, `bank_int_rat`, `indicators_cpd`, `Infl_exp`; четыре в `rosstat/`: `ipc_mes`, `salaries_mes`, `vvp_kvartal`, `labour_force`), `CrawFunc` — только `bank/sber_fin_rez.go`. Имена/`Name()` сверены с реестрами `rosstat/importers.go` и таблицей литералов, `Name()` Сбера = `TableName` = `sber_finansovie_rezultaty` (`bank/sber_fin_rez.go:23`).
- **env-имена и дефолты**: `archive/fromenv.go` (`EnvBackend/EnvYandexToken/EnvYandexDir/EnvLocalDir`, `DefaultYandexDir = "disk:/rosstat-archive"`, пустой бэкенд → `(nil, nil)`, ошибка конфигурации → вызывающий логирует и работает без архива — `util/archive.go:45–61`); `main.go:48–52` (`archiveActionRegistry = "registry"`, `envArchiveRegistryOut`, `defaultRegistryOut = "docs/archive/registry.jsonl"`, выход **до** `ParseDSN`); `Makefile:161` (строка `ARCHIVE_YANDEX_TOKEN` в `env-check`), `Makefile:173–178` (цель `archive-registry` = сборка `-native` + `ARCHIVE_ACTION=registry`), `.PHONY`.
- **Формат реестра**: `archive/registry.go` — `RegistryKey = "registry.jsonl"`, комментарий «в корне хранилища», `Entry` из восьми полей (`source, name, fingerprint, sha256, size, url, key, first_seen`); `docs/archive/README.md` — те же восемь полей, «истина — файл на Диске», обновление `make archive-registry`, коммит ручной/периодический (на staging нет git-дерева), чтение без ClickHouse.
- **Семантика префлайта и архива**: `util/clickhouse_import.go:118–224` — цикл в комментарии `Import` совпадает с кодом (DDL → `BeforeImport` → префлайт → точки → архив → `AfterImport` → версия), проверка «версия в архиве» стоит под `noop && fingerprint != "" && activeArchive() != nil && s.ImportFunc != nil`, сбой `save` ставит `archiveFailed = true`, и `SavePublication` вызывается только при `fingerprint != "" && !archiveFailed`; `util/archive.go` — `save` (sha256 → ключ → `Exists` → `Put` → `Registry.Ensure`) и `versionExists` (один `Store.Exists`).
- **Оба scope токена**: `deploy/staging/clickhouse-import-rosstat.env.example` (строки 12–14) + спека §2; в docs/archive/README.md примеры команд уже используют `ARCHIVE_BACKEND=yandex`, пробовал сверить с кодом — в `archive/yandex.go` scope не упоминается (там только `Authorization: OAuth`), поэтому формулировку про оба scope взял из env-образца владельца и спеки, а не из кода.
- **PDF/HTML/XML/JSON и прочие пакеты вне покрытия**: спека §1.3 + код (архив живёт только в ветке `ImportFunc`).

## Проверки

```
$ gofmt -l cbr/mo_br.go util/clickhouse_import.go archive/registry.go
(пусто, exit 0)
$ go vet ./cbr/ ./util/ ./archive/
OK
$ go test -race -count=1 ./cbr/ ./util/ ./archive/
ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr     35.375s
ok  github.com/kmlebedev/clickhouse-import-rosstat/util     3.471s
ok  github.com/kmlebedev/clickhouse-import-rosstat/archive  2.497s
```

Плюс `make all` — зелёный (exit 0, 113 с): `golangci-lint` — `0 issues.`, `go vet ./...`, `go test -race ./...` по всем пакетам (`archive 3.314s`, `cbr 37.267s`, `rosstat 15.883s`, `util 4.151s`, `wgc 93.569s`, остальные ok), обе сборки (`build/clickhouse-import-rosstat`, `build/ingest`).

**GoLand MCP** (`get_file_problems`): `util/clickhouse_import.go` и `archive/registry.go` — `errors: []`; `cbr/mo_br.go` — только предсуществующие предупреждения (CVE в `excelize`, «SQL dialect is not configured»), ошибок правки нет.

## Находки self-review

1. Первая редакция README-абзаца про сбой архива утверждала, что при `401` в логе будет «архив версий файлов выключен» — неверно: эта фраза относится только к ошибке конфигурации `FromEnv` (её сеть не трогает), а `401` (`ErrAuth`) даёт ошибку/ warn архивации версии. Формулировка переписана: различение `ErrAuth` (обновить токен) и `ErrQuota` (освободить место), импорт данных продолжается, строка версии не пишется.
2. В ROADMAP_INFRA шаг 1.2 сначала называл `make archive-list`/`archive-get` как существующие цели — таких целей в Makefile нет (имена взяты из спеки §1.3 как план). Переформулировано: «будущие цели вида … — сегодня их нет».
3. Скил требует строку нового пакета и в «Структуре репозитория» README — добавлена строка `archive/` (бриф называл только «Ключи и окружение» и «Ограничения»; это тот же пункт, не чужая тема).
4. В §14/§16 стандарта пришлось перенумеровать пункты (9–13 → 10–14 и 6 → 7): вставка нового пункта в середину логического порядка. Ссылок на номера этих пунктов из других документов нет (проверено grep'ом по `§14`/`§16`).
5. `docs/archive/registry.jsonl` в репозитории пока **нет** (только `docs/archive/README.md`) — это ожидаемо: зеркало заполняет приёмка. Ни один документ не утверждает, что файл уже существует: везде сказано «обновляет цель `make archive-registry`».

## Изменённые файлы

Новый: `docs/ROADMAP_INFRA.md`.
Изменены: `docs/IMPORTER_PACKAGE_STANDARD.md`, `AGENTS.md`, `ARCHITECTURE.md`, `README.md`, `docs/superpowers/specs/2026-10-14-source-file-archive-design.md`, `docs/superpowers/plans/2026-10-14-source-file-archive.md`, `cbr/mo_br.go`, `util/clickhouse_import.go`, `archive/registry.go`.
Не тронут: `deploy/staging/dagu.service` (незакоммиченная правка владельца — в коммит не вошла).

## Сомнения

- Таблица фазы 1 получила четыре колонки (`Статус` добавлен к трём из брифа) — иначе статус «⏳/✅» некуда положить; так же устроены `GOLD_THESIS_DATA_ROADMAP.md` и `ROADMAP_DCF_METALS.md`.
- В §8 п. 13 число «14 из 15» и гейт `ImportFunc` — утверждение, вытекающее из кода (`util/clickhouse_import.go:150`); гейт покрыт тестом `TestClickHouseImportArchiveSkippedForCrawFunc` (`util/archive_test.go:217`), отдельного энфорсера именно на число 14 нет.
- Статус шага 1.1 намеренно оставлен ⏳ (приёмка — Task 7), поэтому критерий приёмки в таблице — план проверки, а не результат.

---

# Fix round 1 (ревью Task 6)

**Дата:** 2026-10-14 · **BASE фикса:** `ebc2ef1`

## Finding 1 (Important): staging в ловушке env-файла

`ARCHITECTURE.md`, абзац «⚠️ В env-файле из `GOST_*`…» — dev-правило было распространено на staging («в env-файле её не держат, бэкенд выбирают аргументом цели»). Сверено с `deploy/staging/dagu.service` (строка 12: `EnvironmentFile=/etc/clickhouse-import-rosstat.env`) и `deploy/staging/clickhouse-import-rosstat.env.example` (строка 37: `#ARCHIVE_BACKEND=yandex`): на staging бэкенд задаётся именно в env-файле, префикса команды там нет — команду запускает расписание.

Стало: «**Ловушка та же у архива, но только в dev-файле:** в `~/.config/rosstat/env` … `ARCHIVE_BACKEND` туда не пишут … в dev бэкенд выбирают аргументом цели … **На staging — наоборот:** там команду запускает не человек, а расписание, и `ARCHIVE_BACKEND=yandex` задаётся именно в `/etc/clickhouse-import-rosstat.env` рядом с токеном (файл читает `deploy/staging/dagu.service` через `EnvironmentFile`; образец — `deploy/staging/clickhouse-import-rosstat.env.example`)». README в этой части не тронут (он был корректен).

## Finding 2 (Important): `403` — это не всегда квота

**README** («Ключи и окружение»): абзац переписан в разбор кодов. `401` (`ErrAuth`) — токен не принят, действие — перевыпустить; `403` (`ErrQuota`) объявлен **двояким** — нехватка прав (не отданы оба scope; архив на `disk:/`, а токен выдан приложению с доступом к `app:/`) или исчерпанная квота; к `ErrQuota` сводятся также `413`/`423`/`507`; причину различает фрагмент ответа Диска в тексте ошибки (`; ответ Диска: …`) — «про права → перевыпускать токен и сверять путь», «про место → освобождать», с явным «прежде чем идти освобождать место, прочитайте фрагмент».

**Код (`archive/yandex.go`).** `statusError(op, diskPath string, status int, body []byte)` получил четвёртый аргумент — тело ответа; маппинг кодов **не менялся** (`401` → `ErrAuth`, `403/413/423/507` → `ErrQuota`, остальное — «статус N»). Появилась локальная `errSnippet(body []byte) string` (`errSnippetLimit = 200`): `strings.Fields` + join сжимает ответ в одну строку, длинный режется по байту и чистится `strings.ToValidUTF8` от разрезанной руны. Импорт `util.ErrorSnippet` не сделан сознательно: `util` уже импортирует `archive`, обратная зависимость замкнула бы цикл — это названо в комментарии функции.

**Решение по href-хостам (названо явно):** фрагмент добавляется только у запросов к API с токеном (`signedHref`, `mkdir`, `Exists`, и через них — `Put`/`Get` до выдачи ссылки); для ответов хостов по подписанной ссылке (`upload` PUT на href, `Get` GET с href) передаётся `nil`. Причина: ошибка такого хоста бывает HTML-страницей прокси, повторяющей URL запроса вместе с подписью, а подпись — временный секрет (её уже прячет `transportError`/`signedHref`); ценности у фрагмента там меньше, чем риска утечки. Решение закреплено в комментарии `statusError` и у обоих вызовов с `nil`.

**Тесты (`archive/yandex_test.go`).**
- Эмулятор: очередь подменённых ответов стала `[]fakeFail{code, body}` (было `[]int`), добавлен `failNextWithBody`, общий хелпер `fail(w, fakeFail)` пишет тело **после** `WriteHeader` (иначе неявный 200 подменил бы статус); `take` возвращает `fakeFail`.
- `TestYandexForbiddenKeepsBodySnippet`: сервер отвечает `403` с телом `ForbiddenError: недостаточно прав на запись в disk:/rosstat-archive`; проверяются оба утверждения сразу — `errors.Is(err, ErrQuota)` (маппинг не изменился) и наличие фразы Диска в тексте ошибки, плюс отсутствие значения токена в тексте.
- `TestErrSnippet`: пустое тело → пустая строка; переводы строк/табы сжимаются в одну строку; тело длиннее лимита режется до 200 байт без разрезанной руны (+ инварианты «одна строка» и «не длиннее лимита»).

## Finding 3 (Minor): `429`/`503` в таблице рисков спеки

`docs/superpowers/specs/2026-10-14-source-file-archive-design.md` §5, первая строка таблицы: `429`/`503`/`423` стояли в одном ряду с квотой. Приведено к коду (`do`: `429`/`503` → повтор шага, до трёх попыток с паузами `retrySleeps`; `423` → `ErrQuota`): «Токен истёк/отозван (`401` → `ErrAuth`), квота или нехватка прав у токена (`403`; к тому же `ErrQuota` сводятся `413`/`423`/`507`), всплеск запросов и работы Диска (`429`/`503` — повтор шага)» с митигацией, где перечислены различие `403` по фрагменту тела, повтор `429`/`503` и self-heal.

## Проверки Fix round 1

```
$ gofmt -l archive/
(пусто)
$ go vet ./archive/
(чисто)
$ go test -race -count=1 ./archive/
ok  github.com/kmlebedev/clickhouse-import-rosstat/archive  1.569s
$ go build ./...
(успех, без вывода)
$ go test -race -count=1 -run '…' -v ./archive/
PASS: TestYandexQuotaError (403/413/423/507), TestYandexForbiddenKeepsBodySnippet, TestErrSnippet (3 подтеста), TestYandexUnexpectedStatus
```

`get_file_problems` (GoLand MCP): `archive/yandex.go` — `errors: []`, `archive/yandex_test.go` — `errors: []`.

## Diff Fix round 1

`ARCHITECTURE.md` (1 абзац), `README.md` (1 абзац), `docs/superpowers/specs/2026-10-14-source-file-archive-design.md` (1 строка таблицы), `archive/yandex.go` (5 мест вызовов + `errSnippet` + `statusError`), `archive/yandex_test.go` (эмулятор + 2 теста). Правка владельца `deploy/staging/dagu.service` по-прежнему не в коммите.
