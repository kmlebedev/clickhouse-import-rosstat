# SDD ledger — plan: docs/superpowers/plans/2026-10-14-source-file-archive.md

Ветка: `source-file-archive` (основной каталог). HEAD на старте SDD: `cd76576`.
Спека: `docs/superpowers/specs/2026-10-14-source-file-archive-design.md` (576c294).

## Pre-flight скан (до Task 1)

| Пара | Что производит → что потребляет | Находка |
|---|---|---|
| T1→T2 | `Store`, `ErrAuth`/`ErrQuota` → реализация бэкенда Диска | согласовано |
| T1→T3 | `Store`, `LocalStore` → `Registry` и его тесты | согласовано |
| T1→T4 | `Store`, `Key(sum [sha256.Size]byte)`, `FromEnv` → синглтон/сохранение/проверка | согласовано (в T4 `[32]byte{}` — та же размерность) |
| T3→T4 | `Registry`/`Entry`/`RegistryKey` → `archiveRuntime`, `Entry` в `save` | согласовано (восемь полей Entry совпадают с примером спеки) |
| T3→T5 | `RegistryKey`, `Store.Get`/`Exists` → `runArchiveRegistry` | согласовано |
| T5→T6 | имена env, цель make, путь зеркала → README/ARCHITECTURE/ROADMAP | согласовано |
| T6→T7 | `docs/ROADMAP_INFRA.md` → статус ✅; цель make → зеркало | согласовано |
| T4 (внутренняя) | текст плана: «`ImportXls` остаётся тонкой обёрткой (его используют тесты)» | **неверно по факту**: grep — потребителей нет, только определение и вызов внутри `util/clickhouse_import.go` |

Ruling: `ImportXls` в Task 4 **удаляется** (его место занимает приватный `importXlsBody`), а не сохраняется обёрткой: потребителей вне `util` нет, непотребляемый экспорт — мёртвый код (стандарт §3.3, рубрика ревью). Стоимость ошибки: если внешний потребитель появится, обёртка возвращается одной строкой. Указание плана заменено этим ruling'ом — внесено в диспатч T4.

## Ход

- Task 1: BASE `cd76576`; бриф `.superpowers/sdd/2026-10-14-source-file-archive/task-1-brief.md`; диспатч имплементера 2026-10-14 (deepseek/deepseek-flash).

- Task 1: implementer DONE_WITH_CONCERNS (agent-164) — коммит `47be21a`, 14/14 тестов, отчёт `task-1-report.md`.
- Task 1: Ruling (по DONE_WITH_CONCERNS): yandex-ветка `FromEnv` в T1 остаётся заглушкой (валидация токена/корня + явная ошибка «бэкенд Диска не реализован (корень …)») — `NewYandex` появляется только в T2, а шаг 7 T1 требует зелёный пакет. Возврат `(nil, nil)` отвергнут вместе с имплементером: он выглядел бы как «архивация включена». Стоимость ошибки: одна строка + перепись `TestFromEnvYandexRoot` в T2 (след зафиксирован в отчёте и в диспатче T2). Отступление от буквы брифа — осознанное, рубрика ревью уведомлена.
- Task 1: ревью-пакет `review-cd76576..47be21a.diff` (36555 байт), диспатч task-reviewer (haiku).

- Task 1: review (agent-165, haiku) — вердикт «Needs fixes»: Critical нет; Important 1 (неатомарная `LocalStore.Put`, plan-mandated), Important 2 (yandex-заглушка — решение контроллера, уже зарулено выше).
- Task 1: minor (deferred): fingerprint не санитизируется (`key.go` `prefix16`; риск теоретический — md5-hex из `HeadFingerprint`).
- Task 1: minor (deferred): алиасинг `a/../b` → `b` в `LocalStore.path` (ключи строит наш код; выход за корень уже отвергается).
- Task 1: minor (deferred): краевой `Dir="/"` даёт `"//"` в проверке префикса.
- Task 1: minor (deferred): `hc` в `FromEnv` не используется до T2 (задокументировано).
- Task 1: minor (deferred): подтверждение видимости 16 символов отпечатка на живой БД не выполнено (схема подтверждена DDL).
- Task 1: Ruling (Important 1): неатомарную запись фиксим в T1 (temp-файл в том же каталоге + `Sync` + `Rename`, при ошибке — удаление временного) — расхождение со буквой спеки §3.1 (`os.WriteFile`) в пользу её же инварианта «архив — источник истины»; стоил бы потери версии при прерванной записи. Стоимость ошибки: +8 строк в dev-бэкенде.
- Task 1: Ruling (миноры 3–4 ревью): два comment-only минора (`key.go` «убираются» vs `_`; арифметика комментария в `key_test.go`) свёрнуты в fix round 1 — те же файлы того же раунда, отдельный цикл ради комментариев не заводим. Стоимость ошибки: нулевая.
- Task 1: fix round 1/5 — диспатч resume implementer agent-164 (1 addressed + 2 свёрнутых минора).

- Task 1: fix round 1/5 (1 addressed, 0 open — атомарная запись `58bdd56`; 2 свёрнутых минора addressed).
- Task 1: re-review (agent-166, haiku): All findings addressed, no new Critical/Important breakage.
- Task 1: minor (deferred): двойное «запись: запись:» в тексте ошибки `LocalStore.Put` (косметика).
- Task 1: minor (deferred): `TestLocalPutIsAtomic` не отличает старую `os.WriteFile` от новой реализации — защита структурная, признано имплементером.
- Task 1: minor (deferred, важно для восстановления): при SIGKILL между `CreateTemp` и `Rename` остаётся осиротевший `*.tmp`; будущие листинги/восстановление архива обязаны исключать `*.tmp`.
- Task 1: complete (commits cd76576..58bdd56, review clean).
- Task 2: BASE `58bdd56`; бриф task-2-brief.md; диспатч имплементера (deepseek-flash) + достройка yandex-ветки `FromEnv` по ruling'у T1.
- Task 2: implementer DONE (agent-167) — коммит `941921d`, 25/25 тестов, `make all` зелёный, отчёт `task-2-report.md`.
- Task 2: DONE-концерны (приняты к сведению): повтор понят как повтор HTTP-шага (пинится `TestYandexRetriesOn429`); `Put` заводит все уровни каталогов до корня (409 неотличим от «родитель не найден» — молча пропущенный уровень сломался бы позже); семантика живого API (`409`, `path=disk:/…`) не подтверждена — вынесена в живую приёмку T7; self-review нашёл утечку signed `href` через `*url.Error` — починена.
- Task 2: ревью-пакет `review-58bdd56..941921d.diff`, диспатч task-reviewer (haiku).

- Task 2: review (agent-168, haiku) — **Approved**: Critical/Important нет; ⚠️ (а) гранулярность повтора (HTTP-шаг, а не вся операция) — Ruling: подтверждено, `href` живёт 30 минут, повтор PUT по тому же href безопасен; (б) живая семантика `409` — вынесена в приёмку T7 (уже в плане); (г) заявленные прогоны без вывода — резолвнуто контроллером собственным прогоном: `gofmt` пусто, `go vet` OK, `go test -race -count=1 ./archive/` ok, `golangci-lint` 0 issues; (в)(д)(е) информационные.
- Task 2: minor (deferred): `NewYandex` не проверяет nil-клиент (паника вместо ошибки) — YAGNI, принято.
- Task 2: minor (deferred): `dirsToCreate` ищет `:` по всему пути (теоретический риск при `:` внутри ключа).
- Task 2: minor (deferred): текст ошибки исчерпания повторов на 429/503 не говорит, что попытки кончились.
- Task 2: minor (deferred): три одинаковых `context.WithTimeout`-блока (абстракция преждевременна).
- Task 2: minor (deferred): `503` заявлен повторяемым брифом, но тестом не покрыт (есть только 429) — триаж на финальном ревью.
- Task 2: complete (commits 58bdd56..941921d, review clean).
- Task 3: BASE `941921d`; бриф task-3-brief.md; диспатч имплементера (deepseek-flash).

- Task 3: implementer DONE (agent-169) — коммит `865f258`, 33/33 тестов (+8 `TestRegistry*`), `make all` зелёный, отчёт `task-3-report.md`.
- Task 3: DONE-концерны: `json.Marshal` экранирует `&` в url как `\u0026` (по букве брифа; альтернатива `Encoder.SetEscapeHTML(false)` — читаемость git-зеркала; кандидат в триаж); пустая строка файла = «не запись» (толерантно); `Entries()` до первого `Ensure` пуст (без ctx — по сигнатуре брифа); мьютекс не разводит процессы (как в спеке §3.4).
- Task 3: ревью-пакет `review-941921d..865f258.diff`, диспатч task-reviewer (haiku).

- Task 3: review (agent-170, haiku) — **Needs fixes**: Important 1 (мьютекс/кэш на экземпляр, не на процесс — трасса с потерей строки B), Important 2 (порядок `first_seen` при замене — решение контроллера), Minor 3 (`Entries()` до `Ensure` пуст), Minor 4 (не покрыты: состояние при ошибке Put, ошибки load), Minor 5 (`\u0026` в url — читаемость git-зеркала), Minor 6 (док-комментарий порядка «объект→реестр»), Minor 7 (Exists+Get vs «один Get» в спеке §3.4).
- Task 3: Ruling (Important 1): убираем `loaded`-кэш, перечитываем `registry.jsonl` при каждом `Ensure` под **пакетным** мьютексом (`var registryMu sync.Mutex`); `Ensure` вызывается только при смене версии, лишний `Get` дёшев, зато любые два экземпляра в процессе сходятся. Расхождение со спекой §3.4 («чтение лениво один раз») — сознательное: инвариант «ни одна строка не теряется» дороже одного `Get`. Стоимость ошибки: лишний `Get` на смене версии.
- Task 3: Ruling (Important 2): при замене строки (тот же `Key`, другой `SHA256`) строка получает новые `FirstSeen`/`sha256`/`size`/`url` и **переставляется в конец** — файл всегда упорядочен по `first_seen`, как декларирует спека. Стоимость ошибки: одна лишняя перестановка строки.
- Task 3: Ruling (миноры 5 и 4): свёрнуты в fix round 1 — `json.Encoder` с `SetEscapeHTML(false)` (git-зеркало — требование владельца «вести реестр в git», читаемость важна) и тест «ошибка Put → состояние не принято». Минор 3, 6, 7 — в deferred.
- Task 3: fix round 1/5 — диспатч resume implementer agent-169.

- Task 3: fix round 1/5 (4 addressed, 0 open — пакетный мьютекс + перечитывание, замена в конец, `SetEscapeHTML(false)`, тест сбоя Put; коммит `84ace77`).
- Task 3: re-review (agent-171, haiku): все четыре находки ADDRESSED, новых Critical/Important нет. ⚠️ заявления о прогонах — резолвнуто контроллером: `gofmt` пусто, `go vet` OK, `go test -race -count=1 ./archive/` ok.
- Task 3: minor (deferred): M1 замок держится через I/O хранилища (`Entries` может ждать зависший `Put`) — конкурентных `Registry` в продакшене нет.
- Task 3: minor (deferred): M2 инвариант «файл упорядочен по `first_seen`» держится на вызывающем, doc `upsert` сформулирован как гарантия.
- Task 3: minor (deferred): M3 `upsert` заменяет только первое совпадение по `Key` (файл с дублями ключа из ручной правки останется с лишней строкой).
- Task 3: minor (deferred): M4 ссылки на «(Review Focus №1)» в комментариях `registry.go`/`registry_test.go` — комментарий должен описывать контракт, а не ревью-артефакт.
- Task 3: complete (commits 941921d..84ace77, review clean).
- Task 4: BASE `84ace77`; бриф task-4-brief.md; диспатч имплементера (deepseek-flash). Контроллерский ruling: `ImportXls` удаляется (потребителей нет), его место — приватный `importXlsBody`.

- Task 4: implementer DONE_WITH_CONCERNS (agent-172) — коммит `d663605`, `gofmt`/`vet`/tests зелёные, `publications_test.go` не изменён и зелёный, `ImportXls` удалён по ruling'у, +6-й тест (sha-ключ при пустом отпечатке).
- Task 4: **находка имплементера (регрессия, подтверждена пробником)**: у `CrawFunc`-импортёра с точкой контроля (`bank/sber_fin_rez.go`) при включённом архиве no-op не наступает никогда — `versionExists` смотрит объект, которого CrawFunc-путь не пишет → ежедневный полный краул + warn.
- Task 4: Ruling (регрессия): проверка архива в префлайте гейтится `s.ImportFunc != nil` — архив касается только импортёров, чьи байты шаблон держит в руках; для CrawFunc-пути включение архива обязано быть no-op по поведению (спека §1.2/§3.3 «CrawFunc не трогается»). Плюс регрессионный тест: CrawFunc + точка контроля + настроенный архив → no-op наступает, хранилище не трогается. Стоимость ошибки: строка условия.
- Task 4: fix round 0 (до ревью, по концерну имплементера) — resume agent-172.

- Task 4: review (agent-173, haiku) — **Approved**: Critical/Important нет; ⚠️ прогоны без вывода — резолвнуто контроллером: `gofmt` пусто, `go vet ./util/ ./archive/` OK, `go test -race -count=1 ./util/ ./archive/` ok.
- Task 4: minor (deferred): комментарий `importXlsBody` рассказывает историю, а не поведение; лог `Get xlsx file %+v` печатает теперь DataUrl вместо пустого `xlsx.Path` (осознанно); при ошибке скачивания потерян прежний `log.Errorf("GetFile %v")` (ошибка всё равно логируется `main.go`); `TestClickHouseImportArchiveOffNoCalls` не утверждает «ноль обращений» явно; многоабзацные комментарии со ссылками «спека §…» (устаревают быстрее кода).
- Task 4: дрейф (out-of-diff, подтверждён контроллером, свёрнут в T6): спека §3.1 и план (строка про `ImportXls`-обёртку) противоречат ruling'у; `cbr/mo_br.go:97–99` описывает старый путь (`GetXlsx`, без архива); `util/clickhouse_import.go:88` — история вместо контракта.
- Task 4: complete (commits 84ace77..4a86c31, review clean).
- Task 5: BASE `4a86c31`; бриф task-5-brief.md; диспатч имплементера (deepseek-flash).

- Task 5: implementer DONE (agent-174) — коммит `23356b2`, ручные проверки (а)–(в) с выводами, `go vet ./...`/`golangci-lint`/tests зелёные, отчёт `task-5-report.md`.
- Task 5: review (agent-176, haiku) — **Needs fixes**: Important 1 [plan-mandated] — scope токена в `env.example` только `disk.write`, а код читает Диск (`Exists`/`Get`) и спека требует `write`+`read`; Important 2 [plan-mandated] — пример включает `ARCHIVE_BACKEND=yandex` с заглушкой `REPLACE_ME` (непустой токен → FromEnv собирается → 401 каждый прогон → no-op ломается, полный импорт ежедневно); Minor 3 — противоречие в `docs/archive/README.md` («4 поля и есть весь состав» vs таблица из 8); Minor 4 — `os.WriteFile` зеркала неатомарен (артефакт в git, восстановим); Minor 5 — `ctx` затеняет пакетную переменную; Minor 6 — «1 строк»; Minor 7 — зеркало молча заменяется пустым при отсутствии реестра в хранилище (предписано брифом, стоит упомянуть в README).
- Task 5: Ruling (Important 1): требовать оба scope (`cloud_api:disk.write` + `cloud_api:disk.read`) — код читает Диск, а покрывает ли `write` чтение, справка Яндекса не говорит; правка env-примера и плана (строка ~285). Стоимость ошибки: лишний scope в токене.
- Task 5: Ruling (Important 2): `ARCHIVE_BACKEND=yandex` в примере закомментировать (включение — после заполнения токена), комментарий переписать: пустой backend = выключено, нерабочий токен при включённом = деградация в полные прогоны. Стоимость ошибки: лишний шаг оператора при включении.
- Task 5: Ruling (минор 3): свёрнут в fix round 1 (doc-противоречие в README зеркала). Миноры 4–7 — deferred.
- Task 5: fix round 1/5 — resume agent-174.

- Task 5: fix round 1/5 (3 addressed, 0 open; коммит `3c0f060`).
- Task 5: re-review (agent-177, haiku): все находки ADDRESSED, новых Critical/Important нет. Вне диффа замечено: `Makefile:161` `env-check` покажет OK на `REPLACE_ME` (косметика, deferred); комментарий `archive/registry.go:23` повторяет снятое в README противоречие («4 поля = весь состав») — свернуть в T6.
- Task 5: complete (commits 4a86c31..3c0f060, review clean).
- Task 6: BASE `3c0f060`; бриф task-6-brief.md; диспатч (deepseek-flash). Свёрнуты дрейфы: спека §3.1 и план (ImportXls-обёртка), `cbr/mo_br.go:97–99`, `util/clickhouse_import.go:88`, `archive/registry.go:23`.

- Task 6: implementer DONE (agent-180) — коммит `ebc2ef1` (10 файлов, `make all` зелёный), отчёт `task-6-report.md`. AGENTS.md изменён ровно на ссылку `docs/ROADMAP_INFRA.md` — подтверждено диффом коммита.
- Task 6: review (agent-181, haiku) — **Needs fixes**: Important 1 — `ARCHITECTURE.md:253` обобщил dev-ловушку «ARCHIVE_BACKEND не держать в env» на staging, где бэкенд как раз задаётся в `/etc/clickhouse-import-rosstat.env` (`env.example:35–37`, `dagu.service:12`); Important 2 — `README.md:428` трактует 403 однозначно как квоту, но по свидетельству (rclone #9848) нехватка scope даёт `ForbiddenError`; официальные страницы кодов Яндекса 404. Minor: комментарий «правило 4» к `opTimeout` (`yandex.go:22,61`); расхождение пробелов `env-check`/README; объединённые env-строки в §5; формулировка §14 стандарта; `PR #61` в ROADMAP_INFRA не проверен; спека §5 относит 423 к квоте, а 429/503 — к повторам (код: 423 → ErrQuota, 429/503 → повтор) и др.
- Task 6: Ruling (Important 2): правим README на двустороннюю подсказку (403 = нехватка прав ИЛИ квота; проверять scope и путь `disk:/` vs `app:/`) **и** минимальный код-фикс: фрагмент тела ответа (≤200 байт, одной строкой, как `util.ErrorSnippet`) в текст ошибки статуса — дежурный видит причину в логе; маппинг 403 → `ErrQuota` не меняем (сниппет различает причины в рантайме). Стоимость ошибки: ~15 строк + тест в `archive/`.
- Task 6: Ruling (минор «спека §5»): свёрнут в fix round 1 — привести классификацию 423/429/503 в спеке к коду.
- Task 6: fix round 1/5 — resume agent-180.

- Task 7: живая приёмка (коммит `bf62e11`): (1) `make all` зелёный (lint 0 issues, race-тесты все ok, обе сборки); (2) local-цикл: warn «версия известна, но не в архиве» → `Imported 309 rows`, объект + `registry.jsonl`, sha256 объекта = реестр (`b1027ae6…`), повтор — чистый no-op, удаление объекта → self-heal восстановил его с тем же sha256; строка `source_publications` проверена admin-SQL (`points = 309`, fingerprint `1024093306…`); (3) Яндекс.Диск: загрузка в `disk:/rosstat-archive/cbr_m2/…`, `make archive-registry` скачал реестр с Диска («1 строк»), повтор — no-op против Диска, папка и файл подтверждены владельцем в веб-интерфейсе; (4) `make env-check` — `ARCHIVE_YANDEX_TOKEN задан`; (5) `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА. Статус шага 1.1 в `docs/ROADMAP_INFRA.md` — ✅ с блоком приёмки; зеркало `docs/archive/registry.jsonl` закоммичено.
- Task 7: complete (commits 43539ad..bf62e11).
- Конфиг окружения (по AGENTS.md, финальное ревью — effort max): `[thinking].effort` поднят `high → max` в `~/.kimi-code/config.toml` (бэкап `config.toml.20261014-230741.bak`, `kimi doctor config` — OK, права 600 сохранены). Ожидается `/reload` владельца; финальный ревьюер будет диспатчен с `model: "primary"` (primary = deepseek-flash + наследует effort сессии = max).
- Финальное ревью: пакет `review-9b92d1d..bf62e11.diff` (394 919 байт) уже собран; после /reload — диспатч по `code-reviewer.md` на primary.

- Финальное ревью ветки (agent-186, primary/max, пакет `review-9b92d1d..bf62e11.diff`): **With fixes** — Critical нет; ревьюер лично прогнал `gofmt`/`vet`/`lint`/`go build`/`go test -race -count=1 ./...` (все ok), сверил артефакты приёмки по sha256/размеру/правам, подтвердил «схема/гранты/зависимости не тронуты», PR #61 = MERGED.
- Финальное ревью, Important 1: строка реестра может описывать содержимое, которого нет в архиве, когда отпечаток не изменился, а содержимое — да (`save` пропускает Put при существующем объекте, но пишет новый sha в реестр). Ruling: **убрать `Exists`-предпроверку из `save`** — Put всегда, затем Ensure; инвариант становится конструктивным, цена — повторная загрузка идентичных байтов в редких ветках (смена версии/самолечение); обновить формулировки спеки §3.1/§3.3 и др. + regression-тест «объект с другими байтами под тем же ключом перезаписывается, sha строки = sha объекта». Стоимость ошибки: лишний `Put` в редком пути.
- Финальное ревью, Important 2: утверждение «потерянная строка реестра самолечится» неверно при живом объекте (self-heal срабатывает по потере объекта). Ruling: привести спека §3.4/§5 к реальности + добавить в `docs/ROADMAP_INFRA.md` шаг 1.2 явные подпункты: реконсиляция «объект без строки», исключение `*.tmp` в листингах, дубли под `sha-`/fp-ключами. Дорогой путь (проверка реестра на каждом no-op) отклонён. Стоимость ошибки: шаг 1.2 должен реализовать реконсиляцию.
- Финальное ревью, рекомендация 3 (включена ruling'ом): защита git-зеркала от молчаливого затирания пустым реестром — отказ без `ARCHIVE_REGISTRY_ALLOW_EMPTY=1` при пустом скачанном реестре и непустом существующем зеркале; обновить `docs/archive/README.md` (+ env-таблицы, если там уже есть переменные архива).
- Финальное ревью, triage отложенных миноров: «обязано быть починено до merge — ничего из deferred»; миноры №№ из отчёта оставлены на отдельный hardening-коммит после merge (рекомендация 4 ревьюера).
- Final fix wave: ONE диспатч (deepseek-flash) на три пункта; отчёт `final-fix-report.md`.

- Final fix wave: `bbf4a8b` + `71f3a49`; scoped re-review (agent-189, haiku): все три находки ADDRESSED, новых Critical/Important нет; ⚠️-прогоны резолвнуты контроллером (`gofmt` пусто, `vet`/тесты зелёные, защита зеркала проверена вручную — отказ + sha зеркала не изменился).
- Финальная верификация (свежая, в том же сообщении с отчётом): `make all` RC=0; `make mcp-check` ПРОВЕРКА ПРОЙДЕНА; HEAD `71f3a49`; рабочее дерево чистое, кроме правки владельца `deploy/staging/dagu.service`.
- Ветка запушена, **PR #62** открыт: https://github.com/kmlebedev/clickhouse-import-rosstat/pull/62 (17 коммитов от `main` 9b92d1d; merge решает владелец по AGENTS.md).
- Workspace SDD сохранён до merge (отчёты, брифы, диффы, этот ledger); после merge — `rm -rf .superpowers/sdd/2026-10-14-source-file-archive`.
- Отложенные миноры (hardening, не блокируют merge): nil-guard в `NewYandex`; текст «после 3 попыток» при исчерпании повторов; тест на `503`; двойное «запись: запись:» в `LocalStore`; `Dir="/"` и `a/../b` в `LocalStore`; замена только первого дубля key в `upsert`; `GetFileBytes` двойное чтение тела; тесты для `runArchiveRegistry`; конвенция «(Review Focus №N)» в комментариях.
