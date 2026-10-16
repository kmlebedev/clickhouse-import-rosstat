# План: восстановление БД из архива (`archive-restore`) — срез инфра-пункта 1.2

**Спека:** [2026-10-16-archive-restore-design.md](../superpowers/specs/2026-10-16-archive-restore-design.md).
**Пункт:** [ROADMAP_INFRA.md](ROADMAP_INFRA.md) 1.2; ветка `archive-restore` (поверх ветки `rosstat-ipc-weeks-bulletins` — первый потребитель восстановления).
**Ветка пункта:** стек на `rosstat-ipc-weeks-bulletins` (PR #78): восстановление демонстрируется на источнике бюллетеней; после merge #78 база PR переключается на `main`.

## Критерий приёмки среза

Критерий пункта 1.2: «Восстановленный из архива ряд совпадает с таблицей по числу точек и spot-значениям; прогон на пустой БД доходит до витрин». Проверка: пустая `rosstat_ipc_weeks_bulletins` → `make archive-restore STAT=rosstat_ipc_weeks_bulletins ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` → **6 106 точек / 71 неделя** без обращений к сети; споты протокола PR #78; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; повтор — идемпотентен; `make archive-list` (в т.ч. дубли и локальная реконсиляция); `make all`; `dagu` не затрагивается (новых шагов нет).

## Задачи

### Task 1: `archive` — `LoadEntries` + `LocalStore.List` (спека §3.2)
Файлы: `archive/registry.go`, `archive/local.go`, `archive/*_test.go`.
`LoadEntries(ctx, store)` — публичное чтение реестра (переиспользовать разбор приватного `read`); `LocalStore.List(ctx)` — обход каталога, возврат ключей `<источник>/<файл>`, пропуск `*.tmp` и `registry.jsonl`. Тесты: roundtrip через `Registry.Ensure`; пропуск tmp/registry; отсутствие каталога — пустой список (не ошибка).
Готово: `go test -race -count=1 ./archive/`.

### Task 2: `util` — `RestoreVersion` + режим restore в шаблоне (спека §3.1)
Файлы: `util/archive.go`, `util/clickhouse_import.go`, `util/*_test.go`.
`ArchiveRestoreMode()` (env `ARCHIVE_ACTION == "restore"`); `RestoreVersion(ctx, source, url)` — резолв по реестру (последняя по `first_seen`), `Store.Get`, проверка sha256 (расхождение — ошибка), ошибки «архив не настроен»/«версии нет» с URL. В `Import()`: режим restore — байты из `RestoreVersion` вместо `GetFileBytes` (обе ветки: `ImportFunc`/`BodyFunc`), префлайт пропущен, повторная архивация пропущена, строка версии — из `fingerprint` строки реестра; `CrawFunc` + restore → ошибка. Лог «версия из архива: key (url)».
Готово: тесты восстановления (успех/нет версии/битый sha/не настроен) + импорт из архива на фейковом соединении без сети (URL заведомо нерезолвимый); `go test -race -count=1 ./util/`.

### Task 3: `main.go` + `Makefile` (спека §3.1, §3.3)
Файлы: `main.go`, `Makefile`.
`ARCHIVE_ACTION`: `registry` (как было), `list`, `restore` (не завершает процесс — продолжает импорт с выбранными `CLICKHOUSE_IMPORT_STAT`; при пустом фильтре… уточнить и назвать в отчёте: restore без `CLICKHOUSE_IMPORT_STAT` — ошибка, чтобы случайно не поднять «полный restore всего»). `runArchiveList`: печать версий, пометка дублей по sha256, `ARCHIVE_RECONCILE=1` — объекты без строк (через листинг; для бэкендов без листинга — явное сообщение, exit 1). Цели `archive-list`, `archive-restore` по образцу `archive-registry` (не печатают секретов).
Готово: `go build ./...`, `make -n archive-list archive-restore` (печатает команды без секретов), ручная проверка `ARCHIVE_ACTION=list` на локальном архиве.

### Task 4: документация (стандарт §13)
Файлы: `README.md` (`ARCHIVE_ACTION` в таблице env; цели `archive-list`/`archive-restore`; эксплуатационный абзац), `ARCHITECTURE.md` (раздел про архив: семантика restore + «версия из архива», границы CrawFunc), `docs/ROADMAP_INFRA.md` (статус 1.2: срез ✅/остаток — `archive-get`, листинг yandex; протокол).
Готово: `make all` зелёный; grep-сверка чисел.

### Task 5: живая приёмка (контроллер, не субагент)
По критерию выше: `make archive-list` (числа), опустошение таблицы (`ALTER TABLE … DELETE` всех 71 дат), `make archive-restore …`, сверка 6 106/71 и спотов, `make mcp-check`, повтор restore. Протокол — в план и комментарий PR.

### Task 6: PR (контроллер)
Ветка запушена, PR с базой `rosstat-ipc-weeks-bulletins` (после merge #78 — переключить на `main`); описание: что, как запускать, «важно для деплоя» (restore — ручное действие, в расписании отсутствует).

## Порядок диспатча

Task 1–4 — один субагент `deepseek/deepseek-flash` (связный срез, ~1 файл кода на кусок); ревью — `anthropic/haiku` (не на модели автора), fix-раунд при необходимости; Task 5–6 — контроллер. В coder-промпт — блок про `mcp__jetbrains__*`/`mcp__clickhouse__*` (AGENTS.md).

## Чек-лист жёстких правил для ревью-промпта

(1) секретов нет (значения env не печатаются); (2) вставка — батчем шаблона; (3) нет сети в `init()` (env читается в момент действия); (4) сеть restore-путём не вызывается вовсе; (5) ошибки не проглатываются, sha256 проверяется; (6) `CrawFunc`-границы названы; (7) тесты офлайн; (8) документация тем же PR; (9) роадмап 1.2 обновлён честно (срез/остаток); (10) `make all` зелёный.
