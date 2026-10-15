# SDD ledger — plan: docs/superpowers/plans/2026-10-10-staging-deploy-phase1.md

Branch: staging-deploy-phase1
Spec: docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md

## Pre-flight scan

Shared interfaces checked (task → task, produces vs consumes):

- Task 1 → Tasks 3,4,5,6: produces `deploy/staging/dagu.service`, `mcp-clickhouse.service`,
  `clickhouse-import-rosstat.env.example`; tasks 3–6 `scp` these exact paths. MATCH.
- Task 2 → Task 6: produces `CH_SETUP_URL`/`CH_SETUP_USER`/`CH_SETUP_PASSWORD` and Basic-auth;
  Task 6 `mcp-user-staging` sets exactly these three. MATCH.
- Task 3 → Tasks 4,5,6: produces `STAGING_HOST`, `STAGING_BIN_DIR`, `STAGING_ENV_FILE`,
  `MCP_VERSION`, `DAGU_VERSION`, target `staging-precheck`, `deploy-staging-binary`;
  Tasks 4,5,6 consume all of them verbatim. MATCH.
- Task 3 → Task 6: `staging-precheck` used as prerequisite of `deploy-staging-dagu`/`-mcp`. MATCH.
- Task 4 → Task 6: produces `deploy-staging-dagu`; Task 6 `upgrade-staging` depends on it. MATCH.
- Task 5 → Task 6: produces `deploy-staging-mcp`; Task 6 `upgrade-staging` depends on it. MATCH.
- Task 6 → Task 7: produces live services on :8000/:8123; Task 7 checks via tunnel. MATCH.
- Task 6 internal: `deploy-staging` calls `deploy-staging-preflight` and `deploy-staging-smoke`,
  both defined in Task 6 itself. Self-consistent.
- Task 6 → Task 4: `deploy-staging` depends on `deploy-staging-dagu`, which runs `staging-precheck`.
  Chained, no conflict.
- Task 2 self-consistency: `auth_headers` defined and used in the same step. OK.
- Task 1 self-consistency: `.gitignore` negations are ordered (`*.env` before `!*.env.example`). OK.

Pre-flight: no unresolved interface conflicts.

## Environment findings (before Task 1, live probes 2026-10-10)

Probed the actual `/etc/clickhouse-import-rosstat.env` the user placed on staging. Names and
emptiness only; values never read or printed.

- `CLICKHOUSE_URL` — задан. Форма: `clickhouse://localhost:9000/default` — **без имени пользователя**.
- `CLICKHOUSE_PASSWORD` — задан.
- `CLICKHOUSE_MCP_AUTH_TOKEN` — **ПУСТ**. mcp-clickhouse в HTTP-режиме падает на старте без него.
- Права файла — **644** (спека требует 600).

Task 6: Ruling: `CLICKHOUSE_MCP_AUTH_TOKEN` пуст — сервис mcp-clickhouse не стартует, пока владелец
не сгенерирует токен (`openssl rand -hex 32`) и не впишет его в env-файл. Исполнять Tasks 3–6
можно, но `deploy-staging-smoke` и старт сервиса MCP будут падать на пустом токене; это ожидаемое
поведение, а не дефект кода. Стоимость ошибки: деплой сообщит о провале там, где нужен один
`openssl rand -hex 32` от владельца.

Task 6: Ruling: права env-файла на staging — 644, спека требует 600. Правку прав выполняет владелец
хоста; деплой-цели права не меняют (иначе скрипт трогает секреты, что запрещено правилом 1 AGENTS.md).
Стоимость ошибки: пароль ClickHouse читаем локальными пользователями хоста.

Task 6: Ruling: `CLICKHOUSE_URL` без имени пользователя — импортёры пойдут от `default`. Это
рабочая конфигурация для staging (у `default` есть права на запись), менять не буду: имя
пользователя-писателя — решение владельца, а не деплой-скрипта. Стоимость ошибки: при смене
пароля `default` импортёры перестанут писать; лечится правкой одной строки в env-файле.

## Task log

Task 1: complete (commits 4fc9ecc..c2f5715, tests: git check-ignore -q (env ignored, example not) → pass; systemd-analyze verify on host → mcp clean, dagu missing-binary warning only)

Task 2: Ruling: план не предусматривал автотест (в проекте для scripts/ тестов нет, спека §3.7 называет проверку ручной). Добавил scripts/mcp_setup_user_test.py и вынес логику в чистые функции setup_url()/auth_headers(), чтобы поведение можно было проверить без сети. Причина: TDD-скил требует наблюдать падение теста; ручная проверка «на staging» недоступна до Tasks 3–6, то есть весь Task 2 остался бы без единой проверки. Стоимость ошибки: один лишний файл теста в репозитории.
Task 2: Ruling: скрипт получил функцию setup_url() — план встраивал выбор URL инлайном. Причина: инлайн-выражение нельзя проверить тестом, а именно оно и было предметом правки. Стоимость ошибки: небольшое расхождение структуры с планом.

Task 2: Ruling: добавлен CH_SETUP_SQL. План не предусматривал: SQL_PATH был относительным, и на staging скрипт падал с FileNotFoundError при запуске из /tmp (проверено живой пробой). Без этого правки Task 2 неработоспособны на хосте. Стоимость ошибки: одна лишняя переменная окружения.
Task 2: Ruling: исправлено маскирование пароля (mask()). Исходный слепой replace(password, "***") уродовал слово DB::Exception при коротком пароле (проверено живой пробой: 'x' → 'E***ception'). Это предсуществующий дефект, вскрытый правкой. Стоимость ошибки: искажённый текст ошибки в логах.
Task 2: complete (commits c2f5715..HEAD, tests: python3 scripts/mcp_setup_user_test.py → 11/11 pass; go test -race ./... → ok; живая проба на staging: Basic-auth уходит, отвергается 194, пароль не течёт)

Task 3: complete (commits e54e303..HEAD, tests: make -n deploy-staging-binary → targets expand; live: first run "бинарь обновлён", second run "не изменился … копирование пропущено"; precheck with absent env → clear error + exit 1)
Note: .PHONY получил deploy-staging-preflight вместо plan's deploy-staging-smoke только как расширение — обе цели объявлены в Task 6.

Task 4: complete (commits 5d1f004..HEAD, tests: make -n deploy-staging-dagu → URL correct; live: "dagu обновлён до v2.18.2", rerun → "dagu 2.18.2 уже установлен"; ssh dagu version → 2.18.2; systemd-analyze verify /etc/systemd/system/dagu.service → clean)

Task 5: complete (commits fae981c..HEAD, tests: make -n deploy-staging-mcp → expands; live: unit installed, uv resolves mcp-clickhouse==0.7.0 (download observed), systemd-analyze verify → clean)

Task 6: Ruling: НЕ генерирую CLICKHOUSE_MCP_AUTH_TOKEN сам. Запись живого секрета в инфраструктуру заказчика — это остановка по правилам executing-plans (security-sensitive), и правило 1 AGENTS.md отдаёт секреты владельцу. Стоимость ошибки: деплой не доведён до конца в этой сессии; владельцу нужно выполнить одну команду (openssl rand -hex 32 → в env-файл), после чего make deploy-staging завершается без правок кода.
Task 6: Ruling: smoke «MCP с токеном» добавлен заголовок Accept: application/json, text/event-stream. План его не содержал, но mcp-clickhouse 0.7.0 реализует MCP 2026-07-28 и может ответить SSE-потоком; без Accept запрос рискует получить 406. Стоимость ошибки: одна лишняя строка заголовка.
Task 6: partial (commits 0c30747..b7e3a3a) — код готов и проверен, живой прогон остановлен на preflight.
  Проверено живыми прогонами: `make deploy-staging-binary` (бинарь обновлён, повторно — пропуск по sha256);
  `make deploy-staging-dagu` (v2.18.2, повторно «уже установлен», dagu version → 2.18.2);
  `make deploy-staging-mcp` (юнит установлен, uv разрешил mcp-clickhouse==0.7.0);
  13 DAG-файлов скопированы в /root/dagu; `systemd-analyze verify` обоих юнитов → clean;
  `make deploy-staging-preflight` → 3 OK, затем корректный FAIL на пустом CLICKHOUSE_MCP_AUTH_TOKEN.
  НЕ проверено (требует токена): enable --now, smoke dagu/:8000/:mcp, upgrade-staging, mcp-user-staging.
  Сервисы остались disabled/inactive — DAG'и не запускались (проверено).

Task 7: complete (commits b7e3a3a..HEAD, tests: make -n mcp-check default → localhost:8123; override → 127.0.0.1:8123; unreachable port → clear error naming host:port + exit 1)
Note: план правил только mcp-check; правка заодно затронула проверочный curl внутри той же цели (он тоже был прибит к localhost и падал бы при удалённом адресе).

Task 8: complete (commits 4b5eef7..8b2aa05, tests: make all → green (gofmt, golangci-lint, go vet, go test -race, оба билда); python3 scripts/mcp_setup_user_test.py → 11/11)

Final: fixed missing version pins — scripts/mcp_check.py + .kimi-code/mcp.json RED (0 pins) → GREEN (pinned; env override verified, default 0.7.0), suite make all green
Final: fixed dagu binary swapped without restart — RED (restart only in upgrade-staging) → GREEN (restart guarded by is-active), make -n verified
Final: fixed /health failure naming no next step — RED (0 mcp-user-staging mentions) → GREEN (message names make mcp-user-staging), verified
Final: fixed silent enable-leave on smoke failure — RED (0 disable hints) → GREEN (prints systemctl disable --now dagu), make -n verified
Final: minor (deferred): env-file perms 644 vs spec 600 — deploy neither checks nor warns
Final: minor (deferred): uv warm cache uses `uv tool run --from` while unit uses `uv run --with` — different caches, warm-up may not help first start
Final: minor (deferred): token smoke posts tools/list without prior initialize — never exercised live
Final: minor (deferred): mask() can under-mask a secret embedded in a wider alphanumeric token
Final: minor (deferred): staging-precheck pins localhost:8123; preflight hardcodes /root/dagu/cbr.yaml
Final: minor (deferred): mcp-user-staging leaves /tmp scripts on host

Final: fixed dagu unit missing HOME — живой отказ "could not determine home directory: $HOME is not defined" на старте сервиса. RED: воспроизведено на хосте `env -u HOME dagu start-all` → та же ошибка; `dagu version` при этом работает, поэтому дефект невидим на этапе deploy-staging-dagu. GREEN: Environment="HOME=/root" в юните → systemctl start dagu → active, curl :8080 → 200. Suite: make all green.
  Побочно подтверждено живым прогоном: catchup-запуски cbr/fred/minfin → status=succeeded (импортёры реально отработали); DAGU_COORDINATOR_ENABLED=false действительно отключает координатор; сервис слушает только 127.0.0.1:8080.
