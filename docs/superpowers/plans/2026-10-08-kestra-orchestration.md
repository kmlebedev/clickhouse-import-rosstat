# Dagu: оркестрация импортов ClickHouse — план внедрения

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перенести расписание наполнения ClickHouse в self-hosted Dagu (бинарник, без Docker) на одном сервере: сначала пилотный домен `cbr`, затем остальные домены; ошибки уходят в Telegram.

**Architecture:** `main.go` получает корректный код выхода и фильтр запуска. Dagu ставится бинарником под systemd от пользователя `dagu`, аутентификация `builtin`, UI на `127.0.0.1`. Один DAG-файл на домен в `dagu/`, каждый шаг — `run:` с бинарником `/opt/rosstat-import/bin/clickhouse-import-rosstat` и `CLICKHOUSE_IMPORT_STAT=<имя таблицы>`. Секреты — файлы `0600` через блок `secrets` в `base.yaml`. Telegram-уведомление — `handler_on.failure` в `base.yaml`.

**Tech Stack:** Go (репозиторий), ClickHouse, Dagu (бинарник, актуальная версия после проверки advisory), systemd, Telegram Bot API через `curl`.

**Spec:** `docs/superpowers/specs/2026-10-08-kestra-orchestration-design.md`

## Global Constraints

- Сборка бинарника: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, `-ldflags="-s -w"` (как в `makefile`).
- Dagu: `DAGU_AUTH_MODE=builtin`, `DAGU_HOST=127.0.0.1`. Версия — после проверки всех advisory из спецификации §7; `basic` и `none` не используются.
- Секреты — только файлы `/etc/dagu/secrets/<имя>` с правами `0600`, владелец `dagu`. В YAML, коде и документации значений нет.
- В коде только `os.Getenv`. Сетевых вызовов в `init()` нет (за исключением пакета `cbr`, см. Task 6).
- Проверка сборки: `go build ./... && go vet ./...`.
- В репозитории нет тестов. Тестовые файлы не создаются. Проверки выполняются командами из шагов.
- `git commit` выполняется только по прямой просьбе пользователя.
- Серверные задачи (Task 2, 3, 4, 7 и серверная часть Task 1) отложены. Деплой будет выполняться из GitHub Actions; до этого работаем в dev-окружении на Mac: ClickHouse через `clickhousectl`, Dagu 2.18.2 из Homebrew, dev-home вне репозитория.
- Ключи конфигурации Dagu (`base.yaml`, `retry_policy`, `handler_on`, `secrets`, `schedule`) сверяются с документацией установленной версии перед использованием.

## Review Focus

1. Пустой `CLICKHOUSE_IMPORT_STAT` запускает все импортёры, а не ни одного. Проверка — Task 1, шаг 4.
2. Опечатка в имени импортёра не даёт зелёного прогона без данных молча. Проверка — Task 1, шаг 4.
3. Падение одного импортёра при успешных остальных: остальные выполняются, код выхода 1. Проверка — Task 1, шаг 4.
4. Невалидный `CLICKHOUSE_URL`: ненулевой код, DSN не попадает в вывод. Проверка — Task 1, шаг 4.
5. Недоступный Telegram в момент алерта: DAG завершается с ошибкой, видной в UI, и не зависает. Проверка — Task 7, шаг 3.

---

## Этап A. Код и инфраструктура

### Task 1: Исправить `main.go`, дополнить `go.sum`

**Files:**
- Modify: `main.go:25-80`
- Modify: `go.sum` (дополняется через `go mod tidy`, `go.mod` не меняется)

**Interfaces:**
- Consumes: `chimport.Stats []ImportStat`, `ImportStat.Name() string`, `ImportStat.Import(ctx, conn) (int64, error)`.
- Produces: поведение бинарника: код выхода 1 при ошибке любого импортёра; `CLICKHOUSE_IMPORT_STAT` пустой — все импортёры; неизвестное имя — предупреждение в логе, код 0.

- [x] **Step 1: Базовая сборка** — выполнено; сборка падала из-за неполного `go.sum`, см. Step 5.
- [x] **Step 2: Фильтр** — `parseImportFilter` разбирает список, пустые элементы отбрасываются, неизвестные имена дают `log.Warnf`.
- [x] **Step 3: Код выхода и ParseDSN** — флаг `failed`, `os.Exit(1)` после цикла; ошибка DSN — `log.Fatal("invalid CLICKHOUSE_URL")`.
- [x] **Step 4: Проверка поведения (часть)** — невалидный DSN: выход 1, DSN в выводе нет. Недоступный ClickHouse: выход 1, пароль в выводе нет. Фильтр `parseImportFilter` проверен временным тестом, удалён.
- [x] **Step 4a: Проверка на реальном ClickHouse (dev, 2026-10-08)**
  - `CLICKHOUSE_IMPORT_STAT=no_such_table` → предупреждение, код 0. ✔
  - `CLICKHOUSE_IMPORT_STAT=cbr_key_rate` → `Imported 3275 rows`, код 0. ✔
  - Падение одного импортёра: во временной копии (`/tmp/rosstat-test`, с фиктивными `zz_fake_fail` и `zz_fake_ok`, в репозитории не сохранено) фильтр `zz_fake_fail,zz_fake_ok` → код 1, оба импортёра выполнены. ✔
  - Только успешный импортёр → код 0. ✔
  - Пустой фильтр запускает импортёры (в первые 90 секунд стартовали 4 разных, включая legacy `financial`). ✔
  - Найдено: `polyus_financial_metrics` падает без `pdftotext` (на Mac его нет). С новым кодом выхода это сделает весь прогон красным. Нужно установить `poppler` на сервер или исправить импортёр. Отдельная задача, записано в журнал.
- [x] **Step 5: `go.sum`** — `go mod tidy` выполнен, `go.mod` не изменился, `go.sum` 196 → 260 строк. Сборка проходит.
- [x] **Step 6: Сборка** — `go build ./... && go vet ./...` проходят.

Commit: только по просьбе пользователя; `main.go` и `go.sum` коммитятся вместе.

### Task 2: Установить Dagu на сервер

**Files:**
- Create (на сервере): `/usr/local/bin/dagu`, `/etc/systemd/system/dagu.service`, `/etc/dagu/config.yaml`, `/opt/dagu/dags/`

**Interfaces:**
- Consumes: сервер с ClickHouse, Linux.
- Produces: работающий сервис `dagu` на `127.0.0.1`, пользователь `dagu`, каталог DAG `/opt/dagu/dags`.

- [ ] **Step 1: Проверить advisory и выбрать версию**

Проверено 2026-10-08 по NVD и GitHub Advisory:
- CVE-2026-33344 (GHSA-ph8x-4jfv-v9v8): затронуты 2.0.0 — до 2.3.1, исправлено в 2.3.1.
- CVE-2026-31882 и CVE-2026-31886: исправлены в 2.2.4.
- CVE-2026-27598: исправление вошло в 2.0.0, но было неполным (см. CVE-2026-33344).
- Актуальная стабильная версия по странице релизов: v2.18.2.
- Нам нужна `handler_on`, а не маршрутизация уведомлений по областям. Баг в 2.11.0–2.11.2 касается только второй, поэтому на выбор версии не влияет.

Ставить v2.18.2 (или более новую стабильную). Версии ниже 2.3.1 не ставить.
Expected: записанная в заметке версия и ссылки на NVD/advisory.

- [ ] **Step 2: Установить бинарник**

Скачать архив релиза выбранной версии под linux/amd64, проверить контрольную сумму из релиза, положить `dagu` в `/usr/local/bin/dagu` (`root:root`, `0755`).
Run: `dagu version`
Expected: выбранная версия.

- [ ] **Step 3: Создать пользователя и каталоги**

Системный пользователь `dagu` без входа. Каталоги `/opt/dagu/dags`, `/etc/dagu/secrets` (`0700`, владелец `dagu`), `/var/lib/dagu` (владелец `dagu`).

- [ ] **Step 4: Конфигурация сервера**

В `/etc/dagu/config.yaml` или через переменные окружения systemd задать: `DAGU_AUTH_MODE=builtin`, `DAGU_HOST=127.0.0.1`, `DAGU_TZ=Europe/Moscow`, `DAGU_HOME=/var/lib/dagu`. Ключи сверить с документацией Dagu выбранной версии.
Expected: `dagu config` (или аналог) показывает значения без ошибок.

- [ ] **Step 5: systemd-сервис**

Unit: `User=dagu`, `ExecStart=/usr/local/bin/dagu start-all --dags /opt/dagu/dags`, `Restart=on-failure`, `TimeoutStopSec` не меньше `DAGU_WORKER_SHUTDOWN_TIMEOUT` + 15 с (значение по умолчанию 60 с, итого 75 с).
Run: `systemctl daemon-reload && systemctl enable --now dagu`
Expected: `systemctl is-active dagu` выводит `active`.

- [ ] **Step 6: Проверить привязку и доступность**

Run: `ss -ltnp | grep 8080`
Expected: адрес `127.0.0.1:8080`, не `0.0.0.0`.
Run: `curl -sf http://127.0.0.1:8080/` с учётом аутентификации, как указано в документации.
Expected: ответ сервера (не отказ в соединении).

### Task 3: Развернуть бинарник импортёра

**Files:**
- Create (на сервере): `/opt/rosstat-import/bin/clickhouse-import-rosstat` (`root:root`, `0755`), `.prev` для отката

**Interfaces:**
- Consumes: бинарник из Task 1 (`make build`).
- Produces: исполняемый файл, доступный пользователю `dagu` на запуск.

- [ ] **Step 1: Собрать бинарник**

Run: `make build`
Expected: `build/clickhouse-import-rosstat`, `file` показывает `ELF 64-bit ... x86-64, statically linked`.

- [ ] **Step 2: Скопировать на сервер**

Сохранить предыдущую версию как `.prev`, скопировать новую, права `0755`, владелец `root`.
Expected: `ls -l /opt/rosstat-import/bin/` показывает оба файла.

- [ ] **Step 3: Проверить запуск от пользователя dagu**

Run: `sudo -u dagu env CLICKHOUSE_URL=<из секрета> CLICKHOUSE_IMPORT_STAT=<имя таблицы> /opt/rosstat-import/bin/clickhouse-import-rosstat; echo $?`
Expected: импорт выполняется, код `0`, DSN и пароль в выводе отсутствуют.

### Task 4: Секреты и Telegram в `base.yaml`

**Files:**
- Create (на сервере): `/etc/dagu/secrets/clickhouse_url`, `/etc/dagu/secrets/clickhouse_cert_files`, `/etc/dagu/secrets/telegram_bot_token`, `/etc/dagu/secrets/telegram_chat_id` (`0600`, владелец `dagu`)
- Create (на сервере): `/var/lib/dagu/base.yaml` (путь по умолчанию в режиме `DAGU_HOME`)

**Interfaces:**
- Consumes: секреты из Task 2 (каталог), бинарник из Task 3.
- Produces: переменные `CLICKHOUSE_URL`, `CERT_FILES`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` для всех DAG; обработчик `handler_on.failure`.

- [ ] **Step 1: Записать секреты в файлы**

Значения вводятся вручную на сервере, в чат и в git не попадают. Права `0600`, владелец `dagu`.
Expected: `ls -l /etc/dagu/secrets/` показывает четыре файла с `-rw-------`.

- [ ] **Step 2: Написать `base.yaml`**

Содержимое по спецификации §3.5 и §3.6: блок `secrets` с провайдером `file`, `shell: ["bash", "-e", "-o", "pipefail"]`, `retry_policy` в шагах (а не в base), `handler_on.failure` с вызовом `curl` к `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage` и данными `chat_id`, текстом `${context.dag.name}` и `${context.run.id}`. Синтаксис сверить с документацией Dagu выбранной версии (раздел Base Configuration и Secrets).
Expected: `dagu validate` на тестовом DAG не выдаёт ошибок.

- [ ] **Step 3: Проверить Telegram**

Временный DAG `telegram-check.yaml` в `/opt/dagu/dags` с шагом `run: exit 1`. Запуск из UI.
Expected: сообщение приходит в Telegram, DAG в статусе `failed`. Файл `telegram-check.yaml` после проверки удаляется.

### Task 5: Пилотный DAG `cbr`

**Files:**
- Create: `dagu/cbr.yaml` (в репозитории), копия на сервере: `/opt/dagu/dags/cbr.yaml`

**Interfaces:**
- Consumes: бинарник (Task 3), секреты и handler (Task 4).
- Produces: DAG `cbr`, по одному шагу на каждый импортёр пакета `cbr`, `id` шага = имя таблицы.

- [ ] **Step 1: Собрать имена таблиц пакета `cbr`**

Для каждого импортёра выписать `Name()`. Для структур с полем `TableName` в `util.HdBase` и `util.ClickHouseImport` имя берётся из `TableName`.
Run: `grep -n "Name() string" -A2 cbr/*.go` и `grep -n "TableName" cbr/*.go`.
Expected: список из имён, записанный в заметке плана (не в YAML).

Результат по `cbr` (сделано 2026-10-08): зарегистрировано 14 импортёров, уникальных имён таблиц 12.
- `cbr_currency_usd` — импортёры `currency_usd` и `avgproc_stav` (оба возвращают `cbr_currency_usd`).
- `households_b_mes` — импортёры `households` и `queries` (`cbrQueriesDataset`, `Name()` возвращает `households_b_mes`, но пишет в `cbr_queries_<...>`; `DatasetId: 0`).
- `cbr_key_rate`, `cbr_m2`, `cbr_gold`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_infl_exp`, `cbr_bank_int_rate`, `cbr_credit_m2x`, `cbr_indicators_cpd`, `cbr_ruania`.
Dagu требует уникальные `id` шагов, поэтому шаг на каждое уникальное имя. Фильтр `CLICKHOUSE_IMPORT_STAT` выбирает все импортёры с совпавшим `Name()`, так что шаг `households_b_mes` запускает и `queries`. Это записано в журнал как решение; исправление `Name()` у `cbrQueriesDataset` — отдельная задача.

- [ ] **Step 2: Написать `dagu/cbr.yaml`**

Содержимое (готово локально, 12 шагов):
- `type: chain` — последовательное выполнение. В Dagu по умолчанию `type: graph`, где шаги без `depends` идут параллельно.
- `schedule: "17 9 * * *"`, `overlap_policy: skip`.
- Для каждого импортёра — шаг:
  - `id: <имя таблицы>`;
  - `run: "$ROSSTAT_IMPORT_BIN"` (путь задаёт `base.yaml`: `env: ROSSTAT_IMPORT_BIN=/opt/rosstat-import/bin/clickhouse-import-rosstat` на сервере);
  - `env: [CLICKHOUSE_IMPORT_STAT=<имя таблицы>]`;
  - `retry_policy: { limit: 3, interval_sec: 300 }`.
- Шаги последовательные. Если таблица зависит от другой, — `depends:`.

Синтаксис `schedule`, `env`, `retry_policy`, `depends` сверить с документацией выбранной версии.

- [ ] **Step 3: Проверить YAML**

Run: `dagu validate dagu/cbr.yaml`
Expected: без ошибок.

- [ ] **Step 4: Запустить вручную**

Запуск из UI или `dagu start cbr`.
Expected: все шаги `success`; в ClickHouse `SELECT count() FROM <таблица> FINAL` больше нуля для каждой таблицы.

- [ ] **Step 5: Проверить лог шага**

Expected: строки `Imported N rows of <таблица>`; значения секретов в логе отсутствуют.

---

## Этап B. Остальные домены и эксплуатация

### Task 6: DAG для доменов `rosstat`, `minfin`, `customs`, `fao`, `bank`, `craw`, `financial`

**Files:**
- Create: `dagu/rosstat.yaml`, `dagu/minfin.yaml`, `dagu/customs.yaml`, `dagu/fao.yaml`, `dagu/bank.yaml`, `dagu/craw.yaml`, `dagu/financial.yaml`

**Interfaces:**
- Consumes: шаблон Task 5.
- Produces: один DAG на домен, имя файла = имя DAG.

- [ ] **Step 1: Выписать имена таблиц по доменам**

Методика — как в Task 5, Step 1. Для `financial` (legacy): проверить переменные `TICKER`, `INVESTING_EMAIL`, `INVESTING_PASSWORD`. `INVESTING_*` удаляются вместе с импортёрами по спецификации §3.5.

- [ ] **Step 2: Написать DAG по шаблону Task 5**

Расписание выбирается по самой частой частоте источника в домене; решение записывается в заметку.

- [ ] **Step 3: Проверить и запустить**

Для каждого файла: `dagu validate dagu/<домен>.yaml`, затем запуск вручную.
Expected: все шаги `success`, либо отказ источника, задокументированный в заметке (блокировка, недоступность), с ретраями и уведомлением.

Note: `cbr` содержит сетевые вызовы при старте (см. спецификацию §7). Этот пункт не блокирует Task 6, но в заметке фиксируется, что каждый запуск `cbr`-бинарника обращается к cbr.ru.

### Task 7: Эксплуатационные проверки

**Files:**
- Нет новых файлов.

**Interfaces:**
- Consumes: DAG из Task 5 и Task 6.
- Produces: подтверждённое поведение ретраев, уведомлений и перезапуска.

- [ ] **Step 1: Проверить ретраи**

Временно задать `CERT_FILES` на несуществующий файл в секретах `clickhouse_cert_files`, запустить DAG.
Expected: три повтора с паузой 300 с, затем статус `failed`, сообщение в Telegram. Значение `CERT_FILES` в сообщении не видно.

- [ ] **Step 2: Вернуть корректную конфигурацию**

Восстановить секрет, запустить DAG заново.
Expected: статус `success`.

- [ ] **Step 3: Недоступный Telegram**

Временно записать неверный `telegram_bot_token`, вызвать сбой шага.
Expected: DAG завершается с ошибкой, она видна в UI, сервис не зависает. Токен восстанавливается.

- [ ] **Step 4: Перезапуск сервиса во время выполнения**

Запустить DAG, на середине выполнить `systemctl restart dagu`.
Expected: после старта статус прерванного прогона отображается по политике Dagu; повторный запуск проходит, `SELECT count() FROM <таблица> FINAL` не уменьшается.

### Task 8: Обновить документацию

**Files:**
- Modify: `ARCHITECTURE.md` (§5 конфигурация, §7 календарь триггеров)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (§6 оркестрация, §11 Kimi Work)

**Interfaces:**
- Consumes: итоговые имена DAG и расписания из Task 5–6.
- Produces: документы, согласованные с кодом и DAG (AGENTS.md §10).

- [ ] **Step 1: ARCHITECTURE.md §5**

Описать `CLICKHOUSE_IMPORT_STAT` (пустая строка — все импортёры; неизвестное имя — предупреждение; код выхода 1 при ошибке импортёра) и секреты Dagu (файлы `0600` в `/etc/dagu/secrets`).

- [ ] **Step 2: ARCHITECTURE.md §7**

Указать, что расписание импортов задаётся DAG в `dagu/`. Календарь событий остаётся в этом разделе.

- [ ] **Step 3: ROADMAP §6 и §11**

§6: cron → Dagu. §11: Kimi Work остаётся для аналитики и MCP, запуск импортов в нём не используется.

- [ ] **Step 4: Проверка сборки**

Run: `go build ./... && go vet ./...`
Expected: без ошибок.

---

## Что не входит в план

- Переход на Kubernetes и распределённые воркеры Dagu. Отдельный дизайн, при росте нагрузки.
- Новые импортёры (`fred`, `eia`, `lbma_gold`, `moex_iss`).
- Чтение расписаний из `events_calendar`.
- Исправление init-вызовов в пакете `cbr` (отдельная задача).
- Коммиты — только по просьбе пользователя.
