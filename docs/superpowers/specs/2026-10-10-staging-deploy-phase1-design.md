# Дизайн: деплой на staging — Фаза 1 (dagu, mcp-clickhouse, бинарь импортёра)

**Дата:** 2026-10-10
**Статус:** на ревью
**Основание:** решение заказчика от 2026-10-10 — вариант A («единый хост-контур»), только Фаза 1.
**Связанные документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.5 (MCP), [README.md](../../../README.md) («MCP для Kimi»), [AGENTS.md](../../../AGENTS.md) правила 1 и 10, [спека gold-nav](2026-10-10-gold-nav-plugin-design.md) §7 (таблица прав), [ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md) §7 фаза 0.

---

## 1. Задача и критерии успеха

Развернуть на staging три компонента и научиться их обновлять **одной командой**: планировщик `dagu`, MCP-сервер `mcp-clickhouse` (HTTP, read-only для агентов) и бинарник `clickhouse-import-rosstat` (который запускают DAG'и).

Сейчас на staging: ClickHouse и Grafana работают (не трогаем), **dagu и mcp-clickhouse отсутствуют**, а `clickhouse-import-rosstat.service` — сломанный артефакт (пароль в открытом виде, `Restart=always` на короткоживущем процессе, **38 856 перезапусков** в журнале, `disabled`).

**Критерии успеха:**

1. `make deploy-staging` разворачивает все три компонента идемпотентно; повторный прогон без изменений не перезапускает сервисы.
2. `mcp-clickhouse` слушает `127.0.0.1:8000`, требует Bearer-токен: запрос к `/mcp` без токена → **401**, с токеном → работает.
3. `dagu` слушает `127.0.0.1:8080`, видит DAG'и из `/root/dagu`, переменная `ROSSTAT_IMPORT_BIN` указывает на наш бинарь.
4. `make mcp-check` из репозитория против staging через SSH-туннель → **ПРОВЕРКА ПРОЙДЕНА**.
5. `make mcp-user-staging` создаёт `kimi_reader` и выдаёт гранты; запрос к сырой таблице от него → `ACCESS_DENIED`.
6. `make upgrade-staging` обновляет dagu и mcp-clickhouse до версий, **зафиксированных в репозитории**.
7. Наружу ничего не публикуется: оба сервиса слушают только loopback.
8. Секретов в репозитории нет; юнит импортёра с паролем в открытом виде не воспроизводится.

---

## 2. Что проверено (фактическая база, живые пробы 2026-10-10)

### 2.1 Staging `palmshell`

SSH-алиас `palmshell` = `rock5b.sh3h.ru:12222`, вход `root`. Ubuntu 24.04.4 LTS, x86_64, 4 ядра, 15 ГБ RAM, 376 ГБ свободно.

| Объект | Состояние |
|---|---|
| `clickhouse-server.service` | active, слушает `0.0.0.0:8123` и `0.0.0.0:9000`, данные `/var/lib/clickhouse` — **31 ГБ** |
| `grafana-server.service` | active, Grafana 13.1.1, плагин `grafana-clickhouse-datasource` установлен |
| `dagu` | **отсутствует** (ни бинарника, ни юнита) |
| `mcp-clickhouse` | **отсутствует** |
| `caddy`/`nginx` | отсутствуют (не нужны в этой фазе) |
| `clickhouse-import-rosstat.service` | `disabled`, `inactive`; в юните пароль ClickHouse открытым текстом, `Description=My Go Application Service`, `Group=www-data` при `User=root` |
| бинарник `/root/bin/clickhouse-import-rosstat` | есть, от 8 июля 2026 |
| `uv` | **0.13.0**, в `/root/.local/bin/uv` — **не в PATH systemd** |
| `python3` | 3.12.3 (системный) |
| `go`, `node`, `pip3`, `kubectl`, `helm` | отсутствуют |
| GitHub Actions runner'ы | три, на **другие** репозитории (`Igoryok86/avito`, `kmlebedev/transaq-clickhouse-exporter`, `kmlebedev/txmlconnector`); для `clickhouse-import-rosstat` **не зарегистрирован** |

Ключевой факт: `systemd` не читает `.bashrc`/`.profile`, поэтому `PATH` юнитов — `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin`. `uv` и наши бинарники живут в `/root/.local/bin` и **не видны по имени**: в юнитах обязателен абсолютный путь.

### 2.2 Возможности `mcp-clickhouse` (PyPI 0.7.0, [README](https://github.com/ClickHouse/mcp-clickhouse))

| Возможность | Переменная |
|---|---|
| HTTP-транспорт | `CLICKHOUSE_MCP_SERVER_TRANSPORT=http` |
| Адрес прослушивания | `CLICKHOUSE_MCP_BIND_HOST`, `CLICKHOUSE_MCP_BIND_PORT` (по умолчанию `127.0.0.1:8000`) |
| **Авторизация (обязательна для HTTP)** | `CLICKHOUSE_MCP_AUTH_TOKEN`; **старт падает**, если для HTTP/SSE режим аутентификации не задан |
| Health-check | `GET /health`, намеренно **без** аутентификации |
| Read-only | `CLICKHOUSE_ALLOW_WRITE_ACCESS=false` (по умолчанию) |

Это означает: **reverse-proxy для авторизации в Фазе 1 не нужен** — сервер сам отвергает запросы без Bearer-токена. Caddy остаётся нужен позже (TLS и единый вход), но не как средство аутентификации.

### 2.3 `dagu`

Актуальный релиз — `dagu-org/dagu` **v2.18.2** (04.10.2026), артефакт `dagu_2.18.2_linux_amd64.tar.gz`. **Go на staging не нужен**: ставится готовый бинарник.

### 2.4 Расписание DAG'ов

Все **13** файлов `dagu/*.yaml` используют `$ROSSTAT_IMPORT_BIN`. Расписания срабатывают в течение суток:

```
17 9 * * *   23 9 * * 1   23 10 * * 1   41 10 10 * *   31 11 1 * *   41 11 * * *
41 11 15 * * 23 12 * * *  17 12 * * 4   37 12 * * *    47 10 5 * *   13 19 * * *   47 18 * * *
```

Следствие, которое обязано быть осознанным: **после `enable --now dagu` импортёры начнут реально ходить во внешние источники** (ЦБ, Росстат, FRED, BLS, BEA, MOEX, ФАО, ФТС, Минфин, банки) при первом совпадении времени. Деплой запускает прод-нагрузку, а не только ставит сервис.

### 2.5 Версии не зафиксированы

`mcp-clickhouse` вызывается **без пина** в четырёх местах: `Makefile:19`, `Makefile:149`, `.kimi-code/mcp.json:7`, `scripts/mcp_check.py:37`. `uv run --with mcp-clickhouse` разрешает версию при каждом запуске, поэтому сервис может молча получить новую версию при рестарте, а откатиться некуда.

---

## 3. Решения

### 3.1 Артефакты деплоя живут в репозитории

Новая папка `deploy/staging/`:

```
deploy/staging/
  dagu.service                              шаблон юнита dagu
  mcp-clickhouse.service                    шаблон юнита MCP
  clickhouse-import-rosstat.env.example     образец секретов (без значений)
  README.md                                 развёртывание, отладка, туннель
```

Сейчас юнит импортёра существует **только на staging** и не воспроизводим. Шаблоны в git делают конфигурацию staging проверяемой в PR — это тот же принцип, по которому в проекте лежат DDL и `sql/mcp_kimi_reader.sql`.

### 3.2 Раскладка на staging

```
/root/.local/bin/                       ← бинарники (по решению заказчика)
  clickhouse-import-rosstat
  dagu
  uv

/etc/systemd/system/
  dagu.service                          ← из deploy/staging/
  mcp-clickhouse.service                ← из deploy/staging/

/etc/clickhouse-import-rosstat.env      ← секреты, 600, вне git
/root/dagu/                             ← DAG'и (рабочая директория dagu)
```

**Выбор `/root/.local/bin` осознан против `/root/bin`** (где лежит старый бинарник): это стандартный пользовательский каталог, он уже прописан в `.bashrc`/`.profile` для интерактивных сессий, и в нём уже лежит `uv`. Старый `/root/bin/clickhouse-import-rosstat` деплой не трогает и не удаляет — удаление чужого артефакта вне объёма работы.

**Абсолютные пути в юнитах обязательны** (§2.1): `ExecStart=/root/.local/bin/dagu`, `Environment="ROSSTAT_IMPORT_BIN=/root/.local/bin/clickhouse-import-rosstat"`.

### 3.3 Юниты

`dagu` — долгоживущий сервис, `Restart=always` уместен:

```ini
[Unit]
Description=dagu — планировщик импортёров clickhouse-import-rosstat
After=network-online.target clickhouse-server.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/root/dagu
# start-all: сервер + планировщик + исполнитель в одном процессе (single-machine
# режим dagu). Порт и хост по умолчанию 127.0.0.1:8080 — проверяется при первом
# прогоне, и если версия dagu слушает иначе, адрес задаётся явными флагами.
ExecStart=/root/.local/bin/dagu start-all --dags /root/dagu
EnvironmentFile=/etc/clickhouse-import-rosstat.env
Environment="ROSSTAT_IMPORT_BIN=/root/.local/bin/clickhouse-import-rosstat"
Environment="DAGU_HOME=/root/dagu"
Restart=always
RestartSec=10
LimitNOFILE=65536
PrivateTmp=true
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

`mcp-clickhouse` — тоже долгоживущий:

```ini
[Unit]
Description=mcp-clickhouse (HTTP, Bearer) — read-only MCP для агентов
After=network-online.target clickhouse-server.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/root/.local/bin/uv run --with mcp-clickhouse==0.7.0 --python 3.12 mcp-clickhouse
EnvironmentFile=/etc/clickhouse-import-rosstat.env
Environment="CLICKHOUSE_HOST=localhost"
Environment="CLICKHOUSE_PORT=8123"
Environment="CLICKHOUSE_USER=kimi_reader"
Environment="CLICKHOUSE_DATABASE=default"
Environment="CLICKHOUSE_MCP_SERVER_TRANSPORT=http"
Environment="CLICKHOUSE_MCP_BIND_HOST=127.0.0.1"
Environment="CLICKHOUSE_MCP_BIND_PORT=8000"
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Секреты (`CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_MCP_AUTH_TOKEN`) — **только в `EnvironmentFile`**, в юнитах их нет. Это исправляет нарушение правила 1 AGENTS.md, которое сейчас допускает юнит на staging.

**`CLICKHOUSE_HOST=localhost`, а не `192.168.3.9`** (как в старом юните): внутри хоста адрес не должен зависеть от сетевого IP.

### 3.4 Секреты

Один файл `/etc/clickhouse-import-rosstat.env`, права `600`, владелец `root`, **не в git**. Образец в репозитории:

```bash
# deploy/staging/clickhouse-import-rosstat.env.example
CLICKHOUSE_URL=clickhouse://:REPLACE_ME@localhost:9000/default
CLICKHOUSE_PASSWORD=REPLACE_ME
CLICKHOUSE_MCP_AUTH_TOKEN=REPLACE_ME
```

- `CLICKHOUSE_URL`/`CLICKHOUSE_PASSWORD` нужны импортёрам (dagu) и серверу MCP соответственно.
- `CLICKHOUSE_MCP_AUTH_TOKEN` генерируется отдельно (`openssl rand -hex 32`) и **не равен** паролю ClickHouse: утечка токена лечится его отзывом, а не сменой пароля базы.
- `deploy-staging` **падает с понятной ошибкой**, если файла нет, и **никогда его не создаёт**: секреты не должны рождаться в деплой-скрипте.
- **`.gitignore` сейчас не защищает этот файл.** Проверено: в `.gitignore` есть только `/build/`, `/.idea/`, `coverage.out`, `/dist/` — паттерна для `*.env` нет. Поэтому в том же PR в `.gitignore` добавляется `*.env` (кроме `*.env.example`) — иначе файл с секретами можно случайно закоммитить, и правило 1 AGENTS.md будет нарушено не по злому умыслу, а по недосмотру.

### 3.5 Версии фиксируются в репозитории

В `Makefile` вводятся переменные:

```make
MCP_VERSION  ?= 0.7.0
DAGU_VERSION ?= v2.18.2
STAGING_HOST ?= palmshell
```

Следствия:

- `deploy-staging` ставит **известную** версию `mcp-clickhouse` (пин попадает и в юнит);
- обновление = **правка пина в репозитории** → PR → деплой → проверка; обновление становится ревьюируемым, а не невидимым;
- откат = вернуть прежний пин и задеплоить снова.

Тот же пин добавляется в `Makefile:19`/`:149` и `.kimi-code/mcp.json`, чтобы локальная разработка и staging запускали одну версию.

### 3.6 Makefile-цели

```make
deploy-staging:            # всё: бинарь + dagu + mcp + юниты + DAG'и + smoke
deploy-staging-binary:     # только бинарник импортёра
deploy-staging-dagu:       # только dagu
deploy-staging-mcp:        # только mcp-clickhouse
upgrade-staging:           # обновить dagu и mcp-clickhouse до версий из репозитория
```

Порядок `deploy-staging`:

1. Проверки: env-файл существует, `systemctl` доступен, ClickHouse отвечает на staging.
2. Сборка: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w"` (те же флаги, что в `.goreleaser.yaml`).
3. Бинарник: `scp` → `/root/.local/bin/clickhouse-import-rosstat.new` → `mv` (атомарно; `scp` поверх работающего файла даёт `Text file busy`).
4. dagu: скачивание tarball нужной версии (если бинарник отсутствует или версия иная) → `.new` → `mv`; сохраняется `.bak` для отката.
5. Юниты: `scp` в `/etc/systemd/system/` → `systemctl daemon-reload`.
6. DAG'и: `scp dagu/*.yaml` → `/root/dagu/`.
7. `systemctl enable --now dagu mcp-clickhouse`.
8. Smoke-проверки (§4).

**Идемпотентность:** перед подменой бинарника сравнивается `sha256`; при совпадении файл не копируется и сервис не перезапускается. Для dagu — сравнение версии. Иначе каждый прогон дёргал бы dagu, который может выполнять DAG.

**При провале smoke** цель выводит команды отката (`journalctl -u <unit> -n 50`, как вернуть `.bak`) и возвращает код ошибки.

`upgrade-staging` **не включает** `apt upgrade`: обновление ClickHouse с 31 ГБ данных и Grafana с дашбордами не должно происходить машинально. Обновляются только два сервиса — по решению заказчика.

### 3.7 Создание `kimi_reader`: правка `scripts/mcp_setup_user.py`

Скрипт написан для локального dev-ClickHouse, где `default` **без пароля**, и потому собирает URL как `http://localhost:{port}/` без заголовков аутентификации. На staging у `default` пароль есть — скрипт получит `Authentication failed` (проверено живой пробой). Правка минимальная и не меняет поведение на dev:

**Новые переменные окружения (обе необязательные — отсутствие сохраняет текущее поведение):**

| Переменная | Смысл | По умолчанию |
|---|---|---|
| `CH_SETUP_URL` | базовый URL ClickHouse для создания пользователя | `http://localhost:{CH_HTTP_PORT}/` — как сейчас |
| `CH_SETUP_USER` | логин, от имени которого исполняется SQL создания | пусто (без аутентификации) |
| `CH_SETUP_PASSWORD` | пароль этого логина | пусто |

**Почему отдельные переменные, а не переиспользование `CLICKHOUSE_PASSWORD`:** в скрипте `CLICKHOUSE_PASSWORD` — это пароль **создаваемого** пользователя `kimi_reader` (он подставляется в SQL вместо `<your-password>`). Пароль **исполнителя** DDL — другая сущность, и совмещение двух ролей в одной переменной сделало бы невозможным случай, когда они различаются (на staging так и есть). Это ровно тот класс ошибки, который в репозитории уже ловили в `company_financials` (измерение источника в ключе).

**Аутентификация в HTTP:** `urllib.request` с `Authorization: Basic base64(user:password)`, заголовок добавляется только если `CH_SETUP_USER` задан.

**Проверка прав доступа (обязательна):** скрипт должен **падать с внятной ошибкой**, если DDL не выполнился из-за прав, а не печатать `OK`. Сейчас ошибки `HTTPError` обрабатываются и возвращают код 1 — этого достаточно, но сообщение должно называть логин исполнителя (без пароля), чтобы отличать «нет прав» от «нет пользователя».

**Тесты:** в проекте для `scripts/` тестов нет (`make test` гоняет только Go). Поэтому проверка — ручная, на staging: `make mcp-user-staging` создаёт `kimi_reader`, повторный прогон идемпотентен (`CREATE USER IF NOT EXISTS`), запрос к витрине от имени `kimi_reader` проходит, запрос к сырой таблице — `ACCESS_DENIED`.

**Цель Makefile** `mcp-user-staging` (отдельно от `mcp-user`, чтобы dev-поведение не менялось): передаёт `CH_SETUP_URL`, `CH_SETUP_USER`, `CH_SETUP_PASSWORD` из env-файла и запускает скрипт **на staging** по SSH. Порядок: **после** деплоя (§3.6), но **до** приёмки §4.2 — без пользователя MCP-сервис не отдаст данные.

**Порядок и зависимость:** `mcp-clickhouse` стартует и без пользователя, но smoke-проверка «с токеном → работает» (§4) пройдёт только после создания `kimi_reader`. Поэтому `deploy-staging` **не** вызывает создание пользователя неявно (секреты и права — не дело деплоя бинарников), а `deploy/staging/README.md` описывает порядок двумя командами: `make deploy-staging` → `make mcp-user-staging`.

---

## 4. Проверка (приёмка)

Smoke-проверки внутри `deploy-staging`, три уровня:

| Что | Команда | Ожидание |
|---|---|---|
| dagu поднялся | `systemctl is-active dagu` + `curl -sf http://127.0.0.1:8080/` | `active`, HTTP отвечает; если порт иной — зафиксировать флагами и поправить юнит |
| MCP отвечает | `curl -sf http://127.0.0.1:8000/health` | 200 |
| **MCP требует токен** | `POST /mcp` без `Authorization` | **401** |
| MCP работает с токеном | JSON-RPC `initialize` + `tools/list` с Bearer | список инструментов |
| бинарь исполняется | запуск импортёра на безобидном `STAT` | код 0 или понятная ошибка источника, не `203/EXEC` |

Проверка «без токена → 401» — не формальность: если `CLICKHOUSE_MCP_AUTH_TOKEN` окажется пустой строкой, сервис может стартовать и **отдавать данные без авторизации**. Проверка ловит именно это.

Приёмочный прогон целиком (выполняет человек):

1. `make deploy-staging` → все компоненты установлены, smoke зелёный (кроме проверки «с токеном», если `kimi_reader` ещё не создан).
2. `make mcp-user-staging` → пользователь создан, гранты выданы (§3.7).
3. SSH-туннель `-L 8000:127.0.0.1:8000`, затем `make mcp-check` против staging → **ПРОВЕРКА ПРОЙДЕНА**.
4. `systemctl is-active dagu mcp-clickhouse` → `active`; `journalctl -u dagu` показывает регистрацию DAG'ов.
5. `make upgrade-staging` → сервисы перезапущены; версии в логе соответствуют пинам.
6. Повторный `make deploy-staging` без изменений → «ничего не изменилось», сервисы **не** перезапускаются.
7. **Негативная проверка прав:** запрос к витрине от `kimi_reader` проходит; запрос к сырой таблице (например `company_financials`) → `ACCESS_DENIED`.

---

## 5. Что осознанно НЕ делается

- **Юнит `clickhouse-import-rosstat.service` не создаётся.** По решению заказчика: импортёр запускается DAG'ами dagu, а сервис понадобится позже, когда потребуется скрейпинг в реальном времени. Сломанный юнит на staging остаётся как есть — его переписывание вне объёма фазы.
- **Caddy, TLS, публикация наружу.** Сервисы слушают `127.0.0.1`; доступ — через SSH-туннель. Публичный MCP-эндпоинт для внешних пользователей — Фаза 2.
- **Создание `kimi_reader` на staging — в плане этой фазы** (решение заказчика §8.2). `scripts/mcp_setup_user.py` жёстко ходит на `http://localhost:PORT` **без аутентификации** и потому на staging **не сработает**: у `default` там пароль (`Authentication failed` — проверено). Требуется правка (§3.7).
- **CI-деплой.** Для `clickhouse-import-rosstat` на `palmshell` раннер не зарегистрирован. Регистрация раннера даёт репозиторию право исполнять код на хосте с ClickHouse и Grafana — это отдельное решение, не часть Фазы 1.
- **Grafana provisioning и issue #23** (UID датасорсов, валидация дашбордов) — отдельная работа.
- **`apt upgrade`** пакетов ОС, ClickHouse и Grafana.

---

## 6. Риски

| Риск | Митигация |
|---|---|
| Деплой запускает 13 DAG'ов → внешние запросы к API | smoke-проверка бинарника **до** `enable --now dagu`; явное предупреждение в выводе |
| Часть источников требует ключей, которых на staging нет | smoke на одном импортёре; полный прогон — при обкатке dagu, ошибки видны в `journalctl` |
| `kimi_reader` не создан → MCP-запросы падают | smoke сообщает честно; создание пользователя — отдельный шаг (§5) |
| Непинованная версия менялась при рестарте | пин `MCP_VERSION`/`DAGU_VERSION` в репозитории (§3.5) |
| `scp` поверх работающего бинарника | подмена через `.new` + `mv` |
| Публикация эндпоинта до TLS | только `127.0.0.1` (§3.2) |
| `203/EXEC` из-за `PATH` systemd | абсолютные пути в юнитах (§3.2) |
| Секрет попадает в репозиторий | `EnvironmentFile` вне git; в git — только `.example`; в `.gitignore` **добавляется паттерн, которого там сейчас нет** (см. §3.4) |
| Перезапуск сервисов на каждый деплой | сравнение `sha256`/версии (§3.6) |

---

## 7. Документы к обновлению в том же PR

По skill `sync-readme-architecture`:

- **`README.md`** — новый раздел «Деплой на staging»: цели `deploy-staging`, `upgrade-staging`, переменные `STAGING_HOST`/`MCP_VERSION`/`DAGU_VERSION`, где лежат секреты, как подключиться через SSH-туннель; в таблице целей Makefile — новые строки; в структуре репозитория — `deploy/`.
- **`ARCHITECTURE.md`** — §6.5: HTTP-режим `mcp-clickhouse` **реализован** (сейчас там записано «не реализовано»); параметры `CLICKHOUSE_MCP_SERVER_TRANSPORT=http`, `CLICKHOUSE_MCP_AUTH_TOKEN`, `/health`; раскладка на staging.
- **`docs/ROADMAP_DCF_POLYUS.md`** — §7 фаза 0: отражено состояние деплоя; раздел, где перечислен незакрытый пункт «mcp-clickhouse в HTTP-режиме» (§ таблицы фаз), обновляется.
- **`scripts/mcp_setup_user.py`** — новые необязательные переменные `CH_SETUP_URL`/`CH_SETUP_USER`/`CH_SETUP_PASSWORD` и Basic-auth заголовок (§3.7); поведение на dev не меняется.
- **`deploy/staging/README.md`** — новый файл: развёртывание, секреты, туннель, отладка, откат, порядок `deploy-staging` → `mcp-user-staging`.

---

## 8. Открытые вопросы

1. **Ключи внешних источников на staging** (`BEA_API_KEY`, `BLS_API_KEY` и др.) — какие есть. Определяет, какие DAG'и пройдут сразу, а какие упадут. Выясняется при обкатке.
2. **Судьба старого `/root/bin/clickhouse-import-rosstat`** (от 8 июля) и `disabled`-юнита — по решению заказчика **не трогаем** в этой фазе.
