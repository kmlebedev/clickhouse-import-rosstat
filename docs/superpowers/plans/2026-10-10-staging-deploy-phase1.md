# Staging Deploy Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Развернуть на staging три компонента (dagu, mcp-clickhouse, бинарник импортёра) с обновлением одной командой и секретами вне репозитория.

**Architecture:** Артефакты деплоя лежат в репозитории (`deploy/staging/`), Makefile-цели копируют их на хост по SSH и управляют systemd-юнитами. Секреты живут только на хосте в `/etc/clickhouse-import-rosstat.env` (600). Наружу ничего не публикуется — сервисы слушают `127.0.0.1`, доступ через SSH-туннель.

**Tech Stack:** GNU Make, bash, ssh/scp, systemd, Go 1.27 (сборка), `uv` 0.13.0 + Python 3.12 (mcp-clickhouse 0.7.0), dagu v2.18.2 (статический Go-бинарь).

**Spec:** `docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md`

## Global Constraints

- Хост staging: SSH-алиас `palmshell` (`rock5b.sh3h.ru:12222`, вход `root`), Ubuntu 24.04 x86_64.
- Бинарники — в `/root/.local/bin` (по решению заказчика). **В юнитах только абсолютные пути**: systemd не читает `.bashrc`, его PATH не содержит `.local/bin`.
- Секреты — только в `/etc/clickhouse-import-rosstat.env`, права `600`, вне git. В репозитории — лишь `*.env.example` с плейсхолдерами.
- `deploy-staging` **падает**, если env-файла нет, и **никогда** его не создаёт.
- Версии пинуются в репозитории: `MCP_VERSION = 0.7.0`, `DAGU_VERSION = v2.18.2`.
- `upgrade-staging` обновляет **только** dagu и mcp-clickhouse. Никакого `apt upgrade`.
- Оба сервиса слушают только `127.0.0.1` (dagu `:8080`, MCP `:8000`).
- ClickHouse и Grafana на хосте **не трогаем** (31 ГБ данных). ClickHouse доступен на `localhost:8123`/`:9000`.
- Старый `/root/bin/clickhouse-import-rosstat` и `disabled`-юнит `clickhouse-import-rosstat.service` **не трогаем**.
- Юнит импортёра в этой фазе **не создаётся**.
- Прогон `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) обязан быть зелёным — те же проверки гоняет CI.
- Запрещено: печатать значения секретов в выводе (ни в ответах, ни в логах, ни в командах). `make env-check` — достаточная проверка наличия.
- dagu: `dagu start-all` — сервер + планировщик в одном процессе; `--dags` по умолчанию `$HOME/.config/dagu/dags`, флаги `--host` (default `localhost`), `--port` (default `8080`). Координатор включён по умолчанию на `127.0.0.1:50055` — для loopback-развёртывания отключается `DAGU_COORDINATOR_ENABLED=false`.
- mcp-clickhouse: HTTP **требует** `CLICKHOUSE_MCP_AUTH_TOKEN` — старт падает без него. `/health` намеренно без аутентификации; проверка авторизации — запросом к `/mcp`.

## Review Focus

- **Отсутствие env-файла на хосте.** Ожидание: `deploy-staging` останавливается с понятным сообщением и путём, а не деплоит сервис, который затем падает в петлю рестартов (как старый юнит с 38 856 перезапусками).
- **Пустой `CLICKHOUSE_MCP_AUTH_TOKEN`.** Ожидание: smoke-проверка «`POST /mcp` без токена → 401» обязана провалиться, если токен пуст, а сервер всё же стартовал и отдаёт данные.
- **Отсутствие `kimi_reader`.** Ожидание: MCP-сервис стартует, но smoke явно сообщает, что данных нет, и называет следующий шаг (`make mcp-user-staging`); `deploy-staging` не должен молча рапортовать успех.
- **Повторный прогон `deploy-staging`.** Ожидание: при неизменившихся артефактах сервисы **не** перезапускаются (иначе dagu дёргается посреди выполнения DAG).
- **Секрет в выводе команды.** Ожидание: ни `scp`, ни `ssh`, ни `systemctl` не печатают содержимое env-файла; в логах только имена переменных.
- **Отсутствие `uv` по абсолютному пути.** Ожидание: юнит MCP падает с `203/EXEC` только если путь неверен, поэтому путь фиксируется `/root/.local/bin/uv` и проверяется smoke-проверкой бинаря.

---

### Task 1: Артефакты деплоя в репозитории + защита секретов

**Files:**
- Create: `deploy/staging/dagu.service`
- Create: `deploy/staging/mcp-clickhouse.service`
- Create: `deploy/staging/clickhouse-import-rosstat.env.example`
- Create: `deploy/staging/README.md`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: ничего (первая задача).
- Produces: три файла-шаблона по путям `deploy/staging/dagu.service`, `deploy/staging/mcp-clickhouse.service`, `deploy/staging/clickhouse-import-rosstat.env.example`; README. Задачи 3–6 копируют эти файлы на хост.

- [ ] **Step 1: Добавить защиту env-файлов в `.gitignore`**

В конец `.gitignore` (сейчас там `/build/`, `/.idea/`, `coverage.out`, `/dist/`) добавить:

```
*.env
!*.env.example
```

Проверка: `git check-ignore -v deploy/staging/x.env` печатает правило (файл игнорируется), а `git check-ignore -v deploy/staging/clickhouse-import-rosstat.env.example` даёт пустой вывод и код 1 (образец отслеживается).

- [ ] **Step 2: Написать `deploy/staging/clickhouse-import-rosstat.env.example`**

```bash
# Секреты staging. РЕАЛЬНЫЙ файл: /etc/clickhouse-import-rosstat.env, права 600, вне git.
# deploy-staging НЕ создаёт этот файл и падает, если его нет.
#
# CLICKHOUSE_URL       — для импортёров (запускает dagu); пароль того пользователя, что пишет в БД
# CLICKHOUSE_PASSWORD  — пароль пользователя kimi_reader; его читает mcp-clickhouse
# CLICKHOUSE_MCP_AUTH_TOKEN — Bearer-токен MCP-эндпоинта; генерировать: openssl rand -hex 32
CLICKHOUSE_URL=clickhouse://:REPLACE_ME@localhost:9000/default
CLICKHOUSE_PASSWORD=REPLACE_ME
CLICKHOUSE_MCP_AUTH_TOKEN=REPLACE_ME
```

- [ ] **Step 3: Написать `deploy/staging/dagu.service`**

```ini
[Unit]
Description=dagu — планировщик импортёров clickhouse-import-rosstat
After=network-online.target clickhouse-server.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/root/dagu
# start-all: веб-сервер + планировщик в одном процессе.
# Координатор выключен: развёртывание на одной машине, распределённых воркеров нет.
ExecStart=/root/.local/bin/dagu start-all --dags /root/dagu --host 127.0.0.1 --port 8080
EnvironmentFile=/etc/clickhouse-import-rosstat.env
Environment="ROSSTAT_IMPORT_BIN=/root/.local/bin/clickhouse-import-rosstat"
Environment="DAGU_COORDINATOR_ENABLED=false"
Restart=always
RestartSec=10
LimitNOFILE=65536
PrivateTmp=true
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 4: Написать `deploy/staging/mcp-clickhouse.service`**

Секретов в юните нет: `CLICKHOUSE_PASSWORD` и `CLICKHOUSE_MCP_AUTH_TOKEN` приходят из `EnvironmentFile`.

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

- [ ] **Step 5: Написать `deploy/staging/README.md`**

Разделы: назначение; порядок `make deploy-staging` → `make mcp-user-staging`; где секреты и как сгенерировать токен (`openssl rand -hex 32`); SSH-туннель для проверки (`ssh -N -L 8000:127.0.0.1:8000 palmshell`); отладка (`journalctl -u dagu -n 50`, `journalctl -u mcp-clickhouse -n 50`); откат (вернуть `.bak` из `/root/.local/bin`, `systemctl restart`); предупреждение, что после `enable --now dagu` DAG'и начинают ходить во внешние источники.

- [ ] **Step 6: Проверить, что юниты синтаксически валидны**

Run: `systemd-analyze verify deploy/staging/dagu.service deploy/staging/mcp-clickhouse.service 2>&1 | head -20`
Expected: предупреждения о несуществующих путях (`/root/.local/bin/dagu`) допустимы; ошибок разбора синтаксиса нет. Если `systemd-analyze` недоступен на macOS — пропустить и проверить позже на хосте (Task 7, Step 4).

- [ ] **Step 7: Commit**

```bash
git add .gitignore deploy/staging/
git commit -m "deploy: add staging artifact templates and env protection"
```

---

### Task 2: Правка `scripts/mcp_setup_user.py` для аутентификации

**Files:**
- Modify: `scripts/mcp_setup_user.py`

**Interfaces:**
- Consumes: ничего.
- Produces: скрипт читает `CH_SETUP_URL` (базовый URL), `CH_SETUP_USER`, `CH_SETUP_PASSWORD`; при незаданном `CH_SETUP_USER` работает как раньше (без заголовка). Задача 6 вызывает его с этими переменными.

- [ ] **Step 1: Заменить сборку URL и добавить Basic-auth заголовок**

Заменить блок чтения порта на чтение переменных:

```python
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not password or "'" in password or "\\" in password:
        print("CLICKHOUSE_PASSWORD не задан или содержит кавычку/обратный слеш", file=sys.stderr)
        return 2
    port = os.environ.get("CH_HTTP_PORT", "8123")
    url = os.environ.get("CH_SETUP_URL") or f"http://localhost:{port}/"
    setup_user = os.environ.get("CH_SETUP_USER", "")
    setup_password = os.environ.get("CH_SETUP_PASSWORD", "")
```

Добавить функцию заголовков (модуль `base64` импортируется рядом с `os`):

```python
def auth_headers(user, password):
    if not user:
        return {}
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}
```

В вызове `urllib.request.Request` добавить заголовки и в сообщении об ошибке — логин исполнителя (не пароль):

```python
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    url, data=stmt.encode("utf-8"), headers=auth_headers(setup_user, setup_password)
                ),
                timeout=30,
            ).read()
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace").strip().replace(password, "***")
            who = setup_user or "anonymous"
            print(f"FAIL {label} ( исполнитель: {who}): {body}", file=sys.stderr)
            return 1
```

- [ ] **Step 2: Проверить, что поведение без новых переменных не изменилось**

Run: `CLICKHOUSE_PASSWORD=x python3 -c "import ast;ast.parse(open('scripts/mcp_setup_user.py').read())" && echo SYNTAX_OK`
Expected: `SYNTAX_OK`

Run (на dev, где ClickHouse отвечает без пароля): `make mcp-user`
Expected: как раньше — `OK` по каждому оператору, либо внятная ошибка ClickHouse. Заголовок `Authorization` не отправляется.

- [ ] **Step 3: Проверить, что при заданном `CH_SETUP_USER` отправляется Basic-auth**

Run: `CH_SETUP_USER=default CH_SETUP_PASSWORD=wrong CLICKHOUSE_PASSWORD=x python3 scripts/mcp_setup_user.py 2>&1 | head -3`
Expected: сообщение, начинающееся с `FAIL ... (исполнитель: default)`, содержащее `Authentication failed` — доказывает, что заголовок ушёл и сервер его отверг. Пароль в выводе отсутствует.

- [ ] **Step 4: Commit**

```bash
git add scripts/mcp_setup_user.py
git commit -m "fix: allow mcp_setup_user to authenticate to remote ClickHouse"
```

---

### Task 3: Makefile — переменные и цель деплоя бинарника

**Files:**
- Modify: `Makefile` (блок переменных в шапке; цель рядом с `import`)

**Interfaces:**
- Consumes: `deploy/staging/*` из Task 1.
- Produces: переменные `STAGING_HOST`, `MCP_VERSION`, `DAGU_VERSION`, `STAGING_BIN_DIR`, `STAGING_ENV_FILE`; цель `deploy-staging-binary`. Задачи 4–6 используют эти переменные.

- [ ] **Step 1: Добавить переменные в шапку Makefile**

После блока `MCP_CMD`:

```make
# Деплой на staging (см. deploy/staging/README.md). Хост задаётся без правок Makefile.
STAGING_HOST    ?= palmshell
STAGING_BIN_DIR ?= /root/.local/bin
STAGING_ENV_FILE ?= /etc/clickhouse-import-rosstat.env
MCP_VERSION     ?= 0.7.0
DAGU_VERSION    ?= v2.18.2
```

Добавить новые цели в `.PHONY`: `deploy-staging deploy-staging-binary deploy-staging-dagu deploy-staging-mcp upgrade-staging mcp-user-staging`.

- [ ] **Step 2: Закрепить версию mcp-clickhouse в существующих вызовах**

Заменить в `MCP_CMD` и в цели `mcp-check` строку `mcp-clickhouse` на `mcp-clickhouse==$(MCP_VERSION)` (две правки: строка `MCP_CMD :=` и команда `uv run --with mcp-clickhouse --with mcp ...`).

Проверка: `grep -n "mcp-clickhouse" Makefile` — все вхождения содержат `==$(MCP_VERSION)`.

- [ ] **Step 3: Написать цель `deploy-staging-binary`**

Общая проверка предусловий выносится в служебную цель, чтобы не дублировать её в четырёх местах:

```make
# Проверки перед деплоем: env-файл на хосте существует, ClickHouse отвечает.
staging-precheck:
	@ssh $(STAGING_HOST) 'test -f $(STAGING_ENV_FILE)' || { \
		echo "на $(STAGING_HOST) нет $(STAGING_ENV_FILE) — создайте его из deploy/staging/clickhouse-import-rosstat.env.example (права 600)"; exit 1; }
	@ssh $(STAGING_HOST) 'curl -sf -m 3 http://localhost:8123/ping >/dev/null' || { \
		echo "ClickHouse на $(STAGING_HOST) не отвечает на :8123"; exit 1; }

# Бинарник импортёра: сборка, атомарная подмена, пропуск копирования при совпадении sha256.
deploy-staging-binary: staging-precheck build
	@set -e; \
	local_sum=$$(shasum -a 256 $(TARGET) | cut -d' ' -f1); \
	remote_sum=$$(ssh $(STAGING_HOST) 'sha256sum $(STAGING_BIN_DIR)/clickhouse-import-rosstat 2>/dev/null | cut -d" " -f1' || true); \
	if [ "$$local_sum" = "$$remote_sum" ]; then \
		echo "бинарь не изменился ($$local_sum) — копирование пропущено"; \
	else \
		scp $(TARGET) $(STAGING_HOST):$(STAGING_BIN_DIR)/clickhouse-import-rosstat.new; \
		ssh $(STAGING_HOST) 'mv $(STAGING_BIN_DIR)/clickhouse-import-rosstat.new $(STAGING_BIN_DIR)/clickhouse-import-rosstat && chmod 755 $(STAGING_BIN_DIR)/clickhouse-import-rosstat'; \
		echo "бинарь обновлён"; \
	fi
```

Служебную цель `staging-precheck` добавить в `.PHONY`.

- [ ] **Step 4: Проверить предусловие на живом хосте**

Run: `make deploy-staging-binary`
Expected: сборка проходит; при первом прогоне печатает «бинарь обновлён»; повторный прогон печатает «бинарь не изменился … — копирование пропущено».

Если env-файла на хосте ещё нет — цель должна напечатать путь и выйти с кодом 1 (это ожидаемое поведение, проверка Review Focus №1).

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "make: add staging deploy variables and binary deploy target"
```

---

### Task 4: Makefile — деплой dagu

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: `DAGU_VERSION`, `STAGING_HOST`, `STAGING_BIN_DIR` (Task 3); `deploy/staging/dagu.service` (Task 1).
- Produces: цель `deploy-staging-dagu`.

- [ ] **Step 1: Написать цель `deploy-staging-dagu`**

Установка идемпотентна: версия проверяется запуском `dagu version` на хосте; при совпадении бинарник не скачивается.

```make
DAGU_URL := https://github.com/dagu-org/dagu/releases/download/$(DAGU_VERSION)/dagu_$(DAGU_VERSION:v%=%)_linux_amd64.tar.gz

# dagu: скачать релизный tarball, подменить бинарник, поставить юнит. Идемпотентно по версии.
deploy-staging-dagu: staging-precheck
	@set -e; \
	want=$$(echo $(DAGU_VERSION) | sed 's/^v//'); \
	have=$$(ssh $(STAGING_HOST) '$(STAGING_BIN_DIR)/dagu version 2>/dev/null' | head -1 || true); \
	if [ "$$have" = "$$want" ]; then \
		echo "dagu $$have уже установлен"; \
	else \
		curl -fsSL -o /tmp/dagu_$(DAGU_VERSION).tar.gz '$(DAGU_URL)'; \
		tar xzf /tmp/dagu_$(DAGU_VERSION).tar.gz -C /tmp dagu; \
		scp /tmp/dagu $(STAGING_HOST):$(STAGING_BIN_DIR)/dagu.new; \
		ssh $(STAGING_HOST) 'test -f $(STAGING_BIN_DIR)/dagu && cp $(STAGING_BIN_DIR)/dagu $(STAGING_BIN_DIR)/dagu.bak || true; mv $(STAGING_BIN_DIR)/dagu.new $(STAGING_BIN_DIR)/dagu; chmod 755 $(STAGING_BIN_DIR)/dagu'; \
		echo "dagu обновлён до $(DAGU_VERSION)"; \
	fi
	scp deploy/staging/dagu.service $(STAGING_HOST):/etc/systemd/system/dagu.service
	ssh $(STAGING_HOST) 'systemctl daemon-reload'
```

- [ ] **Step 2: Проверить установку на хосте**

Run: `make deploy-staging-dagu`
Expected: скачивание tarball, `dagu обновлён до v2.18.2`, юнит скопирован.

Run: `make deploy-staging-dagu` (повторно)
Expected: `dagu 2.18.2 уже установлен` — без повторного скачивания.

- [ ] **Step 3: Проверить, что бинарник исполняется на хосте**

Run: `ssh palmshell '/root/.local/bin/dagu version'`
Expected: строка версии `2.18.2`, не `203/EXEC` и не «No such file».

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "make: add dagu staging deploy target"
```

---

### Task 5: Makefile — деплой mcp-clickhouse

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: `MCP_VERSION`, `STAGING_HOST` (Task 3); `deploy/staging/mcp-clickhouse.service` (Task 1).
- Produces: цель `deploy-staging-mcp`.

- [ ] **Step 1: Написать цель `deploy-staging-mcp`**

Отдельного «скачивания» нет: `uv run --with mcp-clickhouse==$(MCP_VERSION)` разрешает пакет сам. Цель ставит юнит и прогревает кэш `uv`, чтобы первый старт сервиса не зависел от сети.

```make
# mcp-clickhouse: юнит + прогрев кэша uv (пакет разрешается при старте сервиса).
deploy-staging-mcp: staging-precheck
	scp deploy/staging/mcp-clickhouse.service $(STAGING_HOST):/etc/systemd/system/mcp-clickhouse.service
	ssh $(STAGING_HOST) 'systemctl daemon-reload'
	@echo "прогрев кэша uv (скачивание mcp-clickhouse==$(MCP_VERSION))..."
	ssh $(STAGING_HOST) '/root/.local/bin/uv tool run --from mcp-clickhouse==$(MCP_VERSION) python -c "pass" >/dev/null 2>&1 || true'
```

- [ ] **Step 2: Проверить, что юнит установлен, а пакет разрешается**

Run: `make deploy-staging-mcp`
Expected: юнит скопирован, прогрев без падения ssh.

Run: `ssh palmshell '/root/.local/bin/uv tool run --from mcp-clickhouse==0.7.0 python -c "pass" 2>&1 | head -5'`
Expected: справка CLI без `No solution found` — версия `0.7.0` существует и доступна.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "make: add mcp-clickhouse staging deploy target"
```

---

### Task 6: Makefile — `deploy-staging`, smoke, `upgrade-staging`, `mcp-user-staging`

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: `deploy-staging-binary` (Task 3), `deploy-staging-dagu` (Task 4), `deploy-staging-mcp` (Task 5); `scripts/mcp_setup_user.py` с `CH_SETUP_*` (Task 2).
- Produces: цели `deploy-staging`, `deploy-staging-smoke`, `upgrade-staging`, `mcp-user-staging`.

- [ ] **Step 1: Написать цель `deploy-staging` (композиция) и копирование DAG'ов**

Порядок критичен и состоит из двух разных проверок:

1. **до** `enable --now` — проверяется только **бинарь** (`deploy-staging-preflight`): сервисы ещё не запущены, поэтому проверять их бессмысленно. Это защищает от запуска 13 DAG'ов на нерабочем бинаре.
2. **после** `enable --now` — полный smoke (`deploy-staging-smoke`): сервисы уже работают.

```make
# Полный деплой: бинарь → dagu → mcp → DAG'и → проверка бинаря → включение → smoke.
deploy-staging: deploy-staging-binary deploy-staging-dagu deploy-staging-mcp
	ssh $(STAGING_HOST) 'install -d -m 755 /root/dagu'
	scp dagu/*.yaml $(STAGING_HOST):/root/dagu/
	@$(MAKE) deploy-staging-preflight --no-print-directory
	ssh $(STAGING_HOST) 'systemctl enable --now dagu mcp-clickhouse'
	@$(MAKE) deploy-staging-smoke --no-print-directory
	@echo "Готово. Далее: make mcp-user-staging (если kimi_reader ещё не создан)."
```

**Второй smoke не глушится `|| true`**: если после включения сервисы не отвечают, деплой обязан завершиться с ошибкой, а не рапортовать успех.

- [ ] **Step 2: Написать цель `deploy-staging-preflight`**

Выполняется **до** включения сервисов: проверяет только то, что можно проверить без запущенных сервисов.

```make
# Проверка ДО включения сервисов: бинарь исполняем и не падает на отсутствующем
# пользователе ClickHouse. Сервисы ещё не запущены — их проверяет deploy-staging-smoke.
deploy-staging-preflight:
	@ssh $(STAGING_HOST) 'test -x /root/.local/bin/clickhouse-import-rosstat && echo "OK   бинарь импортёра исполняем"' || { echo "FAIL бинарь отсутствует или не исполняем"; exit 1; }
	@ssh $(STAGING_HOST) 'test -x /root/.local/bin/dagu && echo "OK   dagu исполняем"' || { echo "FAIL dagu отсутствует или не исполняем"; exit 1; }
	@ssh $(STAGING_HOST) 'test -f /root/dagu/cbr.yaml && echo "OK   DAG-файлы на месте"' || { echo "FAIL /root/dagu/cbr.yaml отсутствует"; exit 1; }
	@ssh $(STAGING_HOST) 'set -a; . /etc/clickhouse-import-rosstat.env; set +a; \
		test -n "$$CLICKHOUSE_MCP_AUTH_TOKEN" && echo "OK   CLICKHOUSE_MCP_AUTH_TOKEN задан" || { echo "FAIL CLICKHOUSE_MCP_AUTH_TOKEN пуст"; exit 1; }'
```

- [ ] **Step 3: Написать цель `deploy-staging-smoke`**

Выполняется **после** включения сервисов. Токен читается на хосте и **не печатается**.

```make
# Smoke-проверки после включения сервисов. Токен не выводится: берётся из env-файла на хосте.
deploy-staging-smoke:
	@echo "--- smoke: dagu ---"
	@ssh $(STAGING_HOST) 'systemctl is-active --quiet dagu && curl -sf -m 5 -o /dev/null http://127.0.0.1:8080/ && echo "OK   dagu отвечает"' || { echo "FAIL dagu: journalctl -u dagu -n 50"; exit 1; }
	@echo "--- smoke: mcp-clickhouse ---"
	@ssh $(STAGING_HOST) 'systemctl is-active --quiet mcp-clickhouse && curl -sf -m 5 -o /dev/null http://127.0.0.1:8000/health && echo "OK   MCP /health = 200"' || { echo "FAIL mcp-clickhouse: journalctl -u mcp-clickhouse -n 50"; exit 1; }
	@echo "--- smoke: MCP требует токен ---"
	@ssh $(STAGING_HOST) 'set -a; . /etc/clickhouse-import-rosstat.env; set +a; \
		test -n "$$CLICKHOUSE_MCP_AUTH_TOKEN" || { echo "FAIL CLICKHOUSE_MCP_AUTH_TOKEN пуст — сервер отдаёт данные без авторизации"; exit 1; }; \
		code=$$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}"); \
		[ "$$code" = "401" ] && echo "OK   без токена → 401" || { echo "FAIL без токена получен $$code, ожидался 401"; exit 1; }'
	@echo "--- smoke: бинарник импортёра ---"
	@ssh $(STAGING_HOST) 'test -x /root/.local/bin/clickhouse-import-rosstat && echo "OK   бинарь исполняем"' || { echo "FAIL бинарь отсутствует или не исполняем"; exit 1; }
```

- [ ] **Step 4: Добавить проверку «с токеном → работает»**

Дополнить цель `deploy-staging-smoke` шагом, который вызывает `tools/list` с Bearer-токеном:

```make
	@echo "--- smoke: MCP с токеном ---"
	@ssh $(STAGING_HOST) 'set -a; . /etc/clickhouse-import-rosstat.env; set +a; \
		body=$$(curl -s -m 10 -X POST http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -H "Authorization: Bearer $$CLICKHOUSE_MCP_AUTH_TOKEN" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}"); \
		echo "$$body" | grep -q "list_tables\|result" && echo "OK   MCP отвечает с токеном" || { echo "FAIL MCP с токеном ответил: $$body"; exit 1; }'
```

- [ ] **Step 5: Написать `upgrade-staging`**

```make
# Обновление ТОЛЬКО dagu и mcp-clickhouse до версий из репозитория.
# apt upgrade сюда НЕ входит: ClickHouse (31 ГБ данных) и Grafana обновляются вручную.
upgrade-staging: deploy-staging-dagu deploy-staging-mcp
	ssh $(STAGING_HOST) 'systemctl restart dagu mcp-clickhouse'
	@echo "сервисы перезапущены на версиях: dagu $(DAGU_VERSION), mcp-clickhouse $(MCP_VERSION)"
	@$(MAKE) deploy-staging-smoke --no-print-directory
```

- [ ] **Step 6: Написать `mcp-user-staging`**

Создаёт `kimi_reader` от имени `default` через HTTP. Пароль исполнителя и пароль создаваемого пользователя — **разные** переменные; в вывод не печатаются.

Секреты передаются на хост **через stdin локального ssh-процесса**, а не аргументами: аргументы видны в `ps`. Способ выбран один, без альтернатив:

```make
# Создание kimi_reader на staging. Пароль исполнителя (default) и пароль создаваемого
# пользователя (kimi_reader) — разные сущности, поэтому две переменные.
# Секреты идут через stdin ssh (не через argv): аргументы видны в ps на обеих машинах.
mcp-user-staging:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль kimi_reader)"; exit 1; }
	@test -n "$$CH_SETUP_PASSWORD" || { echo "задайте CH_SETUP_PASSWORD (пароль пользователя default на $(STAGING_HOST))"; exit 1; }
	ssh $(STAGING_HOST) 'test -x /usr/bin/python3' || { echo "python3 не найден на $(STAGING_HOST)"; exit 1; }
	scp scripts/mcp_setup_user.py $(STAGING_HOST):/tmp/mcp_setup_user.py
	scp sql/mcp_kimi_reader.sql $(STAGING_HOST):/tmp/mcp_kimi_reader.sql
	printf '%s\n%s\n' "$$CLICKHOUSE_PASSWORD" "$$CH_SETUP_PASSWORD" | ssh $(STAGING_HOST) 'read -r kimi_pass; read -r setup_pass; \
		cd /tmp && CLICKHOUSE_PASSWORD="$$kimi_pass" CH_SETUP_URL=http://localhost:8123/ CH_SETUP_USER=default CH_SETUP_PASSWORD="$$setup_pass" python3 /tmp/mcp_setup_user.py'
```

Проверка, что секрет не утекает: во время выполнения `ssh palmshell "ps aux | grep -c '[p]ython3 /tmp/mcp_setup_user.py'"` возвращает процесс **без** пароля в командной строке (пароль приходит через stdin).

- [ ] **Step 7: Проверить полный деплой**

Run: `make deploy-staging`
Expected: бинарь обновлён (или пропущен), dagu установлен, MCP прогрет, DAG'и скопированы, smoke зелёный по всем пяти строкам, оба сервиса `enabled`+`active`.

Run: `ssh palmshell 'systemctl is-enabled dagu mcp-clickhouse'`
Expected: обе строки `enabled`.

Run: `ssh palmshell 'journalctl -u dagu -n 20 --no-pager | grep -i "dags\|registered" | head -5'`
Expected: регистрация DAG-файлов (или отсутствие ошибок чтения каталога `/root/dagu`).

- [ ] **Step 8: Проверить идемпотентность и обновление**

Run: `make deploy-staging` (повторно, без изменений)
Expected: «бинарь не изменился … копирование пропущено», «dagu 2.18.2 уже установлен»; **сервисы не перезапускаются** (время `ActiveEnterTimestamp` не меняется: `ssh palmshell 'systemctl show dagu -p ActiveEnterTimestamp'` до и после совпадает).

Run: `make upgrade-staging`
Expected: сервисы перезапущены, smoke зелёный, в выводе названы версии.

- [ ] **Step 9: Проверить создание пользователя и негативную проверку прав**

Run: `make mcp-user-staging`
Expected: `OK` по каждому оператору; повторный прогон идемпотентен (`CREATE USER IF NOT EXISTS`).

Run (на хосте): `curl -s 'http://localhost:8123/?user=kimi_reader&password=<пароль>' --data-binary 'SELECT count() FROM v_company_financials'`
Expected: число (доступ к витрине есть).

Run (на хосте): `curl -s 'http://localhost:8123/?user=kimi_reader&password=<пароль>' --data-binary 'SELECT count() FROM company_financials'`
Expected: `ACCESS_DENIED` / код 497 — сырая таблица закрыта.

- [ ] **Step 10: Commit**

```bash
git add Makefile
git commit -m "make: add staging deploy, smoke, upgrade and mcp-user targets"
```

---

### Task 7: Проверка MCP end-to-end из репозитория через туннель

**Files:**
- Modify: `Makefile` (добавить переменную адреса для `mcp-check`)

**Interfaces:**
- Consumes: `deploy-staging` (Task 6); `scripts/mcp_check.py`.
- Produces: возможность прогнать `make mcp-check` против staging.

- [ ] **Step 1: Добавить переопределение хоста/порта в `mcp-check`**

`Makefile` и `scripts/mcp_check.py` жёстко берут `CLICKHOUSE_HOST=localhost`. Для проверки staging через туннель адрес задаётся переменной:

```make
# Хост MCP-проверки. Для staging: поднимите туннель и запустите
#   make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=<локальный порт туннеля>
MCP_CHECK_HOST ?= localhost
MCP_CHECK_PORT ?= $(CH_HTTP_PORT)
```

В команде цели `mcp-check` заменить `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=$(CH_HTTP_PORT)` на `CLICKHOUSE_HOST=$(MCP_CHECK_HOST) CLICKHOUSE_PORT=$(MCP_CHECK_PORT)`.

- [ ] **Step 2: Проверить через SSH-туннель**

Сначала создать туннель в отдельном терминале:
Run: `ssh -N -L 8123:127.0.0.1:8123 palmshell`
Затем:
Run: `CLICKHOUSE_PASSWORD=<пароль kimi_reader> make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=8123`
Expected: `ПРОВЕРКА ПРОЙДЕНА`.

Если нужен доступ именно к HTTP MCP (порт 8000), туннель `-L 8000:127.0.0.1:8000` и проверка JSON-RPC вручную — как в Task 6, Step 3.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "make: allow mcp-check against a remote host via tunnel"
```

---

### Task 8: Документация (README, ARCHITECTURE, ROADMAP)

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md` (§6.5)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (§7 фаза 0)

**Interfaces:**
- Consumes: всё выше (цели, пути, версии).
- Produces: документация, синхронная с кодом.

- [ ] **Step 1: Обновить `ARCHITECTURE.md` §6.5**

Утверждение «HTTP-режим mcp-clickhouse … не реализовано» (строка ~524) заменить на описание реализованного: `CLICKHOUSE_MCP_SERVER_TRANSPORT=http`, `CLICKHOUSE_MCP_BIND_HOST/PORT=127.0.0.1:8000`, обязательный `CLICKHOUSE_MCP_AUTH_TOKEN` (старт падает без него), `/health` без аутентификации, юнит `deploy/staging/mcp-clickhouse.service`, версия пинуется `MCP_VERSION`.

- [ ] **Step 2: Обновить `README.md`**

Добавить раздел «Деплой на staging»: цели `deploy-staging`, `deploy-staging-binary|dagu|mcp`, `upgrade-staging`, `mcp-user-staging`; переменные `STAGING_HOST`, `MCP_VERSION`, `DAGU_VERSION`; где лежат секреты и как создать (`deploy/staging/clickhouse-import-rosstat.env.example` → `/etc/clickhouse-import-rosstat.env`, 600); SSH-туннель для проверки. В таблицу целей Makefile добавить новые строки; в структуру репозитория — `deploy/`.

- [ ] **Step 3: Обновить `docs/ROADMAP_DCF_POLYUS.md`**

В §7 (фаза 0) отразить: деплой на staging развёрнут (dagu, mcp-clickhouse, бинарь), HTTP-режим MCP закрыт; отметить, что осталось (Caddy/TLS, CI-деплой, Grafana provisioning / issue #23). Обновить строку про незакрытый «mcp-clickhouse в HTTP-режиме».

- [ ] **Step 4: Прогнать обязательные проверки**

Run: `make all`
Expected: gofmt, golangci-lint, `go vet`, `go test -race`, сборка — зелёные.

- [ ] **Step 5: Commit**

```bash
git add README.md ARCHITECTURE.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "docs: sync staging deploy phase 1"
```
