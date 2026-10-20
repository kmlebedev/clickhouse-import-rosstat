ROSSTAT_ENV ?= $(HOME)/.config/rosstat/env
-include $(ROSSTAT_ENV)
export

BINARY    := clickhouse-import-rosstat
BUILD_DIR := build
TARGET    := $(BUILD_DIR)/$(BINARY)

CH_BIN       ?= $(or $(shell command -v clickhouse 2>/dev/null),$(lastword $(sort $(wildcard $(HOME)/.clickhouse/versions/*/clickhouse))))
CH_DIR       ?= $(HOME)/clickhouse-dev/.clickhouse/servers/dev
CH_DATA      := $(CH_DIR)/data
CH_PID       := $(CH_DIR)/clickhouse.pid
CH_LOG       := $(CH_DIR)/clickhouse.log
CH_HTTP_PORT ?= 8123
CH_TCP_PORT  ?= 9000

# Локальный стенд Grafana (make grafana-up/down): данные, логи и плагины лежат вне
# репозитория в $(GF_DIR), в репозитории — только провижининг датасорса.
GF_BIN       ?= $(shell command -v grafana 2>/dev/null)
GF_PREFIX    := $(realpath $(dir $(realpath $(GF_BIN)))/..)
# И CLI, и серверу нужен homepath: без него они не находят conf/defaults.ini
# («Could not find config defaults»). У brew-раскладки homepath — share/grafana рядом
# с bin/grafana, у tarball — сам префикс.
GF_HOMEPATH  ?= $(or $(wildcard $(GF_PREFIX)/share/grafana),$(GF_PREFIX))
GF_DIR       ?= $(HOME)/grafana-dev
GF_PORT      ?= 3000
# Версия плагина: формат запроса дашборда подтверждён живым экспортом 4.3.2 (grafana/clickhouse.go).
GRAFANA_PLUGIN_VERSION ?= 4.3.2
GF_PID       := $(GF_DIR)/grafana.pid
GF_LOG       := $(GF_DIR)/grafana.log
GF_PLUGINS   := $(GF_DIR)/plugins

# Деплой дашбордов на тест-стенд и smoke (make grafana-deploy-test / grafana-check).
# gcx работает на переменных окружения: конфиг ~/.config/gcx не нужен, контексты не сохраняются,
# поэтому в CI достаточно GRAFANA_SERVER/GRAFANA_TOKEN. Для on-prem gcx 1.5.0 дополнительно
# требует org-id — без него «missing stacks.default.grafana.org-id» (проверено на стенде 13.2.3).
# Токен подставляется в GRAFANA_TOKEN на уровне make, а не префиксом команды в рецепте: тогда он
# уходит в окружение процесса gcx (make экспортирует переменные) и не светится в командной строке.
# Список uid всех дашбордов контура — источник истины для деплоя и smoke: цели ниже идут по нему
# циклом, поэтому новый дашборд добавляется здесь одной строкой (свои uid генератор grafana/ знает сам).
DASHBOARDS       := gold-thesis key-rate-publications finance-polyus
GRAFANA_TEST_URL ?= http://localhost:3000
GRAFANA_ORG_ID   ?= 1
GRAFANA_TOKEN    ?= $(GRAFANA_TEST_TOKEN)

# Staging-стенд Grafana (make grafana-deploy-staging): доступ только через ssh-туннель на :3001 —
# порт 3001, чтобы не конфликтовать с локальным стендом на :3000; туннель поднимает владелец руками
# (конвенция та же, что у make mcp-check).
GRAFANA_STAGING_URL ?= http://localhost:3001

MCP_USER     ?= kimi_reader
MCP_PY       ?= 3.12
MCP_VERSION  ?= 0.7.0
MCP_CMD      := uv run --with mcp-clickhouse==$(MCP_VERSION) --python $(MCP_PY) mcp-clickhouse

# Адрес MCP-проверки. Для staging поднимите туннель и переопределите:
#   ssh -N -L 8123:127.0.0.1:8123 palmshell &
#   make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=8123
MCP_CHECK_HOST ?= localhost
MCP_CHECK_PORT ?= $(CH_HTTP_PORT)

# MLflow (infra/4.2): сервер на 127.0.0.1:$(MLFLOW_PORT), SQLite-бэкенд и локальные артефакты
# в $(MLFLOW_HOME) (build/ gitignored) — без PostgreSQL, MinIO, S3 и Docker.
MLFLOW_VERSION      ?= 3.17.1
MLFLOW_HOME         ?= $(BUILD_DIR)/mlflow
MLFLOW_PORT         ?= 5000
MLFLOW_TRACKING_URI ?= http://127.0.0.1:$(MLFLOW_PORT)
MLFLOW_DB           := $(abspath $(MLFLOW_HOME)/mlflow.db)
MLFLOW_ARTIFACTS    := $(abspath $(MLFLOW_HOME)/artifacts)
MLFLOW_PID          := $(MLFLOW_HOME)/mlflow.pid
MLFLOW_LOG          := $(MLFLOW_HOME)/mlflow.log

# 1 по умолчанию — все ML-прогоны идут в MLflow. Перед стартом цель вызывает
# `mlflow_preflight.py`: если API не отвечает, скрипт явно спрашивает, запускать
# ли без лога; в неинтерактивном режиме запуск запрещён, пока не выставлен
# MLFLOW_ALLOW_NO_LOG=1. При MLFLOW=0 MLFLOW_ENV передаёт пустой
# MLFLOW_TRACKING_URI и preflight пропускается.
MLFLOW ?= 1
MLFLOW_PKG    := $(if $(filter 1,$(MLFLOW)),--with "mlflow==$(MLFLOW_VERSION)",)
MLFLOW_ENV    := $(if $(filter 1,$(MLFLOW)),MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI),MLFLOW_TRACKING_URI=)
MLFLOW_HTTP_ENV := MLFLOW_HTTP_REQUEST_TIMEOUT=10 MLFLOW_HTTP_REQUEST_MAX_RETRIES=2
MLFLOW_PREFLIGHT := $(if $(filter 1,$(MLFLOW)),python3 scripts/mlflow_preflight.py --uri $(MLFLOW_TRACKING_URI),true)

# Деплой на staging (см. deploy/staging/README.md). Хост задаётся без правок Makefile.
STAGING_HOST     ?= palmshell
STAGING_BIN_DIR  ?= /root/.local/bin
STAGING_ENV_FILE ?= /etc/clickhouse-import-rosstat.env
DAGU_VERSION     ?= v2.18.2

# Docs-портал GitHub Pages: версия mkdocs-material общая с .github/workflows/pages.yml.
MKDOCS_VERSION ?= 9.7.7
MKDOCS := uv run --with mkdocs-material==$(MKDOCS_VERSION) --python 3.12 mkdocs

.PHONY: all lint fmt vet test build run deps clean info ch-up ch-down ch-status ch-sql mlflow-up mlflow-down mlflow-status mlflow-export mlflow-backfill mlflow-backfill-test mlflow-preflight mlflow-register mlflow-promote mlflow-models ml-gate ml-gate-test ml-live-check ml-live-check-test dev-check env-check import mp-llm-eval archive-registry archive-list archive-restore mcp-run mcp-user mcp-check build-ingest run-ingest deploy-staging deploy-staging-binary deploy-staging-dagu deploy-staging-mcp deploy-staging-mlflow deploy-staging-preflight deploy-staging-smoke upgrade-staging mcp-user-staging staging-precheck grafana-build grafana-validate grafana-up grafana-down grafana-deploy-test grafana-deploy-staging grafana-provision-staging grafana-check grafana-test-token grafana-provision-alerting-staging key-rate-model shock-classifier shock-classifier-test key-rate-llm-forecast key-rate-forecast-log cpi-nowcast cpi-monthly cpi-monthly-v3 cpi-monthly-v3-test cpi-monthly-industry cpi-monthly-industry-test cpi-public-audit cpi-public-audit-test cpi-weekly-panel cpi-weekly-panel-test cb-comp-parity cb-comp-parity-test cb-comp-live cb-comp-live-test cb-comp-live-compare cb-comp-proxy edisclosure-login edisclosure-collect docs-check docs-build docs-serve

all: lint test build build-ingest

# lint падает, если что-то не отформатировано, затем запускает golangci-lint,
# если он установлен. CI выполняет те же проверки.
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; \
	else echo "golangci-lint not installed, skipping"; fi

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go vet ./...
	go test -race ./...

# Статическая сборка linux/amd64 без cgo: сервер ClickHouse + Dagu (ARCHITECTURE.md §5).
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(TARGET) .

# Нужен CLICKHOUSE_URL в окружении; CLICKHOUSE_IMPORT_STAT фильтрует импортёры.
run: build
	./$(TARGET)

# Локальный ClickHouse для проверок; данные вне репозитория (CH_DIR), бинарник ищется в PATH или ~/.clickhouse.
ch-up:
	@if curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1; then \
		echo "ClickHouse уже отвечает на порту $(CH_HTTP_PORT)"; \
	elif [ -f "$(CH_PID)" ] && kill -0 $$(cat "$(CH_PID)") 2>/dev/null; then \
		echo "ClickHouse уже запускается/запущен (PID $$(cat "$(CH_PID)")), второй сервер не поднимается"; \
	else \
		test -n "$(CH_BIN)" || { echo "clickhouse не найден: задайте CH_BIN=/путь/к/clickhouse"; exit 1; }; \
		mkdir -p "$(CH_DATA)"; \
		cd "$(CH_DIR)"; \
		nohup "$(CH_BIN)" server -- --path="$(CH_DATA)/" --http_port=$(CH_HTTP_PORT) --tcp_port=$(CH_TCP_PORT) >"$(CH_LOG)" 2>&1 & \
		echo $$! > "$(CH_PID)"; \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(CH_PID)") 2>/dev/null || break; curl -sf -m 1 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 && break; sleep 1; done; \
		if kill -0 $$(cat "$(CH_PID)") 2>/dev/null && curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1; then \
			echo "ClickHouse запущен, PID $$(cat "$(CH_PID)"), лог $(CH_LOG)"; \
		else \
			echo "ClickHouse не поднялся, смотрите $(CH_LOG)"; exit 1; \
		fi; \
	fi

# Останавливает сервер и ждёт завершения до 60 с. Если процесс пережил сигнал, pid-файл
# НЕ удаляется: иначе следующий ch-down не найдёт процесс, а ch-up скажет «уже отвечает».
ch-down:
	@if [ -f "$(CH_PID)" ] && kill -0 $$(cat "$(CH_PID)") 2>/dev/null; then \
		kill $$(cat "$(CH_PID)"); \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(CH_PID)") 2>/dev/null || break; sleep 1; done; \
		if kill -0 $$(cat "$(CH_PID)") 2>/dev/null; then \
			echo "ClickHouse не завершился за 60 с, PID-файл $(CH_PID) сохранён: повторите make ch-down"; \
		else \
			rm -f "$(CH_PID)"; echo "ClickHouse остановлен"; \
		fi; \
	else \
		echo "PID-файл $(CH_PID) не найден или процесс не запущен (сервер, поднятый вне make, не останавливается)"; \
	fi

ch-status:
	@curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 && echo "ClickHouse отвечает на порту $(CH_HTTP_PORT)" || echo "ClickHouse не отвечает на порту $(CH_HTTP_PORT)"

# Прогон SQL-файла в локальный ClickHouse: make ch-sql FILE=sql/events_calendar_q4_2026.sql
# Файл должен лежать в репозитории (путь относительный). Многострочные скрипты — через --multiquery.
# Подключается пользователем CH_USER (по умолчанию default, локальный dev-сервер без пароля);
# CH_USER_PASSWORD задавайте, только если пользователю нужен пароль.
FILE ?=
CH_USER ?= default
CH_USER_PASSWORD ?=
ch-sql:
	@test -n "$(FILE)" || { echo "укажите файл: make ch-sql FILE=sql/events_calendar_q4_2026.sql"; exit 1; }
	@test -f "$(FILE)" || { echo "файл не найден: $(FILE)"; exit 1; }
	@test -n "$(CH_BIN)" || { echo "clickhouse не найден: задайте CH_BIN=/путь/к/clickhouse"; exit 1; }
	@curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 || { echo "ClickHouse не отвечает: make ch-up"; exit 1; }
	"$(CH_BIN)" client --user "$(CH_USER)" --password "$(CH_USER_PASSWORD)" --multiquery < "$(FILE)"

# Локальный MLflow (infra/4.2). Сервер запускается через uv run --with: mlflow не попадает
# в зависимости репозитория. Окружение прогревается до старта, чтобы 60-секундный цикл
# готовности не тратился на первое скачивание пакета.
#
# Артефакты — через прокси сервера: --artifacts-destination задаёт каталог хранения, а
# --default-artifact-root НЕ передаётся, чтобы новый эксперимент получил корень
# mlflow-artifacts:/ и --serve-artifacts действительно проксировал upload с клиента.
# С --default-artifact-root <локальный путь> MLflow возвращает его как artifact_location,
# и удалённый клиент (staging через SSH-туннель) писал бы в несуществующий локальный путь.
# ⚠️ Уже созданный default experiment хранит старый artifact_location в SQLite: его нужно
# сбросить/пересоздать (или завести новый эксперимент) — иначе проксирование не включится.
#
# PID-файл проверяется по идентичности процесса (`ps -p PID -o command=` содержит mlflow):
# устаревший/чужой PID не считается работающим сервером.
mlflow-up:
	@if curl -sf -m 2 http://127.0.0.1:$(MLFLOW_PORT)/health >/dev/null 2>&1; then \
		echo "MLflow уже отвечает на порту $(MLFLOW_PORT)"; \
	else \
		if [ -f "$(MLFLOW_PID)" ]; then \
			pid=$$(cat "$(MLFLOW_PID)"); \
			if kill -0 $$pid 2>/dev/null && ps -p $$pid -o command= | grep -q mlflow; then \
				echo "MLflow уже запускается/запущен (PID $$pid), второй сервер не поднимается"; \
				exit 0; \
			fi; \
			if kill -0 $$pid 2>/dev/null; then \
				echo "PID-файл $(MLFLOW_PID) устарел: PID $$pid принадлежит не MLflow — файл удалён, процесс не тронут"; \
			else \
				echo "PID-файл $(MLFLOW_PID) устарел: процесс $$pid не запущен — файл удалён"; \
			fi; \
			rm -f "$(MLFLOW_PID)"; \
		fi; \
		mkdir -p "$(MLFLOW_HOME)" "$(MLFLOW_ARTIFACTS)"; \
		echo "прогрев uv-окружения mlflow==$(MLFLOW_VERSION) (первый запуск скачивает пакет)..."; \
		uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 python -c "pass" || exit 1; \
		nohup uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 mlflow server \
			--host 127.0.0.1 --port $(MLFLOW_PORT) --workers 1 \
			--backend-store-uri "sqlite:///$(MLFLOW_DB)" \
			--artifacts-destination "$(MLFLOW_ARTIFACTS)" --serve-artifacts >"$(MLFLOW_LOG)" 2>&1 & \
		echo $$! > "$(MLFLOW_PID)"; \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(MLFLOW_PID)") 2>/dev/null || break; curl -sf -m 1 http://127.0.0.1:$(MLFLOW_PORT)/health >/dev/null 2>&1 && break; sleep 1; done; \
		if kill -0 $$(cat "$(MLFLOW_PID)") 2>/dev/null && curl -sf -m 2 http://127.0.0.1:$(MLFLOW_PORT)/health >/dev/null 2>&1; then \
			echo "MLflow запущен на порту $(MLFLOW_PORT), PID $$(cat "$(MLFLOW_PID)"), лог $(MLFLOW_LOG)"; \
		else \
			pid=$$(cat "$(MLFLOW_PID)"); \
			kill $$pid 2>/dev/null || true; \
			for i in $$(seq 1 10); do kill -0 $$pid 2>/dev/null || break; sleep 1; done; \
			kill -9 $$pid 2>/dev/null || true; \
			rm -f "$(MLFLOW_PID)"; \
			echo "MLflow не поднялся, хвост $(MLFLOW_LOG):"; tail -30 "$(MLFLOW_LOG)"; exit 1; \
		fi; \
	fi

# Останавливает сервер и ждёт завершения до 60 с. PID-файл удаляется только после
# фактического завершения; БД и артефакты не трогаются. Процесс из PID-файла
# проверяется по идентичности: чужой/устаревший PID не убивается, файл удаляется.
mlflow-down:
	@if [ -f "$(MLFLOW_PID)" ]; then \
		pid=$$(cat "$(MLFLOW_PID)"); \
		if ! kill -0 $$pid 2>/dev/null; then \
			echo "PID-файл $(MLFLOW_PID) устарел: процесс $$pid не запущен — файл удалён"; \
			rm -f "$(MLFLOW_PID)"; \
		elif ! ps -p $$pid -o command= | grep -q mlflow; then \
			echo "PID $$pid из $(MLFLOW_PID) принадлежит не MLflow — файл удалён, процесс не тронут"; \
			rm -f "$(MLFLOW_PID)"; \
		else \
			kill $$pid; \
			for i in $$(seq 1 60); do kill -0 $$pid 2>/dev/null || break; sleep 1; done; \
			if kill -0 $$pid 2>/dev/null; then \
				echo "MLflow не завершился за 60 с, PID-файл $(MLFLOW_PID) сохранён: повторите make mlflow-down"; \
			else \
				rm -f "$(MLFLOW_PID)"; echo "MLflow остановлен"; \
			fi; \
		fi; \
	else \
		echo "PID-файл $(MLFLOW_PID) не найден или процесс не запущен (сервер, поднятый вне make, не останавливается)"; \
	fi

mlflow-status:
	@curl -sf -m 2 http://127.0.0.1:$(MLFLOW_PORT)/health >/dev/null 2>&1 && echo "MLflow отвечает на порту $(MLFLOW_PORT)" || echo "MLflow не отвечает на порту $(MLFLOW_PORT)"

# Preflight MLflow API: ML-цели вызывают его до старта прогона. Если API не
# отвечает, скрипт явно спрашивает, запускать ли без лога; в неинтерактивном
# режиме — запрещает, пока не выставлен MLFLOW_ALLOW_NO_LOG=1.
mlflow-preflight:
	python3 scripts/mlflow_preflight.py --uri $(MLFLOW_TRACKING_URI)

# Backfill существующих прогонов ClickHouse ml_runs в MLflow по run_id:
# make mlflow-backfill ARGS="--run-id <uuid> [--run-id <uuid>] [--force]"
mlflow-backfill:
	MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI) $(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 \
		python3 scripts/mlflow_backfill.py --tracking-uri $(MLFLOW_TRACKING_URI) $(ARGS)

mlflow-backfill-test:
	python3 scripts/mlflow_backfill_test.py -v

# Экспорт прогонов MLflow в ClickHouse (ml_runs/ml_predictions): make mlflow-export
# [ARGS="--since 2026-10-01 --limit 20 --dry-run"]. mlflow ставится через uv --with,
# в зависимости репозитория не попадает.
mlflow-export:
	MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI) $(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 \
		python3 scripts/mlflow_export.py --tracking-uri $(MLFLOW_TRACKING_URI) $(ARGS)

# Реестр моделей MLflow: register/promote/list. Аргументы подкоманды передаются через ARGS:
# make mlflow-register ARGS="--run-id <id> --name cpi_nowcast_catboost_direct"
# make mlflow-promote ARGS="--name cpi_nowcast_catboost_direct --version 1 [--alias challenger]"
# make mlflow-models [ARGS="--name cpi_nowcast_catboost_direct"]
mlflow-register:
	$(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) register $(ARGS)

mlflow-promote:
	$(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) promote $(ARGS)

mlflow-models:
	$(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) list $(ARGS)

# Gate промоушена (infra/4.3): champion vs challenger на общих фолдах —
# RMSE, DM-HLN, 95% блочный бутстрап, robustness-окна и 90% MCS (>2 кандидатов).
# make ml-gate ARGS="--challenger-run <uuid> --champion-run <uuid> --model ordered_logit"
# Live-оценка shadow/canary: make ml-gate ARGS="--live --live-track cbr_rate:ordered_logit"
# Артефакт — build/ml-gate/<run>.json; при MLFLOW=1 ставятся теги решения.
ml-gate:
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) \
		--with "statsmodels>=0.15" --with scipy --with arch --python 3.12 \
		python3 scripts/ml_gate.py $(ARGS)

ml-gate-test:
	uv run --with "statsmodels>=0.15" --with scipy --python 3.12 \
		python3 scripts/ml_gate_test.py -v

# Проверка live-прогноза до публикации (infra/4.3): production-строки
# cbr_rate:taylor/ordered_logit на ближайшее заседание, записанные до target_date.
# make ml-live-check [ARGS="--require-llm --warn-only"]
ml-live-check:
	uv run --python 3.12 python3 scripts/ml_live_check.py $(ARGS)

ml-live-check-test:
	uv run --python 3.12 python3 scripts/ml_live_check_test.py -v

# Проверка окружения разработчика: наличие инструментов и версий.
dev-check:
	@for t in go gofmt golangci-lint uv curl; do \
		if command -v $$t >/dev/null 2>&1; then echo "OK   $$t: $$(command -v $$t)"; else echo "НЕТ  $$t (установка: см. README, раздел «Зависимости для разработки»)"; fi; \
	done
	@test -n "$(CH_BIN)" && echo "OK   clickhouse: $(CH_BIN)" || echo "НЕТ  clickhouse (задайте CH_BIN или установите ClickHouse)"

# Показывает, какие ключи заданы в окружении (файл $(ROSSTAT_ENV)); значения не выводятся.
env-check:
	@echo "файл окружения: $(ROSSTAT_ENV) $$(test -f $(ROSSTAT_ENV) && echo найден || echo НЕ НАЙДЕН)"
	@for v in BEA_API_KEY BLS_API_KEY EIA_API_KEY CLICKHOUSE_URL CLICKHOUSE_PASSWORD; do \
		if [ -n "$$(printenv $$v)" ]; then echo "OK   $$v задан"; else echo "НЕТ  $$v не задан (см. README, раздел «Ключи и окружение»)"; fi; \
	done
	@if [ -n "$$ARCHIVE_YANDEX_TOKEN" ]; then echo "OK   ARCHIVE_YANDEX_TOKEN задан"; else echo "НЕТ  ARCHIVE_YANDEX_TOKEN не задан (нужен при ARCHIVE_BACKEND=yandex)"; fi
	@if [ -n "$$GRAFANA_TEST_TOKEN" ]; then echo "OK   GRAFANA_TEST_TOKEN задан"; else echo "НЕТ  GRAFANA_TEST_TOKEN не задан (нужен для make grafana-deploy-test: сервис-аккаунт Editor на $(GRAFANA_TEST_URL))"; fi
	@if [ -n "$$GRAFANA_STAGING_TOKEN" ]; then echo "OK   GRAFANA_STAGING_TOKEN задан"; else echo "НЕТ  GRAFANA_STAGING_TOKEN не задан (нужен для make grafana-deploy-staging: сервис-аккаунт Editor на staging, доступ через туннель $(GRAFANA_STAGING_URL))"; fi

# Локальный прогон одного импортёра: make import STAT=bea (нужны CLICKHOUSE_URL и ключ импортёра из $(ROSSTAT_ENV)).
STAT ?=
import:
	@test -n "$(STAT)" || { echo "укажите импортёр: make import STAT=bea"; exit 1; }
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	CLICKHOUSE_IMPORT_STAT=$(STAT) ./$(BUILD_DIR)/$(BINARY)-native

# Живой harness сравнения LLM-моделей разметки cbr_mp_summary (спека §3.4): читает документы
# ЦБ и ClickHouse, пишет артефакты в $(CURDIR)/build/mp-llm-eval (MP_LLM_EVAL_OUT).
# Требует MP_LLM_EVAL=1 и MP_SUMMARY_LLM_MODELS в командной строке make; без первого тест
# пропускается. Ключи провайдеров — из $(ROSSTAT_ENV) (в командной строке их не передают).
mp-llm-eval:
	MP_LLM_EVAL_OUT=$(or $(MP_LLM_EVAL_OUT),$(CURDIR)/build/mp-llm-eval) \
	go test -tags mpeval ./cbr -run TestMPLLMEvalLive -v -count=1 -timeout 3h

# Git-зеркало реестра винтажного архива (make archive-registry [ARCHIVE_BACKEND=yandex]):
# скачивает registry.jsonl из хранилища в docs/archive/; коммит — ручной/периодический.
archive-registry:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=registry ./$(BUILD_DIR)/$(BINARY)-native

# Листинг версий винтажного архива (make archive-list [ARCHIVE_BACKEND=yandex]
# [ARCHIVE_LIST_SOURCE=…] [ARCHIVE_RECONCILE=1]): печатает строки реестра, помечает дубли
# содержимого, а с ARCHIVE_RECONCILE=1 сверяет хранилище с реестром (нужен бэкенд с листингом —
# сегодня local) и завершается кодом 1 при объектах без строк. Значения env не печатаются.
archive-list:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=list ./$(BUILD_DIR)/$(BINARY)-native

# Восстановление БД из винтажного архива (make archive-restore STAT=<импортёр> [ARCHIVE_BACKEND=…]):
# байты версии берутся из архива, префлайт и повторная архивация не выполняются — сеть источника
# не вызывается вовсе (сам архив может быть сетевым: yandex). STAT обязателен: случайный
# «восстановить всё» недопустим.
archive-restore:
	@test -n "$(STAT)" || { echo "укажите импортёр: make archive-restore STAT=rosstat_ipc_weeks_bulletins"; exit 1; }
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=restore CLICKHOUSE_IMPORT_STAT=$(STAT) ./$(BUILD_DIR)/$(BINARY)-native

# Ingest-сервис контура прогноза (единственный путь записи в model_runs/forecast_log/macro_series).
# Нужны CLICKHOUSE_URL и INGEST_TOKEN из $(ROSSTAT_ENV); INGEST_ADDR по умолчанию :8081.
build-ingest:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/ingest ./cmd/ingest

run-ingest: build-ingest
	./$(BUILD_DIR)/ingest

# MCP-сервер mcp-clickhouse в режиме stdio (обычно его запускает Kimi сам; здесь — для ручной отладки).
# Пароль берётся из CLICKHOUSE_PASSWORD окружения, в Makefile не хранится.
mcp-run:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль пользователя $(MCP_USER))"; exit 1; }
	CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=$(CH_HTTP_PORT) CLICKHOUSE_SECURE=false CLICKHOUSE_VERIFY=false \
	CLICKHOUSE_USER=$(MCP_USER) CLICKHOUSE_DATABASE=default CLICKHOUSE_MCP_SERVER_TRANSPORT=stdio \
	$(MCP_CMD)

# Создание пользователя MCP и грантов из sql/mcp_kimi_reader.sql на локальном ClickHouse.
# Пароль берётся из CLICKHOUSE_PASSWORD и подставляется в памяти, в файлы не пишется.
# Запускать после импорта витрин (make import STAT=...): GRANT на несуществующую витрину падает.
mcp-user:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль пользователя $(MCP_USER))"; exit 1; }
	@curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 || { echo "ClickHouse не отвечает: make ch-up"; exit 1; }
	python3 scripts/mcp_setup_user.py

# Сид поактивного AISC Полюса, экстраполированный на LOM: калибровка по пресс-релизам
# 4Q/FY 2018-2021 (фикстуры в scripts/testdata), факт добычи из databook_polyus.
# Пишет sql/dcf_mine_plans_seed.sql и polyus_lom_reconciled_long.csv, в ClickHouse НЕ пишет —
# вставка отдельно: make ch-sql FILE=sql/dcf_mine_plans_seed.sql.
dcf-aisc:
	python3 scripts/extrapolation_aisc.py

# ОПР-выпуск Сухого Лога 2025-2027: руда перерабатывается на Вернинской ЗИФ, и Полюс
# показывает результат в производственных результатах Вернинского (сноска 13 пресс-релиза 1П2026),
# но в модели эти годы не входили в NAV ни одного актива. Пишет sql/polyus_output_seed.sql и
# polyus_output_attribution_long.csv, в ClickHouse НЕ пишет — вставка отдельно:
# make ch-sql FILE=sql/polyus_output_seed.sql
polyus-output:
	python3 scripts/polyus_output_model.py

# Дополнительные аргументы цели key-rate-model (например, экспорт фикстур тестов).
ARGS ?=

# Первый срез модели ключевой ставки (фундаментальный контур v0): выборка решений,
# бенчмарки, методы №2–3, walk-forward-бэктест. Читает ClickHouse, пишет локальные
# артефакты; в ClickHouse по умолчанию не пишет; с ML_LOG=1 пишет паспорт
# и fold-предсказания. Фикстуры офлайн-тестов: make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"
# --python 3.12 — не украшение: statsmodels>=0.15 требует Python>=3.10, а `python3` из
# PATH не везде 3.10+ (на машине разработки системный /usr/bin/python3 — 3.9.6, и
# без пина резолвер uv падает). Пин делает цель независимой от порядка PATH и
# повторяет конвенцию репозитория для uv-прогонов (README/ARCHITECTURE: --python 3.12).
key-rate-model:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model.py $(ARGS)

# Классификатор шоков ключевой ставки (спека 2026-10-19, §8): панель заседаний,
# as-of каналы, причинные веса (Махаланобис + Ledoit–Wolf) и описательный
# логистический классификатор явных шоков. Читает ClickHouse, пишет ЛОКАЛЬНЫЕ
# артефакты в каталог прогона (по умолчанию текущий): shock_classifier_report.md,
# shock_classifier_weights.csv, shock_classifier_forecast.json. В ClickHouse
# напрямую не пишет; ML_LOG=1 (или ARGS="--log") — паспорт, метрики и
# предсказания через ml_experiment (ml_runs/ml_predictions).
# ARGS="--features core|all --min-train N --out-dir DIR"; фикстуры:
# make shock-classifier ARGS="--export-fixtures scripts/testdata/shock_classifier"
shock-classifier:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/shock_classifier.py $(ARGS)

# Офлайн-тесты классификатора шоков: без ClickHouse и сети.
shock-classifier-test:
	uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v

# Запись текущего прогноза ставки в forecast_log через ingest (POST /v1/key_rate_forecast).
# Читает локальные артефакты key_rate_model_forecast.json (и, при наличии,
# key_rate_llm_forecast.json), в ClickHouse напрямую не пишет. Нужны INGEST_URL и
# INGEST_TOKEN из $(ROSSTAT_ENV); ingest-сервис — make run-ingest.
# Отладка: make key-rate-forecast-log ARGS="--dry-run"
key-rate-forecast-log:
	uv run --python 3.12 python3 scripts/key_rate_forecast_log.py $(ARGS)

# Экспериментальный LLM-трек контура ставки (issue #189): as-of снимок витрин,
# промпт v1, три OpenAI-совместимых повтора, медиана и разброс, артефакты
# key_rate_llm_forecast.json / key_rate_llm_forecast_report.md (gitignored).
# Зависимостей вне stdlib нет; нужны ClickHouse env и LLM_BASE_URL/LLM_API_KEY/
# LLM_MODEL (последние — только в ~/.config/rosstat/env). В ClickHouse скрипт
# НЕ пишет; ARGS="--log" готовит payload для ingest #127 и печатает его.
key-rate-llm-forecast:
	uv run --python 3.12 python3 scripts/key_rate_llm_forecast.py $(ARGS)
# ML-трек контура ставки: nowcast месячного ИПЦ до публикации Росстата (CatBoost + бенчмарки).
# Читает ClickHouse, пишет локальные артефакты; в ClickHouse по умолчанию не пишет;
# с ML_LOG=1 пишет паспорт и fold-предсказания.
# Фикстуры: make cpi-nowcast ARGS="--export-fixtures scripts/testdata/cpi_nowcast"
cpi-nowcast:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_nowcast.py $(ARGS)

# ML-трек v2: месячный nowcast ИПЦ с мониторингом предприятий ЦБ (с МП vs без МП).
# Читает ClickHouse, пишет локальные артефакты; по умолчанию в ClickHouse НЕ пишет;
# с ML_LOG=1 пишет паспорт и fold-предсказания в ml_runs/ml_predictions.
# Фикстуры: make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"
cpi-monthly:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly.py $(ARGS)

# Stage 1 #76: публичные недельные признаки в месячном nowcast ИПЦ.
cpi-monthly-v3:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_v3.py $(ARGS)

cpi-monthly-v3-test:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_v3_test.py -v

# Live-адаптация production-пайплайна CB_COMP (схема авторов): официальные
# 4 компонента + недельные товары + МП + FAO/USD/ставка → 3 компонентных
# CatBoost → агрегат, ошибки, дашборд, ЖКУ, экспертные корректировки, Россия.
# Читает ClickHouse и архив; в БД не пишет.
cb-comp-live:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_live_pipeline.py $(ARGS)

# Офлайн-тесты live-пайплайна: без ClickHouse и архива.
cb-comp-live-test:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_live_pipeline_test.py -v

# Сравнение live 3-компонентной модели, per-sheet и CB_COMP на общем таргете
# TARGET_FULL (54 normal-месяца, origin M−(h−1)). Читает ClickHouse и архив.
cb-comp-live-compare:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_live_compare.py $(ARGS)

# Stage 2 #235: отраслевые МП-признаки (16 листов mp_survey_data.xlsx) в
# компонентном nowcast ИПЦ. Читает ClickHouse и публичный xlsx; в БД не пишет.
cpi-monthly-industry:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_industry.py $(ARGS)

cpi-monthly-industry-test:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_industry_test.py -v

# Task 1 #234: аудит публичной панели ИПЦ. Читает ClickHouse и публичные xlsx,
# в БД не пишет; JSON — docs/research/2026-10-19-cpi-public-data-audit.json.
cpi-public-audit:
	uv run --with openpyxl --python 3.12 python3 scripts/cpi_public_panel_audit.py $(ARGS)

cpi-public-audit-test:
	uv run --with openpyxl --python 3.12 python3 scripts/cpi_public_panel_audit_test.py -v

# Task 3 #236: недельная компонентная панель ИПЦ — месячный m/m товаров,
# реконструкция food/nonfood/services, покрытие весов и гейт bottom-up.
# Читает ClickHouse, в БД не пишет; артефакты — docs/research/.
cpi-weekly-panel:
	uv run --with openpyxl --python 3.12 python3 scripts/cpi_weekly_component_panel.py $(ARGS)

cpi-weekly-panel-test:
	uv run --with openpyxl --python 3.12 python3 scripts/cpi_weekly_component_panel_test.py -v

# Live-диагностика per_sheet: экспорт v_cbr_mp_public_panel (87 кодов × 3
# индикатора, но 33 различные серии) и v_cbr_mp_bsu_map (код → industry_slug)
# в build/cb-comp-parity. Только чтение ClickHouse.
cb-comp-public-export:
	uv run --with pandas --python 3.12 python3 scripts/cb_comp_public_export.py --out-dir build/cb-comp-parity

# Паритет ML-трека с CB_COMP (issue #190): таргет TARGET_FULL из архива авторов,
# архивные per-period прогнозы CB_COMP, две CatBoost-direct модели (с МП/без МП),
# DM-HLN, TOST с margin 10% RMSE CB_COMP, RMSE ratio. Архив не скачивается:
# путь — --archive или CB_COMP_ARCHIVE (по умолчанию worktree-сосед cb-comp-archive).
# Читает ClickHouse (ряды v2), пишет локальные артефакты; в ClickHouse не пишет.
cb-comp-parity:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_parity.py $(ARGS)

# Офлайн-тесты parity-скрипта: без ClickHouse и без архива.
cb-comp-parity-test:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_parity_test.py -v
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cb_comp_bottomup_test.py -v

# Прокси-путь CB_COMP (issue #190): SA ИПЦ без КУ из публичных рядов ЦБ/Росстата
# и RMSE нашего месячного nowcast на окне статьи 2019-01…2024-07. Читает ClickHouse,
# в ClickHouse не пишет. Нужен CSV месячного трека v2 (--nowcast-csv), который
# создаётся `make cpi-monthly ARGS="--sample long"` в ветке feat/key-rate-ml-v2.
cb-comp-proxy:
	uv run --with pandas --with scipy --with "statsmodels>=0.15" --python 3.12 python3 scripts/cb_comp_proxy.py $(ARGS)

# Документы e-disclosure.ru: headed Chrome с постоянным профилем build/edisclosure-profile
# (gitignored, 0700). Капчу решает владелец вручную; коллектор только ждёт её --wait секунд.
# login — сохранить профиль; collect — собрать build/edisclosure/{manifest.json,documents.jsonl}
# (каталог переопределяется EDISCLOSURE_OUT или ARGS="--out ..."). Секреты не печатаются.
edisclosure-login:
	uv run --with playwright --with beautifulsoup4 --with lxml --with requests --python 3.12 \
		python3 scripts/edisclosure_collect.py login

edisclosure-collect:
	uv run --with playwright --with beautifulsoup4 --with lxml --with requests --python 3.12 \
		python3 scripts/edisclosure_collect.py collect $(ARGS)

# Smoke-проверка MCP end-to-end: поднимает сервер, вызывает list_tables и run_query по витринам,
# проверяет, что сырая таблица закрыта. Нужен запущенный ClickHouse (make ch-up) и CLICKHOUSE_PASSWORD.
mcp-check:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль пользователя $(MCP_USER))"; exit 1; }
	@curl -sf -m 2 http://$(MCP_CHECK_HOST):$(MCP_CHECK_PORT)/ping >/dev/null 2>&1 || { echo "ClickHouse на $(MCP_CHECK_HOST):$(MCP_CHECK_PORT) не отвечает: make ch-up (или поднимите туннель)"; exit 1; }
	CLICKHOUSE_HOST=$(MCP_CHECK_HOST) CLICKHOUSE_PORT=$(MCP_CHECK_PORT) CLICKHOUSE_SECURE=false CLICKHOUSE_VERIFY=false \
	CLICKHOUSE_USER=$(MCP_USER) CLICKHOUSE_DATABASE=default MCP_VERSION=$(MCP_VERSION) \
	uv run --with mcp-clickhouse==$(MCP_VERSION) --with mcp --python $(MCP_PY) python scripts/mcp_check.py

# Проверки перед деплоем: env-файл на хосте существует, ClickHouse отвечает.
# Секреты не читаются и не печатаются: проверяется только наличие файла.
staging-precheck:
	@ssh $(STAGING_HOST) 'test -f $(STAGING_ENV_FILE)' || { \
		echo "на $(STAGING_HOST) нет $(STAGING_ENV_FILE)"; \
		echo "создайте его из deploy/staging/clickhouse-import-rosstat.env.example (права 600)"; exit 1; }
	@ssh $(STAGING_HOST) 'curl -sf -m 3 http://localhost:8123/ping >/dev/null' || { \
		echo "ClickHouse на $(STAGING_HOST) не отвечает на :8123"; exit 1; }

# Бинарник импортёра: сборка, атомарная подмена, пропуск копирования при совпадении sha256.
# Подмена через .new + mv: scp поверх работающего файла даёт Text file busy.
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

deps:
	go mod download
	go mod tidy

clean:
	rm -rf $(BUILD_DIR)

DAGU_URL := https://github.com/dagu-org/dagu/releases/download/$(DAGU_VERSION)/dagu_$(DAGU_VERSION:v%=%)_linux_amd64.tar.gz

# dagu: скачать релизный tarball, подменить бинарник, поставить юнит. Идемпотентно по версии.
# Старый бинарник сохраняется в .bak — им пользуется откат (см. deploy/staging/README.md).
deploy-staging-dagu: staging-precheck
	@set -e; \
	want=$$(echo $(DAGU_VERSION) | sed 's/^v//'); \
	have=$$(ssh $(STAGING_HOST) '$(STAGING_BIN_DIR)/dagu version 2>/dev/null' | head -1 || true); \
	restart=0; \
	if [ "$$have" = "$$want" ]; then \
		echo "dagu $$have уже установлен"; \
	else \
		curl -fsSL -o /tmp/dagu_$(DAGU_VERSION).tar.gz '$(DAGU_URL)'; \
		tar xzf /tmp/dagu_$(DAGU_VERSION).tar.gz -C /tmp dagu; \
		scp /tmp/dagu $(STAGING_HOST):$(STAGING_BIN_DIR)/dagu.new; \
		ssh $(STAGING_HOST) 'test -f $(STAGING_BIN_DIR)/dagu && cp $(STAGING_BIN_DIR)/dagu $(STAGING_BIN_DIR)/dagu.bak || true; mv $(STAGING_BIN_DIR)/dagu.new $(STAGING_BIN_DIR)/dagu; chmod 755 $(STAGING_BIN_DIR)/dagu'; \
		echo "dagu обновлён до $(DAGU_VERSION)"; \
		restart=1; \
	fi; \
	if [ "$$restart" = "1" ] && ssh $(STAGING_HOST) 'systemctl is-active --quiet dagu'; then \
		ssh $(STAGING_HOST) 'systemctl restart dagu'; \
		echo "dagu перезапущен на новой версии"; \
	fi
	scp deploy/staging/dagu.service $(STAGING_HOST):/etc/systemd/system/dagu.service
	ssh $(STAGING_HOST) 'systemctl daemon-reload'

# mcp-clickhouse: юнит + прогрев кэша uv (пакет разрешается при старте сервиса).
# Отдельного «скачивания» нет: uv разрешает пакет по пину MCP_VERSION.
deploy-staging-mcp: staging-precheck
	scp deploy/staging/mcp-clickhouse.service $(STAGING_HOST):/etc/systemd/system/mcp-clickhouse.service
	ssh $(STAGING_HOST) 'systemctl daemon-reload'
	@echo "прогрев кэша uv (скачивание mcp-clickhouse==$(MCP_VERSION))..."
	@ssh $(STAGING_HOST) '$(STAGING_BIN_DIR)/uv tool run --from mcp-clickhouse==$(MCP_VERSION) python -c "pass" >/dev/null 2>&1 || true'

# MLflow: юнит + каталоги состояния + прогрев кэша uv. Версия в юните обязана совпадать
# с MLFLOW_VERSION: расхождение — ошибка деплоя (grep ниже), иначе сервис молча
# поднялся бы на другой версии, чем Makefile/бэкап-инструкция.
# Прогрев не роняет деплой: пакет всё равно разрешится при старте сервиса.
deploy-staging-mlflow: staging-precheck
	@grep -qF "mlflow==$(MLFLOW_VERSION)" deploy/staging/mlflow.service || { \
		echo "deploy/staging/mlflow.service: ExecStart не содержит mlflow==$(MLFLOW_VERSION)"; \
		echo "обновите версию в юните (она обязана совпадать с MLFLOW_VERSION) и повторите"; exit 1; }
	scp deploy/staging/mlflow.service $(STAGING_HOST):/etc/systemd/system/mlflow.service
	ssh $(STAGING_HOST) 'systemctl daemon-reload'
	ssh $(STAGING_HOST) 'install -d -m 755 /var/lib/mlflow /var/lib/mlflow/artifacts'
	@echo "прогрев кэша uv (скачивание mlflow==$(MLFLOW_VERSION))..."
	@ssh $(STAGING_HOST) '$(STAGING_BIN_DIR)/uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 python -c "pass" >/dev/null 2>&1 || true'

# Полный деплой: бинарь → dagu → mcp → mlflow → DAG'и → проверка ДО включения → включение → smoke.
#
# Порядок критичен. Сервисы включаются последними: dagu после `enable --now` немедленно
# начинает выполнять DAG'и по расписанию, поэтому сначала проверяется, что бинарь и
# DAG-файлы на месте (deploy-staging-preflight), и только потом сервисы запускаются.
deploy-staging: deploy-staging-binary deploy-staging-dagu deploy-staging-mcp deploy-staging-mlflow
	ssh $(STAGING_HOST) 'install -d -m 755 /root/dagu'
	scp dagu/*.yaml $(STAGING_HOST):/root/dagu/
	# scp копирует, но не удаляет: снятый с репозитория DAG остался бы на хосте и продолжил бы
	# выполняться по своему расписанию параллельно новой цепочке (первым удалён dagu/surveys.yaml —
	# cbr_mp_survey переехал 13-м шагом в цепочку dagu/cbr.yaml 09:17 МСК, а старый файл запускал бы
	# его в 09:47). Список пополнять при каждом удалении DAG из dagu/, иначе прунинг неполон.
	ssh $(STAGING_HOST) 'rm -f /root/dagu/surveys.yaml'
	@$(MAKE) deploy-staging-preflight --no-print-directory
	ssh $(STAGING_HOST) 'systemctl enable --now dagu mcp-clickhouse mlflow'
	@$(MAKE) deploy-staging-smoke --no-print-directory || { \
		echo ""; \
		echo "Сервисы оставлены ВКЛЮЧЁННЫМИ, а dagu выполняет DAG-и по расписанию."; \
		echo "Чтобы остановить: ssh $(STAGING_HOST) 'systemctl disable --now dagu mcp-clickhouse mlflow'"; \
		echo "После исправления повторите: make deploy-staging"; exit 1; }
	@echo "Готово. Далее: make mcp-user-staging (если kimi_reader ещё не создан)."

# Проверки ДО включения сервисов: бинарь исполняем, DAG-файлы на месте, токен непуст.
# Сервисы ещё не запущены, поэтому их проверяет deploy-staging-smoke.
# Токен проверяется на непустоту; значение не печатается.
deploy-staging-preflight:
	@ssh $(STAGING_HOST) 'test -x $(STAGING_BIN_DIR)/clickhouse-import-rosstat && echo "OK   бинарь импортёра исполняем"' || { echo "FAIL бинарь отсутствует или не исполняем"; exit 1; }
	@ssh $(STAGING_HOST) 'test -x $(STAGING_BIN_DIR)/dagu && echo "OK   dagu исполняем"' || { echo "FAIL dagu отсутствует или не исполняем"; exit 1; }
	@ssh $(STAGING_HOST) 'test -f /root/dagu/cbr.yaml && echo "OK   DAG-файлы на месте"' || { echo "FAIL /root/dagu/cbr.yaml отсутствует"; exit 1; }
	@ssh $(STAGING_HOST) 'set -a; . $(STAGING_ENV_FILE); set +a; \
		test -n "$$CLICKHOUSE_MCP_AUTH_TOKEN" && echo "OK   CLICKHOUSE_MCP_AUTH_TOKEN задан" || { echo "FAIL CLICKHOUSE_MCP_AUTH_TOKEN пуст — mcp-clickhouse не стартует в HTTP-режиме"; exit 1; }'

# Smoke после включения сервисов. Токен читается на хосте и не выводится.
deploy-staging-smoke:
	@echo "--- smoke: dagu ---"
	@ssh $(STAGING_HOST) 'systemctl is-active --quiet dagu && curl -sf -m 5 -o /dev/null http://127.0.0.1:8080/ && echo "OK   dagu отвечает"' || { echo "FAIL dagu: journalctl -u dagu -n 50"; exit 1; }
	@echo "--- smoke: mcp-clickhouse /health ---"
	@ssh $(STAGING_HOST) 'systemctl is-active --quiet mcp-clickhouse && curl -sf -m 5 -o /dev/null http://127.0.0.1:8000/health && echo "OK   MCP /health = 200"' || { \
		echo "FAIL mcp-clickhouse /health — сервис не отвечает или не видит ClickHouse."; \
		echo "  Частая причина на первом развёртывании: пользователь kimi_reader ещё не создан."; \
		echo "  Проверьте: systemctl status mcp-clickhouse; journalctl -u mcp-clickhouse -n 50"; \
		echo "  Создать пользователя: make mcp-user-staging"; exit 1; }
	@echo "--- smoke: MCP требует токен ---"
	@ssh $(STAGING_HOST) 'set -a; . $(STAGING_ENV_FILE); set +a; \
		code=$$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}"); \
		[ "$$code" = "401" ] && echo "OK   без токена → 401" || { echo "FAIL без токена получен $$code, ожидался 401"; exit 1; }'
	@echo "--- smoke: MCP с токеном ---"
	@ssh $(STAGING_HOST) 'set -a; . $(STAGING_ENV_FILE); set +a; \
		body=$$(curl -s -m 10 -X POST http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -H "Authorization: Bearer $$CLICKHOUSE_MCP_AUTH_TOKEN" -H "Accept: application/json, text/event-stream" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}"); \
		echo "$$body" | grep -q "Missing session ID" && echo "OK   MCP отвечает с токеном" || { echo "FAIL MCP с токеном ответил: $$body"; exit 1; }'
	@echo "--- smoke: mlflow /health ---"
	@ssh $(STAGING_HOST) 'systemctl is-active --quiet mlflow || { \
		echo "FAIL mlflow не запущен: systemctl status mlflow; journalctl -u mlflow -n 50"; exit 1; }; \
		code=000; \
		for i in $$(seq 1 10); do \
			code=$$(curl -s -o /dev/null -w "%{http_code}" -m 5 http://127.0.0.1:5000/health); \
			[ "$$code" = "200" ] && { echo "OK   MLflow /health = 200"; exit 0; }; \
			sleep 2; \
		done; \
		echo "FAIL mlflow /health: последний ответ HTTP $$code после 10 попыток — смотрите journalctl -u mlflow -n 50"; exit 1'

# Обновление ТОЛЬКО dagu, mcp-clickhouse и mlflow до версий из репозитория.
# apt upgrade сюда НЕ входит: ClickHouse (данные) и Grafana обновляются вручную.
#
# ⚠️ MLflow: этот таргет только копирует юнит и перезапускает сервис. Обновление
# ВЕРСИИ MLflow обязано идти документированной последовательностью
# stop → `mlflow db upgrade` → start (deploy/staging/README.md, «MLflow на staging»):
# новая версия на непромигрированной SQLite не поднимется, а `db upgrade` под живым
# сервером трогает БД, которой владеет процесс (SQLite — один писатель).
upgrade-staging: deploy-staging-dagu deploy-staging-mcp deploy-staging-mlflow
	@echo "⚠️  MLflow: смена версии — только stop → mlflow db upgrade → start (deploy/staging/README.md); upgrade-staging версию не мигрирует"
	ssh $(STAGING_HOST) 'systemctl restart dagu mcp-clickhouse mlflow'
	@echo "сервисы перезапущены на версиях: dagu $(DAGU_VERSION), mcp-clickhouse $(MCP_VERSION), mlflow $(MLFLOW_VERSION)"
	@$(MAKE) deploy-staging-smoke --no-print-directory

# Создание kimi_reader на staging. Пароль исполнителя (default) и пароль создаваемого
# пользователя (kimi_reader) — разные сущности, поэтому две переменные.
# Секреты идут через stdin ssh (не через argv): аргументы видны в ps на обеих машинах.
mcp-user-staging:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль kimi_reader)"; exit 1; }
	@test -n "$$CH_SETUP_PASSWORD" || { echo "задайте CH_SETUP_PASSWORD (пароль пользователя default на $(STAGING_HOST))"; exit 1; }
	scp scripts/mcp_setup_user.py $(STAGING_HOST):/tmp/mcp_setup_user.py
	scp sql/mcp_kimi_reader.sql $(STAGING_HOST):/tmp/mcp_kimi_reader.sql
	printf '%s\n%s\n' "$$CLICKHOUSE_PASSWORD" "$$CH_SETUP_PASSWORD" | ssh $(STAGING_HOST) 'read -r kimi_pass; read -r setup_pass; \
		CH_SETUP_SQL=/tmp/mcp_kimi_reader.sql CH_SETUP_URL=http://localhost:8123/ CH_SETUP_USER=default \
		CLICKHOUSE_PASSWORD="$$kimi_pass" CH_SETUP_PASSWORD="$$setup_pass" python3 /tmp/mcp_setup_user.py'

# Дашборды Grafana как код: отдельный Go-модуль grafana/ (SDK не тянется в ETL-бинарь).
# Артефакты dashboard/<uid>.json для всех дашбордов ($(DASHBOARDS)) коммитятся; генерация
# детерминирована — повторный прогон не меняет файлы (иначе CI-проверка diff вечно красная).
# `-out-dir ../dashboard` и дефолт `-dashboard all` собирают весь список модуля; пересобрать один
# дашборд — `cd grafana && go run . -dashboard <uid> -out-dir ../dashboard` (каталог должен существовать).
grafana-build:
	cd grafana && go run . -out-dir ../dashboard

# Валидация дашбордов как кода: vet + тесты модуля grafana/ (в т.ч. таблица негативных правил Validate).
# Серверная `gcx resources validate` в цель НЕ входит: gcx 1.5.0 проверяет ресурс на живом инстансе
# (server-side dry-run), то есть требует сервер, org-id и креды — в CI секретов нет и шаг всегда падал бы.
# Рабочая форма проверена живьём на стенде 13.2.3 (gcx 1.5.0) и оставлена тут, а не в рецепте
# (команда берёт один файл — при желании прогоняется циклом по $(DASHBOARDS)):
#   GRAFANA_SERVER=$(GRAFANA_TEST_URL) GRAFANA_ORG_ID=$(GRAFANA_ORG_ID) \
#     gcx resources validate dashboards --path dashboard/gold-thesis.json   # ✔ No errors found
# Без конфига/кредов команда падает: «context references no stack with grafana config» (exit 1),
# с неверными кредами — 401 (exit 3); basic auth поддержан (GRAFANA_USER/GRAFANA_PASSWORD).
# Офлайновая альтернатива в CI (тоже gcx, без секретов) — линтер по файлам артефактов:
# `gcx dev lint run PATH...` принимает несколько путей и линтит их одним прогоном (проверено на gcx 1.5.0:
# «3 files linted. No violations found.», exit 0):
#   gcx dev lint run --disable uneditable-dashboard --disable panel-title-description \
#     dashboard/gold-thesis.json dashboard/key-rate-publications.json dashboard/finance-polyus.json
grafana-validate:
	cd grafana && go vet ./... && go test ./...
	@go test ./views -run TestMLLiveAlertProvisioning
	@uv run --python 3.12 python3 scripts/grafana_alert_check_test.py

# Локальный стенд Grafana для проверки дашборда: тот же провижининг, что уедет в деплой.
# Плагин ставится CLI Grafana в $(GF_PLUGINS) (флаг --pluginsDir), сервер читает его
# оттуда же через GF_PATHS_PLUGINS. Установка идемпотентна по наличию каталога плагина.
# И CLI, и серверу нужен --homepath: у brew-бинаря не найти conf/defaults.ini ни из
# рабочего каталога репозитория, ни по GF_PATHS_HOME (проверено на 13.2.3).
grafana-up:
	@if curl -sf -m 2 http://localhost:$(GF_PORT)/api/health >/dev/null 2>&1; then \
		echo "Grafana уже отвечает на порту $(GF_PORT)"; \
	elif [ -f "$(GF_PID)" ] && kill -0 $$(cat "$(GF_PID)") 2>/dev/null; then \
		echo "Grafana уже запускается/запущена (PID $$(cat "$(GF_PID)")), второй сервер не поднимается"; \
	else \
		test -n "$(GF_BIN)" || { echo "grafana не найден: задайте GF_BIN=/путь/к/grafana"; exit 1; }; \
		if [ ! -d "$(GF_PLUGINS)/grafana-clickhouse-datasource" ]; then \
			"$(GF_BIN)" cli --homepath "$(GF_HOMEPATH)" --pluginsDir "$(GF_PLUGINS)" \
				plugins install grafana-clickhouse-datasource $(GRAFANA_PLUGIN_VERSION) || exit 1; \
		fi; \
		mkdir -p "$(GF_DIR)/data" "$(GF_DIR)/logs"; \
		nohup env GF_PATHS_DATA="$(GF_DIR)/data" GF_PATHS_LOGS="$(GF_DIR)/logs" GF_PATHS_PLUGINS="$(GF_PLUGINS)" \
			GF_PATHS_PROVISIONING="$(CURDIR)/grafana/provisioning" GF_SERVER_HTTP_PORT=$(GF_PORT) \
			GF_ANALYTICS_REPORTING_ENABLED=false GF_ANALYTICS_CHECK_FOR_UPDATES=false \
			"$(GF_BIN)" server --homepath "$(GF_HOMEPATH)" >"$(GF_LOG)" 2>&1 & \
		echo $$! > "$(GF_PID)"; \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(GF_PID)") 2>/dev/null || break; curl -sf -m 1 http://localhost:$(GF_PORT)/api/health >/dev/null 2>&1 && break; sleep 1; done; \
		if kill -0 $$(cat "$(GF_PID)") 2>/dev/null && curl -sf -m 2 http://localhost:$(GF_PORT)/api/health >/dev/null 2>&1; then \
			echo "Grafana запущена, PID $$(cat "$(GF_PID)"), лог $(GF_LOG)"; \
		else \
			echo "Grafana не поднялась, хвост $(GF_LOG):"; tail -20 "$(GF_LOG)"; exit 1; \
		fi; \
	fi

# Останавливает сервер и ждёт завершения до 60 с. Если процесс пережил сигнал, pid-файл
# НЕ удаляется: иначе следующий grafana-down не найдёт процесс, а grafana-up скажет «уже отвечает».
grafana-down:
	@if [ -f "$(GF_PID)" ] && kill -0 $$(cat "$(GF_PID)") 2>/dev/null; then \
		kill $$(cat "$(GF_PID)"); \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(GF_PID)") 2>/dev/null || break; sleep 1; done; \
		if kill -0 $$(cat "$(GF_PID)") 2>/dev/null; then \
			echo "Grafana не завершилась за 60 с, PID-файл $(GF_PID) сохранён: повторите make grafana-down"; \
		else \
			rm -f "$(GF_PID)"; echo "Grafana остановлена"; \
		fi; \
	else \
		echo "PID-файл $(GF_PID) не найден или процесс не запущен (сервер, поднятый вне make, не останавливается)"; \
	fi

# Выгрузка артефактов dashboard/<uid>.json всех трёх дашбордов на тест-стенд и smoke-проверка панелей.
# Рабочая форма вызова подобрана живьём на стенде Grafana 13.2.3 (gcx 1.5.0): selector dashboards
# + --path на файл-артефакт; push принимает apiVersion dashboard.grafana.app/v1beta1 и идемпотентен
# (повторный прогон обновляет тот же ресурс metadata.name=<uid>, дубль не создаётся).
# Фолбэки (тоже проверены живьём, в цели не нужны):
#   gcx resources push -p dashboard/<uid>.json
#   gcx api /api/dashboards/db -d @payload.json   # REST-фолбэк; payload = {"dashboard": <spec из артефакта>,
#                                                 # "overwrite": true} — сама spec без обёртки не принимается
# Полезно перед живым прогоном: gcx resources push dashboards --path dashboard/<uid>.json --dry-run
grafana-deploy-test:
	@test -n "$$GRAFANA_TOKEN" || { echo "задайте GRAFANA_TEST_TOKEN в $(ROSSTAT_ENV) (сервис-аккаунт Editor на $(GRAFANA_TEST_URL))"; exit 1; }
	@for uid in $(DASHBOARDS); do \
		echo "push dashboard/$$uid.json"; \
		GRAFANA_SERVER=$(GRAFANA_TEST_URL) GRAFANA_ORG_ID=$(GRAFANA_ORG_ID) \
			gcx resources push dashboards --path dashboard/$$uid.json || exit 1; \
	done
	@$(MAKE) grafana-check --no-print-directory

# Smoke задеплоенных дашбордов: каждый виден по uid и первые панели отдают данные.
# Проверка читает дашборд со стенда, поэтому ловит и «артефакт не доехал», и «панель пустая».
# Цикл по $(DASHBOARDS) останавливается на первой ошибке: чинить и передеплоивать по одному дашборду проще,
# чем разбирать три перемешанных отчёта (SQL каждого дашборда скрипт берёт из снятого со стенда артефакта).
grafana-check:
	@test -n "$$GRAFANA_TOKEN" || { echo "задайте GRAFANA_TEST_TOKEN в $(ROSSTAT_ENV) (сервис-аккаунт Editor на $(GRAFANA_TEST_URL))"; exit 1; }
	@for uid in $(DASHBOARDS); do \
		echo "smoke uid=$$uid"; \
		GRAFANA_URL=$(GRAFANA_TEST_URL) python3 scripts/grafana_check.py --uid $$uid || exit 1; \
	done
	@echo "smoke alert rules"
	@GRAFANA_URL=$(GRAFANA_TEST_URL) python3 scripts/grafana_alert_check.py

# Токен локального стенда одной командой: сервис-аккаунт создаётся идемпотентно (повторный прогон
# переиспользует $(GRAFANA_SA_NAME) и говорит «уже есть»), а токен — всегда новый: Grafana показывает
# ключ один раз, поэтому старый не переиспользуется (отозвать — в UI: Administration → Users and
# access → Service accounts → $(GRAFANA_SA_NAME) → Tokens). Токен проверяется GET /api/user (Bearer)
# ДО печати и в argv не попадает: уходит в curl конфигом на stdin (-K -), python3 читает JSON по pipe.
# Имя токена — deploy-<UTC-метка>-<pid>, а не фиксированное `deploy`: Grafana 13.2.3 на повтор имени
# отвечает 400 ErrTokenAlreadyExists «service account token with given name already exists in the
# organization» (проверено живьём), поэтому фиксированное имя сломало бы второй прогон цели. Префикс
# `deploy` — чтобы токены цели были узнаваемы в UI при отзыве, метка/pid — чтобы имя было уникальным
# без окна гонки (два запуска в одну секунду не столкнутся).
# Последние две строки — готовая строка для $(ROSSTAT_ENV); остальное в stdout (id аккаунта, служебные
# сообщения, подсказка про GRAFANA_TOKEN для ручных вызовов gcx), чтобы владелец видел только то, что копирует.
# Админ-креды — дефолт локального одноразового стенда (admin/admin, см. README §Grafana),
# переопределяются окружением, в т.ч. $(ROSSTAT_ENV). В рецепте стоят ссылки на переменные
# окружения, а не их значения, поэтому `make -n` секретов не печатает.
# Список аккаунтов — /api/serviceaccounts/search: GET /api/serviceaccounts на Grafana 13.2.3 отвечает
# 404 «Not found» (проверено живьём), поиск и есть документированный list-эндпоинт; точное сравнение
# имени делает python3 (query у Grafana — подстрока, совпадение по подстроке дало бы чужой id).
GRAFANA_ADMIN_USER     ?= admin
GRAFANA_ADMIN_PASSWORD ?= admin
GRAFANA_SA_NAME        ?= dashboards-cd
GRAFANA_SA_ROLE        ?= Editor
grafana-test-token:
	@curl -sf -m 2 $(GRAFANA_TEST_URL)/api/health >/dev/null 2>&1 || { echo "стенд не отвечает: make grafana-up"; exit 1; }
	@sa_json=$$(curl -sf -m 5 -u "$$GRAFANA_ADMIN_USER:$$GRAFANA_ADMIN_PASSWORD" "$(GRAFANA_TEST_URL)/api/serviceaccounts/search?perpage=100") \
		|| { echo "не удалось прочитать список сервис-аккаунтов $(GRAFANA_TEST_URL): проверьте GRAFANA_ADMIN_USER/GRAFANA_ADMIN_PASSWORD"; exit 1; }; \
	sa_id=$$(printf '%s' "$$sa_json" | python3 -c "import json,sys; name=sys.argv[1]; accounts=json.load(sys.stdin).get('serviceAccounts') or []; found=[a for a in accounts if a.get('name') == name]; print(found[0]['id'] if found else '')" "$(GRAFANA_SA_NAME)") \
		|| { echo "ответ $(GRAFANA_TEST_URL)/api/serviceaccounts/search не разобран (ждали JSON с serviceAccounts)"; exit 1; }; \
	if [ -n "$$sa_id" ]; then \
		echo "сервис-аккаунт $(GRAFANA_SA_NAME) уже есть (id $$sa_id)"; \
	else \
		sa_json=$$(curl -sf -m 5 -u "$$GRAFANA_ADMIN_USER:$$GRAFANA_ADMIN_PASSWORD" -X POST \
			-H "Content-Type: application/json" -d '{"name":"$(GRAFANA_SA_NAME)","role":"$(GRAFANA_SA_ROLE)"}' \
			"$(GRAFANA_TEST_URL)/api/serviceaccounts") \
			|| { echo "не удалось создать сервис-аккаунт $(GRAFANA_SA_NAME) на $(GRAFANA_TEST_URL)"; exit 1; }; \
		sa_id=$$(printf '%s' "$$sa_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") \
			|| { echo "в ответе на создание сервис-аккаунта нет id"; exit 1; }; \
		echo "сервис-аккаунт $(GRAFANA_SA_NAME) создан (id $$sa_id, роль $(GRAFANA_SA_ROLE))"; \
	fi; \
	token_name=deploy-$$(date -u +%Y%m%dT%H%M%SZ)-$$$$; \
	token_json=$$(curl -sf -m 10 -u "$$GRAFANA_ADMIN_USER:$$GRAFANA_ADMIN_PASSWORD" -X POST \
		-H "Content-Type: application/json" -d "$$(printf '{"name":"%s"}' "$$token_name")" \
		"$(GRAFANA_TEST_URL)/api/serviceaccounts/$$sa_id/tokens") \
		|| { echo "не удалось создать токен $$token_name для сервис-аккаунта $$sa_id"; exit 1; }; \
	token=$$(printf '%s' "$$token_json" | python3 -c "import json,sys; key=json.load(sys.stdin).get('key') or ''; key.startswith('glsa_') or sys.exit('в ответе нет ключа glsa_...'); print(key)") \
		|| { echo "в ответе на создание токена нет ключа glsa_..."; exit 1; }; \
	code=$$(printf 'url = "%s"\nheader = "Authorization: Bearer %s"\n' "$(GRAFANA_TEST_URL)/api/user" "$$token" | curl -s -o /dev/null -w "%{http_code}" -m 10 -K -); \
	[ "$$code" = "200" ] || { echo "токен не прошёл проверку GET /api/user (HTTP $$code) — в $(ROSSTAT_ENV) вставлять нельзя"; exit 1; }; \
	echo "Ручные вызовы gcx читают только GRAFANA_TOKEN (не GRAFANA_TEST_TOKEN); make-цели подставляют его сами."; \
	echo "Добавьте строку в ~/.config/rosstat/env (секрет, не коммитить):"; \
	echo "GRAFANA_TEST_TOKEN=$$token"

# Провижининг датасорса на staging (идемпотентный). Шаблон с плейсхолдером копируется в /tmp хоста,
# пароль подставляется НА ХОСТЕ из $(STAGING_ENV_FILE) — python3, а не sed (у sed сломались бы / & \ и др.).
# Пароль стоит в YAML-скаляре в двойных кавычках, поэтому перед подстановкой экранируются \ и " —
# без этого `a\b"c` делает YAML невалидным, а `x\\y` молча превращается в `x\y` (проверено на фикстурах).
# Пароль с управляющим символом (перевод строки, таб, 0x7f) отвергается с понятной ошибкой, а не портит YAML.
# После подстановки файл перечитывается: значение датасорса декодируется обратно (json.loads — те же
# \\ и \" , что в YAML-скаляре) и сравнивается с паролем, то есть round-trip обязан сойтись.
# Результат сравнивается с установленным файлом по sha256 и переустанавливается только при отличии,
# поэтому повторный прогон ничего не пишет.
# Пароль не печатается; временные копии (root, 600) удаляются по trap, в т.ч. при ошибке.
# ВАЖНО (проверено на Grafana 13.1.1): каталог provisioning/datasources читается ТОЛЬКО при старте
# сервера (pkg/services/provisioning: ProvisionDatasources вызывается из starting(); поллинг есть лишь
# у дашбордов). Поэтому новый/изменённый файл датасорса сам по себе не активируется: нужен admin-reload
# (POST /api/admin/provisioning/datasources/reload) или перезапуск Grafana — руками владельца; наша цель
# Grafana не рестартует. Цель вызывается из grafana-deploy-staging, но запускается и отдельно.
GRAFANA_DATASOURCE_TMPL   ?= deploy/staging/grafana-datasource.yaml
GRAFANA_DATASOURCE_REMOTE ?= /etc/grafana/provisioning/datasources/clickhouse.yaml
GRAFANA_DATASOURCE_TMP    ?= /tmp/grafana-datasource.staging.yaml
grafana-provision-staging:
	@test -f "$(GRAFANA_DATASOURCE_TMPL)" || { echo "нет шаблона $(GRAFANA_DATASOURCE_TMPL)"; exit 1; }
	@scp -q "$(GRAFANA_DATASOURCE_TMPL)" "$(STAGING_HOST):$(GRAFANA_DATASOURCE_TMP)" || { echo "scp: шаблон не доехал до $(STAGING_HOST)"; exit 1; }
	@ssh $(STAGING_HOST) 'set -eu; \
		trap "rm -f $(GRAFANA_DATASOURCE_TMP) $(GRAFANA_DATASOURCE_TMP).rendered" EXIT; \
		[ -f $(STAGING_ENV_FILE) ] || { echo "НЕТ  $(STAGING_ENV_FILE) не найден на $(STAGING_HOST)"; exit 1; }; \
		set -a; . $(STAGING_ENV_FILE); set +a; \
		test -n "$${CLICKHOUSE_PASSWORD:-}" || { echo "НЕТ  CLICKHOUSE_PASSWORD не задан в $(STAGING_ENV_FILE)"; exit 1; }; \
		install -m 600 /dev/null "$(GRAFANA_DATASOURCE_TMP).rendered"; \
		CLICKHOUSE_PASSWORD="$${CLICKHOUSE_PASSWORD:-}" python3 -c "import os, sys; BS = chr(92); DQ = chr(34); pw = os.environ[\"CLICKHOUSE_PASSWORD\"]; bad = [c for c in pw if ord(c) < 32 or ord(c) == 127]; bad and sys.exit(\"пароль содержит управляющий символ 0x%02x — в YAML его подставить нельзя (перевод строки/таб?)\" % ord(bad[0])); src = open(sys.argv[1], encoding=\"utf-8\").read(); \"__CLICKHOUSE_PASSWORD__\" in src or sys.exit(\"в шаблоне нет плейсхолдера __CLICKHOUSE_PASSWORD__\"); open(sys.argv[2], \"w\", encoding=\"utf-8\").write(src.replace(\"__CLICKHOUSE_PASSWORD__\", pw.replace(BS, BS + BS).replace(DQ, BS + DQ)))" "$(GRAFANA_DATASOURCE_TMP)" "$(GRAFANA_DATASOURCE_TMP).rendered"; \
		CLICKHOUSE_PASSWORD="$${CLICKHOUSE_PASSWORD:-}" python3 -c "import json, os, sys; DQ = chr(34); pw = os.environ[\"CLICKHOUSE_PASSWORD\"]; lines = [l for l in open(sys.argv[1], encoding=\"utf-8\").read().splitlines() if \"password:\" in l]; (len(lines) == 1 and json.loads(DQ + lines[0].split(\":\", 1)[1].strip()[1:-1] + DQ) == pw) or sys.exit(\"round-trip пароля не сошёлся: YAML-скаляр собрался бы неверно\")" "$(GRAFANA_DATASOURCE_TMP).rendered"; \
		target="$(GRAFANA_DATASOURCE_REMOTE)"; \
		if [ -f "$$target" ] && [ "$$(sha256sum < "$(GRAFANA_DATASOURCE_TMP).rendered" | cut -c1-64)" = "$$(sha256sum < "$$target" | cut -c1-64)" ]; then \
			echo "OK   провижининг датасорса актуален ($$target)"; \
		else \
			install -o root -g grafana -m 640 "$(GRAFANA_DATASOURCE_TMP).rendered" "$$target"; \
			echo "OK   провижининг датасорса обновлён ($$target)"; \
			echo "ВНИМАНИЕ: Grafana применяет датасорсы из provisioning только при старте. Пока владелец не сделает admin-reload"; \
			echo "  curl -u admin:<пароль> -X POST $(GRAFANA_STAGING_URL)/api/admin/provisioning/datasources/reload"; \
			echo "  (или не перезапустит grafana-server), датасорс ch-rosstat не активен и smoke упадёт: см. deploy/staging/README.md"; \
		fi'

# Provisioning алертов live-мониторинга ML (infra/4.3) на staging: файл кладётся
# в /etc/grafana/provisioning/alerting/ рядом с датасорсом. Grafana применяет
# alerting-provisioning при старте; после копирования нужен admin-reload
# (POST /api/admin/provisioning/alert-rules/reload) или рестарт grafana-server.
# Админ-пароль не хранится в репозитории: цель только печатает команду.
grafana-provision-alerting-staging:
	@test -f grafana/provisioning/alerting/ml-live.yaml || { echo "нет grafana/provisioning/alerting/ml-live.yaml"; exit 1; }
	@scp -q grafana/provisioning/alerting/ml-live.yaml "$(STAGING_HOST):/tmp/ml-live-alerting.yaml" || { echo "scp: alerting-файл не доехал до $(STAGING_HOST)"; exit 1; }
	@ssh $(STAGING_HOST) 'set -eu; \
		install -d -o root -g grafana -m 755 /etc/grafana/provisioning/alerting; \
		install -o root -g grafana -m 640 /tmp/ml-live-alerting.yaml /etc/grafana/provisioning/alerting/ml-live.yaml; \
		rm -f /tmp/ml-live-alerting.yaml; \
		echo "OK   alerting provisioning установлен: /etc/grafana/provisioning/alerting/ml-live.yaml"; \
		echo "     reload: curl -u admin:<пароль> -X POST http://127.0.0.1:3000/api/admin/provisioning/alert-rules/reload"; \
		echo "     (или systemctl restart grafana-server)"'

# Деплой всех трёх дашбордов ($(DASHBOARDS)) на staging: провижининг датасорса → push → smoke.
# Токен (GRAFANA_STAGING_TOKEN, сервис-аккаунт Editor на staging) живёт только у владельца
# в $(ROSSTAT_ENV); gcx общается с инстансом через туннель, а панели — с ClickHouse хоста.
# Grafana и systemd на хосте не трогаются: нужен только поднятый туннель.
grafana-deploy-staging:
	@test -n "$$GRAFANA_STAGING_TOKEN" || { echo "задайте GRAFANA_STAGING_TOKEN в $(ROSSTAT_ENV) — токен сервис-аккаунта Editor на staging; создаётся одноразово вручную (runbook: docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md §6)"; exit 1; }
	@curl -sf -m 3 $(GRAFANA_STAGING_URL)/api/health >/dev/null 2>&1 || { echo "staging Grafana не отвечает на $(GRAFANA_STAGING_URL): поднимите туннель — ssh -N -L 3001:127.0.0.1:3000 $(STAGING_HOST)"; exit 1; }
	@$(MAKE) grafana-provision-staging --no-print-directory
	@$(MAKE) grafana-provision-alerting-staging --no-print-directory
	@for uid in $(DASHBOARDS); do \
		echo "push dashboard/$$uid.json"; \
		GRAFANA_SERVER=$(GRAFANA_STAGING_URL) GRAFANA_ORG_ID=$(GRAFANA_ORG_ID) GRAFANA_TOKEN="$$GRAFANA_STAGING_TOKEN" \
			gcx resources push dashboards --path dashboard/$$uid.json || exit 1; \
	done
	@for uid in $(DASHBOARDS); do \
		echo "smoke uid=$$uid"; \
		GRAFANA_URL=$(GRAFANA_STAGING_URL) GRAFANA_TOKEN="$$GRAFANA_STAGING_TOKEN" \
			python3 scripts/grafana_check.py --uid $$uid || exit 1; \
	done

info:
	@echo "BINARY    = $(TARGET)"
	@echo "GOOS      = linux"
	@echo "GOARCH    = amd64"
	@echo "CGO_ENABLED = 0"

# Docs-портал: генератор (stdlib) собирает build/site-docs и build/mkdocs.yml,
# mkdocs-material строит статический сайт. Сеть нужна только для скачивания mkdocs-material через uv.
docs-check:
	python3 scripts/docs_build.py --check
	python3 scripts/docs_build_test.py

docs-build:
	python3 scripts/docs_build.py --out build/site-docs
	$(MKDOCS) build -f build/mkdocs.yml --strict

docs-serve:
	python3 scripts/docs_build.py --out build/site-docs
	$(MKDOCS) serve -f build/mkdocs.yml

