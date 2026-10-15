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
# Список uid обоих дашбордов контура — источник истины для деплоя и smoke: цели ниже идут по нему
# циклом, поэтому третий дашборд добавляется здесь одной строкой (свои uid генератор grafana/ знает сам).
DASHBOARDS       := gold-thesis key-rate-publications
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

# Деплой на staging (см. deploy/staging/README.md). Хост задаётся без правок Makefile.
STAGING_HOST     ?= palmshell
STAGING_BIN_DIR  ?= /root/.local/bin
STAGING_ENV_FILE ?= /etc/clickhouse-import-rosstat.env
DAGU_VERSION     ?= v2.18.2

.PHONY: all lint fmt vet test build run deps clean info ch-up ch-down ch-status ch-sql dev-check env-check import archive-registry mcp-run mcp-user mcp-check build-ingest run-ingest deploy-staging deploy-staging-binary deploy-staging-dagu deploy-staging-mcp deploy-staging-preflight deploy-staging-smoke upgrade-staging mcp-user-staging staging-precheck grafana-build grafana-validate grafana-up grafana-down grafana-deploy-test grafana-deploy-staging grafana-provision-staging grafana-check grafana-test-token

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

# Git-зеркало реестра винтажного архива (make archive-registry [ARCHIVE_BACKEND=yandex]):
# скачивает registry.jsonl из хранилища в docs/archive/; коммит — ручной/периодический.
archive-registry:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=registry ./$(BUILD_DIR)/$(BINARY)-native

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

# Полный деплой: бинарь → dagu → mcp → DAG'и → проверка ДО включения → включение → smoke.
#
# Порядок критичен. Сервисы включаются последними: dagu после `enable --now` немедленно
# начинает выполнять DAG'и по расписанию, поэтому сначала проверяется, что бинарь и
# DAG-файлы на месте (deploy-staging-preflight), и только потом сервисы запускаются.
deploy-staging: deploy-staging-binary deploy-staging-dagu deploy-staging-mcp
	ssh $(STAGING_HOST) 'install -d -m 755 /root/dagu'
	scp dagu/*.yaml $(STAGING_HOST):/root/dagu/
	# scp копирует, но не удаляет: снятый с репозитория DAG остался бы на хосте и продолжил бы
	# выполняться по своему расписанию параллельно новой цепочке (первым удалён dagu/surveys.yaml —
	# cbr_mp_survey переехал 13-м шагом в цепочку dagu/cbr.yaml 09:17 МСК, а старый файл запускал бы
	# его в 09:47). Список пополнять при каждом удалении DAG из dagu/, иначе прунинг неполон.
	ssh $(STAGING_HOST) 'rm -f /root/dagu/surveys.yaml'
	@$(MAKE) deploy-staging-preflight --no-print-directory
	ssh $(STAGING_HOST) 'systemctl enable --now dagu mcp-clickhouse'
	@$(MAKE) deploy-staging-smoke --no-print-directory || { \
		echo ""; \
		echo "Сервисы оставлены ВКЛЮЧЁННЫМИ, а dagu выполняет DAG-и по расписанию."; \
		echo "Чтобы остановить: ssh $(STAGING_HOST) 'systemctl disable --now dagu mcp-clickhouse'"; \
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

# Обновление ТОЛЬКО dagu и mcp-clickhouse до версий из репозитория.
# apt upgrade сюда НЕ входит: ClickHouse (данные) и Grafana обновляются вручную.
upgrade-staging: deploy-staging-dagu deploy-staging-mcp
	ssh $(STAGING_HOST) 'systemctl restart dagu mcp-clickhouse'
	@echo "сервисы перезапущены на версиях: dagu $(DAGU_VERSION), mcp-clickhouse $(MCP_VERSION)"
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
# Артефакты dashboard/<uid>.json для обоих дашбордов ($(DASHBOARDS)) коммитятся; генерация
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
# «2 files linted. No violations found.», exit 0):
#   gcx dev lint run --disable uneditable-dashboard --disable panel-title-description \
#     dashboard/gold-thesis.json dashboard/key-rate-publications.json
grafana-validate:
	cd grafana && go vet ./... && go test ./...

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

# Выгрузка артефактов dashboard/<uid>.json обоих дашбордов на тест-стенд и smoke-проверка панелей.
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
# чем разбирать два перемешанных отчёта (SQL каждого дашборда скрипт берёт из снятого со стенда артефакта).
grafana-check:
	@test -n "$$GRAFANA_TOKEN" || { echo "задайте GRAFANA_TEST_TOKEN в $(ROSSTAT_ENV) (сервис-аккаунт Editor на $(GRAFANA_TEST_URL))"; exit 1; }
	@for uid in $(DASHBOARDS); do \
		echo "smoke uid=$$uid"; \
		GRAFANA_URL=$(GRAFANA_TEST_URL) python3 scripts/grafana_check.py --uid $$uid || exit 1; \
	done

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

# Деплой обоих дашбордов ($(DASHBOARDS)) на staging: провижининг датасорса → push → smoke.
# Токен (GRAFANA_STAGING_TOKEN, сервис-аккаунт Editor на staging) живёт только у владельца
# в $(ROSSTAT_ENV); gcx общается с инстансом через туннель, а панели — с ClickHouse хоста.
# Grafana и systemd на хосте не трогаются: нужен только поднятый туннель.
grafana-deploy-staging:
	@test -n "$$GRAFANA_STAGING_TOKEN" || { echo "задайте GRAFANA_STAGING_TOKEN в $(ROSSTAT_ENV) — токен сервис-аккаунта Editor на staging; создаётся одноразово вручную (runbook: docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md §6)"; exit 1; }
	@curl -sf -m 3 $(GRAFANA_STAGING_URL)/api/health >/dev/null 2>&1 || { echo "staging Grafana не отвечает на $(GRAFANA_STAGING_URL): поднимите туннель — ssh -N -L 3001:127.0.0.1:3000 $(STAGING_HOST)"; exit 1; }
	@$(MAKE) grafana-provision-staging --no-print-directory
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
