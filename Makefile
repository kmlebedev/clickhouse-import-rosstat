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

MCP_USER     ?= kimi_reader
MCP_PY       ?= 3.12
MCP_CMD      := uv run --with mcp-clickhouse --python $(MCP_PY) mcp-clickhouse

.PHONY: all lint fmt vet test build run deps clean info ch-up ch-down ch-status ch-sql dev-check env-check import mcp-run mcp-user mcp-check build-ingest run-ingest

all: lint test build

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
	else \
		test -n "$(CH_BIN)" || { echo "clickhouse не найден: задайте CH_BIN=/путь/к/clickhouse"; exit 1; }; \
		mkdir -p "$(CH_DATA)"; \
		cd "$(CH_DIR)"; \
		nohup "$(CH_BIN)" server -- --path="$(CH_DATA)/" --http_port=$(CH_HTTP_PORT) --tcp_port=$(CH_TCP_PORT) >"$(CH_LOG)" 2>&1 & \
		echo $$! > "$(CH_PID)"; \
		for i in $$(seq 1 60); do curl -sf -m 1 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 && break; sleep 1; done; \
		if curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1; then \
			echo "ClickHouse запущен, PID $$(cat "$(CH_PID)"), лог $(CH_LOG)"; \
		else \
			echo "ClickHouse не поднялся, смотрите $(CH_LOG)"; exit 1; \
		fi; \
	fi

ch-down:
	@if [ -f "$(CH_PID)" ] && kill -0 $$(cat "$(CH_PID)") 2>/dev/null; then \
		kill $$(cat "$(CH_PID)"); \
		for i in $$(seq 1 60); do kill -0 $$(cat "$(CH_PID)") 2>/dev/null || break; sleep 1; done; \
		rm -f "$(CH_PID)"; echo "ClickHouse остановлен"; \
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
	@for v in BEA_API_KEY BLS_API_KEY CLICKHOUSE_URL CLICKHOUSE_PASSWORD; do \
		if [ -n "$$(printenv $$v)" ]; then echo "OK   $$v задан"; else echo "НЕТ  $$v не задан (см. README, раздел «Ключи и окружение»)"; fi; \
	done

# Локальный прогон одного импортёра: make import STAT=bea (нужны CLICKHOUSE_URL и ключ импортёра из $(ROSSTAT_ENV)).
STAT ?=
import:
	@test -n "$(STAT)" || { echo "укажите импортёр: make import STAT=bea"; exit 1; }
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	CLICKHOUSE_IMPORT_STAT=$(STAT) ./$(BUILD_DIR)/$(BINARY)-native

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

# Smoke-проверка MCP end-to-end: поднимает сервер, вызывает list_tables и run_query по витринам,
# проверяет, что сырая таблица закрыта. Нужен запущенный ClickHouse (make ch-up) и CLICKHOUSE_PASSWORD.
mcp-check:
	@test -n "$$CLICKHOUSE_PASSWORD" || { echo "задайте CLICKHOUSE_PASSWORD (пароль пользователя $(MCP_USER))"; exit 1; }
	@curl -sf -m 2 http://localhost:$(CH_HTTP_PORT)/ping >/dev/null 2>&1 || { echo "ClickHouse не отвечает: make ch-up"; exit 1; }
	CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=$(CH_HTTP_PORT) CLICKHOUSE_SECURE=false CLICKHOUSE_VERIFY=false \
	CLICKHOUSE_USER=$(MCP_USER) CLICKHOUSE_DATABASE=default \
	uv run --with mcp-clickhouse --with mcp --python $(MCP_PY) python scripts/mcp_check.py

deps:
	go mod download
	go mod tidy

clean:
	rm -rf $(BUILD_DIR)

info:
	@echo "BINARY    = $(TARGET)"
	@echo "GOOS      = linux"
	@echo "GOARCH    = amd64"
	@echo "CGO_ENABLED = 0"
