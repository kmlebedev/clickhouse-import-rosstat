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

.PHONY: all lint fmt vet test build run deps clean info ch-up ch-down ch-status ch-sql dev-check env-check import mcp-run mcp-user mcp-check build-ingest run-ingest deploy-staging deploy-staging-binary deploy-staging-dagu deploy-staging-mcp deploy-staging-preflight deploy-staging-smoke upgrade-staging mcp-user-staging staging-precheck grafana-build grafana-up grafana-down

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
# Артефакт dashboard/gold-thesis.json коммитится; генерация детерминирована —
# повторный прогон не должен менять файл (иначе CI-проверка diff вечно красная).
grafana-build:
	cd grafana && go run . -out ../dashboard/gold-thesis.json

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

info:
	@echo "BINARY    = $(TARGET)"
	@echo "GOOS      = linux"
	@echo "GOARCH    = amd64"
	@echo "CGO_ENABLED = 0"
