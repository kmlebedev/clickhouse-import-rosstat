# Спека: `infra/5.9` — публичный MCP по TLS и подключение плагина

**Дата:** 2026-10-31
**Пункт:** `infra/5.9` (issue [#456](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/456))
**Роль:** Go-инженер (deploy, Makefile, манифесты плагина)

## 1. Цель

LLM-плагин репозитория (`kimi.plugin.json`, `.claude-plugin/`, `.codex-plugin/`, `.cursor-plugin/`, `qwen-extension.json`) подключает агента к read-only MCP staging (`kimi_reader`, только витрины `v_*`, включая point-in-time слой) по публичному адресу `https://mcp.selfinvesting.ru:8443/mcp`. Токен выдаёт владелец; пользователь кладёт его в переменную окружения `ROSSTAT_MCP_TOKEN`, в манифестах токена нет.

## 2. Исходное состояние (приёмка 2026-10-31)

- `mcp-clickhouse` 0.7.0 на staging слушает `0.0.0.0:8000` по **plain HTTP** (коммит `db83d6f`), Bearer-токен идёт открытым текстом; `ARCHITECTURE.md` §6.5 и `deploy/staging/README.md` при этом описывают loopback и SSH-туннель.
- `/health` = 200, `POST /mcp` без токена = 401 — авторизация работает.
- В манифестах плагина `mcpServers` нет (решение спеки 5.7 §3.3: не дублировать локальный `clickhouse` из `.kimi-code/mcp.json`).

## 3. Решение

### 3.1 TLS в самом `mcp-clickhouse`

В 0.7.0 (последний релиз PyPI) нет переменной окружения для серверного TLS: `main()` вызывает `mcp.run(transport, host, port)` без параметров uvicorn. FastMCP 4 принимает `uvicorn_config`, а uvicorn — `ssl_certfile`/`ssl_keyfile`. Поэтому юнит запускает не консольный скрипт, а тонкий запускальщик `deploy/staging/mcp_clickhouse_tls.py`, который повторяет `main()` пакета и добавляет `uvicorn_config={"ssl_certfile", "ssl_keyfile"}` из `MCP_TLS_CERTFILE`/`MCP_TLS_KEYFILE`. Reverse proxy не вводится — это прямое требование владельца.

- Бинд: `0.0.0.0:8443`; `CLICKHOUSE_MCP_ALLOWED_HOSTS` — `mcp.selfinvesting.ru:8443` и loopback-алиасы (для smoke на хосте).
- Порт `8000` больше не слушается; проброс `8000` на роутере снимает владелец.
- Без файлов сертификата запускальщик падает на старте громко (не откатывается в plain HTTP).

### 3.2 Сертификат

Let's Encrypt, HTTP-01, `certbot --standalone` (пакет Ubuntu 24.04). Требование к сети: внешний `:80` проброшен на хост (делает владелец; нужен и для продления). `--deploy-hook` перезапускает `mcp-clickhouse`; продление — штатный `certbot.timer`. Файлы — `/etc/letsencrypt/live/mcp.selfinvesting.ru/{fullchain,privkey}.pem` (сервис работает от root).

### 3.3 Плагин

Сервер в манифестах называется `rosstat-staging` — не `clickhouse`, чтобы не конфликтовать с локальным stdio-сервером `.kimi-code/mcp.json` (снимает возражение спеки 5.7 §3.3). Транспорт — streamable HTTP, заголовок `Authorization: Bearer <ROSSTAT_MCP_TOKEN>`; синтаксис подстановки переменной — по документации каждого клиента. `TestLLMPluginManifestsNoSecrets` остаётся в силе: ключей `token`/`secret` в манифестах нет, значение — только ссылка на переменную.

### 3.4 Проверки

- `deploy-staging-smoke` ходит в `https://mcp.selfinvesting.ru:8443` с `--resolve` на `127.0.0.1` — проверяется и сертификат, и имя.
- `plugin_manifest_test.go`: у каждого манифеста объявлен `rosstat-staging` с `https://mcp.selfinvesting.ru:8443/mcp`, токен только как ссылка на `ROSSTAT_MCP_TOKEN`.
- Живая приёмка снаружи: `/health` 200 по валидному сертификату, без токена 401, с токеном — `initialize`, `tools/list`, `run_query` по `v_series_catalog`, `v_point_in_time`, `v_point_in_time_asof`; сырая `macro_series` и DDL — отказ.

## 4. Вне объёма

- Персональные токены и их ротация (сервер поддерживает один статический токен).
- Rate limit на MCP.
- Публикация Grafana/MLflow/dagu наружу.

## 5. Риски

- Продление сертификата зависит от проброса `:80`; если его снимут — сертификат истечёт через 90 дней. Smoke показывает срок действия.
- Запускальщик зависит от внутренних имён `mcp_clickhouse` (`mcp_server.mcp`, `get_mcp_config`, `setup_middleware`); при смене `MCP_VERSION` проверить `deploy-staging-smoke`.
