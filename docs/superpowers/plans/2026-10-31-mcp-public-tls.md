# План: `infra/5.9` — публичный MCP по TLS и подключение плагина

**Спека:** [2026-10-31-mcp-public-tls-design.md](../specs/2026-10-31-mcp-public-tls-design.md)
**Issue:** [#456](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/456)
**Ветка:** `feat/infra-5.9-mcp-public-tls` (worktree `.worktrees/mcp-public-tls`)

1. **Запускальщик TLS** — `deploy/staging/mcp_clickhouse_tls.py`; юнит `mcp-clickhouse.service` запускает его через `uv run --with mcp-clickhouse==$(MCP_VERSION)`, бинд `0.0.0.0:8443`, `MCP_TLS_CERTFILE`/`MCP_TLS_KEYFILE`.
2. **Makefile** — `deploy-staging-mcp` копирует запускальщик; новая цель `deploy-staging-mcp-cert` (certbot standalone + deploy-hook); `deploy-staging-smoke` — по HTTPS с `--resolve`, плюс срок сертификата.
3. **Env-пример** — `CLICKHOUSE_MCP_ALLOWED_HOSTS` с `mcp.selfinvesting.ru:8443`.
4. **Манифесты плагина** — `mcpServers.rosstat-staging` во всех пяти манифестах; тест в `plugin_manifest_test.go` (тест — до манифестов, сначала красный).
5. **Деплой на staging** — сертификат, юнит, правка `ALLOWED_HOSTS` в `/etc/clickhouse-import-rosstat.env`, перезапуск, `deploy-staging-smoke`.
6. **Внешняя приёмка** — с токеном пользователя (скрипт приёмки или клиент плагина).
7. **Документация** — `ARCHITECTURE.md` §6.5, `README.md`, `deploy/staging/README.md`, `docs/ROADMAP_INFRA.md` строка 5.9.
8. **`make all`**, PR с `Closes #456`, протокол приёмки комментарием в PR.

Действия владельца: проброс внешних `:80` и `:8443` на хост, снятие проброса `:8000`, выдача токена.
