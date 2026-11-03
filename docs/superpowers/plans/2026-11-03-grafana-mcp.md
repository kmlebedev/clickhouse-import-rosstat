# План: Grafana MCP (`infra/5.13`)

**Спека:** [2026-11-03-grafana-mcp-design.md](../specs/2026-11-03-grafana-mcp-design.md)
**Ветка:** `feat/grafana-mcp`
**Роль:** Go-инженер

## Task 1. Спека, план и роадмап

- [x] Спека `docs/superpowers/specs/2026-11-03-grafana-mcp-design.md`.
- [x] План `docs/superpowers/plans/2026-11-03-grafana-mcp.md`.
- [x] Пункт `5.13` в `docs/ROADMAP_INFRA.md`; схема ID infra в `docs/ROADMAP_INDEX.md`.
- [x] Issue `[infra/5.13] Grafana MCP: локальный stdio и loopback на staging`.

## Task 2. Локальный MCP

- [x] Версия `GRAFANA_MCP_VERSION=2.2.0`, порт 8000, туннель 8001 в Makefile.
- [x] Сервер `grafana` в `.kimi-code/mcp.json` (stdio, `--disable-write`,
      `--usage-stats=disabled`, `GRAFANA_MCP_TOKEN` или `GRAFANA_TEST_TOKEN`).
- [x] Цели `grafana-mcp-run`, `grafana-mcp-check`; `GRAFANA_MCP_TOKEN` в `env-check`.
- [x] `scripts/grafana_mcp_check.py`: stdio и streamable HTTP; инструменты, write-guard,
      datasource, dashboard, сводка; подсказка при 403.

## Task 3. Staging-сервис

- [x] `deploy/staging/mcp-grafana.service` — loopback `127.0.0.1:8000`,
      `--allowed-hosts` для `:8000`/`:8001`, версия в `Environment=`.
- [x] `deploy/staging/mcp_grafana_launch.sh` — `GRAFANA_MCP_TOKEN` →
      `GRAFANA_SERVICE_ACCOUNT_TOKEN`, `--disable-write`, `--usage-stats=disabled`.
- [x] `deploy-staging-mcp-grafana`: проверки версии/адреса/Host-allowlist, токена и Grafana,
      копирование юнита/обёртки, прогрев uv.
- [x] `deploy-staging`, `deploy-staging-preflight`, `deploy-staging-smoke`,
      `upgrade-staging` включают `mcp-grafana`.
- [x] `make deploy` — алиас `deploy-staging`.

## Task 4. Токен

- [x] Env-пример: `GRAFANA_MCP_TOKEN=REPLACE_ME` (Admin-токен `mcp-grafana-admin`).
- [x] Документировано: Viewer/Editor не видят gcx-дашборды (403 на Grafana 13);
      Admin видит; `--disable-write` оставляет MCP read-only.

## Task 5. Документация

- [x] `deploy/staging/README.md`: компонент, пути, раздел «Grafana MCP на staging»,
      smoke, troubleshooting 403, откат.
- [x] `ARCHITECTURE.md` §6.5, `README.md`, `docs/CONFIGURATION.md`,
      `docs/DEVELOPMENT.md`, `docs/GRAFANA.md`, `docs/LIMITATIONS.md`, `AGENTS.md`.

## Task 6. Проверка

- [x] `make all` — зелёный.
- [x] `make docs-check` — зелёный.
- [x] `python3 -m py_compile scripts/grafana_mcp_check.py`; `sh -n` обёртки;
      `python3 -m json.tool .kimi-code/mcp.json`; `git diff --check`.
- [x] `make -n deploy`, `make -n deploy-staging`, `make -n grafana-mcp-check`,
      `make -n deploy-staging-mcp-grafana`.
- [x] Локально: `make grafana-up` + `make grafana-mcp-check` → `ПРОВЕРКА ПРОЙДЕНА`.
- [x] Staging (owner-assisted): `GRAFANA_MCP_TOKEN` = Admin-токен `mcp-grafana-admin`;
      `make deploy-staging`; `make grafana-mcp-check GRAFANA_MCP_URL=http://localhost:8001/mcp`
      → `ПРОВЕРКА ПРОЙДЕНА` (подтверждено владельцем).

## Откат

- Вернуть прежний `GRAFANA_MCP_VERSION` в Makefile и юните,
  `make deploy-staging-mcp-grafana && ssh palmshell 'systemctl restart mcp-grafana'`.
- Полное отключение: `ssh palmshell 'systemctl disable --now mcp-grafana'`.
- Смена токена: обновить `GRAFANA_MCP_TOKEN` в `/etc/clickhouse-import-rosstat.env`,
  `systemctl restart mcp-grafana`.

## Вне объёма

Публичный HTTPS/reverse proxy, кастомные RBAC-роли, изменения Go-кода/DDL/витрин/грантов
ClickHouse и `dagu/*`.
