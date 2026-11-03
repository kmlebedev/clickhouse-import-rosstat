# Grafana MCP: локальный stdio и loopback на staging

**Дата:** 2026-11-03
**Пункт:** `infra/5.13`
**Роль:** Go-инженер (Makefile, deploy, docs)
**Статус:** In Review после открытия PR

## 1. Задача

Дать агенту read-only доступ к Grafana через официальный MCP-сервер
[`grafana/mcp-grafana`](https://github.com/grafana/mcp-grafana), по той же схеме, что уже
работает для ClickHouse MCP: локально — stdio, на staging — systemd-сервис.

Grafana MCP нужен, чтобы агент видел дашборды, datasources, alert rules и аннотации
в тех же терминах, в которых они существуют на стенде, а не только SQL-витрины ClickHouse.

## 2. Решения

1. **Один пин версии** `GRAFANA_MCP_VERSION=2.2.0` в Makefile; та же версия в
   `deploy/staging/mcp-grafana.service` и в `.kimi-code/mcp.json`. Расхождение версии
   в юните и Makefile — ошибка деплоя (`deploy-staging-mcp-grafana`).
2. **Локально — stdio.** Сервер `grafana` в `.kimi-code/mcp.json` запускается через
   `uv tool run --from mcp-grafana==2.2.0 mcp-grafana` с `--disable-write` и
   `--usage-stats=disabled`. Токен берётся из `~/.config/rosstat/env`:
   `GRAFANA_MCP_TOKEN`, при его отсутствии — `GRAFANA_TEST_TOKEN`; команда перекладывает
   его в `GRAFANA_SERVICE_ACCOUNT_TOKEN`.
3. **На staging — только loopback.** `mcp-grafana` слушает `127.0.0.1:8000` (streamable HTTP);
   наружу порт не публикуется. Доступ — через ssh-туннель `-L 8001:127.0.0.1:8000`.
   TLS и reverse proxy не нужны: у `mcp-grafana` нет TLS-листенера, а Grafana на staging
   и так слушает только `127.0.0.1:3000`.
4. **Host-проверка.** `--allowed-hosts` разрешает loopback `:8000` и туннельный `:8001`
   (`127.0.0.1`, `localhost`, `[::1]`). Без этого клиент через туннель получает 403:
   Host-заголовок содержит локальный порт туннеля, а не порт сервера.
5. **Токен — Admin.** На Grafana 13 дашборды, залитые через `gcx`/k8s API, доступны только
   роли **Admin**; Viewer и Editor получают `403`, а `search_dashboards` возвращает пустой
   список. Проверено на чистой Grafana 13.2.3: dashboard, созданный через legacy API,
   Viewer читает; созданный через `gcx` — нет; Admin читает оба. Поэтому на staging
   используется отдельный сервис-аккаунт `mcp-grafana-admin` (роль Admin), а не
   `GRAFANA_STAGING_TOKEN` (Editor). `--disable-write` оставляет MCP read-only на уровне
   инструментов.
6. **Телеметрия выключена.** `--usage-stats=disabled` — анонимная статистика
   `mcp-grafana` не отправляется.
7. **`make deploy`** — алиас `deploy-staging`: единственный стенд проекта.

## 3. Компоненты

| Компонент | Файл |
|---|---|
| Локальный клиент | `.kimi-code/mcp.json`, сервер `grafana` |
| Make-цели | `Makefile`: `grafana-mcp-run`, `grafana-mcp-check`, `deploy-staging-mcp-grafana`, `deploy` |
| staging-юнит | `deploy/staging/mcp-grafana.service` |
| Обёртка запуска | `deploy/staging/mcp_grafana_launch.sh` |
| Smoke | `scripts/grafana_mcp_check.py` |
| Env-пример | `deploy/staging/clickhouse-import-rosstat.env.example` (`GRAFANA_MCP_TOKEN`) |
| Документация | `deploy/staging/README.md`, `ARCHITECTURE.md` §6.5, `docs/GRAFANA.md`, `docs/CONFIGURATION.md`, `docs/DEVELOPMENT.md`, `docs/LIMITATIONS.md`, `README.md`, `AGENTS.md` |

## 4. Поток

```text
Kimi / stdio ── mcp-grafana --disable-write ── Grafana API ── datasource ch-rosstat ── ClickHouse
ssh -L 8001:127.0.0.1:8000 ── mcp-grafana (staging, loopback) ── Grafana 127.0.0.1:3000
```

- Локально сервер запускает сам клиент (stdio), отдельный порт не открывается.
- На staging сервис постоянно работает под systemd (`Restart=always`), включается
  `deploy-staging` вместе с `dagu`, `mcp-clickhouse` и `mlflow`.
- `deploy-staging-mcp-grafana` копирует юнит и обёртку, проверяет версию/адрес/Host-allowlist,
  непустой `GRAFANA_MCP_TOKEN` и доступность Grafana, прогревает кэш uv.
- `deploy-staging-smoke` проверяет `systemctl is-active mcp-grafana` и `/healthz` на loopback.
- `make grafana-mcp-check GRAFANA_MCP_URL=http://localhost:8001/mcp` — tool-level smoke
  через туннель: инструменты, отсутствие `update_dashboard`, `ch-rosstat`, `gold-thesis`,
  сводка по uid.

## 5. Безопасность

- Публичного HTTPS-эндпоинта у Grafana MCP нет; порт `8000` не пробрасывается.
- `GRAFANA_MCP_TOKEN` — секрет, живёт только в `/etc/clickhouse-import-rosstat.env` (600);
  в репозитории — `REPLACE_ME`, в логах и argv не печатается.
- MCP-сервер не получает доступ к ClickHouse напрямую: только к Grafana API. SQL-инструменты
  `mcp-grafana` по умолчанию выключены; `--disable-write` убирает write-инструменты.
- Admin-токен выбран вынужденно: Viewer/Editor не видят gcx-дашборды. Область риска
  ограничена loopback-сервисом и MCP-инструментами без записи; ротация токена — обновить
  env-файл и `systemctl restart mcp-grafana`.

## 6. Проверка

- `make all` — зелёный (Go-код не менялся, но Makefile/доки затронуты).
- `make docs-check` — зелёный.
- `python3 -m py_compile scripts/grafana_mcp_check.py`, `sh -n deploy/staging/mcp_grafana_launch.sh`,
  `python3 -m json.tool .kimi-code/mcp.json`, `git diff --check` — без ошибок.
- `make -n deploy`, `make -n deploy-staging`, `make -n grafana-mcp-check`,
  `make -n deploy-staging-mcp-grafana` — без ошибок.
- Локально: `make grafana-up` + `make grafana-mcp-check` → `ПРОВЕРКА ПРОЙДЕНА`
  (63 инструмента, `update_dashboard` отсутствует, `ch-rosstat`, `gold-thesis`, сводка).
- Staging (owner-assisted): `GRAFANA_MCP_TOKEN` = Admin-токен `mcp-grafana-admin`;
  `make deploy-staging`; `make grafana-mcp-check GRAFANA_MCP_URL=http://localhost:8001/mcp` →
  `ПРОВЕРКА ПРОЙДЕНА`.

## 7. Вне объёма

- Публичный HTTPS-эндпоинт и reverse proxy для Grafana MCP.
- Кастомные RBAC-роли Grafana и выдача Viewer/Editor прав на gcx-дашборды.
- Изменения Go-кода, DDL, витрин, грантов ClickHouse и `dagu/*`.
- Плагин для внешних LLM-клиентов: staging-сервер доступен только через ssh-туннель.

## 8. Риски

| Риск | Смягчение |
|---|---|
| Admin-токен шире необходимого | Только loopback, `--disable-write`, отдельный аккаунт `mcp-grafana-admin`, ротация токена |
| Версия `mcp-grafana` расходится в трёх местах | `deploy-staging-mcp-grafana` сверяет юнит с Makefile; `.kimi-code/mcp.json` пинится той же версией |
| Grafana 13 не отдаёт gcx-дашборды не-Admin | Документировано; smoke печатает подсказку при 403; используется Admin-токен |
| Порт туннеля не совпадает с `--allowed-hosts` | `GRAFANA_MCP_TUNNEL_PORT=8001` и явный список Host в юните/обёртке |

## 9. Ссылки

- Grafana MCP docs: <https://grafana.com/docs/grafana/latest/ai-tools/mcp/>
- Репозиторий: <https://github.com/grafana/mcp-grafana>
- План: [2026-11-03-grafana-mcp.md](../plans/2026-11-03-grafana-mcp.md)
