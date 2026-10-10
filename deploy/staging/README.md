# Деплой на staging (Фаза 1)

Артефакты деплоя на хост `palmshell` (Ubuntu 24.04, x86_64, вход `root`). Разворачивает три
компонента: бинарник `clickhouse-import-rosstat`, планировщик `dagu`, MCP-сервер
`mcp-clickhouse` (HTTP, read-only). ClickHouse и Grafana на хосте **не трогаются**.

Спецификация: [docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md](../../docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md).

## Что где лежит на хосте

| Путь | Что |
|---|---|
| `/root/.local/bin/clickhouse-import-rosstat` | бинарник импортёра (запускают DAG'и) |
| `/root/.local/bin/dagu` | планировщик (+ `.bak` для отката) |
| `/etc/systemd/system/dagu.service` | юнит dagu (копия из этого каталога) |
| `/etc/systemd/system/mcp-clickhouse.service` | юнит MCP (копия из этого каталога) |
| `/etc/clickhouse-import-rosstat.env` | секреты, права `600`, **вне git** |
| `/root/dagu/` | DAG-файлы из `dagu/` репозитория |

В юнитах пути **абсолютные**: systemd не читает `.bashrc`, его `PATH` не содержит `.local/bin`.

## Порядок развёртывания

```bash
# 1. Создать файл секретов на хосте (один раз). Значения задаёт владелец.
ssh palmshell 'install -m 600 /dev/null /etc/clickhouse-import-rosstat.env'
# вписать три переменные по образцу clickhouse-import-rosstat.env.example
# токен: openssl rand -hex 32 — ОБЯЗАТЕЛЕН, без него mcp-clickhouse не стартует

# 2. Развернуть
make deploy-staging

# 3. Создать пользователя kimi_reader (если ещё нет)
CH_SETUP_PASSWORD=<пароль default> make mcp-user-staging
```

`deploy-staging` **падает**, если env-файла нет, и никогда его не создаёт: секреты не должны
рождаться в деплой-скрипте.

## Что произойдёт после деплоя

**`dagu` начнёт выполнять DAG'и по расписанию при первом совпадении времени** — это реальные
запросы к внешним источникам (ЦБ, Росстат, FRED, BLS, BEA, MOEX, ФАО, ФТС, Минфин, банки).
Расписания — в `dagu/*.yaml`. Если часть источников требует ключей, которых на хосте нет,
соответствующие DAG'и упадут; ошибки видны в `journalctl -u dagu`.

## Обновление

```bash
make upgrade-staging     # только dagu и mcp-clickhouse, до версий из репозитория
```

`apt upgrade` сюда **не входит**: ClickHouse (данные) и Grafana обновляются вручную.

## Проверка

```bash
# через туннель — MCP end-to-end из репозитория
ssh -N -L 8123:127.0.0.1:8123 palmshell &
CLICKHOUSE_PASSWORD=<пароль kimi_reader> make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=8123
```

Smoke-проверки внутри `deploy-staging`: наличие бинарей и DAG-файлов, непустой токен (до
включения сервисов), затем `dagu` отвечает, MCP `/health` = 200, MCP **без токена → 401**,
MCP с токеном отвечает.

## Отладка и откат

```bash
ssh palmshell 'journalctl -u dagu -n 50 --no-pager'
ssh palmshell 'journalctl -u mcp-clickhouse -n 50 --no-pager'

# откат dagu на предыдущую версию
ssh palmshell 'mv /root/.local/bin/dagu.bak /root/.local/bin/dagu && systemctl restart dagu'

# откат mcp-clickhouse: вернуть прежний MCP_VERSION в Makefile и повторить make deploy-staging-mcp
```

## Что осознанно не делается

Юнит `clickhouse-import-rosstat.service` не создаётся: импортёр — короткоживущий процесс,
его запускают DAG'и. Публикация наружу (Caddy, TLS) — следующая фаза; сейчас оба сервиса
слушают только `127.0.0.1`.
