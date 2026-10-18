# Деплой на staging (Фаза 1)

Артефакты деплоя на хост `palmshell` (Ubuntu 24.04, x86_64, вход `root`). Разворачивает четыре
компонента: бинарник `clickhouse-import-rosstat`, планировщик `dagu`, MCP-сервер
`mcp-clickhouse` (HTTP, read-only) и MLflow (SQLite + локальные артефакты). ClickHouse на хосте
**не трогается**; у Grafana — только
файл провижининга датасорса для дашбордов `gold-thesis` и `key-rate-publications` (раздел
[«Grafana на staging»](#grafana-на-staging-датасорс-провижининг-и-деплой-дашбордов)),
сам сервис не перезапускается.

Спецификация: [docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md](../../docs/superpowers/specs/2026-10-10-staging-deploy-phase1-design.md).

## Что где лежит на хосте

| Путь | Что |
|---|---|
| `/root/.local/bin/clickhouse-import-rosstat` | бинарник импортёра (запускают DAG'и) |
| `/root/.local/bin/dagu` | планировщик (+ `.bak` для отката) |
| `/etc/systemd/system/dagu.service` | юнит dagu (копия из этого каталога) |
| `/etc/systemd/system/mcp-clickhouse.service` | юнит MCP (копия из этого каталога) |
| `/etc/systemd/system/mlflow.service` | юнит MLflow (копия из этого каталога) |
| `/var/lib/mlflow/` | SQLite `mlflow.db` и артефакты MLflow (бэкап — см. «MLflow на staging») |
| `/etc/clickhouse-import-rosstat.env` | секреты, права `600`, **вне git** |
| `/root/dagu/` | DAG-файлы из `dagu/` репозитория |

В юнитах пути **абсолютные**: systemd не читает `.bashrc`, его `PATH` не содержит `.local/bin`.

## Порядок развёртывания

```bash
# 1. Создать файл секретов на хосте (один раз). Значения задаёт владелец.
ssh palmshell 'install -m 600 /dev/null /etc/clickhouse-import-rosstat.env'
# вписать переменные по образцу clickhouse-import-rosstat.env.example
# (включая GOST_APP_ID — токен gtn.access.appid портала Росстандарта: без него падает часовой шаг
#  gost_vehicle_safety_certificate из dagu/craw.yaml; режимные GOST_MODE/GOST_FROM/GOST_TO/
#  GOST_SETTLE_DAYS здесь не хранят — их задаёт сам DAG)
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

## После деплоя

**Одноразово: миграция таблицы домохозяйств.** На staging старое имя `households_b_mes` не
переименовано: рефакторинг `cbr/` пишет в `cbr_households_b_mes` (её читает витрина
`v_cbr_macro`), а таблица прежних прогонов осталась отдельной. Гейт тот же, что прошла
dev-приёмка 2026-10-13: сначала прогнать импорт домохозяйств новым бинарём, затем сверить
обе таблицы

```sql
SELECT 'old' AS t, count(), uniqExact(name), min(date), max(date) FROM households_b_mes FINAL
UNION ALL
SELECT 'new',      count(), uniqExact(name), min(date), max(date) FROM cbr_households_b_mes FINAL
```

и **только при совпадении** удалять старую (`DROP TABLE households_b_mes`). На dev тот же гейт
дал `7350 / 70 / 2018-01-01 / 2026-09-01`; на staging сверять с его собственной базовой линией —
снимком старой таблицы до деплоя (если выгрузка ЦБ с тех пор обновилась, у новой таблицы будет
больше точек, и сверять надо срез до `max(date)` старой). Запрос идёт под пользователем с
доступом к сырым таблицам — у `kimi_reader` его нет (ему выданы только витрины), — и не через
`make`: Makefile и SQL-файл на хост не попадают (то же ограничение, что у `make ch-sql` в
README репозитория).

## Обновление

```bash
make upgrade-staging     # dagu, mcp-clickhouse и mlflow, до версий из репозитория
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
MCP с токеном отвечает, MLflow `/health` = 200.

## MLflow на staging

Трекер экспериментов и реестр моделей (infra/4.2). Хранилище — SQLite
`sqlite:////var/lib/mlflow/mlflow.db` и каталог артефактов `/var/lib/mlflow/artifacts`,
который сервер проксирует: юнит запускает `mlflow server` с
`--artifacts-destination /var/lib/mlflow/artifacts --serve-artifacts` и **без**
`--default-artifact-root` (новый эксперимент получает корень `mlflow-artifacts:/`,
и upload идёт через сервер — это единственный корректный режим для клиента,
подключённого по SSH-туннелю). Если БД создана раньше со старым локальным
artifact root, у default experiment остался локальный `artifact_location` —
сбросьте/пересоздайте default experiment или заведите новый. `--workers 1`,
потому что у SQLite один писатель. Сервер слушает только `127.0.0.1:5000`,
**аутентификации нет** — наружу порт не выставляется, доступ только через ssh-туннель.

```bash
# туннель; UI и REST API — http://localhost:5000
ssh -N -L 5000:127.0.0.1:5000 palmshell

# состояние и логи
ssh palmshell 'systemctl status mlflow'
ssh palmshell 'journalctl -u mlflow -n 50 --no-pager'
```

Бэкап (обязателен до обновления версии). `.backup` — корректный онлайн-бэкап SQLite:
копирование файла живой БД `cp`/`scp` может дать битую копию.

```bash
STAMP=$(date +%F)   # одна метка на оба бэкапа: часы локальной машины и хоста могут расходиться
ssh palmshell "sqlite3 /var/lib/mlflow/mlflow.db \".backup '/root/mlflow-$STAMP.db'\""
ssh palmshell "tar czf /root/mlflow-artifacts-$STAMP.tgz -C /var/lib/mlflow artifacts"
```

Обновление версии: бэкап → поменять версию **в двух местах** — `ExecStart` в
`deploy/staging/mlflow.service` и `MLFLOW_VERSION` в Makefile (они обязаны совпадать;
`make deploy-staging-mlflow` перед `scp` проверяет это `grep`-ом и падает при
расхождении) → `make deploy-staging-mlflow` (он копирует юнит и греет кэш uv, но
**не перезапускает** уже работающий сервис) → остановить сервис, выполнить миграцию
на остановленной БД, запустить сервис заново и прогнать smoke:

```bash
make deploy-staging-mlflow
ssh palmshell 'systemctl stop mlflow'
ssh palmshell '/root/.local/bin/uv run --with mlflow==<версия> --python 3.12 \
  mlflow db upgrade sqlite:////var/lib/mlflow/mlflow.db'
ssh palmshell 'systemctl start mlflow'
make deploy-staging-smoke
```

`systemctl enable --now` (его выполняет `deploy-staging`) не перезапускает уже запущенный
сервис: без stop/start новый юнит не вступит в силу, а `mlflow db upgrade` под живым сервером
трогает ту же SQLite, которой владеет процесс (один писатель). `make upgrade-staging`
для смены версии MLflow **недостаточно** — он только копирует юнит и перезапускает сервис;
всегда используйте последовательность stop → `db upgrade` → start выше.

Dev-сервер (`make mlflow-up`) хранит БД и артефакты в `build/mlflow/`; staging и dev
независимы — переносить данные между ними не нужно.

## Grafana на staging: датасорс-провижининг и деплой дашбордов

Все дашборды контура — `gold-thesis`, `key-rate-publications` и `finance-polyus` — едут на staging как код: артефакты
`dashboard/<uid>.json` пушутся в Grafana через ssh-туннель, а датасорс описывается шаблоном
`grafana-datasource.yaml` в этом каталоге.

```bash
# 1. Туннель к Grafana хоста (:3001, чтобы не конфликтовать с локальным стендом на :3000)
ssh -N -L 3001:127.0.0.1:3000 palmshell &

# 2. Токен сервис-аккаунта Editor на staging — в ~/.config/rosstat/env, в репозиторий не попадает:
#      GRAFANA_STAGING_TOKEN=<токен>

# 3. Провижининг датасорса → push всех дашбордов → smoke панелей
make grafana-deploy-staging
```

Цель по шагам: проверяет непустой `GRAFANA_STAGING_TOKEN` → проверяет туннель
(`curl $(GRAFANA_STAGING_URL)/api/health`, `GRAFANA_STAGING_URL ?= http://localhost:3001`) →
идемпотентный провижининг датасорса → цикл по `DASHBOARDS` (`gold-thesis`, `key-rate-publications`, `finance-polyus`):
`gcx resources push dashboards --path dashboard/<uid>.json`
против `GRAFANA_SERVER=$(GRAFANA_STAGING_URL)` → smoke `python3 scripts/grafana_check.py --uid <uid>`
(дашборд виден по uid + первые панели отдают точки). Цикл останавливается на первой ошибке.
Grafana и systemd на хосте цель **не трогает**: только `scp` файла и HTTP-запросы через туннель.

### Датасорс

- Шаблон: `grafana-datasource.yaml` — `host: localhost`, `port: 8123`, `protocol: http`,
  пользователь `kimi_reader` (тот же readonly-пользователь, что у MCP), uid `ch-rosstat`
  (совпадает с `grafana/clickhouse.go` и с таргетами панелей дашборда).
- Пароля в репозитории нет: плейсхолдер `__CLICKHOUSE_PASSWORD__` подставляет **на хосте**
  `make grafana-provision-staging` из `/etc/clickhouse-import-rosstat.env` (600). Подстановка —
  через `python3`, а не `sed`: пароль может содержать `/ & \` и другие спецсимволы. Скаляр в шаблоне
  стоит в двойных кавычках, поэтому `\` и `"` экранируются перед подстановкой (иначе `a\b"c`
  делает YAML невалидным, а `x\\y` молча превращается в `x\y`); пароль с управляющим символом
  (перевод строки, таб, `0x7f`) отвергается с понятной ошибкой, а собранный файл перечитывается и
  значение датасорса декодируется обратно и сверяется с паролем (round-trip). Временные копии
  (root, `600`) удаляются сразу, значение пароля не печатается.
- Идемпотентность: sha256 собранного файла сравнивается с
  `/etc/grafana/provisioning/datasources/clickhouse.yaml`; установка через
  `install -o root -g grafana -m 640` выполняется только при отличии. Повторный прогон печатает
  `OK   провижининг датасорса актуален` и файл не трогает.
- Проверить, что датасорс реально авторизуется в ClickHouse, можно так (пароль не печатается):

  ```bash
  ssh palmshell 'pw=$(grep "^ *password:" /etc/grafana/provisioning/datasources/clickhouse.yaml | cut -d\" -f2); \
    printf "header = \"X-ClickHouse-User: kimi_reader\"\nheader = \"X-ClickHouse-Key: %s\"\n" "$pw" | \
    curl -s -K - http://localhost:8123/ --data-binary "SELECT currentUser()"'
  # → kimi_reader
  ```

- **Ловушка (проверено на Grafana 13.1.1):** каталог `provisioning/datasources` читается
  **только при старте** Grafana — поллинг есть лишь у файловых дашбордов
  (`pkg/services/provisioning`: `ProvisionDatasources` вызывается из `starting()`). Поэтому после
  первой установки/правки файла датасорс сам не появится: до тех пор, пока владелец не выполнит
  admin-reload (без перезапуска сервиса) — или не перезапустит `grafana-server`:

  ```bash
  # через уже поднятый туннель; нужен admin-доступ к Grafana (basic auth)
  curl -u admin:<пароль-админа> -X POST http://localhost:3001/api/admin/provisioning/datasources/reload
  # → {"message":"Datasources config reloaded"}
  ```

  До reload датасорса `ch-rosstat` в Grafana нет, и smoke падает на `POST /api/ds/query`
  («datasource not found») — это ожидаемо и лечится одним reload, перезапуск всей Grafana не нужен.

### Что требуется от владельца (одноразово)

- Сервис-аккаунт Grafana на staging (роль **Editor** — нужен push дашбордов) и его токен →
  `GRAFANA_STAGING_TOKEN` в `~/.config/rosstat/env`. На самом хосте токен не нужен: push идёт через туннель.
- admin-reload провижининга датасорса (см. выше) — при первом деплое и после каждой правки
  `grafana-datasource.yaml`. Цель печатает напоминание, когда файл был переустановлен.
- Пользователь ClickHouse `kimi_reader` уже есть на хосте (гранты readonly на витрины `v_*`).

Приёмка полного прогона: `make grafana-deploy-staging` → по каждому uid из `DASHBOARDS`
(`gold-thesis`, `key-rate-publications`, `finance-polyus`) `OK   дашборд виден` и непустые точки по первым панелям,
затем `gcx dashboards list` показывает все дашборды из `DASHBOARDS` (существующие на staging не затронуты).

## Отладка и откат

```bash
ssh palmshell 'journalctl -u dagu -n 50 --no-pager'
ssh palmshell 'journalctl -u mcp-clickhouse -n 50 --no-pager'
ssh palmshell 'journalctl -u mlflow -n 50 --no-pager'

# откат dagu на предыдущую версию
ssh palmshell 'mv /root/.local/bin/dagu.bak /root/.local/bin/dagu && systemctl restart dagu'

# откат mcp-clickhouse: вернуть прежний MCP_VERSION в Makefile и повторить make deploy-staging-mcp

# откат mlflow: бэкап (см. «MLflow на staging»), вернуть прежнюю версию в юните и Makefile,
# повторить make deploy-staging-mlflow; откат схемы SQLite не автоматизирован
```

## Что осознанно не делается

Юнит `clickhouse-import-rosstat.service` не создаётся: импортёр — короткоживущий процесс,
его запускают DAG'и. Публикация наружу (Caddy, TLS) — следующая фаза; сейчас все сервисы
слушают только `127.0.0.1`.
