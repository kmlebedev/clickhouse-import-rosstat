# Grafana

> Часть документации проекта; входная точка — [README.md](../README.md).

Установка Grafana 11.2.0 (Debian/Ubuntu, arm64):

```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_11.2.0_arm64.deb
sudo dpkg -i grafana_11.2.0_arm64.deb
```

Дашборды контура — **`gold-thesis`** (статья про золото), **`key-rate-publications`** (иллюстрации к публикациям по ключевой ставке) и **`finance-polyus`** («Полюс (PLZL) — финансы и прогноз»: графики статьи PLZL) — живут как код, а не как экспорт из UI. Конвейер:

```text
grafana/ (Go, grafana-foundation-sdk) --make grafana-build--> dashboard/<uid>.json
                                                              (gold-thesis, key-rate-publications, finance-polyus; все коммитятся)
        |                                                              |
   make grafana-validate                                       make grafana-deploy-test / -staging (gcx resources push всех трёх)
   (go vet + тесты)                                                |
   CI dashboards.yml: тесты, сборка,                           make grafana-check
   diff-проверка артефактов, gcx dev lint                 (smoke: uid + точки панелей, по всем трём)
```

Легаси-экспорты `dashboard/rosstat.json` и `dashboard/chmf.json` остаются как есть — их миграция на код (процесс — скил `.kimi-code/skills/dashboard-migration/SKILL.md`, живая очередь — [docs/ROADMAP_INFRA.md](ROADMAP_INFRA.md), Фаза 2), алерты Grafana и PNG-рендер панелей для статей — отдельные пункты вне объёма этой работы. Прежний легаси-экспорт `dashboard/finance-polyus.json` заменён генерируемым артефактом 2026-10-16 (история — в git).

### Дашборд как код

- Источник истины — `grafana/` (отдельный Go-модуль): дашборды `gold-thesis` (9 панелей id 1–9 в 5 рядах), `key-rate-publications` (19 панелей id 1–19 в 4 рядах, «Ключевая ставка — публикации») и `finance-polyus` (19 панелей id 1–19 в 4 рядах id 31–34, «Полюс (PLZL) — финансы и прогноз»); все запросы — только по витринам `v_*`, датасорс каждой панели — константа uid `ch-rosstat`. CLI — флаги `-dashboard <uid>|all` (дефолт `all`) и `-out-dir` (дефолт `../dashboard`): файл артефакта — `<out-dir>/<uid>.json`. Артефакты `dashboard/gold-thesis.json`, `dashboard/key-rate-publications.json` и `dashboard/finance-polyus.json` (k8s-ресурс `dashboard.grafana.app/v1beta1`, uid = `metadata.name`) руками не правятся: `make grafana-build` перезаписывает их детерминированно, а CI падает, если артефакт разошёлся с кодом.
- `make grafana-validate` — `go vet` + тесты модуля (состав панелей всех трёх дашбордов и негативные правила `Validate`). Серверный `gcx resources validate` намеренно не входит в цель: он проверяет ресурс на живом инстансе (server-side dry-run) и требует кредов — рабочая форма записана в комментарии цели.
- Локальный стенд: `make grafana-up` (brew-Grafana; данные, логи и плагины в `$(GF_DIR)`, провижининг датасорса — `grafana/provisioning/datasources/clickhouse.yaml`, порт 3000) и `make grafana-down`. При первом входе в UI смените пароль `admin/admin`.

### Переменные окружения стендов

| Переменная | Назначение |
|---|---|
| `GRAFANA_TEST_URL` | адрес тест-стенда, по умолчанию `http://localhost:3000` |
| `GRAFANA_TEST_TOKEN` | токен сервис-аккаунта **Editor** на тест-стенде: `make grafana-deploy-test`, `make grafana-check` (в `gcx` уходит как `GRAFANA_TOKEN` — make-цели подставляют его сами; при ручных вызовах `gcx` подставляйте явно, см. «Цикл разработки и деплой») |
| `GRAFANA_STAGING_URL` | адрес staging, по умолчанию `http://localhost:3001` — **только через ssh-туннель** |
| `GRAFANA_STAGING_TOKEN` | токен сервис-аккаунта **Editor** на staging: `make grafana-deploy-staging` |
| `GRAFANA_ORG_ID` | org-id инстанса для `gcx` (on-prem без него отвергается), по умолчанию `1` |

Значения — только в `~/.config/rosstat/env`; `make env-check` печатает «задан/не задан» без значений. Версии: `gcx` 1.5.0 (`brew install gcx`), локальный стенд — Grafana 13.2.3 + плагин `grafana-clickhouse-datasource` 4.3.2 (ставится `make grafana-up`), staging — Grafana 13.1.1.

### Одноразовая настройка (владелец)

1. `brew install gcx`.
2. Локальный стенд: `make grafana-up` → войти `admin/admin` (первый запуск) → **Administration → Users and access → Service accounts** → аккаунт с ролью **Editor** → **Add service account token** → значение в `~/.config/rosstat/env` как `GRAFANA_TEST_TOKEN`; то же одной командой — `make grafana-test-token` (создаёт аккаунт `dashboards-cd` роли Editor и печатает строку для env-файла).
3. Staging: то же в UI staging (через туннель) → токен как `GRAFANA_STAGING_TOKEN`.

Токены не попадают в репозиторий, CI и логи (`gcx` получает их из окружения, а не аргументом командной строки).

### Цикл разработки и деплой

```bash
make ch-up && make grafana-up           # ClickHouse + локальный стенд с провижинингом датасорса
make grafana-build && make grafana-validate
make grafana-deploy-test                # gcx resources push всех трёх дашбордов на $(GRAFANA_TEST_URL) + smoke
make grafana-check                      # только smoke (все три uid)
```

Push идемпотентен: повторный прогон обновляет тот же ресурс (`metadata.name=<uid>` — uid дашборда из списка `DASHBOARDS`), дубль не создаётся. Перед публикацией — серверная проверка ресурса на живом инстансе (шаг ручной, нужны креды; по файлу на дашборд — `dashboard/<uid>.json`):

```bash
set -a; . ~/.config/rosstat/env; set +a             # источник значений; ничего не печатает
# если в env-файле нет адреса/токена — подставьте их: GRAFANA_TEST_URL=http://localhost:3000, GRAFANA_TEST_TOKEN=<токен Editor>
GRAFANA_SERVER="$GRAFANA_TEST_URL" GRAFANA_TOKEN="$GRAFANA_TEST_TOKEN" GRAFANA_ORG_ID=1 \
  gcx resources validate dashboards --path dashboard/gold-thesis.json   # пример вывода: ✔ No errors found
```

**Ручные вызовы `gcx`.** `gcx` читает креды только из `GRAFANA_SERVER`/`GRAFANA_TOKEN`/`GRAFANA_ORG_ID`; переменную `GRAFANA_TEST_TOKEN` он не знает — make-цели подставляют её в `GRAFANA_TOKEN` сами. Запуская `gcx` руками, подставляйте `GRAFANA_TOKEN` явно (как в примере выше), иначе push падает с `401 Unauthorized: the server has asked for the client to provide credentials` — это не признак недействительного токена.

Staging — через туннель `:3001` (порт не конфликтует с локальным стендом `:3000`):

```bash
ssh -N -L 3001:127.0.0.1:3000 palmshell &
make grafana-deploy-staging             # провижининг датасорса → push дашборда → smoke
```

**Ловушка staging:** Grafana (проверено на 13.1.1) применяет `provisioning/datasources` **только при старте** — сразу после первого `make grafana-provision-staging` датасорса `ch-rosstat` в инстансе ещё нет, и smoke упадёт. Активировать без рестарта сервиса:

```bash
curl -u admin:<пароль> -X POST http://localhost:3001/api/admin/provisioning/datasources/reload
```

(либо один `systemctl restart grafana-server`). Цель Grafana не перезапускает и сама печатает эту команду; подробности — `deploy/staging/README.md`.

### Алерты live-мониторинга ML (`infra/4.3`)

`grafana/provisioning/alerting/ml-live.yaml` — два правила Grafana Alerting:

| UID | Условие | Порог |
|---|---|---|
| `ml-live-degradation` | rolling MAE трёх последних разрешённых production-заседаний по `v_ml_live_accuracy` | > 100 б.п. |
| `ml-live-freshness` | последний прошедший production-прогноз не разрешён (`is_resolved=1`) | > 3 дней |

Оба — `severity=warning`, `contour=ml-live`, `for: 5m`; contact point не задаётся (алерт виден и уходит в Firing в UI, маршрутизация уведомлений — решение владельца). Оба запроса исключают `model='gate-selftest'`: синтетические строки приёмки не поднимают production-алерт. Локальный стенд читает каталог напрямую: `make grafana-up` передаёт `GF_PATHS_PROVISIONING=$(CURDIR)/grafana/provisioning`. Staging получает файл через `make grafana-provision-alerting-staging` (вызывается из `grafana-deploy-staging`); после копирования нужен admin-reload `POST /api/admin/provisioning/alert-rules/reload` или рестарт `grafana-server`. Smoke `make grafana-check` после дашбордов проверяет оба UID через `scripts/grafana_alert_check.py`. Офлайн-контракт YAML — `views/ml_live_alert_test.go` (входит в `make grafana-validate` и `make all`).

Проверка firing на локальном стенде: временно понизить порог `ml-live-degradation` через API/UI, дождаться состояния Firing, вернуть 100; в протоколе PR фиксируются UID, порог и наблюдённое состояние. Прод-данные для этого не нужны.

### Проверки и приёмка (gold-thesis — 2026-10-12; key-rate-publications — 2026-10-16; finance-polyus — v1 2026-10-16; живой деплой + smoke трёх uid на локальном стенде — 2026-10-16)

**Проверено на стенде/локально (пункты с пометкой «по отчёту исполнителя» приводятся со слов исполнителя задачи и при финальном ревью повторно не проверялись):**

- **Grafana-smoke на локальном стенде (13.2.3).** Дашборд виден по uid, панель 1 «Цена золота и пол $4 000» вернула **250 точек** — на тот момент токена сервис-аккаунта у владельца ещё не было, и запрос шёл через Grafana (`/api/ds/query`) временной обёрткой с basic-auth стенда.
- **SQL-проверка через MCP — отдельная проверка, не то же самое.** То же окно дашборда (`now-1y`) в `v_gold_prices` под `kimi_reader` через MCP даёт `SELECT count()` = **250** (250 — скользящее окно; за календарный 2025 — 252) — Grafana и агент читают одни данные, но это два разных канала (Grafana-запрос и MCP-запрос).
- `make grafana-build` — детерминирован (повторный прогон — пустой `git diff`); `make grafana-validate`, `make all`, `make mcp-check` — зелёные **по отчёту исполнителя задачи; повторный прогон при финальном ревью ветки не выполнялся**.
- `gcx resources push` идемпотентен (повторный push не создаёт дубль, растёт только `version`); серверная `gcx resources validate` на стенде отдала `✔ No errors found` — **по отчёту исполнителя задачи**.
- Провижининг датасорса применён на staging и идемпотентен: повторный прогон печатает «актуален», sha256/mtime/inode не меняются; в репозитории лежит только шаблон с плейсхолдером, пароль подставляется на хосте — **по отчёту исполнителя; полный live-прогон с токеном — owner-assisted**.
- **Живой прогон цели с Bearer-токеном (после того, как владелец задал `GRAFANA_TEST_TOKEN`).** `make grafana-deploy-test` — `✔ 1 resources pushed, 0 errors`; smoke `make grafana-check` — «ПРОВЕРКА ПРОЙДЕНА»: дашборд виден (версия 8), 9 панелей с таргетами, панель 1 «Цена золота и пол $4 000» — 250+6 точек, панель 2 «Золото vs DXY» — 250+1.
- **CI на PR #41 — зелёный:** workflow `dashboards` (run 38229317402) и `Go` (run 38229317450); офлайн `gcx dev lint run` и diff-шаг артефакта в CI прошли.
- **Второй дашборд `key-rate-publications` (v1, 2026-10-16).** Артефакт собран кодом (19 панелей id 1–19 в рядах «Ставка и инфляция», «Деньги, кредит, бюджет, труд», «Рыночный контур», «Сигнал, консенсус, календарь»; каждый запрос — по витринам `v_*`, датасорс `ch-rosstat`); `make grafana-build` детерминирован — повторный прогон даёт пустой `git diff -- dashboard/`; `gcx` 1.5.0 по обоим файлам артефактов — «2 files linted. No violations found.» (exit 0).
- **Третий дашборд `finance-polyus` (v1, 2026-10-16).** Артефакт собран кодом (19 панелей id 1–19 / 48 таргетов в рядах «Золото, ставки, рынок», «Операционные показатели», «Финансовые показатели», «Прогноз и точность»; каждый запрос — по витринам `v_*`, датасорс `ch-rosstat`); `make grafana-build` детерминирован (повторная сборка — байт-идентичный артефакт), `gcx` 1.5.0 по всем трём файлам артефактов — «3 files linted. No violations found.» (exit 0), SQL панелей прогнан живьём под `kimi_reader` через MCP — **по отчёту исполнителя задачи**.
- **Живой прогон трёх дашбордов на локальном стенде (2026-10-16), уже с Bearer-токеном.** `make grafana-deploy-test` (стенд Grafana 13.2.3, `gcx` 1.5.0) — по `✔ 1 resources pushed, 0 errors` на `gold-thesis`, `key-rate-publications` и `finance-polyus`; smoke `make grafana-check` — «ПРОВЕРКА ПРОЙДЕНА» на каждом uid: `gold-thesis` виден (версия 13, 9 панелей с таргетами, панель 1 — 252 точки), `key-rate-publications` (версия 5, 19 панелей, первая панель — 784 точки), `finance-polyus` (версия 1, 19 панелей, первые две панели — по 1232 точки; расхождение с ручным `count()` = 1233 — пограничный день окна `now-5y`, объяснено в протоколе PR).

**Ещё не подтверждено (ожидает владельца):**

- Полный staging-прогон `make grafana-deploy-staging` (туннель `:3001` → провижининг датасорса → push → smoke): `GRAFANA_STAGING_TOKEN` задан, туннель ещё не поднимался; после первого провижининга нужен один admin-reload датасорса (см. «Ловушка staging»). Цель идёт циклом по всем трём дашбордам — smoke по `gold-thesis`, `key-rate-publications` и `finance-polyus`.
- Серверная `gcx resources validate` по артефактам `key-rate-publications` и `finance-polyus` — не выполнялась (нужны креды и стек-конфиг стенда; в `make grafana-validate` серверная валидация не входит); у `gold-thesis` — по отчёту исполнителя задачи.
- Визуальный просмотр дашбордов в браузере (`http://localhost:3000`).
