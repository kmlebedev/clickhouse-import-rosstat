# Дашборд «Золото — тезисы статьи» (Grafana): dashboards-as-code и provisioning CI/CD — design

Дата: 2026-10-12. Пункт: `docs/GOLD_THESIS_DATA_ROADMAP.md` §4 «Графики для иллюстрации статьи (Grafana)»
+ issue [#23 «Automate dashboard provisioning with CI/CD»](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/23).
Ветка: `feat/grafana-gold-thesis`.

**Приёмка пункта (кратко):** дашборд `gold-thesis` собран кодом (grafana-foundation-sdk) в артефакт `dashboard/gold-thesis.json`,
проведён по конвейеру issue #23 (сборка → валидация → деплой на тестовый стенд → проверка панелей → merge → деплой на staging),
все панели читаются и в Grafana, и через MCP `run_query` одним определением; провижининг датасорса с фиксированным UID;
`make all` и `make mcp-check` без регрессий; README/ARCHITECTURE синхронизированы. Детали — §7.

## 1. Пробел и место в роадмапе

§4 требует отдельный дашборд под тезисы статьи (таблица панелей и приёмка — в роадмапе), но дашборда нет,
а provisioning дашбордов в Grafana — ручные JSON-экспорты из UI. Issue #23 фиксирует целевой конвейер;
в staging-спеке он был явно вынесен отдельной работой («Grafana provisioning и issue #23 (UID датасорсов,
валидация дашбордов) — отдельная работа»).

Отображение шагов issue #23 на реализацию:

| Шаг issue #23 | Реализация |
|---|---|
| сборка dashboard JSON (grafana-foundation-sdk) | `make grafana-build` — Go-генератор `grafana/` → `dashboard/gold-thesis.json` |
| проверка JSON, UID, ссылок на datasource и правил | `make grafana-validate`: Go-тесты + `gcx resources validate`; в CI — то же |
| деплой в тестовую Grafana | `make grafana-deploy-test` → локальный стенд (Grafana 13.2.3 + локальный ClickHouse) |
| проверка ключевых панелей и запросов | `make grafana-check` (API-запросы к панелям) + глазами в браузере |
| merge | обычный PR в `main` |
| деплой проверенного артефакта в production | `make grafana-deploy-staging` → staging Grafana на `palmshell` (после merge) |

**Решения владельца (2026-10-12, брейншторм):** подход A — дашборд как код + деплой через API (gcx);
тест — локальная Grafana + локальный ClickHouse, production — staging; деплой — make-цели (CI не имеет доступа
к стендам и секретов не несёт); дашборд v1 — на существующих витринах; §3-витрины (`v_gold_floor`,
`v_macro_releases`, `v_rates_snapshot`, `v_scenario_state`, `v_forecast_accuracy_live`, `v_gold_technicals`) —
отдельным пунктом позже, панели переключаются на них точечной правкой SQL без смены визуала.

## 2. Живые факты (проверено 2026-10-12)

| Факт | Проверка |
|---|---|
| staging Grafana: `active`, версия **13.1.1**, `127.0.0.1:3000`, `/api/health` = 200 | `ssh palmshell` |
| Плагин `grafana-clickhouse-datasource` на staging установлен | `ls /var/lib/grafana/plugins` |
| На staging **нет ни датасорсов, ни дашбордов**: провижининг-каталоги содержат только стоковые `sample.yaml`; в `grafana.db` нет вхождений `grafana-clickhouse-datasource` и `bdx7lxrdelxq8b` | `strings /var/lib/grafana/grafana.db`, `ls /etc/grafana/provisioning/*/` |
| Локальная Grafana **13.2.3** (brew), не запущена, каталог плагинов пуст | `grafana --version`, `ls /opt/homebrew/var/lib/grafana/plugins` |
| **gcx поддерживает self-hosted Grafana 13+** полностью (resources push/pull/validate, `dev lint/serve/import`, `api`-passthrough; сервис-токены; env `GRAFANA_SERVER`/`GRAFANA_TOKEN`) | README gcx (вопрос #23 ссылается на его раздел CI/CD) |
| Экспортированные `dashboard/*.json` и корневой `grafana_dashboard_rosstat` прибиты к UID чужой инсталляции `bdx7lxrdelxq8b` (4 / 17 / 107 вхождений; `chmf.json` — 0) и **на staging не развёрнуты** | grep |
| Колонки витрин v1 подтверждены (`v_gold_dashboard.indicator/value/threshold/flag/stale/as_of`, `v_gold_prices.source/series/date/value`, `v_cb_gold_dynamics` — MoM/YTD/holdings и т.д.) | `system.columns` через MCP |

Следствие: staging — «чистый лист»; провижининг датасорса со своим UID ничего не ломает, `gold-thesis` станет там первым дашбордом.

## 3. Архитектура

```
grafana/ (отдельный Go-модуль, grafana-foundation-sdk)
   └─ make grafana-build → dashboard/gold-thesis.json   (ресурс dashboard.grafana.app/v1beta1, коммитится)
         │
         ├─ CI «dashboards» (PR): go vet/test + build + diff-проверка артефакта + gcx resources validate   (без секретов)
         │
         ├─ make grafana-deploy-test    → gcx resources push → локальная Grafana :3000
         │   (перед этим: make ch-up && make grafana-up; после — make grafana-check)
         └─ make grafana-deploy-staging → ssh-туннель :3001 → gcx push → staging Grafana (+идемпотентный провижининг датасорса)
```

### 3.1 Генератор `grafana/`

Отдельный Go-модуль (`github.com/kmlebedev/clickhouse-import-rosstat/grafana`, свой `go.mod`) —
SDK не попадает в зависимости основного ETL-бинаря и его CI. `main.go` собирает дашборд типизированным кодом
(`dashboard.NewDashboardBuilder`, панели `timeseries/barchart/table`) и пишет **`dashboard/gold-thesis.json`** —
ресурс в Kubernetes-стиле (`apiVersion: dashboard.grafana.app/v1beta1`, `kind: Dashboard`, `metadata.name = uid = gold-thesis`,
`spec` — сам дашборд), формат из официального примера Foundation SDK, пригодный для `gcx resources push`
(шаблон — https://grafana.com/docs/grafana/latest/as-code/observability-as-code/foundation-sdk/dashboard-automation/).
Чистый dashboard JSON при необходимости извлекается `jq .spec`.

Требования к генерации: **детерминированность** (без дат/времени; повторный прогон не меняет файл — это проверяет CI diff-шагом).

### 3.2 Датасорс: константа UID и провижининг

Один UID на все стенды — константа `ch-rosstat` (в Go-коде и в файлах провижининга; меняется только вместе).
Панели ссылаются только на неё — дашборд переносим между инсталляциями (это и есть корень пункта «UID датасорсов»
из staging-спеки: текущие экспорты прибиты к UID конкретной инсталляции).

- `grafana/provisioning/datasources/clickhouse.yaml` — локальный стенд: `url: http://127.0.0.1:8123`, `user: default`, без пароля.
- `deploy/staging/grafana-datasource.yaml` — шаблон для staging: тот же UID, `url: http://localhost:8123`,
  `user: kimi_reader` (readonly, доступны только витрины `v_*` — панели читают только витрины), пароль —
  плейсхолдер, подставляется **на хосте** в момент применения из `/etc/clickhouse-import-rosstat.env`;
  в репозиторий пароль не попадает никогда.

Grafana применяет provisioning-каталог «на старте и на лету», поэтому отдельный рестарт не нужен.

### 3.3 Валидация

`make grafana-validate`:
1. Go-тесты `grafana/`: артефакт парсится; `metadata.name = uid = gold-thesis`; все ссылки на датасорс равны константе;
   uid панелей уникальны; негативные тесты (чужой UID датасорса / дубль uid панели) падают ожидаемо.
2. `gcx resources validate` по **артефакту** `dashboard/gold-thesis.json` — офлайн-проверка формата ресурса
   (каталог `dashboard/` целиком не валидируем: легаси-экспорты `rosstat.json` и др. — не ресурсы и валидацию не пройдут); точный CLI — §9.

`make grafana-check` (живой, требует `make ch-up`): SQL ключевых панелей исполняется на локальном ClickHouse,
2–3 панели обязаны вернуть непустой результат; те же SQL прогоняются через MCP `run_query` под `kimi_reader` —
«одно определение для Grafana и агента» (§4 роадмапа). Запросы берутся из сгенерированного артефакта (не дублируются).

### 3.4 Деплой

- `make grafana-deploy-test`: `gcx resources push` артефакта против `GRAFANA_TEST_URL` с токеном
  `GRAFANA_TEST_TOKEN` (переменные gcx `GRAFANA_SERVER`/`GRAFANA_TOKEN` выставляются из них), затем smoke.
- `make grafana-deploy-staging` (после merge): требует поднятого туннеля
  `ssh -N -L 3001:127.0.0.1:3000 palmshell` (порт 3001 — чтобы не конфликтовать с локальным стендом на :3000;
  конвенция поднятого руками туннеля — как у `make mcp-check`); шаги: (1) проверка доступности;
  (2) идемпотентный провижининг датасорса (scp шаблона + подстановка пароля на хосте из env-файла, `root:grafana 640`) —
  только если файл изменился; (3) `gcx resources push`; (4) smoke.
- Smoke `scripts/grafana_check.py` (стиль `scripts/mcp_check.py`, `urllib`, токен из env): `GET /api/dashboards/uid/gold-thesis` = 200;
  `POST /api/ds/query` по 2–3 ключевым панелям — непустые series; короткий отчёт; exit-коды: `0` — все проверки прошли,
  `1` — хотя бы одна упала, `2` — ошибка конфигурации (не заданы `GRAFANA_URL`/`GRAFANA_TOKEN`).
- Фолбэк, если `gcx resources push` под self-hosted поведёт себя иначе: `gcx api /api/dashboards/db -d @<plain>.json`,
  где чистый dashboard JSON извлекается из артефакта через `jq .spec` — тот же токен и туннель.

### 3.5 CI

Новый workflow `.github/workflows/dashboards.yml` (на PR и push в `main`): setup Go 1.27.x; `cd grafana && go vet ./... && go test ./...`;
`make grafana-build`; `git diff --exit-code -- dashboard/gold-thesis.json` (артефакт синхронен коду);
установка gcx (официальный `install.sh`, версия пинована) + `gcx resources validate`; upload артефакта.
**Секретов в CI нет, деплой не выполняется.** Существующий `go.yml` не трогается; `gofmt -l .` автоматически
покроет файлы нового модуля (его держим отформатированным), `go test ./...` основного модуля вложенный модуль не видит.

### 3.6 Make-цели и env

Новые цели: `grafana-up`, `grafana-down`, `grafana-build`, `grafana-validate`, `grafana-check`,
`grafana-deploy-test`, `grafana-deploy-staging`. `make all` не меняется.

`make env-check` дополняется проверкой **наличия** `GRAFANA_TEST_TOKEN` и `GRAFANA_STAGING_TOKEN`
(значения не печатаются; URL имеют дефолты `http://localhost:3000` и `http://localhost:3001`).

### 3.7 Локальный стенд (`make grafana-up` / `grafana-down`)

По образцу `ch-up`/`ch-down`: pid-файл и лог вне репозитория (`GF_DIR ?= $(HOME)/grafana-dev`),
данные/логи/плагины стенда — в `$(GF_DIR)`, провижининг — `$(CURDIR)/grafana/provisioning`; порт 3000.
`grafana-up` ставит при отсутствии плагин `grafana-clickhouse-datasource` (через `grafana cli` в плагин-каталог стенда,
версия пинуется при реализации) и стартует brew-Grafana с явными `GF_PATHS_*`. Первый старт создаёт `admin/admin` —
в runbook: войти и создать сервис-аккаунт с токеном (одноразово). Системный `brew services` не используется.

## 4. Дашборд `gold-thesis` v1 — панели

Метаданные: uid `gold-thesis`, заголовок «Золото — тезисы статьи», теги `gold`, `article`, refresh 5m,
диапазон по умолчанию — 1 год, timezone browser.

| № | Панель (§4) | Источник v1 (колонки подтверждены) | Позже |
|---|---|---|---|
| 1 | Цена + порог $4 000 | `v_gold_prices` (`series='comex_front'`): линия цены + пороговая линия 4000; точки ниже 4000 отдельным запросом | `v_gold_floor` (§3); после merge bold — добавить `series='bold'` |
| 2 | Gold vs DXY (две оси) | `v_gold_prices` + `v_gold_dashboard` (`indicator='dxy'`) | — |
| 3 | Gold vs DFII10 | `v_gold_prices` + `v_fred_macro` (`series='DFII10'`) | `v_rates_snapshot` (§3) |
| 4 | Crack ULSD + запасы дистиллятов (пороги $50/$30) | `v_gold_dashboard` (`crack_ulsd_proxy`, `distillate_stocks`) + threshold-линии | — |
| 5 | CPI/PPI отпечатки, YoY | `v_bls_macro` (лаг 12 месяцев в SQL панели) | `v_macro_releases` (§3) |
| 6 | ETF-потоки по месяцам (bar) | `v_gold_etf_flows` (`region='world'`, `flow_usd_mln`) | — |
| 7 | Покупки ЦБ по странам (YTD-дельты, bar) | `v_cb_gold_dynamics` (`ytd_change_tonnes` по `country`) | — |
| 8 | Сценарный веер Q4 + факт | `v_gold_forecasts` (`own` и банки, `horizon`/`scenario`) + `v_gold_prices` | — |
| 9 | Ошибки прогнозов (predicted vs actual) | `v_forecast_accuracy` (знаковая `error_pct` = (predicted − actual) / actual × 100, плюс — прогноз выше факта; `is_resolved=1`) | `v_forecast_accuracy_live` (§3) |

Все панели — только на витринах `v_*` (сырые таблицы — прямое нарушение политики §4 роадмапа).

## 5. Секреты и безопасность

- Токены Grafana и URL — только в `~/.config/rosstat/env` (Makefile подключает файл сам); в CI, репозиторий и логи не попадают.
- Пароль staging-датасорса живёт только на хосте (`/etc/clickhouse-import-rosstat.env`, 600): подставляется в provisioning-файл
  в момент деплоя, в репозитории — шаблон без пароля.
- Датасорс staging читает ClickHouse пользователем `kimi_reader` (readonly, гранты только на `v_*`) — панели физически не могут
  обратиться к сырым таблицам.
- Сервис-аккаунт Grafana для деплоя: роль **Editor** (нужен push дашбордов); создание токенов — одноразово вручную (runbook).

## 6. Runbook (операционный порядок)

1. **Одноразово:** `brew install gcx`; локально — `make grafana-up`, войти `admin/admin`, создать сервис-аккаунт + токен →
   `GRAFANA_TEST_TOKEN` в env; на staging — создать сервис-аккаунт + токен → `GRAFANA_STAGING_TOKEN` в env.
2. **Разработка панели:** правка кода `grafana/` → `make grafana-build` → `make grafana-validate`.
3. **Пре-merge:** `make ch-up && make grafana-up` → `make grafana-deploy-test` → `make grafana-check` → визуальная проверка в браузере.
4. **PR** → CI «dashboards» зелёный → merge.
5. **Прод:** `ssh -N -L 3001:127.0.0.1:3000 palmshell &` → `make grafana-deploy-staging` → smoke зелёный → визуальная проверка.

## 7. Приёмка

1. `make grafana-build` → `dashboard/gold-thesis.json` создан; повторный прогон — diff пуст (детерминированность).
2. `make grafana-validate` — зелёный; негативные тесты (чужой UID датасорса, дубль uid панели) падают ожидаемо.
3. CI `dashboards` на PR — зелёный: go vet/test, сборка, diff-проверка артефакта, `gcx resources validate`.
4. Живой цикл локально: `make ch-up && make grafana-up && make grafana-deploy-test` → `GET /api/dashboards/uid/gold-thesis` = 200;
   `make grafana-check` — ключевые панели с данными; те же SQL через MCP `run_query` под `kimi_reader` дают те же числа.
5. После merge: `make grafana-deploy-staging` → дашборд на staging виден (`gcx dashboards list`), датасорс провижинится
   с константным UID, smoke зелёный; на staging `gold-thesis` — первый дашборд (существующие не затронуты).
6. `make env-check` показывает новые переменные; `make all` и `make mcp-check` — без регрессий;
   README.md и ARCHITECTURE.md синхронизированы (скил `sync-readme-architecture`).

## 8. Вне объёма

Миграция существующих `dashboard/rosstat.json`, `dashboard/chmf.json`, `dashboard/finance-polyus.json`,
корневого `grafana_dashboard_rosstat` в код (легаси-экспорты остаются как есть); алерты Grafana и алерт-правила;
PNG-рендер панелей для статьи; §3-витрины; self-hosted runner и автодеплой из CI; правки ETL/ingest;
отдельный пользователь ClickHouse под датасорс (используется `kimi_reader`).

## 9. Риски и открытые вопросы (закрываются при реализации)

| Вопрос | План |
|---|---|
| Точный синтаксис `gcx resources push` для файла и офлайн-режим `gcx resources validate` | проверить на живом стенде в реализации; фолбэк `gcx api /api/dashboards/db -d` уже зафиксирован |
| Роль/поведение gcx против self-hosted при push (app-platform API включены в 13+ по умолчанию) | проверить живьём на локальном стенде; при несовместимости — только фолбэк-путь, форма конвейера не меняется |
| Применимость `gcx dev lint` (Rego) к нашему layout модуля | опционально; минимальный контур — свои Go-тесты + `resources validate` |
| Пин версий: плагин `grafana-clickhouse-datasource` для 13.x, версия gcx в CI | зафиксировать константами в Makefile/workflow при реализации |
| Первый запуск локального стенда (данные/админ) на чистой машине | `grafana-up` задаёт `GF_PATHS_*` явно, данные — в `$(GF_DIR)`; шаги в runbook |
| Нужен ли и как `gcx dev serve` (live-preview) | опционально, решение при реализации |
