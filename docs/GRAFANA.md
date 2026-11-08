# Grafana

> Часть документации проекта; входная точка — [README.md](../README.md).

Установка Grafana 11.2.0 (Debian/Ubuntu, arm64):

```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_11.2.0_arm64.deb
sudo dpkg -i grafana_11.2.0_arm64.deb
```

Дашборды контура — **`gold-thesis`** (статья про золото), **`key-rate-publications`** (иллюстрации к публикациям по ключевой ставке), **`finance-polyus`** («Полюс (PLZL) — финансы и прогноз»: графики статьи PLZL), **`transaq-trades`** («Сделки MOEX — нетто покупок и продаж»: лента сделок Transaq), **`urals-budget-rule`** («Urals: реальная цена и бюджетное правило»: Urals в постоянных рублях и бюджетное правило с ценой отсечения $59, пункт `infra/5.15`) и **`bond-attractiveness`** («Привлекательность облигаций — ставка, ОФЗ, вклады»: светофор статьи, кривая ОФЗ против ключа, вклады и кредиты против полосы 11–12%, средства клиентов банков SORS, пункт `ofz-market/1.8`) — живут как код, а не как экспорт из UI. Конвейер:

```text
grafana/ (Go, grafana-foundation-sdk) --make grafana-build--> dashboard/<uid>.json
                                                              (gold-thesis, key-rate-publications, finance-polyus, transaq-trades, urals-budget-rule, bond-attractiveness; все коммитятся)
        |                                                              |
   make grafana-validate                                       make grafana-deploy-test / -staging (gcx resources push всех шести)
   (go vet + тесты)                                                |
   CI dashboards.yml: тесты, сборка,                           make grafana-check
   diff-проверка артефактов, gcx dev lint                 (smoke: uid + точки панелей, по всем пяти)
```

Легаси-экспорты `dashboard/rosstat.json` и `dashboard/chmf.json` остаются как есть — их миграция на код (процесс — скил `.kimi-code/skills/dashboard-migration/SKILL.md`, живая очередь — [docs/ROADMAP_INFRA.md](ROADMAP_INFRA.md), Фаза 2), алерты Grafana и PNG-рендер панелей для статей — отдельные пункты вне объёма этой работы. Прежний легаси-экспорт `dashboard/finance-polyus.json` заменён генерируемым артефактом 2026-10-16 (история — в git).

### Дашборд как код

- Источник истины — `grafana/` (отдельный Go-модуль): дашборды `gold-thesis` (9 панелей id 1–9 в 5 рядах), `key-rate-publications` (38 панелей id 1–19, 25–33, 35–39 и 41–45 в 8 рядах, «Ключевая ставка — публикации»; панель 41 — календарная теплокарта недельного ИПЦ на Apache ECharts через плагин Business Charts — `grafana/echarts.go`, infra/5.14; панели 42–45 — ряд «Квартальные метрики ЦБ» (квартальный SAAR текущего роста и базовой, таблица «4к26 к дате», 3-месячный SAAR недельного агрегата, ревизии квартальных оценок), пункт `key-rate/phase-4-cpi-quarterly-metrics`, #542; линии графиков — smooth-интерполяция), `finance-polyus` (19 панелей id 1–19 в 4 рядах id 31–34, «Полюс (PLZL) — финансы и прогноз»), `transaq-trades` (5 панелей id 1–5 в 2 рядах id 21–22, «Сделки MOEX — нетто покупок и продаж»: top-10 по дневной витрине, графики потоков по минутной) и `urals-budget-rule` (7 панелей id 1–7 в 2 рядах id 21–22, «Urals: реальная цена и бюджетное правило»: Urals в постоянных рублях и бюджетное правило $59 — ECharts-панели через `grafana/echarts.go`, infra/5.15) и `bond-attractiveness` (21 панель id 1–19, 50, 51 в 6 рядах id 20–25, «Привлекательность облигаций — ставка, ОФЗ, вклады»: чек-лист «Все триггеры статьи» одной stat-панелью из десяти плиток (вердикт «облигации» / «акции» по каждому условию плюс спред ставок «вторичка − первичка» как индикатор искажения первички льготной ипотекой), шесть снимков-«светофоров» статьи, ставка против средневзвешенной с 2013, кривая ОФЗ против ключа, вклады и кредиты против полосы 11–12%, спред «вклад − ОФЗ», средства клиентов банков SORS; пункт `ofz-market/1.8`); все запросы — только по витринам `v_*`, датасорс каждой панели — константа uid `ch-rosstat`. CLI — флаги `-dashboard <uid>|all` (дефолт `all`) и `-out-dir` (дефолт `../dashboard`): файл артефакта — `<out-dir>/<uid>.json`. Артефакты `dashboard/gold-thesis.json`, `dashboard/key-rate-publications.json`, `dashboard/finance-polyus.json`, `dashboard/transaq-trades.json`, `dashboard/urals-budget-rule.json` и `dashboard/bond-attractiveness.json` (k8s-ресурс `dashboard.grafana.app/v1beta1`, uid = `metadata.name`) руками не правятся: `make grafana-build` перезаписывает их детерминированно, а CI падает, если артефакт разошёлся с кодом.
- `make grafana-validate` — `go vet` + тесты модуля (состав панелей всех пяти дашбордов и негативные правила `Validate`). Серверный `gcx resources validate` намеренно не входит в цель: он проверяет ресурс на живом инстансе (server-side dry-run) и требует кредов — рабочая форма записана в комментарии цели.
- Локальный стенд: `make grafana-up` (brew-Grafana; данные, логи и плагины в `$(GF_DIR)`, провижининг датасорса — `grafana/provisioning/datasources/clickhouse.yaml`, порт 3000) и `make grafana-down`. При первом входе в UI смените пароль `admin/admin`.
- **`$__timeFilter` — по умолчанию, `$__dateFilter` — для Date.** Каждый timeseries-таргет несёт `$__timeFilter` по своей временной колонке (`date`, `period_date`, `meeting_date`): без него панель игнорирует выбор периода в правом верхнем углу. Для `Date`-колонок используется документированный макрос `$__dateFilter(date)` (`$__timeFilter` по Date даёт неявный каст), для `DateTime` — `$__timeFilter(time)`. Если время на графике вычисляется (наложение прошлого года `date_add(YEAR, 1, date)`, 4-недельная средняя), фильтр ставится на вычисленное `time`, а не на сырой `date` — иначе ряд выходит за окно (у панели 2 прошлый год дал бы 0 точек вместо 42). Норма машинная: `TestKeyRateAllTimeseriesUseTimeFilter`, `TestKeyRateWeeklyYearOverlay` и `TestTransaqTradesSQLContract` в `grafana/` валят `make grafana-validate`; табличные и stat-панели не проверяются — у них время не ось. Практики ClickHouse для time-series SQL (зерно витрины, бакеты, пропуск ≠ ноль, `WITH FILL`) — раздел «Time-series SQL: практики ClickHouse» скила `dashboard-migration`.

### Переменные окружения стендов

| Переменная | Назначение |
|---|---|
| `GRAFANA_TEST_URL` | адрес тест-стенда, по умолчанию `http://localhost:3000` |
| `GRAFANA_TEST_TOKEN` | токен сервис-аккаунта **Editor** на тест-стенде: `make grafana-deploy-test`, `make grafana-check` (в `gcx` уходит как `GRAFANA_TOKEN` — make-цели подставляют его сами; при ручных вызовах `gcx` подставляйте явно, см. «Цикл разработки и деплой») |
| `GRAFANA_STAGING_URL` | адрес staging, по умолчанию `http://localhost:3001` — **только через ssh-туннель** |
| `GRAFANA_STAGING_TOKEN` | токен сервис-аккаунта **Editor** на staging: `make grafana-deploy-staging` |
| `GRAFANA_MCP_TOKEN` | **Admin**-токен для read-only MCP: gcx-дашборды видны только Admin (Viewer/Editor → 403). Локально — `GRAFANA_TEST_TOKEN`, если его аккаунт Admin; на staging — отдельный Admin-токен `mcp-grafana-admin`. MCP остаётся read-only благодаря `--disable-write` |
| `GRAFANA_MCP_URL` | streamable HTTP URL Grafana MCP для `make grafana-mcp-check`; пусто — stdio к локальному стенду; staging — `http://localhost:8001/mcp` через туннель |
| `GRAFANA_ORG_ID` | org-id инстанса для `gcx` (on-prem без него отвергается), по умолчанию `1` |

Значения — только в `~/.config/rosstat/env`; `make env-check` печатает «задан/не задан» без значений. Версии: `gcx` 1.5.0 (`brew install gcx`), локальный стенд — Grafana 13.2.3 + плагины `grafana-clickhouse-datasource` 4.3.2 и `volkovlabs-echarts-panel` 7.2.5 (ставятся `make grafana-up`), staging — Grafana 13.1.1.

### Одноразовая настройка (владелец)

1. `brew install gcx`.
2. Локальный стенд: `make grafana-up` → войти `admin/admin` (первый запуск) → **Administration → Users and access → Service accounts** → аккаунт с ролью **Editor** → **Add service account token** → значение в `~/.config/rosstat/env` как `GRAFANA_TEST_TOKEN`; то же одной командой — `make grafana-test-token` (создаёт аккаунт `dashboards-cd` роли Editor и печатает строку для env-файла).
3. Staging: то же в UI staging (через туннель) → токен как `GRAFANA_STAGING_TOKEN`.
4. Grafana MCP требует **Admin**-токен: gcx-дашборды видны только Admin (Viewer/Editor → 403 на Grafana 13). Локально подходит `GRAFANA_TEST_TOKEN`, если его аккаунт Admin; на staging создать `mcp-grafana-admin` роли Admin и положить его токен в `/etc/clickhouse-import-rosstat.env` как `GRAFANA_MCP_TOKEN`.

Токены не попадают в репозиторий, CI и логи (`gcx` получает их из окружения, а не аргументом командной строки).

### MCP-сервер Grafana (read-only)

`mcp-grafana` даёт агенту читать Grafana через MCP: `search_dashboards`,
`get_dashboard_summary`, `list_datasources`, `alerting_rules_read`, аннотации. Требует
**Admin**-токен: на Grafana 13 дашборды, залитые через `gcx`/k8s API, видны только Admin
(Viewer/Editor → 403; проверено на 13.2.3). `--disable-write` убирает write-инструменты.

- Локально: если `GRAFANA_TEST_TOKEN` — Admin, отдельный токен не нужен — `.kimi-code/mcp.json`
  берёт его, когда `GRAFANA_MCP_TOKEN` не задан. Иначе создайте Admin-токен и задайте
  `GRAFANA_MCP_TOKEN`. Сервер `grafana` уже объявлен там
  (stdio, `uv tool run --from mcp-grafana==2.2.0 mcp-grafana --disable-write --usage-stats=disabled`).
- Проверка: `make grafana-mcp-check` (нужны `make grafana-up` и задеплоенные дашборды
  `make grafana-deploy-test`); ожидается `ПРОВЕРКА ПРОЙДЕНА`.
- Staging: сервис `mcp-grafana` на `127.0.0.1:8000`, наружу не публикуется; в
  `/etc/clickhouse-import-rosstat.env` на хосте `GRAFANA_MCP_TOKEN` = Admin-токен `mcp-grafana-admin`.
  Туннель `ssh -N -L 8001:127.0.0.1:8000 palmshell`, затем
  `make grafana-mcp-check GRAFANA_MCP_URL=http://localhost:8001/mcp`.
  Клиент Kimi (опционально) — `"grafana-staging": {"url": "http://localhost:8001/mcp"}`.
- Подробности деплоя и отката — `deploy/staging/README.md`, «Grafana MCP на staging».

### Цикл разработки и деплой

```bash
make ch-up && make grafana-up           # ClickHouse + локальный стенд с провижинингом датасорса
make grafana-build && make grafana-validate
make grafana-deploy-test                # gcx resources push всех пяти дашбордов на $(GRAFANA_TEST_URL) + smoke
make grafana-check                      # только smoke (все пять uid)
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

**Датасорс и `readonly`.** `kimi_reader` создаётся с `readonly = 2` (`sql/mcp_kimi_reader.sql`, обновляется `make mcp-user` / `make mcp-user-staging`): запись на постоянные таблицы запрещена и на этом уровне, но плагин `grafana-clickhouse-datasource` 4.14+ при подключении выставляет `max_execution_time`. На `readonly = 1` health датасорса возвращает `failed to create ClickHouse client`, а в логе Grafana — `Cannot modify 'max_execution_time' setting in readonly mode` (проверено на staging 2026-10-31).

### Алерты live-мониторинга ML (`infra/4.3`)

`grafana/provisioning/alerting/ml-live.yaml` — два правила Grafana Alerting:

| UID | Условие | Порог |
|---|---|---|
| `ml-live-degradation` | rolling MAE трёх последних разрешённых production-заседаний по `v_ml_live_accuracy` | > 100 б.п. |
| `ml-live-freshness` | последний прошедший production-прогноз не разрешён (`is_resolved=1`) | > 3 дней |

Оба — `severity=warning`, `contour=ml-live`, `for: 5m`; contact point не задаётся (алерт виден и уходит в Firing в UI, маршрутизация уведомлений — решение владельца). Оба запроса исключают `model='gate-selftest'`: синтетические строки приёмки не поднимают production-алерт. Локальный стенд читает каталог напрямую: `make grafana-up` передаёт `GF_PATHS_PROVISIONING=$(CURDIR)/grafana/provisioning`. Staging получает файл через `make grafana-provision-alerting-staging` (вызывается из `grafana-deploy-staging`); после копирования нужен admin-reload `POST /api/admin/provisioning/alert-rules/reload` или рестарт `grafana-server`. Smoke `make grafana-check` после дашбордов проверяет оба UID через `scripts/grafana_alert_check.py`. Офлайн-контракт YAML — `views/ml_live_alert_test.go` (входит в `make grafana-validate` и `make all`).

Проверка firing на локальном стенде: временно понизить порог `ml-live-degradation` через API/UI, дождаться состояния Firing, вернуть 100; в протоколе PR фиксируются UID, порог и наблюдённое состояние. Прод-данные для этого не нужны.

### Проверки и приёмка (gold-thesis — 2026-10-12; key-rate-publications — 2026-10-16; finance-polyus — v1 2026-10-16; transaq-trades — v1 2026-10-31; urals-budget-rule — v1 2026-11-03; bond-attractiveness — v1 2026-11-08; живой деплой + smoke трёх uid на локальном стенде — 2026-10-16)

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
- **Четвёртый дашборд `transaq-trades` (v1, 2026-10-31).** Артефакт собран кодом (5 панелей id 1–5 в 2 рядах id 21–22: top-10 нетто покупок/продаж IMOEX по дневной витрине с `$__dateFilter(date)`; нетто-поток IMOEX и покупки/продажи IMOEX и ОФЗ по минутной с `$__timeFilter(minute)` + `$__timeInterval(minute)`; все запросы — `v_transaq_net_flows*`, датасорс `ch-rosstat`); `make grafana-build` детерминирован (повторная сборка — байт-идентичный артефакт); `gcx` 1.5.0 по всем четырём файлам — «4 files linted. No violations found.» (exit 0); `make all`, `make grafana-validate`, `make docs-check` зелёные; SQL всех пяти таргетов прогнан под `kimi_reader` через MCP после публикации витрин (`make import STAT=transaq_net_flows`; локальная лента пуста — результат пустой, ошибок нет), `EXPLAIN ESTIMATE` панели 3 — 0 частей. Локальный smoke не показателен: dev-лента Transaq пуста; staging smoke — owner-assisted.
- **Пятый дашборд `urals-budget-rule` (v1, 2026-11-03).** Артефакт собран кодом (7 панелей id 1–7 в 2 рядах id 21–22; SQL всех таргетов прогнан живьём под `kimi_reader` через MCP, значения сошлись с перепроверкой публикации: 8 418 ₽/барр., 5 471/6 609 млрд, −17.2% г/г); `make grafana-build` детерминирован (повторная сборка — байт-идентичный артефакт); `make grafana-validate` зелёный; живой деплой/рендер ECharts — owner-assisted (staging-плагин — действие владельца); спека — [2026-11-03-urals-budget-rule-design.md](superpowers/specs/2026-11-03-urals-budget-rule-design.md), план — [2026-11-03-urals-budget-rule.md](superpowers/plans/2026-11-03-urals-budget-rule.md).
- **Шестой дашборд `bond-attractiveness` (v1, 2026-11-08).** Артефакт собран кодом (21 панель id 1–19, 50, 51 в 6 рядах id 20–25; первая панель — чек-лист «Все триггеры статьи»: десять stat-плиток — девять вердиктов «облигации» / «акции» / «—» и спред ставок «вторичка − первичка»; шесть снимков-«светофоров» статьи: ключевая ставка, реальная ставка, ОФЗ 10л − ключ, ОФЗ 1 год, вклад 181 дн–1 год, кредит юрлицам >1 года; графики: ставка против средневзвешенной с 2013 (считается на момент запроса), ставка против ИПЦ, реальная ставка, кривая ОФЗ 1г/2г/10л против ключа, спреды и наклон, вклады по срокам, кредиты юрлицам и физлицам, иллюстрация «кредиты vs вклады» против полосы 11–12%, иллюстрация «жёсткая риторика» (ставка, реальная ставка, вменённая ставка рынка), спред «вклад − ОФЗ» по месяцам, средства клиентов SORS: всего/вклады/депозиты/счета, темпы вкладов, эскроу и ИП, доля инвалюты); SQL всех таргетов прогнан живьём под `kimi_reader` через MCP 2026-11-08 — значения сошлись с факт-чеком статьи (14.00, 10.4314, 7.65, 14.16/14.99/16.75, 12.17, 12.98, 154.2 трлн ₽); `make grafana-build` детерминирован (повторная сборка — байт-идентичный артефакт); `make grafana-validate` зелёный; живой деплой/smoke — owner-assisted; спека — [2026-11-08-grafana-bond-attractiveness-design.md](superpowers/specs/2026-11-08-grafana-bond-attractiveness-design.md), план — [2026-11-08-grafana-bond-attractiveness.md](superpowers/plans/2026-11-08-grafana-bond-attractiveness.md), пункт — `ofz-market/1.8`.

**Ещё не подтверждено (ожидает владельца):**

- Полный staging-прогон `make grafana-deploy-staging` (туннель `:3001` → провижининг датасорса → push → smoke): `GRAFANA_STAGING_TOKEN` задан, туннель ещё не поднимался; после первого провижининга нужен один admin-reload датасорса (см. «Ловушка staging»). Цель идёт циклом по всем шести дашбордам — smoke по `gold-thesis`, `key-rate-publications`, `finance-polyus`, `transaq-trades`, `urals-budget-rule` и `bond-attractiveness`.
- Серверная `gcx resources validate` по артефактам `key-rate-publications` и `finance-polyus` — не выполнялась (нужны креды и стек-конфиг стенда; в `make grafana-validate` серверная валидация не входит); у `gold-thesis` — по отчёту исполнителя задачи.
- Визуальный просмотр дашбордов в браузере (`http://localhost:3000`).
