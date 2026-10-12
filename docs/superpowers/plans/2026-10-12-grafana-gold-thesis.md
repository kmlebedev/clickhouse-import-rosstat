# Gold-thesis dashboard as code + provisioning CI/CD — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дашборд `gold-thesis` (§4 золотого роадмапа) описан Go-кодом на grafana-foundation-sdk, собирается в коммитимый артефакт `dashboard/gold-thesis.json`, валидируется и деплоится make-целями на локальный тест-стенд и staging Grafana (шаги issue #23).

**Architecture:** Отдельный Go-модуль `grafana/` собирает K8s-style манифест (`resource.Manifest`, `dashboard.grafana.app/v1beta1`) с дашбордом и запросами к плагину `grafana-clickhouse-datasource` — кастомный dataquery-вариант по образцу `examples/go/custom-query` из SDK. Артефакт коммитится; CI проверяет синхронность «код → артефакт»; деплой — `gcx resources push` через API с сервис-токеном из env (локальный стенд :3000, staging — через ssh-туннель :3001).

**Tech Stack:** Go 1.27, `github.com/grafana/grafana-foundation-sdk/go`, gcx, Grafana 13.x + плагин `grafana-clickhouse-datasource` 4.3.2, ClickHouse, GNU make, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md`

## Global Constraints

- Работа — в обычном рабочем каталоге репозитория на ветке `feat/grafana-gold-thesis` (создана от `main`; параллельных задач нет — отдельный worktree не нужен). Первые коммиты ветки — спека и план; правки только в файлах этого пункта (импортёры и другие контуры репозитория не трогаем).
- Панели читают ТОЛЬКО витрины `v_*`; сырые таблицы запрещены (staging-датасорс — `kimi_reader`, у него нет грантов на сырые таблицы).
- Константы: UID датасорса `ch-rosstat`, тип `grafana-clickhouse-datasource`, UID дашборда `gold-thesis`.
- Артефакт `dashboard/gold-thesis.json` — K8s-style ресурс, коммитится, генерация детерминирована (повторный `make grafana-build` не меняет файл).
- Секреты — только в `~/.config/rosstat/env` (и `/etc/clickhouse-import-rosstat.env` на staging); пароль staging-датасорса — только на хосте; в репозитории и CI секретов нет.
- Деплой — только make-цели; CI — сборка+валидация, без секретов и без деплоя.
- Локальный стенд: данные/логи/плагины вне репо (`GF_DIR ?= $(HOME)/grafana-dev`), порт 3000; staging-туннель — локальный порт 3001.
- Плагин-пин `GRAFANA_PLUGIN_VERSION ?= 4.3.2` (формат запросов подтверждён живым экспортом 4.3.2); версия gcx в CI — пином.
- Коммиты по-английски (стиль истории репозитория); TDD: сначала падающий тест.
- `make all` и `make mcp-check` обязаны остаться зелёными; README.md и ARCHITECTURE.md синхронизируются (скил `sync-readme-architecture`).

## Review Focus

1. **Запросы панелей возвращают данные** (макросы `$__timeFilter` на `Date`/`Date32`, корректные имена рядов) — smoke `scripts/grafana_check.py` прогоняет запросы из артефакта и требует непустой ответ (Task 3, Task 4).
2. **Датасорс-ссылки** — каждая панель указывает на константу `ch-rosstat`; чужой UID ловится валидатором и негативным тестом (Task 1, Task 5).
3. **Детерминированность артефакта** — повторная сборка не меняет файл, иначе CI diff-шаг вечно красный (Task 1).
4. **Легаси-файлы `dashboard/*.json`** не участвуют в сборке и валидации и не ломают CI (валидируется только `gold-thesis.json`) (Task 5).
5. **Секреты в диффе** — шаблон staging содержит плейсхолдер, пароль подставляется только на хосте (Task 6).
6. **Ряды dxy/crack сейчас по 1 точке** — панели рисуют точки (`showPoints=always`), а не «пустые линии»; история накапливается еженедельным вводом (Task 4).

## File Structure

- Create: `grafana/go.mod`, `grafana/go.sum` — отдельный модуль (SDK не в зависимостях ETL-бинаря)
- Create: `grafana/clickhouse.go` — dataquery-вариант плагина ClickHouse + билдер запроса
- Create: `grafana/gold_thesis.go` — билдер дашборда (ряды, панели, SQL), `Manifest()`, helper `ptr`
- Create: `grafana/validate.go` — `Validate(dashboard.Dashboard) error`
- Create: `grafana/main.go` — CLI: пишет артефакт по `-out`
- Create: `grafana/*_test.go` — тесты генератора и валидатора
- Create: `grafana/provisioning/datasources/clickhouse.yaml` — провижининг датасорса локального стенда
- Create: `dashboard/gold-thesis.json` — генерируемый артефакт (коммитится)
- Create: `scripts/grafana_check.py` — smoke дашборда и панелей по API
- Create: `deploy/staging/grafana-datasource.yaml` — шаблон датасорса staging (плейсхолдер пароля)
- Create: `.github/workflows/dashboards.yml` — CI сборки/валидации
- Modify: `Makefile` (цели `grafana-*`, env-проверки), `README.md`, `ARCHITECTURE.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

---

### Task 1: Модуль `grafana/`: генератор, CH-вариант запроса, базовая панель (TDD)

**Files:**
- Create: `grafana/go.mod`, `grafana/clickhouse.go`, `grafana/gold_thesis.go`, `grafana/validate.go`, `grafana/main.go`
- Create: `grafana/clickhouse_test.go`, `grafana/gold_thesis_test.go`, `grafana/validate_test.go`
- Modify: `Makefile` (цель `grafana-build`, добавить в `.PHONY`)
- Create: `dashboard/gold-thesis.json` (генерируется и коммитится)

**Interfaces:**
- Consumes: ничего (первый таск).
- Produces: константы `DatasourceType = "grafana-clickhouse-datasource"`, `DatasourceUID = "ch-rosstat"`, `DashboardUID = "gold-thesis"`, `ApiVersion = "dashboard.grafana.app/v1beta1"`;
  `NewCHQuery(refID, rawSQL string) *CHQueryBuilder` с методами `Format(int) *CHQueryBuilder`, `QueryType(string) *CHQueryBuilder`, `Build() (variants.Dataquery, error)`;
  `CHQueryVariant() variants.DataqueryConfig`; `NewGoldThesis() *dashboard.DashboardBuilder`; `Manifest(d dashboard.Dashboard) resource.Manifest`;
  `Validate(d dashboard.Dashboard) error`; helper `ptr[T any](v T) *T`; CLI `go run . -out ../dashboard/gold-thesis.json`.

- [ ] **Step 1: Создать модуль и получить зависимости**

```bash
mkdir -p grafana && cd grafana && \
go mod init github.com/kmlebedev/clickhouse-import-rosstat/grafana && \
go get github.com/grafana/grafana-foundation-sdk/go@latest
```

Ориентир по структуре кастомного варианта — пример SDK `examples/go/custom-query` (файлы `main.go`, `customquery.go`): `plugins.RegisterDefaultPlugins()`, `cog.NewRuntime().RegisterDataqueryVariant(...)`, `resource.Manifest{ApiVersion, Kind: resource.DashboardKind, Metadata: resource.Metadata{Name}, Spec}`.

- [ ] **Step 2: Написать падающие тесты**

`grafana/clickhouse_test.go` — JSON запроса в формате плагина 4.3.2 (живой экспорт: `rawSql`/`editorType`/`format`/`queryType`/`refId`):

```go
func TestCHQueryMarshalsPluginShape(t *testing.T) {
	q, err := NewCHQuery("A", "SELECT 1").Format(0).QueryType("timeseries").Build()
	if err != nil { t.Fatal(err) }
	b, err := json.Marshal(q)
	if err != nil { t.Fatal(err) }
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil { t.Fatal(err) }
	if m["refId"] != "A" || m["rawSql"] != "SELECT 1" || m["editorType"] != "sql" ||
		m["format"] != float64(0) || m["queryType"] != "timeseries" || m["pluginVersion"] != "4.3.2" {
		t.Fatalf("неожиданный JSON запроса: %s", b)
	}
}
```

`grafana/gold_thesis_test.go`:

```go
func TestManifestWrapper(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil { t.Fatal(err) }
	m := Manifest(d)
	if m.ApiVersion != ApiVersion || m.Kind != resource.DashboardKind || m.Metadata.Name != DashboardUID {
		t.Fatalf("обёртка: %+v", m)
	}
}

func TestPricePanelUsesDatasourceConstant(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil { t.Fatal(err) }
	if len(d.Panels) == 0 { t.Fatal("панелей нет") }
	for _, p := range d.Panels {
		if p.Datasource == nil || p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
			t.Fatalf("панель %q без константного UID датасорса", p.Title)
		}
		if p.Datasource.Type == nil || *p.Datasource.Type != DatasourceType {
			t.Fatalf("панель %q: тип датасорса %v", p.Title, p.Datasource.Type)
		}
	}
}

func TestGenerateDeterministic(t *testing.T) {
	d1, err := NewGoldThesis().Build(); if err != nil { t.Fatal(err) }
	d2, err := NewGoldThesis().Build(); if err != nil { t.Fatal(err) }
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) { t.Fatal("генерация недетерминирована") }
}
```

`grafana/validate_test.go`:

```go
func TestValidateRejectsForeignDatasource(t *testing.T) {
	d, err := NewGoldThesis().Build(); if err != nil { t.Fatal(err) }
	foreign := "bdx7lxrdelxq8b"
	d.Panels[0].Datasource = &common.DataSourceRef{Type: ptr(DatasourceType), Uid: &foreign}
	if err := Validate(d); err == nil { t.Fatal("чужой UID датасорса не отвергнут") }
}
```

- [ ] **Step 3: Убедиться, что тесты падают**

Run: `cd grafana && go test ./...`
Expected: FAIL — не определены `NewCHQuery`/`NewGoldThesis`/`Manifest`/`Validate`.

- [ ] **Step 4: Реализовать `clickhouse.go`, `gold_thesis.go`, `validate.go`, `main.go`**

`clickhouse.go` — по образцу `examples/go/custom-query/customquery.go`: структура `CHQuery` с тегами — `RefId string \`json:"refId"\``, `Hide *bool \`json:"hide,omitempty"\``, `RawSql string \`json:"rawSql"\``, `EditorType string \`json:"editorType"\`` (всегда `"sql"`), `Format int \`json:"format"\`` (**без** omitempty — плагин всегда пишет ноль), `QueryType string \`json:"queryType"\``, `PluginVersion string \`json:"pluginVersion"\`` (константа `"4.3.2"`); `Equals/Validate/ImplementsDataqueryVariant/DataqueryType` (возвращает `DatasourceType`); `CHQueryVariant()` с `Identifier: DatasourceType` и `DataqueryUnmarshaler`; билдер `CHQueryBuilder` с `Format/QueryType/Build`.

`gold_thesis.go` — `NewGoldThesis()`: `dashboard.NewDashboardBuilder("Золото — тезисы статьи").Uid(DashboardUID).Tags([]string{"gold","article"}).Refresh("5m").Time("now-1y","now").Timezone(common.TimeZoneBrowser)`, ряд `dashboard.NewRowBuilder("Цена и пол")` и панель 1 (см. Task 4 — на этом таске достаточно одной панели с запросом A):

```sql
SELECT date AS time, value AS "Цена, USD/oz"
FROM v_gold_prices
WHERE source = 'gold' AND series = 'comex_front' AND $__timeFilter(date)
ORDER BY date
```
(SQL здесь и далее — дословно, отступы не важны). Панель: `timeseries.NewPanelBuilder().Title("Цена золота и пол $4 000").Datasource(common.DataSourceRef{Type: ptr(DatasourceType), Uid: ptr(DatasourceUID)}).WithTarget(NewCHQuery("A", sql).Format(0).QueryType("timeseries")).Thresholds(dashboard.NewThresholdsConfigBuilder().Mode(dashboard.ThresholdsModeAbsolute).Steps([]dashboard.Threshold{{Value: nil, Color: "green"}, {Value: ptr(4000.0), Color: "red"}}))`.
`Manifest(d)` — обёртка из примера SDK. `Validate(d)` — проверки: `*d.Uid == DashboardUID`; у каждой панели datasource = константы; id панелей уникальны и непусты; у каждой панели ≥1 target типа `CHQuery` с непустым `RawSql`, содержащим `"FROM v_"`.

`main.go` — `plugins.RegisterDefaultPlugins()`, `cog.NewRuntime().RegisterDataqueryVariant(CHQueryVariant())`, построить → `Validate` → `json.MarshalIndent(Manifest(d), "", "  ")` + `"\n"` → записать в файл из флага `-out` (default `../dashboard/gold-thesis.json`).

- [ ] **Step 5: Прогнать тесты и сборку**

Run: `cd grafana && go vet ./... && go test ./...`
Expected: PASS (все тесты).

- [ ] **Step 6: Цель `make grafana-build` и живая проверка**

В `Makefile`: `grafana-build:` → `cd grafana && go run . -out ../dashboard/gold-thesis.json`; добавить цель в `.PHONY`.
Run: `make grafana-build && git diff --stat && make grafana-build && git diff --stat`
Expected: после первого прогона — новый `dashboard/gold-thesis.json`; после второго `git diff` пуст; в артефакте `"uid": "gold-thesis"` и datasource `ch-rosstat` (проверить `jq`).

- [ ] **Step 7: Commit**

```bash
git add grafana dashboard/gold-thesis.json Makefile
git commit -m "add grafana dashboards generator"
```

---

### Task 2: Локальный стенд `make grafana-up/down` + провижининг датасорса

**Files:**
- Create: `grafana/provisioning/datasources/clickhouse.yaml`
- Modify: `Makefile` (цели + переменные `GF_DIR`, `GRAFANA_PLUGIN_VERSION`, `.PHONY`)

**Interfaces:**
- Consumes: константа `DatasourceUID` из Task 1 (значение `ch-rosstat` продублировано в yaml — менять только вместе).
- Produces: `make grafana-up` поднимает brew-Grafana 13.x на :3000 с провижинингом репо; `make grafana-down` останавливает.

- [ ] **Step 1: Файл провижининга**

`grafana/provisioning/datasources/clickhouse.yaml`:

```yaml
apiVersion: 1
datasources:
  - name: ClickHouse (rosstat)
    type: grafana-clickhouse-datasource
    uid: ch-rosstat
    access: proxy
    isDefault: true
    editable: false
    jsonData:
      host: 127.0.0.1
      port: 8123
      protocol: http
      secure: false
      username: default
      defaultDatabase: default
    secureJsonData:
      password: ""
```

- [ ] **Step 2: Цели `grafana-up` / `grafana-down`**

`grafana-up` (по образцу `ch-up`): если :3000 отвечает — уже поднят; иначе: проверить, что каталог `$(GF_DIR)/plugins/grafana-clickhouse-datasource` есть, иначе `grafana cli --pluginsDir "$(GF_DIR)/plugins" plugins install grafana-clickhouse-datasource $(GRAFANA_PLUGIN_VERSION)`; `mkdir -p $(GF_DIR)/{data,logs}`; запуск `nohup env GF_PATHS_DATA=... GF_PATHS_LOGS=... GF_PATHS_PLUGINS=... GF_PATHS_PROVISIONING=$(CURDIR)/grafana/provisioning GF_SERVER_HTTP_PORT=3000 GF_ANALYTICS_REPORTING_ENABLED=false GF_ANALYTICS_CHECK_FOR_UPDATES=false grafana server >$(GF_DIR)/grafana.log 2>&1 &`, pid в `$(GF_DIR)/grafana.pid`, ожидание `curl -sf http://localhost:3000/api/health` (до 60 с), при провале — печать хвоста лога и exit 1.
Если `grafana cli --pluginsDir` не поддерживается установленной версией CLI — поставить плагин в путь из `GF_PATHS_PLUGINS` другим способом и зафиксировать фактический способ в комментарии цели; цель обязана быть идемпотентной.
`grafana-down`: по pid-файлу (как `ch-down`).

- [ ] **Step 3: Живая проверка стенда**

Run: `make grafana-up && curl -sf http://localhost:3000/api/health && curl -s -u admin:admin http://localhost:3000/api/datasources | jq -r '.[0].uid'`
Expected: health `200`; UID = `ch-rosstat`. Затем `make ch-up` и в UI (`http://localhost:3000`, admin/admin) панель Explore → datasource `ch-rosstat` → `SELECT count() FROM v_gold_prices` возвращает число (проверка живости провижининга).

- [ ] **Step 4: Commit**

```bash
git add Makefile grafana/provisioning
git commit -m "grafana: local test stand"
```

---

### Task 3: Деплой на тест-стенд и smoke (walking skeleton закрыт)

**Files:**
- Create: `scripts/grafana_check.py`
- Modify: `Makefile` (`grafana-deploy-test`, `grafana-check`, `GRAFANA_TEST_URL`, env-check)

**Interfaces:**
- Consumes: артефакт из Task 1, стенд из Task 2.
- Produces: `make grafana-deploy-test`, `make grafana-check`; script `scripts/grafana_check.py` (env `GRAFANA_URL`, `GRAFANA_TOKEN`; аргумент `--uid`; exit 0/1, печать строк `OK ...`/`FAIL ...` в стиле `scripts/mcp_check.py`).

- [ ] **Step 1: Определить рабочую форму вызова gcx на локальном стенде**

Токен (одноразово, владелец): UI → Administration → Users and access → Service accounts → New (`dashboards-cd`, роль Editor) → Add token → сохранить в `~/.config/rosstat/env` как `GRAFANA_TEST_TOKEN`. URL по умолчанию `http://localhost:3000`.
Проверить варианты (живьём, на локальном стенде):

```bash
export GRAFANA_SERVER=http://localhost:3000 GRAFANA_TOKEN=<токен>
gcx resources push dashboards --path dashboard/gold-thesis.json     # вариант из issue #23
gcx resources push -p dashboard/gold-thesis.json                    # вариант README gcx
gcx api /api/dashboards/db -d @<(jq .spec dashboard/gold-thesis.json)  # фолбэк-путь
```

Зафиксировать в цели тот вариант, который отвечает успехом (idempotent: повторный push не создаёт дубль); остальные — закомментировать в цели как фолбэки. Если `resources push` отклоняет `apiVersion` — взять канонический формат из `gcx resources pull dashboards` (со своего же стенда) и поправить константу `ApiVersion` в Task 1 (артефакт перегенерировать).

- [ ] **Step 2: Написать `scripts/grafana_check.py`**

Контракт: читает `GRAFANA_URL`/`GRAFANA_TOKEN`, `--uid` (default `gold-thesis`); проверки: (1) `GET /api/dashboards/uid/<uid>` → 200, тело содержит uid; (2) берёт из ответа первые 2 панели с таргетами, для каждого таргета запускает `POST /api/ds/query` (`{"queries":[{"refId":"A","datasource":{"type":"grafana-clickhouse-datasource","uid":"ch-rosstat"},"rawSql":..., "format":0,"queryType":...}],"from":"now-1y","to":"now"}`) и требует непустой `results.A.frames`/ряды; печатает `OK дашборд виден`, `OK панель "<title>" вернула N точек` и exit 0; при любой неудаче — `FAIL ...` и exit 1. Запросы берутся из артефакта (не дублируются).

- [ ] **Step 3: Цели**

`grafana-deploy-test`: проверить `GRAFANA_TEST_TOKEN` непуст; `GRAFANA_SERVER=$(GRAFANA_TEST_URL) GRAFANA_TOKEN=$$GRAFANA_TEST_TOKEN <выбранный вызов gcx>`; затем `$(MAKE) grafana-check --no-print-directory`.
`grafana-check`: `GRAFANA_URL=$(GRAFANA_TEST_URL) GRAFANA_TOKEN=$$GRAFANA_TEST_TOKEN python3 scripts/grafana_check.py --uid $(DASHBOARD_UID)` (константа `DASHBOARD_UID ?= gold-thesis`).
`GRAFANA_TEST_URL ?= http://localhost:3000`. В `env-check` добавить проверку **наличия** `GRAFANA_TEST_TOKEN` (значение не печатать).

- [ ] **Step 4: Живая приёмка walkingskeleton**

Run: `make ch-up && make grafana-up && make grafana-deploy-test`
Expected: push без ошибок; `OK дашборд виден`; панель «Цена золота и пол $4 000» вернула >0 точек; в браузере на :3000 панель нарисована (визуальная проверка — скриншот не нужен, достаточно открыть). Повторный `make grafana-deploy-test` идемпотентен.
Если токена нет — выполнить только шаги 1–3, а приёмку пометить `owner-assisted` и запросить токен (секрет не генерируем и в репо не пишем).

- [ ] **Step 5: Commit**

```bash
git add Makefile scripts/grafana_check.py
git commit -m "grafana: deploy to test stand with smoke"
```

---

### Task 4: Полный состав панелей v1 (9 панелей, 5 рядов)

**Files:**
- Modify: `grafana/gold_thesis.go` (панели 2–9, ряды), `grafana/gold_thesis_test.go` (новые тесты)
- Regenerate: `dashboard/gold-thesis.json`

**Interfaces:**
- Consumes: `NewCHQuery`, `ptr`, константы, `Validate` из Task 1; имена витрин/колонок — сверены 2026-10-12 (см. ниже).
- Produces: полный дашборд; проверки: 9 панелей, у каждой константный датасорс, уникальные id, каждый SQL — `FROM v_`.

- [ ] **Step 1: Добавить тесты состава**

```go
func TestGoldThesisHasNinePanels(t *testing.T)          // len(d.Panels) == 9
func TestPanelIDsAreUniqueAndSet(t *testing.T)          // id 1..9, без повторов
func TestAllTargetsUseViews(t *testing.T)               // каждый CHQuery.RawSql содержит "FROM v_"
```
Плюс расширить `Validate` правилами из `TestPanelIDsAreUniqueAndSet`/`TestAllTargetsUseViews`, если их ещё нет.

- [ ] **Step 2: Панели — SQL дословно**

Ряд 1 «Цена и пол»:
- **p1** «Цена золота и пол $4 000» (span 8), запросы: A — `SELECT date AS time, value AS "Цена, USD/oz" FROM v_gold_prices WHERE source = 'gold' AND series = 'comex_front' AND $__timeFilter(date) ORDER BY date`; B — тот же с `AND value < 4000` и алиасом `"Касания пола $4 000"`. Серия B — только точки (через `Overrides`: `custom.lineWidth=0`, `custom.showPoints=always`). Порог 4000 (steps green → red) — как в Task 1.
- **p2** «Золото vs DXY» (span 8): A — цена (как p1A, алиас `"Золото, USD/oz"`); B — `SELECT as_of AS time, value AS "DXY" FROM v_gold_dashboard WHERE indicator = 'dxy'` — точки, правая ось (Override `custom.axisPlacement=right`, `custom.lineWidth=0`, `custom.showPoints=always`; сейчас у ряда 1 точка, линия появится с накоплением).
- **p3** «Золото vs DFII10» (span 8): A — цена; B — `SELECT date AS time, value AS "DFII10, %" FROM v_fred_macro WHERE series = 'DFII10' AND $__timeFilter(date) ORDER BY date` — правая ось.

Ряд 2 «Дизельный канал»:
- **p4** «Crack ULSD и запасы дистиллятов» (span 12): A — `SELECT date AS time, value AS "Запасы дистиллятов, тыс. барр." FROM v_eia_petroleum WHERE series = 'distillate_stocks' AND $__timeFilter(date) ORDER BY date`; B — `SELECT as_of AS time, value AS "Crack ULSD, USD/барр." FROM v_gold_dashboard WHERE indicator = 'crack_ulsd_proxy'` — точки, правая ось, пороги $30/$50 (Override-thresholds: steps green(30) → yellow(50) → red; thresholdsStyle — линия).

Ряд 3 «Инфляция»:
- **p5** «CPI/PPI отпечатки, YoY» (span 12): для каждого ряда окно YoY считается по ПОЛНОМУ ряду, затем фильтр времени (иначе первые 12 точек искажены): A — `SELECT time, yoy AS "CPI SA, YoY" FROM (SELECT date AS time, value / lagInFrame(value, 12) OVER (ORDER BY date) - 1 AS yoy FROM v_bls_macro WHERE series = 'CUSR0000SA0') WHERE $__timeFilter(time) ORDER BY time`; B — то же с `series = 'WPSFD4'` и алиасом `"PPI FD, YoY"`. Единица — проценты (из пакета `units`).

Ряд 4 «Спрос: ETF и ЦБ»:
- **p6** «ETF-потоки по месяцам, мир (bar)» (span 12): `SELECT toStartOfMonth(date) AS time, flow_usd_mln AS "Поток ETF, млн $" FROM v_gold_etf_flows WHERE region = 'world' AND $__timeFilter(date) ORDER BY time` — barchart.
- **p7** «ЦБ: покупки YTD, т (bar)» (span 12): `SELECT country AS "Страна", ytd_change_tonnes AS "YTD, т" FROM v_cb_gold_dynamics ORDER BY ytd_change_tonnes` — barchart, цвет по знаку (палитра thresholds: отрицательные — красный, положительные — зелёный; допустимо `color.mode=continuous`).

Ряд 5 «Прогноз и точность»:
- **p8** «Консенсус Q4-2026 и own-сетка (bar)» (span 12): `SELECT concat(author, ' / ', scenario) AS "Прогноз", midpoint AS "Середина, USD/oz", low, high FROM v_gold_forecasts WHERE horizon = 'Q4-2026' ORDER BY midpoint` — barchart; факт — в панели p1 (не дублируется).
- **p9** «Точность прошлых прогнозов (table)» (span 12): `SELECT metric, forecast_date, target_date, predicted, actual, error_pct FROM v_forecast_accuracy WHERE is_resolved = 1 ORDER BY target_date` — table.

- [ ] **Step 3: Сборка, тесты, живой прогон**

Run: `make grafana-build && (cd grafana && go vet ./... && go test ./...)`
Expected: PASS; артефакт обновлён, повторный прогон — diff пуст.
Run: `make grafana-deploy-test`
Expected: smoke OK по ≥2 панелям; открыть дашборд в браузере: все 9 панелей отрисованы, p1 показывает цену и линию $4 000, p4/p8 bar-диаграммы заполнены, p9 содержит строки (визуальная приёмка).
Сверка «одно определение» (приёмка спеки §7.4): прогнать 2 ключевых SQL панелей (p1A и p4A) через MCP `run_query` под `kimi_reader` и убедиться, что числа совпадают со smoke-ответом Grafana (источник один — витрины).

- [ ] **Step 4: Commit**

```bash
git add grafana dashboard/gold-thesis.json
git commit -m "grafana: gold-thesis panels"
```

---

### Task 5: Валидация (`make grafana-validate`) и CI

**Files:**
- Modify: `grafana/validate.go`, `grafana/validate_test.go` (негативные правила), `Makefile` (`grafana-validate`)
- Create: `.github/workflows/dashboards.yml`

**Interfaces:**
- Consumes: артефакт и `Validate` из Task 1/4; рабочую форму `gcx resources validate` (определить здесь).
- Produces: `make grafana-validate`; CI-workflow «Dashboards».

- [ ] **Step 1: Негативные тесты валидатора (падающие)**

```go
func TestValidateRejectsDuplicatePanelID(t *testing.T) // двух панелям один id
func TestValidateRejectsMissingDatasource(t *testing.T) // панель без .Datasource
func TestValidateRejectsRawTableSQL(t *testing.T)       // target.RawSql = "SELECT 1 FROM macro_series"
```
Run: `cd grafana && go test ./...` → FAIL; дополнить `Validate` до зелёного.

- [ ] **Step 2: `make grafana-validate` и живая проверка gcx**

Цель: `cd grafana && go vet ./... && go test ./... && cd .. && gcx resources validate <вариант из Task 3> dashboard/gold-thesis.json` (точную форму `validate` зафиксировать после живой проверки; при её недоступности — оставить только Go-тесты и пометить в комментарии цели).
Run: `make grafana-validate` → зелёный.

- [ ] **Step 3: CI workflow**

`.github/workflows/dashboards.yml`:

```yaml
name: Dashboards
on:
  pull_request:
  push:
    branches: [main]
permissions:
  contents: read
env:
  GCX_VERSION: v0.0.0   # пин: подставить актуальный релиз при реализации
jobs:
  dashboards:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.27.x'
          cache-dependency-path: grafana/go.sum
      - name: tests
        run: cd grafana && go vet ./... && go test ./...
      - name: build
        run: make grafana-build
      - name: artifact is in sync
        run: git diff --exit-code -- dashboard/gold-thesis.json
      - name: install gcx
        run: |
          curl -fsSL -o /tmp/gcx.tar.gz "https://github.com/grafana/gcx/releases/download/${GCX_VERSION}/gcx_Linux_x86_64.tar.gz"
          tar -xzf /tmp/gcx.tar.gz -C /tmp gcx && chmod +x /tmp/gcx && sudo mv /tmp/gcx /usr/local/bin/gcx
          gcx --version
      - name: validate resources
        run: gcx resources validate <вариант> dashboard/gold-thesis.json
      - uses: actions/upload-artifact@v4
        with:
          name: gold-thesis.json
          path: dashboard/gold-thesis.json
```

- [ ] **Step 4: Проверка**

Run: `make grafana-validate` и локально повторить шаги workflow (`make grafana-build && git diff --exit-code -- dashboard/gold-thesis.json`); YAML — `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/dashboards.yml'))"`.
Expected: зелёное; реальный CI-прогон — на PR в Task 7.

- [ ] **Step 5: Commit**

```bash
git add grafana/validate.go grafana/validate_test.go Makefile .github/workflows/dashboards.yml
git commit -m "grafana: validation and ci"
```

---

### Task 6: Staging: провижининг датасорса и деплой-цель

**Files:**
- Create: `deploy/staging/grafana-datasource.yaml`
- Modify: `Makefile` (`GRAFANA_STAGING_URL`, `grafana-deploy-staging`, env-check), `deploy/staging/README.md` (раздел про датасорс-провижининг)

**Interfaces:**
- Consumes: `scripts/grafana_check.py` из Task 3, форма push из Task 3.
- Produces: `make grafana-deploy-staging` (туннель `ssh -N -L 3001:127.0.0.1:3000 palmshell`), идемпотентный провижининг `/etc/grafana/provisioning/datasources/clickhouse.yaml` на хосте.

- [ ] **Step 1: Шаблон для staging**

`deploy/staging/grafana-datasource.yaml` — как локальный файл Task 2, но: `host: localhost`, `username: kimi_reader`, `secureJsonData.password: "__CLICKHOUSE_PASSWORD__"`. В шапке комментарий: «плейсхолдер подставляется на хосте из /etc/clickhouse-import-rosstat.env при `make grafana-deploy-staging`; в репозитории пароля нет».

- [ ] **Step 2: Шаг провижининга (идемпотентный)**

В `grafana-deploy-staging` перед push: `scp` шаблона во временный файл на хосте; `ssh` читает env-файл, подставляет пароль, сравнивает sha256 с целевым `/etc/grafana/provisioning/datasources/clickhouse.yaml`; при отличии — `install -o root -g grafana -m 640`; печатает `OK провижининг датасорса обновлён`/`актуален`. Пароль не выводится.
Проверка: `ssh palmshell 'ls -l /etc/grafana/provisioning/datasources/'` → файл владельца root:grafana, 640; `sshd` лог Grafana: `ssh palmshell 'journalctl -u grafana-server -n 20 --no-pager'` содержит запись об апдейте датасорса (формат записи зафиксировать в отчёте).

- [ ] **Step 3: Цель деплоя**

`grafana-deploy-staging`: проверить `GRAFANA_STAGING_TOKEN`; проверить туннель (`curl -sf $(GRAFANA_STAGING_URL)/api/health`, при провале — подсказка про `ssh -N -L 3001:127.0.0.1:3000 palmshell`); провижининг (шаг 2); push (форма Task 3) с `GRAFANA_SERVER=$(GRAFANA_STAGING_URL)`; smoke `python3 scripts/grafana_check.py --uid gold-thesis` c staging-переменными. `GRAFANA_STAGING_URL ?= http://localhost:3001`; env-check дополняется наличием `GRAFANA_STAGING_TOKEN`.

- [ ] **Step 4: Приёмка (owner-assisted)**

Локально проверяемое: шаг 2 выполняется и идемпотентен; `make grafana-deploy-staging` без токена останавливается с понятным сообщением; без туннеля — с подсказкой. Полный прогон (push+smoke) выполняется с токеном владельца: `make grafana-deploy-staging` → `OK дашборд виден`, панели отдают данные; после — `gcx dashboards list` показывает `gold-thesis` (первый дашборд на staging).
Если токен недоступен — зафиксировать в отчёте шага, приёмку пункта не объявлять пройденной (Task 7 отражает статус в роадмапе честно).

- [ ] **Step 5: Commit**

```bash
git add deploy/staging Makefile
git commit -m "grafana: staging datasource provisioning and deploy"
```

---

### Task 7: Документация, статус роадмапа, финализация

**Files:**
- Modify: `README.md`, `ARCHITECTURE.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

**Interfaces:**
- Consumes: все цели, переменные и факты приёмки предыдущих тасков.
- Produces: синхронизированные доки, обновлённый статус §4, PR.

- [ ] **Step 1: Доки по скилу**

Вызвать скил `sync-readme-architecture` и обновить по нему: README (раздел Grafana: конвейер «код → артефакт → валидация → тест-стенд → staging», таблица новых make-целей, env-переменные `GRAFANA_TEST_*`/`GRAFANA_STAGING_*`, одноразовые шаги сервис-аккаунтов, туннель :3001) и ARCHITECTURE.md (новый модуль `grafana/`, CI-workflow, схема деплоя).

- [ ] **Step 2: Статус пункта в роадмапе**

`docs/GOLD_THESIS_DATA_ROADMAP.md` §4 — отметить пункт выполненным с фактами приёмки (числа из Task 3/4/6: 9 панелей, артефакт+идемпотентность, локальный цикл, staging — с честной пометкой, если полный staging-прогон остался owner-assisted). Отметить, что §3-витрины и миграция легаси-дашбордов — отдельные follow-up.

- [ ] **Step 3: Финальные проверки**

Run: `make all && make mcp-check && (cd grafana && go test ./...) && make grafana-validate`
Expected: всё зелёное; `git status` чистый.

- [ ] **Step 4: Commit и PR**

```bash
git add README.md ARCHITECTURE.md docs/GOLD_THESIS_DATA_ROADMAP.md
git commit -m "docs: sync grafana dashboards pipeline"
gh pr create --base main --title "Фаза 4 роадмапа золота: дашборд gold-thesis как код + provisioning CI/CD (issue #23)" \
  --body "Спека: docs/superpowers/specs/2026-10-12-grafana-gold-thesis-design.md; план: docs/superpowers/plans/2026-10-12-grafana-gold-thesis.md; запуск: make grafana-build/validate/deploy-test/deploy-staging; проверка: make grafana-check, make mcp-check"
```

PR открывается один; владелец смотрит и решает merge. После merge: применить на staging провижининг и `make grafana-deploy-staging` (runbook из README).
