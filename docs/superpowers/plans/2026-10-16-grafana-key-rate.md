# Grafana `key-rate-publications` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дашборд Grafana `key-rate-publications` живёт как код в модуле `grafana/` (графики-иллюстрации «что произошло с данными» для `docs/KEY_RATE_PUBLICATIONS.md`), покрывает все данные статьи и проходит конвейер сборки/деплоя наравне с `gold-thesis`.

**Architecture:** Новый builder `NewKeyRatePublications()` в пакете `main` модуля `grafana/` (упорядоченные ряды/панели, SQL только по витринам `v_*`), генерация артефакта `dashboard/key-rate-publications.json` через расширенный `main.go` (`-dashboard`/`-out-dir`), Makefile и CI собирают/диффят/линтят оба дашборда.

**Tech Stack:** Go 1.27, `grafana-foundation-sdk`, плагин `grafana-clickhouse-datasource` 4.3.2 (датасорс `ch-rosstat`), ClickHouse 26.10, Make, GitHub Actions, `gcx` 1.5.0.

**Spec:** `docs/superpowers/specs/2026-10-16-grafana-key-rate-design.md` — дословный контракт (SQL всех 19 панелей, конвенции из UI-экспорта, приёмка). Исполнитель читает спеку и план вместе; SQL берётся из спеки §5 без «улучшений».

## Global Constraints

- Каждый таргет читает **только витрины `v_*`** (инвариант `Validate`); `FINAL` в панельных запросах не писать (он внутри витрин); `LIMIT` — только в table-запросах, на timeseries запрещён.
- Датасорс каждой панели — константы `DatasourceType`/`DatasourceUID` (`ch-rosstat`); `editorType: "sql"`, `pluginVersion: "4.3.2"` — уже в `NewCHQuery`.
- id панелей/рядов — явные, ненулевые, уникальные (панели 1–19, ряды 21–24).
- Единицы: `percentunit` — доля (SQL отдаёт `pow(...)-1`), `percent` — ставки/п.п., `currencyRUB` — млрд руб., `short` — пункты/индекс; смешение на панели — только пер-серийным `unit`-override. Одна серия — одни единицы, без «двух формул одного ряда».
- Окно — `$__timeFilter(date)`; YoY/темпы — окном по полному ряду с фильтром снаружи (образец — `yoyTemplate` в `gold_thesis.go`); прошлые годы — пунктир `{"dash":[10,10],"fill":"dash"}`; разрежённые серии — точки (`custom.lineWidth: 0` + `custom.showPoints: "always"`); пороги — со `ThresholdsStyle` ≠ off.
- SQL-идиомы и визуал — из UI-экспорта `grafana_dashboard_rosstat` по спеке §6; запросы UI-экспорта на сырых таблицах НЕ копировать.
- Алиасы серий — русские `AS "…"`; `ORDER BY date/time ASC`; refId осмысленный (легенда = `<refId> <alias>`, по нему матчеры override).
- Детерминизм: список дашбордов в `main.go` — упорядоченный слайс; повторный прогон не меняет артефакты (на этом стоит CI).
- Комментарии — в стиле `gold_thesis.go`: русские, поясняют неочевидное (формулы, источник, почему override/порог); без «пересказа кода».
- Новых зависимостей не добавлять (`grafana/go.mod` не трогать).
- Модели субагентов: имплементер — `deepseek/deepseek-flash` (явный `model`), ревью задачи — `anthropic/haiku`, финальное ревью ветки — `deepseek/deepseek-flash`. В каждый промпт — блок AGENTS.md про `mcp__jetbrains__*`/`mcp__clickhouse__*`; SQL-проверки — `mcp__clickhouse__run_query`; сборки/тесты — через `make`.

## Review Focus

1. **Смешанные единицы (панели 5, 9, 13, 16).** Перепутанный `unit`-override даёт десятикратную ошибку чтения («пункты» как проценты). Тесты: `TestKeyRateUnits` (T1 — панель 4; T2 — панель 9-B; T3 — панели 5-C/13).
2. **Сдвинутые окна на границе года (панель 2).** Наложение прошлого года ломается, если сдвиг времени и фильтр года разъезжаются. Тест: `TestKeyRateWeeklyYearOverlay` (T1) — `date_add(YEAR, 1, date)` и `toYear(now()) - 1` в одном таргете.
3. **Ряды без данных (теноры ОФЗ — 1 точка; KKG7 без сделок).** Линия по одной точке не читается, серия «исчезает». Тест: `TestKeyRateSparseSeriesPoints` (T3) — overrides `lineWidth 0` + `showPoints always`.
4. **JOIN с `v_series_catalog` по (source, series) (панели 2, 3).** Товар без строки каталога молча выпадает из `avg/count` (счёт становится неполным). Тест: в T1 `TestKeyRateAllTargetsUseViews` + проверка наличия `JOIN v_series_catalog` и `frequency = 'W'` в обоих недельных запросах.
5. **Гранты `kimi_reader`.** Панель на витрине без гранта валит staging-smoke. Тест — процедурный: T7 сверяет `sql/mcp_kimi_reader.sql` со списком витрин из спеки §2 (все 14 выданы; при добавлении витрины — отдельный пункт).

---

### Task 1: Каркас, селектор сборки и ряд «Ставка и инфляция» (панели 1–5)

**Files:**
- Create: `grafana/key_rate_publications.go`
- Create: `grafana/key_rate_publications_test.go`
- Modify: `grafana/clickhouse.go` (переименовать `DashboardUID` → `GoldThesisUID`)
- Modify: `grafana/validate.go` (`Validate(d dashboard.Dashboard, wantUID string) error`)
- Modify: `grafana/gold_thesis.go` (`Uid(GoldThesisUID)`; `Manifest` берёт имя из `*d.Uid`)
- Modify: `grafana/gold_thesis_test.go` (адаптировать вызовы `Validate`/`Manifest`/константу — не более)
- Modify: `grafana/main.go` (флаги `-dashboard`, `-out-dir`; упорядоченный список дашбордов)
- Create (генерируется): `dashboard/key-rate-publications.json`

**Interfaces:**
- Consumes: `CHQuery`/`NewCHQuery` (`grafana/clickhouse.go`); `datasourceRef`, `timeseriesQuery`, `tableQuery`, `pointsOnlyProps`, `axisPlacementRightProp`, `ptr` (`grafana/gold_thesis.go`).
- Produces: `const KeyRatePublicationsUID = "key-rate-publications"`; `NewKeyRatePublications() *dashboard.DashboardBuilder`; `Validate(d dashboard.Dashboard, wantUID string) error`; `Manifest(d dashboard.Dashboard) resource.Manifest` (name = `*d.Uid`); CLI `go run . -dashboard <uid|all> -out-dir <dir>` (панели 1–5, ряд id 21).

- [ ] **Step 1: Тесты (red)**

`grafana/key_rate_publications_test.go` — имена и ассерты этой задачи:
- `TestKeyRatePublicationsPanelCount` — панелей 5 (число обновляется в T2–T4);
- `TestKeyRatePublicationsRowTitles` — заголовки рядов `["Ставка и инфляция"]` (обновляется в T2–T4);
- `TestKeyRateAllTargetsUseViews` — у всех таргетов всех панелей `strings.Contains(rawSql, "FROM v_")`;
- `TestKeyRatePanelIDsUnique` — `Validate(NewKeyRatePublications().Build(), KeyRatePublicationsUID)` без ошибки; тот же вызов с чужим uid — ошибка (проверка параметризации `Validate`);
- `TestKeyRateWeeklyYearOverlay` — у панели 2 второй таргет содержит `date_add(YEAR, 1, date)` и `toYear(now()) - 1`;
- `TestKeyRateWeeklyJoinCatalog` — у панелей 2 и 3 — `JOIN v_series_catalog` и `frequency = 'W'`;
- `TestKeyRateUnits` — панель 4 unit `percentunit`; панель 1 unit `percent`;
- `TestKeyRateThresholdsStyleSet` — у панелей 2 и 4 `ThresholdsStyle` ≠ `off` и шаг `0.04`;
- `TestKeyRateGenerateDeterministic` — две сборки `NewKeyRatePublications().Build()` дают байт-идентичный JSON (как `TestGenerateDeterministic` в gold).

- [ ] **Step 2: Прогон — красный**

Run: `cd grafana && go test ./...`
Expected: FAIL — `undefined: NewKeyRatePublications`.

- [ ] **Step 3: Реализация**

1) `clickhouse.go`: `DashboardUID` → `GoldThesisUID`; обновить ссылки (`validate.go`, `gold_thesis.go`, тесты).
2) `validate.go`: сигнатура `Validate(d dashboard.Dashboard, wantUID string) error`; внутри — сравнение с `wantUID` (передать в `validateIDs` как есть — там uid не нужен; правится только проверка uid).
3) `gold_thesis.go`: `Uid(GoldThesisUID)`; `Manifest`: `Metadata: resource.Metadata{Name: *d.Uid}` (uid гарантированно непустой — проверяет `Validate`; в комментарии сохранить инвариант «Metadata.Name и spec.uid равны»).
4) `key_rate_publications.go` — по спеке §4–§5: константа uid; `NewKeyRatePublications()` = `dashboard.NewDashboardBuilder("Ключевая ставка — публикации").Uid(KeyRatePublicationsUID).Tags([]string{"key-rate", "publications"}).Refresh("30m").Time("now-3y", "now").Timezone(common.TimeZoneBrowser)` + `WithRow(dashboard.NewRowBuilder("Ставка и инфляция").Id(21))` + панели 1–5 (`krRatePanel`, `krWeeklyAnnualizedPanel`, `krWeeklyRisersPanel`, `krInflationPanel`, `krExpectationsPanel`; имена хелперов — с префиксом `kr`, в пакете уже есть `inflationPanel`/`crisisPanel` золота). SQL — из спеки §5**дословно** (включая JOIN каталога, формулы `pow(avg(value)/100, 52) - 1`, `pow(value/100, 12) - 1`, `pow(1 + value/100, 12) - 1`, `countIf(value > 100) / count() * 100`); панель 2: A — текущий год, B — прошлый год с `date_add`; панель 5: C — override `unit: "short"` + fixed color; пороги 0.04 (`ThresholdsStyle` line) у панелей 2 и 4; у панелей — `Description` по спеке; panel id 1–5, `Span(12)`.
5) `main.go`: слайс `dashboards := []struct{ uid string; build func() *dashboard.DashboardBuilder }{{GoldThesisUID, NewGoldThesis}, {KeyRatePublicationsUID, NewKeyRatePublications}}`; флаги `-dashboard` (дефолт `all`) и `-out-dir` (дефолт `../dashboard`); для каждого выбранного: Build → `Validate(board, uid)` → `Manifest` → запись `filepath.Join(outDir, uid+".json")`; обновить doc-комментарий команды.

- [ ] **Step 4: Прогон — зелёный**

Run: `cd grafana && go vet ./... && go test ./...`
Expected: PASS (в т.ч. обновлённые gold-тесты).

- [ ] **Step 5: Артефакт и детерминизм**

Run: `cd grafana && go run . -dashboard key-rate-publications -out-dir ../dashboard && git status --short` → `dashboard/key-rate-publications.json` создан; повторить прогон → файл не меняется (`git diff --stat` пуст для старого файла, повторная запись идентична). Проверить, что `dashboard/gold-thesis.json` не изменился (`git diff --exit-code -- ../dashboard/gold-thesis.json` — с учётом переименований в коде артефакт gold должен остаться байт-в-байт; если разошёлся — разобраться до коммита).

- [ ] **Step 6: Commit**

```bash
git add grafana/ dashboard/key-rate-publications.json dashboard/gold-thesis.json
git commit -m "grafana: key-rate-publications scaffold and rate/inflation row"
```

### Task 2: Ряд «Деньги, кредит, бюджет, труд» (панели 6–10)

**Files:**
- Modify: `grafana/key_rate_publications.go` (ряд id 22, панели 6–10)
- Modify: `grafana/key_rate_publications_test.go` (счёт 10, ряды 2, новые тесты)
- Modify (генерируется): `dashboard/key-rate-publications.json`

**Interfaces:**
- Consumes: `NewKeyRatePublications()` из T1; хелперы `kr…Panel`/`kr…SQL`.
- Produces: панели 6–10 (SQL — спека §5): М2/кредит г/г (subquery-паттерн), ставки vs ключевая, бюджет (`source='minfin_mesyats'` — **проверить живьём через `mcp__clickhouse__run_query`, что все три ряда есть и свежи**), труд, Сбер.

- [ ] **Step 1: Тесты (red)**

Добавить/обновить:
- `TestKeyRatePublicationsPanelCount` → 10; `TestKeyRatePublicationsRowTitles` → +`"Деньги, кредит, бюджет, труд"`;
- `TestKeyRateYoYPanelsFilterOutsideWindow` — новые yoy-таргеты панелей 6 и 9: `lagInFrame` присутствует, `$__timeFilter(time)` стоит после закрывающей скобки подзапроса (по образцу gold-теста);
- `TestKeyRateUnits` → панель 9-B: override `unit = percentunit`.

- [ ] **Step 2: Прогон — красный.** Run: `cd grafana && go test ./...` → FAIL (панелей пока 5).

- [ ] **Step 3: Реализация** — SQL панелей 6–10 дословно из спеки §5; источники бюджета — `source = 'minfin_mesyats'` (если живая проверка покажет иное — остановиться и зафиксировать расхождение в отчёте, не выдумывать); параметры: панель 10 — `Span(24)`; порог нуля у панели 6 (`[{nil,red},{0,green}]`, line); у панели 9 — правая ось для зарплаты; `Description` у 10 — атрибуция СберИндекс + лицензионная оговорка.

- [ ] **Step 4: Прогон — зелёный.** Run: `cd grafana && go vet ./... && go test ./...` → PASS.

- [ ] **Step 5: Артефакт.** Run: `cd grafana && go run . -dashboard key-rate-publications -out-dir ../dashboard` (детерминизм — как в T1).

- [ ] **Step 6: Commit** — `git commit -m "grafana: key-rate-publications money/credit/budget/labour row"`.

### Task 3: Ряд «Рыночный контур» (панели 11–14)

**Files:**
- Modify: `grafana/key_rate_publications.go` (ряд id 23, панели 11–14)
- Modify: `grafana/key_rate_publications_test.go`
- Modify (генерируется): `dashboard/key-rate-publications.json`

**Interfaces:**
- Produces: панели 11–14 (спека §5): KEYRATE траектория (3 серии, KKG7 — точки), срез-таблица с ликвидностью, ОФЗ+RGBI, RUONIA-спред (самоджойн витрины).

- [ ] **Step 1: Тесты (red)**
- счёт 14; ряды → +`"Рыночный контур"`;
- `TestKeyRateSparseSeriesPoints` — панель 11: override KKG7 c `custom.lineWidth: 0` и `custom.showPoints: "always"`; панель 13: у теноров то же;
- `TestKeyRateUnits` → панели 5-C (`short`) и 13-A (`short` для RGBI).

- [ ] **Step 2: Прогон — красный.**
- [ ] **Step 3: Реализация** — дословно по спеке §5; панель 12 — `tableQuery` (LIMIT 1 подзапрос — как в спеке); панель 13 — B..E алиасы `"1y, %"` и т.д.
- [ ] **Step 4: Прогон — зелёный.**
- [ ] **Step 5: Артефакт** — regenerate, детерминизм.
- [ ] **Step 6: Commit** — `grafana: key-rate-publications market contour row`.

### Task 4: Ряд «Сигнал, консенсус, календарь» (панели 15–19)

**Files:**
- Modify: `grafana/key_rate_publications.go` (ряд id 24, панели 15–19)
- Modify: `grafana/key_rate_publications_test.go`
- Modify (генерируется): `dashboard/key-rate-publications.json`

**Interfaces:**
- Produces: панели 15–19 (спека §5): консенсус (медианы+p10/p90), резюме-таблица, каналы-таблица (regex + risk_pro/risk_dez последнего заседания), календарь, лента.

- [ ] **Step 1: Тесты (red)** — счёт 19 (финальное имя `TestKeyRatePublicationsHasNineteenPanels` вместо промежуточного счётного — переименовать); ряды → 4 заголовка; все 19 таргетов проходят `FROM v_`.
- [ ] **Step 2: Прогон — красный.**
- [ ] **Step 3: Реализация** — дословно по спеке §5 (табличные запросы — `tableQuery`).
- [ ] **Step 4: Прогон — зелёный.**
- [ ] **Step 5: Артефакт** — regenerate + детерминизм (`git diff --stat` пуст).
- [ ] **Step 6: Commit** — `grafana: key-rate-publications signal/consensus/calendar row`.

### Task 5: Сборка и деплой обоих дашбордов (Makefile + CI)

**Files:**
- Modify: `Makefile` (секция «Дашборды Grafana как код»: `DASHBOARDS := gold-thesis key-rate-publications`; цели `grafana-build`, `grafana-deploy-test`, `grafana-check`, `grafana-deploy-staging` — цикл по списку)
- Modify: `.github/workflows/dashboards.yml` (сборка обоих, `git diff --exit-code -- dashboard/`, lint по каждому файлу, upload обоих)
- Modify: `grafana/main.go` (doc-комментарий `-dashboard`/`-out-dir`, если не сделан в T1)

**Interfaces:**
- Consumes: артефакты T1–T4; `scripts/grafana_check.py --uid` (без изменений).
- Produces: `make grafana-build` собирает оба; `make grafana-check` проверяет оба uid.

- [ ] **Step 1: Makefile** — правки секции; сохранить идемпотентность и комментарии-обоснования (стиль репозитория); `make grafana-build` дважды → пустой `git diff`.
- [ ] **Step 2: CI** — правки workflow (комментарии — как в существующем: почему нет серверной валидации и т.п.).
- [ ] **Step 3: Проверки**

Run: `make grafana-validate && make grafana-build && git diff --exit-code -- dashboard/`
Expected: зелёные; артефакты не изменились.

- [ ] **Step 4: Commit** — `grafana: build and deploy both dashboards`.

### Task 6: Документация

**Files:**
- Modify: `docs/KEY_RATE_PUBLICATIONS.md` (раздел «Иллюстрации: дашборд `key-rate-publications`» — таблица «раздел статьи → панели», где смотреть: локальный стенд (`make grafana-up`, `make grafana-deploy-test`) и staging; правило «дашборд — код: `make grafana-build`, руками не править»)
- Modify: `README.md` (раздел Grafana: второй дашборд, синтаксис `-dashboard`/`-out-dir`, артефакты, цели)
- Modify: `ARCHITECTURE.md` (графа `grafana/`, `dashboard/`, секция про дашборды: два дашборда, CLI, CI)
- Modify: `docs/ROADMAP_KEY_RATE.md` (§6 фаза 4: «панель Grafana `key-rate.json`» → `key-rate-publications.json`, статус ✅ с датой и объёмом v1)

- [ ] **Step 1: Синк документации** — скил `sync-readme-architecture` (`.kimi-code/skills/sync-readme-architecture/SKILL.md`): пройти по его чек-листу; правки — по фактическому состоянию кода; в ROADMAP не затрагивать чужие правки (ветка `key-rate-model-v0`).
- [ ] **Step 2: Проверка согласованности** — grep: в README/ARCHITECTURE не осталось `-out`-примеров старого синтаксиса и утверждений «дашборд один»; в статье — раздел на месте и таблица панелей совпадает с артефактом (числа панелей).
- [ ] **Step 3: Commit** — `docs: key-rate publications illustrations section and grafana docs`.

### Task 7: Приёмка, живая сверка и финальное ревью

**Files:**
- Modify: `.superpowers/sdd/2026-10-16-grafana-key-rate/progress.md` (создать; итог приёмки)

- [ ] **Step 1: Полный прогон** — `make all` (в корне), `make grafana-validate`, `make grafana-build` + `git diff --exit-code -- dashboard/`.
- [ ] **Step 2: Живая SQL-сверка всех таргетов** — выгрузить `rawSql` всех таргетов из `dashboard/key-rate-publications.json` (python3) и прогнать каждый через `mcp__clickhouse__run_query`, заменив `$__timeFilter(<col>)` на `<col> >= '2023-10-16' AND <col> <= '2026-10-16'` (для панели 10/2-3 — окно 1 год); зафиксировать в отчёте: таргетов всего, непустых, разрежённость (теноры ОФЗ, KKG7, табличные срезы), найденные расхождения.
- [ ] **Step 3: Локальный деплой и smoke** (owner-assisted при отсутствии токена) — `make grafana-deploy-test`; smoke `make grafana-check` (оба uid): дашборд виден, первые панели отдают точки; при возможности — визуальный просмотр в браузере.
- [ ] **Step 4: Протокол приёмки** — подготовить текст комментария в PR (команды, числа, что не проверено) по стандарту `docs/IMPORTER_PACKAGE_STANDARD.md` §16.1 п. 8.
- [ ] **Step 5: Финальное ревью ветки** — `superpowers:requesting-code-review` (ревьюер — `deepseek/deepseek-flash`), затем `superpowers:verification-before-completion`; fix-раунды — тем же имплементером.
- [ ] **Step 6: Commit** — `docs: key-rate dashboard acceptance notes`.

---

## Self-Review (выполнен автором плана)

- **Покрытие спеки:** §4–5 → T1–T4 (SQL дословно); §6 → констрейнты T1–T4; §7 → T1 (структура/тесты), T5 (Makefile/CI); §7-док и §8-доки → T6; §8-приёмка → T7; §9 → границы зафиксированы, ничего из них в задачах нет.
- **Скан шагов:** каждая задача — тесты-red → реализация → green → артефакт → commit; шаги без «доведи сам»; числовые значения (0.04, окна дат, имена рядов) — из спеки, не из головы.
- **Согласованность имён:** `KeyRatePublicationsUID`, `NewKeyRatePublications`, `GoldThesisUID`, `Validate(d, wantUID)`, `Manifest(d)` — единые во всех задачах и тестах; хелперы `kr…` не конфликтуют с gold.
- **Review Focus:** пункты 1–4 привязаны к тестам задач (указаны в Review Focus и в шагах тестов); пункт 5 — процедурный шаг T7.
- **Пропорция:** план не воспроизводит тела билдеров (их пишет имплементер по спеке); SQL-контракт живёт в спеке §5 — один источник.
