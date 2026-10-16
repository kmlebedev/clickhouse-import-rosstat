# Grafana `finance-polyus` + шаг 0 (`dashboard-migration`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Скил `dashboard-migration` (механизм миграции/добавления дашбордов + очередь) и миграция легаси-дашборда `dashboard/finance-polyus.json` в код `grafana/`: 19 панелей на существующих витринах — макро-входы прогноза, операционные и финансовые показатели, прогнозные точки (веер own-сценариев, точность) — с полной приёмкой как у gold-thesis/key-rate-publications.

**Architecture:** Новый builder `NewFinancePolyus()` в пакете `main` модуля `grafana/` (SQL только по `v_*`, дословный контракт — спека §5); артефакт `dashboard/finance-polyus.json` **заменяет** легаси-экспорт (история — git); Makefile/CI ведут три дашборда; скил + пункты роадмапов — первыми коммитами.

**Tech Stack:** Go 1.27, `grafana-foundation-sdk`, `grafana-clickhouse-datasource` 4.3.2, ClickHouse 26.10, Make, GitHub Actions, `gcx` 1.5.0.

**Spec:** `docs/superpowers/specs/2026-10-16-grafana-finance-polyus-design.md` — нормативный контракт (SQL всех 19 панелей §5, скил §3, приёмка §7, границы §8). Исполнитель читает спеку и план вместе; SQL берётся из спеки §5 без «улучшений».

## Global Constraints

- Каждый таргет — только `v_*` (`Validate` требует `FROM v_`); `FINAL` не писать; `LIMIT` запрещён на timeseries (таблицы 18/19 — без `LIMIT`, строк мало).
- Датасорс — константы `ch-rosstat`; id панелей 1–19, ряды 31–34 — явные, уникальные.
- Единицы: `currencyRUB` (панели 1, 6), `currencyUSD` (2, 12–16), `percent` (4, 5), `short` (3 + decimals 4; 7–11, 14), decimals 2 у панели 1.
- Разрежённые серии — точки (`custom.lineWidth: 0` + `custom.showPoints: "always"`): панели 4-B..E (теноры ОФЗ), 15 (TCC), 16-B..D (own-сценарии). Разные размерности на панели — правой осью (`axisPlacement: right`): 10-B (м³/т против тыс. м³), 11-B (% против г/т).
- Панели 12–15: ось времени вычисляется из `period` (`multiIf(endsWith(period,'H1'), …, …)`), `$__timeFilter` **не применяется осознанно** (паттерн gold `dxy`/`crack`; окно дашборда клипает) — фиксируется тестом и описанием панели.
- Панель 16: маппинг горизонта — регуляркой по ярлыку (`^(Q[1-4]|YE)-[0-9]{4}$` → 31 декабря года; `^[0-9]{4}$` → 31 декабря), немэппируемые отсекаются `time IS NOT NULL`; **никаких хардкодов годов и `UNION ALL`-точек в SQL** (анти-паттерны легаси).
- Русские алиасы `AS "…"`, `ORDER BY time/date ASC`, у каждой панели `Description`; комментарии — в стиле `gold_thesis.go`/`key_rate_publications.go`.
- Детерминизм: повторный `go run . -out-dir ../dashboard` не меняет ни один артефакт; `gold-thesis.json` и `key-rate-publications.json` — байт-в-байт.
- Новых зависимостей нет (`grafana/go.mod` не трогать); имена хелперов нового файла — с префиксом `fp` (не конфликтуют с gold/`kr`).
- Скил `.kimi-code/skills/dashboard-migration/SKILL.md` пишется по спеке §3 (3 режима, 9-шаговый чек-лист, ссылки на нормативные конвенции, очередь); пункты роадмапов — дословно по спеке §1/§3.
- Дисциплина субагентов: имплементеры — `deepseek/deepseek-flash` (все задачи), ревью задач — `anthropic/haiku`, финальное ревью — `deepseek/deepseek-flash`; fix-раунды ≤ 5; в каждый диспатч — блок MCP из AGENTS.md.

## Review Focus

1. **Панели 12–15 без `$__timeFilter`** — риск: кто-то «починит» добавлением фильтра по неизвестной колонке и сломает запрос. Тест T4: в SQL этих панелей нет `$__timeFilter`, есть `multiIf(`; в описании — оговорка.
2. **Маппинг горизонтов (панель 16)** — риск: хардкод годов/даты (анти-паттерн легаси) или молчаливое включение `12M`/`short`. Тест T5: SQL содержит `match(horizon`, `time IS NOT NULL`, `NULL`; не содержит `UNION ALL` и литералов `'20`.
3. **Смешанные единицы/оси (2, 10-B, 11-B, 14, 15)** — риск: перепутанный юнит даёт ошибку чтения. Тест: `TestFinancePolyusUnits` (точные юниты по панелям), `TestFinancePolyusRightAxis` (10-B/11-B), точки (4-B..E/15/16-B..D).
4. **JOIN панели 2-A** — риск: `INNER JOIN` двух рядов одной витрины по дате; выпадение дней без курса. Тест: SQL содержит `INNER JOIN v_cbr_macro` и `31.1034768`; живая сверка в T8 (непустые точки).
5. **Замена легаси-экспорта** — риск: артефакт закоммичен, но легаси остался вторым файлом/потерян. Проверка: в `dashboard/` один `finance-polyus.json` (генерированный), удаления/добавления видны в diff; README-список легаси-экспортов обновлён (остаются `rosstat.json`, `chmf.json`).

---

### Task 1: Скил `dashboard-migration` + регистрация пунктов роадмапов

**Files:**
- Create: `.kimi-code/skills/dashboard-migration/SKILL.md`
- Modify: `docs/ROADMAP_INFRA.md` (Фаза 2 «Процесс приёмки» — новый пункт «Шаг 0: механизм миграции и добавления дашбордов»)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (новый пункт «Дашборд статьи PLZL» — статус «⏳ этим PR», остаток ❌)
- Modify: `docs/GOLD_THESIS_DATA_ROADMAP.md` (§4 follow-up: пометка «механизм и очередь — ROADMAP_INFRA; finance-polyus ✅ после мержа PR»)

**Interfaces:**
- Consumes: спека §1 (формулировки пунктов) и §3 (нормативное содержание скила).
- Produces: скил-файл и зарегистрированные пункты — на них ссылаются T7 (финализация статусов) и PR.

- [ ] **Step 1: Скил.** Написать `SKILL.md` по спеке §3: назначение (3 режима A/B/C), 9-шаговый чек-лист, блок «Конвенции» со ссылками на `ARCHITECTURE.md` §6.7 и спеки gold/key-rate §6, блок «Очередь» (finance-polyus → chmf → rosstat + корневой экспорт → банки/металлы и др.), короткий пример «что читать перед стартом» (спека текущего дашборда, `sql/mcp_kimi_reader.sql`, `views/*.go`). Формат — как у `sync-readme-architecture/SKILL.md` (frontmatter `name`/`description`, затем инструкции).
- [ ] **Step 2: Роадмапы.** Внести пункты дословно по спеке: INFRA — механизм + очередь (статус «⏳ этим PR»); POLYUS — «Дашборд статьи PLZL» (v1 «⏳ этим PR», остаток ❌: витрины `mine_plans`/`price_decks`/`model_runs`, панели «план vs наш прогноз vs факт», НАВ по активам, сценарии НАВ, апсайд); GOLD §4 — пометка follow-up.
- [ ] **Step 3: Проверка.** `git grep -n "dashboard-migration" .kimi-code docs` — ссылки согласованы; `git diff --stat` — только четыре файла; в INFRA пункт не дублирует существующие.
- [ ] **Step 4: Commit** — `docs: dashboard migration skill and roadmap items`.

### Task 2: Каркас + ряд «Золото, ставки, рынок» (панели 1–6)

**Files:**
- Create: `grafana/finance_polyus.go`
- Create: `grafana/finance_polyus_test.go`
- Modify: `grafana/main.go` (константа `FinancePolyusUID`, ordered slice + запись `{FinancePolyusUID, NewFinancePolyus}`)
- Create (генерируется): `dashboard/finance-polyus.json` (заменяет легаси-экспорт — удаление старого файла входит в коммит)

**Interfaces:**
- Consumes: `CHQuery`/`NewCHQuery`, `datasourceRef`, `timeseriesQuery`, `tableQuery`, `pointsOnlyProps`, `axisPlacementRightProp`, `ptr` (из `grafana/*.go`).
- Produces: `const FinancePolyusUID = "finance-polyus"`; `NewFinancePolyus() *dashboard.DashboardBuilder`; хелперы `fp…Panel()/fp…SQL`; ряд 31, панели 1–6.

- [ ] **Step 1: Тесты (red)** — `grafana/finance_polyus_test.go`:
  - `TestFinancePolyusPanelCount` (6; растёт в T3–T5), `TestFinancePolyusRowTitles` (`["Золото, ставки, рынок"]`);
  - `TestFinancePolyusAllTargetsUseViews` (каждый `rawSql` содержит `FROM v_`), `TestFinancePolyusPanelIDsUnique` (через `Validate(…, FinancePolyusUID)`), `TestFinancePolyusUnits` (панель 1 `currencyRUB`; панель 2 `currencyUSD`; панель 4/5 `percent`), `TestFinancePolyusDcfParityJoin` (панель 2-A: `INNER JOIN v_cbr_macro` + `31.1034768`), `TestFinancePolyusSparseSeriesPoints` (панель 4-B..E — `lineWidth 0` + `showPoints always`), `TestFinancePolyusGenerateDeterministic` (две сборки — байт-идентично).
- [ ] **Step 2: Прогон — красный.** Run: `cd grafana && go test ./...` → FAIL (`undefined: NewFinancePolyus`).
- [ ] **Step 3: Реализация.** `finance_polyus.go`: uid-константа; `NewFinancePolyus()` = builder («Полюс (PLZL) — финансы и прогноз», tags `["polyus","dcf","article"]`, `Refresh("1h")`, `Time("now-5y","now")`, `Timezone(browser)`) + `WithRow(NewRowBuilder("Золото, ставки, рынок").Id(31))` + панели 1–6 (Span 24 — панель 2), SQL дословно из спеки §5, `Description` у каждой; `main.go` — регистрация в ordered slice.
- [ ] **Step 4: Прогон — зелёный.** Run: `cd grafana && go vet ./... && go test ./...` → PASS.
- [ ] **Step 5: Артефакт.** Run: `cd grafana && go run . -out-dir ../dashboard` → новый `finance-polyus.json`; повторный прогон — без diff; `git diff --exit-code -- dashboard/gold-thesis.json dashboard/key-rate-publications.json`.
- [ ] **Step 6: Commit** — `grafana: finance-polyus scaffold and gold/rates/market row`.

### Task 3: Ряд «Операционные показатели» (панели 7–11)

**Files:** Modify `grafana/finance_polyus.go`, `grafana/finance_polyus_test.go`; regenerate `dashboard/finance-polyus.json`.

**Interfaces:** Produces: ряд 32, панели 7–11 (SQL — спека §5: v_company_operating, консолидированно и по 9 активам; единицы из алиасов; 10-B/11-B — правая ось).

- [ ] **Step 1: Тесты (red)** — `TestFinancePolyusPanelCount` → 11; `TestFinancePolyusRowTitles` → +`"Операционные показатели"`; `TestFinancePolyusRightAxis` (10-B, 11-B — `axisPlacement: right`); `TestFinancePolyusNineAssets` (панель 8: 9 таргетов, алиасы активов).
- [ ] **Step 2: Прогон — красный.**
- [ ] **Step 3: Реализация** — SQL панелей 7–11 дословно; Span 24 у 7/8.
- [ ] **Step 4: Прогон — зелёный.** + `git diff --exit-code` по двум старым артефактам.
- [ ] **Step 5: Артефакт** — regenerate + детерминизм.
- [ ] **Step 6: Commit** — `grafana: finance-polyus operating row`.

### Task 4: Ряд «Финансовые показатели» (панели 12–15)

**Files:** те же.

**Interfaces:** Produces: ряд 33, панели 12–15 (SQL — спека §5; ось из `period`, без `$__timeFilter` — осознанно; панель 15 — точки).

- [ ] **Step 1: Тесты (red)** — счёт 15; ряды +; `TestFinancePolyusNoTimeFilterOnDerivedAxis` (12–15: нет `$__timeFilter`, есть `multiIf(`); `TestFinancePolyusUnits` += 12–15 `currencyUSD`/`short`; точки панели 15.
- [ ] **Step 2: Прогон — красный.**
- [ ] **Step 3: Реализация** — SQL дословно; в описаниях 12–15 — оговорка про ось из периода и отсутствие оконного фильтра.
- [ ] **Step 4–5: Прогон, артефакт** — как выше.
- [ ] **Step 6: Commit** — `grafana: finance-polyus financials row`.

### Task 5: Ряд «Прогноз и точность» (панели 16–19)

**Files:** те же.

**Interfaces:** Produces: ряд 34, панели 16–19 (SQL — спека §5: точки own-сценариев с regexp-маппингом; бар сценариев/консенсуса; таблицы прогона и точности).

- [ ] **Step 1: Тесты (red)** — счётный тест **переименовать** в `TestFinancePolyusHasNineteenPanels` (19); ряды → 4 заголовка; `TestFinancePolyusHorizonMapping` (панель 16-B..D: `match(horizon`, `time IS NOT NULL`, `NULL`; нет `UNION ALL`, нет литералов `'20`); `TestFinancePolyusNoInlinePoints` (во всём файле SQL нет `UNION ALL SELECT toDateTime` и `toDateTime('20`); табличные панели 18/19 — `queryType table`, format 1.
- [ ] **Step 2: Прогон — красный.**
- [ ] **Step 3: Реализация** — SQL дословно; панель 16-B..D — overrides точек + фикс-цвета; 17 — `tableQuery` + `XField("Прогноз")`.
- [ ] **Step 4–5: Прогон, артефакт** — детерминизм; старые артефакты байт-в-байт.
- [ ] **Step 6: Commit** — `grafana: finance-polyus forecast and accuracy row`.

### Task 6: Интеграция (Makefile + CI)

**Files:** Modify `Makefile` (`DASHBOARDS := gold-thesis key-rate-publications finance-polyus`); Modify `.github/workflows/dashboards.yml` (списки lint/upload — добавить `finance-polyus.json`); при необходимости — комментарии.

**Interfaces:** Consumes: артефакты T2–T5. Produces: три дашборда в `grafana-build`/deploy/check и в CI.

- [ ] **Step 1: Правки** Makefile/CI (комментарии — по факту «три дашборда»; формы gcx-вызовов не менять).
- [ ] **Step 2: Проверки.** Run: `make grafana-validate && make grafana-build && git diff --exit-code -- dashboard/` (пусто); `make -n grafana-deploy-test` и `make -n grafana-check` — три uid; `gcx dev lint run --disable uneditable-dashboard --disable panel-title-description dashboard/gold-thesis.json dashboard/key-rate-publications.json dashboard/finance-polyus.json` → no violations; YAML-парс (`python3 -c "import yaml, …"`).
- [ ] **Step 3: Commit** — `grafana: build and deploy three dashboards`.

### Task 7: Документация

**Files:** Modify `README.md`, `docs/ARCHITECTURE.md` (скил `sync-readme-architecture` — графы `grafana/`/`dashboard/`, §6.7: три дашборда, список легаси-экспортов теперь `rosstat.json`+`chmf.json`); Modify `docs/ROADMAP_INFRA.md` и `docs/ROADMAP_DCF_POLYUS.md` (статусы «⏳ этим PR» → «✅ 2026-10-16» с ссылкой на спеку/план и оговоркой «статус финализируется после мержа PR» — по образцу key-rate); `docs/GOLD_THESIS_DATA_ROADMAP.md` — пометка с датой.

- [ ] **Step 1: Синк** по чек-листу `sync-readme-architecture`.
- [ ] **Step 2: Проверка** — `grep` по README/ARCHITECTURE: нет утверждений «два дашборда»; в списках артефактов три файла; легаси — два (`rosstat.json`, `chmf.json`); роадмапы ссылаются на спеку/план.
- [ ] **Step 3: Commit** — `docs: finance-polyus in readme/architecture and roadmap statuses`.

### Task 8: Приёмка и финальное ревью

**Files:** `.superpowers/sdd/2026-10-16-grafana-finance-polyus/progress.md` (леджер ведёт контроллер).

- [ ] **Step 1: Полный прогон** — `make all`; `make grafana-validate`; `make grafana-build` ×2 + `git diff --exit-code -- dashboard/` (три артефакта стабильны); свежий `go test -count=1` модуля.
- [ ] **Step 2: Живая SQL-сверка всех таргетов** — выгрузить `rawSql` из `dashboard/finance-polyus.json`, подставить окна (`$__timeFilter` → 2023-10-16…2026-10-16; панели 12–15 — без подстановки, полная история) и прогнать каждый через `mcp__clickhouse__run_query`; зафиксировать: всего/непустых, разрежённость (теноры, TCC, точки сценариев), расхождения.
- [ ] **Step 3: Локальный цикл** — `make grafana-deploy-test` + `make grafana-check` (три uid; smoke по первым 2 панелям каждого).
- [ ] **Step 4: Протокол** в PR (стандарт §16.1 п. 8): команды, числа, непокрытое (staging — owner-assisted, визуальный просмотр, серверный `gcx resources validate`, CI — на PR).
- [ ] **Step 5: Финальное ревью** — `superpowers:requesting-code-review` (deepseek/deepseek-flash) по диффу `1345989..HEAD`; fix-волна — один диспатч; затем `superpowers:verification-before-completion`; один PR (мержит владелец).

---

## Self-Review (выполнен автором плана)

- **Покрытие спеки:** §1/§3 → T1; §4–§5 → T2–T5 (SQL дословно из спеки); §7-интеграция → T6; §7-доки → T7; §7-приёмка → T8; §8 — границы, в задачах отсутствуют.
- **Скан шагов:** каждая задача — тесты-red → реализация → green → артефакт → commit; числовые значения (id рядов 31–34, Span 24 у 2/7/8, юниты) — из спеки; шагов без «доведи сам» нет.
- **Согласованность имён:** `FinancePolyusUID`, `NewFinancePolyus`, префиксы `fp…`; тесты — единые имена с инкрементальным счётом до финального `TestFinancePolyusHasNineteenPanels`.
- **Review Focus:** пункты 1–4 привязаны к тестам задач (указаны); пункт 5 — процедурный в T6/T7/T8.
- **Пропорция:** план не воспроизводит тела билдеров и SQL (контракт — спека §5); дублирования нет.
