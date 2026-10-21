# Четыре панели цен на key-rate-publications — план

**Пункт:** `key-rate/phase-4-dashboard-prices-ppi-fao` ([issue #347](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/347)). **Спека:** [2026-10-22-key-rate-dashboard-prices-ppi-fao-design.md](../specs/2026-10-22-key-rate-dashboard-prices-ppi-fao-design.md). **Следствие:** ресерч `key-rate/phase-4-weekly-core-proxy` ([issue #348](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/348)) — отдельный PR.

## T1. Спека и план

- [x] `docs/superpowers/specs/2026-10-22-key-rate-dashboard-prices-ppi-fao-design.md`
- [x] `docs/superpowers/plans/2026-10-22-key-rate-dashboard-prices-ppi-fao.md`
- Коммит: `add key-rate prices panels design and plan`.

## T2. Код дашборда

`grafana/key_rate_publications.go`:

- константы: `krWeeklyHistoryStart = "2022-01-10"`, `krWeeklyAggregateSeries`, `krFedstatRegion`, `pricesRowID = 40`, слайс `krFedstatPriceSeries` (6 пар «ряд → алиас»);
- шаблоны: `krWeeklyHistorySQL`, `krWeeklyAvg4Template` (обе базы: товарная средняя и агрегат), `krPPIAnnualTemplate`, `krPPISAARTemplate`, `krFAOTemplate`, `krFedstatPriceTemplate`;
- панели: `krWeeklyAvg4Panel()` (36), `krPPIPanel()` (37), `krFAOPanel()` (38), `krFedstatPricesPanel()` (39);
- регистрация: панель 36 — в ряд 22 сразу после панели 2; новый ряд `pricesRowID = 40` «Цены производителей, продовольствие, средние цены» с панелями 37–39 в конце `NewKeyRatePublications()`;
- `TimeFrom`: 36 — `5y`, 37 и 38 — `10y`, 39 — `20y` (диапазон дашборда `now-3y` не меняется);
- `Description` каждой панели — по §5 спеки.

Коммит: `add key-rate prices panels`.

## T3. Тесты

`grafana/key_rate_publications_test.go`:

- обновить счётчики: 33 панели, шесть рядов `[21, 22, 23, 24, 34, 40]`;
- `TestKeyRateWeeklyAvg4Panel` — три таргета, полный ряд с `date >= '2022-01-10'` без `toYear(`, окно `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`, `count()`, `n = 4`, `avg(ln(` + `exp(`, агрегат по имени ряда, `TimeFrom("5y")`;
- `TestKeyRatePPIPanel` — четыре таргета, обе формулы, коды `ppi_sa:*:saar3`, unit `percentunit`, отсутствие thresholds;
- `TestKeyRateFAOPanel` — шесть таргетов, `period_date AS time`, `FROM v_fao_food_price`, уникальные алиасы, unit `short`;
- `TestKeyRateFedstatPricesPanel` — шесть таргетов, `region = 'Российская Федерация'` в каждом, шесть разных `series`, unit `currencyRUB`, `TimeFrom("20y")`;
- `TestKeyRateDashboardTimeUnchanged` — общий диапазон остался `now-3y`;
- существующие тесты (`AllTargetsUseViews`, `PanelIDsUnique`, `Units`, `WeeklyJoinCatalog`, `WeeklyYearOverlay`) — зелёные без ослабления.

Коммит: `test key-rate prices panels`.

## T4. Артефакт

- `make grafana-build`; изменился только `dashboard/key-rate-publications.json`;
- повторный прогон — пустой `git diff -- dashboard/`.

Коммит: `build key-rate-publications artifact`.

## T5. Документация

- `docs/KEY_RATE_PUBLICATIONS.md` §11 — ряд 40 и панели 36–39 в таблице «блок статьи → панели»; «Ловушки панелей 36–39»;
- `docs/KEY_RATE_CHANNELS.md` §8 — панели 36–39 у «Текущего роста цен»; §8.1 — ИЦП, ФАО и средние цены как инструменты;
- `ARCHITECTURE.md` §6.7 — 33 панели, 6 рядов, состав нового ряда; строка «Последнее обновление»;
- `docs/GRAFANA.md` — счётчики панелей/рядов;
- `docs/ROADMAP_KEY_RATE.md` §6 — пункт `phase-4-dashboard-prices-ppi-fao` (issue #347) и ресерч `phase-4-weekly-core-proxy` (issue #348).

Коммит: `document key-rate prices panels and register research item`.

## T6. Приёмка

- `cd grafana && go vet ./... && go test ./...`;
- `make all`, `make docs-check`;
- живой SQL всех таргетов через MCP под `kimi_reader` (ожидаемо: 36 — 250 / 247 / 247 точек, 37 — 344/68/164/68, 38 — 441×6, 39 — 767×6);
- `make grafana-deploy-test` + `make grafana-check` — owner-assisted; протокол — комментарием в PR.

## T7. Вне объёма

Панель 2 не меняется; 2026 по ФЕДСТАТ не добавляется (другое определение РФ); сезонная корректировка недельной средней — ресерч #348.
