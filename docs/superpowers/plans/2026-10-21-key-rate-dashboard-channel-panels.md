# Панели четырёх каналов без покрытия — план

**Пункт:** `key-rate/phase-4-dashboard-channel-panels` ([issue #328](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/328)). **Спека:** [2026-10-21-key-rate-dashboard-channel-panels-design.md](../specs/2026-10-21-key-rate-dashboard-channel-panels-design.md).

## T1. Спека и план

- [x] `docs/superpowers/specs/2026-10-21-key-rate-dashboard-channel-panels-design.md`
- [x] `docs/superpowers/plans/2026-10-21-key-rate-dashboard-channel-panels.md`
- Коммит: `add key-rate channel panels design and plan`.

## T2. Код дашборда

`grafana/key_rate_publications.go`:

- SQL-константы/билдеры: `krBrentSQL`, `krBopSeriesSQL`, `krSurveySeriesSQL`, `krCcybSeriesSQL`, `krDepositsLevelSQL`, `krDepositsInflowSQL`;
- панели: `krExternalFxPanel` (29), `krExternalBopPanel` (30), `krSavingsPanel` (31), `krOutputGapPanel` (32), `krAutonomousFactorsPanel` (33);
- ряд `channelRowID = 34` «Остальные каналы решения» в конце `NewKeyRatePublications()`;
- `Description` каждой панели — по §5 спеки.

Коммит: `add key-rate channel panels`.

## T3. Тесты

`grafana/key_rate_publications_test.go`:

- 28 панелей; пять рядов с явными id `[21, 22, 23, 24, 34]`;
- `TestKeyRateChannelPanels` — заголовки, units, таргеты, `FROM v_*`, series/source, `$__timeFilter`, `ORDER BY`, правые оси;
- `TestKeyRateSparseSeriesPoints` — 30/B (месячная оценка БОП);
- окно 31/B: `$__timeFilter` один и снаружи подзапроса, `lagInFrame(value, 1, NULL)`;
- оговорки в `Description` (Brent не спот; депозиты = ФЛ + ИП; ccyb 2024-10 и NULL ≠ 0).

Коммит: `test key-rate channel panels`.

## T4. Артефакт

- `make grafana-build`; проверить, что изменился только `dashboard/key-rate-publications.json`;
- повторный `make grafana-build` — пустой `git diff -- dashboard/`.

## T5. Документация

- `docs/KEY_RATE_PUBLICATIONS.md` §11 — 28 панелей / 5 рядов, покрытие 11/11, таблица «блок → панели», ловушки 29–33.
- `docs/KEY_RATE_CHANNELS.md` §8 — срез и колонка «Панель Grafana» для четырёх каналов.
- `ARCHITECTURE.md` §2 и §6.7 — состав дашборда; шапка «Последнее обновление».
- `docs/GRAFANA.md` — актуальные 28 панелей / 5 рядов.
- `docs/ROADMAP_KEY_RATE.md` — пункт `In Review` со ссылкой на PR и протоколом.

Коммит: `document key-rate channel panels`.

## T6. Приёмка

- `make grafana-validate`, `make all`, `make docs-check`.
- Живой SQL всех новых таргетов через MCP `kimi_reader` — числа в протокол.
- `make grafana-deploy-test` + `make grafana-check` — если стенд и токен доступны; иначе «не проверено» с причиной.
- `git status` — только файлы пункта.
- PR: `Closes #328` вне backticks; протокол — комментарием; Project V2 → `In Review`.
