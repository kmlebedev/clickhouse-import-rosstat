# Общие хелперы пакета `grafana/` — план

Спека: [2026-11-10-grafana-shared-helpers-design.md](../specs/2026-11-10-grafana-shared-helpers-design.md). Пункт: `infra/5.16`.

## Шаги

1. **Эталон.** `cd grafana && go run . -out-dir $TMP/before`; убедиться, что совпадает с `dashboard/*.json`.
2. **Перенос из `gold_thesis.go`.** Константы форматов/ширины, `ptr`, `datasourceRef`, `timeseriesQuery`, `tableQuery`, `Manifest`, `pointsOnlyProps`, `axisPlacementRightProp` переносятся в `shared.go`. `spanFull` переносится из `transaq_trades.go`. Импорт `resource` уходит из `gold_thesis.go`.
3. **Дедупликация.**
   - `fixedColorProp` заменяет `fpFixedColorProp` и `krFixedColorProp`.
   - `thresholdLineStyle` заменяет `krThresholdLineStyle` и инлайн-вызовы `NewGraphThresholdsStyleConfigBuilder().Mode(GraphThresholdsStyleModeLine)`.
   - `zeroThresholds(color)` заменяет `baZeroThresholds` (`"blue"`) и `krZeroThresholds` (`"red"`).
   - `timeseriesDefaults(b, calcs...)` становится базой `ba`/`fp`/`kr`/`tt` `PanelDefaults`.
   - `statDefaults(b)` становится базой `baStatDefaults`, `uralsBudgetStatDefaults` и `baTriggersPanel`.
4. **Тесты.** `shared_test.go` получает `mustBuild` и перенесённые `panelByID`, `targetByRefID`, `thresholdStyleMode`, `overridePropByRefID`, `assertPointsOnly`. `builtX` становятся однострочниками. Плюс тест на `zeroThresholds` и `timeseriesDefaults`: проверка `Calcs` и общих полей.
5. **Проверка.**
   - `go run . -out-dir $TMP/after`, затем `diff -r $TMP/before $TMP/after` — пусто.
   - `make grafana-validate`, `make all`.
6. **Документы.** Строка `infra/5.16` в `ROADMAP_INFRA.md`. В `ARCHITECTURE.md` §6.7 упоминается файл `shared.go`, если там описана раскладка пакета.
