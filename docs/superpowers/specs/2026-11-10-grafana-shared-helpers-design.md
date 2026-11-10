# Общие хелперы пакета `grafana/` — дизайн

**Пункт роадмапа:** `infra/5.16` в [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md), issue #560.

## Проблема

Пакет `grafana/` (дашборды как код, один пакет `main`) вырос до шести дашбордов. Функции, которые нужны всем, определены в файле первого дашборда, `gold_thesis.go`: `ptr`, `datasourceRef`, `timeseriesQuery`, `tableQuery`, `Manifest`, константы форматов запросов и ширины панелей. Граф кода (graphify) показывает `ptr()` и `datasourceRef()` как мосты между всеми сообществами дашбордов. На деле это просто общий код, который лежит не на своём месте.

Параллельно каждый новый дашборд копировал хелперы предыдущего под своим префиксом:

| Дубль | Где | Отличие |
|---|---|---|
| `fpFixedColorProp` / `krFixedColorProp` | finance_polyus, key_rate_publications | нет, побайтно одинаковы |
| `baPanelDefaults` / `fpPanelDefaults` / `krPanelDefaults` / `ttPanelDefaults` | 4 дашборда | список `Calcs` легенды, `LineInterpolation`, высота/ширина, единицы |
| `baStatDefaults` / `uralsBudgetStatDefaults` + инлайн в `baTriggersPanel` | bond_attractiveness, urals_budget_rule | у ba добавлены высота и ширина |
| `baZeroThresholds` / `krZeroThresholds` | bond_attractiveness, key_rate_publications | цвет ниже нуля (`blue` / `red`) |
| `krThresholdLineStyle` + 6 инлайн-вызовов | bond_attractiveness, gold_thesis, key_rate_publications | нет |
| `builtX(t)` ×6 | все `*_test.go` | только конструктор дашборда |
| `panelByID`, `targetByRefID`, `thresholdStyleMode`, `overridePropByRefID`, `assertPointsOnly` | лежат в тестах одного дашборда, используются в нескольких | — |

Из-за этого правка общего поведения (например, легенды) требует четырёх синхронных изменений, а удаление или переписывание `gold_thesis.go` ломает весь пакет.

## Решение

1. **`grafana/shared.go`**: общий код пакета.
   - Константы `queryFormat*`, `queryType*`, `spanThird`/`spanHalf`/`spanFull`.
   - `ptr`, `datasourceRef`, `timeseriesQuery`, `tableQuery`, `Manifest`.
   - Field-props: `fixedColorProp` (вместо `fp`/`kr`), `pointsOnlyProps`, `axisPlacementRightProp`.
   - Пороги: `thresholdLineStyle`, `zeroThresholds(belowColor)`.
   - Базовые настройки панелей: `timeseriesDefaults(builder, calcs...)` (датасорс, палитра, легенда-таблица снизу с заданными `Calcs`, одиночный тултип, линия 1 px без заливки и градиента, без соединения пропусков) и `statDefaults(builder)` (датасорс, `lastNotNull`, авто-текст).
   - Префиксные `*PanelDefaults` и `*StatDefaults` остаются тонкими обёртками: `timeseriesDefaults` плюс собственные отличия дашборда. Имена в панелях не меняются.
2. **`grafana/shared_test.go`**: `mustBuild(t, builder)` и межфайловые тестовые хелперы. Перенос без изменения тела. `builtX(t)` становятся однострочными обёртками над `mustBuild`.
3. Хелперы, которые используются в одном файле (`rightAxisPointsProps`, `crackProps`, `goldPriceSQL`, SQL-строители `kr*`/`fp*`, специфичные пороги), остаются на месте.

## Вне пункта

- Общий `absoluteThresholds(steps...)` для 9 инлайн-конструкторов порогов и унификация SQL-строителей `kr*`/`fp*`: меняют много панелей без выигрыша в связности. Если понадобится, это будет отдельный пункт.
- Изменение внешнего вида дашбордов.

## Критерий приёмки

- `cd grafana && go run . -out-dir <tmp>` даёт JSON, **побайтно совпадающий** с `dashboard/*.json` до рефакторинга (все 6 дашбордов). Это главный инвариант: рефакторинг не меняет артефактов.
- `make grafana-validate` и `make all` зелёные.
- В `gold_thesis.go` не остаётся определений, которые используются другими файлами пакета.
- Дублей из таблицы выше нет.
