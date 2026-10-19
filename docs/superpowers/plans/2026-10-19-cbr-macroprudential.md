# План реализации: макропруденциальные требования ЦБ (`key-rate/phase-2-ccyb`, #115)

**Спека:** [2026-10-19-cbr-macroprudential-design.md](../specs/2026-10-19-cbr-macroprudential-design.md)
**Issue:** [#115](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/115)
**Ветка:** `feat/key-rate-phase-2-ccyb`
**Worktree:** `.worktrees/key-rate-phase-2-ccyb`

## 1. Подготовка

- [x] Worktree и ветка созданы от `main`.
- [x] Issue захвачен комментарием, Project V2: `In Progress`, `Branch`.
- [ ] Спека и план — первый коммит.
- [ ] `Spec / Plan` в Project V2 после коммита.

## 2. CCyB-индикаторы

- [ ] `cbr/ccyb_indicators.go`: DDL `cbr_ccyb_indicators`, `DataUrl`,
      `ImportFunc`, `PublicationCountPoints`, кастомный `Import()` с `version` и
      публикацией `afterCbrCcyb`.
- [ ] `cbr/ccyb_parse.go`: разбор листа `Ряды данных`, 24 ряда, даты, значения;
      fail-fast на нечисловой ячейке и неизвестной строке.
- [ ] `cbr/ccyb_meta.go`: 24 `util.SeriesMeta` (`indicator_no`, `indicator_group`,
      title, unit, frequency `M`, origin, description).
- [ ] `cbr/ccyb_parse_test.go` на реальной фикстуре.

## 3. Надбавки к коэффициентам риска

- [ ] `cbr/risk_weight_addons.go`: DDL, `Import()` (landing → 33 файла →
      локальные копии шаблона → per-file preflight → `version`-проверка →
      `afterCbrRiskWeightAddons`).
- [ ] `cbr/risk_weight_addons_page.go`: goquery-разбор landing, подписи, даты,
      сортировка файлов.
- [ ] `cbr/risk_weight_addons_parse.go`: листы-матрицы, коды, `н/п`, `valid_to`,
      `matrix`, `table_no`, версия.
- [ ] `cbr/risk_weight_addons_meta.go`: 3 записи каталога.
- [ ] `cbr/risk_weight_addons_test.go` (page + parse) на реальных фикстурах.

## 4. МПЛ

- [ ] `cbr/mpl.go`: DDL, `Import()` (landing → ranges в память → limit-файлы →
      локальные копии шаблона → per-file preflight → `version`-проверка →
      `afterCbrMPL`).
- [ ] `cbr/mpl_page.go`: goquery-разбор landing, классификация limit/ranges,
      bank/mfo, `document_from`.
- [ ] `cbr/mpl_ranges.go`: разбор `Limits_ranges_*`, map границ.
- [ ] `cbr/mpl_parse.go`: v1/v2/v3, нормализация полос и процентов, подстроки.
- [ ] `cbr/mpl_meta.go`: 7 записей каталога.
- [ ] `cbr/mpl_test.go` (page + parse + ranges) на реальных фикстурах.

## 5. Витрины, реестр, расписание, MCP

- [ ] `cbr/views.go` (или `cbr/macroprud_views.go`): три витрины, комментарии,
      `argMax` для надбавок, `FINAL` для CCyB/МПЛ.
- [ ] `cbr/importers.go`: три записи реестра с литералами таблиц и глобальными
      count-запросами.
- [ ] `cbr/importers_test.go`: три имени в `expectedPublicationCountPoints`,
      проверка per-file preflight в объявлении.
- [ ] `dagu/cbr.yaml`: три шага в хвосте после `cbr_zcyc`.
- [ ] `sql/mcp_kimi_reader.sql`: три `GRANT SELECT`.
- [ ] `scripts/mcp_check.py`: блок проверок (витрины, каталоги 24/3/7, сырые
      таблицы закрыты 497).

## 6. Фикстуры

- [ ] `cbr/testdata/ccyb_indicators_time_series.xlsx` — реальный файл/срез.
- [ ] `cbr/testdata/risk_weight_addons_table1_20260904.xlsx`,
      `…table2_20261015.xlsx`, `…2018.10.01_2019.09.30.xlsx`,
      `risk_weight_addons_page.html`.
- [ ] `cbr/testdata/mpl_banks_2025.xlsx`, `…banks_2024.xlsx`, `…banks_2023.xlsx`,
      `…mfo_2025.xlsx`, `…ranges_banks_2023.xlsx`, `…ranges_banks_2024.xlsx`,
      `mpl_page.html`.
- [ ] Тесты на реальных фикстурах, без синтетики.

## 7. Документация

- [ ] `docs/IMPORTERS.md` — три импортёра в cbr-секции.
- [ ] `README.md` — cbr-строка.
- [ ] `ARCHITECTURE.md` — §1/§2/§6.1, §6.2 (DDL), §6.3 (витрины), §8 п. 13
      (per-file preflight).
- [ ] `docs/ROADMAP_KEY_RATE.md` §4.2 №12 — `✅ сделан этим PR` + ссылки.
- [ ] `docs/ROADMAP_INDEX.md` — статус пункта.

## 8. Проверка и приёмка

- [ ] `make all`.
- [ ] `make import STAT=cbr_ccyb_indicators` — 3 910 строк; повтор `Imported 0 rows`.
- [ ] `make import STAT=cbr_risk_weight_addons` — 33 файла; повтор `Imported 0 rows`.
- [ ] `make import STAT=cbr_mpl` — 10 файлов; повтор `Imported 0 rows`.
- [ ] Spot-проверки из спеки §8.
- [ ] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- [ ] `make docs-check`.
- [ ] `dagu validate dagu/cbr.yaml`.
- [ ] Протокол приёмки — комментарием в PR; `Closes #115` вне backticks.
- [ ] PR открыт, Project V2: `In Review`, `PR`, `Spec / Plan`.

## 9. Протокол живой приёмки (заполнить после прогона)

```text
Команда:
Результат:
Крайние даты:
Повтор:
MCP:
Dagu:
```

## 10. Стоп-правила

- Приёмка не проверена — писать «не проверено», не «готово».
- Объём вырос (например, фактическое значение АЦН, расшифровка кодов) — отдельный
  пункт роадмапа, не «попутно».
- Противоречие issue/роадмапа — Q&A в issue и стоп.
