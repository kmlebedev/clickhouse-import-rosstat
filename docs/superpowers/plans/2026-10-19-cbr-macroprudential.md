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
      `afterCbrRiskWeightAddons`). `version` — дата документа в старших 32 битах
      и номер файла в младших, а не wall-clock: переимпорт старого архивного файла
      не должен обгонять более новый документ.
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
      `argMax` для надбавок (ключ группы `matrix, valid_from, row_code, col_code`;
      `valid_to` — атрибут победившей версии), `FINAL` для CCyB/МПЛ.
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

- [x] `make all` — gofmt, golangci-lint (0 issues), `go vet`, `go test -race`, обе сборки.
- [x] `make import STAT=cbr_ccyb_indicators` — `Imported 3910 rows`; повтор `Imported 0 rows`.
- [x] `make import STAT=cbr_risk_weight_addons` — 33 файла, `Imported 130125 rows`;
      повтор: 32 файла no-op, последний переимпортирован из-за сбоя записи архива,
      следующий повтор — `Imported 0 rows`.
- [x] `make import STAT=cbr_mpl` — `Imported 229 rows`; повтор `Imported 0 rows`.
- [x] Spot-проверки из спеки §8 (SQL в ClickHouse, ниже).
- [x] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- [x] `make docs-check` — 36 страниц, 33 теста docs.
- [x] `dagu validate dagu/cbr.yaml` — exit 0.
- [x] Протокол приёмки — комментарием в PR; `Closes #115` вне backticks.
- [x] PR открыт, Project V2: `In Review`, `PR`, `Spec / Plan`.

## 9. Протокол живой приёмки (2026-10-19)

```text
Команда: make import STAT=cbr_ccyb_indicators
Результат: Imported 3910 rows; 24 ряда; 2008-02-01 … 2024-10-01
Повтор: no-op версии, Imported 0 rows
MCP: v_cbr_ccyb_indicators — 3910 строк / 24 ряда; каталог 24

Команда: make import STAT=cbr_risk_weight_addons
Результат: 33 файла, Imported 130125 rows (сырые ячейки всех версий);
           витрина v_cbr_risk_weight_addons — 28707 строк (retail 7200,
           corporate 720, legacy 20787), 2013-07-01 … 2026-10-15
Повтор: 32 файла no-op; последний (table2_20261015.xlsx) переимпортирован,
        т.к. первая запись версии в архив упала на Yandex Disk (connection reset);
        следующий повтор — Imported 0 rows, все 33 файла no-op
Spot: retail 2025-09-01 2002.i×1000.i=1.3, 2002.i×1008.i is_applicable=0;
      corporate 2026-10-15 8002.i×8002.i=1;
      legacy 2019-10-01 в последней версии (файл 2022-01-01) — н/п,
      в архивном файле 2018-10-01 — 0.6 (витрина показывает последнюю версию)
MCP: v_cbr_risk_weight_addons — 28707 строк, 3 матрицы, н/п 24001; каталог 3

Команда: make import STAT=cbr_mpl
Результат: 10 файлов, Imported 229 rows; 7 видов лимитов, 2023-01-01 … 2026-10-01
Повтор: 6 limit-файлов no-op, Imported 0 rows; 4 ranges-файла перечитываются
Spot: bank_consumer 2026Q4 row 51/51.1 — «более 50», лимит 10;
      bank_mortgage 2025Q3 row 1/1.1 — «[0; 50]», лимит 5,
      row 1/1.2 — «более 80», лимит 5 (наследован);
      bank_consumer_with_limit 2023Q1 — 0.25 → 25, «более 80»;
      2024Q2 — «(50; 80]», лимит 10
MCP: v_cbr_mpl — 229 строк, 7 рядов; каталог 7

MCP: make mcp-check — ПРОВЕРКА ПРОЙДЕНА (новый блок: 3 витрины, каталоги
     24/3/7, три сырые таблицы закрыты 497)
Dagu: dagu validate dagu/cbr.yaml — exit 0
Docs: make docs-check — checked 36 pages, 33 tests OK
```

### Review fixes (2026-10-19, после ревью PR #243)

- **Блокирующее:** `version` надбавок был wall-clock, поэтому переимпорт старого
  архивного файла мог обогнать более новый документ. Исправлено: `version` —
  `document_from.Unix() << 32 | номер файла`, `max(version)` того же `source_file`
  сдвигает младшие биты вперёд. Живой регрессионный тест: удалили строки файла
  `2018.10.01_2019.09.30.xlsx`, переимпортировали его после `2022.01.01_2022.02.27.xlsx` —
  витрина для `legacy 2019-10-01 2004.i×1000.i` по-прежнему отдаёт `н/п` из 2022-го,
  а не `0.6` из 2018-го. После фикса таблица очищена и переимпортирована:
  `Imported 130125 rows`, повтор — `Imported 0 rows`.
- **No-op count:** запрос «сколько строк записала эта версия» теперь фильтруется и по
  `source_file` — иначе версия одного файла могла совпасть с версией соседнего и
  повтор ложно показывал импорт. Повтор надбавок и МПЛ — `Imported 0 rows`.
- **МПЛ v3:** поиск колонок взноса/LTV/суммы — по основам слов (падежи источника),
  обязательные колонки проверяются; подстрока наследует снимок родительской строки,
  а не предыдущей подстроки; пустой `по` — сентинел `2100-01-01`. После правок МПЛ
  переимпортирован финальным парсером: 229 строк, повтор — `Imported 0 rows`.
- **Restore:** `cbr_risk_weight_addons` и `cbr_mpl` в `ARCHIVE_ACTION=restore`
  отказывают до сети (набор файлов с landing; ranges не архивируются); тесты добавлены.
- Добавлены тесты версии, формы `Append` для трёх таблиц, метаданных 24/3/7,
  fail-fast (неизвестная строка/ячейка/полоса, пустая подпись ссылки) и трёх шагов
  `dagu/cbr.yaml`; `keyScheduled` расширен.
- Счётчики в `ARCHITECTURE.md`, `util/publications.go`, `cbr/importers.go` и
  `cbr/importers_test.go` приведены к 28/19; в частоту каталога добавлен `irregular`.

## 10. Стоп-правила

- Приёмка не проверена — писать «не проверено», не «готово».
- Объём вырос (например, фактическое значение АЦН, расшифровка кодов) — отдельный
  пункт роадмапа, не «попутно».
- Противоречие issue/роадмапа — Q&A в issue и стоп.
