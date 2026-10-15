### Task 2: каталог рядов

- `cbr/series_meta.go`: `cbrIntRateLoansSeriesMeta` — 16 записей `util.SeriesMeta`
  (`Source: cbrSource`), через `namedSeriesMeta` с `seriesTemplate` (спека §3.6):
  unit «% годовых», F=M, origin «cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx,
  лист «ставки_руб», группа …, колонка срока «…»», description: средневзвешенная ставка по
  кредитам нефинансовым организациям в рублях за месяц (взвешена объёмом размещений;
  динамика определяется и ставками, и объёмами — из сноски источника), дата — предпоследний
  день месяца отчётного периода (сопоставлять по toStartOfMonth(date)), темп — разница
  в п.п., пустая ячейка источника — пропуск точки.
- Подключить `AfterImport: afterCbrMacro(cbrIntRateLoansSeriesMeta)` в импортёре T1.

