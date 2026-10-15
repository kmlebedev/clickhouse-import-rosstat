### Task 2: `cbr_bop_standard` — импортёр, фикстура, тесты

- `cbr/bop_standard.go`: список 17 подписей и имён кода (спека §3.3, таблица стандартной
  презентации); обход двух листов (`Кварталы` → `квартал`, `Годы` → `год`), отсутствие листа —
  ошибка; `DataUrl: https://www.cbr.ru/vfs/statistics/credit_statistics/bop/bal_of_payments_standart.xlsx`
  (опечатка «standart» — в источнике), `PublicationCountPoints:
  SELECT count() FROM cbr_bop_standard FINAL`,
  `AfterImport: afterCbrBop(cbrBopStandardSeriesMeta)`.
- `cbr/bop_meta.go`: секция `cbr_bop_standard` — 34 записи (`Source: cbr_bop_standard`,
  `Unit: млн долларов США`, `Frequency: Q/A`).
- `cbr/views.go`: вторая ветка `cbrBopView` — `Tables` += `cbr_bop_standard`, `UNION ALL` со
  `source` = имя таблицы, комментарии таблицы и колонок уточняются (две выгрузки: оценка и
  стандартные компоненты).
- Фикстура `cbr/testdata/bal_of_payments_standart.xlsx` — живой файл целиком (462 206 байт,
  снимок 30.09.2026).
- `cbr/bop_standard_test.go`: контракт каталога; 2 754 точки (2 210 + 544) и даты по правилам;
  spot-значения (`Счет текущих операций (квартал)` 1994-03-31 = 1 996,33 и 2026-06-30 = 16 949,24;
  `Счет текущих операций (год)` 1994-12-31 = 8 941,89 и 2025-12-31 = 38 588,14;
  `Резервные активы (квартал)` 1994-03-31 = −1 261 и 2026-06-30 = 7 251,98); fail-fast ветки.

