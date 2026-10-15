### Task 3: витрина и реестр

- `cbr/views.go`: `cbrMacroView.Tables` += `cbr_int_rate_loans`; `UNION ALL SELECT 'cbr',
  toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans FINAL` после строки
  `cbr_bank_int_rate`; Comment/Columns: «ставки по вкладам и кредитам», в описании `series`
  и `date` — добавить кредитные ставки и ту же привязку даты.
- `cbr/importers.go`: запись `{cbrIntRateLoansImporter, "cbr_int_rate_loans", "SELECT count() FROM cbr_int_rate_loans FINAL"}`
  после `cbr_bank_int_rate` (порядок = порядок `dagu/cbr.yaml`).
- `cbr/importers_test.go`: + `"cbr_int_rate_loans": "SELECT count() FROM cbr_int_rate_loans FINAL"`
  в `expectedPublicationCountPoints`; счётные формулировки комментариев «десять» → «одиннадцать».

