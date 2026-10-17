# form101: точный маппинг счетов по методологии ЦБ (cbr-809p-sber-1)

**Дата:** 17 октября 2026 г.
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №22 (продолжение №21).
**Issue:** [#103](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/103).
**Ресерч:** [2026-10-17-cbr-form101-mapping-methodology.md](../../research/2026-10-17-cbr-form101-mapping-methodology.md).
**Предшественник:** pilot-1, PR [#100](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/100).

## 1. Цель

Заменить эмпирический pilot-1 на маппинг, опирающийся на 809-П и официальный формат DBF формы 0409101, и снизить расхождение с пресс-релизом Сбера до долей процента на двух месяцах.

## 2. Источник истины

- 809-П: 441–454, 456 — кредиты ЮЛ; 455, 457 — кредиты ФЛ; 458 — просроченный основной долг; 459 — просроченные проценты; 460–473 — прочие размещённые средства; 477 — лизинг; 478 — приобретённые права.
- Формат DBF 0409101: `45.0` (A_P=1) = 408 + 452 + 456; `45.1` (A_P=2) = 452 + 456 (резервы); `45.2` (A_P=1) = 455 + 457; `45.2` (A_P=2) = 455 + 457 (резервы).
- 6406-У: ИП классифицируются как ЮЛ; отчёт в тыс. руб.

## 3. Алгоритм `form101LoanTotals`

Вход — строки Сбера за отчётную дату (`[]form101Row`), выход — `corporate, retail float64` в млрд ₽.

1. `corporatePerforming` = сумма `balance_total` по:
   - `45.0` при `active_passive == "1"`;
   - активным (`A_P=1`) счетам `441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 453, 454`;
   - активным «прочим размещённым средствам»: `460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 472`, агрегату `47.1`, счёту `477`.
2. `retailPerforming` = сумма `balance_total` по `45.2` при `active_passive == "1"`.
3. `overdue` = `balance_total` по `458` при `active_passive == "1"` (просроченный основной долг).
4. `share = corporatePerforming / (corporatePerforming + retailPerforming)`.
5. `corporate = corporatePerforming + overdue * share`; `retail = retailPerforming + overdue * (1 - share)`.
6. Деление на `1e6` (в источнике тыс. руб.).
7. Если `corporatePerforming <= 0` или `retailPerforming <= 0` — ошибка (набор счетов разошёлся).

Исключены: пассивные `45.1`/`45.2` (резервы), `459` (проценты), `474+` (расчёты), `478` (приобретённые права).

`mapping_version` — `cbr-809p-sber-1` (в витрине и документации).

## 4. Витрина `v_cbr_form101_loans`

SQL повторяет алгоритм:

- `f` — CTE с `corporate_performing`, `retail_performing`, `overdue`, `overdue_interest`;
- `m` — CTE с долей и итоговыми `corporate_loans_bln_rub` / `retail_loans_bln_rub`;
- `pr` — kpi-эталон пресс-релиза (сырая `company_financials FINAL`, `source_kind='kpi'`);
- `overdue_bln_rub` — 458 A_P=1 (всего, без разбивки);
- `overdue_interest_bln_rub` — 459 A_P=1;
- `mapping_version='cbr-809p-sber-1'`.

## 5. Запись в `company_financials`

`writeForm101Loans` пишет те же два ряда (`loans_corporations`, `loans_individuals`), `source_kind='ras'`, `period = report_date − 1 месяц`. Июльский сид `sql/sber_ras_form101_mapped_2026_07.sql` обновляется новыми значениями (31 756,324775 / 19 963,292514). Август перезаписывается импортёром на no-op-пути.

Сентябрь остаётся `kpi`-сидом до появления формы 101 на 01.10.2026.

## 6. Тесты

- `TestForm101LoanTotals`: синтетические строки с `45.0`, `45.1`, `45.2` (оба A_P), детальными 44x/45x, `46x`, `47.1`, `477`, `458`, `459`; проверка сумм и игнорирования резервов/процентов.
- `TestCbrForm101LoansViewContract`: маркеры `cbr-809p-sber-1`, `'458'`, `'459'`, `company_financials FINAL`, `source_kind = 'kpi'`.
- Живой прогон: `make import STAT=cbr_form101` на no-op-пути перезаписывает август; `v_cbr_form101_loans` за 2026-09-01 показывает новые значения и diff.

## 7. Приёмка

- `gofmt -l cbr/`, `go vet ./cbr/`, `go test ./cbr/ -count=1` — чисто.
- `make all` — зелёный.
- `make ch-sql FILE=sql/sber_ras_form101_mapped_2026_07.sql` — идемпотентно.
- `make import STAT=cbr_form101` — `Mapped SBER 2026M08: ...` с новыми значениями.
- MCP: `v_company_financials` — июль/август `ras`; `v_bank_loan_portfolio_forward` — splits; `v_cbr_form101_loans` — diff в пределах ±1 %.
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- `dagu validate dagu/cbr.yaml` — exit 0.

## 8. Риски

- Разрез `458` по ЮЛ/ФЛ в форме 101 отсутствует; пропорциональная аллокация — оценка, помечена в коде и research-документе.
- Включение `46x/47x/477` — калибровочное решение для сопоставимости со Сбером, а не буквальное «кредиты» по 809-П; выделено отдельным набором с комментарием.
- При появлении формы 101 на 01.10.2026 сентябрьский `kpi`-сид будет перекрыт `ras`-рядом (ожидаемо).
