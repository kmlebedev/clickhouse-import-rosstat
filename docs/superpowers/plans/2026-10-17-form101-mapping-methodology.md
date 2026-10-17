# План: form101 — точный маппинг по методологии ЦБ

**Спека:** [2026-10-17-form101-mapping-methodology-design.md](../specs/2026-10-17-form101-mapping-methodology-design.md).
**Ресерч:** [2026-10-17-cbr-form101-mapping-methodology.md](../../research/2026-10-17-cbr-form101-mapping-methodology.md).
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №22.
**Issue:** [#103](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/103).

## T1. Код маппинга

**Файл:** `cbr/form101.go`.

1. Заменить `cbrForm101CorporateAccounts` на три набора:
   - `cbrForm101CorporateDirect` = 441–451, 453, 454 (активные; 452/456 уже в `45.0`);
   - `cbrForm101CorporateOther` = 460–470, 472, `47.1`, 477 (прочие размещённые средства и лизинг — для сопоставимости со Сбером);
   - `cbrForm101RetailAggregate` = `45.2`.
2. `form101LoanTotals`: считать `corporatePerforming`, `retailPerforming`, `overdue` (458 A_P=1), долю и итоги; пассивные `45.1`/`45.2`, `459`, `474+`, `478` игнорировать.
3. Обновить комментарии: `cbr-809p-sber-1`, источники (809-П, формат DBF 0409101, 6406-У), допущение о пропорциональной аллокации 458.
4. Обновить `cbrForm101LoansView`:
   - CTE `f` — компоненты (`45.0`, direct, other, `45.2`, `458`, `459`);
   - CTE `m` — доля и итоги;
   - `mapping_version='cbr-809p-sber-1'`;
   - комментарии колонок — новая формула.

## T2. Сид июля

**Файл:** `sql/sber_ras_form101_mapped_2026_07.sql`.

- Значения: `loans_corporations = 31756.324775`, `loans_individuals = 19963.292514`.
- В шапке — новая формула и калибровка (−0,05 % / +0,11 %), ссылка на issue #103.
- `source_url` — `https://www.cbr.ru/vfs/credit/forms/101-20260801.rar`.

## T3. Тесты

**Файл:** `cbr/form101_parse_test.go`.

- `TestForm101LoanTotals`: синтетика с `45.0`/`45.1`/`45.2` (A_P=1/2), детальными, `46x`, `47.1`, `477`, `458`, `459`; ожидаемые суммы.
- `TestCbrForm101LoansViewContract`: маркеры `cbr-809p-sber-1`, `'458'`, `'459'`, `company_financials FINAL`, `source_kind = 'kpi'`.

## T4. MCP и живые проверки

**Файл:** `scripts/mcp_check.py`.

- Обновить проверку `v_cbr_form101_loans`: новые значения (≈32 455,69 / 20 075,25), diff ≈ −0,0017 / −0,0062, `mapping_version='cbr-809p-sber-1'`.
- Проверка ras в `v_bank_loan_portfolio_forward` остаётся.

## T5. Документация

- `ARCHITECTURE.md` §6.3: формула, `cbr-809p-sber-1`, ссылка на research.
- `README.md`: абзац `cbr_form101` — новые значения и версия.
- `docs/ROADMAP_KEY_RATE.md` §4.2 №22: статус ✅ с калибровкой; №21 — ссылка на новый маппинг.
- `docs/KEY_RATE_CHANNELS.md` §8: новые значения.
- `docs/research/...` и спека/план — уже в ветке.

## Приёмка

```bash
gofmt -l cbr/
go vet ./cbr/
go test ./cbr/ -count=1
make ch-sql FILE=sql/sber_ras_form101_mapped_2026_07.sql
make import STAT=cbr_form101
make mcp-user && make mcp-check
dagu validate dagu/cbr.yaml
make all
```

Ожидаемо: `Mapped SBER 2026M08: loans_corporations=32455.69, loans_individuals=20075.25 (ras, cbr-809p-sber-1)`, MCP — ПРОВЕРКА ПРОЙДЕНА.
