# Дизайн: закрытие deferred minors ревью Фазы 1 контура золота

**Дата:** 2026-10-11
**Ветка:** `fix/deferred-minors-phase1-review`
**Основание:** список deferred minors от code review PR #33 (Фаза 1: `comex_front`, резервы ЦБ, `v_imf_macro`).

## Объём

Семь мелких правок — документация, комментарии, валидация, тесты. Схема БД и контракты endpoint'ов не меняются, единственное видимое поведение: точка с датой вне диапазона Date32 теперь получает `400` на валидации вместо `500 insert failed` на вставке.

| # | Дефект | Фикс |
|---|--------|------|
| 1 | «Единственный путь записи» в ARCHITECTURE §6.5 и doc-комментарии `ingest.Server` не называет `gold_prices` | дополнить список таблиц |
| 2 | Комментарий DDL `macro_series` в ARCHITECTURE §6.2 (`'fred','eia','wgc','cme'`) не совпадает с код-копией `ingest/schema.go` (`'fred','bls','bea','manual','webbridge','imf'`) | привести DDL-блок к коду, примечание переписать |
| 3 | AGENTS.md п.5 называл только `ROADMAP_DCF_POLYUS.md` | правка уже в рабочем дереве — закоммитить в ветке |
| 4 | Комментарий `chStore` «нужен InsertModelRun» устарел (методов записи три) | переписать |
| 5 | Валидация дат не ограничивает диапазон Date32 ([1900-01-01, 2299-12-31] в ClickHouse 26.x) | общая проверка диапазона в `ValidateManualSeriesPoint` и `ValidateGoldPricePoint` + табличные тесты на границы |
| 6 | Runbook Фазы 1 читает `v_gold_prices`, но на чистой БД витрину создаёт только импортёр `gold_prices` | добавить `make import STAT=gold_prices` в шаг 4 и пояснение в шаг 5 |
| 7 | Нет HTTP-теста на явную пустую строку `source: ""` | `TestManualSeriesExplicitEmptySourceDefaults` |

## Решения

- **П.5, диапазон:** константы `minDate32`/`maxDate32` в `ingest/model_run.go`; проверка после разбора даты, единый текст ошибки `date %q out of Date32 range [1900-01-01, 2299-12-31]`. Даты до 1970 остаются валидными (CPIAUCSL с 1947, FEDFUNDS с 1954, LBMA с 1968 — всё внутри диапазона).
- **П.5, горизонт model_run** не трогаем: `forecast_log.target_date` имеет тип `Date`, это отдельная таблица и вне списка deferred minors.
- **П.7:** фейк `BatchWriter` из существующих тестов фиксирует аргументы; новый тест отличается от `TestManualSeriesSourceDefault` только явным `"source": ""` в JSON.

## Приёмка

- `make all` зелёный (gofmt, golangci-lint, vet, `go test -race`, сборка).
- `go test ./ingest/... -race` проходит с новыми тестами.
- README сверен со skill `sync-readme-architecture` (список source в DDL меняется — README это не дублирует).
- ClickHouse не требуется: правки не меняют ни схему, ни витрины; `make mcp-check` не затрагивается.
