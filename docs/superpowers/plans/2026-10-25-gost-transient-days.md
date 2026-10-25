# gost: временные 5xx/таймауты дня — план

Спека: [2026-10-25-gost-transient-days-design.md](../specs/2026-10-25-gost-transient-days-design.md). Issue: [#389](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/389).

## Task 1. Классификация дня

- [x] `gostRunDays`: `errors.As` по `*backoff.RetryAfterError` → временный сбой; иначе постоянный.
- [x] Временные дни — отдельный список, `warning`; постоянные — `failed`, как сейчас.
- [x] `readDays` считает успешные ответы, включая пустые дни.
- [x] Если `failed` непуст — ошибка; если временные и `readDays == 0` — ошибка; иначе warning и успех.

## Task 2. Тесты

- [x] `TestGostRunDaysSkipsEmptyAndAggregatesErrors` → успех + warning (один 500 из трёх).
- [x] `TestGostRunDaysCapsFailedDayList` (все 500) → ошибка «не прочитаны дни (N)»; остаётся.
- [x] Новый тест: 400 + успешный день → ошибка.
- [x] `go test ./craw/`, `make all`.

## Task 3. Документация и приёмка

- [x] `ARCHITECTURE.md`/`docs/IMPORTERS.md`: временный 5xx дня — warning, повтор следующим прогоном.
- [x] `docs/ROADMAP_INFRA.md`: строка 1.8.
- [x] `make deploy-staging-binary`; живой прогон `GOST_MODE=since` на staging с 500-ми днями — код 0, warning, без письма.
- [x] PR с `Closes #389`, протокол комментарием.
