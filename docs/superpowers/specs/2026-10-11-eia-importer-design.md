# EIA importer — design

Дата: 2026-10-11. Пункт: `docs/GOLD_THESIS_DATA_ROADMAP.md` §2, «Фаза 2 — автоматизация EIA».

## Пробел

`distillate_stocks` в `v_gold_dashboard` — единственный индикатор дизельного канала без
автоматического источника: значения вводятся вручную через `POST /v1/manual_series`
(`source='manual'`). Загрузка НПЗ (второй вход дизельного канала по §6 статьи) автоматического
источника тоже не имеет.

## Цель

Импортёр `eia` (пакет `eia/`, `Name()` = `eia`): EIA API v2 → `macro_series` (`source='eia'`),
dagu-расписание по средам. После фазы 2 `distillate_stocks` обновляется без ручного ввода,
`stale=0` после каждой среды; `make mcp-check` зелёный.

## Источник и серии

EIA API v2, ключ `EIA_API_KEY` из env (секрет только через `os.Getenv`, в ошибках маскируется
`strings.ReplaceAll(err, key, "***")` — как в `bea`). Проверено 11.10.2026: endpoint доступен
с этой сети (403 `API_KEY_MISSING` без ключа — роут валиден).

Коды серий подтверждены по фасетам API (шаг 1 плана):

| Ряд в `macro_series` | Код EIA | Маршрут | Единица | Частота |
|---|---|---|---|---|
| `distillate_stocks` | WDISTUS1 | `/v2/petroleum/stoc/wstk/data/` | thousand barrels (`units: MBBL`) | W |
| `refinery_utilization` | WPULEUS3 | `/v2/petroleum/pnp/wiup/data/` | percent (`units: %`) | W |

Коды подтверждены фасетами и данными API 11.10.2026: `WDISTUS1` — «U.S. Ending Stocks of
Distillate Fuel Oil (Thousand Barrels)», последнее 2026-10-02 = 105 138; `WPULEUS3` — «U.S.
Percent Utilization of Refinery Operable Capacity», последнее 2026-10-02 = 92,7. Ожидавшийся
`WUPRUUS2` в фасетах отсутствует — используется фактический `WPULEUS3`.

Запрос: `api_key`, `frequency=weekly`, `data[0]=value`, `facets[series][]=<code>`,
`start=2019-01-01`, `sort[0][column]=period`, `sort[0][direction]=asc`, `length=5000`, `offset`
(пагинация по `response.total`). Ответ: `response.data[]` с `period` (`YYYY-MM-DD`), `value` (строка), `units` (`MBBL` / `%`),
`series-description`. Ряд из 405 точек (с 2019-01-01) укладывается в лимит 5000 строк — пагинация
реализуется превентивно по `response.total`. Записи без значения пропускаются; ошибки парсинга
даты/значения — с контекстом серии, импорт падает.

Имя ряда `distillate_stocks` совпадает с индикатором `v_gold_dashboard` намеренно (единая
линия значений по источникам); нативный код WDISTUS1 фиксируется в `series_catalog.origin`.

## Схема и витрины

- `macro_series` — канонический DDL из ARCHITECTURE.md §6.2 (копия в коде, как у fred/bls/bea).
- `v_eia_petroleum` — `SELECT date, series, value FROM macro_series FINAL WHERE source = 'eia'`
  + комментарии таблицы и колонок (`ALTER TABLE ... MODIFY COMMENT`, `COMMENT ON` не работает
  в CH 26.10). Грант `kimi_reader` в `sql/mcp_kimi_reader.sql`.
- `series_catalog` — 2 ряда через `util.UpsertSeriesCatalog` (source `eia`, frequency `W`).
- `v_gold_dashboard` (`views/gold_dashboard.go`) — источники расширяются до
  `('eia','webbridge','manual')` с приоритетом `eia > webbridge > manual` (агрегат
  `GROUP BY series, source`, снаружи `coalesce(maxIf(...))` по приоритету). Автоматический
  источник канонический, ручной — фолбэк. Пороги и логика `stale` (14 дней) не меняются.

## Расписание

`dagu/eia.yaml`: `type: chain`, `schedule: "43 19 * * 3"` (среда 19:43 МСК), `overlap_policy: skip`,
retry 3 × 300 с (праздники США).

Фактура по времени: WPSR выходит в 10:30 ET = 17:30 МСК, то есть прогон 19:43 МСК идёт примерно
через 2 часа после релиза. Релиз, перенесённый из-за праздника США на четверг, подхватится
следующим прогоном (следующая среда), а 14-дневное окно `stale` в `v_gold_dashboard` это переживает
без ложной просрочки.

## Приёмка

1. `make all` зелёный (gofmt, golangci-lint, vet, `go test -race`, сборка).
2. `make import STAT=eia` — обе серии в логе.
3. MCP: `v_eia_petroleum` отдаёт обе серии; `v_gold_dashboard` по `distillate_stocks`:
   `stale=0`, свежее значение.
4. `make mcp-check` зелёный (добавлены проверки витрины, каталога и дашборда).
5. Повторный импорт идемпотентен (ReplacingMergeTree, без дублей).
