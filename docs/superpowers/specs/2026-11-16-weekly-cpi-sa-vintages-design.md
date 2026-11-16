# Винтажи недельного SA-ИПЦ — #630

Пункт `key-rate/phase-4-weekly-cpi-sa-vintages`, role:go. Контекст — дефект Ф4 бэктеста
`economist-oracle` (#622; отчёт `docs/research/2026-11-16-economist-oracle-backtest-data-gaps.md`
в PR #623).

## Дефект

`weekly_cpi_sa` (`weeklysa/`) пересчитывается целиком на каждом прогоне: одна строка на месяц,
ключ `(date, series, method, spec_version)`, `ReplacingMergeTree(loaded_at)` — новая версия
затирает старую. Винтажей по неделям нет, поэтому «инфляция с начала месяца» (nowcast-месяц)
и предыдущие SA-значения на прошлую дату не восстанавливаются: бэктест видит сегодняшний
пересчёт, а не то, что было известно на дату прогона.

## Винтаж

Винтаж = состояние входа, из которого посчитан ряд:

- `vintage Date` — последняя дата 7-дневной сетки (отсечка данных), читаемая «по неделям»;
- `computed_at DateTime('Europe/Moscow')` — момент расчёта: различает ревизии одного и того же
  последнего воскресенья (источник правит прошлые недели, хвост не двигается).

История винтажей — отдельная append-only таблица `weekly_cpi_sa_vintage` с ключом
`(date, series, method, spec_version, vintage, computed_at)`: каждый новый вход даёт полный
винтаж (≈211 строк), ревизия источника не затирает прежний расчёт. Основная `weekly_cpi_sa`
остаётся «последней» и не меняет ключ: `MODIFY ORDER BY` существующей таблицы ClickHouse для
уже добавленных колонок запрещает. Гейт по SHA-256 входа не меняется — повтор с тем же входом
остаётся no-op и новых строк не пишет.

## Схема и миграция

```sql
-- основная таблица: последний винтаж; ключ не меняется
CREATE TABLE IF NOT EXISTS weekly_cpi_sa (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
  , vintage        Date
  , computed_at    DateTime('Europe/Moscow')
  , sa_index       Float64
  , sa_4w          Float64
  , saar           Float64
  , as_of          Date DEFAULT '1970-01-01'
  , partial        UInt8 DEFAULT 0
  , published_at   Nullable(Date)
  , loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version);

-- история винтажей: append-only, ключ с отсечкой входа и моментом расчёта
CREATE TABLE IF NOT EXISTS weekly_cpi_sa_vintage (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
  , vintage        Date
  , computed_at    DateTime('Europe/Moscow')
  , sa_index       Float64
  , sa_4w          Float64
  , saar           Float64
  , as_of          Date
  , partial        UInt8
  , published_at   Nullable(Date)
  , loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version, vintage, computed_at);
```

Миграция живой таблицы идемпотентна: `ADD COLUMN IF NOT EXISTS` для `vintage`/`computed_at`;
бэкфилл старых строк — `vintage = as_of, computed_at = loaded_at` (у них `vintage='1970-01-01'`,
а строки поколения с `as_of='1970-01-01'` исключены из guard-а и UPDATE), однократный
`ALTER TABLE … UPDATE … SETTINGS mutations_sync=2` под guard-запросом. Пустая
таблица истории при первом прогоне заполняется копией текущих строк (`INSERT … SELECT …
FROM weekly_cpi_sa FINAL`) под guard-запросом `count() = 0`.
`weeklySAFeatureVersion` поднимается (`weekly-cpi-sa-vintage-v2`): первый прогон после фикса
обязан пересчитать и записать винтаж, а не стать no-op на старом отпечатке.

## Витрины

- `v_weekly_cpi_sa` — как раньше, последний винтаж: `WHERE computed_at = (SELECT max(computed_at)
  FROM weekly_cpi_sa FINAL)`; к прежним колонкам добавлен `vintage`. Расчёт «вчерашнего» вида
  не меняется.
- `v_weekly_cpi_sa_vintage` — все винтажи, long-форма: `source, series, date, value, unit,
  frequency, as_of, partial, vintage, computed_at, published_at`.
- `v_weekly_cpi_sa_asof(vintage_as_of='YYYY-MM-DD')` — срез «что было известно строго до даты»:
  граница `published_at < {vintage_as_of:Date}` (как `v_point_in_time_asof`), для каждого
  `(source, series, date)` — последняя версия по тай-брейку `(published_at, vintage, computed_at)`.
  У параметризованной витрины нет колонковых комментариев — смысл в комментарии таблицы и в
  `v_weekly_cpi_sa_vintage`.
- `v_weekly_cpi_quarterly` и `v_weekly_cpi_rolling3m` читают последний винтаж (тот же фильтр
  `max(computed_at)`), иначе строки размножатся по винтажам.

Гранты `kimi_reader` — на `v_weekly_cpi_sa_vintage` и `v_weekly_cpi_sa_asof`; сырая
`weekly_cpi_sa` остаётся закрытой.

## Хранение

TTL у истории нет: ~633 строки на новый вход, около 30 тыс. строк в год. При необходимости
политику хранения (TTL/архив) заводить отдельным пунктом — это осознанно не часть фикса.

## Инварианты

- `v_weekly_cpi_sa` возвращает ровно один ряд на месяц (211 строк на 2026-11-08: 210 полных +
  nowcast), как до фикса.
- `v_weekly_cpi_sa_vintage` содержит ≥1 винтаж: на неизменном входе винтаж один по построению
  (гейт по SHA-256 не пишет дубль), второй появляется с новым входом.
- `v_weekly_cpi_sa_asof` до `published_at` нового винтажа возвращает предыдущий срез; если
  известных винтажей нет — 0 строк, а не ошибка.
- Формула X-13, `spec_version`, сетка, источники и каталог (`weekly_cpi:*`) не меняются.
- Повтор импорта — `Imported 0 rows`; сырая таблица закрыта.

## Приёмка

- Unit: DDL/ALTER/бэкфилл/INSERT/витрины (vintage/computed_at в ключе и проекциях), фильтр
  последнего винтажа, as-of SQL.
- Dev: `make import STAT=weekly_cpi_sa` — бэкфилл истории и текущий винтаж; повтор — 0.
  `v_weekly_cpi_sa` — 633 строки с `vintage`; `v_weekly_cpi_sa_vintage` — 1 винтаж;
  `v_weekly_cpi_sa_asof('2026-11-14')` — 0 строк (до `published_at`), `as_of('2026-11-16')` —
  текущий винтаж.
- `make all`, `make docs-check`, `make mcp-check`.
