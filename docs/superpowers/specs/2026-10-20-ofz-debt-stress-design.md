# Спека: долговой контур ОФЗ — аукционы, госдолг, обслуживание

**Дата:** 2026-10-20
**Пункт:** `key-rate/phase-2-ofz-debt-stress` (issue [#170](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/170))
**Владелец:** [docs/ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №25
**Source report:** [2026-10-20-minfin-ofz-debt-sources.md](../../research/2026-10-20-minfin-ofz-debt-sources.md)
**План:** [2026-10-20-ofz-debt-stress.md](../plans/2026-10-20-ofz-debt-stress.md)

## 1. Цель

Закрыть долговой канал контура ключевой ставки: аукционы ОФЗ, запас внутреннего/внешнего долга и номинал ОФЗ, расходы на обслуживание и факт заимствований. Витрина `v_ofz_stress` даёт агенту и панели четыре метрики: покрытие спроса на аукционе, спред доходности отсечения к кривой, долю обслуживания в доходах и изменение запаса ОФЗ.

## 2. Источники

Источник и доступная история — по [source report](../research/2026-10-20-minfin-ofz-debt-sources.md). Кратко:

- аукционы: годовые XLSX 2023–2026, 2026 обновляется еженедельно;
- внутренний долг: legacy `.xls` 2009–2026 (всего и гарантии);
- внешний долг: XLSX 2011–2026 (всего и гарантии);
- ОФЗ-номинал: 2022 год — legacy `.xls`; 2023-05+ — месячные `debtDynamicsMonth_*.xlsx`, с пропусками;
- обслуживание: строка `2.13` федерального бюджета, 2011+;
- заимствования: факт из `indicatorsBudget_*.xlsx`, текущий год.

## 3. Таблицы

Все таблицы — `minfin_`-пространство, `ReplacingMergeTree`, значения `Float64`, измерения `LowCardinality(String)`. DDL — `%s`-форма шаблона `util.ClickHouseImport`.

### 3.1. `minfin_ofz_auction`

```sql
CREATE TABLE IF NOT EXISTS %s (
    date Date,
    format LowCardinality(String),
    issue LowCardinality(String),
    paper_type LowCardinality(String),
    maturity_date Date,
    days_to_maturity UInt32,
    offer_volume Float64,
    cutoff_price Nullable(Float64),
    weighted_price Nullable(Float64),
    cutoff_yield Nullable(Float64),
    weighted_yield Nullable(Float64),
    demand Nullable(Float64),
    placed Nullable(Float64),
    revenue Nullable(Float64),
    coverage Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (date, issue, format);
```

- Единицы: объёмы — млн руб.; цены — % от номинала; доходности — % годовых; `coverage` — доля.
- `format`: `Аукцион` (в 2023 — единственное значение) или `ДРПА`.
- `-`/`****`/пустая ячейка — `NULL`; `coverage` — из источника, при отсутствии `placed/demand`.
- Ключ `(date, issue, format)`; повторный импорт перезаписывает строку.

### 3.2. `minfin_debt_stock`

```sql
CREATE TABLE IF NOT EXISTS %s (
    name LowCardinality(String),
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (name, date);
```

Ряды и единицы:

| `name` | Единица | Источник |
|---|---|---|
| `Внутренний долг, всего` | млрд руб. | `Obem_gosdolga...xls`, строка «всего» |
| `Внутренний долг, в т.ч. гарантии` | млрд руб. | тот же файл, строка гарантий |
| `ОФЗ, номинал` | млрд руб. | сумма ОФЗ-ПК/ПД/АД/ИН (и ОФЗ-н в 2022) |
| `ОФЗ-ПК`, `ОФЗ-ПД`, `ОФЗ-АД`, `ОФЗ-ИН`, `ОФЗ-н` | млрд руб. | те же файлы, по видам; `ОФЗ-н` — только 2022 |
| `Внешний долг, всего` | млн долл. США | `GVD_*.xlsx`, колонка «Всего» |
| `Внешний долг, в т.ч. гарантии` | млн долл. США | тот же файл, колонка гарантий |

Дата — 1-е число месяца. `ОФЗ, номинал` не включает `ГСО-ППС`/`ГСО-ФПС`. Пропуски ОФЗ-номинала — NULL, не ноль.

### 3.3. `minfin_debt_service`

```sql
CREATE TABLE IF NOT EXISTS %s (
    name LowCardinality(String),
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (name, date);
```

| `name` | Единица | Источник |
|---|---|---|
| `Обслуживание государственного и муниципального долга` | млрд руб. за месяц | бюджетный XLSX, строка 2.13, накопление → разности; **2011-01 … 2021-12** (с 2022 строка не заполняется) |
| `Привлечение` | млрд руб. за месяц | `indicatorsBudget`, млн → млрд; 2024–2026 |
| `Погашение основного долга` | млрд руб. за месяц | там же |
| `Чистое привлечение` | млрд руб. за месяц | там же |
| `Расходы на обслуживание` | млрд руб. за месяц | там же (федеральные ценные бумаги; уже, чем строка 2.13); 2024–2026 |

План заимствований не импортируется — нет машиночитаемого стабильного ряда; факт — названная замена. Годовые `indicatorsBudget_01.01.2025.xlsx` (2024) и `01.01.2026.xlsx` (2025) подтверждены; 2022–2023 — пробел.

## 4. Импортёры

Все — в `minfin/`, регистрация только в `minfin/importers.go`.

### 4.1. `minfin_ofz_auction`

- Свой `Import()` по списку годовых файлов; на каждый файл — отдельный `util.ClickHouseImport` (прецедент `cbr_dip_bulletin`).
- Текущий год: резолв со страницы тега `1349` → документ «Результаты проведенных аукционов … в <год> году» → XLSX; `PublicationCountPoints = "SELECT count() FROM minfin_ofz_auction FINAL"` (HEAD-префлайт и архив версии).
- 2023, 2024, 2025 — константы source report; 2024 — прямой URL файла. Для исторических файлов — гейт `SELECT count() FROM minfin_ofz_auction FINAL WHERE toYear(date) = <год>`; финальный файл повторно не качается.
- Разбор — по нормализованным подписям шапки, обе версии (14/15 колонок), fail-fast на пропавшую шапку/колонку/ноль точек.

### 4.2. `minfin_debt_stock`

- Свой `Import()` из трёх блоков:
  1. внутренний `.xls` — `BodyFunc` (`xlsReader`) со страницы `id_4=93479`;
  2. внешний `.xlsx` — `ImportFunc` со страницы `id_4=69444`;
  3. ОФЗ-номинал: годовой `.xls` 2022 (`BodyFunc`) + месячные `debtDynamicsMonth_*.xlsx` (`ImportFunc`).
- Месячные файлы: на первом прогоне (ряд `ОФЗ, номинал` пуст) — бэкфилл доступных месяцев 2023-05…текущий по документированному шаблону URL; отсутствующие файлы (404) пропускаются. На повторных прогонах — только последний документ со страницы тега `76` (первый матч «Структура и динамика…») и его XLSX; принудительный полный проход — `MINFIN_DEBT_STOCK_BACKFILL=1` (ручной догон пропущенного месяца).
- `AfterImport` — каталог всех рядов и `v_ofz_stress`.

### 4.3. `minfin_debt_service`

- Свой `Import()` из двух файлов:
  1. бюджетный `*_mes.xlsx` (та же страница `fedbud/execute`) — `ImportFunc` с `fedbudLayout` из одной строки `Обслуживание государственного и муниципального долга`;
  2. `indicatorsBudget_*.xlsx` — резолв со страницы тега `1192` по документу «Основные показатели исполнения федерального бюджета … в части государственных внутренних заимствований» → XLSX; `ImportFunc` с разбором месяцев и колонок, млн → млрд.
- Оба файла обновляются раз в месяц; шаг DAG — monthly, префлайт не объявляется. Повтор перезаписывает точки, `count()` не меняется.

## 5. Витрина `v_ofz_stress`

Одна строка на аукцион (`date`, `issue`, `format`). Колонки:

| Колонка | Смысл |
|---|---|
| `date`, `issue`, `paper_type`, `format`, `days_to_maturity` | атрибуты аукциона |
| `demand`, `placed` | млн руб. |
| `coverage_ratio` | `coalesce(coverage, placed/demand)`; NULL — нет данных |
| `secondary_tenor`, `secondary_yield` | ближайший тенор КБД ЦБ (`cbr_zcyc`) на дату аукциона по `days_to_maturity`; NULL — нет точки |
| `cutoff_yield` | доходность отсечения, % годовых |
| `cutoff_minus_secondary_pp` | `cutoff_yield − secondary_yield`, п.п. |
| `debt_service`, `debt_service_source`, `revenue` | месячные потоки, млрд руб.; `debt_service_source` = `budget` (до 2022, шире) / `securities` (с 2024, федеральные бумаги) / пусто (нет месяца) |
| `debt_service_to_revenue` | доля, 0.05 = 5 %; NULL — нет месяца; 2022–2023 NULL (пробел источника) |
| `ofz_stock`, `ofz_stock_change` | номинал ОФЗ на 1-е число месяца и изменение к предыдущему месяцу, млрд руб. |

Месячные метрики повторяются на каждом аукционе месяца — это свойство витрины, оговорено в комментарии. `cbr_zcyc` — кривая бескупонной доходности, а не сделки вторичного рынка; в комментарии колонки это названо. Витрина читает `minfin_ofz_auction`, `minfin_debt_stock`, `minfin_debt_service`, `minfin_fed_bud_mes` (`Доходы, всего`) и `cbr_zcyc`; создаётся `util.CreateView`, когда все пять таблиц существуют.

## 6. Каталог, комментарии, грант

- `series_catalog`: все ряды `minfin_debt_stock` и `minfin_debt_service` с `title`, `unit`, `frequency`, `origin`, `description`; метрики аукциона (`demand`, `placed`, `cutoff_yield`, `weighted_yield`, `coverage_ratio`) — с `frequency='D'` и оговоркой, что дата — дата аукциона.
- Комментарии таблицы и каждой колонки `v_ofz_stress` — через `util.CreateView`.
- `GRANT SELECT ON default.v_ofz_stress TO kimi_reader` в `sql/mcp_kimi_reader.sql`; сырые таблицы агенту не выдаются.
- `scripts/mcp_check.py`: витрина читается и непуста, каталог содержит ряды, сырые таблицы отвечают 497, spot-точка аукциона и месячная метрика.

## 7. Расписание

- `dagu/minfin.yaml` (10-е число 10:41, monthly): в хвост — `minfin_debt_stock`, `minfin_debt_service`.
- Новый `dagu/ofz.yaml` (weekly, четверг после аукционов): `minfin_ofz_auction`.
- Тест реестра `minfin/importers_test.go` читает оба файла и проверяет шаги в обе стороны.

## 8. Grafana

Панели в существующем `key-rate-publications` (юниты не смешивать):

- «Аукционы ОФЗ: покрытие и спрос» — `coverage_ratio` (percentunit), `cutoff_minus_secondary_pp` (pp);
- «Долг: обслуживание и запас» — `debt_service_to_revenue` (percentunit), `ofz_stock_change` (currencyRUB, млрд).
- Запросы только к `v_ofz_stress`, `$__timeFilter(date)`; артефакт `dashboard/key-rate-publications.json` — `make grafana-build`.

## 9. Приёмка

- 3 таблицы + `v_ofz_stress`; история: аукционы 2023+, внутренний долг 2009+, внешний 2011+, ОФЗ-номинал 2022+ с названными пропусками, обслуживание 2011+.
- Тесты на живых фикстурах (обе шапки аукционов, legacy `.xls`, внешний XLSX, 2022 структура, месячный `debtDynamicsMonth`, indicators).
- Каталог, комментарии, грант, MCP-check, панели Grafana.
- `make all`, `make docs-check`, `dagu validate`; живой прогон: три `make import STAT=...`, повтор идемпотентен, `make mcp-check` — «ПРОВЕРКА ПРОЙДЕНА».

## 10. Названные отступления

1. Аукционы до 2023 не импортируются: годового машиночитаемого файла нет.
2. ОФЗ-номинал: пропуски 2023-02…04, 2024-05 и 2025-04…08 (файлы источника отсутствуют); `ΔOFZ_stock` в эти месяцы — NULL.
3. «Вторичный рынок» — КБД ЦБ (`cbr_zcyc`), ближайший тенор; не MOEX G-curve и не сделки.
4. «Программа заимствований» — факт привлечения/погашения из `indicatorsBudget`; план не машиночитаем.
5. Обслуживание: бюджетная строка 2011–2021, `indicatorsBudget` 2024–2026; 2022–2023 — NULL, с 2024 охват уже (федеральные бумаги).
6. Внешний долг — в млн долл. США; витрина использует только рублёвые ряды, единицы — в каталоге.
7. 2024 год аукционов — прямой URL файла: страница документа не найдена в теге `1349`.

## 11. Вне объёма

- Аукционы до 2023 и парсинг HTML-сообщений.
- План заимствований из приложения к закону о бюджете.
- Новый дашборд; панели добавляются в `key-rate-publications`.
- Изменение контракта `minfin_fed_bud_mes`/`minfin_fed_bud_mesyats`.
