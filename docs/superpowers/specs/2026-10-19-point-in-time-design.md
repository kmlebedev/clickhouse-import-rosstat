# Спека: point-in-time слой значений с датой публикации

**Дата:** 19 октября 2026 г.
**Пункт роадмапа:** `key-rate/phase-4-point-in-time` — [ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) §6, фаза 4; issue [#126](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/126).
**План:** [2026-10-19-point-in-time.md](../superpowers/plans/2026-10-19-point-in-time.md).
**Статус документа:** спека дизайна. Нормативные правила не пересказываются — на них даются ссылки вида «стандарт §6.4» ([IMPORTER_PACKAGE_STANDARD.md](IMPORTER_PACKAGE_STANDARD.md)).

---

## 1. Зачем

Честный бэктест требует значений с датой **публикации**, а не периода: ИПЦ за сентябрь публикуется в октябре, и на заседании в начале октября его ещё нет ([KEY_RATE_MODEL.md](KEY_RATE_MODEL.md) §4). Сейчас скрипты контура (`key_rate_model.py`, `cpi_nowcast.py`, `cpi_monthly.py`) пользуются **консервативными лагами** («месяц M доступен в конце M+1»), а ревизии объясняющих рядов не учитываются вовсе.

Единственный источник контура с настоящими винтажами — приложения к бюллетеням ДИП ЦБ, уже собранные импортёром `cbr_dip_bulletin` ([ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) §4.4). В таблице 18 000 точек, 49 выпусков, 8 рядов, `publication_date` 2020-10-14 … 2026-10-14; SA-безработица за 2023-11-30 равна 2.885274 в выпуске `24-01` (опубликован 2024-02-06) и 2.916127 в `26-06` (2026-09-01). Таблица уже винтажная — не хватает **слоя чтения**: нормализованного long-представления и готового ответа на вопрос «что было известно строго до даты D».

## 2. Решение

### 2.1 Логический слой, без физической таблицы

Слой — **три витрины** над `cbr_dip_bulletin`, а не новая таблица. Причины:

- `cbr_dip_bulletin` уже хранит значение, период, дату публикации и идентификатор выпуска — копия 18 000 строк не даёт новой информации;
- принцип модели данных — «переиспользовать максимум, не заводить параллельных сущностей» ([ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) §5);
- витрины читают таблицу динамически: новый выпуск появляется в слое сразу после импорта, без пересборки.

Физическая таблица `point_in_time` понадобится, когда появится источник **без** собственных винтажей — для «заморозки» значений «как было опубликовано». Это отдельный пункт; в этой итерации он не делается.

### 2.2 Три витрины

| Витрина | Назначение |
|---|---|
| `v_point_in_time` | нормализованный long-слой: одна строка = одна версия значения |
| `v_point_in_time_asof` | параметризованный срез «что было известно строго до даты `as_of`» |
| `v_point_in_time_version` | версия набора данных для записи рядом с прогнозом (`forecast_log.data_version`) |

`v_point_in_time`:

```sql
SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
       publication_date AS published_at, release_id AS vintage, release AS vintage_label
FROM cbr_dip_bulletin FINAL
```

| Колонка | Тип | Смысл |
|---|---|---|
| `source` | `String` | источник слоя: `'cbr_dip_bulletin'` (совпадает с `v_series_catalog.source`) |
| `series` | `String` | имя ряда (8 рядов ДИП) |
| `period_date` | `Date` | отчётный период (для ДИП — последний день месяца), **не** дата публикации |
| `value` | `Float64` | значение версии |
| `published_at` | `Date` | дата, когда версия стала публичной (для ДИП — `publication_date`) |
| `vintage` | `String` | идентификатор версии (для ДИП — `release_id`) |
| `vintage_label` | `String` | читаемая метка (для ДИП — номер выпуска `26-07`) |

`v_point_in_time_asof` — параметризованная витрина ClickHouse (`{as_of:Date}`), вызов `SELECT … FROM v_point_in_time_asof(as_of='2024-02-07')`:

```sql
SELECT source, series, period_date,
       argMax(value, (published_at, vintage))         AS value,
       max(published_at)                              AS published_at,
       argMax(vintage, (published_at, vintage))       AS vintage,
       argMax(vintage_label, (published_at, vintage)) AS vintage_label
FROM (
    SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
           publication_date AS published_at, release_id AS vintage, release AS vintage_label
    FROM cbr_dip_bulletin FINAL
    WHERE publication_date < {as_of:Date}
)
GROUP BY source, series, period_date
```

`v_point_in_time_version`:

```sql
SELECT source,
       count()                                    AS points,
       uniqExact(vintage)                         AS vintages,
       min(published_at)                          AS first_publication,
       max(published_at)                          AS last_publication,
       hex(groupBitXor(cityHash64(series, period_date, published_at, vintage, vintage_label, value))) AS data_version
FROM (
    SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
           publication_date AS published_at, release_id AS vintage, release AS vintage_label
    FROM cbr_dip_bulletin FINAL
)
GROUP BY source
```

### 2.3 Семантика среза

- Граница **строгая**: `published_at < as_of`. Значение, опубликованное ровно в дату `as_of`, в срез не входит: решение ЦБ публикуется в 13:30 и может быть раньше публикации приложения. Это буквальная формулировка «что было известно строго до неё» ([ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) §5).
- Тай-брейк версий — `(published_at, vintage)`, а не только дата: два выпуска одного дня не должны давать произвольную строку.
- Отсутствие ряда в новом выпуске значит «этот выпуск его не показывает», а не «ряд удалён»: последняя известная версия остаётся видимой. Это свойство самого источника (лист рынка труда отсутствует в 13 выпусках из 49; в `26-07` безработицу заменил уровень занятости).
- Отчётный период не фильтруется: слой отвечает на вопрос «что было опубликовано до D», а не «какие периоды закончились до D». Для ДИП период всегда раньше публикации (проверяется инвариантом приёмки `period_date <= published_at`).

### 2.4 Версия набора данных

`data_version` — `hex(groupBitXor(cityHash64(...)))` по всем строкам источника: 16 hex-символов, меняется при добавлении/ревизии/удалении любой строки и стабилен на повторе. Для `forecast_log.data_version` рекомендована строка `pit:cbr_dip_bulletin:<data_version>`; запись в журнал — существующий контракт пункта `key-rate/phase-4-forecast-log` (issue #127), в этом пункте не меняется.

### 2.5 Где создаются витрины

- Определения — новый файл `cbr/pit_views.go` (`cbrPitViews`, `ensurePitViews`).
- `cbrDipStat.Import` вызывает `ensurePitViews` **после** цикла выпусков и на no-op-пути, перед возвратом: сбой витрин не откатывает уже записанные данные, а самолечение работает и без новых выпусков (прецедент — `cbr_mt_forecast`, где производные публикуются на no-op-пути).
- `afterCbrDip` (каталог рядов + `v_cbr_dip_bulletin`) не меняется.
- `util.CreateView` с `Tables: []string{"cbr_dip_bulletin"}`; слой не зависит от `v_cbr_dip_bulletin`.
- Отдельного импортёра, пакета и шага `dagu` нет: слой — производная существующего ежедневного шага `cbr_dip_bulletin` (`dagu/cbr.yaml`, 09:17 МСК). Витрины статичны, данные читаются динамически.

### 2.6 MCP

- `GRANT SELECT` на три витрины в `sql/mcp_kimi_reader.sql`; сырая `cbr_dip_bulletin` агенту не выдаётся (как и раньше).
- `scripts/mcp_check.py`: слой непуст, 49 версий, 8 рядов; as-of на трёх датах даёт ожидаемую ревизию (см. §4); версия набора непуста; сырая таблица закрыта (497).
- Ограничение ClickHouse: у параметризованной витрины нет схемы без аргументов, поэтому `system.columns`/`DESCRIBE` без параметров её не описывают и колонковые комментарии к ней не ставятся. Смысл колонок — в `v_point_in_time`; табличный комментарий параметризованной витрины содержит форму вызова и пример.

## 3. Отступления и решения владельца

| ID | Решение | Обоснование |
|---|---|---|
| О1 | Слой — **витрины**, а не физическая таблица | `cbr_dip_bulletin` уже винтажная; копия не даёт информации, физическая таблица — отдельный пункт для источников без винтажей |
| О2 | As-of — **параметризованная витрина** ClickHouse | Готовый вызов для агента и скриптов; CH 26.10 поддерживает параметризованные витрины |
| О3 | Граница среза — **строгая** (`<`) | Решение ЦБ в 13:30 может быть раньше публикации; формулировка роадмапа «строго до неё» |

## 4. Критерий приёмки

1. `make all` — зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки).
2. `make import STAT=cbr_dip_bulletin` — витрины созданы/пересозданы, повтор стабилен; на no-op-пути создание выполняется (самолечение). Затем `make mcp-user`, `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
3. MCP (`kimi_reader`):
   - `v_point_in_time` — 18 000 строк, 49 версий, 8 рядов, `min(published_at)` 2020-10-14, `max` 2026-10-14;
   - `v_point_in_time_asof(as_of='2024-02-06')` по `unemployment_sa` / 2023-11-30 — **пусто**;
   - `as_of='2024-02-07'` — **2.885274**, `vintage=47847`, `vintage_label='24-01'`;
   - `as_of='2026-09-02'` — **2.916127**, `vintage=62343`, `vintage_label='26-06'`;
   - `v_point_in_time_version` — одна строка `source='cbr_dip_bulletin'`, 49 версий, непустой `data_version`;
   - сырая `cbr_dip_bulletin` — `ACCESS_DENIED` (497).
4. `dagu validate dagu/cbr.yaml` — exit 0; `make docs-check` — зелёный.
5. Документация обновлена тем же PR: `ARCHITECTURE.md` §6.1/§6.3/§6.5, `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/KEY_RATE_MODEL.md` §4, `docs/ROADMAP_KEY_RATE.md`.

## 5. Ограничения

- Слой покрывает только ряды ДИП (8 рядов). Остальные источники контура по-прежнему используют консервативные лаги публикации; это названо в `docs/LIMITATIONS.md` и `KEY_RATE_MODEL.md` §4.
- `published_at` — дата публикации выпуска из атрибута ссылки ЦБ, а не время суток. Строгий `<` — консервативная граница для решения в 13:30.
- Замена файла выпуска под тем же `release_id` и правка `publication_date` под тем же `id` гейтом `cbr_dip_bulletin` не отслеживаются (известное ограничение импортёра): слой читает текущее состояние таблицы.
- `data_version` — вероятностная свёртка (`groupBitXor` от `cityHash64`), а не криптографический хэш; для версии набора данных достаточно.
- Параметризованная витрина не имеет колонковых комментариев и не описывается `DESCRIBE` без аргумента; это ограничение ClickHouse, а не слоя.

## 6. Что осознанно не входит

- Физическая таблица `point_in_time` и заморозка значений источников без винтажей.
- Перевод `scripts/key_rate_model.py` / `cpi_monthly.py` на PIT: их признаки (`cbr_indicators_cpd`, `cbr_survey`) не входят в ДИП.
- Запись `data_version` в `forecast_log` — существующий контракт #127.
- Расширение набора рядов ДИП (SA-выпуск, розница, кредиты и т.п.) — отдельный пункт спеки `cbr_dip_bulletin` §6.

## 7. Ссылки

- [ROADMAP_KEY_RATE.md](ROADMAP_KEY_RATE.md) §4.4, §5, §6 фаза 4.
- [KEY_RATE_MODEL.md](KEY_RATE_MODEL.md) §4 — требование point-in-time.
- [IMPORTER_PACKAGE_STANDARD.md](IMPORTER_PACKAGE_STANDARD.md) §10 — витрины, §11 — тесты, §16 — приёмка.
- [2026-10-15-cbr-dip-bulletin-design.md](2026-10-15-cbr-dip-bulletin-design.md) — источник и винтажная таблица.
- [ARCHITECTURE.md](../ARCHITECTURE.md) §6.2 — канонические DDL, §6.3 — витрины, §6.5 — MCP.
