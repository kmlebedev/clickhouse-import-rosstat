# Спека: недельный агрегат ИПЦ РФ из BI Росстата (`rosstat_bi_cpi_weekly`)

- Пункт: `key-rate/phase-4-bi-cpi-weekly` ([ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md))
- Issue: [#259](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/259), задачи [#260](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/260) (spike) и [#261](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/261) (импортёр)
- Research: [2026-10-20-cb-comp-public-sources-and-bi.md](../../research/2026-10-20-cb-comp-public-sources-and-bi.md) §5
- Роль: Go + research (spike в этой же ветке)

## 1. Цель

Закрыть недельный агрегат ИПЦ РФ за 2022+ и полный 2019 год из гостевого BI Росстата (`Dashboard/Prices_week`), не меняя имя и семантику существующего ряда `Индекс потребительских цен (недельная оценка)`. Товарные и региональные ряды BI — вне объёма.

## 2. Источник и транспорт

UI-путь (гость, без логина): `Consumer Price Index Estimation` → карточка `Consumer price index` → `⋮` → `Export` → CSV → `Export List` → `Export(csv)` → `Download`.

Spike 2026-10-20 нашёл воспроизводимый GWT-RPC:

- `POST https://bi.rosstat.gov.ru/biportal/grid`
- заголовки: `Content-Type: text/x-gwt-rpc; charset=UTF-8`, `X-GWT-Permutation: 0D850A6CF67C443893E598332DD51877`, `X-GWT-Module-Base: https://bi.rosstat.gov.ru/biportal/ddb/`
- тело: GWT-RPC `GridViewService.exportGrid` — литерал `biCpiWeeklyExportPayload` в `rosstat/bi_cpi_weekly.go` (снят из Network 2026-10-20)
- ответ: `//OK[1,["tmp/exp<uuid>/Индекс потребительских цен.csv"],0,7]` (`application/json`, `Content-Disposition: attachment`)
- файл: `GET https://bi.rosstat.gov.ru/biportal/<path>` — публичен, 200 без cookies, `text/csv`, `ETag`/`Last-Modified`

Повторный POST создаёт новый `exp<uuid>`; воспроизведённая выгрузка совпала с UI 398/398 (0 расхождений), `12.10.2026 = 100,11`.

**Хрупкость (осознанно принимается):** `X-GWT-Permutation`, serialization-policy hash `4E54CCD3C262C2F21A605D6A0465BA40`, id `report15`/`report18`/`slice1`/`view1` — часть сборки BI. При обновлении портала payload может перестать работать. Обновление: перехватить новый `POST /biportal/grid` в браузере и заменить константу; импортёр обязан падать с явной ошибкой, а не молча.

## 3. Данные

- 398 точек `2019-01-09 … 2026-10-12` (405 календарных недель).
- Семь новогодних недель источник не публикует: `2019-12-30`, `2021-01-04`, `2022-01-03`, `2023-01-02`, `2024-01-01`, `2025-01-01`, `2025-12-29`/`2026-01-05`. В бюллетенях их тоже нет. Это поведение источника, не пропуск разбора.
- CSV: 7 колонок `period,id_indicator,territory,territory.ccube.0,territory.ccube.1,territory.ccube.2,value`; дата `DD.MM.YY` (ru) или `M/D/YY` (en); десятичный разделитель — точка; порядок строк не гарантирован; значение — индекс, 100 = без изменения.
- Значения текущие, ревизии возможны; PIT/винтажей BI не даёт.

## 4. Схема

```sql
CREATE TABLE IF NOT EXISTS rosstat_bi_cpi_weekly (
    series LowCardinality(String),
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (series, date);
```

- Один ряд: `Индекс потребительских цен (недельная оценка)` (существующая строка, имя не меняется).
- DDL — канон в `ARCHITECTURE.md` §6.2; копия в импортёре обязана совпадать.
- Сырая таблица агенту не выдаётся; грант `kimi_reader` — только на `v_rosstat_macro` (уже есть).

## 5. Ряд и витрина

Отдельная таблица, а не `rosstat_ipc_weeks`:

- `rosstat_ipc_weeks` — XLSX 2022+ без агрегатной строки; смешивать два источника в одной таблице и ловить гонку с недельным XLSX-импортёром не нужно.
- Витрина `v_rosstat_macro` получает ветку BI **до** бюллетеней и анти-джойн по `(series, date)`:
  ```sql
  UNION ALL SELECT 'rosstat', series, date, value FROM rosstat_bi_cpi_weekly FINAL
  UNION ALL SELECT 'rosstat', name, date, toFloat64(percent)
    FROM rosstat_ipc_weeks_bulletins FINAL
    WHERE (name, date) NOT IN (SELECT series, date FROM rosstat_bi_cpi_weekly FINAL)
  ```
- Для 2019+ BI приоритетен (текущие ревизии), до 2019 — бюллетени. Имя ряда сохраняется, потребители `scripts/cpi_monthly_v3.py` и `scripts/cpi_public_panel_audit.py` не меняются.
- `series_catalog`: одна общая мета агрегата для бюллетеней и BI (иначе upsert флапает между прогонами). В описании: 2009–2018 — бюллетени с разрывами; 2019-01-09+ — BI; семь новогодних перерывов; значения текущие, ревизии, без PIT.

## 6. Импортёр

Пакет `rosstat/`, файл `bi_cpi_weekly.go`, структура `BICpiWeeklyStat` со встроенным `util.ClickHouseImport`:

- `TableName = "rosstat_bi_cpi_weekly"`, `CreateTable`, `BodyFunc` — CSV-парсер, `AfterImport: afterRosstatMacro(biCpiWeeklySeriesMeta())`.
- Свой `Import()` (прецедент `rosstat_ipc_weeks`): POST export-RPC → разбор `//OK[...]` → URL-encode пути → `s.DataUrl = https://bi.rosstat.gov.ru/biportal/<path>` → делегирование в `s.ClickHouseImport.Import(ctx, conn)`; шаблон качает CSV (`GetFileBytes`) и отдаёт байты в `BodyFunc`.
- Новый util-хелпер `util.PostFileBytes(url, contentType string, body []byte, headers map[string]string)` с тем же `HttpClient`, UA и дедлайном `fileTimeout`; ответ — байты, не-2xx — ошибка.
- `PublicationCountPoints` **не заполняется**: export-URL динамический (новый `exp<uuid>` на каждый POST), HEAD-префлайт и архив версий недостижимы. Идемпотентность — `ReplacingMergeTree` + `FINAL`; повторный прогон скачивает CSV, но не плодит точки; `max(date)` не откатывается (нет delete).
- Парсер:
  - `encoding/csv` (`Comma: ','`), снятие BOM; поиск шапки по `period`/`value`; пропуск строк до шапки.
  - Дата: `02.01.06`, `1/2/06`, `2006-01-02`; значение: `ParseFloat` с заменой запятой на точку; NaN/Inf — ошибка.
  - Проверка `id_indicator`/`territory`: принимаются английские и русские подписи агрегата РФ; чужая карточка — ошибка.
  - Сортировка по дате, дубликаты — последнее значение; минимум = `2019-01-09`; 0 точек — ошибка; разрывы не ошибка (новогодние перерывы), но `max(date)` логируется.
- Логирование — logrus, без `fmt.Print`.

## 7. Тесты и фикстуры

- `rosstat/testdata/bi_cpi_weekly.csv` — реальная урезанная выгрузка (шапка + 3 строки: 09.01.19, 2022-неделя, 12.10.26 = 100.11), 7 колонок.
- Парсер: число точек, spot-значения, оба формата даты, BOM, десятичная точка/запятая, нечисло/битая дата → ошибка, пустая выгрузка → ошибка, дубликат → одно значение, разрыв не ломает разбор, сортировка не по порядку.
- RPC-ответ: `//OK[1,["tmp/exp.../Индекс потребительских цен.csv"],0,7]` → путь; не-`//OK` → ошибка.
- URL-encode: пробелы/кириллица.
- `httptest` для POST-хелпера (200/не-2xx).
- `importers_test.go`: новый импортёр с префиксом, в реестре, шаг dagu, без preflight; `series_meta_test.go`: общая мета агрегата.

## 8. Приёмка

1. `make all` — зелёный.
2. `make import STAT=rosstat_bi_cpi_weekly` — `Imported N rows`; `min(date)=2019-01-09`, `max(date)` = текущая неделя, 398+ уникальных дат.
3. `v_rosstat_macro`: 12.10.2026 = 100,11; 2–3 недели 2019/2022 против UI.
4. Повтор — те же точки, `count() FINAL` не растёт, `max(date)` не уменьшается; сеть есть (URL динамический), результат идемпотентен.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/rosstat.yaml`; `make docs-check`.
6. Протокол — комментарием в PR.

## 9. Границы

- Только агрегат РФ; товарные/региональные ряды BI — отдельный пункт.
- 2009–2018 BI не покрывает; бюллетени остаются.
- PIT/винтажи не заявляются.
- `rosstat_ipc_weeks` и `rosstat_ipc_weeks_bulletins` не ломаются.
