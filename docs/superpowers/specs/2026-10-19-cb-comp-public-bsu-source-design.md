# Публичный источник 87 кодов ОКВЭД2 и снятие оговорки (Task 7)

> **Объект:** Task 7 пункта `key-rate/phase-4-ml-components` (issue [#248](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/248), родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
> **Задача:** снять формулировку «87 внутренних кодов ОКВЭД2»: (1) завести публичный источник самих кодов — классификатор ОКВЭД2 Росстата; (2) построить публичный кроссворд 87 кодов → 16 отраслевых листов `mp_survey_data.xlsx` и витрину публичной панели; (3) зафиксировать, что live-источника значений на 87 кодах нет, и переформулировать оговорку.
> **Роль:** research + go (смешанный пункт; Go-часть — импортёры и витрины, research-часть — отчёт и решение).
> **Зависимости:** #221 (merged), #235 (closed), #76 (parent), #238 (Task 5, open). Работа не блокирует #238 и не заявляет вердикт v3.

**Статус:** реализация.

---

## 1. Проверенная предпосылка

Проверено живыми запросами 2026-10-19.

| Источник | URL / id | HTTP / формат | 87 кодов? | Live/static | Вывод |
|---|---|---|---|---|---|
| ЦБ, данные опросов | `https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx` | 200, XLSX, 2 193 461 байт, 31 лист | **нет**: 16 отраслевых листов, разрез по отраслям, не по кодам | live, обновляется ~10-го числа | 16 отраслевых агрегатов; код-уровневых значений нет |
| ЦБ, группы предприятий | `https://www.cbr.ru/Content/Document/File/162126/survey_data_group.xlsx` | 200, XLSX, 2 669 154 байт, 16 листов | нет: крупные/средние/малые | live | не код-уровень |
| ЦБ, данные по ГУ | `https://www.cbr.ru/Content/Document/File/178789/mp_survey_data_gu.xlsx` | 200, XLSX, 921 209 байт, 14 листов | нет: 7 макрорегионов | live | не код-уровень |
| ЦБ, факторы | `https://www.cbr.ru/Content/Document/File/194116/mp_survey_data_facts.xlsx` | 200, XLSX, 202 623 байт, 17 листов | нет: факторы | live | не код-уровень |
| ЦБ, сервис данных | `https://www.cbr.ru/statistics/data-service/` | HTML/JSON API; `/datasets` — 501 | нет документированного разреза по 87 кодам | live | per-code series не найдены |
| Росстат, ОКВЭД2 | `https://www.rosstat.gov.ru/opendata/7708234640-okvedva/meta.csv` → `data-20261001T1110-structure-20180402T1704.csv` | 200; meta CP1251; data UTF-8 BOM, `;`, 464 582 байт, 3 102 строки (20 разделов + 3 082 кода) | **да, 87/87** с названиями | live, ежемесячные выпуски | публичный словарь кодов; значений МП нет |
| Архив авторов CB_COMP | figshare DOI `10.6084/m9.figshare.27990260.v1`, `Models/CpiComponents/data/model/model.xlsx`, лист `1.0.4.2`; `data_panel_bi_sa.fst` | 7z/static; 87 уникальных кодов; панель 2009-01…2024-09 | **да**: 87 кодов × 3 индикатора | **static v1** | единственный публичный источник код-уровневых значений; не live |
| ЕМИСС / Росстат | показатели 57783/57806 и др. | HTML/API | нет мониторинга ЦБ по 87 кодам | live | другое обследование/ряды, не замена |

**Решение.** Live-источника значений на 87 кодах нет. Коды — стандартные ОКВЭД2 и публично проверяемы (Росстат, 87/87). Значения — из публичного архива авторов (статический срез 2009-01…2024-09). Публичная книга ЦБ даёт 16 отраслевых агрегатов; кроссворд 87→16 — публичная аппроксимация, а не замена код-уровневой панели.

## 2. Цель и границы

### 2.1. Цель

1. Импортёр `rosstat_okved2` — публичный классификатор ОКВЭД2 (все 3 102 строки, включая 87/87 BSU-кодов) в таблицу `rosstat_okved2`, витрина `v_rosstat_okved2`.
2. Импортёр `cbr_mp_bsu_map` — публичный кроссворд 87 кодов → отраслевой лист/SA-серия в таблицу `cbr_mp_bsu_map`, витрины `v_cbr_mp_bsu_map` и `v_cbr_mp_public_panel`.
3. Research-отчёт `docs/research/2026-10-19-cb-comp-public-bsu-source.md` с таблицей источников и решением.
4. Переформулировка оговорки в роадмапе, отчётах, статье, спеках и issue.

### 2.2. Не входит

- Точный внутренний mapping ЦБ «код → лист» и data request в ЦБ.
- Rerun per-BSU на публичной панели — отдельный ML-пункт: кроссворд схлопывает 87 рядов в 33 публичные серии (11 листов × 3; у 87 кодов нет электричества и водоснабжения) и не является код-уровневой репликацией.
- Изменение существующих 13 агрегатных и 368 отраслевых рядов `cbr_survey`, их имён и значений.
- Изменения в `financial/`.
- Новые ряды в `series_catalog`: `rosstat_okved2` и `cbr_mp_bsu_map` — reference-таблицы, не временные ряды.

## 3. `rosstat_okved2` — публичный классификатор

### 3.1. Источник и транспорт

- `meta.csv`: `https://www.rosstat.gov.ru/opendata/7708234640-okvedva/meta.csv`, CP1251, `property,value`, строка `modified=20261001`; первая (самая свежая) строка `data-*.csv` даёт URL выпуска.
- Данные: UTF-8 BOM, `;`, quoted CSV, поля `Razdel;Code;Name` (структура `structure-20180402T1704.csv`).
- `BeforeImport` качает `meta.csv` через `util.GetFileBytes`, декодирует CP1251 (`golang.org/x/text/encoding/charmap`), выбирает первый `data-*.csv` и заполняет `DataUrl`; в `init()` сети нет.
- `BodyFunc` получает байты data CSV от шаблона (тот же срез уходит в винтажный архив), снимает BOM, парсит `encoding/csv` с `Comma=';'`.

### 3.2. Разбор

- Строка раздела: `Code` = `" "` (пробел) → `code=""`, `level=1`, `parent_code=""`.
- Остальные: `level = strings.Count(code, ".") + 2` (класс 2, подкласс 3, группа 4, подгруппа 5, вид 6); `parent_code` — код без последнего сегмента.
- `section` — первое поле (буква раздела A…U); `name` — третье поле; пустые/битые строки — ошибка с номером строки, а не молчаливый пропуск.
- Ожидание: 3 102 строки, 20 разделов, 3 082 кода. 87/87 BSU-кодов проверяет `cbr_mp_bsu_map` по `rosstat_okved2` (live-приёмка — тем же запросом); пакет `rosstat` не зависит от списка CB_COMP.

### 3.3. DDL

```sql
CREATE TABLE IF NOT EXISTS rosstat_okved2 (
    section        LowCardinality(String),  -- 'A'…'U'
    code           LowCardinality(String),  -- '01', '01.1', '01.11', …; у раздела пусто
    name           String,
    level          UInt8,                   -- 1 раздел, 2 класс, 3 подкласс, 4 группа, 5 подгруппа, 6 вид
    parent_code    LowCardinality(String),
    source_version LowCardinality(String),  -- basename data-*.csv
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (section, code);
```

Витрина `v_rosstat_okved2` (`util.View`, комментарии таблицы и колонок):
`section, code, name, level, parent_code, source_version`.

Точка контроля публикации: `PublicationCountPoints = "SELECT count() FROM rosstat_okved2 FINAL"`, `PublicationSource = "rosstat_okved2"`. Повтор — no-op без скачивания; новый месячный выпуск — импорт.

### 3.4. Расписание и регистрация

- `rosstat/importers.go`: запись `{rosstatOkved2Importer, "rosstat_okved2", "SELECT count() FROM rosstat_okved2 FINAL"}`.
- `dagu/rosstat.yaml`: последний шаг `rosstat_okved2` (после `rosstat_ipc_weeks_bulletins`), retry 3 × 300 с.
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_rosstat_okved2 TO kimi_reader`.

## 4. `cbr_mp_bsu_map` — публичный кроссворд 87 → 16

### 4.1. Вход

87 уникальных кодов из публичного `model.xlsx` (`1.0.4.2`, поля `мониторинг_код1..4`), figshare DOI `10.6084/m9.figshare.27990260.v1`. Список фиксируется в Go (`cbr/mp_bsu_codes.go`) и в фикстуре `cbr/testdata/cb_comp_bsu_codes.csv` (`code;name;section`). Тест сверяет Go-список с фикстурой и с классификатором (87/87, без дублей).

### 4.2. Правило кроссворда

`publicIndustryForCode(code) (industrySheet, basis, confidence)` — детерминированная функция по публичному ОКВЭД2:

| Раздел/класс | Лист | Slug | confidence |
|---|---|---|---|
| A `01–03` | Сельское хозяйство | `agriculture` | rule |
| B `05–09` | Добыча | `mining` | rule |
| C `10–33` | Обработка + подкатегория (см. ниже) | `manufacturing*` | manual/fallback |
| D `35` | ЭЭГП | `electricity` | rule |
| E `36–39` | Водоснабжение | `water` | rule |
| F `41–43` | Строительство | `construction` | rule |
| G `45` | Торговля авто (45) | `trade_auto` | rule |
| G `46` | Торговля оптовая (46) | `trade_wholesale` | rule |
| G `47` | Торговля розничная (47) | `trade_retail` | rule |
| H `49–53` | Транспортировка и хранение | `transport` | rule |
| I–U `55–96` | Услуги | `services` | rule |

Подкатегории обработки (26 кодов):

- `manufacturing_consumer` (`confidence=manual`): `10.1, 10.20, 10.3, 10.4, 10.5, 10.61, 10.7, 10.81, 10.82, 11.0, 11.07, 13.20, 14, 14.1, 15.20, 18.1, 20.4, 21.20, 31.0, 32.1, 32.99`.
- `manufacturing_investment` (`manual`): `29.10`.
- `manufacturing_intermediate` (`manual`): `10.9, 17.2, 19.20, 23`.
- fallback — `manufacturing` (`fallback`); в текущем наборе не используется.

`basis` — текст: `"OKVED2 section A"`, `"OKVED2 class 47 (retail)"`, `"OKVED2 class 10; consumer goods (manual public crosswalk)"` и т.п. Спорные `29.10`, `18.1`, `32.99` названы в research-отчёте; правка — одна строка маппера.

### 4.3. DDL

```sql
CREATE TABLE IF NOT EXISTS cbr_mp_bsu_map (
    code           LowCardinality(String),
    industry_sheet LowCardinality(String),
    industry_slug  LowCardinality(String),
    basis          String,
    confidence     LowCardinality(String),  -- rule|manual|fallback
    source         LowCardinality(String),  -- figshare DOI + model.xlsx#1.0.4.2
    loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY code;
```

### 4.4. Импортёр

Свой `Import()` (без сети, прецедент `bank_company_metrics`/`ml_registry`):
1. DDL `cbr_mp_bsu_map`.
2. Один батч 87 строк из Go-списка; `batch.Append` с проверкой; `Send`.
3. Витрины через `util.CreateView` (см. §4.5); ошибка витрины — ошибка импорта.
4. `Imported 87 rows`; повтор идемпотентен (87 строк `FINAL`).

Регистрация: `cbr/importers.go`, `{cbrMpBsuMapImporter, "cbr_mp_bsu_map", ""}` (префлайта нет — сети нет).
`dagu/cbr.yaml`: последний шаг `cbr_mp_bsu_map` (после `cbr_mp_survey`); в `keyScheduled` теста реестра — причина «публичный кроссворд BSU не пересобирается».

### 4.5. Витрины

1. `v_cbr_mp_bsu_map` — `code, okved_name, section, industry_sheet, industry_slug, basis, confidence, source`.
   `LEFT JOIN rosstat_okved2` (если классификатор ещё не импортирован, `CreateView` пропустит витрину; она появится после первого импорта `rosstat_okved2`). Комментарии: код стандартный, назначение — публичная аппроксимация, не внутренний mapping ЦБ.
2. `v_cbr_mp_public_panel` — `code, industry_sheet, industry_slug, indicator, date, value`.
   `INNER JOIN macro_series FINAL` по явным именам:
   - `ibk` → `industry_<slug>_ibk_sa`;
   - `price_exp` → `industry_<slug>_price_expectations_sa`;
   - `costs` → `industry_<slug>_cost_change_sa`.
   На снимке 2026-10: 87 × 3 × 297 = **77 517 строк**, 33 различные серии (11 используемых листов × 3), 3 индикатора. `Промышленность` (`total`), `Обработка` (`manufacturing`) и `Торговля (45+46+47)` (`trade`) — агрегаты, для отдельных кодов не используются; `manufacturing` остаётся fallback. Электричество (`electricity`) и водоснабжение (`water`) в 87 кодах не встречаются, поэтому кроссворд их не использует (16 листов книги — не то же самое, что 11 используемых).

Гранты: `GRANT SELECT ON default.v_cbr_mp_bsu_map TO kimi_reader`, `GRANT SELECT ON default.v_cbr_mp_public_panel TO kimi_reader`.

### 4.6. MCP-check

- `v_rosstat_okved2`: 3 102 строки; 87/87 BSU-кодов.
- `v_cbr_mp_bsu_map`: 87 строк, 0 пустых `okved_name`.
- `v_cbr_mp_public_panel`: 87 кодов, 11 `industry_slug`, 3 `indicator`, 297 дат, `count() > 0`; точное число строк не хардкодить (рост месяцев не должен ломать проверку).
- Spot: `10.1 → manufacturing_consumer`, `29.10 → manufacturing_investment`, `23 → manufacturing_intermediate`, `47.22 → trade_retail`, `46.32 → trade_wholesale`, `45.11 → trade_auto`, `01.4 → agriculture`, `05 → mining`, `43.2 → construction`, `49.3 → transport`, `86.90.4 → services`.

## 5. Тесты

`rosstat/okved2_test.go`:
- `parseOkved2Meta` на CP1251-фикстуре: находит первый `data-*.csv`, не путает `standardversion`/`identifier`.
- `parseOkved2` на структурной фикстуре: раздел, класс, подкласс, группа, подгруппа, вид; `level`/`parent_code`; ожидаемое число строк на срезе.
- DDL drift: `rosstatOkved2Ddl` совпадает с каноническим блоком `ARCHITECTURE.md` §6.2.
- Реестр + `dagu/rosstat.yaml`: `Name()`/таблица с префиксом `rosstat_`, объявление префлайта, шаг в DAG.

`cbr/mp_bsu_map_test.go`:
- 87 уникальных кодов; Go-список == фикстура `cbr/testdata/cb_comp_bsu_codes.csv`, у каждого кода непустые name/section.
- Каждый код маппится в slug из `mpSurveyIndustrySheets`; 87/87 назначены; `confidence` ∈ {rule, manual, fallback}.
- 26 производственных кодов: 21 consumer + 1 investment + 4 intermediate; fallback не используется.
- `v_cbr_mp_public_panel` SQL содержит `cbr_survey`, `_ibk_sa`, `_price_expectations_sa`, `_cost_change_sa`.
- DDL drift; реестр + `dagu/cbr.yaml`; `keyScheduled`.

## 6. Research-отчёт и оговорка

Отчёт `docs/research/2026-10-19-cb-comp-public-bsu-source.md`: таблица §1, решение «live 87 кодов — нет», таблица кроссворда (сколько кодов на лист, `rule`/`manual`), ограничения.

Новая формулировка оговорки:

> 87 кодов — стандартные ОКВЭД2, публично проверяемые по классификатору Росстата (87/87). Значения по кодам — из публичного архива авторов CB_COMP (статический срез 2009-01…2024-09), не из живого фида ЦБ. Публичная книга ЦБ даёт 16 отраслевых агрегатов; кроссворд 87→16 — публичная аппроксимация (39 серий), а не замена код-уровневой панели. per-BSU остаётся исторической диагностикой, production-паритет не заявляется.

Обновить: `docs/ROADMAP_KEY_RATE.md` (абзац #221, Task 2/6 — если есть «внутренние»), `docs/research/2026-10-18-cb-comp-parity.md`, `docs/superpowers/specs/2026-10-19-cb-comp-feature-parity-design.md`, `docs/research/2026-10-19-cpi-industry-mp-features.md`, `docs/superpowers/specs/2026-10-19-cpi-industry-mp-features-design.md`, `docs/publications/2026-10-18-key-rate.md` §9/footnote, `docs/publications/2026-10-18/checklist.md`.

## 7. Приёмка

- Живые запросы источников записаны (URL, HTTP, формат, дата среза).
- `make import STAT=rosstat_okved2` — 3 102 строки; повтор — no-op; 87/87.
- `make import STAT=cbr_mp_bsu_map` — 87 строк; повтор — 87 `FINAL`.
- `v_cbr_mp_public_panel` — 77 517 строк / 11 slug / 3 indicator / 297 дат на снимке 2026-10.
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `make all`, `make docs-check`, `dagu validate` — зелёные.
- Оговорка переформулирована; паритет не заявлен.

## 8. Ограничения

- Точный внутренний mapping ЦБ не публичен; кроссворд — экспертная публичная аппроксимация.
- Подкатегории обработки не выводятся из ОКВЭД2 однозначно; 26 кодов размечены вручную (`manual`), спорные названы.
- Публичная панель — 39 серий, а не 87; per-BSU на ней — диагностика потери гранулярности.
- `v_cbr_mp_bsu_map` требует `rosstat_okved2`; на первом прогоне витрина появится после импорта классификатора. В приёмке запускать классификатор первым.
- Restore для `rosstat_okved2` с вычисляемым URL не поддерживается (как у computed-URL импортёров); классификатор восстанавливается повторным импортом.
- `GetFileBytes` не проверяет HTTP-статус: парсер обязан падать на пустом/HTML-ответе с именем URL.
- `docs/KEY_RATE_PUBLICATIONS.md` в корневом worktree имеет локальную правку — не трогать.
