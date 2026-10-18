# edisclosure-documents — дизайн

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** `dcf-polyus/phase-3-remaining-edisclosure-documents` — [ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md), «Остаток фазы 3».
**Issue:** [#182](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/182).
**План:** [2026-10-18-edisclosure-documents.md](../plans/2026-10-18-edisclosure-documents.md).
**Сопутствующие документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §2/§6.2/§6.3/§7/§8; [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md) §3–§13, §16; [CONFIGURATION.md](../../CONFIGURATION.md); прототип в `build/edisclosure-probe/` (gitignored, в поставку не входит).

## 1. Пробел

Контур DCF по Полюсу ([ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md)) сверяет допущения по отчётности, но в репозитории нет источника официальных раскрытий эмитента: финансовые отчёты, аффилированные лица и документы годовых собраний живут в `polyus/` и `financial/` как уже разобранные числа, без привязки к исходному документу и без истории версий. Если эмитент перевыпускает отчётность за тот же период или меняет документ под тем же `fileid`, это сейчас не видно.

[e-disclosure.ru](https://e-disclosure.ru/) — официальный агрегатор раскрытия акционерных обществ. Живой пилот 2026-10-18 (PLZL, SBER, CHMF; разделы IFRS/RAS/AFFILIATES/AGM) собрал **5 783 события за 2025–2026** и **290 документов за все годы страницы (2009–2026)**, а вручную отобранный архив — **20 версий, 54 701 575 байт**. Значимость для DCF: отчётность можно сверять с первоисточником, а винтажный архив позволяет восстановить, какой именно документ действовал на дату расчёта.

Первый срез — **только документы и винтажный архив**. События (`events`) и полный текст `event.aspx` требуют отдельного решения: страницы событий отдают ротационную капчу даже с сохранённым профилем.

## 2. Цель и объём

**Цель:** полуавтоматический сбор карточек эмитентов в `manifest.json` + `documents.jsonl` и детерминированный Go-импортёр, который скачивает документы, пишет строки в ClickHouse и сохраняет каждую версию файла в винтажном архиве.

**В объёме:**

- `scripts/edisclosure_collect.py` — Playwright + сохранённый Chrome-профиль: логин/решение капчи владельцем в открытом окне, сбор документов по компаниям и разделам (страница раздела отдаёт все годы), атомарная запись `manifest.json` + `documents.jsonl`;
- пакет `edisclosure/`: импортёр `edisclosure_documents`, свой `Import()` (набор документов, а не один файл), DDL, разбор/валидация входа, скачивание, батч-запись, архив версий;
- таблица `edisclosure_documents`, витрина `v_edisclosure_documents`, комментарии таблицы и колонок, `GRANT SELECT` пользователю `kimi_reader`;
- точка контроля — **по содержимому**: `sha256` скачанного файла плюс метаданные карточки; повторный прогон не делает сетевых вызовов;
- `dagu/edisclosure.yaml`, цель Makefile для коллектора, `scripts/mcp_check.py`;
- `ARCHITECTURE.md`, `README.md`, `docs/CONFIGURATION.md`, `sql/mcp_kimi_reader.sql`, статус роадмапа.

**Вне объёма:**

- события `events` и полный текст `event.aspx` — следующим пунктом роадмапа;
- автоматический обход капчи и любые платные подписки;
- исторический бэкфилл документов не нужен: страница раздела отдаёт все годы (в пилоте — 2009–2026); окно `--years` относится только к событиям следующего среза;
- извлечение текста PDF, LLM-разметка и числовые ряды — документы не являются рядами;
- изменение существующих таблиц и витрин.

**Компании первого среза** (карточки проверены пилотом):

| Ticker | company_id | Карточка |
|---|---|---|
| `PLZL` | 7832 | `https://e-disclosure.ru/portal/company.aspx?id=7832` |
| `SBER` | 3043 | `https://e-disclosure.ru/portal/company.aspx?id=3043` |
| `CHMF` | 30 | `https://e-disclosure.ru/portal/company.aspx?id=30` |

## 3. Источник и живая проба (2026-10-18)

| Что проверено | Результат |
|---|---|
| Карточка компании с сохранённым Chrome-профилем | HTML ≈161 КБ, маркеры «Существенные события»/«Дата события» присутствуют |
| Плоский HTTP с cookies из браузера | HTTP 200, но ≈415 КБ JS-challenge: страницы читаются только браузером |
| `event.aspx` с сохранённым профилем | ротационная капча: полный текст события недоступен |
| `FileLoad.ashx?Fileid=…` | скачивается **без браузера и cookies**; 287 zip + 3 rar на 290 документов |
| Пилотный парсер | 5 783 события, 290 документов (PLZL 211, CHMF 56, SBER 23; RAS 114, AFFILIATES 89, IFRS 84, AGM 3) |
| Архив пилота | 20 версий, 54 701 575 байт; `archive.Entry`/`archive.Key` совместимы |

Выводы для дизайна:

1. Список документов собирает браузерный коллектор; **скачивание файлов** делает Go-импортёр обычным `util.GetFileBytes` — это позволяет держать импортёр детерминированным и тестируемым офлайн.
2. `FileLoad.ashx` — динамический обработчик; версионных заголовков для HEAD нет, поэтому точка контроля строится не на HEAD-отпечатке, а на содержимом и метаданных карточки (§5.4).
3. Капча — ограничение сбора, а не импорта; MVP допускает ручное решение капчи владельцем.

## 4. Коллектор `scripts/edisclosure_collect.py`

### 4.1. Режимы и запуск

```
uv run --with playwright --with beautifulsoup4 --with lxml --with requests \
  --python 3.12 python3 scripts/edisclosure_collect.py login
uv run ... python3 scripts/edisclosure_collect.py collect
```

- `login` — открывает headed Chrome (`launch_persistent_context`, `channel="chrome"`) на карточке первой компании, ждёт появления контента; если показана капча, владелец решает её в окне. Профиль сохраняется в `build/edisclosure-profile/` (gitignored, каталог 0700).
- `collect` — тем же профилем обходит компании и разделы и пишет выход. Профиль и cookies — секреты: значения не печатаются, файлы состояния (если пишутся) — mode 0600.
- `--companies PLZL,SBER,CHMF` — подмножество списка; `--sections IFRS,RAS,AFFILIATES,AGM`; `--out` — каталог выхода, по умолчанию `build/edisclosure` (или `EDISCLOSURE_OUT`); `--wait` — сколько секунд ждать ручного решения капчи.

Капча во время `collect`: коллектор пишет в лог инструкцию, ждёт `--wait` секунд (по умолчанию 300) и завершается ошибкой, если контент так и не появился. Автоматического обхода капчи нет.

### 4.2. Выход

Коллектор пишет два файла; `manifest.json` — **последним**, он является маркером завершённого прогона:

- `documents.jsonl.tmp` → `documents.jsonl`;
- `manifest.json.tmp` → `manifest.json`.

Порядок записи защищает импортёр от частичного снапшота: расхождение `document_count` и числа строк — ошибка входа.

`manifest.json`:

```json
{
  "schema_version": 1,
  "generated_at": "2026-10-18T12:00:00+03:00",
  "collector": {"name": "edisclosure_collect.py", "version": "1"},
  "sections": ["IFRS", "RAS", "AFFILIATES", "AGM"],
  "companies": [
    {"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
     "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"}
  ],
  "document_count": 290,
  "documents_file": "documents.jsonl",
  "empty_sections": [{"ticker": "SBER", "sections": ["AFFILIATES", "AGM"]}]
}
```

`empty_sections` — разделы, страница которых редиректит на карточку компании (пустой раздел у этого эмитента); импортёр поле игнорирует, оно для аудита. Если у компании **все** запрошенные разделы пусты, коллектор завершается ошибкой и манифест не пишет: пустая компания — признак сломанного сбора, а не пустого источника.

`documents.jsonl` — одна строка = один документ (реальная строка пилота):

```json
{"ticker":"PLZL","company_id":"7832","section":"IFRS",
 "doc_type":"Промежуточная консолидированная финансовая отчетность по МСФО или иным международно признанным стандартам",
 "period":"2026, 6 месяцев","basis_date":"28.08.2026","publication_date":"31.08.2026",
 "size":"zip, 645.07 КБ","fileid":"1943701",
 "url":"https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701","table_num":"1"}
```

Поля строки:

| Поле | Обяз. | Правило |
|---|---|---|
| `ticker` | да | непустой, верхний регистр; обязан быть в `manifest.companies` |
| `company_id` | нет | если есть — совпадает с `manifest.companies[ticker].company_id` |
| `section` | да | `IFRS` \| `RAS` \| `AFFILIATES` \| `AGM` |
| `doc_type` | да | непустой текст карточки |
| `period` | нет | текст карточки как есть (`2025`, `2026, 6 месяцев`) |
| `basis_date` | нет | `DD.MM.YYYY` или пусто |
| `publication_date` | нет | `DD.MM.YYYY` или пусто |
| `size` | нет | человекочитаемая метка источника; **игнорируется** импортёром |
| `fileid` | да | только цифры |
| `url` | да | `https`, host `e-disclosure.ru`, path `/portal/FileLoad.ashx`, query `Fileid` = `fileid` |
| `table_num` | нет | номер строки таблицы источника (аудит) |

Дубликаты `(ticker, section, fileid)` схлопываются коллектором; если строки различаются содержимым — это ошибка коллектора.

### 4.3. Что коллектор не делает

- не скачивает тела документов (это делает Go-импортёр);
- не ходит в ClickHouse;
- не пишет `events.jsonl` в этом срезе;
- не обходит капчу и не хранит cookies вне профиля браузера.

## 5. Импортёр `edisclosure_documents`

### 5.1. Пакет

```
edisclosure/
  importers.go       — реестр и init()-регистрация в chimport.Stats
  documents.go       — Import(), гейт, батч, архив
  documents_parse.go — разбор manifest/documents.jsonl и валидация
  documents_archive.go — имя/ключ архива, определение расширения
  schema.go          — DDL и константы таблицы
  views.go           — v_edisclosure_documents и комментарии
  testdata/          — реальные строки пилота (manifest + 6 документов)
  *_test.go
```

Пакет регистрируется одной записью в `chimport.Stats`; `Name()` = `edisclosure_documents`. Пустой импорт `_` в `main.go` добавляется рядом с остальными пакетами.

### 5.2. DDL

```sql
CREATE TABLE IF NOT EXISTS edisclosure_documents (
    ticker LowCardinality(String),       -- биржевой тикер эмитента: PLZL, SBER, CHMF
    company_id String,                   -- id карточки e-disclosure (7832, 3043, 30)
    section LowCardinality(String),      -- раздел карточки: IFRS, RAS, AFFILIATES, AGM
    doc_type String,                     -- тип документа из карточки источника
    period String,                       -- отчётный период как в источнике: «2025», «2026, 6 месяцев»
    basis_date Nullable(Date),           -- дата составления/основания (DD.MM.YYYY), пусто → NULL
    publication_date Nullable(Date),     -- дата публикации документа, пусто → NULL
    fileid String,                       -- Fileid из FileLoad.ashx — идентичность документа
    url String,                          -- исходная ссылка на файл
    size_bytes UInt64,                   -- фактический размер скачанного файла
    sha256 String,                       -- sha256 содержимого — версия документа
    archive_key String,                  -- ключ объекта в винтажном архиве ("" — архив выключен)
    first_seen DateTime,                 -- когда конвейер впервые увидел (ticker, section, fileid)
    loaded_at DateTime DEFAULT now()     -- время записи строки: версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (ticker, section, fileid);
```

Ключ `(ticker, section, fileid)` — идентичность документа. Одна строка хранит **последнюю** версию; история версий живёт в реестре архива (`archive.Entry` на каждый `sha256`). Запросы к сырой таблице обязаны использовать `FINAL`; витрина делает это за агента.

### 5.3. Контракт импортёра

`Import(ctx, conn)` читает вход из `EDISCLOSURE_INPUT_DIR` (по умолчанию `build/edisclosure`) и работает по шагам:

1. **Чтение и валидация до сети и БД.** `manifest.json` (schema_version = 1, непустой `companies`, `document_count` = числу строк, `documents_file` = ровно `documents.jsonl`), затем `documents.jsonl`. Ошибка любой строки — fail-fast с номером строки; ни одного сетевого вызова и ни одной записи в БД.
2. **DDL** — `CREATE TABLE IF NOT EXISTS`.
3. **Витрина** — `util.CreateView` вызывается на каждом прогоне (в том числе на no-op): сбой публикации витрины не должен застревать за гейтом данных.
4. **Гейт.** Читаются существующие строки (`SELECT … FROM edisclosure_documents FINAL`). Документ скачивается, если:
   - строки нет; или
   - изменились метаданные карточки (`doc_type`, `period`, `publication_date`, `url`); или
   - архив включён, а `archive_key` пуст (самолечение); или
   - задан `EDISCLOSURE_RECHECK=1` (глубокая проверка содержимого).
   Иначе — пропуск без сети. Второй прогон того же снапшота: **0 скачиваний, 0 записей**, лог `no-op`.
5. **Скачивание и архив.** Для каждого документа: `util.GetFileBytes(url)` → проверка тела (непустое, не HTML) → `sha256` → при включённом архиве `store.Put` + `registry.Ensure` → строка батча. `first_seen` новой строки — `now()`, у существующей — сохраняется.
6. **Единственный `PrepareBatch` → `Append` → `Send`** на весь план. Ошибка скачивания/архива/`Append` — `Abort`, прогон падает; частичных строк в таблице нет.
7. Лог: `edisclosure_documents: документов в манифесте N, к загрузке M, пропущено K`.

`EDISCLOSURE_RECHECK=1` — ручной/периодический глубокий прогон: скачивает всё, но переписывает строку только при смене `sha256` или метаданных. Он же лечит «контент заменён под тем же `fileid` без изменения карточки» — случай, который обычный гейт по метаданным не видит (названный риск §10).

Отсутствие каталога входа — ошибка прогона, кроме `EDISCLOSURE_ALLOW_MISSING=1`: этот флаг ставит только шаг dagu, чтобы ежемесячное расписание не падало на хосте, куда коллектор ещё не доставил снапшот. Ручной `make import` флаг не ставит и падает громко.

### 5.4. Почему нет HEAD-префлайта и `source_publications`

Стандарт §9.1 требует точку контроля публикации для артефактов, обновляющихся реже расписания. Для набора документов HEAD-отпечаток не работает: `FileLoad.ashx` — динамический обработчик, версионных заголовков не отдаёт, а `Content-Length` без скачивания не отличает перевыпуск. Поэтому:

- **версия определяется `sha256` содержимого** и метаданными карточки, а не HEAD;
- `source_publications` не ведётся: её колонка `points` означала бы «число точек ряда», которых у документов нет. Роль журнала версий играет `archive.Registry` (`registry.jsonl`) — одна строка на каждый сохранённый `sha256`, с `source`, `name`, `sha256`, `size`, `url`, `key`, `first_seen`;
- гейт прогона — по таблице `edisclosure_documents` (прецедент `cbr_dip_bulletin`, спека §3.5), а не по реестру версий: пропавшая строка таблицы лечится полным импортом, а не молчаливым no-op.

Отступления названы в §8.

### 5.5. Архив версий

Архив включается переменными `ARCHIVE_BACKEND` / `ARCHIVE_LOCAL_DIR` / `ARCHIVE_YANDEX_*` (общий `archive.FromEnv(util.HttpClient)`):

- `source` = `edisclosure_documents` (== имя импортёра и таблицы);
- `name` = `<ticker>_<section>_<period>_<fileid><ext>`; `period` санитизируется (`2026, 6 месяцев` → `2026_6_месяцев`), `ext` — по сигнатуре тела: `PK\x03\x04` → `.zip`, `Rar!\x1a\x07` → `.rar`, `%PDF` → `.pdf`, иначе `.bin`;
- `key` = `archive.Key(source, name, fingerprint=sha256hex, sum)` → `edisclosure_documents/<sha16>_<name>`;
- `registry.Ensure` пишется **после** `store.Put`; `first_seen` версии — `now()`;
- архив выключен (`ARCHIVE_BACKEND` пуст) — строки пишутся с `archive_key=""`, в лог один warn; при включённом архиве сбой `Put`/`Ensure` — ошибка прогона (иначе строка объявила бы версию, которой в архиве нет).

Пилотный реестр `build/edisclosure-probe/archive/registry.jsonl` (source `edisclosure`) **не импортируется**: боевой прогон создаёт свои ключи под `edisclosure_documents`; это осознанная граница MVP, а не миграция.

### 5.6. Витрина

```sql
CREATE OR REPLACE VIEW v_edisclosure_documents
DEFINER = default SQL SECURITY DEFINER AS
SELECT ticker, company_id, section, doc_type, period, basis_date, publication_date,
       fileid, url, size_bytes, sha256, archive_key, first_seen, loaded_at
FROM edisclosure_documents FINAL
```

Комментарии — через `util.View` (таблица и каждая колонка): что означает версия (`sha256` + `archive_key`), как читать `period`/`basis_date`/`publication_date`, почему `first_seen` не равен дате публикации. Сырая `edisclosure_documents` агенту не выдаётся; `GRANT SELECT` — только на витрину.

`series_catalog` не заводится: документы не числовые ряды, у них нет `unit`/`frequency`. Это названное отступление от правила 11(а) AGENTS.md; смысл колонок компенсируется комментариями витрины.

### 5.7. Расписание

`dagu/edisclosure.yaml`:

```yaml
type: chain
schedule: "17 21 5 * *"
overlap_policy: skip

steps:
  - id: edisclosure_documents
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=edisclosure_documents
      - EDISCLOSURE_ALLOW_MISSING=1
    retry_policy:
      limit: 3
      interval_sec: 300
```

Коллектор в расписание не входит: он полуавтоматический (headed Chrome + ручная капча). Оператор доставляет `EDISCLOSURE_INPUT_DIR` на хост и запускает шаг вручную или ждёт ежемесячного прогона; без каталога шаг пропускается, а не падает.

### 5.8. Makefile

- `make edisclosure-collect` — `uv run --with … python3 scripts/edisclosure_collect.py collect` (выход `build/edisclosure`);
- `make edisclosure-login` — тот же скрипт в режиме `login`;
- импорт — существующий `make import STAT=edisclosure_documents`.

## 6. MCP и документация

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_edisclosure_documents TO kimi_reader;`.
- `scripts/mcp_check.py`: витрина читается и непуста; сырая таблица закрыта (`ACCESS_DENIED`); в `list_tables` у витрины есть комментарий, а у колонок — описания; выборочная проверка строки PLZL IFRS с непустыми `sha256`/`archive_key`.
- `ARCHITECTURE.md`: §6.2 — таблица и ключ; §6.3 — витрина; §7 — расписание; §8 п. 13 — архив/точка контроля (отступления).
- `README.md`: пакет, источник, запуск, переменные.
- `docs/CONFIGURATION.md`: `EDISCLOSURE_INPUT_DIR`, `EDISCLOSURE_RECHECK`, `EDISCLOSURE_ALLOW_MISSING`, `EDISCLOSURE_OUT`.
- Роадмап: статус пункта обновляется тем же PR после живой приёмки.

## 7. Тесты

Офлайн, без сети и ClickHouse:

1. `documents_parse_test.go` — реальные строки пилота в `testdata/` (6 документов, manifest с 3 компаниями): успешный разбор; `document_count` ≠ числу строк; неизвестный `schema_version`; пустой `companies`; `fileid` не цифры; host/path/`Fileid` в URL не совпадают; неизвестный `section`; `DD.MM.YYYY` и пустые даты; дубликат `(ticker, section, fileid)` с расхождением; `ticker` вне manifest.
2. `documents_test.go` — чистый гейт `planFetches`: нет строки → скачивать; метаданные совпали и `archive_key` есть → пропуск; изменился `publication_date` → скачивать; архив включён и `archive_key` пуст → скачивать; `RECHECK=1` → скачивать всё, но неизменившийся `sha256` не переписывает строку.
3. `documents_archive_test.go` — имя и ключ архива детерминированы; санитизация `period`; расширение по сигнатуре (zip/rar/pdf/bin); `archive.Key` даёт `<source>/<sha16>_<name>`.
4. `documents_test.go` — валидация тела: пустое и HTML → ошибка; бинарное → ок.
5. `importers_test.go` — `Name()` = `edisclosure_documents`, импортёр зарегистрирован в `chimport.Stats`, таблица в DDL совпадает с именем, шаг `dagu/edisclosure.yaml` указывает на зарегистрированное имя; DDL содержит `ReplacingMergeTree`, ключ `(ticker, section, fileid)`, `archive_key`/`sha256`.
6. `views_test.go` — витрина объявлена, `SELECT` содержит `FINAL`, комментарии покрывают все колонки.
7. `appendRecorder` (по образцу `cbr/series_meta_test.go`) — порядок и типы значений строки: даты приходят `time.Time`/`nil`, `size_bytes` — `uint64`, `first_seen` — `time.Time`.

Живая приёмка — §9.

## 8. Отступления от стандарта (названы явно)

| № | Отступление | Почему |
|---|---|---|
| О1 | Свой `Import()`, а не `util.ClickHouseImport` | Артефакт — набор из сотен документов, у шаблона один `DataUrl`; прецедент — `cbr_dip_bulletin` |
| О2 | Нет HEAD-префлайта и `source_publications` | `FileLoad.ashx` не даёт версионных заголовков; версия — `sha256` содержимого, журнал версий — `archive.Registry` (§5.4) |
| О3 | Архив пишется своим кодом через `archive.Registry`, ключ — с человекочитаемым именем | Шаблонный ключ берёт basename URL (`FileLoad.ashx`) и нечитаем; источник и имя документа нужны для аудита |
| О4 | Нет `series_catalog` | Документы не числовые ряды; комментарии витрины описывают колонки |
| О5 | Коллектор полуавтоматический, капча решается владельцем | Автоматический обход капчи вне объёма MVP |
| О6 | Таблица хранит последнюю версию `fileid`; история — в реестре архива | Потребителю нужен актуальный документ; винтажность обеспечивает архив |
| О7 | `EDISCLOSURE_ALLOW_MISSING=1` только в dagu | Расписание не должно падать на хосте без доставленного снапшота; ручной прогон падает громко |
| О8 | Пилотный реестр `edisclosure` не мигрируется | Чистый старт под `edisclosure_documents`; пилот в `build/` — не поставка |

## 9. Критерий приёмки

1. `make all` — gofmt, golangci-lint, `go vet ./...`, `go test -race ./...`, обе сборки; отдельно `go test -race -count=1 ./edisclosure/`.
2. `make edisclosure-collect` (или прямой запуск скрипта) на машине с Chrome: получен `manifest.json` + `documents.jsonl`, число строк = `document_count`, в логе нет секретов.
3. `make import STAT=edisclosure_documents` на dev-ClickHouse: команда, число строк, крайние даты `publication_date`, число документов по тикерам.
4. Повторный прогон: `Imported 0 rows`, ни одного сетевого вызова (лог no-op); `SELECT count() FROM edisclosure_documents FINAL` не изменился.
5. `EDISCLOSURE_RECHECK=1 make import STAT=edisclosure_documents` — идемпотентен: `sha256` не меняются, строки не переписываются.
6. Архив: `make archive-list ARCHIVE_LIST_SOURCE=edisclosure_documents` показывает версии; в `registry.jsonl` есть строки с `source=edisclosure_documents`, `sha256`, `key`; объекты читаются (`store.Get`/`make archive-list`).
7. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; витрина непуста, сырая таблица закрыта, комментарии видны.
8. `dagu validate dagu/edisclosure.yaml`.
9. Протокол живой приёмки — комментарием в PR; статус роадмапа обновлён тем же PR.

## 10. Риски и ограничения

- **Капча.** Сбор зависит от ручного решения; при недоступности источника коллектор падает, а импортёр продолжает работать со старым снапшотом (manifest не перезаписан). Это принято для MVP.
- **Хрупкость HTML.** Коллектор fail-fast: отсутствие таблицы документов или ссылки без `fileid` — ошибка, а не «документов нет».
- **HTML-заглушка вместо файла.** `util.GetFile` не проверяет HTTP-статус; импортёр отвергает пустое тело и тело, начинающееся с `<`.
- **Тихая замена контента под тем же `fileid`.** Обычный гейт по метаданным её не видит; лечится `EDISCLOSURE_RECHECK=1` (ручной/квартальный прогон) и историей архива.
- **Объём архива.** 290 документов пилота — десятки–сотни МБ; бэкенд `local`/`yandex` уже рассчитан на такой объём. Скачивание последовательное, без параллелизма — бережём источник.
- **`ReplacingMergeTree`.** Без `FINAL` видны несмёрженные версии; витрина использует `FINAL`, импортёр читает существующие строки с `FINAL`.
- **Тикер ≠ эмитент.** `company_id` фиксирован в списке; смена id на сайте — ошибка коллектора (карточка не открылась), а не молчаливый пропуск.
- **Право и этика.** Только публичные раскрытия; без обхода капчи, без платных подписок, без параллельных загрузок.

## 11. Что дальше

- `events` и полный текст `event.aspx` — отдельный пункт: 5 783 события пилота, ротационная капча, нужен другой подход к сбору (возможно, ручной экспорт/иной источник); `--years` понадобится там.
- Извлечение текста PDF и привязка к `polyus/`/`company_financials` — после стабилизации архива.
