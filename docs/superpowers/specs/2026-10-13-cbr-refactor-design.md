# cbr/ — рефакторинг к единому стандарту (design)

Дата: 2026-10-13. Пункт: `docs/ROADMAP_KEY_RATE.md`, **фаза 3 — «Рефакторинг `cbr/`»**, расширенный правкой владельца: **все источники с `www.cbr.ru` живут в одной папке `cbr/`, у каждого — таблица с префиксом `cbr_`**.

## 1. Задача

Пакет `cbr/` — самый старый контур импорта: 16 нетестовых файлов, 14 зарегистрированных импортёров, из которых два — файлы-дубли, ломающие реестр `Name()`. Технический долг, зафиксированный роадмапом и подтверждённый живой инвентаризацией 2026-10-13:

- **построчный `conn.Exec` в цикле** (нарушение правила 2 AGENTS.md) — 9 файлов: `currency_usd.go:68`, `avgproc_stav.go:58`, `key_rate.go:63`, `metal_gold.go:71`, `households.go:106`, `m2.go:87`, `new_loans_corp.go:102`, `new_loans_ind.go:103`, `queries.go:111`;
- **`colly` напрямую** (нарушение правила 4) — 7 файлов: 4 скрапят HTML-таблицы данных (`currency_usd`, `avgproc_stav`, `key_rate`, `metal_gold`), 3 ищут ссылку на XLSX (`Infl_exp`, `indicators_cpd`, `credit_m2x`);
- **`fmt.Print*` в прод-коде** — 8 активных мест: `ruonia.go:23`, `currency_usd.go:46`, `key_rate.go:41`, `metal_gold.go:36`, `avgproc_stav.go:36`, `m2.go:64`, `new_loans_ind.go:79`, `queries.go:65,85`;
- **файлы-дубли**: `avgproc_stav.go` (тот же `Name()` = `cbr_currency_usd`, тот же URL, та же таблица — `make import STAT=cbr_currency_usd` запускает два импортёра) и `queries.go` (`Name()` = `households_b_mes`, пишет в чужую таблицу, мёртвый DDL `cbr_queries_*` и нерабочий `DatasetId: 0`);
- **`purcb.go`** — 4-строчная заглушка без импортёра;
- **вне папки**: `surveys/` (`cbr_mp_survey` — источник cbr.ru) и мёртвый legacy `financial/cbr.go` (тот же KeyRate-URL, таблица `cbr_rates`, `database/sql`; вызывается только из никогда не вызываемой `loadAllData` — проверено grep'ом);
- **таблица без префикса**: `households_b_mes` (остальные 12 таблиц пакета уже с `cbr_`);
- **нет «живого» теста полноты каталога** — есть статичный снимок имён `householdsBMesDBNames` (`series_meta_test.go`), у эталонных пакетов (`surveys`, `minfin`) тест гоняет парсер на фикстуре и сверяет каталог в обе стороны.

## 2. Цель и объём

1. **Транспорт — только через `util`**: `colly` уходит из `cbr/` целиком; XML ЦБ — через `util.GetXML`; HTML-страницы — `util.GetFile` + `goquery` (уже прямая зависимость `go.mod`); файлы — `util.GetXlsx`.
2. **Единый шаблон импортёра** (шаблон Б `ARCHITECTURE.md` §3) для всех 13 импортёров пакета: `util.ClickHouseImport` (XLSX — `ImportFunc`, XML/HTML — `CrawFunc`), динамический `DataUrl` — в override `Import()`; каталог + витрина — через `publishedStat`; один батч; logrus.
3. **Реестр и тест-энфорсер**: единый список импортёров пакета + тест «`Name()` с префиксом `cbr_`, таблица — `cbr_*` либо общий allowlist (`macro_series`)».
4. **Удаления**: `avgproc_stav.go`, `queries.go`, `purcb.go` (URL из заглушки сохраняются здесь, §9), `financial/cbr.go` (+ вызов + DDL-константа).
5. **`surveys/` → `cbr/`**: `cbr_mp_survey` переезжает в пакет `cbr` (имя, таблица, витрина, гранты — без изменений); `dagu/surveys.yaml` сворачивается в `dagu/cbr.yaml`.
6. **`households_b_mes` → `cbr_households_b_mes`** со всеми ссылками и миграцией.
7. **Тесты**: реальные урезанные фикстуры `cbr/testdata/` + двусторонние тесты каталога на каждый импортёр; статичный снимок имён удаляется.
8. **`util.GetXML`**: `CharsetReader` под `windows-1251` (XML ЦБ) + тест.
9. Документация: `ARCHITECTURE.md`, `README.md`, `ROADMAP_KEY_RATE.md` (фаза 3 ✅ с записью приёмки), `KEY_RATE_CHANNELS.md` §8, `sql/key_rate_coverage.sql`.

**Критерий приёмки пункта** (роадмап критерия не даёт — фиксируем здесь): `grep`-чистота (нет `colly`/`gocolly`, нет `fmt.Print` в `cbr/`), нет дублей `Name()`, все таблицы/ряды с префиксом `cbr_`, живые прогоны всех импортёров идемпотентны и **данные не изменились** относительно базовой линии, `make all`/`make mcp-check`/`dagu validate` зелёные.

**Вне объёма:** пакет `financial/` (legacy, кроме удаления `cbr.go`); RSS-ленты ЦБ в `news/` (мульти-источниковая лента из 5 фидов — явное исключение из правила «источников cbr.ru в `cbr/`»); перенос cbr-рядов в `macro_series`; изменение имён рядов, семантики дат, состава серий; `dagu/cbr.yaml` расписание остаётся `17 9 * * *`.

## 3. Инвентарь импортёров (живая проба 2026-10-13)

| Файл | `Name()` | Таблица | Транспорт сейчас | Запись сейчас | Целевой транспорт |
|---|---|---|---|---|---|
| `key_rate.go` | `cbr_key_rate` | `cbr_key_rate` | colly, HTML-таблица `hd_base/KeyRate` | построчно | `util.GetFile` + goquery (XML/Excel у ЦБ нет — проверено) |
| `currency_usd.go` | `cbr_currency_usd` | `cbr_currency_usd` | colly, HTML `currency_base/dynamics` | построчно | `util.GetXML` + `XML_dynamic.asp?VAL_NM_RQ=R01235` (сверено: 85,4811 = БД) |
| `metal_gold.go` | `cbr_gold` | `cbr_gold` | colly, HTML `metall_base_new` | построчно | `util.GetXML` + `xml_metall.asp` Code=1 (сверено: 11422,95 = БД) |
| `households.go` | `households_b_mes` | `households_b_mes` | `util.GetXlsx` | построчно | без изменений; таблица → `cbr_households_b_mes` |
| `m2.go` | `cbr_m2` | `cbr_m2` | `util.GetXlsx` | построчно | без изменений |
| `new_loans_corp.go` | `cbr_loans_to_corporations` | `cbr_loans_to_corporations` | `util.GetXlsx` | построчно | без изменений |
| `new_loans_ind.go` | `cbr_loans_to_individuals` | `cbr_loans_to_individuals` | `util.GetXlsx` | построчно | без изменений |
| `Infl_exp.go` | `cbr_infl_exp` | `cbr_infl_exp` | colly (поиск ссылки) + GetXlsx | **батч (CHI)** | `util.GetFile` + goquery (ссылка) + GetXlsx |
| `indicators_cpd.go` | `cbr_indicators_cpd` | `cbr_indicators_cpd` | colly (ссылка) + GetXlsx | **батч** | то же |
| `credit_m2x.go` | `cbr_credit_m2x` | `cbr_credit_m2x` | colly (ссылка) + GetXlsx | **батч** | то же |
| `bank_int_rat.go` | `cbr_bank_int_rate` | `cbr_bank_int_rate` | CHI + GetXlsx | **батч** | без изменений (+ тесты) |
| `ruonia.go` | `cbr_ruania` | `cbr_ruania` | CHI + GetXlsx (DownloadExcel/14315) | **батч** | без изменений (+ удалить `fmt.Printf:23`, тесты) |
| `avgproc_stav.go` | `cbr_currency_usd` ⚠ дубль | `cbr_currency_usd` | colly | построчно | **удаляется** |
| `queries.go` | `households_b_mes` ⚠ дубль | `households_b_mes` (мёртвый `cbr_queries_*`) | GetXlsx (`DatasetId: 0`) | построчно | **удаляется** |
| `purcb.go` | — | — | — | — | **удаляется** (заглушка) |
| `financial/cbr.go` | — (не зарегистрирован) | `cbr_rates` | colly | построчно, `database/sql` | **удаляется** (мёртвый legacy) |

Коллизии `Name()`: `cbr_currency_usd` (currency_usd + avgproc_stav) и `households_b_mes` (households + queries) — обе устраняются удалениями. Других коллизий в репозитории нет.

## 4. Транспорты (решения с живой проверкой)

- **Курс USD** — `https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=DD/MM/YYYY&date_req2=DD/MM/YYYY&VAL_NM_RQ=R01235`; запись `<Record Date="08.10.2026"><Value>85,4811</Value></Record>`; сравнение с `cbr_currency_usd` (Float32 85.4811019897461) — совпадает. Глубина с 2013-09-17 сохраняется (параметр `date_req1`), поведение — полный upsert истории как сейчас.
- **Золото** — `https://www.cbr.ru/scripts/xml_metall.asp?date_req1=…&date_req2=…`, `Code="1"` = золото, `<Buy>11422,95</Buy>` = учётная цена ₽/грамм; совпадает с `cbr_gold`. Дублирующие `Buy`/`Sell` равны — берём `Buy` (как источник и сегодня).
- **KeyRate** — XML/Excel-выгрузки у страницы `hd_base/KeyRate` нет (проверено: ни `DownloadExcel`, ни `xlsx`, ни Excel-кнопки в HTML); остаётся HTML-таблица, но **транспорт** — `util.GetFile` (нац. сертификаты РФ, ротация UA, cookie-jar), **разбор** — `goquery` по сохранённым селекторам (`.table-wrapper .table .data tr`, колонки 1–2, дата `02.01.2006`, запятая → точка).
- **Страницы-поисковики ссылок** (`Infl_exp`, `indicators_cpd`, `credit_m2x`) — `util.GetFile` + `goquery`; селекторы сохраняются как есть (`a.versions_item[href$=".xlsx"]` — первая ссылка; `a.referenceable` для aipd; `a.referenceable[href$="credit_m2x.xlsx"]`); ошибка «ссылка не найдена» — fail-fast (как сейчас у Infl_exp).
- **Явная деталь**: `util.GetFile` не проверяет HTTP-статус (WAF-заглушка придёт как 200) — поэтому каждый разбор обязан иметь fail-fast (пустой результат → ошибка), а для XML статус проверяет сам `util.GetXML`.
- **Зависимости не меняются**: `goquery` — уже прямая зависимость (`go.mod`), `golang.org/x/text` — тоже (для charset в §10); `colly` остаётся в `go.mod` (его используют legacy-файлы `financial/`, вне объёма), но из `cbr/` исчезает полностью.

## 5. Стандарт импортёра (целевой вид)

```go
type cbrXxx struct{ util.ClickHouseImport } // либо обёртка над publishedStat

// XLSX-источники: ImportFunc(xlsx, batch) — разбор → batch.Append(err-проверка)…
// XML/HTML-источники: CrawFunc(url, conn) — fetch → parse → один PrepareBatch→Append(err)→Send.
// Динамический URL (дата/поиск ссылки): override Import() ставит s.DataUrl и делегирует.
// CreateTable — DDL в %s-форме (обязательно: util.ClickHouseImport делает Sprintf;
//   литерал без %s даёт %!(EXTRA…) — дефект, пойманный в пункте 1.3).
// Регистрация — через единый реестр §6; каталог/витрина — publishedStat.
```

Разбор отделяется от сети: функции разбора принимают `[]byte`/`*excelize.File` и не ходят в интернет — это условие офлайн-тестов на фикстурах (минимальные сигнатуры: `parseCurrencyUsd(data []byte) ([]cbrPoint, error)`, `keyRateRows(html []byte)`, `findXlsxLink(page []byte, …)`).

**Логирование**: logrus; `fmt.Print*` в пакете не остаётся ни одного. **Данные не меняются**: имена рядов, единицы, семантика дат, полный upsert истории — как есть.

## 6. Реестр импортёров и тест-энфорсер префиксов

Новый файл `cbr/importers.go`:

```go
type cbrImporter struct {
    chimport.ImportStat           // регистрируемый импортёр (CHI или обёрнутый publishedStat)
    table            string       // таблица, в которую пишет ("" для общей)
}
var cbrImporters = []cbrImporter{
    {…, "cbr_key_rate"}, …, {mpSurveyImporter, "macro_series"}, // allowlist общей таблицы
}
func init() { for _, i := range cbrImporters { chimport.Stats = append(chimport.Stats, i.ImportStat) } }
```

Тест `TestCbrImportersPrefixedAndRegistered` (`cbr/importers_test.go`): (а) каждый `Name()` с префиксом `cbr_`; (б) `table` пуст/`macro_series` — только для `cbr_mp_survey`, иначе — префикс `cbr_`; (в) имена уникальны и зарегистрированы в `chimport.Stats`; (г) каждый `Name()` из `dagu/cbr.yaml` есть в реестре (обратная сверка расписания). `init()` остаётся единственной точкой регистрации.

## 7. Переименование `households_b_mes` → `cbr_households_b_mes` (таблица, `Name()`, шаг DAG)

Переименовывается всё трио, чтобы выполнялось правило §6: **таблица** `households_b_mes` → `cbr_households_b_mes` (единственная из 13 таблиц пакета без префикса), **`Name()`** импортёра → `cbr_households_b_mes` (единственное имя-исключение в реестре), **id шага** в `dagu/cbr.yaml` → `cbr_households_b_mes` (id шага = `Name()`); ручной запуск — `make import STAT=cbr_households_b_mes`.

Ссылки (grep по репо, включая dagu/scripts/dashboards): `cbr/households.go`, `cbr/series_meta.go`, `cbr/series_meta_test.go`, `cbr/views.go` (SELECT витрины `v_cbr_macro`), `dagu/cbr.yaml`, `sql/key_rate_coverage.sql:37`, `ARCHITECTURE.md:654`, `README.md:13/485`, `docs/KEY_RATE_CHANNELS.md:139/144`, `docs/ROADMAP_KEY_RATE.md:56/102` (таблица покрытия — обновляется), `financial/cbr.go` (удаляется).

Миграция (единственный разрушающий шаг, только после сверки): прогон обновлённого `households_b_mes`-импортёра заливает полную историю в новую таблицу → сверка `count()/min/max` и выборочных значений новой таблицы с `v_cbr_macro`-базовой линией и старой таблицей → `DROP TABLE households_b_mes` (данные полностью воспроизводимы из источника; при любом расхождении — стоп и разбор). DDL-копий в `ARCHITECTURE.md` §6.2 у этой таблицы нет (legacy) — обновляется только код и упоминания.

## 8. Переезд `surveys/` → `cbr/`

| Сейчас (`surveys/`) | Станет (`cbr/`) |
|---|---|
| `mp_survey.go` | `mp_survey.go` (package `cbr`; `importer` → `mpSurveyImporter`) |
| `parse.go` | `mp_survey_parse.go` (имена функций не меняются) |
| `series_meta.go` (13 записей) | вливается в `cbr/series_meta.go` (имена переменных сохраняются) |
| `views.go` (`v_cbr_survey`) | вливается в `cbr/views.go` |
| `mp_survey_test.go`, `parse_test.go` | `cbr/mp_survey_test.go`, `cbr/mp_survey_parse_test.go` |
| `testdata/mp_survey_data.xlsx` | `cbr/testdata/` |

`Name()` = `cbr_mp_survey`, таблица `macro_series`, `source='cbr_survey'`, витрина `v_cbr_survey`, грант — **без изменений**. `dagu/surveys.yaml` удаляется, шаг `cbr_mp_survey` добавляется последним в `dagu/cbr.yaml` (цепочка 09:17; для файла, обновляемого раз в месяц, смена времени несущественна). `main.go` теряет blank-import `surveys`. Документация: `ARCHITECTURE.md` (строка 4 — чейнджлог, §2 строки 57–62, §6.1:240, §6.5:802, §7:896), `README.md` (20, 115, 143, 247), `ROADMAP_KEY_RATE.md` (блок приёмки 1.3 — исторический, не переписывается; фаза 3 фиксирует переезд).

## 9. Удаления

- `avgproc_stav.go`, `queries.go` — дубли (см. §1); вместе с `queries.go` из README уходят упоминания `cbr_queries_*`.
- `purcb.go` — заглушка; сохраняемые URL: `https://www.cbr.ru/statistics/rcb/purcb_stat/#a_162226file` (реестр ПУРЦБ — «получение, утилизация, размещение» отходов) и `https://www.cbr.ru/press/event/?id=28012`. При возврате к теме — новый пункт роадмапа и новый импортёр по стандарту §5.
- `financial/cbr.go` + вызов `crawCbr` в `financial/main.go` + DDL-константа `createCbrRates` в `financial/db.go` (проверить grep'ом, что у константы не осталось живых исполнителей). Остальной legacy `financial/` не трогается.

## 10. `util.GetXML`: `windows-1251`

XML ЦБ объявлен `encoding="windows-1251"` — сейчас `util.GetXML` падает (`Decoder.CharsetReader is nil`, проверено живьём). Добавить `decoder.CharsetReader`: `windows-1251`/`cp1251` — `golang.org/x/text/encoding/charmap.Windows1251` (уже прямая зависимость `go.mod`), `utf-8`/`utf8` — pass-through, прочее — ошибка. Тест в `util/xml_test.go` на windows-1251-фикстуре (короткий XML-сэмпл в байтах). Обновить §3 `ARCHITECTURE.md` (описание `GetXML`).

## 11. Тесты и фикстуры

Фикстуры — реальные артефакты источников в `cbr/testdata/` (урезанные как `surveys/testdata`: zip-хирургия листов XLSX, контейнер страницы без изменения состава детей — чтобы селекторы остались валидны):

- XML: `currency_usd.xml` (XML_dynamic), `metal_gold.xml` (xml_metall);
- HTML: `key_rate.html`, `infl_exp_page.html`, `aipd_page.html`, `credit_m2x_page.html`;
- XLSX (по листу, который читает парсер): `households_bm.xlsx`, `monetary_agg_SA.xlsx`, `new_loans_corp.xlsx`, `new_loans_ind.xlsx`, `regulatory_rates.xlsx`, `ruonia.xlsx`, `infl_exp.xlsx`, `aipd.xlsx`, `credit_m2x.xlsx`.

Тесты (офлайн, сеть не участвует):
1. **Двусторонний каталог** на каждый импортёр (идиом `minfin/series_meta_test.go`: фейковый `driver.Batch` — `appendRecorder`): «каждое имя из разбора описано в `series_meta`» и «каждое описанное имя встретилось при разборе». Прогоняется реальный разбор на фикстуре.
2. **Переименование статичного снимка**: `TestHouseholdsNamesCovered` + `householdsBMesDBNames` удаляются (заменены фикстурным двусторонним тестом).
3. **Fail-fast** для XML (`пустой документ`, `0 записей`) и HTML (`страница без таблицы/ссылки`) — точечные тесты.
4. `TestCbrImportersPrefixedAndRegistered` (§6).
5. `util/xml_test.go` — charset (§10).
6. Существующий `TestSeriesMetaWellFormed` сохраняется.

## 12. Приёмка

**Базовая линия (до кода):** снимок per-series через `v_cbr_macro` (MCP `kimi_reader`): `count()`, `min(date)`, `max(date)` по каждому ряду + спот-значения (например, `cbr_currency_usd` 2026-10-08 = 85,4811; `cbr_gold` 2026-10-08 = 11 422,95; последняя RUONIA; счётчики `cbr_loans_*`, `households_b_mes`). Числа фиксируются в отчёте приёмки.

**После рефакторинга:**

| Проверка | Ожидание |
|---|---|
| `make all` | зелёный (gofmt, `0 issues.`, vet, тесты, сборки) |
| `grep -rn "colly\|gocolly" cbr/` | пусто |
| `grep -rnE "fmt\.Print" cbr/` | пусто |
| `make import STAT=<каждый из 13>` | строка `Imported N rows of <name>`; повтор — идемпотентен |
| Даные vs базовая линия | per-series `count/min/max` и спот-значения совпадают (обновление может добавить свежие дни — тогда «не меньше» + прежние значения на месте) |
| `make import STAT=cbr_currency_usd` | запускает **ровно один** импортёр (в логе один `Imported`) |
| `make import STAT=households_b_mes` | не существует; `cbr_households_b_mes` — один импортёр |
| `households_b_mes` миграция | `count/min/max` новой таблицы = старой → `DROP TABLE households_b_mes` → повторный `SELECT` в `key_rate_coverage.sql` читается |
| `make mcp-check` | ПРОВЕРКА ПРОЙДЕНА (в т.ч. `v_cbr_macro` и `v_cbr_survey` читаются, `SELECT count() FROM macro_series`/raw-таблиц — ACCESS_DENIED) |
| `dagu validate dagu/cbr.yaml` | exit 0; `dagu/surveys.yaml` отсутствует |
| Тесты | все новые тесты §11 зелёные |

**Документы тем же PR**: `ARCHITECTURE.md` (чейнджлог, §2, §3, §6.1, §6.3, §6.5, §7), `README.md`, `ROADMAP_KEY_RATE.md` (фаза 3 → ✅ + блок «Живая приёмка … фаза 3» с командами и числами), `KEY_RATE_CHANNELS.md` §8, `KEY_RATE_PUBLICATIONS.md` §6 (если упоминает), `sql/key_rate_coverage.sql`.

## 13. Риски

| Риск | Митигация |
|---|---|
| XML-сервис ЦБ и HTML-страница дают расхождения с прежним скрейпом | Живые сверки до реализации выполнены (курс/золото совпали до знака); в приёмке — сверка с базовой линией по всем 13 рядам |
| Хрупкий позиционный селектор `indicators_cpd` | Сохраняется как есть; фикстурный тест пинит его поведение; если сломается на источнике — отдельный фикс |
| Переименование `households_b_mes` затронуло потребителей | Полный grep-чек-лист §7; DROP только после сверки; данные воспроизводимы из источника |
| Переезд `surveys/` ломает ссылки в доках/скриптах | Чек-лист упоминаний §8; grep `surveys` по репо в приёмке (кроме docs/superpowers и исторических записей) |
| Размер фикстур | Урезанные копии (zip-хирургия листов, обрезка страниц до нужного контейнера); бюджеты: XML/HTML ≤ ~500 КБ каждая, XLSX — только нужный лист |
| Потеря данных при DROP старой таблицы | DROP — единственный разрушающий шаг, после сверки; при расхождении — стоп |

## 14. Не является инвестиционной рекомендацией

Документ носит технический характер и не является инвестиционной рекомендацией.
