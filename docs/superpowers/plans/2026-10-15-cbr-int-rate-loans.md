# План: `cbr_int_rate_loans` — ставки по кредитам юрлицам (пункт 4.2 №8, фаза 2)

**Дата:** 15 октября 2026 г.
**Спека:** [2026-10-15-cbr-int-rate-loans-design.md](../superpowers/specs/2026-10-15-cbr-int-rate-loans-design.md)
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №8.
**Ветка:** `key-rate-4-2-8-loan-rates`. **Исполнение:** `superpowers:subagent-driven-development`
(coder — `deepseek/deepseek-flash`, ревью задач — `haiku`, fix-round'ы ≤ 3 на том же coder).

---

## Критерий приёмки пункта

Зелёный `make all`; живой прогон `make import STAT=cbr_int_rate_loans` с записью факта
(команда, число строк, крайние даты); no-op повторного прогона; `make mcp-check` — ПРОВЕРКА
ПРОЙДЕНА; `dagu validate dagu/cbr.yaml` — exit 0; `ARCHITECTURE.md` / `README.md` / статус
роадмапа обновлены тем же PR.

## Задачи

### Task 1: импортёр, фикстура, тесты разбора

- `cbr/int_rate_loans.go`: `cbrIntRateLoans` = `util.ClickHouseImport` со статичным
  `DataUrl: https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx`, DDL из спеки
  §3.1, `ImportFunc` — разбор листа `ставки_руб` по спеке §3.4, имена рядов — константа
  из кода (16 штук, спека §3.2), `PublicationCountPoints: SELECT count() FROM cbr_int_rate_loans FINAL`,
  `AfterImport` — в Task 2/3 закроется (в этой задаче поставить заглушку на мета из Task 2,
  задачи мержатся в одну ветку — порядок диспатча последовательный, T1 сначала без AfterImport
  или сразу с финальной сигнатурой — диспатчер передаёт T1 и T2 вместе одному агенту,
  см. порядок ниже).
- Фикстура `cbr/testdata/loans_nonfin.xlsx` — живой файл целиком (скачать тем же
  `curl`/браузером, 124 КБ; стандарт §11.2).
- `cbr/int_rate_loans_test.go`:
  - двусторонний контракт каталога через `checkCbrSeriesMeta` (имена из кода);
  - разбор фикстуры: 16 × 152 = 2 432 точки; даты — предпоследний день месяца
    (правило даты, не литерал списка — по образцу `bank_int_rat_test.go`);
  - spot-значения: 2014-01 «Всего: до 30 дней, включая «до востребования»» = 7.35;
    2026-08 то же = 19.06; 2026-08 «СМП: свыше 1 года» = 14.22;
  - fail-fast: книга без листа — ошибка; пустой/короткий лист — ошибка с именем таблицы
    в тексте; при ошибке в рекордер записано 0 строк.

### Task 2: каталог рядов

- `cbr/series_meta.go`: `cbrIntRateLoansSeriesMeta` — 16 записей `util.SeriesMeta`
  (`Source: cbrSource`), через `namedSeriesMeta` с `seriesTemplate` (спека §3.6):
  unit «% годовых», F=M, origin «cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx,
  лист «ставки_руб», группа …, колонка срока «…»», description: средневзвешенная ставка по
  кредитам нефинансовым организациям в рублях за месяц (взвешена объёмом размещений;
  динамика определяется и ставками, и объёмами — из сноски источника), дата — предпоследний
  день месяца отчётного периода (сопоставлять по toStartOfMonth(date)), темп — разница
  в п.п., пустая ячейка источника — пропуск точки.
- Подключить `AfterImport: afterCbrMacro(cbrIntRateLoansSeriesMeta)` в импортёре T1.

### Task 3: витрина и реестр

- `cbr/views.go`: `cbrMacroView.Tables` += `cbr_int_rate_loans`; `UNION ALL SELECT 'cbr',
  toString(name), date, toFloat64(rate) FROM cbr_int_rate_loans FINAL` после строки
  `cbr_bank_int_rate`; Comment/Columns: «ставки по вкладам и кредитам», в описании `series`
  и `date` — добавить кредитные ставки и ту же привязку даты.
- `cbr/importers.go`: запись `{cbrIntRateLoansImporter, "cbr_int_rate_loans", "SELECT count() FROM cbr_int_rate_loans FINAL"}`
  после `cbr_bank_int_rate` (порядок = порядок `dagu/cbr.yaml`).
- `cbr/importers_test.go`: + `"cbr_int_rate_loans": "SELECT count() FROM cbr_int_rate_loans FINAL"`
  в `expectedPublicationCountPoints`; счётные формулировки комментариев «десять» → «одиннадцать».

### Task 4: расписание, снимок покрытия, MCP-проверки

- `dagu/cbr.yaml`: шаг `cbr_int_rate_loans` после `cbr_bank_int_rate`, `retry_policy` по
  образцу соседей.
- `sql/key_rate_coverage.sql`: `UNION ALL SELECT 'cbr_int_rate_loans', name, count(),
  min(date), max(date) FROM cbr_int_rate_loans FINAL GROUP BY name` — по образцу
  `cbr_bank_int_rate`.
- `scripts/mcp_check.py`: в блоке проверок контура ЦБ — (а) каталог содержит 16 рядов
  источника: `SELECT count() FROM v_series_catalog WHERE source='cbr' AND origin LIKE '%loans_nonfin.xlsx%'`;
  (б) сырая `cbr_int_rate_loans` закрыта для `kimi_reader` (отказ 497) — по образцу
  проверки `cbr_households_b_mes`.

### Task 5: документация

- `ARCHITECTURE.md`: шапка §2 (новая запись), §6.1 (пакет `cbr/` — 16 импортёров; с
  префлайтом 11), §6.2 (канонический DDL `cbr_int_rate_loans`), §6.3 (`v_cbr_macro` —
  добавить таблицу в список), §7 (контур ЦБ — 16 шагов), §8 п. 13 («Объявлено у 15…» → 16,
  список `cbr/` + `cbr_int_rate_loans`). Сверка — skill `sync-readme-architecture`.
- `README.md`: строка пакета `cbr` — добавить таблицу в список таблиц, «15 шагов» → 16,
  «десять импортёров с точкой контроля» → одиннадцать (в README формулировки свои — искать
  все места счётчиков по `grep -n "15 шагов\|десять" README.md`).
- Коммит документов — ПОСЛЕДНИМ перед приёмкой вместе с роадмапом (T6), либо в T5 без
  роадмапа; финальный порядок коммитов не критичен, важен один PR.

### Task 6: живая приёмка и статус роадмапа

Требуется dev-ClickHouse (если не отвечает — попросить владельца поднять `make ch-up`,
агент его не поднимает).

1. `make all` — зелёный.
2. `go test -race -count=1 ./cbr/` — зелёный.
3. `make import STAT=cbr_int_rate_loans` — ожидание: `Imported 2432 rows`, диапазон
   2014-01-30 … 2026-08-30; spot-сверка из ClickHouse с живым xlsx (3 значения из T1).
4. Повторный `make import STAT=cbr_int_rate_loans` — no-op («версия … уже импортирована»).
5. MCP: `SELECT count() FROM v_cbr_macro` (вырос на 2 432), `SELECT count() FROM
   v_series_catalog WHERE source='cbr' AND origin LIKE '%loans_nonfin.xlsx%'` = 16,
   сырой SELECT под `kimi_reader` — отказ.
6. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА. `dagu validate dagu/cbr.yaml` — exit 0.
7. `make ch-sql FILE=sql/key_rate_coverage.sql` — новая таблица в снимке с 16 рядами.
8. `docs/ROADMAP_KEY_RATE.md`: §4.2 №8 — «✅ фаза 2, этим PR (`cbr_int_rate_loans`: 16 рядов,
   2 432 точки, 2014-01 … 2026-08, приёмка 2026-10-15)»; абзац фазы 2 в §6 — отметить
   закрытие №8. Факты приёмки — команда/числа/даты — в коммит и этот план.

**Результат приёмки (2026-10-15, воспроизведено дословно)**

Ветка `key-rate-4-2-8-loan-rates`, dev-ClickHouse; запуск — через `make`, SQL-проверки — через MCP.

| Проверка | Команда | Результат |
|---|---|---|
| Доступность БД | `make ch-status` | `ClickHouse отвечает на порту 8123` |
| Импорт | `make import STAT=cbr_int_rate_loans` | `Imported 2432 rows of cbr_int_rate_loans` — скачан `https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx` |
| Снимок покрытия | `make ch-sql` | `rows_total 2432`, `dates 2014-01-30 .. 2026-08-30`, `series 16`; каждый из 16 рядов — 152 точки, 2014-01-30 … 2026-08-30 |
| Spot-сверка | spot-запрос к витрине | `Всего: до 30 дней, включая «до востребования»` — 2014-01-30 = 7.35, 2026-08-30 = 19.06; `СМП: свыше 1 года` — 2026-08-30 = 14.22 |
| Каталог | `SELECT count() FROM series_catalog FINAL WHERE source='cbr' AND origin LIKE '%loans_nonfin.xlsx%'` | `catalog_rows 16` |
| Витрина | `SELECT count(), uniqExact(series) FROM v_cbr_macro FINAL` по новым рядам | `view_rows 2432`, `view_series 16`; всего строк витрины — 48 942 |
| Строка версии | `SELECT … FROM source_publications` | `cbr_int_rate_loans` / `loans_nonfin.xlsx` / `points 2432` — дословная строка ниже |
| Идемпотентность | повтор `make import STAT=cbr_int_rate_loans` | `cbr_int_rate_loans: no-op, версия 32dafeda77a8bcddd8b9feb846a40840 уже импортирована, 2432 точек на месте` |
| MCP end-to-end | `make mcp-check` | `ПРОВЕРКА ПРОЙДЕНА`, в т.ч. `OK каталог cbr_int_rate_loans полон: рядов cbr_int_rate_loans: 16 (8 сроков × 2 группы: Всего/СМП)` и `OK сырая cbr_int_rate_loans закрыта: отказ ClickHouse 497 на cbr_int_rate_loans (ожидаемо)` |
| Расписание | `dagu validate dagu/cbr.yaml` | exit 0 |
| Реестр архива | `make archive-registry` | `реестр архива: 2 строк -> docs/archive/registry.jsonl`; строка архива: `cbr_int_rate_loans/32dafeda77a8bcdd_loans_nonfin.xlsx`, sha256 `87492617bf86c30fb280ffee15c3d2eab3bafa8fd0e248b9f659ef37bbd2bc3e`, 124 288 байт, `first_seen 2026-10-15T12:53:12.43343+05:00` |

Строка версии в `source_publications` (дословно, разделители — вертикальные черты):

```text
cbr_int_rate_loans | loans_nonfin.xlsx | points 2432 | first_seen 2026-10-15 12:53:23 | url …/int_rat/loans_nonfin.xlsx
```

Независимая перепроверка ревьюером через MCP под `kimi_reader`: `v_cbr_macro` — 48 942,
каталог — 16, ряды ставок — 2 432; совпало. Те же факты — в теле коммита `key rate roadmap:
close 4.2 #8 loan rates`.

### Task 7: финальное ревью и PR

- Финальное ревью ветки (`superpowers:requesting-code-review`) на самой способной доступной
  модели с effort `max`. **Примечание владельцу:** в сессии effort `high`
  (`~/.kimi-code/config.toml`, `[thinking].effort`); если финальное ревью нужно строго с
  `max` — скажите, подниму, молча не подменяю.
- Один PR: источник, схема таблицы, запуск (`CLICKHOUSE_IMPORT_STAT=cbr_int_rate_loans`),
  пример проверки (`SELECT … FROM v_cbr_macro WHERE series LIKE 'Всего:%' ORDER BY date DESC LIMIT 5`).

## Порядок диспатча

T1+T2 одному coder-субагенту (импортёр не компилируется без мета и наоборот), затем ревью
(haiku), затем T3, ревью, T4, ревью, T5, ревью. После зелёного `make all` — T6 (живая
приёмка контроллером через `make`/MCP), T7. Fix-round'ы: до 3 на задачу, далее эскалация
контроллеру.

## Чек-лист жёстких правил для ревью-промптов

Батч одним `PrepareBatch`; HTTP только `util.GetXlsx`; сети в `init()` нет; DDL
`CREATE TABLE IF NOT EXISTS`/`ReplacingMergeTree`/`LowCardinality(String)`/Float64; все
`strconv`/`batch.Append` с проверкой ошибок; logrus, не `fmt.Print*`; секретов нет; новый код
не в `financial/`; имена рядов из кода; фикстура — реальный файл, не синтетика.
