# Task 4 — Окружение: расписание, грант, канонический DDL (отчёт)

Дата: 2026-10-13. Ветка `key-rate-forecast`, база — HEAD `5f5b95c`.
Правки: `dagu/moex.yaml`, `sql/mcp_kimi_reader.sql`, `ARCHITECTURE.md`, `README.md`.
Go-код, тесты и зависимости не тронуты; `moex/` не изменялся. `deploy/staging/dagu.service`
(посторонняя модификация рабочего дерева) не тронут и не застейджен.

## 1. `dagu/moex.yaml` — третий шаг

Добавлен после `ofz_curve` (файл теперь: `stock_prices` → `ofz_curve` → `keyrate_futures`),
ровно по образцу соседей — тот же `run`, тот же `env`, та же политика retry:

```yaml
  - id: keyrate_futures
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=keyrate_futures
    retry_policy:
      limit: 3
      interval_sec: 300
```

`dagu validate dagu/moex.yaml` → **exit 0** (OK). Полный вывод:

```
time=2026-10-13T13:10:12.940+05:00 level=WARN msg="No auth.mode configured — defaulting to 'builtin'. Set auth.mode to 'none' to disable authentication, or complete /setup to create an admin account."
exit=0
```

Предупреждение `auth.mode` — общеe для dagu в этом окружении, не связано с правкой: на
нетронутом `dagu/gold.yaml` вывод байт-в-байт такой же, exit 0 (проверено).

## 2. `sql/mcp_kimi_reader.sql` — грант

Добавлена одна строка (после грантов СБКТС):

```sql
GRANT SELECT ON default.v_keyrate_expectations TO kimi_reader;
```

Сырые `keyrate_futures` и `keyrate_contract` **не выданы** (в файле их нет — проверено
`grep`), как и требует правило 11(в) AGENTS.md.

## 3. `ARCHITECTURE.md` — канонический DDL, §6.1, §6.3, §7 (+ §2)

Источник текста — **код** (`moex/keyrate.go`), а не план: в плане (Task 3 Step 3, стр. 211–218)
и в спеке остались отвергнутые формы (`FROM keyrate_futures FINAL f`, `CROSS JOIN
(SELECT argMax(rate, date) …) k`). Проверено на живом сервере (read-only `DESCRIBE`,
версия 26.10.1.1853):

- принятая форма (`FROM keyrate_futures AS f FINAL` + скалярный подзапрос с `nullIf`) →
  `code: 60 … UNKNOWN_TABLE` (таблиц ещё нет, синтаксис принят);
- отвергнутая форма (`FROM keyrate_futures FINAL f`) → `code: 62 … (SYNTAX_ERROR)`,
  позиция `line 2, col 28`.

### §6.2 — канонический DDL двух таблиц (добавлено после `ofz_curve`)

Скопировано **побайтово** из `keyrateDdl` / `keyrateContractDdl`:

```sql
CREATE TABLE IF NOT EXISTS keyrate_futures (
      secid LowCardinality(String)
    , date Date
    , close Float64
    , settle Float64
    , waprice Float64
    , volume UInt64
    , numtrades UInt64
    , openposition UInt64
) ENGINE = ReplacingMergeTree ORDER BY (secid, date);
```

```sql
CREATE TABLE IF NOT EXISTS keyrate_contract (
      secid LowCardinality(String)
    , series_name String
    , first_trade Date
    , last_trade Date
    , updated_at DateTime
) ENGINE = ReplacingMergeTree ORDER BY (secid);
```

Перед ними — комментарий в стиле соседей по блоку (`bold_fund_snapshot`, `gost_…`): что за
инструмент, что цена дня — `SETTLEPRICE` (п.п. ставки), что `LAST` не заводится и не
используется, почему `MINSTEP`/`STEPPRICE` в схему **не** заводятся (другой блок ISS;
множитель 0,01 п.п. = 10 ₽ → 1 п.п. = 1 000 ₽ живёт в комментариях витрины и в каталоге),
что ключ `(secid, date)` даёт идемпотентность, что `last_trade` = `LSTDELDATE` (с откатом на
`LSTTRADE`) — дата экспирации = дата заседания СД ЦБ. Добавлено также упоминание обеих таблиц
в вводном абзаце §6.2 («Реализованы в коде … DDL — копия блока ниже в `moex/keyrate.go`,
копии обязаны совпадать»).

### §6.3 — строка витрины `v_keyrate_expectations` + каноническое определение

Строка-буллет (в стиле соседей: назначение, все 10 колонок, семантика `nullIf`, ⚠️-оговорки
про алиас-до-`FINAL`, про низкую ликвидность и «это НЕ прогноз и НЕ ценовое обязательство»,
про NULL = «ставка ещё не загружена», гейт по трём таблицам, где комментарии и грант, где
каталог серий).

После «Правил витрин» в конце §6.3 — каноническое определение из `var keyrateView`
(`moex/keyrate.go`), скопированное побайтово. `Select` (в файле с нулевым отступом, чтобы
строка совпадала с Go-литералом):

```
SELECT f.date AS date, f.secid AS secid, c.series_name AS series_name,
       c.last_trade AS decision_date, f.settle AS settle, f.close AS close,
       f.volume AS volume, f.openposition AS openposition,
       round(f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0), 4) AS expected_change_pp,
       round((f.settle - nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)) * 100, 1) AS expected_change_bp
FROM keyrate_futures AS f FINAL
INNER JOIN keyrate_contract AS c FINAL ON f.secid = c.secid
```

`Comment` и все 10 `Columns` — дословно, в виде `ALTER TABLE v_keyrate_expectations MODIFY
COMMENT '…'` / `COMMENT COLUMN <col> '…'` (то, что исполняет `util.CreateView`), в двух
блоках ```sql после буллета-строки витрины. Оба блока — с шапкой-комментарием, что это
`keyrateView` и что копии обязаны совпадать.

### §6.1 — строка импортёра + §2

- Строка `keyrate_futures` (`moex/`) после `ofz_curve`: `Name()` и id шага DAG; источник
  (три эндпоинта ISS с блоками), что список серий не хардкодится (борд `RFUD`, префикс `KK`,
  `is_traded = 1`), инкремент по каждой серии от её `max(date) + 1 день`, пагинация `start`
  (+ `TOTAL` из `history.cursor`), отказ одной серии vs отказ всех (ошибка **до** публикации),
  DDL до сети, `SETTLEPRICE` как цена дня, ⚠️ `100 − ставка` неприменима; метод+расписание
  (`dagu/moex.yaml`, 19:13 МСК, шаг 3 после `ofz_curve`, retry 3 × 300 с), каталог
  (`series_catalog`, source `moex`, одна запись на серию, частота `D`) и витрина.
- В списке `Name()` добавлен `keyrate_futures`; в абзац про пакеты — «Пакет `moex/`
  регистрирует ТРИ импортёра: `stock_prices`, `ofz_curve`, `keyrate_futures`…».
- §2 «Структура репозитория»: строка `moex/` расширена (фьючерс KEYRATE → `keyrate_futures`
  + `keyrate_contract`, порядок шагов DAG, витрина). Это п.1 чек-листа скилла
  `sync-readme-architecture`.

### §7 — календарь

После абзаца про `events_calendar` добавлена пометка: дата экспирации серии KEYRATE
(`LSTDELDATE`) — это и есть дата заседания, импортёр берёт её у биржи и кладёт в
`keyrate_contract.last_trade` (обновляется ежедневно, `dagu/moex.yaml` 19:13 МСК, шаг 3),
видна агенту из `v_keyrate_expectations` (§6.3) и `v_series_catalog` (source `moex`);
календарь заседаний ЦБ публикуется на [cbr.ru/dkp/cal_mp/](https://www.cbr.ru/dkp/cal_mp/),
а в таблицу §7 и сид `events_calendar` заседания попадают вручную — при сверке дат
ориентироваться на календарь ЦБ и биржевой `last_trade`. Замыкающая оговорка: инструмент
низколиквиден, значение индикативно.

## 4. `README.md`

- Таблица «Что импортируется»: строка `moex` расширена — источник (фьючерс KEYRATE с
  примерами серий), таблицы (`keyrate_futures`, ключ `(secid, date)`; `keyrate_contract`,
  `ORDER BY secid`), витрина `v_keyrate_expectations`, расписание (`dagu/moex.yaml`,
  19:13, шаги по порядку `stock_prices` → `ofz_curve` → `keyrate_futures`, retry 3 × 300 с;
  вручную `make import STAT=keyrate_futures`).
- «Структура репозитория»: строка `moex/` дополнена (`фьючерс KEYRATE → keyrate_futures +
  keyrate_contract (витрина v_keyrate_expectations)`).
- Новый абзац-описание импортёра (в стиле абзацев wgc/bold/news/craw) + runbook-блок
  `make import STAT=keyrate_futures`: динамическое обнаружение серий, паспорт и `LSTDELDATE`,
  `SETTLEPRICE` и множитель, `LAST` = 0 в дни без сделок, инкремент по каждой серии,
  fail-fast «сбой всех серий — до публикации», состав витрины, `nullIf(…, 0)` и смысл NULL,
  ⚠️ низкая ликвидность (индикатив), расписание, запрет сырых таблиц агенту, каталог.
- «Ограничения и известные проблемы»: новый буллет про KEYRATE (история с 29.09.2026 —
  `FRSTTRADE` первой серии; квартальная экспирация серий, сравнивать по `decision_date`;
  низкая ликвидность и стартовый `SETTLEPRICE` дальней серии; `close`/`LAST` ожиданий не
  отражают; NULL в `expected_change_*` = «ставка не загружена»; даты — биржевые и
  `cbr.ru/dkp/cal_mp/`).

## 5. Проверка байт-идентичности (код → ARCHITECTURE.md)

Скрипт извлекает из `moex/keyrate.go` raw-строки `keyrateDdl`, `keyrateContractDdl`,
`keyrateView.Select`, склеенный `Comment` и все 10 значений `Columns` (склейка `"…"+ "…"`)
и ищет каждую строку в `ARCHITECTURE.md` подстрокой. Результат:

```
keyrateDdl                   BYTE-IDENTICAL in ARCHITECTURE.md
keyrateContractDdl           BYTE-IDENTICAL in ARCHITECTURE.md
Select                       BYTE-IDENTICAL in ARCHITECTURE.md
Comment                      BYTE-IDENTICAL in ARCHITECTURE.md
Columns[date]                BYTE-IDENTICAL in ARCHITECTURE.md
Columns[secid]               BYTE-IDENTICAL in ARCHITECTURE.md
Columns[series_name]         BYTE-IDENTICAL in ARCHITECTURE.md
Columns[decision_date]       BYTE-IDENTICAL in ARCHITECTURE.md
Columns[settle]              BYTE-IDENTICAL in ARCHITECTURE.md
Columns[close]               BYTE-IDENTICAL in ARCHITECTURE.md
Columns[volume]              BYTE-IDENTICAL in ARCHITECTURE.md
Columns[openposition]        BYTE-IDENTICAL in ARCHITECTURE.md
Columns[expected_change_pp]  BYTE-IDENTICAL in ARCHITECTURE.md
Columns[expected_change_bp]  BYTE-IDENTICAL in ARCHITECTURE.md
columns: 10 | failures: 0
```

Отвергнутые формы встречаются в документе **только в пояснительной прозе** (в буллете
§6.3: «форма `keyrate_futures FINAL f` — `SYNTAX_ERROR` code 62» и «а не `CROSS JOIN`») и
в шапке-комментарии блока §6.3 — в самих канонических SQL-блоках их нет (`grep`: строки 652
и 672, оба — пояснения). Подстрока `k.rate` (плановая форма) в документе отсутствует.

## 6. Прочие проверки

- `dagu validate dagu/moex.yaml` → exit 0 (вывод выше).
- `gofmt -l moex/` → пусто; `go vet ./moex/` → чисто; `go test ./moex/...` →
  `ok github.com/kmlebedev/clickhouse-import-rosstat/moex 0.664s` (санити-чек: Go не менялся).
- GoLand MCP `get_file_problems README.md` → `{"errors":[]}`; `get_file_problems
  ARCHITECTURE.md` → только два **предсуществующих** замечания GoLand на строках 112/134
  («package expected, got 'type'» — Go-примеры §3 внутри markdown-фенса), к правкам
  отношения не имеют и до них существовали.
- Число строк-фенсов в `ARCHITECTURE.md` и `README.md` чётное (блоки сбалансированы);
  новые буллеты §6.3 не разорвали список (канонический блок вынесен в конец §6.3, ниже
  «Правил витрин»).
- Кликхаус-запросы — только read-only `DESCRIBE` через `mcp__clickhouse__run_query`
  (два запроса, см. §3). `CREATE`/`INSERT` не выполнялись; `make ch-up`/`ch-down` и импортёры
  не запускались (`make all` — за контролёром).

## 7. Файлы

Изменены: `dagu/moex.yaml` (+8/−0), `sql/mcp_kimi_reader.sql` (+1/−0), `ARCHITECTURE.md`
(+75/−3), `README.md` (+9/−2). Коммит `71b79f1` — «wire keyrate futures importer: dagu step,
MCP grant, canonical DDL». Посторонняя модификация `deploy/staging/dagu.service` не затронута
и не застейджена (`git status` после коммита показывает только её).

## 8. Self-review

1. **План/спека как источник SQL — отвергнуты.** И план, и спека содержат невалидную для
   ClickHouse форму (`FINAL f`) и `CROSS JOIN`, который был заменён скалярным подзапросом.
   В `ARCHITECTURE.md` перенесён код (`AS f FINAL`, `nullIf((SELECT argMax(...) …), 0)`),
   что подтверждено `DESCRIBE` на сервере (UNKNOWN_TABLE против SYNTAX_ERROR).
2. **`Nullable(Float64)` у вычисленных колонок** — следствие `nullIf`: без обёртки пустая
   `cbr_key_rate` давала бы `settle × 100` б.п. Это оговорено и в буллете §6.3, и в README.
3. **Канонический блок §6.3 пришлось вынести из буллета.** Первая редакция держала ```sql-блок
   внутри элемента списка (отступ 2 пробела) — читалось красиво, но ломало байт-идентичность:
   в файле у строк `Select` появлялись лишние ведущие пробелы. Блоки вынесены в конец §6.3
   (нулевой отступ), буллет на них ссылается; проверка байт-идентичности проходит.
4. **Не добавлен абзац «правило 11 удовлетворяет» в §6.5.** У соседей по контуру такой абзац
   пишется **после** живой проверки (там указано «проверено»), а таблиц ещё нет: их создаёт
   первый прогон импортёра, который в этой задаче запускать запрещено. Кроме того
   `scripts/mcp_check.py` содержит **явные** списки витрин (в них `v_keyrate_expectations`
   пока нет) — заявлять «проверяется `make mcp-check`» было бы неправдой. Считаю, что этот
   абзац относится к Task 5 (живая приёмка) — см. «Concerns».
5. **§7 — минимальное вмешательство.** Строку таблицы про заседание 23.10.2026 не менял:
   привязка серии `KKV6` к этой дате следует из спеки, но в §7 таблица — сводка для людей, а
   заметка про источник дат добавлена отдельным абзацем (как просил брифинг).
6. **`README.md`:** в MCP-раздел (строка ~195, список витрин для агента) правку не вносил —
   там список неполный и последние импортёры (СБКТС, `company_views`, `dcf`) его тоже не
   трогали; вместо этого грант и запрет сырых таблиц описаны в абзаце импортёра и в буллете
   ограничений, как у соседей.
7. Мелочь, поправленная по ходу: в §6.1 формулировка про порядок батчей была неточной
   («паспорт пишется до истории») — в коде оба батча отправляются в конце, паспорта первыми;
   переписано в «двумя батчами (сначала паспорта, затем дни)».

## 9. Concerns

1. **`make mcp-check` не проверяет keyrate.** В `scripts/mcp_check.py` витрины перечислены
   явно (`v_company_*`, `v_news_events`, `v_gost_*`), `v_keyrate_expectations` там нет, а
   запрет сырых `keyrate_futures`/`keyrate_contract` скрипт не проверяет. Приёмка Task 5
   («`make mcp-check` → ПРОВЕРКА ПРОЙДЕНА … `keyrate_futures` — ACCESS_DENIED») без правки
   скрипта пройдёт формально, ничего про keyrate не проверив. Правка `scripts/mcp_check.py`
   в объём Task 4 не входила (брифинг: четыре файла, Go/тесты не трогать) — выношу на решение
   контролёра: добавить в Task 5 или завести отдельным пунктом.
2. **Доступность витрины зависит от `cbr_key_rate`.** Гейт `util.CreateView` требует все три
   таблицы, поэтому на чистой базе (или до первого прогона `cbr`) витрина не появится, и
   грант из `sql/mcp_kimi_reader.sql` на несуществующую витрину, вероятно, упадёт
   (ClickHouse: `UNKNOWN_TABLE … v_keyrate_expectations`). Это не дефект правки, но при
   `make mcp-user` на пустой БД порядок «сначала импорт `keyrate_futures` + `cbr`, затем
   грант» нужно соблюсти. В документе это оговорено (§6.3 и §6.1: «гейтится по трём таблицам»),
   но в отчёте фиксирую явно.
3. **Тексты комментариев — аргумент к коду.** Если `moex/keyrate.go` (`Comment`/`Columns`)
   будет отредактирован, §6.3 разойдётся; проверка байт-идентичности — внешний скрипт, в
   репозитории её нет (теста на синхронность DDL/витрины с документацией в проекте нет и
   раньше не было).

---

# Fix report — ревью Task 4 («accept with fixes»), 2026-10-13

База фикса: `71b79f1` (первый коммит Task 4). Коммит фикса: **`c60b600`** —
«fix keyrate docs and teach mcp-check the keyrate contour» (`ARCHITECTURE.md` +2/−2,
`README.md` +1/−1, `scripts/mcp_check.py` +32/−0). Посторонний `deploy/staging/dagu.service`
по-прежнему не застейджен (`git status` после коммита показывает только его).

## R1 — `ARCHITECTURE.md`, буллет `v_keyrate_expectations` (§6.3)

Было: «…поэтому при незагруженной ставке не публикуется…» — неверно: это свойство
**отсутствующей таблицы**, а не пустой.

Стало: «Витрина гейтится по трём таблицам (`keyrate_futures`, `keyrate_contract`,
`cbr_key_rate`): пока таблицы `cbr_key_rate` нет, витрина не создаётся; если таблица есть, но
пуста, `expected_change_pp`/`expected_change_bp` = NULL (`nullIf` выше), а не отказ
публикации; …».

Проверка, что формулировка не разошлась с кодом: `util.CreateView` гейтит только по
`system.tables` (наличие), а `nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)` —
источник NULL при пустой таблице; колонки объявлены как `Nullable(Float64)` (§6.3).

## R2 — `README.md`, абзац импортёра

`s.log.Warn\` с \`secid\`` → `log.Warnf` с `secid``. Оба вызова в `moex/keyrate.go`
(`log.Warnf("keyrate_futures: серия %q пропущена…")` и `log.Warnf("keyrate_futures: паспорт
серии %s не получен: %v")`) — через `log.Warnf`, исправлено на него.
`grep -c "log.Warn\` с \`secid\`" README.md` → `0`.

## R4 — `ARCHITECTURE.md`, блок `Comment и Columns` (§6.3)

В документированном SQL-литерале витринного `MODIFY COMMENT` кавычка внутри текста была не
экранирована (`…source='moex'';`), то есть документ показывал **невалидный** SQL. Исправлено
на `…source=\'moex\'';` — ровно так, как её производит `util/views.go` `escapeComment`
(`\` → `\\`, затем `'` → `\'`).

Следствие для проверки байт-идентичности (важно): теперь `Comment` присутствует в
`ARCHITECTURE.md` не «как есть», а в **экранированном** виде, потому что он лежит внутри
строкового литерала. Проверка обновлена: документированное `ALTER TABLE … MODIFY COMMENT
'…';` обязано совпасть с `escapeComment(Comment)`, а литерал после снятия экранирования —
с самим `Comment`. Результат:

```
Comment (escaped, as util.CreateView emits): BYTE-IDENTICAL
  unescaped literal == Go Comment: True
DDL/Select/Columns byte-identity: OK 13/13
broken SQL literal (source='moex''): none
```

`Select`, `keyrateDdl`, `keyrateContractDdl` и все 10 колонок — по-прежнему байт-в-байт
(13 позиций; `Comment` проверяется отдельно, экранированным). Остальные комментарии колонок
кавычек не содержат, поэтому их литералы экранирования не требуют.

## R5 — `scripts/mcp_check.py`: контур keyrate

Добавлен блок +32 строки (в конце `check()`, сразу после блока СБКТС). Место: в скрипте
**нет** группы MOEX/market — `v_stock_prices` и `v_ofz_curve` не проверяются вовсе, а
структура файла — по одному снабжённому комментарием блоку на контур, дописываемому в порядке
появления контуров (… `bold` → `news` → СБКТС → **keyrate**). Поэтому новый блок следует форме
соседей (bold/news/gost): положительная проверка — разобранный `result_rows` ответа
`SELECT count() FROM <view>` (доказывает и существование, и право чтения; `UNKNOWN_TABLE`/
`ACCESS_DENIED` приходят ошибкой и в `result_rows` не разбираются), отрицательная —
`ACCESS_DENIED`/`Not enough privileges` на сырой таблице:

```python
keyrate_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_keyrate_expectations"}))
keyrate_view_rows = result_rows(keyrate_view)
…
failed += not expect("витрина v_keyrate_expectations существует и читается", bool(keyrate_view_rows), keyrate_view_detail)

for table, label in (("keyrate_futures", "дневные итоги серий"),
                     ("keyrate_contract", "паспорта серий")):
    raw = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {table}"}))
    denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
    failed += not expect(f"сырая {table} закрыта ({label})", denied,
                         f"отказ ClickHouse 497 на {table} (ожидаемо)" if denied else raw[:200])
```

Сознательные решения: (1) проверка **обеих** сырых таблиц, а не только `keyrate_futures` —
запрет по брифингу Task 4 распространяется на обе, вторая строка цикла стоит одну строку кода;
(2) `count()` не сравнивается с числом: витрина гейтится по трём таблицам, до первого прогона
импортёров строк может не быть вовсе, а пустая `cbr_key_rate` даёт NULL-значения при читаемых
строках (та же логика, что у `v_dcf_assumptions`, `v_gold_etf_flows_daily`, `v_news_events`).

**Живого MCP-сервера в этом окружении нет** (`make mcp-check` запускает `uv run --with
mcp-clickhouse …` и требует `CLICKHOUSE_PASSWORD`/живого ClickHouse-пользователя), поэтому
скрипт не «прогонялся по-настоящему», и я не заявляю, что проверка проходит: её запускает
контролёр в Task 5. Вместо этого новые ветки проверены офлайн стендом-заглушкой `mcp`-клиента
(скрипт на stdin, без файлов): фальшивая сессия возвращает заданные ответы `run_query`, и
печатается вывод `expect`. Три сценария:

1. гранты верные (витрина читается, сырые закрыты):

```
OK   витрина v_keyrate_expectations существует и читается: строк: 42
OK   сырая keyrate_futures закрыта (дневные итоги серий): отказ ClickHouse 497 на keyrate_futures (ожидаемо)
OK   сырая keyrate_contract закрыта (паспорта серий): отказ ClickHouse 497 на keyrate_contract (ожидаемо)
```

2. регрессия — сырые таблицы случайно читаемы: обе строки дают `FAIL` с сырым ответом
(`{"columns": ["count()"], "rows": [[7]]}`), то есть проверка не «зеленеет» молча;

3. витрины нет (`UNKNOWN_TABLE`): строка витрины даёт `FAIL … витрина не читается: Code: 60 …
(UNKNOWN_TABLE)`, обе сырые — `OK`.

(В стенде остальные 27 проверок скрипта дают `FAIL` — это артефакт заглушки, которая на все
прочие запросы отвечает одним и тем же `count() = 0`; к правке отношения не имеет.)

## R8 — дата заседания 19:00 МСК: утверждение сохранено, источник назван

Пункт не переписывался (рулинг «no change»), но я воспользовался разрешением в нём и сделал
источник явным: `(19:00 МСК — `EXPIRATION_TIME` паспорта ISS)`. Это то самое поле
`EXPIRATION_TIME = 19:00:00` из блока `description` `/iss/securities/KKV6.json`, которое
зафиксировано в спеке (строка «Паспорт серии»). Сам факт не менялся, добавлен только провенанс.

## R3 — не трогал

`GRANT SELECT` на возможно отсутствующую витрину оставлен как есть; поведение
`util.CreateView` (молчаливый пропуск при неполном наборе таблиц-источников) не менялось.

## Команды и выводы (fix-итерация)

- `python3 -m py_compile scripts/mcp_check.py` → OK (ошибок нет). Отдельного теста для
  `mcp_check.py` в репозитории нет (`scripts/*test*.py` — только `extrapolation_aisc_test.py`,
  `mcp_setup_user_test.py`, `polyus_output_model_test.py`; `pytest` в окружении отсутствует),
  поэтому дополнительно прогнан офлайн-стенд выше.
- `dagu validate dagu/moex.yaml` → exit 0 (то же generic-предупреждение `auth.mode`, что и на
  нетронутых DAG-файлах).
- `gofmt -l moex/ scripts/` → пусто (Go не менялся).
- Скрипт проверки байт-идентичности (см. выше) → `DDL/Select/Columns 13/13`, `Comment` —
  экранированным литералом, round-trip после снятия экранирования сходится.
- `grep -c "log.Warn\` с \`secid\`" README.md` → `0`; функция поиска нового текста R1/R4
  находит исправленные формулировки по одному вхождению.

## Обновление к предыдущему разделу «Проверка байт-идентичности»

Пункт 5 отчёта (таблица `14/14`) верен для состояния `71b79f1`; после R4 `Comment`
документируется в экранированном виде, и корректная формулировка проверки —
«13 позиций байт-в-байт + `Comment` как `escapeComment(Comment)`».
