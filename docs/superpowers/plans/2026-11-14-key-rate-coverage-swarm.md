# План: рой агентов на закрытие пробелов контура ключевой ставки

**Дата:** 2026-11-14. **Вход:** [research 2026-11-14-key-rate-data-coverage-audit.md](../research/2026-11-14-key-rate-data-coverage-audit.md) (аудит полноты, свежести и винтажей на dev). **Роадмап:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2. **Правила диспатча:** [subagent-dispatch](../../../.kimi-code/skills/subagent-dispatch/SKILL.md).

## 0. Принципы роя

1. **Один пункт — одна ветка — один worktree — один PR.** Ветки создаются от `main` после мержа research-PR; чужие worktree не трогаем.
2. **Общие артефакты — один писатель.** `cbr/importers.go`, `cbr/series_meta.go`, `sql/mcp_kimi_reader.sql`, `dagu/cbr.yaml`, `docs/IMPORTERS.md`, `ARCHITECTURE.md` §6.2/§6.3 правит **только интегратор** (волна 2). Агенты волны 1 готовят «заявку на проводку»: список строк, которые надо добавить, с готовым текстом.
3. **Модель — только явным `model:`.** Пул на 2026-11-14: `deepseek/deepseek-flash` (крупный Go, финальное ревью), `qwencloud/qwen3.8-flash` (ревью задач), `xiaomimimo/flash` (мелкие механические правки). Модель автора не ревьюит свой код.
4. **Одновременных coder-агентов — не больше 4.** Одна dev-ClickHouse: параллельные импорты безопасны, только если пишут в **разные таблицы** (так и разложено ниже).
5. **Промпт субагента** обязан содержать блок про `mcp__jetbrains__*` / `mcp__clickhouse__*` и роль (скил `go-engineer` или `research-engineer`) — `AGENTS.md` субагенту автоматически не передаётся.

## 1. Волны

| Волна | Что | Параллелизм | Выход |
|---|---|---|---|
| **0. Recon** | подтвердить источник и глубину по каждому ряду | 5 read-only агентов | source-report на строку + заявка на импортёр |
| **1. Реализация** | импортёры/правки по своим пакетам | до 4 coder | ветка + зелёные `make all` |
| **2. Интеграция** | реестр, каталог, витрины, гранты, dagu, доки | 1 интегратор | одна ветка `feat/key-rate-coverage-wiring` |
| **3. Приёмка** | ревью задач, финальное ревью, живой прогон | 2 ревьюера | протокол в PR |

## 2. Потоки

| ID | Пункт / issue | Роль | Модель | Ветка | Пишет (только эти файлы) |
|---|---|---|---|---|---|
| R1 | ОНДКП: извлечение чисел, 28 выпусков — #572 | research | `deepseek/deepseek-flash` | `research/ondkp-extraction` | `docs/research/2026-11-14-ondkp-extraction.md`, `docs/research/ondkp-extract.csv` |
| R2 | RUONIA с 2010 — #574 | research | `deepseek/deepseek-flash` | `research/ruania-history-source` | `docs/research/2026-11-14-ruania-history-source.md` |
| R3 | Ожидания с 2017 — #575 | research | `deepseek/deepseek-flash` | `research/infl-exp-history-source` | `docs/research/2026-11-14-infl-exp-history-source.md` |
| R4 | MIACR + ликвидность — #576, #577 | research | `deepseek/deepseek-flash` | `research/cbr-html-tables-recon` | `docs/research/2026-11-14-cbr-html-tables-recon.md` |
| R5 | АЦН-обрыв 2024-10 — #578 | research | `xiaomimimo/flash` | `research/ccyb-gap-diagnosis` | `docs/research/2026-11-14-ccyb-gap.md` |
| T1 | импортёр `cbr_ondkp` + `cbr_ondkp_forecast` | go | `deepseek/deepseek-flash` | `feat/key-rate-ondkp-vintages` | `cbr/ondkp*.go`, `cbr/ondkp_test.go`, `cbr/testdata/ondkp*` |
| T2 | RUONIA: догрузка 2010–2018 | go | `deepseek/deepseek-flash` | `feat/ruania-history` | `cbr/ruania*.go`, `cbr/ruania_test.go` |
| T3 | Ожидания: догрузка 2017–2022 | go | `xiaomimimo/flash` | `feat/infl-exp-history` | `cbr/infl_exp*.go`, `cbr/infl_exp_test.go` |
| T4 | импортёр `cbr_mirac` | go | `deepseek/deepseek-flash` | `feat/cbr-mirac` | `cbr/mirac*.go`, `cbr/mirac_test.go` |
| T5 | импортёр `cbr_liquidity` | go | `deepseek/deepseek-flash` | `feat/cbr-liquidity` | `cbr/liquidity*.go`, `cbr/liquidity_test.go` |
| T6 | АЦН: починка или фиксация обрыва | go | `xiaomimimo/flash` | `fix/ccyb-refresh` | `cbr/ccyb*.go` (если правка в коде) |
| I1 | Проводка: реестр, каталог, витрины, гранты, dagu | go | `deepseek/deepseek-flash` | `feat/key-rate-coverage-wiring` | `cbr/importers.go`, `cbr/series_meta.go`, `sql/mcp_kimi_reader.sql`, `dagu/cbr.yaml`, `docs/IMPORTERS.md`, `ARCHITECTURE.md` |
| V1 | Ревью T1 (ОНДКП) | review | `qwencloud/qwen3.8-flash` | — | отчёт в PR |
| V2 | Ревью T4 + T5 (HTML-таблицы) | review | `qwencloud/qwen3.8-flash` | — | отчёт в PR |
| V3 | Финальное ревью интеграционной ветки | review | `deepseek/deepseek-flash` | — | отчёт в PR |

## 3. Задачи

### Волна 0 — recon (все read-only, могут идти одновременно)

**R1. ОНДКП: извлечение чисел.**
1. По каждому из 28 выпусков вытащить: цель по инфляции, строку «Ключевая ставка, в среднем за год» (значения по годам), прогноз инфляции и ВВП.
2. Три эпохи разными инструментами: `.doc` (ZIP + `textutil` + перекодировка cp1251), PDF 2011–2020, PDF 2021–2027.
3. Проверить 2022 год: существует ли итоговый документ помимо `on_project_2022(2023-2024).pdf`.
4. Выход: таблица 28 × (цель, ставка по годам) + CSV + раздел «где вёрстка не совпала».
**Проверка:** каждое число имеет ссылку на файл и строку; повторный разбор даёт тот же CSV.

**R2. RUONIA с 2010.** Найти способ отдать историю до 2019: полный querystring формы, XLSX-выгрузка индекса, срочная версия. Зафиксировать URL, параметры, глубину, HTTP-код.
**Проверка:** пробная выборка за январь 2010 возвращает непустые строки (или явное «источник не отдаёт» с доказательством).

**R3. Инфляционные ожидания с 2017.** Найти архив волн: страница `analytics/dkp/inflationary_expectations`, паттерн `Infl_exp_YY-MM.*`, XLSX или PDF-коллекция. Отдельно — менялась ли методика инФОМ до 2020.
**Проверка:** ссылка на волну 2017-01 и 2022-12, обе отдают 200.

**R4. MIACR и ликвидность.** Для обеих форм `UniDbQuery` — обязательные параметры, глубина, имена колонок, число строк за пробный месяц. Проверить, что `util.ParseHTMLTable` берёт таблицу без правок.
**Проверка:** пробный месяц 2000-08 (MIACR) и 2014-02 (ликвидность) разбираются в числа.

**R5. АЦН-обрыв.** Проверить три версии: источник перестал публиковать файл, импортёр не находит ссылку, шаг выпал из DAG. Выход — одна строка диагноза + доказательство.
**Проверка:** назван конкретный отказ (URL/код/лог), а не «не работает».

### Волна 1 — реализация (по одной ветке на пункт)

**T1. `cbr_ondkp`.** Таблицы: `cbr_ondkp` (выпуск: год, дата, URL, формат, sha256) и `cbr_ondkp_forecast` (ключ `release_year, indicator, year`; `lo`/`hi`, `lo = hi` для точки). Парсер — по образцу `cbr_mt_forecast` (PDF через `pdftotext -layout`, DOC через `textutil` + перекодировка). Фикстуры трёх эпох.
**Проверка:** `Imported 28 releases`, повтор — no-op; числа 2027 г. совпадают с PDF (12,5–13,5 / 9,0–10,0 / 7,5–8,5).

**T2. RUONIA.** Инкремент по `max(date)` заменить на бэкфилл окнами 2010→2019, если источник отдаёт диапазон; иначе — новый транспорт из R2.
**Проверка:** `min(date) = 2010-01-11`, 10 точек сверены руками, повтор — no-op.

**T3. Ожидания.** Догрузка волн 2017–2022 тем же парсером; ключ `(name, date)` не меняется.
**Проверка:** `min(date) = 2017-01-*`, оба ряда одной длины, повтор — no-op.

**T4. MIACR.** Новый импортёр: MIACR, MIACR-IG, средневзвешенные ставки по операциям ЦБ; таблица `cbr_mirac`, ключ `(name, date)`.
**Проверка:** `min(date) ≤ 2000-07-13`, число торговых дней месяца сходится с календарём.

**T5. Ликвидность.** Два ряда: дневной (`bliquidity`) и периоды усреднения (`BSliquidityMA`); таблица `cbr_liquidity`.
**Проверка:** дневной с 2014-01-01, усреднение с 2018-01-10; знак дефицита/профицита описан в комментарии колонки.

**T6. АЦН.** По диагнозу R5: либо починить ссылку/шаг, либо (если источник закрыт) зафиксировать в `LIMITATIONS.md` и снять `has_ccyb` из веера — решение фиксирует владелец.
**Проверка:** `max(date) ≥ 2026-09-01` либо документированная причина.

### Волна 2 — интеграция (один писатель)

**I1.** После мержа T1–T6 в одну ветку: записи в `cbr/importers.go`, `series_meta.go` (title/unit/frequency/origin на каждый ряд), витрины `v_cbr_ondkp_forecast`/`v_cbr_mirac`/`v_cbr_liquidity` с комментариями, `GRANT SELECT` в `sql/mcp_kimi_reader.sql`, шаги в `dagu/cbr.yaml`, `docs/IMPORTERS.md`, `ARCHITECTURE.md` §6.2/§6.3.
**Проверка:** `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; агент видит новые витрины под `kimi_reader`.

### Волна 3 — приёмка

**V1/V2** — ревью задач по чек-листу импортёра (стандарт §14/§15): батчевая вставка, `os.Getenv`, отсутствие сети в `init()`, ошибки не проглочены, фикстуры.
**V3** — финальное ревью ветки: сверка чисел с research-доком, отсутствие регрессий в существующих рядах, протокол живой приёмки в PR.

## 4. Владение файлами (во избежание конфликтов)

| Артефакт | Владелец | Кто ещё касается |
|---|---|---|
| `cbr/importers.go`, `cbr/series_meta.go` | I1 | никто |
| `sql/mcp_kimi_reader.sql`, `dagu/cbr.yaml` | I1 | никто |
| `docs/IMPORTERS.md`, `ARCHITECTURE.md` §6.2/§6.3 | I1 | никто |
| `docs/ROADMAP_KEY_RATE.md` §4.2, статусы issue | владелец пункта (PR каждого потока) | I1 — только строка покрытия |
| `docs/LIMITATIONS.md` | тот поток, что нашёл ограничение | I1 — при конфликте строк |
| `cbr/<поток>*.go` | соответствующий T-агент | никто |

**Порядок мержа:** R1–R5 (параллельно) → T1–T6 (параллельно, каждый свой PR) → I1 (один PR) → приёмка. Если два T-потока конфликтуют по `cbr/testdata/`, папки называются по префиксу потока (`ondkp/`, `mirac/`, `liq/`).

## 5. Шаблон промпта coder-субагента

```text
Пункт: <slug>/<item-id>, issue #<N>.
Ветка: <branch>; worktree: .worktrees/<slug>.
Роль: go → скил .kimi-code/skills/go-engineer/SKILL.md (или research → research-engineer).
Правила: AGENTS.md правила 1–12, docs/IMPORTER_PACKAGE_STANDARD.md §14/§15.
Пишешь только: <список файлов>. Общие артефакты (реестр, series_meta, гранты, dagu)
не трогаешь — в отчёт добавляешь «Заявка на проводку» с готовыми строками.
У тебя есть mcp__jetbrains__* (GoLand MCP) и mcp__clickhouse__*: SQL — только через
mcp__clickhouse__run_query, навигация и проверка правок — через GoLand MCP.
Если MCP-инструментов нет в наборе — скажи в отчёте (DONE_WITH_CONCERNS), не молчи.
Приёмка: <критерий из issue>. Отчёт: команды, числа, повтор (no-op), что не проверено.
```

## 6. Риски

| Риск | Что делаем |
|---|---|
| Источник не отдаёт историю (RUONIA/ожидания) | R-агент закрывает no-go с доказательством; issue получает `Blocked`, а не «сделано» |
| `.doc`-корпус ОНДКП не парсится на какой-то эпохе | фикстура на эпоху валит тест; выпуск пропускается с ошибкой, не с «0 строк» |
| Параллельные импорты в dev мешают друг другу | таблицы разведены; общий `series_catalog` пишет только I1 |
| Агент трогает чужие файлы | ревью V1/V2 проверяет дифф на владение; выход за список — возврат |
| Модель наследуется молча | `model:` обязателен в каждом `Agent`-вызове |

## 7. Критерий готовности роя

- Все 7 issue закрыты или переведены в `Blocked` с доказательством.
- `make mcp-check` зелёный; новые витрины видны под `kimi_reader`.
- `docs/ROADMAP_KEY_RATE.md` §4.1 обновлён снимком после догона.
- Протокол живой приёмки — комментарием в каждом PR.
