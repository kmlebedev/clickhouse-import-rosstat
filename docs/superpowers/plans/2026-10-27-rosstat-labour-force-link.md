# План: `rosstat_labour_force_mes` — регистронезависимый поиск ссылки

**Пункт:** `key-rate/phase-2-source-fixes` (фаза 2).
**Issue:** [#337](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/337).
**Спека:** [2026-10-27-rosstat-labour-force-link-design.md](../specs/2026-10-27-rosstat-labour-force-link-design.md).
**Ветка:** `fix/rosstat-labour-force-link`, worktree `.worktrees/rosstat-labour-force-link`.

---

## 1. Задачи

### Task 1 — селектор (`rosstat/labour_force.go`)

1. `labourForceLinkSelector` → `` `a[href$="trud-1_15-s.xlsx" i]` ``.
2. Комментарий у константы: регистр в имени файла источник менял (`trud-` → `Trud-` в выпуске от 14.10.2026), поэтому селектор регистронезависим; полное имя держит его от соседних выгрузок карточки (`trud_1_15-72.xlsx`, `Trud-2_15-s.xlsx`, `Trud-3_15-s.xlsx`); любое иное переименование обязано падать громко.
3. Текст ошибки в `labourForceDataUrl`: «ссылка на trud-1_15-s.xlsx (любой регистр) не найдена на …».
4. Комментарий у `labourForceDataUrl` — та же причина (регистр), остальное без изменений.

### Task 2 — фикстуры (`rosstat/testdata/`)

1. HTML: снять `https://rosstat.gov.ru/labour_force?print=1`, вырезать карточку «Занятость и безработица» целиком в `labour_force_page.html`; шапка-комментарий — дата 2026-10-27, размер, шесть ссылок карточки, заглавная `T` у нашей выгрузки.
2. XLSX: скачать живой `Trud-1_15-s.xlsx` (345 320 байт, 14.10.2026), урезать zip-хирургией (§11.2) до листов `Содержание` и `5`, заменить `labour_force.xlsx`.
3. Проверка урезания: `unzip -t` — ok; список листов `{Содержание, 5}`; все значения листа «5» равны значениям исходного архива (посравнению по всем строкам); шапка, якоря, сноски на местах.

### Task 3 — тесты (`rosstat/labour_force_test.go`)

| Место | Правка |
|---|---|
| шапка файла | «строки 132–133» → «133–134», редакция книги — до августа 2026 |
| `TestLabourForceReadsFixture` | 690→696, 115→116, последняя дата `2026-07-31`→`2026-08-31` |
| `TestLabourForceSpotValues` | прежние споты остаются; добавить споты августа 2026: `Рабочая сила` B132 = 76668.705, `Уровень безработицы` G132 = 2.2 |
| `TestLabourForceEmptyCellIsNotPoint` | baseline 690→696; чистить `B132:G132` вместо `B131:G131`; комментарий — «последний месяц выгрузки» |
| `TestLabourForceGarbageLabelOnLastMonthFails` | `A131`→`A132`, «июля 2026»→«августа 2026» |
| misspelled / unparsed / lost-month / mismatched-year / removed-december | литералы в сообщениях 690→696; изменяемые ячейки и `RemoveRow` не меняются (строки до 131 не сдвинулись) |
| `TestLabourForceIgnoresTrailingFootnoteRows` | wantMonths 115→116, wantPoints 690→696, last `2026-08-31`, «строки 133–134» |
| `TestLabourForceImportAppendsFloat64` | 690→696, 115→116, «2017-01 … 2026-08» |
| `TestLabourForceFindLink` | ожидаемый href `/storage/mediabank/Trud-1_15-s.xlsx`; проверка фикстуры на `Trud-1_15-s.xlsx` и соседа `trud_1_15-72.xlsx`; отрицательный кейс сохраняется |

### Task 4 — документация

1. `ARCHITECTURE.md:4` — «Последнее обновление»: пункт `key-rate/phase-2-source-fixes` (#337), регистр имени файла, флаг `i`, цифры приёмки.
2. `ARCHITECTURE.md:451` — строка `rosstat_labour_force_mes`: селектор с `i`, причина, новая приёмка (`Imported 696 rows`, 6 × 116, `2017-01-31 … 2026-08-31`).
3. `rosstat/series_meta.go` — `origin` шести рядов: текущее написание имени файла с оговоркой о регистре.
4. `docs/ROADMAP_KEY_RATE.md:220` — второй фикс пункта (issue #337): причина, решение, приёмка, ссылки на спеку/план.
5. `docs/ROADMAP_KEY_RATE.md:76` — строка покрытия `2017-01-31 … 2026-08-31 (696)`.
6. `docs/KEY_RATE_CHANNELS.md:214` — `2017-01-31 … 2026-08-31 (696 точек)`.

### Task 5 — приёмка

1. `make import STAT=rosstat_labour_force_mes` → `Imported 696 rows`; повтор — no-op по версии, 696 точек.
2. MCP `v_rosstat_macro` (`source='rosstat'`, шесть имён): 696 строк, 6 рядов, 116 на ряд, максимум `2026-08-31`; `series_catalog` — 6 записей, имена прежние.
3. `make all` — зелёный; `make docs-check`; `dagu validate dagu/rosstat.yaml` — exit 0; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

### Task 6 — ревью и PR

1. task-reviewer на `anthropic/haiku`, fix-loop; финальное ревью — `deepseek/deepseek-flash`.
2. Один PR: `[key-rate/phase-2-source-fixes] fix rosstat labour force file link`, `Closes #337` вне backticks.
3. Project V2: `Status = In Review`, `PR`, `Spec / Plan`; `Done` — после мержа владельцем.

## 2. Протокол живой приёмки (шаблон для PR)

```text
Пункт: key-rate/phase-2-source-fixes (#337)
Ветка: fix/rosstat-labour-force-link

- make import STAT=rosstat_labour_force_mes — <Imported N rows>; повтор — <no-op, версия …, 696 точек>
- v_rosstat_macro — <696 строк / 6 рядов / 116 на ряд / 2017-01-31 … 2026-08-31>; series_catalog — <6, имена прежние>
- make all — <результат>; make docs-check — <результат>
- dagu validate dagu/rosstat.yaml — <exit code>
- make mcp-check — <результат; чужие провалы — списком>
```
