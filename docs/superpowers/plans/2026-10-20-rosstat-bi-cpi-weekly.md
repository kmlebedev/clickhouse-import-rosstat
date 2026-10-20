# План: `key-rate/phase-4-bi-cpi-weekly` — импортёр `rosstat_bi_cpi_weekly`

- Issue: [#261](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/261), Task 1 [#260](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/260), эпик [#259](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/259)
- Спека: [2026-10-20-rosstat-bi-cpi-weekly-design.md](../specs/2026-10-20-rosstat-bi-cpi-weekly-design.md)
- Research: [2026-10-20-cb-comp-public-sources-and-bi.md](../../research/2026-10-20-cb-comp-public-sources-and-bi.md) §5
- Ветка: `feat/rosstat-bi-cpi-weekly`, worktree `.worktrees/rosstat-bi-cpi-weekly`

## 1. Spike (Task 1, #260) — выполнен

- UI-экспорт BI: `Consumer Price Index Estimation` → карточка `Consumer price index` → `⋮` → `Export` → CSV → `Export List` → `Export(csv)` → `Download`.
- CSV: 398 точек 2019-01-09 … 2026-10-12; 7 новогодних недель не публикуются; 12.10.2026 = 100,11.
- Воспроизводимость: `POST https://bi.rosstat.gov.ru/biportal/grid` (`GridViewService.exportGrid`) из чистой HTTP-сессии без cookies → `//OK[1,["tmp/exp<uuid>/Индекс потребительских цен.csv"],0,7]` → GET публичного CSV; повторный POST даёт новый `exp<uuid>`; значения совпали с UI 398/398.
- Хрупкость: `X-GWT-Permutation`, serialization policy hash, report/slice/view ids — часть сборки BI; при обновлении payload заменить по перехвату.

## 2. Код

1. `util/xls.go` (или отдельный `util/post.go`): `PostFileBytes` — POST через общий `HttpClient`, UA, `fileTimeout`; не-2xx — ошибка; тест `httptest`.
2. `rosstat/bi_cpi_weekly.go`:
   - константы: таблица, DDL, `biCpiWeeklyExportPayload`, `biCpiWeeklyExportURL`, `biCpiWeeklyBaseURL`, `X-GWT-Permutation`, `X-GWT-Module-Base`, имя ряда;
   - `BICpiWeeklyStat` со встроенным `util.ClickHouseImport` (`BodyFunc`, `AfterImport`);
   - `Import()`: POST → разбор `//OK[...]` → URL-encode → `DataUrl` → `ClickHouseImport.Import`;
   - CSV-парсер: шапка, даты `02.01.06`/`1/2/06`/`2006-01-02`, значение с точкой/запятой, сортировка, дубликаты, проверка подписи агрегата РФ, 0 точек — ошибка, разрывы допустимы;
   - тесты на реальной урезанной фикстуре + spot 100.11.
3. `rosstat/series_meta.go`: общая мета агрегата для бюллетеней и BI; описание 2009–2018/2019+/7 новогодних перерывов/ревизии/без PIT.
4. `rosstat/importers.go`: `{biCpiWeeklyImporter, "rosstat_bi_cpi_weekly", ""}`; обновить комментарии (восемь импортёров, preflight нет).
5. `rosstat/views.go` / `rosstatMacroView`: `Tables += rosstat_bi_cpi_weekly`; ветка BI до бюллетеней + `NOT IN` по `(series, date)`; комментарии витрины/колонок.
6. `dagu/rosstat.yaml`: восьмой шаг в хвосте, `CLICKHOUSE_IMPORT_STAT=rosstat_bi_cpi_weekly`, retry 3 × 300 с.
7. `rosstat/testdata/bi_cpi_weekly.csv` — реальная урезанная выгрузка.
8. `rosstat/importers_test.go`, `rosstat/series_meta_test.go` — энфорсер и мета.

## 3. MCP и документация

- `sql/mcp_kimi_reader.sql`: грант на `v_rosstat_macro` уже есть; новая сырая таблица гранта не получает.
- `scripts/mcp_check.py`: `rosstat_bi_cpi_weekly` в список закрытых сырых таблиц; чек агрегата — `count() > 502`, `max(date) > '2021-12-27'` (после приёмки — точное наблюдённое число); detail печатает наблюдённые значения.
- `ARCHITECTURE.md`: §2, §6.1, §6.2 (DDL), §6.3 (`v_rosstat_macro`), §7 (расписание), §8 (без preflight), верхняя строка.
- `README.md`, `docs/IMPORTERS.md`: новый импортёр, схема, запуск, покрытие.
- `docs/ROADMAP_KEY_RATE.md`: статус `phase-4-bi-cpi-weekly` — ✅ после живой приёмки, с командой, числом строк, датами, повтором и `make mcp-check`.
- `docs/site-pages.json` — если `make docs-check` потребует.

## 4. Приёмка

1. `make all` — зелёный.
2. `make import STAT=rosstat_bi_cpi_weekly` — `Imported N rows`; `min(date)=2019-01-09`, `max(date)` = текущая неделя, 398+ уникальных дат.
3. `v_rosstat_macro`: 12.10.2026 = 100,11; 2–3 недели 2019/2022 против UI.
4. Повтор: те же точки, `count() FINAL` не растёт, `max(date)` не уменьшается; сеть есть, результат идемпотентен.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/rosstat.yaml`; `make docs-check`.
6. Протокол — комментарием в PR: команды, числа строк, крайние даты, повтор, MCP, dagu.
7. PR: `Closes #259`, `Closes #260`, `Closes #261` (вне backticks); Project `In Review`; merge решает владелец.

## 5. Риски

| Риск | Митигация |
|---|---|
| BI обновил GWT-RPC (permutation/policy/ids) | Явная ошибка импортёра + инструкция в спеке: перехватить новый `POST /biportal/grid` и заменить константу |
| CSV меняет локаль/формат даты | Парсер принимает `DD.MM.YY`, `M/D/YY`, ISO; точка/запятая |
| 7 новогодних недель | Приёмка 398 точек; разрывы зафиксированы в описании ряда |
| Повтор не no-op по сети | URL динамический; идемпотентность по данным, `Imported 0 rows` при отсутствии изменений |
| `mcp_check` зашит на 502 | Инвариант `> 502` + `max(date) > 2021-12-27`, затем точное число после приёмки |

## 6. Протокол живой приёмки (2026-10-20)

- `make import STAT=rosstat_bi_cpi_weekly` — `Imported 398 rows`, `2019-01-09 … 2026-10-12`; семь новогодних перерывов (14–21 день) — единственные пропуски публикаций, остальные интервалы 4–10 дней — сдвиг даты регистрации.
- Повтор — `Imported 398 rows` (новая tmp-выгрузка), `count() FINAL` не изменился: агрегат в `v_rosstat_macro` — **760** точек, `uniqExact(date)=760`, `2009-04-06 … 2026-10-12`; 362 бюллетеня до 2019-01-09 + 398 BI, дублей дат нет.
- Spot: `12.10.2026 = 100.11`, `2019-01-09 = 100.5`, `2022-01-14 = 100.1`.
- Витрина `v_rosstat_macro` — 52 530 строк / 165 рядов (снимок 2026-10-20); каталог `source='rosstat'` — 165 (агрегат общий с бюллетенями, новых строк нет).
- Сырая `rosstat_bi_cpi_weekly` агенту закрыта: отказ ClickHouse 497.
- `make all` — зелёный (golangci-lint 0 issues, `go test -race ./...`, обе сборки); `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `make docs-check` — 43 страницы, 33 теста OK; `dagu validate dagu/rosstat.yaml` — exit 0.
