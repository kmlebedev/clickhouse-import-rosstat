# План: источник пресс-релизов ЦБ по ключевой ставке

Пункт `key-rate/phase-4-cbr-key-rate-press`, issue [#368](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/368).
Спека: [2026-10-23-cbr-key-rate-press-design.md](../specs/2026-10-23-cbr-key-rate-press-design.md).

## Task 1. Спека и план

- [x] `docs/superpowers/specs/2026-10-23-cbr-key-rate-press-design.md`
- [x] `docs/superpowers/plans/2026-10-23-cbr-key-rate-press.md`
- [x] Коммит «docs: spec and plan for cbr_key_rate_press».

## Task 2. Парсер (`cbr/key_rate_press_parse.go`)

- [x] `pressCalendarMeetings(page []byte) ([]pressMeeting, error)` — `div.main-events_day`: дата `div.date` («16 февраля 2024 года»), ссылка `a[href*="/press/pr/"]` в том же блоке, признак «Заседание Совета директоров…». Ноль заседаний — ошибка.
- [x] `pressParseDoc(page []byte, meeting time.Time) (*pressDoc, error)` — `div.landing-text p`; решение, сигнал, блоки, квартальные абзацы, следующее заседание; sentinel `errPressNotPublished`.
- [x] `pressParseQuarter(text string, seq int, meeting time.Time) pressQuarter` — period/form/числа; устойчиво к «после в среднем», «по сравнению с», двум базисам, «II кварталу», «увеличилась … до» для годовой.
- [x] `pressBlockSlug(text string) string` — словарь слагов; `inflation_quarter` — только абзац с «текущий рост цен» и «поправкой на сезонность» (отсекает упоминания без чисел).
- [x] Ошибки: нет контейнера/абзацев; нет абзаца решения; не разобрана ставка; числа не выдумываются (`parsed=0` + warn).

## Task 3. Импортёр (`cbr/key_rate_press.go`)

- [x] Константы: имена трёх таблиц, DDL-литералы, INSERT-ы, запросы гейта/полноты/прошлых решений, `pressHistoryStart = 2024-02-16`, `pressCalendarPath`, `pressSiteURL`.
- [x] `Import()`: DDL → витрины+каталог → календарь → отбор дат → гейт (шапки + неполные) → цикл по выпускам → три батча → проверка записи; сегодняшний неопубликованный релиз — warn.
- [x] `pressToday` (фиксированный UTC+3), `pressDeltas`.
- [x] Витрины `v_cbr_key_rate_press`, `v_cbr_key_rate_press_blocks`, `v_cbr_key_rate_press_quarters`; каталог 5 рядов.
- [x] `cbrKeyRatePressImporter`; регистрация в `cbr/importers.go` (таблица литералом, `countPoints=""`).

## Task 4. Тесты и фикстуры

- [x] `cbr/testdata/key_rate_press/cal_mp.html` + шесть страниц релизов (16.02.2024, 22.03.2024, 26.07.2024, 24.10.2025, 11.09.2026, 23.10.2026).
- [x] `cbr/key_rate_press_parse_test.go`: календарь (56 заседаний 2021–2027, 23 даты ≥ 2024-02-16, ссылки), решение (hold/cut/hike, шаг, effective_from), сигнал, блоки (число и слаги), квартал (все формы + `parsed=0`), два базиса, `после в среднем`.
- [x] `cbr/key_rate_press_test.go`: DDL-пин, `Name()`, дельта, отбор дат, перекачка неполных.
- [x] `go test -race -count=1 ./cbr/` — зелёный.

## Task 5. Интеграция

- [x] `dagu/cbr.yaml` — шаг в хвосте (после `cbr_infl_exp_op`) с комментарием про 13:30.
- [x] `sql/mcp_kimi_reader.sql` — три `GRANT SELECT`.
- [x] `scripts/mcp_check.py` — три витрины непусты, три сырые таблицы закрыты.
- [x] `docs/IMPORTERS.md` — описание источника в строке `cbr`.
- [x] `ARCHITECTURE.md` §6.1, §6.2 (три таблицы), §6.3 (три витрины), §7 (28 шагов), §8 (гейт по дате), список `Name()`.
- [x] `docs/ROADMAP_KEY_RATE.md` — новый пункт фазы 4.

## Task 6. Живая приёмка (2026-10-23)

- [x] `make import STAT=cbr_key_rate_press` — `Imported 23 rows`: **23 шапки, 313 абзацев, 23 квартальные строки**, каталог 5; единственный `parsed=0` — 2024-03-22 (в релизе нет чисел, годовая 7,7 на 18.03).
- [x] Спот-значения: 23.10.2026 — 8,0 / 4,8 / 6,2 / 4,5 / 6,8 на 19.10; 11.09.2026 — 11,6 / 5,3 / 7,0 / 4,6 / 6,3 на 07.09; 16.02.2024 — 6,6 / 11,5 / 7,0 / 10,2 / 7,4 на 12.02; 26.07.2024 — 8,6 / 5,8 / 9,2 / 6,8 / 9,0 на 22.07.
- [x] Решения: 16,0 (hold, шаг NULL) → 18,0 (+200) → 19,0 (+100) → 21,0 (+200) → 20,0 (−100) → 18,0 (−200) → … → 14,25 (−25) → 14,0 (hold, 0) → 14,0 (hold, 0).
- [x] Повтор — `Imported 0 rows`, «новых выпусков нет (релизов в таблице: 23; заседаний в календаре: 23 с 2024-02-16)», без скачиваний.
- [x] `make mcp-user` — три новых `GRANT SELECT` применены.
- [x] `make mcp-check`: три новые проверки **OK** — витрины 23 / 313 / 23 строк, три сырые таблицы закрыты (497). Общий прогон — 1 провал, чужой чек `news_watch` (bootstrap-разметка новых каналов), вне пункта: код новостей не менялся.
- [x] `make all` — зелёный (gofmt, golangci-lint 0 issues, `go vet`, `go test -race ./...`, обе сборки); `dagu validate dagu/cbr.yaml` — exit 0.
- [x] `make docs-check` — 62 страницы, 33 теста; на `main` красный из-за отсутствующего SVG в чужой публикации `docs/publications/2026-10-22-weekly-sa-cpi` (ассет не закоммичен, локально лежит в рабочем дереве владельца); с локально скопированным ассетом — зелёный, наши страницы собираются.
- [x] Протокол — здесь и комментарием PR.

## Task 7. PR

- [x] Один PR: пункт, `Closes #368`, источник, схема, запуск, пример проверки; протокол приёмки — комментарием: [#369](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/369).
- [x] Роадмап и Project V2 — `In Review`; `Done` — после мержа владельцем.
