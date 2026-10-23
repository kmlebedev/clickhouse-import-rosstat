# План: источник пресс-релизов ЦБ по ключевой ставке

Пункт `key-rate/phase-4-cbr-key-rate-press`, issue [#368](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/368).
Спека: [2026-10-23-cbr-key-rate-press-design.md](../specs/2026-10-23-cbr-key-rate-press-design.md).

## Task 1. Спека и план

- [x] `docs/superpowers/specs/2026-10-23-cbr-key-rate-press-design.md`
- [x] `docs/superpowers/plans/2026-10-23-cbr-key-rate-press.md`
- [x] Коммит «docs: spec and plan for cbr_key_rate_press».

## Task 2. Парсер (`cbr/key_rate_press_parse.go`)

- [ ] `pressCalendarDates(page []byte) ([]pressMeeting, error)` — `div.main-events_day`: дата `div.date` («16 февраля 2024 года»), ссылка `a[href*="/press/pr/"]` в том же блоке, признак «Заседание Совета директоров…». Ноль заседаний — ошибка.
- [ ] `pressParseDoc(page []byte, meeting time.Time) (*pressDoc, error)` — `div.landing-text p`; решение, сигнал, блоки, квартальные абзацы, следующее заседание.
- [ ] `pressQuarter(text string, meeting time.Time) (pressQuarter, bool)` — period/form/числа; устойчиво к «после в среднем», «по сравнению с», двум базисам, «II кварталу».
- [ ] `pressBlockSlug(text string) string` — словарь слагов.
- [ ] Ошибки: нет контейнера/абзацев; нет абзаца решения; не разобрана ставка; `lo > hi`-подобных проверок нет — числа не выдумываются (`parsed=0` + warn).

## Task 3. Импортёр (`cbr/key_rate_press.go`)

- [ ] Константы: имена трёх таблиц, DDL-литералы, INSERT-ы, запросы гейта/полноты/прошлых решений, `pressHistoryStart = 2024-02-16`, `pressCalendarUrl`, `pressSiteUrl`.
- [ ] `Import()`: DDL → витрины+каталог → календарь → отбор дат → гейт (шапки + неполные) → цикл по выпускам → три батча → проверка записи.
- [ ] `pressDay`, `pressTodayMSK` (фиксированный UTC+3), дельта ставки.
- [ ] Витрины `v_cbr_key_rate_press`, `v_cbr_key_rate_press_blocks`, `v_cbr_key_rate_press_quarters`; каталог 5 рядов.
- [ ] `cbrKeyRatePressImporter`; регистрация в `cbr/importers.go` (таблица литералом, `countPoints=""`).

## Task 4. Тесты и фикстуры

- [ ] `cbr/testdata/key_rate_press/cal_mp.html` + шесть страниц релизов (16.02.2024, 22.03.2024, 26.07.2024, 25.10.2025, 11.09.2026, 23.10.2026).
- [ ] `cbr/key_rate_press_parse_test.go`: календарь (23 даты ≥ 2024-02-16, ссылки), решение (hold/cut/hike, шаг, effective_from), сигнал, блоки (число и слаги квартала/решения), квартал (все шесть форм + `parsed=0`), новые формулировки → `parsed=0`.
- [ ] `cbr/key_rate_press_test.go`: DDL-пин, `Name()`, дельта, отбор дат, no-op-сообщение.
- [ ] `go test -race -count=1 ./cbr/` — зелёный.

## Task 5. Интеграция

- [ ] `dagu/cbr.yaml` — шаг в хвосте (после `cbr_infl_exp_op`) с комментарием про 13:30.
- [ ] `sql/mcp_kimi_reader.sql` — три `GRANT SELECT`.
- [ ] `scripts/mcp_check.py` — три витрины непусты, три сырые таблицы закрыты; сообщение «прогнать `make import STAT=cbr_key_rate_press`».
- [ ] `docs/IMPORTERS.md` — описание источника в строке `cbr`.
- [ ] `ARCHITECTURE.md` §6.1, §6.2 (три таблицы), §6.3 (три витрины), §7 (число шагов цепочки), §8 (гейт по дате, префлайта нет).
- [ ] `docs/ROADMAP_KEY_RATE.md` — новый пункт фазы 4 рядом с `phase-4-post-meeting-quarter-block`.
- [ ] `make all`, `make docs-check`, `dagu validate dagu/cbr.yaml` — зелёные.

## Task 6. Живая приёмка

- [ ] `make import STAT=cbr_key_rate_press ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` — 23 шапки, блоки, 23 квартальные строки; спот-значения (23.10.2026: 8,0/4,8/6,2/4,5/6,8 на 19.10; 11.09.2026: 11,6/5,3/7,0/4,6; 16.02.2024: 6,6/11,5/7,0; 22.03.2024 — `parsed=0`).
- [ ] Повтор — `Imported 0 rows`, no-op-сообщение, без скачиваний.
- [ ] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (три новые проверки).
- [ ] Протокол — в план и комментарий PR.

## Task 7. PR

- [ ] Один PR: пункт, `Closes #368`, источник, схема, запуск, пример проверки; протокол приёмки — комментарием.
- [ ] Роадмап и Project V2 — `In Review`; `Done` — после мержа владельцем.
