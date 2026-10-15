# SDD ledger — plan: docs/superpowers/plans/2026-10-13-cbr-mo-br.md

- Spec: `docs/superpowers/specs/2026-10-13-cbr-mo-br-design.md` (commits 3d8a8b5, 52c50d3 — одобрена пользователем)
- Plan: commits 864e664 + ff46259 (fix T5 grep gate)
- Branch: `feat/cbr-mo-br` (from main `a3ed11a`, PR #53 merged)
- BASE (branch start, Task 1): `ff46259`
- Models: implementers — `deepseek/deepseek-flash`; task reviewers — `haiku`; final whole-branch review — `primary`
- Workspace: `.superpowers/sdd/2026-10-13-cbr-mo-br/` (git-ignored)
- Standing caution: untracked owner edit `deploy/staging/dagu.service` — never stage/commit it.

## Pre-flight scan (before Task 1)

| Pair | Producer → Consumer | Finding |
|---|---|---|
| T1 → T3 | fixture sheets «1»,«3»,«16» + фактические числа/споты → ожидания тестов | ok (числа переснимаются из фактической фикстуры) |
| T2 → T4 | `util.HeadFingerprint`/`PublicationKnown`/`SavePublication`/`Publication` → префлайт и запись версии | ok |
| T3 → T4 | `parseMoBr` → батч + каталог + витрина | ok |
| T4 → T5 | имена/расписание/витрина → документы | ok |
| T4 → T6 | префлайт no-op → шаги приёмки 2–3 | ok |
| T5 → T6 | правки ROADMAP §4–5 → статус §6 (разные секции одного файла) | ok |
| T1–T6 (self) | тесты ↔ код внутри задач; fail-fast-конструкции (нет листа / 0 точек / неизвестный лист) уточнены в плане до старта | ok |

Verdict: один конфликт найден и устранён **правкой плана** до диспатча:

- Task 5: Ruling: гейт `grep "source='cbr_survey'" docs/ROADMAP_KEY_RATE.md` → «пусто» неверен — в файле есть легитимная строка `source='cbr_survey'` (мониторинг предприятий, §5, фаза 1.3). Заменён на «ровно одна строка (мониторинг; строка 1.5 обязана стать `cbr_mo_br`» (план, коммит ff46259). Cost if wrong: тривиальная правка доков.

## Progress

- Task 1: dispatched (implementer `deepseek/deepseek-flash`, BASE `ff46259`).
- Task 1: implementer DONE (commit `5349d6b`); fixture verified: 114 series / 2 058 points; reviewer (haiku): Approved, 0 Critical/Important.
- Task 1: complete (commits ff46259..5349d6b, review clean).
- Task 1: minor (deferred): orphan `xl/printerSettings/*.bin` + stale `docProps/app.xml` remain (same as the `mp_survey_data.xlsx` precedent; openpyxl/excelize read fine).
- Task 1: minor (deferred): surgery/count script not archived in the repo (reproducible from the plan precedent).
- Task 1: minor (deferred): report counts 9 stat blocks vs 6 used — wording only.
- Task 2: dispatched (implementer `deepseek/deepseek-flash`, BASE `5349d6b`).
- Task 2: implementer DONE (commit `9bdd1d2`); 12 tests PASS (перезапуск ревьюером); reviewer (haiku): Approved, 0 Critical/Important.
- Task 2: complete (commits 5349d6b..9bdd1d2, review clean).
- Task 2: minor (deferred): `publications_test.go:197` double `server.Close()` (helper уже ставит `t.Cleanup`); идемпотентно, тесты зелёные.
- Task 2: minor (deferred): `Content-Length` только из `resp.Header`, без fallback на `resp.ContentLength` (по спеке допустимо; живой клиент CBR — HTTP/1.1).
- Task 2: minor (deferred): `path.Base(url)` — на ревью T4 сверить согласованность с `name := path.Base(url)` в импортёре.

## T3 pre-flight ruling (правка плана, коммит `d892183`)

Конфликт: план T3 требовал «нет хоть одного из 17 известных листов → ошибка» И тест (а) «DeleteSheet("3") → ошибка», но фикстура содержит лишь 3 листа (1/3/16) — буквальная строгость с ней несовместима (прецедент mp_survey держит в фикстуре все листы реестра; здесь это невозможно).

Ruling: **полнота книги гейтится навигационным листом**. Живой файл ЦБ всегда содержит `Навигация (Navigation) ` (проверено живьём повторно 2026-10-13: 18 листов, sha256 сентябрьский совпал; имя с хвостовым пробелом → матч по подстроке «Навигация»). Обход книги — по `GetSheetList`: навигация — пропуск; неизвестный лист — ошибка всегда; навигация есть (полная книга) → все 17 листов реестра обязательны; навигации нет (фикстура) → сокращённая книга разбирается как есть. Тест (а) переписан: фикстура + `NewSheet("Навигация (Navigation) ")` + `DeleteSheet("3")` → ошибка про отсутствующие листы полной книги. Тесты (б) расширены: «нет шапки» / «нет блоков» / «0 точек» — три отдельные конструкции.

- Task 3: dispatched (implementer `deepseek/deepseek-flash`, BASE `d892183`).
- Task 3: implementer DONE (commit `c2436b0`); 14 тестов MoBr PASS (120 PASS в `-v`), гейты зелёные; reviewer (haiku): Approved, 0 Critical/Important.
- Task 3: complete (commits d892183..c2436b0, review clean).
- Task 3: minor (deferred): спот `gdp_long_term_median=1.775` не в тестах — листа «17» нет в фикстуре; проверить на живой приёмке T6.
- Task 3: minor (deferred): перенос года при пустой ячейке D внутри блока не покрыт фикстурой (логика есть) — подтвердится на живых листах 2,4–15 в T6.
- Task 3: minor (deferred): `wantMoBrIndicators` в тесте — копия реестра; независимая сверка со спекой сделана ревьюером вручную (расхождений нет).
- Task 4: dispatched (implementer `deepseek/deepseek-flash`, BASE `c2436b0`).
- Task 4: implementer DONE (commit `8a97de3`, 7 файлов); гейты зелёные (dagu validate exit 0, registry 14); reviewer — запускается.
- Task 4 ruling: `PublicationKnown`-ошибка в импортёре не глотается `_`, а логируется warn'ом и трактуется как «версия неизвестна» (полный импорт) — SELECT-сбой не роняет ежедневный прогон.
- Task 4: reviewer (haiku): Approved, 0 Critical/Important.
- Task 4: complete (commits c2436b0..8a97de3, review clean).
- Task 4: minor (deferred): `SavePublication` до каталога/витрины — при падении последних no-op следующий прогон их не восстановит (порядок из спеки §3.7; риск принят спекой).
- Task 4: minor (deferred): self-heal-петля, если точка исчезнет из upstream-файла (count FINAL > points) — гипотетический риск из спеки §7.
- Task 5: dispatched (implementer `deepseek/deepseek-flash`, BASE `8a97de3`).
- Task 5: implementer DONE (commit `e92c5e4`, 6 файлов +49/−18); гейты: cbr_mo_br в ARCH+README = 20, cbr_survey в ROADMAP = 1, go build ok; reviewer — запускается.
- Task 5: reviewer (haiku): Approved, 0 Critical/Important.
- Task 5: complete (commits 8a97de3..e92c5e4, review clean).
- Task 5: minor (deferred → T6): `ROADMAP_KEY_RATE.md:108` — статус строки оставлен «фаза 1.5» без ✅ (rilling контроллера: статус 1.5 в §6 и здесь ставит T6 вместе с приёмкой).
- Task 5: minor (deferred → T6): `README.md:122` — «полный путь (≈560 КБ XLSX…)»: 560 КБ — это сумма страница+файл (спека §3.1), формулировка неточна; поправить в T6 одной строкой («~560 КБ: страница + XLSX»).
- Task 6: dispatched (implementer `deepseek/deepseek-flash`, BASE `e92c5e4`).
- Task 6: implementer DONE (commit `583e82d`). Приёмка: `make all` зелёный; 1-й прогон — `Imported 9348 rows of cbr_mo_br` (642 ряда, глубина 2021-05-01…2026-09-01); `source_publications`: 1 строка, fp `4fef4f25716d388e9048149fa296039c`, points=9348; повтор — no-op `…9348 точек на месте`; MCP: витрина 9348/642/2021-05-01/2026-09-01, каталог 642, споты 14.5/6.6/8.0; `make mcp-check` ПРОВЕРКА ПРОЙДЕНА; `dagu validate` exit 0; §6 статус 1.5 ✅ + блок приёмки; §5:108 ✅; README fix. Reviewer — запускается.
- Task 6: reviewer (haiku): Approved, 0 Critical/Important; живая приёмка перепроверена независимо (MCP-запросы, no-op, админ-SELECT, mcp-check rc=0).
- Task 6: complete (commits e92c5e4..583e82d, review clean).
- Task 6: minor (deferred): в отчёте имплементера неточные ссылки на строки ROADMAP и стат диффа — только текст отчёта, на доки не влияет.
- Task 6: minor (deferred): косметика лога — `Get xlsx file ` с пустым Path (`util/xls.go:163`) и двойной `Imported … rows`; вне объёма шага 1.5.

## Финальное ревью ветки (primary, code-reviewer.md) — 2026-10-13

- Вердикт: **With fixes** — 1 Important + рекомендации. Независимо воспроизведены: HEAD-отпечаток `4fef4f25…` из живых заголовков (curl + md5), sha256 файла, 9348/642/43 волны, двусторонность каталога (LEFT JOIN = 0), закрытость `macro_series`/`source_publications` под kimi_reader, `gdp_long_term_median` = 1.775; построчный разбор живого full.xlsx подтвердил парсер на всех 17 листах (числа спеки «CDS 8 / Urals 21 / Brent 31 волн» — длины сеток листов, не пропуск данных).
- Important: каталог/витрина несамовосстанавливались после `Send` (SavePublication раньше них → сбой пост-шагов маскировался no-op). Решение: SavePublication последним.
- Fix `4cd4338` (agent-73 resume): SavePublication после UpsertSeriesCatalog+CreateView; спека §3.4/§3.7, README:122, ARCHITECTURE :179/:245/:953 синхронизированы; mcp_check.py — гард «source_publications закрыта»; mp_survey.go:21 — `'cbr_mo_br'` в списке source; `make all` и `make mcp-check` зелёные (новый гард проходит, ACCESS_DENIED 497).
- Scoped re-review фикса (haiku): Approved, 0 Critical/Important/Minor.
- Не взятые рекомендации (отложено, при желании — отдельным пунктом): юнит-тест Import-ветвей через fake conn + var URL; гард-комментарий об асимметрии ошибок count-запроса; косметика лога; перенос остальных импортёров на source_publications.
- План `docs/superpowers/plans/2026-10-13-cbr-mo-br.md` намеренно не правился после исполнения (исторический артефакт; порядок T4 Step 3 читать вместе с фиксом `4cd4338`).
- **PR #54 открыт**: https://github.com/kmlebedev/clickhouse-import-rosstat/pull/54 (push `feat/cbr-mo-br` @ `4cd4338`, base main; владелец ревьюит и решает merge).
