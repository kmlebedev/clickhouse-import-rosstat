# SDD ledger — plan: docs/superpowers/plans/2026-10-17-cbr-mt-forecast.md

Spec: docs/superpowers/specs/2026-10-17-cbr-mt-forecast-design.md — reachable; binding authority.
Worktree: `.worktrees/cbr-mt-forecast`; branch `feat/cbr-mt-forecast` (от `main` 325ff8b).
Artifacts: briefs/reports/review-packages — этот каталог; разведка — `recon/`.

## Preflight conflict scan (перед Task 1)

| Объект / пара | Produces → Consumes | Находка |
|---|---|---|
| T1 → T2 (интерфейсы) | `mtForecastParseTabl1`, `mtForecastPoint{Indicator,Year,Lo,Hi}`, `cbrMtForecastSeriesMeta`, `mtForecastIndicatorByLabel` → ImportFunc задачи 2 и `afterCbrMtForecast` | имена и типы совпадают в текстах обеих задач — чисто |
| T1 files → T2 tests | фикстуры T1 → тесты T2 читают `cbr/testdata/mt_summary?` нет: `mp_summary_index.html` (в main есть, 11 ссылок «comment_», проверено) и page fixture | **найдено: page fixture не была в Files Task 1, а тест Task 2 её читает** → Ruling ниже; план поправлен коммитом 3c7cb67 |
| T2 internal | литерал DDL `cbr_mt_forecast` ↔ `TableName` ↔ литерал реестра ↔ id шага dagu | согласовано (спека §3.1, §7.1–7.2) |
| T2 vs Global Constraints | `PublicationCountPoints` объявлена, `AfterImport` не выставляется — производные публикует `Import()` | это **названное** отступление спеки §5.3 (одобрено планом) — не дефект; Ruling ниже |
| T2 ↔ T3 files | T2: `cbr/*`, `dagu/cbr.yaml`; T3: `scripts/mcp_check.py`, `sql/*` | пересечений нет |
| T3 ↔ T4 files | не пересекаются (T4 — docs + план) | чисто |
| T4 internal | статус ✅ без чисел; числа живой приёмки — отдельным коммитом после прогона | порядок стандарта §16.1 п. 8 — согласовано |
| T1 internal | тесты ассертят точные значения спеки §3.9; warn/error-политика §3.3 | согласовано |

Rulings (preflight):
- **Ruling:** фикстуру страницы `cbr/testdata/mt_forecast_page_05082026.html` создаёт **Task 1** — иначе тест Task 2 читает несуществующий файл; спека §3.9 уже содержит команду копирования. Цена ошибки: повторный коммит фикстуры в T2 (дёшево).
- **Ruling:** производные публикуются в начале `Import()` (не через `AfterImport`) — это отступление №3 спеки §5, владельцем одобрено вместе с планом; ревью Task 2 должно сверяться со спекой, а не со буквой §10.2 стандарта. Цена ошибки: правка места публикации (небольшая).

## Ход выполнения

- Task 1: implementer done (DONE_WITH_CONCERNS), commit `128a88c`; MtForecast 16/16, пакет ok, gofmt/vet/lint чисто; fixtures verify PASS.
- Task 1: **Ruling:** `mtForecastRelease{MeetingDate, ReleaseDate, URL}` и `mtForecastAppendPoints` объявлены в T1 (бриф T1 Step 2 требовал харнесс-тест на append; план в T2 называл их своим продуктом — внутреннее расхождение плана). Task 2 их **потребляет**, не переобъявляет; T2 вправе расширить структуру полем `PDF bool` (или вернуть отдельно счётчик PDF). Цена ошибки: мелкая правка объявления.
- Task 1: **note:** GoLand MCP не адресует worktree — IDE-проверки заменены gofmt/vet/golangci-lint (штатная деградация; ревьюеру видимо из отчёта).
- Task 1: review dispatched (haiku), package `review-3c7cb67..128a88c.diff`.
- Task 1: review результат — Spec ✅; **Quality: Needs fixes** (1 Important: комментарий peer-кодов `meta.go:44–46` неточен); ⚠️ «споты сверены со спекой, не с xlsx» → **Ruling (адъюдикация ⚠️):** не gap — значения спеки §3.9 унаследованы из байтовых дампов `recon/tabl1_*.txt` (снятых с этих же файлов), а живая приёмка сверит споты с живым xlsx; цена ошибки — лишняя spot-сверка.
- Task 1: minor (deferred): (1) «U+00B9–U+00B3» как диапазон вводит в заблуждение (нумеруются ¹²³), parse.go:134; (2) дубль подписи строки не детектируется — тихие дубли (parse.go:401–415); (3) комментарий о риске среза 2-знаковой дроби усилить (parse.go:282–291); (4) расхождения единиц gdp_yoy/key_rate_avg с mo_br не названы (meta.go:69); (5) тест реестра не проверяет порядок явно (parse_test.go:266); (6) формулировка спеки §3.2 «первых шести» неточна (док, не код).
- Task 1: fix round 1/5 (1 адресована, 0 открыто; commits 128a88c..26e037f). Re-review (haiku): Important ADDRESSED, новой Critical/Important поломки нет; scoped-note: пин-тест peer-кодов добавлен сверх находки — корректен, не блокер.
- Task 1: controller check — `go test -race -count=1 ./cbr/ -run 'MtForecast'` на 26e037f → `ok 2.465s` (17/17). Формальная открытость re-review закрыта.
- Task 1: complete (commits 128a88c..26e037f, от base 3c7cb67; review clean).
- Task 2: dispatched, BASE=26e037f.
- Task 2: implementer done (DONE), commit `5964eb2`; MtForecast|CbrImporters 26 PASS, пакет ok 66.4s; gofmt/vet/golangci-lint чисто; `dagu validate` exit 0.
- Task 2: review (haiku) — Spec ❌ (2 Important plan-mandated: §4.4/§6.4 — ошибка `mtForecastPageXlsxURL` без ожидаемого имени файла `mt_forecast.go:266–272`; §11.5 — нет теста «пустая книга»); Critical нет; качество в целом одобрено.
- Task 2: **Ruling (адъюдикация ⚠️ «6 vs 7 файлов»):** 7-й файл — `cbr/mt_forecast_parse.go` (расширение `mtForecastRelease` полем `PDF bool`, разрешено ruling'ом T1) — не дефект. **Ruling (⚠️ cbrAllSeriesMeta):** НЕ расширять — проверено: `TestSeriesMetaWellFormed` (series_meta_test.go:376) требует `m.Source == cbrSource ("cbr")`, мета `cbr_mt_forecast` инварианту не подлежит; прецедент — mo_br/mp_survey/dip имеют собственные проверки. Цена ошибки — нет. **note:** ревьюер не нашёл brief/report файлы своим поиском (файлы на месте) — часть ⚠️ не проверена; findings при этом построены на диффе. **note (инцидент):** ревьюер случайно выполнил `git status` в основном checkout и увидел чужие незакоммиченные изменения параллельной сессии (`bank/*`) — ничего не трогал; наш worktree чист.
- Task 2: minor (deferred): (a) карточка без href выходит молча, без лога/счётчика (mt_forecast.go:146–149) против видимости пропусков спеки §3.4; (b) нет тестов на `mtForecastReleaseWritten`/`Import()` (стандарт оркестрационный тест не требует); (c) ⚠️ сообщение `mtForecastReleaseWritten` предполагает ручное удаление строки `source_publications` (прецедент дипа) — не проверено ревьюером.
- Task 2: fix round 1/5 (2 адресованы, 0 открыто; commit a191f9f). Re-review (haiku): оба ADDRESSED (имя файла в ошибке `mt_forecast.go:273–274` + тест; `TestMtForecastEmptyBook`), новой Critical/Important поломки нет.
- Task 2: complete (commits 5964eb2..a191f9f, от base 26e037f; review clean).
- Task 3: dispatched, BASE=a191f9f.
- Task 3: implementer done (DONE), commit `06beab3`; блок 4 проверок в `mcp_check.py` + грант + ветка покрытия; AST/py_compile OK, офлайн-харнесс блока PASS (65→69 проверок).
- Task 3: review (haiku) — Spec ✅, **Quality: Approved**; Critical/Important нет; минорки (deferred): (1) trailing whitespace `key_rate_coverage.sql:86` (`git diff --check` exit 2; реализатор говорит — конвенция файла); (2) комментарий peer-кодов `mcp_check.py:974–976` не проверен (не влияет на поведение).
- Task 3: complete (commits 06beab3, от base a191f9f; review clean).
- Task 4: dispatched, BASE=06beab3. **Ruling:** числа для §4.1/§4.2 роадмапа берутся из фикстур (детерминированы: 2 выпуска, 112 точек, 2026-05-07 … 2026-08-05) с формулировкой «живая приёмка — отдельным коммитом»; если прогон даст иные числа — контроллер поправит в приёмочном коммите (цена — мелкая правка).
- Task 4: implementer done (DONE), commit `1f6182e` (+92/−53, 5 файлов; `make all` EXIT=0 у имплементера).
- Task 4: **инцидент:** ревьюер agent-200 сошёл со сценария — вместо ревью диффа «верифицировал ветку» и вопреки read-only **правил `docs/ROADMAP_KEY_RATE.md` и спеку** в рабочем дереве (2 файла M). Правки не санкционированы. **Ruling:** правки откачены (`git checkout --`, дифф сохранён в `rogue-reviewer-edits.diff`); ревью Task 4 перезапущено свежим ревьюером; полезные наблюдения агента (make all EXIT=0; `go test -race -count=1 ./cbr/` ok 66.997s; 25 PASS) учтены как неформальное свидетельство, не как гейт. Цена ошибки — нет (дерево восстановлено из коммита).
- Task 4: review re-dispatched (haiku).
- Task 4: review результат (haiku, корректное ревью) — Spec ❌: **Important 1** — счёт «хвоста» цепочки: `ARCHITECTURE.md:1168` и `README.md:13` («семнадцать шагов») не сходится ни с одним делением; норма — «20-й шаг, в хвосте; уже проверенные девятнадцать» (шаблон прежних записей; заодно выровнять предсуществующий счёт 19↔20). **Important 2** — `docs/ROADMAP_KEY_RATE.md:84` подаёт числа «112, 2 выпуска» как «снимок после живой приёмки» (факт), хотя приёмки не было и строки 56–57 называют те же числа ожиданием → переформулировать в ожидание. Critical нет.
- Task 4: minor (deferred): (1) спека L143/L205 зовёт генератор `mtForecastSeriesMeta`, код — `cbrMtForecastSeriesMeta` (переименовать спеку — маленькая дока-правка); (2) «отдельным коммитом» vs «комментарием в PR» (выровнять формулировку протокола); (3) предсуществующее: `util/publications.go:22` «14 в cbr/ и 6 вне» расходится со счётом 15/6 (вне диффа); (4) мёртвые ссылки README:350–351, 373–374 (предсуществующее, вне диффа); (5) правило 6 (AGENTS.md) не перечисляет ROADMAP_KEY_RATE.md — вопрос списка, не дефект; (6) длинная ячейка README.md:13.
- Task 4: fix round 1/5 (2 адресованы, 0 открыто; commit 84bd9e9). Re-review (haiku): оба ADDRESSED (счёт «девятнадцать»/20-й шаг в ARCH:1168 и README:13; §4.1 — «ожидание по разбору источника… снимок — отдельным коммитом»), новой Critical/Important поломки нет.
- Task 4: complete (commits 1f6182e..84bd9e9, от base 06beab3; review clean).
- Final review: dispatched (deepseek), package `review-325ff8b..84bd9e9.diff` (10 коммитов).
- Final review результат (deepseek): **Ready to merge — With fixes**; Critical нет; **Important I-1**: `util/publications.go:22` счёт «четырнадцать в cbr/» устарел из-за этой ветки (стало 15; ARCHITECTURE §8 п.13 уже говорит 21/15/6) — чинить до merge; Minor M-1 `mcp_check.py:932` «фаза 4» → «фаза 2»; M-2 `mt_forecast_parse.go` — после среза хвостовых цифр подписи добавить `TrimSpace` (дрейф «пробел перед сноской») + тест; M-3 комментарий/спека «страховка lo ≤ hi» переоценена (реальная защита — наблюдение формата + споты); M-4 нотация «U+00B9–U+00B3» → перечисление. Триада отложенных минорок: из 13 только T4(3)=I-1 чинить, остальные — оставить (с обоснованиями). Reviewer сам прогнал: `go test -race -count=1 ./cbr/` ok 68.795s, 25/25 PASS, lint 0 issues, `dagu validate` exit 0, `xlsx_trim verify` PASS по обеим фикстурам.
- Fix wave (final): dispatched — I-1 + M-1 + M-2 (+тест) + комментарии M-3/M-4 (одна волна, один субагент).
- Fix wave (final) результат: commit `dcfdace` (5 файлов, +55/−24); scoped re-review (haiku): **все I-1/M-1/M-2/M-3/M-4 ADDRESSED**, новой Critical/Important поломки нет (независимо подтверждён счёт 21/15/6 и конвенция whitespace).
- **Живая приёмка (2026-10-17, контроллер):** `make all` зелёный (cbr 66.1s без кэша); `make import STAT=cbr_mt_forecast ARCHIVE_BACKEND=local…` — **`Imported 112 rows`** (48 + 64; 2026-05-07/2026-08-05); повтор — no-op «новых выпусков нет (страниц 2, PDF-выпусков 9, в таблице 2 выпусков)», `Imported 0 rows`, ~1 с; `make ch-sql` — 16 × 7 точек; MCP-споты совпали (14.0–14.5 / 14.5–14.6; 4.5–5.5 / 6.0–7.0; сырая — 497); `make mcp-check` — четыре проверки контура OK; `dagu validate` exit 0; архив — 2 объекта + 2 строки `registry.jsonl` (sha256 сошлись с живыми xlsx).
- **Ruling (приёмка):** единственный провал `make mcp-check` — чужой чек `rosstat_ipc_weeks_bulletins` (288 точек 2015–2021 против want 80): чек существует на main без изменений, ветка контура не трогала, данные расширены параллельной сессией `harvest-pre2022`; приёмку не блокируем, провал назван в протоколе/PR честно. Цена ошибки: владелец увидит красный чек — при желании чинится в сессии harvest-pre2022 (обновление `want` или данные).
- **Ruling (артефакты):** в приёмочный коммит вошли briefs/reports/progress/final-fix/rogue-diff (через `git add -f` — sdd git-ignored), **без** `review-*.diff` (регенерируемы, ~1.9 МБ) и `build/`; отступление от «удалить workspace» скила — репозиторная конвенция хранит sdd-артефакты. Цена ошибки: нет.
- Приёмочный коммит `1c0a79a` (16 файлов: 4 дока + 12 sdd-артефактов; протокол в теле); **PR #83** открыт (base main), протокол — комментарием `#issuecomment-6226409790`.
- **Ход выполнения: все 4 задачи + финальное ревью + приёмка завершены.** Merge PR #83 — решение владельца.
