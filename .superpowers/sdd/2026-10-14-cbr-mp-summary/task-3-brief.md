### Task 3: Парсер индекса и страницы резюме

**Files:**
- Create: `cbr/mp_summary_parse.go`
- Test: `cbr/mp_summary_parse_test.go`

**Interfaces:**
- Consumes: `*goquery` (уже в go.mod), нормализация — локальная функция `normMPText(string) string`.
- Produces (используют Task 4–6):
  - `type mpSummaryRef struct { Published time.Time; Href string }`
  - `func parseMPDecIndex(page []byte) ([]mpSummaryRef, error)` — только карточки с названием «Резюме обсуждения ключевой ставки» и ссылкой, содержащей `summary_key_rate_`; дата — из `document-regular_date`; 0 совпадений → ошибка;
  - `type mpSummaryBlock struct { Block, Subsection, Kind string; Seq int; Text string }`
  - `type mpSummaryDoc struct { MeetingDate time.Time; Action string; Rate float64; EffectiveFrom *time.Time; Signal string; Blocks []mpSummaryBlock }`
  - `func parseMPSummary(page []byte) (mpSummaryDoc, error)` — именно пять `h2`-блоков обязательны (`"Ситуация в экономике и инфляция"`, `"Денежно-кредитные условия"`, `"Внешние условия"`, `"Инфляционные риски"`, `"Выводы для денежно-кредитной политики и решение по ключевой ставке"`); `Subsection` — из `div.title_20` («Основные факты»/«Дискуссия») до следующего `h2`; `Kind` — `paragraph` (`p`) или `bullet` (`li`, с сохранением порядка); `Seq` — сквозной по блоку;
  - регексы решения (после `normMPText`): дата заседания «Совет директоров Банка России \<D\> \<месяц\> \<YYYY\> года (сохранил|повысил|снизил)», фолбэк — «данных, доступных к \<D\> \<месяц\> \<YYYY\> года»; действие `hold|hike|cut` по глаголу; ставка — `(\d+),(\d+)\s*%\s*годовых`; `EffectiveFrom` — «с \<D\> \<месяц\> \<YYYY\> года» после ставки (только для cut/hike; отсутствие — допустимо, NULL); `Signal` — предложения блока 5, содержащие подстроку «сигнал» (регистронезависимо; соединение `\n`);
  - fail-fast: нет любого из пяти блоков / не распознано решение / 0 абзацев → ошибка с указанием места.

- [ ] **Step 1: Написать падающие тесты** (`cbr/mp_summary_parse_test.go`, `openFixture`-хелпер как в `cbr/mp_survey_parse_test.go`):
  - `TestNormMPText`: NBSP (`\u00a0`), узкий NBSP (`\u202f`) и неразрывный дефис (`\u2011`) нормализуются, двойные пробелы схлопываются (пины на парах символов, фактически присутствующих в фикстурах — снять в Task 2);
  - `TestParseMPDecIndexFixture`: 22 ссылки; первая (в порядке страницы) — `2024-02-27`/`…summary_key_rate_27022024/`, последняя — `2026-09-23`/`…summary_key_rate_23092026/`; все `Href` содержат `summary_key_rate_`;
  - `TestParseMPSummaryHold` (23092026): `MeetingDate` = 2026-09-11; `Action` = `hold`; `Rate` = 14.0; `EffectiveFrom` = nil; `Signal` содержит «не давать сигнала»; ровно 5 блоков с ожидаемыми именами; `Подзаголовки`: ровно 6 пар «Основные факты»/«Дискуссия» (по 3 в трёх блоках); числа `paragraph`/`bullet` по блокам — фактические из Task 2 (пины);
  - `TestParseMPSummaryHike` (06112024): `MeetingDate` = 2024-10-25; `Action` = `hike`; `Rate` = 21.0; `EffectiveFrom` = 2024-10-28; `Signal` содержит «направленный сигнал»;
  - `TestParseMPSummaryCut` (07052026): `Action` = `cut`; `Rate` = 14.5; `MeetingDate`/`EffectiveFrom` — фактические из Task 2;
  - `TestParseMPFailFast`: подмена в фикстуре строки (в памяти, `strings.Replace` над содержимым файла) — переименованный `h2` (одного блока нет) → ошибка; «решение» вычищено → ошибка; индекс с нулём карточек резюме (пустая html-строка) → ошибка.

- [ ] **Step 2: Прогнать — FAIL**: `go test ./cbr/ -run 'MPDec|MPSummary|MPFailFast' -v` → компиляция падает.

- [ ] **Step 3: Реализовать `cbr/mp_summary_parse.go`** — обход goquery в линейном порядке узлов (`doc.Find("h1, h2, div.title_20, p, li")` с фильтром по позиции либо ручной обход через `Each`); нормализация `normMPText` (NBSP `\u00a0`, узкий NBSP `\u202f`, неразрывный дефис `\u2011` → `-`, схлопывание пробелов); месяцы — карта русских названий в `time.Month`; ошибки «резюме: …», «индекс: …».

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run 'MPDec|MPSummary|MPFailFast' -v && go test ./cbr/ -count=1 && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary_parse.go cbr/mp_summary_parse_test.go && git commit -m "cbr: mp_dec index and summary page parser"`.

---

