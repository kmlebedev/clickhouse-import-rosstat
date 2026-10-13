# cbr_mp_summary (резюме обсуждения ЦБ) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Импортёр `cbr_mp_summary` — все 22 «Резюме обсуждения ключевой ставки» ЦБ (даты, решение, сигнал, дословный текст 5 блоков) + двухслойная разметка по 11 каналам (регексы + LLM с указанием модели) в ClickHouse с тремя витринами; плюс автоматизация календаря заседаний `cal_mp` → `events_calendar`.

**Architecture:** Дельта-прогон по индексу `mp_dec/decision_key_rate/` (обычный день — только индекс, новых вставок ноль); сбор документов, отсутствующих в БД, в батчи по таблицам; LLM-слой — вторым проходом для документов без строк текущей пары (model, prompt_version), с дословной валидацией цитат и деградацией до регексов. Календарь — расширение существующего импортёра `events_calendar` (канонические строки заседаний, ручные строки-триггеры не трогаются).

**Tech Stack:** Go, goquery (уже в go.mod), `util.GetFile`/новый `util.PostJSON`, excelize не нужен, logrus; ClickHouse ReplacingMergeTree; dagu.

**Spec:** `docs/superpowers/specs/2026-10-14-cbr-mp-summary-design.md` (читать вместе с планом: спека — связывающая власть).

## Предусловие и база

- **Порядок мержей:** ветка `feat/publication-preflight` (другая сессия) вливается в `main` **до старта этой ветки** — она трогает `cbr/importers.go`, `cbr/importers_test.go`, `cbr/mo_br.go`, `util/clickhouse_import.go`, `util/publications.go` и те же доки (ARCHITECTURE/README/ROADMAP), иначе конфликты гарантированы. Ветка 1.2 создаётся от **актуального main** после её мержа (имя ветки — `feat/cbr-mp-summary`).
- **При старте:** `git fetch && git log --oneline -3 main` — сверить; посмотреть, какие поля появились у `util.ClickHouseImport` (в preflight-ветке — публикация каталога/витрины через `AfterImport`). Наш импортёр **остаётся с кастомным `Import()`**: мультидокументная дельта по индексу не вписывается в single-artifact фреймворк (спека §3.1), и `source_publications`/HEAD к нему не применяются. Если preflight-ветка ввела реестр «деклараций» о префлайте по источникам — добавить туда строку «`cbr_mp_summary`: дельта по индексу, без HEAD-префлайта» со ссылкой на спеку.
- **ЗАПРЕТ до подтверждения плана:** не создавать ветку и не коммитить; первые коммиты ветки — спека и этот план (после подтверждения).

## Global Constraints

- Вставки только батчами `PrepareBatch → Append(err!) → Send`; **батч принадлежит одной таблице** — у нас три батча на слой (шапка / блоки / каналы), плюс четвёртый для LLM-строк каналов.
- HTTP — только через `util.*` (`util.GetFile` для страниц, новый `util.PostJSON` для LLM); сетевых вызовов в `init()` нет; логирование — logrus.
- Секреты только `os.Getenv` (`LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL`); значения ключа не печатать ни в логах, ни в ошибках (ключ передаётся заголовком `Authorization`, не в URL).
- DDL трёх таблиц — литералы (фиксированные имена, как у `moex/keyrate.go`); канонические копии — ARCHITECTURE §6.2 и код, обязаны совпадать.
- Даты: `meeting_date` — дата заседания (UTC, `time.Date(y,m,d,0,0,0,0,time.UTC)`); `published` — дата публикации из индекса; в слаге URL — дата **публикации**.
- Тексты хранятся нормализованными (NBSP → пробел, неразрывный дефис → `-`, `ё` не трогаем) и дословными; LLM-цитаты валидируются на вхождение в нормализованный текст.
- Реестр `cbrImporters`: 15-я запись; тест `TestCbrImportersPrefixedAndRegistered` держит инварианты (имя == таблица для нашей записи — так и есть).
- Новых зависимостей нет; `deploy/staging/dagu.service` — правка владельца, не трогать; каждый таск заканчивается gofmt-чистым кодом и зелёными `go build ./...`, `go test ./cbr/... ./calendar/... ./util/...`.

## Review Focus

1. **NBSP/неразрывные дефисы и «ё» в живом тексте** — нормализуются до сопоставлений; LLM-валидация не отбрасывает валидную цитату из-за пробела-двойника. Тест: Task 3.
2. **Формулировки решения чужих эпох** (cut: «снизил … до»; hike с «с DD месяца YYYY года»; hold без даты вступления) — не падают и не пишут NULL туда, где дата есть. Тесты: Task 3 (три фикстуры).
3. **Лист без части блоков / пустые списки** — fail-fast с именем места, не тихая запись. Тест: Task 3.
4. **LLM: обрезанный JSON / HTML-ошибка / галлюцинированная цитата** — цитата отбрасывается, документ остаётся со слоем 1, конвейер не падает. Тесты: Task 5.
5. **`cal_mp`: карточки-не-заседания (пресс-конференции) и даты без года** — в результат не попадают и не срывают разбор. Тест: Task 7.

---

### Task 1: `util.PostJSON` — POST JSON для LLM-вызовов

**Files:**
- Modify: `util/json.go` (рядом с `GetJSON`)
- Test: `util/json_test.go`

**Interfaces:**
- Produces (использует Task 5):
  - `var postJSONTimeout = 65 * time.Second` — переменная (не константа), тест подставляет короткую;
  - `func PostJSON(url string, headers map[string]string, request any, out any) error` — тело запроса сериализуется как JSON; HTTP — общий `HttpClient`; не-2xx — ошибка с кодом и началом тела (обрезка как `maxErrorSnippet`); пустое тело — ошибка; дедлайн `postJSONTimeout`; ключ — только в заголовке (в ошибках `url.Error` URL без секретов).

- [ ] **Step 1: Написать падающие тесты** (`httptest`): успех (200, `{...}` → структура); не-200 (500 с телом → ошибка содержит код); не-JSON тело (200, HTML → ошибка разбора); таймаут (`postJSONTimeout` = 50 ms, сервер спит → ошибка); заголовок `Authorization: Bearer test` доезжает до сервера (assert в обработчике).

- [ ] **Step 2: Прогнать — FAIL**: `go test ./util/ -run TestPostJSON -v` → `undefined: PostJSON`.

- [ ] **Step 3: Реализовать `PostJSON`** по образцу `GetJSON` (та же структура ошибок; `http.NewRequest(http.MethodPost, …)`, `bytes.NewReader(jsonBody)`, `Content-Type: application/json`; `defer resp.Body.Close()`; ошибки оборачивать без URL, когда он может нести секреты — ключ у нас в заголовке, но конвенцию сохранить).

- [ ] **Step 4: Прогнать — PASS**: `go test ./util/ -count=1 && gofmt -l util/`.

- [ ] **Step 5: Commit**: `git add util/json.go util/json_test.go && git commit -m "util: PostJSON helper for LLM calls (deadline, status check)"`.

---

### Task 2: Фикстуры (полные страницы) и снятие фактических чисел

**Files:**
- Create: `cbr/testdata/mp_summary_index.html`, `cbr/testdata/mp_summary_23092026.html`, `cbr/testdata/mp_summary_06112024.html`, `cbr/testdata/mp_summary_07052026.html`
- Create: `calendar/testdata/cal_mp.html`
- Test: нет (артефакты; числа фиксируются в отчёте)

**Interfaces:**
- Produces: пять файлов-фикстур; в отчёте — sha256 и фактические числа: по каждому резюме (число абзацев/пунктов по блокам, споты решения), по календарю (полный список дат заседаний из карточек).

- [ ] **Step 1: Скачать живьём** (curl с `russian_trusted_root_ca_pem.crt`, User-Agent как в Task T1 прецедента mo_br): индекс `https://www.cbr.ru/dkp/mp_dec/decision_key_rate/`; резюме `…/summary_key_rate_23092026/`, `…/06112024/`, `…/07052026/`; календарь `https://www.cbr.ru/dkp/cal_mp/` в `calendar/testdata/cal_mp.html`. **Структуру не резать** — страницы кладутся как есть (довод: хирургия HTML рискованнее искажения разбора; тысячи байт допустимы).

- [ ] **Step 2: Снять и записать фактические числа** (python3-скрипт по тексту — в отчёт, не в репозиторий): по каждому резюме — точные заголовки 5 блоков, пары «Основные факты/Дискуссия», число `p` и `li` по блокам, дата заседания, действие/ставка/дата вступления/предложения сигнала, 2–3 предложения-кандидата для пинов каналов (например, «инфляционные ожидания» для `expectations`, «безработиц» для `labor_market`); по `cal_mp` — полный список дат заседаний из карточек (ожидание: 2026-10-23, 2026-12-18 + восемь дат 2027: 02-12, 03-19, 04-30, 06-11, 07-30, 09-10, 10-22, 12-17; фактический список зафиксировать).

- [ ] **Step 3: sha256 фикстур** — зафиксировать в отчёте (для будущего детекта подмены).

- [ ] **Step 4: Commit**: `git add cbr/testdata/mp_summary_*.html calendar/testdata/cal_mp.html && git commit -m "cbr: mp summary fixtures (index, hold/hike/cut pages, cal_mp)"`.

---

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

### Task 4: Реестр 11 каналов и regex-разметка

**Files:**
- Create: `cbr/mp_summary_channels.go`
- Test: `cbr/mp_summary_channels_test.go`

**Interfaces:**
- Consumes: `mpSummaryBlock` (Task 3).
- Produces (используют Task 5–6):
  - `type mpChannel struct { Slug, Title, Sign string; Patterns []*regexp.Regexp }`
  - `var mpChannels = []mpChannel{…}` — ровно 11 из спеки §3.4 (slug/имя/направление/паттерны; паттерны по стемам, например для `labor_market`: `безработиц`, `рынок труда`, `дефицит кадров`, `заработн`; для `expectations`: `инфляционн\w* ожидан`, `ценов\w* ожидан`; и т.п.);
  - `type mpChannelMatch struct { Channel, Kind string; Seq int; Text string }`
  - `func tagMPChannels(blocks []mpSummaryBlock) []mpChannelMatch` — пункты `bullet` из блока «Инфляционные риски» → `kind=risk_pro|risk_dez` (по тому, в чей список попал пункт: после подстроки «проинфляционные риски включают» / «дезинфляционные риски включают»); предложения прозы (`paragraph`) остальных блоков → `kind=prose`; `Seq` — сквозной; `Text` — дословный (нормализованный) текст пункта/предложения.

- [ ] **Step 1: Написать падающие тесты**: `TestMPChannelsRegistry` (11 записей, slug уникальны, у каждой непустые Title/Sign/Patterns); `TestTagMPChannelsHoldFixture` — на 23092026: есть матчи `expectations` с текстом, содержащим «инфляционн» и «ожидан»; есть ≥1 `risk_pro` и ≥1 `risk_dez`; `labor_market` содержит предложение с «безработиц» (фактическое из Task 2 — пин); негативный: предложение про нефть/Brent не попало в `labor_market`; `TestTagMPChannelsCounts` — числа матчей по каналам на фикстуре hold — фактические из Task 2.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать** реестр и `tagMPChannels` (сплит прозы на предложения: по `(?<=[.!?])\s+` после нормализации; один матч на канал на предложение — дедупликация по `(Channel, Kind, Text)`).

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run TestMPChannels -v && go test ./cbr/ -count=1 && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary_channels.go cbr/mp_summary_channels_test.go && git commit -m "cbr: 11-channel registry and regex tagging for mp summary"`.

---

### Task 5: LLM-слой разметки (с указанием модели)

**Files:**
- Create: `cbr/mp_summary_llm.go`
- Test: `cbr/mp_summary_llm_test.go`

**Interfaces:**
- Consumes: `util.PostJSON` (Task 1); `mpSummaryDoc`, `mpSummaryBlock` (Task 3); `mpChannel`, `mpChannelMatch` (Task 4).
- Produces (использует Task 6):
  - `const mpPromptVersion = 1`
  - `func mpLLMConfigFromEnv() (baseURL, apiKey, model string, ok bool)` — читает `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL`; любой пустой → `ok=false` (деградация без ошибки);
  - `func tagMPChannelsLLM(doc mpSummaryDoc, baseURL, apiKey, model string) ([]mpChannelMatch, error)` — POST `{baseURL}/chat/completions`, `Authorization: Bearer <apiKey>`, strict JSON; один ретрай при ошибке формата; валидация: каждая цитата обязана дословно встречаться в нормализованном тексте документа, иначе отбрасывается (warn), `slug` — только из `mpChannels`; результат — матчи с `Kind` из ответа и отдельные матчи `Kind='note'` (текст заметки); возвращает только матчи — `Method`/`model`/`prompt_version` в строки подставляет вызывающий (импортёр).

- [ ] **Step 1: Написать падающие тесты** (httptest-мок LLM; `t.Setenv` для переменных):
  - `TestMpLLMConfigFromEnv`: все три переменные → ok; без ключа → `ok=false`;
  - `TestTagMPChannelsLLMOK`: мок отдаёт валидный JSON с 11 каналами (цитаты существуют в фикстуре hold, загруженной через `parseMPSummary`) → матчи созданы, `Kind` сохранён, есть `note`-матч; запрос к моку содержит 11 slug и `Authorization`;
  - `TestTagMPChannelsLLMDropsHallucination`: мок добавляет цитату-галлюцинацию → она отсутствует в результате, остальные на месте;
  - `TestTagMPChannelsLLMBrokenJSON`: первый ответ — обрезанный JSON, второй (ретрай) — валидный → успех; оба битые → ошибка (вызывающий деградирует);
  - `TestTagMPChannelsLLM500`: мок отвечает 500 → ошибка.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать `cbr/mp_summary_llm.go`**: system-промпт (роль + 11 каналов с определениями из `mpChannels`), user — текст документа (блоки с заголовками), требование JSON-схемы; парсинг ответа — в структуру `{channels:[{slug,quotes:[{kind,text}],note}]}`; нормализация текста документа один раз.

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run TestTagMPChannelsLLM -v && go test ./cbr/ -count=1 && gofmt -l cbr/ && golangci-lint run ./cbr/...`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary_llm.go cbr/mp_summary_llm_test.go && git commit -m "cbr: LLM channel tagging with quote validation"`.

---

### Task 6: Импортёр `cbr_mp_summary` + проводка (реестр, dagu, гранты, MCP, витрины)

**Files:**
- Create: `cbr/mp_summary.go`, `cbr/mp_summary_test.go`
- Modify: `cbr/importers.go` (15-я запись), `cbr/importers_test.go` (позитивная проверка dagu-шага), `dagu/cbr.yaml` (шаг `cbr_mp_summary` последним), `sql/mcp_kimi_reader.sql` (три гранта), `scripts/mcp_check.py` (три чтения + закрытость трёх сырых)

**Interfaces:**
- Consumes: всё выше; `getPage`/`cbrUrl` (`cbr/links.go`, `cbr/households.go`).
- Produces: `mpSummaryImporter` с `Name() == "cbr_mp_summary"`; витрины `v_cbr_mp_summary`, `v_cbr_mp_summary_blocks`, `v_cbr_mp_summary_channels`; шаг dagu `cbr_mp_summary`.

- [ ] **Step 1: Написать тесты** (`cbr/mp_summary_test.go`): `TestMPSummaryImporterNameAndTable` (Name == "cbr_mp_summary", TableName == "cbr_mp_summary" — литералами); `TestMPSummaryDdlPins` — три DDL-литерала содержат `cbr_mp_summary`, `cbr_mp_summary_block`, `cbr_mp_summary_channel`, ReplacingMergeTree и ожидаемые ключи ORDER BY; `TestMPSummaryViewPins` — `Select` трёх витрин содержит фильтры (`source`-фильтров нет — таблицы свои; проверить `FINAL` и имена таблиц); в `cbr/importers_test.go` — реестр 15 записей и `dagu/cbr.yaml` содержит `CLICKHOUSE_IMPORT_STAT=cbr_mp_summary`.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать `cbr/mp_summary.go`** (`Import(ctx, conn)`):
  1. `conn.Exec` × 3 DDL-литерала (таблицы фиксированных имён);
  2. `getPage(mpSummaryIndexUrl)` → `parseMPDecIndex` → известные url: `SELECT DISTINCT url FROM cbr_mp_summary FINAL`; отсутствующие — `getPage(cbrUrl + href)` + `parseMPSummary` (+`Published` из ref);
  3. если нет новых и нет отстающих LLM — `log.Infof("cbr_mp_summary: no-op, все %d резюме на месте", N)` и `return 0, nil`;
  4. батч №1 шапка, №2 блоки, №3 regex-каналы (по документу: `tagMPChannels`; каждому матчу `Method='regex'`, `Model=''`, `PromptVersion=0`), `Seq` — сквозной по документу на таблицу; `Send` каждого (сбой = следующий прогон повторит, ключи Replacing идемпотентны);
  5. LLM-догон: если `mpLLMConfigFromEnv()` ok — документы без строк `(method='llm', model=?, prompt_version=1)` (SELECT DISTINCT meeting_date). `tagMPChannelsLLM` по каждому; ошибка документа → `log.Warnf` и продолжение; батч №4 каналы (`Method='llm'`, `Model=model`, `PromptVersion=mpPromptVersion`); если конфига нет — `log.Warnf("…: LLM-разметка пропущена (нет LLM_BASE_URL/LLM_API_KEY/MP_SUMMARY_LLM_MODEL)")`;
  6. `log.Infof` итогов: «резюме: %d новых, блоков: %d, каналов regex: %d, LLM: %d»; `UpsertSeriesCatalog` не нужен; витрины — `util.CreateView` × 3 (в конце);
  7. `return count, nil` (count = сумма Append'ов).
- DDL-литералы — дословно из спеки §3.3; Go-структура `mpSummaryStat{ util.ClickHouseImport }` с `Name()` override (как `mpSurveyStat`).
- Проводка: `cbr/importers.go` — `{mpSummaryImporter, "cbr_mp_summary"}` (обновить комментарий про 15 записей и `Name()==TableName`); `dagu/cbr.yaml` — шаг-шаблон существующих; `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_cbr_mp_summary TO kimi_reader;` ×3; `scripts/mcp_check.py` — по образцу `v_cbr_mo_br`: чтения `v_cbr_mp_summary` (>0), `v_cbr_mp_summary_blocks` (>0), `v_cbr_mp_summary_channels` (>0) и `ACCESS_DENIED`-проверки трёх сырых таблиц.

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -count=1 && go build ./... && gofmt -l cbr/ && golangci-lint run ./cbr/...`; `grep -c "  - id:" dagu/cbr.yaml` → 15; `dagu validate dagu/cbr.yaml` → exit 0; `python3 -m py_compile scripts/mcp_check.py`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary.go cbr/mp_summary_test.go cbr/importers.go cbr/importers_test.go dagu/cbr.yaml sql/mcp_kimi_reader.sql scripts/mcp_check.py && git commit -m "cbr: cbr_mp_summary importer with channel tagging, views and wiring"`.

---

### Task 7: Календарь заседаний (cal_mp → events_calendar)

**Files:**
- Modify: `calendar/events.go`
- Create: `calendar/events_test.go`, `dagu/calendar.yaml`

**Interfaces:**
- Consumes: фикстура `calendar/testdata/cal_mp.html` (Task 2); `util.GetFile`; `util.CreateView` (существующая `eventsView`).
- Produces:
  - `const calMPUrl = "https://www.cbr.ru/dkp/cal_mp/"`
  - `func parseCalMP(page []byte) ([]time.Time, error)` — карточки-заседания (текст содержит «Заседание Совета директоров» и «денежно-кредитной политике») с полной датой «\<D\> \<месяц\> \<YYYY\> года»; даты без года (старые табличные секции) игнорируются; дедупликация и сортировка; 0 заседаний → ошибка;
  - в `Import()`: после создания таблицы и витрины — fetch `calMPUrl` → `parseCalMP` → INSERT канонических строк (`category='cbr'`, `title='СД ЦБ РФ: заседание по ключевой ставке'`, `event_time='13:30'`, `threshold='{}'`, `status='pending'`) одним батчем (`PrepareBatch → Append(err!) → Send`); пофетчевая ошибка — warn + успешный выход (таблица/витрина уже созданы), ошибка разбора — ошибка прогона.
- Новый `dagu/calendar.yaml` по образцу одншаговых DAG (bold/gold): `schedule: "7 10 * * *"`, `overlap_policy: skip`, шаг `events_calendar`, `retry_policy: 3 × 300`.

- [ ] **Step 1: Написать падающие тесты**: `TestParseCalMPFixture` — даты сортированы, содержат 2026-10-23, 2026-12-18 и восемь дат 2027 (02-12, 03-19, 04-30, 06-11, 07-30, 09-10, 10-22, 12-17), все — из карточек-заседаний; карточки пресс-конференций в результат не попали (проверка: в результате нет дат, которых нет в списке выше — по фикстуре 2022–2025 заседаний будет больше, пин — «включает 2024-02-16, 2022-02-28», фактические из Task 2); `TestParseCalMPFailFast` — html без заседаний → ошибка.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать** `parseCalMP` (goquery; месяцы — общая карта с Task 3? — своя локальная копия в `calendar/`, чтобы не тянуть `cbr/`) и дописать `Import()`; создать `dagu/calendar.yaml`.

- [ ] **Step 4: Прогнать — PASS**: `go test ./calendar/ -count=1 && go build ./... && gofmt -l calendar/ && dagu validate dagu/calendar.yaml` → exit 0.

- [ ] **Step 5: Commit**: `git add calendar/events.go calendar/events_test.go dagu/calendar.yaml && git commit -m "calendar: fetch CB meeting calendar from cal_mp into events_calendar"`.

---

### Task 8: Документация

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/KEY_RATE_PUBLICATIONS.md`, `docs/KEY_RATE_CHANNELS.md`, `docs/ROADMAP_KEY_RATE.md` (§4.2/§5; §6 — Task 9)

**Interfaces:**
- Consumes: фактические имена/расписание после Task 1–7.

- [ ] **Step 1: `ARCHITECTURE.md`**: чейнджлог (запись 2026-10-14: пункт 1.2 — `cbr_mp_summary`, двухслойная разметка каналов, `dagu/calendar.yaml`); §2 — строки `cbr/` и `calendar/`; §3 — `util.PostJSON`; §6.1 — строка `cbr_mp_summary` (15-я запись реестра; кастомный Import: дельта по индексу); §6.2 — канонические DDL `cbr_mp_summary`/`_block`/`_channel` (копии в коде обязаны совпадать); §6.3 — три витрины; §6.5 — контур; §7 — расписание: 15-й шаг `cbr.yaml`, новый `dagu/calendar.yaml` (10:07 МСК), различи «каноническая строка заседания / ручной триггер» в `events_calendar`, заменить предупреждение «заседания попадают вручную» на фактическое; §8 — оговорка: `cbr_mp_summary` сознательно без HEAD-префлайта (мультидокументная дельта по индексу) + LLM-слой и переменные окружения.
- [ ] **Step 2: `README.md`** — таблица импортёров `cbr` (+`cbr_mp_summary`: три таблицы, три витрины, LLM-слой), расписание (15 шагов + calendar.yaml), дерево/тестовые пакеты.
- [ ] **Step 3: `docs/KEY_RATE_PUBLICATIONS.md`** §6 — «Резюме обсуждения (каналы и веса)» ❌→✅ с `v_cbr_mp_summary`/`_channels`; `docs/KEY_RATE_CHANNELS.md` — §2 «Практический вывод для ETL» → «реализовано в `cbr_mp_summary`»; §8 «Текстовый сигнал ЦБ» → ✅.
- [ ] **Step 4: `docs/ROADMAP_KEY_RATE.md`** — §4.2 пункт 2 «Текстовый сигнал ЦБ» → ✅; §5 — строки таблиц/витрин; §6 не трогать (Task 9).
- [ ] **Step 5: Проверки и коммит**: `grep -rn "cbr_mp_summary" ARCHITECTURE.md README.md | wc -l` ≥ 4; `go build ./...`; коммит `docs: sync ARCHITECTURE/README/key-rate docs for cbr_mp_summary (key-rate phase 1.2)`.

---

### Task 9: Живая приёмка и статус 1.2

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Interfaces:**
- Consumes: всё выше; спека §4.

- [ ] **Step 1: `make all`** — зелёный.
- [ ] **Step 2: Полный прогон** (предусловие — владелец добавил `LLM_BASE_URL`/`LLM_API_KEY`/`MP_SUMMARY_LLM_MODEL` в `~/.config/rosstat/env`): `make import STAT=cbr_mp_summary` → «резюме: 22 …» (числа зафиксировать); повтор → `no-op, все 22 резюме на месте`, счётчики витрин не изменились.
- [ ] **Step 3: Сверка 22/22** с реестром `KEY_RATE_CHANNELS.md` §1 (meeting_date/published/решение) — SQL по `v_cbr_mp_summary` + сверка; зафиксировать 9 сохранений / 13 изменений.
- [ ] **Step 4: Калибровка каналов** — отчёт «канал → матчи regex / цитаты LLM» по всем 22 (scratch-скрипт, не коммитится); цифры в блок приёмки.
- [ ] **Step 5: Календарь**: `make import STAT=events_calendar` → канонические строки (23.10/18.12.2026 + 8×2027), ручные строки на месте, повтор идемпотентен; `dagu validate` обоих DAG.
- [ ] **Step 6: MCP**: `make mcp-user` → `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (включая три новые строки и закрытость сырых); споты витрин через `mcp__clickhouse__run_query`.
- [ ] **Step 7: Статус 1.2** в `docs/ROADMAP_KEY_RATE.md`: `✅ **сделан этим PR** — приёмка ниже` + блок «Живая приёмка 2026-10-14 — шаг 1.2 (резюме обсуждения) — ✅ выполнен» с фактами шагов 1–6; коммит `roadmap key rate: mark 1.2 done (mp summary and calendar acceptance)`.

---

## После задач (вне нумерации)

Финальное ревью всей ветки (сильная модель) + `verification-before-completion`; один PR `feat/cbr-mp-summary` → `main` (как фаза 3/1.5). Спека и план — первыми коммитами ветки.
