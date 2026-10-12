# news_watch (RSS-события РФ-канала) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Новый пакет `news/` с импортёром `news_watch`: пять RSS-лент РФ-канала (Интерфакс, РБК, ЦБ РФ ×2, TASS) → `news_events`, витрина `v_news_events` с фильтром по стемам тезисов, три режима работы (`all`/`since`/`check`), расписание dagu каждые 30 минут.

**Architecture:** Разбор RSS и логика режимов — чистые функции (`news/parse.go` и часть `news/news.go`), импортёр — тонкая обвязка: DDL, публикация витрины в начале прогона, `util.GetXML`, один батч, агрегированная ошибка лент (сбойная не блокирует остальные). Витрину создаёт сам импортёр (образец `eia/`), грант — в `sql/mcp_kimi_reader.sql`.

**Tech Stack:** Go, `encoding/xml`, `net/http/httptest`, `logrus`, ClickHouse (`clickhouse-go/v2`, `PrepareBatch → Append → Send`), `util.GetXML` (новый), dagu (yaml).

**Spec:** `docs/superpowers/specs/2026-10-12-news-watch-design.md`

## Global Constraints

- Новых внешних зависимостей нет; HTTP — только через `util` (новый `util.GetXML` — зеркало `util.GetJSON`); сетевых вызовов в `init()` нет.
- Запись в ClickHouse — только батчами: `PrepareBatch → Append` (err проверяется) `→ Send`; DDL `IF NOT EXISTS` (таблица), `CREATE OR REPLACE VIEW` (витрина); комментарии — `ALTER TABLE … MODIFY COMMENT` / `COMMENT COLUMN`.
- `news_events` — канонический DDL ARCHITECTURE.md §6.2; копия в `news/schema.go` обязана совпадать; ключ `(date, source, title)`; `sentiment` всегда NULL (`(*float32)(nil)`).
- Метки источников: `interfax`, `rbc`, `cbr` (оба фида ЦБ), `tass`; URL-ы лент — пакетные переменные (подмена `httptest` в тестах).
- Режим — env `NEWS_WATCH_MODE` (`""`/`all`, `since`, `check`); неизвестное значение — ошибка до сети и до БД.
- Фильтр витрины — строка из спеки §«Схема и витрина» дословно (43 стема, RE2-совместимо, `(?i)`); порядок `date DESC`; витрина = 8 стейтментов (VIEW + таблица-комментарий + 6 колонок).
- Тесты офлайн: сеть только `httptest`, фикстуры — `news/testdata/*.rss`; `go test ./...` без сети и БД.
- Логи — logrus по-русски (конвенция проекта); коммиты — по-английски, в духе истории.
- Расписание: `dagu/news.yaml`, `schedule: "7,37 * * * *"`, `overlap_policy: skip`, retry 3 × 300, `NEWS_WATCH_MODE=all`.

## Review Focus

1. **Дата без секунд («…13:43 +0300») или с «GMT»** в отдельной позиции не должна молча терять позицию — `parsePubDate` принимает три раскладки (Task 3).
2. **Регистр режима** (`ALL`, `Since`) — ошибка до сети, а не тихий дефолт `all` (Task 4).
3. **Два фида ЦБ под одним `source=cbr`**: `feedSpans` сливает их в одно окно (min/min, max/max, count+), дыра считается по объединённому окну (Task 4).
4. **Дыра в одном источнике не глушит отчёт остальных** и названа в тексте ошибки именем источника (Task 4).
5. **Полностью нераспознанная лента** (все item без даты/заголовка или 0 item) — ошибка ленты, а не «успешные 0 строк» (Task 3).

---

### Task 1: `util.GetXML` — транспорт RSS

**Files:**
- Create: `util/xml.go`, `util/xml_test.go`

**Interfaces:**
- Consumes: `HttpClient`, `randomUA`, `errorSnippet` (уже в `util/`), шаблон `GetJSON` (`util/json.go`).
- Produces (нужно Task 3–5): `func GetXML(url string, out any) error`; `var xmlTimeout = 60 * time.Second` (переменная — ради теста дедлайна).

- [ ] **Step 1: Написать падающие тесты** — `util/xml_test.go`, зеркало `util/json_test.go`:

```go
func TestGetXMLDecodesBody(t *testing.T)        // 200 + `<root><value>42</value></root>` → out.Value == 42; запрос ушёл с непустым User-Agent
func TestGetXMLNonOKStatusFails(t *testing.T)   // 403 + `<html>blocked</html>` → ошибка содержит "403" и "blocked", но не URL сервера
func TestGetXMLMalformedBodyFails(t *testing.T) // 200 + "not xml" → ошибка
func TestGetXMLEmptyBodyFails(t *testing.T)     // 200 + пустое тело → ошибка
func TestGetXMLTimeoutFails(t *testing.T)       // xmlTimeout = 50ms, хендлер спит 2s → errors.Is(err, context.DeadlineExceeded)
func TestGetXMLHTMLEntityDecodes(t *testing.T)  // `<title>a&nbsp;b</title>` → "a\u00a0b" без ошибки (HTMLEntity)
```

- [ ] **Step 2: Прогнать — падает**

Run: `go test ./util/ -run TestGetXML -v`
Expected: FAIL — `undefined: GetXML`.

- [ ] **Step 3: Реализовать `util/xml.go`**

Зеркало `GetJSON` с точностью до декодера: `context.WithTimeout(xmlTimeout)`, `http.NewRequestWithContext`, `req.Header.Set("User-Agent", randomUA)`, `HttpClient.Do`, `io.ReadAll`, не-200 → `fmt.Errorf("http status %d: %s", resp.StatusCode, errorSnippet(body))`, пустое тело → `fmt.Errorf("пустое тело")`, затем декодер:

```go
decoder := xml.NewDecoder(bytes.NewReader(body))
decoder.Entity = xml.HTMLEntity // ленты встречаются с &nbsp;: одна сущность не должна обнулять ленту целиком
if err = decoder.Decode(out); err != nil {
	return err
}
```

Комментарий функции: почему не `GetFile` (статус не проверяет — HTML WAF читался бы «успехом»), почему `HTMLEntity`, почему дедлайн переменной.

- [ ] **Step 4: Прогнать — проходит + саботаж**

Run: `go test ./util/ -run TestGetXML -v`
Expected: PASS (6/6). Саботаж: убрать проверку статуса → падает `TestGetXMLNonOKStatusFails`; вернуть `xml.Unmarshal` без `HTMLEntity` → падает `TestGetXMLHTMLEntityDecodes`.

- [ ] **Step 5: Commit**

```bash
gofmt -l util/xml.go util/xml_test.go
git add util/xml.go util/xml_test.go
git commit -m "util: add status-checked GetXML helper tolerant to html entities"
```

---

### Task 2: `news/schema.go` — DDL, фильтр тезисов, стейтменты витрины

**Files:**
- Create: `news/schema.go`, `news/schema_test.go`

**Interfaces:**
- Produces (нужно Task 5): `const newsEventsDdl string` — копия канонического DDL §6.2; `const newsFilter string` — фильтр из спеки дословно; `var newsViewStatements []string` — 8 стейтментов (VIEW + комментарий таблицы + 6 комментариев колонок), SQL собирается через `fmt.Sprintf` с `newsFilter`.

- [ ] **Step 1: Написать падающие тесты** — `news/schema_test.go` (пакет `news`, офлайн):

```go
func TestNewsEventsDdlPinsCanonicalKey(t *testing.T) // содержит "ENGINE = ReplacingMergeTree ORDER BY (date, source, title)" и "sentiment Nullable(Float32)"
func TestNewsFilterMatchesThesisStems(t *testing.T)  // regexp.MustCompile(newsFilter) — RE2, тот же движок, что match() в CH:
                                                     // true для «Дизельное топливо», «Экспорт бензина», «нефть Brent», «санкции ЕС»,
                                                     // «ключевая ставка», «инфляция», «цены на золото», «Банк России», «Минфин», «НДПИ»,
                                                     // «Полюс», «Сухой Лог», «Трамп», «поставки СПГ»;
                                                     // false для «погода в Москве», «футбольный матч»
func TestNewsViewStatementsShape(t *testing.T)       // len == 8; [0]: "CREATE OR REPLACE VIEW v_news_events", "DEFINER = default SQL SECURITY DEFINER",
                                                     // "match(title, '<newsFilter>')", "toDate(date, 'Europe/Moscow')", "ORDER BY date DESC";
                                                     // [1]: MODIFY COMMENT; далее COMMENT COLUMN по date, date_msk, source, title, url, sentiment
```

- [ ] **Step 2: Прогнать — падает** (`undefined: newsEventsDdl`)

- [ ] **Step 3: Реализовать `news/schema.go`**

- `newsEventsDdl` — дословно из ARCHITECTURE.md §6.2 (стиль отступов как у `eia/eia.go`), комментарий-ссылка «копия §6.2 — копии обязаны совпадать».
- `newsFilter` — одна строка из спеки (43 стема: `(?i)дизел|бензин|…|PLZL`).
- `newsViewStatements`: SELECT `date, toDate(date, 'Europe/Moscow') AS date_msk, source, title, url, sentiment FROM news_events FINAL WHERE match(title, '<newsFilter>') ORDER BY date DESC`; тексты комментариев — из спеки (история копится с первого прогона; полный поток — в сырой `news_events`, агенту не выдаётся; `sentiment` — follow-up; оба фида ЦБ под `cbr`).

- [ ] **Step 4: Прогнать + саботаж** — PASS; саботаж: выкинуть стем «дизел» → падает `TestNewsFilterMatchesThesisStems`.

- [ ] **Step 5: Commit** — `git add news/schema.go news/schema_test.go && git commit -m "news: add news_events ddl, thesis filter and view statements"`

---

### Task 3: `news/parse.go` — RSS-разбор и фикстуры

**Files:**
- Create: `news/parse.go`, `news/parse_test.go`, `news/testdata/interfax.rss`, `news/testdata/rbc.rss`, `news/testdata/cbr_news.rss`, `news/testdata/cbr_press.rss`, `news/testdata/tass.rss`

**Interfaces:**
- Produces (нужно Task 4–5):
  - `type rssItem struct { Title, Link, PubDate string }` (теги `title`/`link`/`pubDate`); `type rssDoc struct { Channel struct { Items []rssItem `xml:"item"` } `xml:"channel"` }`.
  - `type newsItem struct { Date time.Time; Source, Title, URL string }`.
  - `func itemsFromDoc(source string, doc rssDoc) (items []newsItem, skipped int, err error)` — заголовок нормализуется `strings.Join(strings.Fields(...), " ")`; позиция без распознанной даты или с пустым после нормализации заголовком → `skipped++`; ноль валидных позиций → ошибка «пустая лента: нет распознанных позиций (item-ов N)»; `URL` = `strings.TrimSpace(link)` (может быть пустым).
  - `func parsePubDate(s string) (time.Time, bool)` — раскладки по порядку: `time.RFC1123Z`, `"Mon, 02 Jan 2006 15:04 -0700"`, `time.RFC1123`.

- [ ] **Step 1: Снять фикстуры живых лент (полные окна, как есть)**

```bash
mkdir -p news/testdata
UA='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
curl -sS -A "$UA" -o news/testdata/interfax.rss   "https://www.interfax.ru/rss"
curl -sS -A "$UA" -o news/testdata/rbc.rss        "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"
curl -sS -A "$UA" -o news/testdata/cbr_news.rss   "https://www.cbr.ru/rss/RssNews"
curl -sS -A "$UA" -o news/testdata/cbr_press.rss  "https://www.cbr.ru/rss/eventrss"
curl -sS -A "$UA" -o news/testdata/tass.rss       "https://tass.ru/rss/v2.xml"
wc -c news/testdata/*.rss   # суммарно ~450 КБ, все непустые
```

Если лента недоступна — зафиксировать факт в отчёте задачи, фикстуру не выдумывать.

- [ ] **Step 2: Написать падающие тесты** — `news/parse_test.go`:

```go
func TestParsePubDateVariants(t *testing.T)   // "Mon, 12 Oct 2026 13:43:00 +0300" → ok; "Mon, 12 Oct 2026 13:43 +0300" → ok;
                                              // "Mon, 12 Oct 2026 13:43:00 GMT" → ok; "12.10.2026" → false
func TestItemsFromDocMapsFixtureItem(t *testing.T)  // фикстура interfax.rss: ≥1 позиция, все Date ненулевые, source == "interfax";
                                                    // ПЕРВАЯ позиция фикстуры — title/date/url вписаны пин-значениями, прочитанными из закоммиченного файла
func TestItemsFromDocSkipsInvalidItems(t *testing.T) // искусственный rssDoc: item без pubDate, item с пробельным заголовком, item с битой датой + один валидный → items == 1, skipped == 3
func TestItemsFromDocEmptyFeedFails(t *testing.T)    // 0 item → ошибка; все item битые → ошибка (не «успешные 0»)
func TestFixturesParse(t *testing.T)                 // все 5 фикстур: itemsFromDoc без ошибки, len(items) > 0; skipped пинится ФАКТИЧЕСКИМ значением,
                                                     // прочитанным исполнением (ожидание 0; не 0 — разобраться и зафиксировать причину комментарием)
```

- [ ] **Step 3: Реализовать `news/parse.go`** — по интерфейсу выше; комментарий: пропуск позиции — сигнал смены формы ленты, поэтому считается и печатается, а не молчит; заголовок нормализуется в одну строку (логи/витрина).

- [ ] **Step 4: Прогнать + саботаж**

Run: `go test ./news/ -run 'TestParsePubDate|TestItemsFromDoc|TestFixturesParse' -v`
Expected: PASS. Саботаж: оставить только `time.RFC1123Z` → падает кейс без секунд; снять `Fields`-нормализацию → падает кейс пробельного заголовка.

- [ ] **Step 5: Commit** — `git add news/parse.go news/parse_test.go news/testdata/*.rss && git commit -m "news: add rss parsing with live feed fixtures"`

---

### Task 4: `news/news.go` — конфиг лент, fetch, режимы

**Files:**
- Create: `news/news.go` (конфиг/fetch/режимы; `Import()` — Task 5), `news/news_test.go`

**Interfaces:**
- Consumes: Task 1 (`util.GetXML`), Task 3 (`rssDoc`, `itemsFromDoc`, `newsItem`, фикстуры).
- Produces (нужно Task 5):
  - `type feed struct { Key, Source, URL string }`; `func feeds() []feed` — 5 лент из пакетных переменных `feedInterfaxURL`, `feedRBCURL`, `feedCBRNewsURL`, `feedCBRPressURL`, `feedTASSURL` (URL-ы — из спеки §«Источники»), фиксированный порядок; `Key` = имя фикстуры (`cbr_news`/`cbr_press`), `Source` = `cbr` у обоих.
  - `const (modeAll = "all"; modeSince = "since"; modeCheck = "check"; modeEnv = "NEWS_WATCH_MODE")`; `func parseMode(v string) (string, error)`.
  - `type sourceSpan struct { Count uint64; Min, Max time.Time }`; `type feedResult struct { Feed feed; Items []newsItem; Skipped int; Err error }`; `func fetchFeeds(feeds []feed) []feedResult`.
  - `func feedSpans(results []feedResult) map[string]sourceSpan`; `func selectSince(items []newsItem, cursors map[string]time.Time) []newsItem`.
  - `func detectGaps(feed, stored map[string]sourceSpan) []string`; `func coverageLines(feed, stored map[string]sourceSpan) []string`.

- [ ] **Step 1: Написать падающие тесты** — `news/news_test.go`:

```go
func TestParseMode(t *testing.T)          // "" → all; "all"/"since"/"check" → ok; "ALL", "full", " " → ошибка
func TestFetchFeedsUsesFixturesAndIsolatesFailure(t *testing.T) // httptest: /interfax,/rbc,/cbr_news,/cbr_press,/tass;
                                          // cbr_news отвечает 500; подмена feed*URL на server.URL+путь; после: 4 результата с Items>0 и Err==nil,
                                          // у cbr_news Err != nil; sources == ["interfax","rbc","cbr","cbr","tass"]
func TestFeedsHaveFixtures(t *testing.T)  // каждый feed: testdata/<Key>.rss существует, непуст, разбирается (Items>0);
                                          // и обратно — в testdata нет *.rss без ленты (glob)
func TestSelectSinceFiltersStrictlyAfterCursor(t *testing.T) // курсор 12:00: позиция 11:59 — нет, 12:00 (равная) — нет, 12:01 — да; пустой курсор → все
func TestFeedSpansMergesCbrFeeds(t *testing.T) // два результата cbr с разными окнами → один span: count суммируется, min = min, max = max
func TestDetectGapsFindsMissedWindow(t *testing.T) // stored cbr max 10.10 12:00, feed min 10.12 09:00 → 1 строка, содержит "cbr" и обе границы
func TestDetectGapsQuietWhenOverlap(t *testing.T)  // feed min ≤ stored max → пусто; нет stored у источника → пусто (первый прогон)
```

- [ ] **Step 2: Реализовать**

- `feeds()` — фиксированный порядок: interfax, rbc, cbr_news, cbr_press, tass.
- `fetchFeeds` — цикл: `var doc rssDoc; err := util.GetXML(f.URL, &doc)` → `itemsFromDoc(f.Source, doc)`; ошибки — в `feedResult.Err`, цикл не прерывается.
- `feedSpans` — merge по `Source`: `Count` суммируется, `Min`/`Max` — крайние среди непустых.
- `selectSince` — пропускает только `item.Date.After(cursors[item.Source])`; нулевой/отсутствующий курсор пропускает всё.
- `detectGaps` — по ключам feed, отсортированным (`sort.Strings` — детерминированный вывод): `stored.Count > 0 && feed.Min.After(stored.Max)` → `fmt.Sprintf("%s: дыра %s … %s (окно не собрано: пропущенный прогон)", src, stored.Max.Format(time.RFC3339), feed.Min.Format(time.RFC3339))`.
- `coverageLines` — `"%s: сохранено %d (%s…%s), в ленте %d (%s…%s)"`; источник без сохранённых строк — `"%s: сохранённых строк нет (первый прогон не выполнялся)"`.

- [ ] **Step 3: Прогнать + саботаж**

Run: `go test ./news/ -run 'TestParseMode|TestFetchFeeds|TestFeedsHaveFixtures|TestSelectSince|TestFeedSpans|TestDetectGaps' -v`
Expected: PASS. Саботаж: `!item.Date.Before(cursor)` вместо `After` → падает равный курсор в `TestSelectSinceFiltersStrictlyAfterCursor`; убрать merge в `feedSpans` → падает `TestFeedSpansMergesCbrFeeds`.

- [ ] **Step 4: Commit** — `git add news/news.go news/news_test.go && git commit -m "news: add feed config, fetch isolation and watch modes"`

---

### Task 5: импортёр `news_watch` — `Import()`, регистрация, dagu, main.go

**Files:**
- Modify: `news/news.go` (Import/Name/init/readStoredSpans/insertItems), `main.go` (blank-import)
- Create: `dagu/news.yaml`

**Interfaces:**
- Consumes: Task 1–4; `driver.Conn`, `chimport.Stats`, `log`.
- Produces: `type newsImport struct{}`; `func (s *newsImport) Name() string` → `"news_watch"`; `func readStoredSpans(ctx context.Context, conn driver.Conn) (map[string]sourceSpan, error)`; `func insertItems(ctx context.Context, conn driver.Conn, items []newsItem) (int64, error)`.

- [ ] **Step 1: Написать падающий тест**

```go
func TestImportRejectsUnknownModeBeforeDB(t *testing.T) // t.Setenv("NEWS_WATCH_MODE", "bogus"); var s newsImport;
                                                        // _, err := s.Import(context.Background(), nil) → err != nil, содержит "NEWS_WATCH_MODE";
                                                        // nil-conn не паникует (режим проверяется первым действием)
```

- [ ] **Step 2: Реализовать `Import()` — порядок жёсткий**

1. `mode, err := parseMode(os.Getenv(modeEnv))` — **до** БД и сети; ошибка → `return 0, err`.
2. `conn.Exec(ctx, newsEventsDdl)`.
3. `mode != modeCheck` → выполнить `newsViewStatements` (публикация витрины в начале прогона, как у `eia`; в no-op-ветке `since` витрина уже опубликована).
4. `results := fetchFeeds(feeds())`; по каждой ленте: ошибка → `log.Errorf("news_watch: %s: %v", …)` + `feedErrs = append(feedErrs, fmt.Errorf("%s: %w", r.Feed.Key, r.Err))`; успех → `log.Infof("news_watch: %s: позиций %d, пропущено %d", …)`.
5. `spans := feedSpans(results)`.
6. `modeCheck`: `stored, err := readStoredSpans`; `coverageLines(spans, stored)` — в лог построчно; `gaps := detectGaps(spans, stored)` ≠ пусто → `feedErrs = append(feedErrs, fmt.Errorf("дыры покрытия: %s", strings.Join(gaps, "; ")))`; `return 0, errors.Join(feedErrs...)`.
7. `modeAll`/`modeSince`: собрать items успешных лент; для `since` — `cursors` из `stored` + `selectSince`; пусто: `since` — `log.Infof("news_watch: новых позиций нет — нечего обновлять (0 строк)")`; `all` с пустым списком — только `return 0, errors.Join(feedErrs...)` (все ленты упали).
8. `count, err := insertItems(ctx, conn, items)`: `PrepareBatch("INSERT INTO news_events")`; в цикле `batch.Append(it.Date, it.Source, it.Title, it.URL, (*float32)(nil))` — err проверяется, при ошибке `_ = batch.Abort()`; `batch.Send()`.
9. `return count, errors.Join(feedErrs...)` — успешные ленты записаны, сбойная видна в dagu (retry 3 × 300 идемпотентен).

- [ ] **Step 3: `readStoredSpans`** — `SELECT source, count(), min(date), max(date) FROM news_events FINAL GROUP BY source`; скан `(string, uint64, time.Time, time.Time)`; пустая таблица → пустая map без ошибки.

- [ ] **Step 4: `main.go`** — `_ "github.com/kmlebedev/clickhouse-import-rosstat/news"` между `moex` и `polyus` (алфавит).

- [ ] **Step 5: `dagu/news.yaml`**

```yaml
type: chain
schedule: "7,37 * * * *"
overlap_policy: skip

# Ленты топ-новостей РФ-канала обновляются минутами, а окна лент конечны
# (Интерфакс 25 позиций, РБК 30, ЦБ и TASS по 100): прогон каждые 30 минут
# гарантирует, что окно не проскочит целиком между запусками. Режим all
# перечитывает окно целиком — пропущенный прогон не теряет данные, повторный
# идемпотентен (ключ date/source/title). Ноль позиций или сбой ленты — ошибка
# (лента не «стареет молча»), но остальные ленты при этом пишутся; сбой виден
# в dagu. Проверка дыр/окон (NEWS_WATCH_MODE=check) — ручная, не в расписании.
steps:
  - id: news_watch
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=news_watch
      - NEWS_WATCH_MODE=all
    retry_policy:
      limit: 3
      interval_sec: 300
```

- [ ] **Step 6: Прогнать**

Run: `go test ./news/... -count=1 && go build ./... && gofmt -l news/`
Expected: PASS; пустой вывод gofmt.

- [ ] **Step 7: Commit** — `git add news/news.go dagu/news.yaml main.go && git commit -m "news: add news_watch importer, dagu schedule and main wire-up"`

---

### Task 6: MCP-контур — грант и проверки

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`

**Interfaces:**
- Consumes: витрина `v_news_events` и таблица `news_events` (Task 5).
- Produces: грант `kimi_reader`; блок проверок в `check()` перед `return failed`.

- [ ] **Step 1: Грант** — в конец `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_news_events TO kimi_reader;`

- [ ] **Step 2: Проверки** — в `scripts/mcp_check.py`, после bold-блока (образец — комментарий и код bold-блока):

```python
            # Контур событий РФ-канала: витрина отфильтрованных событий читается агентом,
            # а сырая news_events — источник витрины — агенту закрыта (как macro_series и
            # bold_fund_snapshot: читать её агент обязан только через витрину). Существование
            # витрины подтверждается запросом к ней, а не поиском имени в list_tables (урок bold).
            news_view = text(await session.call_tool("run_query", {"query": "SELECT count() FROM v_news_events"}))
            news_view_rows = result_rows(news_view)
            if news_view_rows is None:
                news_view_detail = f"витрина не читается: {news_view[:200]}"
            elif news_view_rows:
                news_view_detail = f"строк: {news_view_rows[0].get('count()')}"
            else:
                news_view_detail = "в ответе нет строк"
            failed += not expect("витрина v_news_events существует и читается", bool(news_view_rows), news_view_detail)

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM news_events"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырая news_events закрыта", denied,
                                 "отказ ClickHouse 497 на news_events (ожидаемо)" if denied else raw[:200])
```

- [ ] **Step 3: Синтаксис** — `python3 -c "import ast; ast.parse(open('scripts/mcp_check.py').read())"` → без вывода.

- [ ] **Step 4: Commit** — `git add sql/mcp_kimi_reader.sql scripts/mcp_check.py && git commit -m "news: add mcp grant and smoke checks for v_news_events"`

---

### Task 7: Документация — ARCHITECTURE.md и README.md

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`

**Interfaces:** Consumes всё предыдущее; Produces синхронизированные документы (скил `sync-readme-architecture`).

- [ ] **Step 1: ARCHITECTURE.md**

- §2 — блок `news/` в структуре (после `moex/`): новости РФ-канала, импортёр `news_watch`, витрина `v_news_events`, три режима, `dagu/news.yaml`.
- §3 — пункт `util.GetXML` в списке хелперов (проверка статуса, `HTMLEntity`; отличие от `GetFile`).
- §5 — строка env `NEWS_WATCH_MODE` (`all` по умолчанию; `since`; `check`).
- §6.1 — строка `news_watch` в таблице импортёров: пакет `news/`, статус «реализован; `Name()` = `news_watch`», источник — 5 RSS-лент, метод — режимы, изоляция ошибок лент, окна лент.
- §6.2 — `news_events` убрать из списка «Не реализованы (в коде нет)»; добавить в реализованные (создаёт импортёр, копия DDL — `news/schema.go`); сам DDL-блок не менять.
- §6.3 — новый пункт `v_news_events` (фильтр по стемам, колонки, история с первого прогона, `sentiment` NULL — follow-up, сырая не выдана).
- §7 — строка календаря: «каждые 30 минут — ленты РФ-канала (Интерфакс/РБК/ЦБ РФ/TASS), импортёр `news_watch` — события-решения вне календаря».
- Шапка «Последнее обновление» — запись 2026-10-12 о контуре news_watch.

- [ ] **Step 2: README.md — по скилу `sync-readme-architecture`** (`.kimi-code/skills/sync-readme-architecture/SKILL.md`): таблица импортёров/расписаний (news_watch — «каждые 30 минут», таблица `news_events`, витрина), «Конфигурация» (`NEWS_WATCH_MODE`), MCP-раздел, runbook режимов `since`/`check` (включая ограничение `check`: дыра видна до «залечивания» следующей записью).

- [ ] **Step 3: Проверка**

Run: `grep -n "news_watch\|NEWS_WATCH_MODE\|v_news_events" ARCHITECTURE.md README.md && make all`
Expected: попадания во всех трёх разделах; `make all` зелёный.

- [ ] **Step 4: Commit** — `git add ARCHITECTURE.md README.md && git commit -m "docs: sync architecture and readme for news_watch"`

---

### Task 8: Живая приёмка и статус фазы 4

**Files:**
- Modify: `docs/GOLD_THESIS_DATA_ROADMAP.md`

- [ ] **Step 1: Живые прогоны трёх режимов**

```bash
make import STAT=news_watch                        # all: 5/5 лент со счётчиками; повтор — идемпотентен
NEWS_WATCH_MODE=since make import STAT=news_watch  # no-op: «новых позиций нет», 0 строк
NEWS_WATCH_MODE=check make import STAT=news_watch  # сводка покрытия 5 лент, дыр нет
NEWS_WATCH_MODE=bogus make import STAT=news_watch; echo $?   # ошибка до сети, ненулевой код возврата
```

- [ ] **Step 2: Гранты и MCP (`kimi_reader`)** — `make mcp-user`; через MCP `run_query`:

```sql
SELECT date_msk, source, title, url FROM v_news_events WHERE date >= now() - INTERVAL 7 DAY ORDER BY date DESC LIMIT 10;
SELECT date_msk, source, title FROM v_news_events
 WHERE date >= now() - INTERVAL 7 DAY AND match(title, '(?i)дизел|поставк|сделк|санкц') ORDER BY date DESC LIMIT 10;
SELECT count() FROM news_events;   -- ожидание ACCESS_DENIED
```

Зафиксировать фактические числа: строк в витрине, лент в покрытии, попадания событийного следа (ожидание: сюжет о поставках дизеля, если он в окне лент; иначе — фактическое покрытие).

- [ ] **Step 3: Финальная проверка** — `make all` зелёный; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (витрина читается, сырая закрыта).

- [ ] **Step 4: Роадмап** — в `docs/GOLD_THESIS_DATA_ROADMAP.md`: у фазы 4 заменить статус «⏳ в работе (шаг «сбор»…)» на «✅ выполнен (дата фактической приёмки; шаг «сбор»)»; вписать «Реализовано» и «Результат — приёмка» (команды и наблюдённые числа); follow-up 1–4 оставить ⏳.

- [ ] **Step 5: Commit** — `git add docs/GOLD_THESIS_DATA_ROADMAP.md && git commit -m "docs: mark gold roadmap phase 4 news step done"`

- [ ] **Step 6: PR** — `superpowers:finishing-a-development-branch`: один PR (источник данных, схема таблицы, `CLICKHOUSE_IMPORT_STAT=news_watch`, режимы, пример проверки `SELECT … FROM v_news_events`); без промежуточных PR.
