# Telegram-молния Интерфакса + винтажные версии + LLM-разметка — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development`
> (or `superpowers:executing-plans` only if the owner explicitly chose inline execution). Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** добавить в `news_watch` публичный Telegram-канал `@interfaxonline` как быстрый слой
коротких сообщений (`source='interfax'`, `transport='telegram'`), винтажный слой версий
(правки/удаления) и LLM-разметку (sentiment/relevance/теги/каналы ставки). Живой LLM-прогон
отложен до мерджа `feat/mp-summary-llm-models` (пункт 1.8); в этом PR — код разметки и
офлайн-тесты.

**Architecture:** `news/` получает второй транспорт (`feedKind` RSS/Telegram), парсер публичного
веб-превью `t.me/s/<channel>`, таблицу версий `news_message_version` и таблицу разметки
`news_annotation`; `news_events` расширяется колонкой `content_hash`; витрины `v_news_flash`,
`v_news_message_versions`, `v_news_tags`; `v_news_events` получает `transport` и фильтр по
`title OR description`.

**Tech Stack:** Go, `goquery` (уже в проекте), `anthropic-sdk-go` v1.80.0 (уже в проекте),
`clickhouse-go/v2`, logrus; ClickHouse `ReplacingMergeTree`; без новых зависимостей.

**Spec:** `docs/superpowers/specs/2026-10-17-telegram-flash-feed-design.md` (связывающая власть;
этот план — детализация).

## Предусловие и база

- Ветка `feat/telegram-flash-feed` от `main`; первые коммиты — спека и этот план (до кода).
- Пункты роадмапа — **1.9** (этот PR) и **1.10** ✅ (живой прогон 2026-10-17, профиль `glm`);
  1.8 занят веткой `feat/mp-summary-llm-models`, её файлы и статус не трогаем.
- Живой прогон 1.9: `make all`, `make import STAT=news_watch`, `NEWS_WATCH_MODE=since|check`,
  MCP (`kimi_reader`), `make mcp-check`; LLM-переменные при приёмке 1.9 **не выставляются**.
  1.10 — отдельный прогон профилем `glm` (2026-10-17).
- Секреты — только `os.Getenv`; значения не печатать ни в логах, ни в ошибках, ни в отчёте.
- Вставки — только батчами `PrepareBatch → Append(err!) → Send`; батч принадлежит одной таблице.
- Сетевых вызовов в `init()` нет; HTTP — только `util.*`; DDL — канон `ARCHITECTURE.md` §6.2.

## Review Focus

1. **Стабильность ключа:** Telegram-title в `news_events` фиксируется по v1; правка первой строки
   не разъезжает ключ `(date, source, title)`, актуальный текст — в `v_news_flash`.
2. **Версии:** правка и tombstone дают новую версию, повтор — no-op; `first_seen`/`date` не
   перезаписываются; `SeenIDs` защищает медиа-сообщения от ложного tombstone.
3. **Изоляция лент:** сбой `t.me` не отменяет 7 RSS; `check` помечает неполный источник.
4. **LLM-деградация:** без env/при отказе провайдера — warn, импорт зелёный, догон следующим
   прогоном; ключ `(url, content_hash, kind, method, model, prompt_version, dimension, tag)`.
5. **Безопасность:** сырые `news_message_version`/`news_annotation` агенту закрыты; гранты
   только на витрины; секреты не в логах.
6. **Документы:** ARCHITECTURE/README/ROADMAP/KEY_RATE_CHANNELS/GOLD_THESIS синхронны; 1.8 не
   затронут; 1.10 ✅ после живой приёмки 2026-10-17.

---

### Task 1: Telegram-транспорт и парсер, 8 лент

**Files:**
- Create: `news/parse_telegram.go`
- Create: `news/parse_telegram_test.go`
- Create: `news/testdata/interfax_tg.html` (реальное окно, обрезанное; 20 сообщений)
- Modify: `news/news.go` (`feedKind`, `feed`, `feeds`, `fetchFeeds`)
- Modify: `news/parse.go` (`newsItem.ContentHash`, `Channel`, `MessageID`; `contentHash`)
- Modify: `news/news_test.go`, `news/parse_test.go` (пины 8 лент, httptest HTML)

**Interfaces:**
- Produces (используют Task 2–4):
  - `type feedKind int`; `const (feedRSS feedKind = iota; feedTelegram)`
  - `type telegramMessage struct { Channel string; MessageID uint64; Date time.Time; Title, Text, URL string }`
  - `type telegramPage struct { Messages []telegramMessage; SeenIDs map[uint64]struct{}; Skipped int }`
  - `func parseTelegramPage(source string, page []byte) (telegramPage, error)`
  - `func contentHash(title, description string) string`
  - `newsItem{ContentHash, Channel, MessageID}`

- [ ] **Step 1: Написать фикстуру.** Сохранить реальное окно `https://t.me/s/interfaxonline`
  (20 сообщений; можно обрезать до 8–10, сохранив `<time>`, `data-post`, текст, подпись
  `@interfaxonline`/`@ifax_go`, хотя бы одно медиа без текста). Файл — `news/testdata/interfax_tg.html`.
- [ ] **Step 2: Падающие тесты парсера** (`parse_telegram_test.go`):
  - 8–10 сообщений; у первого точные `channel`, `message_id`, `date` (UTC), `url`, `title`,
    `description`; `title` = первая строка; подпись `@handle` снята; `&quot;` раскрыт;
  - медиа без текста → `Skipped`, но ID в `SeenIDs`;
  - битый `data-post`/дата → `Skipped`, ID (если есть) в `SeenIDs`;
  - пустая страница/чужая разметка → ошибка; ноль распознанных сообщений → ошибка;
  - `contentHash` стабилен и различается при изменении текста.
- [ ] **Step 3: Прогнать — FAIL:** `go test ./news/ -run TestParseTelegram -v`.
- [ ] **Step 4: Реализовать `parse_telegram.go`** по спеке §4.1: goquery-селекторы, `data-post`,
  `time[datetime]`, inner HTML → текст, снятие подписи, title 300 рун, `SeenIDs`, ошибки.
- [ ] **Step 5: Реализовать `feedKind`/диспетчер.** `feeds()` — 8 записей:
  `interfax` (RSS), `interfax_tg` (Telegram, `Source='interfax'`), затем `rbc`, `cbr_news`,
  `cbr_press`, `tass`, `smartlab_lebedev`, `smartlab_spydell`. `fetchFeeds` — switch по Kind;
  RSS — `util.GetXML`, Telegram — `util.GetPage` + парсер. URL превью — переменная
  `feedInterfaxTelegramURL` для httptest.
- [ ] **Step 6: Обновить пины тестов** `news/news_test.go`/`parse_test.go`: 8 лент, порядок,
  `sources` (`interfax,interfax,…`), фикстура `interfax_tg.html`; `serveFixtures` отдаёт HTML
  по пути `/interfax_tg` (content-type `text/html`), RSS — как раньше.
- [ ] **Step 7: Прогнать — PASS:** `go test ./news/ -count=1`, `gofmt -l news/`.
- [ ] **Step 8: Commit:** `news: add interfax telegram feed`.

---

### Task 2: `content_hash`, `news_message_version`, версии и tombstone

**Files:**
- Create: `news/telegram_version.go`
- Create: `news/telegram_version_test.go`
- Modify: `news/schema.go` (DDL `news_events` + `content_hash`, ALTER, `news_message_version`)
- Modify: `news/schema_test.go` (пины DDL/ALTER)
- Modify: `news/news.go` (`newsInsert`, `insertItems`, чтение версий, `diffTelegram`, запись)
- Modify: `news/news_test.go` (fake conn, композиция Import)

**Interfaces:**
- Produces:
  - `type telegramVersion struct { Channel string; MessageID uint64; Version uint32; Date time.Time; Source, Title, URL, Description, ContentHash string; FirstSeen time.Time; Deleted uint8 }`
  - `func diffTelegram(existing []telegramVersion, current []newsItem, seen map[uint64]struct{}, windowMin, windowMax, now time.Time) (items []newsItem, versions []telegramVersion, stats telegramDiffStats)`
  - `func readTelegramVersions(ctx, conn, channel string, since time.Time) ([]telegramVersion, error)`
  - SQL-константы: `telegramVersionsQuery`, `telegramVersionInsert`.

- [ ] **Step 1: Падающие тесты `diffTelegram`** (чистая функция, без БД):
  - пустая история → у каждого сообщения v1, title = первая строка;
  - та же история/hash → новых версий 0, items содержат текущие сообщения;
  - изменённый hash → v2, `news_events` title = title v1;
  - известное сообщение в окне, которого нет в `seen` → tombstone v+1 (`deleted=1`,
    `content_hash=''`, `description=''`);
  - сообщение за `windowMin`/`windowMax` — не tombstone;
  - медиа-сообщение в `SeenIDs` — не tombstone;
  - `first_seen` нового сообщения = `now`, `date` = дата публикации.
- [ ] **Step 2: Прогнать — FAIL:** `go test ./news/ -run TestDiffTelegram -v`.
- [ ] **Step 3: Реализовать `telegram_version.go`** и SQL-константы; `readTelegramVersions`
  читает `channel`/`date >= since`, сортирует `message_id, version`; ошибки Scan/rows.Err
  проверяются.
- [ ] **Step 4: Схема.** `newsEventsDdl` + `content_hash String` после `description`;
  `newsAlterStatements` — два стейтмента (`description`, `content_hash`); DDL
  `news_message_version` — копия канона; обновить `newsSchemaStatements`.
- [ ] **Step 5: Запись.** `newsInsert` — `(date, source, title, url, description, content_hash,
  sentiment)`; `insertItems` передаёт `it.ContentHash`; `Append` с проверкой. В `Import`:
  Telegram-ленты → прочитать версии → `diffTelegram` → добавить текущие items в общий батч
  `news_events` → записать новые версии отдельным батчем (`telegramVersionInsert`), включая
  tombstone. Порядок: `news_events` → версии. Логи: `interfax_tg: позиций N, новых версий M,
  удалений K`.
- [ ] **Step 6: Обновить fake conn/композицию** `news_test.go`: query версий, батч версий,
  tombstone; сбой версий не отменяет запись текущих; `since` upsert текущих Telegram.
- [ ] **Step 7: Прогнать — PASS:** `go test ./news/ -count=1`, `gofmt -l news/`.
- [ ] **Step 8: Commit:** `news: version telegram messages and detect deletions`.

---

### Task 3: `news_annotation`, реестр тегов, LLM-разметка (офлайн)

**Files:**
- Create: `news/tags.go`, `news/tags_test.go`
- Create: `news/annotation.go`, `news/annotation_test.go`
- Create: `news/annotation_llm.go`, `news/annotation_llm_test.go`
- Modify: `news/schema.go` (DDL `news_annotation`)
- Modify: `news/schema_test.go` (пин DDL)
- Modify: `news/news.go` (кандидаты, вызов разметки, запись батча, логи)
- Modify: `news/news_test.go` (fake conn, композиция)

**Interfaces:**
- Produces:
  - `type newsTag struct { Dimension, Slug, Title, Direction string }`; `var newsTags []newsTag`
  - `const newsPromptVersion = 1`
  - `func newsLLMConfigFromEnv() (baseURL, apiKey, model string, ok bool)`
  - `func newsAnnotationCandidates(ctx, conn, model string, promptVersion uint16) ([]newsItem, error)`
  - `func validateNewsAnnotations(payload newsAnnotationPayload, inputs []newsItem) ([]newsAnnotationRow, error)`
  - `func tagTelegramLLM(ctx, cfg, inputs []newsItem) ([]newsAnnotationRow, error)`
  - SQL: `newsAnnotationInsert`, `newsAnnotationCandidatesQuery`.
- Env: `LLM_BASE_URL`, `LLM_API_KEY`, `NEWS_LLM_MODEL` (**живой прогон не выполняется**).

- [ ] **Step 1: Реестр тегов + тест.** Все `dimension` из спеки §4.4, 11 каналов ставки;
  уникальность `(dimension, slug)`, непустые title, `cb_channel` с направлением; тест пинит
  количество и ключевые слаги (`person:trump`, `country:china`, `event:energy_transition`,
  `event:deglobalization`, `cb_channel:budget_impulse`).
- [ ] **Step 2: Падающие тесты валидации** (`annotation_test.go`):
  - валидный payload → doc + tag строки; sentiment/relevance clamp; summary trim;
  - неизвестный URL/тег/dimension/direction — drop с warn;
  - `evidence` не подстрока — drop; дубли — collapse; >5 тегов — обрезка;
  - пустой tags → doc-строка без тегов; пустой ответ/нет URL → doc не пишется (ретрай).
- [ ] **Step 3: Падающие тесты транспорта** (`annotation_llm_test.go`, mock Anthropic):
  - 200 валидный JSON; fenced JSON; 500; битый JSON → один ретрай; timeout; пустой text-блок;
  - отсутствие env → `ok=false`, LLM-шаг пропущен (warn), импорт не падает.
- [ ] **Step 4: Прогнать — FAIL:** `go test ./news/ -run 'TestNewsTag|TestValidateNews|TestNewsLLM' -v`.
- [ ] **Step 5: Реализовать `tags.go`/`annotation.go`** по спеке §4.4: промпт (system+user),
  парсинг JSON, валидация, `newsAnnotationRow`; `annotation_llm.go` — SDK-транспорт
  (base без `/v1`, `x-api-key`, `WithMaxRetries(0)`, 65 с), один ретрай формата.
- [ ] **Step 6: Схема + запись.** DDL `news_annotation` — копия канона; `newsAnnotationInsert`
  (явный список колонок); батч после `news_events`/версий; кандидаты — SQL из спеки §4.4
  (`url LIKE 'https://t.me/%'`, `content_hash != ''`, 35 дней — окно покрывает edisclosure,
  `NOT IN` по паре model/prompt, limit 50); батч 10; ошибка провайдера — warn и остановка
  LLM-шага.
- [ ] **Step 7: Композиция Import.** Без env — warn и зелёный; с фейковым LLM — батч разметки;
  ошибка LLM не отменяет запись сообщений/версий.
- [ ] **Step 8: Прогнать — PASS:** `go test ./news/ -count=1`, `gofmt -l news/`.
- [ ] **Step 9: Commit:** `news: annotate telegram flash with llm tags`.

---

### Task 4: Витрины, фильтр, гранты, MCP, документы

**Files:**
- Modify: `news/schema.go` (view statements: `v_news_events`, `v_news_flash`,
  `v_news_message_versions`, `v_news_tags`; комментарии)
- Modify: `news/schema_test.go` (пины витрин/фильтра)
- Modify: `sql/mcp_kimi_reader.sql` (3 гранта)
- Modify: `scripts/mcp_check.py` (проверки)
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_KEY_RATE.md`,
  `docs/KEY_RATE_CHANNELS.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

**Interfaces:**
- Produces:
  - `v_news_events`: + `transport` (`multiIf(url LIKE 'https://t.me/%','telegram','rss')`),
    фильтр `match(title, newsFilter) OR match(description, newsFilter)`;
  - `v_news_flash`: последняя версия Telegram + аннотации (doc/tag), `tags
    Array(Tuple(dimension, tag, direction))`;
  - `v_news_message_versions`: все версии, включая tombstone;
  - `v_news_tags`: реестр из `newsTags`.

- [ ] **Step 1: Витрины + тесты.** `newsViewStatements` — view + комментарии таблицы и каждой
  колонки; тест пинит число стейтментов, `transport`, `match(title) OR match(description)`,
  `tags`, `v_news_tags`; `newsFilter` + `Китай|китайск|деглобализац|энергопереход|энергетическ|тариф`.
- [ ] **Step 2: Гранты.** `GRANT SELECT` на `v_news_flash`, `v_news_message_versions`,
  `v_news_tags` в `sql/mcp_kimi_reader.sql`; сырые таблицы не выдаются.
- [ ] **Step 3: MCP-проверки.** `scripts/mcp_check.py`: обе витрины читаются и непусты,
  `v_news_tags` содержит все слаги, сырые `news_message_version`/`news_annotation` →
  `ACCESS_DENIED`; LLM-теги — информационная строка (warn, не fail).
- [ ] **Step 4: Документы.** ARCHITECTURE §2/§3/§5/§6.2/§6.3/§7/§8 п. 14; README; ROADMAP 1.9/1.10;
  KEY_RATE_CHANNELS §9; GOLD_THESIS news section. 1.8 не трогать.
- [ ] **Step 5: Прогнать — PASS:** `go test ./news/ -count=1`, `make all`, `make mcp-check`
  (после живого прогона).
- [ ] **Step 6: Commit:** `news: publish telegram flash views and mcp checks`.

---

### Task 5: Живая приёмка 1.9 и PR

- [ ] **Step 1:** `make all` — зелёный.
- [ ] **Step 2:** `make import STAT=news_watch` (all) — 8 лент, `interfax_tg: позиций 20`,
  `Imported N rows`, `новых версий 20`; LLM-шаг — warn (env не выставлен).
- [ ] **Step 3:** `NEWS_WATCH_MODE=since make import STAT=news_watch` — upsert текущих,
  `новых версий 0`; повтор стабилен.
- [ ] **Step 4:** `NEWS_WATCH_MODE=check make import STAT=news_watch` — покрытие, дыр нет.
- [ ] **Step 5:** MCP (`kimi_reader`): `v_news_flash`, `v_news_message_versions`, `v_news_tags`
  читаются; сырые таблицы → `ACCESS_DENIED`; `make mcp-check` — зелёный.
- [ ] **Step 6:** Обновить статусы 1.9 ✅ / 1.10 ✅ (приёмка 2026-10-17) в роадмапе; протокол — комментарием в PR.
- [ ] **Step 7:** Финальное ревью ветки (`deepseek/deepseek-flash`); PR.
- [x] **Step 8 (✅ выполнен 2026-10-17):** профиль `NEWS_LLM_PROFILE=glm`,
  `zai-org/GLM-5.3-Flash`, 44/44 сообщения `v_news_flash`, 25 с тегами, 39 тегов;
  статус 1.10 — ✅.

**Модели субагентов:** T1–T4 — `deepseek/deepseek-flash`; task-review — `anthropic/haiku`;
финальное ревью — `deepseek/deepseek-flash`. В каждый coder-промпт — блок AGENTS.md:
> У тебя есть инструменты `mcp__jetbrains__*` (GoLand MCP) и `mcp__clickhouse__*`. Используй
> `mcp__jetbrains__*` для проверки правок и навигации вместо grep и лишней сборки; SQL к
> ClickHouse — только через `mcp__clickhouse__run_query`. Если вдруг MCP-инструментов в твоём
> наборе нет — скажи об этом в отчёте (`DONE_WITH_CONCERNS`), не молчи.

**Global Constraints:**
- Секреты только `os.Getenv`; значения не печатать.
- Вставки только батчами; `Append` с проверкой ошибки.
- Сетевых вызовов в `init()` нет; HTTP — `util.*`.
- `make all` зелёный после каждого таска; `gofmt -l` пустой.
- Спека и план — первые коммиты ветки; один PR в конце.
