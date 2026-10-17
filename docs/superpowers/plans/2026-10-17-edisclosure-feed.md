# Корпоративный Telegram-канал edisclosure — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development`
> (or `superpowers:executing-plans` only if the owner explicitly chose inline execution). Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** добавить в `news_watch` второй Telegram-источник — корпоративные раскрытия
`https://t.me/s/edisclosure` (`edisclosure_tg`, `source='edisclosure'`), не меняя схему:
`v_news_flash` уже отдаёт `channel`, версии и LLM-разметку.

**Architecture:** вторая запись `feedTelegram` в `feeds()`; общий парсер `parseTelegramPage`,
общая таблица версий и разметки; полный корпоративный поток читается из
`v_news_flash WHERE channel='edisclosure'`. Отдельная витрина не заводится.

**Spec:** `docs/superpowers/specs/2026-10-17-edisclosure-feed-design.md`.

**База:** ветка `feat/edisclosure-feed` стоит **поверх** `feat/telegram-flash-feed` (пункт 1.9,
PR #92); PR 1.11 — stacked, base `feat/telegram-flash-feed`, merge после #92. Первые коммиты —
спека и этот план.

## Global Constraints

- Секреты только `os.Getenv`; значения не печатать.
- Вставки только батчами; `Append`/`Scan` с проверкой ошибок.
- Сетевых вызовов в `init()` нет; HTTP — только `util.*`.
- `make all` зелёный после каждого таска; `gofmt -l` пустой.
- Не трогать `cbr/`, `dagu/` (кроме комментария в Task 2), схему БД, LLM-код.
- В каждый coder-промпт — блок AGENTS.md:
> У тебя есть инструменты `mcp__jetbrains__*` (GoLand MCP) и `mcp__clickhouse__*`. Используй
> `mcp__jetbrains__*` для проверки правок и навигации вместо grep и лишней сборки; SQL к
> ClickHouse — только через `mcp__clickhouse__run_query`. Если вдруг MCP-инструментов в твоём
> наборе нет — скажи об этом в отчёте (`DONE_WITH_CONCERNS`), не молчи.

---

### Task 1: Лента `edisclosure_tg`, фикстура и тесты

**Files:**
- Modify: `news/news.go` (`feedEdisclosureURL`, `feeds()`, комментарии «восемь» → «девять»)
- Create: `news/testdata/edisclosure_tg.html` (4 реальных сообщения из `/tmp/tg_edisclosure.html`)
- Modify: `news/news_test.go` (`pointFeedsAt`, 9 лент, порядок, sources, `TestFetchFeeds`,
  `TestFeedsHaveFixtures`, `fixtureInventory`)
- Modify: `news/parse_telegram_test.go` (пин первого сообщения edisclosure — если фикстура
  разбирается общим тестом; отдельный тест не обязателен)

- [ ] **Step 1:** Добавить `feedEdisclosureURL = "https://t.me/s/edisclosure"` и запись
  `{Key: "edisclosure_tg", Source: "edisclosure", URL: feedEdisclosureURL, Kind: feedTelegram}`
  сразу после `interfax_tg`. Обновить комментарии `feeds()`/`fetchFeeds` («девять лент»).
- [ ] **Step 2:** Фикстура: 4 подряд идущих реальных сообщения (`data-post="edisclosure/…"`,
  `time`, текст) без правки разметки; минимум одно с `&quot;`/ссылкой. Размер ≤ 25 КБ.
- [ ] **Step 3:** Тесты: `pointFeedsAt` — массив 9 URL и подмена `feedEdisclosureURL`;
  `TestFetchFeeds` — 9 результатов, сбой `cbr_news` не задевает `edisclosure_tg`;
  порядок `interfax,interfax_tg,edisclosure_tg,rbc,cbr_news,cbr_press,tass,smartlab_lebedev,smartlab_spydell`;
  sources `interfax,interfax,edisclosure,rbc,cbr,cbr,tass,smartlab,smartlab`;
  `TestFeedsHaveFixtures` — 9, `edisclosure_tg.html` разбирается в >0 сообщений;
  `fixtureInventory` — capacity 9.
- [ ] **Step 4:** `gofmt -l news/`, `go vet ./news/`, `go test -count=1 ./news/`, `go build ./...`.
- [ ] **Step 5:** Commit: `news: add edisclosure telegram feed`.

### Task 2: Комментарии витрин, MCP-проверка, документы

**Files:**
- Modify: `news/schema.go` (`v_news_flash` table/`source`/`channel` comments; `v_news_events`
  `source`/table comment; `news_message_version` `channel` comment)
- Modify: `news/schema_test.go` (если пины проверяют текст комментариев)
- Modify: `scripts/mcp_check.py` (`v_news_flash WHERE channel='edisclosure'` > 0)
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_KEY_RATE.md` (1.11 ⏳),
  `docs/KEY_RATE_CHANNELS.md` §9, `docs/GOLD_THESIS_DATA_ROADMAP.md`, `dagu/news.yaml` (комментарий)

- [ ] **Step 1:** `v_news_flash` — «Telegram-каналы: interfaxonline (молния) и edisclosure
  (корпоративные раскрытия)»; `source` — `interfax | edisclosure`; `channel` —
  `interfaxonline | edisclosure`; `v_news_events` — source/table comment + edisclosure;
  `news_message_version.channel` — `interfaxonline | edisclosure`.
- [ ] **Step 2:** `scripts/mcp_check.py`: `SELECT count() AS n FROM v_news_flash WHERE channel = 'edisclosure'`
  > 0, с подсказкой «прогнать make import STAT=news_watch»; существующие проверки не менять.
- [ ] **Step 3:** Документы: 8→9 лент, `edisclosure` источник/окно ≈30 дней, пример
  `SELECT … FROM v_news_flash WHERE channel='edisclosure'`; ROADMAP 1.11 ⏳ до приёмки;
  KEY_RATE_CHANNELS §9 — корпоративный поток; GOLD_THESIS — 9 лент; dagu-комментарий.
- [ ] **Step 4:** `git diff --check`; `go test -count=1 ./news/`; `python3 -m py_compile scripts/mcp_check.py`.
- [ ] **Step 5:** Commit: `docs: add edisclosure corporate feed`.

### Task 3: Живая приёмка 1.11

- [ ] **Step 1:** `make all` — зелёный.
- [ ] **Step 2:** `make import STAT=news_watch` (all): 9 лент; `edisclosure_tg: позиций 20,
  новых версий 20, удалений 0`; `Imported N rows`; LLM warn.
- [ ] **Step 3:** `NEWS_WATCH_MODE=since` — upsert обоих каналов, `новых версий 0`;
  `NEWS_WATCH_MODE=check` — `edisclosure` в покрытии, дыр нет.
- [ ] **Step 4:** MCP: `v_news_flash` — 40 (20+20), `channel='edisclosure'` — 20;
  `v_news_message_versions` — 40; сырые таблицы закрыты; `make mcp-check` — зелёный.
- [ ] **Step 5:** ROADMAP 1.11 ✅ с числами; протокол — комментарием в PR; финальное ревью
  (`deepseek/deepseek-flash`); stacked PR (base `feat/telegram-flash-feed`).

**Модели:** Task 1–2 — `deepseek/deepseek-flash` (multi-file, тесты); task-review —
`anthropic/haiku`; финальное ревью — `deepseek/deepseek-flash`.
