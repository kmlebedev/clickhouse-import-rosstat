# news-source-onboarding — план реализации

> **Для агентов:** исполнять через `superpowers:subagent-driven-development` (диспатч субагентов); нативное исполнение — только если пользователь явно выбрал inline. Шаги отмечать `- [ ]`.

**Goal:** скил `news-source-onboarding` + шесть новых Telegram-лент и общий RSS Смартлаба в `news_watch`; bootstrap LLM-разметки нового потока; docs и приёмка.

**Architecture:** конфиг лент — статический `feeds()` в `news/news.go`; транспорт и парсинг переиспользуются; схема `news_events`/`news_message_version`/`news_annotation` generic и не меняется. Витрины публикует сам импортёр. Приёмка — живой прогон `news_watch`, bootstrap-разметка по runbook PR #176, MCP-проверки.

**Tech Stack:** Go, `encoding/xml`, `goquery`, `httptest`, logrus, ClickHouse (`PrepareBatch`), dagu, MCP.

**Spec:** `docs/superpowers/specs/2026-10-18-news-source-onboarding-design.md`

## Global Constraints

- Новых внешних зависимостей нет; сеть — только `util.GetXML`/`util.GetPage`.
- Вставка — только батчами; DDL `IF NOT EXISTS`; витрины `CREATE OR REPLACE VIEW`.
- Схема не меняется; новые таблицы/витрины/гранты не заводятся.
- `feeds()` — 16 лент в фиксированном порядке; `source` группирует провайдеров, `channel`/`url` различают ленты.
- Тесты офлайн: сеть только `httptest`, БД не нужна.
- Bootstrap-разметка — обязательный шаг: `NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch`; критерий 0 аннотируемых с `model=''`, повтор no-op.
- Протокол живой приёмки — комментарием в PR; статус `infra/2.3` — `✅ сделан этим PR` до мержа.

---

### Task 1: Пункт роадмапа, issue, спека и план

**Files:**
- Modify: `docs/ROADMAP_INFRA.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`
- Create: `docs/superpowers/specs/2026-10-18-news-source-onboarding-design.md`, `docs/superpowers/plans/2026-10-18-news-source-onboarding.md`

- [ ] **Step 1:** в `docs/ROADMAP_INFRA.md` §3 (фаза 2) добавить пункт **2.3** после 2.2: скил, первый пакет #46, критерий приёмки, ссылки на spec/plan.
- [ ] **Step 2:** в `docs/GOLD_THESIS_DATA_ROADMAP.md` follow-up 4 добавить: Telegram-пакет #46 выполняется в `infra/2.3`, остальные источники остаются ⏳.
- [ ] **Step 3:** создать issue `[infra/2.3] Механизм добавления новостных источников — скил`; поля Project V2: `Roadmap=infra`, `Phase=2`, `Item=2.3`, `Status=In Progress`, `Branch=feat/news-source-onboarding`, `Spec / Plan`, `Acceptance`.
- [ ] **Step 4:** в #46 комментарий о зависимости от `infra/2.3`; Project #46 — `Depends on` = infra issue, `Status=In Progress`.
- [ ] **Step 5:** commit docs.

**Acceptance:** issue и Project видны через MCP; spec/plan в ветке; п. 2.3 в роадмапе.

---

### Task 2: Скил — RED и GREEN

**Files:**
- Create: `.kimi-code/skills/news-source-onboarding/SKILL.md`
- Modify: `docs/superpowers/specs/2026-10-18-news-source-onboarding-design.md` (раздел о baseline)

- [ ] **Step 1 (RED):** read-only `plan`-субагент: «добавить `https://t.me/markettwits` в `news_watch`; не редактируй файлы; составь пошаговый план». Записать, что пропущено (фикстуры/пины, комментарии витрин, bootstrap, `dagu validate`, MCP, протокол).
- [ ] **Step 2:** написать скил по структуре спеки §4.1: 11 шагов, правила имён, bootstrap, таблица 16 лент, «чего не делать».
- [ ] **Step 3 (GREEN):** повторить промпт с текстом скила; проверить, что названы все 11 шагов и bootstrap; пробелы закрыть.
- [ ] **Step 4:** commit skill + spec baseline.

**Acceptance:** скил виден в project skills; `wc -c` ≤ ≈16 КБ; GREEN-прогон покрывает чек-лист.

---

### Task 3: Telegram-ленты

**Files:**
- Modify: `news/news.go`, `news/news_test.go`, `news/schema.go`, `dagu/news.yaml`
- Create: `news/testdata/ifax_go_tg.html`, `markettwits_tg.html`, `newssmartlab_tg.html`, `russianmacro_tg.html`, `marketoverview_tg.html`, `mozgovikresearch_tg.html`

- [ ] **Step 1:** снять живые HTML-фикстуры (3–4 сообщения), сохранить разметку.
- [ ] **Step 2:** добавить URL-переменные и `feeds()` в порядке: `interfax, interfax_tg, ifax_go_tg, edisclosure_tg, rbc, cbr_news, cbr_press, tass, smartlab, smartlab_lebedev, smartlab_spydell, newssmartlab_tg, markettwits_tg, russianmacro_tg, marketoverview_tg, mozgovikresearch_tg`.
- [ ] **Step 3:** обновить `pointFeedsAt`, `TestFetchFeeds...` (9→16), `TestFeedsHaveFixtures` (9→16), тесты «все Telegram-ленты»/«все упали», добавить пины новых фикстур.
- [ ] **Step 4:** обновить комментарии `v_news_events`/`v_news_flash`/`v_news_message_versions`; `dagu/news.yaml` — состав и окна, расписание не менять.
- [ ] **Step 5:** `go test ./news/... -count=1`; саботаж: убрать ленту → падает пин; подменить source → падает пин.

**Acceptance:** 16 лент, все фикстуры разбираются, пины зелёные.

---

### Task 4: Общий RSS Смартлаба

**Files:**
- Create: `news/testdata/smartlab.rss`
- Modify: `news/news.go`, `news/news_test.go`, `news/parse_test.go`

- [ ] **Step 1:** снять `https://smart-lab.ru/rss/` в `news/testdata/smartlab.rss`.
- [ ] **Step 2:** добавить `feedSmartlabURL` и запись `{Key: "smartlab", Source: "smartlab", Kind: feedRSS}` перед персональными блогами.
- [ ] **Step 3:** обновить пины числа/порядка/источников и таблицу фикстур в `parse_test.go`.
- [ ] **Step 4:** проверить пересечение с персональными лентами по `(date, title)`; результат — в протокол/`LIMITATIONS.md`.
- [ ] **Step 5:** `go test ./news/... -count=1`.

**Acceptance:** `smartlab.rss` разбирается, `source='smartlab'`, пины зелёные.

---

### Task 5: Витрины, MCP, docs

**Files:**
- Modify: `news/schema_test.go` (если пины), `scripts/mcp_check.py`, `ARCHITECTURE.md`, `README.md`, `docs/IMPORTERS.md`, `docs/OPERATIONS.md`, `docs/LIMITATIONS.md`, `docs/ROADMAP_INFRA.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

- [ ] **Step 1:** `news/schema_test.go` — обновить пины комментариев/состава, если есть.
- [ ] **Step 2:** `scripts/mcp_check.py` — агрегированный чек новых каналов: `count()` и `countIf(model != '')`; подсказка про `make import` и bootstrap.
- [ ] **Step 3:** docs по `sync-readme-architecture`: §2/§6.1/§6.3/§7, README, IMPORTERS, OPERATIONS, LIMITATIONS (окна, дубли, bootstrap).
- [ ] **Step 4:** `dagu validate dagu/news.yaml`.
- [ ] **Step 5:** commit.

**Acceptance:** docs находят новые каналы/16 лент; `dagu validate` exit 0.

---

### Task 6: `make all`

- [ ] **Step 1:** `make all` — gofmt, golangci-lint, vet, `go test -race`, обе сборки.
- [ ] **Step 2:** `go test ./news/... -count=1`, `go test ./util/...`.

**Acceptance:** зелёный `make all`.

---

### Task 7: Живая приёмка и bootstrap-разметка

- [ ] **Step 1:** `make env-check`, `make ch-status`.
- [ ] **Step 2:** `make import STAT=news_watch` — 16 лент, счётчики, повтор идемпотентен.
- [ ] **Step 3:** `NEWS_WATCH_MODE=since make import STAT=news_watch`; `NEWS_WATCH_MODE=check make import STAT=news_watch` — дыр нет.
- [ ] **Step 4:** bootstrap: `NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch`; проверить `annotated = annotatable` в `v_news_events`, `annotated = messages` в `v_news_flash`, doc/tag строки v2; повтор — no-op; токены/стоимость записать.
- [ ] **Step 5:** MCP-запросы: новые каналы/источники, версии, RSS Смартлаба, сырая таблица закрыта; `make mcp-check`.
- [ ] **Step 6:** `dagu validate`; дубли Смартлаба.
- [ ] **Step 7:** commit приёмочных правок docs.

**Acceptance:** все новые источники видны, разметка 100 %, no-op, MCP зелёный.

---

### Task 8: PR и трекер

- [ ] **Step 1:** `make all` и приёмка зелёные; дерево чистое.
- [ ] **Step 2:** обновить статус `infra/2.3` — `✅ сделан этим PR`; #46 — комментарий о выполнении.
- [ ] **Step 3:** открыть PR: `Closes #46`, `Roadmap: infra/2.3`, spec/plan, таблица лент, команды проверки.
- [ ] **Step 4:** Project: `Status=In Review`, `PR`; после мержа — `Done`.
- [ ] **Step 5:** протокол живой приёмки — комментарием в PR.
- [ ] **Step 6:** финальное ревью `superpowers:requesting-code-review` на `deepseek/deepseek-flash`.

**Acceptance:** PR открыт, протокол есть, статусы сходятся с роадмапом.
