# LLM-разметка RSS-потока батчами на `zai-org/GLM-5.3-Flash` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` (или `superpowers:executing-plans` только если владелец явно выбрал inline-исполнение). Шаги используют чекбоксы (`- [ ]`). Пункт роадмапа — `key-rate/phase-2-rss-llm-annotation`, issue [#122](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/122).

**Goal:** размечать RSS-позиции, которые уже видит агент в `v_news_events`, моделью `zai-org/GLM-5.3-Flash` батчами: `sentiment`, `relevance`, `summary`, теги; отдать разметку агенту в той же витрине `v_news_events`, не меняя схему `news_events` и не заполняя `news_events.sentiment`.

**Architecture:** кандидаты читаются из `news_events FINAL` тем же широким фильтром, что и `v_news_events` (все Telegram-сообщения + RSS, проходящие `newsFilter`); разметка пишется в generic `news_annotation` (`kind='doc'|'tag'`, `model`/`prompt_version` в ключе); `v_news_events` получает LEFT JOIN последней doc-строки и её tag-строк. Промпт — редакция 2, реестр — 46 тегов. Батч — до 10 сообщений и до 40 КБ текста. Штатный предел бэклога — 50 позиций за прогон, разовый бэкфилл — `NEWS_LLM_BACKLOG_LIMIT`. Транспорт — существующий OpenAI-совместимый `/v1/chat/completions` профиля `glm`; новых зависимостей нет.

**Tech Stack:** Go, `clickhouse-go/v2` (`PrepareBatch`), logrus, существующий `util.HttpClient`, ClickHouse `ReplacingMergeTree`, Makefile/dagu. Модель — `zai-org/GLM-5.3-Flash` через `NEWS_LLM_PROFILE=glm` и `GLM_LLM_*`.

**Spec:** [2026-10-18-rss-llm-annotation-design.md](../specs/2026-10-18-rss-llm-annotation-design.md) — связывающая власть; этот файл — детализация.

## Ветка и предусловия

- Ветка `feat/rss-llm-annotation` от актуального `origin/main`. Текущая ветка `roadmap/market-signals` — чужая работа другой сессии; её не трогаем и в PR не тащим.
- Первые коммиты ветки — спека и план (до кода). PR — один, в конце, после зелёного `make all` и живой приёмки.
- `~/.config/rosstat/env`: `GLM_LLM_BASE_URL`/`GLM_LLM_API_KEY`/`GLM_LLM_MODEL` уже заданы; `NEWS_LLM_PROFILE` не задан. Живая приёмка запускается с `NEWS_LLM_PROFILE=glm` в командной строке; env-файл не правим.
- ClickHouse поднимает владелец. Если `make ch-status` не отвечает — остановиться и попросить владельца, `make ch-up` не запускать.
- Все секреты — только `os.Getenv`; значения не печатать ни в логах, ни в отчётах. В предупреждениях — только имена переменных.

## Решения, зафиксированные владельцем

| Вопрос | Решение |
|---|---|
| Охват | Только позиции, видимые в `v_news_events`: все Telegram-сообщения (сохраняем поведение 1.10) + RSS, проходящие `newsFilter`. Сырой RSS вне фильтра не размечается. |
| `news_events.sentiment` | Остаётся NULL: сырая таблица — ingestion-слой; `sentiment`/`relevance`/`summary`/`tags` живут в `news_annotation` и отдаются через `v_news_events`. |
| Бэклог | Штатно 50 позиций за прогон; разовый бэкфилл — `NEWS_LLM_BACKLOG_LIMIT` (1…1000), по умолчанию 50. |
| Витрина | Новых витрин не заводим; расширяем существующую `v_news_events` (одна строка на позицию). |
| График | `dagu/news.yaml` не меняется: каждые 30 минут, режим `since`, LLM-шаг после записи. |

## Global Constraints

- Вставки только батчами `PrepareBatch → Append(err!) → Send`; построчный `Exec` в цикле запрещён.
- Сетевых вызовов в `init()` нет; HTTP — только через существующий `util.HttpClient`.
- `news_events` и `news_annotation` DDL не меняются: `news_annotation` generic, `prompt_version` входит в ключ.
- `news_events.sentiment` не заполняется; в `v_news_events` NULL означает «не размечено», а не «нейтрально».
- Смена модели/промпта не затирает старую разметку: `(model, prompt_version)` — часть ключа `ReplacingMergeTree`.
- Без LLM env или при отказе провайдера импорт остаётся зелёным: warn и догон следующим прогоном.
- Модели субагентов — только явным `model:`; в каждый coder-промпт — блок AGENTS.md про `mcp__jetbrains__*`/`mcp__clickhouse__*`.

## Review Focus

1. **Охват:** кандидатная выборка совпадает с `v_news_events` (RSS по тому же `newsFilter`), Telegram не сужен; окно 35 дней снято осознанно, предел — backlog.
2. **Идемпотентность:** повторный прогон не платит за уже размеченные пары; смена `prompt_version=2` добавляет строки рядом с v1, не затирая их.
3. **Батчи:** 10 сообщений и 40 КБ — оба предела соблюдаются; длинный пост smart-lab не ломает батч и не раздувает запрос.
4. **Витрина:** `v_news_events` остаётся одна строка на позицию; `count()` не меняется; типы `sentiment`/`relevance` — `Nullable(Float32)`, `summary` — `Nullable(String)`.
5. **Секреты:** ни одного значения ключа в логах/ошибках; usage-логи содержат только токены.
6. **Стоимость:** фактические токены в логе, оценка USD по опубликованному тарифу с датой; цифры — в research-заметке и протоколе PR.

---

### Task 1: Спека и план — первые коммиты ветки

**Files:**
- Create: `docs/superpowers/specs/2026-10-18-rss-llm-annotation-design.md`
- Create: `docs/superpowers/plans/2026-10-18-rss-llm-annotation.md`

**Содержание спеки (обязательные разделы):**
- Пробел и цель: issue #122, §4.2 №24; сейчас `news_annotation` заполняется только Telegram-молнией.
- Охват и не-охват: только `v_news_events`; сырой RSS вне фильтра, заполнение `news_events.sentiment`, новые витрины — вне объёма.
- Источник и объём: снимок `v_news_events` на 2026-10-18 — 312 строк (283 RSS + 29 Telegram), окна лент 20–100 позиций; история копится с первого прогона.
- Дизайн: кандидатный SQL, батчи 10/40 КБ, промпт v2, 46 тегов, `v_news_events` LEFT JOIN, NULL-семантика, backlog env.
- Модель и стоимость: `zai-org/GLM-5.3-Flash`, тариф Joingonka от 2026-10-17 ($0,0076/1M input, $0,023/1M output; +10% network, +5% конвертация), предварительная оценка и формула; фактические токены — из логов приёмки.
- Риски: длинные посты, отказ провайдера, `LIMIT 1 BY` на маленькой таблице, повторная разметка Telegram при смене редакции.
- Критерий приёмки — из раздела ниже.

**Steps:**
- [ ] **Step 1:** Написать спеку и план.
- [ ] **Step 2:** Самопроверка спеки: нет `TBD`/`TODO`, нет противоречий, охват — один план, требования однозначны.
- [ ] **Step 3:** `git switch -c feat/rss-llm-annotation origin/main`; закоммитить спеку и план первыми коммитами.
- [ ] **Step 4:** Обновить issue #122: в шапке тела заполнить `Acceptance`, `Spec`, `Plan`; комментарием зафиксировать три решения владельца (охват, sentiment, backlog). Проверить Project V2 (`projects_list`); если проект доступен — добавить issue и заполнить `Roadmap=key-rate`, `Phase=2`, `Item=phase-2-rss-llm-annotation`, `Status=In Progress`, `Branch`, `Spec / Plan`. Если проекта нет — пропустить по скилу `github-tracker` §9.

---

### Task 2: Кандидаты RSS + `NEWS_LLM_BACKLOG_LIMIT`

**Files:**
- Modify: `news/news.go` (query, `readNewsAnnotationCandidates`, `Import`)
- Modify: `news/annotation_llm.go` (константы и парсер backlog)
- Test: `news/news_test.go`, `news/annotation_llm_test.go`

**Interfaces:**
- Produces (используют Task 3–4):
  - `func newsAnnotationCandidatesQuery(limit int) string` — SQL без окна 35 дней; `match(title, '<newsFilter>') OR match(description, '<newsFilter>')`; Telegram — по `url LIKE 'https://t.me/%'`; `LIMIT` из аргумента.
  - `func newsLLMBacklogLimitFromEnv() int` — `NEWS_LLM_BACKLOG_LIMIT` (1…1000), пусто/ошибка → 50 + warn.
  - `readNewsAnnotationCandidates(ctx, conn, model, promptVersion, limit)`.

**Steps:**
- [ ] **Step 1: Падающие тесты.**
  - `TestNewsAnnotationCandidatesQueryCoversRSS`: запрос содержит `match(title, '<newsFilter>')`, `match(description, '<newsFilter>')`, `url LIKE 'https://t.me/%'`, `LIMIT 50`; не содержит `INTERVAL 35 DAY`.
  - `TestNewsLLMBacklogLimitFromEnv`: пусто → 50; `"120"` → 120; `"0"`, `"-1"`, `"1001"`, `"abc"` → 50.
  - `TestImportUsesBacklogLimit`: `NEWS_LLM_BACKLOG_LIMIT=123` → записанный вызов кандидатов содержит `LIMIT 123`.
  - Обновить фейк `newsFakeConn.Query`: определять кандидатов по префиксу (`newsAnnotationCandidatesQueryPrefix`), а не по точному равенству; аргументы остаются `(model, promptVersion)` — фильтр вшит в SQL литералом.
- [ ] **Step 2: Прогнать — FAIL:** `go test ./news/ -run 'TestNewsAnnotationCandidates|TestNewsLLMBacklog|TestImportUsesBacklog' -count=1`.
- [ ] **Step 3: Реализовать.**
  - Заменить `var newsAnnotationCandidatesQuery` на функцию; убрать `date >= now() - INTERVAL 35 DAY`; комментарий объясняет: охват — весь видимый поток, стоимость ограничивает backlog.
  - `newsLLMBacklogLimitDefault = 50`, `newsLLMBacklogLimitMax = 1000`, `newsLLMBacklogLimitEnv = "NEWS_LLM_BACKLOG_LIMIT"`.
  - В `Import` вычислить `limit` до чтения кандидатов, логировать не-дефолт.
  - Обновить комментарии и сигнатуру `readNewsAnnotationCandidates`.
- [ ] **Step 4: Прогнать — PASS:** `go test ./news/ -count=1 && gofmt -l news/`.
- [ ] **Step 5: Commit:** `news: annotate RSS backlog with configurable limit`.

---

### Task 3: Батчи 10/40 КБ, промпт v2, реестр 46 тегов, лимит цитаты

**Files:**
- Modify: `news/annotation_llm.go` (батчи, промпт, usage)
- Modify: `news/annotation.go` (лимит evidence)
- Modify: `news/tags.go` (7 новых тегов, `newsPromptVersion = 2`)
- Test: `news/annotation_llm_test.go`, `news/annotation_test.go`, `news/tags_test.go`

**Interfaces:**
- Produces:
  - `const newsLLMBatchBytes = 40_000`
  - `func newsLLMBatches(inputs []newsItem) [][]newsItem` — по `newsLLMBatchSize` и суммарным `len(title)+len(description)`; один элемент больше бюджета — отдельный батч.
  - `tagNewsLLM(ctx, cfg, inputs)` — переименование `tagTelegramLLM`; логирует номер батча, сообщения, usage.
  - `newsEvidenceMax = 300`.
- Реестр: `person` + `siluanov`, `mishustin`; `country` + `iran`, `turkey`; `sector` + `utilities`; `event` + `opec`, `ofz_auctions`. Итого 46: 8+7+8+12+11.

**Steps:**
- [ ] **Step 1: Падающие тесты.**
  - `TestNewsLLMBatches`: 11 коротких → `[10,1]`; три по 30 КБ → `[1,1,1]`; один 100 КБ → `[1]`.
  - `TestNewsPromptVersionAndRegistry`: метка `Редакция инструкции: 2.`; все 46 слагов; новые правила (длинные посты, пустой description, лимит цитаты).
  - `TestNewsTagsRegistry`: счётчики 8/7/8/12/11, итого 46; `theme` — 0.
  - `TestNewsEvidenceTruncated`: цитата 400 рун, первые 300 — подстрока текста → тег принят, `len([]rune(evidence)) == 300`; цитата вне текста → drop.
  - Обновить ожидания `Редакция инструкции: 1.` → 2 и `tagTelegramLLM` → `tagNewsLLM` во всех тестах.
- [ ] **Step 2: Прогнать — FAIL.**
- [ ] **Step 3: Реализовать.**
  - `newsLLMBatches`; `tagNewsLLM` идёт по батчам.
  - Промпт: «новости и аналитические посты»; для длинных постов резюме — одно короткое предложение; если `description` пуст — опираться на `title`; `relevance` для общих новостей (спорт, происшествия, культура) низкая; `evidence` — дословная подстрока не длиннее 300 символов.
  - `newsPromptVersion = 2`.
  - `newsEvidenceMax` и `truncateNewsRunes` перед проверкой подстроки.
  - Usage-логи в обоих транспортах: OpenAI — `usage.prompt_tokens`/`usage.completion_tokens`; Anthropic — `msg.Usage.InputTokens`/`OutputTokens`. Сигнатуры не меняются, лог — на каждый запрос (ретраи видны как отдельные расходы).
- [ ] **Step 4: Прогнать — PASS:** `go test ./news/ -count=1 && go test -race -count=1 ./news/ && gofmt -l news/`.
- [ ] **Step 5: Commit:** `news: batch RSS annotation with prompt v2 and 46 tags`.

---

### Task 4: `v_news_events` — разметка в витрине

**Files:**
- Modify: `news/schema.go` (определение `v_news_events`, комментарии)
- Test: `news/schema_test.go`

**Новые колонки (14):** `date`, `date_msk`, `source`, `transport`, `title`, `url`, `description`, `content_hash`, `sentiment`, `relevance`, `summary`, `model`, `prompt_version`, `tags`. `content_hash` нужен, чтобы отличать «ещё не размечено» от «неразмечаемо»: строки до миграции `content_hash` имеют `''` и в кандидаты не попадают.

**SQL-контракт:**
- `FROM (SELECT * FROM news_events FINAL) AS e`
- `LEFT JOIN (SELECT ... FROM news_annotation FINAL WHERE kind='doc' AND method='llm' AND content_hash != '' ORDER BY annotated_at DESC, model DESC, prompt_version DESC LIMIT 1 BY url, content_hash) AS d` — пустой хеш не аннотируется и не должен склеиваться с пустым хешем позиции.
- tag-строки — по `(url, content_hash, model, prompt_version)`, тоже с `content_hash != ''`.
- `if(d.model = '', NULL, d.sentiment)` / `if(d.model = '', NULL, d.relevance)` / `if(d.model = '', NULL, d.summary)` — NULL означает «не размечено».
- `groupArrayIf(tuple(t.dimension, t.tag, t.direction), t.tag != '') AS tags`.
- `WHERE match(e.title, '<newsFilter>') OR match(e.description, '<newsFilter>')`, `ORDER BY date DESC`.
- Одна строка на позицию: `LIMIT 1 BY` в doc-подзапросе, `GROUP BY` по полям позиции и разметке.

**Steps:**
- [ ] **Step 1: Падающие тесты.**
  - `TestNewsViewStatementsShape`: `v_news_events` — 14 колонок; в `wants` — `if(d.model = '', NULL, d.sentiment)`, `LIMIT 1 BY url, content_hash`, `groupArrayIf`, `FROM (SELECT * FROM news_events FINAL)`, `match(e.title`, `match(e.description`; число стейтментов 55 (4 VIEW + 15 events + 18 flash + 13 versions + 5 tags).
  - Проверка комментариев: таблица + 14 колонок; новые колонки `content_hash`, `relevance`, `summary`, `model`, `prompt_version`, `tags` имеют `COMMENT COLUMN`.
- [ ] **Step 2: Прогнать — FAIL.**
- [ ] **Step 3: Реализовать** новое определение и комментарии; обновить комментарий `sentiment` (NULL — не размечено; Telegram — `v_news_flash.sentiment`).
- [ ] **Step 4: Прогнать — PASS:** `go test ./news/ -run TestNewsViewStatementsShape -count=1 && go test ./news/ -count=1`.
- [ ] **Step 5: Commit:** `news: expose LLM annotation in v_news_events`.

---

### Task 5: MCP-проверка разметки RSS

**Files:**
- Modify: `scripts/mcp_check.py`
- Test: живой `make mcp-check` (Task 7)

**Steps:**
- [ ] **Step 1:** Добавить после блока `LLM-теги` информационную строку:
  ```sql
  SELECT countIf(content_hash != '') AS n, countIf(content_hash != '' AND model != '') AS annotated
  FROM v_news_events WHERE transport = 'rss'
  ```
  Если `annotated > 0` — `OK LLM-разметка RSS: <annotated> из <n> позиций`; иначе `WARN` с командой догона. Не fail: без env разметки штатно нет. Строки с пустым `content_hash` (до миграции) не аннотируемы и в знаменатель не входят.
- [ ] **Step 2:** Проверить синтаксис: `python3 -m py_compile scripts/mcp_check.py`.
- [ ] **Step 3: Commit:** `news: check RSS annotation in mcp-check`.

---

### Task 6: Task review

- [ ] **Step 1:** Диспатч `anthropic/haiku` (`subagent_type: coder`) с диффом ветки, спекой и критерием приёмки; фокус — Review Focus, отсутствие регрессий в `news_events`/`v_news_flash`, секреты, тесты.
- [ ] **Step 2:** Fix-loop: критические замечания — исправить и пере-прогнать `go test -race -count=1 ./news/`; отложенные — назвать в PR/Ruling.

---

### Task 7: Живая приёмка на dev-ClickHouse

**Команды и ожидания:**

```bash
make all
go test -race -count=1 ./news/
NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch
NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch
make mcp-check
dagu validate dagu/news.yaml
```

- [ ] **Step 1:** `make all` — зелёный; `go test -race -count=1 ./news/` — `ok`.
- [ ] **Step 2:** Первый живой прогон с `NEWS_LLM_BACKLOG_LIMIT=500`: в логе — `LLM zai-org/GLM-5.3-Flash: размечено N сообщений, тегов M`, usage-строки `prompt … completion …`. Бэклог — все аннотируемые видимые RSS (`content_hash != ''`) + Telegram в `news_events`; при обрыве провайдера повторить прогон (идемпотентно).
- [ ] **Step 3:** MCP-запросы (`kimi_reader`):
  ```sql
  SELECT count() AS total,
         countIf(content_hash != '') AS annotatable,
         countIf(content_hash != '' AND model != '') AS annotated,
         countIf(transport = 'rss' AND content_hash != '') AS rss_annotatable,
         countIf(transport = 'rss' AND content_hash != '' AND model != '') AS rss_annotated
  FROM v_news_events;

  SELECT count() AS tags FROM v_news_tags;  -- 46
  ```
  Ожидание: `annotatable == annotated`, `rss_annotatable == rss_annotated`, `count() FROM v_news_events` не изменился; строки с `content_hash = ''` (до миграции) остаются с `model = ''` и в критерий не входят. `news_events.sentiment` остаётся NULL (проверить `make ch-sql`/логом; агенту сырая таблица закрыта).
- [ ] **Step 4:** Повторный прогон: кандидатов нет, провайдер не вызывается, число строк витрины и `news_annotation` не растёт.
- [ ] **Step 5:** `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; строка `LLM-разметка RSS: <annotated> из <annotatable>` (число — по факту). `dagu validate dagu/news.yaml` — exit 0.
- [ ] **Step 6:** Стоимость: суммировать usage-строки, посчитать `input/1e6*0.0076 + output/1e6*0.023`, добавить оговорку о наценке шлюза; записать в research-заметку и протокол.
- [ ] **Step 7:** Закоммитить статус ✅ в роадмапе и план с протоколом в теле коммита (до приёмочного комментария в PR).

**Если ClickHouse не отвечает** — остановиться, сообщить владельцу, `make ch-up` не запускать.

---

### Task 8: Документация и роадмап

**Files:**
- Create: `docs/research/2026-10-18-rss-llm-annotation.md` — оценка стоимости до/после, фактические токены, время, рекомендация (50 + env).
- Modify: `README.md` — таблица импортёров (`news`), раздел `news/`, env-таблица (`NEWS_LLM_BACKLOG_LIMIT`, уточнить `NEWS_LLM_PROFILE`/`NEWS_LLM_MODEL`), «Ограничения» (raw `news_events.sentiment` NULL; `v_news_events.sentiment` из разметки), MCP-пример.
- Modify: `ARCHITECTURE.md` — шапка «Последнее обновление», §2 (`news/`), §5 env, §6.3 `v_news_events`, §7 расписание.
- Modify: `docs/ROADMAP_KEY_RATE.md` — §4.2 №24 → ✅ с приёмкой; абзац фазы 2; §8 критерий/протокол.
- Modify: `docs/KEY_RATE_PUBLICATIONS.md` — только если там есть утверждение «sentiment RSS NULL» (grep).
- Проверить `docs/ROADMAP_INDEX.md` — без изменений.

**Steps:**
- [ ] **Step 1:** Написать research-заметку с фактическими числами.
- [ ] **Step 2:** Обновить README/ARCHITECTURE по скилу `sync-readme-architecture`; grep-проверка: не осталось «sentiment … пока NULL» в контексте RSS-витрины, «39 тегов», «Telegram-молнии» в описании модели.
- [ ] **Step 3:** Обновить роадмап; статус — `✅ сделан этим PR (приёмка 2026-10-18: …)`.
- [ ] **Step 4: Commit:** `docs: document RSS LLM annotation`.

---

### Task 9: Финальное ревью, PR и трекер

- [ ] **Step 1:** `superpowers:requesting-code-review` — финальное ревью всей ветки моделью `deepseek/deepseek-flash` (самая способная конфигурация пула; если effort ниже `max` — сказать владельцу, модель не подменять).
- [ ] **Step 2:** `make all` ещё раз после fix-раунда.
- [ ] **Step 3:** Push ветки; PR с заголовком `[key-rate/phase-2-rss-llm-annotation] LLM-разметка RSS батчами на GLM-5.3-Flash`, `Closes #122`; в описании — источник, схема `v_news_events`, команда запуска, пример запроса.
- [ ] **Step 4:** Приёмочный комментарий в PR: команды, числа, крайние даты, повтор (no-op), `make mcp-check`, `dagu validate`, стоимость. Обновить issue #122 и Project V2: `Status=In Review`, `PR`, `Branch`, `Spec / Plan`.
- [ ] **Step 5:** Merge — решение владельца; после мержа — `Status=Done` и ✅ в роадмапе (если ещё не стоит).

---

## Критерий приёмки (фиксируется в issue #122)

1. `make all` — зелёный; `go test -race -count=1 ./news/` — `ok`.
2. Живой прогон `NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch` размечает все **аннотируемые** позиции `v_news_events` для `prompt_version=2` (`content_hash != ''`): `countIf(content_hash != '' AND model != '') = countIf(content_hash != '')`, по RSS — то же; строки с `content_hash = ''` (до миграции) остаются `model = ''` и в критерий не входят; витрина остаётся «одна строка на позицию» (дублей нет), число строк может вырасти за счёт новых видимых позиций.
3. `v_news_tags` — 46 тегов, новые слаги видны; `news_annotation` содержит doc- и tag-строки `model='zai-org/GLM-5.3-Flash'`, `prompt_version=2`.
4. Повторный прогон — no-op: кандидатов 0, провайдер не вызывается, строки не растут.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА, `LLM-разметка RSS` — OK; `dagu validate dagu/news.yaml` — exit 0.
6. `news_events.sentiment` остаётся NULL; `v_news_events.sentiment`/`relevance`/`summary` — NULL у неразмеченных, значения у размеченных; `tags` — `[]` или массив.
7. Стоимость зафиксирована: фактические токены из логов, оценка USD по тарифу от 2026-10-17, оговорка о наценке шлюза; research-заметка и протокол PR.
8. README, ARCHITECTURE, роадмап обновлены тем же PR; issue #122 содержит `Acceptance`/`Spec`/`Plan`, PR — `Closes #122`.

## Оценка стоимости и времени (предварительная)

- Охват: `v_news_events` — 312 строк (283 RSS + 29 Telegram) на 2026-10-18; аннотируемых (с непустым `content_hash`) — меньше; точное число даст живой снимок перед прогоном. Кандидатов с `prompt_version=2` — все аннотируемые видимые RSS + Telegram в `news_events` (≈400–430, включая переразметку Telegram v1 → v2).
- Тариф GLM-5.3-Flash (Joingonka, 2026-10-17): $0,0076/1M input, $0,023/1M output; шлюз добавляет 10% network cost и 5% конвертации.
- Оценка: input ≈140–170K токенов, output ≈50–70K → ≈$0,002–0,003 за бэкфилл; ≈$0,007 за 1000 позиций. Фактические числа — из usage-логов.
- Время: 43–60 запросов, ~5–15 мин на бэкфилл с лимитом 500; штатный прогон 50 позиций — 5–10 запросов, укладывается в 30-минутное расписание.

## Откат

- Код и витрина откатываются revert'ом PR; `v_news_events` пересоздаётся прежним `CREATE OR REPLACE VIEW` при следующем прогоне.
- Строки `news_annotation` с `prompt_version=2` аддитивны и на другие витрины не влияют; при необходимости удаляются по `(model, prompt_version)` — отдельным решением владельца. `news_events` и `news_message_version` не меняются.

## Вне объёма

- Разметка сырого RSS вне `v_news_events`.
- Заполнение `news_events.sentiment`.
- Новые витрины (расширяем `v_news_events`).
- Provider Batch API (`/v1/batches`): используется существующий синхронный `/v1/chat/completions`, «батч» — несколько сообщений в одном запросе.
- Изменение расписания `dagu/news.yaml`, перевод иноязычных текстов, торговые сигналы.

## Протокол живой приёмки (2026-10-18)

**Команды и результаты:**

- `make all` — exit 0: golangci-lint `0 issues`, `go vet` чисто, `go test -race ./...` — все `ok` (`news` 12,155 с), обе сборки.
- `go test -race -count=1 ./news/` — `ok` (после фикса `86a6350`).
- `NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch` (основной прогон) — 32 батча, `размечено 239 сообщений, тегов 276`, `Imported 435 rows of news_watch`; 21 мин 16 с.
- Догон тем же приказом — 1 батч, `размечено 3 сообщений, тегов 3`; 21 с.
- Повторный прогон (no-op) — ни одной строки LLM-батча, провайдер не вызывался, `Imported 435 rows` (upsert Telegram); 4 с.
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `OK LLM-разметка RSS: 191 из 191 позиций`, `OK реестр v_news_tags полон: слагов 46`, `OK витрина v_news_events ... строк: 339`.
- `dagu validate dagu/news.yaml` — exit 0.

**Данные после приёмки:**

- `v_news_events`: 339 строк; аннотируемых 221, размечено 221; RSS 191/191, Telegram 30/30.
- `news_annotation` v2: 242 doc + 279 tag; v1 (1.10): 44 doc + 39 tag; `news_events.sentiment` — 0 непустых.
- `v_news_tags`: 46 (11 `cb_channel`); `v_news_flash`: 95 строк.
- Стоимость: 33 батча, 175 620 prompt + 29 325 completion токенов → **$0,0020** по тарифу (≈$0,0023 с наценкой шлюза).

**Найденный и исправленный дефект:** `newsFilter` начинается с `(?i)`, и драйвер ClickHouse считал `?` внутри регулярки позиционным плейсхолдером (4 `?` при 2 аргументах). Исправлено коммитом `86a6350`: фильтр передаётся SQL-параметром (`match(title, ?)`, `match(description, ?)`), после чего прогон прошёл штатно.
