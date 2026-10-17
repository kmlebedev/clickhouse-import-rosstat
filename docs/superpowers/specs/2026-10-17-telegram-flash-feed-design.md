# Telegram-молния Интерфакса + винтажные версии + LLM-разметка — design

Дата: 2026-10-17. Пункты: `docs/ROADMAP_KEY_RATE.md` §6, фаза 1 — новые **1.9** «Telegram-молния
Интерфакса и винтажные версии» и **1.10** «LLM-разметка новостной молнии». Номер 1.8 занят
соседней веткой `feat/mp-summary-llm-models` (сравнение LLM-моделей на `cbr_mp_summary`).

**Гейт по LLM:** живой прогон 1.10 и выбор модели отложены до мерджа
`feat/mp-summary-llm-models`. В этом PR — код разметки, промпт, валидация и офлайн-тесты на
моке; `NEWS_LLM_MODEL` в живом окружении не выставлялся, статус 1.10 был ⏳ (на момент
оформления спеки). После мерджа
1.8 модель выбирается по отчёту сравнения (скорость/качество/цена), при необходимости правится
промпт с повышением `prompt_version`, затем выполняется живая приёмка 1.10.

**Обновление 2026-10-17:** 1.8 смерджен, профиль `glm` реализован, живой прогон 1.10 выполнен
(`zai-org/GLM-5.3-Flash`); статус 1.10 — ✅, детали — §8.

## 1. Пробел

`news_watch` (`news/`) собирает 7 RSS-лент (Интерфакс, РБК, ЦБ РФ ×2, TASS, smart-lab ×2) в
`news_events` и публикует `v_news_events`. Окно RSS-Интерфакса — 25 позиций, лаг ~1–2 часа;
короткие «молнии» Telegram-канала `@interfaxonline` в контуре не собираются. Правки и удаления
постов нигде не фиксируются, а `sentiment` новостей остаётся NULL — агенту нечем ответить на
вопросы «что Трамп сказал про пошлины», «что происходит с энергопереходом», «какие новости про
Китай и деглобализацию влияют на сектор/компанию и канал ставки ЦБ».

## 2. Цель и объём

Цель 1.9: короткое сообщение Интерфакса попадает в контур в течение 30 минут, хранится с
моментом публикации и полным текстом; каждая правка и удаление оставляют версию; агент видит
молнию в отдельной витрине и может отфильтровать её по `transport='telegram'`.

Цель 1.10: LLM-разметка каждой версии молнии — `sentiment`, `relevance`, `summary` и теги
`person|country|sector|event|theme|cb_channel` с направлением влияния на инфляцию; модель и
версия промпта входят в ключ, поэтому смена модели/промпта не затирает старую разметку.

**Вне объёма:** китайские Telegram-каналы (Xinhua/CGTN/Global Times) — отдельный пункт §4.2 №20;
LLM-разметка всего RSS-потока — отдельный пункт №21 (таблица `news_annotation` generic);
перевод иноязычных текстов; торговые сигналы; бэкфилл истории канала глубже окна 20 сообщений;
изменение расписания `dagu/news.yaml`.

## 3. Источник (живая проба 2026-10-17)

| Параметр | Значение |
|---|---|
| URL превью | `https://t.me/s/interfaxonline` — HTTP 200, ~117 КБ HTML |
| Окно | 20 сообщений; 2026-10-16 15:53 UTC … 2026-10-17 09:14 UTC ≈ 17,3 часа, максимальный разрыв 2,4 часа |
| Разметка | `div.tgme_widget_message[data-post="interfaxonline/<id>"]`, `time[datetime]` (RFC3339, UTC), `div.tgme_widget_message_text` |
| Особенности | репосты с подписью `@ifax_go`; медиа без текста; HTML-сущности (`&quot;`), `<br/>`, `<tg-emoji>`, вложенные `<b>/<i>/<a>` |
| Публичность | без авторизации, без ключей; 48 запросов/сутки при расписании 30 минут |

30-минутный опрос перекрывает окно с большим запасом. Простой дольше окна теряет позиции —
`NEWS_WATCH_MODE=check` покажет дыру; «залечить» окно задним числом нечем.

## 4. Архитектура

### 4.1 Транспорт и парсер

- `news/parse_telegram.go`: `parseTelegramPage(source string, page []byte) (telegramPage, error)`.
- Селекторы — константы пакета (`div.tgme_widget_message`, `time[datetime]`,
  `div.tgme_widget_message_text`), чтобы смена разметки ловилась тестом, а не тихим нулём.
- `data-post` → `channel`, `message_id` (`strconv.ParseUint`); URL сообщения —
  `https://t.me/<channel>/<id>`.
- Дата — `time.RFC3339` из `datetime`; офсет сохраняется как момент (UTC в источнике).
- Текст: inner HTML → `<br>`, `</p>`, `</div>` в `\n` → снятие тегов → `html.UnescapeString` →
  нормализация переводов строк/пробелов → удаление последней строки-подписи `@handle`.
- `title` — первая непустая строка, `strings.Fields` + `Join`, обрезка до 300 рун по границе
  слова; `description` — полный нормализованный текст.
- `telegramPage` несёт `Messages []telegramMessage`, `SeenIDs map[uint64]struct{}` (включая
  пропущенные медиа-сообщения — чтобы не объявить их удалёнными) и `Skipped`.
- Ноль распознанных сообщений — ошибка ленты (как пустой RSS); сбой одной ленты не отменяет
  остальные (`fetchFeeds` уже изолирует).
- `feed` получает `Kind` (`feedRSS`/`feedTelegram`); `feeds()` — 8 записей, `interfax_tg` сразу
  после `interfax`, `Source='interfax'`. `fetchFeeds` диспетчеризует: RSS → `util.GetXML`,
  Telegram → `util.GetPage` + парсер. URL превью — переменная пакета для httptest.
- `newsItem` получает `ContentHash`, `Channel`, `MessageID`; `contentHash(title, description)` —
  sha256 hex от `title+"\n"+description` (общая для RSS и Telegram).

### 4.2 Схема (канон — `ARCHITECTURE.md` §6.2)

`news_events` — миграция живой таблицы:

```sql
ALTER TABLE news_events ADD COLUMN IF NOT EXISTS content_hash String AFTER description;
```

```sql
CREATE TABLE IF NOT EXISTS news_events (
    date DateTime,
    source LowCardinality(String),
    title String,
    url String,
    description String,
    content_hash String,               -- sha256 текущей версии title+description (у Telegram title в news_events
                                       -- может быть от v1, а hash — от текущей версии); ключ LLM-разметки
    sentiment Nullable(Float32)
) ENGINE = ReplacingMergeTree ORDER BY (date, source, title);
```

`news_message_version` — винтажный слой Telegram:

```sql
CREATE TABLE IF NOT EXISTS news_message_version (
    channel LowCardinality(String),   -- interfaxonline
    message_id UInt64,
    version UInt32,                   -- 1..N; растёт при правке/удалении
    date DateTime,                    -- момент публикации (не меняется)
    source LowCardinality(String),    -- interfax
    title String,                     -- первая строка версии
    url String,                       -- https://t.me/<channel>/<id>
    description String,               -- полный текст версии; '' у tombstone
    content_hash String,              -- sha256; '' у tombstone
    first_seen DateTime,              -- когда версия впервые увидена
    deleted UInt8                     -- 1 — сообщение исчезло из окна канала
) ENGINE = ReplacingMergeTree ORDER BY (channel, message_id, version);
```

`news_annotation` — LLM-разметка (generic по `url`+`content_hash`; в этом пункте заполняется
только для Telegram):

```sql
CREATE TABLE IF NOT EXISTS news_annotation (
    url String,
    content_hash String,
    kind LowCardinality(String),      -- 'doc' | 'tag'
    method LowCardinality(String),    -- 'llm'
    model LowCardinality(String),     -- фактическое имя модели
    prompt_version UInt16,
    dimension LowCardinality(String), -- '' у doc; person|country|sector|event|theme|cb_channel
    tag LowCardinality(String),       -- '' у doc; slug реестра
    direction LowCardinality(String), -- '' у doc; у tag — proinflationary|disinflationary|neutral|mixed
                                      -- (у не-cb_channel только neutral|mixed)
    confidence Float32,
    evidence String,                  -- дословная цитата; '' у doc
    sentiment Float32,                -- -1..1, только doc
    relevance Float32,                -- 0..1, только doc
    summary String,                   -- 1 предложение, только doc
    annotated_at DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (url, content_hash, kind, method, model, prompt_version, dimension, tag);
```

Копии DDL в `news/schema.go` обязаны совпадать с каноном; миграции идемпотентны
(`ADD COLUMN IF NOT EXISTS`), выполняются при каждом прогоне до чтения лент.

### 4.3 Версии правок и удалений

- Перед записью читаем версии канала в окне:
  `SELECT … FROM news_message_version FINAL WHERE channel = ? AND date >= ? ORDER BY message_id, version`.
- Чистая функция `diffTelegram(existing, current, seen, windowMin, windowMax, now)`:
  - нет версии → v1;
  - `content_hash` изменился → v+1 (title версии — новая первая строка; в `news_events` title
    фиксируется по v1, чтобы ключ `(date, source, title)` не разъезжался);
  - hash тот же → новой версии нет;
  - известное сообщение с `date` внутри `[windowMin, windowMax]`, которого нет в `SeenIDs` →
    tombstone v+1 (`deleted=1`, `content_hash=''`, `description=''`);
  - сообщение за пределами окна не трогаем (оно просто вышло из окна).
- `news_events` для Telegram пишется всегда (20 строк за прогон) — upsert текущего текста; ключ
  стабилен благодаря title v1. Версии пишутся отдельным батчем; tombstone в `news_events` не
  пишется.
- Порядок записи: сначала `news_events`, затем `news_message_version` — сбой версий
  повторяется следующим прогоном, а не теряет текущий текст.
- `first_seen` — время прогона (Go `time.Now()`), `date` — момент публикации.

### 4.4 LLM-разметка

- Реестр `news/tags.go`: `dimension` ∈ `person|country|sector|event|theme|cb_channel`;
  стартовый набор: `person:trump/putin/xi_jinping/powell/lagarde/nabiullina`,
  `country:china/usa/eu/russia/india`, `sector:oil_gas/metals/gold/banks/tech/real_estate/agriculture`,
  `event:energy_transition/deglobalization/sanctions/tariffs/energy_prices/budget_impulse/labor_market/inflation_expectations/credit/key_rate`,
  `cb_channel:` 11 каналов из `KEY_RATE_CHANNELS.md` §3 (`current_inflation`, `core_inflation`,
  `expectations`, `labor_market`, `credit_transmission`, `output_gap`, `budget_impulse`,
  `money_supply`, `external_conditions`, `savings`, `macroprudential`).
- Промпт (system + user, русский): для каждого сообщения вернуть строгий JSON
  `{"messages":[{"url","sentiment","relevance","summary","tags":[{"dimension","tag","direction","confidence","evidence"}]}]}`.
  Правила: только слаги реестра; `evidence` — дословная подстрока текста; не более 5 тегов;
  person/country/event тегировать только если новость про действие/заявление/влияние, а не
  случайное упоминание; `direction` — влияние на инфляцию (для `cb_channel` — через канал).
- Транспорт — официальный `anthropic-sdk-go`, как в `cbr/mp_summary_llm.go` (Anthropic Messages
  API, `LLM_BASE_URL` без `/v1`, ключ заголовком `x-api-key`, `WithMaxRetries(0)`, дедлайн 65 с).
  `cbr/` не рефакторим: там свои промпт/валидация/тесты; дублирование транспорта ~100 строк
  осознанное, с комментарием «третий потребитель → вынос в util».
- Конфиг: профиль `default` — `LLM_BASE_URL`, `LLM_API_KEY`, `NEWS_LLM_MODEL` (Anthropic SDK);
  профиль `glm` — `NEWS_LLM_PROFILE=glm` и `GLM_LLM_BASE_URL`/`GLM_LLM_API_KEY`/`GLM_LLM_MODEL`
  (OpenAI-совместимый `/v1/chat/completions`, `response_format=json_object`,
  `reasoning_effort=none`). Любая пустая → warn и пропуск LLM-шага (импорт зелёный, догон
  следующим прогоном). Модель пишется в `model`, версия промпта `newsPromptVersion = 1` — в
  `prompt_version`; обе входят в ключ таблицы.
- Кандидаты: `news_events FINAL` с `url LIKE 'https://t.me/%'`, `content_hash != ''`,
  `date >= now() - INTERVAL 35 DAY` (окно покрывает и корпоративные раскрытия edisclosure),
  которых нет в `news_annotation` для пары
  `(model, prompt_version)`, `ORDER BY date DESC LIMIT 50`. Батч — 10 сообщений; при ошибке
  провайдера — warn и остановка LLM-шага на этот прогон (без долбёжки).
- Валидация: неизвестный URL/тег/направление — drop с warn; `evidence` не подстрока — drop;
  sentiment/relevance/confidence — clamp; дубли тегов — collapse; summary — trim/обрезка.
- Пишем один `kind='doc'` ряд на сообщение (даже без тегов, чтобы не переспрашивать) и
  `kind='tag'` ряды. Сбой записи разметки — warn, не ошибка импорта.

### 4.5 Витрины и фильтр

- `v_news_events` — в SELECT добавляется `transport`:
  `multiIf(url LIKE 'https://t.me/%', 'telegram', 'rss')`; фильтр —
  `match(title, newsFilter) OR match(description, newsFilter)`; стемы дополняются
  `Китай|китайск|деглобализац|энергопереход|энергетическ|тариф`. Комментарии колонок и таблицы
  обновляются; `sentiment` в этой витрине остаётся из `news_events` (NULL для Telegram —
  sentiment молнии в `v_news_flash`).
- `v_news_flash` — текущее состояние Telegram: последняя версия из `news_message_version`
  (`argMax(field, version)`), `deleted=0`, LEFT JOIN `news_annotation` (doc + tag, `method='llm'`),
  колонки: `date`, `date_msk`, `source`, `channel`, `message_id`, `version`, `first_seen`,
  `title`, `url`, `description`, `content_hash`, `sentiment`, `relevance`, `summary`,
  `model`, `prompt_version`, `tags Array(Tuple(dimension, tag, direction))`.
- `v_news_message_versions` — все версии, включая tombstone: `channel`, `message_id`, `version`,
  `date`, `date_msk`, `source`, `title`, `url`, `description`, `content_hash`, `first_seen`,
  `deleted`.
- `v_news_tags` — статическая витрина реестра из `news/tags.go`
  (`dimension`, `tag`, `title`, `direction`) для расшифровки слагов агентом.
- Комментарии таблицы и каждой колонки — как у существующих витрин; `GRANT SELECT` на три
  новые витрины в `sql/mcp_kimi_reader.sql`; сырые таблицы агенту не выдаются.
- `scripts/mcp_check.py`: `v_news_flash`/`v_news_message_versions` читаются и непусты,
  `v_news_tags` содержит все слаги, сырые `news_message_version`/`news_annotation` →
  `ACCESS_DENIED`. Проверка «есть LLM-теги» — информационная (warn, не fail): без env
  разметка штатно отсутствует.

## 5. Расписание и режимы

`dagu/news.yaml` не меняется: `:07/:37`, `since`, retry 3 × 300 с, `overlap_policy: skip`.
Окно Telegram ≈17 часов при 30-минутном опросе — запас большой; простой дольше окна теряет
позиции, `check` покажет дыру. `all` — ручной полный прогон (текущее окно), `check` — покрытие.

## 6. Тесты (офлайн, без сети и БД)

- **Парсер:** фикстура реального окна (20 сообщений, обрезанная); дата/ID/URL/канал; `<br>` и
  сущности; снятие подписи `@interfaxonline`/`@ifax_go`; title = первая строка; медиа без
  текста → skipped, но `SeenIDs`; пустая/чужая страница → ошибка; битый `data-post`/дата →
  skipped.
- **Ленты:** `feeds()` = 8, порядок `interfax, interfax_tg, rbc, …`, `source` обоих Интерфаксов
  = `interfax`; `fetchFeeds` на httptest (HTML и RSS); сбой одной ленты не задевает остальные.
- **Версии:** чистая `diffTelegram`: v1; правка → v2; тот же hash → без версии; tombstone в окне;
  сообщение за окном не трогается; `SeenIDs` защищает медиа-сообщение от ложного tombstone;
  title v1 остаётся в `news_events`; `first_seen`/`date` не перезаписываются.
- **LLM-валидация:** неизвестный URL/тег/dimension/direction; `evidence` не подстрока;
  clamp sentiment/relevance/confidence; дубли; >5 тегов; doc-строка без тегов; пустой ответ.
- **LLM-транспорт:** mock Anthropic-совместимого API (образец `cbr/mp_summary_llm_test.go`):
  200 валидный JSON; fenced JSON; 500; битый JSON → один ретрай; timeout; пустой text-блок;
  отсутствие env → skip.
- **Схема:** DDL-пины `content_hash`, `news_message_version`, `news_annotation`; ALTER-пин;
  число и порядок стейтментов витрин; `v_news_events` содержит `transport` и
  `match(title) OR match(description)`; `newsFilter` ловит «Китай», «деглобализация»,
  «энергопереход», «тарифы» и не ловит «погода».
- **Композиция Import:** fake conn (расширить `newsFakeConn`): Telegram upsert текущих,
  батч версий, tombstone, LLM-батч, no-op при отсутствии нового; ошибка версий/LLM не
  отменяет запись текущих сообщений; `check` не пишет.

## 7. Документы (тем же PR)

1. `ARCHITECTURE.md` — §2 (пакет `news/`: 8 лент, Telegram-превью, версии, разметка), §3
   (строка `news_watch`), §5 (`NEWS_LLM_MODEL` + общие `LLM_BASE_URL`/`LLM_API_KEY`), §6.2
   (канонический DDL), §6.3 (`v_news_events` transport/фильтр, `v_news_flash`,
   `v_news_message_versions`, `v_news_tags`, гранты), §7 (расписание), §8 п. 14 (LLM).
2. `README.md` — пакет, таблицы, витрины, расписание, env, примеры MCP-запросов.
3. `docs/ROADMAP_KEY_RATE.md` — 1.9 ✅ после приёмки, 1.10 ✅ (приёмка 2026-10-17: `GLM-5.3-Flash`, 44/44, 25 с тегами, 39 тегов);
   §4.1 — строка `news_events`; §4.2 — №6 (Telegram), №20 (китайские каналы — отдельный пункт),
   №21 (LLM-разметка RSS — отдельный пункт); §5 — новые таблицы/витрины; §8 — критерий приёмки.
   1.8 не трогаем — он принадлежит ветке `feat/mp-summary-llm-models`.
4. `docs/KEY_RATE_CHANNELS.md` — §9 «Быстрый новостной слой: Telegram и реестр тегов».
5. `docs/GOLD_THESIS_DATA_ROADMAP.md` — новостной раздел (Telegram-молния, версии, разметка);
   follow-up 5 (исторический слой) остаётся открытым.

## 8. Приёмка 1.9 (живая, через `make`; LLM-прогон не выполняется)

1. `make all` — зелёный.
2. `make import STAT=news_watch` (режим `all`): 8 лент; `interfax_tg: позиций 20`;
   `Imported N rows`; в логе `новых версий 20`; LLM-шаг — `warn: LLM-разметка пропущена`
   (env не выставлен).
3. `NEWS_WATCH_MODE=since make import STAT=news_watch` — текущие Telegram-сообщения upsert,
   `новых версий 0`; повтор стабилен.
4. `NEWS_WATCH_MODE=check make import STAT=news_watch` — покрытие `interfax` (RSS+Telegram),
   дыр нет; сбой `t.me` виден как `неполно: не прочитаны ленты interfax_tg`.
5. MCP (`kimi_reader`): `v_news_flash` — сообщения и текст (теги пусты — LLM не запускался);
   `v_news_message_versions` — версии; `v_news_tags` — реестр; сырые `news_message_version`/
   `news_annotation` → `ACCESS_DENIED`.
6. `make mcp-check` — зелёный (LLM-теги — информационная строка, env не задан).
7. `make env-check` — без изменений (LLM-переменные опциональны; описаны в README).
8. Протокол приёмки — комментарием в PR; статус 1.9 — ✅, 1.10 — ✅ (живой прогон 2026-10-17).

**Живой прогон 1.10 (2026-10-17):** профиль `NEWS_LLM_PROFILE=glm` (`GLM_LLM_*`, OpenAI-совместимый
`/v1/chat/completions`, `response_format=json_object`, `reasoning_effort=none`), модель
`zai-org/GLM-5.3-Flash`; размечены 44/44 сообщения `v_news_flash` (24 `interfaxonline` +
20 `edisclosure` из общего dev-БД), 25 с тегами, 39 тегов, `prompt_version=1`; окно кандидатов —
35 дней; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (`LLM-теги: 25`).

## 9. Ограничения и риски

- Разметка `t.me/s/` может смениться — фикстура и тест парсера падают первыми, лента даёт
  ошибку, а не тихий ноль.
- Окно 20 сообщений конечно: простой дольше ~17 часов теряет позиции; `check` покажет дыру.
- Tombstone — эвристика: отсутствие в окне при живом соседстве; скрытое/закреплённое сообщение
  может дать ложный tombstone. Вернувшееся сообщение получает новую версию `deleted=0`;
  история не разрушается. `SeenIDs` включает медиа-сообщения, чтобы не объявлять их удалёнными.
- Title в `news_events` фиксируется по v1 (стабильный ключ); актуальная первая строка — в
  `v_news_flash`/`v_news_message_versions`. Это осознанный размен.
- LLM-разметка — подсказка, не истина: цитата проверяется, confidence сохраняется; модель и
  версия промпта видны в данных. Без env/при отказе провайдера — деградация, догон следующим
  прогоном.
- Стоимость LLM ограничена: только Telegram, только новые версии, батч 10, не более 50
  сообщений за прогон.
- `news_events` для Telegram пишется каждые 30 минут (20 строк) — цена upsert'а пренебрежима,
  зато текущий текст и sentiment-кандидаты всегда свежие.
