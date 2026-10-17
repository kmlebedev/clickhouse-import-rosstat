# LLM-разметка RSS-потока батчами на `zai-org/GLM-5.3-Flash` — дизайн

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** `key-rate/phase-2-rss-llm-annotation` — [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №24, issue [#122](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/122).
**План:** [2026-10-18-rss-llm-annotation.md](../plans/2026-10-18-rss-llm-annotation.md).
**Сопутствующие документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md), [README.md](../../../README.md), [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md), спека пунктов 1.9–1.11 [2026-10-17-telegram-flash-feed-design.md](2026-10-17-telegram-flash-feed-design.md), отчёт сравнения моделей [2026-10-17-mp-summary-llm-comparison.md](../../research/2026-10-17-mp-summary-llm-comparison.md).

## 1. Пробел

`news_watch` (`news/`) собирает девять лент (семь RSS + два Telegram-канала) в `news_events` и публикует `v_news_events`. Пункт 1.10 завёл generic-таблицу `news_annotation` и разметил ею **только Telegram-молнию** (`url LIKE 'https://t.me/%'`): `sentiment`, `relevance`, `summary` и теги. RSS-позиции в `v_news_events` остаются без разметки, а `v_news_events.sentiment` всегда NULL — агенту нечем ответить на вопросы «насколько это важно для контура ставки», «какой у новости тон», «какие тезисы затронуты», кроме regex-фильтра.

Issue #122 требует: разметку RSS батчами, оценку стоимости (батчи/бэклог), расширение реестра тегов и решение по `news_events.sentiment`.

## 2. Цель и объём

**Цель:** размечать RSS-позиции, которые уже видит агент в `v_news_events`, моделью `zai-org/GLM-5.3-Flash` батчами; отдать `sentiment`/`relevance`/`summary`/теги в той же витрине, не меняя схему сырых таблиц.

**В объёме:**

- кандидатная выборка: все Telegram-сообщения (поведение 1.10 сохраняется) + RSS-позиции, проходящие `newsFilter` — тот же фильтр, что у `v_news_events`;
- батчи до 10 сообщений и до 40 КБ текста, промпт-редакция 2;
- расширение реестра тегов до 46 (7 новых слагов);
- колонки разметки в `v_news_events` (`content_hash`, `sentiment`, `relevance`, `summary`, `model`, `prompt_version`, `tags`);
- `NEWS_LLM_BACKLOG_LIMIT` для разового бэкфилла при штатных 50 позициях за прогон;
- usage-логи (токены) и оценка стоимости;
- MCP-проверка разметки RSS, документация и роадмап.

**Вне объёма:**

- разметка сырого RSS вне `v_news_events`;
- заполнение `news_events.sentiment`;
- новые витрины (расширяем существующую `v_news_events`);
- Provider Batch API (`/v1/batches`): используется существующий синхронный `/v1/chat/completions`, «батч» — несколько сообщений в одном запросе;
- изменение расписания `dagu/news.yaml`, перевод иноязычных текстов, торговые сигналы.

## 3. Источник и объём

| Параметр | Значение |
|---|---|
| Ленты | 9: RSS Интерфакс, РБК, ЦБ РФ ×2, TASS, smart-lab ×2; Telegram `interfaxonline`, `edisclosure` |
| Окна | RSS 20–100 позиций, Telegram 20 сообщений (≈17 ч и ≈30 дней) |
| `v_news_events` на 2026-10-18 | 312 строк: 283 RSS + 29 Telegram |
| Текст RSS | у smart-lab — полный пост (до ≈31 КБ), у новостных лент — краткое описание (≈100–500 байт), у ЦБ описания нет |
| История | копится с первого прогона (2026-10-13), глубже окна лент не перечитывается |
| `content_hash` | добавлен миграцией 1.9; строки до миграции имеют `''` и не аннотируемы |

Модель — `zai-org/GLM-5.3-Flash` через профиль `NEWS_LLM_PROFILE=glm` и `GLM_LLM_BASE_URL`/`GLM_LLM_API_KEY`/`GLM_LLM_MODEL`. Профиль `default` (Anthropic SDK) остаётся рабочим и не меняется.

## 4. Дизайн

### 4.1 Кандидатная выборка

`newsAnnotationCandidatesQuery(limit)`:

```sql
SELECT date, source, title, url, description, content_hash
FROM news_events FINAL
WHERE content_hash != ''
  AND (url LIKE 'https://t.me/%'
       OR match(title, '<newsFilter>')
       OR match(description, '<newsFilter>'))
  AND (url, content_hash) NOT IN (
      SELECT url, content_hash FROM news_annotation FINAL
      WHERE method = 'llm' AND model = ? AND prompt_version = ?
  )
ORDER BY date DESC
LIMIT <limit>
```

- `newsFilter` — та же Go-константа, что в определении `v_news_events`; это код, а не пользовательский ввод, поэтому она подставляется литералом.
- Все Telegram-сообщения остаются кандидатами: поведение 1.10 не сужается, а смена `prompt_version` переразмечает их новой редакцией рядом со старой.
- Окно `date >= now() - INTERVAL 35 DAY` **снято**: охват — весь видимый поток, а стоимость ограничивает `LIMIT` (бэклог). Повторный прогон не платит за уже размеченные пары.
- `LIMIT` — `NEWS_LLM_BACKLOG_LIMIT` (1…1000), по умолчанию 50.

### 4.2 Батчи

- `newsLLMBatchSize = 10`, `newsLLMBatchBytes = 40_000`; `newsLLMBatches` делит вход по обоим пределам. Один пост больше бюджета становится батчем из одного элемента.
- `tagNewsLLM` идёт по батчам; на батч — один запрос с одним повтором на ошибку формата.
- Отказ провайдера останавливает LLM-шаг на этот прогон, но уже полученные строки пишутся; следующий прогон догоняет по кандидатной выборке.
- OpenAI-транспорт: `temperature=0`, `max_tokens=8192`, `response_format=json_object`, `reasoning_effort=none`, `Authorization: Bearer`. Дедлайн — 300 с на запрос.

### 4.3 Промпт-редакция 2 и реестр тегов

`newsPromptVersion = 2` — меняется роль и правила, поэтому старые строки v1 не затираются, а новая разметка ложится рядом.

Промпт:

- роль — «новости и аналитические посты для контура ключевой ставки и DCF»;
- для длинных постов резюме — одно короткое предложение, без пересказа всего текста;
- если `description` пуст — опираться на `title`;
- `relevance` — важность для контура ставки/DCF; общие новости (спорт, происшествия, культура) — низкая;
- `evidence` — дословная подстрока текста не длиннее 300 символов; `newsEvidenceMax = 300`, цитата обрезается до проверки подстроки.

Реестр — 46 тегов (+7):

| Измерение | Новые слаги |
|---|---|
| `person` | `siluanov`, `mishustin` |
| `country` | `iran`, `turkey` |
| `sector` | `utilities` |
| `event` | `opec`, `ofz_auctions` |

Итог по измерениям: 8 персон + 7 стран + 8 секторов + 12 событий + 11 каналов = 46. `theme` остаётся пустым: сюжеты деглобализации и энергоперехода живут в `event`.

### 4.4 Хранение и витрина

Схема `news_annotation` не меняется: doc-строка на сообщение и tag-строки, `model`/`prompt_version` в ключе. `news_events.sentiment` **остаётся NULL** — сырая таблица остаётся ingestion-слоем; ReplacingMergeTree без колонки версии не даёт безопасно перезаписывать её разметкой нескольких моделей.

`v_news_events` расширяется:

- `content_hash` — ключ разметки; позволяет отличить «ещё не размечено» от «неразмечаемо» (пустой хеш);
- `sentiment`, `relevance`, `summary` — из последней doc-строки (`ORDER BY annotated_at DESC, model DESC, prompt_version DESC LIMIT 1 BY url, content_hash`);
- `model`, `prompt_version` — какая редакция дала разметку;
- `tags Array(Tuple(dimension, tag, direction))` — tag-строки той же модели/редакции.

NULL-семантика: `NULL` у `sentiment`/`relevance`/`summary` — «не размечено», а не «нейтрально». Одна строка на позицию сохраняется: doc-подзапрос сворачивается `LIMIT 1 BY`, tag-строки собираются `groupArrayIf`. Пустой `content_hash` в JOIN не участвует.

Новых витрин и грантов нет: `v_news_events` уже выдан `kimi_reader`; сырые `news_events`/`news_annotation` остаются закрытыми.

### 4.5 Ошибки и деградация

- Нет LLM env — warn с именами переменных, LLM-шаг пропускается, импорт зелёный.
- Отказ провайдера — warn, накопленные строки пишутся, догон следующим прогоном.
- Ошибка формата — один повтор; второй подряд — батч пропускается, догон следующим прогоном.
- Ошибка чтения кандидатов/записи — warn, импорт не краснеет; LLM-разметка не отменяет запись новостей.
- Вставка — одним `PrepareBatch`; `Abort` при ошибке `Append`, частичный батч не отправляется.

### 4.6 Стоимость и бюджет

- Тариф `zai-org/GLM-5.3-Flash` (Joingonka, 2026-10-17): $0,0076/1M input, $0,023/1M output; шлюз добавляет 10% network cost и 5% конвертации.
- Предварительно: текущий бэклог ≈400–430 позиций (включая переразметку Telegram v1 → v2), input ≈140–170K токенов, output ≈50–70K → ≈$0,002–0,003; ≈$0,007 за 1000 позиций.
- Фактические токены пишутся в лог на каждый запрос: OpenAI — `usage.prompt_tokens`/`usage.completion_tokens`, Anthropic — `msg.Usage.InputTokens`/`OutputTokens`.
- Время: 43–60 запросов, ≈5–15 мин на бэкфилл с лимитом 500; штатные 50 позиций — 5–10 запросов, укладываются в 30-минутное расписание.
- `NEWS_LLM_BACKLOG_LIMIT` — операционный рычаг: штатно 50, разовый бэкфилл — больше (до 1000).

## 5. Модель данных

DDL не меняется. Новые колонки — только в витрине `v_news_events`:

| Колонка | Тип | Смысл |
|---|---|---|
| `content_hash` | `String` | sha256 `title+"\n"+description`; `''` — строка до миграции, не аннотируема |
| `sentiment` | `Nullable(Float32)` | −1…1; NULL — не размечено |
| `relevance` | `Nullable(Float32)` | 0…1; NULL — не размечено |
| `summary` | `Nullable(String)` | одно предложение; NULL — не размечено |
| `model` | `LowCardinality(String)` | модель последней разметки; `''` — разметки нет |
| `prompt_version` | `UInt16` | редакция промпта; 0 — разметки нет |
| `tags` | `Array(Tuple(LowCardinality(String), LowCardinality(String), LowCardinality(String)))` | `(dimension, tag, direction)`; `[]` — тегов нет/не размечено |

## 6. Тесты

- Кандидатный SQL: содержит `match(title, newsFilter)`, `match(description, newsFilter)`, Telegram-условие, `LIMIT`; не содержит окна 35 дней.
- `newsLLMBacklogLimitFromEnv`: дефолт, валидное значение, 0/−1/>1000/нечисло.
- `newsLLMBatches`: 11 коротких → `[10,1]`; длинные → по бюджету; один сверхбюджетный → `[1]`.
- Промпт/реестр: редакция 2, все 46 слагов, новые правила; счётчики измерений.
- Evidence: цитата >300 рун обрезается и принимается, если префикс — подстрока; цитата вне текста отбрасывается.
- `v_news_events`: 14 колонок, 55 стейтментов витрин, комментарии таблицы и каждой колонки, экранирование литералов, `LIMIT 1 BY`, NULL-семантика.
- Фейковое соединение: кандидаты по префиксу запроса; аргументы `(model, prompt_version)`; запись doc/tag строк.
- Живая приёмка: см. §7.

## 7. Критерий приёмки

1. `make all` — зелёный; `go test -race -count=1 ./news/` — `ok`.
2. Живой прогон `NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch` размечает все аннотируемые позиции `v_news_events` (`content_hash != ''`) для `prompt_version=2`: `countIf(content_hash != '' AND model != '') = countIf(content_hash != '')`, по RSS — то же; строки с `content_hash = ''` остаются `model = ''`; число строк витрины не меняется.
3. `v_news_tags` — 46 тегов; `news_annotation` содержит doc- и tag-строки `model='zai-org/GLM-5.3-Flash'`, `prompt_version=2`.
4. Повторный прогон — no-op: кандидатов 0, провайдер не вызывается, строки не растут.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/news.yaml` — exit 0.
6. `news_events.sentiment` — NULL; `v_news_events.sentiment`/`relevance`/`summary` — NULL у неразмеченных, значения у размеченных; `tags` — `[]` или массив.
7. Стоимость зафиксирована: токены из логов, USD по тарифу от 2026-10-17, оговорка о наценке шлюза; research-заметка и протокол PR.
8. README, ARCHITECTURE, роадмап обновлены тем же PR; issue #122 содержит `Acceptance`/`Spec`/`Plan`, PR — `Closes #122`.

## 8. Риски

| Риск | Митигация |
|---|---|
| Длинный пост smart-lab раздувает батч | Бюджет 40 КБ; один пост — отдельный батч |
| Отказ/таймаут GLM | warn, накопленные строки пишутся, догон следующим прогоном; идемпотентность по ключу |
| Смена промпта переразмечает Telegram | Ожидаемо: v2 ложится рядом с v1; стоимость мала, старые строки не затираются |
| `LIMIT 1 BY` и `FINAL` замедляют витрину | `news_annotation` — сотни строк; при росте — отдельное решение (materialized view) |
| Строки до миграции без `content_hash` | В кандидаты не попадают, остаются `model=''`; в витрине виден `content_hash`, критерий приёмки это учитывает |
| Ошибка формата на длинном батче | Один повтор; при повторе — батч пропускается, догон следующим прогоном |

## 9. Вне объёма

- Разметка сырого RSS вне `v_news_events`.
- Заполнение `news_events.sentiment`.
- Новые витрины.
- Provider Batch API.
- Изменение расписания `dagu/news.yaml`, перевод, торговые сигналы.
