# news-source-onboarding — дизайн

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** `infra/2.3` — [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md) §3, фаза 2.
**Пакет работ:** issue [#46](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/46) (Telegram-каналы и RSS Смартлаба), родитель — [#146](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/146) (`gold-thesis/phase-4-followup-4`).
**План:** [2026-10-18-news-source-onboarding.md](../plans/2026-10-18-news-source-onboarding.md).
**Сопутствующие документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §2/§6.1/§6.2/§6.3/§7; [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md) §16.1 п. 8; спека [2026-10-18-rss-llm-annotation-design.md](2026-10-18-rss-llm-annotation-design.md) и PR [#176](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/176) — runbook bootstrap-разметки; скилы `github-tracker`, `sync-readme-architecture`.

## 1. Пробел

Контур `news_watch` (`news/`) собирает новости в `news_events` и публикует витрины `v_news_events`, `v_news_flash`, `v_news_message_versions`, `v_news_tags`. Добавление источника сейчас — знание в голове и в истории коммитов: нужно одновременно поправить `feeds()`, снять фикстуру, обновить несколько пинов тестов, комментарии витрин, `dagu/news.yaml`, docs и провести живое bootstrap-разметки. Пропуск любого шага даёт тихую деградацию: лента не покрыта фикстурой, витрина не описывает канал, новый поток остаётся без LLM-разметки, а `check` не видит дыру.

Issue #46 просит добавить шесть новых Telegram-каналов и общий RSS Смартлаба. Без повторяемого механизма следующая пачка источников повторит те же грабли.

## 2. Цель и объём

**Цель:** зафиксировать процесс добавления новостного источника скилом `news-source-onboarding` и применить его к пакету #46 в том же PR.

**В объёме:**

- скил `.kimi-code/skills/news-source-onboarding/SKILL.md` — 11-шаговый чек-лист от пункта роадмапа до живой приёмки, с обязательным bootstrap-разметки;
- шесть новых Telegram-лент: `ifax_go`, `markettwits`, `newssmartlab`, `russianmacro`, `MarketOverview`, `mozgovikresearch`;
- общий RSS Смартлаба `https://smart-lab.ru/rss/` (`smartlab`);
- фикстуры, пины парсинга, обновление тестов и комментариев витрин;
- bootstrap LLM-разметки нового потока по runbook PR #176;
- docs, MCP-проверка, трекер `infra/2.3`, закрытие #46.

**Вне объёма:**

- новые таблицы, витрины, колонки и гранты (схема generic, не меняется);
- приватные каналы, Telegram API/bot-token, авторизация;
- расширение `newsFilter` и реестра тегов без отдельного решения;
- изменение расписания `dagu/news.yaml` (30 минут достаточно);
- бэкфилл истории за годы (follow-up 5) и остальные источники #146.

## 3. Источники и живая проба (2026-10-18)

| Key | Kind | Source | URL | Окно |
|---|---|---|---|---|
| `interfax` | RSS | `interfax` | `https://www.interfax.ru/rss` | 25 |
| `interfax_tg` | TG | `interfax` | `https://t.me/s/interfaxonline` | 20, уже есть |
| `ifax_go_tg` | TG | `interfax` | `https://t.me/s/ifax_go` | 20, ≈12–36 ч |
| `edisclosure_tg` | TG | `edisclosure` | `https://t.me/s/edisclosure` | 20, уже есть |
| `rbc` | RSS | `rbc` | `https://rssexport.rbc.ru/rbcnews/news/30/full.rss` | 30 |
| `cbr_news` | RSS | `cbr` | `https://www.cbr.ru/rss/RssNews` | 100 |
| `cbr_press` | RSS | `cbr` | `https://www.cbr.ru/rss/eventrss` | 100 |
| `tass` | RSS | `tass` | `https://tass.ru/rss/v2.xml` | 100 |
| `smartlab` | RSS | `smartlab` | `https://smart-lab.ru/rss/` | 20, ≈14,5 ч |
| `smartlab_lebedev` | RSS | `smartlab` | `https://smart-lab.ru/rss/personal_blog/KonstantinLebedev/` | 20 |
| `smartlab_spydell` | RSS | `smartlab` | `https://smart-lab.ru/rss/personal_blog/spydell-telegram/` | 20 |
| `newssmartlab_tg` | TG | `smartlab` | `https://t.me/s/newssmartlab` | 20, ≈23 ч |
| `markettwits_tg` | TG | `markettwits` | `https://t.me/s/markettwits` | 19–20, ≈12 ч |
| `russianmacro_tg` | TG | `russianmacro` | `https://t.me/s/russianmacro` | 7–20, ≈2 сут |
| `marketoverview_tg` | TG | `marketoverview` | `https://t.me/s/MarketOverview` | 17–20, ≈2 сут |
| `mozgovikresearch_tg` | TG | `mozgovikresearch` | `https://t.me/s/mozgovikresearch` | 20, ≈14 сут |

Проба: все URL отдают 200 без авторизации и ключей; `data-post` совпадает с именем канала в URL (у `MarketOverview` — с сохранением регистра); `time[datetime]` — RFC3339; текстовые узлы разбираются. Самое короткое окно (≈12 ч) с запасом перекрывает 30-минутный опрос; отдельный быстрый DAG не нужен.

Проверка фильтра по живым заголовкам: 3–14 попаданий из ≈20 сообщений на канал. Существующий `newsFilter` покрывает новый поток; расширение не требуется.

## 4. Дизайн

### 4.1 Скил

Скил — reference/technique, а не narrative. Frontmatter: `name: news-source-onboarding`, `description` только с триггером («Use when adding an RSS feed or public Telegram channel…»), `type: prompt`, `whenToUse`.

Структура:

1. назначение и границы (только публичные источники, generic-схема, `util`-транспорт);
2. что прочитать (ARCHITECTURE, `news/`, стандарт импортёров, спека/PR #176, `github-tracker`, `sync-readme-architecture`);
3. 11-шаговый чек-лист;
4. правила имён и группировки `source`;
5. bootstrap-разметки;
6. таблица текущих 16 лент;
7. приёмка и «чего не делать».

### 4.2 Правила источника

- `Key` — имя фикстуры: `<канал>_tg` для Telegram, имя источника для RSS.
- `Source` — редакция/провайдер. Несколько лент одного провайдера делят `source` (`interfax` ×3, `smartlab` ×4); `v_news_flash.channel` и `url` различают каналы.
- `MarketOverview` в URL сохраняет регистр; `source` — `marketoverview`.
- Порядок `feeds()` — контракт лога и отчёта `check`; новые ленты вставляются логическими группами, пин порядка обновляется явно.

### 4.3 Транспорт и парсинг

Новых парсеров нет: RSS идёт через `util.GetXML`/`rssDoc`, Telegram — через `util.GetPage`/`parseTelegramPage`. Для каждого нового Telegram-канала добавляется реальная HTML-фикстура (3–4 сообщения или полное живое окно; для полного окна пин `len(Messages)` и `Skipped`) и пин `Channel`/`MessageID`/`Date`/`Title`/`URL`. `checkTelegramChannel` защищает от редиректа/подмены канала. Пустая лента — ошибка, `skipped` — сигнал смены формы.

### 4.4 Расписание и фильтр

`dagu/news.yaml` остаётся `7,37 * * * *`, `overlap_policy: skip`, режим `since`. Окна новых каналов 12 ч–14 сут, общий RSS 14,5 ч — 30 минут перекрывает. `newsFilter` не меняется; сырая `news_events` хранит весь поток.

### 4.5 Bootstrap LLM-разметки

Новые позиции попадают в кандидаты автоматически: Telegram — всегда (`url LIKE 'https://t.me/%'`), RSS — при прохождении `newsFilter`. Штатные 50 позиций за прогон растянули бы первый проход, поэтому onboarding включает разовый прогон по runbook PR #176:

```bash
NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch
```

Критерии:

- `v_news_events`: `countIf(content_hash != '' AND model != '') = countIf(content_hash != '')` по новому потоку и по всему видимому;
- `v_news_flash`: `uniqExactIf(message_id, model != '') = uniqExact(message_id)` для новых каналов;
- `news_annotation`: doc/tag строки `model='zai-org/GLM-5.3-Flash'`, `prompt_version=2`;
- повтор — no-op (кандидатов 0, провайдер не вызывается);
- токены и USD зафиксированы в протоколе.

Без LLM env или при отказе провайдера импорт зелёный, но onboarding не закрыт: в PR — пометка «разметка не выполнена» и follow-up issue.

### 4.6 Схема и витрины

DDL не меняется. Обновляются только комментарии `v_news_events` (состав `source`/`transport`), `v_news_flash` и `v_news_message_versions` (список каналов). `v_news_events`/`v_news_flash` generic, новые источники видны без правки определений. Новых грантов нет.

### 4.7 Тесты

- `TestFeedsHaveFixtures`: 16 лент, у каждой фикстура, лишних фикстур нет.
- `TestFetchFeeds...`: 16 результатов, фиксированный порядок ключей и меток источников.
- Пины новых Telegram-фикстур: канал, ID, дата, заголовок, URL первого сообщения.
- RSS-фикстура `smartlab.rss` разбирается, `source='smartlab'`.
- Тесты «все Telegram-ленты» и «все ленты упали» переводятся на helper, производный от `feeds()`, либо дополняются новыми ключами; assertions не ослабляются.
- Офлайн: сеть только `httptest`, БД не нужна.

### 4.8 MCP и docs

`scripts/mcp_check.py` получает per-channel чек по шести новым каналам: каждый канал присутствует и `uniqExactIf(message_id, model != '') = uniqExact(message_id)` (bootstrap покрыл все сообщения), а не только «одна строка размечена». `v_news_flash` уже выдан `kimi_reader`; `sql/mcp_kimi_reader.sql` не меняется. Docs — по `sync-readme-architecture`: ARCHITECTURE §2/§6.1/§6.3/§7, README, IMPORTERS, LIMITATIONS, DEVELOPMENT, ROADMAP_KEY_RATE, GOLD_THESIS (текущие таблицы; OPERATIONS — только если там есть числа лент); роадмапы `infra/2.3` и follow-up 4.

### 4.9 RED baseline скила (2026-10-18)

Read-only `plan`-субагент без скила составил план добавления `markettwits` и нашёл базовые шаги, но часть важного вынес только в «что легко пропустить»: явную проверку `scripts/mcp_check.py`, правило `Closes` только для полного пакета, `uniqExact` вместо `count()` в `v_news_flash`, семантику `0` vs `NULL` между витринами, inline-комментарии DDL, docs `DEVELOPMENT.md`/`ROADMAP_KEY_RATE.md`, точный предикат bootstrap по транспорту, актуальную модель из разметки, `fixtureInventory`, `make mcp-user` не нужен, tombstone/title v1 в ограничениях. Скил делает эти пункты обязательными шагами; GREEN-прогон с текстом скила назвал все 11 шагов и bootstrap.

## 5. Критерий приёмки

1. Скил существует, виден в project skills, проверен RED/GREEN-сценарием; bootstrap-разметки — обязательный пункт.
2. `feeds()` — 16 лент; пины фикстур, порядка и источников зелёные.
3. `make all` — зелёный.
4. Живой прогон: все новые каналы и RSS дают позиции; `check` без дыр; витрины видят новые источники; сырая `news_events` закрыта.
5. Bootstrap-разметка: 100 % аннотируемых позиций нового потока, повтор — no-op, токены/стоимость записаны.
6. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/news.yaml` — exit 0.
7. Docs синхронизированы; `infra/2.3` и #46 обновлены; протокол приёмки — комментарием в PR.

## 6. Риски

| Риск | Митигация |
|---|---|
| Смена разметки Telegram | селекторы-константы, HTML-фикстуры, пин первого сообщения, `skipped` в логе |
| Канал переименован/закрыт | `checkTelegramChannel`; пустой канал — ошибка ленты, а не тихий ноль |
| Окно канала короче расписания | живая проба перед кодом; при необходимости — решение о расписании/фильтре отдельно |
| Дубли общего RSS Смартлаба с персональными блогами | проверка `(date, title)` на приёмке; фиксация в LIMITATIONS, удаление лент — решение владельца |
| WAF/редирект | `util.GetXML` проверяет статус и пин `rss`; Telegram-парсер требует узлы сообщений |
| LLM-бэклог больше лимита | `NEWS_LLM_BACKLOG_LIMIT=500` (макс. 1000), повтор; без env — follow-up, onboarding не закрыт |

## 7. Вне объёма

Новые таблицы/витрины/гранты, приватные каналы и Telegram API, расширение `newsFilter`/реестра тегов, изменение расписания, исторический бэкфилл, остальные источники #146.
