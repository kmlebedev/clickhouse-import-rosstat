---
name: news-source-onboarding
description: Use when adding an RSS feed or public Telegram channel to the news_watch news contour in clickhouse-import-rosstat
type: prompt
whenToUse: When a roadmap item or issue asks to add a news source, Telegram channel, or RSS feed to news_watch
---

# Добавление источника в `news_watch`

**Назначение.** Повторяемый процесс добавления публичного RSS или Telegram-превью в контур `news_watch`: от пункта роадмапа до живой приёмки и bootstrap-разметки. Источник истины о лентах — `feeds()` в `news/news.go`; нормативная схема — [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.2/§6.3; стандарт пакета — [IMPORTER_PACKAGE_STANDARD.md](../../../docs/IMPORTER_PACKAGE_STANDARD.md) §16.1 п. 8. Спека первого применения — [2026-10-18-news-source-onboarding-design.md](../../../docs/superpowers/specs/2026-10-18-news-source-onboarding-design.md).

**Что прочитать перед стартом.** `ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7; `news/news.go`, `news/parse.go`, `news/parse_telegram.go`, `news/telegram_version.go`, `news/schema.go`, `news/tags.go`, `news/annotation_llm.go`; спека/план [2026-10-18-rss-llm-annotation-design.md](../../../docs/superpowers/specs/2026-10-18-rss-llm-annotation-design.md) и PR [#176](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/176) — runbook bootstrap-разметки; скилы `github-tracker`, `sync-readme-architecture`.

## 1. Пункт роадмапа и issue

Назвать владеющий пункт и критерий приёмки до кода. Пакет источников живёт в issue (например, #46), механизм — в `infra/2.3`. Ветка — от `origin/main`; спека и план — первыми коммитами. В PR: `Roadmap: <slug>/<item-id>`, `Closes #N`, протокол живой приёмки — комментарием.

**Правило закрытия:** `Closes #N` — только если выполнен весь пакет issue. Один канал из пакета — `Refs #N`, issue остаётся открытым. Перед объявлением готовности сверить фактический `feeds()` и статус роадмапа: статус не должен опережать код.

## 2. Живая проба источника

До кода проверить и записать в спеку:

- HTTP 200 без авторизации, ключей и bot-token; только публичные источники. Telegram — **`https://t.me/s/<channel>`** (превью), не `https://t.me/<channel>`: второй адрес не отдаёт окно сообщений.
- RSS: корень `<rss><channel><item>`; `pubDate` — одна из трёх форм (`RFC1123Z`, без секунд, `RFC1123` только `GMT`/`UTC`); `title`/`link`/`description`; число позиций и окно (min…max).
- Telegram: `div.tgme_widget_message[data-post]`, `time[datetime]` (RFC3339), `div.tgme_widget_message_text`; `data-post` совпадает с именем канала в URL (регистр `MarketOverview` сохраняется); медиа без текста — `skipped`, но ID остаётся в `SeenIDs`; подпись канала (`@channel`) снимается парсером.
- Окно против расписания: `dagu/news.yaml` — каждые 30 минут. Если 20 сообщений канала покрывают меньше 30 минут, сначала решить про расписание/фильтр, потом писать код.
- Фильтр: прогнать заголовки через `newsFilter`; 0 попаданий — повод для отдельного решения, а не молчаливой правки.
- Зафиксировать окно в спеке; закреплённый старый пост может растянуть `min(date)`.

## 3. Транспорт и `source`

- `Key` — имя фикстуры: `<канал>_tg` для Telegram, имя источника для RSS.
- `Source` — редакция/провайдер. Несколько лент одного провайдера делят `source` (`interfax` ×3, `smartlab` ×4); `v_news_flash.channel` и `url` различают каналы. Не делать `source` из имени фикстуры.
- `Kind` обязателен: `feedTelegram` для превью, `feedRSS` для RSS. Нулевое значение `feedKind` — это `feedRSS`; пропущенный `Kind` у Telegram ищет `.rss`-фикстуру и падает.
- Новый парсер — только если формат действительно другой; новые раскладки даты — с тестом. `util.GetXML`/`util.GetPage` — единственный транспорт.

## 4. Конфиг `feeds()`

Добавить URL-переменную и запись в `feeds()` в логической группе. Порядок — контракт лога и отчёта `check`: обновить пин порядка и комментарий («N лент», группировка, окна). Для RSS — позиция рядом с лентами того же провайдера; для Telegram — рядом с другими каналами.

**Правило порядка:** весь пакет issue вставляется строго по таблице источников; одиночный канал можно дописать в конец, но если он часть пакета — место фиксируется таблицей (например, `markettwits_tg` после `newssmartlab_tg`), иначе следующие каналы встанут не в тот ряд.

## 5. Фикстуры и тесты

- Снять реальную фикстуру: `news/testdata/<key>.rss` или `.html` (3–4 сообщения или полное живое окно; для полного окна пинить `len(Messages)` и `Skipped`). Разметку не собирать руками; `TestFeedsHaveFixtures` ловит и отсутствие, и «мёртвую» фикстуру.
- Обновить `pointFeedsAt` — единую таблицу «переменная → путь» (подмена и восстановление всех URL на `httptest`); отдельный save/restore в `TestImportRejectsUnknownModeBeforeDB` не нужен, если тест использует helper.
- `TestFetchFeeds...`: число лент и фиксированный порядок ключей/источников. `TestFeedsHaveFixtures`: число лент. `fixtureInventory`: capacities/comments.
- Тесты, где перечислены Telegram-каналы, перевести на helper по `Kind == feedTelegram` (или дополнить все ключи); тест «все ленты упали» — на helper по `feeds()`. Не ослаблять assertions.
- Имена, которые ломаются чаще всего: `TestImportAllModeWritesAllFeeds` (версионный батч из двух ключей), `TestImportSinceModeWritesOnlyMissing` (`tgCount`), `TestImportSinceModeCbrSecondFeedNoWatermark` (switch по `source`), `TestImportSinceModeTelegramVersionsAndTombstone` (`wantVersions` из `edisclosureItems`), `TestImportTelegramFeedFailureNoTombstone` и `TestImportNoopRunAnnotatesBacklog` (валят только два канала), `TestImportOneTelegramFeedFailureIsolated` (предполагает ровно два канала).
- Добавить пин первого сообщения каждой Telegram-фикстуры: `Channel`, `MessageID`, `Date`, `Title`, `URL`; для RSS — пин первой позиции.
- `news/schema_test.go`: число стейтментов не меняется, если править только текст комментариев; проверить баланс кавычек.
- Саботаж: убрать ленту → падает пин числа; `Kind: feedRSS` у Telegram → падает фикстура; подменить `Source` → падает пин; переставить порядок → падает пин.

## 6. Схема, витрины, теги

Новых таблиц и витрин не заводить: `news_events` / `news_message_version` / `news_annotation` generic. Обновить только SQL-комментарии `news/schema.go` (`v_news_events` — состав `source`/`transport`; `v_news_flash` — список каналов; `v_news_message_versions` — каналы) и синхронно `ARCHITECTURE.md`. Одиночные кавычки в литералах удваивать.

Inline-комментарии DDL (`newsMessageVersionDdl`, `-- interfaxonline | edisclosure`) меняются только вместе с копией DDL в `ARCHITECTURE.md §6.2`; если DDL по спеке не меняется — оставить и не создавать расхождение.

Семантика разметки: в `v_news_flash` `sentiment`/`relevance` = 0 и `summary` пусто при отсутствии doc-строки; в `v_news_events` — `NULL`. Это «не размечено», а не «нейтрально»; не путать в комментариях и проверках.

`newsFilter` расширять только с живым примером и тестом; сырая `news_events` хранит весь поток. Реестр тегов не трогать: новые Telegram-сообщения — кандидаты всегда, RSS — при прохождении фильтра; смена модели/промпта не затирает старую разметку.

## 7. Расписание

`dagu/news.yaml`: обновить комментарий состава/окон; `schedule: "7,37 * * * *"`, `overlap_policy: skip`, `NEWS_WATCH_MODE=since`, retry — не менять без отдельного решения. Проверить `dagu validate dagu/news.yaml`.

## 8. Bootstrap LLM-разметки нового потока (обязательно)

Новые позиции становятся кандидатами автоматически, но штатные 50 за прогон растянут первый проход. После записи новых лент выполнить разовый прогон по runbook PR #176:

```bash
NEWS_LLM_PROFILE=glm NEWS_LLM_BACKLOG_LIMIT=500 make import STAT=news_watch
```

- лимит 500 — разовый; максимум 1000; если кандидатов больше — повторить;
- модель/промпт — фактические `model` и `prompt_version=2` (обычно `zai-org/GLM-5.3-Flash`); ключ `news_annotation` включает модель и редакцию;
- точный предикат: Telegram — `v_news_flash WHERE channel = '<channel>'`; RSS — `v_news_events WHERE source = '<source>'`; не считать формулу по пустому срезу;
- `v_news_events`: `countIf(content_hash != '' AND model != '') = countIf(content_hash != '')` по новому потоку и по всему видимому;
- `v_news_flash`: для новых каналов `uniqExactIf(message_id, model != '') = uniqExact(message_id)` (не `count()`: после смены модели/промпта строк несколько);
- `news_annotation`: doc/tag строки с фактической `model` и `prompt_version=2`;
- повтор — no-op: кандидатов 0, провайдер не вызывается;
- токены prompt/completion и USD по тарифу — в протокол;
- без LLM env/при отказе провайдера импорт зелёный, но onboarding **не закрыт**: пометка «разметка не выполнена» и follow-up issue.

## 9. `make all`

`make all` — gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки. Отдельно `go test ./news/... -count=1`.

## 10. Живая приёмка и MCP

```bash
make env-check
make ch-status                 # если ClickHouse не отвечает — попросить владельца, не поднимать самому
make import STAT=news_watch
NEWS_WATCH_MODE=since make import STAT=news_watch
NEWS_WATCH_MODE=check make import STAT=news_watch
```

Проверить через MCP `kimi_reader`:

- новые каналы: `uniqExact(message_id)` и `uniqExactIf(message_id, model != '')` в `v_news_flash` — сообщения есть, `annotated = messages`;
- новые источники в `v_news_events` — `countIf(content_hash != '' AND model != '') = countIf(content_hash != '')`; если канал не проходит `newsFilter`, 0 строк там — не ошибка, полный поток в `v_news_flash`;
- `v_news_message_versions` — версии есть, tombstone не обязателен (эвристика: закреплённое/скрытое сообщение может дать ложный tombstone; title в `news_events` фиксируется по v1 — это ограничение, а не баг);
- сырые `news_events`/`news_message_version`/`news_annotation` — `ACCESS_DENIED`;
- `scripts/mcp_check.py`: добавить per-channel проверку нового канала — `uniqExact(message_id) = uniqExactIf(message_id, model != '')`; иначе `make mcp-check` зеленеет от одной размеченной строки в одном канале;
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/news.yaml` — exit 0. `make mcp-user` не нужен: новых витрин/грантов нет.

## 11. Docs и PR

`ARCHITECTURE.md` §2/§6.1/§6.2/§6.3/§7 + шапка; `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/DEVELOPMENT.md`, `docs/ROADMAP_KEY_RATE.md` (текущая таблица, не исторические ✅), `docs/GOLD_THESIS_DATA_ROADMAP.md` (владеющий follow-up), `docs/ROADMAP_INFRA.md` (статус пункта); при необходимости `docs/KEY_RATE_CHANNELS.md`; `docs/OPERATIONS.md` — только если там есть текущие числа лент. Синк — по `sync-readme-architecture`.

Роадмап — статус пункта с командой и результатом. Перед PR сверить статус с фактическим `feeds()` и кодом. Протокол приёмки — комментарием в PR: `make all`, число лент, покрытие `check`, bootstrap (батчи, 100 % аннотируемых, no-op, токены/стоимость), MCP-числа, дубли (если есть), `dagu validate`.

## Текущий состав лент (обновлять при каждом добавлении)

| Key | Kind | Source | Окно |
|---|---|---|---|
| `interfax` | RSS | `interfax` | 25 |
| `interfax_tg` | TG | `interfax` | 20, ≈17 ч |
| `ifax_go_tg` | TG | `interfax` | 20, ≈12–36 ч |
| `edisclosure_tg` | TG | `edisclosure` | 20, ≈30 дн |
| `rbc` | RSS | `rbc` | 30 |
| `cbr_news` | RSS | `cbr` | 100 |
| `cbr_press` | RSS | `cbr` | 100 |
| `tass` | RSS | `tass` | 100 |
| `smartlab` | RSS | `smartlab` | 20, ≈14,5 ч |
| `smartlab_lebedev` | RSS | `smartlab` | 20 |
| `smartlab_spydell` | RSS | `smartlab` | 20 |
| `newssmartlab_tg` | TG | `smartlab` | 20, ≈23 ч |
| `markettwits_tg` | TG | `markettwits` | 19–20, ≈12 ч |
| `russianmacro_tg` | TG | `russianmacro` | 7–20, ≈2 сут |
| `marketoverview_tg` | TG | `marketoverview` | 17–20, ≈2 сут |
| `mozgovikresearch_tg` | TG | `mozgovikresearch` | 20, ≈14 сут |

## Чего не делать

- Не добавлять приватные/auth-источники, bot-token, Telegram API; только `t.me/s/<channel>`.
- Не заводить per-source таблицы/витрины/гранты; не дублировать `v_news_flash` отдельной витриной канала.
- Не считать `0 items` успехом: пустая лента — ошибка, `skipped` — сигнал смены формы.
- Не менять расписание только в комментарии и не подменять `schedule` «на глаз».
- Не смешивать несвязанные редакции под одним `source` и не делать `source` из имени фикстуры.
- Не ослаблять тесты вместо обновления пинов; `pointFeedsAt` — единая таблица подмены и восстановления всех URL.
- Не расширять `newsFilter`/реестр тегов без живого примера и отдельного решения.
- Не закрывать источник без bootstrap-разметки; без LLM env — follow-up, а не «принято».
- Не закрывать пакет issue одним каналом (`Closes` только для полного пакета).
- Не переписывать исторические ✅-строки роадмапов; менять текущие таблицы и статус пункта.
