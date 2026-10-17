# Корпоративный Telegram-канал edisclosure — design

Дата: 2026-10-17. Пункт: `docs/ROADMAP_KEY_RATE.md` §6, фаза 1 — новый **1.11**
«Корпоративный Telegram-канал `edisclosure`». База — пункт **1.9** (Telegram-транспорт
`news_watch`, ветка `feat/telegram-flash-feed`): схема и парсер уже готовы, новый источник —
вторая лента того же транспорта.

## 1. Пробел

`news_watch` после 1.9 собирает Telegram-молнию Интерфакса, но корпоративные раскрытия
(эмитенты, дивиденды, отчётность, IPO/SPO, советы директоров, листинг) в контуре не
представлены. Для DCF-моделей секторов и конкретных компаний нужен отдельный поток
корпоративных новостей, который не обязан проходить через фильтр тезисов ключевой ставки.

## 2. Источник (живая проба 2026-10-17)

| Параметр | Значение |
|---|---|
| URL превью | `https://t.me/s/edisclosure` — HTTP 200, ~167 КБ HTML |
| Канал | `edisclosure`, заголовок «Интерфакс-ЦРКИ» — корпоративные раскрытия |
| Окно | 20 сообщений; 2026-09-16 09:18 UTC … 2026-10-16 10:03 UTC ≈ 30 дней |
| Разметка | та же, что у `interfaxonline`: `div.tgme_widget_message[data-post="edisclosure/<id>"]`, `time[datetime]` (RFC3339), `div.tgme_widget_message_text` |
| Особенности | все 20 сообщений с текстом (медиа без текста в окне нет); тексты длиннее молнии, первая строка — заголовок; ссылки и кавычки в HTML |

30-минутный опрос при окне ~30 дней имеет большой запас; простой дольше месяца теряет
позиции, `NEWS_WATCH_MODE=check` покажет дыру.

## 3. Решение

- Новая лента `edisclosure_tg` в `feeds()` сразу после `interfax_tg`:
  `Source='edisclosure'`, `URL=https://t.me/s/edisclosure`, `Kind=feedTelegram`.
- Отдельный `source='edisclosure'` (не `interfax`): это другой канал и другая редакция
  корпоративного контура; в `news_message_version` канал различается полем `channel`.
- Схема **не меняется**: `news_events`, `news_message_version`, `news_annotation` уже
  generic; `content_hash`, версии правок/удалений и LLM-разметка работают для любого
  Telegram-канала.
- `newsFilter` **не расширяется** корпоративными стемами: `v_news_events` остаётся
  фильтром тезисов ставки/золота, а полный корпоративный поток агент читает из
  `v_news_flash`:
  `SELECT date, title, url, description, tags FROM v_news_flash WHERE channel = 'edisclosure' ORDER BY date DESC`.
  Отдельная витрина `v_news_corporate` не заводится: она дублировала бы `v_news_flash`
  (те же колонки, комментарии и грант), а `channel` уже входит в витрину. Это осознанное
  решение в пользу переиспользования.
- LLM-разметка 1.10 покрывает `edisclosure` автоматически: кандидаты выбираются по
  `url LIKE 'https://t.me/%'`, без привязки к каналу. До живой приёмки 1.10 теги пусты.
- `v_news_flash` — это слой всех Telegram-каналов, а не только «молнии»: комментарии
  таблицы и колонок `source`/`channel` обновляются (interfaxonline + edisclosure).

## 4. Тесты

- Фикстура `news/testdata/edisclosure_tg.html` — 4 реальных сообщения из окна (первое,
  с кавычками/ссылками, и ещё три; без правки разметки).
- `news_test.go`: `feeds()` = 9, порядок `interfax, interfax_tg, edisclosure_tg, rbc, …`,
  `sources` = `interfax, interfax, edisclosure, rbc, cbr, cbr, tass, smartlab, smartlab`;
  `pointFeedsAt` подменяет и `feedEdisclosureURL`; `TestFetchFeeds` = 9; `TestFeedsHaveFixtures`
  = 9; `fixtureInventory` вмещает 9.
- `parse_telegram_test.go`: `checkTelegramChannel` уже покрыт; для `edisclosure` достаточно
  пина первого сообщения в общей фикстуре (channel/id/date/url/title/description).
- `schema_test.go`: комментарии `v_news_flash`/`v_news_events` о двух каналах (если пины
  проверяют текст комментариев; иначе — только код).
- `scripts/mcp_check.py`: `SELECT count() FROM v_news_flash WHERE channel = 'edisclosure'` > 0
  (новая проверка; существующая `v_news_flash` > 0 остаётся).

## 5. Документы

- `ARCHITECTURE.md` — §2/§3 (9 лент, `edisclosure`), §6.2 комментарий `channel`, §6.3
  `v_news_flash` (два канала), §7 расписание, верхняя строка (1.11).
- `README.md` — 9 лент, `edisclosure`, пример запроса по `channel='edisclosure'`.
- `docs/ROADMAP_KEY_RATE.md` — пункт 1.11 (✅ после приёмки).
- `docs/KEY_RATE_CHANNELS.md` §9 — корпоративный поток и его отличие от молнии.
- `docs/GOLD_THESIS_DATA_ROADMAP.md` — новостной раздел: 9 лент.
- `dagu/news.yaml` — комментарий (9 лент, окно edisclosure ≈30 дней).

## 6. Приёмка

1. `make all` — зелёный.
2. `make import STAT=news_watch` (режим `all`): 9 лент; `edisclosure_tg: позиций 20,
   новых версий 20, удалений 0`; `Imported N rows`; LLM-шаг — warn (env не выставлен).
3. `NEWS_WATCH_MODE=since` — текущие сообщения обоих каналов upsert, `новых версий 0`.
4. `NEWS_WATCH_MODE=check` — источник `edisclosure` в покрытии, дыр нет.
5. MCP (`kimi_reader`): `v_news_flash` — 40 строк (20 interfaxonline + 20 edisclosure),
   `WHERE channel='edisclosure'` — 20; `v_news_message_versions` — 40; сырые таблицы
   закрыты; `make mcp-check` — зелёный.
6. Статус 1.11 — ✅ с числами; протокол — комментарием в PR.

## 7. Ограничения

- Окно 20 сообщений ≈ 30 дней; простой дольше окна теряет позиции (как у всех лент).
- Медиа без текста пропускаются, ID остаётся в `SeenIDs` (не ложный tombstone).
- LLM-теги появятся после живой приёмки 1.10; до этого `v_news_flash.tags` пуст,
  `sentiment`/`relevance` = 0 — «не размечено», а не «нейтрально».
- Компания-эмитент в тегах не выделяется: реестр 1.10 — `person|country|sector|event|cb_channel`;
  конкретные компании — follow-up расширения реестра, не этот пункт.
