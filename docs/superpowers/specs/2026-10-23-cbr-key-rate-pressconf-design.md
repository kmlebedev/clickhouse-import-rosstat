# Источник стенограмм пресс-конференций ЦБ по ключевой ставке — спека

**Дата:** 23 октября 2026 г.
**Пункт:** `key-rate/phase-4-cbr-key-rate-pressconf`, issue [#370](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/370).
**Роль:** Go.
**Зависимости:** нет; follow-up к `key-rate/phase-4-cbr-key-rate-press` (#368, смержен).

## 1. Контекст и цель

В контуре ключевой ставки уже есть два постзаседания-источника: пресс-релиз (`cbr_key_rate_press`, #368) и резюме обсуждения (`cbr_mp_summary`). Не хватает третьего — **пресс-конференции Председателя Банка России**: вопросов-ответов журналистов, где ЦБ живым языком проговаривает сигнал, риски, оговорки и объясняет решение. Пользовательский триггер — видео [«Пресс-конференция по ключевой ставке 23 октября 2026 года»](https://www.youtube.com/watch?v=WjLQNQPRc6o).

Цель — импортёр `cbr_key_rate_pressconf`: шапка видео и слой реплик с таймкодами, витрина полного текста для MCP, история с 16.02.2024 (как у пресс-релизов и резюме).

## 2. Источник (проверено на живом YouTube 2026-10-23)

**Плейлист** `https://www.youtube.com/playlist?list=PLQocLHHmkYbCsWDJphENzmIqMcrcU-SxD` — «Пресс-конференции Председателя Банка России по итогам заседания Совета директоров». Первая страница отдаёт **100 позиций**, из них **24 пресс-конференции по ключевой ставке**; **23** попадают в историю контура `2024-02-16 … 2026-10-23`. Позиция `15.12.2023` (`wOVVPkgFOUY`) в плейлисте есть, но отсекается: история контура начинается с 16.02.2024 (первый пресс-релиз с квартальным блоком, первое резюме обсуждения).

Страница плейлиста содержит:

- `var ytInitialData = {...}` — JSON; элементы плейлиста лежат в `contents.twoColumnBrowseResultsRenderer.tabs[0].tabRenderer.content.sectionListRenderer.contents[0].itemSectionRenderer.contents[].lockupViewModel` и дают `contentId` (videoId) и `metadata.lockupMetadataViewModel.title.content` (заголовок);
- `"INNERTUBE_API_KEY":"..."` — публичный ключ YouTube (не секрет; берётся со страницы, в код не хардкодится);
- `"visitorData":"..."` — visitor id для player API.

Страница скачивается через `util.HttpClient` с consent-cookie `SOCS=CAI`: без неё YouTube из EU отдаёт ConsentUi (HTTP 200, ~0,6 МБ) без `ytInitialData` и `visitorData`; `util.GetPage` этого не умеет, потому что не принимает заголовки.

**Player API** — без `yt-dlp`:

```
POST https://www.youtube.com/youtubei/v1/player?key=<INNERTUBE_API_KEY>
Content-Type: application/json
X-Goog-Visitor-Id: <visitorData>
X-YouTube-Client-Name: 101
X-YouTube-Client-Version: 1.02
Origin: https://www.youtube.com
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 15_7_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15

{"context":{"client":{"clientName":"VISIONOS","clientVersion":"1.02","deviceMake":"Apple",
 "deviceModel":"RealityDevice17,1","osName":"visionOS","osVersion":"26.5.23O471",
 "hl":"ru","gl":"RU","visitorData":"..."}},"videoId":"...","contentCheckOk":true,"racyCheckOk":true}
```

Проверено на `WjLQNQPRc6o`: `playabilityStatus.status=OK`, `videoDetails.lengthSeconds=4712`, в `captions.playerCaptionsTracklistRenderer.captionTracks` — `ru` / `kind=asr` (иногда есть и `en-US/asr`). `baseUrl` дорожки + `&fmt=json3` отдаёт JSON3 (~1.1 МБ на часовую конференцию); `events[].segs[].utf8` — текст, `events[].aAppend` — служебные переводы строк.

**Почему не watch-страница и не `timedtext` напрямую.** `baseUrl` из `ytInitialData` watch-страницы отдаёт HTTP 200 с телом 0 байт; `timedtext` без player API — то же. Рабочий путь — player API с клиентом `VISIONOS` и `visitorData` со страницы плейлиста. `yt-dlp` локально работает, но в прод-импортёре не используется: внешний бинарник не гарантирован на staging, а весь HTTP воспроизводится через `util`.

## 3. Схема

Канонический DDL — [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.2; копии в `cbr/key_rate_pressconf.go` сверяются тестом-пином.

```sql
CREATE TABLE IF NOT EXISTS cbr_key_rate_pressconf (
    meeting_date Date,
    published Date,                          -- = meeting_date: эфир в день решения
    video_id String,
    url String,                              -- https://www.youtube.com/watch?v=<video_id>
    title String,
    duration_ms UInt32,
    caption_lang LowCardinality(String),     -- 'ru'
    caption_kind LowCardinality(String),     -- 'asr' | 'manual'
    cues UInt16,                             -- число записанных реплик
    loaded_at DateTime('Europe/Moscow') DEFAULT now()  -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at) ORDER BY meeting_date;

CREATE TABLE IF NOT EXISTS cbr_key_rate_pressconf_cue (
    meeting_date Date,
    seq UInt16,                              -- порядок реплики с 1
    start_ms UInt32,
    end_ms UInt32,
    text String                              -- дословная реплика ASR
) ENGINE = ReplacingMergeTree ORDER BY (meeting_date, seq);
```

Ключ `ReplacingMergeTree` — `meeting_date` (шапка) и `(meeting_date, seq)` (реплики): повторный прогон идемпотентен, порядок реплик — часть ключа. `loaded_at` — версия шапки: перезапись той же даты должна детерминированно заменять прежнюю строку.

## 4. Разбор

### 4.1. Плейлист

`pressconfPlaylistItems(page []byte) ([]pressconfItem, error)`:

- извлечь `ytInitialData` из HTML: найти маркер `ytInitialData`, первый `{` после него и декодировать **одно** JSON-значение через `json.Decoder` (не счётчик скобок — внутри JSON есть `{`/`}` в строках);
- рекурсивно обойти дерево: для каждого объекта с ключом `lockupViewModel` взять `contentId` и `metadata.lockupMetadataViewModel.title.content`; fallback — объект с `videoId` и `title.runs[].text` (старый `playlistVideoRenderer`); дубликаты videoId схлопнуть;
- заголовок фильтруется регуляркой `(?i)^пресс-конференция по ключевой ставке (\d{1,2}) ([а-яё]+) (\d{4}) года$`; дата — через `mpMonths` и `mpDay`; пробелы нормализуются `normMPText`;
- ноль совпадений — ошибка «вёрстка или заголовки изменились», а не 0 строк.

`pressconfPageConfig(page []byte) (visitor, apiKey string, err error)` — regex по `"visitorData"`/`"VISITOR_DATA"` и `"INNERTUBE_API_KEY"`.

### 4.2. Окно истории

`pressconfWanted(items, start, today)` — `meeting_date >= 2024-02-16` и `<= сегодня` (МСК = фиксированный UTC+3, без DST). Будущие даты не импортируются. Дубликаты дат схлопываются, сортировка по дате.

### 4.3. Player и дорожка

`pressconfPlayer(videoID, visitor, apiKey)` — `util.PostFileBytes` (общий `util.HttpClient`, UA, дедлайн, проверка статуса и пустого тела), разбор:

- `playabilityStatus.status != "OK"` — ошибка с `status`/`reason`; `LOGIN_REQUIRED` — отдельная ошибка (бот-проверка), импортёр может один раз обновить `visitorData` со страницы плейлиста и повторить;
- `videoDetails.title` обязан парситься в ту же дату, что и заголовок плейлиста; иначе — ошибка (плейлист перенаправил на чужое видео);
- `videoDetails.lengthSeconds` — `duration_ms` (проверка `strconv.ParseUint`);
- дорожка: `ru` + `kind=""` (manual) приоритетнее `ru` + `kind="asr"`; допускается `ru-RU`; нет `ru` — sentinel `errPressconfNoCaptions`.

### 4.4. Субтитры

`util.GetJSON(baseUrl + "&fmt=json3", &doc)` (проверка статуса и пустого тела). Разбор `events`:

- `aAppend != nil` — пропустить;
- `text = strings.Join(strings.Fields(strings.ReplaceAll(join(segs.utf8), "\u00a0", " ")), " ")`; пустой — пропустить;
- `start_ms = max(0, tStartMs)`, `end_ms = start_ms + max(0, dDurationMs)`; если длительность 0 — `end_ms = start_ms`;
- `seq` с 1, порядок — по `tStartMs` (стабильная сортировка);
- ноль реплик — ошибка: «субтитры пусты/формат изменился».

Текст — **автосубтитры ASR**, не официальная стенограмма: ошибки распознавания, повторы и `>>` смены говорящего — свойство источника, а не дефект импортёра. Это прямо написано в комментариях витрин.

## 5. Импорт и идемпотентность

Свой `Import()` (у импортёра две таблицы; шаблон `util.ClickHouseImport` ведёт одну — прецедент `cbr_key_rate_press`):

1. DDL обеих таблиц.
2. Витрины `util.CreateView` (до сети; на no-op самолечение комментариев).
3. `util.GetPage(pressconfPlaylistURL)` → `ytInitialData`, `visitorData`, `INNERTUBE_API_KEY`.
4. Разбор и окно истории (§4.1–4.2).
5. Гейт: `SELECT meeting_date, cues FROM cbr_key_rate_pressconf FINAL` + `SELECT meeting_date, count() FROM cbr_key_rate_pressconf_cue FINAL GROUP BY meeting_date`. Перекачиваются неизвестные даты, шапки с `cues=0` и даты, где `count() < cues` (самолечение сбоя батча реплик; хвостовые лишние строки не считаются дырой).
6. По каждой недостающей дате — player + субтитры (§4.3–4.4). Сбой одного видео не отменяет остальные: ошибки копятся `errors.Join`, прогон завершается ошибкой — dagu видит красный шаг.
7. Один батч шапок и один батч реплик на прогон: `PrepareBatch` → `Append` с проверкой → `Send`; на ошибке — `Abort`.
8. Нет субтитров у сегодняшней конференции — warn и пропуск до следующего прогона (ASR генерируется с задержкой); для прошлой даты то же состояние — ошибка.
9. Если новых нет — `Imported 0 rows` и лог с числом конференций/реплик; повторный прогон качает только страницу плейлиста.

Регистрация — `cbr/importers.go`, запись `{cbrKeyRatePressconfImporter, "cbr_key_rate_pressconf", ""}`: префлайта нет, известность — по `meeting_date` (артефакт — набор видео, а не файл; прецедент `cbr_key_rate_press`). `Name() == TableName`.

## 6. Производные

Витрины (комментарии таблиц и колонок обязательны):

- `v_cbr_key_rate_pressconf` — шапки: `meeting_date`, `published`, `video_id`, `url`, `title`, `duration_ms`, `caption_lang`, `caption_kind`, `cues`;
- `v_cbr_key_rate_pressconf_cues` — реплики: `meeting_date`, `seq`, `start_ms`, `end_ms`, `text`;
- `v_cbr_key_rate_pressconf_text` — одна строка на заседание: шапка + `text` — конкатенация реплик в порядке `seq`:

```sql
SELECT h.*, arrayStringConcat(arrayMap(x -> x.2, arraySort(c.cues)), ' ') AS text
FROM (SELECT * FROM cbr_key_rate_pressconf FINAL) AS h
LEFT JOIN (
    SELECT meeting_date, groupArray((seq, text)) AS cues
    FROM cbr_key_rate_pressconf_cue FINAL
    GROUP BY meeting_date
) AS c ON h.meeting_date = c.meeting_date
```

Каталог `series_catalog` **не заводится**: числовых рядов у текстового контура нет; понимание источника дают комментарии витрин (прецедент `cbr_mp_summary`, ARCHITECTURE §6.5). Гранты — `sql/mcp_kimi_reader.sql` на три витрины; сырые таблицы агенту не выдаются. Проверки — `scripts/mcp_check.py`: три витрины непусты (у текстовой — ещё `min(length(text)) > 0`), две сырые таблицы закрыты (`ACCESS_DENIED`/497).

## 7. Тесты (офлайн, реальные фикстуры)

- `cbr/testdata/key_rate_pressconf/playlist.html` — урезанная реальная страница: `ytcfg` с плейсхолдерами `visitorData`/`INNERTUBE_API_KEY`, `ytInitialData` с реальными `lockupViewModel` (включая 2023-12-15, 2024-02-16, 2026-09-11, 2026-10-23).
- `player.json` — реальный player response без подписанных URL: `playabilityStatus`, `videoDetails`, дорожки `en-US/asr` и `ru/asr`.
- `captions.json3` — урезанный реальный JSON3 (первые ~100 событий + последние), с `aAppend` и пустыми событиями.
- `pressconf_parse_test.go`:
  - плейлист: 100 позиций, 24 ключевые, 23 после `2024-02-16`; первая `2024-02-16` (`-BlT1bt9kC4`), последняя `2026-10-23` (`WjLQNQPRc6o`); 2023-12-15 отсечена;
  - заголовок: все даты, битый заголовок — ошибка;
  - player: выбран `ru/asr`, `lengthSeconds` разобран, чужой заголовок — ошибка;
  - cues: число, первый/последний текст, `aAppend` пропущен, таймкоды не убывают;
  - `TestCbrKeyRatePressconfDdlPins`: DDL-литералы называют свои таблицы, `Name() == pressconfTable`.
- `cbr/importers_test.go` — новая запись подхватывается автоматически (префикс `cbr_`, уникальность, шаг dagu).

## 8. Расписание и приёмка

Шаг `cbr_key_rate_pressconf` — в хвосте `dagu/cbr.yaml` (ежедневно 09:17): падение нового шага не лишает данных проверенные шаги контура. В день заседания пресс-конференция начинается в 15:00 МСК; импорт после неё — ручным `make import STAT=cbr_key_rate_pressconf` (публикационный контур), на следующее утро — штатным шагом.

Приёмка: `make import STAT=cbr_key_rate_pressconf` → **23 шапки**, `2024-02-16 … 2026-10-23`, все реплики (число — в протоколе; ориентир ~41 тыс.); повтор — `Imported 0 rows` без скачивания player/субтитров; `v_cbr_key_rate_pressconf` — 23, `v_..._cues` — > 0, `v_..._text` — 23 непустые строки; `make all`, `make docs-check`, `dagu validate dagu/cbr.yaml`, `make mcp-check` — зелёные; протокол — комментарием в PR.

## 9. Ограничения

- Текст — **автосубтитры ASR**, не официальная стенограмма; ошибки распознавания не исправляются.
- Зависимость от вёрстки YouTube: `ytInitialData`/`lockupViewModel` и клиент `VISIONOS` могут измениться. Импортёр падает громко (ноль позиций, `LOGIN_REQUIRED`, нет `ru`-дорожки), а не пишет 0 строк молча.
- Плейлист отдаёт первые 100 позиций; сейчас это 24 конференции по ключевой ставке, из них 23 в истории. Если плейлист вырастет и старая конференция выпадет из первой страницы, для чистой установки понадобится backfill/пагинация — отдельный follow-up; на уже наполненной таблице гейт по дате этого не замечает.
- Полный архив видео/субтитров не ведётся (`source_publications`/винтажный архив не заводятся): это набор внешних видео, а не файл версии.
- Если видео перезалито с меньшим числом реплик, хвостовые старые строки `seq` могут остаться; лечение — удалить строки заседания и повторить прогон (как у `cbr_key_rate_press`).
- `published` — дата заседания, а не точный timestamp загрузки YouTube.
