# План: импортёр `cbr_key_rate_pressconf`

**Пункт:** `key-rate/phase-4-cbr-key-rate-pressconf`, issue [#370](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/370).
**Спека:** [2026-10-23-cbr-key-rate-pressconf-design.md](../specs/2026-10-23-cbr-key-rate-pressconf-design.md).
**Ветка:** `feat/cbr-key-rate-pressconf`; worktree `.worktrees/cbr-key-rate-pressconf`.

## Задачи

### 1. Разбор (`cbr/key_rate_pressconf_parse.go`)

- [ ] Константы `pressconfPlaylistURL`, `pressconfTitleRe`, `pressconfHistoryStart`, клиент VISIONOS (`clientName`, `clientVersion`, id, UA), `errPressconfNoCaptions`.
- [ ] `pressconfPageConfig(page)` — `visitorData`/`VISITOR_DATA` и `INNERTUBE_API_KEY` регулярками; ошибка, если чего-то нет.
- [ ] `pressconfPlaylistItems(page)` — `ytInitialData` через `json.Decoder`; рекурсивный обход `lockupViewModel` + fallback `playlistVideoRenderer`; дедуп по videoId; ноль элементов — ошибка.
- [ ] `pressconfDate(title)` — регулярка заголовка, `mpMonths`/`mpDay`, `normMPText`.
- [ ] `pressconfWanted(items, start, today)` и `pressconfMissing(wanted, headers, cueCounts)`.
- [ ] `pressconfPlayer(videoID, visitor, apiKey)` — `util.PostFileBytes`, проверка `playabilityStatus`, `videoDetails`, `captions`; сверка даты из `videoDetails.title`.
- [ ] `pressconfPickTrack(tracks)` — `ru` manual > `ru/asr`; `errPressconfNoCaptions`.
- [ ] `pressconfJSON3URL(base)` — `&fmt=json3`.
- [ ] `pressconfParseCues(doc)` — пропуск `aAppend`/пустых, нормализация, `seq`, таймкоды.

### 2. Импортёр (`cbr/key_rate_pressconf.go`)

- [ ] DDL двух таблиц, INSERT-константы, запросы гейта.
- [ ] `pressconfStat` с `Name()`, свой `Import()`:
  - DDL → витрины → плейлист → разбор → окно истории → гейт → player/субтитры → батчи шапок и реплик;
  - `pressconfBatch` (копия `pressBatch` с именем своего импортёра);
  - `pressconfMoscow`, `pressconfToday`, `errors.Join`, warn для сегодняшней конференции без субтитров.
- [ ] Витрины `v_cbr_key_rate_pressconf`, `..._cues`, `..._text` с комментариями таблиц и колонок.
- [ ] `cbrKeyRatePressconfImporter` — `TableName: pressconfTable`.

### 3. Регистрация и расписание

- [ ] `cbr/importers.go` — `{cbrKeyRatePressconfImporter, "cbr_key_rate_pressconf", ""}` в хвосте, комментарий про гейт по `meeting_date`.
- [ ] `dagu/cbr.yaml` — шаг `cbr_key_rate_pressconf` последним, комментарий про 09:17 / 15:00 МСК / ручной импорт.

### 4. Тесты и фикстуры

- [ ] `cbr/testdata/key_rate_pressconf/playlist.html` — реальный `ytInitialData` с 100 `lockupViewModel`, `ytcfg` с плейсхолдерами `visitorData`/`INNERTUBE_API_KEY`.
- [ ] `player.json` — `playabilityStatus`, `videoDetails` (title, lengthSeconds), `captions` с `en-US/asr` и `ru/asr`, без подписанных URL.
- [ ] `captions.json3` — реальные первые ~100 событий + последние, с `aAppend` и пустыми событиями.
- [ ] `pressconf_parse_test.go`:
  - 100 позиций, 24 ключевые, 23 после 2024-02-16; первая/последняя даты; `WjLQNQPRc6o` = 2026-10-23; 2023-12-15 отсечена;
  - даты заголовков, битый заголовок — ошибка;
  - player: `ru/asr`, `lengthSeconds`, чужой заголовок — ошибка;
  - cues: число, первый/последний текст, `aAppend` пропущен, таймкоды не убывают;
  - DDL-пин `TestCbrKeyRatePressconfDdlPins`.
- [ ] `cbr/importers_test.go` — новая запись проходит автоматически.

### 5. Гранты и MCP

- [ ] `sql/mcp_kimi_reader.sql` — `GRANT SELECT` на три витрины.
- [ ] `scripts/mcp_check.py` — после блока `cbr_key_rate_press`: три витрины непусты (у текстовой `min(length(text)) > 0`), две сырые таблицы закрыты.

### 6. Документация

- [ ] `ARCHITECTURE.md`: шапка «Последнее обновление», §2/§6.1, §6.2 (DDL), §6.3 (витрины), §6.5 (гранты/проверка), §7 (29-й шаг), §8 п.13 (гейт по `meeting_date`).
- [ ] `README.md`: cbr-строка, 29 шагов, ограничения.
- [ ] `docs/IMPORTERS.md`: cbr-строка.
- [ ] `docs/KEY_RATE_PUBLICATIONS.md`: постзаседание и §6 контракта.
- [ ] `docs/ROADMAP_KEY_RATE.md`: статус и протокол.

## Приёмка

```bash
make import STAT=cbr_key_rate_pressconf
make import STAT=cbr_key_rate_pressconf   # Imported 0 rows
make all
make docs-check
dagu validate dagu/cbr.yaml
make mcp-check
```

SQL:

```sql
SELECT count(), min(meeting_date), max(meeting_date) FROM v_cbr_key_rate_pressconf;
SELECT count() FROM v_cbr_key_rate_pressconf_cues;
SELECT meeting_date, length(text) FROM v_cbr_key_rate_pressconf_text ORDER BY meeting_date;
```

Ожидание: 23 шапки, `2024-02-16 … 2026-10-23`, реплики всех 23 конференций, `v_..._text` — 23 непустые строки; повтор — `Imported 0 rows`; все проверки зелёные. Протокол — комментарием в PR.

## Протокол живой приёмки

Заполняется после прогона: дата, команда, число шапок/реплик, диапазон дат, повтор, результаты `make all`/`make docs-check`/`dagu validate`/`make mcp-check`, спот-проверка текста 2026-10-23.
