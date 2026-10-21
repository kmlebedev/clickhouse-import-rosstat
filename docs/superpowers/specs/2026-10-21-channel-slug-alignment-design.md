# Унификация слагов канала ключевой ставки

**Пункт:** `key-rate/phase-4-channel-slug-alignment`, issue [#329](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/329).
**Дата:** 21 октября 2026 г.
**Роль:** `role:go`.
**Зависимости:** #327 (карта каналов, PR [#330](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/330)) — закрыт.

---

## 1. Проблема

Одни и те же 11 каналов решения ЦБ закодированы в двух реестрах Go разными слагами:

| Канал §3 `KEY_RATE_CHANNELS.md` | Канон (`cbr/mp_summary_channels.go`) | Теги (`news/tags.go`) |
|---|---|---|
| Кредитование и трансмиссия ДКП | `lending_transmission` | `credit_transmission` |
| Внешние условия и курс | `external_fx` | `external_conditions` |
| Сберегательное поведение | `savings_behavior` | `savings` |
| Автономные (немонетарные) факторы | `autonomous_factors` | `macroprudential` |

Канон — реестр разметки резюме: он привязан к дословным формулировкам ЦБ, и на нём уже лежат 6 380 строк `cbr_mp_summary_channel` (`regex` и `llm`, `prompt_version` 0/1). Теги новостей — производный слой: реестр `newsTags` задаёт промпт LLM и валидацию `news_annotation`, а старые строки `prompt_version=1/2` уже содержат legacy-слаги.

## 2. Решение

**Один канонический слаг в обоих реестрах + чтение истории через алиас-маппинг.**

1. `news/tags.go`: 4 слага переименовываются в канон. `newsPromptVersion` повышается 2 → 3, поэтому переразметка пишет новые строки рядом со старыми (ключ `ReplacingMergeTree` содержит `prompt_version`).
2. Legacy-строки `news_annotation` (`prompt_version ≤ 2`) **не переписываются**. Витрины `v_news_events` и `v_news_flash` нормализуют `cb_channel`-теги на чтении: `credit_transmission → lending_transmission`, `external_conditions → external_fx`, `savings → savings_behavior`, `macroprudential → autonomous_factors`. Агент всегда видит 11 канонических слагов.
3. Алиасы живут в отдельном реестре `newsTagAliases` и в `newsTags`/промпт не попадают: модель не должна возвращать legacy-слаги.
4. `cbr/mp_summary_channels.go` не меняется. Живая проверка — счётчики `v_cbr_mp_summary_channels` до и после совпадают, legacy-слагов в витрине нет.

### Почему не миграция

`ALTER TABLE … UPDATE` по `news_annotation` переписал бы историю разметки, потребовал бы ad-hoc DML в обход импортёра (правило 11 AGENTS.md) и был бы необратим. Требование issue — «старые строки не затираются» — выполняется именно алиас-маппингом: сырые строки остаются как есть, нормализация происходит на границе чтения.

### Вне объёма

- Имена групп признаков веера v3 (`credit`, `external`, `savings`, `ccyb` в `scripts/models/key_rate/`) — это не слаги, хранимые в БД, а ключи Python-словарей ML-трека; критерий #329 ограничен двумя Go-реестрами.
- Панели Grafana — пункт `key-rate/phase-4-dashboard-channel-panels` (#328).

## 3. Изменения

| Файл | Что меняется |
|---|---|
| `news/tags.go` | 4 слага → канон; `newsPromptVersion = 3`; реестр `newsTagAliases`; генератор SQL `newsTagCanonicalSQL` |
| `news/schema.go` | `v_news_events` и `v_news_flash`: канонизация `cb_channel`-тегов в агрегате; комментарии колонок `tags` |
| `news/tags_test.go` | пины канона, негативные пины legacy, тесты алиасов и полного списка 11 |
| `news/schema_test.go` | проверка сгенерированного `multiIf` в обеих витринах; `v_news_tags` без legacy |
| `news/annotation_test.go` | `savings` → `savings_behavior` в кейсах направления |
| `news/annotation_llm_test.go` | метка редакции промпта 2 → 3 |
| `docs/KEY_RATE_CHANNELS.md` | §3.1/§8/§9: канон во всех реестрах, legacy — только история |
| `docs/IMPORTERS.md`, `docs/CONFIGURATION.md`, `ARCHITECTURE.md` | `newsPromptVersion`/`prompt_version` 2 → 3; описание нормализации |
| `docs/ROADMAP_KEY_RATE.md` | статус пункта |

## 4. Поведение чтения

- `v_news_tags` — статический реестр: 46 тегов, 11 `cb_channel`, только канонические слаги.
- `v_news_flash`/`v_news_events` — теги канонические для всех редакций промпта; `prompt_version` и `model` различают строки разметки, поэтому старые и новые строки видны раздельно.
- `v_cbr_mp_summary_channels` — без изменений: 11 слагов, `method`/`prompt_version` как были.

## 5. Проверка

- `make all`, `make docs-check`, `make mcp-check`.
- `make import STAT=news_watch`: публикация витрин + переразметка `prompt_version=3` (при наличии LLM env).
- MCP: `v_news_flash`/`v_news_events` — только канонические `cb_channel`; `v_news_flash` содержит обе редакции; `v_cbr_mp_summary_channels` — 11 каналов, счётчики как до PR.

## 6. Риски

- **SQL-совместимость.** `multiIf` внутри `groupArrayIf` проверяется только живым ClickHouse при публикации витрин; ошибка уронит импорт, а не пройдёт молча.
- **Нет LLM env.** Переразметка не выполнится, импорт останется зелёным; чтение истории всё равно канонично. В протоколе — «не проверено», догон следующим прогоном.
- **Коллизия алиаса с чужим измерением.** Тест запрещает: legacy-слаг не должен встречаться в реестре ни в одном измерении.
