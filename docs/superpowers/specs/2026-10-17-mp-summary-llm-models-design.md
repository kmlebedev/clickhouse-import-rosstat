# Сравнение LLM-моделей разметки каналов `cbr_mp_summary` (дизайн)

> Пункт **1.8** роадмапа [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §6 (фаза 1). Продолжение пункта 1.2 — контура резюме обсуждения ключевой ставки: спека [2026-10-14-cbr-mp-summary-design.md](2026-10-14-cbr-mp-summary-design.md) §3.5 предусматривает ансамбль моделей «добавлением модели в конфиг», §6 относит его на «позже». План реализации — [2026-10-17-mp-summary-llm-models.md](../plans/2026-10-17-mp-summary-llm-models.md).

## 1. Цель

Дать LLM-слою разметки каналов `cbr_mp_summary` штатную поддержку нескольких моделей (профилей провайдеров) и на тех же 22 документах ЦБ сравнить три модели по скорости, качеству и цене:

- **база** — `deepseek-chat` (DeepSeek API, `https://api.deepseek.com/anthropic`), уже лежит в ClickHouse: 1089 строк, `prompt_version=1`, 22 документа;
- **GLM** — `zai-org/GLM-5.3-Flash` через провайдер `joingonka` (алиас Kimi Code `joingonka/zai-org/GLM-5.3-Flash`);
- **MiMo** — `mimo-v2.6-flash` через провайдер `xiaomimimo` (алиас Kimi Code `xiaomimimo/flash`).

`deepseek/deepseek-flash` из пула Kimi Code — **другая** модель (провайдер `deepseek`, модель `deepseek-flash`), её разметки в ClickHouse нет; в этом пункте она не сравнивается. В отчёте различие фиксируется явно.

Результат пункта: разметка GLM и MiMo рядом с базой в `cbr_mp_summary_channel`, отчёт `docs/research/2026-10-17-mp-summary-llm-comparison.md` с числами и рекомендацией, поведение импортёра по умолчанию остаётся одномодельным.

## 2. Исходное состояние (проверено 2026-10-17)

- `v_cbr_mp_summary_channels`, `method='llm'`: `model='deepseek-chat'`, 22 документа, 1089 строк (`note` 242, `prose` 550, `risk_dez` 53, `risk_pro` 244), `prompt_version=1`.
- Импортёр читает одну модель из `LLM_BASE_URL`/`LLM_API_KEY`/`MP_SUMMARY_LLM_MODEL` (`cbr/mp_summary_llm.go`, `mpLLMConfigFromEnv`); схема `cbr_mp_summary_channel` уже включает `model` и `prompt_version` в ключ `ReplacingMergeTree`, поэтому разметка новых моделей ложится рядом без миграции.
- Провайдеры `joingonka` (`https://gate.joingonka.ai`) и `xiaomimimo` (`https://api.xiaomimimo.com/anthropic`) в конфиге Kimi Code имеют тип `anthropic`, то есть совместимы с продовым SDK `anthropic-sdk-go` (`POST <base>/v1/messages`). Их ключи лежат в `~/.kimi-code/config.toml`, в `~/.config/rosstat/env` их нет.
- Промпт (`mpLLMSystemPrompt`, редакция 1), валидация цитат (дословное вхождение в нормализованный корпус), `temperature=0`, `max_tokens=8192`, дедлайн 65 с — общие для всех моделей; менять их в этом пункте нельзя, иначе сравнение потеряет смысл.

## 3. Решения

### 3.1 Профили моделей (конфигурация)

Новый env-контракт, без миграции схемы:

| Переменная | Смысл |
|---|---|
| `MP_SUMMARY_LLM_MODELS` | Список профилей через запятую; пусто → `default` (полная обратная совместимость). Пример: `default,glm,mimo`. |
| `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL` | Профиль `default` (текущий DeepSeek; имена не меняются). |
| `<PROFILE>_LLM_BASE_URL`, `<PROFILE>_LLM_API_KEY`, `<PROFILE>_LLM_MODEL` | Дополнительные профили; имя профиля — `[a-z0-9_]+`, переменные — `<PROFILE>` в верхнем регистре (`glm` → `GLM_LLM_*`, `mimo` → `MIMO_LLM_*`). |

Правила:

1. Профиль с любой пустой переменной — `log.Warnf` с **именами** недостающих переменных (не значениями) и пропуск; остальные профили работают.
2. Ни одного полного профиля → `nil` → прежняя деградация до regex-слоя (warn, документ остаётся со слоем 1).
3. Дубликат `model` между профилями — warn, остаётся первый: иначе строки с одинаковым ключом `(meeting_date, channel, kind, method, model, prompt_version, seq)` перетирали бы друг друга.
4. `model` в данных — фактическое имя модели API (`deepseek-chat`, `zai-org/GLM-5.3-Flash`, `mimo-v2.6-flash`); имя профиля в данные не пишется.
5. `MP_SUMMARY_LLM_MODELS` задаётся **командной строкой make** только на прогон эксперимента; в `~/.config/rosstat/env` не добавляется, чтобы ежедневный `dagu/cbr.yaml` остался одномодельным. Пустое значение — default.
6. Ключи новых провайдеров владелец добавляет в `~/.config/rosstat/env` сам (`GLM_LLM_*`, `MIMO_LLM_*`); в репозиторий, логи и тексты ошибок они не попадают (значения — только `os.Getenv`).

Замена `mpLLMConfigFromEnv` на `mpLLMProfilesFromEnv() []mpLLMProfile`; старая функция и её тест переписываются.

### 3.2 Инструментация (скорость/токены/качество)

- `mpLLMUsage{InputTokens, OutputTokens, CacheReadInputTokens, CacheCreationInputTokens int64}` — из `msg.Usage` SDK `anthropic-sdk-go` v1.80.0.
- `mpLLMDocStats{Model, Attempts, Duration, Usage, QuotesTotal, QuotesKept, QuotesDropped, Notes, ChannelsWithQuotes}`.
- `mpLLMAskOnce` возвращает `(payload, usage, error)`; `mpLLMAsk` — `(payload, usage, attempts, error)`; `tagMPChannelsLLM` — `(matches, stats, error)`; `mpTagLLM` — `(stats, error)` и **не глотает** ошибку LLM: warn и продолжение — у вызывающего импортёра, harness записывает ошибку в stats.
- Счётчики качества: все цитаты ответа = `QuotesKept + QuotesDropped` (пустой текст, нет в корпусе, неизвестный slug — dropped); `ChannelsWithQuotes` — уникальные slug'и с сохранённой цитатой; `Notes` — строки `kind='note'`.
- Импортёр логирует итог по профилю (`LLM <model>: N документов, M строк, K отброшено, T с, in/out ...`) на Info; по документу — Debug. Секреты в логи не попадают.

### 3.3 Импортёр

- `pending` считается по каждому профилю: `mpLLMTagged(model, promptVersion)` × профили; no-op — если нет новых документов и ни у одного профиля нет отстающих.
- Задачи `(doc, profile)` собираются чистой функцией; страница отстающего документа скачивается **один раз** на прогон, затем размечается теми профилями, которым нужна.
- Один батч каналов на прогон (как сейчас); строки всех моделей в одной таблице, `method='llm'`, `model=profile.Model`, `prompt_version=1`.
- Ошибка LLM одного документа/профиля — warn и продолжение (деградация до regex-слоя для этой пары); ошибка БД — наверх.
- Поведение по умолчанию (без `MP_SUMMARY_LLM_MODELS`) не меняется: один `default`-профиль, при отсутствии env — warn и regex.

### 3.4 Harness сравнения

Отдельный build-tagged тест в пакете `cbr`: `cbr/mp_summary_llm_eval_test.go`, `//go:build mpeval`, `TestMPLLMEvalLive`. Не входит в `make all`; запускается целью `make mp-llm-eval` (`go test -tags mpeval ./cbr -run TestMPLLMEvalLive -v -count=1 -timeout 3h`).

Env harness:

| Переменная | Смысл |
|---|---|
| `MP_LLM_EVAL=1` | Обязательный флаг запуска; иначе `t.Skip`. |
| `MP_SUMMARY_LLM_MODELS` | Профили прогона (см. §3.1). |
| `MP_LLM_EVAL_DOCS` | Необязательный список `YYYY-MM-DD` (smoke). |
| `MP_LLM_EVAL_WRITE=0\|1` | Писать ли строки в ClickHouse; по умолчанию 1; `0` — baseline dry-run. |
| `MP_LLM_EVAL_FORCE=0\|1` | Игнорировать уже размеченные пары (нужно для baseline). |
| `MP_LLM_EVAL_RESUME=0\|1` | Пропускать успешно завершённые пары из `stats.jsonl`; пары с ошибкой повторяются. По умолчанию 1. |
| `MP_LLM_EVAL_OUT` | Каталог артефактов; Makefile передаёт абсолютный `$(CURDIR)/build/mp-llm-eval`. |
| `MP_LLM_EVAL_TIMEOUT` | Дедлайн одного вызова; по умолчанию `180s`; подменяет `mpLLMTimeout` только в harness. |
| `MP_LLM_EVAL_MAX_TOKENS` | `max_tokens`; по умолчанию 8192; подменяет `mpLLMMaxTokens` только в harness. |
| `MP_LLM_EVAL_DELAY` | Пауза между вызовами; по умолчанию `1s` (rate limit). |
| `MP_LLM_EVAL_TRANSPORT` | `anthropic` (продовый SDK, по умолчанию) или `openai` (OpenAI-совместимый `POST <base>/v1/chat/completions` с `response_format=json_object`). Второй нужен GLM/MiMo: на Anthropic-эндпоинте они тратят `max_tokens` на reasoning в text-блоке и не доходят до JSON. В отчёте транспорт каждой модели указан явно. |
| `MP_LLM_EVAL_REASONING` | OpenAI-совместимый `reasoning_effort` (`none\|low\|medium\|high`, пусто — поле не отправляется). GLM с `none` перестаёт писать длинный анализ в `content` и доходит до JSON; для MiMo не требуется (у него reasoning вынесен в отдельное поле и не мешает). |
| `MP_LLM_EVAL_CONCISE` | `0\|1` (по умолчанию 0): добавить в OpenAI-запрос инструкцию «не более одной цитаты на канал, note — одно короткое предложение». Нужен для повторной разметки GLM-документов, которые не влезли в лимит 8192; обычный прогон и прод-путь его не используют. |
| `MP_LLM_EVAL_PREFILL` | `0\|1` (по умолчанию 0): последнее сообщение ассистента «{» — запасная совместимость Anthropic-транспорта; в финальном прогоне не используется, если работает `openai`. |
| `MP_LLM_EVAL_THINKING` | `default\|disabled` (по умолчанию `default`): Anthropic-поле `thinking`; `disabled` — запасная совместимость, в финальном прогоне не используется. |
| `<PROFILE>_LLM_PRICE_IN`, `<PROFILE>_LLM_PRICE_OUT` | Цена за 1М токенов, USD (необязательно; без них цена в отчёте «н/д»). |

Проверка 2026-10-17: GLM на Anthropic-эндпоинте вернул 49 КБ reasoning в text-блоке с `stop_reason=max_tokens` (JSON не начат), MiMo — ответ без text-блока/обрезанный JSON; их OpenAI-эндпоинты с `response_format=json_object` отвечают валидным JSON, а GLM с `reasoning_effort=none` перестаёт писать анализ. Поэтому GLM и MiMo размечиваются через `openai` (GLM — с `reasoning_effort=none`), baseline `deepseek-chat` — через продовый `anthropic`; это единственное отличие условий, и оно фиксируется в отчёте.

Логика теста:

1. Подключение к ClickHouse по `CLICKHOUSE_URL`; `CREATE TABLE IF NOT EXISTS` для `cbr_mp_summary_channel` (литерал из `mp_summary.go`).
2. Документы — 22 шапки `SELECT meeting_date, url FROM cbr_mp_summary FINAL ORDER BY meeting_date`; страница скачивается `util.GetPage(url)` и разбирается `parseMPSummary` (тот же текст, что у продового слоя; проверка `meeting_date`).
3. Для каждой пары (профиль, документ): пропуск по `RESUME`/`WRITE`; при `WRITE=0` — прямой вызов `tagMPChannelsLLM` (без батча), при `WRITE=1` — `mpTagLLM` (та же функция, что у импортёра) с батчем на документ и `Send`; запись stats в `stats.jsonl` после каждого вызова; пауза `DELAY`.
4. Агрегаты: `summary.csv` (строка на пару), `report.md` (сырые таблицы), `notes-sample.md` (3 документа × 11 каналов × модели для ручной оценки).
5. Ошибка одного вызова — в stats, прогон продолжается; инфраструктурная ошибка (нет ClickHouse, не разобрался документ) — fail.

Артефакты — в `build/mp-llm-eval/` (в `.gitignore`), в репозиторий попадает только выверенный отчёт `docs/research/2026-10-17-mp-summary-llm-comparison.md`.

### 3.5 Метрики отчёта

**Скорость:** число вызовов, повторы (`Attempts>1`), таймауты/ошибки; суммарное время; mean/p50/p95 латентности на документ (по успешным вызовам без повтора — отдельно); input/output токены; output tokens/s.

**Качество:**

- формат: доля документов, разобранных с первой попытки;
- точность цитат: `QuotesDropped / QuotesTotal` (галлюцинации/пустые/неизвестный slug);
- покрытие: среднее `ChannelsWithQuotes` из 11, доля документов с 11/11, среднее число заметок;
- согласие с baseline `deepseek-chat`: Jaccard множеств цитат по `(meeting_date, channel)` (макро-среднее), доля общих/уникальных цитат;
- согласие с regex-слоем: recall предложений regex, найденных LLM (по каналу и документу; предложение считается найденным, если нормализованные тексты входят друг в друга), и доля LLM-цитат, не совпавших с regex;
- межмодельное согласие: Jaccard GLM↔MiMo, доля цитат, найденных ≥2 моделями;
- ручная оценка: `notes-sample.md` (3 документа: 27.02.2024, 06.11.2024, 23.09.2026) — атрибуция канала, релевантность цитат, точность заметки, краткость; оценка ведущего агента с оговоркой о субъективности.

**Цена:** токены × опубликованный тариф (за 1М input/output), дата и источник тарифа; стоимость одного документа, полного бэкфилла 22 документов и одного нового документа. Если тариф не опубликован — «н/д» + только токены.

**Вывод:** рекомендация (какая модель в прод/ансамбль, нужен ли он), с оговорками (один прогон, temperature 0, дефолты провайдера, нет человеческого gold standard, regex — слабый ориентир).

### 3.6 Отчёт

`docs/research/2026-10-17-mp-summary-llm-comparison.md`: условия прогона, модели/провайдеры, таблицы скорости, качества и цены, примеры заметок, вывод и рекомендация, оговорки, ссылки на сырые артефакты и SQL-проверки. Сырые данные — `build/mp-llm-eval/` (не коммитятся).

## 4. Приёмка

1. `make all` — зелёный.
2. `make import STAT=cbr_mp_summary MP_SUMMARY_LLM_MODELS=default,glm,mimo` — no-op (все 22 документа размечены всеми тремя моделями).
3. В `v_cbr_mp_summary_channels`: `deepseek-chat` — 22 документа, 1089 строк (не изменились); `zai-org/GLM-5.3-Flash` — 22 документа, >0 строк; `mimo-v2.6-flash` — 22 документа, >0 строк; у всех `method='llm'`, `prompt_version=1`.
4. Отчёт содержит скорость, качество, цену и рекомендацию; числа согласуются с `summary.csv` и SQL.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; витрина читается `kimi_reader`, сырые таблицы закрыты (497).
6. `go test -tags mpeval ./cbr -run TestMPLLMEvalLive -count=1` без `MP_LLM_EVAL` — SKIP, не ошибка.
7. В репозиторий не попали ключи/значения секретов; в логах и отчёте их нет.
8. Повторный прогон harness с `RESUME=1` не делает новых вызовов; повтор импортёра идемпотентен.
9. `dagu validate dagu/cbr.yaml` — exit 0 (расписание не меняется).

## 5. Вне объёма

Изменение промпта (`prompt_version` остаётся 1), миграция схемы, автоматический выбор модели в проде, ежедневный ансамбль, сравнение с `deepseek/deepseek-flash`, судейство заметок отдельной LLM-моделью (ручная оценка ведущего агента), изменение `dagu`.

## 6. Риски

| Риск | Митигация |
|---|---|
| Base URL/path провайдера не тот | Smoke на одном документе до полного прогона; при 404 — уточнить base у владельца, не гадать |
| Провайдер не принимает Anthropic-поля (`temperature`, `system`, `max_tokens`) | Smoke покажет 400; зафиксировать как несовместимость, не менять прод-транспорт в этом шаге |
| GLM упирается в 8192 и обрезает JSON | `MP_LLM_EVAL_MAX_TOKENS=16384` для повторного smoke/прогона; в отчёте — отдельная пометка |
| Таймаут 65 с режет медленную модель | В harness 180 с; в отчёте — оговорка, что продовый дефолт 65 с и его надо будет поднять при выборе модели |
| 429/rate limit | Пауза 1 с, `RESUME=1` для догона; ошибки в stats |
| Случайно включить мультимодельность в ежедневном dagu | `MP_SUMMARY_LLM_MODELS` не пишется в env-файл, только в командную строку эксперимента; пусто → default |
| Тарифы не опубликованы | Токены + «н/д»; не выдумывать цену |
| Секреты | Только `os.Getenv`; в промптах/логах/отчёте ключей нет; ключи добавляет владелец |
