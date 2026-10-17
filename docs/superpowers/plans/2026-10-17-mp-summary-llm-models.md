# Сравнение LLM-моделей разметки каналов `cbr_mp_summary` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` (or `superpowers:executing-plans` only if the owner explicitly chose inline execution). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** добавить LLM-слою `cbr_mp_summary` штатную поддержку нескольких моделей-профилей, инструментировать скорость/токены/качество и получить разметку 22 резюме ЦБ моделями `zai-org/GLM-5.3-Flash` (провайдер `joingonka`) и `mimo-v2.6-flash` (провайдер `xiaomimimo`) рядом с существующей `deepseek-chat`, с отчётом скорость/качество/цена и рекомендацией.

**Architecture:** env-профили (`MP_SUMMARY_LLM_MODELS` + `<PROFILE>_LLM_*`) → `mpLLMProfilesFromEnv` → pending по каждому профилю → задачи `(doc, profile)` при одном скачивании страницы → батч каналов с `model=profile.Model`; инструментация `mpLLMDocStats` (usage SDK, attempts, duration, счётчики цитат); отдельный build-tagged harness (`//go:build mpeval`) для живого замера и отчёта. Схема БД не меняется, `prompt_version` остаётся 1.

**Tech Stack:** Go, `anthropic-sdk-go` v1.80.0, `clickhouse-go/v2`, `goquery` (уже в проекте), logrus; ClickHouse `ReplacingMergeTree`; Makefile; без новых зависимостей.

**Spec:** `docs/superpowers/specs/2026-10-17-mp-summary-llm-models-design.md` (связывающая власть; этот план — детализация).

## Предусловие и база

- Ветка `feat/mp-summary-llm-models` от актуального `origin/main`; работа — в отдельном worktree, текущая ветка другой сессии не трогается.
- Первые коммиты ветки — спека и этот план (до кода).
- Пункт роадмапа — новый **1.8** в `docs/ROADMAP_KEY_RATE.md` §6 (фаза 1); статус «в работе» до живой приёмки.
- Живой прогон требует, чтобы владелец добавил в `~/.config/rosstat/env` ключи `GLM_LLM_*` и `MIMO_LLM_*` (см. Task 6); секреты из `~/.kimi-code/config.toml` не копируются.
- **ЗАПРЕТ до подтверждения плана:** не коммитить и не менять код; первые коммиты — спека и план.

## Global Constraints

- Секреты только `os.Getenv`; значения ключей не печатать ни в логах, ни в ошибках, ни в отчёте. В предупреждениях о неполном профиле — только **имена** переменных.
- Вставки только батчами `PrepareBatch → Append(err!) → Send`; батч принадлежит одной таблице.
- Сетевых вызовов в `init()` нет; HTTP к провайдеру — только через официальный SDK `anthropic-sdk-go` (как сейчас), страницы ЦБ — через `util.GetPage`.
- Промпт, `temperature=0`, `max_tokens=8192`, валидация цитат и `prompt_version=1` **не меняются в прод-пути**; harness может подменять `max_tokens`/дедлайн и добавлять OpenAI-режим, `reasoning_effort` и concise-инструкцию как совместимость — каждое отличие фиксируется в отчёте.
- Поведение по умолчанию без `MP_SUMMARY_LLM_MODELS` — ровно одно модельное профиль `default` (текущий прод).
- `MP_SUMMARY_LLM_MODELS` задаётся командной строкой make на прогон эксперимента; в `~/.config/rosstat/env` не добавляется.
- Каждый таск заканчивается gofmt-чистым кодом и зелёными `go test ./cbr/...` + `go build ./...`; финал — `make all`.
- Модели субагентов — явным `model:`: T1–T4 — `deepseek/deepseek-flash`; T5 (мелкие `.md`) — `joingonka/zai-org/GLM-5.3-Flash`; ревью задач — `anthropic/haiku`; финальное ревью ветки — `deepseek/deepseek-flash`. В каждый coder-промпт — блок AGENTS.md про `mcp__jetbrains__*`/`mcp__clickhouse__*`.

## Review Focus

1. **Обратная совместимость:** без `MP_SUMMARY_LLM_MODELS` импортёр ведёт себя как раньше; неполный профиль не роняет прогон; `default` читает прежние `LLM_*`.
2. **Идемпотентность:** строки `(model, prompt_version)` не перетираются между профилями; повторный прогон — no-op; `model` в данных — фактическое имя API.
3. **Секреты:** ни одного значения ключа в логах/ошибках/отчёте; предупреждения называют только переменные.
4. **Замеры:** `mpLLMDocStats` не искажает поведение (retry, деградация, ошибки); usage суммируется по попыткам; duration — полное время документа.
5. **Harness:** `WRITE=0` не пишет в БД; `RESUME` не пропускает пары с ошибкой; smoke не съедает документ полного прогона (отдельный каталог).
6. **Честность отчёта:** цена только по опубликованным тарифам с датой/источником; чего нет — «н/д»; ручная оценка помечена как субъективная.

---

### Task 1: Профили моделей — конфигурация

**Files:**
- Modify: `cbr/mp_summary_llm.go`
- Test: `cbr/mp_summary_llm_test.go`

**Interfaces:**
- Produces (используют Task 2–4):
  - `type mpLLMProfile struct { Name, BaseURL, APIKey, Model string }`
  - `func mpLLMProfilesFromEnv() []mpLLMProfile` — список профилей; пусто → `default`; неполный профиль — warn и пропуск; дубликат `Model` — warn и пропуск; все неполные → `nil`.

- [ ] **Step 1: Написать падающие тесты** (`TestMpLLMProfilesFromEnv`):
  - legacy: заданы только `LLM_BASE_URL`/`LLM_API_KEY`/`MP_SUMMARY_LLM_MODEL`, `MP_SUMMARY_LLM_MODELS` пуст → один профиль `default`;
  - явный `MP_SUMMARY_LLM_MODELS=default,glm,mimo` при полных переменных → три профиля в порядке списка, имена/значения точные;
  - неполный `glm` (нет `GLM_LLM_MODEL`) → `glm` пропущен, `default` остался; в логе (не в тесте) — имена переменных;
  - пробелы и регистр (`" GLM , MIMO "`) нормализуются; невалидное имя (`"glm-x"`) — warn и пропуск;
  - дубликат `Model` между `glm` и `mimo` → остаётся первый;
  - `MP_SUMMARY_LLM_MODELS=glm` при пустых `GLM_LLM_*` → `nil`.
  - Старый `TestMpLLMConfigFromEnv` переписать под новый API (или удалить, если все сценарии покрыты).

- [ ] **Step 2: Прогнать — FAIL**: `go test ./cbr/ -run TestMpLLMProfiles -v` → `undefined: mpLLMProfilesFromEnv`.

- [ ] **Step 3: Реализовать** `mpLLMProfile` + `mpLLMProfilesFromEnv`:
  - парсинг списка: `strings.Split`, `TrimSpace`, `ToLower`, дедуп по имени, валидация `^[a-z0-9_]+$`;
  - `default` → `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL`;
  - остальные → `strings.ToUpper(name) + "_LLM_BASE_URL"` и т.д.;
  - `TrimSpace` значений, `TrimRight(baseURL, "/")`;
  - неполный профиль: `log.Warnf` с именами пустых переменных; профиль не добавляется;
  - дубликат `Model`: warn, второй профиль не добавляется.

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run 'TestMpLLMProfiles|TestMpLLMConfig' -count=1 && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `cbr: multi-model LLM profiles for mp summary tagging`.

---

### Task 2: Инструментация usage/stats

**Files:**
- Modify: `cbr/mp_summary_llm.go`, `cbr/mp_summary_llm_test.go`
- Modify: `cbr/mp_summary.go` (сигнатура `mpTagLLM`, логи) — в части, нужной для компиляции; полная проводка — Task 3.

**Interfaces:**
- Produces (используют Task 3–4):
  - `type mpLLMUsage struct { InputTokens, OutputTokens, CacheReadInputTokens, CacheCreationInputTokens int64 }`
  - `type mpLLMDocStats struct { Model string; Attempts int; Duration time.Duration; Usage mpLLMUsage; QuotesTotal, QuotesKept, QuotesDropped, Notes, ChannelsWithQuotes int }`
  - `mpLLMAskOnce(doc, baseURL, apiKey, model) (mpLLMPayload, mpLLMUsage, error)`
  - `mpLLMAsk(...) (mpLLMPayload, mpLLMUsage, int, error)` — attempts;
  - `tagMPChannelsLLM(doc, profile) ([]mpChannelMatch, mpLLMDocStats, error)`
  - `mpTagLLM(batch, doc, profile) (mpLLMDocStats, error)` — ошибку LLM **возвращает**, не глотает.

- [ ] **Step 1: Написать падающие тесты**:
  - `TestTagMPChannelsLLMStats`: мок с `usage: {input_tokens: 100, output_tokens: 50}` → `stats.Usage` = (100, 50), `Attempts=1`, `Duration>0`, `QuotesTotal/Kept/Dropped`, `Notes`, `ChannelsWithQuotes` на валидном ответе;
  - `TestTagMPChannelsLLMStatsHallucination`: цитата-галлюцинация → `QuotesDropped` +1, `QuotesKept` не изменился;
  - `TestTagMPChannelsLLMStatsRetry`: первый ответ битый, второй валидный → `Attempts=2`, usage суммируется, duration > 0;
  - `TestTagMPChannelsLLMStats500`: 500 → `Attempts=1`, ошибка, stats с нулевым usage;
  - `TestMpTagLLMReturnsError`: ошибка LLM доходит до вызывающего (не проглатывается).

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать**:
  - SDK: `msg.Usage.InputTokens`, `OutputTokens`, `CacheReadInputTokens`, `CacheCreationInputTokens`;
  - `mpLLMAskOnce` возвращает usage и на успехе, и на ошибке формата, если конверт получен;
  - `mpLLMAsk` суммирует usage, считает попытки, логирует повтор как сейчас;
  - `tagMPChannelsLLM`: замер `time.Since(start)`, счётчики до/после валидации (неизвестный slug, пустая цитата, нет в корпусе — dropped); `ChannelsWithQuotes` — уникальные slug'и;
  - `mpTagLLM`: вернуть stats и ошибку; warn о пропуске документа перенести в вызывающего импортёра (Task 3);
  - `mp_summary.go`: временно адаптировать вызовы, чтобы пакет собирался (полная логика — Task 3).

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run 'TestTagMPChannelsLLM|TestMpTagLLM' -count=1 && go build ./... && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `cbr: LLM usage and per-document tagging stats`.

---

### Task 3: Импортёр — мультипрофильная проводка

**Files:**
- Modify: `cbr/mp_summary.go`, `cbr/mp_summary_test.go`, `cbr/mp_summary_llm_test.go` (если нужно)

**Interfaces:**
- Consumes: `mpLLMProfile`, `mpLLMProfilesFromEnv` (Task 1); `mpLLMDocStats`, `mpTagLLM` (Task 2).
- Produces: мультипрофильный `Import`; чистые функции задач для тестов.

- [ ] **Step 1: Написать падающие тесты** (`mp_summary_test.go`):
  - `TestMpLLMTasksTwoProfiles`: новый документ, оба профиля не размечены → две задачи `(doc, profile)`, один `fetch` не нужен;
  - `TestMpLLMTasksOneTagged`: документ размечен `A`, не размечен `B` → только задача `B`;
  - `TestMpLLMTasksPendingFetchedOnce`: отстающий документ нужен двум профилям → один fetch, две задачи;
  - `TestMpLLMTasksNoDuplicates`: дата и в новых, и в pending → одна задача на профиль;
  - `TestMpLLMPendingDates` (существующий) — обновить под per-profile.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать**:
  - `profiles := mpLLMProfilesFromEnv()`; для каждого — `mpLLMTagged(ctx, conn, profile.Model, mpPromptVersion)` и `mpLLMPendingDates`;
  - no-op: `len(missing)==0` и все pending пусты;
  - чистая функция `mpLLMTasks(docs, past, pendingByModel, taggedByModel)` → `ready []mpLLMTask`, `fetch map[time.Time]bool`; страница отстающего документа скачивается один раз;
  - цикл по задачам: `mpTagLLM`; ошибка — `log.Warnf` с датой/url/моделью и продолжение; ошибка батча — наверх;
  - один батч каналов на прогон; `model=profile.Model`, `prompt_version=mpPromptVersion`;
  - итоговые логи: `резюме N новых, блоков M, каналов regex X, LLM: <model> Y строк (K отброшено, T с, in/out ...)` по каждому профилю;
  - `mpTagLLM` больше не логирует ошибку сам — это делает цикл импортёра (и harness).

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -count=1 && go build ./... && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `cbr: tag mp summary channels with multiple LLM profiles`.

---

### Task 4: Harness сравнения

**Files:**
- Create: `cbr/mp_summary_llm_eval_test.go` (`//go:build mpeval`)
- Modify: `Makefile` (цель `mp-llm-eval`)
- Test: unit-тесты чистых агрегатов — в том же файле или в обычном `cbr/mp_summary_llm_eval_agg_test.go` (без тега), если нужно.

**Interfaces:**
- Consumes: всё выше; `util.GetPage`, `parseMPSummary`, `mpTagChannels`, `mpTagLLM`, `mpBatch`, DDL-литерал `mpSummaryChannelDdl`.
- Produces: `TestMPLLMEvalLive`; артефакты `stats.jsonl`, `summary.csv`, `report.md`, `notes-sample.md`.

- [ ] **Step 1: Написать тест**:
  - `MP_LLM_EVAL != "1"` → `t.Skip`;
  - `CLICKHOUSE_URL` → `clickhouse.Open` + `Ping`;
  - `CREATE TABLE IF NOT EXISTS` для канала (`mpSummaryChannelDdl`);
  - документы: `SELECT meeting_date, url FROM cbr_mp_summary FINAL ORDER BY meeting_date`; фильтр `MP_LLM_EVAL_DOCS`; `util.GetPage` + `parseMPSummary`; проверка `MeetingDate == meeting_date`;
  - пары (профиль, документ): `MP_LLM_EVAL_RESUME` (success-only) → `MP_LLM_EVAL_FORCE`/`WRITE` → вызов; `WRITE=0` — `tagMPChannelsLLM` напрямую; `WRITE=1` — `mpTagLLM` с батчем на документ + `Send`;
  - `stats.jsonl` — append после каждого вызова (JSON-строка: model, date, attempts, duration_ms, input/output/cache tokens, counters, error);
  - `DELAY` между вызовами;
  - агрегаты: mean/p50/p95, Jaccard с baseline (`deepseek-chat`) и regex, покрытие, цена по `<PROFILE>_LLM_PRICE_*`; `summary.csv` + `report.md` + `notes-sample.md` (3 даты × 11 каналов);
  - ошибки вызовов — в stats, прогон продолжается; инфраструктура — fail;
  - транспорт: `MP_LLM_EVAL_TRANSPORT=anthropic|openai`; `openai` — JSON-режим `/chat/completions` для GLM/MiMo (на Anthropic-эндпоинте они уходят в reasoning); `MP_LLM_EVAL_REASONING=none` для GLM снимает длинный анализ в `content` (MiMo не требует); `MP_LLM_EVAL_CONCISE=1` — повторная разметка GLM-документов, не влезших в 8192 (одна цитата на канал, короткая note); `MP_LLM_EVAL_REPORT_ONLY=1` — сводный отчёт по готовому `stats.jsonl` без вызовов; `MP_LLM_EVAL_PREFILL` и `MP_LLM_EVAL_THINKING` — запасные совместимости Anthropic-транспорта.

- [ ] **Step 2: Прогнать — компиляция и SKIP**: `go test -tags mpeval ./cbr -run TestMPLLMEvalLive -count=1` без `MP_LLM_EVAL` → `SKIP`.

- [ ] **Step 3: Добавить Make-цель**:
  ```make
  mp-llm-eval:
      MP_LLM_EVAL_OUT=$(or $(MP_LLM_EVAL_OUT),$(CURDIR)/build/mp-llm-eval) \
      go test -tags mpeval ./cbr -run TestMPLLMEvalLive -v -count=1 -timeout 3h
  ```

- [ ] **Step 4: Прогнать — PASS**: `go test -tags mpeval ./cbr -run TestMPLLMEvalLive -count=1` (SKIP), `gofmt -l cbr/`, `go vet -tags mpeval ./cbr/`.

- [ ] **Step 5: Commit**: `cbr: live LLM model comparison harness for mp summary`.

---

### Task 5: Документация

**Files:**
- Modify: `ARCHITECTURE.md` (§5 — новые переменные; §6.1/§6.3 — мультимодельность; §11 п. 14 — профили), `README.md` (строка `cbr_mp_summary`), `docs/KEY_RATE_CHANNELS.md` (§2 — ансамбль и отчёт), `docs/ROADMAP_KEY_RATE.md` (новый пункт 1.8, статус «в работе»).

- [ ] **Step 1: `ARCHITECTURE.md`** — заменить/дополнить строку `LLM_BASE_URL, LLM_API_KEY, MP_SUMMARY_LLM_MODEL`: добавить `MP_SUMMARY_LLM_MODELS` и `<PROFILE>_LLM_*`, правила пропуска/дедупа, дефолт `default`; §6.1 — «LLM-слой поддерживает несколько профилей; `model` в данных — фактическое имя API»; §6.3 — витрина уже говорит «все размеченные модели», добавить ссылку на отчёт; §11 п. 14 — профили и запрет ключей в логах.
- [ ] **Step 2: `README.md`** — строка `cbr_mp_summary`: «LLM-разметка каналов, профили моделей (`MP_SUMMARY_LLM_MODELS`)».
- [ ] **Step 3: `docs/KEY_RATE_CHANNELS.md` §2** — «ансамбль включается профилями; сравнение моделей — `docs/research/2026-10-17-mp-summary-llm-comparison.md`».
- [ ] **Step 4: `docs/ROADMAP_KEY_RATE.md` §6** — новый пункт 1.8 с критерием приёмки и ссылками на спеку/план; статус «в работе» до Task 6.
- [ ] **Step 5: Проверки и коммит**: `grep -n "MP_SUMMARY_LLM_MODELS\|GLM_LLM\|MIMO_LLM" ARCHITECTURE.md README.md`; `go build ./...`; коммит `docs: multi-model LLM tagging for cbr_mp_summary (key-rate 1.8)`.

---

### Task 6: Живой прогон, отчёт и приёмка

**Files:**
- Create: `docs/research/2026-10-17-mp-summary-llm-comparison.md`
- Modify: `docs/ROADMAP_KEY_RATE.md` (статус 1.8 — ✅)

**Предусловие:** владелец добавил в `~/.config/rosstat/env`:
```
GLM_LLM_BASE_URL=https://gate.joingonka.ai
GLM_LLM_API_KEY=...
GLM_LLM_MODEL=zai-org/GLM-5.3-Flash
MIMO_LLM_BASE_URL=https://api.xiaomimimo.com/anthropic
MIMO_LLM_API_KEY=...
MIMO_LLM_MODEL=mimo-v2.6-flash
```
Проверка — имена переменных, без значений; `make env-check` (или точечно).

- [ ] **Step 1: `make all`** — зелёный.
- [ ] **Step 2: Smoke** (2 вызова, отдельные каталоги):
  - GLM: `make mp-llm-eval MP_LLM_EVAL=1 MP_SUMMARY_LLM_MODELS=glm MP_LLM_EVAL_DOCS=2026-09-11 MP_LLM_EVAL_FORCE=1 MP_LLM_EVAL_WRITE=0 MP_LLM_EVAL_TRANSPORT=openai MP_LLM_EVAL_REASONING=none MP_LLM_EVAL_MAX_TOKENS=8192 MP_LLM_EVAL_TIMEOUT=600s MP_LLM_EVAL_OUT=$(pwd)/build/mp-llm-eval-smoke-glm`
  - MiMo: то же с `MP_SUMMARY_LLM_MODELS=mimo`, `MP_LLM_EVAL_MAX_TOKENS=16384`, без `MP_LLM_EVAL_REASONING`.
  - Anthropic-эндпоинт GLM/MiMo: reasoning в text-блоке, `stop_reason=max_tokens`, JSON не начинается; OpenAI-режим с `response_format=json_object` — рабочий путь, GLM дополнительно требует `reasoning_effort=none` (проверено 2026-10-17). 404/400 → зафиксировать base/path; обрезка JSON → увеличить `MP_LLM_EVAL_MAX_TOKENS` (MiMo до 32768; у GLM предел модели 8192).
- [ ] **Step 3: Baseline dry-run** (22 вызова, без записи, продовый Anthropic-транспорт): `make mp-llm-eval MP_LLM_EVAL=1 MP_SUMMARY_LLM_MODELS=default MP_LLM_EVAL_FORCE=1 MP_LLM_EVAL_WRITE=0 MP_LLM_EVAL_OUT=$(pwd)/build/mp-llm-eval`.
- [ ] **Step 4: Новые модели** (44 вызова, с записью, OpenAI JSON-режим; общий каталог `build/mp-llm-eval`, `RESUME=1`):
  - GLM: `make mp-llm-eval MP_LLM_EVAL=1 MP_SUMMARY_LLM_MODELS=glm MP_LLM_EVAL_WRITE=1 MP_LLM_EVAL_RESUME=1 MP_LLM_EVAL_TRANSPORT=openai MP_LLM_EVAL_REASONING=none MP_LLM_EVAL_MAX_TOKENS=8192 MP_LLM_EVAL_TIMEOUT=600s MP_LLM_EVAL_OUT=$(pwd)/build/mp-llm-eval GLM_LLM_PRICE_IN=0.0076 GLM_LLM_PRICE_OUT=0.023 DEFAULT_LLM_PRICE_IN=0.15 DEFAULT_LLM_PRICE_OUT=0.60`
  - MiMo: `make mp-llm-eval MP_LLM_EVAL=1 MP_SUMMARY_LLM_MODELS=mimo MP_LLM_EVAL_WRITE=1 MP_LLM_EVAL_RESUME=1 MP_LLM_EVAL_TRANSPORT=openai MP_LLM_EVAL_MAX_TOKENS=16384 MP_LLM_EVAL_TIMEOUT=600s MP_LLM_EVAL_OUT=$(pwd)/build/mp-llm-eval MIMO_LLM_PRICE_IN=0.14 MIMO_LLM_PRICE_OUT=0.28 DEFAULT_LLM_PRICE_IN=0.15 DEFAULT_LLM_PRICE_OUT=0.60`.
  - После первого прохода: пары с `error != ""` повторить (тот же каталог, `RESUME=1` пропускает успешные) — GLM с `MP_LLM_EVAL_CONCISE=1`, MiMo с `MP_LLM_EVAL_MAX_TOKENS=32768`; это добирает документы, не влезшие в лимит.
- [ ] **Step 5: Импортёр no-op**: `make import STAT=cbr_mp_summary MP_SUMMARY_LLM_MODELS=default,glm,mimo` → «no-op, все 22 резюме на месте».
- [ ] **Step 6: Тарифы** — DeepSeek `deepseek-flash` (off-peak $0.15 input / $0.60 output за 1M; `deepseek-chat` в прайсе не указан — legacy-алиас, оценка по Flash), GLM через Joingonka ($0.0076/$0.023), MiMo `mimo-v2.6-flash` ($0.14/$0.28; cache hit $0.0028). Дата/источник — в отчёте; чего нет — «н/д».
- [ ] **Step 7: Отчёт** — свести `stats.jsonl` прогонов (baseline, GLM, MiMo, догоны) в `build/mp-llm-eval-all/`, запустить harness с `MP_LLM_EVAL_REPORT_ONLY=1` и `MP_SUMMARY_LLM_MODELS=default,glm,mimo` (общий report без новых вызовов); затем `docs/research/2026-10-17-mp-summary-llm-comparison.md` по §3.5 спеки; сырые артефакты — `build/mp-llm-eval*/`.
- [ ] **Step 8: SQL-сверка**:
  ```sql
  SELECT model, count() rows, uniqExact(meeting_date) docs
  FROM v_cbr_mp_summary_channels
  WHERE method='llm' AND prompt_version=1
  GROUP BY model ORDER BY model;
  ```
  Ожидание: `deepseek-chat` 1089/22, GLM 22, MiMo 22.
- [ ] **Step 9: MCP**: `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `v_cbr_mp_summary_channels` читается `kimi_reader`, сырые три таблицы закрыты.
- [ ] **Step 10: Статус 1.8** — ✅ с числами и командами; коммит `roadmap key rate: mark 1.8 done (LLM model comparison)`.

---

## После задач (вне нумерации)

- Финальное ревью всей ветки (`superpowers:requesting-code-review`, модель `deepseek/deepseek-flash`) + `superpowers:verification-before-completion`.
- Один PR `feat/mp-summary-llm-models` → `main`; в описании — модели/провайдеры, env-контракт, команды прогона, ссылка на отчёт; протокол живой приёмки — комментарием в PR (стандарт §16.1 п. 8).
- После мержа владелец решает, включать ли ансамбль в ежедневный прогон; по умолчанию — одна модель.
