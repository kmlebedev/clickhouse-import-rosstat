# Task 5 — отчёт: LLM-слой разметки каналов (`cbr/mp_summary_llm.go`)

**Status: DONE**
**Коммит:** `02755b9` — `cbr: LLM channel tagging with quote validation` (ветка `feat/cbr-mp-summary`, BASE `a7cf7b4`; 2 files changed, 917 insertions)
**Файлы (ровно два, оба новые):** `cbr/mp_summary_llm.go` (328 строк), `cbr/mp_summary_llm_test.go` (589 строк)
`deploy/staging/dagu.service` не трогался и в индекс не попадал (`git status --short` до и после коммита: ` M deploy/staging/dagu.service`).

---

## 1. Гейты (все — на закоммиченном состоянии)

```
$ go test ./cbr/ -run 'MPLLM|TagMPChannelsLLM' -v     # гейт из задания дословно
--- PASS: TestTagMPChannelsLLMOK (0.01s)
--- PASS: TestTagMPChannelsLLMDropsHallucination (0.01s)
--- PASS: TestTagMPChannelsLLMValidatesSlugAndKind (0.00s)
--- PASS: TestTagMPChannelsLLMBrokenJSON (0.01s)
--- PASS: TestTagMPChannelsLLMFencedJSON (0.00s)
--- PASS: TestTagMPChannelsLLM500 (0.00s)
--- PASS: TestTagMPChannelsLLMTransportError (0.00s)
ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  0.324s

$ go test ./cbr/ -run 'MpLLM|TagMPChannelsLLM|MpPromptVersion' -v -count=1   # + тесты, которые гейт-паттерн не ловит
--- PASS: TestMpLLMConfigFromEnv (0.00s)   ← 5 подтестов
--- PASS: TestMpPromptVersion (0.00s)
--- PASS: TestTagMPChannelsLLMOK (0.01s)
--- PASS: TestTagMPChannelsLLMDropsHallucination (0.01s)
--- PASS: TestTagMPChannelsLLMValidatesSlugAndKind (0.00s)
--- PASS: TestTagMPChannelsLLMBrokenJSON (0.01s)   ← 4 подтеста
--- PASS: TestTagMPChannelsLLMFencedJSON (0.00s)
--- PASS: TestTagMPChannelsLLM500 (0.00s)
--- PASS: TestTagMPChannelsLLMTransportError (0.00s)
ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  0.317s

$ go test ./cbr/ -count=1            → ok  github.com/kmlebedev/clickhouse-import-rosstat/cbr  2.048s
$ gofmt -l cbr/                      → (пусто)
$ go build ./...                     → ok
$ golangci-lint run ./cbr/...        → 0 issues
$ go test ./...                      → все пакеты ok (cbr, util, news, rosstat, wgc, moex, gold, minfin, fred …)
```

**Важно про гейт-паттерн:** `-run 'MPLLM|TagMPChannelsLLM'` case-sensitive и НЕ ловит
`TestMpLLMConfigFromEnv` (в имени `MpLLM`, а не `MPLLM`) и `TestMpPromptVersion` — прогон
выше по нему показывает только `TagMPChannelsLLM*`. Поэтому дополнительно прогнан широкий
паттерн `'MpLLM|TagMPChannelsLLM|MpPromptVersion'`; тесты конфига в нём зелёные.

MCP (JetBrains): `get_file_problems` (errorsOnly=false) по обоим файлам → `{"errors":[]}`;
`lint_files` по обоим файлам → `{"items":[]}`; `search_symbol tagMPChannelsLLM` →
`cbr/mp_summary_llm.go:267`. `mcp__clickhouse__*` не требовался (SQL в задаче нет).
По ходу правок MCP-инспекция нашла и я убрал три `fmt.Fprintf(&strings.Builder, …)`
(«Unhandled error») — заменены на `WriteString("…" + x + "…")`, и одно QF1012 от
staticcheck (`WriteString(fmt.Sprintf(…))` → `strconv.Itoa`).

TDD-порядок соблюдён: тесты → FAIL (`undefined: mpLLMCorpus / mpKindNote /
mpLLMConfigFromEnv / mpPromptVersion / tagMPChannelsLLM`), затем реализация → PASS.

---

## 2. Что произведено (интерфейс для Task 6)

```go
const mpPromptVersion = 1
const mpKindNote = "note"                       // вид матча-заметки
func mpLLMConfigFromEnv() (baseURL, apiKey, model string, ok bool)   // LLM_BASE_URL, LLM_API_KEY, MP_SUMMARY_LLM_MODEL
func tagMPChannelsLLM(doc mpSummaryDoc, baseURL, apiKey, model string) ([]mpChannelMatch, error)
```

Внутренние типы: `mpLLMMessage`, `mpLLMRequest` (`model`/`messages`/`temperature`),
`mpLLMChatResponse` (`choices[0].message.content`), `mpLLMQuote`, `mpLLMChannel`,
`mpLLMPayload` (`{channels:[…]}`), `mpLLMFormatError` (класс «формат»). Хелперы:
`mpLLMSystemPrompt`, `mpLLMUserPrompt`, `mpLLMCorpus`, `mpLLMJSONBody`, `mpLLMAskOnce`,
`mpLLMAsk`, `mpLLMQuoteKind`, `mpIsFormatError`, `mpIsJSONDecodeError`.

Строк в результат не подставляет ничего своего: `Method`/`Model`/`PromptVersion` — за
импортёром (T6). Порядок выдачи: сначала цитаты (в порядке ответа модели), затем заметки;
`Seq` — сквозной с 1.

---

## 3. Решения (сверх зафиксированных; каждое — с причиной)

1. **Транспорт и URL:** `util.PostJSON(strings.TrimRight(baseURL, "/") + "/chat/completions",
   {"Authorization": "Bearer "+apiKey}, req, &resp)`. Content-Type/User-Agent/дедлайн — из T1.
   Хвостовой слэш в baseURL обрезается (тест `TestTagMPChannelsLLMOK` передаёт `mock.url+"/"`
   и пинит путь `"/chat/completions"`). Ключ только в заголовке — в ошибки не попадает
   (отдельно пин `strings.Contains(err.Error(), "test-key") == false` в тестах 500 и транспорта).
2. **Тело:** `temperature: 0` (воспроизводимость: идемпотентность «(model, prompt_version) уже
   размечен» опирается на стабильность ответа), system — роль + 11 каналов `slug — имя
   (направление)` из `mpChannels` + схема ответа + 4 правила, user — текст документа с
   заголовками блоков `##`, подзаголовками `###` и маркером `- ` у пунктов. `tools`/
   `response_format` не используются (спека фиксирует промпт-схему, первичный провайдер — DeepSeek).
3. **Версия промпта — и в промпте:** `mpLLMSystemPrompt` начинается с
   `Редакция инструкции: <mpPromptVersion>.` — константа обязана быть использована в этом
   коммите (иначе dead code/`unused`), а пин `TestMpPromptVersion` проверяет литерал
   `"Редакция инструкции: 1."` подстрокой — так проверяется и значение константы, и её
   отражение в запросе (сравнение двух констант IDE гасит как «условие всегда ложно»).
4. **Ошибка формата vs отказ:** тип `mpLLMFormatError` + `mpIsFormatError`. Формат (ретраится
   один раз): обрезанный/нечитаемый конверт (`*json.SyntaxError`/`*json.UnmarshalTypeError` от
   `PostJSON`), пустой `choices`, пустой `content`, `content` без JSON, `content` не по схеме.
   Сразу ошибка без ретрая: не-2xx и ошибка транспорта (`*url.Error`) — повтором не лечатся,
   импортёр должен деградировать, а не тратить второй вызов.
5. **Извлечение JSON из `content`:** `mpLLMJSONBody` берёт от первой `{` до последней `}` —
   снимает ```-обёртку и фразу вокруг (иначе документ терял бы LLM-слой из-за оформления);
   тест `TestTagMPChannelsLLMFencedJSON`. Пин «строгости» сохранён: без скобок — ошибка формата.
6. **Корпус для сверки:** `mpLLMCorpus` = `normMPText(блок.Text)` по блокам, склейка `"\n"`
   (форма `mpChannelCorpus` из Task 4). Цитата нормализуется (`normMPText`) до сверки, чтобы
   NBSP/неразрывный дефис из ответа модели не выглядел галлюцинацией. Проверка — `Contains`,
   регистрозависимо (дословность).
7. **Валидация:** цитата вне корпуса → warn + отброс; пустая после нормализации → warn + отброс;
   `slug` вне `mpChannels` → warn + отброс канала целиком (и цитат, и заметки); `kind` вне
   тройки → warn + приведение к `prose`. Дедупликация цитат — существующим ключом
   `mpChannelMatchKey{Channel,Kind,Text}`; заметка — одна на канал (первая непустая), ключ
   таблицы (`…, kind, …, seq`) дублей бы не различил.
8. **Конфигурация:** `os.Getenv` + `strings.TrimSpace` (env-файл легко приносит хвостовой
   перевод строки, а пробел в baseURL — молча битый URL); любая пустая → `ok=false` без ошибки.
9. **Тесты офлайн:** все вызовы — на `httptest`-мок (`mpNewLLMMock`: скрипт ответов по порядку
   вызовов + запись запросов; после исчерпания скрипта — 500, чтобы лишний вызов не
   маскировался успехом); env — только `t.Setenv`; сеть в тестах не трогается.

---

## 4. Тесты

Обязательные по брифу: `TestMpLLMConfigFromEnv` (5 подтестов: все переменные; нет каждой из
трёх; пробелы), `TestTagMPChannelsLLMOK` (11 каналов, 11 цитат + 10 заметок = 21 матч,
`Kind` сохранён для `prose`/`risk_pro`/`risk_dez`, у `money_supply` note пуста → матча нет,
порядок «цитаты → заметки», `Seq` 1..21; в запросе — метод/путь/`Authorization`/`Content-Type`,
все 11 slug, `"temperature":0`, `"model":"test-model"`, дословный текст документа, заголовок
блока, схема), `TestTagMPChannelsLLMDropsHallucination` (выдуманная цитата отсутствует,
дословные на месте), `TestTagMPChannelsLLMBrokenJSON` (4 подтеста: обрезанный content,
обрезанное тело конверта, пустой content → ретрай вылечил, ровно 2 запроса и одинаковое тело;
два битых подряд → ошибка без третьего запроса), `TestTagMPChannelsLLM500` (ошибка, ровно один
запрос, кода 500 в тексте, ключа нет).

Дополнительно (в рамках «решений» выше): `TestMpPromptVersion`,
`TestTagMPChannelsLLMValidatesSlugAndKind` (slug вне реестра; `kind="inflation_risk"` → `prose`),
`TestTagMPChannelsLLMFencedJSON`, `TestTagMPChannelsLLMTransportError` (закрытый сервер).
Пред-условия тестов проверяются явно: `mpLLMCheckPins` (пины-цитаты обязаны быть подстроками
документа) и `mpLLMCheckMatches` (инварианты: `Seq` сквозной, канал из реестра, вид из
четвёрки, цитаты дословны). Пины-цитаты — те же дословные предложения фикстуры
`mp_summary_23092026.html`, что в `mp_summary_channels_test.go` (Task 4).

---

## 5. Наблюдения (не блокеры)

1. **Стоимость вызова:** для hold-фикстуры system-промпт ≈ 2 КБ, user-сообщение ≈ 46 КБ
   (корпус 46 056 байт, 50 блоков) — ~12k токенов на документ; для 22 резюме бэкфилла приёмки
   это разумно, но при ретрае промпт отправляется заново (тело запроса в тесте сверяется как
   идентичное). Если объём станет проблемой, резать user-сообщение по блокам — отдельная задача.
2. **Warn при отбросе цитаты** печатается `logrus`-ом в выводе теста (ожидаемо; пины на
   отсутствии текста в результате, а не на логе).
3. `mpPromptVersion` теперь виден в самом промпте («Редакция инструкции») — сознательно:
   так константа используется в этом коммите и пин не вырождается в сравнение двух констант.

Проверка соответствия интерфейсов: `tagMPChannelsLLM` и `mpLLMConfigFromEnv` объявлены
точь-в-точь как в брифе (сигнатуры сверены с `docs/superpowers/plans/2026-10-14-cbr-mp-summary.md`
§Task 5 и c `task-5-brief.md`); `mpChannelMatch`/`mpChannelMatchKey`/`mpChannels`/`mpKindProse`
взяты из Task 4 без изменений.
