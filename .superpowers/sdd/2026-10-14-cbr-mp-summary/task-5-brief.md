### Task 5: LLM-слой разметки (с указанием модели)

**Files:**
- Create: `cbr/mp_summary_llm.go`
- Test: `cbr/mp_summary_llm_test.go`

**Interfaces:**
- Consumes: `util.PostJSON` (Task 1); `mpSummaryDoc`, `mpSummaryBlock` (Task 3); `mpChannel`, `mpChannelMatch` (Task 4).
- Produces (использует Task 6):
  - `const mpPromptVersion = 1`
  - `func mpLLMConfigFromEnv() (baseURL, apiKey, model string, ok bool)` — читает `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL`; любой пустой → `ok=false` (деградация без ошибки);
  - `func tagMPChannelsLLM(doc mpSummaryDoc, baseURL, apiKey, model string) ([]mpChannelMatch, error)` — POST `{baseURL}/chat/completions`, `Authorization: Bearer <apiKey>`, strict JSON; один ретрай при ошибке формата; валидация: каждая цитата обязана дословно встречаться в нормализованном тексте документа, иначе отбрасывается (warn), `slug` — только из `mpChannels`; результат — матчи с `Kind` из ответа и отдельные матчи `Kind='note'` (текст заметки); возвращает только матчи — `Method`/`model`/`prompt_version` в строки подставляет вызывающий (импортёр).

- [ ] **Step 1: Написать падающие тесты** (httptest-мок LLM; `t.Setenv` для переменных):
  - `TestMpLLMConfigFromEnv`: все три переменные → ok; без ключа → `ok=false`;
  - `TestTagMPChannelsLLMOK`: мок отдаёт валидный JSON с 11 каналами (цитаты существуют в фикстуре hold, загруженной через `parseMPSummary`) → матчи созданы, `Kind` сохранён, есть `note`-матч; запрос к моку содержит 11 slug и `Authorization`;
  - `TestTagMPChannelsLLMDropsHallucination`: мок добавляет цитату-галлюцинацию → она отсутствует в результате, остальные на месте;
  - `TestTagMPChannelsLLMBrokenJSON`: первый ответ — обрезанный JSON, второй (ретрай) — валидный → успех; оба битые → ошибка (вызывающий деградирует);
  - `TestTagMPChannelsLLM500`: мок отвечает 500 → ошибка.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать `cbr/mp_summary_llm.go`**: system-промпт (роль + 11 каналов с определениями из `mpChannels`), user — текст документа (блоки с заголовками), требование JSON-схемы; парсинг ответа — в структуру `{channels:[{slug,quotes:[{kind,text}],note}]}`; нормализация текста документа один раз.

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run TestTagMPChannelsLLM -v && go test ./cbr/ -count=1 && gofmt -l cbr/ && golangci-lint run ./cbr/...`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary_llm.go cbr/mp_summary_llm_test.go && git commit -m "cbr: LLM channel tagging with quote validation"`.

---

