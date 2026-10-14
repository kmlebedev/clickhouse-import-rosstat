package cbr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Пины-цитаты — дословные предложения фикстуры 23.09.2026 (hold), те же, что в
// mp_summary_channels_test.go (Task 4): если они перестанут быть подстроками документа,
// сломается не только regex-слой, но и это пред-условие LLM-тестов (mpLLMCheckPins).
const (
	mpLLMLaborPin        = "Дефицит работников сокращался, безработица несколько выросла, темпы роста номинальных и реальных заработных плат замедлились."
	mpLLMLaborPin2       = "Напряженность на рынке труда продолжала постепенно снижаться."
	mpLLMExpectationsPin = "Инфляционные ожидания населения в августе снизились, тогда как ценовые ожидания бизнеса в августе \u2013 сентябре несколько выросли."
	mpLLMRiskProPin      = "Усиление дисбаланса спроса и предложения."
	mpLLMRiskDezPin      = "Значительное замедление роста внутреннего спроса."
	mpLLMOilPin          = "Цены на нефть складывались выше июльского прогноза на фоне сохраняющихся ограничений судоходства через Ормузский пролив."
	mpLLMMultiPin        = "Внутренний спрос может оставаться высоким под влиянием ускорения кредитования, значительного бюджетного импульса и продолжающегося роста заработных плат темпами выше производительности труда."
)

// mpLLMMockResponse — один ответ мока: статус (0 → 200) и тело.
type mpLLMMockResponse struct {
	Status int
	Body   string
}

// mpLLMMockRequest — записанный мок-запрос: путь, заголовки и тело — предмет пинов
// TestTagMPChannelsLLMOK (ключ едет заголовком x-api-key, а не в URL).
type mpLLMMockRequest struct {
	Method      string
	Path        string
	APIKey      string
	ContentType string
	Body        string
}

// mpLLMMock — httptest-мок Anthropic-совместимого провайдера: отдаёт ответы скрипта по
// порядку вызовов и записывает все запросы. Запрос после исчерпания скрипта получает 500 —
// лишний вызов (третий после ретрая) не должен маскироваться успешным ответом.
type mpLLMMock struct {
	script []mpLLMMockResponse
	url    string

	mu       sync.Mutex
	requests []mpLLMMockRequest
}

// mpNewLLMMock поднимает мок и снимает его по завершении теста.
func mpNewLLMMock(t *testing.T, responses ...mpLLMMockResponse) *mpLLMMock {
	t.Helper()
	m := &mpLLMMock{script: responses}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.requests = append(m.requests, mpLLMMockRequest{
			Method:      r.Method,
			Path:        r.URL.Path,
			APIKey:      r.Header.Get("x-api-key"),
			ContentType: r.Header.Get("Content-Type"),
			Body:        string(body),
		})
		idx := len(m.requests) - 1
		m.mu.Unlock()
		if idx >= len(m.script) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if resp := m.script[idx]; resp.Status != 0 {
			w.WriteHeader(resp.Status)
		}
		_, _ = io.WriteString(w, m.script[idx].Body)
	}))
	t.Cleanup(srv.Close)
	m.url = srv.URL
	return m
}

// calls — число полученных запросов.
func (m *mpLLMMock) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

// request — запрос по номеру (пустая структура, если такого не было).
func (m *mpLLMMock) request(i int) mpLLMMockRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < 0 || i >= len(m.requests) {
		return mpLLMMockRequest{}
	}
	return m.requests[i]
}

// mpLLMContentBody — тело успешного ответа провайдера: Anthropic-конверт с одним text-блоком
// (content — то, что в OpenAI-конверте лежало в choices[0].message.content).
func mpLLMContentBody(t *testing.T, content string) string {
	t.Helper()
	return mpLLMEnvelope(t, []any{map[string]any{"type": "text", "text": content}})
}

// mpLLMEmptyContentBody — успешный ответ (200) без единого блока контента: провайдер прислал
// валидный конверт, но разметки в нём нет.
func mpLLMEmptyContentBody(t *testing.T) string {
	t.Helper()
	return mpLLMEnvelope(t, []any{})
}

// mpLLMEnvelope собирает конверт ответа Anthropic Messages API с заданным content.
func mpLLMEnvelope(t *testing.T, content []any) string {
	t.Helper()
	if content == nil {
		content = []any{}
	}
	body, err := json.Marshal(map[string]any{
		"id":            "msg_1",
		"type":          "message",
		"role":          "assistant",
		"model":         "test-model",
		"content":       content,
		"stop_reason":   "end_turn",
		"stop_sequence": nil,
		"usage":         map[string]any{"input_tokens": 1, "output_tokens": 1},
	})
	if err != nil {
		t.Fatalf("сборка конверта ответа: %v", err)
	}
	return string(body)
}

// mpLLMQuoteSpec — цитата канала в мок-ответе модели.
type mpLLMQuoteSpec struct {
	Kind string
	Text string
}

// mpLLMChannelSpec — разметка одного канала в мок-ответе модели.
type mpLLMChannelSpec struct {
	Slug   string
	Quotes []mpLLMQuoteSpec
	Note   string
}

// mpLLMPayloadBody — content ответа модели ({"channels":[…]}). Собирается json.Marshal-ом,
// чтобы тестовые цитаты с кавычками и кириллицей не приходилось экранировать вручную.
func mpLLMPayloadBody(t *testing.T, channels ...mpLLMChannelSpec) string {
	t.Helper()
	out := make([]any, 0, len(channels))
	for _, ch := range channels {
		quotes := make([]any, 0, len(ch.Quotes))
		for _, q := range ch.Quotes {
			quotes = append(quotes, map[string]any{"kind": q.Kind, "text": q.Text})
		}
		out = append(out, map[string]any{"slug": ch.Slug, "quotes": quotes, "note": ch.Note})
	}
	body, err := json.Marshal(map[string]any{"channels": out})
	if err != nil {
		t.Fatalf("сборка content ответа: %v", err)
	}
	return string(body)
}

// mpLLMFixtureDoc — документ резюме 23.09.2026 (hold): цитаты-пины взяты из него, документ
// разбирается тем же парсером, что и на проде.
func mpLLMFixtureDoc(t *testing.T) mpSummaryDoc {
	t.Helper()
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_23092026.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	return doc
}

// mpLLMCheckPins — пред-условие тестов: каждая цитата-пин дословно входит в нормализованный
// текст документа, иначе «отброшенная галлюцинация» проверяла бы не то, что кажется.
func mpLLMCheckPins(t *testing.T, doc mpSummaryDoc, pins ...string) {
	t.Helper()
	corpus := mpLLMCorpus(doc)
	for _, pin := range pins {
		if !strings.Contains(corpus, pin) {
			t.Errorf("пин-цитата отсутствует в тексте документа: %q", pin)
		}
	}
}

// mpLLMCheckMatches — инварианты результата LLM-слоя: Seq — сквозной с 1 в порядке выдачи,
// текст непустой, канал из реестра, вид из risk_pro|risk_dez|prose|note, а цитаты (не note)
// дословно входят в нормализованный текст документа.
func mpLLMCheckMatches(t *testing.T, doc mpSummaryDoc, matches []mpChannelMatch) {
	t.Helper()
	corpus := mpLLMCorpus(doc)
	slugs := map[string]bool{}
	for _, slug := range mpRegistrySlugs() {
		slugs[slug] = true
	}
	for i, m := range matches {
		if m.Seq != i+1 {
			t.Errorf("матч %d: Seq=%d, ожидался %d (сквозной от 1 в порядке выдачи)", i, m.Seq, i+1)
		}
		if m.Text == "" {
			t.Errorf("матч %d (%s, %s): пустой текст", i, m.Channel, m.Kind)
		}
		if !slugs[m.Channel] {
			t.Errorf("матч %d: канал %q не из реестра", i, m.Channel)
		}
		switch m.Kind {
		case mpKindNote:
			// Заметка — формулировка модели, сверке с текстом резюме не подлежит.
		case mpKindRiskPro, mpKindRiskDez, mpKindProse:
			if !strings.Contains(corpus, m.Text) {
				t.Errorf("матч %d (%s, %s): цитата не дословна: %q", i, m.Channel, m.Kind, m.Text)
			}
		default:
			t.Errorf("матч %d: вид %q вне risk_pro|risk_dez|prose|note", i, m.Kind)
		}
	}
}

// mpLLMValidChannels — валидный ответ модели: по одной дословной цитате на каждый из 11
// каналов реестра, два вида рисков (risk_pro/risk_dez) и десять заметок — у money_supply
// note пуста, её матча быть не должно.
func mpLLMValidChannels() []mpLLMChannelSpec {
	return []mpLLMChannelSpec{
		{Slug: "current_inflation", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMMultiPin}},
			Note: "Внутренний спрос и рост зарплат поддерживают текущий рост цен."},
		{Slug: "core_inflation", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}},
			Note: "Устойчивая инфляция замедляется вместе с рынком труда."},
		{Slug: "expectations", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMExpectationsPin}},
			Note: "Ценовые ожидания бизнеса выросли."},
		{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin2}},
			Note: "Напряжённость на рынке труда снижается."},
		{Slug: "lending_transmission", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMMultiPin}},
			Note: "Кредитование остаётся драйвером спроса."},
		{Slug: "output_gap", Quotes: []mpLLMQuoteSpec{{Kind: mpKindRiskPro, Text: mpLLMRiskProPin}},
			Note: "Дисбаланс спроса и предложения — проинфляционный риск."},
		{Slug: "budget_impulse", Quotes: []mpLLMQuoteSpec{{Kind: mpKindRiskDez, Text: mpLLMRiskDezPin}},
			Note: "Замедление внутреннего спроса — дезинфляционный риск."},
		// money_supply без заметки: канал упомянут цитатой, но note пуста.
		{Slug: "money_supply", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMOilPin}}},
		{Slug: "external_fx", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMOilPin}},
			Note: "Нефть выше прогноза поддерживает рубль."},
		{Slug: "savings_behavior", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin2}},
			Note: "Сберегательное поведение сдерживает спрос."},
		{Slug: "autonomous_factors", Quotes: []mpLLMQuoteSpec{{Kind: mpKindRiskPro, Text: mpLLMRiskProPin}},
			Note: "Разовые факторы учтены в решении."},
	}
}

// TestMpLLMConfigFromEnv — конфигурация LLM-слоя: все три переменные заполнены → ok; любая
// пустая → ok=false без ошибки (импортёр деградирует до regex-слоя). Окружающие пробелы
// обрезаются: env-файл легко приносит хвостовой перевод строки.
func TestMpLLMConfigFromEnv(t *testing.T) {
	const (
		base  = "https://api.example/v1"
		key   = "secret-key"
		model = "deepseek-chat"
	)
	cases := []struct {
		name             string
		base, key, model string
		ok               bool
	}{
		{"все переменные заполнены", base, key, model, true},
		{"нет LLM_BASE_URL", "", key, model, false},
		{"нет LLM_API_KEY", base, "", model, false},
		{"нет MP_SUMMARY_LLM_MODEL", base, key, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LLM_BASE_URL", tc.base)
			t.Setenv("LLM_API_KEY", tc.key)
			t.Setenv("MP_SUMMARY_LLM_MODEL", tc.model)
			gotBase, gotKey, gotModel, ok := mpLLMConfigFromEnv()
			if ok != tc.ok {
				t.Fatalf("ok = %v, ожидалось %v", ok, tc.ok)
			}
			if !tc.ok {
				return
			}
			if gotBase != base || gotKey != key || gotModel != model {
				t.Errorf("конфигурация = (%q, %q, %q), ожидалось (%q, %q, %q)",
					gotBase, gotKey, gotModel, base, key, model)
			}
		})
	}
	t.Run("окружающие пробелы обрезаются", func(t *testing.T) {
		t.Setenv("LLM_BASE_URL", "  "+base+"\n")
		t.Setenv("LLM_API_KEY", "\t"+key)
		t.Setenv("MP_SUMMARY_LLM_MODEL", model+" ")
		gotBase, gotKey, gotModel, ok := mpLLMConfigFromEnv()
		if !ok {
			t.Fatal("ok = false, ожидалось true при непустых значениях в пробелах")
		}
		if gotBase != base || gotKey != key || gotModel != model {
			t.Errorf("конфигурация = (%q, %q, %q), ожидалось (%q, %q, %q)",
				gotBase, gotKey, gotModel, base, key, model)
		}
	})
}

// TestMpPromptVersion — версия промпта: mpPromptVersion пишется импортёром в колонку
// prompt_version (ключ ReplacingMergeTree), поэтому новая редакция промпта обязана добавлять
// строки разметки рядом со старыми, а не затирать их. Проверяется по метке в system-промпте:
// так пин держит и значение константы, и её отражение в запросе.
func TestMpPromptVersion(t *testing.T) {
	const want = "Редакция инструкции: 1."
	if prompt := mpLLMSystemPrompt(); !strings.Contains(prompt, want) {
		t.Fatalf("system-промпт не содержит метку редакции %q: смена mpPromptVersion обязана менять и её", want)
	}
}

// TestTagMPChannelsLLMOK — валидный ответ с 11 каналами: цитаты становятся матчами с
// видом из ответа, заметки — матчами Kind='note' (у money_supply note пуста — матча нет),
// Seq сквозной; запрос уходит на /v1/messages (Anthropic-совместимый транспорт) с ключом в
// заголовке x-api-key, обязательным max_tokens и промптом, содержащим все 11 slug'ов и текст
// документа.
func TestTagMPChannelsLLMOK(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	mpLLMCheckPins(t, doc, mpLLMLaborPin, mpLLMLaborPin2, mpLLMExpectationsPin,
		mpLLMRiskProPin, mpLLMRiskDezPin, mpLLMOilPin, mpLLMMultiPin)

	channels := mpLLMValidChannels()
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, channels...))})

	// baseURL с хвостовым слэшем: путь запроса не должен получиться «//v1/messages».
	matches, err := tagMPChannelsLLM(doc, mock.url+"/", "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v", err)
	}

	// --- запрос ---
	if mock.calls() != 1 {
		t.Fatalf("запросов %d, ожидался 1 (без ретрая при валидном ответе)", mock.calls())
	}
	req := mock.request(0)
	if req.Method != http.MethodPost {
		t.Errorf("метод %q, ожидался POST", req.Method)
	}
	if req.Path != "/v1/messages" {
		t.Errorf("путь %q, ожидался /v1/messages", req.Path)
	}
	if req.APIKey != "test-key" {
		t.Errorf("x-api-key = %q, ожидалось %q", req.APIKey, "test-key")
	}
	if req.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, ожидался application/json", req.ContentType)
	}
	for _, ch := range channels {
		if !strings.Contains(req.Body, ch.Slug) {
			t.Errorf("в теле запроса нет slug %q", ch.Slug)
		}
	}
	for _, want := range []string{
		`"model":"test-model"`,
		`"temperature":0`,
		`"max_tokens":8192`, // в Anthropic API max_tokens обязателен
		`"system":[{"text":"Редакция инструкции: 1.`, // system — отдельное поле, не роль в messages
		`"role":"user"`,
		mpLLMLaborPin,        // user-сообщение несёт дословный текст документа
		"Инфляционные риски", // …и заголовки блоков
		mpKindRiskPro,        // …и требование схемы
		mpKindRiskDez,
		mpKindProse,
		`\"quotes\"`, // в теле запроса кавычки промпта экранированы JSON-кодированием
		`\"note\"`,
	} {
		if !strings.Contains(req.Body, want) {
			t.Errorf("в теле запроса нет %q", want)
		}
	}

	// --- результат ---
	mpLLMCheckMatches(t, doc, matches)
	quoteMatches := len(channels)
	wantNotes := 0
	for _, ch := range channels {
		if strings.TrimSpace(ch.Note) != "" {
			wantNotes++
		}
	}
	if len(matches) != quoteMatches+wantNotes {
		t.Fatalf("матчей %d, ожидалось %d (цитат %d + заметок %d)",
			len(matches), quoteMatches+wantNotes, quoteMatches, wantNotes)
	}
	if got := mpMatchKinds(matches)[mpKindNote]; got != wantNotes {
		t.Errorf("матчей note %d, ожидалось %d", got, wantNotes)
	}
	// Порядок выдачи: сначала все цитаты, затем заметки.
	for i, m := range matches {
		switch {
		case i < quoteMatches && m.Kind == mpKindNote:
			t.Errorf("матч %d: note раньше конца блоков цитат", i)
		case i >= quoteMatches && m.Kind != mpKindNote:
			t.Errorf("матч %d (%s): после заметок ожидался только note", i, m.Kind)
		}
	}
	// Вид из ответа сохранён (risk_pro/risk_dez не превратились в prose), словарный матч —
	// ровно один (Channel, Kind, Text) на канал; через mpMatchCounts: канал с заметкой — 2.
	for _, want := range []struct {
		channel, kind, text string
	}{
		{"expectations", mpKindProse, mpLLMExpectationsPin},
		{"output_gap", mpKindRiskPro, mpLLMRiskProPin},
		{"budget_impulse", mpKindRiskDez, mpLLMRiskDezPin},
	} {
		if !mpHasMatchKindText(matches, want.channel, want.kind, want.text) {
			t.Errorf("нет матча (%s, %s, %q)", want.channel, want.kind, want.text)
		}
	}
	counts := mpMatchCounts(matches)
	for _, ch := range channels {
		want := 1
		if strings.TrimSpace(ch.Note) != "" {
			want = 2
		}
		if counts[ch.Slug] != want {
			t.Errorf("канал %s: матчей %d, ожидалось %d (1 цитата + заметка)", ch.Slug, counts[ch.Slug], want)
		}
	}
}

// TestTagMPChannelsLLMDropsHallucination — цитата, которой нет в тексте резюме, отбрасывается
// с warn, а остальные матчи документа остаются: модель не имеет права «доцитировать»
// формулировку, которой ЦБ не писал.
func TestTagMPChannelsLLMDropsHallucination(t *testing.T) {
	const hallucination = "Совет директоров допускает повышение ключевой ставки до 25,00% годовых на ближайшем заседании."

	doc := mpLLMFixtureDoc(t)
	if strings.Contains(mpLLMCorpus(doc), hallucination) {
		t.Fatalf("цитата-галлюцинация случайно встречается в документе: %q", hallucination)
	}
	channels := []mpLLMChannelSpec{
		{Slug: "output_gap", Quotes: []mpLLMQuoteSpec{
			{Kind: mpKindRiskPro, Text: mpLLMRiskProPin},
			{Kind: mpKindRiskPro, Text: hallucination},
		}},
		{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}},
			Note: "Рынок труда смягчается."},
	}
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, channels...))})

	matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v", err)
	}
	mpLLMCheckMatches(t, doc, matches)
	for _, m := range matches {
		if m.Text == hallucination {
			t.Errorf("цитата-галлюцинация попала в результат: %+v", m)
		}
	}
	if !mpHasMatchKindText(matches, "output_gap", mpKindRiskPro, mpLLMRiskProPin) {
		t.Errorf("дословная цитата output_gap потеряна: %v", mpMatchTexts(matches, "output_gap"))
	}
	if !mpHasMatchKindText(matches, "labor_market", mpKindProse, mpLLMLaborPin) {
		t.Errorf("дословная цитата labor_market потеряна: %v", mpMatchTexts(matches, "labor_market"))
	}
	if len(matches) != 3 { // 2 дословные цитаты + 1 заметка
		t.Errorf("матчей %d, ожидалось 3 (галлюцинация отброшена)", len(matches))
	}
}

// TestTagMPChannelsLLMValidatesSlugAndKind — slug вне реестра отбрасывается целиком (цитаты и
// заметка), вид вне тройки приводится к prose: в данные не должно попасть ничего, чего нет
// в реестре каналов и в схеме видов.
func TestTagMPChannelsLLMValidatesSlugAndKind(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	channels := []mpLLMChannelSpec{
		{Slug: "quantum_inflation", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMOilPin}},
			Note: "Канал, которого нет в реестре."},
		{Slug: "expectations", Quotes: []mpLLMQuoteSpec{{Kind: "inflation_risk", Text: mpLLMExpectationsPin}}},
	}
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, channels...))})

	matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v", err)
	}
	mpLLMCheckMatches(t, doc, matches)
	if len(matches) != 1 {
		t.Fatalf("матчей %d, ожидался 1: %v", len(matches), matches)
	}
	if got := matches[0]; got.Channel != "expectations" || got.Kind != mpKindProse || got.Text != mpLLMExpectationsPin {
		t.Errorf("матч = %+v, ожидался expectations/%s с дословной цитатой", got, mpKindProse)
	}
	for _, m := range matches {
		if m.Channel == "quantum_inflation" {
			t.Errorf("канал вне реестра попал в результат: %+v", m)
		}
	}
}

// mpLLMCheckRetryToLaborMatch — общий хвост тестов «первый ответ битый → ретрай → разметка»:
// ретрай вылечил формат (ровно два запроса с тем же телом промпта) и дал один дословный матч
// labor_market.
func mpLLMCheckRetryToLaborMatch(t *testing.T, mock *mpLLMMock, matches []mpChannelMatch, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v (ретрай должен был вылечить формат)", err)
	}
	if mock.calls() != 2 {
		t.Errorf("запросов %d, ожидалось 2 (первый + ретрай)", mock.calls())
	}
	if len(matches) != 1 || !mpHasMatchKindText(matches, "labor_market", mpKindProse, mpLLMLaborPin) {
		t.Errorf("матчи после ретрая = %+v, ожидался один дословный матч labor_market", matches)
	}
	if first, second := mock.request(0).Body, mock.request(1).Body; first != second {
		t.Error("ретрай отправил другое тело запроса: промпт должен быть тем же")
	}
}

// TestTagMPChannelsLLMBrokenJSON — один ретрай на ошибку формата: обрезанный content,
// обрезанное тело ответа и пустой text-блок лечатся вторым запросом; два битых ответа подряд
// дают ошибку (импортёр деградирует до regex-слоя) и третьего запроса не делают.
func TestTagMPChannelsLLMBrokenJSON(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	valid := mpLLMContentBody(t, mpLLMPayloadBody(t,
		mpLLMChannelSpec{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}}}))

	// Обрезанный content: модель не дописала JSON разметки.
	const truncatedContent = `{"channels":[{"slug":"labor_market","quotes":[{"kind":"prose","text":"Дефицит работников`
	// Обрезанное тело ответа: конверт не дописан — json.Decoder внутри SDK падает на середине.
	const truncatedEnvelope = `{"id":"msg_1","type":"message","content":[{"type":"text","text":"{\"channels\":[]}`

	cases := []struct {
		name  string
		first string
	}{
		{"обрезанный content", mpLLMContentBody(t, truncatedContent)},
		{"обрезанное тело ответа", truncatedEnvelope},
		{"пустой text-блок", mpLLMContentBody(t, "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := mpNewLLMMock(t,
				mpLLMMockResponse{Body: tc.first},
				mpLLMMockResponse{Body: valid})
			matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
			mpLLMCheckRetryToLaborMatch(t, mock, matches, err)
		})
	}
	t.Run("оба ответа битые → ошибка", func(t *testing.T) {
		mock := mpNewLLMMock(t,
			mpLLMMockResponse{Body: mpLLMContentBody(t, truncatedContent)},
			mpLLMMockResponse{Body: mpLLMContentBody(t, truncatedContent)})
		matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
		if err == nil {
			t.Fatalf("ожидалась ошибка после двух битых ответов, получены матчи %+v", matches)
		}
		if mock.calls() != 2 {
			t.Errorf("запросов %d, ожидалось 2 (больше одного ретрая не делаем)", mock.calls())
		}
	})
}

// TestTagMPChannelsLLMNoTextBlock — успешный ответ (200, валидный конверт) без единого
// text-блока: разметки в нём нет, и это ошибка формата, а не «разметки нет» — иначе документ
// молча остался бы без слоя 2 при зелёном прогоне. Классификация пинится напрямую по
// mpLLMAskOnce (формат, не отказ провайдера), а через tagMPChannelsLLM — ретрай формата:
// первый вызов лечится вторым, то есть ровно два запроса.
func TestTagMPChannelsLLMNoTextBlock(t *testing.T) {
	doc := mpLLMFixtureDoc(t)

	t.Run("один вызов без text-блока → ошибка формата", func(t *testing.T) {
		mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMEmptyContentBody(t)})
		_, err := mpLLMAskOnce(doc, mock.url, "test-key", "test-model")
		if err == nil {
			t.Fatal("ответ без text-блока: ожидалась ошибка")
		}
		if !mpIsFormatError(err) {
			t.Errorf("ошибка %v не классифицирована как ошибка формата", err)
		}
		if mock.calls() != 1 {
			t.Errorf("запросов %d, ожидался 1 (ретрейт решает mpLLMAsk, а не mpLLMAskOnce)", mock.calls())
		}
	})

	t.Run("без text-блока → один ретрай → разметка", func(t *testing.T) {
		valid := mpLLMContentBody(t, mpLLMPayloadBody(t,
			mpLLMChannelSpec{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}}}))
		mock := mpNewLLMMock(t,
			mpLLMMockResponse{Body: mpLLMEmptyContentBody(t)},
			mpLLMMockResponse{Body: valid})
		matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
		mpLLMCheckRetryToLaborMatch(t, mock, matches, err)
	})
}

// TestTagMPChannelsLLMFencedJSON — модель обрамила JSON ```-блоком и сопроводила фразой:
// такой ответ разбирается, а не трактуется как ошибка формата (иначе документ терял бы
// LLM-слой из-за оформления).
func TestTagMPChannelsLLMFencedJSON(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	payload := mpLLMPayloadBody(t,
		mpLLMChannelSpec{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}},
			Note: "Рынок труда смягчается."})
	content := "```json\n" + payload + "\n```\nНадеюсь, разметка полезна."
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, content)})

	matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v", err)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, ожидался 1 (формат валиден, ретрай не нужен)", mock.calls())
	}
	mpLLMCheckMatches(t, doc, matches)
	if len(matches) != 2 || !mpHasMatchKindText(matches, "labor_market", mpKindProse, mpLLMLaborPin) {
		t.Errorf("матчи = %+v, ожидались цитата и заметка labor_market", matches)
	}
}

// TestTagMPChannelsLLM500 — отказ провайдера (500) возвращается ошибкой сразу, без ретрая:
// повтором он не лечится, а импортёр по ошибке уходит в деградацию. Счётчик запросов пинит и
// выключенные повторы SDK (WithMaxRetries(0): с дефолтными двумя их было бы три — 500 для SDK
// ретраебелен). Ключ в текст ошибки не попадает (он едет заголовком x-api-key).
func TestTagMPChannelsLLM500(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	mock := mpNewLLMMock(t, mpLLMMockResponse{Status: http.StatusInternalServerError, Body: `{"error":"internal"}`})

	matches, err := tagMPChannelsLLM(doc, mock.url, "test-key", "test-model")
	if err == nil {
		t.Fatalf("500: ожидалась ошибка, получены матчи %+v", matches)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("ошибка %v не содержит код 500", err)
	}
	if strings.Contains(err.Error(), "test-key") {
		t.Errorf("в текст ошибки утёк ключ API: %v", err)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, ожидался 1 (отказ провайдера не повторяется)", mock.calls())
	}
}

// TestTagMPChannelsLLMTransportError — недоступный провайдер: ошибка, без ретрая и без ключа
// в тексте (net/http печатает URL запроса, но не заголовки).
func TestTagMPChannelsLLMTransportError(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	_, err := tagMPChannelsLLM(doc, url, "test-key", "test-model")
	if err == nil {
		t.Fatal("недоступный провайдер: ожидалась ошибка")
	}
	if strings.Contains(err.Error(), "test-key") {
		t.Errorf("в текст ошибки утёк ключ API: %v", err)
	}
}

// TestTagMPChannelsLLMTimeout — зависший провайдер: запрос падает по дедлайну (mpLLMTimeout,
// короткий дедлайн подставлен тестом), а не ждёт вечно — иначе шаг dagu повис бы вместе с
// провайдером. Ошибка дедлайна — не формат: ретрая формата нет (одна попытка SDK).
func TestTagMPChannelsLLMTimeout(t *testing.T) {
	const handlerDelay = 5 * time.Second
	doc := mpLLMFixtureDoc(t)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Тело вычитывается до конца до select: на разрыв соединения (отмену контекста)
		// хендлер завершается сразу и server.Close() в defer не ждёт полный handlerDelay.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(handlerDelay):
		}
	}))
	defer srv.Close()

	old := mpLLMTimeout
	mpLLMTimeout = time.Second
	t.Cleanup(func() { mpLLMTimeout = old })

	_, err := tagMPChannelsLLM(doc, srv.URL, "test-key", "test-model")
	if err == nil {
		t.Fatal("зависший провайдер: ожидалась ошибка дедлайна")
	}
	// Именно дедлайн, а не любая другая ошибка: на него и рассчитывает деградация до
	// regex-слоя — иначе «получили ошибку» ничего не говорит о причине.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не является ошибкой дедлайна (context.DeadlineExceeded)", err)
	}
	if strings.Contains(err.Error(), "test-key") {
		t.Errorf("в текст ошибки утёк ключ API: %v", err)
	}
}
