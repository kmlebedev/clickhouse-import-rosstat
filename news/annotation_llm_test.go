package news

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// newsTestLLMResponse — один ответ мока: статус (0 → 200) и тело.
type newsTestLLMResponse struct {
	Status int
	Body   string
}

// newsTestLLMRequest — записанный мок-запрос: путь, заголовки и тело — предмет пинов
// TestNewsLLMAskOnceOK (ключ едет заголовком x-api-key, а не в URL).
type newsTestLLMRequest struct {
	Method      string
	Path        string
	APIKey      string
	Auth        string // Authorization: Bearer … (OpenAI-путь); у Anthropic пусто
	ContentType string
	Body        string
}

// newsTestLLMMock — httptest-мок Anthropic-совместимого провайдера: отдаёт ответы скрипта
// по порядку вызовов и записывает все запросы. Запрос после исчерпания скрипта получает 500 —
// лишний вызов (третий после ретрая) не должен маскироваться успешным ответом.
type newsTestLLMMock struct {
	script []newsTestLLMResponse
	url    string

	mu       sync.Mutex
	requests []newsTestLLMRequest
}

// newNewsTestLLMMock поднимает мок и снимает его по завершении теста.
func newNewsTestLLMMock(t *testing.T, responses ...newsTestLLMResponse) *newsTestLLMMock {
	t.Helper()
	m := &newsTestLLMMock{script: responses}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.requests = append(m.requests, newsTestLLMRequest{
			Method:      r.Method,
			Path:        r.URL.Path,
			APIKey:      r.Header.Get("x-api-key"),
			Auth:        r.Header.Get("Authorization"),
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

func (m *newsTestLLMMock) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *newsTestLLMMock) request(i int) newsTestLLMRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < 0 || i >= len(m.requests) {
		return newsTestLLMRequest{}
	}
	return m.requests[i]
}

// newsTestEnvelope собирает конверт ответа Anthropic Messages API с заданным content.
func newsTestEnvelope(t *testing.T, content []any) string {
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

// newsTestContentBody — тело успешного ответа с одним text-блоком.
func newsTestContentBody(t *testing.T, content string) string {
	t.Helper()
	return newsTestEnvelope(t, []any{map[string]any{"type": "text", "text": content}})
}

// newsTestEmptyContentBody — успешный ответ (200) без единого блока контента.
func newsTestEmptyContentBody(t *testing.T) string {
	t.Helper()
	return newsTestEnvelope(t, nil)
}

// newsTestOpenAIBody — тело успешного OpenAI-совместимого ответа: content в
// choices[0].message.content (путь GLM в профиле glm).
func newsTestOpenAIBody(t *testing.T, content string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]any{"content": content},
		}},
	})
	if err != nil {
		t.Fatalf("сборка OpenAI-ответа: %v", err)
	}
	return string(body)
}

// newsTestPayloadBody — content ответа модели ({"messages":[…]}). Собирается json.Marshal-ом,
// чтобы цитаты с кавычками и кириллицей не приходилось экранировать вручную.
func newsTestPayloadBody(t *testing.T, messages ...newsAnnotationMessage) string {
	t.Helper()
	if messages == nil {
		messages = []newsAnnotationMessage{}
	}
	body, err := json.Marshal(newsAnnotationPayload{Messages: messages})
	if err != nil {
		t.Fatalf("сборка content ответа: %v", err)
	}
	return string(body)
}

// newsTestLLMInputs возвращает n Telegram-сообщений с уникальными URL и общим текстом:
// цитата-пин есть в каждом, поэтому валидный ответ проходит проверку evidence.
func newsTestLLMInputs(n int) []newsItem {
	const text = "Трамп объявил о новых пошлинах на импорт из Китая."
	inputs := make([]newsItem, 0, n)
	for i := 0; i < n; i++ {
		title := "Трамп объявил о пошлинах"
		inputs = append(inputs, newsItem{
			Date:        time.Date(2026, time.October, 17, 9, i, 0, 0, time.UTC),
			Source:      "interfax",
			Title:       title,
			URL:         "https://t.me/interfaxonline/" + strconv.Itoa(i+1),
			Description: text,
			ContentHash: contentHash(title, text),
		})
	}
	return inputs
}

// newsTestValidMessage — валидная разметка одного сообщения: sentiment/relevance в диапазоне,
// один тег person:trump с дословной цитатой.
func newsTestValidMessage(url string) newsAnnotationMessage {
	return newsAnnotationMessage{
		URL:       url,
		Sentiment: 0.4,
		Relevance: 0.8,
		Summary:   "Трамп объявил о пошлинах.",
		Tags: []newsAnnotationTag{{
			Dimension:  newsDimPerson,
			Tag:        "trump",
			Direction:  newsDirectionNeutral,
			Confidence: 0.9,
			Evidence:   "Трамп объявил о новых пошлинах",
		}},
	}
}

// TestNewsLLMConfigFromEnv — конфигурация LLM-разметки по профилю: default читает общие
// LLM_* и идёт Anthropic-путём, glm — GLM_LLM_* и OpenAI-путём. Любая пустая переменная →
// ok=false (импорт деградирует, догон следующим прогоном); неизвестный профиль — тоже false.
// Окружающие пробелы обрезаются: env-файл легко приносит хвостовой перевод строки.
func TestNewsLLMConfigFromEnv(t *testing.T) {
	const (
		base  = "https://api.example"
		key   = "test-key"
		model = "test-model"
	)
	t.Run("default", func(t *testing.T) {
		t.Setenv(newsLLMProfileEnv, "")
		t.Setenv(newsLLMBaseURLEnv, base)
		t.Setenv(newsLLMAPIKeyEnv, key)
		t.Setenv(newsLLMModelEnv, model)
		cfg, ok := newsLLMConfigFromEnv()
		if !ok {
			t.Fatal("ok = false, want true при полном default-профиле")
		}
		if cfg.BaseURL != base || cfg.APIKey != key || cfg.Model != model || cfg.API != "anthropic" {
			t.Errorf("default-конфигурация = %+v, want base/key/model и api=anthropic", cfg)
		}
	})
	t.Run("glm", func(t *testing.T) {
		t.Setenv(newsLLMProfileEnv, "glm")
		t.Setenv(newsLLMGLMBaseURLEnv, base)
		t.Setenv(newsLLMGLMAPIKeyEnv, key)
		t.Setenv(newsLLMGLMModelEnv, model)
		cfg, ok := newsLLMConfigFromEnv()
		if !ok {
			t.Fatal("ok = false, want true при полном glm-профиле")
		}
		if cfg.BaseURL != base || cfg.APIKey != key || cfg.Model != model || cfg.API != "openai" {
			t.Errorf("glm-конфигурация = %+v, want base/key/model и api=openai", cfg)
		}
	})
	t.Run("glm без ключа", func(t *testing.T) {
		t.Setenv(newsLLMProfileEnv, "glm")
		t.Setenv(newsLLMGLMBaseURLEnv, base)
		t.Setenv(newsLLMGLMAPIKeyEnv, "")
		t.Setenv(newsLLMGLMModelEnv, model)
		if _, ok := newsLLMConfigFromEnv(); ok {
			t.Fatal("ok = true при пустом GLM_LLM_API_KEY, want false")
		}
	})
	t.Run("неизвестный профиль", func(t *testing.T) {
		t.Setenv(newsLLMProfileEnv, "mimo")
		if _, ok := newsLLMConfigFromEnv(); ok {
			t.Fatal("ok = true при неизвестном NEWS_LLM_PROFILE, want false")
		}
	})
	t.Run("окружающие пробелы обрезаются", func(t *testing.T) {
		t.Setenv(newsLLMProfileEnv, " default ")
		t.Setenv(newsLLMBaseURLEnv, "  "+base+"\n")
		t.Setenv(newsLLMAPIKeyEnv, "\t"+key)
		t.Setenv(newsLLMModelEnv, model+" ")
		cfg, ok := newsLLMConfigFromEnv()
		if !ok {
			t.Fatal("ok = false, want true при непустых значениях в пробелах")
		}
		if cfg.BaseURL != base || cfg.APIKey != key || cfg.Model != model || cfg.API != "anthropic" {
			t.Errorf("конфигурация = %+v, want (%q, %q, %q, anthropic)", cfg, base, key, model)
		}
	})
}

// TestNewsLLMBacklogLimitFromEnv — NEWS_LLM_BACKLOG_LIMIT: пусто — штатные 50, валидное
// значение — как есть, а 0/−1/1001/нечисло — warn и дефолт. Опечатка в переменной бэкфилла
// не должна молча давать бюджет другого размера или LIMIT 0 (пустой кандидатный запрос).
func TestNewsLLMBacklogLimitFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want int
	}{
		{"пусто — штатные 50", "", 50},
		{"валидное значение", "120", 120},
		{"верхняя граница", "1000", 1000},
		{"ноль", "0", 50},
		{"отрицательное", "-1", 50},
		{"выше предела", "1001", 50},
		{"нечисло", "abc", 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(newsLLMBacklogLimitEnv, tc.raw)
			if got := newsLLMBacklogLimitFromEnv(); got != tc.want {
				t.Errorf("newsLLMBacklogLimitFromEnv() = %d при %s=%q, want %d",
					got, newsLLMBacklogLimitEnv, tc.raw, tc.want)
			}
		})
	}
}

// TestNewsPromptVersionAndRegistry — system-промпт несёт метку редакции и все слаги реестра:
// смена newsPromptVersion или реестра обязана отражаться в запросе, иначе модель размечает
// по устаревшей инструкции, а данные получают старый prompt_version.
func TestNewsPromptVersionAndRegistry(t *testing.T) {
	prompt := newsLLMSystemPrompt()
	const wantRevision = "Редакция инструкции: 2."
	if !strings.Contains(prompt, wantRevision) {
		t.Fatalf("system-промпт не содержит метку редакции %q", wantRevision)
	}
	for _, tag := range newsTags {
		slug := tag.Dimension + ":" + tag.Slug
		if !strings.Contains(prompt, slug) {
			t.Errorf("system-промпт не содержит слаг %q", slug)
		}
	}
	for _, want := range []string{
		`"messages"`,
		`"sentiment"`,
		`"relevance"`,
		`"evidence"`,
		"не более 5 тегов",
		"дословная подстрока",
		// Редакция 2: роль расширена на RSS-посты и аналитику, а правила закрывают
		// длинные посты smart-lab, пустой description, общие новости и лимит цитаты.
		"новости и аналитические посты",
		"не длиннее 300 символов",
		"у длинного поста — одно короткое предложение резюме",
		"если text пуст, опирайся на title",
		"спорт, происшествия, культура",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("system-промпт не содержит %q", want)
		}
	}
}

// TestNewsLLMAskOnceOK — валидный 200: запрос уходит на /v1/messages с ключом в заголовке
// x-api-key, max_tokens и temperature=0, system-промпт отдельным полем; ответ разобран.
func TestNewsLLMAskOnceOK(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	payloadBody := newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL))
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestContentBody(t, payloadBody)})

	payload, err := newsLLMAskOnce(context.Background(),
		newsLLMConfig{BaseURL: mock.url + "/", APIKey: "test-key", Model: "test-model"}, inputs)
	if err != nil {
		t.Fatalf("newsLLMAskOnce: %v", err)
	}
	if mock.calls() != 1 {
		t.Fatalf("запросов %d, want 1 (без ретрая при валидном ответе)", mock.calls())
	}
	req := mock.request(0)
	if req.Method != http.MethodPost || req.Path != "/v1/messages" {
		t.Errorf("запрос = %s %s, want POST /v1/messages (хвостовой слэш baseURL не должен давать //v1)", req.Method, req.Path)
	}
	if req.APIKey != "test-key" {
		t.Errorf("x-api-key = %q, want %q", req.APIKey, "test-key")
	}
	if req.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", req.ContentType)
	}
	for _, want := range []string{
		`"model":"test-model"`,
		`"max_tokens":4096`,
		`"temperature":0`,
		`"system":[{"text":"Редакция инструкции: 2.`,
		`"role":"user"`,
		inputs[0].URL,
		inputs[0].Description,
		"=== Сообщение 1 ===",
	} {
		if !strings.Contains(req.Body, want) {
			t.Errorf("в теле запроса нет %q", want)
		}
	}
	if len(payload.Messages) != 1 || payload.Messages[0].URL != inputs[0].URL {
		t.Errorf("payload = %+v, want одно сообщение %s", payload, inputs[0].URL)
	}
}

// TestNewsLLMAskOnceOpenAI — профиль glm: запрос уходит на /v1/chat/completions с
// Authorization: Bearer, response_format=json_object и reasoning_effort=none; ответ читается
// из choices[0].message.content. Это транспорт, которым GLM размечивал резюме в сравнении 1.8.
func TestNewsLLMAskOnceOpenAI(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	payloadBody := newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL))
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestOpenAIBody(t, payloadBody)})

	payload, err := newsLLMAskOnce(context.Background(), newsLLMConfig{
		BaseURL: mock.url, APIKey: "test-key", Model: "zai-org/GLM-5.3-Flash", API: "openai",
	}, inputs)
	if err != nil {
		t.Fatalf("newsLLMAskOnce(openai): %v", err)
	}
	if mock.calls() != 1 {
		t.Fatalf("запросов %d, want 1 (без ретрая при валидном ответе)", mock.calls())
	}
	req := mock.request(0)
	if req.Method != http.MethodPost || req.Path != "/v1/chat/completions" {
		t.Errorf("запрос = %s %s, want POST /v1/chat/completions", req.Method, req.Path)
	}
	if req.Auth != "Bearer test-key" {
		t.Errorf("Authorization = %q, want %q", req.Auth, "Bearer test-key")
	}
	if req.APIKey != "" {
		t.Errorf("x-api-key = %q, want пусто (OpenAI-путь шлёт ключ в Authorization)", req.APIKey)
	}
	for _, want := range []string{
		`"model":"zai-org/GLM-5.3-Flash"`,
		`"max_tokens":8192`,
		`"temperature":0`,
		`"response_format":{"type":"json_object"}`,
		`"reasoning_effort":"none"`,
		`"role":"system"`,
		`"role":"user"`,
		"Редакция инструкции: 2.",
		inputs[0].URL,
		inputs[0].Description,
	} {
		if !strings.Contains(req.Body, want) {
			t.Errorf("в теле OpenAI-запроса нет %q", want)
		}
	}
	if len(payload.Messages) != 1 || payload.Messages[0].URL != inputs[0].URL {
		t.Errorf("payload = %+v, want одно сообщение %s", payload, inputs[0].URL)
	}
}

// TestNewsLLMOpenAIURL — путь OpenAI-транспорта из base профиля: корень гейтвея, хвостовой
// /v1 (не удваивать) и Anthropic-префикс /anthropic (снять).
func TestNewsLLMOpenAIURL(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"https://gate.joingonka.ai", "https://gate.joingonka.ai/v1/chat/completions"},
		{"https://gate.joingonka.ai/", "https://gate.joingonka.ai/v1/chat/completions"},
		{"https://gate.joingonka.ai/v1", "https://gate.joingonka.ai/v1/chat/completions"},
		{"https://api.example/anthropic", "https://api.example/v1/chat/completions"},
	} {
		if got := newsLLMOpenAIURL(tc.base); got != tc.want {
			t.Errorf("newsLLMOpenAIURL(%q) = %q, want %q", tc.base, got, tc.want)
		}
	}
}

// TestNewsLLMAskOnceOpenAIErrors — отказ провайдера не ретраится, а пустой content и битый
// конверт — ошибки формата (их повторяет newsLLMAsk, а не newsLLMAskOnce).
func TestNewsLLMAskOnceOpenAIErrors(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	cfg := func(url string) newsLLMConfig {
		return newsLLMConfig{BaseURL: url, APIKey: "test-key", Model: "glm-test", API: "openai"}
	}

	t.Run("500", func(t *testing.T) {
		mock := newNewsTestLLMMock(t, newsTestLLMResponse{Status: http.StatusInternalServerError, Body: `{"error":"boom"}`})
		_, err := newsLLMAskOnce(context.Background(), cfg(mock.url), inputs)
		if err == nil {
			t.Fatal("newsLLMAskOnce(openai, 500) вернул nil-ошибку")
		}
		if newsIsFormatError(err) {
			t.Errorf("500 классифицирован как формат: %v (ретрай не нужен)", err)
		}
		if mock.calls() != 1 {
			t.Errorf("запросов %d, want 1 (500 не ретраится)", mock.calls())
		}
	})
	t.Run("пустой content", func(t *testing.T) {
		mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: `{"choices":[{"message":{"content":""}}]}`})
		_, err := newsLLMAskOnce(context.Background(), cfg(mock.url), inputs)
		if !newsIsFormatError(err) {
			t.Fatalf("пустой content: err = %v, want ошибку формата", err)
		}
	})
	t.Run("битый конверт", func(t *testing.T) {
		mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: `{not-json`})
		_, err := newsLLMAskOnce(context.Background(), cfg(mock.url), inputs)
		if !newsIsFormatError(err) {
			t.Fatalf("битый конверт: err = %v, want ошибку формата", err)
		}
	})
}

// TestNewsLLMAskOpenAIRetry — ошибка формата OpenAI-пути лечится одним повтором: первый ответ
// с пустым content, второй валидный; newsLLMAsk делает два вызова и возвращает разметку.
func TestNewsLLMAskOpenAIRetry(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	payloadBody := newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL))
	mock := newNewsTestLLMMock(t,
		newsTestLLMResponse{Body: `{"choices":[{"message":{"content":""}}]}`},
		newsTestLLMResponse{Body: newsTestOpenAIBody(t, payloadBody)})

	payload, err := newsLLMAsk(context.Background(), newsLLMConfig{
		BaseURL: mock.url, APIKey: "test-key", Model: "glm-test", API: "openai",
	}, inputs)
	if err != nil {
		t.Fatalf("newsLLMAsk(openai): %v", err)
	}
	if mock.calls() != 2 {
		t.Errorf("запросов %d, want 2 (пустой content + повтор)", mock.calls())
	}
	if len(payload.Messages) != 1 || payload.Messages[0].URL != inputs[0].URL {
		t.Errorf("payload = %+v, want одно сообщение %s", payload, inputs[0].URL)
	}
}

// TestNewsLLMFencedJSON — модель обрамила JSON ```-блоком и сопроводила фразой: ответ
// разбирается, а не трактуется как ошибка формата.
func TestNewsLLMFencedJSON(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	payloadBody := newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL))
	content := "```json\n" + payloadBody + "\n```\nНадеюсь, разметка полезна."
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestContentBody(t, content)})

	payload, err := newsLLMAskOnce(context.Background(),
		newsLLMConfig{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"}, inputs)
	if err != nil {
		t.Fatalf("newsLLMAskOnce: %v", err)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, want 1 (формат валиден, ретрай не нужен)", mock.calls())
	}
	if len(payload.Messages) != 1 {
		t.Errorf("payload = %+v, want одно сообщение", payload)
	}
}

// TestNewsLLMProviderError500 — отказ провайдера возвращается сразу, без ретрая: повтором
// он не лечится, а импортёр по ошибке предупреждает и догоняет разметку следующим прогоном.
// Счётчик запросов пинит, что SDK-повторы выключены. Ключ в текст ошибки не попадает, тело
// провайдера обрезается фрагментом.
func TestNewsLLMProviderError500(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	body := `{"error":{"type":"api_error","message":"` + strings.Repeat("провайдер упал; ", 300) + `"}}`
	mock := newNewsTestLLMMock(t, newsTestLLMResponse{Status: http.StatusInternalServerError, Body: body})

	_, err := newsLLMAsk(context.Background(),
		newsLLMConfig{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"}, inputs)
	if err == nil {
		t.Fatal("500: ожидалась ошибка")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("ошибка %v не содержит код 500", err)
	}
	if !strings.Contains(err.Error(), "провайдер упал") {
		t.Errorf("ошибка %v не содержит узнаваемого начала тела ответа", err)
	}
	if got := len(err.Error()); got > 1024 {
		t.Errorf("текст ошибки %d байт — тело провайдера не обрезано: %v", got, err)
	}
	if strings.Contains(err.Error(), "test-key") {
		t.Errorf("в текст ошибки утёк ключ API: %v", err)
	}
	if newsIsFormatError(err) {
		t.Errorf("отказ провайдера классифицирован как ошибка формата: %v", err)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, want 1 (отказ провайдера не повторяется)", mock.calls())
	}
}

// TestNewsLLMEmptyAnswer — ответ без text-блока и ответ с пустым messages — ошибки формата:
// doc-строк не будет ни на одно сообщение, поэтому newsLLMAsk делает один повтор.
func TestNewsLLMEmptyAnswer(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	cfg := func(m *newsTestLLMMock) newsLLMConfig {
		return newsLLMConfig{BaseURL: m.url, APIKey: "test-key", Model: "test-model"}
	}

	t.Run("нет text-блока", func(t *testing.T) {
		mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestEmptyContentBody(t)})
		_, err := newsLLMAskOnce(context.Background(), cfg(mock), inputs)
		if err == nil {
			t.Fatal("ответ без text-блока: ожидалась ошибка")
		}
		if !newsIsFormatError(err) {
			t.Errorf("ошибка %v не классифицирована как ошибка формата", err)
		}
		if mock.calls() != 1 {
			t.Errorf("запросов %d, want 1 (ретрай решает newsLLMAsk, а не newsLLMAskOnce)", mock.calls())
		}
	})

	t.Run("пустой messages", func(t *testing.T) {
		mock := newNewsTestLLMMock(t, newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t))})
		_, err := newsLLMAskOnce(context.Background(), cfg(mock), inputs)
		if err == nil {
			t.Fatal("пустой messages: ожидалась ошибка формата")
		}
		if !newsIsFormatError(err) {
			t.Errorf("ошибка %v не классифицирована как ошибка формата", err)
		}
	})

	t.Run("ретрай лечит пустой ответ", func(t *testing.T) {
		valid := newsTestContentBody(t, newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL)))
		mock := newNewsTestLLMMock(t,
			newsTestLLMResponse{Body: newsTestEmptyContentBody(t)},
			newsTestLLMResponse{Body: valid})
		payload, err := newsLLMAsk(context.Background(), cfg(mock), inputs)
		if err != nil {
			t.Fatalf("newsLLMAsk: %v (ретрай должен был вылечить формат)", err)
		}
		if mock.calls() != 2 {
			t.Errorf("запросов %d, want 2 (первый + ретрай)", mock.calls())
		}
		if len(payload.Messages) != 1 {
			t.Errorf("payload = %+v, want одно сообщение после ретрая", payload)
		}
	})
}

// TestNewsLLMBrokenJSONRetry — обрезанный content лечится одним повтором формата; два битых
// ответа подряд дают ошибку и третьего запроса не делают.
func TestNewsLLMBrokenJSONRetry(t *testing.T) {
	inputs := newsTestLLMInputs(1)
	cfg := func(m *newsTestLLMMock) newsLLMConfig {
		return newsLLMConfig{BaseURL: m.url, APIKey: "test-key", Model: "test-model"}
	}
	const truncated = `{"messages":[{"url":"https://t.me/interfaxonline/1","summary":"Трамп`
	valid := newsTestContentBody(t, newsTestPayloadBody(t, newsTestValidMessage(inputs[0].URL)))

	t.Run("один ретрай вылечивает", func(t *testing.T) {
		mock := newNewsTestLLMMock(t,
			newsTestLLMResponse{Body: newsTestContentBody(t, truncated)},
			newsTestLLMResponse{Body: valid})
		payload, err := newsLLMAsk(context.Background(), cfg(mock), inputs)
		if err != nil {
			t.Fatalf("newsLLMAsk: %v (ретрай должен был вылечить формат)", err)
		}
		if mock.calls() != 2 {
			t.Errorf("запросов %d, want 2 (первый + ретрай)", mock.calls())
		}
		if len(payload.Messages) != 1 {
			t.Errorf("payload = %+v, want одно сообщение", payload)
		}
		if first, second := mock.request(0).Body, mock.request(1).Body; first != second {
			t.Error("ретрай отправил другое тело запроса: промпт должен быть тем же")
		}
	})

	t.Run("два битых ответа — ошибка", func(t *testing.T) {
		mock := newNewsTestLLMMock(t,
			newsTestLLMResponse{Body: newsTestContentBody(t, truncated)},
			newsTestLLMResponse{Body: newsTestContentBody(t, truncated)})
		_, err := newsLLMAsk(context.Background(), cfg(mock), inputs)
		if err == nil {
			t.Fatal("два битых ответа: ожидалась ошибка")
		}
		if !newsIsFormatError(err) {
			t.Errorf("финальная ошибка %v не является ошибкой формата", err)
		}
		if mock.calls() != 2 {
			t.Errorf("запросов %d, want 2 (больше одного ретрая не делаем)", mock.calls())
		}
	})
}

// TestNewsLLMTimeout — зависший провайдер падает по дедлайну (newsLLMTimeout, короткий
// дедлайн подставлен тестом), а не ждёт вечно; ошибка дедлайна — не формат, повтора нет.
func TestNewsLLMTimeout(t *testing.T) {
	const handlerDelay = 5 * time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Тело вычитывается до конца до select: на отмену контекста хендлер завершается
		// сразу и srv.Close() не ждёт полный handlerDelay.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(handlerDelay):
		}
	}))
	defer srv.Close()

	old := newsLLMTimeout
	newsLLMTimeout = time.Second
	t.Cleanup(func() { newsLLMTimeout = old })

	_, err := tagNewsLLM(context.Background(),
		newsLLMConfig{BaseURL: srv.URL, APIKey: "test-key", Model: "test-model"}, newsTestLLMInputs(1))
	if err == nil {
		t.Fatal("зависший провайдер: ожидалась ошибка дедлайна")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не является ошибкой дедлайна (context.DeadlineExceeded)", err)
	}
	if strings.Contains(err.Error(), "test-key") {
		t.Errorf("в текст ошибки утёк ключ API: %v", err)
	}
}

// TestTagNewsLLMBatches — батчи по 10: 11 сообщений дают два запроса, и второе
// сообщение-хвост уходит отдельным батчем, а не теряется. Валидация даёт doc и tag на
// каждое вернувшееся сообщение.
func TestTagNewsLLMBatches(t *testing.T) {
	inputs := newsTestLLMInputs(11)
	first := make([]newsAnnotationMessage, 0, newsLLMBatchSize)
	for _, it := range inputs[:newsLLMBatchSize] {
		first = append(first, newsTestValidMessage(it.URL))
	}
	second := []newsAnnotationMessage{newsTestValidMessage(inputs[newsLLMBatchSize].URL)}
	mock := newNewsTestLLMMock(t,
		newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t, first...))},
		newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t, second...))})

	rows, err := tagNewsLLM(context.Background(),
		newsLLMConfig{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"}, inputs)
	if err != nil {
		t.Fatalf("tagNewsLLM: %v", err)
	}
	if mock.calls() != 2 {
		t.Fatalf("запросов %d, want 2 (10 + 1)", mock.calls())
	}
	if docs := len(newsAnnotationKindRows(rows, newsKindDoc)); docs != len(inputs) {
		t.Errorf("doc-строк %d, want %d", docs, len(inputs))
	}
	if tags := len(newsAnnotationKindRows(rows, newsKindTag)); tags != len(inputs) {
		t.Errorf("tag-строк %d, want %d (по одному тегу на сообщение)", tags, len(inputs))
	}
	if body := mock.request(1).Body; !strings.Contains(body, inputs[newsLLMBatchSize].URL) {
		t.Errorf("во втором батче нет URL хвостового сообщения %s", inputs[newsLLMBatchSize].URL)
	}
}

// TestTagNewsLLMPartialRowsOnLaterBatchError — ошибка второго батча не выбрасывает
// строки первого: tagNewsLLM возвращает накопленное вместе с ошибкой, и вызывающий
// пишет успешные батчи (иначе за них пришлось бы платить повторно).
func TestTagNewsLLMPartialRowsOnLaterBatchError(t *testing.T) {
	inputs := newsTestLLMInputs(11)
	first := make([]newsAnnotationMessage, 0, newsLLMBatchSize)
	for _, it := range inputs[:newsLLMBatchSize] {
		first = append(first, newsTestValidMessage(it.URL))
	}
	mock := newNewsTestLLMMock(t,
		newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t, first...))},
		newsTestLLMResponse{Status: http.StatusInternalServerError, Body: `{"error":"boom"}`})

	rows, err := tagNewsLLM(context.Background(),
		newsLLMConfig{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"}, inputs)
	if err == nil {
		t.Fatal("tagNewsLLM при отказе второго батча вернул nil-ошибку")
	}
	if got := len(newsAnnotationKindRows(rows, newsKindDoc)); got != newsLLMBatchSize {
		t.Errorf("doc-строк %d, want %d (успешный первый батч не должен теряться)", got, newsLLMBatchSize)
	}
	if got := len(newsAnnotationKindRows(rows, newsKindTag)); got != newsLLMBatchSize {
		t.Errorf("tag-строк %d, want %d (успешный первый батч не должен теряться)", got, newsLLMBatchSize)
	}
	if mock.calls() != 2 {
		t.Errorf("запросов %d, want 2 (первый батч + отказ второго; третий батч не шлётся)", mock.calls())
	}
}

// TestNewsLLMBatches — деление на батчи по двум пределам: 10 сообщений и 40 КБ текста.
// 11 коротких дают [10,1] (хвост не теряется); три по 30 КБ вдвоём не влезают (60 КБ > 40 КБ)
// и идут по одному; пост больше бюджета — батч из одного элемента, а не потеря и не зацикливание.
func TestNewsLLMBatches(t *testing.T) {
	sizes := func(batches [][]newsItem) []int {
		out := make([]int, 0, len(batches))
		for _, batch := range batches {
			out = append(out, len(batch))
		}
		return out
	}
	check := func(name string, inputs []newsItem, want []int) {
		t.Helper()
		got := sizes(newsLLMBatches(inputs))
		if len(got) != len(want) {
			t.Fatalf("%s: батчи %v, want %v", name, got, want)
		}
		total := 0
		for i := range want {
			total += got[i]
			if got[i] != want[i] {
				t.Errorf("%s: батчи %v, want %v", name, got, want)
				return
			}
		}
		if total != len(inputs) {
			t.Errorf("%s: в батчах %d сообщений, want %d (ни одно не потеряно)", name, total, len(inputs))
		}
	}

	check("11 коротких", newsTestLLMInputs(11), []int{10, 1})

	long := make([]newsItem, 3)
	for i := range long {
		long[i] = newsItem{Title: "Длинный пост", Description: strings.Repeat("я", 30_000)}
	}
	check("три по 30 КБ", long, []int{1, 1, 1})

	check("один 100 КБ", []newsItem{{Title: "Сверхбюджетный пост", Description: strings.Repeat("я", 100_000)}}, []int{1})
}
