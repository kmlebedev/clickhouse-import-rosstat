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

// TestNewsLLMConfigFromEnv — конфигурация LLM-разметки: все три переменные заполнены → ok;
// любая пустая → ok=false без ошибки (импорт деградирует, догон следующим прогоном).
// Окружающие пробелы обрезаются: env-файл легко приносит хвостовой перевод строки.
func TestNewsLLMConfigFromEnv(t *testing.T) {
	const (
		base  = "https://api.example"
		key   = "test-key"
		model = "test-model"
	)
	cases := []struct {
		name             string
		base, key, model string
		ok               bool
	}{
		{"все переменные заполнены", base, key, model, true},
		{"нет LLM_BASE_URL", "", key, model, false},
		{"нет LLM_API_KEY", base, "", model, false},
		{"нет NEWS_LLM_MODEL", base, key, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(newsLLMBaseURLEnv, tc.base)
			t.Setenv(newsLLMAPIKeyEnv, tc.key)
			t.Setenv(newsLLMModelEnv, tc.model)
			gotBase, gotKey, gotModel, ok := newsLLMConfigFromEnv()
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !tc.ok {
				return
			}
			if gotBase != base || gotKey != key || gotModel != model {
				t.Errorf("конфигурация = (%q, %q, %q), want (%q, %q, %q)",
					gotBase, gotKey, gotModel, base, key, model)
			}
		})
	}
	t.Run("окружающие пробелы обрезаются", func(t *testing.T) {
		t.Setenv(newsLLMBaseURLEnv, "  "+base+"\n")
		t.Setenv(newsLLMAPIKeyEnv, "\t"+key)
		t.Setenv(newsLLMModelEnv, model+" ")
		gotBase, gotKey, gotModel, ok := newsLLMConfigFromEnv()
		if !ok {
			t.Fatal("ok = false, want true при непустых значениях в пробелах")
		}
		if gotBase != base || gotKey != key || gotModel != model {
			t.Errorf("конфигурация = (%q, %q, %q), want (%q, %q, %q)",
				gotBase, gotKey, gotModel, base, key, model)
		}
	})
}

// TestNewsPromptVersionAndRegistry — system-промпт несёт метку редакции и все слаги реестра:
// смена newsPromptVersion или реестра обязана отражаться в запросе, иначе модель размечает
// по устаревшей инструкции, а данные получают старый prompt_version.
func TestNewsPromptVersionAndRegistry(t *testing.T) {
	prompt := newsLLMSystemPrompt()
	const wantRevision = "Редакция инструкции: 1."
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
		`"system":[{"text":"Редакция инструкции: 1.`,
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

	_, err := tagTelegramLLM(context.Background(),
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

// TestTagTelegramLLMBatches — батчи по 10: 11 сообщений дают два запроса, и второе
// сообщение-хвост уходит отдельным батчем, а не теряется. Валидация даёт doc и tag на
// каждое вернувшееся сообщение.
func TestTagTelegramLLMBatches(t *testing.T) {
	inputs := newsTestLLMInputs(11)
	first := make([]newsAnnotationMessage, 0, newsLLMBatchSize)
	for _, it := range inputs[:newsLLMBatchSize] {
		first = append(first, newsTestValidMessage(it.URL))
	}
	second := []newsAnnotationMessage{newsTestValidMessage(inputs[newsLLMBatchSize].URL)}
	mock := newNewsTestLLMMock(t,
		newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t, first...))},
		newsTestLLMResponse{Body: newsTestContentBody(t, newsTestPayloadBody(t, second...))})

	rows, err := tagTelegramLLM(context.Background(),
		newsLLMConfig{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"}, inputs)
	if err != nil {
		t.Fatalf("tagTelegramLLM: %v", err)
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
