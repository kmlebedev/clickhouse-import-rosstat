package news

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// newsLLMProfileEnv — профиль LLM-разметки. "default" (или пусто) — прежний путь:
	// LLM_BASE_URL/LLM_API_KEY/NEWS_LLM_MODEL и Anthropic SDK. "glm" — GLM_LLM_* и
	// OpenAI-совместимый /v1/chat/completions: у GLM Anthropic-эндпоинт не отдаёт JSON
	// (в сравнении 2026-10-17 он вернул reasoning в text-блоке), а OpenAI-режим с
	// response_format=json_object и reasoning_effort=none работает.
	newsLLMProfileEnv     = "NEWS_LLM_PROFILE"
	newsLLMProfileDefault = "default"
	newsLLMProfileGLM     = "glm"

	newsLLMBaseURLEnv = "LLM_BASE_URL"
	newsLLMAPIKeyEnv  = "LLM_API_KEY"
	newsLLMModelEnv   = "NEWS_LLM_MODEL"

	newsLLMGLMBaseURLEnv = "GLM_LLM_BASE_URL"
	newsLLMGLMAPIKeyEnv  = "GLM_LLM_API_KEY"
	newsLLMGLMModelEnv   = "GLM_LLM_MODEL"

	// newsLLMBatchSize — сообщений в одном запросе к провайдеру: 10 коротких молний
	// укладываются в контекст и дают приемлемую цену за прогон.
	newsLLMBatchSize = 10
	// newsLLMBacklogLimit — предел кандидатов за прогон (LIMIT в newsAnnotationCandidatesQuery):
	// 50 сообщений = 5 батчей, стоимость LLM ограничена даже после долгого простоя.
	newsLLMBacklogLimit = 50
	// newsLLMMaxTokens — предел вывода Anthropic-пути: запас на 10 сообщений с резюме и цитатами.
	// В Anthropic Messages API max_tokens обязателен — без него запрос отвергается.
	newsLLMMaxTokens = 4096
	// newsLLMOpenAIMaxTokens — предел вывода OpenAI-пути (GLM): в сравнении 1.8 рабочим был
	// 8192; 10 сообщений с резюме и до пяти цитат в 4096 не гарантированно влезают.
	newsLLMOpenAIMaxTokens = 8192
)

// newsLLMTimeout — дедлайн одного запроса к Anthropic-провайдеру. Переменная, а не константа:
// тест подставляет короткий дедлайн вместо минутного. Зависший провайдер обязан дать ошибку, по
// которой импортёр предупреждает и догоняет разметку следующим прогоном, а не подвесить шаг dagu.
var newsLLMTimeout = 65 * time.Second

// newsLLMOpenAITimeout — дедлайн OpenAI-пути (GLM). В сравнении 1.8 GLM отвечал ~111 с на
// документ; 300 с дают запас на батч из 10 сообщений, не подвешивая шаг dagu.
var newsLLMOpenAITimeout = 300 * time.Second

// newsLLMConfig — конфигурация одного клиента провайдера. Значения приходят только из
// os.Getenv у вызывающего; структура не печатается, поэтому ключ не утекает.
type newsLLMConfig struct {
	BaseURL string
	APIKey  string
	Model   string
	API     string // "anthropic" (пусто) или "openai"
}

// newsLLMConfigFromEnv читает конфигурацию LLM-разметки по профилю. Любая пустая переменная →
// ok = false: это не ошибка, а основание для деградации — импорт зелёный, разметка догоняется
// следующим прогоном, когда владелец дополнит env. Окружающие пробелы обрезаются: env-файл
// приносит хвостовой перевод строки, а лишний пробел в baseURL — молча битый URL.
//
// Профиль default использует общие LLM_* (как cbr_mp_summary) и Anthropic SDK; профиль glm —
// GLM_LLM_* и OpenAI-совместимый путь. Неизвестный профиль — warn с именем переменной и
// деградация, а не молчаливый default: иначе опечатка в NEWS_LLM_PROFILE разметила бы не той
// моделью/транспортом, чем ожидал владелец.
func newsLLMConfigFromEnv() (newsLLMConfig, bool) {
	profile := strings.ToLower(strings.TrimSpace(os.Getenv(newsLLMProfileEnv)))
	if profile == "" {
		profile = newsLLMProfileDefault
	}
	var baseEnv, keyEnv, modelEnv, api string
	switch profile {
	case newsLLMProfileDefault:
		baseEnv, keyEnv, modelEnv, api = newsLLMBaseURLEnv, newsLLMAPIKeyEnv, newsLLMModelEnv, "anthropic"
	case newsLLMProfileGLM:
		baseEnv, keyEnv, modelEnv, api = newsLLMGLMBaseURLEnv, newsLLMGLMAPIKeyEnv, newsLLMGLMModelEnv, "openai"
	default:
		log.Warnf("news_watch: неизвестный %s=%q (default|glm), LLM-разметка пропущена", newsLLMProfileEnv, profile)
		return newsLLMConfig{}, false
	}

	cfg := newsLLMConfig{
		BaseURL: strings.TrimSpace(os.Getenv(baseEnv)),
		APIKey:  strings.TrimSpace(os.Getenv(keyEnv)),
		Model:   strings.TrimSpace(os.Getenv(modelEnv)),
		API:     api,
	}
	var missing []string
	if cfg.BaseURL == "" {
		missing = append(missing, baseEnv)
	}
	if cfg.APIKey == "" {
		missing = append(missing, keyEnv)
	}
	if cfg.Model == "" {
		missing = append(missing, modelEnv)
	}
	if len(missing) > 0 {
		// Только имена переменных: значения ключа в лог не попадают.
		log.Warnf("news_watch: LLM-разметка пропущена: не заданы %s", strings.Join(missing, ", "))
		return newsLLMConfig{}, false
	}
	return cfg, true
}

// Транспорт и классификация ошибок ниже сознательно дублируют cbr/mp_summary_llm.go: это
// третий потребитель Anthropic-совместимого API; общая обвязка — кандидат на вынос в util,
// но cbr в этом PR не рефакторится.

// newsLLMFormatError — ошибка формата ответа модели: конверт не разобран, в ответе нет
// text-блока, content пуст, не JSON, не та схема или в ответе нет ни одного сообщения.
// Именно её имеет смысл повторить один раз: разовый сбой генерации лечится вторым запросом.
// Не формат (и потому без повтора): отказ провайдера (*anthropic.Error, не-2xx) и ошибки до
// чтения тела — транспорт и дедлайн контекста; о них вызывающий должен узнать сразу, чтобы
// деградировать, а не тратить второй вызов.
type newsLLMFormatError struct{ err error }

func (e newsLLMFormatError) Error() string { return e.err.Error() }
func (e newsLLMFormatError) Unwrap() error { return e.err }

// newsIsFormatError — ошибка формата (в том числе обёрнутая).
func newsIsFormatError(err error) bool {
	var fe newsLLMFormatError
	return errors.As(err, &fe)
}

// newsIsJSONDecodeError — ответ не получен целиком или не разобран. Классификация повторяет
// cbr/mp_summary_llm.go: обрыв тела и обрезанный JSON приходят одним io.ErrUnexpectedEOF,
// поэтому оба случая — формат (цена — один повтор) и различать их по тексту ошибки SDK не нужно.
func newsIsJSONDecodeError(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr) || errors.Is(err, io.ErrUnexpectedEOF)
}

// newsLLMSystemPrompt — system-сообщение: метка редакции (newsPromptVersion), роль, реестр
// тегов из newsTags (единственный источник правды) и строгая JSON-схема. Схема повторена
// дословно: её ждёт newsLLMAskOnce, а расхождение стоит сообщению разметки.
func newsLLMSystemPrompt() string {
	var b strings.Builder
	b.WriteString("Редакция инструкции: " + strconv.Itoa(newsPromptVersion) + ".\n")
	b.WriteString("Ты размечаешь короткие новости для контура ключевой ставки и DCF.\n")
	b.WriteString("Для каждого сообщения определи тональность, релевантность, краткое резюме и теги влияния на инфляцию.\n\n")
	b.WriteString("Реестр тегов (dimension:slug — расшифровка; у cb_channel в скобках статический знак канала):\n")
	for _, tag := range newsTags {
		b.WriteString("- " + tag.Dimension + ":" + tag.Slug + " — " + tag.Title)
		if tag.Direction != "" {
			b.WriteString(" (" + tag.Direction + ")")
		}
		b.WriteString("\n")
	}
	b.WriteString("\nВерни строго один JSON-объект, без текста и пояснений вокруг него:\n")
	b.WriteString(`{"messages":[{"url":"<url сообщения>","sentiment":<число -1..1>,"relevance":<число 0..1>,"summary":"<1 предложение>","tags":[{"dimension":"<person|country|sector|event|theme|cb_channel>","tag":"<slug из реестра>","direction":"<proinflationary|disinflationary|neutral|mixed>","confidence":<число 0..1>,"evidence":"<дословная подстрока текста>"}]}]}` + "\n")
	b.WriteString("\nПравила:\n")
	b.WriteString("- url копируй из запроса без изменений; на каждое сообщение запроса — один элемент messages;\n")
	b.WriteString("- используй только dimension и tag из реестра; тег вне реестра будет отброшен;\n")
	b.WriteString("- evidence — дословная подстрока текста сообщения, без пересказа, сокращений и многоточий; пустая или неточная цитата отбрасывает тег;\n")
	b.WriteString("- не более 5 тегов на сообщение; если подходящих тегов нет, верни пустой массив tags;\n")
	b.WriteString("- person, country и event тегируй только при действии, заявлении или влиянии, а не при случайном упоминании;\n")
	b.WriteString("- direction — влияние на инфляцию: proinflationary ускоряет рост цен, disinflationary замедляет, neutral/mixed — влияния нет или оно смешанное; у cb_channel направление — знак канала;\n")
	b.WriteString("- у person, country, sector и event допустимы только neutral или mixed; proinflationary и disinflationary — лишь у cb_channel;\n")
	b.WriteString("- sentiment — тональность новости от -1 до 1; relevance — важность для контура ставки и DCF от 0 до 1;\n")
	b.WriteString("- summary — одно короткое предложение по-русски, без выдумок и оценок сверх текста.\n")
	return b.String()
}

// newsLLMUserPrompt — user-сообщение: сообщения батча с url/date/source/title/text и явными
// разделителями. Модель копирует url в ответ, поэтому url идёт первой строкой блока.
func newsLLMUserPrompt(inputs []newsItem) string {
	var b strings.Builder
	b.WriteString("Разметь сообщения:\n")
	for i, it := range inputs {
		b.WriteString("\n=== Сообщение " + strconv.Itoa(i+1) + " ===\n")
		b.WriteString("url: " + it.URL + "\n")
		b.WriteString("date: " + it.Date.Format(time.RFC3339) + "\n")
		b.WriteString("source: " + it.Source + "\n")
		b.WriteString("title: " + it.Title + "\n")
		b.WriteString("text: " + it.Description + "\n")
	}
	return b.String()
}

// newsLLMText склеивает текстовые блоки ответа в порядке выдачи. Anthropic-конверт может нести
// и блоки других типов (thinking и т.п.): к разметке они отношения не имеют и пропускаются.
// Пустой результат — не ответ, а ошибка формата (её ловит newsLLMAskOnce).
func newsLLMText(msg *anthropic.Message) string {
	var b strings.Builder
	for _, block := range msg.Content {
		if block.Type != "text" {
			continue
		}
		b.WriteString(block.Text)
	}
	return b.String()
}

// newsLLMJSONBody вырезает JSON-объект из content ответа модели: модель может обрамить его
// ```-блоком или сопроводить фразой, и терять из-за этого разметку целого батча не нужно.
// Берём всё от первой «{» до последней «}». ok = false, если скобок нет: это ошибка формата,
// а не «разметки нет».
func newsLLMJSONBody(content string) (string, bool) {
	s := strings.TrimSpace(content)
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end < start {
		return "", false
	}
	return s[start : end+1], true
}

// newsLLMAskOnce — диспетчер транспорта: профиль glm идёт OpenAI-совместимым путём
// (/v1/chat/completions, response_format=json_object, reasoning_effort=none), профиль default —
// прежним Anthropic SDK. Ошибки формата и отказы провайдера классифицируются одинаково, поэтому
// верхний уровень (newsLLMAsk, newsLLMAskOnce) не зависит от транспорта.
func newsLLMAskOnce(ctx context.Context, cfg newsLLMConfig, inputs []newsItem) (newsAnnotationPayload, error) {
	if cfg.API == "openai" {
		return newsLLMAskOnceOpenAI(ctx, cfg, inputs)
	}
	return newsLLMAskOnceAnthropic(ctx, cfg, inputs)
}

// newsLLMAskOnceAnthropic — один запрос к провайдеру официальным SDK (Anthropic Messages API:
// POST <baseURL>/v1/messages) и разбор ответа до структуры. Ошибки формата (нет text-блока,
// пустой/неразобранный content, не та схема, пустой messages) оборачиваются newsLLMFormatError
// и повторяются newsLLMAsk; отказ провайдера возвращается компактной ошибкой
// «http status <код>: <фрагмент тела>», транспорт и дедлайн — как есть.
//
// Клиент строится на каждый вызов: конфигурация приходит аргументами, общий util.HttpClient
// сохраняет конвенцию транспорта проекта (национальные TLS-roots, cookie jar). baseURL — без
// «/v1»: SDK сам дописывает «v1/messages». Повторы SDK выключены (WithMaxRetries(0)): отказ
// провайдера означает догон следующим прогоном, а не скрытые ретраи внутри одного вызова.
// temperature = 0: разметка должна быть воспроизводимой при том же промпте.
func newsLLMAskOnceAnthropic(ctx context.Context, cfg newsLLMConfig, inputs []newsItem) (newsAnnotationPayload, error) {
	client := anthropic.NewClient(
		option.WithAPIKey(cfg.APIKey),
		option.WithBaseURL(strings.TrimRight(cfg.BaseURL, "/")),
		option.WithHTTPClient(util.HttpClient),
		option.WithMaxRetries(0),
	)
	reqCtx, cancel := context.WithTimeout(ctx, newsLLMTimeout)
	defer cancel()

	msg, err := client.Messages.New(reqCtx, anthropic.MessageNewParams{
		Model:     cfg.Model,
		MaxTokens: newsLLMMaxTokens,
		// System — отдельное поле Anthropic-конверта: системной роли в messages там нет.
		System: []anthropic.TextBlockParam{{Text: newsLLMSystemPrompt()}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(newsLLMUserPrompt(inputs))),
		},
		Temperature: anthropic.Float(0),
	})
	if err != nil {
		var apiErr *anthropic.Error
		if errors.As(err, &apiErr) {
			// Тело — из RawJSON(): сам Error печатает его целиком, а в лог ему место только
			// нарезкой. Ключа в теле нет — он едет заголовком x-api-key.
			return newsAnnotationPayload{}, fmt.Errorf("http status %d: %s",
				apiErr.StatusCode, util.ErrorSnippet([]byte(apiErr.RawJSON())))
		}
		if newsIsJSONDecodeError(err) {
			return newsAnnotationPayload{}, newsLLMFormatError{err}
		}
		return newsAnnotationPayload{}, err
	}

	content := newsLLMText(msg)
	if content == "" {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("в ответе нет text-блока")}
	}
	body, ok := newsLLMJSONBody(content)
	if !ok {
		return newsAnnotationPayload{}, newsLLMFormatError{
			fmt.Errorf("в content нет JSON-объекта (content: %d байт)", len(content)),
		}
	}
	var payload newsAnnotationPayload
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("content не разобран: %w", err)}
	}
	if len(inputs) > 0 && len(payload.Messages) == 0 {
		// Пустой messages при непустом батче — не «разметки нет», а потерянный ответ:
		// doc-строки не будет ни на одно сообщение, поэтому это формат и один повтор.
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("в ответе нет messages")}
	}
	return payload, nil
}

// Конверт OpenAI-совместимого /chat/completions: JSON-режим (response_format=json_object) и
// reasoning_effort=none — рабочий режим GLM в сравнении 1.8. GLM на Anthropic-эндпоинте вернул
// reasoning в text-блоке без JSON, поэтому профиль glm идёт этим путём.
type newsLLMOpenAIRequest struct {
	Model           string                      `json:"model"`
	Messages        []newsLLMOpenAIMessage      `json:"messages"`
	Temperature     float64                     `json:"temperature"`
	MaxTokens       int                         `json:"max_tokens"`
	ResponseFormat  newsLLMOpenAIResponseFormat `json:"response_format"`
	ReasoningEffort string                      `json:"reasoning_effort,omitempty"`
}

type newsLLMOpenAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type newsLLMOpenAIResponseFormat struct {
	Type string `json:"type"`
}

type newsLLMOpenAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// newsLLMOpenAIURL — OpenAI-совместимый путь из base профиля: у GLM base — корень гейтвея,
// у MiMo — Anthropic-префикс /anthropic; обоим нужен /v1/chat/completions. Хвостовой /v1
// не удваиваем.
func newsLLMOpenAIURL(baseURL string) string {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if strings.HasSuffix(base, "/v1") {
		return base + "/chat/completions"
	}
	base = strings.TrimSuffix(base, "/anthropic")
	return base + "/v1/chat/completions"
}

// newsLLMAskOnceOpenAI — один запрос OpenAI-совместимого транспорта (профиль glm): тот же
// system/user-промпт, temperature 0, response_format=json_object, reasoning_effort=none,
// max_tokens из newsLLMOpenAIMaxTokens. Ответ — choices[0].message.content; формат/пустой
// content — newsLLMFormatError (один повтор), не-2xx — компактная ошибка без повтора.
func newsLLMAskOnceOpenAI(ctx context.Context, cfg newsLLMConfig, inputs []newsItem) (newsAnnotationPayload, error) {
	body, err := json.Marshal(newsLLMOpenAIRequest{
		Model: cfg.Model,
		Messages: []newsLLMOpenAIMessage{
			{Role: "system", Content: newsLLMSystemPrompt()},
			{Role: "user", Content: newsLLMUserPrompt(inputs)},
		},
		Temperature:     0,
		MaxTokens:       newsLLMOpenAIMaxTokens,
		ResponseFormat:  newsLLMOpenAIResponseFormat{Type: "json_object"},
		ReasoningEffort: "none",
	})
	if err != nil {
		return newsAnnotationPayload{}, fmt.Errorf("сборка OpenAI-запроса: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, newsLLMOpenAITimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, newsLLMOpenAIURL(cfg.BaseURL), bytes.NewReader(body))
	if err != nil {
		return newsAnnotationPayload{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Ключ — заголовком Authorization, не в URL/query: в логи и ошибки не попадает.
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return newsAnnotationPayload{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return newsAnnotationPayload{}, newsLLMFormatError{err}
		}
		return newsAnnotationPayload{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return newsAnnotationPayload{}, fmt.Errorf("http status %d: %s", resp.StatusCode, util.ErrorSnippet(raw))
	}
	var envelope newsLLMOpenAIResponse
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("OpenAI-конверт не разобран: %w", err)}
	}
	if len(envelope.Choices) == 0 {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("в OpenAI-ответе нет choices")}
	}
	content := strings.TrimSpace(envelope.Choices[0].Message.Content)
	if content == "" {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("в choices[0].message.content пусто")}
	}
	jsonBody, ok := newsLLMJSONBody(content)
	if !ok {
		return newsAnnotationPayload{}, newsLLMFormatError{
			fmt.Errorf("в content нет JSON-объекта (content: %d байт)", len(content)),
		}
	}
	var payload newsAnnotationPayload
	if err := json.Unmarshal([]byte(jsonBody), &payload); err != nil {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("content не разобран: %w", err)}
	}
	if len(inputs) > 0 && len(payload.Messages) == 0 {
		return newsAnnotationPayload{}, newsLLMFormatError{fmt.Errorf("в ответе нет messages")}
	}
	return payload, nil
}

// newsLLMAsk — запрос с одним ретраем на ошибку формата. Второй подряд провал формата
// возвращается вызывающему: батч пропускается, догон — следующим прогоном.
func newsLLMAsk(ctx context.Context, cfg newsLLMConfig, inputs []newsItem) (newsAnnotationPayload, error) {
	payload, err := newsLLMAskOnce(ctx, cfg, inputs)
	if err == nil || !newsIsFormatError(err) {
		return payload, err
	}
	log.Warnf("news_watch: ответ LLM (%s) не разобран (%v), повтор запроса", cfg.Model, err)
	return newsLLMAskOnce(ctx, cfg, inputs)
}

// tagTelegramLLM размечает батч Telegram-сообщений: батчи по newsLLMBatchSize, на каждый —
// запрос с одним ретраем формата и валидация против входа. Ошибка провайдера/формата
// останавливает LLM-шаг на этот прогон, чтобы не долбить упавшего провайдера оставшимися
// батчами, но уже полученные строки возвращаются вместе с ошибкой: вызывающий пишет их,
// и следующий прогон не платит за них повторно (кандидатная выборка исключает размеченные
// пары). Ошибка остаётся ошибкой прогона для лога, но не отменяет запись сообщений/версий.
func tagTelegramLLM(ctx context.Context, cfg newsLLMConfig, inputs []newsItem) ([]newsAnnotationRow, error) {
	now := time.Now()
	rows := make([]newsAnnotationRow, 0, len(inputs)*2)
	for start := 0; start < len(inputs); start += newsLLMBatchSize {
		end := start + newsLLMBatchSize
		if end > len(inputs) {
			end = len(inputs)
		}
		batch := inputs[start:end]
		payload, err := newsLLMAsk(ctx, cfg, batch)
		if err != nil {
			return rows, fmt.Errorf("news_watch: LLM-разметка (%s): %w", cfg.Model, err)
		}
		batchRows, err := validateNewsAnnotations(payload, batch, cfg.Model, newsPromptVersion, now)
		if err != nil {
			return rows, err
		}
		rows = append(rows, batchRows...)
	}
	return rows, nil
}
