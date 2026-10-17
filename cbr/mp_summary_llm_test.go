package cbr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
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

// mpClearLLMEnv очищает все переменные, которые читает mpLLMProfilesFromEnv: профили в
// тестах задаются явно, и окружение разработчика не должно подмешивать свои значения.
func mpClearLLMEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"MP_SUMMARY_LLM_MODELS",
		"LLM_BASE_URL", "LLM_API_KEY", "MP_SUMMARY_LLM_MODEL",
		"GLM_LLM_BASE_URL", "GLM_LLM_API_KEY", "GLM_LLM_MODEL",
		"MIMO_LLM_BASE_URL", "MIMO_LLM_API_KEY", "MIMO_LLM_MODEL",
	} {
		t.Setenv(name, "")
	}
}

// mpLLMSetProfile заполняет переменные профиля name: default читает прежние LLM_*, остальные —
// <NAME>_LLM_*.
func mpLLMSetProfile(t *testing.T, name, base, key, model string) {
	t.Helper()
	if name == "default" {
		t.Setenv("LLM_BASE_URL", base)
		t.Setenv("LLM_API_KEY", key)
		t.Setenv("MP_SUMMARY_LLM_MODEL", model)
		return
	}
	prefix := strings.ToUpper(name) + "_LLM_"
	t.Setenv(prefix+"BASE_URL", base)
	t.Setenv(prefix+"API_KEY", key)
	t.Setenv(prefix+"MODEL", model)
}

// mpLLMWarnHook перехватывает предупреждения logrus: правила профилей — «предупредить и
// пропустить», и тест обязан видеть, что причина названа.
func mpLLMWarnHook(t *testing.T) *test.Hook {
	t.Helper()
	hook := test.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
	return hook
}

// mpLLMWarnText — все warn-сообщения хука одной строкой.
func mpLLMWarnText(hook *test.Hook) string {
	var messages []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel {
			messages = append(messages, entry.Message)
		}
	}
	return strings.Join(messages, "\n")
}

// mpLLMProfilesCheck — профили совпадают полностью: имена, порядок и значения.
func mpLLMProfilesCheck(t *testing.T, got, want []mpLLMProfile) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("профили = %+v, ожидалось %+v", got, want)
	}
}

// TestMpLLMProfilesFromEnv — конфигурация LLM-профилей (спека §3.1): пустой
// MP_SUMMARY_LLM_MODELS даёт один default на прежних переменных; список перечисляет профили в
// порядке; неполный, невалидно названный и дублирующий модель профиль пропускается с warn, а
// остальные работают.
func TestMpLLMProfilesFromEnv(t *testing.T) {
	const (
		deepseekBase  = "https://api.deepseek.com/anthropic"
		deepseekKey   = "test-key-default"
		deepseekModel = "deepseek-chat"

		glmBase  = "https://gate.joingonka.ai"
		glmKey   = "test-key-glm"
		glmModel = "zai-org/GLM-5.3-Flash"

		mimoBase  = "https://api.xiaomimimo.com/anthropic"
		mimoKey   = "test-key-mimo"
		mimoModel = "mimo-v2.6-flash"
	)

	t.Run("legacy env → один профиль default", func(t *testing.T) {
		mpClearLLMEnv(t)
		// Пробелы и хвостовой слэш: env-файл приносит перевод строки, а слэш в base дал бы
		// «//v1/messages».
		mpLLMSetProfile(t, "default", " "+deepseekBase+"/\n", " "+deepseekKey+" ", " "+deepseekModel+" ")
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "default", BaseURL: deepseekBase, APIKey: deepseekKey, Model: deepseekModel},
		})
	})

	t.Run("явный список default,glm,mimo → три профиля в порядке списка", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", "default,glm,mimo")
		mpLLMSetProfile(t, "default", deepseekBase, deepseekKey, deepseekModel)
		mpLLMSetProfile(t, "glm", glmBase, glmKey, glmModel)
		mpLLMSetProfile(t, "mimo", mimoBase, mimoKey, mimoModel)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "default", BaseURL: deepseekBase, APIKey: deepseekKey, Model: deepseekModel},
			{Name: "glm", BaseURL: glmBase, APIKey: glmKey, Model: glmModel},
			{Name: "mimo", BaseURL: mimoBase, APIKey: mimoKey, Model: mimoModel},
		})
	})

	t.Run("неполный glm пропущен, default остался", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", "default,glm")
		mpLLMSetProfile(t, "default", deepseekBase, deepseekKey, deepseekModel)
		t.Setenv("GLM_LLM_BASE_URL", glmBase)
		t.Setenv("GLM_LLM_API_KEY", glmKey)
		hook := mpLLMWarnHook(t)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "default", BaseURL: deepseekBase, APIKey: deepseekKey, Model: deepseekModel},
		})
		warns := mpLLMWarnText(hook)
		if !strings.Contains(warns, "GLM_LLM_MODEL") {
			t.Errorf("warn не называет недостающую переменную GLM_LLM_MODEL: %q", warns)
		}
		if !strings.Contains(warns, "glm") {
			t.Errorf("warn не называет профиль glm: %q", warns)
		}
		for _, key := range []string{glmKey, deepseekKey} {
			if strings.Contains(warns, key) {
				t.Errorf("в warn утёк ключ API: %q", warns)
			}
		}
	})

	t.Run("пробелы и регистр нормализуются", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", " GLM , MIMO ")
		mpLLMSetProfile(t, "glm", glmBase+"/", glmKey, glmModel)
		mpLLMSetProfile(t, "mimo", mimoBase, mimoKey, mimoModel)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "glm", BaseURL: glmBase, APIKey: glmKey, Model: glmModel},
			{Name: "mimo", BaseURL: mimoBase, APIKey: mimoKey, Model: mimoModel},
		})
	})

	t.Run("пустые элементы списка пропускаются молча", func(t *testing.T) {
		mpClearLLMEnv(t)
		// Хвостовая и двойная запятая — не профиль: warn про пустое имя заставил бы оператора
		// искать несуществующую переменную, а список от этого не становится неполным.
		t.Setenv("MP_SUMMARY_LLM_MODELS", "glm,,mimo,")
		mpLLMSetProfile(t, "glm", glmBase, glmKey, glmModel)
		mpLLMSetProfile(t, "mimo", mimoBase, mimoKey, mimoModel)
		hook := mpLLMWarnHook(t)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "glm", BaseURL: glmBase, APIKey: glmKey, Model: glmModel},
			{Name: "mimo", BaseURL: mimoBase, APIKey: mimoKey, Model: mimoModel},
		})
		if warns := mpLLMWarnText(hook); warns != "" {
			t.Errorf("пустые элементы списка дали warn: %q", warns)
		}
	})

	t.Run("невалидное имя пропущено, остальные работают", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", "glm-x,glm")
		mpLLMSetProfile(t, "glm", glmBase, glmKey, glmModel)
		hook := mpLLMWarnHook(t)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "glm", BaseURL: glmBase, APIKey: glmKey, Model: glmModel},
		})
		if warns := mpLLMWarnText(hook); !strings.Contains(warns, "glm-x") {
			t.Errorf("warn не называет невалидное имя glm-x: %q", warns)
		}
	})

	t.Run("дубликат модели пропущен, остаётся первый", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", "glm,mimo")
		mpLLMSetProfile(t, "glm", glmBase, glmKey, glmModel)
		// mimo отдаёт ту же модель, что glm: строки перетирались бы по ключу (model, prompt_version).
		mpLLMSetProfile(t, "mimo", mimoBase, mimoKey, glmModel)
		hook := mpLLMWarnHook(t)
		mpLLMProfilesCheck(t, mpLLMProfilesFromEnv(), []mpLLMProfile{
			{Name: "glm", BaseURL: glmBase, APIKey: glmKey, Model: glmModel},
		})
		warns := mpLLMWarnText(hook)
		if !strings.Contains(warns, "mimo") || !strings.Contains(warns, glmModel) {
			t.Errorf("warn не называет пропущенный профиль mimo и дубликат модели: %q", warns)
		}
	})

	t.Run("legacy: любая пустая переменная → nil", func(t *testing.T) {
		for _, missing := range []string{"LLM_BASE_URL", "LLM_API_KEY", "MP_SUMMARY_LLM_MODEL"} {
			t.Run(missing, func(t *testing.T) {
				mpClearLLMEnv(t)
				mpLLMSetProfile(t, "default", deepseekBase, deepseekKey, deepseekModel)
				t.Setenv(missing, "")
				if got := mpLLMProfilesFromEnv(); got != nil {
					t.Fatalf("профили = %+v, ожидался nil", got)
				}
			})
		}
	})

	t.Run("ни одного полного профиля → nil", func(t *testing.T) {
		mpClearLLMEnv(t)
		t.Setenv("MP_SUMMARY_LLM_MODELS", "glm")
		if got := mpLLMProfilesFromEnv(); got != nil {
			t.Fatalf("профили = %+v, ожидался nil", got)
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
	matches, _, err := mpTagTest(doc, mock.url+"/", "test-key", "test-model")
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

	matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
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

	matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
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
			matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
			mpLLMCheckRetryToLaborMatch(t, mock, matches, err)
		})
	}
	t.Run("оба ответа битые → ошибка", func(t *testing.T) {
		mock := mpNewLLMMock(t,
			mpLLMMockResponse{Body: mpLLMContentBody(t, truncatedContent)},
			mpLLMMockResponse{Body: mpLLMContentBody(t, truncatedContent)})
		matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
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
		_, _, err := mpLLMAskOnce(doc, mock.url, "test-key", "test-model")
		if err == nil {
			t.Fatal("ответ без text-блока: ожидалась ошибка")
		}
		if !mpIsFormatError(err) {
			t.Errorf("ошибка %v не классифицирована как ошибка формата", err)
		}
		if mock.calls() != 1 {
			t.Errorf("запросов %d, ожидался 1 (ретрай решает mpLLMAsk, а не mpLLMAskOnce)", mock.calls())
		}
	})

	t.Run("без text-блока → один ретрай → разметка", func(t *testing.T) {
		valid := mpLLMContentBody(t, mpLLMPayloadBody(t,
			mpLLMChannelSpec{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}}}))
		mock := mpNewLLMMock(t,
			mpLLMMockResponse{Body: mpLLMEmptyContentBody(t)},
			mpLLMMockResponse{Body: valid})
		matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
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

	matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
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
// повтором он не лечится, а импортёр по ошибке уходит в деградацию. Счётчик запросов пинит, что
// запрос ровно один: повтор для SDK отключён (`option.WithMaxRetries(0)`), иначе SDK повторил бы
// 500 своими ретраями (по умолчанию их два — тогда запросов было бы три). Ключ в текст ошибки не
// попадает (он едет заголовком x-api-key), а тело ответа обрезается до фрагмента: *anthropic.Error
// печатает его целиком, и сырое тело (десятки килобайт JSON) уехало бы в warn-лог импортёра.
func TestTagMPChannelsLLM500(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	// Тело заметно больше фрагмента (512 байт) и начинается с узнаваемой метки.
	body := `{"error":{"type":"api_error","message":"` + strings.Repeat("провайдер упал; ", 300) + `"}}`
	mock := mpNewLLMMock(t, mpLLMMockResponse{Status: http.StatusInternalServerError, Body: body})

	matches, _, err := mpTagTest(doc, mock.url, "test-key", "test-model")
	if err == nil {
		t.Fatalf("500: ожидалась ошибка, получены матчи %+v", matches)
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
	// Ошибка провайдера — не формат: ретрая формата нет (иначе вызовов было бы два).
	if mpIsFormatError(err) {
		t.Errorf("отказ провайдера классифицирован как ошибка формата: %v", err)
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

	_, _, err := mpTagTest(doc, url, "test-key", "test-model")
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

	_, _, err := mpTagTest(doc, srv.URL, "test-key", "test-model")
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

// TestTagMPChannelsLLMBodyReadBroken — обрыв чтения тела успешного ответа (200, обещанный
// Content-Length больше записанного, соединение закрыто) классифицируется форматом: один ретрай,
// ровно два запроса, финальная ошибка — mpLLMFormatError, документ деградирует до regex-слоя
// (ошибка наверх, как у любого второго подряд провала формата).
//
// Повтор тут осознанный: SDK приносит обрыв чтения как io.ErrUnexpectedEOF через
// «error reading response body: %w», а обрезанный JSON — тем же io.ErrUnexpectedEOF через
// «error parsing response json: %w». Различить их по типам нельзя, а матчинг по тексту SDK хрупок
// (переживёт не любую версию), поэтому оба случая — формат (см. mpIsJSONDecodeError): цена одного
// лишнего вызова меньше риска оставить документ без разметки из-за обрыва TCP.
func TestTagMPChannelsLLMBodyReadBroken(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	var mu sync.Mutex
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// Обещаем больше, чем запишем: для клиента это обрыв тела, а не конец ответа.
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","content":[{"type":"text","text":"`)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// http.ErrAbortHandler обрывает соединение, не дописав тело (в лог сервера не шумит).
		panic(http.ErrAbortHandler)
	}))
	defer srv.Close()

	t.Run("mpLLMAskOnce классифицирует обрыв как формат", func(t *testing.T) {
		_, _, err := mpLLMAskOnce(doc, srv.URL, "test-key", "test-model")
		if err == nil {
			t.Fatal("обрыв чтения тела: ожидалась ошибка")
		}
		if !mpIsFormatError(err) {
			t.Errorf("ошибка %v не классифицирована как ошибка формата", err)
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ошибка %v не несёт io.ErrUnexpectedEOF — изменилась семантика SDK", err)
		}
	})

	t.Run("tagMPChannelsLLM делает один ретрай и деградирует", func(t *testing.T) {
		mu.Lock()
		before := calls
		mu.Unlock()
		matches, _, err := mpTagTest(doc, srv.URL, "test-key", "test-model")
		if err == nil {
			t.Fatalf("обрыв чтения тела: ожидалась ошибка, получены матчи %+v", matches)
		}
		if !mpIsFormatError(err) {
			t.Errorf("финальная ошибка %v не является ошибкой формата (импортёр деградирует по любой ошибке, но ретрай ждём именно формата)", err)
		}
		mu.Lock()
		got := calls - before
		mu.Unlock()
		if got != 2 {
			t.Errorf("запросов %d, ожидалось 2 (первый + ретрай формата)", got)
		}
		if strings.Contains(err.Error(), "test-key") {
			t.Errorf("в текст ошибки утёк ключ API: %v", err)
		}
	})
}

// mpTagTest — tagMPChannelsLLM с тестовым профилем: продовый вызов принимает профиль, а
// тестам важно лишь, что base/api/model доехали до запроса.
func mpTagTest(doc mpSummaryDoc, baseURL, apiKey, model string) ([]mpChannelMatch, mpLLMDocStats, error) {
	return tagMPChannelsLLM(doc, mpLLMProfile{Name: "test", BaseURL: baseURL, APIKey: apiKey, Model: model})
}

// TestTagMPChannelsLLMStats — валидный ответ: usage из конверта (мок отдаёт 1/1), одна
// попытка, положительная длительность и счётчики качества на полном наборе 11 каналов.
// money_supply приходит без note — заметок 10, а не 11; каждая цитата сохранена, поэтому
// dropped = 0, а каналов с цитатами — все 11.
func TestTagMPChannelsLLMStats(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	channels := mpLLMValidChannels()
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, channels...))})

	matches, stats, err := mpTagTest(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v", err)
	}
	if stats.Model != "test-model" {
		t.Errorf("Model = %q, ожидалось %q", stats.Model, "test-model")
	}
	if stats.Attempts != 1 {
		t.Errorf("Attempts = %d, ожидалась 1 попытка", stats.Attempts)
	}
	if stats.Duration <= 0 {
		t.Errorf("Duration = %v, ожидалась положительная длительность", stats.Duration)
	}
	if want := (mpLLMUsage{InputTokens: 1, OutputTokens: 1}); stats.Usage != want {
		t.Errorf("Usage = %+v, ожидалось %+v (usage конверта мока)", stats.Usage, want)
	}
	const wantQuotes = 11
	if stats.QuotesTotal != wantQuotes {
		t.Errorf("QuotesTotal = %d, ожидалось %d", stats.QuotesTotal, wantQuotes)
	}
	if stats.QuotesKept != wantQuotes {
		t.Errorf("QuotesKept = %d, ожидалось %d", stats.QuotesKept, wantQuotes)
	}
	if stats.QuotesDropped != 0 {
		t.Errorf("QuotesDropped = %d, ожидалось 0", stats.QuotesDropped)
	}
	const wantNotes = 10 // money_supply в валидном ответе без note
	if stats.Notes != wantNotes {
		t.Errorf("Notes = %d, ожидалось %d", stats.Notes, wantNotes)
	}
	if stats.ChannelsWithQuotes != len(mpChannels) {
		t.Errorf("ChannelsWithQuotes = %d, ожидалось %d (все каналы реестра)", stats.ChannelsWithQuotes, len(mpChannels))
	}
	if len(matches) != wantQuotes+wantNotes {
		t.Errorf("матчей %d, ожидалось %d (цитаты + заметки)", len(matches), wantQuotes+wantNotes)
	}
}

// TestTagMPChannelsLLMStatsHallucination — цитата, которой нет в тексте, увеличивает
// QuotesTotal и QuotesDropped, но не QuotesKept: доля отброшенного в отчёте считается от
// всего, что прислала модель, включая галлюцинации.
func TestTagMPChannelsLLMStatsHallucination(t *testing.T) {
	const hallucination = "Совет директоров допускает повышение ключевой ставки до 25,00% годовых на ближайшем заседании."

	doc := mpLLMFixtureDoc(t)
	if strings.Contains(mpLLMCorpus(doc), hallucination) {
		t.Fatalf("цитата-галлюцинация случайно встречается в документе: %q", hallucination)
	}
	base := []mpLLMChannelSpec{
		{Slug: "output_gap", Quotes: []mpLLMQuoteSpec{{Kind: mpKindRiskPro, Text: mpLLMRiskProPin}},
			Note: "Дисбаланс спроса и предложения."},
		{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}},
			Note: "Рынок труда смягчается."},
	}
	withHallucination := []mpLLMChannelSpec{
		{Slug: "output_gap", Quotes: []mpLLMQuoteSpec{
			{Kind: mpKindRiskPro, Text: mpLLMRiskProPin},
			{Kind: mpKindRiskPro, Text: hallucination},
		}, Note: "Дисбаланс спроса и предложения."},
		base[1],
	}

	baseMock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, base...))})
	_, baseStats, err := mpTagTest(doc, baseMock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM (без галлюцинации): %v", err)
	}
	mock := mpNewLLMMock(t, mpLLMMockResponse{Body: mpLLMContentBody(t, mpLLMPayloadBody(t, withHallucination...))})
	_, stats, err := mpTagTest(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM (с галлюцинацией): %v", err)
	}
	if stats.QuotesTotal != baseStats.QuotesTotal+1 {
		t.Errorf("QuotesTotal = %d, ожидалось %d (галлюцинация входит в общий счёт)",
			stats.QuotesTotal, baseStats.QuotesTotal+1)
	}
	if stats.QuotesKept != baseStats.QuotesKept {
		t.Errorf("QuotesKept = %d, ожидалось %d (отброшенная цитата не сохраняется)",
			stats.QuotesKept, baseStats.QuotesKept)
	}
	if stats.QuotesDropped != baseStats.QuotesDropped+1 {
		t.Errorf("QuotesDropped = %d, ожидалось %d", stats.QuotesDropped, baseStats.QuotesDropped+1)
	}
}

// TestTagMPChannelsLLMStatsRetry — битый первый ответ лечится ретраем: попыток две, usage
// суммируется по обоим вызовам (мок отдаёт 1/1 в каждом конверте), длительность покрывает
// оба вызова.
func TestTagMPChannelsLLMStatsRetry(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	const truncatedContent = `{"channels":[{"slug":"labor_market","quotes":[{"kind":"prose","text":"Дефицит работников`
	valid := mpLLMContentBody(t, mpLLMPayloadBody(t,
		mpLLMChannelSpec{Slug: "labor_market", Quotes: []mpLLMQuoteSpec{{Kind: mpKindProse, Text: mpLLMLaborPin}}}))
	mock := mpNewLLMMock(t,
		mpLLMMockResponse{Body: mpLLMContentBody(t, truncatedContent)},
		mpLLMMockResponse{Body: valid})

	matches, stats, err := mpTagTest(doc, mock.url, "test-key", "test-model")
	if err != nil {
		t.Fatalf("tagMPChannelsLLM: %v (ретрай должен был вылечить формат)", err)
	}
	if mock.calls() != 2 {
		t.Errorf("запросов %d, ожидалось 2 (первый + ретрай)", mock.calls())
	}
	if stats.Attempts != 2 {
		t.Errorf("Attempts = %d, ожидалось 2", stats.Attempts)
	}
	if want := (mpLLMUsage{InputTokens: 2, OutputTokens: 2}); stats.Usage != want {
		t.Errorf("Usage = %+v, ожидалось %+v (сумма двух попыток)", stats.Usage, want)
	}
	if stats.Duration <= 0 {
		t.Errorf("Duration = %v, ожидалась положительная длительность", stats.Duration)
	}
	if len(matches) != 1 || !mpHasMatchKindText(matches, "labor_market", mpKindProse, mpLLMLaborPin) {
		t.Errorf("матчи = %+v, ожидался один дословный матч labor_market", matches)
	}
	if stats.QuotesTotal != 1 || stats.QuotesKept != 1 || stats.QuotesDropped != 0 {
		t.Errorf("счётчики после ретрая = total %d, kept %d, dropped %d, ожидалось 1/1/0",
			stats.QuotesTotal, stats.QuotesKept, stats.QuotesDropped)
	}
}

// TestTagMPChannelsLLMStats500 — отказ провайдера: ошибка наверх, попытка одна (500 не
// повторяется), usage нулевой — конверта с расходом токенов провайдер не прислал.
func TestTagMPChannelsLLMStats500(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	mock := mpNewLLMMock(t, mpLLMMockResponse{
		Status: http.StatusInternalServerError,
		Body:   `{"error":{"type":"api_error","message":"провайдер упал"}}`,
	})

	matches, stats, err := mpTagTest(doc, mock.url, "test-key", "test-model")
	if err == nil {
		t.Fatalf("500: ожидалась ошибка, получены матчи %+v", matches)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, ожидался 1 (500 не повторяется)", mock.calls())
	}
	if stats.Attempts != 1 {
		t.Errorf("Attempts = %d, ожидалась 1 попытка", stats.Attempts)
	}
	if stats.Usage != (mpLLMUsage{}) {
		t.Errorf("Usage = %+v, ожидался нулевой", stats.Usage)
	}
	if stats.Model != "test-model" {
		t.Errorf("Model = %q, ожидалось %q", stats.Model, "test-model")
	}
}

// TestMpTagLLMReturnsError — ошибка LLM не глотается внутри mpTagLLM: она доходит до
// вызывающего (тот решает, что это деградация одного документа), батч при этом не трогается.
func TestMpTagLLMReturnsError(t *testing.T) {
	doc := mpLLMFixtureDoc(t)
	mock := mpNewLLMMock(t, mpLLMMockResponse{
		Status: http.StatusInternalServerError,
		Body:   `{"error":{"type":"api_error","message":"провайдер упал"}}`,
	})
	// Батч без соединения: ошибка LLM обязана вернуться до первой записи, иначе append
	// упал бы на nil-соединении.
	rows := &mpBatch{}
	d := mpLLMDoc{
		Date: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC),
		Url:  "https://www.cbr.ru/example",
		Doc:  doc,
	}
	stats, err := mpTagLLM(rows, d, mpLLMProfile{BaseURL: mock.url, APIKey: "test-key", Model: "test-model"})
	if err == nil {
		t.Fatalf("ошибка LLM не дошла до вызывающего, stats = %+v", stats)
	}
	if mock.calls() != 1 {
		t.Errorf("запросов %d, ожидался 1 (500 не повторяется)", mock.calls())
	}
	if !mpIsLLMTagError(err) {
		t.Errorf("ошибка %v не опознана как ошибка LLM (а не батча)", err)
	}
	if stats.Attempts != 1 || stats.Usage != (mpLLMUsage{}) {
		t.Errorf("stats = %+v, ожидались Attempts=1 и нулевой Usage", stats)
	}
	if rows.rows != 0 || rows.batch != nil {
		t.Errorf("батч тронут при ошибке LLM: rows=%d, batch=%v", rows.rows, rows.batch)
	}
}
