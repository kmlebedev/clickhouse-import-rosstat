package cbr

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// mpPromptVersion — версия промпта LLM-слоя. Импортёр пишет её в колонку prompt_version,
	// а та входит в ключ ReplacingMergeTree: разметка новой редакции промпта добавляется
	// рядом со старой, а не затирает её (смена модели/промпта — новые строки тех же данных).
	// Значение закреплено тестом (mp_summary_llm_test.go) — менять его только сознательно.
	mpPromptVersion = 1

	// mpKindNote — вид матча-заметки: краткий вывод модели по каналу (колонка kind таблицы
	// cbr_mp_summary_channel). Заметка — формулировка модели, а не цитата документа, и
	// сверке с текстом резюме не подлежит.
	mpKindNote = "note"

	// mpLLMChatPath — метод OpenAI-совместимого API (первичный провайдер — DeepSeek).
	mpLLMChatPath = "/chat/completions"

	// Переменные окружения LLM-слоя (все — обязательные; значения ключа не печатаются ни
	// в логах, ни в ошибках: ключ едет заголовком Authorization, а не в query).
	mpLLMBaseURLEnv = "LLM_BASE_URL"
	mpLLMAPIKeyEnv  = "LLM_API_KEY"
	mpLLMModelEnv   = "MP_SUMMARY_LLM_MODEL"
)

// mpLLMConfigFromEnv читает конфигурацию LLM-слоя (спека §3.5). Любая пустая переменная →
// ok = false: это не ошибка, а основание для деградации — документ остаётся с regex-слоем, а
// LLM догоняется следующим прогоном, когда владелец дополнит env-файл. Окружающие пробелы
// обрезаются: env-файл приносит хвостовой перевод строки, а лишний пробел в baseURL — молча
// битый URL.
func mpLLMConfigFromEnv() (baseURL, apiKey, model string, ok bool) {
	baseURL = strings.TrimSpace(os.Getenv(mpLLMBaseURLEnv))
	apiKey = strings.TrimSpace(os.Getenv(mpLLMAPIKeyEnv))
	model = strings.TrimSpace(os.Getenv(mpLLMModelEnv))
	return baseURL, apiKey, model, baseURL != "" && apiKey != "" && model != ""
}

// mpLLMMessage — одно сообщение чата OpenAI-совместимого API.
type mpLLMMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// mpLLMRequest — тело запроса /chat/completions. temperature = 0: разметка должна быть
// воспроизводимой, а не творческой (смена ответа при том же промпте ломала бы сверку
// «(model, prompt_version) уже размечен»). tools/response_format не используются — первичный
// провайдер их поддержку не гарантирует, схему задаёт сам промпт.
type mpLLMRequest struct {
	Model       string         `json:"model"`
	Messages    []mpLLMMessage `json:"messages"`
	Temperature float64        `json:"temperature"`
}

// mpLLMChatResponse — конверт ответа: нужен только текст первого выбора.
type mpLLMChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// mpLLMQuote — цитата канала в ответе модели: вид (risk_pro|risk_dez|prose) и дословный
// фрагмент документа.
type mpLLMQuote struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// mpLLMChannel — разметка одного канала в ответе модели: цитаты и краткая заметка.
type mpLLMChannel struct {
	Slug   string       `json:"slug"`
	Quotes []mpLLMQuote `json:"quotes"`
	Note   string       `json:"note"`
}

// mpLLMPayload — разобранный content ответа модели (схема описана в mpLLMSystemPrompt).
type mpLLMPayload struct {
	Channels []mpLLMChannel `json:"channels"`
}

// mpLLMFormatError — ошибка формата ответа модели: не разобран конверт (обрезанное или не то
// тело), пустой content, content не JSON или не той схемы. Именно её имеет смысл повторить:
// один ретрай ловит разовый сбой генерации. Отказ провайдера (не-2xx) и ошибка транспорта
// форматом не считаются — повтором они не лечатся, и вызывающий должен узнать о них сразу,
// чтобы деградировать до regex-слоя, а не тратить второй вызов LLM.
type mpLLMFormatError struct{ err error }

func (e mpLLMFormatError) Error() string { return e.err.Error() }
func (e mpLLMFormatError) Unwrap() error { return e.err }

// mpIsFormatError — ошибка формата (в том числе обёрнутая).
func mpIsFormatError(err error) bool {
	var fe mpLLMFormatError
	return errors.As(err, &fe)
}

// mpIsJSONDecodeError — ошибка разбора JSON, которую вернул PostJSON из json.Unmarshal:
// обрезанное или не то (HTML WAF и т.п.) тело ответа. Статус и транспорт дают ошибки других
// типов, поэтому по этой проверке формат отделяется от отказа провайдера.
func mpIsJSONDecodeError(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr)
}

// mpLLMQuoteKind — вид цитаты из разрешённой тройки risk_pro|risk_dez|prose.
func mpLLMQuoteKind(kind string) bool {
	return kind == mpKindRiskPro || kind == mpKindRiskDez || kind == mpKindProse
}

// mpLLMSystemPrompt — system-сообщение: метка редакции инструкции (mpPromptVersion —
// значение, которое импортёр пишет в колонку prompt_version, поэтому оно же стоит и в самом
// промпте), роль, реестр 11 каналов с определениями (slug, имя и направление — из
// mpChannels, единственного источника правды о каналах) и требование строгого JSON описанной
// схемы. Схема повторена дословно: её ждёт mpLLMAskOnce, а расхождение стоит документу
// LLM-слоя (после ретрая он остаётся с regex-разметкой).
func mpLLMSystemPrompt() string {
	var b strings.Builder
	b.WriteString("Редакция инструкции: " + strconv.Itoa(mpPromptVersion) + ".\n")
	b.WriteString("Ты размечаешь резюме обсуждения ключевой ставки Банка России по каналам трансмиссии денежно-кредитной политики.\n")
	b.WriteString("Для каждого канала найди в тексте документа дословные цитаты и кратко сформулируй, что Банк России говорит про этот канал.\n\n")
	b.WriteString("Каналы (slug — определение):\n")
	for _, ch := range mpChannels {
		b.WriteString("- " + ch.Slug + " — " + ch.Title + " (" + ch.Sign + ")\n")
	}
	b.WriteString("\nВерни строго один JSON-объект, без текста и пояснений вокруг него:\n")
	b.WriteString(`{"channels":[{"slug":"<slug из списка>","quotes":[{"kind":"risk_pro|risk_dez|prose","text":"<дословная цитата из документа>"}],"note":"<1–2 предложения: что ЦБ говорит про канал>"}]}` + "\n")
	b.WriteString("\nПравила:\n")
	b.WriteString("- включи в ответ каждый канал из списка; если канал в документе не упоминается, оставь пустой список quotes и напиши об этом в note;\n")
	b.WriteString("- kind: risk_pro — проинфляционный риск, risk_dez — дезинфляционный риск, prose — остальные утверждения про канал;\n")
	b.WriteString("- text — фрагмент текста документа символ в символ, без пересказа, сокращений и многоточий: цитата, которой нет в документе, будет отброшена;\n")
	b.WriteString("- note — по-русски и кратко.\n")
	return b.String()
}

// mpLLMUserPrompt — user-сообщение: текст документа с заголовками блоков и подзаголовками
// (модели нужно понимать, какой блок про что) и маркером «- » у пунктов списка. Цитаты из
// заголовков не выживут: сверка идёт с текстами блоков, а не с заголовками.
func mpLLMUserPrompt(doc mpSummaryDoc) string {
	var b strings.Builder
	b.WriteString("Текст резюме обсуждения:\n")
	block, sub := "", ""
	for _, blk := range doc.Blocks {
		if blk.Block != block {
			block, sub = blk.Block, ""
			b.WriteString("\n## " + block + "\n")
		}
		if blk.Subsection != sub {
			sub = blk.Subsection
			if sub != "" {
				b.WriteString("### " + sub + "\n")
			}
		}
		if blk.Kind == mpKindBullet {
			b.WriteString("- ")
		}
		b.WriteString(normMPText(blk.Text))
		b.WriteString("\n")
	}
	return b.String()
}

// mpLLMCorpus — нормализованный текст документа для сверки цитат: тексты блоков (парсер уже
// применил normMPText, повторная нормализация идемпотентна) склеены переводом строки — та же
// форма, что у mpChannelCorpus в тестах Task 4. Цитата обязана быть подстрокой корпуса: LLM
// охотно «доцитирует» формулировку, и такая цитата в данные попасть не должна.
func mpLLMCorpus(doc mpSummaryDoc) string {
	var b strings.Builder
	for _, blk := range doc.Blocks {
		b.WriteString(normMPText(blk.Text))
		b.WriteString("\n")
	}
	return b.String()
}

// mpLLMJSONBody вырезает JSON-объект из content ответа модели: модель может обрамить его
// ```-блоком или сопроводить фразой, и терять из-за этого разметку целого документа не
// нужно. Берём всё от первой «{» до последней «}» — этого достаточно для одного объекта
// схемы. ok = false, если скобок нет (content пуст или это не JSON): это ошибка формата,
// а не «разметки нет» — пустой ответ не должен выглядеть успешным вызовом.
func mpLLMJSONBody(content string) (string, bool) {
	s := strings.TrimSpace(content)
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end < start {
		return "", false
	}
	return s[start : end+1], true
}

// mpLLMAskOnce — один запрос к провайдеру и разбор ответа до структуры. Обрезанный конверт
// (json.Unmarshal внутри PostJSON), отсутствие choices, пустой или неразобранный content и
// не та схема — ошибки формата (mpLLMFormatError), их повторяет mpLLMAsk. Отказ провайдера
// (не-2xx) и ошибка транспорта уходят как есть: они не формат. Ключ API едет заголовком
// Authorization, поэтому URL в текстах ошибок секрета не несёт (конвенция util.PostJSON).
func mpLLMAskOnce(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, error) {
	req := mpLLMRequest{
		Model: model,
		Messages: []mpLLMMessage{
			{Role: "system", Content: mpLLMSystemPrompt()},
			{Role: "user", Content: mpLLMUserPrompt(doc)},
		},
		Temperature: 0,
	}
	headers := map[string]string{"Authorization": "Bearer " + apiKey}
	url := strings.TrimRight(baseURL, "/") + mpLLMChatPath
	var resp mpLLMChatResponse
	if err := util.PostJSON(url, headers, req, &resp); err != nil {
		if mpIsJSONDecodeError(err) {
			return mpLLMPayload{}, mpLLMFormatError{err}
		}
		return mpLLMPayload{}, err
	}
	if len(resp.Choices) == 0 {
		return mpLLMPayload{}, mpLLMFormatError{fmt.Errorf("в ответе нет choices")}
	}
	content := resp.Choices[0].Message.Content
	body, ok := mpLLMJSONBody(content)
	if !ok {
		return mpLLMPayload{}, mpLLMFormatError{fmt.Errorf("в content нет JSON-объекта (content: %d байт)", len(content))}
	}
	var payload mpLLMPayload
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return mpLLMPayload{}, mpLLMFormatError{fmt.Errorf("content не разобран: %w", err)}
	}
	return payload, nil
}

// mpLLMAsk — запрос с одним ретраем на ошибку формата. Второй подряд провал формата
// возвращается вызывающему: спека §3.5 — разметка документа пропускается (документ остаётся
// с regex-слоем), догон следующим прогоном. Имя модели в warn не секрет: оно же пишется
// в колонку model.
func mpLLMAsk(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, error) {
	payload, err := mpLLMAskOnce(doc, baseURL, apiKey, model)
	if err == nil || !mpIsFormatError(err) {
		return payload, err
	}
	log.Warnf("cbr_mp_summary: ответ LLM (%s) не разобран (%v), повтор запроса", model, err)
	return mpLLMAskOnce(doc, baseURL, apiKey, model)
}

// tagMPChannelsLLM размечает документ резюме по 11 каналам моделью LLM (слой 2, спека §3.5):
// промпт с реестром каналов → OpenAI-совместимый /chat/completions → разбор строгого JSON с
// одним ретраем на ошибку формата (mpLLMAsk).
//
// Валидация против галлюцинаций: каждая цитата обязана дословно встречаться в
// нормализованном тексте документа, иначе отбрасывается с warn (модель «доцитирует»);
// slug вне реестра mpChannels отбрасывается целиком (и цитаты, и заметка); kind вне
// тройки risk_pro|risk_dez|prose приводится к prose с warn.
//
// Результат: сначала матчи цитат в порядке ответа модели, затем по одному матчу Kind='note'
// на каждый канал с непустой note; Seq — сквозной с 1 в порядке выдачи (первое поле ключа
// строк в таблице). Возвращаются только матчи: Method, Model и PromptVersion проставляет
// вызывающий (импортёр) — здесь они неизвестны.
func tagMPChannelsLLM(doc mpSummaryDoc, baseURL, apiKey, model string) ([]mpChannelMatch, error) {
	payload, err := mpLLMAsk(doc, baseURL, apiKey, model)
	if err != nil {
		return nil, err
	}
	corpus := mpLLMCorpus(doc)
	known := make(map[string]bool, len(mpChannels))
	for _, ch := range mpChannels {
		known[ch.Slug] = true
	}
	channels := make([]mpLLMChannel, 0, len(payload.Channels))
	for _, ch := range payload.Channels {
		if !known[ch.Slug] {
			log.Warnf("cbr_mp_summary: LLM вернул канал %q вне реестра, пропущен", ch.Slug)
			continue
		}
		channels = append(channels, ch)
	}

	var (
		out  []mpChannelMatch
		seen = make(map[mpChannelMatchKey]bool)
	)
	for _, ch := range channels {
		for _, q := range ch.Quotes {
			text := normMPText(q.Text)
			if text == "" {
				log.Warnf("cbr_mp_summary: канал %s: пустая цитата, пропущена", ch.Slug)
				continue
			}
			if !strings.Contains(corpus, text) {
				log.Warnf("cbr_mp_summary: канал %s: цитаты нет в тексте резюме, отброшена: %.80q", ch.Slug, text)
				continue
			}
			kind := q.Kind
			if !mpLLMQuoteKind(kind) {
				log.Warnf("cbr_mp_summary: канал %s: вид %q вне risk_pro|risk_dez|prose, приведён к %s",
					ch.Slug, kind, mpKindProse)
				kind = mpKindProse
			}
			key := mpChannelMatchKey{Channel: ch.Slug, Kind: kind, Text: text}
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, mpChannelMatch{Channel: ch.Slug, Kind: kind, Text: text})
		}
	}
	noted := make(map[string]bool, len(channels))
	for _, ch := range channels {
		note := strings.TrimSpace(ch.Note)
		if note == "" || noted[ch.Slug] {
			continue
		}
		noted[ch.Slug] = true
		out = append(out, mpChannelMatch{Channel: ch.Slug, Kind: mpKindNote, Text: note})
	}
	for i := range out {
		out[i].Seq = i + 1
	}
	return out, nil
}
