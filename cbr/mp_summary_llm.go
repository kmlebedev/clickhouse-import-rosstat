package cbr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// mpPromptVersion — версия промпта LLM-слоя. Импортёр пишет её в колонку prompt_version,
	// а та входит в ключ ReplacingMergeTree: разметка новой редакции промпта добавляется
	// рядом со старой, а не затирает её (смена модели/промпта — новые строки тех же данных).
	// Значение закреплено тестом (mp_summary_llm_test.go) — менять его только сознательно.
	mpPromptVersion = 1

	// mpKindNote — вид матча-заметки: краткий вывод модели по каналу (колонка kind таблицы
	// cbr_mp_summary_channel). Заметка — формулировка модели, а не цитата документа, и
	// сверке с текстом резюме не подлежит.
	mpKindNote = "note"

	// mpLLMMaxTokens — предел вывода модели: запас на разметку 11 каналов с дословными
	// цитатами. В Anthropic Messages API max_tokens обязателен — без него запрос отвергается;
	// фактический лимит обеспечивает провайдер (своё значение для модели он отвергнет или
	// урежет сам).
	mpLLMMaxTokens = 8192

	// Переменные окружения LLM-слоя (все — обязательные; значения ключа не печатаются ни
	// в логах, ни в ошибках: SDK шлёт ключ заголовком x-api-key, а не в query).
	mpLLMBaseURLEnv = "LLM_BASE_URL"
	mpLLMAPIKeyEnv  = "LLM_API_KEY"
	mpLLMModelEnv   = "MP_SUMMARY_LLM_MODEL"
)

// mpLLMTimeout — дедлайн одного запроса к провайдеру (преемник util.postJSONTimeout, удалённого
// вместе с самописным транспортом). Переменная, а не константа: тест подставляет короткий
// дедлайн вместо минутного. 65 с — не минута GetJSON: вызов LLM генерирует разметку резюме
// (десятки цитат и заметок) и отвечает заметно дольше обычного фида, а дедлайн нужен затем,
// чтобы зависший провайдер дал ошибку, по которой импортёр деградирует до regex-слоя: без него
// шаг dagu повис бы вместе с провайдером.
var mpLLMTimeout = 65 * time.Second

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

// mpLLMFormatError — ошибка формата ответа модели: конверт не разобран (обрезанное, оборванное
// при чтении или не то тело), в ответе нет text-блока, content пуст, не JSON или не той схемы.
// Именно её имеет смысл повторить: один ретрай ловит и разовый сбой генерации, и обрыв ответа.
// Не формат (и потому без повтора): отказ провайдера (*anthropic.Error, не-2xx) и ошибки до
// чтения тела — транспорт и дедлайн контекста. Такие ошибки вызывающий должен узнать сразу,
// чтобы деградировать до regex-слоя, а не тратить второй вызов LLM.
type mpLLMFormatError struct{ err error }

func (e mpLLMFormatError) Error() string { return e.err.Error() }
func (e mpLLMFormatError) Unwrap() error { return e.err }

// mpIsFormatError — ошибка формата (в том числе обёрнутая).
func mpIsFormatError(err error) bool {
	var fe mpLLMFormatError
	return errors.As(err, &fe)
}

// mpIsJSONDecodeError — тело успешного ответа не получено целиком или не разобрано: обрыв
// чтения тела, обрезанный конверт, не-JSON или не та схема. Имя историческое (класс шире одного
// JSON-декодирования), но проверка по типам остаётся точной:
//   - SDK читает тело целиком (io.ReadAll) и оборачивает провал в «error reading response
//     body: %w», а декодирование — в «error parsing response json: %w»; обёртка через %w — та
//     причина, по которой errors.As/errors.Is видят исходную ошибку;
//   - обрыв тела на середине в обеих ветках даёт io.ErrUnexpectedEOF (проверено на
//     anthropic-sdk-go v1.80.0), не-JSON мусор (HTML заглушки WAF, отданной как
//     application/json) — *json.SyntaxError, не тот тип поля — *json.UnmarshalTypeError.
//
// Различить «обрыв чтения» и «обрезанный JSON» по типам нельзя — обе ветки SDK приносят один и
// тот же io.ErrUnexpectedEOF, а матчинг по тексту ошибки SDK («error reading response body» /
// «error parsing response json») хрупок и переживёт не любую версию SDK. Поэтому оба случая
// классифицируются форматом осознанно: цена — один повтор, максимум два вызова на документ, а
// событие редкое (обрыв TCP посреди ответа). См. TestTagMPChannelsLLMBodyReadBroken.
//
// Не формат: отказ провайдера (*anthropic.Error) и ошибки до чтения тела — транспорт и дедлайн
// контекста (у них другие типы). Пустое тело 200 с Content-Type application/json SDK тоже
// отвергает своей ошибкой декодирования («error parsing response json: EOF»), но это io.EOF, а
// не io.ErrUnexpectedEOF: повтора нет — паритет со старым «пустое тело» util.PostJSON. Тело с
// чужим Content-Type (HTML заглушки WAF отдаются как text/html) SDK отвергает ещё до
// json.Decoder — по типам такую ошибку не опознать, и она уходит как есть.
func mpIsJSONDecodeError(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr) || errors.Is(err, io.ErrUnexpectedEOF)
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

// mpLLMText склеивает текстовые блоки ответа в порядке выдачи. Anthropic-конверт может нести
// и блоки других типов (например, thinking): к разметке они отношения не имеют и пропускаются.
// Пустой результат — не ответ, а ошибка формата (её ловит mpLLMAskOnce).
func mpLLMText(msg *anthropic.Message) string {
	var b strings.Builder
	for _, block := range msg.Content {
		if block.Type != "text" {
			continue
		}
		b.WriteString(block.Text)
	}
	return b.String()
}

// mpLLMAskOnce — один запрос к провайдеру официальным SDK (Anthropic Messages API:
// POST <baseURL>/v1/messages) и разбор ответа до структуры. Ответ без text-блока, пустой или
// неразобранный content, не та схема JSON и не полученное целиком тело (обрыв чтения или
// обрезанный конверт — по типам они неразличимы, см. mpIsJSONDecodeError) — ошибки формата
// (mpLLMFormatError), их повторяет mpLLMAsk: максимум два вызова на документ. Отказ провайдера
// (*anthropic.Error, не-2xx) и ошибки до чтения тела (транспорт, дедлайн контекста) уходят как
// есть: они не формат — повтором не лечатся, и вызывающий должен узнать о них сразу, чтобы
// деградировать до regex-слоя, не тратя второй вызов.
//
// Клиент строится на каждый вызов: конфигурация приходит аргументами (значения — только из
// os.Getenv у вызывающего), а общий util.HttpClient сохраняет конвенцию транспорта проекта
// (национальные TLS-roots, cookie jar; свой Timeout у него не выставлен — дедлайн задаёт
// контекст запроса). baseURL — base без «/v1»: SDK сам дописывает путь «v1/messages», и «/v1» в
// base дал бы «/v1/v1/messages». User-Agent SDK свой (Anthropic/Go …) и намеренно не
// переопределяется: ротация randomUA нужна сайтам РФ, а не API провайдера. Повторы SDK
// выключены сознательно (WithMaxRetries(0), у SDK по умолчанию их два): спека §3.5 — отказ
// провайдера и транспортная ошибка означают деградацию до regex-слоя и догон следующим прогоном,
// а не скрытые ретраи внутри одного вызова, из-за которых импортёр молча ждал бы провайдера
// дольше дедлайна.
//
// Дедлайн запроса (mpLLMTimeout): зависший провайдер обязан дать ошибку, по которой импортёр
// деградирует, а не подвесить шаг dagu. Ключ API SDK шлёт заголовком x-api-key (не в
// URL/query), поэтому ни сам ключ, ни URL секрета в текстах ошибок не несут. Отказ провайдера
// возвращается компактной ошибкой «http status <код>: <фрагмент тела ≤512 байт>» — как в старом
// util.PostJSON: *anthropic.Error несёт тело ответа целиком (сотни байт в JSON), а ошибка уезжает
// в warn-лог импортёра (конвенция §3: код и начало тела, без URL).
func mpLLMAskOnce(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, error) {
	client := anthropic.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(strings.TrimRight(baseURL, "/")),
		option.WithHTTPClient(util.HttpClient),
		option.WithMaxRetries(0),
	)
	ctx, cancel := context.WithTimeout(context.Background(), mpLLMTimeout)
	defer cancel()
	msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     model,
		MaxTokens: mpLLMMaxTokens,
		// System — отдельное поле Anthropic-конверта: системной роли в сообщениях там нет.
		System: []anthropic.TextBlockParam{{Text: mpLLMSystemPrompt()}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(mpLLMUserPrompt(doc))),
		},
		// temperature = 0: разметка должна быть воспроизводимой, а не творческой (смена ответа
		// при том же промпте ломала бы сверку «(model, prompt_version) уже размечен»).
		// tools/response_format не используются — провайдер их поддержку не гарантирует,
		// схему задаёт сам промпт.
		Temperature: anthropic.Float(0),
	})
	if err != nil {
		var apiErr *anthropic.Error
		if errors.As(err, &apiErr) {
			// Тело — из RawJSON() (SDK v1.80.0): сам Error печатает его целиком, а в лог ему
			// место только нарезкой. Ключа в теле нет — он едет заголовком.
			return mpLLMPayload{}, fmt.Errorf("http status %d: %s", apiErr.StatusCode, util.ErrorSnippet([]byte(apiErr.RawJSON())))
		}
		if mpIsJSONDecodeError(err) {
			return mpLLMPayload{}, mpLLMFormatError{err}
		}
		return mpLLMPayload{}, err
	}
	content := mpLLMText(msg)
	if content == "" {
		return mpLLMPayload{}, mpLLMFormatError{fmt.Errorf("в ответе нет text-блока")}
	}
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
// промпт с реестром каналов → Anthropic-совместимый POST <baseURL>/v1/messages (клиент SDK
// строит mpLLMAskOnce) → разбор строгого JSON с одним ретраем на ошибку формата (mpLLMAsk).
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
