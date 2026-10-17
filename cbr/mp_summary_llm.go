package cbr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
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

	// Переменные окружения профиля default (прежний контракт; значения ключа не печатаются
	// ни в логах, ни в ошибках: SDK шлёт ключ заголовком x-api-key, а не в query).
	mpLLMBaseURLEnv = "LLM_BASE_URL"
	mpLLMAPIKeyEnv  = "LLM_API_KEY"
	mpLLMModelEnv   = "MP_SUMMARY_LLM_MODEL"

	// mpLLMModelsEnv — список профилей LLM-слоя через запятую; пусто/не задано → один default.
	mpLLMModelsEnv = "MP_SUMMARY_LLM_MODELS"

	// mpLLMProfileDefault — профиль, читающий прежние LLM_*: без mpLLMModelsEnv поведение
	// импортёра остаётся одномодельным.
	mpLLMProfileDefault = "default"
)

// mpLLMTimeout — дедлайн одного запроса к провайдеру (преемник util.postJSONTimeout, удалённого
// вместе с самописным транспортом). Переменная, а не константа: тест подставляет короткий
// дедлайн вместо минутного. 65 с — не минута GetJSON: вызов LLM генерирует разметку резюме
// (десятки цитат и заметок) и отвечает заметно дольше обычного фида, а дедлайн нужен затем,
// чтобы зависший провайдер дал ошибку, по которой импортёр деградирует до regex-слоя: без него
// шаг dagu повис бы вместе с провайдером.
var mpLLMTimeout = 65 * time.Second

// mpLLMMaxTokens — предел вывода модели: запас на разметку 11 каналов с дословными
// цитатами. В Anthropic Messages API max_tokens обязателен — без него запрос отвергается;
// фактический лимит обеспечивает провайдер (своё значение для модели он отвергнет или
// урежет сам). Переменная, а не константа: живой harness сравнения моделей подменяет лимит
// (MP_LLM_EVAL_MAX_TOKENS) только на свой прогон, не меняя продовый дефолт.
var mpLLMMaxTokens int64 = 8192

// mpLLMPrefillJSON — предзаполнить ответ ассистента символом «{». Некоторые модели (GLM)
// тратят весь max_tokens на рассуждение в text-блоке и не доходят до JSON: prefill заставляет
// их продолжать уже начатый JSON-объект. Это совместимость harness-прогона, а не прод-дефолт:
// system-промпт, temperature и схема ответа не меняются, но запрос получает лишнее сообщение
// ассистента, поэтому в отчёте такой прогон помечается отдельно.
var mpLLMPrefillJSON bool

// mpLLMThinkingDisabled — отключить extended thinking в запросе (Anthropic `thinking:
// {"type":"disabled"}`). Некоторые модели (GLM, MiMo) по умолчанию тратят весь max_tokens
// на рассуждение и не доходят до JSON; harness включает disabled только на свой прогон,
// продовый дефолт — не отправлять поле вовсе.
var mpLLMThinkingDisabled bool

// mpLLMTransport — транспорт LLM-слоя: "anthropic" (продовый, официальный SDK) или "openai"
// (OpenAI-совместимый POST /chat/completions с response_format=json_object). Второй — только
// для harness-сравнения: GLM и MiMo на Anthropic-эндпоинте уходят в reasoning и не доходят до
// JSON, а их OpenAI-эндпоинты поддерживают JSON-режим. Прод-путь по умолчанию не меняется.
var mpLLMTransport = "anthropic"

// mpLLMReasoningEffort — OpenAI-совместимое поле reasoning_effort (none|low|medium|high),
// только harness: GLM по умолчанию уходит в длинный анализ и тратит весь max_tokens до JSON;
// "none" отключает reasoning (проверено на гейтвее Joingonka 2026-10-17). Пусто — поле не
// отправляется, прод-путь его не использует.
var mpLLMReasoningEffort string

// mpLLMConcise — harness-режим для моделей с жёстким лимитом вывода (GLM — 8192 токенов):
// после пользовательского промпта добавляется инструкция «не более одной цитаты на канал,
// note — одно короткое предложение». Применяется только к повторной разметке документов,
// которые не влезли в лимит; обычный прогон и прод-путь его не используют.
var mpLLMConcise bool

// mpLLMConciseInstruction — текст harness-инструкции concise-режима. Отдельная константа,
// чтобы тест пинил её в теле запроса и её нельзя было случайно потерять при правке промпта.
const mpLLMConciseInstruction = "Сократи ответ: не более одной цитаты на канал, note — одно короткое предложение. Это критично: лимит вывода 8192 токена."

// mpLLMProfile — один профиль разметки: имя (default/glm/mimo) нужно только для конфигурации и
// логов, а в данные пишется фактическое имя модели API (Model) — по нему импортёр отличает,
// какой моделью размечен документ.
type mpLLMProfile struct {
	Name    string
	BaseURL string
	APIKey  string
	Model   string
}

// mpLLMProfileNameRe — допустимое имя профиля: имя становится префиксом env-переменных
// (<NAME>_LLM_*), поэтому вне [a-z0-9_] его нельзя собрать однозначно.
var mpLLMProfileNameRe = regexp.MustCompile(`^[a-z0-9_]+$`)

// mpLLMProfilesFromEnv читает профили LLM-слоя (спека §3.1). MP_SUMMARY_LLM_MODELS — список
// имён через запятую; пусто → один default на прежних LLM_BASE_URL/LLM_API_KEY/
// MP_SUMMARY_LLM_MODEL. Профиль p читает <P>_LLM_BASE_URL/<P>_LLM_API_KEY/<P>_LLM_MODEL.
//
// Неполный профиль не роняет прогон: warn с именами недостающих переменных (значения не
// печатаются — в них ключи) и пропуск, остальные профили работают. Имя нормализуется
// (TrimSpace, ToLower) и обязано быть [a-z0-9_]+. Дубликат модели пропускается: model входит
// в ключ ReplacingMergeTree, и строки одинаковой пары (model, prompt_version) перетирали бы
// друг друга, поэтому остаётся первый профиль с этой моделью. Ни одного полного профиля → nil:
// импортёр деградирует до regex-слоя, а LLM догоняется следующим прогоном, когда владелец
// дополнит env-файл.
func mpLLMProfilesFromEnv() []mpLLMProfile {
	list := strings.TrimSpace(os.Getenv(mpLLMModelsEnv))
	names := []string{mpLLMProfileDefault}
	if list != "" {
		names = strings.Split(list, ",")
	}
	profiles := make([]mpLLMProfile, 0, len(names))
	seenNames := make(map[string]bool, len(names))
	modelOwner := make(map[string]string, len(names))
	for _, raw := range names {
		name := strings.ToLower(strings.TrimSpace(raw))
		// Пустой элемент списка (`glm,,mimo`, хвостовая запятая) — не профиль: молча
		// пропускаем, иначе warn про пустое имя отправил бы оператора искать несуществующую
		// переменную, хотя список просто содержит лишнюю запятую.
		if name == "" {
			continue
		}
		if !mpLLMProfileNameRe.MatchString(name) {
			log.Warnf("cbr_mp_summary: LLM-профиль %q пропущен: имя должно соответствовать %s",
				name, mpLLMProfileNameRe)
			continue
		}
		if seenNames[name] {
			continue
		}
		seenNames[name] = true
		profile, missing := mpLLMProfileFromEnv(name)
		if len(missing) > 0 {
			log.Warnf("cbr_mp_summary: LLM-профиль %q пропущен: не заданы %s",
				name, strings.Join(missing, ", "))
			continue
		}
		if owner, ok := modelOwner[profile.Model]; ok {
			log.Warnf("cbr_mp_summary: LLM-профиль %q пропущен: модель %q уже задана у профиля %q",
				name, profile.Model, owner)
			continue
		}
		modelOwner[profile.Model] = name
		profiles = append(profiles, profile)
	}
	if len(profiles) == 0 {
		return nil
	}
	return profiles
}

// mpLLMProfileFromEnv читает профиль name и перечисляет имена незаполненных переменных:
// значения в warn не попадают, а пустое имя переменной — единственное, что нужно оператору.
// Окружающие пробелы обрезаются (env-файл приносит хвостовой перевод строки), а хвостовой слэш
// baseURL — иначе SDK получил бы «//v1/messages».
func mpLLMProfileFromEnv(name string) (mpLLMProfile, []string) {
	baseURLEnv, apiKeyEnv, modelEnv := mpLLMBaseURLEnv, mpLLMAPIKeyEnv, mpLLMModelEnv
	if name != mpLLMProfileDefault {
		prefix := strings.ToUpper(name) + "_LLM_"
		baseURLEnv, apiKeyEnv, modelEnv = prefix+"BASE_URL", prefix+"API_KEY", prefix+"MODEL"
	}
	profile := mpLLMProfile{
		Name:    name,
		BaseURL: strings.TrimRight(strings.TrimSpace(os.Getenv(baseURLEnv)), "/"),
		APIKey:  strings.TrimSpace(os.Getenv(apiKeyEnv)),
		Model:   strings.TrimSpace(os.Getenv(modelEnv)),
	}
	var missing []string
	if profile.BaseURL == "" {
		missing = append(missing, baseURLEnv)
	}
	if profile.APIKey == "" {
		missing = append(missing, apiKeyEnv)
	}
	if profile.Model == "" {
		missing = append(missing, modelEnv)
	}
	return profile, missing
}

// mpLLMUsage — расход токенов вызова модели (msg.Usage SDK): вход, выход и кэш. Попытки
// одного документа суммируются: отчёт сравнивает модели по фактической цене, включая повтор
// после ошибки формата.
type mpLLMUsage struct {
	InputTokens              int64
	OutputTokens             int64
	CacheReadInputTokens     int64
	CacheCreationInputTokens int64
}

// add складывает расход двух вызовов (попыток одного документа).
func (u mpLLMUsage) add(v mpLLMUsage) mpLLMUsage {
	return mpLLMUsage{
		InputTokens:              u.InputTokens + v.InputTokens,
		OutputTokens:             u.OutputTokens + v.OutputTokens,
		CacheReadInputTokens:     u.CacheReadInputTokens + v.CacheReadInputTokens,
		CacheCreationInputTokens: u.CacheCreationInputTokens + v.CacheCreationInputTokens,
	}
}

// mpLLMDocStats — результат разметки одного документа одной моделью: кто отвечал и сколько
// раз, сколько занял документ целиком (включая ретрай) и что вышло по качеству. QuotesTotal —
// всё, что прислала модель (неизвестный slug, пустая цитата и цитата, которой нет в корпусе,
// тоже считаются), QuotesKept — сохранённое в результате, QuotesDropped = QuotesTotal −
// QuotesKept. Notes — записи kind='note', ChannelsWithQuotes — уникальные slug'и реестра,
// у которых есть хотя бы одна сохранённая цитата.
type mpLLMDocStats struct {
	Model              string
	Attempts           int
	Duration           time.Duration
	Usage              mpLLMUsage
	QuotesTotal        int
	QuotesKept         int
	QuotesDropped      int
	Notes              int
	ChannelsWithQuotes int
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

// mpIsJSONDecodeError — ответ не получен целиком или не разобран: обрыв на неполном ответе
// (оборванный блок заголовков или оборванное тело), обрезанный конверт, не-JSON или не та схема.
// Имя историческое (класс шире одного JSON-декодирования), но проверка по типам остаётся точной:
//   - причину SDK оборачивает через %w, поэтому errors.As/errors.Is её и видят: оборванное тело
//     даёт «error reading response body: %w» (от io.ReadAll), неразобранное — «error parsing
//     response json: %w» (от json.Decoder), оборванный блок заголовков net/http приносит
//     транспортной ошибкой (*url.Error) с тем же io.ErrUnexpectedEOF внутри;
//   - оборванное тело и оборванный блок заголовков дают io.ErrUnexpectedEOF (проверено на
//     anthropic-sdk-go v1.80.0), не-JSON мусор (HTML заглушки WAF, отданной как
//     application/json) — *json.SyntaxError, не тот тип поля — *json.UnmarshalTypeError.
//
// Различить «обрыв ответа» и «обрезанный JSON» по типам нельзя — обе ветки приносят один и тот
// же io.ErrUnexpectedEOF, а матчинг по тексту ошибки SDK («error reading response body» /
// «error parsing response json») хрупок и переживёт не любую версию SDK. Поэтому оба случая
// классифицируются форматом осознанно: цена — один повтор, максимум два вызова на документ, а
// событие редкое (обрыв TCP на неполном ответе). См. TestTagMPChannelsLLMBodyReadBroken.
//
// Не формат: отказ провайдера (*anthropic.Error) и всё, что проверка по типам не опознала, —
// ошибки, при которых от провайдера не пришло ни байта ответа (имя не разрешилось, соединение
// не установилось, рукопожатие TLS, дедлайн контекста), а также редкий обрыв, названный net/http
// иначе (например, «malformed …» на мусорной строке статуса): граница проходит не по «транспорт
// против формата», а по «опознан ли неполный ответ». Пустое тело 200 с Content-Type
// application/json SDK тоже отвергает своей ошибкой декодирования («error parsing response json:
// EOF»), но это io.EOF, а не io.ErrUnexpectedEOF: повтора нет — паритет со старым «пустое тело»
// util.PostJSON. Тело с чужим Content-Type (HTML заглушки WAF отдаются как text/html) SDK
// отвергает ещё до json.Decoder — по типам такую ошибку не опознать, и она уходит как есть.
func mpIsJSONDecodeError(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr) || errors.Is(err, io.ErrUnexpectedEOF)
}

// mpLLMTagError — ошибка LLM-разметки документа (провайдер, формат, транспорт): вызывающий
// (импортёр, harness) обрабатывает её как деградацию одного документа до regex-слоя, а не
// сбой прогона, — в отличие от ошибки записи батча, которая идёт наверх.
type mpLLMTagError struct{ err error }

func (e mpLLMTagError) Error() string { return e.err.Error() }
func (e mpLLMTagError) Unwrap() error { return e.err }

// mpIsLLMTagError — ошибка пришла от LLM, а не от батча БД.
func mpIsLLMTagError(err error) bool {
	var te mpLLMTagError
	return errors.As(err, &te)
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
	if start < 0 {
		return "", false
	}
	// Decode первого значения, а не «первая { — последняя }»: OpenAI-совместимые модели
	// иногда повторяют объект (`{"ok":true}!{"ok":true}!…`) или добавляют хвост после JSON;
	// LastIndex склеил бы повтор в невалидный объект, а Decoder берёт ровно первый.
	dec := json.NewDecoder(strings.NewReader(s[start:]))
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil || len(raw) == 0 || raw[0] != '{' {
		return "", false
	}
	return string(raw), true
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

// mpLLMBlockTypes — типы блоков конверта в порядке ответа. Нужна диагностика отказа модели
// (например, «thinking» без text-блока или ответ без JSON): типы уезжают в текст ошибки,
// который пишется в warn и stats, и позволяют отличить отказ провайдера от нарушения схемы.
func mpLLMBlockTypes(msg *anthropic.Message) string {
	types := make([]string, 0, len(msg.Content))
	for _, block := range msg.Content {
		types = append(types, block.Type)
	}
	return strings.Join(types, ",")
}

// mpLLMSnippet — начало текста ответа для диагностики: ошибка не должна разрастаться до
// размера конверта (49 КБ текста в GLM), а первых сотен символов хватает, чтобы увидеть,
// проза это, обрезанный JSON или thinking.
func mpLLMSnippet(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// mpLLMAskOnce — один запрос к провайдеру официальным SDK (Anthropic Messages API:
// POST <baseURL>/v1/messages) и разбор ответа до структуры. При mpLLMTransport == "openai"
// вызов уходит в mpLLMAskOnceOpenAI: тот же промпт, но OpenAI-совместимый JSON-режим
// (GLM/MiMo на Anthropic-эндпоинте уходят в reasoning и не доходят до JSON). Ответ без text-блока, пустой или
// неразобранный content, не та схема JSON и не полученное целиком тело — ошибки формата
// (mpLLMFormatError), их повторяет mpLLMAsk: максимум два вызова на документ. Отказ провайдера
// (*anthropic.Error, не-2xx) и всё, что не опознано проверкой по типам, — ошибки без единого байта
// ответа (имя не разрешилось, соединение не установилось, TLS, дедлайн контекста) и редкий обрыв,
// названный net/http иначе, — уходят как есть: они не формат — повтором не лечатся, и вызывающий
// должен узнать о них сразу, чтобы деградировать до regex-слоя, не тратя второй вызов. Обрыв на
// неполном ответе — оборванный блок заголовков или оборванное тело — уже формат (один повтор): по
// типам он неотличим от обрезанного конверта, см. mpIsJSONDecodeError.
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
//
// Usage — из msg.Usage SDK (input/output и кэш). Конверт уже получен, поэтому расход токенов
// не теряется и на ошибке формата ниже: провайдер токены потратил, и отчёт должен это видеть.
// На отказе провайдера и транспортной ошибке конверта нет — usage нулевой.
func mpLLMAskOnce(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, mpLLMUsage, error) {
	if mpLLMTransport == "openai" {
		return mpLLMAskOnceOpenAI(doc, baseURL, apiKey, model)
	}
	client := anthropic.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(strings.TrimRight(baseURL, "/")),
		option.WithHTTPClient(util.HttpClient),
		option.WithMaxRetries(0),
	)
	ctx, cancel := context.WithTimeout(context.Background(), mpLLMTimeout)
	defer cancel()
	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(mpLLMUserPrompt(doc))),
	}
	// Prefill (только harness): последнее сообщение ассистента «{» заставляет модель продолжать
	// JSON-объект, а не тратить весь лимит на рассуждение в text-блоке (GLM).
	prefill := mpLLMPrefillJSON
	if prefill {
		messages = append(messages, anthropic.NewAssistantMessage(anthropic.NewTextBlock("{")))
	}
	// Thinking disabled (только harness): GLM/MiMo по умолчанию тратят весь max_tokens на
	// рассуждение в text-блоке. Пустая union-структура опускается SDK — прод-запрос не меняется.
	thinking := anthropic.ThinkingConfigParamUnion{}
	if mpLLMThinkingDisabled {
		disabled := anthropic.NewThinkingConfigDisabledParam()
		thinking = anthropic.ThinkingConfigParamUnion{OfDisabled: &disabled}
	}
	msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     model,
		MaxTokens: mpLLMMaxTokens,
		// System — отдельное поле Anthropic-конверта: системной роли в сообщениях там нет.
		System:   []anthropic.TextBlockParam{{Text: mpLLMSystemPrompt()}},
		Messages: messages,
		Thinking: thinking,
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
			return mpLLMPayload{}, mpLLMUsage{}, fmt.Errorf("http status %d: %s", apiErr.StatusCode, util.ErrorSnippet([]byte(apiErr.RawJSON())))
		}
		if mpIsJSONDecodeError(err) {
			return mpLLMPayload{}, mpLLMUsage{}, mpLLMFormatError{err}
		}
		return mpLLMPayload{}, mpLLMUsage{}, err
	}
	usage := mpLLMUsage{
		InputTokens:              msg.Usage.InputTokens,
		OutputTokens:             msg.Usage.OutputTokens,
		CacheReadInputTokens:     msg.Usage.CacheReadInputTokens,
		CacheCreationInputTokens: msg.Usage.CacheCreationInputTokens,
	}
	content := mpLLMText(msg)
	// Ответ на prefill — продолжение объекта без начальной «{»: возвращаем её, чтобы
	// mpLLMJSONBody увидел целый JSON. Если модель всё же начала с «{», не дублируем.
	if prefill && content != "" && !strings.HasPrefix(strings.TrimSpace(content), "{") {
		content = "{" + content
	}
	if content == "" {
		return mpLLMPayload{}, usage, mpLLMFormatError{fmt.Errorf(
			"в ответе нет text-блока (блоки: %q, stop_reason: %q, in/out: %d/%d)",
			mpLLMBlockTypes(msg), msg.StopReason, usage.InputTokens, usage.OutputTokens)}
	}
	body, ok := mpLLMJSONBody(content)
	if !ok {
		return mpLLMPayload{}, usage, mpLLMFormatError{fmt.Errorf(
			"в content нет JSON-объекта (content: %d байт, блоки: %q, stop_reason: %q, начало: %q)",
			len(content), mpLLMBlockTypes(msg), msg.StopReason, mpLLMSnippet(content, 300))}
	}
	var payload mpLLMPayload
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return mpLLMPayload{}, usage, mpLLMFormatError{fmt.Errorf(
			"content не разобран: %w (блоки: %q, stop_reason: %q, начало: %q)",
			err, mpLLMBlockTypes(msg), msg.StopReason, mpLLMSnippet(content, 300))}
	}
	return payload, usage, nil
}

// mpLLMOpenAIRequest/mpLLMOpenAIResponse — конверт OpenAI-совместимого /chat/completions:
// JSON-режим (response_format=json_object) и usage в токенах. Thinking/prefill в этом
// транспорте не используются: JSON-режим сам по себе является совместимостью для моделей,
// которые на Anthropic-эндпоинте уходят в reasoning.
type mpLLMOpenAIRequest struct {
	Model           string                    `json:"model"`
	Messages        []mpLLMOpenAIMessage      `json:"messages"`
	MaxTokens       int64                     `json:"max_tokens"`
	Temperature     float64                   `json:"temperature"`
	ResponseFormat  mpLLMOpenAIResponseFormat `json:"response_format"`
	ReasoningEffort string                    `json:"reasoning_effort,omitempty"`
}

type mpLLMOpenAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type mpLLMOpenAIResponseFormat struct {
	Type string `json:"type"`
}

type mpLLMOpenAIResponse struct {
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens        int64 `json:"prompt_tokens"`
		CompletionTokens    int64 `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// mpLLMOpenAIURL — OpenAI-совместимый путь из base профиля: у GLM base — корень гейтвея,
// у MiMo — Anthropic-префикс /anthropic; обоим нужен /v1/chat/completions. Хвостовой /v1
// не дублируется, если владелец уже указал его в base.
func mpLLMOpenAIURL(baseURL string) string {
	base := strings.TrimRight(baseURL, "/")
	base = strings.TrimSuffix(base, "/anthropic")
	if !strings.HasSuffix(base, "/v1") {
		base += "/v1"
	}
	return base + "/chat/completions"
}

// mpLLMAskOnceOpenAI — один запрос OpenAI-совместимого транспорта (harness): тот же
// system/user-промпт, temperature 0, response_format=json_object, max_tokens из mpLLMMaxTokens.
// Ошибки классифицируются как в Anthropic-пути: отказ провайдера/транспорт — без повтора,
// неполное или неразобранное тело, конверт без choices/content и content без JSON — формат
// (mpLLMFormatError, один повтор в mpLLMAsk). Ключ уходит заголовком Authorization: Bearer и
// в текст ошибки не попадает: тело отказа режется util.ErrorSnippet, как у Anthropic-пути.
func mpLLMAskOnceOpenAI(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, mpLLMUsage, error) {
	request := mpLLMOpenAIRequest{
		Model: model,
		Messages: []mpLLMOpenAIMessage{
			{Role: "system", Content: mpLLMSystemPrompt()},
			{Role: "user", Content: mpLLMUserPrompt(doc)},
		},
		MaxTokens:       mpLLMMaxTokens,
		Temperature:     0,
		ResponseFormat:  mpLLMOpenAIResponseFormat{Type: "json_object"},
		ReasoningEffort: mpLLMReasoningEffort,
	}
	if mpLLMConcise {
		request.Messages = append(request.Messages, mpLLMOpenAIMessage{Role: "user", Content: mpLLMConciseInstruction})
	}
	body, err := json.Marshal(request)
	if err != nil {
		return mpLLMPayload{}, mpLLMUsage{}, fmt.Errorf("запрос не собран: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), mpLLMTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mpLLMOpenAIURL(baseURL), bytes.NewReader(body))
	if err != nil {
		return mpLLMPayload{}, mpLLMUsage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return mpLLMPayload{}, mpLLMUsage{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return mpLLMPayload{}, mpLLMUsage{}, mpLLMFormatError{fmt.Errorf("тело ответа не прочитано: %w", err)}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return mpLLMPayload{}, mpLLMUsage{}, fmt.Errorf("http status %d: %s", resp.StatusCode, util.ErrorSnippet(respBody))
	}
	var envelope mpLLMOpenAIResponse
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return mpLLMPayload{}, mpLLMUsage{}, mpLLMFormatError{fmt.Errorf("конверт не разобран: %w", err)}
	}
	usage := mpLLMUsage{
		InputTokens:          envelope.Usage.PromptTokens,
		OutputTokens:         envelope.Usage.CompletionTokens,
		CacheReadInputTokens: envelope.Usage.PromptTokensDetails.CachedTokens,
	}
	if len(envelope.Choices) == 0 {
		return mpLLMPayload{}, usage, mpLLMFormatError{fmt.Errorf("в конверте нет choices")}
	}
	choice := envelope.Choices[0]
	content := strings.TrimSpace(choice.Message.Content)
	if content == "" {
		return mpLLMPayload{}, usage, mpLLMFormatError{fmt.Errorf(
			"в ответе нет content (finish_reason: %q, in/out: %d/%d)",
			choice.FinishReason, usage.InputTokens, usage.OutputTokens)}
	}
	raw, ok := mpLLMJSONBody(content)
	if !ok {
		err := fmt.Errorf(
			"в content нет JSON-объекта (content: %d байт, finish_reason: %q, начало: %q)",
			len(content), choice.FinishReason, mpLLMSnippet(content, 300))
		// finish_reason=length — ответ обрезан по max_tokens: повтор с тем же лимитом
		// ничего не изменит, поэтому это не ошибка формата и mpLLMAsk его не повторяет.
		if choice.FinishReason == "length" {
			return mpLLMPayload{}, usage, err
		}
		return mpLLMPayload{}, usage, mpLLMFormatError{err}
	}
	var payload mpLLMPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		parseErr := fmt.Errorf(
			"content не разобран: %w (finish_reason: %q, начало: %q)",
			err, choice.FinishReason, mpLLMSnippet(content, 300))
		if choice.FinishReason == "length" {
			return mpLLMPayload{}, usage, parseErr
		}
		return mpLLMPayload{}, usage, mpLLMFormatError{parseErr}
	}
	return payload, usage, nil
}

// mpLLMAsk — запрос с одним ретраем на ошибку формата. Второй подряд провал формата
// возвращается вызывающему: спека §3.5 — разметка документа пропускается (документ остаётся
// с regex-слоем), догон следующим прогоном. Имя модели в warn не секрет: оно же пишется
// в колонку model. Usage суммируется по попыткам, attempts — фактическое число вызовов
// (1 или 2): повтор разрешён только на ошибку формата.
func mpLLMAsk(doc mpSummaryDoc, baseURL, apiKey, model string) (mpLLMPayload, mpLLMUsage, int, error) {
	payload, usage, err := mpLLMAskOnce(doc, baseURL, apiKey, model)
	if err == nil || !mpIsFormatError(err) {
		return payload, usage, 1, err
	}
	log.Warnf("cbr_mp_summary: ответ LLM (%s) не разобран (%v), повтор запроса", model, err)
	payload, retryUsage, err := mpLLMAskOnce(doc, baseURL, apiKey, model)
	return payload, usage.add(retryUsage), 2, err
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
//
// stats заполняется по ходу: Model/Attempts/Usage — из вызова (usage суммирован по попыткам),
// Duration — полное время документа, включая ретрай. QuotesTotal считается до валидации:
// в нём и каналы вне реестра, и пустые цитаты, и цитаты, которых нет в корпусе; QuotesKept —
// сохранённые, QuotesDropped — их разность. Notes — записи kind='note', ChannelsWithQuotes —
// slug'и реестра с сохранённой цитатой. На ошибке LLM stats несёт Model/Attempts/Usage/
// Duration, а счётчики нулевые: разметки нет.
func tagMPChannelsLLM(doc mpSummaryDoc, profile mpLLMProfile) ([]mpChannelMatch, mpLLMDocStats, error) {
	start := time.Now()
	payload, usage, attempts, err := mpLLMAsk(doc, profile.BaseURL, profile.APIKey, profile.Model)
	stats := mpLLMDocStats{Model: profile.Model, Attempts: attempts, Usage: usage}
	if err != nil {
		stats.Duration = time.Since(start)
		return nil, stats, err
	}
	corpus := mpLLMCorpus(doc)
	known := make(map[string]bool, len(mpChannels))
	for _, ch := range mpChannels {
		known[ch.Slug] = true
	}
	for _, ch := range payload.Channels {
		stats.QuotesTotal += len(ch.Quotes)
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
		out         []mpChannelMatch
		seen        = make(map[mpChannelMatchKey]bool)
		quotedSlugs = make(map[string]bool, len(channels))
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
			stats.QuotesKept++
			quotedSlugs[ch.Slug] = true
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
		stats.Notes++
	}
	for i := range out {
		out[i].Seq = i + 1
	}
	stats.QuotesDropped = stats.QuotesTotal - stats.QuotesKept
	stats.ChannelsWithQuotes = len(quotedSlugs)
	stats.Duration = time.Since(start)
	return out, stats, nil
}
