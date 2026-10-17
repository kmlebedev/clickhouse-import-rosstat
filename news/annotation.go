package news

import (
	"math"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// newsAnnotationTable — generic-таблица LLM-разметки по паре (url, content_hash).
	// news_watch заполняет её для всего видимого потока (Telegram и RSS), но ключ не
	// привязан к транспорту: смена ленты не меняет схему.
	newsAnnotationTable = "news_annotation"

	// Виды строк: doc — одна строка на сообщение (тональность, релевантность, резюме),
	// tag — по строке на тег. method='llm' отделяет модельную разметку от будущих слоёв.
	newsKindDoc   = "doc"
	newsKindTag   = "tag"
	newsMethodLLM = "llm"
	// newsSummaryMax — предел summary в рунах: одно предложение, длиннее модель не пишет.
	newsSummaryMax = 300
	// newsEvidenceMax — предел цитаты-обоснования в рунах: длинная цитата обрезается до
	// проверки подстроки, иначе тег отбрасывался бы из-за хвоста, а не из-за пересказа.
	newsEvidenceMax       = 300
	newsMaxTagsPerMessage = 5
)

// newsAnnotationPayload — разобранный content ответа модели (схема — в newsLLMSystemPrompt).
type newsAnnotationPayload struct {
	Messages []newsAnnotationMessage `json:"messages"`
}

// newsAnnotationMessage — разметка одного сообщения: тональность/релевантность/резюме и теги.
type newsAnnotationMessage struct {
	URL       string              `json:"url"`
	Sentiment float32             `json:"sentiment"`
	Relevance float32             `json:"relevance"`
	Summary   string              `json:"summary"`
	Tags      []newsAnnotationTag `json:"tags"`
}

// newsAnnotationTag — тег ответа модели: измерение и слаг реестра, направление влияния на
// инфляцию, уверенность и дословная цитата-обоснование.
type newsAnnotationTag struct {
	Dimension  string  `json:"dimension"`
	Tag        string  `json:"tag"`
	Direction  string  `json:"direction"`
	Confidence float32 `json:"confidence"`
	Evidence   string  `json:"evidence"`
}

// newsAnnotationRow — строка news_annotation во всех колонках канона. Порядок полей — контракт
// с порядком Append в insertNewsAnnotations и явным списком колонок newsAnnotationInsert.
type newsAnnotationRow struct {
	URL           string
	ContentHash   string
	Kind          string
	Method        string
	Model         string
	PromptVersion uint16
	Dimension     string
	Tag           string
	Direction     string
	Confidence    float32
	Evidence      string
	Sentiment     float32
	Relevance     float32
	Summary       string
	AnnotatedAt   time.Time
}

// validateNewsAnnotations превращает ответ модели в строки news_annotation, проверяя его
// против входных сообщений и реестра тегов. Всё, чего нет во входе или реестре, отбрасывается
// с warn: модель не имеет права добавить URL, тег или направление, которых не было в запросе
// или в реестре.
//
// Контракт:
//   - doc-строка пишется на каждое входное сообщение, которое вернулось в ответе, даже без
//     тегов: иначе следующая пара (model, prompt_version) переспрашивала бы его снова.
//     Сообщение, которого в ответе нет, doc-строки не получает — догон следующим прогоном.
//   - sentiment clamp в [-1, 1], relevance — в [0, 1], summary trim и обрезка до 300 рун.
//   - tag-строка требует пару (dimension, tag) из реестра, направление из четвёрки
//     proinflationary|disinflationary|neutral|mixed и evidence — дословную подстроку
//     нормализованного текста сообщения. У cb_channel направление всегда берётся из реестра
//     (статический знак канала; расхождение с ответом модели — warn), у остальных измерений
//     proinflationary/disinflationary приводятся к neutral: знак задаёт канал ставки, а не
//     персона или сектор.
//   - дубли (dimension, tag) схлопываются (остаётся большая confidence), тегов не больше
//     пяти — сортировка по confidence desc с детерминированным tie-break.
//
// Ошибка здесь не сигнал деградации: некорректные элементы отбрасываются поштучно, а
// оставшиеся строки всё равно пишутся. Возвращаемая ошибка — на случай будущих жёстких
// проверок; сейчас функция всегда возвращает nil.
func validateNewsAnnotations(payload newsAnnotationPayload, inputs []newsItem, model string,
	promptVersion uint16, now time.Time) ([]newsAnnotationRow, error) {
	byURL := make(map[string]newsItem, len(inputs))
	for _, it := range inputs {
		byURL[it.URL] = it
	}

	seen := make(map[string]struct{}, len(payload.Messages))
	rows := make([]newsAnnotationRow, 0, len(payload.Messages)*3)
	for _, msg := range payload.Messages {
		url := strings.TrimSpace(msg.URL)
		it, ok := byURL[url]
		if url == "" || !ok {
			log.Warnf("news_watch: LLM вернул URL %q вне набора разметки, сообщение пропущено", msg.URL)
			continue
		}
		if _, dup := seen[url]; dup {
			log.Warnf("news_watch: LLM вернул URL %q повторно, второй ответ пропущен", url)
			continue
		}
		seen[url] = struct{}{}

		rows = append(rows, newsAnnotationRow{
			URL:           url,
			ContentHash:   it.ContentHash,
			Kind:          newsKindDoc,
			Method:        newsMethodLLM,
			Model:         model,
			PromptVersion: promptVersion,
			Sentiment:     clampNewsFloat(msg.Sentiment, -1, 1),
			Relevance:     clampNewsFloat(msg.Relevance, 0, 1),
			Summary:       truncateNewsRunes(strings.TrimSpace(msg.Summary), newsSummaryMax),
			AnnotatedAt:   now,
		})
		rows = append(rows, validateNewsAnnotationTags(it, msg.Tags, model, promptVersion, now)...)
	}
	return rows, nil
}

// validateNewsAnnotationTags проверяет теги одного сообщения и возвращает tag-строки в
// детерминированном порядке: confidence desc, затем dimension и tag по возрастанию. Пустой
// результат — штатный: doc-строку без тегов пишет вызывающий.
func validateNewsAnnotationTags(it newsItem, tags []newsAnnotationTag, model string,
	promptVersion uint16, now time.Time) []newsAnnotationRow {
	corpus := normNewsText(it.Title + "\n" + it.Description)
	rows := make([]newsAnnotationRow, 0, len(tags))
	index := make(map[string]int, len(tags))
	for _, tag := range tags {
		dimension := strings.TrimSpace(tag.Dimension)
		slug := strings.TrimSpace(tag.Tag)
		registered, ok := newsTagByKey(dimension, slug)
		if !ok {
			log.Warnf("news_watch: LLM вернул тег %s:%s вне реестра, тег пропущен", dimension, slug)
			continue
		}

		direction := strings.TrimSpace(tag.Direction)
		if !newsDirectionAllowed(direction) {
			log.Warnf("news_watch: тег %s:%s: направление %q вне proinflationary|disinflationary|neutral|mixed, тег пропущен",
				dimension, slug, tag.Direction)
			continue
		}
		if registered.Dimension == newsDimCBChannel {
			// Реестр — источник истины о знаке канала: модель вправе ошибиться в
			// направлении, но тег не отбрасывается, а принимает статический знак.
			if direction != registered.Direction {
				log.Warnf("news_watch: тег %s:%s: направление %q не совпадает со знаком реестра %q, принят реестровый",
					dimension, slug, direction, registered.Direction)
			}
			direction = registered.Direction
		} else if direction != newsDirectionNeutral && direction != newsDirectionMixed {
			log.Warnf("news_watch: тег %s:%s: направление %q допустимо только у cb_channel, приведено к %s",
				dimension, slug, direction, newsDirectionNeutral)
			direction = newsDirectionNeutral
		}

		// Цитата нормализуется и обрезается до newsEvidenceMax рун: модель вправе вернуть
		// длинный фрагмент, но evidence — обоснование тега, а не пересказ. После обрезки
		// пустая цитата отбрасывается тем же drop, что и пустая из ответа.
		evidence := truncateNewsRunes(normNewsText(tag.Evidence), newsEvidenceMax)
		if evidence == "" {
			log.Warnf("news_watch: тег %s:%s: пустая цитата, тег пропущен", dimension, slug)
			continue
		}
		if !strings.Contains(corpus, evidence) {
			log.Warnf("news_watch: тег %s:%s: цитаты нет в тексте сообщения %s, тег пропущен: %.80q",
				dimension, slug, it.URL, evidence)
			continue
		}

		row := newsAnnotationRow{
			URL:           it.URL,
			ContentHash:   it.ContentHash,
			Kind:          newsKindTag,
			Method:        newsMethodLLM,
			Model:         model,
			PromptVersion: promptVersion,
			Dimension:     dimension,
			Tag:           slug,
			Direction:     direction,
			Confidence:    clampNewsFloat(tag.Confidence, 0, 1),
			Evidence:      evidence,
			AnnotatedAt:   now,
		}
		key := dimension + "\x00" + slug
		if i, dup := index[key]; dup {
			// Дубль того же тега: остаётся более уверенная версия (при равенстве — первая).
			if rows[i].Confidence < row.Confidence {
				rows[i] = row
			}
			continue
		}
		index[key] = len(rows)
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Confidence != rows[j].Confidence {
			return rows[i].Confidence > rows[j].Confidence
		}
		if rows[i].Dimension != rows[j].Dimension {
			return rows[i].Dimension < rows[j].Dimension
		}
		return rows[i].Tag < rows[j].Tag
	})
	if len(rows) > newsMaxTagsPerMessage {
		rows = rows[:newsMaxTagsPerMessage]
	}
	return rows
}

// newsDirectionAllowed — направление из четвёрки, которую принимает колонка direction.
func newsDirectionAllowed(direction string) bool {
	switch direction {
	case newsDirectionPro, newsDirectionDez, newsDirectionNeutral, newsDirectionMixed:
		return true
	}
	return false
}

// clampNewsFloat ограничивает значение диапазоном [lo, hi]. NaN и ±Inf приводятся к нулю:
// json.Unmarshal их не пропустит, но защита дешёвая, а NaN в Float32-колонке бессмыслен.
func clampNewsFloat(v, lo, hi float32) float32 {
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
		return 0
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// truncateNewsRunes обрезает строку до limit рун (не байт): summary пишется в String, но
// смысловой предел — 300 символов, и кириллица не должна резаться по байтам.
func truncateNewsRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit])
}

// normNewsText схлопывает все пробельные последовательности (переводы строк, табуляции,
// повторные пробелы) в один пробел и обрезает края. По этой форме сравнивается evidence:
// модель вправе перенести строку внутри цитаты, но не вправе пересказать её.
func normNewsText(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
