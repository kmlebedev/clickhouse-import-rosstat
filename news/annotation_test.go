package news

import (
	"strings"
	"testing"
	"time"
)

// newsAnnotationInput — входная Telegram-молния для тестов валидации. Текст содержит
// заголовок первой строкой (как у парсера), чтобы evidence проверялся по той же форме,
// которую видит модель в user-промпте.
func newsAnnotationInput() newsItem {
	const title = "Трамп объявил о пошлинах"
	const text = "Трамп объявил о новых пошлинах на импорт из Китая.\nФРС сохраняет ставку."
	return newsItem{
		Date:        time.Date(2026, time.October, 17, 9, 0, 0, 0, time.UTC),
		Source:      "interfax",
		Title:       title,
		URL:         "https://t.me/interfaxonline/80001",
		Description: text,
		ContentHash: contentHash(title, text),
	}
}

func newsAnnotationNow() time.Time {
	return time.Date(2026, time.October, 17, 10, 0, 0, 0, time.UTC)
}

// newsAnnotationKindRows отбирает строки по виду: тесты проверяют doc и tag отдельно,
// потому что набор полей у них разный.
func newsAnnotationKindRows(rows []newsAnnotationRow, kind string) []newsAnnotationRow {
	out := make([]newsAnnotationRow, 0, len(rows))
	for _, row := range rows {
		if row.Kind == kind {
			out = append(out, row)
		}
	}
	return out
}

// TestValidateNewsAnnotationsValid — валидный ответ даёт doc-строку и tag-строки: doc несёт
// sentiment/relevance/summary, tag — dimension/tag/direction/confidence/evidence; method,
// model, prompt_version и annotated_at проставлены на всех строках.
func TestValidateNewsAnnotationsValid(t *testing.T) {
	input := newsAnnotationInput()
	now := newsAnnotationNow()
	payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL:       input.URL,
		Sentiment: 0.5,
		Relevance: 0.8,
		Summary:   "  Трамп объявил о пошлинах.  ",
		Tags: []newsAnnotationTag{
			{Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
				Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"},
			{Dimension: newsDimCountry, Tag: "china", Direction: newsDirectionMixed,
				Confidence: 0.7, Evidence: "импорт из Китая"},
		},
	}}}

	rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, now)
	if err != nil {
		t.Fatalf("validateNewsAnnotations: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("строк %d, want 3 (doc + 2 tag): %+v", len(rows), rows)
	}

	doc := rows[0]
	if doc.Kind != newsKindDoc {
		t.Fatalf("строка 0: kind = %q, want %q (doc идёт первой)", doc.Kind, newsKindDoc)
	}
	if doc.URL != input.URL || doc.ContentHash != input.ContentHash {
		t.Errorf("doc: url/content_hash = %q/%q, want %q/%q",
			doc.URL, doc.ContentHash, input.URL, input.ContentHash)
	}
	if doc.Method != newsMethodLLM || doc.Model != "test-model" || doc.PromptVersion != newsPromptVersion {
		t.Errorf("doc: method/model/prompt_version = %q/%q/%d, want %q/%q/%d",
			doc.Method, doc.Model, doc.PromptVersion, newsMethodLLM, "test-model", newsPromptVersion)
	}
	if doc.Dimension != "" || doc.Tag != "" || doc.Direction != "" || doc.Confidence != 0 || doc.Evidence != "" {
		t.Errorf("doc: теговые поля обязаны быть пустыми: %+v", doc)
	}
	if doc.Sentiment != 0.5 || doc.Relevance != 0.8 {
		t.Errorf("doc: sentiment/relevance = %v/%v, want 0.5/0.8", doc.Sentiment, doc.Relevance)
	}
	if doc.Summary != "Трамп объявил о пошлинах." {
		t.Errorf("doc: summary = %q, want trim без пробелов по краям", doc.Summary)
	}
	if !doc.AnnotatedAt.Equal(now) {
		t.Errorf("doc: annotated_at = %v, want %v", doc.AnnotatedAt, now)
	}

	tags := newsAnnotationKindRows(rows[1:], newsKindTag)
	if len(tags) != 2 {
		t.Fatalf("tag-строк %d, want 2", len(tags))
	}
	byKey := map[string]newsAnnotationRow{tags[0].Dimension + ":" + tags[0].Tag: tags[0],
		tags[1].Dimension + ":" + tags[1].Tag: tags[1]}
	trump, ok := byKey["person:trump"]
	if !ok {
		t.Fatalf("нет тега person:trump: %+v", tags)
	}
	if trump.Direction != newsDirectionNeutral || trump.Confidence != 0.9 ||
		trump.Evidence != "Трамп объявил о новых пошлинах" {
		t.Errorf("person:trump = %+v, want neutral/0.9 с нормализованной цитатой", trump)
	}
	if trump.Sentiment != 0 || trump.Relevance != 0 || trump.Summary != "" {
		t.Errorf("tag: doc-поля обязаны быть нулевыми: %+v", trump)
	}
	china, ok := byKey["country:china"]
	if !ok {
		t.Fatalf("нет тега country:china: %+v", tags)
	}
	if china.Direction != newsDirectionMixed {
		t.Errorf("country:china: direction = %q, want mixed (neutral/mixed допустимы вне cb_channel)", china.Direction)
	}
}

// TestValidateNewsAnnotationsClampsAndTrims — sentiment clamp в [-1, 1], relevance и
// confidence — в [0, 1], summary тримится и обрезается до 300 рун.
func TestValidateNewsAnnotationsClampsAndTrims(t *testing.T) {
	input := newsAnnotationInput()
	longSummary := strings.Repeat("я", 301)
	payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL:       input.URL,
		Sentiment: 5,
		Relevance: -2,
		Summary:   "  " + longSummary + "  ",
		Tags: []newsAnnotationTag{{
			Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 2, Evidence: "Трамп объявил о новых пошлинах",
		}},
	}}}

	rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations: %v", err)
	}
	doc := newsAnnotationKindRows(rows, newsKindDoc)[0]
	if doc.Sentiment != 1 {
		t.Errorf("sentiment = %v, want 1 (clamp сверху)", doc.Sentiment)
	}
	if doc.Relevance != 0 {
		t.Errorf("relevance = %v, want 0 (clamp снизу)", doc.Relevance)
	}
	if got := len([]rune(doc.Summary)); got != newsSummaryMax {
		t.Errorf("summary длиной %d рун, want %d (обрезка)", got, newsSummaryMax)
	}
	tag := newsAnnotationKindRows(rows, newsKindTag)[0]
	if tag.Confidence != 1 {
		t.Errorf("confidence = %v, want 1 (clamp сверху)", tag.Confidence)
	}

	// Отрицательный sentiment и отрицательная confidence — нижние границы.
	payload.Messages[0].Sentiment = -9
	payload.Messages[0].Tags[0].Confidence = -1
	rows, err = validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations (нижние границы): %v", err)
	}
	if doc = newsAnnotationKindRows(rows, newsKindDoc)[0]; doc.Sentiment != -1 {
		t.Errorf("sentiment = %v, want -1 (clamp снизу)", doc.Sentiment)
	}
	if tag = newsAnnotationKindRows(rows, newsKindTag)[0]; tag.Confidence != 0 {
		t.Errorf("confidence = %v, want 0 (clamp снизу)", tag.Confidence)
	}
}

// TestValidateNewsAnnotationsDropsUnknown — URL, dimension, slug и direction вне реестра
// отбрасываются, а doc-строка сообщения остаётся: плохой тег не должен стоить сообщению
// всей разметки.
func TestValidateNewsAnnotationsDropsUnknown(t *testing.T) {
	input := newsAnnotationInput()
	validTag := newsAnnotationTag{
		Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
		Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах",
	}

	for _, tc := range []struct {
		name string
		url  string
		tag  newsAnnotationTag
	}{
		{"неизвестный URL", "https://t.me/interfaxonline/99999", validTag},
		{"неизвестный dimension", input.URL, newsAnnotationTag{
			Dimension: "org", Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"}},
		{"неизвестный slug", input.URL, newsAnnotationTag{
			Dimension: newsDimPerson, Tag: "nobody", Direction: newsDirectionNeutral,
			Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"}},
		{"слаг из другого измерения", input.URL, newsAnnotationTag{
			Dimension: newsDimSector, Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"}},
		{"направление вне четвёрки", input.URL, newsAnnotationTag{
			Dimension: newsDimPerson, Tag: "trump", Direction: "up",
			Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"}},
		{"пустое направление", input.URL, newsAnnotationTag{
			Dimension: newsDimPerson, Tag: "trump",
			Confidence: 0.9, Evidence: "Трамп объявил о новых пошлинах"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
				URL: tc.url, Sentiment: 0, Relevance: 1, Summary: "Резюме.", Tags: []newsAnnotationTag{tc.tag},
			}}}
			rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
			if err != nil {
				t.Fatalf("validateNewsAnnotations: %v", err)
			}
			if tc.url != input.URL {
				if len(rows) != 0 {
					t.Fatalf("строк %d, want 0: неизвестный URL не должен давать doc: %+v", len(rows), rows)
				}
				return
			}
			if got := len(newsAnnotationKindRows(rows, newsKindDoc)); got != 1 {
				t.Fatalf("doc-строк %d, want 1: сообщение вернулось, doc пишется даже без тегов", got)
			}
			if got := len(newsAnnotationKindRows(rows, newsKindTag)); got != 0 {
				t.Errorf("tag-строк %d, want 0 (тег вне реестра/направления отброшен): %+v", got, rows)
			}
		})
	}
}

// TestValidateNewsAnnotationsEvidence — evidence обязан быть дословной подстрокой текста:
// пересказ и пустая цитата отбрасывают тег, а переносы строк и повторные пробелы внутри
// цитаты прощаются (сравнение идёт по нормализованному тексту).
func TestValidateNewsAnnotationsEvidence(t *testing.T) {
	input := newsAnnotationInput()
	for _, tc := range []struct {
		name     string
		evidence string
		wantTag  bool
	}{
		{"дословная цитата", "Трамп объявил о новых пошлинах", true},
		{"нормализованные пробелы", "Трамп  объявил\nо новых пошлинах", true},
		{"пересказ", "Трамп ввёл налог на китайский импорт", false},
		{"пустая цитата", "   \n\t", false},
		{"цитата из заголовка", "Трамп объявил о пошлинах", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
				URL: input.URL, Summary: "Резюме.", Tags: []newsAnnotationTag{{
					Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
					Confidence: 0.5, Evidence: tc.evidence,
				}},
			}}}
			rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
			if err != nil {
				t.Fatalf("validateNewsAnnotations: %v", err)
			}
			got := len(newsAnnotationKindRows(rows, newsKindTag))
			if tc.wantTag && got != 1 {
				t.Errorf("tag-строк %d, want 1 (цитата дословна): %+v", got, rows)
			}
			if !tc.wantTag && got != 0 {
				t.Errorf("tag-строк %d, want 0 (цитата не найдена): %+v", got, rows)
			}
		})
	}
}

// TestValidateNewsAnnotationsDirections — направленные значения у не-cb_channel приводятся
// к neutral с warn, neutral/mixed сохраняются, а у cb_channel направление всегда берётся из
// реестра: ответ модели с другим знаком не отбрасывает тег, а заменяется статическим знаком.
func TestValidateNewsAnnotationsDirections(t *testing.T) {
	input := newsAnnotationInput()
	for _, tc := range []struct {
		name      string
		dimension string
		tag       string
		direction string
		want      string
	}{
		{"персона pro → neutral", newsDimPerson, "trump", newsDirectionPro, newsDirectionNeutral},
		{"персона dez → neutral", newsDimPerson, "trump", newsDirectionDez, newsDirectionNeutral},
		{"персона neutral", newsDimPerson, "trump", newsDirectionNeutral, newsDirectionNeutral},
		{"страна mixed", newsDimCountry, "china", newsDirectionMixed, newsDirectionMixed},
		{"канал: знак совпал с реестром", newsDimCBChannel, "current_inflation", newsDirectionPro, newsDirectionPro},
		{"канал: pro у disinflationary-канала → реестровый dez", newsDimCBChannel, "savings", newsDirectionPro, newsDirectionDez},
		{"канал: neutral у disinflationary-канала → реестровый dez", newsDimCBChannel, "savings", newsDirectionNeutral, newsDirectionDez},
		{"канал: mixed у pro-канала → реестровый pro", newsDimCBChannel, "money_supply", newsDirectionMixed, newsDirectionPro},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
				URL: input.URL, Summary: "Резюме.", Tags: []newsAnnotationTag{{
					Dimension: tc.dimension, Tag: tc.tag, Direction: tc.direction,
					Confidence: 0.5, Evidence: "Трамп объявил о новых пошлинах",
				}},
			}}}
			rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
			if err != nil {
				t.Fatalf("validateNewsAnnotations: %v", err)
			}
			tags := newsAnnotationKindRows(rows, newsKindTag)
			if len(tags) != 1 {
				t.Fatalf("tag-строк %d, want 1: %+v", len(tags), rows)
			}
			if tags[0].Direction != tc.want {
				t.Errorf("direction = %q, want %q", tags[0].Direction, tc.want)
			}
		})
	}
}

// TestValidateNewsAnnotationsDedupesAndLimits — дубли (dimension, tag) схлопываются с
// сохранением большей confidence, а тегов на сообщение остаётся не больше пяти; порядок —
// confidence desc (при равенстве — dimension/tag), поэтому он воспроизводим.
func TestValidateNewsAnnotationsDedupesAndLimits(t *testing.T) {
	input := newsAnnotationInput()
	const quote = "Трамп объявил о новых пошлинах"
	mkTag := func(dimension, slug string, confidence float32) newsAnnotationTag {
		return newsAnnotationTag{Dimension: dimension, Tag: slug, Direction: newsDirectionNeutral,
			Confidence: confidence, Evidence: quote}
	}

	// Семь разных тегов реестра — все с валидной цитатой; лишние два обязаны отсечься.
	payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL: input.URL, Summary: "Резюме.",
		Tags: []newsAnnotationTag{
			mkTag(newsDimPerson, "trump", 0.1),
			mkTag(newsDimPerson, "putin", 0.2),
			mkTag(newsDimCountry, "china", 0.3),
			mkTag(newsDimCountry, "usa", 0.4),
			mkTag(newsDimSector, "oil_gas", 0.5),
			mkTag(newsDimEvent, "tariffs", 0.6),
			mkTag(newsDimCBChannel, "current_inflation", 0.7),
			// Дубль person:trump с большей уверенностью: остаться должна эта версия.
			mkTag(newsDimPerson, "trump", 0.95),
		},
	}}}
	rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations: %v", err)
	}
	tags := newsAnnotationKindRows(rows, newsKindTag)
	if len(tags) != newsMaxTagsPerMessage {
		t.Fatalf("tag-строк %d, want %d (не более пяти): %+v", len(tags), newsMaxTagsPerMessage, tags)
	}
	keys := make([]string, 0, len(tags))
	for i, tag := range tags {
		keys = append(keys, tag.Dimension+":"+tag.Tag)
		if i > 0 && tags[i-1].Confidence < tag.Confidence {
			t.Errorf("порядок тегов не по confidence desc: %v", tags)
		}
	}
	if got := strings.Join(keys, ","); got != "person:trump,cb_channel:current_inflation,event:tariffs,sector:oil_gas,country:usa" {
		t.Errorf("состав/порядок тегов = %s, want five самых уверенных по убыванию", got)
	}
	for _, tag := range tags {
		if tag.Dimension == newsDimPerson && tag.Tag == "trump" {
			if tag.Confidence != 0.95 {
				t.Errorf("дубль person:trump: confidence = %v, want 0.95 (остаётся большая)", tag.Confidence)
			}
		}
	}
}

// TestValidateNewsAnnotationsMissingURLNoDoc — ответ без сообщения не даёт doc-строки:
// сообщение догоняется следующим прогоном, а не считается размеченным.
func TestValidateNewsAnnotationsMissingURLNoDoc(t *testing.T) {
	first := newsAnnotationInput()
	second := first
	second.URL = "https://t.me/interfaxonline/80002"
	second.Title = "ФРС сохранила ставку"
	second.Description = "ФРС сохранила ставку на прежнем уровне."
	second.ContentHash = contentHash(second.Title, second.Description)

	payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL: first.URL, Summary: "Первое.",
		Tags: []newsAnnotationTag{{Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 0.5, Evidence: "Трамп объявил о новых пошлинах"}},
	}}}
	rows, err := validateNewsAnnotations(payload, []newsItem{first, second}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations: %v", err)
	}
	docs := newsAnnotationKindRows(rows, newsKindDoc)
	if len(docs) != 1 || docs[0].URL != first.URL {
		t.Fatalf("doc-строки = %+v, want одна на вернувшееся сообщение %s (второе догоняется)", docs, first.URL)
	}
	for _, row := range rows {
		if row.URL == second.URL {
			t.Errorf("для отсутствующего в ответе URL %s написана строка: %+v", second.URL, row)
		}
	}
}

// TestValidateNewsAnnotationsEmpty — пустой ответ не ошибка и не даёт строк: догон
// следующим прогоном. Doc без тегов, наоборот, штатен и обязан записаться.
func TestValidateNewsAnnotationsEmpty(t *testing.T) {
	input := newsAnnotationInput()

	rows, err := validateNewsAnnotations(newsAnnotationPayload{}, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("пустой payload: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("пустой payload дал %d строк, want 0", len(rows))
	}

	rows, err = validateNewsAnnotations(newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL: input.URL, Sentiment: 0.1, Relevance: 0.2, Summary: "Резюме.",
	}}}, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("doc без тегов: %v", err)
	}
	if len(rows) != 1 || rows[0].Kind != newsKindDoc {
		t.Errorf("doc без тегов = %+v, want ровно одна doc-строка", rows)
	}
}

// TestValidateNewsAnnotationsDuplicateURL — повторный ответ на тот же URL не даёт второго
// doc-ряда: ключ ReplacingMergeTree один, и детерминированно берётся первый ответ.
func TestValidateNewsAnnotationsDuplicateURL(t *testing.T) {
	input := newsAnnotationInput()
	payload := newsAnnotationPayload{Messages: []newsAnnotationMessage{
		{URL: input.URL, Summary: "Первый."},
		{URL: input.URL, Summary: "Второй."},
	}}
	rows, err := validateNewsAnnotations(payload, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations: %v", err)
	}
	docs := newsAnnotationKindRows(rows, newsKindDoc)
	if len(docs) != 1 || docs[0].Summary != "Первый." {
		t.Fatalf("doc-строки = %+v, want один первый ответ", docs)
	}
}

// TestNewsEvidenceTruncated — цитата длиннее newsEvidenceMax обрезается до проверки подстроки:
// модель вправе вернуть длинный фрагмент, и хвост за пределами 300 рун не должен отбрасывать
// тег. Цитата, которой нет в тексте и после обрезки, по-прежнему отбрасывается.
func TestNewsEvidenceTruncated(t *testing.T) {
	input := newsAnnotationInput()
	input.Title = "Длинный пост"
	input.Description = strings.Repeat("я", 400)
	input.ContentHash = contentHash(input.Title, input.Description)

	accepted := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL: input.URL, Summary: "Резюме.", Tags: []newsAnnotationTag{{
			Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 0.5,
			// Первые 300 рун — подстрока текста, хвост 100 рун — уже нет.
			Evidence: strings.Repeat("я", newsEvidenceMax) + strings.Repeat("ю", 100),
		}},
	}}}
	rows, err := validateNewsAnnotations(accepted, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations (цитата 400 рун): %v", err)
	}
	tags := newsAnnotationKindRows(rows, newsKindTag)
	if len(tags) != 1 {
		t.Fatalf("tag-строк %d, want 1 (первые %d рун цитаты — подстрока текста): %+v",
			len(tags), newsEvidenceMax, rows)
	}
	if got := len([]rune(tags[0].Evidence)); got != newsEvidenceMax {
		t.Errorf("длина evidence = %d рун, want %d (обрезка до проверки подстроки)", got, newsEvidenceMax)
	}

	// Цитата вне текста не принимается и после обрезки: обрезка — не амнистия пересказу.
	rejected := newsAnnotationPayload{Messages: []newsAnnotationMessage{{
		URL: input.URL, Summary: "Резюме.", Tags: []newsAnnotationTag{{
			Dimension: newsDimPerson, Tag: "trump", Direction: newsDirectionNeutral,
			Confidence: 0.5, Evidence: strings.Repeat("ф", 400),
		}},
	}}}
	rows, err = validateNewsAnnotations(rejected, []newsItem{input}, "test-model", newsPromptVersion, newsAnnotationNow())
	if err != nil {
		t.Fatalf("validateNewsAnnotations (цитата вне текста): %v", err)
	}
	if got := len(newsAnnotationKindRows(rows, newsKindTag)); got != 0 {
		t.Errorf("tag-строк %d, want 0 (цитаты нет в тексте): %+v", got, rows)
	}
}
