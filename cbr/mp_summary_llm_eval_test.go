package cbr

// Агрегаты harness сравнения LLM-моделей (спека §3.4–3.5). Файл без build tag: чистые
// функции проверяются обычным `go test ./cbr` и идут в make all, а живой прогон
// (mp_summary_llm_eval_live_test.go, //go:build mpeval) только читает ClickHouse, вызывает
// эти функции и пишет артефакты. Держать агрегаты отдельно от живого теста нужно затем,
// чтобы percentile/Jaccard/дедуп/цена проверялись в CI, а сеть и модель в CI не ходили.

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"testing"
)

// mpEvalPair — ключ пары (модель, дата заседания): по нему stats.jsonl дедуплицируется
// «последней записью», а прогон отличает успешную пару от пары со сбоем.
type mpEvalPair struct {
	Model       string
	MeetingDate string
}

// mpEvalRecord — одна строка stats.jsonl: результат разметки одного документа одной моделью.
// Это и есть единица всех агрегатов; поля JSON — контракт с артефактом (спека §3.4).
// DurationMS — полное время документа, включая ретрай; Usage — сумма по попыткам.
type mpEvalRecord struct {
	Profile                  string `json:"profile"`
	Model                    string `json:"model"`
	MeetingDate              string `json:"meeting_date"`
	Attempts                 int    `json:"attempts"`
	DurationMS               int64  `json:"duration_ms"`
	InputTokens              int64  `json:"input_tokens"`
	OutputTokens             int64  `json:"output_tokens"`
	CacheReadInputTokens     int64  `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64  `json:"cache_creation_input_tokens"`
	QuotesTotal              int    `json:"quotes_total"`
	QuotesKept               int    `json:"quotes_kept"`
	QuotesDropped            int    `json:"quotes_dropped"`
	Notes                    int    `json:"notes"`
	ChannelsWithQuotes       int    `json:"channels_with_quotes"`
	Rows                     int    `json:"rows"`
	Error                    string `json:"error"`
}

// mpEvalPairOf — ключ дедупликации записи.
func mpEvalPairOf(r mpEvalRecord) mpEvalPair {
	return mpEvalPair{Model: r.Model, MeetingDate: r.MeetingDate}
}

// mpEvalLatestRecords — последняя запись на пару (model, meeting_date). stats.jsonl —
// журнал попыток: при RESUME=1 пара со сбоем повторяется, поэтому в файле для неё может
// лежать несколько строк, а summary.csv обязан показать итог, а не историю. Порядок —
// первого появления пары: так вывод воспроизводим и не зависит от map-итерации.
func mpEvalLatestRecords(records []mpEvalRecord) []mpEvalRecord {
	index := make(map[mpEvalPair]int, len(records))
	out := make([]mpEvalRecord, 0, len(records))
	for _, r := range records {
		key := mpEvalPairOf(r)
		if i, ok := index[key]; ok {
			out[i] = r
			continue
		}
		index[key] = len(out)
		out = append(out, r)
	}
	return out
}

// mpEvalPercentile — p-й процентиль набора (p в долях 0…1) методом ближайшего ранга:
// ceil(p·n)-й элемент отсортированного ряда, без интерполяции. Отчёт сравнивает модели по
// p50/p95 латентности, и ближайший ранг даёт воспроизводимое число без спора о том, как
// интерполировать между двумя замерами. Пустой набор — 0 (в отчёте такие ячейки — «н/д»).
func mpEvalPercentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// mpEvalSpeedStats — строка таблицы скорости по одной модели. LatencySamples — число
// успешных вызовов без повтора (только по ним считаются total/mean/p50/p95: ретрай уже
// учтён в DurationMS документа и исказил бы «чистую» латентность модели). Токены —
// фактический расход по всем записям, включая повторы и сбои: провайдер списал их и
// отчёт о цене обязан это видеть.
type mpEvalSpeedStats struct {
	Calls                    int
	Failures                 int
	Retries                  int
	LatencySamples           int
	TotalMS                  int64
	MeanMS                   float64
	P50MS                    int64
	P95MS                    int64
	InputTokens              int64
	OutputTokens             int64
	CacheReadInputTokens     int64
	CacheCreationInputTokens int64
	OutputTokensPerSecond    float64
}

// mpEvalSpeed — агрегат скорости по модели. Повтор — любая запись с Attempts>1 (в том
// числе сбойная: mpLLMAsk повторяет только ошибку формата, и повторный вызов мог не
// вернуть конверт). OutputTokensPerSecond — output-токены успешных вызовов без повтора на
// сумму их длительностей: именно это «скорость генерации», а не время с учётом ретраев.
func mpEvalSpeed(records []mpEvalRecord, model string) mpEvalSpeedStats {
	var s mpEvalSpeedStats
	var latencies []int64
	var fastOutputTokens, fastDurationMS int64
	for _, r := range records {
		if r.Model != model {
			continue
		}
		s.Calls++
		s.InputTokens += r.InputTokens
		s.OutputTokens += r.OutputTokens
		s.CacheReadInputTokens += r.CacheReadInputTokens
		s.CacheCreationInputTokens += r.CacheCreationInputTokens
		if r.Error != "" {
			s.Failures++
		}
		if r.Attempts > 1 {
			s.Retries++
		}
		if r.Error != "" || r.Attempts != 1 {
			continue
		}
		latencies = append(latencies, r.DurationMS)
		s.TotalMS += r.DurationMS
		fastOutputTokens += r.OutputTokens
		fastDurationMS += r.DurationMS
	}
	s.LatencySamples = len(latencies)
	if len(latencies) > 0 {
		s.MeanMS = float64(s.TotalMS) / float64(len(latencies))
		s.P50MS = mpEvalPercentile(latencies, 0.50)
		s.P95MS = mpEvalPercentile(latencies, 0.95)
	}
	if fastDurationMS > 0 {
		s.OutputTokensPerSecond = float64(fastOutputTokens) * 1000 / float64(fastDurationMS)
	}
	return s
}

// mpEvalQualityStats — строка таблицы качества по одной модели. Считается по успешным
// вызовам: сбой не имеет цитат и заметок, и разбавлять им средние значило бы выдавать
// «модель разметила хуже» за «модель не ответила». Сбои вынесены в Failures.
type mpEvalQualityStats struct {
	Docs                   int
	Failures               int
	QuotesTotal            int
	QuotesKept             int
	QuotesDropped          int
	DropRate               float64
	ChannelsWithQuotesSum  int
	MeanChannelsWithQuotes float64
	FullCoverage           int
	MeanNotes              float64
}

// mpEvalQuality — агрегат качества по модели: drop rate = отброшенные / все цитаты ответа,
// покрытие = среднее число каналов с сохранённой цитатой из len(mpChannels), FullCoverage —
// документы с полным покрытием, MeanNotes — среднее число заметок.
func mpEvalQuality(records []mpEvalRecord, model string) mpEvalQualityStats {
	var q mpEvalQualityStats
	var notesSum int
	for _, r := range records {
		if r.Model != model {
			continue
		}
		if r.Error != "" {
			q.Failures++
			continue
		}
		q.Docs++
		q.QuotesTotal += r.QuotesTotal
		q.QuotesKept += r.QuotesKept
		q.QuotesDropped += r.QuotesDropped
		q.ChannelsWithQuotesSum += r.ChannelsWithQuotes
		notesSum += r.Notes
		if r.ChannelsWithQuotes == len(mpChannels) {
			q.FullCoverage++
		}
	}
	if q.QuotesTotal > 0 {
		q.DropRate = float64(q.QuotesDropped) / float64(q.QuotesTotal)
	}
	if q.Docs > 0 {
		q.MeanChannelsWithQuotes = float64(q.ChannelsWithQuotesSum) / float64(q.Docs)
		q.MeanNotes = float64(notesSum) / float64(q.Docs)
	}
	return q
}

// mpEvalModelNames — модели в порядке первого появления в журнале: таблицы отчёта идут в
// порядке профилей прогона, а не в случайном порядке map.
func mpEvalModelNames(records []mpEvalRecord) []string {
	seen := make(map[string]bool, len(records))
	models := make([]string, 0, len(records))
	for _, r := range records {
		if seen[r.Model] {
			continue
		}
		seen[r.Model] = true
		models = append(models, r.Model)
	}
	return models
}

// mpEvalCost — цена токенов в USD по тарифу за 1М токенов (вход/выход). Тариф задаётся
// env-переменными <PROFILE>_LLM_PRICE_IN/_OUT; если их нет, вызывающий пишет «н/д», а не
// выдумывает цену (спека §3.5).
func mpEvalCost(inputTokens, outputTokens int64, priceIn, priceOut float64) float64 {
	return (float64(inputTokens)*priceIn + float64(outputTokens)*priceOut) / 1_000_000
}

// mpEvalDocChannel — ключ множества цитат: документ (дата заседания) и канал. Jaccard
// считается по каждому такому ключу отдельно, а затем макро-усредняется: иначе документ с
// сотней цитат задавил бы документ с одной.
type mpEvalDocChannel struct {
	MeetingDate string
	Channel     string
}

// mpEvalQuoteRow — строка витрины v_cbr_mp_summary_channels, участвующая в Jaccard
// (цитата, не note): дата заседания, канал и дословный (нормализованный) текст.
type mpEvalQuoteRow struct {
	MeetingDate string
	Channel     string
	Text        string
}

// mpEvalJaccardResult — Jaccard и число учтённых пар (meeting_date, channel) с непустым
// объединением. Pairs=0 — сравнивать нечего, в отчёте «н/д», а не 0.
type mpEvalJaccardResult struct {
	Score float64
	Pairs int
}

// mpEvalQuoteSets — множества нормализованных текстов по (meeting_date, channel). Пустые
// тексты пропускаются: витрина их не хранит, но защита бесплатна. Повтор текста внутри
// пары схлопывается множеством — Jaccard считает согласие, а не число упоминаний.
func mpEvalQuoteSets(rows []mpEvalQuoteRow) map[mpEvalDocChannel]map[string]bool {
	sets := make(map[mpEvalDocChannel]map[string]bool, len(rows))
	for _, row := range rows {
		text := normMPText(row.Text)
		if text == "" {
			continue
		}
		key := mpEvalDocChannel{MeetingDate: row.MeetingDate, Channel: row.Channel}
		if sets[key] == nil {
			sets[key] = make(map[string]bool)
		}
		sets[key][text] = true
	}
	return sets
}

// mpEvalJaccard — макро-среднее Jaccard по парам (meeting_date, channel), у которых
// объединение множеств непусто; пары «пусто с обеих сторон» не учитываются, иначе
// отсутствие канала в документе разбавляло бы согласие. fuzzy=true — совпадение цитат по
// включению нормализованных текстов (как в спеке §3.5 для regex-слоя: регекс даёт целое
// предложение, модель может процитировать его фрагмент); fuzzy=false — точное равенство.
//
// Для fuzzy пересечение — жадное паросочетание по отсортированным текстам: сначала ищется
// точное равенство, затем вхождение в любую сторону. Жадность — осознанное упрощение:
// тексты цитат почти не пересекаются частично, а детерминированный результат важнее
// теоретического максимума паросочетания.
func mpEvalJaccard(a, b map[mpEvalDocChannel]map[string]bool, fuzzy bool) mpEvalJaccardResult {
	keys := make(map[mpEvalDocChannel]bool, len(a)+len(b))
	for key := range a {
		keys[key] = true
	}
	for key := range b {
		keys[key] = true
	}
	sorted := make([]mpEvalDocChannel, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].MeetingDate != sorted[j].MeetingDate {
			return sorted[i].MeetingDate < sorted[j].MeetingDate
		}
		return sorted[i].Channel < sorted[j].Channel
	})

	var (
		sum   float64
		pairs int
	)
	for _, key := range sorted {
		as, bs := a[key], b[key]
		if len(as) == 0 && len(bs) == 0 {
			continue
		}
		var inter int
		if fuzzy {
			inter = mpEvalFuzzyIntersection(as, bs)
		} else {
			for text := range as {
				if bs[text] {
					inter++
				}
			}
		}
		union := len(as) + len(bs) - inter
		if union == 0 {
			continue
		}
		sum += float64(inter) / float64(union)
		pairs++
	}
	if pairs == 0 {
		return mpEvalJaccardResult{}
	}
	return mpEvalJaccardResult{Score: sum / float64(pairs), Pairs: pairs}
}

// mpEvalFuzzyIntersection — мощность пересечения двух множеств текстов по включению:
// цитата считается найденной, если нормализованные тексты входят друг в друга (спека §3.5).
func mpEvalFuzzyIntersection(as, bs map[string]bool) int {
	a := mpEvalSortedTexts(as)
	b := mpEvalSortedTexts(bs)
	used := make([]bool, len(b))
	matched := 0
	for _, x := range a {
		idx := -1
		// Точное совпадение сильнее частичного: при равном вхождении оно не должно
		// проиграть более длинному тексту, случайно содержащему цитату.
		for i, y := range b {
			if !used[i] && x == y {
				idx = i
				break
			}
		}
		if idx < 0 {
			for i, y := range b {
				if used[i] || (!strings.Contains(x, y) && !strings.Contains(y, x)) {
					continue
				}
				idx = i
				break
			}
		}
		if idx >= 0 {
			used[idx] = true
			matched++
		}
	}
	return matched
}

// mpEvalSortedTexts — ключи множества отсортированными: fuzzy-паросочетание обязано быть
// воспроизводимым между прогонами, а итерация map в Go случайна.
func mpEvalSortedTexts(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for text := range set {
		out = append(out, text)
	}
	sort.Strings(out)
	return out
}

// TestMPLLMEvalPercentile — ближайший ранг без интерполяции: p50/p95 отчёта должны быть
// воспроизводимы, а не зависеть от способа сглаживания. Проверяются края (0, 1), пустой
// набор и неотсортированный вход.
func TestMPLLMEvalPercentile(t *testing.T) {
	values := []int64{50, 10, 40, 20, 30}
	cases := []struct {
		p    float64
		want int64
	}{
		{0, 10},
		{0.5, 30},
		{0.95, 50},
		{1, 50},
	}
	for _, c := range cases {
		if got := mpEvalPercentile(values, c.p); got != c.want {
			t.Errorf("mpEvalPercentile(%v, %.2f) = %d, ожидалось %d", values, c.p, got, c.want)
		}
	}
	if got := mpEvalPercentile(nil, 0.5); got != 0 {
		t.Errorf("mpEvalPercentile(пусто) = %d, ожидалось 0", got)
	}
	if got := mpEvalPercentile([]int64{7}, 0.95); got != 7 {
		t.Errorf("mpEvalPercentile(один элемент) = %d, ожидалось 7", got)
	}
	// Вход не должен меняться: функция сортирует копию.
	if values[0] != 50 {
		t.Errorf("mpEvalPercentile изменил вход: %v", values)
	}
}

// TestMPLLMEvalLatestRecords — дедуп stats.jsonl последней записью пары: при RESUME=1
// сбойная запись повторяется, и summary.csv обязан показать итог, а не первую попытку.
// Последняя запись побеждает независимо от успеха: история попыток остаётся в stats.jsonl.
func TestMPLLMEvalLatestRecords(t *testing.T) {
	records := []mpEvalRecord{
		{Model: "m1", MeetingDate: "2026-09-11", Attempts: 1, Error: "500"},
		{Model: "m1", MeetingDate: "2026-09-11", Attempts: 1, QuotesKept: 5},
		{Model: "m1", MeetingDate: "2024-02-16", Attempts: 1},
		{Model: "m2", MeetingDate: "2026-09-11", Attempts: 1, Error: "timeout"},
		{Model: "m1", MeetingDate: "2026-09-11", Attempts: 2, QuotesKept: 7},
	}

	latest := mpEvalLatestRecords(records)
	if len(latest) != 3 {
		t.Fatalf("пар %d, ожидалось 3: %+v", len(latest), latest)
	}
	if got := latest[0]; got.MeetingDate != "2026-09-11" || got.Attempts != 2 || got.QuotesKept != 7 || got.Error != "" {
		t.Errorf("пара (m1, 2026-09-11) = %+v, ожидалась последняя успешная запись (Attempts=2, kept=7)", got)
	}
	if got := latest[1]; got.Model != "m1" || got.MeetingDate != "2024-02-16" {
		t.Errorf("порядок пар нарушен: latest[1] = %+v", got)
	}
	if got := latest[2]; got.Model != "m2" || got.Error != "timeout" {
		t.Errorf("последняя запись со сбоем потеряна: %+v", got)
	}
}

// TestMPLLMEvalJaccard — макро-среднее по (meeting_date, channel): пара с пустым
// объединением не учитывается, пара «есть только в одном множестве» даёт 0, точный Jaccard
// считается по текстам, а fuzzy — по вхождению (regex даёт целое предложение, модель может
// процитировать его фрагмент).
func TestMPLLMEvalJaccard(t *testing.T) {
	const (
		day1 = "2026-09-11"
		day2 = "2024-02-16"
	)
	a := map[mpEvalDocChannel]map[string]bool{
		{MeetingDate: day1, Channel: "labor_market"}: {"цитата A": true, "цитата B": true},
		{MeetingDate: day1, Channel: "expectations"}: {"цитата C": true},
		{MeetingDate: day2, Channel: "output_gap"}:   {},
	}
	b := map[mpEvalDocChannel]map[string]bool{
		{MeetingDate: day1, Channel: "labor_market"}: {"цитата B": true, "цитата D": true},
		{MeetingDate: day1, Channel: "expectations"}: {},
		{MeetingDate: day2, Channel: "output_gap"}:   {},
		{MeetingDate: day2, Channel: "money_supply"}: {"цитата E": true},
	}

	// labor_market: 1/3; expectations: 0 (в b пусто, объединение непусто); money_supply: 0
	// (только в b); output_gap: пусто с обеих сторон — пропущен. Среднее (1/3)/3 = 1/9.
	exact := mpEvalJaccard(a, b, false)
	if exact.Pairs != 3 {
		t.Errorf("пар %d, ожидалось 3 (пара с пустым объединением не учитывается)", exact.Pairs)
	}
	if want := (1.0 / 3.0) / 3.0; math.Abs(exact.Score-want) > 1e-9 {
		t.Errorf("Jaccard = %v, ожидалось %v", exact.Score, want)
	}

	// Fuzzy: длинный regex-текст содержит короткую цитату модели — совпадение есть.
	fuzzyA := map[mpEvalDocChannel]map[string]bool{
		{MeetingDate: day1, Channel: "labor_market"}: {"Дефицит работников сокращался.": true},
	}
	fuzzyB := map[mpEvalDocChannel]map[string]bool{
		{MeetingDate: day1, Channel: "labor_market"}: {"Дефицит работников сокращался. Безработица выросла.": true},
	}
	fuzzy := mpEvalJaccard(fuzzyA, fuzzyB, true)
	if fuzzy.Pairs != 1 || math.Abs(fuzzy.Score-1.0) > 1e-9 {
		t.Errorf("fuzzy Jaccard = %+v, ожидалось 1.0 на одной паре", fuzzy)
	}
	if exactFuzzy := mpEvalJaccard(fuzzyA, fuzzyB, false); math.Abs(exactFuzzy.Score) > 1e-9 {
		t.Errorf("точный Jaccard на разных текстах = %v, ожидалось 0", exactFuzzy.Score)
	}

	if empty := mpEvalJaccard(nil, nil, false); empty.Pairs != 0 || empty.Score != 0 {
		t.Errorf("пустые множества = %+v, ожидалось Pairs=0 Score=0", empty)
	}
}

// TestMPLLMEvalQuoteSets — множества цитат: тексты нормализуются (пробелы и NBSP), пустые
// пропускаются, повторы внутри пары схлопываются. Это вход Jaccard, и его ошибка тихо
// сдвинула бы все числа согласия.
func TestMPLLMEvalQuoteSets(t *testing.T) {
	rows := []mpEvalQuoteRow{
		{MeetingDate: "2026-09-11", Channel: "labor_market", Text: "  Дефицит   работников  "},
		{MeetingDate: "2026-09-11", Channel: "labor_market", Text: "Дефицит работников"},
		{MeetingDate: "2026-09-11", Channel: "labor_market", Text: ""},
	}
	sets := mpEvalQuoteSets(rows)
	key := mpEvalDocChannel{MeetingDate: "2026-09-11", Channel: "labor_market"}
	if len(sets[key]) != 1 {
		t.Errorf("текстов %d, ожидался 1: нормализация и дедуп множеством: %v", len(sets[key]), sets[key])
	}
	if !sets[key]["Дефицит работников"] {
		t.Errorf("нормализованный текст потерян: %v", sets[key])
	}
}

// TestMPLLMEvalCost — цена по тарифу за 1М токенов: вход и выход считаются раздельно,
// отсутствие тарифа («Set=false») обрабатывает вызывающий, а нулевой тариф — это ноль, а не
// «н/д».
func TestMPLLMEvalCost(t *testing.T) {
	cases := []struct {
		name              string
		in, out           int64
		priceIn, priceOut float64
		want              float64
	}{
		{"полный миллион", 1_000_000, 2_000_000, 1, 2, 5},
		{"дробный вход", 500_000, 0, 0.5, 0, 0.25},
		{"нулевой тариф", 1_000_000, 1_000_000, 0, 0, 0},
		{"пустой расход", 0, 0, 3, 15, 0},
	}
	for _, c := range cases {
		if got := mpEvalCost(c.in, c.out, c.priceIn, c.priceOut); math.Abs(got-c.want) > 1e-12 {
			t.Errorf("%s: mpEvalCost = %v, ожидалось %v", c.name, got, c.want)
		}
	}
}

// TestMPLLMEvalSpeedQuality — агрегаты скорости и качества: латентность считается только по
// успешным вызовам без повтора, токены — по всем записям, качество — по успешным. Сбой не
// должен разбавлять средние, а повтор — попадать в «чистую» латентность.
func TestMPLLMEvalSpeedQuality(t *testing.T) {
	records := []mpEvalRecord{
		{Model: "m", MeetingDate: "2026-09-11", Attempts: 1, DurationMS: 1000,
			InputTokens: 100, OutputTokens: 50, QuotesTotal: 10, QuotesKept: 9, QuotesDropped: 1,
			Notes: 9, ChannelsWithQuotes: 9},
		{Model: "m", MeetingDate: "2026-07-24", Attempts: 2, DurationMS: 3000,
			InputTokens: 200, OutputTokens: 100, QuotesTotal: 10, QuotesKept: 10, QuotesDropped: 0,
			Notes: 11, ChannelsWithQuotes: 11},
		{Model: "m", MeetingDate: "2026-06-19", Attempts: 1, DurationMS: 500, Error: "timeout"},
		{Model: "other", MeetingDate: "2026-09-11", Attempts: 1, DurationMS: 9999},
	}

	speed := mpEvalSpeed(records, "m")
	if speed.Calls != 3 || speed.Failures != 1 || speed.Retries != 1 {
		t.Errorf("speed = %+v, ожидалось вызовов 3, сбоев 1, повторов 1", speed)
	}
	if speed.LatencySamples != 1 || speed.TotalMS != 1000 || speed.MeanMS != 1000 || speed.P50MS != 1000 || speed.P95MS != 1000 {
		t.Errorf("латентность = %+v, ожидался один замер 1000 мс (ретрай и сбой не входят)", speed)
	}
	if speed.InputTokens != 300 || speed.OutputTokens != 150 {
		t.Errorf("токены = in %d / out %d, ожидалось 300/150 по всем записям", speed.InputTokens, speed.OutputTokens)
	}
	if math.Abs(speed.OutputTokensPerSecond-50) > 1e-9 {
		t.Errorf("output tokens/s = %v, ожидалось 50 (50 токенов за 1 с)", speed.OutputTokensPerSecond)
	}

	quality := mpEvalQuality(records, "m")
	if quality.Docs != 2 || quality.Failures != 1 {
		t.Errorf("quality = %+v, ожидалось документов 2, сбоев 1", quality)
	}
	if quality.QuotesTotal != 20 || quality.QuotesKept != 19 || quality.QuotesDropped != 1 {
		t.Errorf("цитаты = %d/%d/%d, ожидалось 20/19/1", quality.QuotesTotal, quality.QuotesKept, quality.QuotesDropped)
	}
	if math.Abs(quality.DropRate-0.05) > 1e-9 {
		t.Errorf("drop rate = %v, ожидалось 0.05", quality.DropRate)
	}
	if math.Abs(quality.MeanChannelsWithQuotes-10) > 1e-9 {
		t.Errorf("среднее каналов = %v, ожидалось 10", quality.MeanChannelsWithQuotes)
	}
	if quality.FullCoverage != 1 {
		t.Errorf("полное покрытие = %d, ожидался 1 документ", quality.FullCoverage)
	}
	if math.Abs(quality.MeanNotes-10) > 1e-9 {
		t.Errorf("среднее заметок = %v, ожидалось 10", quality.MeanNotes)
	}

	if models := mpEvalModelNames(records); len(models) != 2 || models[0] != "m" || models[1] != "other" {
		t.Errorf("модели = %v, ожидалось [m other] в порядке появления", models)
	}
}

// TestMPLLMEvalRecordJSON — контракт stats.jsonl: имена полей JSON — часть артефакта, по
// которому T6 считает отчёт. Обязательные поля спеки §3.4 пинятся все, включая пустой
// error: пропавшее поле молча обнулило бы метрику в отчёте.
func TestMPLLMEvalRecordJSON(t *testing.T) {
	data, err := json.Marshal(mpEvalRecord{})
	if err != nil {
		t.Fatalf("mpEvalRecord: json.Marshal: %v", err)
	}
	body := string(data)
	for _, key := range []string{
		"profile", "model", "meeting_date", "attempts", "duration_ms",
		"input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens",
		"quotes_total", "quotes_kept", "quotes_dropped", "notes", "channels_with_quotes", "rows", "error",
	} {
		if !strings.Contains(body, `"`+key+`":`) {
			t.Errorf("в записи stats.jsonl нет поля %q: %s", key, body)
		}
	}
}
