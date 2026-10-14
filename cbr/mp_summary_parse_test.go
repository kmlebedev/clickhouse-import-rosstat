package cbr

import (
	"os"
	"strings"
	"testing"
	"time"
)

// mpFixture читает HTML-фикстуру ЦБ из cbr/testdata (загружена живьём, см. task-2-report.md).
func mpFixture(t *testing.T, name string) []byte {
	t.Helper()
	page, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("чтение testdata/%s: %v", name, err)
	}
	return page
}

// mpBlockStat — счётчики узлов по одному блоку: абзацы и пункты списка.
type mpBlockStat struct {
	paragraphs int
	bullets    int
}

// mpBlockStats считает по блокам абзацы (paragraph) и пункты (bullet).
func mpBlockStats(blocks []mpSummaryBlock) map[string]mpBlockStat {
	out := make(map[string]mpBlockStat)
	for _, b := range blocks {
		st := out[b.Block]
		switch b.Kind {
		case "paragraph":
			st.paragraphs++
		case "bullet":
			st.bullets++
		}
		out[b.Block] = st
	}
	return out
}

// mpSubsectionRuns — непрерывные участки подзаголовков: пары (блок, подзаголовок) в порядке
// следования. Пустые подзаголовки (блоки 4–5) в результат не попадают.
func mpSubsectionRuns(blocks []mpSummaryBlock) [][2]string {
	var out [][2]string
	for _, b := range blocks {
		if b.Subsection == "" {
			continue
		}
		if n := len(out); n > 0 && out[n-1] == [2]string{b.Block, b.Subsection} {
			continue
		}
		out = append(out, [2]string{b.Block, b.Subsection})
	}
	return out
}

// mpCheckBlockCounts сверяет имена пяти блоков (в порядке следования) и счётчики узлов.
func mpCheckBlockCounts(t *testing.T, blocks []mpSummaryBlock, want map[string]mpBlockStat) {
	t.Helper()
	order := []string{}
	seen := map[string]bool{}
	for _, b := range blocks {
		if !seen[b.Block] {
			seen[b.Block] = true
			order = append(order, b.Block)
		}
	}
	if len(order) != len(want) {
		t.Errorf("блоков %d (%v), ожидалось %d", len(order), order, len(want))
	}
	for i, name := range mpSummaryBlockNames() {
		if i >= len(order) || order[i] != name {
			t.Errorf("блок %d: %q, ожидался %q (порядок следования)", i, order[i], name)
		}
	}
	got := mpBlockStats(blocks)
	for name, w := range want {
		g := got[name]
		if g.paragraphs != w.paragraphs || g.bullets != w.bullets {
			t.Errorf("блок %q: paragraph=%d bullet=%d, ожидалось paragraph=%d bullet=%d",
				name, g.paragraphs, g.bullets, w.paragraphs, w.bullets)
		}
	}
}

// mpSummaryBlockNames — ожидаемые имена пяти блоков в порядке страницы.
func mpSummaryBlockNames() []string {
	return []string{
		"Ситуация в экономике и инфляция",
		"Денежно-кредитные условия",
		"Внешние условия",
		"Инфляционные риски",
		"Выводы для денежно-кредитной политики и решение по ключевой ставке",
	}
}

// mpCheckSeq проверяет, что Seq в каждом блоке начинается с 1 и идёт подряд без пропусков.
func mpCheckSeq(t *testing.T, blocks []mpSummaryBlock) {
	t.Helper()
	last := map[string]int{}
	for i, b := range blocks {
		if b.Seq != last[b.Block]+1 {
			t.Errorf("запись %d (блок %q): Seq=%d, ожидался %d", i, b.Block, b.Seq, last[b.Block]+1)
		}
		last[b.Block] = b.Seq
	}
}

// mpCheckNormalized проверяет, что в нормализованных текстах не осталось экзотических
// пробелов и неразрывного дефиса (иначе не сойдутся цитаты LLM-слоя).
func mpCheckNormalized(t *testing.T, blocks []mpSummaryBlock) {
	t.Helper()
	for _, b := range blocks {
		for _, bad := range []string{"\u00a0", "\u202f", "\u200a", "\u2011"} {
			if strings.Contains(b.Text, bad) {
				t.Errorf("блок %q seq=%d: в тексте остался %q: %q", b.Block, b.Seq, bad, b.Text)
			}
		}
	}
}

// mpFetchHref достаёт href единственной ссылки-заголовка блока резюме (для пинов состава).
func mpFetchHref(t *testing.T, blocks []mpSummaryBlock, block, kind string, seq int) string {
	t.Helper()
	for _, b := range blocks {
		if b.Block == block && b.Kind == kind && b.Seq == seq {
			return b.Text
		}
	}
	t.Fatalf("не найдена запись block=%q kind=%q seq=%d", block, kind, seq)
	return ""
}

// TestNormMPText — нормализация текста резюме: NBSP, узкий NBSP, «волосяной» пробел
// (U+200A — фактически присутствует в фикстурах вокруг тире), неразрывный дефис U+2011,
// схлопывание пробельных серий и обрезка краёв.
func TestNormMPText(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		// Пары символов из фикстур (23092026: «на\u00a0уровне 14,00% годовых»).
		{"на\u00a0уровне", "на уровне"},
		{"в\u00a0том\u00a0числе", "в том числе"},
		// 23092026: «диапазон 5\u200a–\u200a6%\u00a0с.к.г.» — волосяной пробел + NBSP.
		{"5\u200a–\u200a6%\u00a0с.к.г.", "5 – 6% с.к.г."},
		// 07052026: «2026\u200a–\u200a2027\u00a0годы».
		{"2026\u200a–\u200a2027\u00a0годы", "2026 – 2027 годы"},
		// Узкий неразрывный пробел (U+202F) — по контракту интерфейса (в фикстурах нет).
		{"5,3\u202f%", "5,3 %"},
		// Неразрывный дефис U+2011 (23092026: «по‑прежнему», «из‑за»).
		{"по\u2011прежнему", "по-прежнему"},
		{"из\u2011за", "из-за"},
		// Схлопывание серий пробельных символов и обрезка краёв.
		{"  двойной   пробел\tи\nперевод  ", "двойной пробел и перевод"},
		{"", ""},
	}
	for _, c := range cases {
		if got := normMPText(c.in); got != c.want {
			t.Errorf("normMPText(%q) = %q, ожидалось %q", c.in, got, c.want)
		}
	}
}

// TestParseMPDecIndexFixture — разбор индекса материалов заседаний: ровно 22 карточки
// «Резюме обсуждения ключевой ставки» в порядке страницы (от новых к старым), дата
// публикации — из document-regular_date, href — относительный, как в разметке.
func TestParseMPDecIndexFixture(t *testing.T) {
	refs, err := parseMPDecIndex(mpFixture(t, "mp_summary_index.html"))
	if err != nil {
		t.Fatalf("parseMPDecIndex: %v", err)
	}
	if len(refs) != 22 {
		t.Fatalf("ссылок %d, ожидалось 22", len(refs))
	}
	wantDates := []time.Time{
		utcDate(2026, time.September, 23), utcDate(2026, time.August, 5),
		utcDate(2026, time.July, 1), utcDate(2026, time.May, 7),
		utcDate(2026, time.April, 1), utcDate(2026, time.February, 26),
		utcDate(2025, time.December, 29), utcDate(2025, time.November, 6),
		utcDate(2025, time.September, 24), utcDate(2025, time.August, 6),
		utcDate(2025, time.June, 20), utcDate(2025, time.May, 12),
		utcDate(2025, time.April, 2), utcDate(2025, time.February, 26),
		utcDate(2024, time.December, 28), utcDate(2024, time.November, 6),
		utcDate(2024, time.September, 25), utcDate(2024, time.August, 7),
		utcDate(2024, time.June, 20), utcDate(2024, time.May, 13),
		utcDate(2024, time.April, 1), utcDate(2024, time.February, 27),
	}
	for i, ref := range refs {
		if !ref.Published.Equal(wantDates[i]) {
			t.Errorf("ссылка %d: дата публикации %s, ожидалась %s",
				i, ref.Published.Format("2006-01-02"), wantDates[i].Format("2006-01-02"))
		}
		if !strings.Contains(ref.Href, "summary_key_rate_") {
			t.Errorf("ссылка %d: href %q без summary_key_rate_", i, ref.Href)
		}
	}
	if got := refs[0].Href; got != "/dkp/mp_dec/decision_key_rate/summary_key_rate_23092026/" {
		t.Errorf("первая ссылка: %q, ожидалась summary_key_rate_23092026", got)
	}
	if got := refs[21].Href; got != "/dkp/mp_dec/decision_key_rate/summary_key_rate_27022024/" {
		t.Errorf("последняя ссылка: %q, ожидалась summary_key_rate_27022024", got)
	}
}

// TestParseMPSummaryHold — резюме 23.09.2026 (публикация 2026-09-23): решение «сохранил
// на уровне 14,00%», даты вступления нет, сигнал — «не давать сигнала». Числа узлов по
// блокам — фактические (Task 2 §3.1): в блоках 1–3 по паре подзаголовков div.title_20,
// в блоках 4–5 подзаголовков нет.
func TestParseMPSummaryHold(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_23092026.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	if !doc.MeetingDate.Equal(utcDate(2026, time.September, 11)) {
		t.Errorf("MeetingDate = %s, ожидалось 2026-09-11", doc.MeetingDate.Format("2006-01-02"))
	}
	if doc.Action != "hold" {
		t.Errorf("Action = %q, ожидалось hold", doc.Action)
	}
	if doc.Rate != 14.0 {
		t.Errorf("Rate = %v, ожидалось 14", doc.Rate)
	}
	if doc.EffectiveFrom != nil {
		t.Errorf("EffectiveFrom = %s, ожидалось nil (у hold даты вступления нет)",
			doc.EffectiveFrom.Format("2006-01-02"))
	}
	if !strings.Contains(doc.Signal, "не давать сигнала") {
		t.Errorf("Signal = %q, ожидалась фраза «не давать сигнала»", doc.Signal)
	}
	lines := strings.Split(doc.Signal, "\n")
	if len(lines) != 2 {
		t.Errorf("Signal: %d предложений, ожидалось 2: %q", len(lines), doc.Signal)
	}
	for _, line := range lines {
		if !strings.Contains(strings.ToLower(line), "сигнал") {
			t.Errorf("Signal: предложение без слова «сигнал»: %q", line)
		}
	}
	if strings.Contains(doc.Signal, "По итогам дискуссии") {
		t.Errorf("Signal захватил решение целиком: %q", doc.Signal)
	}

	mpCheckBlockCounts(t, doc.Blocks, map[string]mpBlockStat{
		"Ситуация в экономике и инфляция": {paragraphs: 14},
		"Денежно-кредитные условия":       {paragraphs: 10, bullets: 3},
		"Внешние условия":                 {paragraphs: 7},
		"Инфляционные риски":              {paragraphs: 3, bullets: 5},
		"Выводы для денежно-кредитной политики и решение по ключевой ставке": {paragraphs: 4, bullets: 4},
	})
	mpCheckSeq(t, doc.Blocks)
	mpCheckNormalized(t, doc.Blocks)

	// Подзаголовки: ровно шесть непрерывных участков div.title_20 — по паре
	// «Основные факты»/«Дискуссия» в блоках 1–3; в блоках 4–5 их нет.
	runs := mpSubsectionRuns(doc.Blocks)
	wantRuns := [][2]string{
		{"Ситуация в экономике и инфляция", "Основные факты"},
		{"Ситуация в экономике и инфляция", "Дискуссия"},
		{"Денежно-кредитные условия", "Основные факты"},
		{"Денежно-кредитные условия", "Дискуссия"},
		{"Внешние условия", "Основные факты"},
		{"Внешние условия", "Дискуссия"},
	}
	if len(runs) != len(wantRuns) {
		t.Fatalf("участков подзаголовков %d (%v), ожидалось %d", len(runs), runs, len(wantRuns))
	}
	for i, want := range wantRuns {
		if runs[i] != want {
			t.Errorf("участок %d = %v, ожидался %v", i, runs[i], want)
		}
	}
	// Пины Task 2 §3.1: в блоке 1 «Основные факты» — один абзац, «Дискуссия» — 13.
	sub := map[[2]string]int{}
	for _, b := range doc.Blocks {
		sub[[2]string{b.Block, b.Subsection}]++
	}
	if got := sub[[2]string{"Ситуация в экономике и инфляция", "Основные факты"}]; got != 1 {
		t.Errorf("блок 1 «Основные факты»: %d записей, ожидалось 1", got)
	}
	if got := sub[[2]string{"Ситуация в экономике и инфляция", "Дискуссия"}]; got != 13 {
		t.Errorf("блок 1 «Дискуссия»: %d записей, ожидалось 13", got)
	}

	// Порядок узлов блока 5: абзац, четыре пункта, три абзаца (решение — последний абзац).
	wantKinds := []string{"paragraph", "bullet", "bullet", "bullet", "bullet", "paragraph", "paragraph", "paragraph"}
	var kinds []string
	for _, b := range doc.Blocks {
		if b.Block == "Выводы для денежно-кредитной политики и решение по ключевой ставке" {
			kinds = append(kinds, b.Kind)
		}
	}
	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") {
		t.Errorf("виды узлов блока 5 = %v, ожидалось %v", kinds, wantKinds)
	}
	if first := mpFetchHref(t, doc.Blocks, "Ситуация в экономике и инфляция", "paragraph", 1); !strings.HasPrefix(first, "В июле текущий рост цен увеличился") {
		t.Errorf("первый абзац блока 1 = %q", first)
	}
}

// TestParseMPSummaryHike — резюме 06.11.2024: «повысил … до 21,00% годовых с 28 октября
// 2024 года и дал направленный сигнал». В фикстуре часть <li> закрыта </p> — html-парсер
// создаёт из них пустые <p>, которые не должны попадать ни в текст, ни в счётчики
// (пины Task 2 §3.1 — открывающие <p>/<li>).
func TestParseMPSummaryHike(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_06112024.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	if !doc.MeetingDate.Equal(utcDate(2024, time.October, 25)) {
		t.Errorf("MeetingDate = %s, ожидалось 2024-10-25", doc.MeetingDate.Format("2006-01-02"))
	}
	if doc.Action != "hike" {
		t.Errorf("Action = %q, ожидалось hike", doc.Action)
	}
	if doc.Rate != 21.0 {
		t.Errorf("Rate = %v, ожидалось 21", doc.Rate)
	}
	if doc.EffectiveFrom == nil {
		t.Fatalf("EffectiveFrom = nil, ожидалось 2024-10-28")
	}
	if !doc.EffectiveFrom.Equal(utcDate(2024, time.October, 28)) {
		t.Errorf("EffectiveFrom = %s, ожидалось 2024-10-28", doc.EffectiveFrom.Format("2006-01-02"))
	}
	if !strings.Contains(doc.Signal, "направленный сигнал") {
		t.Errorf("Signal = %q, ожидалась фраза «направленный сигнал»", doc.Signal)
	}

	mpCheckBlockCounts(t, doc.Blocks, map[string]mpBlockStat{
		"Ситуация в экономике и инфляция": {paragraphs: 11},
		"Денежно-кредитные условия":       {paragraphs: 6, bullets: 4},
		"Внешние условия":                 {paragraphs: 6},
		"Инфляционные риски":              {paragraphs: 3, bullets: 5},
		"Выводы для денежно-кредитной политики и решение по ключевой ставке": {paragraphs: 10, bullets: 4},
	})
	mpCheckSeq(t, doc.Blocks)
	mpCheckNormalized(t, doc.Blocks)
	for _, b := range doc.Blocks {
		if b.Text == "" {
			t.Errorf("блок %q seq=%d: пустой текст (пустые узлы p/li должны пропускаться)", b.Block, b.Seq)
		}
	}
}

// TestParseMPSummaryCut — резюме 07.05.2026 (публикация 2026-05-07, заседание 2026-04-24):
// «снизил ключевую ставку на 50 б.п., до 14,50% годовых» — между «ставку» и «до» стоит
// «на 50 б.п.,»; даты вступления в тексте нет (EffectiveFrom = nil).
func TestParseMPSummaryCut(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_07052026.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	if !doc.MeetingDate.Equal(utcDate(2026, time.April, 24)) {
		t.Errorf("MeetingDate = %s, ожидалось 2026-04-24", doc.MeetingDate.Format("2006-01-02"))
	}
	if doc.Action != "cut" {
		t.Errorf("Action = %q, ожидалось cut", doc.Action)
	}
	if doc.Rate != 14.5 {
		t.Errorf("Rate = %v, ожидалось 14.5", doc.Rate)
	}
	if doc.EffectiveFrom != nil {
		t.Errorf("EffectiveFrom = %s, ожидалось nil (дата вступления не указана)",
			doc.EffectiveFrom.Format("2006-01-02"))
	}
	if !strings.Contains(doc.Signal, "При обсуждении сигнала") {
		t.Errorf("Signal = %q, ожидалось предложение «При обсуждении сигнала…»", doc.Signal)
	}
	if strings.Contains(doc.Signal, "Дальнейшие решения по ключевой ставке") {
		t.Errorf("Signal захватил предложение без слова «сигнал»: %q", doc.Signal)
	}

	mpCheckBlockCounts(t, doc.Blocks, map[string]mpBlockStat{
		"Ситуация в экономике и инфляция": {paragraphs: 13, bullets: 2},
		"Денежно-кредитные условия":       {paragraphs: 8, bullets: 7},
		"Внешние условия":                 {paragraphs: 6},
		"Инфляционные риски":              {paragraphs: 3, bullets: 5},
		"Выводы для денежно-кредитной политики и решение по ключевой ставке": {paragraphs: 8, bullets: 9},
	})
	mpCheckSeq(t, doc.Blocks)
	mpCheckNormalized(t, doc.Blocks)
}

// TestParseMPSummaryDecisionFallback — если формулировка решения отличается от закреплённой
// («Совет директоров Банка России <дата> года <глагол>…»), глагол и ставка берутся из
// предложения с глаголом, а дата заседания — из фразы-дублёра в шапке («основаны на данных,
// доступных к <дата> года»).
func TestParseMPSummaryDecisionFallback(t *testing.T) {
	page := mpFixture(t, "mp_summary_23092026.html")
	mutated := strings.ReplaceAll(string(page),
		"Совет директоров Банка России 11&nbsp;сентября 2026 года сохранил",
		"Банк России сохранил")
	if mutated == string(page) {
		t.Fatal("мутация не применилась: не найдена формулировка решения")
	}
	doc, err := parseMPSummary([]byte(mutated))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	if !doc.MeetingDate.Equal(utcDate(2026, time.September, 11)) {
		t.Errorf("MeetingDate = %s, ожидалось 2026-09-11 (из фразы «данных, доступных к…»)",
			doc.MeetingDate.Format("2006-01-02"))
	}
	if doc.Action != "hold" || doc.Rate != 14.0 {
		t.Errorf("Action/Rate = %q/%v, ожидалось hold/14", doc.Action, doc.Rate)
	}
}

// TestParseMPSummaryLevelSet — резюме 01.04.2024 (заседание 22.03.2024): решение в форме
// «принял решение установить ставку на уровне 16,00% годовых с 25 марта 2024 года» — в
// закреплённом ядре нет глагола направления. Направление читается по обороту «на уровне»
// (о неизменном уровне; о смене ЦБ пишет «до»), поэтому действие — hold, а «с 25 марта»
// отбрасывается, как у любого hold. Форма найдена живой приёмкой 2026-10-14 (шаг 1.2):
// без неё импорт падал на двух страницах 2024 года — 01042024 и 27022024.
func TestParseMPSummaryLevelSet(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_01042024.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	if !doc.MeetingDate.Equal(utcDate(2024, time.March, 22)) {
		t.Errorf("MeetingDate = %s, ожидалось 2024-03-22", doc.MeetingDate.Format("2006-01-02"))
	}
	if doc.Action != "hold" {
		t.Errorf("Action = %q, ожидалось hold", doc.Action)
	}
	if doc.Rate != 16.0 {
		t.Errorf("Rate = %v, ожидалось 16", doc.Rate)
	}
	if doc.EffectiveFrom != nil {
		t.Errorf("EffectiveFrom = %s, ожидалось nil (у hold даты вступления нет)",
			doc.EffectiveFrom.Format("2006-01-02"))
	}
	if !strings.Contains(doc.Signal, "не следует давать дополнительный направленный сигнал") {
		t.Errorf("Signal = %q, ожидалась фраза «не следует давать дополнительный направленный сигнал»", doc.Signal)
	}

	mpCheckBlockCounts(t, doc.Blocks, map[string]mpBlockStat{
		"Ситуация в экономике и инфляция": {paragraphs: 10},
		"Денежно-кредитные условия":       {paragraphs: 7, bullets: 5},
		"Внешние условия":                 {paragraphs: 5, bullets: 2},
		"Инфляционные риски":              {paragraphs: 3, bullets: 5},
		"Выводы для денежно-кредитной политики и решение по ключевой ставке": {paragraphs: 7, bullets: 7},
	})
	mpCheckSeq(t, doc.Blocks)
	mpCheckNormalized(t, doc.Blocks)
}

// TestParseMPFailFast — разбор падает (не отдаёт частичные данные) на каждом классе
// дефектов: пропал один из пяти h2-блоков, не распознано решение, пустой индекс, блок без
// абзацев. Ошибка называет место дефекта — иначе подтест прошёл бы на любой другой ошибке.
func TestParseMPFailFast(t *testing.T) {
	fail := func(t *testing.T, err error, wantIn string) {
		t.Helper()
		if err == nil {
			t.Fatalf("ожидалась ошибка, получено nil (упоминание %q)", wantIn)
		}
		if !strings.Contains(err.Error(), wantIn) {
			t.Errorf("ошибка %q не упоминает %q", err, wantIn)
		}
	}

	t.Run("RenamedBlock", func(t *testing.T) {
		page := strings.ReplaceAll(string(mpFixture(t, "mp_summary_23092026.html")),
			"Инфляционные риски", "Инфляционные риски и их динамика")
		_, err := parseMPSummary([]byte(page))
		fail(t, err, "Инфляционные риски")
	})

	t.Run("DecisionRemoved", func(t *testing.T) {
		page := strings.ReplaceAll(string(mpFixture(t, "mp_summary_23092026.html")),
			"сохранил", "оставил")
		_, err := parseMPSummary([]byte(page))
		fail(t, err, "решение")
	})

	t.Run("LevelSetWithoutLevel", func(t *testing.T) {
		page := mpFixture(t, "mp_summary_01042024.html")
		mutated := strings.ReplaceAll(string(page),
			"принял решение установить ставку на&nbsp;уровне",
			"принял решение установить ставку до&nbsp;уровня")
		if mutated == string(page) {
			t.Fatal("мутация не применилась: не найдена формулировка решения")
		}
		_, err := parseMPSummary([]byte(mutated))
		fail(t, err, "решение")
	})

	t.Run("EmptyIndex", func(t *testing.T) {
		refs, err := parseMPDecIndex([]byte(""))
		fail(t, err, "индекс")
		if refs != nil {
			t.Errorf("при ошибке вернулись ссылки: %v", refs)
		}
		fail(t, err, "Резюме обсуждения ключевой ставки")
	})

	t.Run("NoParagraphs", func(t *testing.T) {
		var sb strings.Builder
		sb.WriteString("<html><body><h1>Резюме обсуждения ключевой ставки</h1>")
		for _, name := range mpSummaryBlockNames() {
			sb.WriteString("<h2><span>" + name + "</span></h2>")
		}
		sb.WriteString("</body></html>")
		_, err := parseMPSummary([]byte(sb.String()))
		fail(t, err, "Ситуация в экономике и инфляция")
	})
}
